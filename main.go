package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gdamore/tcell/v2"
	"github.com/pelletier/go-toml/v2"
	"github.com/rivo/tview"
)

type Config struct {
	Host         string
	Port         string
	Database     string
	DBUser       string
	Password     string
	UseTLS       bool
	CAFilePath   string
	QueryHistory []string
}

var (
	app          *tview.Application
	config       *Config
	username     string
	queryHistory []string
	historyIndex int
	lastResult   [][]string
	configLock   sync.Mutex
	historyLock  sync.Mutex
)

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter your username: ")
	username, _ = reader.ReadString('\n')
	username = strings.TrimSpace(username)

	cfg, _ := loadConfig(username)
	if cfg == nil {
		cfg = promptConfig(reader)
		saveConfig(username, cfg)
	}

	config = cfg
	if config.QueryHistory != nil {
		queryHistory = config.QueryHistory
	}

	connectAndStartUI()
}

func promptConfig(reader *bufio.Reader) *Config {
	cfg := &Config{DBUser: username}

	fmt.Print("Enter ClickHouse host (default: localhost): ")
	host, _ := reader.ReadString('\n')
	cfg.Host = defaultIfEmpty(strings.TrimSpace(host), "localhost")

	fmt.Print("Enter port (default: 9000): ")
	port, _ := reader.ReadString('\n')
	cfg.Port = defaultIfEmpty(strings.TrimSpace(port), "9000")

	fmt.Print("Enter database (default: default): ")
	database, _ := reader.ReadString('\n')
	cfg.Database = defaultIfEmpty(strings.TrimSpace(database), "default")

	fmt.Print("Enter DB password: ")
	password, _ := reader.ReadString('\n')
	cfg.Password = strings.TrimSpace(password)

	fmt.Print("Use TLS? (y/n): ")
	tlsAnswer, _ := reader.ReadString('\n')
	cfg.UseTLS = strings.ToLower(strings.TrimSpace(tlsAnswer)) == "y"

	if cfg.UseTLS {
		fmt.Print("Enter CA file path (optional): ")
		caPath, _ := reader.ReadString('\n')
		cfg.CAFilePath = strings.TrimSpace(caPath)
	}

	return cfg
}

func defaultIfEmpty(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func connectAndStartUI() {
	app = tview.NewApplication()

	addr := fmt.Sprintf("%s:%s", config.Host, config.Port)
	var tlsConfig *tls.Config
	if config.UseTLS {
		if config.CAFilePath != "" {
			caCert, err := os.ReadFile(config.CAFilePath)
			if err != nil {
				log.Fatalf("Failed to read CA file: %v", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				log.Fatal("Invalid CA file")
			}
			tlsConfig = &tls.Config{RootCAs: pool}
		} else {
			tlsConfig = &tls.Config{InsecureSkipVerify: true}
		}
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.DBUser,
			Password: config.Password,
		},
		TLS: tlsConfig,
	})
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}

	showUI(conn)
}

func showUI(conn clickhouse.Conn) {
	table := tview.NewTable().SetBorders(false).SetSelectable(true, true) // Enable horizontal and vertical scrolling
	input := tview.NewInputField().SetLabel("Query: ").SetFieldWidth(0)
	status := tview.NewTextView().SetDynamicColors(true).SetChangedFunc(func() { app.Draw() })

	historyIndex = -1

	input.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			runQuery(conn, input.GetText(), table, status)
			input.SetText("")
		}
	})

	focusOnTable := false
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch {
		case event.Key() == tcell.KeyTAB:
			focusOnTable = !focusOnTable
			if focusOnTable {
				app.SetFocus(table)
			} else {
				app.SetFocus(input)
			}
			return nil

		case event.Key() == tcell.KeyUp && !focusOnTable:
			navigateHistory(input, -1)
			return nil

		case event.Key() == tcell.KeyDown && !focusOnTable:
			navigateHistory(input, 1)
			return nil

		case event.Modifiers() == tcell.ModCtrl && event.Rune() == 'r':
			navigateHistory(input, -1)
			return nil

		case event.Modifiers() == tcell.ModCtrl && event.Rune() == 'e':
			exportCSV()
			return nil

		case event.Modifiers() == tcell.ModCtrl && event.Rune() == 'q':
			app.Stop()
			return nil
		}
		return event
	})

	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(table, 0, 1, false).
		AddItem(input, 1, 0, true).
		AddItem(status, 2, 0, false)

	app.SetRoot(layout, true).SetFocus(input)
	
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}

func runQuery(conn clickhouse.Conn, query string, table *tview.Table, status *tview.TextView) {
	query = strings.TrimSpace(query)
	if query == "" {
		return
	}

	go addToHistory(query)
	go saveHistory()

	table.Clear()
	status.Clear()
	start := time.Now()

	go func() {
		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(status, "[red]Query error: %v", err)
			})
			return
		}
		defer rows.Close()

		columns := rows.Columns()
		colTypes := rows.ColumnTypes()
		lastResult = [][]string{columns}

		app.QueueUpdateDraw(func() {
			for i, col := range columns {
				table.SetCell(0, i, tview.NewTableCell(fmt.Sprintf("[::b]%s", col)))
			}
		})

		rowNum := 1
		var scanTargets []interface{}

		for rows.Next() {
			// Initialize scanTargets on first row
			if scanTargets == nil {
				scanTargets = make([]interface{}, len(colTypes))
				for i, ct := range colTypes {
					scanTargets[i] = reflect.New(ct.ScanType()).Interface()
				}
			}

			if err := rows.Scan(scanTargets...); err != nil {
				app.QueueUpdateDraw(func() {
					fmt.Fprintf(status, "[red]Scan error: %v", err)
				})
				continue
			}

			rowStr := make([]string, len(scanTargets))
			for i, v := range scanTargets {
				val := reflect.ValueOf(v)
				if val.Kind() == reflect.Ptr {
					if val.IsNil() {
						rowStr[i] = ""
					} else {
						rowStr[i] = fmt.Sprintf("%v", val.Elem().Interface())
					}
				} else {
					rowStr[i] = fmt.Sprintf("%v", val.Interface())
				}
			}

			lastResult = append(lastResult, rowStr)
			current := rowNum
			app.QueueUpdateDraw(func() {
				for colIdx, cell := range rowStr {
					table.SetCell(current, colIdx, tview.NewTableCell(cell))
				}
			})
			rowNum++
		}

		if err := rows.Err(); err != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(status, "[red]Rows error: %v", err)
			})
		} else {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(status, "[green]Query finished. %d rows in %v", rowNum-1, time.Since(start))
			})
		}
	}()
}

func navigateHistory(input *tview.InputField, dir int) {
	historyLock.Lock()
	defer historyLock.Unlock()

	if len(queryHistory) == 0 {
		return
	}

	historyIndex += dir
	if historyIndex < 0 {
		historyIndex = 0
	} else if historyIndex >= len(queryHistory) {
		historyIndex = len(queryHistory) - 1
	}

	input.SetText(queryHistory[historyIndex])
}

func addToHistory(query string) {
	historyLock.Lock()
	defer historyLock.Unlock()

	for _, q := range queryHistory {
		if q == query {
			return
		}
	}

	queryHistory = append([]string{query}, queryHistory...)
	if len(queryHistory) > 50 {
		queryHistory = queryHistory[:50]
	}
	configLock.Lock()
	config.QueryHistory = queryHistory
	configLock.Unlock()
}

func saveHistory() {
	go func() {
		configLock.Lock()
		defer configLock.Unlock()
		saveConfig(username, config)
	}()
}

func exportCSV() {
	if len(lastResult) == 0 {
		log.Println("No results to export.")
		return
	}

	filename := fmt.Sprintf("results_%s.csv", time.Now().Format("20060102_150405"))
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	for _, row := range lastResult {
		writer.Write(row)
	}
	writer.Flush()
	log.Printf("Exported to %s", filename)
}

func configFilePath(username string) string {
	return fmt.Sprintf("%s.toml", username)
}

func loadConfig(username string) (*Config, error) {
	data, err := os.ReadFile(configFilePath(username))
	if err != nil {
		return nil, err
	}
	var cfg Config
	return &cfg, toml.Unmarshal(data, &cfg)
}

func saveConfig(username string, cfg *Config) error {
	data, err := toml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(configFilePath(username), data, 0600)
}
