package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	configLock   sync.Mutex
	historyLock  sync.Mutex
	lastResult   [][]string
)

func main() {
	// Prompt user for username & connection info
	fmt.Print("Enter your username: ")
	fmt.Scanln(&username)

	cfg, _ := loadConfig(username)
	if cfg == nil {
		cfg = &Config{}
		cfg.DBUser = username

		// Prompt for connection values
		fmt.Print("Enter ClickHouse host (default: localhost): ")
		fmt.Scanln(&cfg.Host)
		if cfg.Host == "" {
			cfg.Host = "localhost"
		}

		fmt.Print("Enter port (default: 9000): ")
		fmt.Scanln(&cfg.Port)
		if cfg.Port == "" {
			cfg.Port = "9000"
		}

		fmt.Print("Enter database (default: default): ")
		fmt.Scanln(&cfg.Database)
		if cfg.Database == "" {
			cfg.Database = "default"
		}

		// fmt.Print("Enter DB username (default: default): ")
		// fmt.Scanln(&cfg.DBUser)
		// if cfg.DBUser == "" {
		// 	cfg.DBUser = "default"
		// }

		fmt.Print("Enter DB password: ")
		fmt.Scanln(&cfg.Password)

		fmt.Print("Use TLS? (y/n): ")
		var tlsAnswer string
		fmt.Scanln(&tlsAnswer)
		cfg.UseTLS = strings.ToLower(tlsAnswer) == "y"

		if cfg.UseTLS {
			fmt.Print("Enter CA file path (optional): ")
			fmt.Scanln(&cfg.CAFilePath)
		}

		// Save config to <username>.toml
		saveConfig(username, cfg)
	}

	config = cfg
	if config.QueryHistory != nil {
		queryHistory = config.QueryHistory
	}

	// Connect and start UI
	connectAndStartUI()
}

func connectAndStartUI() {
	// log.Print("inside connectAndStartUI")
	app = tview.NewApplication()
	// log.Print("created tview application")

	addr := fmt.Sprintf("%s:%s", config.Host, config.Port)
	var tlsConfig *tls.Config
	if config.UseTLS {
		if config.CAFilePath != "" {
			caCert, err := os.ReadFile(config.CAFilePath)
			if err != nil {
				log.Fatalf("Failed to read CA file: %v", err)
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(caCert) {
				log.Fatal("Invalid CA file")
			}
			tlsConfig = &tls.Config{RootCAs: certPool}
		} else {
			tlsConfig = &tls.Config{InsecureSkipVerify: true}
		}
	}

	options := &clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.DBUser,
			Password: config.Password,
		},
		TLS: tlsConfig,
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatalf("Connection failed: %v", err)
	}

	go showQueryUI(conn)

	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}

func showQueryUI(conn clickhouse.Conn) {
	// log.Print("inside showQueryUI")
	input := tview.NewInputField().SetLabel("Query: ").SetFieldWidth(0)
	status := tview.NewTextView().SetDynamicColors(true).SetChangedFunc(func() { app.Draw() })
	table := tview.NewTable().SetBorders(false)
	historyList := tview.NewList().ShowSecondaryText(false)

	refreshHistoryList(historyList, input)

	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch {
		case event.Modifiers() == tcell.ModCtrl && event.Rune() == 'r':
			runQuery(conn, input, table, status)
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

	input.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			runQuery(conn, input, table, status)
		}
	})

	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(input, 1, 0, true).
		AddItem(tview.NewFlex().
			AddItem(historyList, 30, 0, false).
			AddItem(table, 0, 1, false), 0, 1, false).
		AddItem(status, 2, 0, false)

	app.SetRoot(layout, true).SetFocus(input)
}

func runQuery(conn clickhouse.Conn, input *tview.InputField, table *tview.Table, status *tview.TextView) {
	// log.Print("inside runQuery")
	query := strings.TrimSpace(input.GetText())
	if query == "" {
		return
	}
	addToHistory(query)
	refreshHistoryList(nil, input)
	saveHistory()

	status.Clear()
	table.Clear()

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
		lastResult = [][]string{columns}
		app.QueueUpdateDraw(func() {
			for colIdx, col := range columns {
				table.SetCell(0, colIdx, tview.NewTableCell(fmt.Sprintf("[::b]%s", col)).SetSelectable(false))
			}
		})

		rowNum := 1
		for rows.Next() {
			values := rows.ColumnTypes()

			scanTargets := make([]interface{}, len(values))
			for i, col := range values {
				// fallback: use string
				scanTargets[i] = &col
			}

			if err := rows.Scan(scanTargets...); err != nil {
				app.QueueUpdateDraw(func() {
					fmt.Fprintf(status, "[red]Scan error: %v", err)
				})
				continue
			}

			rowStr := make([]string, len(scanTargets))
			for i, v := range scanTargets {
				val := *(v.(*string))
				rowStr[i] = val
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


		if rows.Err() != nil {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(status, "[red]Rows error: %v", rows.Err())
			})
		} else {
			app.QueueUpdateDraw(func() {
				fmt.Fprintf(status, "[green]Query finished. %d rows.", rowNum-1)
			})
		}
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

func refreshHistoryList(list *tview.List, input *tview.InputField) {
	// log.Print("inside refreshHistoryList")
	historyLock.Lock()
	defer historyLock.Unlock()
	if list != nil {
		app.QueueUpdateDraw(func() {
			list.Clear()
			for _, q := range queryHistory {
				query := q
				list.AddItem(query, "", 0, func() {
					input.SetText(query)
					app.SetFocus(input)
				})
			}
		})
	}
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

func configFilePath(username string) string {
	// home, _ := os.UserHomeDir()
	// return filepath.Join(home, fmt.Sprintf("%s.toml", username))
	return filepath.Join(fmt.Sprintf("%s.toml", username))
}

func loadConfig(username string) (*Config, error) {
	data, err := os.ReadFile(configFilePath(username))
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = toml.Unmarshal(data, &cfg)
	return &cfg, err
}

func saveConfig(username string, cfg *Config) error {
	data, err := toml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(configFilePath(username), data, 0600)
}
