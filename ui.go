package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func (ui *ClickHouseUI) Run() error {
	ui.app = tview.NewApplication()
	ui.setupUI()
	return ui.app.Run()
}

func (ui *ClickHouseUI) setupUI() {
	// Create input, status, table, layout (refactor of showUI logic)
	// Set input handlers and global key handlers
	ui.table = tview.NewTable().SetBorders(false).SetSelectable(true, true).SetBorders(true).SetSeparator(rune('|')) // Enable horizontal and vertical scrolling
	ui.status = tview.NewTextView().SetDynamicColors(true).SetChangedFunc(func() { ui.app.Draw() })
	ui.setInput()

	ui.focusTable = false
	ui.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch {
		case event.Key() == tcell.KeyTAB:
			ui.toggleFocus()
			return nil

		case event.Rune() == 5: // Ctrl+E
			go ui.exportCSV()
			return nil

		case event.Rune() == 6: // Ctrl+F
			ui.showHistory()
			return nil
		}
		return event
	})

	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(ui.input, 1, 0, true).
		AddItem(ui.table, 0, 1, false).
		AddItem(ui.status, 2, 0, false)

	ui.app.SetRoot(layout, true).SetFocus(ui.input)

}

func (ui *ClickHouseUI) setInput() {
	ui.input = tview.NewInputField().SetLabel("Query: ").SetFieldWidth(0).SetAutocompleteFunc(func(currentText string) (entries []string) {
		ui.status.Clear()
		// ui.status.SetText(fmt.Sprintf("%v",ui.history.GetSuggestion(currentText)))
		return ui.history.GetQuery(currentText)
	}).SetAutocompletedFunc(func(text string, index, source int) bool {
		ui.status.SetText(strconv.Itoa(source))
		if source != 2 {
			return false
		}

		input := strings.TrimSpace(ui.input.GetText())
		inputForCheck := strings.ToLower(input)
		textForCheck := strings.ToLower(text)

		// If the full suggestion starts with the full input, just complete it
		if strings.HasPrefix(textForCheck, inputForCheck) {
			ui.input.SetText(text)
			return inputForCheck == textForCheck
		}

		// Safely replace the last word
		words := strings.Fields(input)
		if len(words) == 0 {
			ui.input.SetText(text)
			return true
		}

		words[len(words)-1] = text
		ui.input.SetText(strings.Join(words, " "))
		return true
	}).SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			go ui.runQuery(ui.input.GetText())
			ui.input.SetText("")
		}
	})
}

func (ui *ClickHouseUI) runQuery(query string) {
	// Uses ui.client.conn.Query(), updates ui.table, ui.status, and ui.lastResult
	query = strings.TrimSpace(query)
	if query == "" {
		return
	}

	ui.table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey { return event })

	go ui.history.Add(query)

	ui.table.Clear()
	ui.status.Clear()
	start := time.Now()

	go func() {
		ui.status.Clear()
		rows, err := ui.client.conn.Query(context.Background(), query)
		if err != nil {
			ui.app.QueueUpdateDraw(func() {
				fmt.Fprintf(ui.status, "[red]Query error: %v", err)
			})
			return
		}
		defer rows.Close()

		columns := rows.Columns()
		colTypes := rows.ColumnTypes()
		ui.lastResult = [][]string{columns}

		ui.app.QueueUpdateDraw(func() {
			for i, col := range columns {
				ui.table.SetCell(0, i, tview.NewTableCell(fmt.Sprintf("[::b]%s", col)))
			}
		})

		rowNum := 1

		for rows.Next() {
			// Initialize scanTargets on first row
			var scanTargets []interface{}
			// if scanTargets == nil {
			scanTargets = make([]interface{}, len(colTypes))
			for i, ct := range colTypes {
				scanTargets[i] = reflect.New(ct.ScanType()).Interface()
			}
			// }

			if err := rows.Scan(scanTargets...); err != nil {
				ui.app.QueueUpdateDraw(func() {
					fmt.Fprintf(ui.status, "[red]Scan error: %v", err)
				})
				continue
			}

			ui.parseData(scanTargets, rowNum, func(key tcell.Key) {})

			rowNum++
		}
		if err := rows.Err(); err != nil {
			ui.app.QueueUpdateDraw(func() {
				fmt.Fprintf(ui.status, "[red]Rows error: %v", err)
			})
		} else {
			ui.app.QueueUpdateDraw(func() {
				fmt.Fprintf(ui.status, "[green]Query finished. %d rows in %v", rowNum-1, time.Since(start))
			})
		}
	}()
}

func (ui *ClickHouseUI) parseData(scanTargets []interface{}, rowNum int, fun func(key tcell.Key)) {
	rowStr := make([]string, len(scanTargets))
	for i, v := range scanTargets {
		val := reflect.ValueOf(v)
		for val.Kind() == reflect.Ptr && !val.IsNil() {
			val = val.Elem()
		}
		if !val.IsValid() {
			rowStr[i] = ""
		} else {
			rowStr[i] = fmt.Sprintf("%v", val.Interface())
		}
	}
	rowStr = append(rowStr, strconv.Itoa(rowNum))
	ui.lastResult = append(ui.lastResult, rowStr)
	current := rowNum
	go ui.app.QueueUpdateDraw(func() {
		for colIdx, cell := range rowStr {
			ui.table.SetCell(current, colIdx, tview.NewTableCell(cell)).SetDoneFunc(fun)
		}
	})
}

func (ui *ClickHouseUI) showHistory() {
	ui.table.Clear()

	// Setup table input handler specifically for history mode
	ui.table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {

		switch event.Key() {
		case tcell.KeyEnter:
			row, _ := ui.table.GetSelection()
			cell := ui.table.GetCell(row, 0)
			query := cell.Text
			ui.input.SetText(query)
			go ui.runQuery(query)
			ui.input.SetText("")
			return nil

		case tcell.KeyBackspace, tcell.KeyBackspace2:
			row, _ := ui.table.GetSelection()
			if err := ui.history.clear(row); err != "" {
				ui.status.SetText(err)
			} else {
				ui.showHistory() // re-render after delete
			}
			return nil
		}
		return event
	})

	// Render history into table
	for i, query := range ui.history.history {
		ui.parseData([]interface{}{query}, i, func(key tcell.Key) {}) // assume query is a string
	}
}

func (ui *ClickHouseUI) exportCSV() {
	// Writes ui.lastResult to file
	lastResult := ui.lastResult
	ui.status.Clear()
	if len(ui.lastResult) == 0 {
		fmt.Fprintf(ui.status, "[yellow]No results to export.")
		return
	}

	filename := fmt.Sprintf("results_%s.csv", time.Now().Format("20060102_150405"))
	file, err := os.Create(filename)
	if err != nil {
		fmt.Fprintf(ui.status, "[red]Failed to create file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	for _, row := range lastResult {
		writer.Write(row)
	}
	writer.Flush()
	fmt.Fprintf(ui.status, "[green]Exported to %s", filename)
}

func (ui *ClickHouseUI) toggleFocus() {
	ui.focusTable = !ui.focusTable
	if ui.focusTable {
		ui.app.SetFocus(ui.table)
		ui.input.SetDisabled(true)
	} else {
		ui.app.SetFocus(ui.input)
		ui.input.SetDisabled(false)
	}
}
