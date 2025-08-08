package main

import (
	"context"
	"fmt"
	"strings"
)

func (h *QueryHistoryManager) Add(query string) {
	for i, q := range h.history {
		if q == query {
			// Remove the old occurrence
			h.history = append(h.history[:i], h.history[i+1:]...)
			break
		}
	}
	// Append at the end (always)
	h.history = append(h.history, query)
	if len(h.history) > 50 {
		h.history = h.history[len(h.history)-50:]
	}
}

func (h *QueryHistoryManager) clear(a int) string {

	// Convert to 0-based index
	index := a

	if index < 0 || index >= len(h.history) {
		return "Index out of range"
	}

	h.history = append(h.history[:index], h.history[index+1:]...)
	return ""
}

func (h *QueryHistoryManager) Navigate(dir int) string {
	h.index += dir
	if h.index < 0 {
		h.index = 0
	} else if h.index >= len(h.history) {
		h.index = len(h.history) - 1
	}
	if len(h.history) == 0 {
		return ""
	}
	return h.history[h.index]
}

func (h *QueryHistoryManager) GetAll() []string {
	return h.history
}

func (h *QueryHistoryManager) GetQuery(q string) []string {
	if q == "" || len(q)<3 {
		return nil
	}
	q = strings.ToLower(q)
	matching := make([]string, 0, len(h.history))
	for _, entry := range h.history {
		le := strings.ToLower(entry)
		if strings.Contains(le, q) {
			matching = append(matching, entry)
		}
	}

	words := strings.Split(q, " ")
	typing := strings.ToLower(words[len(words)-1])
	if len(typing) > 1 {
		for _, entry := range h.extra {
			le := strings.ToLower(entry)
			if strings.Contains(le, typing) {
				matching = append(matching, entry)
			}
		}
	}

	return matching
}

// func (h *QueryHistoryManager) GetSuggestion(q string) []string {
// 	if q == "" {
// 		return nil
// 	}

// 	matching := make([]string, 0, len(h.history))
// 	words := strings.Split(q, " ")
// 	typing := strings.ToLower(words[len(words)-1])
// 	for _, entry := range h.extra {
// 		le := strings.ToLower(entry)
// 		if strings.Contains(le, typing) {
// 			matching = append(matching, entry)
// 		}
// 	}

// 	return matching
// }

func (h *QueryHistoryManager) setSuggetions(c ClickHouseClient) {
	// 1. Base keywords and functions
	h.extra = []string{
		"SELECT", "INSERT", "INTO", "VALUES", "WITH", "LIMIT", "OFFSET",
		"ORDER BY", "GROUP BY", "HAVING", "JOIN", "UNION", "FORMAT",
		"WHERE", "AND", "OR", "NOT", "IN", "BETWEEN", "LIKE", "ILIKE",
		"AS", "CASE", "WHEN", "THEN", "ELSE", "END", "DISTINCT",
		"CREATE", "TABLE", "DATABASE", "ENGINE", "DROP", "ALTER",
		"RENAME", "ATTACH", "DETACH", "INNER JOIN", "LEFT JOIN",
		"RIGHT JOIN", "FULL JOIN", "CROSS JOIN", "ANY", "ALL",
		"SET", "SHOW", "EXISTS", "DESCRIBE", "OPTIMIZE", "EXPLAIN",
		"KILL QUERY", "SYSTEM", "TRUNCATE", "MATERIALIZED", "VIEW",
		"POPULATE", "FINAL", "SAMPLE", "PREWHERE", "ARRAY JOIN",
		"now", "toDate", "toDateTime", "formatDateTime",
		"count", "avg", "min", "max", "sum", "substring", "length", "position",
		"if", "coalesce", "round", "floor", "ceil", "toString", "toUInt32",
		"MergeTree", "ReplacingMergeTree", "SummingMergeTree",
		"AggregatingMergeTree", "VersionedCollapsingMergeTree",
		"CollapsingMergeTree", "Distributed", "Log", "StripeLog", "TinyLog", "Null",
	}

	dbSet := make(map[string]struct{})
	tableSet := make(map[string]struct{})
	columnSet := make(map[string]struct{})

	// 2. Get databases
	dbRows, err := c.conn.Query(context.Background(), "SHOW DATABASES")
	if err != nil {
		return
	}
	defer dbRows.Close()

	for dbRows.Next() {
		var dbName string
		if err := dbRows.Scan(&dbName); err == nil {
			dbSet[dbName] = struct{}{}
		}
	}

	// 3. Get tables and columns using SELECT * ... LIMIT 0
	for db := range dbSet {
		showTablesQuery := fmt.Sprintf("SHOW TABLES FROM `%s`", db)
		tableRows, err := c.conn.Query(context.Background(), showTablesQuery)
		if err != nil {
			continue
		}

		for tableRows.Next() {
			var tableName string
			if err := tableRows.Scan(&tableName); err != nil {
				continue
			}
			fullTable := fmt.Sprintf("%s.%s", db, tableName)
			tableSet[fullTable] = struct{}{}

			// Efficiently get column names using SELECT * LIMIT 0
			colQuery := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 1", db, tableName)
			colRows, err := c.conn.Query(context.Background(), colQuery)
			if err != nil {
				continue
			}
			cols := colRows.Columns()
			colRows.Close()

			for _, col := range cols {
				columnSet[col] = struct{}{}
			}
		}
		tableRows.Close()
	}

	// 4. Append unique entries
	for db := range dbSet {
		h.dbs = append(h.dbs, db)
		h.extra = append(h.extra, db)
	}
	for table := range tableSet {
		h.tables = append(h.tables, table)
		h.extra = append(h.extra, table)
	}
	for col := range columnSet {
		h.columns = append(h.columns, col)
		h.extra = append(h.extra, col)
	}
}
