package main

import (
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rivo/tview"
)

type ClickHouseUI struct {
	app         *tview.Application
	client      *ClickHouseClient
	history     *QueryHistoryManager
	config      *Config
	table       *tview.Table
	input       *tview.InputField
	status      *tview.TextView
	lastResult  [][]string
	focusTable  bool
	historyLock sync.Mutex
}

type ClickHouseClient struct {
	conn   clickhouse.Conn
	config *Config
}

type QueryHistoryManager struct {
	history []string
	index   int
	dbs		[]string
	tables	[]string
	columns	[]string
	extra	[]string
}

type ConfigManager struct {
	Username string
}

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
