package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter your username: ")
	username, _ := reader.ReadString('\n')
	username = strings.TrimSpace(username)

	cm := &ConfigManager{Username: username}
	cfg, err := cm.Load()
	if err != nil {
		cfg = promptConfig(reader, username)
		_ = cm.Save(cfg)
	}

	client, err := NewClickHouseClient(cfg)
	if err != nil {
		log.Fatalf("Connection error: %v", err)
	}

	history := &QueryHistoryManager{history: cfg.QueryHistory}
	go history.setSuggetions(*client)
	ui := &ClickHouseUI{
		client:  client,
		config:  cfg,
		history: history,
	}

	if err := ui.Run(); err != nil {
		log.Fatal(err)
	}

	cfg.QueryHistory = history.GetAll()
	_ = cm.Save(cfg)
}
