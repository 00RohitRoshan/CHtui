package main

import (
	"bufio"
	"fmt"
	"strings"
)

func promptConfig(reader *bufio.Reader, username string) *Config {
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

func defaultIfEmpty(v string,d string)string{
	if v == "" {
		return d
	}
	return v
}
