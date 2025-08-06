package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func NewClickHouseClient(cfg *Config) (*ClickHouseClient, error) {
	addr := fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)
	var tlsConfig *tls.Config
	if cfg.UseTLS {
		if cfg.CAFilePath != "" {
			caCert, err := os.ReadFile(cfg.CAFilePath)
			if err != nil {
				return nil, err
			}
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(caCert)
			tlsConfig = &tls.Config{RootCAs: pool}
		} else {
			tlsConfig = &tls.Config{InsecureSkipVerify: true}
		}
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: determineProtocol(cfg.Port),
		Addr:     []string{addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.DBUser,
			Password: cfg.Password,
		},
		TLS: tlsConfig,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}
	return &ClickHouseClient{conn: conn, config: cfg}, nil
}

// determineProtocol returns the ClickHouse protocol based on the port number.
func determineProtocol(port string) clickhouse.Protocol {
	switch port {
	case "9000":
		return clickhouse.Native
	case "9440":
		return clickhouse.Native
	case "8123":
		return clickhouse.HTTP
	case "8443":
		return clickhouse.HTTP
	default:
		return clickhouse.Native
	}
}