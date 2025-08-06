package main

import (
	"fmt"
	"os"

	"github.com/pelletier/go-toml/v2"
)

func (cm *ConfigManager) Load() (*Config, error) {
	filePath := fmt.Sprintf("%s.toml", cm.Username)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = toml.Unmarshal(data, &cfg)
	return &cfg, err
}

func (cm *ConfigManager) Save(cfg *Config) error {
	data, err := toml.Marshal(cfg)
	if err != nil {
		return err
	}
	filePath := fmt.Sprintf("%s.toml", cm.Username)
	return os.WriteFile(filePath, data, 0600)
}
