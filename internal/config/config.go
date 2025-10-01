package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {

	AuthZed struct {
		API *string `yaml:"api",omitempty`
		Key *string `yaml:"key",omitempty`
		Schema *string `yaml:"schema",omitempty`
	} `yaml:"authzed"`

	Workload struct {
		ReadRatio       int `yaml:"read_ratio"`
        DurationSec     int `yaml:"duration_sec"`
	} `yaml:"workload"`
}

var AppConfig Config

func LoadConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, &AppConfig); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return nil
}
