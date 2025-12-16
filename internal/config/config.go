package config

import (
	"errors"
	"os"
	"strings"
	"time"
)

type Config struct {
	// Required
	KafkaBrokers []string
	APIKey       string

	// Optional Kafka auth
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
	TLSEnabled    bool

	// Optional tuning
	CollectionInterval time.Duration
	LogLevel           string
}

func Load() (*Config, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		return nil, errors.New("KAFKA_BROKERS is required")
	}

	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		return nil, errors.New("API_KEY is required")
	}

	interval := 30 * time.Second
	if v := os.Getenv("COLLECTION_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		}
	}

	logLevel := "info"
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		logLevel = v
	}

	return &Config{
		KafkaBrokers:       strings.Split(brokers, ","),
		APIKey:             apiKey,
		SASLMechanism:      os.Getenv("KAFKA_SASL_MECHANISM"),
		SASLUsername:       os.Getenv("KAFKA_SASL_USERNAME"),
		SASLPassword:       os.Getenv("KAFKA_SASL_PASSWORD"),
		TLSEnabled:         os.Getenv("KAFKA_TLS_ENABLED") == "true",
		CollectionInterval: interval,
		LogLevel:           logLevel,
	}, nil
}

func (c *Config) HasSASL() bool {
	return c.SASLMechanism != "" && c.SASLUsername != "" && c.SASLPassword != ""
}
