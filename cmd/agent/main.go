package main

import (
	"log/slog"
	"os"

	"kafka-metrics-agent/internal/agent"
	"kafka-metrics-agent/internal/config"
)

// version is injected at build time via
// -ldflags "-X main.version=$(git describe --tags --always --dirty)".
var version = "dev"

func main() {
	// The only os.Exit in the program. Everything else returns an error so
	// that deferred cleanup -- flushing the exporter, closing the Kafka
	// client -- actually runs.
	if err := run(); err != nil {
		slog.Error("agent failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	a, err := agent.New(cfg, version)
	if err != nil {
		return err
	}
	defer func() {
		if err := a.Close(); err != nil {
			slog.Error("shutdown error", "error", err)
		}
	}()

	return a.Run()
}
