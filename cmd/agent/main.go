package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka-metrics-agent/internal/collector"
	"kafka-metrics-agent/internal/config"
	"kafka-metrics-agent/internal/kafka"
)

func main() {
	// Load config from env
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	logLevel := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))

	logger.Info("starting kafka-metrics-agent", "brokers", cfg.KafkaBrokers)

	// Create Kafka client
	client, err := kafka.NewClient(cfg)
	if err != nil {
		logger.Error("failed to create kafka client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	// Verify connection
	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		logger.Error("failed to connect to kafka", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to kafka")

	// Create collector
	coll := collector.New(client)

	// Setup shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutting down")
		cancel()
	}()

	// Collection loop
	ticker := time.NewTicker(cfg.CollectionInterval)
	defer ticker.Stop()

	// Collect immediately on start
	batch := coll.Collect(ctx)
	coll.Print(batch)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			batch := coll.Collect(ctx)
			coll.Print(batch)
		}
	}
}
