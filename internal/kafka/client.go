package kafka

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"kafka-metrics-agent/internal/config"
)

type Client struct {
	kgo   *kgo.Client
	Admin *kadm.Client
}

func NewClient(cfg *config.Config) (*Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.ClientID("kafka-metrics-agent"),
	}

	// SASL auth
	if cfg.HasSASL() {
		saslOpt, err := buildSASL(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, saslOpt)
	}

	// TLS
	if cfg.TLSEnabled {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{}))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &Client{
		kgo:   client,
		Admin: kadm.NewClient(client),
	}, nil
}

func (c *Client) Close() {
	c.kgo.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.Admin.BrokerMetadata(ctx)
	return err
}

func buildSASL(cfg *config.Config) (kgo.Opt, error) {
	switch cfg.SASLMechanism {
	case "PLAIN":
		return kgo.SASL(plain.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsMechanism()), nil
	case "SCRAM-SHA-256":
		return kgo.SASL(scram.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsSha256Mechanism()), nil
	case "SCRAM-SHA-512":
		return kgo.SASL(scram.Auth{
			User: cfg.SASLUsername,
			Pass: cfg.SASLPassword,
		}.AsSha512Mechanism()), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.SASLMechanism)
	}
}
