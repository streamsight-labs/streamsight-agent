package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"kafka-metrics-agent/internal/config"
)

const (
	// maxRetryTimeout caps the per-request retry budget however long the
	// collection window is. franz-go's default is 30s per request across up to
	// 20 tries, which one dead broker can turn into the entire cycle.
	maxRetryTimeout = 15 * time.Second
	// minRetryTimeout keeps a very short COLLECTION_TIMEOUT from disabling
	// retries entirely.
	minRetryTimeout = 2 * time.Second

	// requestRetries overrides franz-go's default of 20. The agent re-collects
	// every interval anyway, so a slow success is worse than a fast failure
	// that is reported as a degraded section.
	requestRetries = 3

	dialTimeout = 10 * time.Second
)

type Client struct {
	kgo   *kgo.Client
	Admin *kadm.Client
}

// NewClient dials the cluster. version is stamped into the Kafka ClientID so
// broker-side request logs and quota entries identify the agent build.
func NewClient(cfg *config.Config, version string) (*Client, error) {
	if version == "" {
		version = "dev"
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.ClientID(fmt.Sprintf("streamsight-agent/%s (%s)", version, cfg.AgentInstanceID)),
		kgo.DialTimeout(dialTimeout),
		// Bound the retry budget so an unreachable broker cannot consume the
		// whole collection window: the phase fails, the section is marked
		// degraded, and the next tick starts on time.
		kgo.RequestRetries(requestRetries),
		kgo.RetryTimeout(retryTimeout(cfg.CollectionTimeout)),
	}

	// SASL auth
	if cfg.HasSASL() {
		saslOpt, err := buildSASL(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, saslOpt)
	}

	// TLS. Deliberately the secure default: system roots, verified certs, no
	// InsecureSkipVerify knob. Only the floor is pinned explicitly.
	if cfg.TLSEnabled {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
		}))
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

// retryTimeout gives retries at most half the collection window, so a failing
// request still leaves time for the phases behind it.
func retryTimeout(collectionTimeout time.Duration) time.Duration {
	d := collectionTimeout / 2
	if d > maxRetryTimeout {
		return maxRetryTimeout
	}
	if d < minRetryTimeout {
		return minRetryTimeout
	}
	return d
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
