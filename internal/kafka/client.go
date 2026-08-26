package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

	// metadataMinAgeFloor is kgo's own lower bound on MetadataMinAge; a smaller
	// value is rejected by kgo.NewClient rather than clamped.
	metadataMinAgeFloor = 10 * time.Millisecond
	// metadataMinAgeCeil matches kgo's default. Never age metadata *more* than
	// stock franz-go would.
	metadataMinAgeCeil = 5 * time.Second
)

// Client is one kgo client wrapped in a kadm admin client. The agent holds
// exactly one for its lifetime; kgo maintains the per-broker connections.
type Client struct {
	kgo   *kgo.Client
	Admin *kadm.Client
	rpc   *rpcHooks

	mu   sync.Mutex
	caps *Capabilities
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
		kgo.RequestRetries(requestRetries),
		kgo.RetryTimeout(retryTimeout(cfg.CollectionTimeout)),
		// kadm >= 1.12 serves Metadata from kgo's metadata cache
		// (RequestCachedMetadata with limit 0, which kgo resolves to
		// MetadataMinAge -- 5s by default). At a collection interval at or below
		// that, a cycle would issue no Metadata request at all and ship a stale
		// broker/topic inventory stamped with a fresh sampled_at. Age it below
		// the interval so every cycle refetches once, while the three metadata
		// reads within one cycle still share a consistent snapshot.
		kgo.MetadataMinAge(metadataMinAge(cfg.CollectionInterval)),
	}

	if cfg.HasSASL() {
		saslOpt, err := buildSASL(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, saslOpt)
	}

	// System roots, verified certs, no InsecureSkipVerify knob.
	if cfg.TLSEnabled {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
		}))
	}

	// Broker RPC health, measured on the traffic the agent already sends: zero
	// extra requests and no extra ACL, so the accumulator is always installed
	// and COLLECT_RPC_STATS decides only whether the section is shipped.
	rpc := newRPCHooks(time.Now())
	opts = append(opts, kgo.WithHooks(rpc))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &Client{
		kgo:   client,
		Admin: kadm.NewClient(client),
		rpc:   rpc,
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

// metadataMinAge picks how long kgo may serve cached metadata, clamped to
// kgo's own bounds. See NewClient for why half the collection interval.
func metadataMinAge(collectionInterval time.Duration) time.Duration {
	d := collectionInterval / 2
	if d > metadataMinAgeCeil {
		return metadataMinAgeCeil
	}
	if d < metadataMinAgeFloor {
		return metadataMinAgeFloor
	}
	return d
}

// Close shuts down the underlying kgo client and its connections.
func (c *Client) Close() {
	c.kgo.Close()
}

// Ping proves the cluster is reachable and the credentials work, so a
// misconfiguration surfaces at startup instead of as a failed section.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.Admin.BrokerMetadata(ctx)
	return err
}

// Request issues a raw kmsg request. It exists for the protocol fields kadm
// drops — DescribeLogDirs v4's TotalBytes/UsableBytes, Metadata v8's
// AuthorizedOperations — not as a general bypass: anything kadm already models
// goes through Admin, which handles sharding, retries and error merging.
func (c *Client) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	return c.kgo.Request(ctx, req)
}

// RequestSharded is Request for a request kgo splits across brokers, returning
// one shard per broker, sorted by node ID. It surfaces []kgo.ResponseShard
// unchanged rather than merging: a merge would have to invent a policy for a
// partly failed fan-out, and only the caller knows whether one dead shard makes
// its section partial or failed.
func (c *Client) RequestSharded(ctx context.Context, req kmsg.Request) []kgo.ResponseShard {
	return c.kgo.RequestSharded(ctx, req)
}

// Probe fingerprints what the cluster can be asked. It issues one ApiVersions
// round trip per broker, in parallel, and caches the result for the client's
// lifetime — so the several startup questions cost one probe, not one each.
// ApiVersions needs no ACL, so it adds nothing to the DESCRIBE-only set.
//
// A broker that did not answer fails the whole probe: a fingerprint missing a
// broker cannot be a minimum across brokers, and the callers below each decide
// what an unknown cluster means for them.
func (c *Client) Probe(ctx context.Context) (*Capabilities, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.caps != nil {
		return c.caps, nil
	}
	versions, err := c.Admin.ApiVersions(ctx)
	if err != nil {
		return nil, fmt.Errorf("probe broker api versions: %w", err)
	}
	brokers, err := probeBrokers(versions)
	if err != nil {
		return nil, err
	}
	c.caps = foldCapabilities(brokers)
	return c.caps, nil
}

// CheckGroupStateFilter reports whether every broker can honour a ListGroups
// states filter, as an error naming the broker that cannot.
//
// The failure mode it detects is invisible (see
// listGroupsStatesFilterVersion): an operator who sets GROUP_STATES on a Kafka
// 2.5 cluster to cut a 40k-group listing down to a few hundred gets all 40k
// back, with no error and no log line. That inverts the setting rather than
// degrading it, which is why the caller fails startup on this one.
func (c *Client) CheckGroupStateFilter(ctx context.Context) error {
	caps, err := c.Probe(ctx)
	if err != nil {
		return err
	}
	return caps.checkGroupStateFilter()
}

// SupportsConsumerGroupDescribe reports whether EVERY broker can answer
// ConsumerGroupDescribe (KIP-848, Kafka 4.0+). Every broker matters because
// kadm shards the request, so one old broker means the merged answer is missing
// groups.
//
// Unlike CheckGroupStateFilter this does not fail startup. GROUP_STATES is
// something an operator asked for, so ignoring it silently is worth refusing to
// start over; consumer-group describe is on by default, so failing closed would
// refuse to start against every Kafka below 4.0. The caller disables the phase
// and logs instead.
func (c *Client) SupportsConsumerGroupDescribe(ctx context.Context) (bool, error) {
	caps, err := c.Probe(ctx)
	if err != nil {
		return false, err
	}
	return caps.SupportsConsumerGroupDescribe(), nil
}

// SupportsLastStableOffset reports whether EVERY broker honours the ListOffsets
// isolation level (Kafka 0.11+). Below that the field is dropped on downgrade
// and the broker answers ListCommittedOffsets with the high watermark, so
// last_stable_offset equals end_offset and an open-transaction backlog reads as
// a constant zero — the one degradation with no data-side tell.
//
// COLLECT_LAST_STABLE_OFFSET defaults on, so this follows the
// ConsumerGroupDescribe precedent: the caller soft-disables the phase and logs,
// leaving an honest skipped section instead of silently equal numbers.
func (c *Client) SupportsLastStableOffset(ctx context.Context) (bool, error) {
	caps, err := c.Probe(ctx)
	if err != nil {
		return false, err
	}
	return caps.SupportsListOffsetsIsolation(), nil
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
