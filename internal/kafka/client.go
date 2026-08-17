package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
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

	// metadataMinAgeFloor is kgo's own lower bound on MetadataMinAge; a smaller
	// value is rejected by kgo.NewClient rather than clamped.
	metadataMinAgeFloor = 10 * time.Millisecond
	// metadataMinAgeCeil matches kgo's default. Never age metadata *more* than
	// stock franz-go would.
	metadataMinAgeCeil = 5 * time.Second

	// listGroupsKey is the ListGroups API key. Hardcoded rather than taken from
	// kmsg so this package keeps one franz-go import path; API keys are wire
	// constants and never change.
	listGroupsKey = 16
	// listGroupsStatesFilterVersion is the first ListGroups version carrying
	// KIP-518's StatesFilter field. kmsg only serialises the field "if version
	// >= 4" (kmsg@v1.13.1 generated.go, ListGroupsRequest.AppendTo) and kgo
	// silently downgrades a request to whatever the broker offers, so against
	// an older broker the filter is dropped on the floor with no error.
	listGroupsStatesFilterVersion = 4
	// listGroupsStatesFilterKafka is the broker release that first served
	// ListGroups v4, named in the failure message because operators run Kafka
	// versions, not API versions.
	listGroupsStatesFilterKafka = "2.6"

	// consumerGroupDescribeKey is the ConsumerGroupDescribe API key (KIP-848).
	// Brokers below Kafka 4.0 do not advertise it at all, which is how its
	// absence is detected.
	consumerGroupDescribeKey = 69
)

// Client is one kgo client wrapped in a kadm admin client. The agent holds
// exactly one for its lifetime; kgo maintains the per-broker connections.
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

// CheckGroupStateFilter reports whether every broker can honour a ListGroups
// states filter. Call it once at startup: it costs one ApiVersions round trip
// per broker, and ApiVersions needs no ACL, so it adds nothing to the agent's
// DESCRIBE-only permission set.
//
// The failure mode it detects is invisible (see
// listGroupsStatesFilterVersion): an operator who sets GROUP_STATES on a Kafka
// 2.5 cluster to cut a 40k-group listing down to a few hundred gets all 40k
// back, with no error and no log line.
func (c *Client) CheckGroupStateFilter(ctx context.Context) error {
	versions, err := c.Admin.ApiVersions(ctx)
	if err != nil {
		return fmt.Errorf("probe broker api versions: %w", err)
	}
	offered, err := listGroupsVersions(versions)
	if err != nil {
		return err
	}
	return checkGroupStateFilter(offered)
}

// SupportsConsumerGroupDescribe reports whether EVERY broker can answer
// ConsumerGroupDescribe (KIP-848, Kafka 4.0+). Every broker matters for the
// same reason as ListGroups: kadm shards the request, so one old broker means
// the merged answer is missing groups.
//
// Unlike CheckGroupStateFilter this does not fail startup. GROUP_STATES is
// something an operator asked for, so ignoring it silently is worth refusing to
// start over; consumer-group describe is on by default, so failing closed would
// refuse to start against every Kafka below 4.0. The caller disables the phase
// and logs instead.
func (c *Client) SupportsConsumerGroupDescribe(ctx context.Context) (bool, error) {
	versions, err := c.Admin.ApiVersions(ctx)
	if err != nil {
		return false, fmt.Errorf("probe broker api versions: %w", err)
	}
	if len(versions) == 0 {
		return false, errors.New("no broker answered the api versions probe")
	}
	for _, v := range versions.Sorted() {
		if v.Err != nil {
			return false, fmt.Errorf("broker %d did not answer the api versions probe: %w", v.NodeID, v.Err)
		}
		// Absent, not merely old: a pre-4.0 broker does not advertise key 69 at
		// all, so "not ok" is the ordinary answer here rather than an error.
		if _, ok := v.KeyMaxVersion(consumerGroupDescribeKey); !ok {
			return false, nil
		}
	}
	return true, nil
}

// brokerVersion is one broker's maximum ListGroups version. It exists because
// kadm.BrokerApiVersions keeps its version map unexported, so a probe result
// cannot be built in a test; lifting the numbers out keeps the decision that
// matters — minimum, not maximum — testable without a cluster.
type brokerVersion struct {
	nodeID  int32
	version int16
}

// listGroupsVersions reads each broker's ListGroups ceiling out of the probe.
// A broker that did not answer, or that does not know the API at all, is a
// failure rather than a skip: it cannot be shown to honour the filter.
func listGroupsVersions(versions kadm.BrokersApiVersions) ([]brokerVersion, error) {
	if len(versions) == 0 {
		return nil, errors.New("no broker answered the api versions probe")
	}
	// Sorted() so the broker named in any message is the same on every run,
	// rather than whichever the map yielded first.
	offered := make([]brokerVersion, 0, len(versions))
	for _, v := range versions.Sorted() {
		if v.Err != nil {
			return nil, fmt.Errorf("broker %d did not answer the api versions probe: %w", v.NodeID, v.Err)
		}
		max, ok := v.KeyMaxVersion(listGroupsKey)
		if !ok {
			return nil, fmt.Errorf("broker %d does not support ListGroups at all", v.NodeID)
		}
		offered = append(offered, brokerVersion{nodeID: v.NodeID, version: max})
	}
	return offered, nil
}

// checkGroupStateFilter takes the MINIMUM ListGroups version across brokers,
// not the maximum. kadm shards ListGroups to every broker and merges the
// responses, so one Kafka 2.5 node in an otherwise modern cluster returns its
// whole listing unfiltered and the merged result is unfiltered too. Trusting
// the maximum would report the cluster as capable precisely when it is not.
func checkGroupStateFilter(offered []brokerVersion) error {
	if len(offered) == 0 {
		return errors.New("no broker answered the api versions probe")
	}
	lowest := brokerVersion{version: math.MaxInt16}
	// Strictly less-than over a node-sorted slice, so a tie deterministically
	// names the lowest broker ID.
	for _, b := range offered {
		if b.version < lowest.version {
			lowest = b
		}
	}
	if lowest.version < listGroupsStatesFilterVersion {
		return fmt.Errorf("broker %d offers ListGroups v%d, but filtering by group state needs v%d (KIP-518, Kafka %s+); an older broker ignores the filter and returns every group",
			lowest.nodeID, lowest.version, listGroupsStatesFilterVersion, listGroupsStatesFilterKafka)
	}
	return nil
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
