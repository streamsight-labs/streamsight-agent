// Package config loads and validates the agent's entire environment surface.
//
// Every value is parsed and validated here, at startup, so that a typo in an
// env var fails immediately and loudly instead of silently falling back to a
// default (or, worse, panicking on the first tick).
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Export modes. These match export.ModeFile / ModeHTTP / ModeStdout; config
// does not import export so the dependency stays one-way.
const (
	ExportModeFile   = "file"
	ExportModeHTTP   = "http"
	ExportModeStdout = "stdout"
)

// Defaults for every optional setting, in one place so they can be asserted.
const (
	DefaultExportFile       = "./metrics.jsonl"
	DefaultExportFileMaxMB  = 100
	DefaultExportMaxBackups = 3
	DefaultExportQueueSize  = 100
	DefaultExportMaxRetries = 3
	DefaultExportBaseDelay  = time.Second
	DefaultExportTimeout    = 10 * time.Second
	DefaultExportGzip       = true
	DefaultInterval         = 30 * time.Second
	DefaultLogLevel         = "info"

	// collectionTimeoutRatio derives COLLECTION_TIMEOUT from
	// COLLECTION_INTERVAL when it is not set explicitly. A cycle that runs
	// longer than this eats into the next tick, which time.Ticker silently
	// coalesces.
	collectionTimeoutRatio = 80
)

type Config struct {
	// Kafka.
	KafkaBrokers  []string
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
	TLSEnabled    bool

	// Export. ExportMode is always one of the ExportMode* constants after a
	// successful Load.
	ExportMode           string
	ExportEndpoint       string
	APIKey               string
	ExportFile           string
	ExportFileMaxMB      int
	ExportFileMaxBackups int
	ExportQueueSize      int
	ExportMaxRetries     int
	ExportBaseDelay      time.Duration
	ExportTimeout        time.Duration
	ExportGzip           bool

	// Collection.
	CollectionInterval    time.Duration
	CollectionTimeout     time.Duration
	IncludeInternalTopics bool
	TopicIncludeRegex     string
	TopicExcludeRegex     string
	GroupIncludeRegex     string
	GroupExcludeRegex     string

	// Agent.
	LogLevel        string
	AgentInstanceID string
}

// Load reads the environment. It reports every problem it finds rather than
// only the first, so a misconfigured deployment needs one restart, not five.
func Load() (*Config, error) {
	p := &parser{}
	c := &Config{}

	brokers := os.Getenv("KAFKA_BROKERS")
	c.KafkaBrokers = splitList(brokers)
	if len(c.KafkaBrokers) == 0 {
		p.errf("KAFKA_BROKERS is required (comma separated host:port list)")
	}

	c.SASLMechanism = os.Getenv("KAFKA_SASL_MECHANISM")
	c.SASLUsername = os.Getenv("KAFKA_SASL_USERNAME")
	c.SASLPassword = os.Getenv("KAFKA_SASL_PASSWORD")
	switch c.SASLMechanism {
	case "", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		p.errf("KAFKA_SASL_MECHANISM %q is not one of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512", c.SASLMechanism)
	}
	if c.SASLMechanism != "" && (c.SASLUsername == "" || c.SASLPassword == "") {
		p.errf("KAFKA_SASL_MECHANISM is set, so KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required")
	}
	c.TLSEnabled = p.boolean("KAFKA_TLS_ENABLED", false)

	// Export mode: explicit wins, otherwise http iff an endpoint is present.
	// Local file mode is the default so the agent runs with no network and no
	// credentials.
	c.ExportEndpoint = strings.TrimSpace(os.Getenv("EXPORT_ENDPOINT"))
	c.APIKey = os.Getenv("API_KEY")
	c.ExportMode = strings.ToLower(strings.TrimSpace(os.Getenv("EXPORT_MODE")))
	if c.ExportMode == "" {
		if c.ExportEndpoint != "" {
			c.ExportMode = ExportModeHTTP
		} else {
			c.ExportMode = ExportModeFile
		}
	}
	switch c.ExportMode {
	case ExportModeFile, ExportModeStdout:
	case ExportModeHTTP:
		if c.ExportEndpoint == "" {
			p.errf("EXPORT_ENDPOINT is required when EXPORT_MODE=http")
		}
		if c.APIKey == "" {
			p.errf("API_KEY is required when EXPORT_MODE=http")
		}
	default:
		p.errf("EXPORT_MODE %q is not one of file, http, stdout", c.ExportMode)
	}

	c.ExportFile = p.str("EXPORT_FILE", DefaultExportFile)
	// A negative EXPORT_FILE_MAX_MB deliberately disables rotation; a negative
	// backup count is meaningless, so it is rejected rather than reinterpreted.
	c.ExportFileMaxMB = p.integer("EXPORT_FILE_MAX_MB", DefaultExportFileMaxMB)
	c.ExportFileMaxBackups = p.integer("EXPORT_FILE_MAX_BACKUPS", DefaultExportMaxBackups)
	if c.ExportFileMaxBackups < 0 {
		p.errf("EXPORT_FILE_MAX_BACKUPS must be >= 0, got %d", c.ExportFileMaxBackups)
	}
	c.ExportQueueSize = p.integer("EXPORT_QUEUE_SIZE", DefaultExportQueueSize)
	if c.ExportQueueSize <= 0 {
		p.errf("EXPORT_QUEUE_SIZE must be > 0, got %d", c.ExportQueueSize)
	}
	c.ExportMaxRetries = p.integer("EXPORT_MAX_RETRIES", DefaultExportMaxRetries)
	if c.ExportMaxRetries < 0 {
		p.errf("EXPORT_MAX_RETRIES must be >= 0, got %d", c.ExportMaxRetries)
	}
	c.ExportBaseDelay = p.duration("EXPORT_BASE_DELAY", DefaultExportBaseDelay)
	if c.ExportBaseDelay <= 0 {
		p.errf("EXPORT_BASE_DELAY must be > 0, got %s", c.ExportBaseDelay)
	}
	c.ExportTimeout = p.duration("EXPORT_TIMEOUT", DefaultExportTimeout)
	if c.ExportTimeout <= 0 {
		p.errf("EXPORT_TIMEOUT must be > 0, got %s", c.ExportTimeout)
	}
	c.ExportGzip = p.boolean("EXPORT_GZIP", DefaultExportGzip)

	c.CollectionInterval = p.duration("COLLECTION_INTERVAL", DefaultInterval)
	if c.CollectionInterval <= 0 {
		// time.NewTicker panics on a non-positive duration. Catch it here.
		p.errf("COLLECTION_INTERVAL must be > 0, got %s", c.CollectionInterval)
	}
	defaultTimeout := c.CollectionInterval * collectionTimeoutRatio / 100
	c.CollectionTimeout = p.duration("COLLECTION_TIMEOUT", defaultTimeout)
	if c.CollectionTimeout <= 0 {
		p.errf("COLLECTION_TIMEOUT must be > 0, got %s", c.CollectionTimeout)
	}

	c.IncludeInternalTopics = p.boolean("INCLUDE_INTERNAL_TOPICS", false)
	c.TopicIncludeRegex = p.regex("TOPIC_INCLUDE_REGEX")
	c.TopicExcludeRegex = p.regex("TOPIC_EXCLUDE_REGEX")
	c.GroupIncludeRegex = p.regex("GROUP_INCLUDE_REGEX")
	c.GroupExcludeRegex = p.regex("GROUP_EXCLUDE_REGEX")

	c.LogLevel = strings.ToLower(strings.TrimSpace(p.str("LOG_LEVEL", DefaultLogLevel)))
	if _, err := parseLevel(c.LogLevel); err != nil {
		p.err(err)
	}

	c.AgentInstanceID = strings.TrimSpace(os.Getenv("AGENT_INSTANCE_ID"))
	if c.AgentInstanceID == "" {
		c.AgentInstanceID = defaultInstanceID()
	}

	if err := p.result(); err != nil {
		return nil, err
	}
	return c, nil
}

// SlogLevel maps LOG_LEVEL onto a slog level. Load has already validated it,
// so the fallback here is unreachable in practice.
func (c *Config) SlogLevel() slog.Level {
	lvl, err := parseLevel(c.LogLevel)
	if err != nil {
		return slog.LevelInfo
	}
	return lvl
}

// Warnings lists settings that are legal but likely a mistake. They are logged
// at startup rather than rejected, because the operator may mean them.
func (c *Config) Warnings() []string {
	var w []string
	if c.CollectionTimeout > c.CollectionInterval {
		w = append(w, fmt.Sprintf("COLLECTION_TIMEOUT (%s) exceeds COLLECTION_INTERVAL (%s): slow cycles will delay ticks",
			c.CollectionTimeout, c.CollectionInterval))
	}
	if c.ExportMode != ExportModeHTTP && c.ExportEndpoint != "" {
		w = append(w, fmt.Sprintf("EXPORT_ENDPOINT is set but EXPORT_MODE=%s, so it is ignored", c.ExportMode))
	}
	if c.ExportMode == ExportModeFile && c.ExportFileMaxMB < 0 {
		w = append(w, "EXPORT_FILE_MAX_MB is negative: file rotation is disabled and the file will grow without bound")
	}
	return w
}

func (c *Config) HasSASL() bool {
	return c.SASLMechanism != "" && c.SASLUsername != "" && c.SASLPassword != ""
}

// ExportTarget is a human-readable description of where batches go, for the
// startup log line.
func (c *Config) ExportTarget() string {
	switch c.ExportMode {
	case ExportModeHTTP:
		return c.ExportEndpoint
	case ExportModeFile:
		return c.ExportFile
	case ExportModeStdout:
		return "stdout"
	default:
		return ""
	}
}

// Redacted renders the config for logging. It never includes API_KEY or
// KAFKA_SASL_PASSWORD; both are reduced to a set/unset marker.
func (c *Config) Redacted() string {
	var b strings.Builder
	fmt.Fprintf(&b, "brokers=%s", strings.Join(c.KafkaBrokers, ","))
	fmt.Fprintf(&b, " tls=%t sasl_mechanism=%s sasl_username=%s sasl_password=%s",
		c.TLSEnabled, orNone(c.SASLMechanism), orNone(c.SASLUsername), secret(c.SASLPassword))
	fmt.Fprintf(&b, " export_mode=%s export_target=%s api_key=%s",
		c.ExportMode, orNone(c.ExportTarget()), secret(c.APIKey))
	if c.ExportMode == ExportModeFile {
		fmt.Fprintf(&b, " export_file_max_mb=%d export_file_max_backups=%d",
			c.ExportFileMaxMB, c.ExportFileMaxBackups)
	}
	if c.ExportMode == ExportModeHTTP {
		fmt.Fprintf(&b, " export_queue_size=%d export_max_retries=%d export_base_delay=%s export_timeout=%s export_gzip=%t",
			c.ExportQueueSize, c.ExportMaxRetries, c.ExportBaseDelay, c.ExportTimeout, c.ExportGzip)
	}
	fmt.Fprintf(&b, " collection_interval=%s collection_timeout=%s include_internal_topics=%t",
		c.CollectionInterval, c.CollectionTimeout, c.IncludeInternalTopics)
	fmt.Fprintf(&b, " topic_include=%s topic_exclude=%s group_include=%s group_exclude=%s",
		orNone(c.TopicIncludeRegex), orNone(c.TopicExcludeRegex), orNone(c.GroupIncludeRegex), orNone(c.GroupExcludeRegex))
	fmt.Fprintf(&b, " log_level=%s agent_instance_id=%s", c.LogLevel, c.AgentInstanceID)
	return b.String()
}

// String is Redacted, so that an accidental %v or %s of the config cannot leak
// a credential.
func (c *Config) String() string { return c.Redacted() }

func secret(v string) string {
	if v == "" {
		return "unset"
	}
	return "***"
}

func orNone(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func parseLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("LOG_LEVEL %q is not one of debug, info, warn, error", s)
	}
}

func splitList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// defaultInstanceID is hostname + a short random suffix, so two agents on the
// same host (or two pods from one image) do not collide in the backend.
func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "agent"
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d", host, time.Now().UnixNano()&0xffffffff)
	}
	return host + "-" + hex.EncodeToString(b[:])
}

// parser accumulates parse and validation failures so Load can report them all
// at once.
type parser struct {
	errs []error
}

func (p *parser) err(err error)                        { p.errs = append(p.errs, err) }
func (p *parser) errf(format string, a ...interface{}) { p.err(fmt.Errorf(format, a...)) }

func (p *parser) result() error {
	if len(p.errs) == 0 {
		return nil
	}
	return errors.Join(p.errs...)
}

func (p *parser) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return def
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.errf("%s %q is not a valid duration (e.g. 30s, 2m): %v", key, v, err)
		return def
	}
	return d
}

func (p *parser) integer(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.errf("%s %q is not a valid integer", key, v)
		return def
	}
	return n
}

func (p *parser) boolean(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.errf("%s %q is not a valid boolean (true/false)", key, v)
		return def
	}
	return b
}

// regex validates the pattern at startup. The compiled value is discarded --
// the collector owns compilation -- but a bad pattern must not wait until the
// first collection cycle to be noticed.
func (p *parser) regex(key string) string {
	v := os.Getenv(key)
	if v == "" {
		return ""
	}
	if _, err := regexp.Compile(v); err != nil {
		p.errf("%s is not a valid regular expression: %v", key, err)
	}
	return v
}
