package config

import (
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

// allKeys is every env var Load reads. Tests blank all of them first so a
// developer's shell environment cannot change the result.
var allKeys = []string{
	"KAFKA_BROKERS",
	"API_KEY",
	"KAFKA_SASL_MECHANISM",
	"KAFKA_SASL_USERNAME",
	"KAFKA_SASL_PASSWORD",
	"KAFKA_TLS_ENABLED",
	"EXPORT_MODE",
	"EXPORT_ENDPOINT",
	"EXPORT_FILE",
	"EXPORT_FILE_MAX_MB",
	"EXPORT_FILE_MAX_BACKUPS",
	"EXPORT_QUEUE_SIZE",
	"EXPORT_MAX_RETRIES",
	"EXPORT_BASE_DELAY",
	"EXPORT_TIMEOUT",
	"COLLECTION_INTERVAL",
	"COLLECTION_TIMEOUT",
	"INCLUDE_INTERNAL_TOPICS",
	"TOPIC_INCLUDE_REGEX",
	"TOPIC_EXCLUDE_REGEX",
	"GROUP_INCLUDE_REGEX",
	"GROUP_EXCLUDE_REGEX",
	"GROUP_STATES",
	"EXPORT_FILE_FSYNC",
	"COLLECT_LAST_STABLE_OFFSET",
	"COLLECT_CONSUMER_GROUPS",
	"COLLECT_LOG_DIRS",
	"LOG_DIRS_EVERY",
	"COLLECT_THROUGHPUT_WINDOW",
	"THROUGHPUT_WINDOW",
	"THROUGHPUT_WINDOW_EVERY",
	"COLLECT_CONFIGS",
	"CONFIGS_EVERY",
	"COLLECT_MAX_TIMESTAMP",
	"MAX_TIMESTAMP_EVERY",
	"COLLECT_TIERED_OFFSETS",
	"COLLECT_SHARE_GROUPS",
	"MAX_ERRORS",
	"MAX_ERROR_SAMPLES",
	"MAX_TOPICS",
	"MAX_PARTITIONS_PER_TOPIC",
	"MAX_GROUPS",
	"MAX_MEMBERS_PER_GROUP",
	"MAX_OFFSETS_PER_GROUP",
	"LOG_LEVEL",
	"AGENT_INSTANCE_ID",
}

// setEnv blanks every known key, then applies env. Load treats an empty value
// as unset everywhere, which is what makes this safe.
func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for _, k := range allKeys {
		t.Setenv(k, "")
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
}

func base(extra map[string]string) map[string]string {
	env := map[string]string{"KAFKA_BROKERS": "localhost:9092"}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

func TestLoadModeResolution(t *testing.T) {
	tests := []struct {
		name     string
		env      map[string]string
		wantMode string
		wantErr  string
	}{
		{
			name:     "no endpoint defaults to file",
			env:      base(nil),
			wantMode: ExportModeFile,
		},
		{
			name:     "endpoint set implies http",
			env:      base(map[string]string{"EXPORT_ENDPOINT": "https://ingest.example/v1", "API_KEY": "k"}),
			wantMode: ExportModeHTTP,
		},
		{
			name:     "explicit mode beats endpoint inference",
			env:      base(map[string]string{"EXPORT_MODE": "file", "EXPORT_ENDPOINT": "https://ingest.example/v1"}),
			wantMode: ExportModeFile,
		},
		{
			name:     "explicit stdout",
			env:      base(map[string]string{"EXPORT_MODE": "stdout"}),
			wantMode: ExportModeStdout,
		},
		{
			name:     "mode is case insensitive and trimmed",
			env:      base(map[string]string{"EXPORT_MODE": "  STDOUT "}),
			wantMode: ExportModeStdout,
		},
		{
			name:    "unknown mode is rejected",
			env:     base(map[string]string{"EXPORT_MODE": "kinesis"}),
			wantErr: `EXPORT_MODE "kinesis"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)
			cfg, err := Load()
			if tc.wantErr != "" {
				requireErrContains(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.ExportMode != tc.wantMode {
				t.Fatalf("ExportMode = %q, want %q", cfg.ExportMode, tc.wantMode)
			}
		})
	}
}

func TestLoadRequiredFieldMatrix(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{
			name:    "brokers always required",
			env:     map[string]string{"EXPORT_MODE": "stdout"},
			wantErr: "KAFKA_BROKERS is required",
		},
		{
			name:    "brokers of only separators is empty",
			env:     map[string]string{"KAFKA_BROKERS": " , , "},
			wantErr: "KAFKA_BROKERS is required",
		},
		{
			name: "file mode needs no api key",
			env:  base(nil),
		},
		{
			name: "stdout mode needs no api key",
			env:  base(map[string]string{"EXPORT_MODE": "stdout"}),
		},
		{
			name:    "http mode needs api key",
			env:     base(map[string]string{"EXPORT_MODE": "http", "EXPORT_ENDPOINT": "https://x"}),
			wantErr: "API_KEY is required when EXPORT_MODE=http",
		},
		{
			// No longer an error: http mode defaults the endpoint. The API key
			// stays required, so an agent cannot post anywhere without one.
			name: "http mode defaults the endpoint",
			env:  base(map[string]string{"EXPORT_MODE": "http", "API_KEY": "k"}),
		},
		{
			name: "http mode fully specified",
			env:  base(map[string]string{"EXPORT_MODE": "http", "EXPORT_ENDPOINT": "https://x", "API_KEY": "k"}),
		},
		{
			name:    "sasl mechanism without credentials",
			env:     base(map[string]string{"KAFKA_SASL_MECHANISM": "PLAIN"}),
			wantErr: "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required",
		},
		{
			name:    "unknown sasl mechanism",
			env:     base(map[string]string{"KAFKA_SASL_MECHANISM": "GSSAPI", "KAFKA_SASL_USERNAME": "u", "KAFKA_SASL_PASSWORD": "p"}),
			wantErr: "KAFKA_SASL_MECHANISM",
		},
		{
			name: "sasl fully specified",
			env:  base(map[string]string{"KAFKA_SASL_MECHANISM": "SCRAM-SHA-512", "KAFKA_SASL_USERNAME": "u", "KAFKA_SASL_PASSWORD": "p"}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)
			_, err := Load()
			if tc.wantErr != "" {
				requireErrContains(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
		})
	}
}

func TestLoadIntervalValidation(t *testing.T) {
	tests := []struct {
		name        string
		env         map[string]string
		wantErr     string
		wantEvery   time.Duration
		wantTimeout time.Duration
	}{
		{
			name:        "defaults",
			env:         base(nil),
			wantEvery:   DefaultInterval,
			wantTimeout: DefaultInterval * collectionTimeoutRatio / 100,
		},
		{
			name:        "timeout derives from interval",
			env:         base(map[string]string{"COLLECTION_INTERVAL": "10s"}),
			wantEvery:   10 * time.Second,
			wantTimeout: 8 * time.Second,
		},
		{
			name:        "explicit timeout wins",
			env:         base(map[string]string{"COLLECTION_INTERVAL": "1m", "COLLECTION_TIMEOUT": "5s"}),
			wantEvery:   time.Minute,
			wantTimeout: 5 * time.Second,
		},
		{
			name:    "unparseable interval is an error, not a fallback",
			env:     base(map[string]string{"COLLECTION_INTERVAL": "30"}),
			wantErr: "COLLECTION_INTERVAL",
		},
		{
			// Accepting zero panics in time.NewTicker.
			name:    "zero interval is rejected",
			env:     base(map[string]string{"COLLECTION_INTERVAL": "0s"}),
			wantErr: "COLLECTION_INTERVAL must be > 0",
		},
		{
			name:    "negative interval is rejected",
			env:     base(map[string]string{"COLLECTION_INTERVAL": "-5s"}),
			wantErr: "COLLECTION_INTERVAL must be > 0",
		},
		{
			name:    "zero timeout is rejected",
			env:     base(map[string]string{"COLLECTION_TIMEOUT": "0s"}),
			wantErr: "COLLECTION_TIMEOUT must be > 0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)
			cfg, err := Load()
			if tc.wantErr != "" {
				requireErrContains(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.CollectionInterval != tc.wantEvery {
				t.Errorf("CollectionInterval = %s, want %s", cfg.CollectionInterval, tc.wantEvery)
			}
			if cfg.CollectionTimeout != tc.wantTimeout {
				t.Errorf("CollectionTimeout = %s, want %s", cfg.CollectionTimeout, tc.wantTimeout)
			}
		})
	}
}

func TestLoadRegexValidation(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr bool
	}{
		{name: "valid topic include", key: "TOPIC_INCLUDE_REGEX", value: "^orders\\..*"},
		{name: "bad topic include", key: "TOPIC_INCLUDE_REGEX", value: "[", wantErr: true},
		{name: "bad topic exclude", key: "TOPIC_EXCLUDE_REGEX", value: "a(", wantErr: true},
		{name: "bad group include", key: "GROUP_INCLUDE_REGEX", value: "*", wantErr: true},
		{name: "bad group exclude", key: "GROUP_EXCLUDE_REGEX", value: "(?P<", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, base(map[string]string{tc.key: tc.value}))
			cfg, err := Load()
			if tc.wantErr {
				requireErrContains(t, err, tc.key)
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.TopicIncludeRegex != tc.value {
				t.Fatalf("regex not preserved: %q", cfg.TopicIncludeRegex)
			}
		})
	}
}

func TestLoadGroupStates(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    []string
		wantErr string
	}{
		{
			name:  "unset lists every state",
			value: "",
		},
		{
			name:  "canonical spelling passes through",
			value: "Stable,Empty",
			want:  []string{"Stable", "Empty"},
		},
		{
			name:  "case is normalised to the broker's spelling",
			value: "stable,PREPARINGREBALANCE,completingRebalance",
			want:  []string{"Stable", "PreparingRebalance", "CompletingRebalance"},
		},
		{
			name:  "whitespace and empty entries are ignored",
			value: " Stable , , Dead ",
			want:  []string{"Stable", "Dead"},
		},
		{
			// A typo is not a no-op: the broker matches literally, so this
			// would list nothing and report a cluster with no groups.
			name:    "a typo is rejected at startup",
			value:   "Stabel",
			wantErr: "GROUP_STATES",
		},
		{
			// The collector describes groups over the classic path, so it could
			// not describe what a KIP-848 state would list.
			name:    "new-protocol-only states are rejected",
			value:   "Reconciling",
			wantErr: "Reconciling",
		},
		{
			name:    "one bad entry fails the whole list",
			value:   "Stable,Zombie",
			wantErr: "Zombie",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, base(map[string]string{"GROUP_STATES": tc.value}))
			cfg, err := Load()
			if tc.wantErr != "" {
				requireErrContains(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if len(cfg.GroupStates) != len(tc.want) {
				t.Fatalf("GroupStates = %v, want %v", cfg.GroupStates, tc.want)
			}
			for i, want := range tc.want {
				if cfg.GroupStates[i] != want {
					t.Errorf("GroupStates[%d] = %q, want %q", i, cfg.GroupStates[i], want)
				}
			}
		})
	}
}

func TestGroupStatesWarnings(t *testing.T) {
	t.Run("a filtered listing warns that offsets shrink too", func(t *testing.T) {
		setEnv(t, base(map[string]string{"GROUP_STATES": "Stable,Empty"}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		w := strings.Join(cfg.Warnings(), "\n")
		if !strings.Contains(w, "GROUP_STATES=Stable,Empty") || !strings.Contains(w, "offsets[]") {
			t.Errorf("warnings = %v", cfg.Warnings())
		}
	})

	t.Run("excluding Empty warns that dead consumers vanish", func(t *testing.T) {
		setEnv(t, base(map[string]string{"GROUP_STATES": "Stable"}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !strings.Contains(strings.Join(cfg.Warnings(), "\n"), "excludes Empty") {
			t.Errorf("warnings = %v", cfg.Warnings())
		}
	})

	t.Run("no filter, no warning", func(t *testing.T) {
		setEnv(t, base(nil))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if strings.Contains(strings.Join(cfg.Warnings(), "\n"), "GROUP_STATES") {
			t.Errorf("an unset GROUP_STATES warns: %v", cfg.Warnings())
		}
	})
}

func TestRedactedShowsTheGroupStateFilter(t *testing.T) {
	setEnv(t, base(nil))
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !strings.Contains(cfg.Redacted(), "group_states=-") {
		t.Errorf("redacted output does not report an absent filter: %s", cfg.Redacted())
	}

	setEnv(t, base(map[string]string{"GROUP_STATES": "stable,empty"}))
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// The normalised spelling is what goes on the wire, so it is what the
	// startup line must show.
	if !strings.Contains(cfg.Redacted(), "group_states=Stable,Empty") {
		t.Errorf("redacted output missing the filter in force: %s", cfg.Redacted())
	}
}

func TestLoadScalarParsing(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{name: "bad int", env: base(map[string]string{"EXPORT_QUEUE_SIZE": "lots"}), wantErr: "EXPORT_QUEUE_SIZE"},
		{name: "bad bool", env: base(map[string]string{"KAFKA_TLS_ENABLED": "yes-please"}), wantErr: "KAFKA_TLS_ENABLED"},
		{name: "bad duration", env: base(map[string]string{"EXPORT_TIMEOUT": "ten"}), wantErr: "EXPORT_TIMEOUT"},
		{name: "non positive queue", env: base(map[string]string{"EXPORT_QUEUE_SIZE": "0"}), wantErr: "EXPORT_QUEUE_SIZE must be > 0"},
		{name: "negative retries", env: base(map[string]string{"EXPORT_MAX_RETRIES": "-1"}), wantErr: "EXPORT_MAX_RETRIES must be >= 0"},
		{name: "negative backups", env: base(map[string]string{"EXPORT_FILE_MAX_BACKUPS": "-1"}), wantErr: "EXPORT_FILE_MAX_BACKUPS must be >= 0"},
		{name: "bool accepts 1", env: base(map[string]string{"KAFKA_TLS_ENABLED": "1"})},
		{name: "negative max mb disables rotation", env: base(map[string]string{"EXPORT_FILE_MAX_MB": "-1"})},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)
			_, err := Load()
			if tc.wantErr != "" {
				requireErrContains(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
		})
	}
}

func TestLoadDefaults(t *testing.T) {
	setEnv(t, base(nil))
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"ExportFile", cfg.ExportFile, DefaultExportFile},
		{"ExportFileMaxMB", cfg.ExportFileMaxMB, DefaultExportFileMaxMB},
		{"ExportFileMaxBackups", cfg.ExportFileMaxBackups, DefaultExportMaxBackups},
		{"ExportQueueSize", cfg.ExportQueueSize, DefaultExportQueueSize},
		{"ExportMaxRetries", cfg.ExportMaxRetries, DefaultExportMaxRetries},
		{"ExportBaseDelay", cfg.ExportBaseDelay, DefaultExportBaseDelay},
		{"ExportTimeout", cfg.ExportTimeout, DefaultExportTimeout},
		{"IncludeInternalTopics", cfg.IncludeInternalTopics, false},
		{"CollectLastStableOffset", cfg.CollectLastStableOffset, DefaultCollectLSO},
		{"CollectConsumerGroups", cfg.CollectConsumerGroups, DefaultCollectConsumerGroups},
		{"CollectLogDirs", cfg.CollectLogDirs, DefaultCollectLogDirs},
		{"LogDirsEvery", cfg.LogDirsEvery, DefaultLogDirsEvery},
		{"CollectThroughputWindow", cfg.CollectThroughputWindow, DefaultCollectThroughputWindow},
		{"ThroughputWindow", cfg.ThroughputWindow, DefaultThroughputWindow},
		{"ThroughputWindowEvery", cfg.ThroughputWindowEvery, DefaultThroughputWindowEvery},
		{"CollectConfigs", cfg.CollectConfigs, DefaultCollectConfigs},
		{"ConfigsEvery", cfg.ConfigsEvery, DefaultConfigsEvery},
		{"CollectMaxTimestamp", cfg.CollectMaxTimestamp, DefaultCollectMaxTimestamp},
		{"MaxTimestampEvery", cfg.MaxTimestampEvery, DefaultMaxTimestampEvery},
		{"CollectTieredOffsets", cfg.CollectTieredOffsets, DefaultCollectTieredOffsets},
		{"CollectShareGroups", cfg.CollectShareGroups, DefaultCollectShareGroups},
		{"MaxErrors", cfg.MaxErrors, DefaultMaxErrors},
		{"MaxErrorSamples", cfg.MaxErrorSamples, DefaultMaxErrorSamples},
		// 0 = unlimited; see DefaultMaxEntities.
		{"MaxTopics", cfg.MaxTopics, 0},
		{"MaxPartitionsPerTopic", cfg.MaxPartitionsPerTopic, 0},
		{"MaxGroups", cfg.MaxGroups, 0},
		{"MaxMembersPerGroup", cfg.MaxMembersPerGroup, 0},
		{"MaxOffsetsPerGroup", cfg.MaxOffsetsPerGroup, 0},
		{"LogLevel", cfg.LogLevel, "info"},
		{"ExportTarget", cfg.ExportTarget(), DefaultExportFile},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:9092" {
		t.Errorf("KafkaBrokers = %v", cfg.KafkaBrokers)
	}
	// An unset GROUP_STATES must list every state, not silently adopt one.
	if len(cfg.GroupStates) != 0 {
		t.Errorf("GroupStates = %v, want empty", cfg.GroupStates)
	}
}

func TestLoadLogLevel(t *testing.T) {
	tests := []struct {
		value   string
		want    slog.Level
		wantErr bool
	}{
		{value: "", want: slog.LevelInfo},
		{value: "debug", want: slog.LevelDebug},
		{value: "info", want: slog.LevelInfo},
		{value: "warn", want: slog.LevelWarn},
		{value: "error", want: slog.LevelError},
		{value: "ERROR", want: slog.LevelError},
		{value: "trace", wantErr: true},
	}

	for _, tc := range tests {
		name := tc.value
		if name == "" {
			name = "unset"
		}
		t.Run(name, func(t *testing.T) {
			setEnv(t, base(map[string]string{"LOG_LEVEL": tc.value}))
			cfg, err := Load()
			if tc.wantErr {
				requireErrContains(t, err, "LOG_LEVEL")
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := cfg.SlogLevel(); got != tc.want {
				t.Fatalf("SlogLevel() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAgentInstanceID(t *testing.T) {
	t.Run("explicit wins", func(t *testing.T) {
		setEnv(t, base(map[string]string{"AGENT_INSTANCE_ID": "prod-agent-7"}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.AgentInstanceID != "prod-agent-7" {
			t.Fatalf("AgentInstanceID = %q", cfg.AgentInstanceID)
		}
	})

	t.Run("default is hostname plus suffix and is unique", func(t *testing.T) {
		setEnv(t, base(nil))
		first, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		second, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !strings.Contains(first.AgentInstanceID, "-") {
			t.Fatalf("AgentInstanceID %q has no suffix", first.AgentInstanceID)
		}
		if first.AgentInstanceID == second.AgentInstanceID {
			t.Fatalf("two agents on one host collided: %q", first.AgentInstanceID)
		}
	})
}

func TestRedactedHidesSecrets(t *testing.T) {
	setEnv(t, base(map[string]string{
		"EXPORT_MODE":          "http",
		"EXPORT_ENDPOINT":      "https://ingest.example/v1",
		"API_KEY":              "super-secret-api-key",
		"KAFKA_SASL_MECHANISM": "PLAIN",
		"KAFKA_SASL_USERNAME":  "metrics-agent",
		"KAFKA_SASL_PASSWORD":  "hunter2-do-not-log",
	}))
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	for _, s := range []string{cfg.Redacted(), cfg.String()} {
		for _, secret := range []string{"super-secret-api-key", "hunter2-do-not-log"} {
			if strings.Contains(s, secret) {
				t.Fatalf("redacted output leaked %q: %s", secret, s)
			}
		}
		for _, want := range []string{"localhost:9092", "https://ingest.example/v1", "metrics-agent", "export_mode=http"} {
			if !strings.Contains(s, want) {
				t.Errorf("redacted output missing %q: %s", want, s)
			}
		}
	}
}

func TestWarnings(t *testing.T) {
	setEnv(t, base(map[string]string{
		"EXPORT_MODE":         "stdout",
		"EXPORT_ENDPOINT":     "https://ingest.example/v1",
		"COLLECTION_INTERVAL": "10s",
		"COLLECTION_TIMEOUT":  "30s",
	}))
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Warnings()) != 2 {
		t.Fatalf("Warnings() = %v, want 2", cfg.Warnings())
	}
}

func TestLoadReportsEveryProblemAtOnce(t *testing.T) {
	setEnv(t, map[string]string{
		"COLLECTION_INTERVAL": "0s",
		"LOG_LEVEL":           "loud",
		"TOPIC_INCLUDE_REGEX": "[",
	})
	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"KAFKA_BROKERS", "COLLECTION_INTERVAL", "LOG_LEVEL", "TOPIC_INCLUDE_REGEX"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

func requireErrContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error %v does not contain %q", err, want)
	}
}

func TestLoadCapValidation(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{name: "negative errors", env: base(map[string]string{"MAX_ERRORS": "-1"}), wantErr: "MAX_ERRORS must be >= 0"},
		{name: "negative topics", env: base(map[string]string{"MAX_TOPICS": "-1"}), wantErr: "MAX_TOPICS must be >= 0"},
		{name: "negative partitions", env: base(map[string]string{"MAX_PARTITIONS_PER_TOPIC": "-1"}), wantErr: "MAX_PARTITIONS_PER_TOPIC must be >= 0"},
		{name: "negative groups", env: base(map[string]string{"MAX_GROUPS": "-1"}), wantErr: "MAX_GROUPS must be >= 0"},
		{name: "negative members", env: base(map[string]string{"MAX_MEMBERS_PER_GROUP": "-1"}), wantErr: "MAX_MEMBERS_PER_GROUP must be >= 0"},
		{name: "negative offsets", env: base(map[string]string{"MAX_OFFSETS_PER_GROUP": "-1"}), wantErr: "MAX_OFFSETS_PER_GROUP must be >= 0"},
		{name: "bad integer", env: base(map[string]string{"MAX_GROUPS": "many"}), wantErr: "MAX_GROUPS"},
		{name: "zero samples", env: base(map[string]string{"MAX_ERROR_SAMPLES": "0"}), wantErr: "MAX_ERROR_SAMPLES must be >= 1"},
		{name: "negative samples", env: base(map[string]string{"MAX_ERROR_SAMPLES": "-1"}), wantErr: "MAX_ERROR_SAMPLES must be >= 1"},
		{name: "zero errors means unlimited", env: base(map[string]string{"MAX_ERRORS": "0"})},
		{name: "zero entity caps mean unlimited", env: base(map[string]string{"MAX_TOPICS": "0", "MAX_GROUPS": "0"})},
		{name: "one sample is legal", env: base(map[string]string{"MAX_ERROR_SAMPLES": "1"})},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)
			_, err := Load()
			if tc.wantErr != "" {
				requireErrContains(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
		})
	}
}

func TestLoadReportsEveryCapProblemAtOnce(t *testing.T) {
	setEnv(t, base(map[string]string{
		"MAX_ERRORS":            "-1",
		"MAX_ERROR_SAMPLES":     "0",
		"MAX_TOPICS":            "-2",
		"MAX_OFFSETS_PER_GROUP": "-3",
	}))
	_, err := Load()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"MAX_ERRORS", "MAX_ERROR_SAMPLES", "MAX_TOPICS", "MAX_OFFSETS_PER_GROUP"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

func TestLoadOptionalCollectors(t *testing.T) {
	t.Run("explicit values override the defaults", func(t *testing.T) {
		setEnv(t, base(map[string]string{
			"COLLECT_LAST_STABLE_OFFSET": "false",
			"COLLECT_LOG_DIRS":           "true",
			"LOG_DIRS_EVERY":             "20",
		}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.CollectLastStableOffset || !cfg.CollectLogDirs || cfg.LogDirsEvery != 20 {
			t.Errorf("collectors = %t/%t/%d, want false/true/20",
				cfg.CollectLastStableOffset, cfg.CollectLogDirs, cfg.LogDirsEvery)
		}
	})

	t.Run("a zero cadence is rejected, not reinterpreted", func(t *testing.T) {
		setEnv(t, base(map[string]string{"LOG_DIRS_EVERY": "0"}))
		_, err := Load()
		requireErrContains(t, err, "LOG_DIRS_EVERY")
	})

	t.Run("a malformed boolean is reported, not defaulted", func(t *testing.T) {
		setEnv(t, base(map[string]string{"COLLECT_LOG_DIRS": "yes please"}))
		_, err := Load()
		requireErrContains(t, err, "COLLECT_LOG_DIRS")
	})

	t.Run("the tier-1 phases are configurable end to end", func(t *testing.T) {
		setEnv(t, base(map[string]string{
			"COLLECT_THROUGHPUT_WINDOW": "true",
			"THROUGHPUT_WINDOW":         "10m",
			"THROUGHPUT_WINDOW_EVERY":   "4",
		}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !cfg.CollectThroughputWindow || cfg.ThroughputWindow != 10*time.Minute || cfg.ThroughputWindowEvery != 4 {
			t.Errorf("throughput window = %t/%s/%d", cfg.CollectThroughputWindow, cfg.ThroughputWindow, cfg.ThroughputWindowEvery)
		}
	})

	// The always-on phases have no environment key at all, so a deployment that
	// still carries one from an older release must not silently look configured.
	t.Run("the always-on phases take no environment key", func(t *testing.T) {
		setEnv(t, base(map[string]string{
			"COLLECT_REASSIGNMENTS": "false",
			"COLLECT_EPOCH_PROBES":  "false",
			"COLLECT_RPC_STATS":     "false",
			"COLLECT_LATEST_TIERED": "false",
		}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if strings.Contains(cfg.Redacted(), "collect_reassignments") ||
			strings.Contains(cfg.Redacted(), "collect_epoch_probes") ||
			strings.Contains(cfg.Redacted(), "collect_rpc_stats") ||
			strings.Contains(cfg.Redacted(), "collect_latest_tiered") {
			t.Errorf("a removed knob still reaches the startup line: %s", cfg.Redacted())
		}
	})

	// Every one of these is a modulus, a ticker period or a window width, so the
	// value Load would otherwise pass on either panics or asks about the future.
	t.Run("nonsensical cadences and widths are rejected", func(t *testing.T) {
		for key, value := range map[string]string{
			"THROUGHPUT_WINDOW":       "0s",
			"THROUGHPUT_WINDOW_EVERY": "0",
		} {
			setEnv(t, base(map[string]string{key: value}))
			_, err := Load()
			requireErrContains(t, err, key)
		}
	})
}

func TestCollectorWarnings(t *testing.T) {
	t.Run("disabling the LSO warns about silent false lag", func(t *testing.T) {
		setEnv(t, base(map[string]string{"COLLECT_LAST_STABLE_OFFSET": "false"}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !strings.Contains(strings.Join(cfg.Warnings(), "\n"), "COLLECT_LAST_STABLE_OFFSET=false") {
			t.Errorf("warnings = %v", cfg.Warnings())
		}
	})

	t.Run("log dirs on every cycle warns", func(t *testing.T) {
		setEnv(t, base(map[string]string{"COLLECT_LOG_DIRS": "true", "LOG_DIRS_EVERY": "1"}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !strings.Contains(strings.Join(cfg.Warnings(), "\n"), "O(replicas)") {
			t.Errorf("warnings = %v", cfg.Warnings())
		}
	})

	t.Run("a slow cadence does not warn", func(t *testing.T) {
		setEnv(t, base(map[string]string{"COLLECT_LOG_DIRS": "true"}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(cfg.Warnings()) != 0 {
			t.Errorf("enabling log dirs at the default cadence warns: %v", cfg.Warnings())
		}
	})

	// A window narrower than the interval is legal and useless: the backend can
	// difference two consecutive batches over that span for free.
	t.Run("a throughput window narrower than the interval warns", func(t *testing.T) {
		setEnv(t, base(map[string]string{"COLLECT_THROUGHPUT_WINDOW": "true", "THROUGHPUT_WINDOW": "1s"}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !strings.Contains(strings.Join(cfg.Warnings(), "\n"), "THROUGHPUT_WINDOW") {
			t.Errorf("warnings = %v", cfg.Warnings())
		}
	})
}

func TestRedactedShowsTheCadenceOnlyWhenItApplies(t *testing.T) {
	setEnv(t, base(nil))
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	s := cfg.Redacted()
	if !strings.Contains(s, "collect_last_stable_offset=true") || !strings.Contains(s, "collect_log_dirs=true") {
		t.Errorf("redacted output missing the collector switches: %s", s)
	}
	// Log dirs are on by default, so their cadence IS in force and must print.
	if !strings.Contains(s, fmt.Sprintf("log_dirs_every=%d", DefaultLogDirsEvery)) {
		t.Errorf("redacted output missing the cadence in force: %s", s)
	}
	// The throughput window is the off-by-default phase this half of the test
	// needs: its cadence must stay hidden while it cannot run.
	if strings.Contains(s, "throughput_window_every=") {
		t.Errorf("redacted output prints a cadence for a disabled phase: %s", s)
	}
	for _, want := range []string{
		"collect_throughput_window=false",
		"collect_consumer_groups=true",
		"collect_max_timestamp=true",
		"collect_tiered_offsets=false",
		"collect_share_groups=false",
		"collect_configs=true",
		// On by default, so its cadence IS in force and must be printed.
		fmt.Sprintf("configs_every=%d", DefaultConfigsEvery),
	} {
		if !strings.Contains(s, want) {
			t.Errorf("redacted output missing %q: %s", want, s)
		}
	}
	// Leading spaces: "collect_throughput_window=" contains the width's own key.
	for _, unwanted := range []string{" throughput_window="} {
		if strings.Contains(s, unwanted) {
			t.Errorf("redacted output prints %q for a disabled phase: %s", unwanted, s)
		}
	}
	// The other direction: enabling a phase brings its cadence into the line,
	// and disabling one that defaults on takes it back out.
	setEnv(t, base(map[string]string{"COLLECT_THROUGHPUT_WINDOW": "true"}))
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !strings.Contains(cfg.Redacted(), fmt.Sprintf("throughput_window_every=%d", DefaultThroughputWindowEvery)) {
		t.Errorf("redacted output missing the cadence in force: %s", cfg.Redacted())
	}

	setEnv(t, base(map[string]string{"COLLECT_LOG_DIRS": "false"}))
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if strings.Contains(cfg.Redacted(), "log_dirs_every=") {
		t.Errorf("redacted output prints a cadence for a disabled phase: %s", cfg.Redacted())
	}

	setEnv(t, base(map[string]string{"COLLECT_CONFIGS": "false"}))
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if strings.Contains(cfg.Redacted(), "configs_every=") {
		t.Errorf("redacted output prints a cadence for a disabled phase: %s", cfg.Redacted())
	}

	setEnv(t, base(map[string]string{"COLLECT_TIERED_OFFSETS": "true"}))
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !strings.Contains(cfg.Redacted(), "collect_tiered_offsets=true") {
		t.Errorf("redacted output missing the phase in force: %s", cfg.Redacted())
	}
}

// TestRedactedNamesEveryCollectorSwitch is the guard on the startup line itself.
// Redacted is the only record of what an agent was running with, and the way it
// went wrong before was by omission: 0226e0c added seven collector settings, five
// of them default-on, and none of them reached this line. Rather than list them
// by hand a third time, this derives the expected keys from the Collect* and
// *Every fields on Config.
func TestRedactedNamesEveryCollectorSwitch(t *testing.T) {
	setEnv(t, base(map[string]string{
		// Every subordinate and cadenced phase forced on, so nothing is legitimately
		// absent and any missing key is a genuine omission.
		"COLLECT_THROUGHPUT_WINDOW": "true",
		"COLLECT_TIERED_OFFSETS":    "true",
		"COLLECT_SHARE_GROUPS":      "true",
	}))
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	line := cfg.Redacted()

	rt := reflect.TypeOf(*cfg)
	for i := 0; i < rt.NumField(); i++ {
		name := rt.Field(i).Name
		if !strings.HasPrefix(name, "Collect") && !strings.HasSuffix(name, "Every") {
			continue
		}
		if name == "CollectionInterval" || name == "CollectionTimeout" {
			continue
		}
		key := snakeCase(name)
		if !strings.Contains(line, " "+key+"=") {
			t.Errorf("Config.%s is a shipped collector setting but %q never reaches the startup line: %s", name, key, line)
		}
	}
}

// snakeCase mirrors the naming Redacted uses for its keys. Acronyms stay one
// word: CollectRPCStats is printed as collect_rpc_stats, not collect_r_p_c_stats.
func snakeCase(s string) string {
	r := []rune(s)
	var b strings.Builder
	for i, c := range r {
		upper := c >= 'A' && c <= 'Z'
		if upper && i > 0 {
			prevLower := r[i-1] >= 'a' && r[i-1] <= 'z'
			nextLower := i+1 < len(r) && r[i+1] >= 'a' && r[i+1] <= 'z'
			if prevLower || nextLower {
				b.WriteByte('_')
			}
		}
		if upper {
			b.WriteRune(c - 'A' + 'a')
			continue
		}
		b.WriteRune(c)
	}
	return b.String()
}

func TestCapWarnings(t *testing.T) {
	t.Run("entity caps warn once each and MAX_GROUPS names the offsets section", func(t *testing.T) {
		setEnv(t, base(map[string]string{"MAX_TOPICS": "100", "MAX_GROUPS": "50"}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		w := strings.Join(cfg.Warnings(), "\n")
		for _, want := range []string{"MAX_TOPICS=100", "MAX_GROUPS=50", "MAX_GROUPS also truncates the offsets section"} {
			if !strings.Contains(w, want) {
				t.Errorf("warnings missing %q: %s", want, w)
			}
		}
	})

	t.Run("unbounded errors warn", func(t *testing.T) {
		setEnv(t, base(map[string]string{"MAX_ERRORS": "0"}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !strings.Contains(strings.Join(cfg.Warnings(), "\n"), "MAX_ERRORS=0 leaves errors[] unbounded") {
			t.Errorf("warnings = %v", cfg.Warnings())
		}
	})

	t.Run("a large sample count defeats dedup", func(t *testing.T) {
		setEnv(t, base(map[string]string{"MAX_ERROR_SAMPLES": "500"}))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !strings.Contains(strings.Join(cfg.Warnings(), "\n"), "largely defeats error deduplication") {
			t.Errorf("warnings = %v", cfg.Warnings())
		}
	})

	t.Run("defaults warn about nothing", func(t *testing.T) {
		setEnv(t, base(nil))
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(cfg.Warnings()) != 0 {
			t.Errorf("default config warns: %v", cfg.Warnings())
		}
	})
}

func TestRedactedShowsOnlySetCaps(t *testing.T) {
	setEnv(t, base(map[string]string{"MAX_GROUPS": "50"}))
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	s := cfg.Redacted()
	if !strings.Contains(s, "max_errors=1000") || !strings.Contains(s, "max_groups=50") {
		t.Errorf("redacted output missing the caps in force: %s", s)
	}
	if strings.Contains(s, "max_topics=") {
		t.Errorf("redacted output prints an unlimited cap: %s", s)
	}
}

// The endpoint default is applied inside http mode, never before it. Assigned
// earlier it would make ExportEndpoint always non-empty, and EXPORT_MODE is
// inferred as http IFF an endpoint is set -- so every agent started with no
// configuration at all would silently begin POSTing at production.
func TestEndpointDefaultCannotFlipAnUnconfiguredAgentIntoHTTPMode(t *testing.T) {
	setEnv(t, map[string]string{"KAFKA_BROKERS": "localhost:9092"})
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ExportMode != ExportModeFile {
		t.Fatalf("ExportMode = %q with no export config, want %q", cfg.ExportMode, ExportModeFile)
	}
	if cfg.ExportEndpoint != "" {
		t.Errorf("ExportEndpoint = %q in file mode, want empty", cfg.ExportEndpoint)
	}

	setEnv(t, map[string]string{"KAFKA_BROKERS": "localhost:9092", "EXPORT_MODE": "http", "API_KEY": "k"})
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ExportEndpoint != DefaultExportEndpoint {
		t.Errorf("ExportEndpoint = %q, want the default %q", cfg.ExportEndpoint, DefaultExportEndpoint)
	}
}

// TestAllKeysCoversEveryEnvVarLoadReads is the guard that keeps this file's
// tests hermetic. setEnv blanks exactly the names in allKeys, so a knob that
// Load reads but allKeys omits is inherited from the developer's or the CI
// runner's shell: exporting COLLECT_CONFIGS=false would then silently change
// what every test in this package loads, including TestLoadDefaults, which
// would stop being able to see that the default moved.
//
// Rather than restate the list by hand a second time, this reads the package
// source. Every env var in this package is spelled as an ALL_CAPS string
// literal with at least one underscore — p.str/p.boolean/p.integer/p.duration/
// p.regex calls, the os.Getenv calls, and the MAX_* table alike — and nothing
// else in the package is spelled that way (SASL "PLAIN" has no underscore).
// The check runs in both directions, so a deleted knob leaves a stale name
// behind just as loudly as an added one leaves a gap.
func TestAllKeysCoversEveryEnvVarLoadReads(t *testing.T) {
	envLit := regexp.MustCompile(`"([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)"`)

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	inSource := map[string]string{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", name, err)
		}
		for _, m := range envLit.FindAllStringSubmatch(string(src), -1) {
			inSource[m[1]] = name
		}
	}
	if len(inSource) == 0 {
		t.Fatal("found no env var literals in the package source; the scan is broken, not the config")
	}

	known := map[string]bool{}
	for _, k := range allKeys {
		known[k] = true
	}
	for key, file := range inSource {
		if !known[key] {
			t.Errorf("%s is read in %s but missing from allKeys: setEnv does not blank it, so the "+
				"ambient environment leaks into every test in this package. Add it to allKeys, "+
				"and add its default to TestLoadDefaults.", key, file)
		}
	}
	for _, k := range allKeys {
		if _, ok := inSource[k]; !ok {
			t.Errorf("allKeys lists %s but no non-test file in this package mentions it; the knob "+
				"was deleted and the name outlived it.", k)
		}
	}
}
