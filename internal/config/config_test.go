package config

import (
	"log/slog"
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
	"EXPORT_GZIP",
	"COLLECTION_INTERVAL",
	"COLLECTION_TIMEOUT",
	"INCLUDE_INTERNAL_TOPICS",
	"TOPIC_INCLUDE_REGEX",
	"TOPIC_EXCLUDE_REGEX",
	"GROUP_INCLUDE_REGEX",
	"GROUP_EXCLUDE_REGEX",
	"GROUP_STATES",
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
			name:    "http mode needs endpoint",
			env:     base(map[string]string{"EXPORT_MODE": "http", "API_KEY": "k"}),
			wantErr: "EXPORT_ENDPOINT is required when EXPORT_MODE=http",
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
			wantEvery:   30 * time.Second,
			wantTimeout: 24 * time.Second,
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
		{name: "bad bool", env: base(map[string]string{"EXPORT_GZIP": "yes-please"}), wantErr: "EXPORT_GZIP"},
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
		{"ExportGzip", cfg.ExportGzip, DefaultExportGzip},
		{"IncludeInternalTopics", cfg.IncludeInternalTopics, false},
		{"CollectLastStableOffset", cfg.CollectLastStableOffset, DefaultCollectLSO},
		{"CollectLogDirs", cfg.CollectLogDirs, DefaultCollectLogDirs},
		{"LogDirsEvery", cfg.LogDirsEvery, DefaultLogDirsEvery},
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
}

func TestRedactedShowsTheCadenceOnlyWhenItApplies(t *testing.T) {
	setEnv(t, base(nil))
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	s := cfg.Redacted()
	if !strings.Contains(s, "collect_last_stable_offset=true") || !strings.Contains(s, "collect_log_dirs=false") {
		t.Errorf("redacted output missing the collector switches: %s", s)
	}
	if strings.Contains(s, "log_dirs_every=") {
		t.Errorf("redacted output prints a cadence for a disabled phase: %s", s)
	}

	setEnv(t, base(map[string]string{"COLLECT_LOG_DIRS": "true"}))
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !strings.Contains(cfg.Redacted(), "log_dirs_every=10") {
		t.Errorf("redacted output missing the cadence in force: %s", cfg.Redacted())
	}
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
