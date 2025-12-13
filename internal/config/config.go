package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v2"
)

// AppConfig holds the application's runtime configuration.
// Struct tags: e.g.: when reading from YAML, the field named uri maps to URI in this struct
type AppConfig struct {
	URI string `yaml:"uri"`
	// DefaultWorkload controls both file loading and generator logic.
	// True: Load only "default.json" and use custom workload logic (e.g. flights).
	// False: Load user files (excluding default.json) and use generic generator.
	DefaultWorkload bool `yaml:"default_workload"`

	CollectionsPath string `yaml:"collections_path"`
	QueriesPath     string `yaml:"queries_path"`
	DropCollections bool   `yaml:"drop_collections"`
	SkipSeed        bool   `yaml:"skip_seed"`
	DocumentsCount  int    `yaml:"documents_count"`
	Concurrency     int    `yaml:"concurrency"`

	Duration         string `yaml:"duration"`
	FindPercent      int    `yaml:"find_percent"`
	UpdatePercent    int    `yaml:"update_percent"`
	DeletePercent    int    `yaml:"delete_percent"`
	InsertPercent    int    `yaml:"insert_percent"`
	AggregatePercent int    `yaml:"aggregate_percent"`
	DebugMode        bool   `yaml:"debug_mode"`

	FindBatchSize         int   `yaml:"find_batch_size"`
	FindLimit             int64 `yaml:"find_limit"`
	UseFindOneForLimitOne bool  `yaml:"use_findone_for_limit_one"`
	InsertCacheSize       int   `yaml:"insert_cache_size"`
	StatusRefreshRateSec  int   `yaml:"status_refresh_rate_sec"`
	OpTimeoutMs           int   `yaml:"op_timeout_ms"`
	RetryAttempts         int   `yaml:"retry_attempts"`
	RetryBackoffMs        int   `yaml:"retry_backoff_ms"`

	// Typed struct view
	ConnectionParams ConnectionParams `yaml:"connection_params"`

	// Raw/dynamic view
	CustomParamsMap map[string]interface{} `yaml:"custom_params"`

	// Enable debugging
	Debug bool `yaml:"debug"`
}

type ConnectionParams struct {
	Username               string `yaml:"username"`
	Password               string `yaml:"-"` // Never load from YAML
	AuthSource             string `yaml:"auth_source"`
	DirectConnection       bool   `yaml:"direct_connection"`
	ConnectionTimeout      int    `yaml:"connection_timeout"`
	ServerSelectionTimeout int    `yaml:"server_selection_timeout"`
	MaxPoolSize            int    `yaml:"max_pool_size"`
	MinPoolSize            int    `yaml:"min_pool_size"`
	MaxIdleTime            int    `yaml:"max_idle_time"`
	ReplicaSetName         string `yaml:"replicaset_name"`
	ReadPreference         string `yaml:"read_preference"`
}

// LoadAppConfig reads the configuration from the specified path and applies environment variable overrides.
func LoadAppConfig(path string) (*AppConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}

	cfg := &AppConfig{}
	if err := yaml.Unmarshal(b, cfg); err != nil {
		return nil, fmt.Errorf("invalid YAML format for config: %w", err)
	}

	applyEnvOverrides(cfg)
	normalizePercentages(cfg)
	applyDefaults(cfg)

	return cfg, nil
}

func applyDefaults(cfg *AppConfig) {
	if cfg.FindBatchSize <= 0 {
		cfg.FindBatchSize = 10
	}
	if cfg.FindLimit <= 0 {
		cfg.FindLimit = 5
	}
	if cfg.InsertCacheSize <= 0 {
		cfg.InsertCacheSize = 1000
	}
	if cfg.StatusRefreshRateSec <= 0 {
		cfg.StatusRefreshRateSec = 1
	}
	if cfg.OpTimeoutMs <= 0 {
		cfg.OpTimeoutMs = 500
	}
	if cfg.RetryAttempts <= 0 {
		cfg.RetryAttempts = 2
	}
	if cfg.RetryBackoffMs <= 0 {
		cfg.RetryBackoffMs = 5
	}

}

func applyEnvOverrides(cfg *AppConfig) {
	if v := os.Getenv("GENMONGOLOAD_USERNAME"); v != "" {
		cfg.ConnectionParams.Username = v
	}
	if v := os.Getenv("GENMONGOLOAD_PASSWORD"); v != "" {
		cfg.ConnectionParams.Password = v
	}
	if v := os.Getenv("GENMONGOLOAD_DEFAULT_WORKLOAD"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.DefaultWorkload = b
		}
	}
	if envDebug := os.Getenv("GENMONGOLOAD_DEBUG_MODE"); envDebug != "" {
		if b, err := strconv.ParseBool(envDebug); err == nil {
			cfg.DebugMode = b
		}
	}
	if envURI := os.Getenv("GENMONGOLOAD_URI"); envURI != "" {
		cfg.URI = envURI
	}
	if v := os.Getenv("GENMONGOLOAD_DIRECT_CONNECTION"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.ConnectionParams.DirectConnection = b
		}
	}
	// Check if the environment variable is explicitly defined (even if empty)
	if v, exists := os.LookupEnv("GENMONGOLOAD_REPLICA_SET"); exists {
		cfg.ConnectionParams.ReplicaSetName = v
	}
	if v, exists := os.LookupEnv("GENMONGOLOAD_READ_PREFERENCE"); exists {
		cfg.ConnectionParams.ReadPreference = v
	}

	// -------------------------------------------------------------------------
	// Custom Workload Logic:
	// If the user provides custom paths via environment variables, we infer
	// that they want to run a custom workload, so we force DefaultWorkload to false.
	// This overrides any previous setting (YAML or GENMONGOLOAD_DEFAULT_WORKLOAD).
	// -------------------------------------------------------------------------
	customWorkloadEnv := false

	if envCollectionsPath := os.Getenv("GENMONGOLOAD_COLLECTIONS_PATH"); envCollectionsPath != "" {
		cfg.CollectionsPath = envCollectionsPath
		customWorkloadEnv = true
	}
	if envQueriesPath := os.Getenv("GENMONGOLOAD_QUERIES_PATH"); envQueriesPath != "" {
		cfg.QueriesPath = envQueriesPath
		customWorkloadEnv = true
	}

	if customWorkloadEnv {
		cfg.DefaultWorkload = false
	}
	// -------------------------------------------------------------------------

	if envDrop := os.Getenv("GENMONGOLOAD_DROP_COLLECTIONS"); envDrop != "" {
		if b, err := strconv.ParseBool(envDrop); err == nil {
			cfg.DropCollections = b
		}
	}
	if envDrop := os.Getenv("GENMONGOLOAD_SKIP_SEED"); envDrop != "" {
		if b, err := strconv.ParseBool(envDrop); err == nil {
			cfg.SkipSeed = b
		}
	}
	if envDocs := os.Getenv("GENMONGOLOAD_DOCUMENTS_COUNT"); envDocs != "" {
		if n, err := strconv.Atoi(envDocs); err == nil && n >= 0 {
			cfg.DocumentsCount = n
		}
	}
	if envConcurrency := os.Getenv("GENMONGOLOAD_CONCURRENCY"); envConcurrency != "" {
		if n, err := strconv.Atoi(envConcurrency); err == nil && n > 0 {
			cfg.Concurrency = n
		}
	}
	if envDuration := os.Getenv("GENMONGOLOAD_DURATION"); envDuration != "" {
		cfg.Duration = envDuration
	}
	if p := os.Getenv("GENMONGOLOAD_FIND_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.FindPercent = n
		}
	}
	if p := os.Getenv("GENMONGOLOAD_UPDATE_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.UpdatePercent = n
		}
	}
	if p := os.Getenv("GENMONGOLOAD_DELETE_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.DeletePercent = n
		}
	}
	if p := os.Getenv("GENMONGOLOAD_INSERT_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.InsertPercent = n
		}
	}
	if p := os.Getenv("GENMONGOLOAD_AGGREGATE_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.AggregatePercent = n
		}
	}
	// runtime optimization
	if v := os.Getenv("GENMONGOLOAD_FIND_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.FindBatchSize = n
		}
	}
	if v := os.Getenv("GENMONGOLOAD_FIND_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.FindLimit = int64(n)
		}
	}
	if v := os.Getenv("GENMONGOLOAD_INSERT_CACHE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.InsertCacheSize = n
		}
	}
	if v := os.Getenv("GENMONGOLOAD_OP_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.OpTimeoutMs = n
		}
	}
	if v := os.Getenv("GENMONGOLOAD_RETRY_ATTEMPTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.RetryAttempts = n
		}
	}
	if v := os.Getenv("GENMONGOLOAD_RETRY_BACKOFF_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.RetryBackoffMs = n
		}
	}
	if v := os.Getenv("GENMONGOLOAD_STATUS_REFRESH_RATE_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.StatusRefreshRateSec = n
		}
	}
}

func normalizePercentages(cfg *AppConfig) {
	total := cfg.FindPercent + cfg.UpdatePercent + cfg.DeletePercent + cfg.InsertPercent + cfg.AggregatePercent
	if total <= 0 {
		cfg.FindPercent = 100
		return
	}
	if total != 100 {
		factor := 100.0 / float64(total)
		cfg.FindPercent = int(float64(cfg.FindPercent) * factor)
		cfg.UpdatePercent = int(float64(cfg.UpdatePercent) * factor)
		cfg.DeletePercent = int(float64(cfg.DeletePercent) * factor)
		cfg.InsertPercent = int(float64(cfg.InsertPercent) * factor)
		cfg.AggregatePercent = int(float64(cfg.AggregatePercent) * factor)

		finalTotal := cfg.FindPercent + cfg.UpdatePercent + cfg.DeletePercent + cfg.InsertPercent + cfg.AggregatePercent
		if finalTotal != 100 {
			cfg.FindPercent += (100 - finalTotal)
		}
	}
}
