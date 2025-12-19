package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v2"
)

// AppConfig holds the application's runtime configuration.
type AppConfig struct {
	URI string `yaml:"uri"`
	// DefaultWorkload controls both file loading and generator logic.
	DefaultWorkload bool `yaml:"default_workload"`

	CollectionsPath string `yaml:"collections_path"`
	QueriesPath     string `yaml:"queries_path"`
	DropCollections bool   `yaml:"drop_collections"`
	SkipSeed        bool   `yaml:"skip_seed"`
	DocumentsCount  int    `yaml:"documents_count"`
	Concurrency     int    `yaml:"concurrency"`

	Duration           string `yaml:"duration"`
	FindPercent        int    `yaml:"find_percent"`
	UpdatePercent      int    `yaml:"update_percent"`
	DeletePercent      int    `yaml:"delete_percent"`
	InsertPercent      int    `yaml:"insert_percent"`
	AggregatePercent   int    `yaml:"aggregate_percent"`
	TransactionPercent int    `yaml:"transaction_percent"`
	BulkInsertPercent  int    `yaml:"bulk_insert_percent"`
	InsertBatchSize    int    `yaml:"insert_batch_size"`
	UseTransactions    bool   `yaml:"use_transactions"`
	MaxTransactionOps  int    `yaml:"max_transaction_ops"`
	DebugMode          bool   `yaml:"debug_mode"`

	FindBatchSize         int   `yaml:"find_batch_size"`
	FindLimit             int64 `yaml:"find_limit"`
	UseFindOneForLimitOne bool  `yaml:"use_findone_for_limit_one"`
	InsertCacheSize       int   `yaml:"insert_cache_size"`
	StatusRefreshRateSec  int   `yaml:"status_refresh_rate_sec"`
	OpTimeoutMs           int   `yaml:"op_timeout_ms"`
	RetryAttempts         int   `yaml:"retry_attempts"`
	RetryBackoffMs        int   `yaml:"retry_backoff_ms"`

	ConnectionParams ConnectionParams       `yaml:"connection_params"`
	CustomParamsMap  map[string]interface{} `yaml:"custom_params"`
	Debug            bool                   `yaml:"debug"`
}

type ConnectionParams struct {
	Username               string `yaml:"username"`
	Password               string `yaml:"-"`
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
	if cfg.InsertBatchSize <= 0 {
		cfg.InsertBatchSize = 10
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
	if cfg.MaxTransactionOps <= 0 {
		cfg.MaxTransactionOps = 3
	}
}

func applyEnvOverrides(cfg *AppConfig) {
	// 1. Credentials
	if v := os.Getenv("PLGM_USERNAME"); v != "" {
		cfg.ConnectionParams.Username = v
	}
	if v := os.Getenv("PLGM_PASSWORD"); v != "" {
		cfg.ConnectionParams.Password = v
	}

	// 2. Default Workload (Explicit Override)
	if v := os.Getenv("PLGM_DEFAULT_WORKLOAD"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.DefaultWorkload = b
		}
	}

	// 3. Other Settings
	if envDebug := os.Getenv("PLGM_DEBUG_MODE"); envDebug != "" {
		if b, err := strconv.ParseBool(envDebug); err == nil {
			cfg.DebugMode = b
		}
	}
	if envURI := os.Getenv("PLGM_URI"); envURI != "" {
		cfg.URI = envURI
	}
	if v := os.Getenv("PLGM_DIRECT_CONNECTION"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.ConnectionParams.DirectConnection = b
		}
	}

	if v, exists := os.LookupEnv("PLGM_REPLICASET_NAME"); exists {
		cfg.ConnectionParams.ReplicaSetName = v
	}

	if v, exists := os.LookupEnv("PLGM_READ_PREFERENCE"); exists {
		cfg.ConnectionParams.ReadPreference = v
	}

	// 4. Custom Paths
	if envCollectionsPath := os.Getenv("PLGM_COLLECTIONS_PATH"); envCollectionsPath != "" {
		cfg.CollectionsPath = envCollectionsPath
	}
	if envQueriesPath := os.Getenv("PLGM_QUERIES_PATH"); envQueriesPath != "" {
		cfg.QueriesPath = envQueriesPath
	}

	if envDrop := os.Getenv("PLGM_DROP_COLLECTIONS"); envDrop != "" {
		if b, err := strconv.ParseBool(envDrop); err == nil {
			cfg.DropCollections = b
		}
	}
	if envSkip := os.Getenv("PLGM_SKIP_SEED"); envSkip != "" {
		if b, err := strconv.ParseBool(envSkip); err == nil {
			cfg.SkipSeed = b
		}
	}
	if envTx := os.Getenv("PLGM_USE_TRANSACTIONS"); envTx != "" {
		if b, err := strconv.ParseBool(envTx); err == nil {
			cfg.UseTransactions = b
		}
	}
	if v := os.Getenv("PLGM_MAX_TRANSACTION_OPS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.MaxTransactionOps = n
		}
	}
	if envDocs := os.Getenv("PLGM_DOCUMENTS_COUNT"); envDocs != "" {
		if n, err := strconv.Atoi(envDocs); err == nil && n >= 0 {
			cfg.DocumentsCount = n
		}
	}
	if envConcurrency := os.Getenv("PLGM_CONCURRENCY"); envConcurrency != "" {
		if n, err := strconv.Atoi(envConcurrency); err == nil && n > 0 {
			cfg.Concurrency = n
		}
	}
	if envDuration := os.Getenv("PLGM_DURATION"); envDuration != "" {
		cfg.Duration = envDuration
	}
	if p := os.Getenv("PLGM_FIND_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.FindPercent = n
		}
	}
	if p := os.Getenv("PLGM_UPDATE_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.UpdatePercent = n
		}
	}
	if p := os.Getenv("PLGM_DELETE_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.DeletePercent = n
		}
	}
	if p := os.Getenv("PLGM_INSERT_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.InsertPercent = n
		}
	}
	if p := os.Getenv("PLGM_AGGREGATE_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.AggregatePercent = n
		}
	}
	if p := os.Getenv("PLGM_TRANSACTION_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.TransactionPercent = n
		}
	}

	if v := os.Getenv("PLGM_FIND_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.FindBatchSize = n
		}
	}
	if v := os.Getenv("PLGM_FIND_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.FindLimit = int64(n)
		}
	}
	if v := os.Getenv("PLGM_INSERT_CACHE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.InsertCacheSize = n
		}
	}
	if v := os.Getenv("PLGM_OP_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.OpTimeoutMs = n
		}
	}
	if v := os.Getenv("PLGM_RETRY_ATTEMPTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.RetryAttempts = n
		}
	}
	if v := os.Getenv("PLGM_RETRY_BACKOFF_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.RetryBackoffMs = n
		}
	}
	if v := os.Getenv("PLGM_STATUS_REFRESH_RATE_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.StatusRefreshRateSec = n
		}
	}
	if p := os.Getenv("PLGM_BULK_INSERT_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.BulkInsertPercent = n
		}
	}
	if v := os.Getenv("PLGM_INSERT_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.InsertBatchSize = n
		}
	}
}

func normalizePercentages(cfg *AppConfig) {
	if !cfg.UseTransactions {
		cfg.TransactionPercent = 0
	}

	total := cfg.FindPercent + cfg.UpdatePercent + cfg.DeletePercent + cfg.InsertPercent + cfg.AggregatePercent + cfg.TransactionPercent + cfg.BulkInsertPercent
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
		cfg.TransactionPercent = int(float64(cfg.TransactionPercent) * factor)
		cfg.BulkInsertPercent = int(float64(cfg.BulkInsertPercent) * factor)

		finalTotal := cfg.FindPercent + cfg.UpdatePercent + cfg.DeletePercent + cfg.InsertPercent + cfg.AggregatePercent + cfg.TransactionPercent + cfg.BulkInsertPercent
		if finalTotal != 100 {
			cfg.FindPercent += (100 - finalTotal)
		}
	}
}
