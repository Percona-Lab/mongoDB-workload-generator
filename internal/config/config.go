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
	SeedBatchSize      int    `yaml:"seed_batch_size"`
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

	// Apply overrides and capture which percentage fields were set
	overriddenStats := applyEnvOverrides(cfg)

	// Normalize based on what was overridden
	normalizePercentages(cfg, overriddenStats)

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
	// Default to 1000 for fast seeding
	if cfg.SeedBatchSize <= 0 {
		cfg.SeedBatchSize = 1000
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

// applyEnvOverrides updates the config from ENV vars and returns a map
// of percentage fields that were explicitly set.
func applyEnvOverrides(cfg *AppConfig) map[string]bool {
	// Track which percentages are overridden
	overrides := make(map[string]bool)

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

	// Percentages - we track these to prioritize them in normalization
	if p := os.Getenv("PLGM_FIND_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.FindPercent = n
			overrides["FindPercent"] = true
		}
	}
	if p := os.Getenv("PLGM_UPDATE_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.UpdatePercent = n
			overrides["UpdatePercent"] = true
		}
	}
	if p := os.Getenv("PLGM_DELETE_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.DeletePercent = n
			overrides["DeletePercent"] = true
		}
	}
	if p := os.Getenv("PLGM_INSERT_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.InsertPercent = n
			overrides["InsertPercent"] = true
		}
	}
	if p := os.Getenv("PLGM_AGGREGATE_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.AggregatePercent = n
			overrides["AggregatePercent"] = true
		}
	}
	if p := os.Getenv("PLGM_TRANSACTION_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.TransactionPercent = n
			overrides["TransactionPercent"] = true
		}
	}
	if p := os.Getenv("PLGM_BULK_INSERT_PERCENT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n >= 0 {
			cfg.BulkInsertPercent = n
			overrides["BulkInsertPercent"] = true
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
	if v := os.Getenv("PLGM_INSERT_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.InsertBatchSize = n
		}
	}
	if v := os.Getenv("PLGM_SEED_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.SeedBatchSize = n
		}
	}

	return overrides
}

func normalizePercentages(cfg *AppConfig, pinned map[string]bool) {
	// 1. Enforce Transaction flag constraint immediately
	if !cfg.UseTransactions {
		cfg.TransactionPercent = 0
		delete(pinned, "TransactionPercent")
	}

	// 2. Calculate the total of "pinned" (Environment overridden) stats
	pinnedTotal := 0
	if pinned["FindPercent"] {
		pinnedTotal += cfg.FindPercent
	}
	if pinned["UpdatePercent"] {
		pinnedTotal += cfg.UpdatePercent
	}
	if pinned["DeletePercent"] {
		pinnedTotal += cfg.DeletePercent
	}
	if pinned["InsertPercent"] {
		pinnedTotal += cfg.InsertPercent
	}
	if pinned["AggregatePercent"] {
		pinnedTotal += cfg.AggregatePercent
	}
	if pinned["TransactionPercent"] {
		pinnedTotal += cfg.TransactionPercent
	}
	if pinned["BulkInsertPercent"] {
		pinnedTotal += cfg.BulkInsertPercent
	}

	// 3. Logic:
	//    If Pinned Total >= 100: Zero out non-pinned, scale pinned if > 100.
	//    If Pinned Total < 100:  Distribute remainder among unpinned.

	if pinnedTotal >= 100 {
		// Zero out all non-pinned fields
		if !pinned["FindPercent"] {
			cfg.FindPercent = 0
		}
		if !pinned["UpdatePercent"] {
			cfg.UpdatePercent = 0
		}
		if !pinned["DeletePercent"] {
			cfg.DeletePercent = 0
		}
		if !pinned["InsertPercent"] {
			cfg.InsertPercent = 0
		}
		if !pinned["AggregatePercent"] {
			cfg.AggregatePercent = 0
		}
		if !pinned["TransactionPercent"] {
			cfg.TransactionPercent = 0
		}
		if !pinned["BulkInsertPercent"] {
			cfg.BulkInsertPercent = 0
		}

		// Normalize if pinned values sum > 100
		if pinnedTotal > 100 {
			factor := 100.0 / float64(pinnedTotal)
			if pinned["FindPercent"] {
				cfg.FindPercent = int(float64(cfg.FindPercent) * factor)
			}
			if pinned["UpdatePercent"] {
				cfg.UpdatePercent = int(float64(cfg.UpdatePercent) * factor)
			}
			if pinned["DeletePercent"] {
				cfg.DeletePercent = int(float64(cfg.DeletePercent) * factor)
			}
			if pinned["InsertPercent"] {
				cfg.InsertPercent = int(float64(cfg.InsertPercent) * factor)
			}
			if pinned["AggregatePercent"] {
				cfg.AggregatePercent = int(float64(cfg.AggregatePercent) * factor)
			}
			if pinned["TransactionPercent"] {
				cfg.TransactionPercent = int(float64(cfg.TransactionPercent) * factor)
			}
			if pinned["BulkInsertPercent"] {
				cfg.BulkInsertPercent = int(float64(cfg.BulkInsertPercent) * factor)
			}
		}

	} else {
		// pinnedTotal < 100. We have space left.
		remaining := 100 - pinnedTotal

		// Sum of unpinned (default) values
		unpinnedTotal := 0
		if !pinned["FindPercent"] {
			unpinnedTotal += cfg.FindPercent
		}
		if !pinned["UpdatePercent"] {
			unpinnedTotal += cfg.UpdatePercent
		}
		if !pinned["DeletePercent"] {
			unpinnedTotal += cfg.DeletePercent
		}
		if !pinned["InsertPercent"] {
			unpinnedTotal += cfg.InsertPercent
		}
		if !pinned["AggregatePercent"] {
			unpinnedTotal += cfg.AggregatePercent
		}
		if !pinned["TransactionPercent"] {
			unpinnedTotal += cfg.TransactionPercent
		}
		if !pinned["BulkInsertPercent"] {
			unpinnedTotal += cfg.BulkInsertPercent
		}

		// Scale unpinned values to fill the remaining space
		if unpinnedTotal > 0 {
			factor := float64(remaining) / float64(unpinnedTotal)

			if !pinned["FindPercent"] {
				cfg.FindPercent = int(float64(cfg.FindPercent) * factor)
			}
			if !pinned["UpdatePercent"] {
				cfg.UpdatePercent = int(float64(cfg.UpdatePercent) * factor)
			}
			if !pinned["DeletePercent"] {
				cfg.DeletePercent = int(float64(cfg.DeletePercent) * factor)
			}
			if !pinned["InsertPercent"] {
				cfg.InsertPercent = int(float64(cfg.InsertPercent) * factor)
			}
			if !pinned["AggregatePercent"] {
				cfg.AggregatePercent = int(float64(cfg.AggregatePercent) * factor)
			}
			if !pinned["TransactionPercent"] {
				cfg.TransactionPercent = int(float64(cfg.TransactionPercent) * factor)
			}
			if !pinned["BulkInsertPercent"] {
				cfg.BulkInsertPercent = int(float64(cfg.BulkInsertPercent) * factor)
			}
		} else {
			// Edge case: Pinned values sum to < 100 (e.g. 80%), but all unpinned defaults are 0.
			// We cannot distribute the remaining 20% proportionally among 0s.
			// Strategy: Assign the remainder to FindPercent (Selects) to ensure the workload sums to 100%.
			cfg.FindPercent += remaining
		}
	}

	// 4. Final check: Ensure total is exactly 100 (fixing integer rounding errors)
	finalTotal := cfg.FindPercent + cfg.UpdatePercent + cfg.DeletePercent + cfg.InsertPercent + cfg.AggregatePercent + cfg.TransactionPercent + cfg.BulkInsertPercent
	if finalTotal != 100 {
		// Add/Subtract difference to FindPercent (simplest safety net)
		cfg.FindPercent += (100 - finalTotal)
	}
}
