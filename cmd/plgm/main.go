package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/Percona-Lab/percona-load-generator-mongodb/internal/config"
	"github.com/Percona-Lab/percona-load-generator-mongodb/internal/db"
	"github.com/Percona-Lab/percona-load-generator-mongodb/internal/logger"
	"github.com/Percona-Lab/percona-load-generator-mongodb/internal/mongo"
	"github.com/Percona-Lab/percona-load-generator-mongodb/internal/stats"
	"golang.org/x/term"
)

// This variable is populated at build time via -ldflags
var version = "1"

func main() {
	// 1. Setup Flags
	configFlag := flag.String("config", "config.yaml", "Path to the configuration file")
	versionFlag := flag.Bool("version", false, "Print version information and exit")

	// Custom Help Output
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nplgm: Percona Load Generator for MongoDB Clusters\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] [config_file]\n\n", os.Args[0])

		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  %s                    # Run with default 'config.yaml'\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s my_test.yaml       # Run with specific config file\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --help             # Show this help message\n\n", os.Args[0])

		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()

		// Environment Variables Section
		fmt.Fprintf(os.Stderr, "\nEnvironment Variables (Overrides):\n")

		fmt.Fprintf(os.Stderr, " [Connection]\n")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_URI", "Connection URI")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_USERNAME", "Database User")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_PASSWORD", "Database Password (Recommended: Use Prompt)")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_DIRECT_CONNECTION", "Force direct connection (true/false)")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_REPLICA_SET", "Replica Set name")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_READ_PREFERENCE", "nearest")

		fmt.Fprintf(os.Stderr, "\n [Workload Core]\n")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_DEFAULT_WORKLOAD", "Use built-in workload (true/false)")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_COLLECTIONS_PATH", "Path to collection JSON")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_QUERIES_PATH", "Path to query JSON")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_DURATION", "Test duration (e.g. 60s, 5m)")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_CONCURRENCY", "Number of active workers")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_DOCUMENTS_COUNT", "Initial seed document count")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_DROP_COLLECTIONS", "Drop collections on start (true/false)")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_SKIP_SEED", "Do not seed initial data on start (true/false)")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_DEBUG_MODE", "Enable verbose logic logs (true/false)")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_USE_TRANSACTIONS", "Enable transactional workloads (true/false)")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_MAX_TRANSACTION_OPS", "Maximum number of operations to group into a single transaction block")

		fmt.Fprintf(os.Stderr, "\n [Operation Ratios] (Must sum to ~100)\n")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_FIND_PERCENT", "% of ops that are FIND")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_UPDATE_PERCENT", "% of ops that are UPDATE")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_INSERT_PERCENT", "% of ops that are INSERT")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_DELETE_PERCENT", "% of ops that are DELETE")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_AGGREGATE_PERCENT", "% of ops that are AGGREGATE")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_TRANSACTION_PERCENT", "% of ops that are TRANSACTIONAL")

		fmt.Fprintf(os.Stderr, "\n [Performance Optimization]\n")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_FIND_BATCH_SIZE", "Docs returned per cursor batch")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_FIND_LIMIT", "Max docs per Find query")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_INSERT_CACHE_SIZE", "Generator buffer size")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_OP_TIMEOUT_MS", "Soft timeout per DB op (ms)")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_RETRY_ATTEMPTS", "Retry attempts for failures")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_RETRY_BACKOFF_MS", "Wait time between retries (ms)")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "PERCONALOAD_STATUS_REFRESH_RATE_SEC", "Status report interval (sec)")
		fmt.Fprintf(os.Stderr, "  %-35s %s\n", "GOMAXPROCS", "Go Runtime CPU limit")
		fmt.Fprintf(os.Stderr, "\n")
	}

	flag.Parse()

	// 2. Handle Version Flag
	if *versionFlag {
		fmt.Printf("plgm v%s\n", version)
		os.Exit(0)
	}

	// 3. Determine Config Path
	configPath := *configFlag
	if len(flag.Args()) > 0 {
		configPath = flag.Args()[0]
	}

	// 4. Validate Config Exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("Error: Configuration file '%s' not found.\n", configPath)
		fmt.Println("Use --help to see usage.")
		os.Exit(1)
	}

	ctx := context.Background()

	// Load app-level YAML
	appCfg, err := config.LoadAppConfig(configPath)
	if err != nil {
		log.Fatal("Failed to load application config:", err)
	}

	// --- Secure Credentials Logic ---

	// 1. Analyze the Base URI to see if it already has credentials
	u, err := url.Parse(appCfg.URI)
	if err != nil {
		log.Fatalf("Invalid PERCONALOAD_URI: %v", err)
	}
	uriHasUser := u.User != nil && u.User.Username() != ""

	// 2. Prompt for Username if missing (from both URI and Env/Config)
	if !uriHasUser && appCfg.ConnectionParams.Username == "" {
		fmt.Print("Enter MongoDB Username: ")
		var inputUser string
		if _, err := fmt.Scanln(&inputUser); err != nil {
			// Handle case where user hits enter or pipe closes
			if err.Error() != "unexpected newline" {
				log.Fatal(err)
			}
		}
		appCfg.ConnectionParams.Username = inputUser
	}

	// 3. Prompt for Password if missing
	// Logic: If we have a username defined in config/prompt (overriding whatever is in URI),
	// and no password is set for it, we must prompt.
	if appCfg.ConnectionParams.Username != "" && appCfg.ConnectionParams.Password == "" {
		fmt.Printf("Enter Password for user '%s': ", appCfg.ConnectionParams.Username)
		bytePassword, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			log.Fatal("\nError reading password:", err)
		}
		appCfg.ConnectionParams.Password = string(bytePassword)
		fmt.Println() // Print newline after input
	}

	// --- Load Collections ---
	collectionsCfg, err := config.LoadCollections(appCfg.CollectionsPath, appCfg.DefaultWorkload)
	if err != nil {
		log.Fatal("Failed to load collections:", err)
	}

	if len(collectionsCfg.Collections) == 0 {
		mode := "custom"
		if appCfg.DefaultWorkload {
			mode = "default"
		}
		log.Fatalf("No collections found in %s with default_workload=%t (mode=%s)",
			appCfg.CollectionsPath, appCfg.DefaultWorkload, mode)
	}

	// --- Load Queries ---
	queriesCfg, err := config.LoadQueries(appCfg.QueriesPath, appCfg.DefaultWorkload)
	if err != nil {
		log.Fatal("Failed to load query templates:", err)
	}

	// --- SMART QUERY FILTERING ---
	validCollections := make(map[string]bool)
	for _, col := range collectionsCfg.Collections {
		validCollections[col.Name] = true
	}

	var filteredQueries []config.QueryDefinition
	skippedCount := 0
	for _, q := range queriesCfg.Queries {
		if validCollections[q.Collection] {
			filteredQueries = append(filteredQueries, q)
		} else {
			skippedCount++
		}
	}
	queriesCfg.Queries = filteredQueries

	// Determine DB name from first collection for the banner
	dbName := collectionsCfg.Collections[0].DatabaseName

	// -----------------------------------------------------------------------------------
	// PRINT BANNER / CONFIGURATION
	// -----------------------------------------------------------------------------------
	stats.PrintConfiguration(appCfg, collectionsCfg.Collections, version)

	// --- Connect to DB ---
	conn, err := db.Connect(ctx, appCfg, dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Disconnect(ctx)

	// --- Log Setup Details (Execution Phase) ---
	// Now we print the logs, so they appear below the plan
	logger.Info("Loaded %d collection definition(s)", len(collectionsCfg.Collections))
	logger.Info("Loaded %d query templates(s)", len(queriesCfg.Queries))

	if skippedCount > 0 {
		logger.Info("Filtered out %d queries because their target collections were not found.", skippedCount)
	}

	// --- Collection & Index creation ---
	if err := mongo.CreateCollectionsFromConfig(ctx, conn.Database, collectionsCfg, appCfg.DropCollections); err != nil {
		log.Fatal(err)
	}

	if err := mongo.CreateIndexesFromConfig(ctx, conn.Database, collectionsCfg); err != nil {
		log.Fatal(err)
	}

	// --- Seed documents (initial dataset) ---
	if !appCfg.SkipSeed {
		if appCfg.DocumentsCount > 0 {
			// NOTE: Logging is handled inside InsertRandomDocuments,
			// but we can add a high-level log here if we want.
			for _, col := range collectionsCfg.Collections {
				if err := mongo.InsertRandomDocuments(ctx, conn.Database, col, appCfg.DocumentsCount, appCfg); err != nil {
					log.Fatal(err)
				}
			}
		}
	} else {
		logger.Info("Skipping data seeding (configured)")
	}

	// --- Workload execution ---
	if appCfg.DebugMode {
		logger.Info("Debug mode enabled: verbose output active")
	}

	if err := mongo.RunWorkload(ctx, conn.Database, collectionsCfg.Collections, queriesCfg.Queries, appCfg); err != nil {
		log.Fatal(err)
	}
}
