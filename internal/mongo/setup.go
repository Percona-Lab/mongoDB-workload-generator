package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/Percona-Lab/percona-load-generator-mongodb/internal/config"
	"github.com/Percona-Lab/percona-load-generator-mongodb/internal/logger"
	"github.com/Percona-Lab/percona-load-generator-mongodb/internal/workloads"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func InsertRandomDocuments(ctx context.Context, db *mongo.Database, col config.CollectionDefinition, count int, cfg *config.AppConfig) error {
	logger.Info("Seeding %d documents into '%s.%s'...", count, col.DatabaseName, col.Name)
	docs := make([]interface{}, count)

	for i := 0; i < count; i++ {
		docs[i] = workloads.GenerateDocument(col, cfg)
	}

	if len(docs) > 0 {
		// Use database from definition
		targetDB := db.Client().Database(col.DatabaseName)
		_, err := targetDB.Collection(col.Name).InsertMany(ctx, docs)
		if err != nil {
			return fmt.Errorf("insert documents into %s.%s: %w", col.DatabaseName, col.Name, err)
		}
	}
	return nil
}

// CreateCollectionsFromConfig creates collections and applies sharding if configured.
func CreateCollectionsFromConfig(ctx context.Context, db *mongo.Database, cfg *config.CollectionsFile, drop bool) error {
	adminDB := db.Client().Database("admin")

	// 1. Check if the cluster is actually sharded
	var helloResult bson.M
	isShardedCluster := false
	if err := adminDB.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&helloResult); err == nil {
		if msg, ok := helloResult["msg"].(string); ok && msg == "isdbgrid" {
			isShardedCluster = true
		}
	}

	for _, col := range cfg.Collections {
		// Derive database handle
		targetDB := db.Client().Database(col.DatabaseName)

		// 2. Drop if requested
		if drop {
			_ = targetDB.Collection(col.Name).Drop(ctx)
		}

		// 3. Create Collection
		if err := targetDB.CreateCollection(ctx, col.Name); err != nil {
			if drop {
				return fmt.Errorf("create collection %s.%s: %w", col.DatabaseName, col.Name, err)
			}
		}

		// 4. Configure Sharding
		if col.ShardConfig != nil {
			if !isShardedCluster {
				logger.Info("Skipping sharding for '%s': Cluster is not sharded (Replica Set)", col.Name)
				continue
			}

			_ = adminDB.RunCommand(ctx, bson.D{{Key: "enableSharding", Value: col.DatabaseName}})

			cmd := bson.D{
				{Key: "shardCollection", Value: fmt.Sprintf("%s.%s", col.DatabaseName, col.Name)},
				{Key: "key", Value: col.ShardConfig.Key},
			}
			if col.ShardConfig.Unique {
				cmd = append(cmd, bson.E{Key: "unique", Value: true})
			}

			if err := adminDB.RunCommand(ctx, cmd).Err(); err != nil {
				logger.Info("Warning: Failed to shard collection '%s': %v", col.Name, err)
			} else {
				logger.Info("Sharding configured for '%s' (Key: %v)", col.Name, col.ShardConfig.Key)
			}
		}
	}
	return nil
}

func CreateIndexesFromConfig(ctx context.Context, db *mongo.Database, cfg *config.CollectionsFile) error {
	for _, col := range cfg.Collections {
		if len(col.Indexes) == 0 {
			continue
		}

		targetDB := db.Client().Database(col.DatabaseName)
		collection := targetDB.Collection(col.Name)
		models := make([]mongo.IndexModel, 0, len(col.Indexes))

		for _, idx := range col.Indexes {
			keysDoc := bson.D{}
			for key, val := range idx.Keys {
				var indexValue interface{}
				if f, ok := val.(float64); ok {
					indexValue = int32(f)
				} else {
					indexValue = val
				}
				keysDoc = append(keysDoc, bson.E{Key: key, Value: indexValue})
			}
			models = append(models, mongo.IndexModel{Keys: keysDoc})
		}

		ctxCreate, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		opts := options.CreateIndexes()

		if _, err := collection.Indexes().CreateMany(ctxCreate, models, opts); err != nil {
			return fmt.Errorf("create indexes on %s.%s: %w", col.DatabaseName, col.Name, err)
		}

		logger.Info("Created %d indexes on '%s.%s'", len(models), col.DatabaseName, col.Name)
	}
	return nil
}
