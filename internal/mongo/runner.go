package mongo

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Percona-Lab/mongoDB-workload-generator/internal/config"
	"github.com/Percona-Lab/mongoDB-workload-generator/internal/stats"
	"github.com/Percona-Lab/mongoDB-workload-generator/internal/workloads"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// queryTask holds the information needed for a worker to run a single query.
type queryTask struct {
	definition config.QueryDefinition
	database   *mongo.Database
	runID      int64
	debug      bool
	rng        *rand.Rand
}

type workloadConfig struct {
	database           *mongo.Database
	appConfig          *config.AppConfig
	concurrency        int
	duration           time.Duration
	collections        []config.CollectionDefinition
	queryMap           map[string][]config.QueryDefinition
	percentages        map[string]int
	debug              bool
	findBatchSize      int32
	findLimit          int64
	maxInsertCache     int
	primaryFilterField string
	collector          *stats.Collector
}

var InsertDocumentCache chan map[string]interface{}

var operationTypes = []string{"find", "update", "delete", "insert", "aggregate"}

func selectOperation(percentages map[string]int, rng *rand.Rand) string {
	if percentages == nil {
		return "find"
	}
	r := rng.Intn(100)
	cum := 0
	for _, op := range operationTypes {
		cum += percentages[op]
		if r < cum {
			switch op {
			case "update":
				if rng.Intn(100) < 90 {
					return "updateOne"
				}
				return "updateMany"
			case "delete":
				if rng.Intn(100) < 90 {
					return "deleteOne"
				}
				return "deleteMany"
			default:
				return op
			}
		}
	}
	return "find"
}

func getPrimaryFilterField(ctx context.Context, db *mongo.Database, col config.CollectionDefinition) string {
	client := db.Client()
	dbName := db.Name()
	namespace := fmt.Sprintf("%s.%s", dbName, col.Name)
	configColl := client.Database("config").Collection("collections")

	var result struct {
		Key bson.M `bson:"key"`
	}
	filter := bson.M{"_id": namespace, "dropped": false}
	err := configColl.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		return "_id"
	}
	for k := range result.Key {
		return k
	}
	return "_id"
}

func generateFallbackQuery(ctx context.Context, db *mongo.Database, opType string, col config.CollectionDefinition, rng *rand.Rand, filterField string, cfg *config.AppConfig) (config.QueryDefinition, bool) {
	collectionName := col.Name
	fieldType := "int"
	if filterField == "_id" {
		fieldType = "string"
	}
	if def, ok := col.Fields[filterField]; ok {
		fieldType = def.Type
	}
	filter := map[string]interface{}{filterField: fmt.Sprintf("<%s>", fieldType)}

	if opType == "updateOne" || opType == "updateMany" {
		updatePayload := workloads.GenerateFallbackUpdate(col, cfg, rng)
		return config.QueryDefinition{
			Collection: collectionName,
			Operation:  opType,
			Filter:     filter,
			Update:     updatePayload,
		}, true
	}
	if opType == "deleteOne" || opType == "deleteMany" {
		return config.QueryDefinition{
			Collection: collectionName,
			Operation:  opType,
			Filter:     filter,
		}, true
	}
	// Fallback for aggregations? Currently skipping, user should define them.
	return config.QueryDefinition{}, false
}

func selectRandomQueryByType(ctx context.Context, db *mongo.Database, opType string, queryMap map[string][]config.QueryDefinition, col config.CollectionDefinition, debug bool, rng *rand.Rand, filterField string, cfg *config.AppConfig) (config.QueryDefinition, bool) {
	candidates, ok := queryMap[opType]
	if !ok || len(candidates) == 0 {
		if opType == "find" || opType == "updateOne" || opType == "updateMany" || opType == "deleteOne" || opType == "deleteMany" {
			if debug {
				log.Printf("Warning: no configured queries for %s, generating fallback", opType)
			}
			return generateFallbackQuery(ctx, db, opType, col, rng, filterField, cfg)
		}
		if debug {
			// Aggregations shouldn't fail silently if explicitly requested
			if opType == "aggregate" {
				log.Printf("Warning: aggregation requested but no queries defined")
			}
		}
		return config.QueryDefinition{}, false
	}
	return candidates[rng.Intn(len(candidates))], true
}

func generateInsertQuery(col config.CollectionDefinition, rng *rand.Rand, cfg *config.AppConfig) config.QueryDefinition {
	var doc map[string]interface{}
	select {
	case doc = <-InsertDocumentCache:
	default:
		doc = workloads.GenerateDocument(col, cfg)
	}
	return config.QueryDefinition{
		Collection: col.Name,
		Operation:  "insert",
		Filter:     doc,
	}
}

func insertDocumentProducer(ctx context.Context, col config.CollectionDefinition, cacheSize int, cfg *config.AppConfig) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			doc := workloads.GenerateDocument(col, cfg)
			select {
			case InsertDocumentCache <- doc:
			case <-ctx.Done():
				return
			}
		}
	}
}

func queryWorkerOnce(ctx context.Context, id int, tasks <-chan *queryTask, wg *sync.WaitGroup) {
	defer wg.Done()
	dbOpCtx := context.Background()

	for task := range tasks {
		q := task.definition
		coll := task.database.Collection(q.Collection)

		// Support Cloning and Randomizing both Filters (Find) and Pipelines (Agg)
		var filter map[string]interface{}
		var pipeline []interface{}

		if q.Operation == "aggregate" {
			// Deep clone the pipeline slice
			if cloned, ok := deepClone(q.Pipeline).([]interface{}); ok {
				pipeline = cloned
				processRecursive(pipeline, task.rng)
			}
		} else {
			// Deep clone the filter map
			filter = cloneMap(q.Filter)
			processRecursive(filter, task.rng)
		}

		switch q.Operation {
		case "find":
			cursor, err := coll.Find(dbOpCtx, filter)
			if err != nil {
				log.Printf("[Worker %d] Find error: %v", id, err)
				continue
			}
			for cursor.Next(dbOpCtx) {
				var m map[string]interface{}
				_ = cursor.Decode(&m)
			}
			cursor.Close(dbOpCtx)
		case "aggregate":
			// Execute Aggregation
			cursor, err := coll.Aggregate(dbOpCtx, pipeline)
			if err != nil {
				log.Printf("[Worker %d] Aggregate error: %v", id, err)
				continue
			}
			// Iterate to ensure DB actually does the work
			for cursor.Next(dbOpCtx) {
				var m map[string]interface{}
				_ = cursor.Decode(&m)
			}
			cursor.Close(dbOpCtx)

		case "updateOne":
			if _, err := coll.UpdateOne(dbOpCtx, filter, q.Update); err != nil {
				log.Printf("[Worker %d] UpdateOne error: %v", id, err)
			}
		case "updateMany":
			if _, err := coll.UpdateMany(dbOpCtx, filter, q.Update); err != nil {
				log.Printf("[Worker %d] UpdateMany error: %v", id, err)
			}
		case "deleteOne":
			if _, err := coll.DeleteOne(dbOpCtx, filter); err != nil {
				log.Printf("[Worker %d] DeleteOne error: %v", id, err)
			}
		case "deleteMany":
			if _, err := coll.DeleteMany(dbOpCtx, filter); err != nil {
				log.Printf("[Worker %d] DeleteMany error: %v", id, err)
			}
		default:
			log.Printf("[Worker %d] Unknown operation %s", id, q.Operation)
		}
	}
}

func RunWorkload(ctx context.Context, db *mongo.Database, collections []config.CollectionDefinition, queries []config.QueryDefinition, cfg *config.AppConfig) error {
	duration, err := time.ParseDuration(cfg.Duration)
	if err != nil {
		return err
	}

	collector := stats.NewCollector()

	if duration <= 0 {
		return runAllQueriesOnce(ctx, db, queries, cfg.DebugMode)
	}

	findBatch := int32(cfg.FindBatchSize)
	if findBatch <= 0 {
		findBatch = 10
	}
	findLimit := int64(cfg.FindLimit)
	if findLimit <= 0 {
		findLimit = 10
	}

	qMap := make(map[string][]config.QueryDefinition)
	for _, q := range queries {
		qMap[q.Operation] = append(qMap[q.Operation], q)
	}

	cachedFilterField := getPrimaryFilterField(ctx, db, collections[0])

	wCfg := workloadConfig{
		database:    db,
		appConfig:   cfg,
		concurrency: cfg.Concurrency,
		duration:    duration,
		collections: collections,
		queryMap:    qMap,
		percentages: map[string]int{
			"find":      cfg.FindPercent,
			"update":    cfg.UpdatePercent,
			"delete":    cfg.DeletePercent,
			"insert":    cfg.InsertPercent,
			"aggregate": cfg.AggregatePercent, // Added Aggregation
		},
		debug:              cfg.DebugMode,
		findBatchSize:      findBatch,
		findLimit:          findLimit,
		maxInsertCache:     cfg.InsertCacheSize,
		primaryFilterField: cachedFilterField,
		collector:          collector,
	}

	return runContinuousWorkload(ctx, wCfg)
}

func runAllQueriesOnce(ctx context.Context, db *mongo.Database, queries []config.QueryDefinition, debug bool) error {
	if len(queries) == 0 {
		return nil
	}
	tasks := make(chan *queryTask, len(queries))
	var wg sync.WaitGroup
	wg.Add(1)
	go queryWorkerOnce(ctx, 1, tasks, &wg)

	for i, q := range queries {
		if q.Operation == "insert" {
			if debug {
				log.Printf("Skipping insert in fixed run")
			}
			continue
		}
		tasks <- &queryTask{
			definition: q,
			database:   db,
			runID:      int64(i + 1),
			debug:      debug,
			rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		}
	}
	close(tasks)
	wg.Wait()
	return nil
}

func runContinuousWorkload(ctx context.Context, wCfg workloadConfig) error {
	InsertDocumentCache = make(chan map[string]interface{}, wCfg.maxInsertCache)

	workloadCtx, cancel := context.WithTimeout(ctx, wCfg.duration)
	defer cancel()

	mainCol := wCfg.collections[0]
	producerCtx, producerCancel := context.WithCancel(workloadCtx)
	defer producerCancel()

	go insertDocumentProducer(producerCtx, mainCol, wCfg.maxInsertCache, wCfg.appConfig)

	monitorDone := make(chan struct{})
	go func() {
		wCfg.collector.Monitor(monitorDone, wCfg.appConfig.StatusRefreshRateSec, wCfg.concurrency)
	}()

	var wg sync.WaitGroup
	for i := 1; i <= wCfg.concurrency; i++ {
		wg.Add(1)
		rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
		go independentWorker(workloadCtx, i, &wg, wCfg, rng)
	}

	<-workloadCtx.Done()
	wg.Wait()
	close(monitorDone)

	wCfg.collector.PrintFinalSummary(wCfg.duration)

	return nil
}

func independentWorker(ctx context.Context, id int, wg *sync.WaitGroup, wCfg workloadConfig, rng *rand.Rand) {
	defer wg.Done()
	dbOpCtx := context.Background()
	mainCol := wCfg.collections[0]

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		opType := selectOperation(wCfg.percentages, rng)
		var q config.QueryDefinition
		var run bool

		switch opType {
		case "insert":
			q = generateInsertQuery(mainCol, rng, wCfg.appConfig)
			run = true
		case "find", "updateOne", "updateMany", "deleteOne", "deleteMany", "aggregate":
			q, run = selectRandomQueryByType(dbOpCtx, wCfg.database, opType, wCfg.queryMap, mainCol, wCfg.debug, rng, wCfg.primaryFilterField, wCfg.appConfig)
		default:
			time.Sleep(100 * time.Microsecond)
			continue
		}

		if !run {
			continue
		}

		db := wCfg.database
		coll := db.Collection(q.Collection)

		// Deep clone logic moved to helpers/below to handle recursion for Pipelines
		var filter map[string]interface{}
		var pipeline []interface{}

		if opType == "aggregate" {
			if cloned, ok := deepClone(q.Pipeline).([]interface{}); ok {
				pipeline = cloned
				processRecursive(pipeline, rng)
			}
		} else {
			filter = cloneMap(q.Filter)
			processRecursive(filter, rng)
		}

		start := time.Now()

		switch q.Operation {
		case "find":
			limit := int64(q.Limit)
			if limit <= 0 {
				limit = wCfg.findLimit
			}
			batch := wCfg.findBatchSize
			if batch <= 0 {
				batch = 10
			}
			cursor, err := coll.Find(dbOpCtx, filter,
				options.Find().SetLimit(limit),
				options.Find().SetBatchSize(batch),
				options.Find().SetProjection(q.Projection),
			)
			if err == nil {
				for cursor.Next(dbOpCtx) {
				}
				_ = cursor.Close(dbOpCtx)
			} else {
				log.Printf("[Worker %d] Find error: %v", id, err)
			}
		case "aggregate":
			cursor, err := coll.Aggregate(dbOpCtx, pipeline)
			if err == nil {
				for cursor.Next(dbOpCtx) {
				}
				_ = cursor.Close(dbOpCtx)
			} else {
				log.Printf("[Worker %d] Aggregate error: %v", id, err)
			}
		case "updateOne":
			if _, err := coll.UpdateOne(dbOpCtx, filter, q.Update); err != nil {
				log.Printf("[Worker %d] UpdateOne error: %v", id, err)
			}
		case "updateMany":
			if _, err := coll.UpdateMany(dbOpCtx, filter, q.Update); err != nil {
				log.Printf("[Worker %d] UpdateMany error: %v", id, err)
			}
		case "deleteOne":
			if _, err := coll.DeleteOne(dbOpCtx, filter); err != nil {
				log.Printf("[Worker %d] DeleteOne error: %v", id, err)
			}
		case "deleteMany":
			if _, err := coll.DeleteMany(dbOpCtx, filter); err != nil {
				log.Printf("[Worker %d] DeleteMany error: %v", id, err)
			}
		case "insert":
			if _, err := coll.InsertOne(dbOpCtx, q.Filter); err != nil {
				log.Printf("[Worker %d] InsertOne error: %v", id, err)
			}
		}

		elapsed := time.Since(start)
		wCfg.collector.Track(q.Operation, elapsed)
	}
}

// deepCloneRecursively copies maps, slices, and primitives to ensure thread safety
func deepClone(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		m := make(map[string]interface{}, len(t))
		for k, val := range t {
			m[k] = deepClone(val)
		}
		return m
	case []interface{}:
		s := make([]interface{}, len(t))
		for i, val := range t {
			s[i] = deepClone(val)
		}
		return s
	default:
		return t
	}
}

func cloneMap(m map[string]interface{}) map[string]interface{} {
	if res, ok := deepClone(m).(map[string]interface{}); ok {
		return res
	}
	return nil
}

// processRecursive traverses both Maps and Slices to find and replace random placeholders
func processRecursive(v interface{}, rng *rand.Rand) {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, val := range t {
			if s, ok := val.(string); ok {
				if s == "<int>" {
					t[k] = rng.Intn(1000)
				} else if s == "<string>" {
					t[k] = fmt.Sprintf("val-%d", rng.Intn(1000))
				}
			} else {
				processRecursive(val, rng)
			}
		}
	case []interface{}:
		for _, val := range t {
			processRecursive(val, rng)
		}
	}
}
