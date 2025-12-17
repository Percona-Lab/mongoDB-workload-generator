package mongo

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Percona-Lab/percona-load-generator-mongodb/internal/config"
	"github.com/Percona-Lab/percona-load-generator-mongodb/internal/stats"
	"github.com/Percona-Lab/percona-load-generator-mongodb/internal/workloads"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

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

var operationTypes = []string{"find", "update", "delete", "insert", "aggregate", "transaction"}

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
	return config.QueryDefinition{}, false
}

func selectRandomQueryByType(ctx context.Context, db *mongo.Database, opType string, queryMap map[string][]config.QueryDefinition, col config.CollectionDefinition, debug bool, rng *rand.Rand, filterField string, cfg *config.AppConfig) (config.QueryDefinition, bool) {
	candidates, ok := queryMap[opType]
	if !ok || len(candidates) == 0 {
		if opType == "find" || opType == "updateOne" || opType == "updateMany" || opType == "deleteOne" || opType == "deleteMany" {
			return generateFallbackQuery(ctx, db, opType, col, rng, filterField, cfg)
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

func runTransaction(ctx context.Context, id int, wCfg workloadConfig, rng *rand.Rand) {
	session, err := wCfg.database.Client().StartSession()
	if err != nil {
		log.Printf("[Worker %d] Failed to start session: %v", id, err)
		return
	}
	defer session.EndSession(ctx)

	mainCol := wCfg.collections[0]
	start := time.Now()

	_, err = session.WithTransaction(ctx, func(sessCtx context.Context) (interface{}, error) {
		// Run 2 to 5 random operations per transaction
		numOps := rng.Intn(4) + 2
		for i := 0; i < numOps; i++ {
			// Select standard CRUD
			innerOp := selectOperation(wCfg.percentages, rng)
			if innerOp == "aggregate" || innerOp == "transaction" {
				innerOp = "find"
			}

			var q config.QueryDefinition
			var run bool

			if innerOp == "insert" {
				q = generateInsertQuery(mainCol, rng, wCfg.appConfig)
				run = true
			} else {
				q, run = selectRandomQueryByType(sessCtx, wCfg.database, innerOp, wCfg.queryMap, mainCol, wCfg.debug, rng, wCfg.primaryFilterField, wCfg.appConfig)
			}

			if !run {
				continue
			}

			coll := wCfg.database.Collection(q.Collection)
			filter := cloneMap(q.Filter)
			processRecursive(filter, rng)

			switch q.Operation {
			case "find":
				cursor, err := coll.Find(sessCtx, filter, options.Find().SetLimit(1))
				if err == nil {
					for cursor.Next(sessCtx) {
					}
					_ = cursor.Close(sessCtx)
				}
			case "updateOne", "updateMany":
				_, err = coll.UpdateOne(sessCtx, filter, q.Update)
			case "deleteOne", "deleteMany":
				_, err = coll.DeleteOne(sessCtx, filter)
			case "insert":
				_, err = coll.InsertOne(sessCtx, q.Filter)
			}

			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})

	if err != nil {
		if wCfg.debug {
			log.Printf("[Worker %d] Transaction aborted: %v", id, err)
		}
		return
	}

	wCfg.collector.Track("transaction", time.Since(start))
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

		// Handle Transaction Block
		if opType == "transaction" {
			runTransaction(ctx, id, wCfg, rng)
			continue
		}

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
			}
		case "aggregate":
			cursor, err := coll.Aggregate(dbOpCtx, pipeline)
			if err == nil {
				for cursor.Next(dbOpCtx) {
				}
				_ = cursor.Close(dbOpCtx)
			}
		case "updateOne", "updateMany":
			coll.UpdateOne(dbOpCtx, filter, q.Update)
		case "deleteOne", "deleteMany":
			coll.DeleteOne(dbOpCtx, filter)
		case "insert":
			coll.InsertOne(dbOpCtx, q.Filter)
		}

		wCfg.collector.Track(q.Operation, time.Since(start))
	}
}

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
			"find":        cfg.FindPercent,
			"update":      cfg.UpdatePercent,
			"delete":      cfg.DeletePercent,
			"insert":      cfg.InsertPercent,
			"aggregate":   cfg.AggregatePercent,
			"transaction": cfg.TransactionPercent,
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

func runContinuousWorkload(ctx context.Context, wCfg workloadConfig) error {
	InsertDocumentCache = make(chan map[string]interface{}, wCfg.maxInsertCache)
	workloadCtx, cancel := context.WithTimeout(ctx, wCfg.duration)
	defer cancel()

	mainCol := wCfg.collections[0]
	go insertDocumentProducer(workloadCtx, mainCol, wCfg.maxInsertCache, wCfg.appConfig)

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

func queryWorkerOnce(ctx context.Context, id int, tasks <-chan *queryTask, wg *sync.WaitGroup) {
	defer wg.Done()
	dbOpCtx := context.Background()
	for task := range tasks {
		q := task.definition
		coll := task.database.Collection(q.Collection)
		filter := cloneMap(q.Filter)
		processRecursive(filter, task.rng)
		switch q.Operation {
		case "find":
			cursor, _ := coll.Find(dbOpCtx, filter)
			if cursor != nil {
				cursor.Close(dbOpCtx)
			}
		case "updateOne":
			coll.UpdateOne(dbOpCtx, filter, q.Update)
		}
	}
}
