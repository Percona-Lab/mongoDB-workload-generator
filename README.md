# plgm: Percona Load Generator for MongoDB Clusters

**plgm** is a high-performance tool written in Go, designed to effortlessly generate data and simulate heavy workloads for both sharded and non-sharded MongoDB clusters.

It simulates real-world usage patterns by generating random data using robust BSON data types and executing standard CRUD operations (Find, Insert, Update, Delete) based on configurable ratios.

This tool is a complete refactor of the previous Python version, offering:
* **Single Binary:** No complex dependencies or Python environment setup.
* **High Concurrency:** Utilizes Go goroutines ("Active Workers") to generate massive load with minimal client-side resource usage.
* **Configuration as Code:** Fully configurable via a simple `config.yaml` file or Environment Variables.
* **Extensive Data Support:** Supports all standard MongoDB BSON data types (ObjectId, Decimal128, Date, Binary, etc.) and realistic data generation via `gofakeit` (supporting complex nested objects and arrays).
* **True Parallelism:** Unlike the previous Python version, this tool automatically detects and utilizes all available logical CPUs (`GOMAXPROCS`) by default to maximize hardware efficiency.

---

## Quick Start

### 1. Installation

**Option A: Download Release (Recommended)**

Navigate to the [Releases] page and download the .tar.gz file matching your operating system.

1. Download and Extract:

```bash
# Example for Linux
tar -xzvf plgm-linux-amd64.tar.gz

# Example for Mac (Apple Silicon)
tar -xzvf plgm-darwin-arm64.tar.gz
```

2. Run:

```bash
# The extracted binary will have the OS suffix
./plgm-linux-amd64 --version
```

**Option B: Build from Source** (Requires Go 1.25+)

This project includes a `Makefile` to simplify building and packaging.

```bash
git clone https://github.com/Percona-Lab/percona-load-generator-mongodb.git
cd plgm
go mod tidy

# Build a binary for your CURRENT machine only (no .tar.gz)
make build-local

# Run it
./bin/plgm --help
```

**Cross-Compilation (Build for different OS)**

If you are preparing binaries for other users (or other servers), use the main build command. This will compile binaries for Linux and Mac and automatically package them into .tar.gz files in the bin/ folder.

```bash
# Generate all release packages
make build

# Output:
# bin/plgm-linux-amd64.tar.gz
# bin/plgm-darwin-amd64.tar.gz
# bin/plgm-darwin-arm64.tar.gz
```

### Usage

To view the full usage guide, including available flags and environment variables, run the help command:

```bash
bin/plgm --help
plgm: Percona Load Generator for MongoDB Clusters
Usage: ./bin/plgm [flags] [config_file]

Examples:
  ./bin/plgm                    # Run with default 'config.yaml'
  ./bin/plgm my_test.yaml       # Run with specific config file
  ./bin/plgm --help             # Show this help message

Flags:
  -config string
    	Path to the configuration file (default "config.yaml")
  -version
    	Print version information and exit

Environment Variables (Overrides):
 [Connection]
  PERCONALOAD_URI                     Connection URI
  PERCONALOAD_USERNAME                Database User
  PERCONALOAD_PASSWORD                Database Password (Recommended: Use Prompt)
  PERCONALOAD_DIRECT_CONNECTION       Force direct connection (true/false)
  PERCONALOAD_REPLICA_SET             Replica Set name
  PERCONALOAD_READ_PREFERENCE         nearest

 [Workload Core]
  PERCONALOAD_DEFAULT_WORKLOAD        Use built-in workload (true/false)
  PERCONALOAD_COLLECTIONS_PATH        Path to collection JSON
  PERCONALOAD_QUERIES_PATH            Path to query JSON
  PERCONALOAD_DURATION                Test duration (e.g. 60s, 5m)
  PERCONALOAD_CONCURRENCY             Number of active workers
  PERCONALOAD_DOCUMENTS_COUNT         Initial seed document count
  PERCONALOAD_DROP_COLLECTIONS        Drop collections on start (true/false)
  PERCONALOAD_SKIP_SEED               Do not seed initial data on start (true/false)
  PERCONALOAD_DEBUG_MODE              Enable verbose logic logs (true/false)
  PERCONALOAD_USE_TRANSACTIONS        Enable transactional workloads (true/false)
  PERCONALOAD_MAX_TRANSACTION_OPS     Maximum number of operations to group into a single transaction block

 [Operation Ratios] (Must sum to ~100)
  PERCONALOAD_FIND_PERCENT            % of ops that are FIND
  PERCONALOAD_UPDATE_PERCENT          % of ops that are UPDATE
  PERCONALOAD_INSERT_PERCENT          % of ops that are INSERT
  PERCONALOAD_DELETE_PERCENT          % of ops that are DELETE
  PERCONALOAD_AGGREGATE_PERCENT       % of ops that are AGGREGATE
  PERCONALOAD_TRANSACTION_PERCENT     % of ops that are TRANSACTIONAL
  PERCONALOAD_BULK_INSERT_PERCENT     % of ops that are BULK INSERTS

 [Performance Optimization]
  PERCONALOAD_FIND_BATCH_SIZE         Docs returned per cursor batch
  PERCONALOAD_INSERT_BATCH_SIZE       Number of docs in batch bulk insert
  PERCONALOAD_FIND_LIMIT              Max docs per Find query
  PERCONALOAD_INSERT_CACHE_SIZE       Generator buffer size
  PERCONALOAD_OP_TIMEOUT_MS           Soft timeout per DB op (ms)
  PERCONALOAD_RETRY_ATTEMPTS          Retry attempts for failures
  PERCONALOAD_RETRY_BACKOFF_MS        Wait time between retries (ms)
  PERCONALOAD_STATUS_REFRESH_RATE_SEC Status report interval (sec)
  GOMAXPROCS                          Go Runtime CPU limit
```

### 2. Run Default Workload
plgm comes with a built-in default workload useful for immediate testing and get you started right away.
```bash
# Edit config.yaml to set your URI, then run:
./bin/plgm
```

**Note about default workload:** plgm comes pre-configured with a [default collection](./resources/collections/default.json) and [default queries](./resources/queries/default.json). If you do not provide any parameters and leave the configuration setting `default_workload: true`, this default workload will be used.

If you wish to use a different default workload, you can replace these two files with your own default.json files in the same paths. This allows you to define a different collection and set of queries as the default workload.

**Note on config file usage:** If you do not specify the config file name (above example), plgm will use the [config.yaml](./config.yaml) by default. You can create separate configuration files if you wish and then pass it as an argument:

```bash
./bin/plgm /path/to/some/custom_config.yaml
```

### 3. Additional Workloads

You will find additional workloads that you can use as references to benchmark your environment in cases where you prefer not to provide your own collection definitions and queries. However, if your goal is to test your application accurately, we strongly recommend creating collection definitions and queries that match those used by your application.

The additional collection and query definitions can be found here:

* [collections](./resources/collections/)
* [queries](./resources/queries/)

### 4. Docker & Kubernetes
Prefer running in a container? We have a dedicated guide for building Docker images and running performance jobs directly inside Kubernetes (recommended for accurate network latency testing).

[View the Docker & Kubernetes Guide](docker.md)

---

## Configuration

plgm is configured primarily through its [config.yaml](./config.yaml) file. This makes it easier to save and version-control your test scenarios.

### Environment Variable Overrides
You can override any setting in `config.yaml` using environment variables. This is useful for CI/CD pipelines, Kubernetes deployments, or quick runtime adjustments without editing the file. These are all the available ENV vars you can configure and each corresponding setting in the [config.yaml](./config.yaml) file:

| Config Setting | Environment Variable | Description | Example |
| :--- | :--- | :--- | :--- |
| **Connection** | | | |
| `uri` | `PERCONALOAD_URI` | Target MongoDB connection URI | `mongodb://user:pass@host:27017` |
| `direct_connection` | `PERCONALOAD_DIRECT_CONNECTION` | Force direct connection (bypass topology discovery) | `true` |
| `replicaset_name` | `PERCONALOAD_REPLICA_SET` | Replica Set name (required for sharded clusters/RS) | `rs0` |
| `read_preference` | `PERCONALOAD_READ_PREFERENCE` | By default, an application directs its read operations to the primary member in a replica set. You can specify a read preference to send read operations to secondaries. | `nearest` |
| `username` | `PERCONALOAD_USERNAME` |	Database User | `admin` |
| ***can not be set via config*** | `PERCONALOAD_PASSWORD` |	Database Password (if not set, plgm will prompt) | `password123` |
| **Workload Control** | | | |
| `concurrency` | `PERCONALOAD_CONCURRENCY` | Number of active worker goroutines | `50` |
| `duration` | `PERCONALOAD_DURATION` | Test duration (Go duration string) | `5m`, `60s` |
| `default_workload` | `PERCONALOAD_DEFAULT_WORKLOAD` | Use built-in "Flights" workload (`true`/`false`) | `false` |
| `collections_path` | `PERCONALOAD_COLLECTIONS_PATH` | Path to custom collection JSON files (supports directories for multi-collection load) | `./schemas` |
| `queries_path` | `PERCONALOAD_QUERIES_PATH` | Path to custom query JSON files or directory. | `./queries` |
| `documents_count` | `PERCONALOAD_DOCUMENTS_COUNT` | Number of documents to seed initially | `10000` |
| `drop_collections` | `PERCONALOAD_DROP_COLLECTIONS` | Drop collections before starting (`true`/`false`) | `true` |
| `skip_seed` | `PERCONALOAD_SKIP_SEED` | Do not seed initial data on start (`true`/`false`) | `true` |
| `debug_mode` | `PERCONALOAD_DEBUG_MODE` | Enable verbose debug logging (`true`/`false`) | `false` |
| `use_transactions` | `PERCONALOAD_USE_TRANSACTIONS` | Enable Transactional Workloads (`true`/`false`) | `false` |
| `max_transaction_ops` | `PERCONALOAD_MAX_TRANSACTION_OPS` | Maximum number of operations to group into a single transaction block | `5` |
| **Operation Ratios** | | (Must sum to ~100) | |
| `find_percent` | `PERCONALOAD_FIND_PERCENT` | Percentage of Find operations | `50` |
| `insert_percent` | `PERCONALOAD_INSERT_PERCENT` | Percentage of Insert operations (this is not related to the initial seed inserts) | `10` |
| `bulk_insert_percent ` | `PERCONALOAD_BULK_INSERT_PERCENT` | Percentage of Bulk Insert operations (this is not related to the initial seed inserts) | `10` |
| `update_percent` | `PERCONALOAD_UPDATE_PERCENT` | Percentage of Update operations | `10` |
| `delete_percent` | `PERCONALOAD_DELETE_PERCENT` | Percentage of Delete operations | `10` |
| `aggregate_percent` | `PERCONALOAD_AGGREGATE_PERCENT` | Percentage of Aggregate operations | `5` |
| `transaction_percent` | `PERCONALOAD_TRANSACTION_PERCENT` | Percentage of Transactional operations | `5` |
| **Performance Optimization** | | | |
| `find_batch_size` | `PERCONALOAD_FIND_BATCH_SIZE` | Documents returned per cursor batch | `100` |
| `insert_batch_size` | `PERCONALOAD_INSERT_BATCH_SIZE` | Number of documents per insert batch | `100` |
| `find_limit` | `PERCONALOAD_FIND_LIMIT` | Hard limit on documents per Find query | `10` |
| `insert_cache_size` | `PERCONALOAD_INSERT_CACHE_SIZE` | Size of the document generation buffer | `1000` |
| `op_timeout_ms` | `PERCONALOAD_OP_TIMEOUT_MS` | Soft timeout for individual DB operations (ms) | `500` |
| `retry_attempts` | `PERCONALOAD_RETRY_ATTEMPTS` | Number of retries for transient errors | `3` |
| `retry_backoff_ms` | `PERCONALOAD_RETRY_BACKOFF_MS` | Wait time between retries (ms) | `10` |
| `status_refresh_rate_sec` | `PERCONALOAD_STATUS_REFRESH_RATE_SEC` | How often to print stats to console (sec) | `5` |


**Example:**
```bash
PERCONALOAD_CONCURRENCY=50 PERCONALOAD_DURATION=5m ./bin/plgm
```

---

## Functionality

When executed, plgm performs the following steps:

1.  **Initialization:** Connects to the database and loads collection/query definitions.
2.  **Setup:**
    * Creates databases and collections defined in your JSON files.
    * Creates indexes.
    * (Optional) Seeds initial data with the number of documents defined by `documents_count` in the config.
3.  **Workload Execution:**
    * Spawns the configured number of **Active Workers**.
    * Continuously generates and executes queries (Find, Insert, Update, Delete, Aggregate, Upsert) based on your configured ratios.
    * Generates realistic BSON data for Inserts and Updates (supports recursion and complex schemas).
    * Workers pick a random collection from the provided list for every operation.
4.  **Reporting:**
    * Outputs a real-time status report every N seconds (configurable).
    * Prints a detailed summary table at the end of the run.

### Sample Output

![plgm](./plgm.gif)

### Interpreting the Output

To show how the Ops/Sec metrics are represented and what they signify, here is a sample of the real-time monitor output and a final summary. This data is modeled after the flights workload used when default_workload is true.

#### Real-Time Monitor Sample
While running a workload, plgm prints a row every second (based on `status_refresh_rate_sec`).

```bash
> Starting Workload...

 TIME    | TOTAL OPS  | SELECT   | INSERT   | UPDATE   | DELETE   | AGG    | TRANS
 -------------------------------------------------------------------------------
 00:01   |      8,300 |    5,004 |      798 |    1,650 |      848 |      0 |      0
 00:02   |      8,048 |    4,736 |      773 |    1,694 |      845 |      0 |      0
 00:03   |      8,168 |    4,728 |      824 |    1,737 |      879 |      0 |      0
```
What this represents:

* TIME: The elapsed time since the workload started (MM:SS).
* TOTAL OPS: The combined number of all operations executed across all workers in that specific 1-second interval.
* SELECT/INSERT/UPDATE/DELETE/AGG: The raw count of each specific operation type completed in that second.
* TRANS: The number of successful transaction blocks completed in that second (reusing the CRUD operations above internally).

#### Final Summary and Latency Sample
At the end of the run, plgm calculates the overall averages and the latency distribution.

```bash
> Workload Finished.

  SUMMARY
  --------------------------------------------------
  Runtime:    10.00s
  Total Ops:  81,746
  Avg Rate:   8,174 ops/sec

  LATENCY DISTRIBUTION (ms)
  --------------------------------------------------
  TYPE             AVG          MIN          MAX          P95          P99
  ----             ---          ---          ---          ---          ---
  SELECT       1.24 ms      0.45 ms     15.20 ms      4.00 ms      9.00 ms
  INSERT      12.11 ms      4.10 ms     85.00 ms     66.00 ms     73.00 ms
  UPDATE       9.71 ms      3.20 ms     78.40 ms     65.00 ms     71.00 ms
  DELETE       9.60 ms      3.05 ms     76.20 ms     65.00 ms     72.00 ms
  TRANS       25.40 ms     12.00 ms    145.00 ms     95.00 ms    112.00 ms
```

What this represents:

* Avg Rate (Ops/Sec): The total throughput of the database cluster. It is calculated by dividing Total Ops by the total Runtime.
* AVG Latency: The average time (in milliseconds) it took the MongoDB driver to receive a response for that operation.
* P95/P99 (Percentiles): These are the most critical metrics for performance tuning. P99 represents the "worst-case" scenario for 99% of your users. For example, if P99 SELECT is 9.00ms, it means 99% of your flight searches completed in under 9ms, while 1% took longer.
* TRANS Latency: This will typically be higher than individual operations because a single transaction block contains 1 to X grouped operations, where X is defined in the config file via `max_transaction_ops` or the env var `PERCONALOAD_MAX_TRANSACTION_OPS`.

---

## Custom Workloads

To run your own workload against your own schema:

1.  **Define Collection Schema:**
    Create a JSON file (e.g., `my_collection.json`) defining your schema.

    ```json
    [
      {
        "database": "ecommerce",
        "collection": "orders",
        "fields": {
          "_id": { "type": "objectid" },
          "customer_name": { "type": "string", "provider": "first_name" },
          "total": { "type": "double" },
          "created_at": { "type": "date" }
        }
      }
    ]
    ```

2.  **Define Query Patterns:**
    Create a JSON file (e.g., `my_queries.json`) defining the operations to run.

    ```json
    [
      {
        "database": "ecommerce",
        "collection": "orders",
        "operation": "find",
        "filter": { "customer_name": "<string>" },
        "limit": 10
      },
      {
        "database": "ecommerce",
        "collection": "orders",
        "operation": "updateOne",
        "filter": { "order_uuid": "<string>" },
        "update": { "$set": { "status": "processed" } },
        "upsert": true
      }
    ]
    ```

3.  **Run:**
    ```bash
    export PERCONALOAD_COLLECTIONS_PATH=./my_collection.json
    export PERCONALOAD_QUERIES_PATH=./my_queries.json
    ./bin/plgm
    ```

### Supported Data Types
* **Primitives:** `int`, `long`, `double`, `decimal128`, `bool`, `string`.
* **Time:** `date`, `timestamp`.
* **Binary/Logic:** `binary`, `uuid`, `objectid`, `regex`, `javascript`.
* **Complex:** `object`, `array`.
* **Providers:** Supports ANY gofakeit provider via reflection. Example: `beer_name`, `car_maker`, `bitcoin_address`, `credit_card`, `city`, `ssn`, etc..

---

## Performance Optimization

plgm is designed to utilize maximum system resources by default, but it can be fine-tuned to fit specific hardware constraints or testing scenarios.

### 1. CPU Utilization (`GOMAXPROCS`)

By default, plgm automatically detects and schedules work across **all available logical CPUs**. You generally do not need to configure this.

However, if you are running in a constrained environment (e.g., a shared CI runner or a container with strict CPU limits) or if you want to throttle the generator's CPU usage, you can override this via the standard Go environment variable:

```bash
# Limit plgm to use only 2 CPU cores
export GOMAXPROCS=2
./plgm
```

### 2. Configuration Optimization (`config.yaml`)

You can fine-tune plgm internal behavior by adjusting the parameters in `config.yaml`.

#### Workload Type
By default, the tool comes preconfigured with the following workload distribution:

| Operation |	Percentage |
| :--- | :--- | 
| Find	| 50% | 
| Update	| 20% | 
| Delete	| 10% | 
| Insert	| 5% | 
| Bulk Inserts | 5% |
| Aggregate	| 5% | 
| Transaction	| 5% | 

You can modify any of the values above to run different types of workloads.

Please note:

* If `use_transactions: false`, the transaction_percent value is ignored.
* If there are no aggregation queries defined in queries.json, the aggregate_percent value is also ignored. 
* Aggregate operations will only generate activity if at least one query with "operation": "aggregate" is defined in your active JSON query files.
* The maximum number of operations within a transaction is defined in the config file via `max_transaction_ops` or the env var `PERCONALOAD_MAX_TRANSACTION_OPS`. The number of operations per transaction will be randomized, with the max number being set as explained above. 
* Multi-Collection Load: If multiple collections are defined in your collections_path, each worker will randomly select a collection for every operation. This includes operations within a transaction, allowing for cross-collection atomic updates.


#### Concurrency & Workers
* **`concurrency`**: Controls the number of "Active Workers" continuously executing operations against the database.
    * *Tip:* Increase this to generate higher load. If set too high on a weak client, you may see increased client-side latency.
    * *Default:* `4`

#### Connection Pooling
These settings control the MongoDB driver's connection pool. Proper sizing is critical to prevent the application from waiting for available connections.

* **`max_pool_size`**: The maximum number of connections allowed in the pool.
    * *Tip:* A good rule of thumb is to set this slightly higher than your `concurrency` setting so that every worker is guaranteed a connection without blocking.
    * *Default:* `1000`
* **`min_pool_size`**: The minimum number of connections to keep open.
    * *Tip:* Setting this higher helps avoid the "cold start" penalty of establishing new connections during the initial ramp-up.
    * *Default:* `20`
* **`max_idle_time`**: How long a connection can remain unused before being closed (in minutes).
    * *Tip:* Keep this high (e.g., `30`) to avoid "reconnect churn" during brief pauses in workload.

#### Operation Optimization
These settings affect the efficiency of individual database operations and memory usage.

* **`find_batch_size`**: The number of documents returned per batch in a cursor.
    * *Tip:* Higher values reduce network round-trips but increase memory usage per worker.
    * *Default:* `10`
* **`insert_batch_size`**: The number of documents to be inserted by bulk inserts.
    * *Default:* `10`   
* **`find_limit`**: The hard limit on documents returned for `find` operations.
    * *Default:* `5`
* **`insert_cache_size`**: The buffer size for the document generator channel.
    * *Tip:* This decouples document generation from database insertion. A larger buffer ensures workers rarely wait for data generation logic.
    * *Default:* `1000`
* **`upserts`**: Any updateOne or updateMany operation in your query JSON files can include "upsert": true. This will cause MongoDB to create the document if no match is found for the filter.  


#### Timeouts & Reliability
Control how plgm reacts to network lag or database pressure.

* **`op_timeout_ms`**: A hard timeout for individual database operations.
    * *Tip:* Lowering this allows plgm to fail fast and retry rather than hanging on stalled requests.
    * *Default:* `500` (0.5 seconds)
* **`retry_attempts`** & **`retry_backoff_ms`**: Logic for handling transient failures.
    * *Tip:* For stress testing, you might want to set `retry_attempts: 0` to see raw failure rates immediately.
    * *Default:* `2` attempts with `5ms` backoff.

### 3. Custom Connection Parameters (`custom_params`)

In the `config.yaml`, the `custom_params` section allows you to pass arbitrary options directly to the MongoDB driver's connection string. These are critical for tuning network throughput and security. Here are some examples you can use, all MongoDB connection parameters are supported.

```yaml
custom_params:
  compressors: "zlib,snappy"
  ssl: false
```

| Parameter | Example Value | Impact on Performance |
| :--- | :--- | :--- |
| **`compressors`** | `"snappy,zlib"` | **High Impact.** Enables network compression. <br>• **`snappy`**: Low CPU overhead, moderate compression. Good for high-throughput, low-latency. <br>• **`zlib`**: Higher CPU overhead, high compression. Good for limited bandwidth. <br>• **Empty**: No compression (saves CPU, uses max bandwidth). |
| **`ssl`** | `false` | **Low/Medium Impact.** Disabling SSL (`false`) saves the CPU overhead of TLS handshakes and encryption, useful for local testing or secured private networks. |
| **`readPreference`**| `"secondary"` | **Medium Impact.** (Optional) Can be added to offload read operations to replica set secondaries, keeping the primary free for writes. |

