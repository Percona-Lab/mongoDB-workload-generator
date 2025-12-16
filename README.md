# genMongoLoad: A Workload Generator for MongoDB Clusters

**genMongoLoad** is a high-performance tool written in Go, designed to effortlessly generate data and simulate heavy workloads for both sharded and non-sharded MongoDB clusters.

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
tar -xzvf genMongoLoad-linux-amd64.tar.gz

# Example for Mac (Apple Silicon)
tar -xzvf genMongoLoad-darwin-arm64.tar.gz
```

2. Run:

```bash
# The extracted binary will have the OS suffix
./genMongoLoad-linux-amd64 --version
```

**Option B: Build from Source** (Requires Go 1.25+)

This project includes a `Makefile` to simplify building and packaging.

```bash
git clone <repository-url>
cd mongoDB-workload-generator
go mod tidy

# Build a binary for your CURRENT machine only (no .tar.gz)
make build-local

# Run it
./bin/genMongoLoad --help
```

**Cross-Compilation (Build for different OS)**

If you are preparing binaries for other users (or other servers), use the main build command. This will compile binaries for Linux and Mac and automatically package them into .tar.gz files in the bin/ folder.

```bash
# Generate all release packages
make build

# Output:
# bin/genMongoLoad-linux-amd64.tar.gz
# bin/genMongoLoad-darwin-amd64.tar.gz
# bin/genMongoLoad-darwin-arm64.tar.gz
```

### Usage

To view the full usage guide, including available flags and environment variables, run the help command:

```bash
genMongoLoad: A Workload Generator for MongoDB Clusters
Usage: bin/genMongoLoad [flags] [config_file]

Examples:
  bin/genMongoLoad                    # Run with default 'config.yaml'
  bin/genMongoLoad my_test.yaml       # Run with specific config file
  bin/genMongoLoad --help             # Show this help message

Flags:
  -config string
    	Path to the configuration file (default "config.yaml")
  -version
    	Print version information and exit

Environment Variables (Overrides):
 [Connection]
  GENMONGOLOAD_URI                    Connection URI
  GENMONGOLOAD_USERNAME               Database User
  GENMONGOLOAD_PASSWORD               Database Password (Recommended: Use Prompt)
  GENMONGOLOAD_DIRECT_CONNECTION      Force direct connection (true/false)
  GENMONGOLOAD_REPLICA_SET            Replica Set name
  GENMONGOLOAD_READ_PREFERENCE        nearest

 [Workload Core]
  GENMONGOLOAD_DEFAULT_WORKLOAD       Use built-in workload (true/false)
  GENMONGOLOAD_COLLECTIONS_PATH       Path to collection JSON
  GENMONGOLOAD_QUERIES_PATH           Path to query JSON
  GENMONGOLOAD_DURATION               Test duration (e.g. 60s, 5m)
  GENMONGOLOAD_CONCURRENCY            Number of active workers
  GENMONGOLOAD_DOCUMENTS_COUNT        Initial seed document count
  GENMONGOLOAD_DROP_COLLECTIONS       Drop collections on start (true/false)
  GENMONGOLOAD_SKIP_SEED              Do not seed initial data on start (true/false)
  GENMONGOLOAD_DEBUG_MODE             Enable verbose logic logs (true/false)

 [Operation Ratios] (Must sum to ~100)
  GENMONGOLOAD_FIND_PERCENT           % of ops that are FIND
  GENMONGOLOAD_UPDATE_PERCENT         % of ops that are UPDATE
  GENMONGOLOAD_INSERT_PERCENT         % of ops that are INSERT
  GENMONGOLOAD_DELETE_PERCENT         % of ops that are DELETE
  GENMONGOLOAD_AGGREGATE_PERCENT      % of ops that are AGGREGATE

 [Performance Optimization]
  GENMONGOLOAD_FIND_BATCH_SIZE        Docs returned per cursor batch
  GENMONGOLOAD_FIND_LIMIT             Max docs per Find query
  GENMONGOLOAD_INSERT_CACHE_SIZE      Generator buffer size
  GENMONGOLOAD_OP_TIMEOUT_MS          Soft timeout per DB op (ms)
  GENMONGOLOAD_RETRY_ATTEMPTS         Retry attempts for failures
  GENMONGOLOAD_RETRY_BACKOFF_MS       Wait time between retries (ms)
  GENMONGOLOAD_STATUS_REFRESH_RATE_SEC Status report interval (sec)
  GOMAXPROCS                          Go Runtime CPU limit
```

### 2. Run Default Workload
The tool comes with a built-in default workload useful for immediate testing and get you started right away.
```bash
# Edit config.yaml to set your URI, then run:
./bin/genMongoLoad
```

**Note about default workload:** genMongoLoad comes pre-configured with a [default collection](./resources/collections/default.json) and [default queries](./resources/queries/default.json). If you do not provide any parameters and leave the configuration setting `default_workload: true`, this default workload will be used.

If you wish to use a different default workload, you can replace these two files with your own default.json files in the same paths. This allows you to define a different collection and set of queries as the default workload.

**Note on config file usage:** If you do not specify the config file name (above example), genMongoLoad will use the [config.yaml](./config.yaml) by default. You can create separate configuration files if you wish and then pass it as an argument:

```bash
./bin/genMongoLoad /path/to/some/custom_config.yaml
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

genMongoLoad is configured primarily through its [config.yaml](./config.yaml) file. This makes it easier to save and version-control your test scenarios.

### Environment Variable Overrides
You can override any setting in `config.yaml` using environment variables. This is useful for CI/CD pipelines, Kubernetes deployments, or quick runtime adjustments without editing the file. These are all the available ENV vars you can configure:

| Environment Variable | Description | Example |
| :--- | :--- | :--- |
| **Connection** | | |
| `GENMONGOLOAD_URI` | Target MongoDB connection URI | `mongodb://user:pass@host:27017` |
| `GENMONGOLOAD_DIRECT_CONNECTION` | Force direct connection (bypass topology discovery) | `true` |
| `GENMONGOLOAD_REPLICA_SET` | Replica Set name (required for sharded clusters/RS) | `rs0` |
| `GENMONGOLOAD_READ_PREFERENCE` | By default, an application directs its read operations to the primary member in a replica set. You can specify a read preference to send read operations to secondaries. | `nearest` |
| `GENMONGOLOAD_USERNAME` |	Database User | `admin` |
| `GENMONGOLOAD_PASSWORD` |	Database Password (if not set, genMongoLoad will prompt) | `password123` |
| **Workload Control** | | |
| `GENMONGOLOAD_CONCURRENCY` | Number of active worker goroutines | `50` |
| `GENMONGOLOAD_DURATION` | Test duration (Go duration string) | `5m`, `60s` |
| `GENMONGOLOAD_DEFAULT_WORKLOAD` | Use built-in "Flights" workload (`true`/`false`) | `false` |
| `GENMONGOLOAD_COLLECTIONS_PATH` | Path to custom collection JSON files | `./schemas` |
| `GENMONGOLOAD_QUERIES_PATH` | Path to custom query JSON files | `./queries` |
| `GENMONGOLOAD_DOCUMENTS_COUNT` | Number of documents to seed initially | `10000` |
| `GENMONGOLOAD_DROP_COLLECTIONS` | Drop collections before starting (`true`/`false`) | `true` |
| `GENMONGOLOAD_SKIP_SEED` | Do not seed initial data on start (`true`/`false`) | `true` |
| `GENMONGOLOAD_DEBUG_MODE` | Enable verbose debug logging (`true`/`false`) | `false` |
| **Operation Ratios** | (Must sum to ~100) | |
| `GENMONGOLOAD_FIND_PERCENT` | Percentage of Find operations | `55` |
| `GENMONGOLOAD_INSERT_PERCENT` | Percentage of Insert operations | `20` |
| `GENMONGOLOAD_UPDATE_PERCENT` | Percentage of Update operations | `10` |
| `GENMONGOLOAD_DELETE_PERCENT` | Percentage of Delete operations | `10` |
| `GENMONGOLOAD_AGGREGATE_PERCENT` | Percentage of Aggregate operations | `5` |
| **Performance Optimization** | | |
| `GENMONGOLOAD_FIND_BATCH_SIZE` | Documents returned per cursor batch | `100` |
| `GENMONGOLOAD_FIND_LIMIT` | Hard limit on documents per Find query | `10` |
| `GENMONGOLOAD_INSERT_CACHE_SIZE` | Size of the document generation buffer | `1000` |
| `GENMONGOLOAD_OP_TIMEOUT_MS` | Soft timeout for individual DB operations (ms) | `500` |
| `GENMONGOLOAD_RETRY_ATTEMPTS` | Number of retries for transient errors | `3` |
| `GENMONGOLOAD_RETRY_BACKOFF_MS` | Wait time between retries (ms) | `10` |
| `GENMONGOLOAD_STATUS_REFRESH_RATE_SEC` | How often to print stats to console (sec) | `5` |


**Example:**
```bash
GENMONGOLOAD_CONCURRENCY=50 GENMONGOLOAD_DURATION=5m ./bin/genMongoLoad
```

---

## Functionality

When executed, genMongoLoad performs the following steps:

1.  **Initialization:** Connects to the database and loads collection/query definitions.
2.  **Setup:**
    * Creates databases and collections defined in your JSON files.
    * Creates indexes.
    * (Optional) Seeds initial data with the number of documents defined by `documents_count` in the config.
3.  **Workload Execution:**
    * Spawns the configured number of **Active Workers**.
    * Continuously generates and executes queries (Find, Insert, Update, Delete, Aggregate) based on your configured ratios.
    * Generates realistic BSON data for Inserts and Updates (supports recursion and complex schemas).
4.  **Reporting:**
    * Outputs a real-time status report every N seconds (configurable).
    * Prints a detailed summary table at the end of the run.

### Sample Output

![genMongoLoad Demo](./genMongoLoadDemo.gif)

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
      }
    ]
    ```

3.  **Run:**
    ```bash
    export GENMONGOLOAD_COLLECTIONS_PATH=./my_collection.json
    export GENMONGOLOAD_QUERIES_PATH=./my_queries.json
    ./bin/genMongoLoad
    ```

### Supported Data Types
* **Primitives:** `int`, `long`, `double`, `decimal128`, `bool`, `string`.
* **Time:** `date`, `timestamp`.
* **Binary/Logic:** `binary`, `uuid`, `objectid`, `regex`, `javascript`.
* **Complex:** `object`, `array`.
* **Providers:** Supports ANY gofakeit provider via reflection. Example: `beer_name`, `car_maker`, `bitcoin_address`, `credit_card`, `city`, `ssn`, etc..

---

## Performance Optimization

genMongoLoad is designed to utilize maximum system resources by default, but it can be fine-tuned to fit specific hardware constraints or testing scenarios.

### 1. CPU Utilization (`GOMAXPROCS`)

By default, genMongoLoad automatically detects and schedules work across **all available logical CPUs**. You generally do not need to configure this.

However, if you are running in a constrained environment (e.g., a shared CI runner or a container with strict CPU limits) or if you want to throttle the generator's CPU usage, you can override this via the standard Go environment variable:

```bash
# Limit genMongoLoad to use only 2 CPU cores
export GOMAXPROCS=2
./genmongoload
```

### 2. Configuration Optimization (`config.yaml`)

You can fine-tune genMongoLoad internal behavior by adjusting the parameters in `config.yaml`.

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
* **`find_limit`**: The hard limit on documents returned for `find` operations.
    * *Default:* `5`
* **`insert_cache_size`**: The buffer size for the document generator channel.
    * *Tip:* This decouples document generation from database insertion. A larger buffer ensures workers rarely wait for data generation logic.
    * *Default:* `1000`

#### Timeouts & Reliability
Control how genMongoLoad reacts to network lag or database pressure.

* **`op_timeout_ms`**: A hard timeout for individual database operations.
    * *Tip:* Lowering this allows genMongoLoad to fail fast and retry rather than hanging on stalled requests.
    * *Default:* `500` (0.5 seconds)
* **`retry_attempts`** & **`retry_backoff_ms`**: Logic for handling transient failures.
    * *Tip:* For stress testing, you might want to set `retry_attempts: 0` to see raw failure rates immediately.
    * *Default:* `2` attempts with `5ms` backoff.

### 3. Custom Connection Parameters (`custom_params`)

In the `config.yaml`, the `custom_params` section allows you to pass arbitrary options directly to the MongoDB driver's connection string. These are critical for tuning network throughput and security.

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

