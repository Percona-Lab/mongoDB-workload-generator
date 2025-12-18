# Percona Load Generator for MongoDB Clusters - Docker & Kubernetes Guide

This guide details how to containerize and run the `plgm` workload generator.

Running the benchmark as a container inside your Kubernetes cluster is the **recommended approach** for performance testing. It bypasses local network proxies (VPNs, Ingress Controllers) and places the load generator on the same high-speed network fabric as the database, ensuring you measure database performance, not network latency.

## 1. Build the Docker Image

We use a multi-stage Dockerfile to build a lightweight Alpine Linux image. This process uses the `Makefile` to automatically inject the version string.

### Create the `Dockerfile`

Create a file named `Dockerfile` in the root of this project. We have provided a full example here: [Dockerfile](./Dockerfile)

### Build & Tag

Build the image locally.

```bash
docker build -t plgm:latest .
```

> **Note for Kubernetes Users:** If your cluster is remote (EKS, GKE, AKS), you might have to tag and push this image to a registry your cluster can access:
> ```bash
> docker tag plgm:latest myregistry.azurecr.io/plgm:v1.0.0
> docker push myregistry.azurecr.io/plgm:v1.0.0
> ```

---

## 2. Run in Kubernetes (Job)

A Kubernetes Job is the ideal choice for benchmarking as it runs to completion and then terminates. However, you may choose the deployment strategy that best fits your specific requirements.

### Create `plgm-job.yaml`

We have provided a comprehensive sample manifest. It uses a Seed List for the URI (listing all three pods) to ensure high availability and utilizes the `PLGM_REPLICA_SET` variable among others to configure our options. This file is provided as an example; please edit [plgm-job.yaml](./plgm-job.yaml) to suit your specific requirements.

### Execute the Benchmark

**1. Launch the Job**
```bash
kubectl apply -f plgm-job.yaml
job.batch/plgm created
```

**2. Watch the Output**
Find the pod created by the job and stream the logs to see the real-time "Ops/Sec" report.
```bash
# Get the pod name (e.g., plgm-xxxxx)
kubectl get pods -l job-name=plgm -n lab
NAME                 READY   STATUS    RESTARTS   AGE
plgm-xfznq   1/1     Running   0          4s

# Stream logs
kubectl logs plgm-xfznq -n lab

  plgm 1
  --------------------------------------------------
  Database:     airline
  Workers:      40 active
  Duration:     10s
  Report Freq:  1s

  WORKLOAD DEFINITION
  --------------------------------------------------
  Batch Size:    1000
  Mode:          Default Workload
  Distribution:  Select (60%)  Update (20%)
                 Insert (10%)  Delete (10%)

  [INFO] Loaded 1 collection definition(s)
  [INFO] Loaded 2 query templates(s)
  [INFO] Skipping sharding for 'flights': Cluster is not sharded (Replica Set)
  [INFO] Created 4 indexes on 'flights'
  [INFO] Skipping data seeding (configured)

> Starting Workload...

 TIME    | TOTAL OPS | SELECT | INSERT | UPDATE | DELETE
 --------------------------------------------------------
 00:01   |     8,300 |  5,004 |    798 |  1,650 |    848
 00:02   |     8,048 |  4,736 |    773 |  1,694 |    845
 00:03   |     8,168 |  4,728 |    824 |  1,737 |    879
 00:04   |     8,182 |  4,893 |    817 |  1,695 |    777
 00:05   |     8,504 |  5,047 |    843 |  1,724 |    890
 00:06   |     8,776 |  5,271 |    851 |  1,757 |    897
 00:07   |     8,546 |  5,145 |    880 |  1,699 |    822
 00:08   |     8,365 |  4,945 |    828 |  1,753 |    839
 00:09   |     8,733 |  5,208 |    907 |  1,716 |    902
 00:10   |     6,084 |  3,718 |    551 |  1,236 |    579

> Workload Finished.

  SUMMARY
  --------------------------------------------------
  Runtime:    10.00s
  Total Ops:  81,746
  Avg Rate:   8,174 ops/sec

  LATENCY DISTRIBUTION (ms)
  --------------------------------------------------
  TYPE             AVG          P95          P99
  ----             ---          ---          ---
  SELECT       1.24 ms      4.00 ms      9.00 ms
  INSERT      12.11 ms     66.00 ms     73.00 ms
  UPDATE       9.71 ms     65.00 ms     71.00 ms
  DELETE       9.60 ms     65.00 ms     72.00 ms
```

**3. Clean Up & Retry**
Jobs are immutable. To run again with new settings, delete the old job first.
```bash
kubectl get jobs -l job-name=plgm -n lab
NAME           STATUS     COMPLETIONS   DURATION   AGE
plgm   Complete   1/1           13s        3m8s

kubectl delete job plgm -n lab
job.batch "plgm" deleted

kubectl apply -f plgm-job.yaml
```

---

## 3. Configuration Reference

You can override almost any setting in `config.yaml` using these Environment Variables in your Kubernetes manifest. More variables are accepted, please see our [readme](./README.md#environment-variable-overrides) for a full list:

| Variable | Description |
| :--- | :--- |
| `PLGM_URI` | Connection String (use Internal DNS) |
| `PLGM_CONCURRENCY` | Number of parallel worker threads |
| `PLGM_DURATION` | Test duration (e.g., `60s`, `5m`) |
| `PLGM_FIND_PERCENT` | % of operations that are Reads |
| `PLGM_INSERT_PERCENT` | % of operations that are Inserts |
| `PLGM_UPDATE_PERCENT` | % of operations that are Updates |
| `PLGM_DELETE_PERCENT` | % of operations that are Deletes |
| `PLGM_DOCUMENTS_COUNT` | Initial seed document count (if seeding) |
| `PLGM_DEFAULT_WORKLOAD`| Set to `true` (use built-in flights) or `false` (custom) |

## 4. Troubleshooting Performance

**Throughput is low (Bottleneck Analysis)**

1.  **Check the Database Pods:**
    Is the database actually stressed?
    ```bash
    kubectl top pods -n <mongo-namespace>
    ```
    * **High CPU?** The DB is the bottleneck (Good test!).
    * **Low CPU?** The bottleneck is elsewhere (Network or Client).

2.  **Check the Benchmark Pod:**
    Is the generator hitting its own limits?
    ```bash
    kubectl top pod plgm-xxxxx
    ```
    * **CPU Maxed?** The generator is CPU-bound. Increase `resources.limits.cpu` in the YAML or lower `GOMAXPROCS`.
    * **CPU Low?** It might be network latency waiting for the DB. Increase `PLGM_CONCURRENCY` to create more parallel requests.

3.  **Read Preference:**
    If your Primary node is at 100% but Secondaries are idle, ensure your URI includes `readPreference=nearest` or `secondaryPreferred`.