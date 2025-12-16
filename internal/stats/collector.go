package stats

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/Percona-Lab/mongoDB-workload-generator/internal/config"
	"github.com/Percona-Lab/mongoDB-workload-generator/internal/logger"
)

const MaxLatencyBin = 10000

type LatencyHistogram struct {
	mu       sync.Mutex
	Buckets  [MaxLatencyBin]int64
	Overflow int64
	Count    int64
	Sum      float64
	Min      float64
	Max      float64
}

func (h *LatencyHistogram) Record(ms float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Count++
	h.Sum += ms

	if ms < h.Min {
		h.Min = ms
	}
	if ms > h.Max {
		h.Max = ms
	}

	bucket := int(math.Round(ms))
	if bucket < 0 {
		bucket = 0
	}
	if bucket >= MaxLatencyBin {
		h.Overflow++
	} else {
		h.Buckets[bucket]++
	}
}

func (h *LatencyHistogram) GetPercentile(p float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.Count == 0 {
		return 0.0
	}
	targetCount := int64(math.Ceil((p / 100.0) * float64(h.Count)))
	var currentCount int64 = 0
	for i, count := range h.Buckets {
		currentCount += count
		if currentCount >= targetCount {
			return float64(i)
		}
	}
	return float64(MaxLatencyBin)
}

type Collector struct {
	FindOps       uint64
	FindTotalNs   uint64
	InsertOps     uint64
	InsertTotalNs uint64
	UpdateOps     uint64
	UpdateTotalNs uint64
	DeleteOps     uint64
	DeleteTotalNs uint64
	AggOps        uint64
	AggTotalNs    uint64

	FindHist   *LatencyHistogram
	InsertHist *LatencyHistogram
	UpdateHist *LatencyHistogram
	DeleteHist *LatencyHistogram
	AggHist    *LatencyHistogram

	startTime  time.Time
	prevFind   uint64
	prevInsert uint64
	prevUpdate uint64
	prevDelete uint64
	prevAgg    uint64
}

func NewCollector() *Collector {
	return &Collector{
		FindHist:   &LatencyHistogram{Min: math.MaxFloat64},
		InsertHist: &LatencyHistogram{Min: math.MaxFloat64},
		UpdateHist: &LatencyHistogram{Min: math.MaxFloat64},
		DeleteHist: &LatencyHistogram{Min: math.MaxFloat64},
		AggHist:    &LatencyHistogram{Min: math.MaxFloat64},
		startTime:  time.Now(),
	}
}

// CHANGED: Second argument is now []config.CollectionDefinition instead of dbName string
func PrintConfiguration(appCfg *config.AppConfig, collections []config.CollectionDefinition, version string) {
	fmt.Println()
	fmt.Printf("  %s\n", logger.CyanString("genMongoLoad %s", version))
	fmt.Println(logger.CyanString("  --------------------------------------------------"))

	safeURI := appCfg.URI
	u, err := url.Parse(appCfg.URI)
	if err == nil && u.User != nil {
		if p, hasPassword := u.User.Password(); hasPassword {
			safeURI = strings.Replace(appCfg.URI, p, "xxxxxx", 1)
		}
	}

	var setEnvVars []string
	knownVars := []string{
		"GENMONGOLOAD_URI", "GENMONGOLOAD_USERNAME", "GENMONGOLOAD_PASSWORD",
		"GENMONGOLOAD_CONCURRENCY", "GENMONGOLOAD_DURATION", "GENMONGOLOAD_DEFAULT_WORKLOAD",
		"GENMONGOLOAD_COLLECTIONS_PATH", "GENMONGOLOAD_QUERIES_PATH",
		"GENMONGOLOAD_DROP_COLLECTIONS", "GENMONGOLOAD_SKIP_SEED", "GENMONGOLOAD_DEBUG_MODE",
		"GENMONGOLOAD_DIRECT_CONNECTION", "GENMONGOLOAD_REPLICA_SET", "GENMONGOLOAD_READ_PREFERENCE",
		"GOMAXPROCS", "GENMONGOLOAD_AGGREGATE_PERCENT",
	}

	for _, v := range knownVars {
		if _, exists := os.LookupEnv(v); exists {
			if v == "GENMONGOLOAD_PASSWORD" {
				setEnvVars = append(setEnvVars, v+"=(set)")
			} else {
				setEnvVars = append(setEnvVars, v)
			}
		}
	}
	sort.Strings(setEnvVars)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  Target URI:\t%s\n", safeURI)
	if len(setEnvVars) > 0 {
		fmt.Fprintf(w, "  Env Overrides:\t%s\n", strings.Join(setEnvVars, ", "))
	}
	w.Flush()
	fmt.Println()

	// Logic to print Namespaces instead of just Database
	var namespaces []string
	for _, col := range collections {
		namespaces = append(namespaces, fmt.Sprintf("%s.%s", col.DatabaseName, col.Name))
	}

	w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  Namespaces:\t%s\n", strings.Join(namespaces, ", ")) // Changed from Database
	fmt.Fprintf(w, "  Workers:\t%d active\n", appCfg.Concurrency)
	fmt.Fprintf(w, "  Duration:\t%s\n", appCfg.Duration)
	fmt.Fprintf(w, "  Report Freq:\t%ds\n", appCfg.StatusRefreshRateSec)
	w.Flush()

	fmt.Println()
	fmt.Println(logger.BoldString("  WORKLOAD DEFINITION"))
	fmt.Println(logger.CyanString("  --------------------------------------------------"))
	w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  Batch Size:\t%d\n", appCfg.InsertCacheSize)

	mode := "Custom Workload"
	if appCfg.DefaultWorkload {
		mode = "Default Workload"
	}
	fmt.Fprintf(w, "  Mode:\t%s\n", mode)

	fmt.Fprintf(w, "  Distribution:\tSelect (%d%%)\tUpdate (%d%%)\n", appCfg.FindPercent, appCfg.UpdatePercent)
	fmt.Fprintf(w, "  \tInsert (%d%%)\tDelete (%d%%)\n", appCfg.InsertPercent, appCfg.DeletePercent)
	fmt.Fprintf(w, "  \tAgg    (%d%%)\n", appCfg.AggregatePercent)
	w.Flush()
	fmt.Println()
}

func (c *Collector) Track(opType string, duration time.Duration) {
	ns := uint64(duration.Nanoseconds())
	ms := float64(ns) / 1e6

	switch opType {
	case "find":
		atomic.AddUint64(&c.FindOps, 1)
		atomic.AddUint64(&c.FindTotalNs, ns)
		c.FindHist.Record(ms)
	case "insert":
		atomic.AddUint64(&c.InsertOps, 1)
		atomic.AddUint64(&c.InsertTotalNs, ns)
		c.InsertHist.Record(ms)
	case "updateOne", "updateMany":
		atomic.AddUint64(&c.UpdateOps, 1)
		atomic.AddUint64(&c.UpdateTotalNs, ns)
		c.UpdateHist.Record(ms)
	case "deleteOne", "deleteMany":
		atomic.AddUint64(&c.DeleteOps, 1)
		atomic.AddUint64(&c.DeleteTotalNs, ns)
		c.DeleteHist.Record(ms)
	case "aggregate":
		atomic.AddUint64(&c.AggOps, 1)
		atomic.AddUint64(&c.AggTotalNs, ns)
		c.AggHist.Record(ms)
	}
}

func (c *Collector) Monitor(done <-chan struct{}, refreshRateSec int, concurrency int) {
	ticker := time.NewTicker(time.Duration(refreshRateSec) * time.Second)
	defer ticker.Stop()

	fmt.Println()
	fmt.Println(logger.GreenString("> Starting Workload..."))
	fmt.Println()

	headerStr := fmt.Sprintf(" %-7s | %9s | %6s | %6s | %6s | %6s | %6s",
		"TIME", "TOTAL OPS", "SELECT", "INSERT", "UPDATE", "DELETE", "AGG")

	fmt.Println(logger.BoldString(headerStr))
	fmt.Println(logger.CyanString(" ---------------------------------------------------------------"))

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			c.printInterval(float64(refreshRateSec))
		}
	}
}

func (c *Collector) printInterval(seconds float64) {
	currFind := atomic.LoadUint64(&c.FindOps)
	currInsert := atomic.LoadUint64(&c.InsertOps)
	currUpdate := atomic.LoadUint64(&c.UpdateOps)
	currDelete := atomic.LoadUint64(&c.DeleteOps)
	currAgg := atomic.LoadUint64(&c.AggOps)

	dFind := float64(currFind - c.prevFind)
	dInsert := float64(currInsert - c.prevInsert)
	dUpdate := float64(currUpdate - c.prevUpdate)
	dDelete := float64(currDelete - c.prevDelete)
	dAgg := float64(currAgg - c.prevAgg)

	c.prevFind = currFind
	c.prevInsert = currInsert
	c.prevUpdate = currUpdate
	c.prevDelete = currDelete
	c.prevAgg = currAgg

	totalDelta := dFind + dInsert + dUpdate + dDelete + dAgg
	elapsed := time.Since(c.startTime).Truncate(time.Second)
	elapsedStr := fmt.Sprintf("%02d:%02d", int(elapsed.Minutes()), int(elapsed.Seconds())%60)

	totalOpsStr := formatInt(int64(totalDelta))
	totalOpsPadded := fmt.Sprintf("%9s", totalOpsStr)
	totalOpsBold := logger.BoldString(totalOpsPadded)

	fmt.Printf(" %-7s | %s | %6s | %6s | %6s | %6s | %6s\n",
		elapsedStr, totalOpsBold,
		formatInt(int64(dFind)), formatInt(int64(dInsert)),
		formatInt(int64(dUpdate)), formatInt(int64(dDelete)),
		formatInt(int64(dAgg)),
	)
}

func (c *Collector) PrintFinalSummary(duration time.Duration) {
	fOps := atomic.LoadUint64(&c.FindOps)
	iOps := atomic.LoadUint64(&c.InsertOps)
	uOps := atomic.LoadUint64(&c.UpdateOps)
	dOps := atomic.LoadUint64(&c.DeleteOps)
	aOps := atomic.LoadUint64(&c.AggOps)

	totalOps := fOps + iOps + uOps + dOps + aOps
	seconds := duration.Seconds()

	fmt.Println()
	fmt.Println(logger.GreenString("> Workload Finished."))
	fmt.Println()
	fmt.Println(logger.BoldString("  SUMMARY"))
	fmt.Println(logger.CyanString("  --------------------------------------------------"))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  Runtime:\t%.2fs\n", seconds)
	fmt.Fprintf(w, "  Total Ops:\t%s\n", formatInt(int64(totalOps)))

	avgRate := 0.0
	if seconds > 0 {
		avgRate = float64(totalOps) / seconds
	}
	fmt.Fprintf(w, "  Avg Rate:\t%s ops/sec\n", logger.BoldString(formatInt(int64(avgRate))))
	w.Flush()

	fmt.Println()
	fmt.Println(logger.BoldString("  LATENCY DISTRIBUTION (ms)"))
	fmt.Println(logger.CyanString("  --------------------------------------------------"))

	const tableLayout = "  %-7s   %10s   %10s   %10s   %10s   %10s"
	headerStr := fmt.Sprintf(tableLayout, "TYPE", "AVG", "MIN", "MAX", "P95", "P99")
	fmt.Println(logger.BoldString(headerStr))

	separatorStr := fmt.Sprintf(tableLayout, "----", "---", "---", "---", "---", "---")
	fmt.Println(logger.CyanString(separatorStr))

	printLatencyRow(tableLayout, "SELECT", c.FindHist)
	printLatencyRow(tableLayout, "INSERT", c.InsertHist)
	printLatencyRow(tableLayout, "UPDATE", c.UpdateHist)
	printLatencyRow(tableLayout, "DELETE", c.DeleteHist)
	printLatencyRow(tableLayout, "AGG", c.AggHist)
	fmt.Println()
}

func printLatencyRow(layout string, label string, h *LatencyHistogram) {
	if h.Count == 0 {
		fmt.Printf(layout+"\n", label, "-", "-", "-", "-", "-")
		return
	}
	avgMs := h.Sum / float64(h.Count)
	p95Ms := h.GetPercentile(95.0)
	p99Ms := h.GetPercentile(99.0)
	fmt.Printf(layout+"\n", label, formatLatency(avgMs), formatLatency(h.Min), formatLatency(h.Max), formatLatency(p95Ms), formatLatency(p99Ms))
}

func formatLatency(ms float64) string {
	if ms < 1000.0 {
		return fmt.Sprintf("%.2f ms", ms)
	}
	if ms < 60000.0 {
		return fmt.Sprintf("%.4f s", ms/1000.0)
	}
	return fmt.Sprintf("%.2f m", ms/60000.0)
}

func formatInt(n int64) string {
	in := strconv.FormatInt(n, 10)
	numOfDigits := len(in)
	if n < 0 {
		numOfDigits--
	}
	numOfCommas := (numOfDigits - 1) / 3
	out := make([]byte, len(in)+numOfCommas)
	if n < 0 {
		in, out[0] = in[1:], '-'
	}
	for i, j, k := len(in)-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		if k++; k == 3 {
			j, k = j-1, 0
			out[j] = ','
		}
	}
}
