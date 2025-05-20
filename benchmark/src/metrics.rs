use histogram::AtomicHistogram;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::info;

/// Constants for metrics
const DEFAULT_HISTOGRAM_PRECISION: u8 = 3;
const DEFAULT_HISTOGRAM_MAX_VALUE_BITS: u8 = 32; // 2^32 ns ~= 4.3 seconds

/// Metrics collector for the benchmark
pub struct Metrics {
    // Operation counters
    total_ops: AtomicU64,
    successful_ops: AtomicU64,

    // Latency histograms for each operation type (in nanoseconds)
    get_latency: LatencyHistogram,
    put_latency: LatencyHistogram,
    scan_latency: LatencyHistogram,
    swap_latency: LatencyHistogram,
    delete_latency: LatencyHistogram,
}

/// A snapshot of the metrics at a point in time
pub struct MetricsSnapshot {
    pub total_ops: u64,
    pub successful_ops: u64,
    pub throughput: f64,
    pub success_rate: f64,

    // Latency percentiles
    pub get_latency_ms: f64,
    pub get_latency_p95_ms: f64,
    pub get_latency_p99_ms: f64,

    pub put_latency_ms: f64,
    pub put_latency_p95_ms: f64,
    pub put_latency_p99_ms: f64,

    pub swap_latency_ms: f64,
    pub swap_latency_p95_ms: f64,
    pub swap_latency_p99_ms: f64,

    pub scan_latency_ms: f64,
    pub scan_latency_p95_ms: f64,
    pub scan_latency_p99_ms: f64,

    pub delete_latency_ms: f64,
    pub delete_latency_p95_ms: f64,
    pub delete_latency_p99_ms: f64,

    // Overall latency
    pub avg_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
}

/// A histogram for tracking latencies
struct LatencyHistogram {
    hist: AtomicHistogram,
}

impl LatencyHistogram {
    /// Create a new latency histogram
    fn new() -> Self {
        Self {
            // Create a histogram with precision and value range
            hist: AtomicHistogram::new(
                DEFAULT_HISTOGRAM_PRECISION,
                DEFAULT_HISTOGRAM_MAX_VALUE_BITS,
            )
            .expect("Failed to create histogram"),
        }
    }

    /// Record a latency value
    fn record(&self, latency: Duration) {
        let nanos = latency.as_nanos() as u64;
        if let Err(e) = self.hist.increment(nanos) {
            // Just log and continue if we can't record the latency
            tracing::warn!("Failed to record latency {}: {}", nanos, e);
        }
    }

    /// Get the percentile in milliseconds
    fn percentile_ms(&self, p: f64) -> f64 {
        if let Some(bucket) = self.hist.load().percentile(p).unwrap() {
            let mid = (bucket.start() + bucket.end()) / 2;
            Duration::from_nanos(mid).as_secs_f64() * 1000.0
        } else {
            0.0
        }
    }

    /// Get the count of recorded values
    fn count(&self) -> u64 {
        self.hist.load().into_iter().map(|b| b.count()).sum()
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            total_ops: AtomicU64::new(0),
            successful_ops: AtomicU64::new(0),
            get_latency: LatencyHistogram::new(),
            put_latency: LatencyHistogram::new(),
            swap_latency: LatencyHistogram::new(),
            scan_latency: LatencyHistogram::new(),
            delete_latency: LatencyHistogram::new(),
        }
    }

    /// Record a successful operation
    pub fn record_success(&self, op_name: &str, latency: Duration) {
        self.total_ops.fetch_add(1, Ordering::Relaxed);
        self.successful_ops.fetch_add(1, Ordering::Relaxed);

        // Record latency based on operation type
        match op_name {
            "GET" => self.get_latency.record(latency),
            "PUT" => self.put_latency.record(latency),
            "SWAP" => self.swap_latency.record(latency),
            "SCAN" => self.scan_latency.record(latency),
            "DELETE" => self.delete_latency.record(latency),
            _ => {}
        }
    }

    /// Create a snapshot of the current metrics
    pub fn snapshot(&self, elapsed: Duration) -> MetricsSnapshot {
        let total_ops = self.total_ops.load(Ordering::Relaxed);
        let successful_ops = self.successful_ops.load(Ordering::Relaxed);

        let throughput = if elapsed.as_secs_f64() > 0.0 {
            total_ops as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        let success_rate = if total_ops > 0 {
            successful_ops as f64 / total_ops as f64 * 100.0
        } else {
            0.0
        };

        // Get latency percentiles
        let get_latency_ms = self.get_latency.percentile_ms(50.0);
        let get_latency_p95_ms = self.get_latency.percentile_ms(95.0);
        let get_latency_p99_ms = self.get_latency.percentile_ms(99.0);

        let put_latency_ms = self.put_latency.percentile_ms(50.0);
        let put_latency_p95_ms = self.put_latency.percentile_ms(95.0);
        let put_latency_p99_ms = self.put_latency.percentile_ms(99.0);

        let swap_latency_ms = self.swap_latency.percentile_ms(50.0);
        let swap_latency_p95_ms = self.swap_latency.percentile_ms(95.0);
        let swap_latency_p99_ms = self.swap_latency.percentile_ms(99.0);

        let scan_latency_ms = self.scan_latency.percentile_ms(50.0);
        let scan_latency_p95_ms = self.scan_latency.percentile_ms(95.0);
        let scan_latency_p99_ms = self.scan_latency.percentile_ms(99.0);

        let delete_latency_ms = self.delete_latency.percentile_ms(50.0);
        let delete_latency_p95_ms = self.delete_latency.percentile_ms(95.0);
        let delete_latency_p99_ms = self.delete_latency.percentile_ms(99.0);

        // Calculate weighted average latency
        let get_count = self.get_latency.count();
        let put_count = self.put_latency.count();
        let swap_count = self.swap_latency.count();
        let scan_count = self.scan_latency.count();
        let delete_count = self.delete_latency.count();

        let total_count = get_count + put_count + swap_count + scan_count + delete_count;

        let avg_latency_ms = if total_count > 0 {
            (get_latency_ms * get_count as f64
                + put_latency_ms * put_count as f64
                + swap_latency_ms * swap_count as f64
                + scan_latency_ms * scan_count as f64
                + delete_latency_ms * delete_count as f64)
                / total_count as f64
        } else {
            0.0
        };

        // Simple approximation of p95 and p99 overall
        let p95_latency_ms = [
            get_latency_p95_ms,
            put_latency_p95_ms,
            swap_latency_p95_ms,
            scan_latency_p95_ms,
            delete_latency_p95_ms,
        ]
        .iter()
        .fold(0.0_f64, |a, &b| a.max(b));

        let p99_latency_ms = [
            get_latency_p99_ms,
            put_latency_p99_ms,
            swap_latency_p99_ms,
            scan_latency_p99_ms,
            delete_latency_p99_ms,
        ]
        .iter()
        .fold(0.0_f64, |a, &b| a.max(b));

        MetricsSnapshot {
            total_ops,
            successful_ops,
            throughput,
            success_rate,
            get_latency_ms,
            get_latency_p95_ms,
            get_latency_p99_ms,
            put_latency_ms,
            put_latency_p95_ms,
            put_latency_p99_ms,
            swap_latency_ms,
            swap_latency_p95_ms,
            swap_latency_p99_ms,
            scan_latency_ms,
            scan_latency_p95_ms,
            scan_latency_p99_ms,
            delete_latency_ms,
            delete_latency_p95_ms,
            delete_latency_p99_ms,
            avg_latency_ms,
            p95_latency_ms,
            p99_latency_ms,
        }
    }
}

impl MetricsSnapshot {
    /// Check if the benchmark is complete (all operations have been executed)
    pub fn is_complete(&self, total_ops: u64) -> bool {
        self.total_ops >= total_ops
    }

    /// Format the snapshot as a JSON string
    pub fn to_json(&self) -> String {
        json!({
            "total_ops": self.total_ops,
            "successful_ops": self.successful_ops,
            "throughput": self.throughput,
            "success_rate": self.success_rate,
            "latency_ms": {
                "overall": {
                    "avg": self.avg_latency_ms,
                    "p95": self.p95_latency_ms,
                    "p99": self.p99_latency_ms
                },
                "get": {
                    "p50": self.get_latency_ms,
                    "p95": self.get_latency_p95_ms,
                    "p99": self.get_latency_p99_ms
                },
                "put": {
                    "p50": self.put_latency_ms,
                    "p95": self.put_latency_p95_ms,
                    "p99": self.put_latency_p99_ms
                },
                "swap": {
                    "p50": self.swap_latency_ms,
                    "p95": self.swap_latency_p95_ms,
                    "p99": self.swap_latency_p99_ms,
                },
                "scan": {
                    "p50": self.scan_latency_ms,
                    "p95": self.scan_latency_p95_ms,
                    "p99": self.scan_latency_p99_ms
                },
                "delete": {
                    "p50": self.delete_latency_ms,
                    "p95": self.delete_latency_p95_ms,
                    "p99": self.delete_latency_p99_ms
                }
            }
        })
        .to_string()
    }

    /// Print the snapshot to stdout in a human-readable format
    pub fn show(&self) {
        info!("Benchmark Results:");
        info!("==================");
        info!("Operations: {}", self.total_ops);
        info!(
            "Successful: {} ({:.2}%)",
            self.successful_ops, self.success_rate
        );
        info!("Throughput: {:.2} ops/sec", self.throughput);
        info!("Latency (ms):");
        info!("-------------");
        info!(
            "Overall: avg={:.2}, p95={:.2}, p99={:.2}",
            self.avg_latency_ms, self.p95_latency_ms, self.p99_latency_ms
        );
        info!(
            "GET: p50={:.2}, p95={:.2}, p99={:.2}",
            self.get_latency_ms, self.get_latency_p95_ms, self.get_latency_p99_ms
        );
        info!(
            "PUT: p50={:.2}, p95={:.2}, p99={:.2}",
            self.put_latency_ms, self.put_latency_p95_ms, self.put_latency_p99_ms
        );
        info!(
            "SWAP: p50={:.2}, p95={:.2}, p99={:.2}",
            self.swap_latency_ms, self.swap_latency_p95_ms, self.swap_latency_p99_ms
        );
        info!(
            "SCAN: p50={:.2}, p95={:.2}, p99={:.2}",
            self.scan_latency_ms, self.scan_latency_p95_ms, self.scan_latency_p99_ms
        );
        info!(
            "DELETE: p50={:.2}, p95={:.2}, p99={:.2}",
            self.delete_latency_ms, self.delete_latency_p95_ms, self.delete_latency_p99_ms
        );
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
