use histogram::AtomicHistogram;
use serde_json::json;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tracing::info;

#[derive(Default)]
pub struct BenchmarkResult {
    finished_cmds: AtomicU32,
    latency: LatencyHistogram,
}

impl BenchmarkResult {
    pub fn inc_finished_cmds(&self) {
        self.finished_cmds.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_latency(&self, latency: Duration) {
        self.latency.record(latency);
    }

    pub fn snapshot(&self, elapsed: Duration) -> BenchmarkResultSnapshot {
        BenchmarkResultSnapshot::new(
            elapsed,
            self.finished_cmds.load(Ordering::Relaxed),
            self.latency.snapshot(),
        )
    }
}

pub struct BenchmarkResultSnapshot {
    elapsed: Duration,
    finished_cmds: u32,
    throughput: f64,
    latency: LatencyHistogramSnapshot,
}

impl BenchmarkResultSnapshot {
    fn new(elapsed: Duration, finished_cmds: u32, latency: LatencyHistogramSnapshot) -> Self {
        Self {
            elapsed,
            finished_cmds,
            throughput: finished_cmds as f64 / elapsed.as_secs_f64(),
            latency,
        }
    }

    pub fn diff(&self, prev: &Self) -> serde_json::Value {
        let diff_cmds = self.finished_cmds - prev.finished_cmds;
        let diff_elapsed = self.elapsed - prev.elapsed;
        let throughput = diff_cmds as f64 / diff_elapsed.as_secs_f64();

        json!({
            "committed": diff_cmds,
            "throughput": throughput,
            "p0": self.latency.percentile_ms(0.0),
            "p50": self.latency.percentile_ms(50.0),
            "p99": self.latency.percentile_ms(99.0),
        })
    }

    pub fn final_report(&self) {
        let output = json!({
            "elapsed": self.elapsed.as_secs_f64(),
            "commands": self.finished_cmds,
            "throughput": self.throughput,
            "p0": self.latency.percentile_ms(0.0),
            "p50": self.latency.percentile_ms(50.0),
            "p99": self.latency.percentile_ms(99.0),
            "p100": self.latency.percentile_ms(100.0),
        });

        info!("Final Benchmark Report:");
        info!("{}", serde_json::to_string_pretty(&output).unwrap());
    }

    pub fn show_diff(&self, prev: &Self) {
        let diff = self.diff(prev);
        print_intermediate(&diff);
    }
}

pub fn print_intermediate(intermediate: &serde_json::Value) {
    info!(
        "committed {:>6} | thrp {:>5.0} | p0 {:>5.1} ms | p50 {:>5.1} ms | p99 {:>5.1} ms",
        intermediate["committed"].as_i64().unwrap_or(0),
        intermediate["throughput"].as_f64().unwrap_or(0.0),
        intermediate["p0"].as_f64().unwrap_or(0.0),
        intermediate["p50"].as_f64().unwrap_or(0.0),
        intermediate["p99"].as_f64().unwrap_or(0.0),
    );
}

struct LatencyHistogram {
    hist: AtomicHistogram,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            hist: AtomicHistogram::new(10, 64).unwrap(),
        }
    }

    fn record(&self, latency: Duration) {
        self.hist
            .increment(latency.as_nanos() as u64)
            .unwrap_or_else(|_| panic!("invalid latency: {} ns", latency.as_nanos()));
    }

    fn snapshot(&self) -> LatencyHistogramSnapshot {
        LatencyHistogramSnapshot {
            snapshot: self.hist.snapshot(),
        }
    }
}

struct LatencyHistogramSnapshot {
    snapshot: histogram::Snapshot,
}

impl LatencyHistogramSnapshot {
    fn percentile_ms(&self, p: f64) -> f64 {
        self.snapshot
            .percentile(p)
            .map(|b| ((b.start() + b.end()) / 2) as f64 / 1e6)
            .unwrap_or(0.0)
    }
}
