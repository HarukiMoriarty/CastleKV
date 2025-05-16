use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_histogram_vec, HistogramTimer};
use prometheus::{CounterVec, HistogramVec};

const METRICS_TIME_BUCKETS: &[f64] = &[
    0.000_001, 0.000_010, 0.000_100, 0.000_500, // 1 us, 10 us, 100 us, 500 us
    0.001_000, 0.002_500, 0.005_000, 0.007_500, // 1 ms, 2.5 ms, 5 ms, 7.5 ms
    0.010_000, 0.025_000, 0.050_000, 0.075_000, // 10 ms, 25 ms, 50 ms, 75 ms
    0.100_000, 0.250_000, 0.500_000, 0.750_000, // 100 ms, 250 ms, 500 ms, 750 ms
    1.000_000, 2.500_000, 5.000_000, 7.500_000, // 1 s, 2 s, 5 s, 10 s
];

lazy_static! {
    pub(super) static ref COMMAND_DURATION: HistogramVec = register_histogram_vec!(
        "castlekv_command_duration_seconds",
        "Time from receiving a command to sending a response",
        &[],
        METRICS_TIME_BUCKETS.into(),
    )
    .unwrap();
    pub(super) static ref LOCK_ACQUISITION_DURATION: HistogramVec = register_histogram_vec!(
        "castlekv_lock_acquisition_duration_seconds",
        "Time to acquire locks",
        &["cc", "annotation"],
        METRICS_TIME_BUCKETS.into(),
    )
    .unwrap();
    pub(super) static ref LOCK_RELEASE_DURATION: HistogramVec = register_histogram_vec!(
        "castlekv_lock_release_duration_seconds",
        "Time to release locks",
        &["cc", "annotation"],
        METRICS_TIME_BUCKETS.into(),
    )
    .unwrap();
    pub(super) static ref LOCK_MANAGER_QUEUE: QueueMetrics = QueueMetrics {
        starts: register_counter_vec!(
            "castlekv_lock_manager_queue_starts_total",
            "Number of commands at the start of the lock manager queue",
            &[]
        )
        .unwrap(),
        wait_time: register_histogram_vec!(
            "castlekv_lock_manager_queue_duration_seconds",
            "Time to wait in queue before reaching the lock manager",
            &[],
            METRICS_TIME_BUCKETS.into(),
        )
        .unwrap(),
    };
}

pub(super) struct QueueMetrics {
    starts: CounterVec,
    wait_time: HistogramVec,
}

impl QueueMetrics {
    pub(super) fn start_timer(&self, labels: &[&str]) -> HistogramTimer {
        self.starts.with_label_values(labels).inc();
        self.wait_time.with_label_values(labels).start_timer()
    }
}
