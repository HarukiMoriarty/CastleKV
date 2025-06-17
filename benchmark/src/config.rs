use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug, Clone)]
#[command(name = "KVClient", version, about = "A benchmark client for KV store")]
pub struct Config {
    #[arg(
        long,
        default_value = "0.0.0.0:24000",
        help = "The address to connect to."
    )]
    pub connect_addr: String,

    #[arg(long, default_value = "1", help = "Number of parallel clients")]
    pub num_clients: usize,

    #[arg(long, help = "Number of commands to execute (exclusive with duration)")]
    pub command_count: Option<usize>,

    #[arg(
        long,
        help = "Benchmark duration in seconds (exclusive with command_count)"
    )]
    pub duration: Option<u64>,

    #[arg(
        long,
        default_value = "1000",
        help = "Key space size (0 to key_range - 1)"
    )]
    pub key_range: usize,

    #[command(subcommand)]
    pub workload: WorkloadType,
}

#[derive(Subcommand, Debug, Clone)]
pub enum WorkloadType {
    #[command(name = "random", about = "Random GET/PUT workload")]
    Random(RandomConfig),

    #[command(name = "ycsb", about = "YCSB-style workload")]
    Ycsb(YcsbConfig),

    #[command(name = "tpcc", about = "TPC-C workload")]
    Tpcc(TpccConfig),
}

#[derive(Args, Debug, Clone)]
pub struct RandomConfig {
    #[arg(
        long,
        default_value = "0.8",
        help = "Ratio of GET operations (0.0 to 1.0)"
    )]
    pub rw_ratio: f64,

    #[arg(long, help = "Optional random seed for reproducible randomness")]
    pub seed: u64,
}

#[derive(Args, Debug, Clone)]
pub struct YcsbConfig {
    #[arg(long, default_value = "A", help = "YCSB workload type (A-F)")]
    pub workload_type: String,
}

#[derive(Args, Debug, Clone)]
pub struct TpccConfig {
    #[arg(long, default_value = "1", help = "Number of warehouses")]
    pub warehouses: usize,
}
