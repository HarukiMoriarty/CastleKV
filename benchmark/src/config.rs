use clap::{ArgGroup, Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
pub struct BenchmarkConfig {
    #[arg(
        long,
        short,
        default_value = "0.0.0.0:24000",
        help = "Manager addresses to connect to"
    )]
    pub connect_addr: String,

    #[arg(long, short, default_value = "0", help = "Random seed")]
    pub seed: u64,

    #[arg(long, help = "Only print the operations without executing them")]
    pub dry_run: bool,

    #[command(subcommand)]
    pub command: BenchmarkCommand,
}

#[derive(Subcommand, Debug, Clone)]
pub enum BenchmarkCommand {
    /// Load data into the database
    Load(LoadConfig),

    /// Execute the benchmark
    Execute(ExecuteConfig),
}

#[derive(Parser, Debug, Clone)]
pub struct LoadConfig {
    #[arg(
        long,
        default_value = "1000",
        help = "Number of key-value pairs to load"
    )]
    pub num_keys: u64,

    #[arg(long, default_value = "100", help = "Size of values in bytes")]
    pub value_size: usize,

    #[arg(long, help = "Prefix to use for generated keys")]
    pub key_prefix: Option<String>,
}

#[derive(Parser, Debug, Clone)]
#[command(group(
    ArgGroup::new("limit")
        .required(true)
        .args(["num_cmds", "duration"]),
))]
pub struct ExecuteConfig {
    #[arg(long, short = 'c', default_value = "1", help = "Number of clients")]
    pub num_clients: usize,

    #[arg(
        long,
        short = 'r',
        default_value = "50",
        help = "Read percentage (0-100). Remaining percentage will be writes"
    )]
    pub read_percentage: u8,

    #[arg(long, help = "Number of commands to perform", group = "limit")]
    pub num_cmds: Option<u64>,

    #[arg(
        long,
        help = "Duration in seconds to run the benchmark",
        group = "limit"
    )]
    pub duration: Option<u64>,

    #[arg(
        long,
        default_value = "1000",
        help = "Number of keys to use during execution"
    )]
    pub num_keys: u64,

    #[arg(long, default_value = "100", help = "Size of values in bytes")]
    pub value_size: usize,

    #[arg(
        long,
        default_value = "uniform",
        help = "Key access distribution pattern"
    )]
    pub key_distribution: KeyDistribution,

    #[arg(long, help = "Prefix to use for generated keys")]
    pub key_prefix: Option<String>,

    #[arg(
        long,
        default_value = "false",
        help = "Perform a warmup phase before measurement"
    )]
    pub warmup: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum KeyDistribution {
    /// Uniform random distribution
    Uniform,
    /// Sequential keys
    Sequential,
    /// Zipfian distribution (power-law)
    Zipfian,
    /// Latest biased (access recently created keys more often)
    Latest,
}

impl BenchmarkConfig {
    /// Parse CLI arguments
    pub fn parse_args() -> Self {
        Self::parse()
    }
}

impl ExecuteConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.read_percentage > 100 {
            return Err("Read percentage must be between 0 and 100".into());
        }

        Ok(())
    }
}
