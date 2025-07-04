use clap::Parser;
use std::fmt::Display;
use std::sync::Arc;
use tokio::time::{sleep, Duration, Instant};
use tracing::{debug, error};

use benchmark::config::{Config, WorkloadType};
use benchmark::metrics::BenchmarkResult;
use benchmark::workload::{RandomWorkload, Workload};
use common::{extract_key, form_key, init_tracing, set_default_rust_log, Session};
use rpc::gateway::{CommandResult, Status};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    set_default_rust_log("info");
    init_tracing();

    // Parse command line arguments
    let config = Config::parse();

    let metrics = Arc::new(BenchmarkResult::default());

    // Spawn reporter task
    {
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let mut prev = metrics.snapshot(Duration::from_secs(0));
            let start = Instant::now();
            loop {
                sleep(Duration::from_secs(1)).await;
                let elapsed = start.elapsed();
                let snapshot = metrics.snapshot(elapsed);
                snapshot.show_diff(&prev);
                prev = snapshot;
            }
        });
    }

    let mut handles = Vec::new();
    for i in 0..config.num_clients {
        let config = config.clone();
        let metrics = Arc::clone(&metrics);
        handles.push(tokio::spawn(async move {
            run_client(i, &config, metrics).await;
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    Ok(())
}

/// Start single client
async fn run_client(thread_id: usize, config: &Config, metrics: Arc<BenchmarkResult>) {
    // Create per-thread workload
    let mut workload: Box<dyn Workload + Send> = match &config.workload {
        WorkloadType::Random(cfg) => Box::new(RandomWorkload::new(
            config.key_range,
            cfg.rw_ratio,
            cfg.seed,
        )),
        _ => unimplemented!(),
    };

    // Connect to the server
    let address = format!("http://{}", config.connect_addr);
    let session_name = format!("benchmark-{}", thread_id);
    let mut session = Session::remote(&session_name, address).await.unwrap();

    for _ in 0..config.command_count.unwrap_or(1) {
        let cmd = workload.next_command().await.unwrap();
        let tokens = tokenize(&cmd);
        if tokens.is_empty() || tokens[0].is_empty() {
            continue;
        }

        let start_time = Instant::now();
        match tokens[0].to_uppercase().as_str() {
            "PUT" | "SCAN" | "SWAP" => {
                if tokens.len() != 3 {
                    error!("PUT/SCAN/SWAP requires 2 arguments: <key> <value>");
                    continue;
                }
                let output =
                    execute_command(&mut session, &tokens[0].to_uppercase(), &tokens[1..], None)
                        .await;
                debug!("{}", output);
            }
            "GET" | "DELETE" => {
                if tokens.len() != 2 {
                    error!("{} requires 1 argument: <key>", tokens[0].to_uppercase());
                    continue;
                }
                let output =
                    execute_command(&mut session, &tokens[0].to_uppercase(), &tokens[1..], None)
                        .await;
                debug!("{}", output);
            }
            "STOP" => {
                debug!("STOP");
                break;
            }
            _ => {
                error!("Unknown command: {}", cmd);
            }
        }

        let elasped = start_time.elapsed();
        metrics.inc_finished_cmds();
        metrics.record_latency(elasped);
    }
}

/// Split a line into tokens by whitespace
fn tokenize(line: &str) -> Vec<String> {
    line.split_whitespace().map(String::from).collect()
}

/// Execute a command with retry logic
async fn execute_command(
    session: &mut Session,
    cmd: &str,
    args: &[String],
    key_len: Option<usize>,
) -> String {
    const MAX_RETRIES: u32 = 10;
    let mut retries = 0;

    loop {
        // Create a new command and execute it
        session.new_command().unwrap();
        session.add_operation(cmd, args, key_len).unwrap();

        match handle_result(session.finish_command().await, key_len) {
            Ok(output) if output == "Aborted" && retries < MAX_RETRIES => {
                retries += 1;
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                // Silently retry aborted commands
                continue;
            }
            Ok(output) if output == "Aborted" => {
                panic!("Command still aborted after {} retries", MAX_RETRIES);
            }
            Ok(output) => return output,
            Err(_) => {
                if retries < MAX_RETRIES {
                    retries += 1;
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    // Silently retry aborted commands
                    continue;
                }
            }
        }
    }
}

/// Process command results and format the output
fn handle_result(
    result: anyhow::Result<Vec<CommandResult>>,
    key_len: Option<usize>,
) -> Result<String, String> {
    match result {
        Ok(cmd_results) => {
            // Ensure we have at least one result
            if cmd_results.is_empty() {
                return Err("No command results returned".to_string());
            }

            let is_scan = !cmd_results[0].ops.is_empty()
                && cmd_results[0].ops[0]
                    .content
                    .trim_start()
                    .starts_with("SCAN ");

            // Handle single-command results (GET, PUT, SWAP, DELETE)
            if !is_scan {
                handle_single_command_result(&cmd_results[0])
            }
            // Handle multi-command results (SCAN across partitions)
            else {
                handle_scan_results(&cmd_results, key_len)
            }
        }
        Err(e) => Err(error(e)),
    }
}

/// Process single-command results (GET, PUT, DELETE, SWAP)
fn handle_single_command_result(cmd_result: &CommandResult) -> Result<String, String> {
    // Check for command errors
    if cmd_result.has_err {
        return Err(if !cmd_result.content.is_empty() {
            cmd_result.content.clone()
        } else {
            "Command failed".to_string()
        });
    }

    // Return result based on command status
    match cmd_result.status() {
        Status::Aborted => Ok("Aborted".to_string()),
        Status::Committed => {
            // Ensure we have exactly one operation result
            if cmd_result.ops.len() != 1 {
                return Err(format!(
                    "Expected 1 operation result, got {}",
                    cmd_result.ops.len()
                ));
            }
            Ok(cmd_result.ops[0].content.to_owned())
        }
        Status::Leaderswitch => unreachable!(),
    }
}

/// Process multi-command results from SCAN operations
fn handle_scan_results(
    cmd_results: &[CommandResult],
    key_len: Option<usize>,
) -> Result<String, String> {
    // Check for errors in any of the results
    let has_err = cmd_results.iter().any(|res| res.has_err);
    if has_err {
        let error_content: String = cmd_results
            .iter()
            .filter_map(|res| {
                if res.has_err {
                    Some(res.content.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        return Err(if !error_content.is_empty() {
            error_content
        } else {
            "Command failed".to_string()
        });
    }

    // Process SCAN operation results
    let mut min_start_key = None;
    let mut max_end_key = None;
    let mut table_name = None;
    let mut scan_entries = Vec::new();

    // Process all results to find boundaries and collect entries
    for result in cmd_results {
        if result.ops.len() != 1 {
            continue;
        }

        let op = &result.ops[0];
        if op.has_err {
            continue;
        }

        let lines: Vec<&str> = op.content.lines().collect();
        if lines.is_empty() {
            continue;
        }

        // Parse the SCAN line to get keys
        if lines[0].trim().starts_with("SCAN ") {
            let parts: Vec<&str> = lines[0].split_whitespace().collect();
            if parts.len() >= 3 {
                let (table_name_start, start_key) = extract_key(parts[1]).unwrap();
                let (table_name_end, end_key) = extract_key(parts[2]).unwrap();
                assert_eq!(table_name_start, table_name_end);

                if table_name.is_none() {
                    table_name = Some(table_name_start.clone())
                }
                assert_eq!(table_name, Some(table_name_start));

                // Update min start key
                if min_start_key.is_none() || start_key < min_start_key.unwrap() {
                    min_start_key = Some(start_key);
                }

                // Update max end key
                if max_end_key.is_none() || end_key > max_end_key.unwrap() {
                    max_end_key = Some(end_key);
                }
            }
        }

        // Collect entries (skipping the SCAN header and footer)
        for line in lines.iter().skip(1) {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.contains("SCAN END") {
                scan_entries.push(trimmed.to_string());
            }
        }
    }

    // If no valid keys were found, return empty result
    if min_start_key.is_none() || max_end_key.is_none() {
        return Ok("SCAN BEGIN\nSCAN END".to_string());
    }

    // Format the combined scan result
    let mut scan_result = Vec::new();
    let table = table_name.unwrap();
    scan_result.push(format!(
        "SCAN {} {} BEGIN",
        form_key(&table, min_start_key.unwrap(), key_len),
        form_key(&table, max_end_key.unwrap(), key_len)
    ));

    // Sort entries by key
    scan_entries.sort_by(|a, b| {
        let a_parts: Vec<&str> = a.split_whitespace().collect();
        let b_parts: Vec<&str> = b.split_whitespace().collect();

        if a_parts.is_empty() || b_parts.is_empty() {
            return std::cmp::Ordering::Equal;
        }

        let a_key = a_parts[0];
        let b_key = b_parts[0];

        if let (Ok((_, a_num)), Ok((_, b_num))) = (extract_key(a_key), extract_key(b_key)) {
            a_num.cmp(&b_num)
        } else {
            a_key.cmp(b_key) // Fallback to string comparison
        }
    });

    // Format keys using key_len if provided
    if let Some(key_len) = key_len {
        for entry in &mut scan_entries {
            let parts: Vec<&str> = entry.split_whitespace().collect();
            let key = parts[0];
            let (_, num) = extract_key(key).unwrap();
            let formatted_key = form_key(&table, num, Some(key_len));
            // Replace the original key with the formatted one
            *entry = entry.replacen(key, &formatted_key, 1);
        }
    }

    // Add sorted entries to result
    for entry in scan_entries {
        scan_result.push(entry);
    }

    scan_result.push("SCAN END".to_string());

    // Determine overall status
    let any_aborted = cmd_results
        .iter()
        .any(|res| res.status == Status::Aborted as i32);

    if any_aborted {
        Ok("Aborted".to_string())
    } else {
        Ok(scan_result.join("\n"))
    }
}

/// Format an error message
fn error(msg: impl Display) -> String {
    format!("ERROR {msg}")
}
