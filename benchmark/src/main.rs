use clap::Parser;
use common::{init_tracing, set_default_rust_log, Session};
use rpc::gateway::{CommandResult, Status};
use std::fmt::Display;
use std::io::{self, BufRead};
use tracing::error;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(
        long,
        short,
        default_value = "0.0.0.0:23000",
        help = "The address to connect to."
    )]
    connect_addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    set_default_rust_log("info");
    init_tracing();

    let cli = Cli::parse();

    let address = format!("http://{}", cli.connect_addr);
    let mut session = Session::remote("terminal", address).await?;

    // Read from stdin
    let stdin = io::stdin();
    let reader = stdin.lock();

    for line in reader.lines() {
        let line = line?;
        let tokens = tokenize(&line);
        if tokens.is_empty() || tokens[0].is_empty() {
            continue;
        }

        match tokens[0].to_uppercase().as_str() {
            "PUT" | "SCAN" | "SWAP" => {
                if tokens.len() != 3 {
                    error!("PUT <key> <value>");
                    continue;
                }
                let output = execute_command(
                    &mut session,
                    tokens[0].to_uppercase().as_str(),
                    &tokens[1..],
                )
                .await;
                println!("{}", output);
            }
            "GET" | "DELETE" => {
                if tokens.len() != 2 {
                    error!("GET <key>");
                    continue;
                }
                let output = execute_command(
                    &mut session,
                    tokens[0].to_uppercase().as_str(),
                    &tokens[1..],
                )
                .await;
                println!("{}", output);
            }
            "STOP" => {
                println!("STOP");
                break;
            }
            _ => {
                error!("Unknown command: {}", line);
            }
        }
    }

    Ok(())
}

fn tokenize(line: &str) -> Vec<String> {
    line.split_whitespace().map(String::from).collect()
}

async fn execute_command(session: &mut Session, cmd: &str, args: &[String]) -> String {
    const MAX_RETRIES: u32 = 10;
    let mut retries = 0;

    loop {
        session.new_command().unwrap();
        session.add_operation(cmd, args).unwrap();
        match handle_result(session.finish_command().await) {
            Ok(output) if output == "Aborted" && retries < MAX_RETRIES => {
                retries += 1;
                // error!("Command aborted, retry {}/{}", retries, MAX_RETRIES);
                continue;
            }
            Ok(output) if output == "Aborted" => {
                panic!("Command still aborted after {} retries", MAX_RETRIES);
            }
            Ok(output) => return output,
            Err(e) => panic!("{}", e),
        }
    }
}

fn handle_result(result: anyhow::Result<Vec<CommandResult>>) -> Result<String, String> {
    match result {
        Ok(cmd_results) => {
            // Specific optimization for YCSB benchmark
            // For PUT, GET, SWAP, DELETE, only has one command result with one operation result
            // For SCAN, might have multiple command result with each has one operation result
            assert!(cmd_results.len() >= 1);
            if cmd_results.len() == 1 {
                let cmd_result = &cmd_results[0];
                let mut output = Vec::new();

                if cmd_result.has_err {
                    return Err(if !cmd_result.content.is_empty() {
                        cmd_result.content.clone()
                    } else {
                        "command failed".to_string()
                    });
                }

                assert!(cmd_result.ops.len() == 1);
                output.push(cmd_result.ops[0].content.to_owned());

                match cmd_result.status() {
                    Status::Aborted => Ok("Aborted".to_string()),
                    Status::Committed => Ok(output.join("\n")),
                }
            } else {
                let has_err = cmd_results.iter().any(|res| res.has_err);

                if has_err {
                    let error_content: String = cmd_results
                        .iter()
                        .map(|res| res.content.clone())
                        .collect::<Vec<_>>()
                        .join("\n");

                    return Err(if !error_content.is_empty() {
                        error_content
                    } else {
                        "command failed".to_string()
                    });
                }

                // Process SCAN operation result
                let mut min_start_key = String::new();
                let mut max_end_key = String::new();
                let mut scan_entries = Vec::new();

                // Process all results to find boundaries and collect entries
                for result in &cmd_results {
                    assert!(result.ops.len() == 1);
                    let op = &result.ops[0];

                    if op.has_err {
                        continue;
                    }

                    let lines: Vec<&str> = op.content.lines().collect();

                    // Parse the SCAN line to get keys
                    if !lines.is_empty() && lines[0].trim().starts_with("SCAN ") {
                        let parts: Vec<&str> = lines[0].trim().split_whitespace().collect();
                        if parts.len() >= 3 {
                            let start_key = parts[1].to_string();
                            let end_key = parts[2].to_string();

                            // Update min start key
                            if min_start_key.is_empty() || start_key < min_start_key {
                                min_start_key = start_key;
                            }

                            // Update max end key
                            if max_end_key.is_empty() || end_key > max_end_key {
                                max_end_key = end_key;
                            }
                        }
                    }

                    // Collect entries (skipping the SCAN header)
                    for line in lines.iter().skip(1) {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() && !trimmed.contains("SCAN END") {
                            scan_entries.push(trimmed.to_string());
                        }
                    }
                }

                // Format the combined scan result
                let mut scan_result = Vec::new();
                scan_result.push(format!("SCAN {} {} BEGIN", min_start_key, max_end_key));

                // Add collected entries
                for entry in scan_entries {
                    scan_result.push(entry);
                }

                scan_result.push("SCAN END".to_string());

                // Determine overall status
                let any_aborted = cmd_results
                    .iter()
                    .any(|res| res.status == Status::Aborted.into());

                if any_aborted {
                    Ok("Aborted".to_string())
                } else {
                    Ok(scan_result.join("\n"))
                }
            }
        }
        Err(e) => Err(error(e)),
    }
}

fn error(msg: impl Display) -> String {
    format!("ERROR {msg}")
}
