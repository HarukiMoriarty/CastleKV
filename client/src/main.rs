use clap::Parser;
use common::{init_tracing, set_default_rust_log, Session};
use rpc::gateway::{CommandResult, OperationResult, Status};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::collections::HashMap;
use std::fmt::Display;
use std::time::Instant;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(
        long,
        short,
        default_value = "0.0.0.0:24000",
        help = "The address to connect to."
    )]
    connect_addr: String,
}

#[tokio::main(worker_threads = 1)]
async fn main() -> anyhow::Result<()> {
    set_default_rust_log("info");
    init_tracing();

    let mut rl = DefaultEditor::new()?;

    let cli = Cli::parse();

    let address = format!("http://{}", cli.connect_addr);
    let mut session = Session::remote("terminal", address).await?;
    let mut measure_time = false;

    loop {
        let readline = rl.readline(&get_prompt(&mut session));
        match readline {
            Ok(line) => {
                let tokens = tokenize(&line);
                if tokens.is_empty() || tokens[0].is_empty() {
                    continue;
                }

                let timer = measure_time.then(Instant::now);

                let output = match tokens[0].to_lowercase().as_str() {
                    "cmd" => handle_cmd(&mut session, &tokens),
                    "op" => handle_op(&mut session, &tokens).await,
                    "done" => handle_done(&mut session, &tokens).await,
                    "time" => {
                        measure_time = !measure_time;
                        format!("TIME: {measure_time}")
                    }
                    "clear" => {
                        rl.clear_screen()?;
                        String::new()
                    }
                    "exit" | "quit" => {
                        break;
                    }
                    // Add direct key-value operations.
                    "get" => handle_direct_op(&mut session, "get", &tokens[1..]).await,
                    "set" => handle_direct_op(&mut session, "set", &tokens[1..]).await,
                    "del" => handle_direct_op(&mut session, "del", &tokens[1..]).await,
                    _ => error(format!("unknown command: {line}")),
                };

                let elapsed = timer.map(|timer| timer.elapsed());

                println!("{output}");

                if let Some(elapsed) = elapsed {
                    let elapsed_secs = elapsed.as_secs();
                    let elapsed_subsec_millis = elapsed.subsec_millis();
                    let elapsed_subsec_micros = elapsed.subsec_micros();
                    let elapsed_subsec_nanos = elapsed.subsec_nanos();
                    let (elapsed, unit): (f32, &str) = if elapsed_secs > 0 {
                        (
                            format!("{elapsed_secs}.{elapsed_subsec_millis}")
                                .parse()
                                .unwrap(),
                            "s",
                        )
                    } else if elapsed_subsec_millis > 0 {
                        (
                            format!("{elapsed_subsec_millis}.{elapsed_subsec_micros}")
                                .parse()
                                .unwrap(),
                            "ms",
                        )
                    } else if elapsed_subsec_micros > 0 {
                        (
                            format!("{elapsed_subsec_micros}.{elapsed_subsec_nanos}")
                                .parse()
                                .unwrap(),
                            "µs",
                        )
                    } else {
                        (elapsed_subsec_nanos as f32, "ns")
                    };
                    println!("TIME: {elapsed:.2} {unit}");
                }
            }
            Err(ReadlineError::Interrupted) => {
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                error(err);
                break;
            }
        }
    }
    Ok(())
}

fn get_prompt(session: &mut Session) -> String {
    let mut prompt = String::new();
    if let Some(op_id) = session.get_next_op_id() {
        prompt.push_str(&format!("[op {op_id}]"));
    }
    prompt.push_str(">> ");
    prompt
}

/// Tokenizes a string.
/// A string wrapped with double quotes is considered as a single token.
fn tokenize(line: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut in_quote = false;
    for c in line.chars() {
        match c {
            ' ' if !in_quote => {
                if !token.is_empty() {
                    tokens.push(std::mem::take(&mut token))
                }
            }
            '"' => {
                in_quote = !in_quote;
            }
            _ => {
                token.push(c);
            }
        }
    }
    if !token.is_empty() {
        tokens.push(token);
    }
    tokens
}

fn handle_cmd(session: &mut Session, _tokens: &[String]) -> String {
    if let Err(e) = session.new_command() {
        return error(e);
    }

    "COMMAND".to_owned()
}

async fn handle_op(session: &mut Session, tokens: &[String]) -> String {
    if tokens.len() < 2 {
        return error("invalid operation");
    }
    let execute_immediately = session.get_next_op_id().is_none();
    if execute_immediately {
        session.new_command().unwrap();
    }
    let next_op_id = session.get_next_op_id().unwrap();
    session.add_operation(&tokens[1], &tokens[2..]).unwrap();
    if execute_immediately {
        format_result(session.finish_command().await)
    } else {
        format!("OP {}", next_op_id)
    }
}

async fn handle_done(session: &mut Session, tokens: &[String]) -> String {
    if tokens.len() != 1 {
        return error("invalid DONE command");
    }
    format_result(session.finish_command().await)
}

async fn handle_direct_op(session: &mut Session, op: &str, args: &[String]) -> String {
    session.new_command().unwrap();
    session.add_operation(op, args).unwrap();
    format_result(session.finish_command().await)
}

fn format_result(result: anyhow::Result<Vec<CommandResult>>) -> String {
    result.map_or_else(error, |cmd_results| {
        let mut output = Vec::new();

        // Combine command content from all results
        let combined_content: String = cmd_results
            .iter()
            .filter(|res| !res.content.is_empty())
            .map(|res| res.content.clone())
            .collect::<Vec<_>>()
            .join("\n");

        if !combined_content.is_empty() {
            output.push(combined_content);
        }

        // Collect all operations from all results
        let mut all_op_results = Vec::new();
        for cmd_result in &cmd_results {
            all_op_results.extend(cmd_result.ops.clone());
        }
        all_op_results.sort_by_key(|op| op.id);

        // Group operations by ID
        let mut op_groups: HashMap<u32, Vec<OperationResult>> = HashMap::new();
        for op_result in &all_op_results {
            op_groups
                .entry(op_result.id)
                .or_default()
                .push(op_result.clone());
        }

        // Process each operation group
        for (op_id, results) in op_groups.iter() {
            if results.is_empty() {
                continue;
            }

            let first_result = results[0].clone();
            let is_scan = first_result.content.trim_start().starts_with("SCAN ");

            if is_scan {
                // Find min start key and max end key across all partitions
                let mut min_start_key = String::new();
                let mut max_end_key = String::new();
                let mut scan_data = Vec::new();
                let scan_has_err = results.iter().any(|o| o.has_err);

                for result in results {
                    let mut content_lines = result.content.lines();
                    if let Some(first_line) = content_lines.next() {
                        if first_line.trim().starts_with("SCAN ") {
                            let parts: Vec<&str> = first_line.split_whitespace().collect();
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
                    }

                    // Collect data lines
                    for line in content_lines {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() && !trimmed.starts_with("SCAN ") {
                            scan_data.push(trimmed.to_string());
                        }
                    }
                }

                let mut scan_output = Vec::new();
                scan_output.push(format!("SCAN {} {} BEGIN", min_start_key, max_end_key));
                for data_line in scan_data {
                    scan_output.push(format!("  {}", data_line));
                }
                scan_output.push("SCAN END".to_string());

                // Add the formatted output
                if scan_has_err {
                    output.push(format!("{}> {}", op_id, error(scan_output.join("\n"))));
                } else {
                    output.push(format!("{}> {}", op_id, scan_output.join("\n")));
                }
            } else {
                // Combine results for non-scan operations
                let mut op_content = Vec::new();
                let has_err = results.iter().any(|o| o.has_err);

                for result in results {
                    if !result.content.trim().is_empty() {
                        op_content.push(result.content.clone());
                    }
                }

                let combined = op_content.join("\n");
                if has_err {
                    output.push(format!("{}> {}", op_id, error(&combined)));
                } else {
                    output.push(format!("{}> {}", op_id, combined));
                }
            }
        }

        // Determine overall status
        let any_aborted = cmd_results
            .iter()
            .any(|res| res.status == Status::Aborted.into());
        if any_aborted {
            output.push("ABORTED".to_owned());
        } else {
            output.push("COMMITTED".to_owned());
        }

        output.join("\n")
    })
}

fn error(msg: impl Display) -> String {
    format!("ERROR {msg}")
}
