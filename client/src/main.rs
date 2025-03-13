use clap::Parser;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::collections::HashMap;
use std::fmt::Display;
use std::time::Instant;

use common::{extract_key, form_key, init_tracing, set_default_rust_log, Session};
use rpc::gateway::{CommandResult, OperationResult, Status};

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

    // Initialize the command line editor
    let mut rl = DefaultEditor::new()?;

    // Parse command line arguments
    let cli = Cli::parse();

    // Connect to the manager node to get partition settings
    let address = format!("http://{}", cli.connect_addr);
    let mut session = Session::remote("terminal", address).await?;
    let mut measure_time = false;

    loop {
        let readline = rl.readline(&get_prompt(&mut session));
        match readline {
            Ok(line) => {
                let mut tokens = tokenize(&line);
                if tokens.is_empty() || tokens[0].is_empty() {
                    continue;
                }

                let timer = measure_time.then(Instant::now);

                // Process commands based on the first token
                let output = match tokens[0].to_lowercase().as_str() {
                    "cmd" => handle_cmd(&mut session, &tokens),
                    "op" => handle_op(&mut session, &mut tokens).await,
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
                    _ => error(format!("unknown command: {line}")),
                };

                // Print command output
                println!("{output}");

                // Print timing information if enabled
                if let Some(timer) = timer {
                    let elapsed = timer.elapsed();
                    print_elapsed_time(elapsed);
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Handle Ctrl-C
                break;
            }
            Err(ReadlineError::Eof) => {
                // Handle Ctrl-D
                break;
            }
            Err(err) => {
                // Handle other errors
                println!("{}", error(err));
                break;
            }
        }
    }
    Ok(())
}

/// Formats and prints elapsed time with appropriate units
fn print_elapsed_time(elapsed: std::time::Duration) {
    let elapsed_secs = elapsed.as_secs();
    let elapsed_millis = elapsed.subsec_millis();
    let elapsed_micros = elapsed.subsec_micros();
    let elapsed_nanos = elapsed.subsec_nanos();

    let (elapsed, unit): (f32, &str) = if elapsed_secs > 0 {
        (
            format!("{}.{:03}", elapsed_secs, elapsed_millis)
                .parse()
                .unwrap(),
            "s",
        )
    } else if elapsed_millis > 0 {
        (
            format!("{}.{:03}", elapsed_millis, elapsed_micros % 1000)
                .parse()
                .unwrap(),
            "ms",
        )
    } else if elapsed_micros > 0 {
        (
            format!("{}.{:03}", elapsed_micros, elapsed_nanos % 1000)
                .parse()
                .unwrap(),
            "µs",
        )
    } else {
        (elapsed_nanos as f32, "ns")
    };

    println!("TIME: {:.2} {}", elapsed, unit);
}

/// Generates the command prompt string based on session state
fn get_prompt(session: &mut Session) -> String {
    let mut prompt = String::new();
    if let Some(op_id) = session.get_next_op_id() {
        prompt.push_str(&format!("[op {op_id}]"));
    }
    prompt.push_str(">> ");
    prompt
}

/// Tokenizes a string into a vector of tokens.
///
/// A string wrapped with double quotes is considered as a single token,
/// allowing spaces within quoted strings.
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

/// Start a new command
fn handle_cmd(session: &mut Session, _tokens: &[String]) -> String {
    if let Err(e) = session.new_command() {
        return error(e);
    }

    "COMMAND".to_owned()
}

/// Add an operation to the current command
///
/// If no command is in progress, creates a new one and executes it immediately.
async fn handle_op(session: &mut Session, tokens: &mut [String]) -> String {
    // Validate operation arguments
    if tokens.len() < 2 {
        return error("operation name required");
    }

    match tokens[1].to_uppercase().as_str() {
        "GET" | "DELETE" => {
            if tokens.len() != 3 {
                return error("GET/DELETE requires exactly one key");
            }
        }
        "SWAP" | "PUT" => {
            if tokens.len() != 4 {
                return error("PUT/SWAP requires key and value");
            }
        }
        "SCAN" => {
            if tokens.len() != 4 {
                return error("SCAN requires start and end keys");
            }

            // Check if both keys are for the same table
            let res1 = extract_key(&tokens[2]).map_err(|e| anyhow::anyhow!(e));
            let res2 = extract_key(&tokens[3]).map_err(|e| anyhow::anyhow!(e));

            if let (Ok((table1, _)), Ok((table2, _))) = (res1, res2) {
                if table1 != table2 {
                    return error(format!(
                        "SCAN keys must be from same table, got {} and {}",
                        table1, table2
                    ));
                }
            }
        }
        _ => return error(format!("unsupported operation: {}", tokens[1])),
    }

    // Create a new command if one isn't already in progress
    let execute_immediately = session.get_next_op_id().is_none();
    if execute_immediately {
        if let Err(e) = session.new_command() {
            return error(e);
        }
    }

    // Get the operation ID before adding the operation
    let next_op_id = session.get_next_op_id().unwrap();

    // Add the operation to the command
    if let Err(e) = session.add_operation(&tokens[1].to_uppercase(), &tokens[2..]) {
        return error(e);
    }

    // Execute immediately if this wasn't part of a multi-operation command
    if execute_immediately {
        format_result(session.finish_command().await)
    } else {
        format!("OP {}", next_op_id)
    }
}

/// Finish and execute the current command
async fn handle_done(session: &mut Session, tokens: &[String]) -> String {
    if tokens.len() != 1 {
        return error("DONE command takes no arguments");
    }
    format_result(session.finish_command().await)
}

/// Formats the results of a command execution for display
fn format_result(result: anyhow::Result<Vec<CommandResult>>) -> String {
    result.map_or_else(error, |cmd_results| {
        let mut output = Vec::new();

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

            let first_result = &results[0];
            let is_scan = first_result.content.trim_start().starts_with("SCAN ");

            if is_scan {
                format_scan_results(*op_id, results, &mut output);
            } else {
                // Regular operation (should only have one result)
                if results.len() != 1 {
                    output.push(format!(
                        "{}> {}",
                        op_id,
                        error("Unexpected multiple results for non-SCAN operation")
                    ));
                    continue;
                }

                let result = &results[0];
                if result.has_err {
                    output.push(format!("{}> {}", op_id, error(&result.content)));
                } else {
                    output.push(format!("{}> {}", op_id, result.content));
                }
            }
        }

        // Determine overall command status
        let any_aborted = cmd_results
            .iter()
            .any(|res| res.status == Status::Aborted.into());

        output.push(if any_aborted {
            "ABORTED".to_owned()
        } else {
            "COMMITTED".to_owned()
        });

        output.join("\n")
    })
}

/// Format scan results from potentially multiple partitions
fn format_scan_results(op_id: u32, results: &[OperationResult], output: &mut Vec<String>) {
    // Find min start key and max end key across all partitions
    let mut min_start_key = None;
    let mut max_end_key = None;
    let mut table_name = None;
    let mut scan_data = Vec::new();
    let scan_has_err = results.iter().any(|o| o.has_err);

    for result in results {
        let mut content_lines = result.content.lines();
        if let Some(first_line) = content_lines.next() {
            if first_line.trim().starts_with("SCAN ") {
                let parts: Vec<&str> = first_line.split_whitespace().collect();
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
        }

        // Collect data lines
        for line in content_lines {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with("SCAN ") {
                scan_data.push(trimmed.to_string());
            }
        }
    }

    // Format scan results
    let mut scan_output = Vec::new();
    let table = table_name.unwrap();

    scan_output.push(format!(
        "SCAN {} {} BEGIN",
        form_key(&table, min_start_key.unwrap()),
        form_key(&table, max_end_key.unwrap())
    ));

    // Sort scan data by key value
    scan_data.sort_by(|a, b| {
        let a_parts: Vec<&str> = a.split_whitespace().collect();
        let b_parts: Vec<&str> = b.split_whitespace().collect();

        if a_parts.is_empty() || b_parts.is_empty() {
            return std::cmp::Ordering::Equal;
        }

        let a_key = a_parts[0];
        let b_key = b_parts[0];

        let a_result = extract_key(a_key).map(|(_, num)| num);
        let b_result = extract_key(b_key).map(|(_, num)| num);

        match (a_result, b_result) {
            (Ok(a_num), Ok(b_num)) => a_num.cmp(&b_num),
            _ => a_key.cmp(b_key), // Fallback to string comparison
        }
    });

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
}

/// Formats an error message
fn error(msg: impl Display) -> String {
    format!("ERROR {msg}")
}
