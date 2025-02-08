use clap::Parser;
use common::{init_tracing, set_default_rust_log, Session};
use rpc::gateway::{CommandResult, Status};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::fmt::Display;
use std::time::Instant;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(
        long,
        short,
        default_value = "http://0.0.0.0:23000",
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

    let mut session = Session::remote("terminal", cli.connect_addr).await?;
    let mut measure_time = false;

    loop {
        let readline = rl.readline(&get_prompt(&session));
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

fn get_prompt(session: &Session) -> String {
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

fn format_result(result: anyhow::Result<CommandResult>) -> String {
    result.map_or_else(error, |cmd_result| {
        let mut output = Vec::new();
        if cmd_result.has_err {
            output.push(if !cmd_result.content.is_empty() {
                error(&cmd_result.content)
            } else {
                error("operation failed")
            });
        } else if !cmd_result.content.is_empty() {
            output.push(cmd_result.content.to_owned());
        };

        let mut sorted_ops = cmd_result.ops.clone();
        sorted_ops.sort_by_key(|op| op.id);
        for op in sorted_ops {
            let line = if op.has_err {
                error(&op.content)
            } else {
                op.content.to_owned()
            };

            output.push(format!("{}> {line}", op.id))
        }
        match cmd_result.status() {
            Status::Aborted => output.push("ABORTED".to_owned()),
            Status::Committed => output.push("COMMITTED".to_owned()),
        }
        output.join("\n")
    })
}

fn error(msg: impl Display) -> String {
    format!("ERROR {msg}")
}
