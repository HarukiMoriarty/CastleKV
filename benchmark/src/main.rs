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
        default_value = "http://0.0.0.0:23000",
        help = "The address to connect to."
    )]
    connect_addr: String,

    #[arg(long, default_value = "1", help = "The number of clients")]
    nclient: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    set_default_rust_log("info");
    init_tracing();

    let cli = Cli::parse();

    // For now, we'll just use single client
    let mut session = Session::remote("terminal", cli.connect_addr).await?;

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
    const MAX_RETRIES: u32 = 3;
    let mut retries = 0;

    loop {
        session.new_command().unwrap();
        session.add_operation(cmd, args).unwrap();
        match handle_result(session.finish_command().await) {
            Ok(output) if output == "Aborted" && retries < MAX_RETRIES => {
                retries += 1;
                error!("Command aborted, retry {}/{}", retries, MAX_RETRIES);
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

fn handle_result(result: anyhow::Result<CommandResult>) -> Result<String, String> {
    match result {
        Ok(cmd_result) => {
            let mut output = Vec::new();

            if cmd_result.has_err {
                return Err(if !cmd_result.content.is_empty() {
                    cmd_result.content
                } else {
                    "command failed".to_string()
                });
            }

            let mut sorted_ops = cmd_result.ops.clone();
            sorted_ops.sort_by_key(|op| op.id);
            for op in sorted_ops {
                if !op.has_err {
                    output.push(op.content.to_owned());
                }
            }

            match cmd_result.status() {
                Status::Aborted => Ok("Aborted".to_string()),
                Status::Committed => Ok(output.join("\n")),
            }
        }
        Err(e) => Err(error(e)),
    }
}

fn error(msg: impl Display) -> String {
    format!("ERROR {msg}")
}
