use crate::{metadata, CommandId};
use anyhow::{bail, ensure};
use rpc::gateway::db_client::DbClient;
use rpc::gateway::{Command, CommandResult, Operation, Status};
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Streaming};
use tracing::debug;

enum Client {
    Remote {
        tx: mpsc::Sender<Command>,
        rx: Streaming<CommandResult>,
    },
    DryRun,
}

/// A session that wraps a connection to the gateway.
///
/// The session provides methods to execute commands and transactions.
pub struct Session {
    client: Client,
    name: String,
    cmd: Option<Command>,
}

impl Session {
    pub async fn remote(name: &str, addr: String) -> anyhow::Result<Self> {
        let mut db = DbClient::connect(addr).await?;

        let (tx, rx) = mpsc::channel(100);

        let mut request = Request::new(ReceiverStream::new(rx));
        request
            .metadata_mut()
            .insert(metadata::SESSION_NAME, FromStr::from_str(name).unwrap());

        let rx = db.connect_executor(request).await?.into_inner();

        Ok(Self {
            client: Client::Remote { tx, rx },
            cmd: None,
            name: name.to_owned(),
        })
    }

    pub fn dry_run(name: &str) -> Self {
        Self {
            client: Client::DryRun,
            cmd: None,
            name: name.to_owned(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Starts a new command with multiple operations.
    ///
    /// The command is not executed until [`finish_command`](Self::finish_command) is called. If a
    /// command is already in progress, return an error.
    pub fn new_command(&mut self) -> anyhow::Result<&mut Self> {
        ensure!(self.cmd.is_none(), "must finish previous command first");
        self.cmd = Some(Command {
            cmd_id: CommandId::INVALID.into(),
            ops: vec![],
        });

        Ok(self)
    }

    /// Returns the id of the next operation.
    pub fn get_next_op_id(&self) -> Option<u32> {
        self.cmd.as_ref().map(|cmd| next_op_id(&cmd.ops))
    }

    /// Adds an operation to the current command.
    ///
    /// If no existing command builder previously created by [`new_command`](Self::new_command)
    /// exists, returns an error.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the operation.
    /// * `args` - The arguments of the operation.
    pub fn add_operation(&mut self, name: &str, args: &[String]) -> anyhow::Result<&mut Self> {
        let Some(cmd) = self.cmd.as_mut() else {
            bail!("no command in progress");
        };

        let id = next_op_id(&cmd.ops);
        cmd.ops.push(Operation {
            id,
            name: name.to_string(),
            args: args.to_vec(),
        });
        Ok(self)
    }

    /// Finishes the current command and execute it.
    ///
    /// If no existing command builder previously created by [`new_command`](Self::new_command)
    /// exists, returns an error.
    pub async fn finish_command(&mut self) -> anyhow::Result<CommandResult> {
        let Some(cmd) = self.cmd.take() else {
            bail!("no command in progress");
        };
        self.execute(cmd).await
    }

    async fn execute(&mut self, cmd: Command) -> anyhow::Result<CommandResult> {
        debug!("Executing command: {cmd:?}");

        match &mut self.client {
            Client::Remote { tx, rx } => {
                // Send the command to the executor.
                tx.send(cmd).await?;

                // Wait for the result.
                let Some(cmd_result) = rx.next().await else {
                    bail!("connection closed");
                };

                let cmd_result = cmd_result?;

                Ok(cmd_result)
            }
            Client::DryRun => {
                println!("[{}] Command", self.name);
                for op in &cmd.ops {
                    println!("\t{} {:?}", op.name, op.args);
                }
                println!();
                Ok(CommandResult {
                    status: Status::Committed.into(),
                    content: "dry-run".to_string(),
                    ..Default::default()
                })
            }
        }
    }
}

fn next_op_id(ops: &[Operation]) -> u32 {
    ops.len() as u32
}
