use rpc::gateway::{Command, CommandResult, Status as CommandStatus};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::Streaming;
use tracing::{debug, warn};

pub type ExecutorSender = mpsc::UnboundedSender<ExecutorMessage>;
type ExecutorReceiver = mpsc::UnboundedReceiver<ExecutorMessage>;

pub enum ExecutorMessage {
    NewClient {
        stream: Streaming<Command>,
        result_tx: mpsc::Sender<Result<CommandResult, tonic::Status>>,
    },
}

pub struct Executor {
    rx: ExecutorReceiver,
}

impl Executor {
    pub fn new(rx: ExecutorReceiver) -> Self {
        Self { rx }
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                ExecutorMessage::NewClient {
                    mut stream,
                    result_tx,
                } => {
                    debug!("Executor: handling new client");

                    tokio::spawn(async move {
                        while let Some(command) = stream.next().await {
                            match command {
                                Ok(cmd) => {
                                    debug!("Command details: {:?}", cmd);

                                    // TODO: Add lock manager interaction
                                    // TODO: Add database interaction
                                    let result = CommandResult {
                                        ops: vec![],
                                        status: CommandStatus::Committed as i32,
                                        content: "hello world".to_string(),
                                        has_err: false,
                                    };

                                    if result_tx.send(Ok(result)).await.is_err() {
                                        warn!("Client disconnected");
                                        break;
                                    }
                                }
                                Err(e) => {
                                    warn!("Error receiving command: {}", e);
                                    break;
                                }
                            }
                        }
                    });
                }
            }
        }
    }
}
