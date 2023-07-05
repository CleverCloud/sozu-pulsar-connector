pub mod cfg;
pub mod cli;
pub mod message;

use std::{
    path::PathBuf,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{bail, Context};
use cfg::Configuration;
use futures::TryStreamExt;
use pulsar::{
    consumer::{InitialPosition, Message as PulsarMessage},
    Authentication, ConnectionRetryOptions, Consumer, ConsumerOptions, OperationRetryOptions,
    Pulsar, SubType, TokioExecutor,
};
use tempdir::TempDir;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    task::{spawn_blocking as blocking, JoinError},
    time::sleep,
};
use tracing::{debug, error, info, trace};

use sozu_command_lib::{
    channel::Channel,
    proto::command::{request::RequestType, Request, Response},
    request::WorkerRequest,
    state::ConfigState,
};

use crate::message::RequestMessage;

/// State machine for the batching process.
/// Either we receive an incoming request to batch,
/// or we reached a tick and stop receiving, to send a batch
pub enum BatchingState {
    /// Receiving a request to be added to the batch
    Receiving(Option<PulsarMessage<RequestMessage>>),
    /// Tick has been reached, the batch should be sent
    Ticked,
}

/*
impl From<std::option::Option<pulsar::consumer::Message<RequestMessage>>> for BatchingState {
    fn from(request: std::option::Option<pulsar::consumer::Message<RequestMessage>>) -> Self {
        Self::Receiving(request)
    }
}
*/

/// A simple connector that consumes Sōzu request messages on a pulsar topic
/// and writes them to a Sōzu instance
pub struct PulsarConnector {
    config: Configuration,
    pulsar_consumer: Consumer<RequestMessage, TokioExecutor>,
    sozu_channel: Channel<Request, Response>,
    /// A Sōzu state to filter redundant requests.
    ///
    /// In order to deduplicate the requests, we feed them to a Sōzu state,
    /// that will tell us if they are redundant.
    ///
    /// Needed: `ConfigState::dispatch()` in `sozu_command_lib` should return a custom error that we could pattern-match on
    sozu_state: ConfigState,
    /// Either waiting for a request, receiving one to batch, or a tick is reached
    batching_state: BatchingState,
    /// the temporary directory in which the batch file will be created
    temp_dir: TempDir,
    /// the path of the file where requests will be batched (cleared and recreated at each tick)
    batch_file: PathBuf,
    /// writes on the batch file
    file_writer: BufWriter<File>,
    /// total number of requests ever received
    ///
    /// May be higher than `requests_sent` because the received requests are
    /// checked for redundancy before batching and sending them to Sōzu.
    requests_received: i32,
    /// Total number of requests ever batched and sent
    requests_sent: u64,
    /// the value of the `requests_sent` counter as the time of the last batch.
    last_sent_idx: u64,
    /// size of the current batch, in bytes
    current_batch_size: u64,
}

impl PulsarConnector {
    pub async fn new(config: Configuration) -> anyhow::Result<Self> {
        let authentication = Authentication {
            name: "token".to_owned(),
            data: config.pulsar.token.clone().into_bytes(),
        };
        let operation_retry_options = OperationRetryOptions {
            retry_delay: Duration::from_secs(30), // default is 500ms
            ..Default::default()
        };

        let connection_retry_options = ConnectionRetryOptions {
            min_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(120),
            max_retries: u32::MAX,
            ..Default::default()
        };

        let pulsar_client: Pulsar<_> = Pulsar::builder(&config.pulsar.url, TokioExecutor)
            .with_auth(authentication)
            .with_connection_retry_options(connection_retry_options)
            .with_operation_retry_options(operation_retry_options)
            .build()
            .await
            .with_context(|| {
                format!("Error when connecting to pulsar at {}", config.pulsar.topic)
            })?;

        let pulsar_consumer = pulsar_client
            .consumer()
            .with_topic(&config.pulsar.topic)
            .with_subscription("sozu-pulsar-connector")
            .with_subscription_type(SubType::Exclusive)
            .with_consumer_name("pulsar-connector")
            .with_options(ConsumerOptions {
                initial_position: InitialPosition::Earliest,
                ..Default::default()
            })
            .build()
            .await
            .with_context(|| "Failed at creating a pulsar consumer")?;

        let command_socket_path = config
            .sozu
            .get_socket_path_from_config()
            .with_context(|| "Could not get absolute path to command socket")?;

        let mut sozu_channel = Channel::from_path(&command_socket_path, 16384, 163840)
            .with_context(|| "Could not create Channel from the given path")?;

        sozu_channel
            .blocking()
            .with_context(|| "Could not block the channel used to communicate with Sōzu")?;

        let temp_dir_path = config.batch.temp_dir.clone();
        let temp_dir = blocking(move || TempDir::new(&temp_dir_path)).await??;

        let batch_file = temp_dir.path().join("requests-0.json");
        let file_writer = BufWriter::new(File::create(&batch_file).await?);

        Ok(Self {
            config,
            sozu_channel,
            pulsar_consumer,
            sozu_state: ConfigState::default(),
            batching_state: BatchingState::Ticked,
            temp_dir,
            batch_file,
            file_writer,
            requests_received: 0,
            requests_sent: 0,
            last_sent_idx: 0,
            current_batch_size: 0,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("Listening for incoming messages");
        while let Some(msg) = self.pulsar_consumer.try_next().await? {
            let message = match msg.deserialize() {
                Ok(m) => m,
                Err(e) => {
                    error!("Error deserializing message: {:?}", e);
                    self.pulsar_consumer.ack(&msg).await?;
                    continue;
                }
            };

            info!("received message: {:?}", message);

            let command_request = message.0.clone();

            match self.write_command_to_sozu(command_request.clone()).await {
                Ok(()) => info!("Command request successfully written to Sōzu"),
                Err(write_error) => error!("Error writing request to sozu: {:#}", write_error),
            }

            debug!("acknowledging message {:?}", message);
            if let Err(e) = self.pulsar_consumer.ack(&msg).await {
                error!("Could not acknowledge message {}", e);
            }
        }

        Ok(())
    }

    pub fn should_send_batch(&self) -> bool {
        (matches!(self.batching_state, BatchingState::Ticked)
            && self.last_sent_idx != self.requests_sent)
            || self.requests_sent % self.config.batch.max_requests == 0
            || self.current_batch_size >= self.config.batch.max_size
    }

    pub async fn send_batched_requests_to_sozu(&mut self) -> anyhow::Result<()> {
        info!(
            requests_received = self.requests_received,
            batches_sent = self.requests_sent,
            state_file = self.batch_file.display().to_string(),
            "Requests forwarded to the proxy"
        );

        self.file_writer.flush().await?;
        let load_state_request =
            RequestType::LoadState(self.batch_file.to_string_lossy().to_string());

        if let Err(err) = self.write_command_to_sozu(load_state_request.into()).await {
            error!(
                error = err.to_string(),
                "Could not send batched requests to Sozu"
            );
        }

        Ok(())
    }

    pub async fn add_request_to_batch(&mut self, request: &Request) -> anyhow::Result<()> {
        // why a WorkerRequest?
        let worker_request = WorkerRequest {
            id: format!("{}-{}", self.config.batch.temp_dir, self.requests_sent).to_uppercase(),
            content: request.clone(),
        };

        let payload = blocking(move || serde_json::to_string(&worker_request)).await??;

        self.file_writer
            .write_all(format!("{payload}\n\0").as_bytes())
            .await?;

        self.current_batch_size += (payload.as_bytes().len() + 2) as u64;

        Ok(())
    }

    pub async fn recreate_batch_file(&mut self) -> anyhow::Result<()> {
        self.batch_file = self
            .temp_dir
            .path()
            .join(format!("requests-{}.json", self.requests_sent));

        debug!("Creating batch file {:?}", self.batch_file);
        let file = File::create(&self.batch_file).await?;
        self.file_writer = BufWriter::new(file);
        Ok(())
    }

    /// Consume the pulsar topic, batch the requests by writing them in a temporary file
    /// and periodically tell Sōzu to load this file as state
    pub async fn run_with_batching(&mut self) -> anyhow::Result<()> {
        loop {
            // try receiving pulsar messages
            self.batching_state = tokio::select! {
                pulsar_message_result = self.pulsar_consumer.try_next() => {
                    let pulsar_message_option = match pulsar_message_result {
                        Ok(pulsar_message_option) => pulsar_message_option,
                        Err(_) => bail!("Error while consuming pulsar message"),
                    };
                    BatchingState::Receiving(pulsar_message_option)
                }
                _ = sleep(Duration::from_secs(self.config.batch.interval)) => BatchingState::Ticked,
            };

            // either send a batch
            if self.should_send_batch() {
                self.send_batched_requests_to_sozu().await?;

                self.recreate_batch_file().await?;

                self.current_batch_size = 0;
                self.last_sent_idx = self.requests_sent;
            }

            // or add the received request to the current batch
            if let BatchingState::Receiving(pulsar_message_option) = &self.batching_state {
                let request = match pulsar_message_option {
                    Some(request_message) => request_message.deserialize()?.0,
                    None => {
                        // send the remaining requests and exit the loop
                        self.send_batched_requests_to_sozu().await?;
                        return Ok(());
                    }
                };
                debug!("Received request {:?}", request);

                self.requests_received += 1;

                // Check if the order is legit and write it on the batch file if it is the case.
                // if the state returns an error when dispatching a request,
                // it most probably means the request is redundant
                if self.sozu_state.dispatch(&request).is_ok() {
                    self.add_request_to_batch(&request).await?;
                    self.requests_sent += 1;
                } else {
                    debug!("This request is redundant");
                }
            }
        }
    }

    async fn write_command_to_sozu(&mut self, command_request: Request) -> anyhow::Result<()> {
        self.sozu_channel
            .write_message(&command_request)
            .with_context(|| "Channel write error")
    }
}
