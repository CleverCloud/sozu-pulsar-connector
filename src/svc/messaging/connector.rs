//! # Connector module
//!
//! This module provides a connector that create a consumer and execute

use std::{path::PathBuf, sync::Arc, time::Duration};

use futures::TryStreamExt;
use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, IntCounterVec};
use pulsar::{
    consumer::{InitialPosition, Message},
    Authentication, ConnectionRetryOptions, Consumer, ConsumerOptions, Pulsar, TokioExecutor,
};
use sozu_client::{
    channel::ConnectionProperties, config::canonicalize_command_socket, Client, Sender,
};
use sozu_command_lib::{
    config::Config,
    proto::{
        command::{request::RequestType, Request},
        display::format_request_type,
    },
    request::WorkerRequest,
    state::ConfigState,
};
use tempdir::TempDir;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    task::{spawn_blocking as blocking, JoinError},
    time::{sleep_until, Instant},
};
use tracing::{debug, error, info, info_span, trace, warn, Instrument};

use crate::svc::{config::ConnectorConfiguration, messaging::message::RequestMessage};

// -----------------------------------------------------------------------------
// Telemetry

static REQUEST_EMITTED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pulsar_connector_request_emitted",
        "Number of request emitted by the pulsar connector",
        &["kind"]
    )
    .expect("'pulsar_connector_request_emitted' to not be already registered")
});

// -----------------------------------------------------------------------------
// ConnectorError

#[derive(thiserror::Error, Debug)]
pub enum ConnectorError {
    #[error("failed to create Sōzu client, {0}")]
    CreateClient(sozu_client::Error),
    #[error("failed to canonicalize path to command socket, {0}")]
    CanonicalizeSocket(sozu_client::config::Error),
    #[error("failed to create pulsar client, {0}")]
    CreatePulsarClient(pulsar::Error),
    #[error("failed to create consumer, {0}")]
    CreateConsumer(pulsar::Error),
    #[error("failed to retrieve hostname to use as consumer name, {0}")]
    Hostname(std::io::Error),
    #[error("failed to consume topic, {0}")]
    Consume(pulsar::Error),
    #[error("failed to send batch message to Sōzu, {0}")]
    Send(sozu_client::Error),
    #[error("failed to spawn task on tokio runtime, {0}")]
    TokioSpawn(JoinError),
    #[error("failed to create temporary directory, {0}")]
    CreateTempDir(std::io::Error),
    #[error("failed to create temporary file, {0}")]
    CreateFile(std::io::Error),
    #[error("failed to serialize request, {0}")]
    Serde(serde_json::Error),
    #[error("failed to write request on writer, {0}")]
    Write(std::io::Error),
    #[error("failed to flush request of writer on disk, {0}")]
    Flush(std::io::Error),
}

impl From<JoinError> for ConnectorError {
    #[tracing::instrument]
    fn from(err: JoinError) -> Self {
        Self::TokioSpawn(err)
    }
}

// -----------------------------------------------------------------------------
// BatchingState

/// State machine for the batching process.
/// Either we receive an incoming request to batch,
/// or we reached a tick and stop receiving, to send a batch
#[derive(Default)]
pub enum BatchingState {
    /// Receiving a request to be added to the batch
    ///
    /// If the payload option is None, it means the stream of messages has ended,
    /// which is normal behaviour
    Receiving(Option<Message<RequestMessage>>),
    /// Tick has been reached, the batch should be sent
    Ticked,
    /// Waiting for the stream to be connected, once it will be connected using
    /// the [`PularConnector::connect`] method. it will take one of the above values
    #[default]
    WaitingForConnection,
}

// -----------------------------------------------------------------------------
// PulsarConnector

/// A simple connector that consumes Sōzu request messages on a pulsar topic
/// and writes them to a Sōzu instance
pub struct PulsarConnector {
    /// Connector configuration where is store batching-related information
    config: Arc<ConnectorConfiguration>,
    /// Pooled client to Sōzu
    client: Client,
    /// Pulsar consumer
    consumer: Consumer<RequestMessage, TokioExecutor>,
    /// A Sōzu state to filter redundant requests.
    ///
    /// In order to deduplicate the requests, we feed them to a Sōzu state,
    /// that will tell us if they are redundant.
    ///
    /// Needed: `ConfigState::dispatch()` in `sozu_command_lib` should return a custom error that we could pattern-match on
    config_state: ConfigState,
    /// Either waiting for a request, receiving one to batch, or a tick is reached
    batching_state: BatchingState,
    /// total number of requests ever received
    ///
    /// May be higher than `requests_sent` because the received requests are
    /// checked for redundancy before batching and sending them to Sōzu.
    requests_received: u64,
    /// Total number of requests ever batched and sent
    requests_sent: u64,
    /// the value of the `requests_sent` counter as the time of the last batch.
    last_sent_idx: u64,
    /// size of the current batch, in bytes
    current_batch_size: u64,
    /// Next tick to respect interval of time between to load state.
    instant: Instant,
    /// the temporary directory in which the batch file will be created
    temp_dir: TempDir,
    /// Path to file that it is used to batch loading of requests
    path: PathBuf,
    /// writes on the batch file
    writer: BufWriter<File>,
}

impl PulsarConnector {
    #[tracing::instrument(skip_all)]
    pub async fn try_new(
        config: Arc<ConnectorConfiguration>,
        sozu_config: Arc<Config>,
    ) -> Result<Self, ConnectorError> {
        let instant = Instant::now() + Duration::from_secs(config.sozu.batch.max_wait_time);
        let temp_dir = blocking(move || {
            TempDir::new(env!("CARGO_PKG_NAME")).map_err(ConnectorError::CreateTempDir)
        })
        .await??;

        let path = temp_dir.path().join("requests-0.json");
        let writer = BufWriter::new(
            File::create(&path)
                .await
                .map_err(ConnectorError::CreateFile)?,
        );

        // ---------------------------------------------------------------------
        // Create Sōzu client
        info!("Create Sōzu client");
        let mut opts = ConnectionProperties::from(&*sozu_config);
        if opts.socket.is_relative() {
            opts.socket = canonicalize_command_socket(&config.sozu.configuration, &sozu_config)
                .map_err(ConnectorError::CanonicalizeSocket)?;
        }

        let client = Client::try_new(opts)
            .await
            .map_err(ConnectorError::CreateClient)?;

        // ---------------------------------------------------------------------
        // Create pulsar consumer
        info!("Create pulsar consumer");
        let hostname = hostname::get()
            .map(|str| str.to_string_lossy().to_string())
            .map_err(ConnectorError::Hostname)?;

        let pulsar_client = Pulsar::builder(config.pulsar.url.to_owned(), TokioExecutor)
            .with_allow_insecure_connection(true)
            .with_auth(Authentication {
                name: "token".to_string(),
                data: config.pulsar.token.to_owned().into_bytes(),
            })
            .with_connection_retry_options(ConnectionRetryOptions {
                connection_timeout: Duration::from_millis(config.pulsar.retry.timeout),
                keep_alive: Duration::from_millis(config.pulsar.retry.keep_alive),
                min_backoff: Duration::from_millis(config.pulsar.retry.min_backoff_duration),
                max_backoff: Duration::from_millis(config.pulsar.retry.min_backoff_duration),
                max_retries: config.pulsar.retry.max_retries,
            })
            .build()
            .instrument(info_span!("Pulsar::builder.build"))
            .await
            .map_err(ConnectorError::CreatePulsarClient)?;

        let consumer = pulsar_client
            .consumer()
            .with_topic(&config.pulsar.topic)
            .with_subscription("sozu-pulsar-connector")
            .with_subscription_type(pulsar::SubType::Failover)
            .with_consumer_name(hostname)
            .with_options(ConsumerOptions {
                initial_position: InitialPosition::Earliest,
                read_compacted: Some(false),
                ..Default::default()
            })
            .build()
            .instrument(info_span!("Consumer::builder.build"))
            .await
            .map_err(ConnectorError::CreateConsumer)?;

        Ok(Self {
            config,
            client,
            consumer,
            config_state: ConfigState::new(),
            batching_state: BatchingState::default(),
            requests_received: 0,
            requests_sent: 0,
            last_sent_idx: 0,
            current_batch_size: 0,
            instant,
            temp_dir,
            path,
            writer,
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn should_send_batch(&self) -> bool {
        (matches!(self.batching_state, BatchingState::Ticked)
            && self.last_sent_idx != self.requests_sent)
            || self.requests_sent % self.config.sozu.batch.max_requests == 0
            || self.current_batch_size >= self.config.sozu.batch.max_size
    }

    #[tracing::instrument(skip_all)]
    pub async fn write_request_on_batch_file(
        &mut self,
        request: &Request,
    ) -> Result<(), ConnectorError> {
        if let Some(request_type) = &request.request_type {
            let request_kind = format_request_type(request_type);
            REQUEST_EMITTED.with_label_values(&[&request_kind]).inc();
        }

        let worker_request = WorkerRequest {
            id: format!("{}-{}", env!("CARGO_PKG_NAME"), self.requests_sent).to_uppercase(),
            content: request.to_owned(),
        };

        let payload =
            blocking(move || serde_json::to_string(&worker_request).map_err(ConnectorError::Serde))
                .await??;

        self.writer
            .write_all(format!("{payload}\n\0").as_bytes())
            .await
            .map_err(ConnectorError::Write)?;

        self.current_batch_size += (payload.as_bytes().len() + 2) as u64;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn send_batched_requests_to_sozu(&mut self) -> Result<(), ConnectorError> {
        info!(
            requests_received = self.requests_received,
            requests_sent = self.requests_sent,
            path = self.path.display().to_string(),
            "Forward requests to Sōzu"
        );

        self.writer.flush().await.map_err(ConnectorError::Flush)?;

        let request_type = RequestType::LoadState(self.path.to_string_lossy().to_string());

        let request_kind = format_request_type(&request_type);
        REQUEST_EMITTED.with_label_values(&[&request_kind]).inc();

        self.client
            .send(request_type)
            .await
            .map_err(ConnectorError::Send)?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn recreate_batch_file(&mut self) -> Result<(), ConnectorError> {
        self.path = self
            .temp_dir
            .path()
            .join(format!("requests-{}.json", self.requests_sent));

        debug!(
            path = self.path.display().to_string(),
            "Create new batching file"
        );
        self.writer = BufWriter::new(
            File::create(&self.path)
                .await
                .map_err(ConnectorError::CreateFile)?,
        );

        Ok(())
    }

    /// Consume the pulsar topic, batch the requests by writing them in a temporary file
    /// and periodically tell Sōzu to load this file as state
    #[tracing::instrument(skip_all)]
    pub async fn connect(&mut self) -> Result<(), ConnectorError> {
        loop {
            // -----------------------------------------------------------------
            // Create a timeout with the instant
            if self.instant.elapsed() > Duration::from_secs(self.config.sozu.batch.max_wait_time) {
                self.instant =
                    Instant::now() + Duration::from_secs(self.config.sozu.batch.max_wait_time);
            }

            // -----------------------------------------------------------------
            // try receiving pulsar messages
            self.batching_state = tokio::select! {
                r = self.consumer.try_next() => BatchingState::Receiving(r.map_err(ConnectorError::Consume)?),
                _ = sleep_until(self.instant) => BatchingState::Ticked,
            };

            // -----------------------------------------------------------------
            // either send a batch
            if self.should_send_batch() {
                self.send_batched_requests_to_sozu().await?;
                self.recreate_batch_file().await?;

                self.current_batch_size = 0;
                self.last_sent_idx = self.requests_sent;
            }

            // -----------------------------------------------------------------
            // or add the received request to the current batch
            if let BatchingState::Receiving(message) = &self.batching_state {
                let result = match message {
                    Some(message) => {
                        // Acknowledge the message
                        if let Err(err) = self.consumer.ack(message).await {
                            error!(
                                error = err.to_string(),
                                message = message.key(),
                                "Could not acknowledge message"
                            )
                        }

                        message.deserialize()
                    }
                    None => {
                        // send the remaining requests and exit the loop
                        self.send_batched_requests_to_sozu().await?;
                        return Ok(());
                    }
                };

                trace!("Received request {:?}", result);

                self.requests_received += 1;
                let request = match result {
                    Ok(RequestMessage {
                        inner:
                            Request {
                                request_type: Some(request_type),
                            },
                    }) => Request::from(request_type),
                    Ok(_) => {
                        warn!("Received a request that do not have content, skipping...");
                        continue;
                    }
                    Err(err) => {
                        error!(
                            error = err.to_string(),
                            "Received a request that we are unable to parse, skipping..."
                        );
                        continue;
                    }
                };

                if self.config.sozu.deduplicate {
                    // Check if the request is legit and write it on the batch file if it is the case.
                    // if the state returns an error when dispatching a request,
                    // it most probably means the request is redundant
                    if self.config_state.dispatch(&request).is_ok() {
                        self.write_request_on_batch_file(&request).await?;
                        self.requests_sent += 1;
                    }
                } else {
                    self.write_request_on_batch_file(&request).await?;
                    self.requests_sent += 1;
                }
            }
        }
    }
}
