pub mod cli;
pub mod message;

use std::time::Duration;

use anyhow::Context;
use futures::TryStreamExt;
use pulsar::{
    consumer::InitialPosition, Authentication, ConnectionRetryOptions, Consumer, ConsumerOptions,
    OperationRetryOptions, Pulsar, SubType, TokioExecutor,
};
use tracing::{debug, error, info};

use sozu_command_lib::{
    channel::Channel,
    proto::command::{Request, Response},
};

use crate::{cli::Args, message::RequestMessage};

/// A simple connector that consumes Sōzu request messages on a pulsar topic
/// and writes them to a Sōzu instance
pub struct PulsarConnector {
    pulsar_consumer: Consumer<RequestMessage, TokioExecutor>,
    sozu_channel: Channel<Request, Response>,
}

impl PulsarConnector {
    pub async fn new(args: Args) -> anyhow::Result<Self> {
        let authentication = Authentication {
            name: "token".to_owned(),
            data: args.token.clone().into_bytes(),
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

        let pulsar_client: Pulsar<_> = Pulsar::builder(&args.pulsar_url, TokioExecutor)
            .with_auth(authentication)
            .with_connection_retry_options(connection_retry_options)
            .with_operation_retry_options(operation_retry_options)
            .build()
            .await
            .with_context(|| format!("Error when connecting to pulsar at {}", args.topic))?;

        let pulsar_consumer = pulsar_client
            .consumer()
            .with_topic(&args.topic)
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

        let command_socket_path = args
            .absolute_path_to_command_socket()
            .with_context(|| "Could not get absolute path to command socket")?;

        let mut sozu_channel = Channel::from_path(&command_socket_path, 16384, 163840)
            .with_context(|| "Could not create Channel from the given path")?;

        sozu_channel
            .blocking()
            .with_context(|| "Could not block the channel used to communicate with Sōzu")?;

        Ok(Self {
            sozu_channel,
            pulsar_consumer,
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

    async fn write_command_to_sozu(&mut self, command_request: Request) -> anyhow::Result<()> {
        self.sozu_channel
            .write_message(&command_request)
            .with_context(|| "Channel write error")
    }
}
