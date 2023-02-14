use std::{any, env};

use anyhow::Context;
use envconfig::Envconfig;
use pulsar::{Authentication, Pulsar, TokioExecutor};
use sozu_command_lib::command::{CommandRequest, CommandRequestOrder};
use tracing::{debug, info};

use sozu_pulsar_connector::RequestMessage;

#[derive(Envconfig, Debug)]
struct Config {
    #[envconfig(from = "PULSAR_URL")]
    pulsar_url: String,

    #[envconfig(from = "PULSAR_TOKEN")]
    pulsar_token: String,

    #[envconfig(from = "PULSAR_TOPIC")]
    pulsar_topic: String,
}

struct RequestSender {
    config: Config,
    pulsar_client: Pulsar<pulsar::TokioExecutor>,
}

impl RequestSender {
    async fn new() -> anyhow::Result<Self> {
        let config =
            Config::init_from_env().with_context(|| "Could not create config from environment")?;

        debug!(
            "The configuration, made from the environment: {:#?}",
            config
        );

        let auth = Authentication {
            name: String::from("token"),
            data: config.pulsar_token.clone().into_bytes(),
        };

        let pulsar_builder = Pulsar::builder(&config.pulsar_url, TokioExecutor).with_auth(auth);

        let pulsar_client: Pulsar<pulsar::TokioExecutor> = pulsar_builder
            .build()
            .await
            .with_context(|| "Can not create pulsar client")?;

        Ok(Self {
            config,
            pulsar_client,
        })
    }

    async fn send_request(&self, request_message: RequestMessage) -> anyhow::Result<()> {
        info!(
            "Trying to send the request: {:#?} on topic {}",
            request_message, self.config.pulsar_topic
        );
        let mut producer = self
            .pulsar_client
            .producer()
            .with_topic(self.config.pulsar_topic.clone())
            .with_name("request-sender")
            .build()
            .await
            .with_context(|| {
                format!(
                    "Could not create a pulsar producer on topic {}",
                    self.config.pulsar_topic
                )
            })?;
        info!("Created a producer");

        producer
            .send(request_message)
            .await
            .with_context(|| "Could not send request message")?
            .await
            .with_context(|| "Did not receive a receipt from the pulsar server")?;

        producer
            .close()
            .await
            .with_context(|| "Could not close producer used to respond")?;

        info!("Request message was successfully sent");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let request_sender = RequestSender::new()
        .await
        .with_context(|| "Could not create request sender")?;

    let request = CommandRequest::new("some-id".into(), CommandRequestOrder::Status, None);

    request_sender.send_request(RequestMessage(request)).await?;

    Ok(())
}
