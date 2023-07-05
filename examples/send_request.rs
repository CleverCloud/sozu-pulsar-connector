use anyhow::Context;
use clap::Parser;
use pulsar::{Authentication, Pulsar, TokioExecutor};
use sozu_command_lib::proto::command::{request::RequestType, Request, Status};
use tracing::{debug, info};

use sozu_pulsar_connector::{cfg::Configuration, cli::Args, message::RequestMessage};

struct RequestSender {
    config: Configuration,
    pulsar_client: Pulsar<pulsar::TokioExecutor>,
}

impl RequestSender {
    async fn new() -> anyhow::Result<Self> {
        let args = Args::parse();

        let config = Configuration::try_from(args.config)?;

        debug!(
            "The configuration, made from the environment: {:#?}",
            config
        );

        let auth = Authentication {
            name: String::from("token"),
            data: config.pulsar.token.clone().into_bytes(),
        };

        let pulsar_builder = Pulsar::builder(&config.pulsar.url, TokioExecutor).with_auth(auth);

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
            request_message, self.config.pulsar.topic
        );
        let mut producer = self
            .pulsar_client
            .producer()
            .with_topic(self.config.pulsar.topic.clone())
            .with_name("request-sender")
            .build()
            .await
            .with_context(|| {
                format!(
                    "Could not create a pulsar producer on topic {}",
                    self.config.pulsar.topic
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

    let request = Request {
        request_type: Some(RequestType::Status(Status {})),
    };

    info!("Creating request {:?}", request);

    request_sender.send_request(RequestMessage(request)).await?;

    Ok(())
}
