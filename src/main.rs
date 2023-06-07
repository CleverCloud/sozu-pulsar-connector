use anyhow::Context;
use clap::Parser;

use sozu_pulsar_connector::{cli::Args, PulsarConnector};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    info!("Hi! Here are the args: {:#?}", args);
    let mut pulsar_connector = PulsarConnector::new(args)
        .await
        .with_context(|| "Could not create the pulsar connector")?;

    pulsar_connector
        .run()
        .await
        .with_context(|| "The pulsar connector got interrupted")?;

    Ok(())
}
