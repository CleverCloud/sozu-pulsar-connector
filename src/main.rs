use anyhow::Context;
use clap::Parser;

use sozu_pulsar_connector::{cfg::Configuration, cli::Args, PulsarConnector};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    info!("Hi! Here are the args: {:#?}", args);

    let config = Configuration::try_from(args.config)?;

    let mut pulsar_connector = PulsarConnector::new(config)
        .await
        .with_context(|| "Could not create the pulsar connector")?;


    pulsar_connector
        .run_with_batching()
        .await
        .with_context(|| "The pulsar connector got interrupted")?;

    Ok(())
}
