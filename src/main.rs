use clap::Parser;

use tracing::info;
use sozu_pulsar_connector::{cli::Args, PulsarConnector};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    info!("Hi! Here are the args: {:#?}", args);
    let mut pulsar_connector = PulsarConnector::new(args).await?;

    pulsar_connector.run().await?;

    Ok(())
}
