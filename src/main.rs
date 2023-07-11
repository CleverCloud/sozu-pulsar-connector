use clap::Parser;

use sozu_pulsar_connector::{
    cfg::Configuration, cli::Args, create_and_run_connector, metrics_server, ConnectorError,
};
use tokio::task::JoinError;
use tracing::{error, info};

#[derive(thiserror::Error, Debug)]
pub enum MainError {
    #[error("Error parsing the configuration: {0}")]
    Config(String),
    #[error("Error with the metrics server: {0}")]
    MetricsServer(metrics_server::MetricsServerError),
    #[error("Error with the pulsar connector: {0}")]
    ConnectorError(ConnectorError),
    #[error("Async runtime error: {0}")]
    TokioError(JoinError),
}

impl From<JoinError> for MainError {
    fn from(err: JoinError) -> Self {
        Self::TokioError(err)
    }
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    info!("Hi! Here are the args: {:#?}", args);

    let config =
        Configuration::try_from(args.config).map_err(|e| MainError::Config(e.to_string()))?;

    let result = tokio::select! {
        r = tokio::spawn(create_and_run_connector(config.clone())) => {
            r?.map_err(MainError::ConnectorError)
        }
        r = tokio::spawn(metrics_server::serve_metrics(config.clone())) => {
            r?.map_err(MainError::MetricsServer)
        }
    };

    if let Err(err) = result {
        error!(
            error = err.to_string(),
            "Could not execute {} properly",
            env!("CARGO_PKG_NAME")
        );

        return Err(err);
    }

    info!("Successfully halted the {}!", env!("CARGO_PKG_NAME"));

    Ok(())
}
