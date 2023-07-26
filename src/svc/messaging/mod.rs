//! # Messaging module
//!
//! This module provides the logic between receiving incoming messages and
//! piping them to Sōzu using the batching way.

use std::sync::Arc;

use sozu_command_lib::config::Config;
use tracing::info;

use crate::svc::config::ConnectorConfiguration;

use self::connector::{ConnectorError, PulsarConnector};

pub mod connector;
pub mod message;

// -----------------------------------------------------------------------------
// Error

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to create connector, {0}")]
    CreateConnector(ConnectorError),
    #[error("failed to connect and consume topic, {0}")]
    Connect(ConnectorError),
}

// -----------------------------------------------------------------------------
// helpers

#[tracing::instrument(skip_all)]
pub async fn consume(
    config: Arc<ConnectorConfiguration>,
    sozu_config: Arc<Config>,
) -> Result<(), Error> {
    let mut connector = PulsarConnector::try_new(config.to_owned(), sozu_config)
        .await
        .map_err(Error::CreateConnector)?;

    info!(
        topic = config.pulsar.topic,
        url = config.pulsar.url,
        "Connect to pulsar and begin consuming messages"
    );

    connector.connect().await.map_err(Error::Connect)
}
