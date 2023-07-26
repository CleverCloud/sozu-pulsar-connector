//! # Configuration module
//!
//! This module provides structures and helpers to interact with the configuration

use std::{
    env::{self, VarError},
    net::SocketAddr,
    path::PathBuf,
};

use config::{Config, ConfigError, File};
use serde::{Deserialize, Serialize};

use crate::svc::logging::SentryContext;

// -----------------------------------------------------------------------------
// Error

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to build configuration, {0}")]
    Build(ConfigError),
    #[error("failed to serialize configuration, {0}")]
    Serialize(ConfigError),
    #[error("failed to retrieve environment variable '{0}', {1}")]
    EnvironmentVariable(&'static str, VarError),
}

// -----------------------------------------------------------------------------
// Batch

/// Batching-related configuration
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct Retry {
    /// Duration between considering that a connection has timeout
    #[serde(rename = "timeout")]
    pub timeout: u64,
    /// Duration to keep alive a connection
    #[serde(rename = "keep-alive")]
    pub keep_alive: u64,
    /// Minimum duration of a backoff retry
    #[serde(rename = "min-backoff-duration")]
    pub min_backoff_duration: u64,
    /// Maximum duration of a backoff retry
    #[serde(rename = "max-backoff-duration")]
    pub max_backoff_duration: u64,
    /// Maximum number of retries
    #[serde(rename = "max-retries")]
    pub max_retries: u32,
}

// -----------------------------------------------------------------------------
// Pulsar

/// Pulsar-related configuration
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct Pulsar {
    /// The URL of the pulsar server
    #[serde(rename = "url")]
    pub url: String,
    /// Biscuit for authentication to Pulsar
    #[serde(rename = "token")]
    pub token: String,
    /// The pulsar tenant/namespace/topic on which to consume messages to transmit to Sōzu
    #[serde(rename = "topic")]
    pub topic: String,
    /// Pulsar retry options
    #[serde(rename = "retry")]
    pub retry: Retry,
}

// -----------------------------------------------------------------------------
// Batch

/// Sōzu batching-related configuration
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct Batch {
    /// limit to the number of requests in a batch
    #[serde(rename = "max-requests")]
    pub max_requests: u64,
    /// limit to the size of a batch, (in bytes)
    #[serde(rename = "max-size")]
    pub max_size: u64,
    /// maximum time to wait before sending a batch, when no new request is incoming
    #[serde(rename = "max-wait-time")]
    pub max_wait_time: u64,
}

// -----------------------------------------------------------------------------
// Sōzu

/// Sōzu-related configuration
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct Sozu {
    #[serde(rename = "batch")]
    pub batch: Batch,
    #[serde(rename = "configuration")]
    pub configuration: PathBuf,
    #[serde(rename = "deduplicate")]
    pub deduplicate: bool,
}

// -----------------------------------------------------------------------------
// Configuration

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct ConnectorConfiguration {
    /// Socket address on which expose metrics server
    #[serde(rename = "listening-address")]
    pub listening_address: SocketAddr,
    /// Pulsar configuration
    #[serde(rename = "pulsar")]
    pub pulsar: Pulsar,
    /// Sōzu configuration
    #[serde(rename = "sozu")]
    pub sozu: Sozu,
    /// Sentry configuration
    #[serde(rename = "sentry")]
    pub sentry: Option<SentryContext>,
}

impl TryFrom<PathBuf> for ConnectorConfiguration {
    type Error = Error;

    #[tracing::instrument]
    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        Config::builder()
            .add_source(File::from(path).required(true))
            .build()
            .map_err(Error::Build)?
            .try_deserialize()
            .map_err(Error::Serialize)
    }
}

impl ConnectorConfiguration {
    #[tracing::instrument]
    pub fn try_new() -> Result<Self, Error> {
        let homedir = env::var("HOME").map_err(|err| Error::EnvironmentVariable("HOME", err))?;

        Config::builder()
            .add_source(
                File::from(PathBuf::from(format!(
                    "/usr/share/{}/config",
                    env!("CARGO_PKG_NAME")
                )))
                .required(false),
            )
            .add_source(
                File::from(PathBuf::from(format!(
                    "/etc/{}/config",
                    env!("CARGO_PKG_NAME")
                )))
                .required(false),
            )
            .add_source(
                File::from(PathBuf::from(format!(
                    "{}/.config/{}/config",
                    homedir,
                    env!("CARGO_PKG_NAME")
                )))
                .required(false),
            )
            .add_source(
                File::from(PathBuf::from(format!(
                    "{}/.local/share/{}/config",
                    homedir,
                    env!("CARGO_PKG_NAME")
                )))
                .required(false),
            )
            .add_source(File::from(PathBuf::from("config")).required(false))
            .build()
            .map_err(Error::Build)?
            .try_deserialize()
            .map_err(Error::Serialize)
    }
}
