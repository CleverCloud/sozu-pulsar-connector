use std::{env::VarError, fs::canonicalize, path::PathBuf};

use anyhow::Context;
use config::{Config, ConfigError, File};
use path_absolutize::Absolutize;
use serde::{Deserialize, Serialize};
use sozu_command_lib::config::FileConfig;
use tracing::{debug, info};

/// Configuration error
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to read configuration from sources, {0}")]
    Read(ConfigError),
    #[error("failed to deserialize configuration, {0}")]
    Deserialize(ConfigError),
    #[error("failed to retrieve environment variable '{0}', {1}")]
    EnvironmentVariable(&'static str, VarError),
}

/// Sōzu-related configuration
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct Sozu {
    /// Absolute path to Sōzu configuration, e.g. config.toml
    pub config_path: String,
}

impl Sozu {
    /// find the value of command_socket in the sozu config and computes its absolute path
    pub fn get_socket_path_from_config(&self) -> anyhow::Result<String> {
        let path = PathBuf::from(&self.config_path);

        let absolute_path = canonicalize(path)
            .with_context(|| format!("Could not get absolute path for {}", self.config_path))?;
        debug!("config file path: {:?}", absolute_path);

        let mut config_file_directory = absolute_path
            .parent()
            .with_context(|| format!("Could not get parent of absolute path {:?}", absolute_path))?
            .to_path_buf();
        debug!("config directory: {:?}", config_file_directory);

        let socket_relative_path = FileConfig::load_from_path(&self.config_path)
            .with_context(|| format!("Could not load Sōzu config on path {}", self.config_path))?
            .command_socket
            .with_context(|| {
                format!(
                    "Missing field command_socket in config file {:?}",
                    self.config_path
                )
            })?;
        debug!(
            "command socket path relative to config file: {:?}",
            socket_relative_path
        );

        config_file_directory.push(socket_relative_path);
        debug!("concatenated path: {:?}", config_file_directory);

        // the absolute path may be of the form /something/something/../config.toml
        // the path_absolutize crate computes an absolute path without the dots
        let socket_absolute_path = config_file_directory
            .absolutize()
            .with_context(|| "Could not absolutize path")?
            .into_owned();
        info!(
            "absolute path to the command socket: {:?}",
            socket_absolute_path
        );

        Ok(socket_absolute_path.to_string_lossy().to_string())
    }
}

/// Batching-related configuration
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct Batch {
    /// Absolute path to the emporary directory in which the JSON requests will be batched
    pub temp_dir: String,
    /// limit to the number of requests in a batch
    pub max_requests: u64,
    /// limit to the size of a batch, (in bytes)
    pub max_size: u64,
    /// maximum time to wait before sending a batch, when no new request is incoming
    pub max_wait_time: u64,
}

/// Pulsar-related configuration
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct Pulsar {
    /// The URL of the pulsar server
    pub url: String,
    /// Biscuit for authentication to Pulsar
    pub token: String,
    /// The pulsar tenant/namespace/topic on which to consume messages to transmit to Sōzu
    pub topic: String,
}

/// The configuration needed by the pulsar connector to function
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct Configuration {
    /// Sōzu-related configuration (config, socket)
    pub sozu: Sozu,
    /// Pulsar-related configuration (broker URL, topic, authentication)
    pub pulsar: Pulsar,
    /// various caps to a batch, such as size and duration
    pub batch: Batch,
    /// Check if requests are redundant by dispatching them on a local Sōzu state
    pub check_request_redundancy: bool,
}

impl TryFrom<String> for Configuration {
    type Error = Error;

    #[tracing::instrument]
    fn try_from(path: String) -> Result<Self, Self::Error> {
        let path = PathBuf::from(path);
        Config::builder()
            .add_source(File::from(path).required(true))
            .build()
            .map_err(Error::Read)?
            .try_deserialize()
            .map_err(Error::Deserialize)
    }
}
