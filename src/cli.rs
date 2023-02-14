use std::{
    fs::canonicalize,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use clap::Parser;
use path_absolutize::*;
use sozu_command_lib::config::FileConfig;
use tracing::{debug, info};

/// Command line tool to consume requests on a Pulsar topic and write them
/// to a Sōzu instance
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long = "config", help = "Path to Sōzu configuration, e.g. config.toml")]
    pub config: Option<String>,

    #[arg(long = "pulsar-url", help = "The URL of the pulsar server")]
    pub pulsar_url: String,

    #[arg(long = "token", help = "Biscuit for authentication to Pulsar")]
    pub token: String,

    #[arg(
        long = "topic",
        help = "The pulsar tenant/namespace/topic on which to consume messages to transmit"
    )]
    pub topic: String,

    #[arg(long = "socket", help = "Path to Sōzu's command socket, a UNIX socket")]
    pub socket: Option<String>,
}

impl Args {
    pub fn absolute_path_to_command_socket(&self) -> anyhow::Result<String> {
        let asbolute_path = match (&self.config, &self.socket) {
            (Some(config_path), _) => self.get_socket_path_from_config(config_path)?,
            (None, Some(socket_path)) => PathBuf::from(socket_path),
            (None, None) => bail!("Please provide either a socket path or a config file"),
        };

        Ok(asbolute_path
            .to_str()
            .with_context(|| "Could not convert command socket path to string")?
            .to_owned())
    }

    /// find the value of command_socket in the sozu config and computes its absolute path
    fn get_socket_path_from_config(&self, config_path: &str) -> anyhow::Result<PathBuf> {
        let path = PathBuf::from(config_path);

        let absolute_path = canonicalize(path)
            .with_context(|| format!("Could not get absolute path for {}", config_path))?;
        debug!("config file path: {:?}", absolute_path);

        let mut config_file_directory = absolute_path
            .parent()
            .with_context(|| format!("Could not get parent of absolute path {:?}", absolute_path))?
            .to_path_buf();
        debug!("config directory: {:?}", config_file_directory);

        let socket_relative_path = FileConfig::load_from_path(&config_path)
            .with_context(|| format!("Could not load Sōzu config on path {}", config_path))?
            .command_socket
            .with_context(|| {
                format!(
                    "Missing field command_socket in config file {:?}",
                    config_path
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

        Ok(socket_absolute_path)
    }
}
