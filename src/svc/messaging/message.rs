//! # Message module
//!
//! This module provides the structure and implemetation to serialize and
//! deserialize messages coming from pulsar.

use prost::Message;
use pulsar::DeserializeMessage;
use serde::{Deserialize, Serialize};
use sozu_command_lib::proto::command::Request;

// -----------------------------------------------------------------------------
// Error

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to decode message from pulsar, {0}")]
    Decode(prost::DecodeError),
}

// -----------------------------------------------------------------------------
// RequestMessage

/// RequestMessage is a wrapper around a [`Request`].
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct RequestMessage {
    pub inner: Request,
}

impl From<Request> for RequestMessage {
    #[tracing::instrument]
    fn from(inner: Request) -> Self {
        Self { inner }
    }
}

impl DeserializeMessage for RequestMessage {
    type Output = Result<Self, Error>;

    #[tracing::instrument(skip_all)]
    fn deserialize_message(payload: &pulsar::Payload) -> Self::Output {
        Request::decode(payload.data.as_slice())
            .map_err(Error::Decode)
            .map(RequestMessage::from)
    }
}
