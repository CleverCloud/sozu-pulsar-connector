use pulsar::{producer, DeserializeMessage, Error as PulsarError, SerializeMessage};

// #[macro_use]
use serde::{Deserialize, Serialize};

use sozu_command_lib::proto::command::{Request, Response};

/// a wrapper arount sozu's Request, so we can serialize it
#[derive(Serialize, Deserialize, Debug)]
pub struct RequestMessage(pub Request);

impl SerializeMessage for RequestMessage {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload =
            serde_json::to_vec(&input.0).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

// TODO: this should be done in sozu_command_lib instead
/// a wrapper around sozu's Response, so we can deserialize it
#[derive(Serialize, Deserialize)]
struct ResponseMessage(Response);

impl DeserializeMessage for ResponseMessage {
    type Output = Result<ResponseMessage, serde_json::Error>;
    fn deserialize_message(payload: &pulsar::Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

impl DeserializeMessage for RequestMessage {
    type Output = Result<RequestMessage, serde_json::Error>;

    fn deserialize_message(payload: &pulsar::Payload) -> Self::Output {
        Ok(RequestMessage(serde_json::from_slice(&payload.data)?))
    }
}
