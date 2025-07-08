use axum::extract::ws::{Message, Utf8Bytes};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Display;
use tracing::debug;

pub(super) trait WebsocketStreamExt {
    async fn next_text(&mut self) -> Option<Utf8Bytes>;

    async fn next_json<T>(&mut self) -> Option<T>
    where
        T: DeserializeOwned,
    {
        let text = self.next_text().await?;
        match serde_json::from_slice(text.as_bytes()) {
            Ok(value) => Some(value),
            Err(err) => {
                debug!("{}", err);
                None
            }
        }
    }
}

impl<S> WebsocketStreamExt for S
where
    S: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    async fn next_text(&mut self) -> Option<Utf8Bytes> {
        let msg = if let Some(msg) = self.next().await {
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    debug!("{}", err);
                    return None;
                }
            }
        } else {
            return None;
        };
        let msg_text = match msg.into_text() {
            Ok(text) => text,
            Err(err) => {
                debug!("{}", err);
                return None;
            }
        };
        Some(msg_text)
    }
}

pub(super) trait WebsocketSinkExt {
    async fn send_json<T: Serialize>(&mut self, value: &T) -> Option<()>;
}

impl<S> WebsocketSinkExt for S
where
    S: Sink<Message> + Unpin,
    S::Error: Display,
{
    async fn send_json<T: Serialize>(&mut self, value: &T) -> Option<()> {
        let json_str = match serde_json::to_string(value).into() {
            Ok(json_str) => json_str,
            Err(err) => {
                debug!("{}", err);
                return None;
            }
        };
        match self.send(Message::Text(json_str.into())).await {
            Ok(_) => Some(()),
            Err(err) => {
                debug!("{}", err);
                None
            }
        }
    }
}
