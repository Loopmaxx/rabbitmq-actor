#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

mod actor;
mod client;
mod config;
mod error;
mod messages;
mod reconnect;
mod topology;
mod traits;

pub use client::AmqpClient;
pub use config::*;
pub use error::{AmqpError, Result, TopologyObject, TransportError};
pub use reconnect::ConnectionState;
pub use traits::{
    HandlerResult, HeaderValue, IncomingMessage, MessageHandler, MessageMeta, MessageProperties,
    MessagePublisher, OutgoingMessage, Publishable, TypedMessageHandler,
};
