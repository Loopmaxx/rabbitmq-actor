use std::{
    collections::HashMap,
    panic::AssertUnwindSafe,
    sync::{Arc, RwLock},
};

use futures_util::FutureExt;

use crate::{HandlerResult, IncomingMessage, MessageHandler};

/// Handlers per queue. Shared between the consumer actor and its consumer tasks.
#[derive(Default)]
pub(crate) struct HandlerRegistry {
    handlers: RwLock<HashMap<String, Vec<Arc<dyn MessageHandler>>>>,
}

impl HandlerRegistry {
    pub(crate) fn register(&self, queue: &str, handler: Arc<dyn MessageHandler>) {
        self.handlers
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .entry(queue.to_owned())
            .or_default()
            .push(handler);
    }

    pub(crate) fn has_handlers(&self, queue: &str) -> bool {
        self.handlers_for(queue).is_some()
    }

    fn handlers_for(&self, queue: &str) -> Option<Vec<Arc<dyn MessageHandler>>> {
        self.handlers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(queue)
            .filter(|h| !h.is_empty())
            .cloned()
    }

    pub(crate) fn queues(&self) -> Vec<String> {
        self.handlers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    /// Runs all handlers of the queue concurrently and combines their results.
    /// A panicking handler counts as [`HandlerResult::Requeue`].
    pub(crate) async fn dispatch(&self, msg: &IncomingMessage) -> HandlerResult {
        let Some(handlers) = self.handlers_for(&msg.meta.queue) else {
            return HandlerResult::Requeue;
        };
        let results = futures_util::future::join_all(handlers.iter().map(|handler| async move {
            match AssertUnwindSafe(handler.handle(msg)).catch_unwind().await {
                Ok(result) => result,
                Err(_) => {
                    tracing::error!(queue = %msg.meta.queue, "message handler panicked");
                    HandlerResult::Requeue
                }
            }
        }))
        .await;
        results
            .into_iter()
            .fold(HandlerResult::Ack, HandlerResult::combine)
    }
}
