//! Generic work dispatcher — polls Heart queues and routes items to handlers.

use crate::{HeartClient, WorkItem};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info};

const POLL_MIN_MS: u64 = 200;
const POLL_MAX_MS: u64 = 2000;
const POLL_BACKOFF_FACTOR: f64 = 1.5;

/// Result from a work handler.
pub enum HandlerResult {
    /// Complete the work item with this metadata.
    Complete(serde_json::Value),
    /// Hold — awaiting external completion (gate, manual approval, etc.).
    Hold,
}

/// A handler for work items from a specific Heart queue.
#[async_trait::async_trait]
pub trait WorkHandler: Send + Sync {
    /// The queue name this handler services.
    fn queue(&self) -> &str;

    /// Process a work item. Return Complete to advance the process, Hold to wait.
    async fn handle(&self, item: &WorkItem, ctx: &WorkContext) -> anyhow::Result<HandlerResult>;
}

/// Shared context passed to all work handlers.
#[derive(Clone)]
pub struct WorkContext {
    pub heart: HeartClient,
    /// Extension data — consumers can downcast from Any to provide DB pools, HTTP clients, etc.
    pub extensions: Arc<dyn std::any::Any + Send + Sync>,
}

/// Work dispatcher — polls registered queues and routes to handlers.
pub struct WorkDispatcher {
    ctx: WorkContext,
    handlers: Vec<Arc<dyn WorkHandler>>,
}

impl WorkDispatcher {
    pub fn new(heart: HeartClient, extensions: Arc<dyn std::any::Any + Send + Sync>) -> Self {
        let ctx = WorkContext {
            heart: heart.clone(),
            extensions,
        };
        Self {
            ctx,
            handlers: Vec::new(),
        }
    }

    /// Register a work handler.
    pub fn register(mut self, handler: Arc<dyn WorkHandler>) -> Self {
        self.handlers.push(handler);
        self
    }

    /// Run the dispatcher loop (blocking — spawn in a tokio task).
    pub async fn run(self) {
        let queues: Vec<&str> = self.handlers.iter().map(|h| h.queue()).collect();
        info!(queues = ?queues, "work dispatcher started (adaptive polling)");

        let mut current_interval_ms = POLL_MIN_MS;

        loop {
            let mut found_work = false;

            for handler in &self.handlers {
                match self.poll_queue(handler.as_ref()).await {
                    Ok(true) => found_work = true,
                    Ok(false) => {}
                    Err(e) => {
                        error!(queue = handler.queue(), error = %e, "handler error");
                    }
                }
            }

            if found_work {
                current_interval_ms = POLL_MIN_MS;
            } else {
                current_interval_ms = ((current_interval_ms as f64) * POLL_BACKOFF_FACTOR) as u64;
                if current_interval_ms > POLL_MAX_MS {
                    current_interval_ms = POLL_MAX_MS;
                }
            }

            sleep(Duration::from_millis(current_interval_ms)).await;
        }
    }

    async fn poll_queue(&self, handler: &dyn WorkHandler) -> anyhow::Result<bool> {
        let item = match self.ctx.heart.pick_work_item(handler.queue()).await {
            Ok(Some(item)) => item,
            Ok(None) => return Ok(false),
            Err(e) => return Err(e.into()),
        };

        debug!(queue = handler.queue(), item_id = %item.id, "picked work item");

        match handler.handle(&item, &self.ctx).await? {
            HandlerResult::Complete(metadata) => {
                self.ctx.heart.complete_work_item(item.id, metadata).await?;
                debug!(queue = handler.queue(), item_id = %item.id, "completed work item");
            }
            HandlerResult::Hold => {
                debug!(queue = handler.queue(), item_id = %item.id, "holding work item");
            }
        }

        Ok(true)
    }
}
