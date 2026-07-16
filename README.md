# ea-heart-client

Standalone Rust client library for the [Heart process engine](https://github.com/SparebankenVest/ea-bank-core) REST API.

## Features

- **HeartClient** — Typed HTTP client for Heart API (definitions, instances, queues, work items)
- **Receive-message API** — Correlate typed payloads through `POST /api/v1/messages`
- **WorkDispatcher** — Generic adaptive-polling work dispatcher
- **WorkHandler** trait — Implement per-queue handlers for your application

## Usage

```toml
[dependencies]
heart-client = { git = "https://github.com/SparebankenVest/ea-heart-client", tag = "v0.1.0" }
```

```rust
use heart_client::{HeartClient, WorkDispatcher, WorkHandler, WorkContext, HandlerResult, WorkItem};
use std::sync::Arc;

struct MyHandler;

#[async_trait::async_trait]
impl WorkHandler for MyHandler {
    fn queue(&self) -> &str { "my-queue" }

    async fn handle(&self, item: &WorkItem, ctx: &WorkContext) -> anyhow::Result<HandlerResult> {
        // Process the work item...
        Ok(HandlerResult::Complete(serde_json::json!({"status": "done"})))
    }
}

#[tokio::main]
async fn main() {
    let heart = HeartClient::from_env().expect("HEART_API_URL must be set");
    let extensions: Arc<dyn std::any::Any + Send + Sync> = Arc::new(());

    let dispatcher = WorkDispatcher::new(heart, extensions)
        .register(Arc::new(MyHandler));

    dispatcher.run().await;
}
```

## Environment Variables

- `HEART_API_URL` — Heart API base URL (e.g., `http://heart:3000`)
- `HEART_NAMESPACE` — Default namespace (default: `"default"`)

## Receive-message contract

Use `send_message` for the configured default namespace or
`send_message_in_namespace` for an explicit namespace. Both return
`SendMessageResponse`, including matched instance IDs and any per-instance run
results. Non-success responses retain the HTTP status but deliberately discard
the provider response body so tokens and process payloads cannot leak through
client errors.

Run the pinned quality gate locally with:

```bash
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
```
