# ea-heart-client

Standalone Rust client library for the [Heart process engine](https://github.com/hbjoroy/heart) REST API.

## Features

- **HeartClient** — Typed HTTP client for Heart API (definitions, instances, queues, work items)
- **Receive-message API** — Correlate typed payloads through `POST /api/v1/messages`
- **WorkDispatcher** — Generic adaptive-polling work dispatcher
- **WorkHandler** trait — Implement per-queue handlers for your application
- **ExternalDispatcher** - Typed, bounded workers for leased Heart v2 external jobs

## Usage

```toml
[dependencies]
heart-client = { git = "https://github.com/hbjoroy/ea-heart-client", tag = "v0.1.0" }
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
cargo test --locked --all-features --all-targets
```

## Heart v2 external workers

Register each supported namespace, task type and contract version with a typed
handler and a capacity per worker process. Connect over HTTPS with the worker's
service bearer token. A literal loopback address can also use HTTP for local
testing or a colocated TLS proxy. `rustls` is enabled by default. To use the
platform TLS implementation, disable default features and enable `native-tls`.

Use `ExternalJobsClient::with_ca_bundle` for a Heart endpoint signed by a
private CA. Pass the HTTPS service URL whose DNS name or IP appears in the
server certificate, plus the PEM CA certificates the client should trust.
Both TLS backends verify the certificate and endpoint identity. A custom
bundle supplies the client's trusted roots. It can contain both roots during
a CA rotation.

```rust
use heart_client::v2::{
    ClaimScope, DispatchConfig, ExternalDispatcher, ExternalHandler, ExternalJobsClient,
    HandlerFailure, JobContext,
};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

struct AccountCheck;

#[derive(Deserialize)]
struct Input { account_id: String }

#[derive(Serialize)]
struct Decision { approved: bool }

#[async_trait::async_trait]
impl ExternalHandler for AccountCheck {
    type Input = Input;
    type Output = Decision;

    async fn handle(
        &self,
        input: Input,
        _job: JobContext,
    ) -> Result<Decision, HandlerFailure> {
        if input.account_id.is_empty() {
            return Err(HandlerFailure::permanent("invalid_account_id"));
        }
        Ok(Decision { approved: input.account_id != "blocked" })
    }
}

async fn run(
    base_url: &str,
    token: &str,
    shutdown: impl std::future::Future<Output = ()>,
) -> anyhow::Result<()> {
    let heart = ExternalJobsClient::new(base_url, token)?;
    ExternalDispatcher::new(heart, DispatchConfig::default())?
        .register(
            ClaimScope::new("payments", "account-check", 1),
            NonZeroUsize::MIN,
            AccountCheck,
        )?
        .run(shutdown)
        .await?;
    Ok(())
}
```

Each grant occupies one slot through its handler and receipt reconciliation.
Heart may deliver the same job again. Use `JobContext::job_id` as a downstream
idempotency key, scoped to the Heart installation when applicable. An attempt
can lose its lease even while an external request is running. Observe
`JobContext::lease_loss` and make the handler safe to cancel. Recover a
previously committed result on redelivery before repeating an external effect.

The dispatcher renews while a handler runs or a receipt is being reconciled.
It retries ambiguous reports with the original token, key and command, even
when a renewal subsequently says the lease is lost. To manage leases yourself,
use `ExternalJobsClient` directly and reuse the same `Completion` or `Failure`
when the response is uncertain. Store its original token, key and payload
securely if your application needs to reconcile after a restart.

When shutdown begins, the dispatcher stops polling and keeps renewing and
reporting active attempts for the configured grace period. The default
shutdown grace and command retry window are each 30 seconds. Heart can
redeliver unfinished work after expiry.

### Pilot

Run `cargo run --example external_worker_pilot` against a Heart installation
with external jobs enabled. Configure `HEART_API_URL`, `HEART_WORKER_TOKEN`,
`HEART_WORKER_NAMESPACE` and `HEART_PILOT_EFFECT_DIR`. The pilot defaults to task
type `account-check`, version 1 and capacity 1. Override its task type or
capacity with `HEART_WORKER_TASK_TYPE` or `HEART_PILOT_CAPACITY`.
Set `HEART_WORKER_CA_FILE` to the PEM bundle path if the service uses a private
CA.

Replicas share the same effect directory on a filesystem that supports hard
links. An attempt publishes one `<job_id>.json` per job and recovers the same
result on redelivery. Set `HEART_PILOT_PAUSE_AFTER_EFFECT_MS` on one replica to
exercise termination after publication. The pause occurs only on the attempt
that creates the result file.
