use super::{
    client::valid_failure_code, ClaimScope, Completion, ExternalJobsClient, Failure, FailureKind,
    JobGrant, JsonObject, LeaseTiming, LeaseToken, Receipt, WorkerError, WorkerErrorCode,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::{JoinError, JoinSet},
    time::{sleep_until, timeout, timeout_at, Instant},
};
use tracing::{debug, warn};
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub struct DispatchConfig {
    pub poll_min: Duration,
    pub poll_max: Duration,
    /// Maximum elapsed time for one completion or failure, including exact replays.
    pub command_retry_window: Duration,
    /// Active attempts can still renew and report while the dispatcher drains.
    pub shutdown_grace: Duration,
    /// A handler can observe lease loss and finish cancellation within this period.
    pub lease_loss_grace: Duration,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            poll_min: Duration::from_millis(200),
            poll_max: Duration::from_secs(5),
            command_retry_window: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(30),
            lease_loss_grace: Duration::from_millis(250),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("invalid Heart worker dispatcher timing configuration")]
    InvalidConfiguration,
    #[error("no Heart worker subscriptions were registered")]
    NoSubscriptions,
    #[error("a Heart worker subscription was registered more than once")]
    DuplicateSubscription,
    #[error("Heart worker subscription stopped: {0}")]
    Subscription(#[from] WorkerError),
    #[error("Heart worker subscription task stopped unexpectedly")]
    SubscriptionTask,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseLossReason {
    /// Heart rejected the grant as lost or inaccessible.
    Rejected,
    /// The dispatcher could not confirm that the grant is still valid.
    Unconfirmed,
    /// The task owning the grant stopped.
    WorkerStopped,
}

#[derive(Clone, Debug)]
pub struct LeaseLoss(watch::Receiver<Option<LeaseLossReason>>);

impl LeaseLoss {
    pub fn reason(&self) -> Option<LeaseLossReason> {
        (*self.0.borrow()).or_else(|| {
            self.0
                .has_changed()
                .is_err()
                .then_some(LeaseLossReason::WorkerStopped)
        })
    }

    pub async fn wait(&mut self) -> LeaseLossReason {
        loop {
            if let Some(reason) = self.reason() {
                return reason;
            }
            if self.0.changed().await.is_err() {
                return self.reason().unwrap_or(LeaseLossReason::WorkerStopped);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct JobContext {
    /// Stable across deliveries. Use this as the downstream idempotency key.
    pub job_id: Uuid,
    pub attempt: u32,
    pub scope: ClaimScope,
    pub lease_loss: LeaseLoss,
}

/// A classified failure with a short, static diagnostic code.
pub struct HandlerFailure {
    kind: FailureKind,
    code: &'static str,
}

impl HandlerFailure {
    pub fn retryable(code: &'static str) -> Self {
        Self::new(FailureKind::Retryable, code)
    }

    pub fn permanent(code: &'static str) -> Self {
        Self::new(FailureKind::Permanent, code)
    }

    fn new(kind: FailureKind, code: &'static str) -> Self {
        if valid_failure_code(code) {
            Self { kind, code }
        } else {
            Self {
                kind: FailureKind::Permanent,
                code: "invalid_failure_code",
            }
        }
    }
}

/// Application handler for a versioned external job. Input and output are JSON objects.
#[async_trait::async_trait]
pub trait ExternalHandler: Send + Sync + 'static {
    type Input: DeserializeOwned + Send;
    type Output: Serialize + Send;

    /// Reconcile effects by stable job ID on redelivery. The future can be cancelled
    /// on lease loss or after the shutdown grace period.
    async fn handle(
        &self,
        input: Self::Input,
        context: JobContext,
    ) -> Result<Self::Output, HandlerFailure>;
}

#[async_trait::async_trait]
trait ErasedHandler: Send + Sync {
    async fn handle(
        &self,
        input: JsonObject,
        context: JobContext,
    ) -> Result<JsonObject, HandlerFailure>;
}

#[async_trait::async_trait]
impl<H: ExternalHandler> ErasedHandler for H {
    async fn handle(
        &self,
        input: JsonObject,
        context: JobContext,
    ) -> Result<JsonObject, HandlerFailure> {
        let input = serde_json::from_value(Value::Object(input))
            .map_err(|_| HandlerFailure::permanent("invalid_input"))?;
        let output = ExternalHandler::handle(self, input, context).await?;
        match serde_json::to_value(output) {
            Ok(Value::Object(object)) => Ok(object),
            _ => Err(HandlerFailure::permanent("invalid_output")),
        }
    }
}

struct Subscription {
    scope: ClaimScope,
    capacity: NonZeroUsize,
    handler: Arc<dyn ErasedHandler>,
}

/// Polls each subscription independently, using at most its configured number of grants.
pub struct ExternalDispatcher {
    client: ExternalJobsClient,
    config: DispatchConfig,
    subscriptions: Vec<Subscription>,
}

impl ExternalDispatcher {
    pub fn new(client: ExternalJobsClient, config: DispatchConfig) -> Result<Self, DispatchError> {
        let now = Instant::now();
        if config.poll_min.is_zero()
            || config.poll_max < config.poll_min
            || config.command_retry_window.is_zero()
            || [
                config.poll_max,
                config.command_retry_window,
                config.shutdown_grace,
                config.lease_loss_grace,
            ]
            .into_iter()
            .any(|duration| now.checked_add(duration).is_none())
        {
            return Err(DispatchError::InvalidConfiguration);
        }
        Ok(Self {
            client,
            config,
            subscriptions: Vec::new(),
        })
    }

    pub fn register(
        mut self,
        scope: ClaimScope,
        capacity: NonZeroUsize,
        handler: impl ExternalHandler,
    ) -> Result<Self, DispatchError> {
        if !scope.is_valid() {
            return Err(WorkerError::InvalidScope.into());
        }
        if self.subscriptions.iter().any(|entry| entry.scope == scope) {
            return Err(DispatchError::DuplicateSubscription);
        }
        self.subscriptions.push(Subscription {
            scope,
            capacity,
            handler: Arc::new(handler),
        });
        Ok(self)
    }

    /// Stop claiming when shutdown resolves, then allow active attempts to drain.
    pub async fn run(self, shutdown: impl Future<Output = ()>) -> Result<(), DispatchError> {
        if self.subscriptions.is_empty() {
            return Err(DispatchError::NoSubscriptions);
        }
        let (stop, signal) = watch::channel(false);
        let (failures, mut failure) = mpsc::channel(1);
        let mut tasks = JoinSet::new();
        for subscription in self.subscriptions {
            tasks.spawn(poll_subscription(
                self.client.clone(),
                self.config,
                subscription,
                signal.clone(),
                failures.clone(),
            ));
        }
        let outcome = tokio::select! {
            biased;
            _ = shutdown => Ok(()),
            error = failure.recv() => Err(error.expect("supervisor owns the sender").into()),
            _ = tasks.join_next() => Err(DispatchError::SubscriptionTask),
        };
        stop.send_replace(true);
        if timeout(self.config.shutdown_grace, async {
            while tasks.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            // Dropping the subscription's JoinSet cancels all of its attempt futures.
            tasks.shutdown().await;
        }
        outcome
    }
}

async fn poll_subscription(
    client: ExternalJobsClient,
    config: DispatchConfig,
    subscription: Subscription,
    mut shutdown: watch::Receiver<bool>,
    failure: mpsc::Sender<WorkerError>,
) {
    let mut jobs = JoinSet::new();
    let mut backoff = Backoff::new(config.poll_min, config.poll_max);
    let mut next_poll = after(jitter(config.poll_min));
    while !*shutdown.borrow_and_update() {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            joined = jobs.join_next(), if !jobs.is_empty() => log_task_failure(joined),
            _ = sleep_until(next_poll), if jobs.len() < subscription.capacity.get() => {
                let requested_at = Instant::now();
                let response = tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    response = client.claim(&subscription.scope) => response,
                };
                match response {
                    Ok(Some(grant)) => {
                        if *shutdown.borrow() {
                            break;
                        }
                        backoff.reset();
                        next_poll = Instant::now();
                        jobs.spawn(run_job(client.clone(), config, subscription.handler.clone(), grant, requested_at));
                    }
                    Ok(None) => next_poll = after(backoff.next(None)),
                    Err(error) if error.should_retry() => {
                        next_poll = after(backoff.next(error.retry_after()));
                        warn!(%error, "Heart external job claim will be retried");
                    }
                    Err(error) => {
                        let _ = failure.try_send(error);
                        break;
                    }
                }
            }
        }
    }
    while !jobs.is_empty() {
        log_task_failure(jobs.join_next().await);
    }
}

fn log_task_failure(joined: Option<Result<(), JoinError>>) {
    if joined.is_some_and(|result| result.is_err()) {
        warn!("Heart external job handler task stopped unexpectedly");
    }
}

async fn run_job(
    client: ExternalJobsClient,
    config: DispatchConfig,
    handler: Arc<dyn ErasedHandler>,
    grant: JobGrant,
    requested_at: Instant,
) {
    let JobGrant {
        job_id,
        scope,
        input,
        attempt,
        lease_token,
        timing,
    } = grant;
    let Some(window) = LeaseWindow::new(requested_at, timing) else {
        warn!(%job_id, attempt, "Heart external job arrived without a usable lease");
        return;
    };
    let (lost, receiver) = watch::channel(None);
    let context = JobContext {
        job_id,
        attempt,
        scope,
        lease_loss: LeaseLoss(receiver),
    };
    let mut renewal = std::pin::pin!(maintain_lease(&client, job_id, lease_token, window));
    let result = {
        let mut work = std::pin::pin!(handler.handle(input, context));
        tokio::select! {
            biased;
            reason = &mut renewal => {
                lost.send_replace(Some(reason));
                warn!(%job_id, attempt, ?reason, "Heart external job lease lost during handling");
                let _ = timeout(config.lease_loss_grace, work).await;
                return;
            },
            result = &mut work => result,
        }
    };

    let mut reporting = std::pin::pin!(report(
        &client,
        config,
        job_id,
        attempt,
        lease_token,
        result
    ));
    let report_result = tokio::select! {
        biased;
        result = &mut reporting => result,
        reason = &mut renewal => {
            lost.send_replace(Some(reason));
            // An accepted command may itself have ended the lease. Replay the
            // original command even after renewal fails, to obtain its receipt.
            reporting.await
        }
    };
    match report_result {
        Ok(receipt) => {
            debug!(%job_id, attempt, state = ?receipt.job_state, "Heart external job receipt accepted")
        }
        Err(error) => warn!(%job_id, attempt, %error, "Heart external job receipt unavailable"),
    }
}

#[derive(Debug, thiserror::Error)]
enum ReportError {
    #[error(transparent)]
    Request(#[from] WorkerError),
    #[error("Heart external job reconciliation timed out")]
    Timeout,
}

enum Command {
    Complete(Completion),
    Fail(Failure),
}

async fn report(
    client: &ExternalJobsClient,
    config: DispatchConfig,
    job_id: Uuid,
    attempt: u32,
    lease_token: LeaseToken,
    result: Result<JsonObject, HandlerFailure>,
) -> Result<Receipt, ReportError> {
    let key = Uuid::new_v4().to_string();
    let command = match result {
        Ok(result) => Command::Complete(Completion::new(job_id, lease_token, &key, result)?),
        Err(failure) => Command::Fail(Failure::new(
            job_id,
            lease_token,
            &key,
            failure.kind,
            failure.code,
        )?),
    };
    let mut backoff = Backoff::new(Duration::from_millis(200), Duration::from_secs(2));
    let receipt = timeout(config.command_retry_window, async {
        loop {
            let reply = match &command {
                Command::Complete(command) => client.complete(command).await,
                Command::Fail(command) => client.fail(command).await,
            };
            let reply = reply.and_then(|receipt| {
                if receipt.attempt == attempt {
                    Ok(receipt)
                } else {
                    Err(WorkerError::InvalidResponse)
                }
            });
            match reply {
                Err(error) if error.should_retry() => {
                    sleep_until(after(backoff.next(error.retry_after()))).await;
                }
                done => return done,
            }
        }
    })
    .await
    .map_err(|_| ReportError::Timeout)??;
    Ok(receipt)
}

struct LeaseWindow {
    renew_at: Instant,
    deadline: Instant,
}

impl LeaseWindow {
    fn new(requested_at: Instant, timing: LeaseTiming) -> Option<Self> {
        let ttl = Duration::from_millis(timing.lease_ttl_ms);
        let until_renewal = Duration::from_millis(timing.renew_after_ms).min(ttl / 2);
        let deadline = requested_at.checked_add(ttl)?;
        if until_renewal.is_zero() || deadline <= Instant::now() {
            return None;
        }
        Some(Self {
            renew_at: requested_at + until_renewal,
            deadline,
        })
    }
}

async fn maintain_lease(
    client: &ExternalJobsClient,
    job_id: Uuid,
    token: LeaseToken,
    mut window: LeaseWindow,
) -> LeaseLossReason {
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(1));
    loop {
        sleep_until(window.renew_at.min(window.deadline)).await;
        let requested_at = Instant::now();
        if requested_at >= window.deadline {
            return LeaseLossReason::Unconfirmed;
        }
        match timeout_at(window.deadline, client.renew(job_id, token)).await {
            Ok(Ok(timing)) if Instant::now() < window.deadline => {
                let Some(renewed) = LeaseWindow::new(requested_at, timing) else {
                    return LeaseLossReason::Unconfirmed;
                };
                window = renewed;
                backoff.reset();
            }
            Ok(Err(error)) if error.should_retry() => {
                window.renew_at = after(backoff.next(error.retry_after()));
            }
            Ok(Err(WorkerError::Rejected { status: 404, .. }))
            | Ok(Err(WorkerError::Rejected {
                status: 409,
                code: WorkerErrorCode::LeaseLost,
                ..
            })) => {
                return LeaseLossReason::Rejected;
            }
            _ => return LeaseLossReason::Unconfirmed,
        }
    }
}

struct Backoff {
    current: Duration,
    min: Duration,
    max: Duration,
}

impl Backoff {
    fn new(min: Duration, max: Duration) -> Self {
        Self {
            current: min,
            min,
            max,
        }
    }

    fn reset(&mut self) {
        self.current = self.min;
    }

    fn next(&mut self, retry_after: Option<Duration>) -> Duration {
        let delay = retry_after
            .unwrap_or_default()
            .saturating_add(jitter(self.current));
        self.current = self.current.saturating_mul(2).min(self.max);
        delay
    }
}

fn jitter(duration: Duration) -> Duration {
    duration.mul_f64(0.5 + fastrand::f64() * 0.5)
}

fn after(delay: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(delay)
        .unwrap_or_else(|| now + Duration::from_secs(100 * 365 * 24 * 60 * 60))
}
