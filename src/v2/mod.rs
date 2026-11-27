//! Authenticated Heart v2 external jobs and their leased worker dispatcher.

mod client;
mod dispatcher;
#[cfg(feature = "rustls")]
mod identity;

pub use client::{
    ClaimScope, Completion, ExternalJobsClient, Failure, FailureKind, JobGrant, JobState,
    JsonObject, LeaseTiming, LeaseToken, Outcome, Receipt, WorkerError, WorkerErrorCode,
};
pub use dispatcher::{
    DispatchConfig, DispatchError, ExternalDispatcher, ExternalHandler, HandlerFailure, JobContext,
    LeaseLoss, LeaseLossReason,
};
#[cfg(feature = "rustls")]
pub use identity::ClientIdentity;
