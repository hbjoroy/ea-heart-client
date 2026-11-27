use reqwest::{
    header::{HeaderValue, AUTHORIZATION, RETRY_AFTER},
    Client, Response, StatusCode, Url,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{fmt, net::IpAddr, time::Duration};
use uuid::Uuid;

const RESPONSE_LIMIT: usize = 512 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const JOBS_PATH: &str = "/api/v2/external-jobs";

pub type JsonObject = Map<String, Value>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClaimScope {
    pub namespace: String,
    pub task_type: String,
    pub contract_version: i32,
}

impl ClaimScope {
    pub fn new(namespace: impl Into<String>, task_type: impl Into<String>, version: i32) -> Self {
        Self {
            namespace: namespace.into(),
            task_type: task_type.into(),
            contract_version: version,
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        !self.namespace.is_empty() && !self.task_type.is_empty() && self.contract_version > 0
    }
}

/// Attempt-scoped Heart credential. Treat its raw UUID as a secret.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseToken(Uuid);

impl LeaseToken {
    /// Restore a securely stored token for reconciliation of a previous command.
    pub fn from_uuid(token: Uuid) -> Self {
        Self(token)
    }

    /// Obtain the raw secret for secure command storage.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for LeaseToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LeaseToken([redacted])")
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct LeaseTiming {
    /// Informational RFC3339 timestamp from Heart. Scheduling uses the relative TTL.
    pub lease_expires_at: String,
    /// Remaining validity measured by Heart against its database clock.
    pub lease_ttl_ms: u64,
    pub renew_after_ms: u64,
}

#[derive(Deserialize)]
pub struct JobGrant {
    pub job_id: Uuid,
    #[serde(flatten)]
    pub scope: ClaimScope,
    pub input: JsonObject,
    pub attempt: u32,
    pub lease_token: LeaseToken,
    #[serde(flatten)]
    pub timing: LeaseTiming,
}

impl fmt::Debug for JobGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobGrant")
            .field("job_id", &self.job_id)
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Retryable,
    Permanent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Leased,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Completed,
    Failed,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
pub struct Receipt {
    pub job_id: Uuid,
    pub attempt: u32,
    pub idempotency_key: String,
    pub outcome: Outcome,
    pub job_state: JobState,
}

impl fmt::Debug for Receipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receipt")
            .field("job_id", &self.job_id)
            .field("attempt", &self.attempt)
            .field("outcome", &self.outcome)
            .field("job_state", &self.job_state)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerErrorCode {
    BadRequest,
    PayloadTooLarge,
    Unauthorized,
    ScopeDenied,
    JobNotFound,
    Throttled,
    LeaseLost,
    IdempotencyConflict,
    Internal,
    RuntimeUnavailable,
    #[serde(other)]
    Unknown,
}

/// Errors omit credential material, request/response payloads, and URLs.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("invalid Heart worker API endpoint")]
    InvalidEndpoint,
    #[error("invalid Heart worker credentials")]
    InvalidCredentials,
    #[error("invalid Heart worker CA bundle")]
    InvalidCaBundle,
    #[error("invalid Heart worker client certificate or key")]
    InvalidClientIdentity,
    #[error("the Heart worker client was built without a TLS feature")]
    TlsUnavailable,
    #[error("failed to initialize the Heart worker HTTP client")]
    ClientInitialization,
    #[error("invalid external job subscription")]
    InvalidScope,
    #[error("invalid idempotency key")]
    InvalidKey,
    #[error("invalid external job failure code")]
    InvalidFailureCode,
    #[error("Heart external job request failed in transport")]
    Transport,
    #[error("Heart external job response did not match the protocol")]
    InvalidResponse,
    #[error("Heart external job request returned HTTP {status} ({code:?})")]
    Rejected {
        status: u16,
        code: WorkerErrorCode,
        retry_after: Option<Duration>,
    },
}

impl WorkerError {
    /// Whether repeating an identical command can still reconcile its outcome.
    pub fn should_retry(&self) -> bool {
        matches!(
            self,
            Self::Transport
                | Self::InvalidResponse
                | Self::Rejected {
                    status: 408 | 429 | 500..=599,
                    ..
                }
        )
    }

    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Rejected { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// An immutable completion command. Reuse this object on an unknown response.
pub struct Completion {
    job_id: Uuid,
    key: HeaderValue,
    body: CompletionBody,
}

#[derive(Serialize)]
struct CompletionBody {
    lease_token: LeaseToken,
    result: JsonObject,
}

impl Completion {
    pub fn new(
        job_id: Uuid,
        lease_token: LeaseToken,
        idempotency_key: &str,
        result: JsonObject,
    ) -> Result<Self, WorkerError> {
        Ok(Self {
            job_id,
            key: idempotency_key_header(idempotency_key)?,
            body: CompletionBody {
                lease_token,
                result,
            },
        })
    }
}

/// An immutable failure command. Reuse this object on an unknown response.
pub struct Failure {
    job_id: Uuid,
    key: HeaderValue,
    body: FailureBody,
}

#[derive(Serialize)]
struct FailureBody {
    lease_token: LeaseToken,
    kind: FailureKind,
    code: String,
}

impl Failure {
    pub fn new(
        job_id: Uuid,
        lease_token: LeaseToken,
        idempotency_key: &str,
        kind: FailureKind,
        code: impl Into<String>,
    ) -> Result<Self, WorkerError> {
        let code = code.into();
        if !valid_failure_code(&code) {
            return Err(WorkerError::InvalidFailureCode);
        }
        Ok(Self {
            job_id,
            key: idempotency_key_header(idempotency_key)?,
            body: FailureBody {
                lease_token,
                kind,
                code,
            },
        })
    }
}

pub(crate) fn valid_failure_code(code: &str) -> bool {
    (1..=64).contains(&code.len())
        && code.as_bytes()[0].is_ascii_lowercase()
        && code.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn authorization_header(bearer_token: &str) -> Result<HeaderValue, WorkerError> {
    if !(32..=256).contains(&bearer_token.len())
        || !bearer_token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(WorkerError::InvalidCredentials);
    }
    let mut authorization = HeaderValue::from_str(&format!("Bearer {bearer_token}"))
        .map_err(|_| WorkerError::InvalidCredentials)?;
    authorization.set_sensitive(true);
    Ok(authorization)
}

/// Trust only the given PEM roots for the Heart worker endpoint.
#[cfg(any(feature = "rustls", feature = "native-tls"))]
fn with_roots(
    mut builder: reqwest::ClientBuilder,
    pem: &[u8],
) -> Result<reqwest::ClientBuilder, WorkerError> {
    let roots =
        reqwest::Certificate::from_pem_bundle(pem).map_err(|_| WorkerError::InvalidCaBundle)?;
    if roots.is_empty() {
        return Err(WorkerError::InvalidCaBundle);
    }
    builder = builder.tls_built_in_root_certs(false);
    for root in roots {
        builder = builder.add_root_certificate(root);
    }
    Ok(builder)
}

/// rustls configuration for mutual TLS: only the given roots are trusted, and
/// the client certificate comes from a (reloading) resolver.
#[cfg(feature = "rustls")]
fn mutual_tls_config(
    pem: &[u8],
    identity: super::ClientIdentity,
) -> Result<rustls::ClientConfig, WorkerError> {
    use rustls::pki_types::{pem::PemObject, CertificateDer};
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(pem) {
        roots
            .add(certificate.map_err(|_| WorkerError::InvalidCaBundle)?)
            .map_err(|_| WorkerError::InvalidCaBundle)?;
    }
    if roots.is_empty() {
        return Err(WorkerError::InvalidCaBundle);
    }
    let resolver = identity.resolver(provider.clone())?;
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| WorkerError::ClientInitialization)?
        .with_root_certificates(roots)
        .with_client_cert_resolver(resolver);
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    // A resumed session would not send the (possibly renewed) client
    // certificate; always perform a full handshake.
    config.resumption = rustls::client::Resumption::disabled();
    Ok(config)
}

fn idempotency_key_header(key: &str) -> Result<HeaderValue, WorkerError> {
    if !(1..=128).contains(&key.len()) || !key.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(WorkerError::InvalidKey);
    }
    let mut header = HeaderValue::from_str(key).map_err(|_| WorkerError::InvalidKey)?;
    header.set_sensitive(true);
    Ok(header)
}

/// Authenticated client for external job operations.
#[derive(Clone)]
pub struct ExternalJobsClient {
    http: Client,
    base_url: String,
    /// Absent when the client authenticates by certificate (mutual TLS).
    authorization: Option<HeaderValue>,
}

/// How the worker proves its identity to Heart.
enum Authentication<'a> {
    Token(&'a str),
    #[cfg(feature = "rustls")]
    Certificate(super::ClientIdentity),
}

impl ExternalJobsClient {
    /// Create a worker client. HTTPS is required except for a loopback HTTP endpoint.
    pub fn new(base_url: &str, bearer_token: &str) -> Result<Self, WorkerError> {
        Self::build(base_url, Authentication::Token(bearer_token), None)
    }

    /// Create an HTTPS worker client that trusts the supplied PEM CA certificates.
    ///
    /// The bundle supplies the trusted roots for this client. TLS still verifies
    /// the server certificate and that its identity matches the endpoint.
    pub fn with_ca_bundle(
        base_url: &str,
        bearer_token: &str,
        pem: &[u8],
    ) -> Result<Self, WorkerError> {
        Self::build(base_url, Authentication::Token(bearer_token), Some(pem))
    }

    /// Create an HTTPS worker client that authenticates with a client
    /// certificate (mutual TLS) and trusts the supplied PEM CA certificates.
    ///
    /// Heart maps the certificate's SPIFFE ID to the worker principal, so no
    /// bearer token is sent. With [`super::ClientIdentity::from_files`] a
    /// renewed certificate is picked up for new connections automatically.
    #[cfg(feature = "rustls")]
    pub fn with_client_identity(
        base_url: &str,
        ca_bundle: &[u8],
        identity: super::ClientIdentity,
    ) -> Result<Self, WorkerError> {
        Self::build(
            base_url,
            Authentication::Certificate(identity),
            Some(ca_bundle),
        )
    }

    fn build(
        base_url: &str,
        authentication: Authentication<'_>,
        ca_bundle: Option<&[u8]>,
    ) -> Result<Self, WorkerError> {
        let url = Url::parse(base_url).map_err(|_| WorkerError::InvalidEndpoint)?;
        let loopback = url
            .host_str()
            .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
        if (url.scheme() != "https" && !(url.scheme() == "http" && loopback && ca_bundle.is_none()))
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(WorkerError::InvalidEndpoint);
        }
        let authorization = match &authentication {
            Authentication::Token(bearer_token) => Some(authorization_header(bearer_token)?),
            #[cfg(feature = "rustls")]
            Authentication::Certificate(_) => {
                // A client certificate is only meaningful over TLS.
                if url.scheme() != "https" {
                    return Err(WorkerError::InvalidEndpoint);
                }
                None
            }
        };
        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(REQUEST_TIMEOUT);
        if loopback {
            builder = builder.no_proxy();
        }
        #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
        if url.scheme() == "https" {
            return Err(WorkerError::TlsUnavailable);
        }
        #[cfg(feature = "rustls")]
        if let Authentication::Certificate(identity) = authentication {
            builder = builder.use_preconfigured_tls(mutual_tls_config(
                ca_bundle.ok_or(WorkerError::InvalidCaBundle)?,
                identity,
            )?);
        } else if let Some(pem) = ca_bundle {
            builder = with_roots(builder, pem)?;
        }
        #[cfg(all(feature = "native-tls", not(feature = "rustls")))]
        if let Some(pem) = ca_bundle {
            builder = with_roots(builder, pem)?;
        }
        let http = builder
            .build()
            .map_err(|_| WorkerError::ClientInitialization)?;
        Ok(Self {
            http,
            base_url: url.as_str().trim_end_matches('/').to_string(),
            authorization,
        })
    }

    /// Request one job for an exact namespace, type and version.
    pub async fn claim(&self, scope: &ClaimScope) -> Result<Option<JobGrant>, WorkerError> {
        if !scope.is_valid() {
            return Err(WorkerError::InvalidScope);
        }
        let response = self.post("/claim", None, scope).await?;
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        let grant: JobGrant = decode(expect_ok(response).await?).await?;
        if &grant.scope != scope || grant.attempt == 0 {
            return Err(WorkerError::InvalidResponse);
        }
        Ok(Some(grant))
    }

    /// Extend a live grant.
    pub async fn renew(
        &self,
        job_id: Uuid,
        lease_token: LeaseToken,
    ) -> Result<LeaseTiming, WorkerError> {
        #[derive(Serialize)]
        struct Renewal {
            lease_token: LeaseToken,
        }
        let path = format!("/{job_id}/renew");
        let response = self.post(&path, None, &Renewal { lease_token }).await?;
        decode(expect_ok(response).await?).await
    }

    pub async fn complete(&self, command: &Completion) -> Result<Receipt, WorkerError> {
        self.command(
            command.job_id,
            &command.key,
            &command.body,
            "complete",
            Outcome::Completed,
        )
        .await
    }

    pub async fn fail(&self, command: &Failure) -> Result<Receipt, WorkerError> {
        self.command(
            command.job_id,
            &command.key,
            &command.body,
            "fail",
            Outcome::Failed,
        )
        .await
    }

    async fn command(
        &self,
        job_id: Uuid,
        key: &HeaderValue,
        body: &impl Serialize,
        operation: &str,
        outcome: Outcome,
    ) -> Result<Receipt, WorkerError> {
        let path = format!("/{job_id}/{operation}");
        let response = self.post(&path, Some(key), body).await?;
        let receipt: Receipt = decode(expect_ok(response).await?).await?;
        if receipt.job_id != job_id
            || receipt.attempt == 0
            || receipt.idempotency_key.as_bytes() != key.as_bytes()
            || receipt.outcome != outcome
        {
            return Err(WorkerError::InvalidResponse);
        }
        Ok(receipt)
    }

    async fn post(
        &self,
        path: &str,
        key: Option<&HeaderValue>,
        body: &impl Serialize,
    ) -> Result<Response, WorkerError> {
        let mut request = self
            .http
            .post(format!("{}{JOBS_PATH}{path}", self.base_url));
        if let Some(authorization) = &self.authorization {
            request = request.header(AUTHORIZATION, authorization.clone());
        }
        if let Some(key) = key {
            request = request.header("Idempotency-Key", key.clone());
        }
        request
            .json(body)
            .send()
            .await
            .map_err(|_| WorkerError::Transport)
    }
}

async fn expect_ok(response: Response) -> Result<Response, WorkerError> {
    let status = response.status();
    if status == StatusCode::OK {
        return Ok(response);
    }
    if status.is_success() {
        return Err(WorkerError::InvalidResponse);
    }
    let retry_after = response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok())
        .map(Duration::from_secs);
    #[derive(Deserialize)]
    struct ErrorBody {
        code: WorkerErrorCode,
    }
    let code = decode::<ErrorBody>(response)
        .await
        .map_or(WorkerErrorCode::Unknown, |body| body.code);
    Err(WorkerError::Rejected {
        status: status.as_u16(),
        code,
        retry_after,
    })
}

async fn decode<T: DeserializeOwned>(mut response: Response) -> Result<T, WorkerError> {
    if response
        .content_length()
        .is_some_and(|length| length > RESPONSE_LIMIT as u64)
    {
        return Err(WorkerError::InvalidResponse);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| WorkerError::Transport)? {
        if chunk.len() > RESPONSE_LIMIT - body.len() {
            return Err(WorkerError::InvalidResponse);
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| WorkerError::InvalidResponse)
}
