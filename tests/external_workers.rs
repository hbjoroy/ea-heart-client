use heart_client::v2::{
    ClaimScope, Completion, DispatchConfig, ExternalDispatcher, ExternalHandler,
    ExternalJobsClient, Failure, FailureKind, HandlerFailure, JobContext, JobState,
    LeaseLossReason, Outcome, WorkerError, WorkerErrorCode,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    future::{pending, Future},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot, Semaphore},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;

const BEARER: &str = "fixture_service_token_0123456789";
const INPUT: &str = "account_fixture_secret";
const BASE: &str = "/api/v2/external-jobs";
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct Request {
    line: String,
    headers: HashMap<String, String>,
    raw_body: Vec<u8>,
    response: oneshot::Sender<Reply>,
}

impl Request {
    fn body(&self) -> Value {
        serde_json::from_slice(&self.raw_body).unwrap()
    }

    fn assert_path(&self, path: &str) {
        assert_eq!(self.line, format!("POST {BASE}{path} HTTP/1.1"));
        assert_eq!(self.headers["authorization"], format!("Bearer {BEARER}"));
        assert_eq!(self.headers["content-type"], "application/json");
    }

    fn reply(self, status: u16, body: Value) {
        self.send(status, body, "");
    }

    fn send(self, status: u16, body: Value, headers: &'static str) {
        self.response
            .send(Reply {
                status,
                body,
                headers,
            })
            .ok()
            .unwrap();
    }
}

struct Reply {
    status: u16,
    body: Value,
    headers: &'static str,
}

struct Server {
    address: String,
    requests: mpsc::Receiver<Request>,
    task: JoinHandle<()>,
}

impl Server {
    async fn start() -> Self {
        Self::listen(None).await
    }

    async fn listen(tls: Option<TlsAcceptor>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let scheme = if tls.is_some() { "https" } else { "http" };
        let address = format!("{scheme}://{}", listener.local_addr().unwrap());
        let (sender, requests) = mpsc::channel(16);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (stream, _) = result.unwrap();
                        let (sender, tls) = (sender.clone(), tls.clone());
                        connections.spawn(async move {
                            if let Some(acceptor) = tls {
                                if let Ok(stream) = acceptor.accept(stream).await {
                                    serve_request(stream, sender).await;
                                }
                            } else {
                                serve_request(stream, sender).await;
                            }
                        });
                    }
                    result = connections.join_next(), if !connections.is_empty() => {
                        result.unwrap().unwrap();
                    }
                }
            }
        });
        Self {
            address,
            requests,
            task,
        }
    }

    fn client(&self) -> ExternalJobsClient {
        ExternalJobsClient::new(&self.address, BEARER).unwrap()
    }

    async fn next(&mut self, path: &str) -> Request {
        let request = within(self.requests.recv()).await.unwrap();
        request.assert_path(path);
        request
    }

    async fn quiet_for(&mut self, duration: Duration) {
        assert!(timeout(duration, self.requests.recv()).await.is_err());
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_request(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    requests: mpsc::Sender<Request>,
) {
    let mut raw = Vec::new();
    let mut buf = [0; 4096];
    let header_end = loop {
        let Ok(count) = stream.read(&mut buf).await else {
            return;
        };
        if count == 0 {
            return;
        }
        raw.extend_from_slice(&buf[..count]);
        if let Some(position) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let head = std::str::from_utf8(&raw[..header_end]).unwrap();
    let mut lines = head.lines();
    let line = lines.next().unwrap().to_owned();
    let headers: HashMap<_, _> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    let body_end = header_end + headers["content-length"].parse::<usize>().unwrap();
    while raw.len() < body_end {
        let Ok(count) = stream.read(&mut buf).await else {
            return;
        };
        if count == 0 {
            return;
        }
        raw.extend_from_slice(&buf[..count]);
    }
    let (response, reply) = oneshot::channel();
    requests
        .send(Request {
            line,
            headers,
            raw_body: raw[header_end..body_end].to_vec(),
            response,
        })
        .await
        .ok()
        .unwrap();
    if let Ok(Reply {
        status,
        body,
        headers,
    }) = reply.await
    {
        let body = if status == 204 {
            Vec::new()
        } else {
            serde_json::to_vec(&body).unwrap()
        };
        let header = format!(
            "HTTP/1.1 {status} Test\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n",
            body.len()
        );
        let _ = stream.write_all(header.as_bytes()).await;
        let _ = stream.write_all(&body).await;
    }
}

fn scope() -> ClaimScope {
    ClaimScope::new("payments", "account-check", 1)
}

fn grant(job: Uuid, lease: Uuid, ttl_ms: u64, renew_ms: u64) -> Value {
    // Deliberate wall clock skew. The dispatcher uses the DB-relative fields.
    json!({
        "job_id": job, "namespace": "payments", "task_type": "account-check",
        "contract_version": 1, "input": {"account_id": INPUT},
        "attempt": 1, "lease_token": lease, "lease_expires_at": "2001-01-01T00:00:00Z",
        "lease_ttl_ms": ttl_ms, "renew_after_ms": renew_ms
    })
}

fn renewal() -> Value {
    json!({
        "lease_expires_at": "2001-01-01T00:00:00Z",
        "lease_ttl_ms": 2000, "renew_after_ms": 700
    })
}

fn receipt(job: Uuid, key: &str, outcome: &str, state: &str) -> Value {
    json!({
        "job_id": job, "attempt": 1, "idempotency_key": key,
        "outcome": outcome, "job_state": state
    })
}

fn error(code: &str) -> Value {
    json!({"status": 0, "error":"test error", "code":code, "message":BEARER, "input":INPUT})
}

async fn within<T>(future: impl Future<Output = T>) -> T {
    timeout(TEST_TIMEOUT, future)
        .await
        .expect("scenario timed out")
}

#[tokio::test]
async fn typed_calls_match_wire_and_redact_payloads_and_credentials() {
    let mut server = Server::start().await;
    let client = server.client();
    let job = Uuid::new_v4();
    let lease = Uuid::new_v4();
    let calls = tokio::spawn(async move {
        assert!(client.claim(&scope()).await.unwrap().is_none());
        let grant = client.claim(&scope()).await.unwrap().unwrap();
        assert_eq!(grant.input["account_id"], INPUT);
        assert_eq!(grant.lease_token.as_uuid(), lease);
        assert!(!format!("{grant:?}").contains(INPUT));
        assert!(!format!("{:?}", grant.lease_token).contains(&lease.to_string()));
        assert_eq!(
            client
                .renew(job, grant.lease_token)
                .await
                .unwrap()
                .lease_ttl_ms,
            2000
        );

        let result = json!({"approved":false}).as_object().unwrap().clone();
        let command =
            Completion::new(job, grant.lease_token, "completion-fixture", result).unwrap();
        let first = client.complete(&command).await.unwrap();
        let replay = client.complete(&command).await.unwrap();
        assert_eq!(first, replay);
        assert_eq!(first.outcome, Outcome::Completed);
        assert_eq!(first.job_state, JobState::Completed);

        let command = Failure::new(
            job,
            grant.lease_token,
            "retryable-fixture",
            FailureKind::Retryable,
            "retry_later",
        )
        .unwrap();
        let returned = client.fail(&command).await.unwrap();
        assert_eq!(returned.outcome, Outcome::Failed);
        assert_eq!(returned.job_state, JobState::Queued);

        let conflict = client.fail(&command).await.unwrap_err();
        assert!(matches!(
            conflict,
            WorkerError::Rejected {
                status: 409,
                code: WorkerErrorCode::IdempotencyConflict,
                ..
            }
        ));
        assert!(matches!(
            client.renew(job, grant.lease_token).await.unwrap_err(),
            WorkerError::Rejected {
                status: 409,
                code: WorkerErrorCode::LeaseLost,
                ..
            }
        ));
        let denied = client.claim(&scope()).await.unwrap_err();
        assert!(matches!(
            denied,
            WorkerError::Rejected {
                status: 403,
                code: WorkerErrorCode::ScopeDenied,
                ..
            }
        ));
        assert!(!format!("{denied:?} {denied}").contains(BEARER));
        assert!(!format!("{denied:?} {denied}").contains(INPUT));
        let throttled = client.claim(&scope()).await.unwrap_err();
        assert!(throttled.should_retry());
        assert_eq!(throttled.retry_after(), Some(Duration::from_secs(3)));
    });

    let request = server.next("/claim").await;
    assert_eq!(
        request.body(),
        json!({"namespace":"payments","task_type":"account-check","contract_version":1})
    );
    request.reply(204, Value::Null);
    server
        .next("/claim")
        .await
        .reply(200, grant(job, lease, 2000, 700));
    let renew = format!("/{job}/renew");
    let request = server.next(&renew).await;
    assert_eq!(request.body(), json!({"lease_token":lease}));
    request.reply(200, renewal());
    for _ in 0..2 {
        let request = server.next(&format!("/{job}/complete")).await;
        assert_eq!(
            request.body(),
            json!({"lease_token":lease,"result":{"approved":false}})
        );
        assert_eq!(request.headers["idempotency-key"], "completion-fixture");
        request.reply(
            200,
            receipt(job, "completion-fixture", "completed", "completed"),
        );
    }
    let fail = format!("/{job}/fail");
    let request = server.next(&fail).await;
    assert_eq!(
        request.body(),
        json!({"lease_token":lease,"kind":"retryable","code":"retry_later"})
    );
    assert_eq!(request.headers["idempotency-key"], "retryable-fixture");
    request.reply(200, receipt(job, "retryable-fixture", "failed", "queued"));
    server
        .next(&fail)
        .await
        .reply(409, error("idempotency_conflict"));
    server.next(&renew).await.reply(409, error("lease_lost"));
    server
        .next("/claim")
        .await
        .reply(403, error("scope_denied"));
    server
        .next("/claim")
        .await
        .send(429, error("throttled"), "retry-after: 3\r\n");
    within(calls).await.unwrap();
}

#[tokio::test]
async fn redirects_are_rejected_without_replaying_a_claim() {
    let mut server = Server::start().await;
    let client = server.client();
    let call = tokio::spawn(async move { client.claim(&scope()).await.unwrap_err() });
    server
        .next("/claim")
        .await
        .send(307, error("unknown"), "location: /unexpected\r\n");
    assert!(matches!(
        within(call).await.unwrap(),
        WorkerError::Rejected { status: 307, .. }
    ));
    server.quiet_for(Duration::from_millis(50)).await;
}

#[cfg(any(feature = "rustls", feature = "native-tls"))]
#[tokio::test]
async fn https_private_ca_requires_a_trusted_root_and_the_correct_endpoint_identity() {
    use rcgen::{
        BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose,
    };
    use tokio_rustls::rustls::{self, pki_types::PrivatePkcs8KeyDer, ServerConfig};

    fn authority(name: &str) -> (Certificate, KeyPair) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name.push(DnType::CommonName, name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca = params.self_signed(&key).unwrap();
        (ca, key)
    }

    fn acceptor(dns_or_ip: &str, ca: &Certificate, ca_key: &KeyPair) -> TlsAcceptor {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![dns_or_ip.to_owned()]).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, dns_or_ip);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let leaf = params.signed_by(&key, ca, ca_key).unwrap();
        let server =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![leaf.der().clone()],
                    PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
                )
                .unwrap();
        Arc::new(server).into()
    }

    let (ca, ca_key) = authority("Heart SDK test root");
    let (unrelated, _) = authority("Heart SDK unrelated root");
    let mut server = Server::listen(Some(acceptor("127.0.0.1", &ca, &ca_key))).await;
    assert!(matches!(
        ExternalJobsClient::with_ca_bundle(&server.address, BEARER, b"invalid CA"),
        Err(WorkerError::InvalidCaBundle)
    ));
    assert!(matches!(
        within(server.client().claim(&scope())).await,
        Err(WorkerError::Transport)
    ));
    let wrong_root =
        ExternalJobsClient::with_ca_bundle(&server.address, BEARER, unrelated.pem().as_bytes())
            .unwrap();
    assert!(matches!(
        within(wrong_root.claim(&scope())).await,
        Err(WorkerError::Transport)
    ));
    let bundle = format!("{}{}", unrelated.pem(), ca.pem());
    let client =
        ExternalJobsClient::with_ca_bundle(&server.address, BEARER, bundle.as_bytes()).unwrap();
    let trusted = tokio::spawn(async move { client.claim(&scope()).await });
    server.next("/claim").await.reply(204, Value::Null);
    assert!(within(trusted).await.unwrap().unwrap().is_none());

    let wrong_name = Server::listen(Some(acceptor("another-service.invalid", &ca, &ca_key))).await;
    let client =
        ExternalJobsClient::with_ca_bundle(&wrong_name.address, BEARER, bundle.as_bytes()).unwrap();
    assert!(matches!(
        within(client.claim(&scope())).await,
        Err(WorkerError::Transport)
    ));
}

#[tokio::test]
async fn mutual_tls_presents_the_client_certificate_instead_of_a_token() {
    use heart_client::v2::ClientIdentity;
    use rcgen::{
        BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose, SanType,
    };
    use tokio_rustls::rustls::{
        self,
        pki_types::{pem::PemObject, PrivatePkcs8KeyDer},
        server::WebPkiClientVerifier,
        RootCertStore, ServerConfig,
    };

    fn authority(name: &str) -> (Certificate, KeyPair) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name.push(DnType::CommonName, name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        (params.self_signed(&key).unwrap(), key)
    }

    fn leaf(ca: &Certificate, ca_key: &KeyPair, client: bool) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(if client {
            vec![]
        } else {
            vec!["127.0.0.1".to_owned()]
        })
        .unwrap();
        if client {
            params.subject_alt_names.push(SanType::URI(
                "spiffe://bjoroy.me/ns/payments/sa/account-worker"
                    .try_into()
                    .unwrap(),
            ));
        }
        params.extended_key_usages = vec![if client {
            ExtendedKeyUsagePurpose::ClientAuth
        } else {
            ExtendedKeyUsagePurpose::ServerAuth
        }];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let cert = params.signed_by(&key, ca, ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    let (ca, ca_key) = authority("Heart worker CA");
    let (foreign_ca, foreign_key) = authority("Unrelated CA");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut client_roots = RootCertStore::empty();
    client_roots.add(ca.der().clone()).unwrap();
    let (server_cert, server_key) = leaf(&ca, &ca_key, false);
    let server_key = KeyPair::from_pem(&server_key).unwrap();
    let config = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_client_cert_verifier(
            WebPkiClientVerifier::builder_with_provider(Arc::new(client_roots), provider)
                .build()
                .unwrap(),
        )
        .with_single_cert(
            vec![
                rustls::pki_types::CertificateDer::from_pem_slice(server_cert.as_bytes()).unwrap(),
            ],
            PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
        )
        .unwrap();
    let mut server = Server::listen(Some(Arc::new(config).into())).await;
    let bundle = ca.pem();

    // Input validation.
    let (cert, key) = leaf(&ca, &ca_key, true);
    let (_, other_key) = leaf(&ca, &ca_key, true);
    assert!(matches!(
        ExternalJobsClient::with_client_identity(
            &server.address,
            bundle.as_bytes(),
            ClientIdentity::from_pem(cert.as_bytes(), other_key.as_bytes()),
        ),
        Err(WorkerError::InvalidClientIdentity)
    ));
    assert!(matches!(
        ExternalJobsClient::with_client_identity(
            "http://127.0.0.1:1",
            bundle.as_bytes(),
            ClientIdentity::from_pem(cert.as_bytes(), key.as_bytes()),
        ),
        Err(WorkerError::InvalidEndpoint)
    ));

    // A token alone, or a certificate from another CA, is refused in the handshake.
    let token_only =
        ExternalJobsClient::with_ca_bundle(&server.address, BEARER, bundle.as_bytes()).unwrap();
    assert!(matches!(
        within(token_only.claim(&scope())).await,
        Err(WorkerError::Transport)
    ));
    let (foreign_cert, foreign_cert_key) = leaf(&foreign_ca, &foreign_key, true);
    let foreign = ExternalJobsClient::with_client_identity(
        &server.address,
        bundle.as_bytes(),
        ClientIdentity::from_pem(foreign_cert.as_bytes(), foreign_cert_key.as_bytes()),
    )
    .unwrap();
    assert!(matches!(
        within(foreign.claim(&scope())).await,
        Err(WorkerError::Transport)
    ));

    // Trusted client certificate: the request goes through without a token.
    let client = ExternalJobsClient::with_client_identity(
        &server.address,
        bundle.as_bytes(),
        ClientIdentity::from_pem(cert.as_bytes(), key.as_bytes()),
    )
    .unwrap();
    let claim = tokio::spawn(async move { client.claim(&scope()).await });
    let request = within(server.requests.recv()).await.unwrap();
    assert_eq!(request.line, format!("POST {BASE}/claim HTTP/1.1"));
    assert!(!request.headers.contains_key("authorization"));
    request.reply(204, Value::Null);
    assert!(within(claim).await.unwrap().unwrap().is_none());
}

#[derive(Deserialize)]
struct Input {
    account_id: String,
}

#[derive(Serialize)]
struct Output {
    approved: bool,
}

struct Handler {
    ready: mpsc::Sender<Uuid>,
    mode: Mode,
}

enum Mode {
    Immediate,
    Fail,
    Wait(Arc<Semaphore>),
    LoseLease(mpsc::Sender<LeaseLossReason>),
}

#[async_trait::async_trait]
impl ExternalHandler for Handler {
    type Input = Input;
    type Output = Output;

    async fn handle(
        &self,
        input: Input,
        mut context: JobContext,
    ) -> Result<Output, HandlerFailure> {
        assert_eq!(input.account_id, INPUT);
        self.ready.send(context.job_id).await.unwrap();
        match &self.mode {
            Mode::Immediate => {}
            Mode::Fail => return Err(HandlerFailure::retryable("effect_not_ready")),
            Mode::Wait(permits) => permits.acquire().await.unwrap().forget(),
            Mode::LoseLease(loss) => loss.send(context.lease_loss.wait().await).await.unwrap(),
        }
        Ok(Output { approved: false })
    }
}

fn start_dispatcher(
    server: &Server,
    mode: Mode,
    capacity: usize,
) -> (oneshot::Sender<()>, JoinHandle<()>, mpsc::Receiver<Uuid>) {
    let config = DispatchConfig {
        poll_min: Duration::from_millis(25),
        poll_max: Duration::from_millis(75),
        command_retry_window: Duration::from_secs(3),
        shutdown_grace: Duration::from_secs(2),
        ..DispatchConfig::default()
    };
    let (ready, started) = mpsc::channel(capacity);
    let (stop, stopping) = oneshot::channel();
    let dispatcher = ExternalDispatcher::new(server.client(), config)
        .unwrap()
        .register(
            scope(),
            NonZeroUsize::new(capacity).unwrap(),
            Handler { ready, mode },
        )
        .unwrap();
    let running = tokio::spawn(async move {
        dispatcher
            .run(async move {
                let _ = stopping.await;
            })
            .await
            .unwrap();
    });
    (stop, running, started)
}

#[tokio::test]
async fn dispatcher_replays_the_original_command_after_renewal_reports_lease_loss() {
    let mut server = Server::start().await;
    let job = Uuid::new_v4();
    let lease = Uuid::new_v4();
    let (stop, running, mut started) = start_dispatcher(&server, Mode::Immediate, 1);
    server
        .next("/claim")
        .await
        .reply(200, grant(job, lease, 2000, 200));
    assert_eq!(within(started.recv()).await, Some(job));
    let path = format!("/{job}/complete");
    let original = server.next(&path).await;
    let key = original.headers["idempotency-key"].clone();
    let original_body = original.raw_body.clone();
    assert!(Uuid::parse_str(&key).is_ok());
    assert_eq!(
        original.body(),
        json!({"lease_token":lease,"result":{"approved":false}})
    );
    stop.send(()).unwrap();
    server
        .next(&format!("/{job}/renew"))
        .await
        .reply(409, error("lease_lost"));
    // Heart committed, but the client receives no HTTP response.
    drop(original);
    let replay = server.next(&path).await;
    assert_eq!(replay.raw_body, original_body);
    assert_eq!(replay.headers["idempotency-key"], key);
    replay.reply(200, receipt(job, &key, "completed", "completed"));
    within(running).await.unwrap();
}

#[tokio::test]
async fn dispatcher_replays_a_classified_failure_after_an_invalid_receipt() {
    let mut server = Server::start().await;
    let job = Uuid::new_v4();
    let lease = Uuid::new_v4();
    let (stop, running, mut started) = start_dispatcher(&server, Mode::Fail, 1);
    server
        .next("/claim")
        .await
        .reply(200, grant(job, lease, 3000, 1000));
    assert_eq!(within(started.recv()).await, Some(job));
    let path = format!("/{job}/fail");
    let first = server.next(&path).await;
    let key = first.headers["idempotency-key"].clone();
    let body = first.raw_body.clone();
    assert_eq!(
        first.body(),
        json!({"lease_token":lease,"kind":"retryable","code":"effect_not_ready"})
    );
    stop.send(()).unwrap();
    first.reply(200, json!({"unexpected":"receipt"}));
    let replay = server.next(&path).await;
    assert_eq!(replay.headers["idempotency-key"], key);
    assert_eq!(replay.raw_body, body);
    replay.reply(200, receipt(job, &key, "failed", "queued"));
    within(running).await.unwrap();
}

#[tokio::test]
async fn dispatcher_does_not_claim_beyond_capacity_and_renews_during_shutdown() {
    let mut server = Server::start().await;
    let permits = Arc::new(Semaphore::new(0));
    let (stop, running, mut started) = start_dispatcher(&server, Mode::Wait(permits.clone()), 2);
    let jobs = [Uuid::new_v4(), Uuid::new_v4()];
    for job in jobs {
        server
            .next("/claim")
            .await
            .reply(200, grant(job, Uuid::new_v4(), 2000, 300));
    }
    for _ in 0..2 {
        assert!(jobs.contains(&within(started.recv()).await.unwrap()));
    }
    server.quiet_for(Duration::from_millis(65)).await;
    stop.send(()).unwrap();

    for _ in 0..2 {
        let request = within(server.requests.recv()).await.unwrap();
        assert!(jobs
            .iter()
            .any(|job| request.line == format!("POST {BASE}/{job}/renew HTTP/1.1")));
        request.reply(200, renewal());
    }
    permits.add_permits(2);
    let mut completed = Vec::new();
    for _ in 0..2 {
        let request = within(server.requests.recv()).await.unwrap();
        let job = *jobs
            .iter()
            .find(|job| request.line == format!("POST {BASE}/{job}/complete HTTP/1.1"))
            .unwrap();
        let key = request.headers["idempotency-key"].clone();
        completed.push(job);
        request.reply(200, receipt(job, &key, "completed", "completed"));
    }
    assert_ne!(completed[0], completed[1]);
    within(running).await.unwrap();
}

#[tokio::test]
async fn dispatcher_stops_renewing_unfinished_jobs_at_the_end_of_shutdown_grace() {
    let mut server = Server::start().await;
    let (ready, mut started) = mpsc::channel(1);
    let (stop, stopping) = oneshot::channel();
    let dispatcher = ExternalDispatcher::new(
        server.client(),
        DispatchConfig {
            shutdown_grace: Duration::from_millis(125),
            ..DispatchConfig::default()
        },
    )
    .unwrap()
    .register(
        scope(),
        NonZeroUsize::MIN,
        Handler {
            ready,
            mode: Mode::Wait(Arc::new(Semaphore::new(0))),
        },
    )
    .unwrap();
    let running = tokio::spawn(dispatcher.run(async {
        stopping.await.unwrap();
    }));
    let job = Uuid::new_v4();
    server
        .next("/claim")
        .await
        .reply(200, grant(job, Uuid::new_v4(), 2000, 500));
    assert_eq!(within(started.recv()).await, Some(job));
    stop.send(()).unwrap();
    timeout(Duration::from_millis(450), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.quiet_for(Duration::from_millis(450)).await;
}

#[tokio::test]
async fn dispatcher_signals_rejected_lease_and_discards_the_handler_output() {
    let mut server = Server::start().await;
    let (loss, mut observed) = mpsc::channel(1);
    let (stop, running, mut started) = start_dispatcher(&server, Mode::LoseLease(loss), 1);
    let job = Uuid::new_v4();
    server
        .next("/claim")
        .await
        .reply(200, grant(job, Uuid::new_v4(), 2000, 100));
    assert_eq!(within(started.recv()).await, Some(job));
    stop.send(()).unwrap();
    server
        .next(&format!("/{job}/renew"))
        .await
        .reply(409, error("lease_lost"));
    assert_eq!(
        within(observed.recv()).await,
        Some(LeaseLossReason::Rejected)
    );
    within(running).await.unwrap();
    server.quiet_for(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn dispatcher_times_out_renewal_at_the_last_confirmed_deadline() {
    let mut server = Server::start().await;
    let (loss, mut observed) = mpsc::channel(1);
    let (stop, running, mut started) = start_dispatcher(&server, Mode::LoseLease(loss), 1);
    let job = Uuid::new_v4();
    server
        .next("/claim")
        .await
        .reply(200, grant(job, Uuid::new_v4(), 350, 75));
    assert_eq!(within(started.recv()).await, Some(job));
    stop.send(()).unwrap();
    let stalled = server.next(&format!("/{job}/renew")).await;
    assert_eq!(
        within(observed.recv()).await,
        Some(LeaseLossReason::Unconfirmed)
    );
    within(running).await.unwrap();
    drop(stalled);
    server.quiet_for(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn dispatcher_waits_for_retry_after_and_exits_cleanly_on_denial() {
    let mut server = Server::start().await;
    let (ready, _started) = mpsc::channel(1);
    let dispatcher = ExternalDispatcher::new(server.client(), DispatchConfig::default())
        .unwrap()
        .register(
            scope(),
            NonZeroUsize::MIN,
            Handler {
                ready,
                mode: Mode::Immediate,
            },
        )
        .unwrap();
    let running = tokio::spawn(dispatcher.run(pending()));
    server
        .next("/claim")
        .await
        .send(429, error("throttled"), "retry-after: 1\r\n");
    server.quiet_for(Duration::from_millis(750)).await;
    server
        .next("/claim")
        .await
        .reply(403, error("scope_denied"));
    assert!(matches!(
        within(running).await.unwrap(),
        Err(heart_client::v2::DispatchError::Subscription(
            WorkerError::Rejected {
                code: WorkerErrorCode::ScopeDenied,
                ..
            }
        ))
    ));
}
