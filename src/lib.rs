//! Heart process engine HTTP client.
//!
//! Provides a typed Rust client for the Heart REST API, covering
//! definitions, instances, queues, and work items.
//!
//! Also includes a generic `WorkDispatcher` + `WorkHandler` trait for
//! building workers that poll Heart queues.

pub mod dispatcher;
pub mod v2;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, warn};
use uuid::Uuid;

pub use dispatcher::{HandlerResult, WorkContext, WorkDispatcher, WorkHandler};

/// Errors from the Heart client.
#[derive(Debug, thiserror::Error)]
pub enum HeartError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Heart API error {status}: {message}")]
    Api { status: u16, message: String },
    #[error("Heart not configured")]
    NotConfigured,
}

/// Response from starting a Heart process instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartInstanceResponse {
    pub instance_id: Uuid,
    pub run: Option<RunResult>,
}

/// Response from correlating a message to Heart receive nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessageResponse {
    pub matched_instances: usize,
    pub instance_ids: Vec<Uuid>,
    #[serde(default)]
    pub runs: Vec<MessageRun>,
}

/// Run result for one instance matched by a receive message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageRun {
    pub instance_id: Uuid,
    pub run: RunResult,
}

/// Result of stepping an instance (returned inside start response).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub outcome: Option<RunOutcome>,
    pub steps: Vec<String>,
    pub step_count: u32,
}

/// Outcome of a run — where the instance stopped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunOutcome {
    pub result: String,
    pub node_id: Option<String>,
    pub work_item_id: Option<Uuid>,
    pub queue_name: Option<String>,
}

/// Summary of a definition from the Heart API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefinitionSummary {
    pub id: Uuid,
    pub namespace: String,
    pub name: String,
    pub version: String,
    pub description: Option<String>,
}

/// Summary of a queue with pending item count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueSummary {
    pub namespace: String,
    pub queue_name: String,
    pub pending_count: i64,
}

/// Instance details from the Heart API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceDetails {
    pub id: Uuid,
    pub definition_id: Uuid,
    pub namespace: String,
    pub status: String,
    pub current_node: Option<String>,
    #[serde(default)]
    pub metadata: Value,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub completed_at: Option<String>,
    pub error_node: Option<String>,
    pub error_message: Option<String>,
}

/// Transition record from Heart instance history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transition {
    pub id: Uuid,
    pub instance_id: Uuid,
    pub seq: u32,
    pub from_node: String,
    pub to_node: String,
    pub transitioned_at: String,
    pub metadata_snapshot: Option<Value>,
}

/// Work item from a queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkItem {
    pub id: Uuid,
    pub instance_id: Uuid,
    pub namespace: String,
    pub node_id: String,
    pub status: String,
    pub metadata: Option<Value>,
}

/// Client for the Heart process engine REST API.
#[derive(Clone)]
pub struct HeartClient {
    http: Client,
    base_url: String,
    namespace: String,
}

impl HeartClient {
    /// Create a new Heart client.
    ///
    /// `base_url` should be the Heart API root, e.g. `http://heart:3000`.
    /// `namespace` is the default namespace for operations.
    pub fn new(base_url: String, namespace: String) -> Self {
        Self {
            http: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            namespace,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Try to create a Heart client from environment variables.
    /// Returns `None` if `HEART_API_URL` is not set.
    pub fn from_env() -> Option<Self> {
        let base_url = std::env::var("HEART_API_URL").ok()?;
        let namespace = std::env::var("HEART_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        Some(Self::new(base_url, namespace))
    }

    /// Deploy a process definition (YAML body).
    pub async fn deploy_definition(&self, yaml: &str) -> Result<Value, HeartError> {
        let resp = self
            .http
            .post(format!("{}/api/v1/definitions", self.base_url))
            .header("Content-Type", "text/plain")
            .body(yaml.to_string())
            .send()
            .await?;

        self.handle_response(resp).await
    }

    /// Start a process instance.
    pub async fn start_instance(
        &self,
        definition_name: &str,
        version: Option<&str>,
        metadata: Value,
    ) -> Result<StartInstanceResponse, HeartError> {
        self.start_instance_in_namespace(&self.namespace, definition_name, version, metadata)
            .await
    }

    /// Start an instance in a specific namespace.
    pub async fn start_instance_in_namespace(
        &self,
        namespace: &str,
        definition_name: &str,
        version: Option<&str>,
        metadata: Value,
    ) -> Result<StartInstanceResponse, HeartError> {
        let mut body = serde_json::json!({
            "definition_name": definition_name,
            "namespace": namespace,
            "metadata": metadata,
        });

        if let Some(v) = version {
            body["version"] = Value::String(v.to_string());
        }

        debug!(
            definition = definition_name,
            namespace = %namespace,
            "starting Heart process instance"
        );

        let resp = self
            .http
            .post(format!("{}/api/v1/instances", self.base_url))
            .json(&body)
            .send()
            .await?;

        let value = self.handle_response(resp).await?;
        serde_json::from_value(value).map_err(|e| HeartError::Api {
            status: 0,
            message: format!("failed to parse start response: {}", e),
        })
    }

    /// Get instance details.
    pub async fn get_instance(&self, instance_id: Uuid) -> Result<InstanceDetails, HeartError> {
        let resp = self
            .http
            .get(format!(
                "{}/api/v1/instances/{}",
                self.base_url, instance_id
            ))
            .send()
            .await?;

        let value = self.handle_response(resp).await?;
        serde_json::from_value(value).map_err(|e| HeartError::Api {
            status: 0,
            message: format!("failed to parse instance: {}", e),
        })
    }

    /// Correlate a message to receive nodes in the default namespace.
    pub async fn send_message(
        &self,
        correlation_key: &str,
        correlation_value: &str,
        payload: Value,
    ) -> Result<SendMessageResponse, HeartError> {
        self.send_message_in_namespace(&self.namespace, correlation_key, correlation_value, payload)
            .await
    }

    /// Correlate a message to receive nodes in a specific namespace.
    pub async fn send_message_in_namespace(
        &self,
        namespace: &str,
        correlation_key: &str,
        correlation_value: &str,
        payload: Value,
    ) -> Result<SendMessageResponse, HeartError> {
        let body = serde_json::json!({
            "namespace": namespace,
            "correlation_key": correlation_key,
            "correlation_value": correlation_value,
            "payload": payload,
        });

        debug!(
            namespace = %namespace,
            correlation_key = %correlation_key,
            "sending correlated Heart message"
        );

        let response = self
            .http
            .post(format!("{}/api/v1/messages", self.base_url))
            .json(&body)
            .send()
            .await?;
        let value = self.handle_response(response).await?;
        serde_json::from_value(value).map_err(|error| HeartError::Api {
            status: 0,
            message: format!("failed to parse send-message response: {error}"),
        })
    }

    /// Pick next pending work item from a queue.
    pub async fn pick_work_item(&self, queue_name: &str) -> Result<Option<WorkItem>, HeartError> {
        let resp = self
            .http
            .post(format!(
                "{}/api/v1/queues/{}/{}/pick",
                self.base_url, self.namespace, queue_name
            ))
            .send()
            .await?;

        if resp.status().as_u16() == 404 {
            return Ok(None);
        }

        let value = self.handle_response(resp).await?;
        Ok(Some(serde_json::from_value(value).map_err(|e| {
            HeartError::Api {
                status: 0,
                message: format!("failed to parse work item: {}", e),
            }
        })?))
    }

    /// Complete a work item with result metadata.
    pub async fn complete_work_item(
        &self,
        work_item_id: Uuid,
        result_metadata: Value,
    ) -> Result<Value, HeartError> {
        let body = serde_json::json!({
            "result_metadata": result_metadata,
        });

        debug!(
            work_item_id = %work_item_id,
            "completing Heart work item"
        );

        let resp = self
            .http
            .post(format!(
                "{}/api/v1/work-items/{}/complete",
                self.base_url, work_item_id
            ))
            .json(&body)
            .send()
            .await?;

        self.handle_response(resp).await
    }

    /// Get transition history for an instance.
    pub async fn get_transitions(&self, instance_id: Uuid) -> Result<Vec<Transition>, HeartError> {
        let resp = self
            .http
            .get(format!(
                "{}/api/v1/instances/{}/transitions",
                self.base_url, instance_id
            ))
            .send()
            .await?;

        let value = self.handle_response(resp).await?;
        serde_json::from_value(value).map_err(|e| HeartError::Api {
            status: 0,
            message: format!("failed to parse transitions: {}", e),
        })
    }

    /// List all definitions.
    pub async fn list_definitions(&self) -> Result<Vec<DefinitionSummary>, HeartError> {
        let resp = self
            .http
            .get(format!("{}/api/v1/definitions", self.base_url))
            .send()
            .await?;

        let value = self.handle_response(resp).await?;
        serde_json::from_value(value).map_err(|e| HeartError::Api {
            status: 0,
            message: format!("failed to parse definitions: {}", e),
        })
    }

    /// List versions of a specific definition.
    pub async fn list_definition_versions(
        &self,
        name: &str,
    ) -> Result<Vec<DefinitionSummary>, HeartError> {
        let resp = self
            .http
            .get(format!(
                "{}/api/v1/definitions/{}/{}",
                self.base_url, self.namespace, name
            ))
            .send()
            .await?;

        let value = self.handle_response(resp).await?;
        serde_json::from_value(value).map_err(|e| HeartError::Api {
            status: 0,
            message: format!("failed to parse definition versions: {}", e),
        })
    }

    /// Get a specific definition version (includes parsed nodes).
    pub async fn get_definition(&self, name: &str, version: &str) -> Result<Value, HeartError> {
        self.get_definition_in_namespace(&self.namespace, name, version)
            .await
    }

    /// Get a definition in a specific namespace.
    pub async fn get_definition_in_namespace(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<Value, HeartError> {
        let resp = self
            .http
            .get(format!(
                "{}/api/v1/definitions/{}/{}/{}",
                self.base_url, namespace, name, version
            ))
            .send()
            .await?;

        self.handle_response(resp).await
    }

    /// List instances, optionally filtered by status.
    pub async fn list_instances(
        &self,
        status: Option<&str>,
    ) -> Result<Vec<InstanceDetails>, HeartError> {
        let mut url = format!("{}/api/v1/instances", self.base_url);
        if let Some(s) = status {
            url = format!("{}?status={}", url, s);
        }

        let resp = self.http.get(&url).send().await?;

        let value = self.handle_response(resp).await?;
        serde_json::from_value(value).map_err(|e| HeartError::Api {
            status: 0,
            message: format!("failed to parse instances: {}", e),
        })
    }

    /// List queues with pending item counts.
    pub async fn list_queues(&self) -> Result<Vec<QueueSummary>, HeartError> {
        let resp = self
            .http
            .get(format!("{}/api/v1/queues", self.base_url))
            .send()
            .await?;

        let value = self.handle_response(resp).await?;
        serde_json::from_value(value).map_err(|e| HeartError::Api {
            status: 0,
            message: format!("failed to parse queues: {}", e),
        })
    }

    /// List work items in a specific queue.
    pub async fn list_queue_items(&self, queue_name: &str) -> Result<Vec<WorkItem>, HeartError> {
        let resp = self
            .http
            .get(format!(
                "{}/api/v1/queues/{}/{}",
                self.base_url, self.namespace, queue_name
            ))
            .send()
            .await?;

        let value = self.handle_response(resp).await?;
        serde_json::from_value(value).map_err(|e| HeartError::Api {
            status: 0,
            message: format!("failed to parse queue items: {}", e),
        })
    }

    /// Single-step an instance (debugging).
    pub async fn step_instance(&self, instance_id: Uuid) -> Result<Value, HeartError> {
        let resp = self
            .http
            .post(format!(
                "{}/api/v1/instances/{}/step",
                self.base_url, instance_id
            ))
            .send()
            .await?;

        self.handle_response(resp).await
    }

    /// Retry a failed instance from its current node.
    pub async fn retry_instance(&self, instance_id: Uuid) -> Result<Value, HeartError> {
        let resp = self
            .http
            .post(format!(
                "{}/api/v1/instances/{}/retry",
                self.base_url, instance_id
            ))
            .send()
            .await?;

        self.handle_response(resp).await
    }

    /// Move a failed instance to a specific node and resume.
    pub async fn move_instance(
        &self,
        instance_id: Uuid,
        target_node: &str,
    ) -> Result<Value, HeartError> {
        let body = serde_json::json!({
            "target_node": target_node,
        });

        let resp = self
            .http
            .post(format!(
                "{}/api/v1/instances/{}/move",
                self.base_url, instance_id
            ))
            .json(&body)
            .send()
            .await?;

        self.handle_response(resp).await
    }

    /// Check if Heart is reachable.
    pub async fn health_check(&self) -> bool {
        match self
            .http
            .get(format!("{}/health", self.base_url))
            .send()
            .await
        {
            Ok(resp) => resp.status().is_success(),
            Err(e) => {
                warn!("Heart health check failed: {}", e);
                false
            }
        }
    }

    async fn handle_response(&self, resp: reqwest::Response) -> Result<Value, HeartError> {
        let status = resp.status().as_u16();
        if status >= 400 {
            return Err(HeartError::Api {
                status,
                message: "Heart API request failed".to_string(),
            });
        }
        let value = resp.json().await?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::oneshot,
    };

    #[derive(Debug)]
    struct CapturedRequest {
        request_line: String,
        body: Value,
    }

    async fn mock_response(
        status: u16,
        response_body: Value,
    ) -> (String, oneshot::Receiver<CapturedRequest>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            let header_end;
            loop {
                let read = stream.read(&mut buffer).await.unwrap();
                assert!(read > 0, "connection closed before request headers");
                request.extend_from_slice(&buffer[..read]);
                if let Some(position) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    header_end = position + 4;
                    break;
                }
            }
            let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            while request.len() - header_end < content_length {
                let read = stream.read(&mut buffer).await.unwrap();
                assert!(read > 0, "connection closed before request body");
                request.extend_from_slice(&buffer[..read]);
            }
            let body = serde_json::from_slice(&request[header_end..header_end + content_length])
                .unwrap_or(Value::Null);
            sender
                .send(CapturedRequest {
                    request_line: headers.lines().next().unwrap().to_string(),
                    body,
                })
                .unwrap();

            let body = serde_json::to_vec(&response_body).unwrap();
            let reason = if status == 200 {
                "OK"
            } else {
                "Internal Server Error"
            };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        });
        (format!("http://{address}"), receiver)
    }

    fn successful_response(instance_id: Uuid) -> Value {
        serde_json::json!({
            "matched_instances": 1,
            "instance_ids": [instance_id],
            "runs": [{
                "instance_id": instance_id,
                "run": {
                    "outcome": {"result":"waiting","node_id":"review","work_item_id":null,"queue_name":null},
                    "steps": ["receive", "review"],
                    "step_count": 2
                }
            }]
        })
    }

    #[tokio::test]
    async fn send_message_uses_default_namespace_and_decodes_runs() {
        let instance_id = Uuid::from_u128(0x11111111111141118111111111111111);
        let (base_url, captured) = mock_response(200, successful_response(instance_id)).await;
        let client = HeartClient::new(base_url, "friends".to_string());

        let response = client
            .send_message("event_id", "event-42", serde_json::json!({"answer":"yes"}))
            .await
            .unwrap();

        let request = captured.await.unwrap();
        assert_eq!(request.request_line, "POST /api/v1/messages HTTP/1.1");
        assert_eq!(
            request.body,
            serde_json::json!({
                "namespace":"friends",
                "correlation_key":"event_id",
                "correlation_value":"event-42",
                "payload":{"answer":"yes"}
            })
        );
        assert_eq!(response.matched_instances, 1);
        assert_eq!(response.instance_ids, vec![instance_id]);
        assert_eq!(response.runs.len(), 1);
        assert_eq!(response.runs[0].instance_id, instance_id);
        assert_eq!(response.runs[0].run.step_count, 2);
    }

    #[tokio::test]
    async fn send_message_supports_explicit_namespace() {
        let instance_id = Uuid::from_u128(0x22222222222242228222222222222222);
        let (base_url, captured) = mock_response(200, successful_response(instance_id)).await;
        let client = HeartClient::new(base_url, "default".to_string());

        client
            .send_message_in_namespace(
                "circle-a",
                "decision_id",
                "decision-7",
                serde_json::json!({"choice":"approve"}),
            )
            .await
            .unwrap();

        assert_eq!(captured.await.unwrap().body["namespace"], "circle-a");
    }

    #[tokio::test]
    async fn api_errors_preserve_status_without_exposing_response_body() {
        let (base_url, _captured) = mock_response(
            500,
            serde_json::json!({"error":"failed","token":"super-secret"}),
        )
        .await;
        let client = HeartClient::new(base_url, "default".to_string());

        let error = client
            .send_message("key", "value", Value::Null)
            .await
            .unwrap_err();

        match &error {
            HeartError::Api { status, message } => {
                assert_eq!(*status, 500);
                assert_eq!(message, "Heart API request failed");
            }
            other => panic!("expected API error, got {other:?}"),
        }
        assert!(!error.to_string().contains("super-secret"));
    }
}
