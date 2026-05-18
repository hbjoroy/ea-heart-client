//! Heart process engine HTTP client.
//!
//! Provides a typed Rust client for the Heart REST API, covering
//! definitions, instances, queues, and work items.
//!
//! Also includes a generic `WorkDispatcher` + `WorkHandler` trait for
//! building workers that poll Heart queues.

pub mod dispatcher;

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
        let namespace =
            std::env::var("HEART_NAMESPACE").unwrap_or_else(|_| "default".to_string());
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
        Ok(serde_json::from_value(value).map_err(|e| HeartError::Api {
            status: 0,
            message: format!("failed to parse start response: {}", e),
        })?)
    }

    /// Get instance details.
    pub async fn get_instance(&self, instance_id: Uuid) -> Result<InstanceDetails, HeartError> {
        let resp = self
            .http
            .get(format!("{}/api/v1/instances/{}", self.base_url, instance_id))
            .send()
            .await?;

        let value = self.handle_response(resp).await?;
        Ok(serde_json::from_value(value).map_err(|e| HeartError::Api {
            status: 0,
            message: format!("failed to parse instance: {}", e),
        })?)
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
    pub async fn get_definition(
        &self,
        name: &str,
        version: &str,
    ) -> Result<Value, HeartError> {
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
    pub async fn list_queue_items(
        &self,
        queue_name: &str,
    ) -> Result<Vec<WorkItem>, HeartError> {
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
        match self.http.get(format!("{}/health", self.base_url)).send().await {
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
            let message = resp.text().await.unwrap_or_default();
            return Err(HeartError::Api { status, message });
        }
        let value = resp.json().await?;
        Ok(value)
    }
}
