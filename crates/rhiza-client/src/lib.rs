//! Typed remote client for the Rhiza public data routes.
//!
//! [`RhizaClient`] uses fixed 2-second connect, 5-second attempt, and 15-second
//! operation deadlines. Retryable failures move through the configured endpoint
//! order. Mutations and local/applied-index reads hedge after 100 milliseconds;
//! read-barrier and unspecified-consistency reads retry sequentially.
//!
//! The default `sql` feature provides write, read, and SQL methods. Wire DTOs are
//! re-exported from this crate for convenience, but are currently defined by
//! `rhiza-node`; the client and node are logically separate, not DTO-independent.

use std::{error::Error as _, fmt, time::Duration};

use reqwest::{header, Method, Response, StatusCode};
use rhiza_core::{ErrorCategory, ErrorClassification};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

const HEDGE_DELAY: Duration = Duration::from_millis(100);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
const SANITIZED_UNKNOWN_SERVER_CODE: &str = "unknown_server_error";

/// Feature-gated wire request and response types used by [`RhizaClient`].
pub mod wire {
    pub use rhiza_core::ReadConsistency;

    #[cfg(feature = "sql")]
    pub use rhiza_node::{
        ReadRequest, ReadResponse, SqlExecuteRequest, SqlExecuteResponse, SqlQueryRequest,
        SqlQueryResponse, WriteRequest, WriteResponse,
    };
    #[cfg(feature = "sql")]
    pub use rhiza_sql::{SqlStatement, SqlValue};

    #[cfg(feature = "graph")]
    pub use rhiza_node::{
        GraphColumnDto, GraphDeleteDocumentRequest, GraphMutationResponse, GraphPutDocumentRequest,
        GraphQueryParameterDto, GraphQueryRequest, GraphQueryResponse, GraphQueryStatementDto,
        GraphResultValueDto,
    };

    #[cfg(feature = "kv")]
    pub use rhiza_node::{
        KvBatchMemberRequest, KvBatchMemberResponse, KvBatchRequest, KvBatchResponse,
        KvDeleteRequest, KvGetRequest, KvGetResponse, KvMutationResponse, KvPutRequest,
        KvScanEntryDto, KvScanRequest, KvScanResponse,
    };
}

/// A retrying client for one ordered set of Rhiza node endpoints.
#[derive(Clone)]
pub struct RhizaClient {
    endpoints: Vec<String>,
    bearer_token: String,
    http: reqwest::Client,
    policy: ClientPolicy,
    #[cfg(feature = "tuner")]
    routing: Option<ClientRouting>,
}

impl RhizaClient {
    /// Creates a client with the fixed Rhiza transport deadlines.
    pub fn new(
        endpoints: impl IntoIterator<Item = String>,
        bearer_token: impl Into<String>,
    ) -> Result<Self, ClientError> {
        Self::build(endpoints, bearer_token, ClientPolicy::default())
    }

    /// Attach an explicitly identified, runtime-controlled routing tuner.
    ///
    /// The ordinary constructor remains static. Tuning is enabled only when the
    /// trusted snapshot maps every configured URL to one unique node id and its
    /// static primary matches the client's existing first endpoint.
    #[cfg(feature = "tuner")]
    pub fn with_routing_tuner(
        mut self,
        tuner: std::sync::Arc<rhiza_tuner::RoutingTuner>,
        snapshot: rhiza_tuner::RoutingSnapshot,
    ) -> Result<Self, ClientError> {
        self.validate_routing_snapshot(&snapshot)?;
        self.routing = Some(ClientRouting {
            tuner,
            snapshot: std::sync::Arc::new(std::sync::RwLock::new(snapshot)),
            cohort_id: uuid::Uuid::new_v4().to_string(),
        });
        Ok(self)
    }

    /// Replace the trusted routing snapshot used by subsequent requests.
    #[cfg(feature = "tuner")]
    pub fn update_routing_snapshot(
        &self,
        snapshot: rhiza_tuner::RoutingSnapshot,
    ) -> Result<(), ClientError> {
        self.validate_routing_snapshot(&snapshot)?;
        let routing = self
            .routing
            .as_ref()
            .ok_or_else(|| ClientError::invalid_configuration("routing tuner is not configured"))?;
        *routing
            .snapshot
            .write()
            .map_err(|_| ClientError::routing_state())? = snapshot;
        Ok(())
    }

    #[cfg(feature = "sql")]
    pub async fn write(
        &self,
        request: wire::WriteRequest,
    ) -> Result<wire::WriteResponse, ClientError> {
        self.json_request(rhiza_node::WRITE_PATH, &request, true)
            .await
    }

    #[cfg(feature = "sql")]
    pub async fn read(
        &self,
        request: wire::ReadRequest,
    ) -> Result<wire::ReadResponse, ClientError> {
        self.json_request(
            rhiza_node::READ_PATH,
            &request,
            read_can_hedge(request.consistency),
        )
        .await
    }

    #[cfg(feature = "sql")]
    pub async fn sql_execute(
        &self,
        request: wire::SqlExecuteRequest,
    ) -> Result<wire::SqlExecuteResponse, ClientError> {
        self.json_request(rhiza_node::SQL_EXECUTE_PATH, &request, true)
            .await
    }

    #[cfg(feature = "sql")]
    pub async fn sql_query(
        &self,
        request: wire::SqlQueryRequest,
    ) -> Result<wire::SqlQueryResponse, ClientError> {
        self.json_request(
            rhiza_node::SQL_QUERY_PATH,
            &request,
            read_can_hedge(request.consistency),
        )
        .await
    }

    #[cfg(feature = "graph")]
    pub async fn graph_query(
        &self,
        request: wire::GraphQueryRequest,
    ) -> Result<wire::GraphQueryResponse, ClientError> {
        self.json_request(
            rhiza_node::GRAPH_QUERY_PATH,
            &request,
            read_can_hedge(request.consistency),
        )
        .await
    }

    #[cfg(feature = "graph")]
    pub async fn graph_put_document(
        &self,
        request: wire::GraphPutDocumentRequest,
    ) -> Result<wire::GraphMutationResponse, ClientError> {
        self.json_request(rhiza_node::GRAPH_PUT_DOCUMENT_PATH, &request, true)
            .await
    }

    #[cfg(feature = "graph")]
    pub async fn graph_delete_document(
        &self,
        request: wire::GraphDeleteDocumentRequest,
    ) -> Result<wire::GraphMutationResponse, ClientError> {
        self.json_request(rhiza_node::GRAPH_DELETE_DOCUMENT_PATH, &request, true)
            .await
    }

    #[cfg(feature = "kv")]
    pub async fn kv_put(
        &self,
        request: wire::KvPutRequest,
    ) -> Result<wire::KvMutationResponse, ClientError> {
        self.json_request(rhiza_node::KV_PUT_PATH, &request, true)
            .await
    }

    #[cfg(feature = "kv")]
    pub async fn kv_delete(
        &self,
        request: wire::KvDeleteRequest,
    ) -> Result<wire::KvMutationResponse, ClientError> {
        self.json_request(rhiza_node::KV_DELETE_PATH, &request, true)
            .await
    }

    #[cfg(feature = "kv")]
    pub async fn kv_get(
        &self,
        request: wire::KvGetRequest,
    ) -> Result<wire::KvGetResponse, ClientError> {
        self.json_request(
            rhiza_node::KV_GET_PATH,
            &request,
            read_can_hedge(request.consistency),
        )
        .await
    }

    #[cfg(feature = "kv")]
    pub async fn kv_scan(
        &self,
        request: wire::KvScanRequest,
    ) -> Result<wire::KvScanResponse, ClientError> {
        self.json_request(
            rhiza_node::KV_SCAN_PATH,
            &request,
            read_can_hedge(request.consistency),
        )
        .await
    }

    #[cfg(feature = "kv")]
    pub async fn kv_batch(
        &self,
        request: wire::KvBatchRequest,
    ) -> Result<wire::KvBatchResponse, ClientError> {
        self.json_request(rhiza_node::KV_BATCH_PATH, &request, true)
            .await
    }

    fn build(
        endpoints: impl IntoIterator<Item = String>,
        bearer_token: impl Into<String>,
        policy: ClientPolicy,
    ) -> Result<Self, ClientError> {
        let endpoints = endpoints.into_iter().collect::<Vec<_>>();
        if endpoints.is_empty() {
            return Err(ClientError::invalid_configuration(
                "at least one endpoint is required",
            ));
        }
        if endpoints.iter().any(String::is_empty) {
            return Err(ClientError::invalid_configuration(
                "endpoint must not be empty",
            ));
        }
        let bearer_token = bearer_token.into();
        if bearer_token.is_empty() {
            return Err(ClientError::invalid_configuration(
                "bearer token must not be empty",
            ));
        }
        let http = reqwest::Client::builder()
            .connect_timeout(policy.connect_timeout)
            .build()
            .map_err(|_| ClientError::client_build())?;
        Ok(Self {
            endpoints,
            bearer_token,
            http,
            policy,
            #[cfg(feature = "tuner")]
            routing: None,
        })
    }

    #[cfg(feature = "tuner")]
    fn validate_routing_snapshot(
        &self,
        snapshot: &rhiza_tuner::RoutingSnapshot,
    ) -> Result<(), ClientError> {
        if self
            .endpoints
            .iter()
            .enumerate()
            .any(|(index, endpoint)| self.endpoints[..index].contains(endpoint))
        {
            return Err(ClientError::invalid_configuration(
                "routing tuner requires unique endpoint URLs",
            ));
        }
        if snapshot.endpoints().len() != self.endpoints.len()
            || self
                .endpoints
                .iter()
                .zip(snapshot.endpoints())
                .any(|(configured, mapped)| configured != mapped.url())
        {
            return Err(ClientError::invalid_configuration(
                "routing snapshot must map every configured endpoint exactly once",
            ));
        }
        let primary_url = snapshot
            .endpoints()
            .iter()
            .find(|endpoint| endpoint.node_id() == snapshot.static_primary())
            .map(rhiza_tuner::RoutingEndpoint::url);
        if primary_url != self.endpoints.first().map(String::as_str) {
            return Err(ClientError::invalid_configuration(
                "routing snapshot static primary must match the first configured endpoint",
            ));
        }
        Ok(())
    }

    #[cfg(all(test, feature = "sql"))]
    fn with_policy(
        endpoints: impl IntoIterator<Item = String>,
        bearer_token: impl Into<String>,
        policy: ClientPolicy,
    ) -> Result<Self, ClientError> {
        Self::build(endpoints, bearer_token, policy)
    }

    async fn json_request<B, T>(
        &self,
        path: &str,
        request: &B,
        hedge: bool,
    ) -> Result<T, ClientError>
    where
        B: Serialize,
        T: DeserializeOwned + Send + 'static,
    {
        let body = serde_json::to_vec(request).map_err(|_| ClientError::request_encoding())?;

        #[cfg(feature = "tuner")]
        let (ordered_endpoints, hedge_delay_duration, routing_observation) =
            if let Some(routing) = &self.routing {
                match routing.snapshot.read() {
                    Ok(snapshot) => {
                        let snapshot = snapshot.clone();
                        let correlation_id = uuid::Uuid::new_v4().to_string();
                        let plan = routing.tuner.plan_for_cohort(
                            &snapshot,
                            routing_request_class(body.len()),
                            &correlation_id,
                            &routing.cohort_id,
                        );
                        match routing_urls(&snapshot, &plan.actual_order) {
                            Some(ordered) => (
                                ordered,
                                plan.hedge_delay,
                                Some(ClientRoutingObservation {
                                    routing: routing.clone(),
                                    planned_snapshot: snapshot,
                                    correlation_id,
                                    ticket: plan.ticket,
                                }),
                            ),
                            None => {
                                let _ = routing.tuner.observe(
                                    plan.ticket,
                                    &snapshot,
                                    rhiza_tuner::ExecutionTrace::new(
                                        correlation_id,
                                        Vec::new(),
                                        rhiza_tuner::AttemptOutcome::Cancelled,
                                    ),
                                );
                                (self.endpoints.clone(), self.policy.hedge_delay, None)
                            }
                        }
                    }
                    Err(_) => (self.endpoints.clone(), self.policy.hedge_delay, None),
                }
            } else {
                (self.endpoints.clone(), self.policy.hedge_delay, None)
            };

        #[cfg(not(feature = "tuner"))]
        let (ordered_endpoints, hedge_delay_duration): (Vec<String>, Duration) =
            (self.endpoints.clone(), self.policy.hedge_delay);

        #[cfg(feature = "tuner")]
        let (mut ordered_endpoints, mut hedge_delay_duration, mut routing_observation) =
            (ordered_endpoints, hedge_delay_duration, routing_observation);

        let operation_start = tokio::time::Instant::now();
        let mut attempts = tokio::task::JoinSet::new();
        let mut attempt_records = Vec::new();
        let mut next = 0;
        let mut last_error = None;

        #[cfg(feature = "tuner")]
        let attempt = {
            let launch_routing = routing_observation
                .as_ref()
                .map(|observation| observation.routing.clone());
            let planned_snapshot = routing_observation
                .as_ref()
                .map(|observation| observation.planned_snapshot.clone());
            let tuned_attempt = ClientAttempt {
                client: self.http.clone(),
                token: self.bearer_token.clone(),
                path: path.to_owned(),
                body: body.clone(),
                server_fields: ServerFieldSanitizer::new(&ordered_endpoints, &self.bearer_token),
                timeout: self.policy.attempt_timeout,
            };
            let launched = launch_routing.as_ref().is_some_and(|routing| {
                routing.launch_if_current(
                    planned_snapshot
                        .as_ref()
                        .expect("routing plan has a snapshot"),
                    || {
                        spawn_client_attempt(
                            &mut attempts,
                            &mut attempt_records,
                            &ordered_endpoints[next],
                            &tuned_attempt,
                            operation_start,
                            ClientAttemptTrigger::Initial,
                        );
                    },
                )
            });
            if launched {
                next += 1;
                tuned_attempt
            } else {
                if let Some(observation) = routing_observation.take() {
                    observation.censor();
                }
                ordered_endpoints = self.endpoints.clone();
                hedge_delay_duration = self.policy.hedge_delay;
                let static_attempt = ClientAttempt {
                    client: self.http.clone(),
                    token: self.bearer_token.clone(),
                    path: path.to_owned(),
                    body,
                    server_fields: ServerFieldSanitizer::new(
                        &ordered_endpoints,
                        &self.bearer_token,
                    ),
                    timeout: self.policy.attempt_timeout,
                };
                spawn_client_attempt(
                    &mut attempts,
                    &mut attempt_records,
                    &ordered_endpoints[next],
                    &static_attempt,
                    operation_start,
                    ClientAttemptTrigger::Initial,
                );
                next += 1;
                static_attempt
            }
        };

        #[cfg(not(feature = "tuner"))]
        let attempt = {
            let attempt = ClientAttempt {
                client: self.http.clone(),
                token: self.bearer_token.clone(),
                path: path.to_owned(),
                body,
                server_fields: ServerFieldSanitizer::new(&ordered_endpoints, &self.bearer_token),
                timeout: self.policy.attempt_timeout,
            };
            spawn_client_attempt(
                &mut attempts,
                &mut attempt_records,
                &ordered_endpoints[next],
                &attempt,
                operation_start,
                ClientAttemptTrigger::Initial,
            );
            next += 1;
            attempt
        };

        let hedge_delay = tokio::time::sleep(hedge_delay_duration);
        let operation_deadline = tokio::time::sleep(self.policy.operation_timeout);
        tokio::pin!(hedge_delay, operation_deadline);

        let result = loop {
            if attempts.is_empty() && next == ordered_endpoints.len() {
                break Err(last_error.unwrap_or_else(ClientError::missing_endpoint));
            }

            tokio::select! {
                result = attempts.join_next(), if !attempts.is_empty() => {
                    match result.expect("a nonempty attempt set must yield a result") {
                        Ok(completed) => {
                            let result = completed.result;
                            attempt_records[completed.attempt_index].completed_after = Some(operation_start.elapsed());
                            attempt_records[completed.attempt_index].outcome = Some(ClientAttemptOutcome::from_result(&result));
                            match result {
                                Ok(response) => {
                                    attempts.abort_all();
                                    break Ok(response);
                                }
                                Err(error) if !error.retryable() => {
                                    attempts.abort_all();
                                    break Err(error);
                                }
                                Err(error) => {
                                    last_error = Some(error);
                                    if let Some(endpoint) = ordered_endpoints.get(next) {
                                        spawn_client_attempt(
                                            &mut attempts,
                                            &mut attempt_records,
                                            endpoint,
                                            &attempt,
                                            operation_start,
                                            ClientAttemptTrigger::Retry,
                                        );
                                        next += 1;
                                        if hedge && next < ordered_endpoints.len() {
                                            hedge_delay.as_mut().reset(
                                                tokio::time::Instant::now() + hedge_delay_duration,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            attempts.abort_all();
                            break Err(ClientError::attempt_task_failed());
                        }
                    }
                }
                () = &mut hedge_delay, if hedge && next < ordered_endpoints.len() => {
                    spawn_client_attempt(
                        &mut attempts,
                        &mut attempt_records,
                        &ordered_endpoints[next],
                        &attempt,
                        operation_start,
                        ClientAttemptTrigger::TimerHedge,
                    );
                    next += 1;
                    if next < ordered_endpoints.len() {
                        hedge_delay.as_mut().reset(
                            tokio::time::Instant::now() + hedge_delay_duration,
                        );
                    }
                }
                () = &mut operation_deadline => {
                    attempts.abort_all();
                    break Err(last_error.unwrap_or_else(ClientError::operation_deadline));
                }
            }
        };

        #[cfg(feature = "tuner")]
        if let Some(observation) = routing_observation {
            observation.observe(&attempt_records, &result, operation_start.elapsed());
        }

        result
    }
}

#[cfg(feature = "tuner")]
#[derive(Clone)]
struct ClientRouting {
    tuner: std::sync::Arc<rhiza_tuner::RoutingTuner>,
    snapshot: std::sync::Arc<std::sync::RwLock<rhiza_tuner::RoutingSnapshot>>,
    cohort_id: String,
}

#[cfg(feature = "tuner")]
impl ClientRouting {
    fn launch_if_current(
        &self,
        planned: &rhiza_tuner::RoutingSnapshot,
        launch: impl FnOnce(),
    ) -> bool {
        let Ok(current) = self.snapshot.read() else {
            return false;
        };
        if current.identity() != planned.identity()
            || current.topology_generation() != planned.topology_generation()
        {
            return false;
        }
        launch();
        true
    }
}

#[cfg(feature = "tuner")]
struct ClientRoutingObservation {
    routing: ClientRouting,
    planned_snapshot: rhiza_tuner::RoutingSnapshot,
    correlation_id: String,
    ticket: rhiza_tuner::ObservationTicket,
}

#[cfg(feature = "tuner")]
impl ClientRoutingObservation {
    fn censor(self) {
        let trace = rhiza_tuner::ExecutionTrace::new(
            self.correlation_id,
            Vec::new(),
            rhiza_tuner::AttemptOutcome::Cancelled,
        );
        match self.routing.snapshot.read() {
            Ok(snapshot) => {
                let _ = self.routing.tuner.observe(self.ticket, &snapshot, trace);
            }
            Err(_) => {
                let _ = self
                    .routing
                    .tuner
                    .observe(self.ticket, &self.planned_snapshot, trace);
            }
        }
    }

    fn observe<T>(
        self,
        attempts: &[ClientAttemptRecord],
        result: &Result<T, ClientError>,
        elapsed: Duration,
    ) {
        let traces = attempts
            .iter()
            .map(|attempt| {
                let node_id = self
                    .planned_snapshot
                    .endpoints()
                    .iter()
                    .find(|endpoint| endpoint.url() == attempt.endpoint)
                    .map(|endpoint| endpoint.node_id().clone())?;
                let outcome = match attempt.outcome {
                    Some(ClientAttemptOutcome::Success) => rhiza_tuner::AttemptOutcome::Success {
                        latency: attempt
                            .completed_after
                            .unwrap_or(elapsed)
                            .saturating_sub(attempt.started_after),
                    },
                    Some(ClientAttemptOutcome::Timeout) => rhiza_tuner::AttemptOutcome::Timeout,
                    Some(
                        ClientAttemptOutcome::RetryableError
                        | ClientAttemptOutcome::NonRetryableError,
                    ) => rhiza_tuner::AttemptOutcome::Error,
                    None => rhiza_tuner::AttemptOutcome::Cancelled,
                };
                Some(rhiza_tuner::AttemptTrace::with_timing(
                    node_id,
                    attempt.started_after,
                    attempt.completed_after,
                    outcome,
                ))
            })
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        let hedge_launched_at = attempts
            .iter()
            .find(|attempt| matches!(attempt.trigger, ClientAttemptTrigger::TimerHedge))
            .map(|attempt| attempt.started_after);
        let winner = traces.iter().find_map(|attempt| {
            matches!(attempt.outcome, rhiza_tuner::AttemptOutcome::Success { .. })
                .then(|| attempt.node_id.clone())
        });
        let terminal_outcome = match result {
            Ok(_) => rhiza_tuner::AttemptOutcome::Success { latency: elapsed },
            Err(error) if error.is_deadline() => rhiza_tuner::AttemptOutcome::Timeout,
            Err(error) if error.code() == "attempt_task_failed" => {
                rhiza_tuner::AttemptOutcome::Cancelled
            }
            Err(_) => rhiza_tuner::AttemptOutcome::Error,
        };
        match self.routing.snapshot.read() {
            Ok(snapshot) => {
                let trace = rhiza_tuner::ExecutionTrace::with_metadata(
                    self.correlation_id,
                    traces,
                    hedge_launched_at,
                    winner,
                    terminal_outcome,
                );
                let _ = self.routing.tuner.observe(self.ticket, &snapshot, trace);
            }
            Err(_) => {
                let trace = rhiza_tuner::ExecutionTrace::with_metadata(
                    self.correlation_id,
                    traces,
                    hedge_launched_at,
                    winner,
                    rhiza_tuner::AttemptOutcome::Cancelled,
                );
                let _ = self
                    .routing
                    .tuner
                    .observe(self.ticket, &self.planned_snapshot, trace);
            }
        }
    }
}

#[cfg(feature = "tuner")]
fn routing_urls(
    snapshot: &rhiza_tuner::RoutingSnapshot,
    order: &[rhiza_tuner::NodeId],
) -> Option<Vec<String>> {
    order
        .iter()
        .map(|node_id| {
            snapshot
                .endpoints()
                .iter()
                .find(|endpoint| endpoint.node_id() == node_id)
                .map(|endpoint| endpoint.url().to_owned())
        })
        .collect()
}

#[cfg(feature = "tuner")]
fn routing_request_class(body_len: usize) -> rhiza_tuner::RequestClass {
    let size = if body_len <= 4 * 1024 {
        rhiza_tuner::SizeBucket::Small
    } else if body_len <= 64 * 1024 {
        rhiza_tuner::SizeBucket::Medium
    } else {
        rhiza_tuner::SizeBucket::Large
    };
    rhiza_tuner::RequestClass {
        durability: rhiza_tuner::DurabilityMode::Sync,
        size,
    }
}

#[derive(Clone, Copy)]
struct ClientPolicy {
    connect_timeout: Duration,
    attempt_timeout: Duration,
    operation_timeout: Duration,
    hedge_delay: Duration,
}

impl Default for ClientPolicy {
    fn default() -> Self {
        Self {
            connect_timeout: CONNECT_TIMEOUT,
            attempt_timeout: ATTEMPT_TIMEOUT,
            operation_timeout: OPERATION_TIMEOUT,
            hedge_delay: HEDGE_DELAY,
        }
    }
}

fn read_can_hedge(consistency: Option<rhiza_node::ReadConsistency>) -> bool {
    matches!(
        consistency,
        Some(rhiza_node::ReadConsistency::Local | rhiza_node::ReadConsistency::AppliedIndex(_))
    )
}

#[derive(Clone)]
struct ServerFieldSanitizer {
    bearer_token: String,
    endpoints: Vec<String>,
}

impl ServerFieldSanitizer {
    fn new(endpoints: &[String], bearer_token: &str) -> Self {
        Self {
            bearer_token: bearer_token.to_owned(),
            endpoints: endpoints.to_vec(),
        }
    }

    fn contains_sensitive_value(&self, value: &str) -> bool {
        value.contains(&self.bearer_token)
            || self
                .endpoints
                .iter()
                .any(|endpoint| value.contains(endpoint))
    }

    fn message(&self, message: Option<String>) -> Option<String> {
        message.filter(|message| !message.is_empty() && !self.contains_sensitive_value(message))
    }
}

#[derive(Clone)]
struct ClientAttempt {
    client: reqwest::Client,
    token: String,
    path: String,
    body: Vec<u8>,
    server_fields: ServerFieldSanitizer,
    timeout: Duration,
}

fn spawn_client_attempt<T>(
    attempts: &mut tokio::task::JoinSet<ClientAttemptResult<T>>,
    records: &mut Vec<ClientAttemptRecord>,
    endpoint: &str,
    attempt: &ClientAttempt,
    operation_start: tokio::time::Instant,
    trigger: ClientAttemptTrigger,
) where
    T: DeserializeOwned + Send + 'static,
{
    let attempt_index = records.len();
    records.push(ClientAttemptRecord {
        endpoint: endpoint.to_owned(),
        started_after: operation_start.elapsed(),
        completed_after: None,
        outcome: None,
        trigger,
    });
    let endpoint = endpoint.to_string();
    let attempt = attempt.clone();
    attempts.spawn(async move {
        let result = tokio::time::timeout(attempt.timeout, async {
            let response =
                protocol_request(&attempt.client, Method::POST, &endpoint, &attempt.path)
                    .bearer_auth(attempt.token)
                    .body(attempt.body)
                    .send()
                    .await
                    .map_err(ClientError::transport)?;
            client_attempt_response(response, &attempt.server_fields).await
        })
        .await
        .unwrap_or_else(|_| Err(ClientError::attempt_deadline()));
        ClientAttemptResult {
            attempt_index,
            result,
        }
    });
}

struct ClientAttemptResult<T> {
    attempt_index: usize,
    result: Result<T, ClientError>,
}

#[cfg_attr(not(feature = "tuner"), allow(dead_code))]
struct ClientAttemptRecord {
    endpoint: String,
    started_after: Duration,
    completed_after: Option<Duration>,
    outcome: Option<ClientAttemptOutcome>,
    trigger: ClientAttemptTrigger,
}

#[derive(Clone, Copy)]
enum ClientAttemptTrigger {
    Initial,
    Retry,
    TimerHedge,
}

#[derive(Clone, Copy)]
enum ClientAttemptOutcome {
    Success,
    Timeout,
    RetryableError,
    NonRetryableError,
}

impl ClientAttemptOutcome {
    fn from_result<T>(result: &Result<T, ClientError>) -> Self {
        match result {
            Ok(_) => Self::Success,
            Err(error) if error.is_deadline() => Self::Timeout,
            Err(error) if error.retryable() => Self::RetryableError,
            Err(_) => Self::NonRetryableError,
        }
    }
}

async fn client_attempt_response<T: DeserializeOwned>(
    response: Response,
    server_fields: &ServerFieldSanitizer,
) -> Result<T, ClientError> {
    let status = response.status();
    let body = response.bytes().await.map_err(ClientError::transport)?;
    if status.is_success() {
        return serde_json::from_slice(&body).map_err(|_| ClientError::invalid_response());
    }
    let Ok(error) = serde_json::from_slice::<ServerErrorResponse>(&body) else {
        return Err(ClientError::http_status(status));
    };
    Err(ClientError::server(status, error, server_fields))
}

fn protocol_request(
    client: &reqwest::Client,
    method: Method,
    endpoint: &str,
    path: &str,
) -> reqwest::RequestBuilder {
    client
        .request(method, format!("{}{path}", endpoint.trim_end_matches('/')))
        .header(rhiza_node::VERSION_HEADER, rhiza_node::PROTOCOL_VERSION)
        .header(header::ACCEPT, "application/json")
        .header(header::CONTENT_TYPE, "application/json")
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerErrorResponse {
    code: String,
    retryable: bool,
    message: String,
    statement_index: Option<usize>,
}

/// A public client failure with stable retry guidance.
#[derive(Clone, Debug)]
pub struct ClientError {
    classification: ErrorClassification,
    statement_index: Option<usize>,
    detail: ClientErrorDetail,
}

#[derive(Clone, Debug)]
enum ClientErrorDetail {
    InvalidConfiguration(&'static str),
    ClientBuild,
    RequestEncoding,
    MissingEndpoint,
    Transport(String),
    AttemptDeadline,
    AttemptTaskFailed,
    OperationDeadline,
    InvalidResponse,
    Http {
        status: StatusCode,
        message: Option<String>,
    },
}

impl ClientError {
    fn new(
        code: impl Into<String>,
        category: ErrorCategory,
        retryable: bool,
        statement_index: Option<usize>,
        detail: ClientErrorDetail,
    ) -> Self {
        Self {
            classification: ErrorClassification::new(code, category, retryable),
            statement_index,
            detail,
        }
    }

    fn invalid_configuration(message: &'static str) -> Self {
        Self::new(
            "invalid_client_configuration",
            ErrorCategory::InvalidRequest,
            false,
            None,
            ClientErrorDetail::InvalidConfiguration(message),
        )
    }

    fn client_build() -> Self {
        Self::new(
            "client_build_failed",
            ErrorCategory::Internal,
            false,
            None,
            ClientErrorDetail::ClientBuild,
        )
    }

    #[cfg(feature = "tuner")]
    fn routing_state() -> Self {
        Self::new(
            "routing_state_unavailable",
            ErrorCategory::Internal,
            false,
            None,
            ClientErrorDetail::ClientBuild,
        )
    }

    fn request_encoding() -> Self {
        Self::new(
            "request_encoding_failed",
            ErrorCategory::InvalidRequest,
            false,
            None,
            ClientErrorDetail::RequestEncoding,
        )
    }

    fn missing_endpoint() -> Self {
        Self::new(
            "missing_endpoint",
            ErrorCategory::Unavailable,
            true,
            None,
            ClientErrorDetail::MissingEndpoint,
        )
    }

    fn transport(error: reqwest::Error) -> Self {
        Self::new(
            "transport_error",
            ErrorCategory::Unavailable,
            true,
            None,
            ClientErrorDetail::Transport(safe_transport_detail(error)),
        )
    }

    fn attempt_deadline() -> Self {
        Self::new(
            "attempt_deadline_exceeded",
            ErrorCategory::Unavailable,
            true,
            None,
            ClientErrorDetail::AttemptDeadline,
        )
    }

    fn attempt_task_failed() -> Self {
        Self::new(
            "attempt_task_failed",
            ErrorCategory::Internal,
            false,
            None,
            ClientErrorDetail::AttemptTaskFailed,
        )
    }

    fn operation_deadline() -> Self {
        Self::new(
            "operation_deadline_exceeded",
            ErrorCategory::Unavailable,
            true,
            None,
            ClientErrorDetail::OperationDeadline,
        )
    }

    fn invalid_response() -> Self {
        Self::new(
            "invalid_response",
            ErrorCategory::Internal,
            false,
            None,
            ClientErrorDetail::InvalidResponse,
        )
    }

    fn http_status(status: StatusCode) -> Self {
        let retryable = matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        );
        Self::new(
            "http_error",
            ErrorCategory::Internal,
            retryable,
            None,
            ClientErrorDetail::Http {
                status,
                message: None,
            },
        )
    }

    fn server(
        status: StatusCode,
        error: ServerErrorResponse,
        server_fields: &ServerFieldSanitizer,
    ) -> Self {
        let classification =
            ErrorClassification::from_server_code(error.code.clone(), error.retryable);
        let classification = if classification.category() == ErrorCategory::Unknown
            && server_fields.contains_sensitive_value(&error.code)
        {
            ErrorClassification::from_server_code(SANITIZED_UNKNOWN_SERVER_CODE, error.retryable)
        } else {
            classification
        };
        Self {
            classification,
            statement_index: error.statement_index,
            detail: ClientErrorDetail::Http {
                status,
                message: server_fields.message(Some(error.message)),
            },
        }
    }

    /// Returns the server or client classification without endpoint or credential data.
    pub fn classification(&self) -> &ErrorClassification {
        &self.classification
    }

    pub fn code(&self) -> &str {
        self.classification.code()
    }

    pub const fn category(&self) -> ErrorCategory {
        self.classification.category()
    }

    pub const fn retryable(&self) -> bool {
        self.classification.retryable()
    }

    fn is_deadline(&self) -> bool {
        matches!(
            self.code(),
            "attempt_deadline_exceeded" | "operation_deadline_exceeded"
        )
    }

    pub const fn statement_index(&self) -> Option<usize> {
        self.statement_index
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.detail {
            ClientErrorDetail::InvalidConfiguration(message) => message.fmt(formatter),
            ClientErrorDetail::ClientBuild => write!(formatter, "cannot build HTTP client"),
            ClientErrorDetail::RequestEncoding => write!(formatter, "cannot encode request"),
            ClientErrorDetail::MissingEndpoint => write!(formatter, "missing request endpoint"),
            ClientErrorDetail::Transport(detail) => {
                write!(formatter, "request failed: {detail}")
            }
            ClientErrorDetail::AttemptDeadline => {
                write!(formatter, "request failed: attempt deadline exceeded")
            }
            ClientErrorDetail::AttemptTaskFailed => write!(formatter, "request task failed"),
            ClientErrorDetail::OperationDeadline => {
                write!(formatter, "request failed: operation deadline exceeded")
            }
            ClientErrorDetail::InvalidResponse => write!(formatter, "invalid JSON response"),
            ClientErrorDetail::Http { status, message } => {
                write!(formatter, "HTTP {status} code={}", self.code())?;
                if let Some(message) = message {
                    write!(formatter, " message={message}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ClientError {}

fn safe_transport_detail(error: reqwest::Error) -> String {
    let error = error.without_url();
    let mut details = vec![error.to_string()];
    let mut source = error.source();
    while let Some(cause) = source {
        let detail = cause.to_string();
        if !detail.is_empty() && !details.contains(&detail) {
            details.push(detail);
        }
        source = cause.source();
    }
    details.join(": ")
}

#[cfg(all(test, feature = "kv"))]
mod kv_tests {
    use std::sync::{Arc, Mutex};

    use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
    use rhiza_core::LogHash;

    use super::{wire::*, RhizaClient};

    type CapturedScan = Arc<Mutex<Option<(HeaderMap, KvScanRequest)>>>;

    #[tokio::test]
    async fn kv_scan_sends_the_typed_request_with_auth() {
        let captured = Arc::new(Mutex::new(None));
        let app = Router::new()
            .route(
                rhiza_node::KV_SCAN_PATH,
                post(
                    |State(captured): State<CapturedScan>,
                     headers: HeaderMap,
                     Json(request): Json<KvScanRequest>| async move {
                        *captured.lock().unwrap() = Some((headers, request));
                        Json(KvScanResponse {
                            entries: Vec::new(),
                            next_cursor: None,
                            applied_index: 7,
                            hash: LogHash::ZERO,
                        })
                    },
                ),
            )
            .with_state(Arc::clone(&captured));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let response = RhizaClient::new([endpoint], "client-secret")
            .unwrap()
            .kv_scan(KvScanRequest {
                start: None,
                end: None,
                prefix: Some("/w==".into()),
                cursor: None,
                limit: Some(10),
                consistency: Some(ReadConsistency::Local),
            })
            .await
            .unwrap();

        server.abort();
        assert_eq!(response.applied_index, 7);
        let captured = captured.lock().unwrap();
        let (headers, request) = captured.as_ref().unwrap();
        assert_eq!(headers["authorization"], "Bearer client-secret");
        assert_eq!(
            headers[rhiza_node::VERSION_HEADER],
            rhiza_node::PROTOCOL_VERSION
        );
        assert_eq!(request.prefix.as_deref(), Some("/w=="));
        assert_eq!(request.limit, Some(10));
    }
}

#[cfg(all(test, feature = "sql"))]
mod tests {
    use std::{
        future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };

    use axum::{
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::post,
        Json, Router,
    };
    use rhiza_core::{ErrorCategory, LogHash};
    use tokio::sync::{mpsc, Notify};

    use super::{wire::*, *};

    fn write_request() -> WriteRequest {
        WriteRequest {
            request_id: "request-1".into(),
            key: "alpha".into(),
            value: "one".into(),
        }
    }

    fn write_response(index: u64) -> Json<WriteResponse> {
        Json(WriteResponse {
            applied_index: index,
            hash: LogHash::ZERO,
        })
    }

    fn test_client(endpoints: Vec<String>, policy: ClientPolicy) -> RhizaClient {
        RhizaClient::with_policy(endpoints, "client-secret", policy).unwrap()
    }

    fn test_policy() -> ClientPolicy {
        ClientPolicy {
            connect_timeout: Duration::from_millis(20),
            attempt_timeout: Duration::from_millis(250),
            operation_timeout: Duration::from_millis(500),
            hedge_delay: Duration::from_millis(10),
        }
    }

    async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (endpoint, task)
    }

    #[cfg(feature = "tuner")]
    fn routing_snapshot(first: &str, second: &str) -> rhiza_tuner::RoutingSnapshot {
        routing_snapshot_at(first, second, 1)
    }

    #[cfg(feature = "tuner")]
    fn routing_snapshot_at(
        first: &str,
        second: &str,
        topology_generation: u64,
    ) -> rhiza_tuner::RoutingSnapshot {
        let identity = rhiza_tuner::LearningIdentity::new(
            rhiza_tuner::Identity {
                cluster_id: "test-cluster".into(),
                epoch: 1,
                config_id: 1,
                membership_digest: [7; 32],
                recovery_generation: 0,
            },
            1,
        );
        rhiza_tuner::RoutingSnapshot::new(
            identity,
            topology_generation,
            vec![
                rhiza_tuner::RoutingEndpoint::new("node-a".into(), first, true),
                rhiza_tuner::RoutingEndpoint::new("node-b".into(), second, true),
            ],
            "node-a".into(),
        )
        .unwrap()
    }

    #[cfg(feature = "tuner")]
    fn train_routing_target(
        tuner: &rhiza_tuner::RoutingTuner,
        snapshot: &rhiza_tuner::RoutingSnapshot,
        correlation_id: &str,
        latency: Duration,
    ) {
        let plan = tuner.plan(
            snapshot,
            rhiza_tuner::RequestClass {
                durability: rhiza_tuner::DurabilityMode::Sync,
                size: rhiza_tuner::SizeBucket::Small,
            },
            correlation_id,
        );
        let target = plan.actual_order[0].clone();
        assert_eq!(
            tuner.observe(
                plan.ticket,
                snapshot,
                rhiza_tuner::ExecutionTrace::new(
                    correlation_id,
                    vec![rhiza_tuner::AttemptTrace::new(
                        target,
                        rhiza_tuner::AttemptOutcome::Success { latency },
                    )],
                    rhiza_tuner::AttemptOutcome::Success { latency },
                ),
            ),
            rhiza_tuner::ObservationResult::Updated,
        );
    }

    #[derive(Clone, Default)]
    struct CapturedRequests(Arc<Mutex<Vec<(HeaderMap, String)>>>);

    #[tokio::test]
    async fn write_sends_protocol_version_bearer_auth_and_json_body() {
        let captured = CapturedRequests::default();
        let app =
            Router::new()
                .route(
                    rhiza_node::WRITE_PATH,
                    post(
                        |State(captured): State<CapturedRequests>,
                         headers: HeaderMap,
                         body: String| async move {
                            captured.0.lock().unwrap().push((headers, body));
                            write_response(1)
                        },
                    ),
                )
                .with_state(captured.clone());
        let (endpoint, server) = serve(app).await;

        let response = RhizaClient::new(vec![endpoint], "client-secret")
            .unwrap()
            .write(write_request())
            .await
            .unwrap();

        server.abort();
        assert_eq!(response.applied_index, 1);
        let captured = captured.0.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].0[rhiza_node::VERSION_HEADER],
            rhiza_node::PROTOCOL_VERSION
        );
        assert_eq!(captured[0].0["authorization"], "Bearer client-secret");
        assert_eq!(captured[0].0["content-type"], "application/json");
        assert_eq!(
            captured[0].1,
            serde_json::to_string(&write_request()).unwrap(),
        );
    }

    #[cfg(feature = "tuner")]
    #[tokio::test]
    async fn routing_tuner_uses_trusted_node_mapping_and_applied_target() {
        let first_app =
            Router::new().route(rhiza_node::WRITE_PATH, post(|| async { write_response(1) }));
        let (first, first_server) = serve(first_app).await;
        let second_app =
            Router::new().route(rhiza_node::WRITE_PATH, post(|| async { write_response(2) }));
        let (second, second_server) = serve(second_app).await;
        let snapshot = routing_snapshot(&first, &second);
        let tuner = Arc::new(rhiza_tuner::RoutingTuner::with_stage(
            rhiza_tuner::RoutingConfig {
                canary_basis_points: 10_000,
                ..Default::default()
            },
            rhiza_tuner::RolloutStage::ProposerCanary,
        ));

        train_routing_target(&tuner, &snapshot, "baseline", Duration::from_millis(100));
        train_routing_target(&tuner, &snapshot, "explore-b", Duration::from_millis(1));

        let response = RhizaClient::new(vec![first, second], "client-secret")
            .unwrap()
            .with_routing_tuner(tuner, snapshot)
            .unwrap()
            .write(write_request())
            .await
            .unwrap();

        first_server.abort();
        second_server.abort();
        assert_eq!(response.applied_index, 2);
    }

    #[cfg(feature = "tuner")]
    #[tokio::test]
    async fn shadow_routing_keeps_the_static_endpoint_order() {
        let first_app =
            Router::new().route(rhiza_node::WRITE_PATH, post(|| async { write_response(1) }));
        let (first, first_server) = serve(first_app).await;
        let second_app =
            Router::new().route(rhiza_node::WRITE_PATH, post(|| async { write_response(2) }));
        let (second, second_server) = serve(second_app).await;
        let snapshot = routing_snapshot(&first, &second);
        let tuner = Arc::new(rhiza_tuner::RoutingTuner::with_stage(
            rhiza_tuner::RoutingConfig::default(),
            rhiza_tuner::RolloutStage::Shadow,
        ));

        let response = RhizaClient::new(vec![first, second], "client-secret")
            .unwrap()
            .with_routing_tuner(tuner, snapshot)
            .unwrap()
            .write(write_request())
            .await
            .unwrap();

        first_server.abort();
        second_server.abort();
        assert_eq!(response.applied_index, 1);
    }

    #[cfg(feature = "tuner")]
    #[tokio::test]
    async fn topology_update_censors_an_in_flight_observation() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let entered_signal = Arc::clone(&entered);
        let release_signal = Arc::clone(&release);
        let first_app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(move || {
                let entered = Arc::clone(&entered_signal);
                let release = Arc::clone(&release_signal);
                async move {
                    entered.notify_one();
                    release.notified().await;
                    write_response(1)
                }
            }),
        );
        let (first, first_server) = serve(first_app).await;
        let second_app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(|| async { future::pending::<Json<WriteResponse>>().await }),
        );
        let (second, second_server) = serve(second_app).await;
        let tuner = Arc::new(rhiza_tuner::RoutingTuner::with_stage(
            rhiza_tuner::RoutingConfig {
                canary_basis_points: 10_000,
                ..Default::default()
            },
            rhiza_tuner::RolloutStage::ProposerCanary,
        ));
        let client = RhizaClient::new(vec![first.clone(), second.clone()], "client-secret")
            .unwrap()
            .with_routing_tuner(Arc::clone(&tuner), routing_snapshot_at(&first, &second, 1))
            .unwrap();
        let request_client = client.clone();
        let operation = tokio::spawn(async move { request_client.write(write_request()).await });

        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("the planned endpoint receives the request");
        client
            .update_routing_snapshot(routing_snapshot_at(&first, &second, 2))
            .unwrap();
        release.notify_one();
        let response = operation.await.unwrap().unwrap();

        first_server.abort();
        second_server.abort();
        assert_eq!(response.applied_index, 1);
        let metrics = tuner.metrics();
        assert_eq!(metrics.updated_observations, 0);
        assert_eq!(metrics.topology_mismatches, 1);
    }

    #[cfg(feature = "tuner")]
    #[test]
    fn routing_snapshot_must_match_the_static_client_configuration() {
        let client = RhizaClient::new(
            vec!["http://first".into(), "http://second".into()],
            "client-secret",
        )
        .unwrap();
        let mismatched = routing_snapshot("http://first", "http://different");
        let tuner = Arc::new(rhiza_tuner::RoutingTuner::new(
            rhiza_tuner::RoutingConfig::default(),
        ));

        let error = match client.with_routing_tuner(tuner, mismatched) {
            Ok(_) => panic!("mismatched routing snapshot must be rejected"),
            Err(error) => error,
        };

        assert_eq!(error.code(), "invalid_client_configuration");

        let identity = rhiza_tuner::LearningIdentity::new(
            rhiza_tuner::Identity {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                membership_digest: [0; 32],
                recovery_generation: 0,
            },
            1,
        );
        let reordered = rhiza_tuner::RoutingSnapshot::new(
            identity,
            1,
            vec![
                rhiza_tuner::RoutingEndpoint::new("node-a".into(), "http://first", true),
                rhiza_tuner::RoutingEndpoint::new("node-c".into(), "http://third", true),
                rhiza_tuner::RoutingEndpoint::new("node-b".into(), "http://second", true),
            ],
            "node-a".into(),
        )
        .unwrap();
        let client = RhizaClient::new(
            vec![
                "http://first".into(),
                "http://second".into(),
                "http://third".into(),
            ],
            "client-secret",
        )
        .unwrap();
        assert!(client
            .with_routing_tuner(
                Arc::new(rhiza_tuner::RoutingTuner::new(Default::default())),
                reordered,
            )
            .is_err());

        let duplicate_client = RhizaClient::new(
            vec!["http://first".into(), "http://first".into()],
            "client-secret",
        )
        .unwrap();
        let duplicate_snapshot = routing_snapshot("http://first", "http://second");
        assert!(duplicate_client
            .with_routing_tuner(
                Arc::new(rhiza_tuner::RoutingTuner::new(Default::default())),
                duplicate_snapshot,
            )
            .is_err());
    }

    #[cfg(feature = "tuner")]
    #[test]
    fn client_routing_uses_one_stable_canary_cohort() {
        let snapshot = routing_snapshot("http://first", "http://second");
        let tuner = Arc::new(rhiza_tuner::RoutingTuner::with_stage(
            rhiza_tuner::RoutingConfig {
                canary_basis_points: 5_000,
                ..Default::default()
            },
            rhiza_tuner::RolloutStage::ProposerCanary,
        ));
        let client = RhizaClient::new(
            vec!["http://first".into(), "http://second".into()],
            "client-secret",
        )
        .unwrap()
        .with_routing_tuner(Arc::clone(&tuner), snapshot.clone())
        .unwrap();
        let cohort = &client.routing.as_ref().unwrap().cohort_id;
        let first = tuner.plan_for_cohort(&snapshot, routing_request_class(1), "request-a", cohort);
        let second =
            tuner.plan_for_cohort(&snapshot, routing_request_class(1), "request-b", cohort);
        assert_eq!(first.proposer_applied, second.proposer_applied);
    }

    #[tokio::test]
    async fn write_retries_retryable_server_error_with_identical_bytes() {
        let first = CapturedRequests::default();
        let first_app =
            Router::new()
                .route(
                    rhiza_node::WRITE_PATH,
                    post(
                        |State(captured): State<CapturedRequests>,
                         headers: HeaderMap,
                         body: String| async move {
                            captured.0.lock().unwrap().push((headers, body));
                            (
                                StatusCode::SERVICE_UNAVAILABLE,
                                Json(serde_json::json!({
                                    "code": "unavailable",
                                    "retryable": true,
                                    "message": "preferred proposer unavailable",
                                })),
                            )
                        },
                    ),
                )
                .with_state(first.clone());
        let (first_endpoint, first_server) = serve(first_app).await;

        let second = CapturedRequests::default();
        let second_app =
            Router::new()
                .route(
                    rhiza_node::WRITE_PATH,
                    post(
                        |State(captured): State<CapturedRequests>,
                         headers: HeaderMap,
                         body: String| async move {
                            captured.0.lock().unwrap().push((headers, body));
                            write_response(1)
                        },
                    ),
                )
                .with_state(second.clone());
        let (second_endpoint, second_server) = serve(second_app).await;

        let response = RhizaClient::new(vec![first_endpoint, second_endpoint], "client-secret")
            .unwrap()
            .write(write_request())
            .await
            .unwrap();

        first_server.abort();
        second_server.abort();
        assert_eq!(response.applied_index, 1);
        let first = first.0.lock().unwrap();
        let second = second.0.lock().unwrap();
        assert_eq!(first[0].1, second[0].1);
        assert_eq!(first[0].1, serde_json::to_string(&write_request()).unwrap());
    }

    #[tokio::test]
    async fn write_uses_next_endpoint_after_bare_service_unavailable() {
        for (status, should_fail_over) in [
            (StatusCode::SERVICE_UNAVAILABLE, true),
            (StatusCode::TOO_MANY_REQUESTS, true),
            (StatusCode::BAD_GATEWAY, true),
            (StatusCode::GATEWAY_TIMEOUT, true),
            (StatusCode::BAD_REQUEST, false),
        ] {
            let first = CapturedRequests::default();
            let first_app = Router::new()
                .route(
                    rhiza_node::WRITE_PATH,
                    post(
                        move |State(captured): State<CapturedRequests>,
                              headers: HeaderMap,
                              body: String| async move {
                            captured.0.lock().unwrap().push((headers, body));
                            (status, "<html>proxy response</html>")
                        },
                    ),
                )
                .with_state(first.clone());
            let (first_endpoint, first_server) = serve(first_app).await;

            let second = CapturedRequests::default();
            let second_app = Router::new()
                .route(
                    rhiza_node::WRITE_PATH,
                    post(
                        |State(captured): State<CapturedRequests>,
                         headers: HeaderMap,
                         body: String| async move {
                            captured.0.lock().unwrap().push((headers, body));
                            write_response(2)
                        },
                    ),
                )
                .with_state(second.clone());
            let (second_endpoint, second_server) = serve(second_app).await;

            let result = RhizaClient::new(vec![first_endpoint, second_endpoint], "client-secret")
                .unwrap()
                .write(write_request())
                .await;

            first_server.abort();
            second_server.abort();
            let first = first.0.lock().unwrap();
            let second = second.0.lock().unwrap();
            assert_eq!(first.len(), 1, "status {status}");
            if should_fail_over {
                assert_eq!(result.unwrap().applied_index, 2, "status {status}");
                assert_eq!(second.len(), 1, "status {status}");
                assert_eq!(first[0].1, second[0].1, "status {status}");
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.code(), "http_error");
                assert!(!error.retryable());
                assert!(second.is_empty(), "status {status}");
            }
        }
    }

    #[tokio::test]
    async fn write_uses_next_endpoint_after_transport_failure() {
        let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable_endpoint = format!("http://{}", unavailable.local_addr().unwrap());
        drop(unavailable);
        let app = Router::new().route(rhiza_node::WRITE_PATH, post(|| async { write_response(1) }));
        let (fallback, server) = serve(app).await;

        let response = RhizaClient::new(vec![unavailable_endpoint, fallback], "client-secret")
            .unwrap()
            .write(write_request())
            .await
            .unwrap();

        server.abort();
        assert_eq!(response.applied_index, 1);
    }

    #[tokio::test]
    async fn attempt_deadline_retries_next_endpoint_with_identical_bytes() {
        let bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let first_received = Arc::new(Notify::new());
        let first_received_by_server = Arc::clone(&first_received);
        let first_bodies = Arc::clone(&bodies);
        let first_app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(move |body: String| {
                let bodies = Arc::clone(&first_bodies);
                let received = Arc::clone(&first_received_by_server);
                async move {
                    bodies.lock().unwrap().push(body);
                    received.notify_one();
                    future::pending::<Json<WriteResponse>>().await
                }
            }),
        );
        let (first, first_server) = serve(first_app).await;
        let second_bodies = Arc::clone(&bodies);
        let second_app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(move |body: String| {
                let bodies = Arc::clone(&second_bodies);
                async move {
                    bodies.lock().unwrap().push(body);
                    write_response(1)
                }
            }),
        );
        let (second, second_server) = serve(second_app).await;
        let policy = ClientPolicy {
            connect_timeout: Duration::from_millis(100),
            attempt_timeout: Duration::from_millis(250),
            operation_timeout: Duration::from_secs(2),
            hedge_delay: Duration::from_secs(5),
        };

        let client = test_client(vec![first, second], policy);
        let operation = tokio::spawn(async move { client.write(write_request()).await });
        tokio::time::timeout(Duration::from_secs(1), first_received.notified())
            .await
            .expect("the first endpoint must receive the serialized request before timing out");
        let response = tokio::time::timeout(Duration::from_secs(1), operation)
            .await
            .expect("the attempt deadline must advance to the fallback endpoint")
            .unwrap()
            .unwrap();

        first_server.abort();
        second_server.abort();
        assert_eq!(response.applied_index, 1);
        assert_eq!(
            *bodies.lock().unwrap(),
            vec![
                serde_json::to_string(&write_request()).unwrap(),
                serde_json::to_string(&write_request()).unwrap(),
            ]
        );
    }

    #[tokio::test]
    async fn write_stops_after_nonretryable_server_error() {
        let first_app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(|| async {
                (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({
                        "code": "request_conflict",
                        "retryable": false,
                        "message": "request id has a different payload",
                    })),
                )
            }),
        );
        let (first, first_server) = serve(first_app).await;
        let (fallback_called, mut fallback_calls) = mpsc::channel(1);
        let fallback_app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(move || {
                let fallback_called = fallback_called.clone();
                async move {
                    let _ = fallback_called.send(()).await;
                    write_response(2)
                }
            }),
        );
        let (fallback, fallback_server) = serve(fallback_app).await;

        let error = RhizaClient::new(vec![first, fallback], "client-secret")
            .unwrap()
            .write(write_request())
            .await
            .unwrap_err();

        first_server.abort();
        fallback_server.abort();
        assert_eq!(error.code(), "request_conflict");
        assert!(!error.retryable());
        assert!(fallback_calls.try_recv().is_err());
    }

    #[tokio::test]
    async fn write_hedges_a_slow_preferred_endpoint() {
        let entered = Arc::new(Notify::new());
        let first_entered = Arc::clone(&entered);
        let first_capture = CapturedRequests::default();
        let first_app = Router::new()
            .route(
                rhiza_node::WRITE_PATH,
                post(
                    move |State(captured): State<CapturedRequests>,
                          headers: HeaderMap,
                          body: String| {
                        let entered = Arc::clone(&first_entered);
                        async move {
                            captured.0.lock().unwrap().push((headers, body));
                            entered.notify_one();
                            future::pending::<Json<WriteResponse>>().await
                        }
                    },
                ),
            )
            .with_state(first_capture.clone());
        let (first, first_server) = serve(first_app).await;
        let fallback_capture = CapturedRequests::default();
        let fallback_app =
            Router::new()
                .route(
                    rhiza_node::WRITE_PATH,
                    post(
                        |State(captured): State<CapturedRequests>,
                         headers: HeaderMap,
                         body: String| async move {
                            captured.0.lock().unwrap().push((headers, body));
                            write_response(1)
                        },
                    ),
                )
                .with_state(fallback_capture.clone());
        let (fallback, fallback_server) = serve(fallback_app).await;

        let client = RhizaClient::new(vec![first, fallback], "client-secret").unwrap();
        let request = write_request();
        let operation = tokio::spawn(async move { client.write(request).await });
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("preferred endpoint receives the request");
        let response = tokio::time::timeout(Duration::from_secs(1), operation)
            .await
            .expect("the hedge must finish before the five-second attempt timeout")
            .unwrap()
            .unwrap();

        first_server.abort();
        fallback_server.abort();
        assert_eq!(response.applied_index, 1);
        let first_body = first_capture.0.lock().unwrap()[0].1.clone();
        let fallback_body = fallback_capture.0.lock().unwrap()[0].1.clone();
        assert_eq!(first_body, fallback_body);
        assert_eq!(
            serde_json::from_str::<WriteRequest>(&fallback_body)
                .unwrap()
                .request_id,
            "request-1"
        );
    }

    #[tokio::test]
    async fn timer_hedges_progress_through_the_bounded_endpoint_set() {
        let primary_entered = Arc::new(Notify::new());
        let primary_signal = Arc::clone(&primary_entered);
        let primary_app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(move || {
                let entered = Arc::clone(&primary_signal);
                async move {
                    entered.notify_one();
                    future::pending::<Json<WriteResponse>>().await
                }
            }),
        );
        let (primary, primary_server) = serve(primary_app).await;

        let hedge_app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(|| async { future::pending::<Json<WriteResponse>>().await }),
        );
        let (hedge, hedge_server) = serve(hedge_app).await;

        let third_calls = Arc::new(AtomicUsize::new(0));
        let third_counter = Arc::clone(&third_calls);
        let third_app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(move || {
                let calls = Arc::clone(&third_counter);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    write_response(3)
                }
            }),
        );
        let (third, third_server) = serve(third_app).await;

        let policy = ClientPolicy {
            connect_timeout: Duration::from_millis(50),
            attempt_timeout: Duration::from_secs(1),
            operation_timeout: Duration::from_secs(2),
            hedge_delay: Duration::from_millis(20),
        };
        let client = test_client(vec![primary, hedge, third], policy);
        let operation = tokio::spawn(async move { client.write(write_request()).await });

        tokio::time::timeout(Duration::from_secs(1), primary_entered.notified())
            .await
            .expect("primary endpoint receives the request");
        let response = tokio::time::timeout(Duration::from_secs(1), operation)
            .await
            .expect("the finite hedge chain reaches the third endpoint")
            .unwrap()
            .unwrap();

        primary_server.abort();
        hedge_server.abort();
        third_server.abort();
        assert_eq!(response.applied_index, 3);
        assert_eq!(third_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn local_and_applied_index_reads_hedge() {
        for consistency in [ReadConsistency::Local, ReadConsistency::AppliedIndex(7)] {
            let entered = Arc::new(Notify::new());
            let first_entered = Arc::clone(&entered);
            let first_capture = CapturedRequests::default();
            let first_app = Router::new()
                .route(
                    rhiza_node::READ_PATH,
                    post(
                        move |State(captured): State<CapturedRequests>,
                              headers: HeaderMap,
                              body: String| {
                            let entered = Arc::clone(&first_entered);
                            async move {
                                captured.0.lock().unwrap().push((headers, body));
                                entered.notify_one();
                                future::pending::<Json<ReadResponse>>().await
                            }
                        },
                    ),
                )
                .with_state(first_capture.clone());
            let (first, first_server) = serve(first_app).await;
            let fallback_capture = CapturedRequests::default();
            let fallback_app = Router::new()
                .route(
                    rhiza_node::READ_PATH,
                    post(
                        |State(captured): State<CapturedRequests>,
                         headers: HeaderMap,
                         body: String| async move {
                            captured.0.lock().unwrap().push((headers, body));
                            Json(ReadResponse {
                                value: Some("one".into()),
                                applied_index: 7,
                                hash: LogHash::ZERO,
                            })
                        },
                    ),
                )
                .with_state(fallback_capture.clone());
            let (fallback, fallback_server) = serve(fallback_app).await;
            let client = RhizaClient::new(vec![first, fallback], "client-secret").unwrap();
            let operation = tokio::spawn(async move {
                client
                    .read(ReadRequest {
                        key: "alpha".into(),
                        consistency: Some(consistency),
                    })
                    .await
            });
            tokio::time::timeout(Duration::from_secs(1), entered.notified())
                .await
                .expect("preferred endpoint receives the read");
            let response = tokio::time::timeout(Duration::from_secs(1), operation)
                .await
                .expect("the hedge must finish before the five-second attempt timeout")
                .unwrap()
                .unwrap();

            first_server.abort();
            fallback_server.abort();
            assert_eq!(response.value.as_deref(), Some("one"));
            let first_body = first_capture.0.lock().unwrap()[0].1.clone();
            let fallback_body = fallback_capture.0.lock().unwrap()[0].1.clone();
            assert_eq!(first_body, fallback_body);
            assert_eq!(
                serde_json::from_str::<ReadRequest>(&fallback_body)
                    .unwrap()
                    .consistency,
                Some(consistency)
            );
        }
    }

    #[tokio::test]
    async fn read_barrier_does_not_hedge() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let first_entered = Arc::clone(&entered);
        let first_release = Arc::clone(&release);
        let first_app = Router::new().route(
            rhiza_node::READ_PATH,
            post(move || {
                let entered = Arc::clone(&first_entered);
                let release = Arc::clone(&first_release);
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Json(ReadResponse {
                        value: Some("one".into()),
                        applied_index: 1,
                        hash: LogHash::ZERO,
                    })
                }
            }),
        );
        let (first, first_server) = serve(first_app).await;
        let (fallback_called, mut fallback_calls) = mpsc::channel(1);
        let fallback_app = Router::new().route(
            rhiza_node::READ_PATH,
            post(move || {
                let fallback_called = fallback_called.clone();
                async move {
                    let _ = fallback_called.send(()).await;
                    Json(ReadResponse {
                        value: Some("fallback".into()),
                        applied_index: 2,
                        hash: LogHash::ZERO,
                    })
                }
            }),
        );
        let (fallback, fallback_server) = serve(fallback_app).await;
        let client = RhizaClient::new(vec![first, fallback], "client-secret").unwrap();
        let operation = tokio::spawn(async move {
            client
                .read(ReadRequest {
                    key: "alpha".into(),
                    consistency: Some(ReadConsistency::ReadBarrier),
                })
                .await
        });
        tokio::time::timeout(Duration::from_millis(100), entered.notified())
            .await
            .expect("preferred endpoint receives the read");
        assert!(
            tokio::time::timeout(Duration::from_millis(150), fallback_calls.recv())
                .await
                .is_err()
        );
        release.notify_one();
        let response = operation.await.unwrap().unwrap();

        first_server.abort();
        fallback_server.abort();
        assert_eq!(response.applied_index, 1);
    }

    #[tokio::test]
    async fn read_barrier_retry_preserves_identical_body_and_consistency() {
        let first_capture = CapturedRequests::default();
        let first_app =
            Router::new()
                .route(
                    rhiza_node::READ_PATH,
                    post(
                        |State(captured): State<CapturedRequests>,
                         headers: HeaderMap,
                         body: String| async move {
                            captured.0.lock().unwrap().push((headers, body));
                            (
                                StatusCode::SERVICE_UNAVAILABLE,
                                Json(serde_json::json!({
                                    "code": "unavailable",
                                    "retryable": true,
                                    "message": "preferred reader unavailable",
                                })),
                            )
                        },
                    ),
                )
                .with_state(first_capture.clone());
        let (first, first_server) = serve(first_app).await;
        let fallback_capture = CapturedRequests::default();
        let fallback_app =
            Router::new()
                .route(
                    rhiza_node::READ_PATH,
                    post(
                        |State(captured): State<CapturedRequests>,
                         headers: HeaderMap,
                         body: String| async move {
                            captured.0.lock().unwrap().push((headers, body));
                            Json(ReadResponse {
                                value: Some("fallback".into()),
                                applied_index: 2,
                                hash: LogHash::ZERO,
                            })
                        },
                    ),
                )
                .with_state(fallback_capture.clone());
        let (fallback, fallback_server) = serve(fallback_app).await;

        let response = RhizaClient::new(vec![first, fallback], "client-secret")
            .unwrap()
            .read(ReadRequest {
                key: "alpha".into(),
                consistency: Some(ReadConsistency::ReadBarrier),
            })
            .await
            .unwrap();

        first_server.abort();
        fallback_server.abort();
        assert_eq!(response.value.as_deref(), Some("fallback"));
        let first_body = first_capture.0.lock().unwrap()[0].1.clone();
        let fallback_body = fallback_capture.0.lock().unwrap()[0].1.clone();
        assert_eq!(first_body, fallback_body);
        assert_eq!(
            serde_json::from_str::<ReadRequest>(&fallback_body)
                .unwrap()
                .consistency,
            Some(ReadConsistency::ReadBarrier)
        );
    }

    #[tokio::test]
    async fn operation_deadline_bounds_single_stalled_endpoint() {
        let entered = Arc::new(Notify::new());
        let entered_by_server = Arc::clone(&entered);
        let app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(move || {
                let entered = Arc::clone(&entered_by_server);
                async move {
                    entered.notify_one();
                    future::pending::<Json<WriteResponse>>().await
                }
            }),
        );
        let (endpoint, server) = serve(app).await;
        let policy = ClientPolicy {
            connect_timeout: Duration::from_millis(100),
            attempt_timeout: Duration::from_secs(2),
            operation_timeout: Duration::from_millis(150),
            hedge_delay: Duration::from_secs(5),
        };
        let client = test_client(vec![endpoint], policy);
        let operation = tokio::spawn(async move { client.write(write_request()).await });
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("the endpoint receives the request before the operation deadline");
        let error = tokio::time::timeout(Duration::from_secs(1), operation)
            .await
            .expect("the operation deadline must bound the stalled endpoint")
            .unwrap()
            .unwrap_err();

        server.abort();
        assert_eq!(error.code(), "operation_deadline_exceeded");
        assert!(error.retryable());
    }

    #[tokio::test]
    async fn operation_deadline_preserves_last_retryable_structured_error() {
        let first_app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(|| async {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "code": "leader_unavailable",
                        "retryable": true,
                        "message": "preferred proposer unavailable",
                    })),
                )
            }),
        );
        let (first, first_server) = serve(first_app).await;
        let second_app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(|| async { future::pending::<Json<WriteResponse>>().await }),
        );
        let (second, second_server) = serve(second_app).await;
        let policy = ClientPolicy {
            operation_timeout: Duration::from_millis(40),
            ..test_policy()
        };

        let error = test_client(vec![first, second], policy)
            .write(write_request())
            .await
            .unwrap_err();

        first_server.abort();
        second_server.abort();
        assert_eq!(error.code(), "leader_unavailable");
        assert!(error.retryable());
        assert_eq!(
            error.to_string(),
            "HTTP 503 Service Unavailable code=leader_unavailable message=preferred proposer unavailable"
        );
    }

    #[tokio::test]
    async fn server_error_exposes_classification_and_statement_index() {
        let app = Router::new().route(
            rhiza_node::SQL_EXECUTE_PATH,
            post(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "code": "invalid_request",
                        "retryable": false,
                        "message": "statement is invalid",
                        "statement_index": 3,
                    })),
                )
            }),
        );
        let (endpoint, server) = serve(app).await;
        let error = RhizaClient::new(vec![endpoint], "client-secret")
            .unwrap()
            .sql_execute(SqlExecuteRequest {
                request_id: "sql-1".into(),
                statements: Vec::new(),
            })
            .await
            .unwrap_err();

        server.abort();
        assert_eq!(error.code(), "invalid_request");
        assert_eq!(error.category(), ErrorCategory::InvalidRequest);
        assert!(!error.retryable());
        assert_eq!(error.statement_index(), Some(3));
        assert_eq!(error.classification().code(), "invalid_request");
    }

    #[tokio::test]
    async fn unauthorized_server_error_has_authentication_category() {
        let app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(|| async {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({
                        "code": "unauthorized",
                        "retryable": false,
                        "message": "client authentication failed",
                    })),
                )
            }),
        );
        let (endpoint, server) = serve(app).await;

        let error = RhizaClient::new(vec![endpoint], "client-secret")
            .unwrap()
            .write(write_request())
            .await
            .unwrap_err();

        server.abort();
        assert_eq!(error.code(), "unauthorized");
        assert_eq!(error.category(), ErrorCategory::Authentication);
        assert!(!error.retryable());
    }

    #[tokio::test]
    async fn incomplete_server_error_is_not_accepted_as_canonical() {
        let app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(|| async {
                (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({"code": "request_conflict"})),
                )
            }),
        );
        let (endpoint, server) = serve(app).await;

        let error = RhizaClient::new(vec![endpoint], "client-secret")
            .unwrap()
            .write(write_request())
            .await
            .unwrap_err();

        server.abort();
        assert_eq!(error.code(), "http_error");
    }

    #[tokio::test]
    async fn sql_execute_decodes_statement_and_returning_results() {
        let app = Router::new().route(
            rhiza_node::SQL_EXECUTE_PATH,
            post(|| async {
                Json(serde_json::json!({
                    "applied_index": 7,
                    "hash": vec![0_u8; 32],
                    "results": [{
                        "statement_index": 0,
                        "rows_affected": 1,
                        "returning": {
                            "columns": ["id"],
                            "rows": [[{"type": "integer", "value": 42}]]
                        }
                    }]
                }))
            }),
        );
        let (endpoint, server) = serve(app).await;

        let response = RhizaClient::new(vec![endpoint], "client-secret")
            .unwrap()
            .sql_execute(SqlExecuteRequest {
                request_id: "returning-1".into(),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO items(id) VALUES (42) RETURNING id".into(),
                    parameters: Vec::new(),
                }],
            })
            .await
            .unwrap();

        server.abort();
        assert_eq!(response.applied_index, 7);
        assert_eq!(response.results[0].rows_affected, 1);
        assert_eq!(
            response.results[0].returning.as_ref().unwrap().rows,
            [vec![SqlValue::Integer(42)]]
        );
    }

    #[tokio::test]
    async fn sql_execute_rejects_obsolete_response_version_field() {
        let app = Router::new().route(
            rhiza_node::SQL_EXECUTE_PATH,
            post(|| async {
                Json(serde_json::json!({
                    "version": 1,
                    "applied_index": 7,
                    "hash": vec![0_u8; 32],
                    "results": []
                }))
            }),
        );
        let (endpoint, server) = serve(app).await;

        let error = RhizaClient::new(vec![endpoint], "client-secret")
            .unwrap()
            .sql_execute(SqlExecuteRequest {
                request_id: "obsolete-response-version".into(),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO items(id) VALUES (42)".into(),
                    parameters: Vec::new(),
                }],
            })
            .await
            .unwrap_err();

        server.abort();
        assert_eq!(error.code(), "invalid_response");
    }

    #[tokio::test]
    async fn sql_execute_rejects_response_without_results() {
        let app = Router::new().route(
            rhiza_node::SQL_EXECUTE_PATH,
            post(|| async {
                Json(serde_json::json!({
                    "applied_index": 7,
                    "hash": vec![0_u8; 32]
                }))
            }),
        );
        let (endpoint, server) = serve(app).await;

        let error = RhizaClient::new(vec![endpoint], "client-secret")
            .unwrap()
            .sql_execute(SqlExecuteRequest {
                request_id: "missing-results".into(),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO items(id) VALUES (42)".into(),
                    parameters: Vec::new(),
                }],
            })
            .await
            .unwrap_err();

        server.abort();
        assert_eq!(error.code(), "invalid_response");
    }

    #[tokio::test]
    async fn transport_error_retains_safe_diagnostic_and_redacts_credentials() {
        let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = unavailable.local_addr().unwrap().to_string();
        drop(unavailable);
        let token = "client-secret-that-must-stay-private";
        let username = "url-user-that-must-stay-private";
        let password = "url-password-that-must-stay-private";

        let error = RhizaClient::new(
            vec![format!("http://{username}:{password}@{address}")],
            token,
        )
        .unwrap()
        .write(write_request())
        .await
        .unwrap_err();

        let rendered = format!("{error} {error:?}");
        assert_eq!(error.code(), "transport_error");
        assert_ne!(error.to_string(), "request failed: transport error");
        assert!(!rendered.contains(token));
        assert!(!rendered.contains(username));
        assert!(!rendered.contains(password));
        assert!(!rendered.contains(&address));
    }

    #[tokio::test]
    async fn transport_diagnostic_does_not_treat_endpoint_port_as_a_secret() {
        let endpoint = "http://url-user:url-password@127.0.0.1:1";
        let error = RhizaClient::new(vec![endpoint.to_owned()], "client-secret")
            .unwrap()
            .write(write_request())
            .await
            .unwrap_err();

        let diagnostic = error.to_string();
        let os_error_code = diagnostic
            .rsplit_once("os error ")
            .and_then(|(_, suffix)| suffix.split_once(')'))
            .map(|(code, _)| code)
            .expect("transport diagnostic should retain the local OS error code");
        assert!(
            !os_error_code.is_empty() && os_error_code.bytes().all(|byte| byte.is_ascii_digit()),
            "expected an uncorrupted numeric OS error code: {diagnostic}"
        );
        assert_ne!(diagnostic, "request failed: transport error");
        let rendered = format!("{diagnostic} {error:?}");
        for secret in [endpoint, "url-user", "url-password"] {
            assert!(!rendered.contains(secret));
        }
    }

    #[tokio::test]
    async fn short_token_does_not_corrupt_a_known_server_code() {
        let app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "code": "invalid_request",
                        "retryable": false,
                        "message": "the bearer token a was rejected",
                    })),
                )
            }),
        );
        let (endpoint, server) = serve(app).await;

        let error = RhizaClient::new(vec![endpoint], "a")
            .unwrap()
            .write(write_request())
            .await
            .unwrap_err();

        server.abort();
        assert_eq!(error.code(), "invalid_request");
        assert_eq!(error.category(), ErrorCategory::InvalidRequest);
        assert_eq!(
            error.to_string(),
            "HTTP 400 Bad Request code=invalid_request"
        );
        assert!(!format!("{error:?}").contains("the bearer token"));
    }

    #[tokio::test]
    async fn unknown_server_code_reflecting_a_short_token_uses_a_fixed_safe_code() {
        let app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(|| async {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "code": "future-token-a-code",
                        "retryable": true,
                        "message": "retry after presenting a",
                    })),
                )
            }),
        );
        let (endpoint, server) = serve(app).await;

        let error = RhizaClient::new(vec![endpoint], "a")
            .unwrap()
            .write(write_request())
            .await
            .unwrap_err();

        server.abort();
        assert_eq!(error.code(), "unknown_server_error");
        assert_eq!(error.category(), ErrorCategory::Unknown);
        assert!(error.retryable());
        assert!(!format!("{error:?}").contains("future-token-a-code"));
    }

    #[tokio::test]
    async fn reflected_server_error_redacts_configured_token_and_endpoint_everywhere() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let token = "reflected-client-secret".to_string();
        let username = "reflected-user";
        let password = "reflected-password";
        let endpoint = format!("http://{username}:{password}@{address}");
        let reflected_token = token.clone();
        let reflected_endpoint = endpoint.clone();
        let app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(move || {
                let code = format!("future-{reflected_endpoint}");
                let message = format!(
                    "server reflected token={reflected_token} endpoint={reflected_endpoint}"
                );
                async move {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({
                            "code": code,
                            "retryable": false,
                            "message": message,
                        })),
                    )
                }
            }),
        );
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let error = RhizaClient::new(vec![endpoint.clone()], token.clone())
            .unwrap()
            .write(write_request())
            .await
            .unwrap_err();

        server.abort();
        let rendered = format!("{error} {error:?} {}", error.code());
        for secret in [&token, &endpoint, username, password, &address] {
            assert!(!rendered.contains(secret));
        }
        assert_eq!(error.code(), SANITIZED_UNKNOWN_SERVER_CODE);
        assert_eq!(error.category(), ErrorCategory::Unknown);
    }

    #[tokio::test]
    async fn unknown_server_code_preserves_server_retryability() {
        let app = Router::new().route(
            rhiza_node::WRITE_PATH,
            post(|| async {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "code": "future_code",
                        "retryable": true,
                        "message": "a newer server knows more",
                    })),
                )
            }),
        );
        let (endpoint, server) = serve(app).await;
        let error = RhizaClient::new(vec![endpoint], "client-secret")
            .unwrap()
            .write(write_request())
            .await
            .unwrap_err();

        server.abort();
        assert_eq!(error.code(), "future_code");
        assert_eq!(error.category(), ErrorCategory::Unknown);
        assert!(error.retryable());
    }

    #[test]
    fn new_rejects_empty_endpoint_set_endpoint_and_token() {
        let error = RhizaClient::new(Vec::new(), "token").err().unwrap();
        assert_eq!(error.code(), "invalid_client_configuration");
        assert!(RhizaClient::new(vec![String::new()], "token").is_err());
        assert!(RhizaClient::new(vec!["http://127.0.0.1:1".into()], "").is_err());
        assert!(RhizaClient::new(
            vec!["http://duplicate".into(), "http://duplicate".into()],
            "token",
        )
        .is_ok());
    }

    #[test]
    fn only_local_and_applied_index_reads_can_hedge() {
        assert!(!read_can_hedge(None));
        assert!(!read_can_hedge(Some(ReadConsistency::ReadBarrier)));
        assert!(read_can_hedge(Some(ReadConsistency::Local)));
        assert!(read_can_hedge(Some(ReadConsistency::AppliedIndex(7))));
    }
}
