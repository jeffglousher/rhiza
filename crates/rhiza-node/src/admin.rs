use std::{
    collections::HashMap,
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::Arc,
};

use axum::{
    extract::{rejection::JsonRejection, Extension, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use rhiza_core::{ConfigChange, ExecutionProfile, LogAnchor, LogEntry, LogHash, StoredCommand};
use rhiza_log::{IndexRange, LogStore};
use rhiza_quepaxa::{Membership, RecorderFileStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    client_authenticated, install_successor_recorder, valid_auth_token, ConfigError, NodeError,
    NodeRuntime, NodeStatus, StopInformation,
};
use crate::{CheckpointCoordinator, DurabilityError};

pub const ADMIN_STATUS_PATH: &str = "/v1/admin/membership/status";
pub const ADMIN_STOP_PATH: &str = "/v1/admin/membership/stop";
pub const ADMIN_INSTALL_SUCCESSOR_PATH: &str = "/v1/admin/membership/install-successor";
pub const ADMIN_ACTIVATE_PATH: &str = "/v1/admin/membership/activate";
pub const ADMIN_COMPACT_PATH: &str = "/v1/admin/checkpoint/compact";

#[derive(Clone, Eq, PartialEq)]
pub struct AdminConfig {
    token: String,
}

impl AdminConfig {
    pub fn new(token: impl Into<String>) -> Result<Self, ConfigError> {
        let token = token.into();
        if !valid_auth_token(&token) {
            return Err(ConfigError::EmptyAdminToken);
        }
        Ok(Self { token })
    }
}

impl fmt::Debug for AdminConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminConfig")
            .field("token", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AdminStatusResponse {
    pub cluster_id: String,
    pub execution_profile: ExecutionProfile,
    pub epoch: u64,
    pub node: NodeStatus,
    pub members: Vec<String>,
    pub recovery_generation: u64,
    pub qlog_root: LogAnchor,
    pub checkpoint_root: Option<LogAnchor>,
    pub stopped_transition: Option<AdminStoppedTransition>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminStopRequest {
    pub operation_id: String,
    pub expected_config_id: u64,
    pub successor: AdminSuccessorBundle,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AdminStopResponse {
    pub operation_id: String,
    pub stop: StopInformation,
    pub successor: AdminSuccessorBundle,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AdminStoppedTransition {
    pub stop: StopInformation,
    pub successor: AdminSuccessorBundle,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminSuccessorBundle {
    pub config_id: u64,
    pub members: Vec<String>,
    pub digest: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminInstallSuccessorRequest {
    pub operation_id: String,
    pub expected_config_id: u64,
    pub expected_stopped_anchor: LogAnchor,
    pub old_members: Vec<String>,
    pub stop: StopInformation,
    pub successor: AdminSuccessorBundle,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AdminInstallSuccessorResponse {
    pub operation_id: String,
    pub config_id: u64,
    pub digest: LogHash,
    pub activated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminActivateRequest {
    pub operation_id: String,
    pub expected_config_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AdminActivateResponse {
    pub operation_id: String,
    pub entry: LogEntry,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminCompactRequest {
    pub operation_id: String,
    pub expected_config_id: u64,
    pub expected_recovery_generation: u64,
    pub expected_root: LogAnchor,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AdminCompactResponse {
    pub operation_id: String,
    pub anchor: rhiza_core::RecoveryAnchor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminErrorCode {
    Unauthorized,
    InvalidRequest,
    OperationConflict,
    PreconditionFailed,
    Unavailable,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AdminErrorResponse {
    pub code: AdminErrorCode,
}

#[derive(Clone)]
struct AdminGateState {
    token: String,
    admission: Arc<tokio::sync::Semaphore>,
    tasks: AdminTaskTracker,
}

#[derive(Clone)]
pub struct AdminTaskTracker {
    state: Arc<AdminTaskState>,
}

struct AdminTaskState {
    accepting: AtomicBool,
    active: AtomicUsize,
    changed: tokio::sync::watch::Sender<()>,
}

impl AdminTaskTracker {
    fn new() -> Self {
        let (changed, _) = tokio::sync::watch::channel(());
        Self {
            state: Arc::new(AdminTaskState {
                accepting: AtomicBool::new(true),
                active: AtomicUsize::new(0),
                changed,
            }),
        }
    }

    fn try_start(&self) -> Option<AdminTaskGuard> {
        if !self.state.accepting.load(Ordering::Acquire) {
            return None;
        }
        self.state.active.fetch_add(1, Ordering::AcqRel);
        if self.state.accepting.load(Ordering::Acquire) {
            Some(AdminTaskGuard {
                state: Arc::clone(&self.state),
            })
        } else {
            finish_admin_task(&self.state);
            None
        }
    }

    pub fn stop_admission(&self) {
        self.state.accepting.store(false, Ordering::Release);
    }

    pub async fn wait_for_idle(&self) {
        let mut changed = self.state.changed.subscribe();
        loop {
            if self.state.active.load(Ordering::Acquire) == 0 {
                return;
            }
            if changed.changed().await.is_err() {
                return;
            }
        }
    }
}

struct AdminTaskGuard {
    state: Arc<AdminTaskState>,
}

impl Drop for AdminTaskGuard {
    fn drop(&mut self) {
        finish_admin_task(&self.state);
    }
}

fn finish_admin_task(state: &AdminTaskState) {
    if state.active.fetch_sub(1, Ordering::AcqRel) == 1 {
        state.changed.send_replace(());
    }
}

struct AdminPermit {
    _admission: tokio::sync::OwnedSemaphorePermit,
    _task: AdminTaskGuard,
}

#[derive(Clone)]
struct AdminRouteState {
    runtime: Arc<NodeRuntime>,
    recorder: RecorderFileStore,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    operations: Arc<tokio::sync::Mutex<Option<HashMap<String, OperationRecord>>>>,
    ledger_path: PathBuf,
}

#[derive(Clone, Deserialize, Serialize)]
struct OperationRecord {
    fingerprint: Vec<u8>,
    status: u16,
    body: Value,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OperationLedger {
    operations: HashMap<String, OperationRecord>,
}

pub fn node_router_with_admin(
    runtime: Arc<NodeRuntime>,
    recorder: RecorderFileStore,
    admin: AdminConfig,
) -> Result<Router, ConfigError> {
    node_router_with_admin_and_tasks(runtime, recorder, admin).map(|(router, _)| router)
}

pub fn node_router_with_admin_and_tasks(
    runtime: Arc<NodeRuntime>,
    recorder: RecorderFileStore,
    admin: AdminConfig,
) -> Result<(Router, AdminTaskTracker), ConfigError> {
    validate_admin_token(&runtime, &admin)?;
    let (admin_router, tasks) = admin_router(runtime.clone(), recorder.clone(), None, admin);
    Ok((
        crate::node_router(runtime, recorder).merge(admin_router),
        tasks,
    ))
}

pub fn node_router_with_checkpoint_and_admin(
    runtime: Arc<NodeRuntime>,
    recorder: RecorderFileStore,
    coordinator: Arc<CheckpointCoordinator>,
    admin: AdminConfig,
) -> Result<Router, ConfigError> {
    node_router_with_checkpoint_and_admin_tasks(runtime, recorder, coordinator, admin)
        .map(|(router, _)| router)
}

pub fn node_router_with_checkpoint_and_admin_tasks(
    runtime: Arc<NodeRuntime>,
    recorder: RecorderFileStore,
    coordinator: Arc<CheckpointCoordinator>,
    admin: AdminConfig,
) -> Result<(Router, AdminTaskTracker), ConfigError> {
    validate_admin_token(&runtime, &admin)?;
    let (admin_router, tasks) = admin_router(
        runtime.clone(),
        recorder.clone(),
        Some(coordinator.clone()),
        admin,
    );
    Ok((
        crate::node_router_with_checkpoint(runtime, recorder, coordinator).merge(admin_router),
        tasks,
    ))
}

fn validate_admin_token(runtime: &NodeRuntime, admin: &AdminConfig) -> Result<(), ConfigError> {
    if runtime.config().client_token() == admin.token
        || runtime
            .config()
            .peers()
            .iter()
            .any(|peer| peer.token() == admin.token)
    {
        return Err(ConfigError::AdminTokenConflictsWithRuntime);
    }
    Ok(())
}

fn admin_router(
    runtime: Arc<NodeRuntime>,
    recorder: RecorderFileStore,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    admin: AdminConfig,
) -> (Router, AdminTaskTracker) {
    let ledger_path = runtime.config().data_dir().join("admin-operations-v1.json");
    let operations = load_operations(&ledger_path)
        .map(Some)
        .unwrap_or_else(|error| {
            eprintln!("admin operation ledger is unavailable: {error}");
            None
        });
    let state = AdminRouteState {
        runtime,
        recorder,
        coordinator,
        operations: Arc::new(tokio::sync::Mutex::new(operations)),
        ledger_path,
    };
    let tasks = AdminTaskTracker::new();
    let router = Router::new()
        .route(ADMIN_STATUS_PATH, get(handle_status))
        .route(ADMIN_STOP_PATH, post(handle_stop))
        .route(ADMIN_INSTALL_SUCCESSOR_PATH, post(handle_install_successor))
        .route(ADMIN_ACTIVATE_PATH, post(handle_activate))
        .route(ADMIN_COMPACT_PATH, post(handle_compact))
        .route_layer(middleware::from_fn_with_state(
            AdminGateState {
                token: admin.token,
                admission: Arc::new(tokio::sync::Semaphore::new(1)),
                tasks: tasks.clone(),
            },
            admin_gate,
        ))
        .with_state(state);
    (router, tasks)
}

async fn admin_gate(
    State(state): State<AdminGateState>,
    mut request: Request,
    next: Next,
) -> Response {
    if !client_authenticated(request.headers(), &state.token) {
        return admin_error(StatusCode::UNAUTHORIZED, AdminErrorCode::Unauthorized);
    }
    let task = match state.tasks.try_start() {
        Some(task) => task,
        None => return admin_error(StatusCode::SERVICE_UNAVAILABLE, AdminErrorCode::Unavailable),
    };
    let permit = match state.admission.try_acquire_owned() {
        Ok(admission) => Arc::new(AdminPermit {
            _admission: admission,
            _task: task,
        }),
        Err(_) => return admin_error(StatusCode::TOO_MANY_REQUESTS, AdminErrorCode::Unavailable),
    };
    request.extensions_mut().insert(permit);
    next.run(request).await
}

async fn handle_status(
    State(state): State<AdminRouteState>,
    Extension(_permit): Extension<Arc<AdminPermit>>,
) -> Response {
    let _operations = state.operations.lock().await;
    match status_response(&state).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => node_admin_error(error),
    }
}

async fn handle_stop(
    State(state): State<AdminRouteState>,
    Extension(permit): Extension<Arc<AdminPermit>>,
    payload: Result<Json<AdminStopRequest>, JsonRejection>,
) -> Response {
    let request = match payload {
        Ok(Json(request)) => request,
        Err(_) => return admin_error(StatusCode::BAD_REQUEST, AdminErrorCode::InvalidRequest),
    };
    let runtime = state.runtime.clone();
    let owned = request.clone();
    run_async_operation(&state, permit, "stop", &request, async move {
        tokio::task::spawn_blocking(move || {
            let successor = validate_successor(
                &owned.successor,
                owned.expected_config_id,
                runtime.config().cluster_id(),
            )?;
            runtime
                .stop_current_configuration_for_successor(&successor)
                .map(|stop| AdminStopResponse {
                    operation_id: owned.operation_id,
                    stop,
                    successor: owned.successor,
                })
                .map_err(OperationError::Node)
        })
        .await
        .unwrap_or(Err(OperationError::Unavailable))
    })
    .await
}

async fn handle_install_successor(
    State(state): State<AdminRouteState>,
    Extension(permit): Extension<Arc<AdminPermit>>,
    payload: Result<Json<AdminInstallSuccessorRequest>, JsonRejection>,
) -> Response {
    let request = match payload {
        Ok(Json(request)) => request,
        Err(_) => return admin_error(StatusCode::BAD_REQUEST, AdminErrorCode::InvalidRequest),
    };
    let runtime = state.runtime.clone();
    let recorder = state.recorder.clone();
    let owned = request.clone();
    run_async_operation(&state, permit, "install_successor", &request, async move {
        tokio::task::spawn_blocking(move || {
            install_successor(&runtime, &recorder, &owned).map_err(OperationError::Node)
        })
        .await
        .unwrap_or(Err(OperationError::Unavailable))
    })
    .await
}

async fn handle_activate(
    State(state): State<AdminRouteState>,
    Extension(permit): Extension<Arc<AdminPermit>>,
    payload: Result<Json<AdminActivateRequest>, JsonRejection>,
) -> Response {
    let request = match payload {
        Ok(Json(request)) => request,
        Err(_) => return admin_error(StatusCode::BAD_REQUEST, AdminErrorCode::InvalidRequest),
    };
    let runtime = state.runtime.clone();
    let owned = request.clone();
    run_async_operation(&state, permit, "activate", &request, async move {
        tokio::task::spawn_blocking(move || {
            runtime
                .activate_successor_if(owned.expected_config_id)
                .map(|entry| AdminActivateResponse {
                    operation_id: owned.operation_id,
                    entry,
                })
                .map_err(OperationError::Node)
        })
        .await
        .unwrap_or(Err(OperationError::Unavailable))
    })
    .await
}

async fn handle_compact(
    State(state): State<AdminRouteState>,
    Extension(permit): Extension<Arc<AdminPermit>>,
    payload: Result<Json<AdminCompactRequest>, JsonRejection>,
) -> Response {
    let request = match payload {
        Ok(Json(request)) => request,
        Err(_) => return admin_error(StatusCode::BAD_REQUEST, AdminErrorCode::InvalidRequest),
    };
    let runtime = state.runtime.clone();
    let coordinator = state.coordinator.clone();
    let owned = request.clone();
    run_async_operation(&state, permit, "compact", &request, async move {
        match coordinator {
            Some(coordinator) => coordinator
                .checkpoint_compact_fenced(
                    &runtime,
                    owned.expected_config_id,
                    owned.expected_recovery_generation,
                    owned.expected_root,
                )
                .await
                .map(|anchor| AdminCompactResponse {
                    operation_id: owned.operation_id,
                    anchor,
                })
                .map_err(OperationError::Durability),
            None => Err(OperationError::Unavailable),
        }
    })
    .await
}

async fn run_async_operation<T, R, F>(
    state: &AdminRouteState,
    permit: Arc<AdminPermit>,
    kind: &str,
    request: &T,
    operation: F,
) -> Response
where
    T: Serialize,
    R: Serialize + Send + 'static,
    F: std::future::Future<Output = Result<R, OperationError>> + Send + 'static,
{
    let operation_id = match serde_json::to_value(request)
        .ok()
        .and_then(|value| value.get("operation_id")?.as_str().map(str::to_owned))
    {
        Some(operation_id) => operation_id,
        None => return admin_error(StatusCode::BAD_REQUEST, AdminErrorCode::InvalidRequest),
    };
    let fingerprint = match operation_fingerprint(kind, request) {
        Ok(fingerprint) => fingerprint,
        Err(()) => return admin_error(StatusCode::BAD_REQUEST, AdminErrorCode::InvalidRequest),
    };
    {
        let operations = state.operations.lock().await;
        let Some(operations) = operations.as_ref() else {
            return admin_error(StatusCode::SERVICE_UNAVAILABLE, AdminErrorCode::Unavailable);
        };
        if let Some(response) = replay(operations, &operation_id, &fingerprint) {
            return response;
        }
    }
    if let Some(response) = validate_operation_id(&operation_id) {
        return response;
    }
    let detached_state = state.clone();
    let detached_operation_id = operation_id.clone();
    let detached_fingerprint = fingerprint.clone();
    let (completed, mut completion) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let result = operation.await;
        let mut operations = detached_state.operations.lock().await;
        if let Some(records) = operations.as_mut() {
            let _ = store_result(records, detached_operation_id, detached_fingerprint, result);
            if let Err(error) = persist_operations(&detached_state.ledger_path, records) {
                eprintln!("admin operation ledger persistence failed: {error}");
                *operations = None;
            }
        }
        drop(operations);
        drop(detached_state);
        drop(permit);
        completed.send_replace(true);
    });
    let waited = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !*completion.borrow() {
            if completion.changed().await.is_err() {
                break;
            }
        }
    })
    .await;
    if waited.is_err() {
        return admin_error(StatusCode::SERVICE_UNAVAILABLE, AdminErrorCode::Unavailable);
    }
    let operations = state.operations.lock().await;
    let Some(operations) = operations.as_ref() else {
        return admin_error(StatusCode::SERVICE_UNAVAILABLE, AdminErrorCode::Unavailable);
    };
    replay(operations, &operation_id, &fingerprint)
        .unwrap_or_else(|| admin_error(StatusCode::INTERNAL_SERVER_ERROR, AdminErrorCode::Internal))
}

fn operation_fingerprint(kind: &str, request: &impl Serialize) -> Result<Vec<u8>, ()> {
    serde_json::to_vec(&(kind, request)).map_err(|_| ())
}

fn replay(
    operations: &HashMap<String, OperationRecord>,
    operation_id: &str,
    fingerprint: &[u8],
) -> Option<Response> {
    operations.get(operation_id).map(|record| {
        if record.fingerprint == fingerprint {
            (
                StatusCode::from_u16(record.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                Json(record.body.clone()),
            )
                .into_response()
        } else {
            admin_error(StatusCode::CONFLICT, AdminErrorCode::OperationConflict)
        }
    })
}

fn validate_operation_id(operation_id: &str) -> Option<Response> {
    (operation_id.trim().is_empty() || operation_id.len() > 256)
        .then(|| admin_error(StatusCode::BAD_REQUEST, AdminErrorCode::InvalidRequest))
}

fn store_result<R: Serialize>(
    operations: &mut HashMap<String, OperationRecord>,
    operation_id: String,
    fingerprint: Vec<u8>,
    result: Result<R, OperationError>,
) -> Response {
    let (status, body) = match result {
        Ok(response) => match serde_json::to_value(response) {
            Ok(body) => (StatusCode::OK, body),
            Err(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_value(AdminErrorCode::Internal),
            ),
        },
        Err(error) => operation_error_value(error),
    };
    operations.insert(
        operation_id,
        OperationRecord {
            fingerprint,
            status: status.as_u16(),
            body: body.clone(),
        },
    );
    (status, Json(body)).into_response()
}

async fn status_response(state: &AdminRouteState) -> Result<AdminStatusResponse, NodeError> {
    let checkpoint_root = match state.coordinator.as_ref() {
        Some(coordinator) => {
            let tip = coordinator
                .refresh_durable_tip()
                .await
                .map_err(|error| NodeError::Unavailable(error.to_string()))?;
            Some(LogAnchor::new(tip.index(), tip.hash()))
        }
        None => None,
    };
    let _commit = state.runtime.lock_commit()?;
    let node = state.runtime.status()?;
    let qlog_root = state.runtime.log_root_unlocked()?;
    let stopped_transition = stopped_transition(&state.runtime)?;
    Ok(AdminStatusResponse {
        cluster_id: state.runtime.config.cluster_id().to_owned(),
        execution_profile: state.runtime.config.execution_profile(),
        epoch: state.runtime.config.epoch(),
        node,
        members: state.runtime.config.membership().members().to_vec(),
        recovery_generation: state.runtime.config.recovery_generation(),
        qlog_root,
        checkpoint_root,
        stopped_transition,
    })
}

fn stopped_transition(runtime: &NodeRuntime) -> Result<Option<AdminStoppedTransition>, NodeError> {
    let configuration = runtime.configuration_state()?;
    let Some(anchor) = configuration.stop().copied() else {
        return Ok(None);
    };
    if configuration.config_id() != runtime.consensus.config_id() {
        return Ok(None);
    }
    let entry = runtime.recover_stop_entry(anchor)?;
    let successor = successor_from_entry(&entry)?;
    let proof = runtime
        .consensus
        .inspect_decision_proof_at(entry.index)
        .map_err(|error| NodeError::Unavailable(error.to_string()))?
        .ok_or_else(|| NodeError::Unavailable("durable Stop proof is unavailable".into()))?;
    Ok(Some(AdminStoppedTransition {
        stop: StopInformation { entry, proof },
        successor,
    }))
}

fn successor_from_entry(entry: &LogEntry) -> Result<AdminSuccessorBundle, NodeError> {
    let command = StoredCommand::new(entry.entry_type, entry.payload.clone());
    let change = ConfigChange::recognize(&command)
        .map_err(|_| NodeError::PreconditionFailed("Stop command is not successor-bound".into()))?;
    let ConfigChange::BoundStop { successor } = change else {
        return Err(NodeError::PreconditionFailed(
            "Stop command is not successor-bound".into(),
        ));
    };
    Ok(AdminSuccessorBundle {
        config_id: successor.config_id(),
        members: successor.members().to_vec(),
        digest: successor.digest(),
    })
}

fn validate_successor(
    bundle: &AdminSuccessorBundle,
    predecessor_config_id: u64,
    cluster_id: &str,
) -> Result<Membership, OperationError> {
    let membership = Membership::from_voters(bundle.members.clone()).map_err(|_| {
        OperationError::Node(NodeError::InvalidRequest(
            "successor membership is invalid".into(),
        ))
    })?;
    if predecessor_config_id.checked_add(1) != Some(bundle.config_id)
        || membership.digest() != bundle.digest
        || cluster_id.is_empty()
    {
        return Err(OperationError::Node(NodeError::PreconditionFailed(
            "successor descriptor does not match the active configuration".into(),
        )));
    }
    Ok(membership)
}

fn load_operations(path: &Path) -> Result<HashMap<String, OperationRecord>, std::io::Error> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(error),
    };
    let ledger: OperationLedger = serde_json::from_slice(&bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(ledger.operations)
}

fn persist_operations(
    path: &Path,
    operations: &HashMap<String, OperationRecord>,
) -> Result<(), std::io::Error> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "ledger has no parent")
    })?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let bytes = serde_json::to_vec(&OperationLedger {
        operations: operations.clone(),
    })
    .map_err(std::io::Error::other)?;
    let mut file = fs::File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    fs::File::open(parent)?.sync_all()
}

fn install_successor(
    runtime: &NodeRuntime,
    recorder: &RecorderFileStore,
    request: &AdminInstallSuccessorRequest,
) -> Result<AdminInstallSuccessorResponse, NodeError> {
    let _commit = runtime.lock_commit()?;
    runtime.ensure_ready()?;
    let state = runtime.configuration_state()?;
    if state.is_active()
        || state.config_id() != request.expected_config_id
        || state.stop().copied() != Some(request.expected_stopped_anchor)
    {
        return Err(NodeError::PreconditionFailed(
            "stopped configuration anchor does not match".into(),
        ));
    }
    let old_membership = Membership::from_voters(request.old_members.clone())
        .map_err(|_| NodeError::InvalidRequest("old membership is invalid".into()))?;
    if old_membership.digest() != state.digest()
        || request.stop.entry.cluster_id != runtime.config.cluster_id()
        || request.stop.entry.epoch != runtime.config.epoch()
        || request.stop.entry.config_id != request.expected_config_id
        || request.stop.entry.index != request.expected_stopped_anchor.index()
        || request.stop.entry.hash != request.expected_stopped_anchor.hash()
    {
        return Err(NodeError::PreconditionFailed(
            "old decision material does not match the stopped runtime".into(),
        ));
    }
    let entries = runtime
        .log_store
        .read_range(
            IndexRange::new(request.stop.entry.index, request.stop.entry.index)
                .map_err(|error| NodeError::Storage(error.to_string()))?,
        )
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    if entries.as_slice() != [request.stop.entry.clone()] {
        return Err(NodeError::PreconditionFailed(
            "old stop entry is not the exact local qlog entry".into(),
        ));
    }
    let successor = Membership::from_voters(request.successor.members.clone())
        .map_err(|_| NodeError::InvalidRequest("successor membership is invalid".into()))?;
    if successor.digest() != request.successor.digest
        || request.expected_config_id.checked_add(1) != Some(request.successor.config_id)
        || successor_from_entry(&request.stop.entry)? != request.successor
    {
        return Err(NodeError::PreconditionFailed(
            "successor bundle or digest does not match".into(),
        ));
    }
    let installed = install_successor_recorder(
        recorder,
        request.successor.config_id,
        successor,
        &request.stop,
    )?;
    Ok(AdminInstallSuccessorResponse {
        operation_id: request.operation_id.clone(),
        config_id: installed.config_id(),
        digest: installed.config_digest(),
        activated: installed.is_activated(),
    })
}

enum OperationError {
    Node(NodeError),
    Durability(DurabilityError),
    Unavailable,
}

fn operation_error_value(error: OperationError) -> (StatusCode, Value) {
    match error {
        OperationError::Node(error) => {
            eprintln!("admin operation failed: {error}");
            let (status, code) = node_admin_status(&error);
            (status, error_value(code))
        }
        OperationError::Durability(DurabilityError::PreconditionFailed) => (
            StatusCode::CONFLICT,
            error_value(AdminErrorCode::PreconditionFailed),
        ),
        OperationError::Durability(error) => {
            eprintln!("admin durability operation failed: {error}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                error_value(AdminErrorCode::Unavailable),
            )
        }
        OperationError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            error_value(AdminErrorCode::Unavailable),
        ),
    }
}

fn node_admin_error(error: NodeError) -> Response {
    let (status, code) = node_admin_status(&error);
    admin_error(status, code)
}

fn node_admin_status(error: &NodeError) -> (StatusCode, AdminErrorCode) {
    match error {
        NodeError::InvalidRequest(_) => (StatusCode::BAD_REQUEST, AdminErrorCode::InvalidRequest),
        #[cfg(feature = "sql")]
        NodeError::InvalidSqlStatement { .. } => {
            (StatusCode::BAD_REQUEST, AdminErrorCode::InvalidRequest)
        }
        NodeError::PreconditionFailed(_) | NodeError::ConfigurationTransition { .. } => {
            (StatusCode::CONFLICT, AdminErrorCode::PreconditionFailed)
        }
        #[cfg(feature = "sql")]
        NodeError::RequestConflict(_) => (StatusCode::CONFLICT, AdminErrorCode::PreconditionFailed),
        NodeError::Unavailable(_)
        | NodeError::ResourceExhausted(_)
        | NodeError::Contention(_)
        | NodeError::WinnerLimitExceeded => {
            (StatusCode::SERVICE_UNAVAILABLE, AdminErrorCode::Unavailable)
        }
        NodeError::UnsupportedAckMode(_)
        | NodeError::ExecutionProfileMismatch { .. }
        | NodeError::DataRootLocked(_)
        | NodeError::SnapshotRequired(_)
        | NodeError::Storage(_)
        | NodeError::Reconciliation(_)
        | NodeError::Invariant(_)
        | NodeError::Fatal(_) => (StatusCode::INTERNAL_SERVER_ERROR, AdminErrorCode::Internal),
    }
}

fn admin_error(status: StatusCode, code: AdminErrorCode) -> Response {
    (status, Json(AdminErrorResponse { code })).into_response()
}

fn error_value(code: AdminErrorCode) -> Value {
    serde_json::to_value(AdminErrorResponse { code })
        .unwrap_or_else(|_| serde_json::json!({"code": "internal"}))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::{AdminTaskTracker, OperationLedger};

    #[test]
    fn operation_ledger_has_one_strict_canonical_shape() {
        let canonical = serde_json::to_value(OperationLedger::default()).unwrap();
        assert_eq!(canonical, serde_json::json!({"operations": {}}));
        assert!(serde_json::from_value::<OperationLedger>(
            serde_json::json!({"version": 1, "operations": {}})
        )
        .is_err());
    }

    #[tokio::test]
    async fn shutdown_observes_a_late_admin_mutation_before_sampling_state() {
        let tasks = AdminTaskTracker::new();
        let guard = tasks.try_start().unwrap();
        let committed_tip = Arc::new(AtomicUsize::new(1));
        let late_tip = Arc::clone(&committed_tip);
        let (release, wait) = tokio::sync::oneshot::channel();
        let operation = tokio::spawn(async move {
            let _ = wait.await;
            late_tip.store(2, Ordering::Release);
            drop(guard);
        });

        tasks.stop_admission();
        assert!(tasks.try_start().is_none());
        let mut idle = Box::pin(tasks.wait_for_idle());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut idle)
                .await
                .is_err()
        );

        let _ = release.send(());
        idle.await;
        operation.await.unwrap();
        assert_eq!(committed_tip.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn shutdown_waits_until_every_admitted_admin_task_finishes() {
        let tasks = AdminTaskTracker::new();
        let first = tasks.try_start().unwrap();
        let second = tasks.try_start().unwrap();
        tasks.stop_admission();

        let mut idle = Box::pin(tasks.wait_for_idle());
        drop(first);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut idle)
                .await
                .is_err()
        );

        drop(second);
        tokio::time::timeout(std::time::Duration::from_secs(1), idle)
            .await
            .expect("last task completion must wake the shutdown waiter");
    }
}
