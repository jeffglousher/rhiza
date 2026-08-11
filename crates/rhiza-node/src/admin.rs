use std::{
    collections::HashMap,
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
};
#[cfg(test)]
use std::{future::Future, pin::Pin};

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
#[cfg(feature = "tuner")]
pub const ADMIN_TUNER_METRICS_PATH: &str = "/v1/admin/tuner/metrics";
const ADMIN_STATUS_SNAPSHOT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const ADMIN_STATUS_SNAPSHOT_MAX_IN_FLIGHT: usize = 1;

#[cfg(test)]
type StatusRefreshHook = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

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
    pub stop_entry: LogEntry,
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
    /// One atomic state word: the high bit closes admission and every lower
    /// bit counts an already-admitted task.  Splitting these facts into two
    /// atomics admits a task after shutdown has observed admission closed.
    ///
    /// Each successful compare-and-swap below is the operation's
    /// linearization point.  In particular, either `try_start` increments
    /// this word before `stop_admission` clears the high bit, or its CAS
    /// retries against the closed word and declines admission.
    state: AtomicUsize,
    changed: tokio::sync::watch::Sender<()>,
    #[cfg(test)]
    before_admission_cas: std::sync::Mutex<Option<AdminAdmissionCasHook>>,
}

const ADMIN_ADMISSION_OPEN: usize = 1usize << (usize::BITS - 1);
const ADMIN_ACTIVE_MASK: usize = !ADMIN_ADMISSION_OPEN;

#[cfg(test)]
type AdminAdmissionCasHook = Arc<dyn Fn() + Send + Sync>;

fn admin_accepting(state: usize) -> bool {
    state & ADMIN_ADMISSION_OPEN != 0
}

fn admin_active(state: usize) -> usize {
    state & ADMIN_ACTIVE_MASK
}

fn admin_is_quiescent(state: usize) -> bool {
    !admin_accepting(state) && admin_active(state) == 0
}

impl AdminTaskTracker {
    fn new() -> Self {
        let (changed, _) = tokio::sync::watch::channel(());
        Self {
            state: Arc::new(AdminTaskState {
                state: AtomicUsize::new(ADMIN_ADMISSION_OPEN),
                changed,
                #[cfg(test)]
                before_admission_cas: std::sync::Mutex::new(None),
            }),
        }
    }

    fn try_start(&self) -> Option<AdminTaskGuard> {
        let mut observed = self.state.state.load(Ordering::Acquire);
        loop {
            if !admin_accepting(observed) || admin_active(observed) == ADMIN_ACTIVE_MASK {
                return None;
            }
            #[cfg(test)]
            self.run_before_admission_cas_hook();
            match self.state.state.compare_exchange_weak(
                observed,
                observed + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.state.changed.send_replace(());
                    return Some(AdminTaskGuard {
                        state: Arc::clone(&self.state),
                    });
                }
                Err(current) => observed = current,
            }
        }
    }

    pub fn stop_admission(&self) {
        let mut observed = self.state.state.load(Ordering::Acquire);
        loop {
            if !admin_accepting(observed) {
                return;
            }
            let closed = observed & ADMIN_ACTIVE_MASK;
            match self.state.state.compare_exchange_weak(
                observed,
                closed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // A waiter may have observed an open-but-idle state.
                    // Publish the close transition as well as completion so
                    // the subscribe-then-observe wait protocol cannot lose
                    // the only transition to quiescence.
                    self.state.changed.send_replace(());
                    return;
                }
                Err(current) => observed = current,
            }
        }
    }

    /// Waits until admission is closed *and* every task admitted before that
    /// closing transition has dropped its guard.  The watcher is subscribed
    /// before the state read: a transition after subscription advances the
    /// watch version, while a transition before it is reflected in the read.
    pub async fn wait_for_idle(&self) {
        let mut changed = self.state.changed.subscribe();
        loop {
            if admin_is_quiescent(self.state.state.load(Ordering::Acquire)) {
                return;
            }
            if changed.changed().await.is_err() {
                return;
            }
        }
    }

    /// Constructs an admission tracker for cross-crate lifecycle tests.
    ///
    /// This is intentionally unavailable in production builds; production
    /// trackers are created only by the authenticated admin router.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn test_tracker() -> Self {
        Self::new()
    }

    /// Starts a synthetic *already-admitted* admin operation for a lifecycle
    /// test. New admission still goes through the real authenticated router.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn test_start_admitted(&self) -> Option<AdminTestTaskGuard> {
        self.try_start()
            .map(|guard| AdminTestTaskGuard { _guard: guard })
    }

    #[cfg(test)]
    fn with_before_admission_cas_hook(hook: AdminAdmissionCasHook) -> Self {
        let tracker = Self::new();
        *tracker
            .state
            .before_admission_cas
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
        tracker
    }

    #[cfg(test)]
    fn run_before_admission_cas_hook(&self) {
        let hook = self
            .state
            .before_admission_cas
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }
}

struct AdminTaskGuard {
    state: Arc<AdminTaskState>,
}

/// Test-only RAII proof of an admin task that passed admission before HA
/// shutdown began. Dropping it is the same completion transition used by the
/// real middleware permit.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub struct AdminTestTaskGuard {
    _guard: AdminTaskGuard,
}

impl Drop for AdminTaskGuard {
    fn drop(&mut self) {
        finish_admin_task(&self.state);
    }
}

fn finish_admin_task(state: &AdminTaskState) {
    let mut observed = state.state.load(Ordering::Acquire);
    loop {
        let active = admin_active(observed);
        assert!(active > 0, "admin task guard completed more than once");
        match state.state.compare_exchange_weak(
            observed,
            observed - 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Notify every state transition, not only 1 -> 0. This keeps
                // the watcher protocol valid even when stop and completion
                // race on opposite sides of its state observation.
                state.changed.send_replace(());
                return;
            }
            Err(current) => observed = current,
        }
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
    status_snapshots: Arc<tokio::sync::Semaphore>,
    #[cfg(test)]
    status_snapshot_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    before_refresh_durable_tip_hook: Option<StatusRefreshHook>,
    #[cfg(test)]
    status_snapshot_timeout: std::time::Duration,
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
        status_snapshots: Arc::new(tokio::sync::Semaphore::new(
            ADMIN_STATUS_SNAPSHOT_MAX_IN_FLIGHT,
        )),
        #[cfg(test)]
        status_snapshot_hook: None,
        #[cfg(test)]
        before_refresh_durable_tip_hook: None,
        #[cfg(test)]
        status_snapshot_timeout: ADMIN_STATUS_SNAPSHOT_TIMEOUT,
        operations: Arc::new(tokio::sync::Mutex::new(operations)),
        ledger_path,
    };
    let tasks = AdminTaskTracker::new();
    let router = Router::new()
        .route(ADMIN_STATUS_PATH, get(handle_status))
        .route(ADMIN_STOP_PATH, post(handle_stop))
        .route(ADMIN_INSTALL_SUCCESSOR_PATH, post(handle_install_successor))
        .route(ADMIN_ACTIVATE_PATH, post(handle_activate))
        .route(ADMIN_COMPACT_PATH, post(handle_compact));
    #[cfg(feature = "tuner")]
    let router = router.route(ADMIN_TUNER_METRICS_PATH, get(handle_tuner_metrics));
    let router = router
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
    let deadline = tokio::time::Instant::now() + status_snapshot_timeout(&state);
    match status_response(&state, deadline).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => node_admin_error(error),
    }
}

#[cfg(feature = "tuner")]
async fn handle_tuner_metrics(
    State(state): State<AdminRouteState>,
    Extension(_permit): Extension<Arc<AdminPermit>>,
) -> Response {
    let runtime = &state.runtime;
    match runtime.config().tuner_telemetry() {
        Some(telemetry) => {
            let collector = telemetry.collector();
            let metrics = serde_json::json!({
                "total_samples": collector.total_samples(),
                "is_fresh": collector.is_fresh(),
                "cold_start_gates_passed": collector.cold_start_gates_passed(),
                "window_size": collector.window_size(),
            });
            Json(metrics).into_response()
        }
        None => {
            let response = serde_json::json!({
                "error": "tuner not configured",
                "code": "tuner_not_available"
            });
            (StatusCode::NOT_FOUND, Json(response)).into_response()
        }
    }
}

fn status_snapshot_timeout(state: &AdminRouteState) -> std::time::Duration {
    #[cfg(test)]
    {
        state.status_snapshot_timeout
    }
    #[cfg(not(test))]
    {
        let _ = state;
        ADMIN_STATUS_SNAPSHOT_TIMEOUT
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

async fn status_response(
    state: &AdminRouteState,
    deadline: tokio::time::Instant,
) -> Result<AdminStatusResponse, NodeError> {
    let admission = admit_status_snapshot(Arc::clone(&state.status_snapshots))?;
    let checkpoint_root = match state.coordinator.as_ref() {
        Some(coordinator) => {
            #[cfg(test)]
            if let Some(hook) = state.before_refresh_durable_tip_hook.as_ref() {
                tokio::time::timeout_at(deadline, hook())
                    .await
                    .map_err(|_| {
                        NodeError::Unavailable(
                            "admin status refresh exceeded its response deadline".into(),
                        )
                    })?;
            }
            let tip = tokio::time::timeout_at(deadline, coordinator.refresh_durable_tip())
                .await
                .map_err(|_| {
                    NodeError::Unavailable(
                        "admin status refresh exceeded its response deadline".into(),
                    )
                })?
                .map_err(|error| NodeError::Unavailable(error.to_string()))?;
            Some(LogAnchor::new(tip.index(), tip.hash()))
        }
        None => None,
    };
    let runtime = Arc::clone(&state.runtime);
    #[cfg(test)]
    let status_snapshot_hook = state.status_snapshot_hook.clone();
    run_status_snapshot_until(deadline, move || {
        // This permit intentionally belongs to the blocking closure, rather
        // than the response future.  A timed-out or disconnected caller must
        // not admit another snapshot until this one has actually stopped.
        let _admission = admission;
        #[cfg(test)]
        if let Some(hook) = status_snapshot_hook {
            hook();
        }
        status_snapshot(&runtime, checkpoint_root)
    })
    .await
}

fn admit_status_snapshot(
    admission: Arc<tokio::sync::Semaphore>,
) -> Result<tokio::sync::OwnedSemaphorePermit, NodeError> {
    admission
        .try_acquire_owned()
        .map_err(|_| NodeError::Unavailable("admin status snapshot capacity is exhausted".into()))
}

// Status collection reads only local qlog and immutable predecessor Stop
// evidence under the commit lock. Keep that synchronous observation off an
// async request worker; dropping the returned future detaches the read-only
// task instead of joining it indefinitely.
#[cfg(test)]
async fn run_status_snapshot<T>(
    snapshot: impl FnOnce() -> Result<T, NodeError> + Send + 'static,
) -> Result<T, NodeError>
where
    T: Send + 'static,
{
    run_status_snapshot_until(
        tokio::time::Instant::now() + ADMIN_STATUS_SNAPSHOT_TIMEOUT,
        snapshot,
    )
    .await
}

async fn run_status_snapshot_until<T>(
    deadline: tokio::time::Instant,
    snapshot: impl FnOnce() -> Result<T, NodeError> + Send + 'static,
) -> Result<T, NodeError>
where
    T: Send + 'static,
{
    if tokio::time::Instant::now() >= deadline {
        return Err(NodeError::Unavailable(
            "admin status snapshot exceeded its response deadline".into(),
        ));
    }
    // `timeout` drops the JoinHandle on expiry.  Tokio cannot interrupt a
    // running blocking closure, so it may finish later; this particular
    // closure is a read-only status snapshot and holds an Arc<NodeRuntime>,
    // never an admin mutation permit or a write capability.
    match tokio::time::timeout_at(deadline, tokio::task::spawn_blocking(snapshot)).await {
        Ok(result) => result.map_err(status_snapshot_join_error)?,
        Err(_) => Err(NodeError::Unavailable(
            "admin status snapshot exceeded its response deadline".into(),
        )),
    }
}

fn status_snapshot_join_error(error: tokio::task::JoinError) -> NodeError {
    if error.is_cancelled() {
        NodeError::Unavailable("admin status snapshot task was cancelled".into())
    } else {
        NodeError::Fatal(format!("admin status snapshot task panicked: {error}"))
    }
}

fn status_snapshot(
    runtime: &NodeRuntime,
    checkpoint_root: Option<LogAnchor>,
) -> Result<AdminStatusResponse, NodeError> {
    let _commit = runtime.lock_commit_for_status_observation()?;
    let node = runtime.status()?;
    let qlog_root = runtime.log_root_unlocked()?;
    let stopped_transition = stopped_transition(runtime)?;
    Ok(AdminStatusResponse {
        cluster_id: runtime.config.cluster_id().to_owned(),
        execution_profile: runtime.config.execution_profile(),
        epoch: runtime.config.epoch(),
        node,
        members: runtime.config.membership().members().to_vec(),
        recovery_generation: runtime.config.recovery_generation(),
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
    let entry = runtime.observe_stop_entry_locally(anchor)?;
    let successor = successor_from_entry(&entry)?;
    Ok(Some(AdminStoppedTransition {
        stop_entry: entry,
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
        | NodeError::OutcomeUnknown(_)
        | NodeError::StartupCancelled { .. }
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
    use rhiza_core::LogHash;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Barrier, Condvar, Mutex,
    };
    use std::time::Duration;
    use std::{collections::HashMap, path::Path, thread::JoinHandle};

    use axum::{body::Body, http::Request, middleware, routing::get, Extension, Router};
    use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
    use rhiza_obj_store::{ObjStore, ObjStoreConfig};
    use rhiza_quepaxa::{Membership, RecorderFileStore, ThreeNodeConsensus};
    use tower::ServiceExt;

    use crate::{CheckpointCoordinator, DurabilityMode, NodeConfig, NodeRuntime};

    use super::{
        admin_gate, admit_status_snapshot, handle_status, node_admin_status, run_status_snapshot,
        run_status_snapshot_until, status_snapshot_join_error, AdminErrorCode, AdminGateState,
        AdminPermit, AdminRouteState, AdminTaskTracker, NodeError, OperationLedger,
        StatusRefreshHook, ADMIN_ACTIVE_MASK, ADMIN_ADMISSION_OPEN, ADMIN_STATUS_PATH,
        ADMIN_STATUS_SNAPSHOT_TIMEOUT,
    };

    fn release_gate(gate: &Arc<(Mutex<bool>, Condvar)>) {
        let (released, changed) = &**gate;
        *released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        changed.notify_all();
    }

    struct GateRelease(Arc<(Mutex<bool>, Condvar)>);

    impl GateRelease {
        fn new(gate: Arc<(Mutex<bool>, Condvar)>) -> Self {
            Self(gate)
        }
    }

    impl Drop for GateRelease {
        fn drop(&mut self) {
            release_gate(&self.0);
        }
    }

    struct CommitPoisoner {
        gate: Arc<(Mutex<bool>, Condvar)>,
        thread: Option<JoinHandle<()>>,
    }

    impl CommitPoisoner {
        fn start(runtime: Arc<NodeRuntime>) -> (Self, std::sync::mpsc::Receiver<()>) {
            let gate = Arc::new((Mutex::new(false), Condvar::new()));
            let worker_gate = Arc::clone(&gate);
            let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(1);
            let thread = std::thread::spawn(move || {
                let _commit = runtime.lock_commit().unwrap();
                locked_tx.send(()).unwrap();
                await_gate(&worker_gate);
                panic!("test-only commit lock poisoner");
            });
            (
                Self {
                    gate,
                    thread: Some(thread),
                },
                locked_rx,
            )
        }

        fn release_and_join(&mut self) {
            release_gate(&self.gate);
            let result = self
                .thread
                .take()
                .expect("commit poisoner must be joined exactly once")
                .join();
            assert!(result.is_err(), "commit poisoner must panic while locked");
        }
    }

    impl Drop for CommitPoisoner {
        fn drop(&mut self) {
            release_gate(&self.gate);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    struct CompletionBeforePermitDrop {
        completed: std::sync::mpsc::SyncSender<()>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl Drop for CompletionBeforePermitDrop {
        fn drop(&mut self) {
            let _ = self.completed.send(());
            await_gate(&self.gate);
        }
    }

    fn await_gate(gate: &Arc<(Mutex<bool>, Condvar)>) {
        let (released, changed) = &**gate;
        let mut released = released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !*released {
            released = changed
                .wait(released)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn status_test_state(
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> (tempfile::TempDir, AdminRouteState, Arc<AdminPermit>) {
        status_test_state_with_options(hook, None, None, ADMIN_STATUS_SNAPSHOT_TIMEOUT)
    }

    fn status_test_state_with_options(
        hook: Arc<dyn Fn() + Send + Sync>,
        coordinator: Option<Arc<CheckpointCoordinator>>,
        before_refresh_durable_tip_hook: Option<StatusRefreshHook>,
        status_snapshot_timeout: Duration,
    ) -> (tempfile::TempDir, AdminRouteState, Arc<AdminPermit>) {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let config = NodeConfig::new_embedded(
            "admin-status-hook",
            "n1",
            root.path().join("node"),
            1,
            1,
            membership.members().iter().map(String::as_str),
        )
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                config.cluster_id(),
                "n1",
                1,
                1,
                [
                    root.path().join("recorders/n1"),
                    root.path().join("recorders/n2"),
                    root.path().join("recorders/n3"),
                ],
                1,
                crate::LogHash::ZERO,
            )
            .unwrap(),
        );
        let runtime = Arc::new(NodeRuntime::open(config, consensus, &[]).unwrap());
        let recorder = RecorderFileStore::new_with_membership(
            root.path().join("admin-recorder"),
            "n1",
            runtime.config().cluster_id(),
            1,
            1,
            membership,
        )
        .unwrap();
        let tasks = AdminTaskTracker::new();
        let permit = Arc::new(AdminPermit {
            _admission: Arc::new(tokio::sync::Semaphore::new(1))
                .try_acquire_owned()
                .unwrap(),
            _task: tasks.try_start().unwrap(),
        });
        let ledger_path = root.path().join("admin-operations-v1.json");
        (
            root,
            AdminRouteState {
                runtime,
                recorder,
                coordinator,
                status_snapshots: Arc::new(tokio::sync::Semaphore::new(1)),
                status_snapshot_hook: Some(hook),
                before_refresh_durable_tip_hook,
                status_snapshot_timeout,
                operations: Arc::new(tokio::sync::Mutex::new(Some(HashMap::new()))),
                ledger_path,
            },
            permit,
        )
    }

    async fn status_test_coordinator(root: &Path) -> Arc<CheckpointCoordinator> {
        let store = ObjStore::new(ObjStoreConfig::Local {
            root: root.to_path_buf(),
        })
        .unwrap();
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            store,
            CheckpointIdentity::new(
                "rhiza:sql:admin-status-test",
                1,
                1,
                LogHash::digest(&[b"admin-test-config"]),
                1,
            ),
        );
        archive.initialize_checkpoint().await.unwrap();
        Arc::new(
            CheckpointCoordinator::open(archive, DurabilityMode::Sync)
                .await
                .unwrap(),
        )
    }

    async fn assert_router_status_offloads_while_blocked() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _gate_release = GateRelease::new(Arc::clone(&gate));
        let worker_gate = Arc::clone(&gate);
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let hook = Arc::new(move || {
            entered_tx.send(()).unwrap();
            await_gate(&worker_gate);
        });
        let (_root, state, permit) = status_test_state(hook);
        let router = Router::new()
            .route("/status", get(handle_status))
            .layer(Extension(permit))
            .with_state(state);
        let status =
            tokio::spawn(router.oneshot(Request::get("/status").body(Body::empty()).unwrap()));

        let entered_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            match entered_rx.try_recv() {
                Ok(()) => break,
                Err(std::sync::mpsc::TryRecvError::Empty)
                    if tokio::time::Instant::now() < entered_deadline =>
                {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("status snapshot hook did not enter: {error}"),
            }
        }
        let (health_tx, health_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            health_tx.send(()).unwrap();
        });
        tokio::time::timeout(Duration::from_secs(1), health_rx)
            .await
            .expect("unrelated health task must progress while the handler snapshot blocks")
            .unwrap();

        release_gate(&gate);
        let response = status.await.unwrap().unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

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

    #[test]
    fn admin_tracker_state_machine_exhaustively_preserves_shutdown_quiescence() {
        #[derive(Clone, Copy, Debug)]
        struct ModelState {
            accepting: bool,
            active: usize,
        }

        impl ModelState {
            fn word(self) -> usize {
                (if self.accepting {
                    ADMIN_ADMISSION_OPEN
                } else {
                    0
                }) | self.active
            }

            fn start(self) -> (Self, bool) {
                if self.accepting {
                    (
                        Self {
                            active: self.active + 1,
                            ..self
                        },
                        true,
                    )
                } else {
                    (self, false)
                }
            }

            fn stop(self) -> Self {
                Self {
                    accepting: false,
                    ..self
                }
            }

            fn finish(self) -> Self {
                Self {
                    active: self.active - 1,
                    ..self
                }
            }
        }

        fn explore(state: ModelState, remaining: usize) {
            let word = state.word();
            assert_eq!(word & ADMIN_ADMISSION_OPEN != 0, state.accepting);
            assert_eq!(word & ADMIN_ACTIVE_MASK, state.active);
            assert_eq!(
                word == 0,
                !state.accepting && state.active == 0,
                "the packed word has exactly one quiescent state"
            );
            if remaining == 0 {
                return;
            }

            let (started, admitted) = state.start();
            assert_eq!(admitted, state.accepting);
            // A completed admission CAS is the only successful admission
            // transition; after Stop its result must be a rejection.
            if !state.accepting {
                assert_eq!(started.word(), state.word());
            }
            explore(started, remaining - 1);

            explore(state.stop(), remaining - 1);
            if state.active > 0 {
                explore(state.finish(), remaining - 1);
            }
        }

        // This visits every legal Start/Stop/Finish schedule through seven
        // linearization points. Loom is not a dependency in this workspace;
        // the companion Barrier test below covers the stale-CAS race in the
        // concrete atomic implementation.
        explore(
            ModelState {
                accepting: true,
                active: 0,
            },
            7,
        );
    }

    #[test]
    fn stop_admission_rejects_a_stale_concurrent_admission_cas() {
        let observed_open = Arc::new(Barrier::new(2));
        let release_admission = Arc::new(Barrier::new(2));
        let hook = {
            let observed_open = Arc::clone(&observed_open);
            let release_admission = Arc::clone(&release_admission);
            Arc::new(move || {
                observed_open.wait();
                release_admission.wait();
            })
        };
        let tracker = AdminTaskTracker::with_before_admission_cas_hook(hook);
        let racing_tracker = tracker.clone();
        let racing_admission = std::thread::spawn(move || racing_tracker.try_start().is_some());

        // The candidate has read the open word but cannot execute its CAS.
        // Stop linearizes first, so the candidate must retry against the
        // closed word and reject rather than increment active.
        observed_open.wait();
        tracker.stop_admission();
        release_admission.wait();
        assert!(!racing_admission.join().unwrap());
        assert_eq!(
            tracker.state.state.load(Ordering::Acquire),
            0,
            "a stale admission cannot survive the stop linearization point"
        );
    }

    #[tokio::test]
    async fn authenticated_keep_alive_admin_request_is_rejected_after_admission_stops() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let tasks = AdminTaskTracker::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
        let router = {
            let calls = Arc::clone(&calls);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Router::new()
                .route(
                    ADMIN_STATUS_PATH,
                    get(move || {
                        let calls = Arc::clone(&calls);
                        let entered = Arc::clone(&entered);
                        let release = Arc::clone(&release);
                        async move {
                            calls.fetch_add(1, Ordering::AcqRel);
                            entered.notify_one();
                            release
                                .lock()
                                .await
                                .take()
                                .expect("only the admitted handler reaches the test gate")
                                .await
                                .expect("test gate release sender remains alive");
                            "first admin request completed"
                        }
                    }),
                )
                .route_layer(middleware::from_fn_with_state(
                    AdminGateState {
                        token: "admin-test-token".into(),
                        admission: Arc::new(tokio::sync::Semaphore::new(1)),
                        tasks: tasks.clone(),
                    },
                    admin_gate,
                ))
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });

        let mut connection = tokio::net::TcpStream::connect(address).await.unwrap();
        let entered_wait = entered.notified();
        connection
            .write_all(
                b"GET /v1/admin/membership/status HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer admin-test-token\r\nx-rhiza-version: 1\r\nConnection: keep-alive\r\n\r\n",
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("authenticated first request was not admitted");

        tasks.stop_admission();
        connection
            .write_all(
                b"GET /v1/admin/membership/status HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer admin-test-token\r\nx-rhiza-version: 1\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        assert_eq!(
            calls.load(Ordering::Acquire),
            1,
            "the queued keep-alive request has not passed admission"
        );

        release_tx.send(()).unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(1),
            connection.read_to_end(&mut response),
        )
        .await
        .expect("keep-alive connection did not close after the second request")
        .unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.contains("200 OK"), "first response: {response}");
        assert!(
            response.contains("503 Service Unavailable"),
            "post-stop response: {response}"
        );
        assert_eq!(
            calls.load(Ordering::Acquire),
            1,
            "post-stop request must not enter the handler body"
        );
        let _ = stop.send(());
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("admin test server did not stop")
            .unwrap();
    }

    #[test]
    fn status_snapshot_offloads_on_current_thread_runtime_without_stalling_health() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let gate = Arc::new((Mutex::new(false), Condvar::new()));
            let _gate_release = GateRelease::new(Arc::clone(&gate));
            let worker_gate = Arc::clone(&gate);
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (health_tx, health_rx) = tokio::sync::oneshot::channel();
            let caller = std::thread::current().id();
            let snapshot = tokio::spawn(run_status_snapshot(move || {
                let worker = std::thread::current().id();
                entered_tx.send(()).unwrap();
                await_gate(&worker_gate);
                Ok(worker)
            }));

            entered_rx.await.unwrap();
            tokio::spawn(async move {
                health_tx.send(()).unwrap();
            });
            tokio::time::timeout(Duration::from_secs(1), health_rx)
                .await
                .expect("unrelated async health work must run while snapshot blocks")
                .unwrap();

            release_gate(&gate);
            assert_ne!(snapshot.await.unwrap().unwrap(), caller);
        });
    }

    #[test]
    fn router_status_handler_offloads_on_current_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(assert_router_status_offloads_while_blocked());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn router_status_handler_offloads_on_one_worker_runtime() {
        assert_router_status_offloads_while_blocked().await;
    }

    #[tokio::test]
    async fn router_status_refresh_deadline_is_absolute_and_never_starts_snapshot() {
        let archive = tempfile::tempdir().unwrap();
        let coordinator = status_test_coordinator(archive.path()).await;
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let refresh_calls_for_hook = Arc::clone(&refresh_calls);
        let refresh_hook: StatusRefreshHook = Arc::new(move || {
            refresh_calls_for_hook.fetch_add(1, Ordering::AcqRel);
            Box::pin(std::future::pending())
        });
        let snapshot_calls = Arc::new(AtomicUsize::new(0));
        let snapshot_calls_for_hook = Arc::clone(&snapshot_calls);
        let snapshot_hook = Arc::new(move || {
            snapshot_calls_for_hook.fetch_add(1, Ordering::AcqRel);
        });
        let (_root, state, permit) = status_test_state_with_options(
            snapshot_hook,
            Some(coordinator),
            Some(refresh_hook),
            Duration::from_millis(25),
        );
        let router = Router::new()
            .route("/status", get(handle_status))
            .layer(Extension(permit))
            .with_state(state);

        let started = tokio::time::Instant::now();
        let response = tokio::time::timeout(
            Duration::from_secs(1),
            router.oneshot(Request::get("/status").body(Body::empty()).unwrap()),
        )
        .await
        .expect("the injected absolute deadline must bound refresh")
        .unwrap();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(started.elapsed() < Duration::from_millis(250));
        assert_eq!(refresh_calls.load(Ordering::Acquire), 1);
        assert_eq!(snapshot_calls.load(Ordering::Acquire), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn timed_out_router_status_never_overadmits_detached_snapshot_work() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _gate_release = GateRelease::new(Arc::clone(&gate));
        let worker_gate = Arc::clone(&gate);
        let started = Arc::new(AtomicUsize::new(0));
        let worker_started = Arc::clone(&started);
        let active = Arc::new(AtomicUsize::new(0));
        let worker_active = Arc::clone(&active);
        let peak = Arc::new(AtomicUsize::new(0));
        let worker_peak = Arc::clone(&peak);
        let (finished_tx, finished_rx) = std::sync::mpsc::sync_channel(1);
        let hook = Arc::new(move || {
            worker_started.fetch_add(1, Ordering::AcqRel);
            let now_active = worker_active.fetch_add(1, Ordering::AcqRel) + 1;
            worker_peak.fetch_max(now_active, Ordering::AcqRel);
            await_gate(&worker_gate);
            worker_active.fetch_sub(1, Ordering::AcqRel);
            finished_tx.send(()).unwrap();
        });
        let (_root, state, permit) =
            status_test_state_with_options(hook, None, None, Duration::from_millis(25));
        let router = Router::new()
            .route("/status", get(handle_status))
            .layer(Extension(permit))
            .with_state(state);

        let first = router
            .clone()
            .oneshot(Request::get("/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(first.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(started.load(Ordering::Acquire), 1);
        assert_eq!(active.load(Ordering::Acquire), 1);

        for _ in 0..20 {
            let repeated = router
                .clone()
                .oneshot(Request::get("/status").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                repeated.status(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            );
            assert_eq!(started.load(Ordering::Acquire), 1);
            assert!(active.load(Ordering::Acquire) <= 1);
        }

        release_gate(&gate);
        let finished_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            match finished_rx.try_recv() {
                Ok(()) => break,
                Err(std::sync::mpsc::TryRecvError::Empty)
                    if tokio::time::Instant::now() < finished_deadline =>
                {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("timed-out status snapshot did not drain: {error}"),
            }
        }
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert_eq!(peak.load(Ordering::Acquire), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_router_status_recovers_snapshot_permit_after_closure_finishes() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _gate_release = GateRelease::new(Arc::clone(&gate));
        let worker_gate = Arc::clone(&gate);
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let hook_entered = Arc::clone(&entered);
        let finished = Arc::new(tokio::sync::Semaphore::new(0));
        let hook_finished = Arc::clone(&finished);
        let hook = Arc::new(move || {
            hook_entered.add_permits(1);
            await_gate(&worker_gate);
            hook_finished.add_permits(1);
        });
        let (_root, state, permit) = status_test_state(hook);
        let snapshots = Arc::clone(&state.status_snapshots);
        let router = Router::new()
            .route("/status", get(handle_status))
            .layer(Extension(permit))
            .with_state(state);
        let caller = tokio::spawn(
            router
                .clone()
                .oneshot(Request::get("/status").body(Body::empty()).unwrap()),
        );

        let entered_permit =
            tokio::time::timeout(Duration::from_secs(1), Arc::clone(&entered).acquire_owned())
                .await
                .expect("status snapshot hook did not enter")
                .expect("entry event semaphore must remain open");
        drop(entered_permit);
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());

        let rejected = router
            .clone()
            .oneshot(Request::get("/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            rejected.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );

        release_gate(&gate);
        let finished_permit = tokio::time::timeout(
            Duration::from_secs(1),
            Arc::clone(&finished).acquire_owned(),
        )
        .await
        .expect("cancelled status snapshot did not leave the hook")
        .expect("completion event semaphore must remain open");
        drop(finished_permit);

        // The hook's completion event is inside the blocking closure, before
        // `_admission` drops.  Taking and returning this permit is the
        // observable boundary that proves the detached closure has exited,
        // rather than racing a new request against its final destructor.
        let returned_admission = tokio::time::timeout(
            Duration::from_secs(1),
            Arc::clone(&snapshots).acquire_owned(),
        )
        .await
        .expect("cancelled status snapshot did not return its admission permit")
        .expect("snapshot admission semaphore must remain open");
        drop(returned_admission);

        let recovered = router
            .oneshot(Request::get("/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(recovered.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn panicked_router_status_recovers_snapshot_permit() {
        let panic_once = Arc::new(AtomicBool::new(false));
        let panic_once_for_hook = Arc::clone(&panic_once);
        let hook = Arc::new(move || {
            if !panic_once_for_hook.swap(true, Ordering::AcqRel) {
                panic!("test-only status snapshot panic");
            }
        });
        let (_root, state, permit) = status_test_state(hook);
        let router = Router::new()
            .route("/status", get(handle_status))
            .layer(Extension(permit))
            .with_state(state);

        let panicked = router
            .clone()
            .oneshot(Request::get("/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            panicked.status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        let recovered = router
            .oneshot(Request::get("/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(recovered.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn poisoned_commit_observation_is_500_without_latching_or_overadmission() {
        let snapshot_started = Arc::new(AtomicUsize::new(0));
        let snapshot_started_for_hook = Arc::clone(&snapshot_started);
        let snapshot_hook = Arc::new(move || {
            snapshot_started_for_hook.fetch_add(1, Ordering::AcqRel);
        });
        let (_root, state, permit) =
            status_test_state_with_options(snapshot_hook, None, None, Duration::from_millis(25));
        let runtime = Arc::clone(&state.runtime);
        let fatal_reason_before = runtime.fatal_reason();
        let (mut poisoner, locked) = CommitPoisoner::start(Arc::clone(&runtime));
        let lock_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            match locked.try_recv() {
                Ok(()) => break,
                Err(std::sync::mpsc::TryRecvError::Empty)
                    if tokio::time::Instant::now() < lock_deadline =>
                {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("commit poisoner did not acquire the lock: {error}"),
            }
        }
        let snapshots = Arc::clone(&state.status_snapshots);
        let router = Router::new()
            .route("/status", get(handle_status))
            .layer(Extension(permit))
            .with_state(state);
        let first = tokio::spawn(
            router
                .clone()
                .oneshot(Request::get("/status").body(Body::empty()).unwrap()),
        );

        let snapshot_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while snapshot_started.load(Ordering::Acquire) == 0 {
            assert!(
                tokio::time::Instant::now() < snapshot_deadline,
                "actual status handler did not reach the blocked commit observation"
            );
            tokio::task::yield_now().await;
        }
        let timed_out = tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .expect("the status response must respect its single deadline")
            .unwrap()
            .unwrap();
        assert_eq!(
            timed_out.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(snapshots.available_permits(), 0);

        poisoner.release_and_join();
        let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while snapshots.available_permits() != 1 {
            assert!(
                tokio::time::Instant::now() < drain_deadline,
                "detached status closure did not release its permit after poison"
            );
            tokio::task::yield_now().await;
        }
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());
        assert_eq!(runtime.fatal_reason(), fatal_reason_before);

        for _ in 0..200 {
            let poisoned = router
                .clone()
                .oneshot(Request::get("/status").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                poisoned.status(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            );
            assert_eq!(snapshots.available_permits(), 1);
            assert!(runtime.is_ready());
            assert!(!runtime.is_fatal());
            assert_eq!(runtime.fatal_reason(), fatal_reason_before);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completion_event_precedes_actual_permit_drop_without_overadmission_under_stress() {
        for _ in 0..200 {
            let gate = Arc::new((Mutex::new(false), Condvar::new()));
            let _gate_release = GateRelease::new(Arc::clone(&gate));
            let admission = Arc::new(tokio::sync::Semaphore::new(1));
            let permit = admit_status_snapshot(Arc::clone(&admission)).unwrap();
            let (completed_tx, completed_rx) = std::sync::mpsc::sync_channel(1);
            let worker_gate = Arc::clone(&gate);
            let snapshot = tokio::spawn(run_status_snapshot_until(
                tokio::time::Instant::now() + Duration::from_secs(1),
                move || {
                    let _permit = permit;
                    let _completion = CompletionBeforePermitDrop {
                        completed: completed_tx,
                        gate: worker_gate,
                    };
                    Ok(())
                },
            ));

            let completion_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            loop {
                match completed_rx.try_recv() {
                    Ok(()) => break,
                    Err(std::sync::mpsc::TryRecvError::Empty)
                        if tokio::time::Instant::now() < completion_deadline =>
                    {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("completion notification was not observed: {error}"),
                }
            }
            assert!(matches!(
                admit_status_snapshot(Arc::clone(&admission)),
                Err(NodeError::Unavailable(message)) if message.contains("capacity")
            ));

            release_gate(&gate);
            assert_eq!(snapshot.await.unwrap(), Ok(()));
            let recovered = tokio::time::timeout(Duration::from_secs(1), admission.acquire_owned())
                .await
                .expect("permit must drop when the closure actually exits")
                .expect("semaphore must remain open");
            drop(recovered);
        }
    }

    #[tokio::test]
    async fn status_snapshot_preserves_exact_node_error() {
        let expected = NodeError::Storage("exact snapshot source".into());
        assert_eq!(
            run_status_snapshot(move || Err::<(), _>(expected)).await,
            Err(NodeError::Storage("exact snapshot source".into()))
        );
    }

    #[tokio::test]
    async fn status_snapshot_join_errors_keep_transport_error_classes() {
        let cancelled = tokio::spawn(std::future::pending::<()>());
        cancelled.abort();
        let cancelled = cancelled.await.unwrap_err();
        let cancelled = status_snapshot_join_error(cancelled);
        assert!(matches!(
            &cancelled,
            NodeError::Unavailable(message) if message.contains("cancelled")
        ));
        assert_eq!(
            node_admin_status(&cancelled),
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                AdminErrorCode::Unavailable
            )
        );

        let panicked = tokio::task::spawn_blocking(|| panic!("snapshot worker panic"))
            .await
            .unwrap_err();
        let panicked = status_snapshot_join_error(panicked);
        assert!(matches!(
            &panicked,
            NodeError::Fatal(message) if message.contains("panicked")
        ));
        assert_eq!(
            node_admin_status(&panicked),
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                AdminErrorCode::Internal
            )
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_status_snapshot_returns_without_joining_and_drains_late_read_only_work() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _gate_release = GateRelease::new(Arc::clone(&gate));
        let worker_gate = Arc::clone(&gate);
        let active = Arc::new(AtomicUsize::new(0));
        let worker_active = Arc::clone(&active);
        let completed = Arc::new(AtomicBool::new(false));
        let worker_completed = Arc::clone(&completed);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(run_status_snapshot(move || {
            worker_active.fetch_add(1, Ordering::AcqRel);
            entered_tx.send(()).unwrap();
            await_gate(&worker_gate);
            worker_completed.store(true, Ordering::Release);
            worker_active.fetch_sub(1, Ordering::AcqRel);
            finished_tx.send(()).unwrap();
            Ok(())
        }));
        entered_rx.await.unwrap();

        caller.abort();
        let cancellation = tokio::time::timeout(Duration::from_secs(1), caller)
            .await
            .expect("cancelling the caller must not wait for the blocking snapshot")
            .expect_err("caller task must be cancelled");
        assert!(cancellation.is_cancelled());
        assert_eq!(active.load(Ordering::Acquire), 1);
        assert!(!completed.load(Ordering::Acquire));

        release_gate(&gate);
        tokio::time::timeout(Duration::from_secs(1), finished_rx)
            .await
            .expect("detached read-only snapshot must drain after its gate opens")
            .unwrap();
        assert!(completed.load(Ordering::Acquire));
        assert_eq!(active.load(Ordering::Acquire), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn status_snapshot_deadline_holds_closure_admission_until_late_work_drains() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _gate_release = GateRelease::new(Arc::clone(&gate));
        let worker_gate = Arc::clone(&gate);
        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = admit_status_snapshot(Arc::clone(&admission)).unwrap();
        let tasks = AdminTaskTracker::new();
        let response_task = tasks.try_start().unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let worker_active = Arc::clone(&active);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let result = tokio::spawn(run_status_snapshot_until(
            tokio::time::Instant::now() + Duration::from_millis(50),
            move || {
                let _permit = permit;
                worker_active.fetch_add(1, Ordering::AcqRel);
                entered_tx.send(()).unwrap();
                await_gate(&worker_gate);
                worker_active.fetch_sub(1, Ordering::AcqRel);
                finished_tx.send(()).unwrap();
                Ok(())
            },
        ));
        entered_rx.await.unwrap();

        assert!(matches!(
            result.await.unwrap(),
            Err(NodeError::Unavailable(message)) if message.contains("deadline")
        ));
        assert_eq!(active.load(Ordering::Acquire), 1);

        // The response task may finish during shutdown without an unbounded
        // blocking join, while the closure-held permit still prevents another
        // detached snapshot from accumulating.
        drop(response_task);
        tasks.stop_admission();
        tokio::time::timeout(Duration::from_secs(1), tasks.wait_for_idle())
            .await
            .expect("shutdown must not wait for a detached read-only snapshot");
        assert!(matches!(
            admit_status_snapshot(Arc::clone(&admission)),
            Err(NodeError::Unavailable(message)) if message.contains("capacity")
        ));

        release_gate(&gate);
        tokio::time::timeout(Duration::from_secs(1), finished_rx)
            .await
            .expect("deadline-detached read-only snapshot must drain after release")
            .unwrap();
        assert_eq!(active.load(Ordering::Acquire), 0);
        let recovered = tokio::time::timeout(Duration::from_secs(1), admission.acquire_owned())
            .await
            .expect("snapshot capacity must recover after the closure drops its permit")
            .expect("semaphore must remain open");
        drop(recovered);
    }
}
