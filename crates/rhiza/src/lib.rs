#[cfg(feature = "graph")]
use std::collections::BTreeMap;
use std::{
    fmt,
    future::Future,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

mod ha;

#[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
use rhiza_node::confirm_write_durability;
#[cfg(feature = "sql")]
use rhiza_node::NodeService;
use rhiza_node::{execution_profile_compiled, ConfigError, NodeRuntime};
use rhiza_quepaxa::{Error as ConsensusError, ThreeNodeConsensus};
use tokio::{
    sync::{watch, OwnedRwLockReadGuard, RwLock},
    task::{JoinError, JoinSet},
    time::Instant,
};

pub use rhiza_archive::ObjectArchiveStore;
pub use rhiza_core::{ErrorCategory, ErrorClassification, ExecutionProfile};
#[cfg(feature = "graph")]
pub use rhiza_graph::{
    CanonicalF64, GraphColumn, GraphCommandResultV1, GraphCommandV1, GraphInternalId,
    GraphLogicalType, GraphNode, GraphParameterValue, GraphQueryResult, GraphRel, GraphResultValue,
    GraphValueV1,
};
#[cfg(feature = "kv")]
pub use rhiza_kv::{KvCommandResultV1, KvCommandV1, KvScanResult, KvScanRow};
pub use rhiza_node::{
    effective_cluster_id, CertifiedTailRecord, CertifiedTailRequest, CertifiedTailResponse,
    CheckpointCoordinator, DurabilityError, DurabilityHealth, DurabilityMode, LearnerProgress,
    LogPeer, NodeConfig, NodeError, NodeStatus, PeerConfig, ReadConsistency, StopInformation,
};
#[cfg(feature = "graph")]
pub use rhiza_node::{GraphMutationOutcome, GraphReadResponse};
#[cfg(feature = "kv")]
pub use rhiza_node::{KvMutationOutcome, KvReadResponse};
#[cfg(feature = "sql")]
pub use rhiza_node::{
    ReadResponse, SqlExecuteResponse, SqlQueryResponse, SqlStatementResult, WriteRequest,
    WriteResponse,
};
pub use rhiza_quepaxa::{Membership, RecorderFileStore, RecorderRpc};
#[cfg(feature = "sql")]
pub use rhiza_sql::{SqlCommand, SqlQueryResult, SqlStatement, SqlValue};

pub use ha::{
    HaCertifiedTailError, HaCertifiedTailSource, HaNode, HaNodeError, HaNodeStatus, HaPredecessor,
    HaRecorderTransport, HaServeConfig, HaShutdownCause, HaShutdownPhase, HaStartupConfig,
    HaStartupError, HaStartupMode, HaSuccessorNode, HaSuccessorPrestageConfig,
    HaSuccessorPrestageIdentity, IngressDisposition, MutationCertainty,
    PreparedHaSuccessorPrestage, PublishedHaSuccessorPrestage, ShutdownToken, TaskDisposition,
};
#[cfg(feature = "test-hooks")]
pub use ha::{HaServiceActivationGate, HaServiceActivationRelease};

const MATERIALIZER_POLL_INTERVAL: Duration = Duration::from_millis(100);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);
const LOCAL_RECORDER_IDS: [&str; 3] = ["recorder-1", "recorder-2", "recorder-3"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmbeddedIdentity {
    cluster_id: String,
    node_id: String,
    epoch: u64,
    config_id: u64,
}

impl EmbeddedIdentity {
    pub fn new(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        epoch: u64,
        config_id: u64,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            node_id: node_id.into(),
            epoch,
            config_id,
        }
    }
}

pub struct EmbeddedConfig {
    identity: EmbeddedIdentity,
    data_dir: PathBuf,
    execution_profile: ExecutionProfile,
    members: Vec<String>,
    recorders: Vec<(String, Box<dyn RecorderRpc>)>,
    log_peers: Vec<Box<dyn LogPeer>>,
    coordinator: Option<Arc<CheckpointCoordinator>>,
}

impl EmbeddedConfig {
    /// Creates a fixed three-recorder configuration for one local process.
    ///
    /// This writes durable state below `root` and is not highly available: the node and all
    /// recorders share one process and failure domain. Use [`Self::new`] when transports or
    /// recorder membership must be supplied explicitly.
    pub fn local_file_backed(
        logical_cluster_id: impl Into<String>,
        root: impl Into<PathBuf>,
        execution_profile: ExecutionProfile,
    ) -> Result<Self, Error> {
        require_embedded_profile(execution_profile)?;
        let logical_cluster_id = logical_cluster_id.into();
        let cluster_id = effective_cluster_id(execution_profile, &logical_cluster_id)?;
        let root = root.into();
        let membership = Membership::new(LOCAL_RECORDER_IDS)?;
        let recorders = membership
            .members()
            .iter()
            .map(|id| {
                let recorder = RecorderFileStore::new_with_membership(
                    root.join("recorders").join(id),
                    id.clone(),
                    &cluster_id,
                    1,
                    1,
                    membership.clone(),
                )?;
                Ok((id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>))
            })
            .collect::<Result<Vec<_>, ConsensusError>>()?;

        Ok(Self::new(
            EmbeddedIdentity::new(logical_cluster_id, LOCAL_RECORDER_IDS[0], 1, 1),
            root.join("node"),
            execution_profile,
            membership.members().to_vec(),
            recorders,
            vec![],
            None,
        ))
    }

    /// Creates an embedded configuration from explicitly supplied transports.
    ///
    /// This is an advanced extension point for custom or remote deployments. The facade
    /// re-exports its `RecorderRpc` and `LogPeer` trait boundaries, but implementing them or
    /// using the component-specific transport vocabulary requires direct dependencies on
    /// `rhiza-quepaxa` and `rhiza-node`. Most applications should use [`Self::local_file_backed`].
    pub fn new(
        identity: EmbeddedIdentity,
        data_dir: impl Into<PathBuf>,
        execution_profile: ExecutionProfile,
        members: impl Into<Vec<String>>,
        recorders: Vec<(String, Box<dyn RecorderRpc>)>,
        log_peers: Vec<Box<dyn LogPeer>>,
        coordinator: Option<Arc<CheckpointCoordinator>>,
    ) -> Self {
        Self {
            identity,
            data_dir: data_dir.into(),
            execution_profile,
            members: members.into(),
            recorders,
            log_peers,
            coordinator,
        }
    }

    /// Adds a checkpoint coordinator to this configuration.
    pub fn with_coordinator(mut self, coordinator: Arc<CheckpointCoordinator>) -> Self {
        self.coordinator = Some(coordinator);
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownPhase {
    InFlightOperations,
    BackgroundWorkers,
    AppliedTipFlush,
}

#[derive(Debug)]
pub enum ShutdownCause {
    DeadlineExceeded,
    RecorderOutcomeUnknown,
    Source(Box<Error>),
    TaskFailure(ShutdownTaskFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownTaskFailure {
    Panicked,
    Cancelled,
}

/// Opaque failure from an internal asynchronous task.
///
/// The Tokio receipt remains available through [`std::error::Error::source`],
/// while callers receive only the stable failure kind.
#[derive(Debug)]
pub struct WorkerError {
    failure: ShutdownTaskFailure,
    source: JoinError,
}

impl WorkerError {
    fn from_join(source: JoinError) -> Self {
        let failure = if source.is_cancelled() {
            ShutdownTaskFailure::Cancelled
        } else {
            ShutdownTaskFailure::Panicked
        };
        Self { failure, source }
    }

    pub fn failure(&self) -> ShutdownTaskFailure {
        self.failure
    }
}

impl fmt::Display for WorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "internal task failure: {:?}", self.failure)
    }
}

impl std::error::Error for WorkerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug)]
pub struct ShutdownError {
    phase: ShutdownPhase,
    cause: ShutdownCause,
    cleanup: Vec<ShutdownError>,
    task_source: Option<Arc<WorkerError>>,
}

impl ShutdownError {
    pub fn phase(&self) -> ShutdownPhase {
        self.phase
    }

    pub fn cause(&self) -> &ShutdownCause {
        &self.cause
    }

    pub fn cleanup(&self) -> &[ShutdownError] {
        &self.cleanup
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ShutdownPhase,
        ShutdownCause,
        Vec<ShutdownError>,
        Option<Arc<WorkerError>>,
    ) {
        (self.phase, self.cause, self.cleanup, self.task_source)
    }

    pub(crate) fn new(phase: ShutdownPhase, cause: ShutdownCause) -> Self {
        Self {
            phase,
            cause,
            cleanup: Vec::new(),
            task_source: None,
        }
    }

    fn task_failure(phase: ShutdownPhase, source: JoinError) -> Self {
        let source = Arc::new(WorkerError::from_join(source));
        Self {
            phase,
            cause: ShutdownCause::TaskFailure(source.failure()),
            cleanup: Vec::new(),
            task_source: Some(source),
        }
    }
}

impl fmt::Display for ShutdownCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExceeded => formatter.write_str("deadline exceeded"),
            Self::RecorderOutcomeUnknown => formatter.write_str("recorder outcome is unknown"),
            Self::Source(error) => write!(formatter, "{error}"),
            Self::TaskFailure(kind) => write!(formatter, "task failure: {kind:?}"),
        }
    }
}

impl std::error::Error for ShutdownCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(error) => Some(error.as_ref()),
            Self::DeadlineExceeded | Self::RecorderOutcomeUnknown | Self::TaskFailure(_) => None,
        }
    }
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        shutdown_error_display(self, formatter)
    }
}

impl std::error::Error for ShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.task_source
            .as_ref()
            .map(|error| error as &(dyn std::error::Error + 'static))
            .or_else(|| <ShutdownCause as std::error::Error>::source(&self.cause))
    }
}

#[derive(Debug)]
pub enum Error {
    Closed,
    ExecutionProfileMismatch {
        expected: ExecutionProfile,
        actual: ExecutionProfile,
    },
    Config(ConfigError),
    Consensus(ConsensusError),
    Node(NodeError),
    Durability(DurabilityError),
    Shutdown(ShutdownError),
    WorkerExited {
        worker: &'static str,
    },
    Worker(WorkerError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "rhiza is closed"),
            Self::ExecutionProfileMismatch { expected, actual } => write!(
                f,
                "execution profile mismatch: expected {expected}, got {actual}"
            ),
            Self::Config(error) => error.fmt(f),
            Self::Consensus(error) => error.fmt(f),
            Self::Node(error) => error.fmt(f),
            Self::Durability(error) => error.fmt(f),
            Self::Shutdown(error) => shutdown_error_display(error, f),
            Self::WorkerExited { worker } => {
                write!(f, "embedded {worker} worker exited before shutdown")
            }
            Self::Worker(error) => write!(f, "embedded worker failed: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Closed | Self::ExecutionProfileMismatch { .. } => None,
            Self::Config(error) => Some(error),
            Self::Consensus(error) => Some(error),
            Self::Node(error) => Some(error),
            Self::Durability(error) => Some(error),
            Self::Shutdown(error) => match &error.cause {
                ShutdownCause::Source(error) => Some(error.as_ref()),
                ShutdownCause::TaskFailure(_) => error.task_source.as_ref().map(|error| error as _),
                ShutdownCause::DeadlineExceeded | ShutdownCause::RecorderOutcomeUnknown => None,
            },
            Self::WorkerExited { .. } => None,
            Self::Worker(error) => Some(error),
        }
    }
}

impl Error {
    /// Returns a stable machine-readable code, category, and retry guidance.
    pub fn classification(&self) -> ErrorClassification {
        match self {
            Self::Node(error) => error.classification(),
            Self::Closed => ErrorClassification::new("closed", ErrorCategory::Unavailable, true),
            Self::ExecutionProfileMismatch { .. } => ErrorClassification::new(
                "execution_profile_mismatch",
                ErrorCategory::Internal,
                false,
            ),
            Self::Config(_) => {
                ErrorClassification::new("config_error", ErrorCategory::Internal, false)
            }
            Self::Consensus(_) => {
                ErrorClassification::new("consensus_error", ErrorCategory::Unavailable, true)
            }
            Self::Durability(_) => {
                ErrorClassification::new("durability_error", ErrorCategory::Unavailable, true)
            }
            Self::Shutdown(error) => shutdown_error_classification(error),
            Self::WorkerExited { .. } => {
                ErrorClassification::new("worker_exited", ErrorCategory::Internal, false)
            }
            Self::Worker(_) => {
                ErrorClassification::new("worker_error", ErrorCategory::Internal, false)
            }
        }
    }
}

fn shutdown_error_display(
    error: &ShutdownError,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    write!(formatter, "shutdown failed during {:?}: ", error.phase)?;
    match &error.cause {
        ShutdownCause::DeadlineExceeded => formatter.write_str("deadline exceeded")?,
        ShutdownCause::RecorderOutcomeUnknown => {
            formatter.write_str("recorder outcome is unknown")?
        }
        ShutdownCause::Source(source) => write!(formatter, "{source}")?,
        ShutdownCause::TaskFailure(kind) => write!(formatter, "task failure: {kind:?}")?,
    }
    for cleanup in &error.cleanup {
        formatter.write_str("; cleanup also failed: ")?;
        shutdown_error_display(cleanup, formatter)?;
    }
    Ok(())
}

fn shutdown_error_classification(error: &ShutdownError) -> ErrorClassification {
    match &error.cause {
        ShutdownCause::DeadlineExceeded => ErrorClassification::new(
            "shutdown_deadline_exceeded",
            ErrorCategory::Unavailable,
            true,
        ),
        ShutdownCause::RecorderOutcomeUnknown => ErrorClassification::new(
            "shutdown_recorder_outcome_unknown",
            ErrorCategory::Unavailable,
            true,
        ),
        ShutdownCause::Source(error) => error.classification(),
        ShutdownCause::TaskFailure(_) => {
            ErrorClassification::new("worker_error", ErrorCategory::Internal, false)
        }
    }
}

/// An outer failure from an embedded typed batch write.
///
/// `NotAttempted` means the complete vector failed validation or admission before any command was
/// attempted. `Indeterminate` means execution may have committed commands but their durability
/// could not be confirmed. After `Indeterminate`, retry the entire unchanged vector with the same
/// request IDs; per-command idempotency makes that retry safe.
#[derive(Debug)]
pub enum BatchWriteError {
    NotAttempted(Error),
    Indeterminate(Error),
}

impl fmt::Display for BatchWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAttempted(error) => write!(f, "batch was not attempted: {error}"),
            Self::Indeterminate(error) => write!(f, "batch outcome is indeterminate: {error}"),
        }
    }
}

impl std::error::Error for BatchWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotAttempted(error) | Self::Indeterminate(error) => Some(error),
        }
    }
}

impl From<ConfigError> for Error {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<ConsensusError> for Error {
    fn from(error: ConsensusError) -> Self {
        Self::Consensus(error)
    }
}

impl From<NodeError> for Error {
    fn from(error: NodeError) -> Self {
        Self::Node(error)
    }
}

impl From<DurabilityError> for Error {
    fn from(error: DurabilityError) -> Self {
        Self::Durability(error)
    }
}

struct Inner {
    runtime: Arc<NodeRuntime>,
    #[cfg(feature = "sql")]
    service: NodeService,
    #[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
    execution_profile: ExecutionProfile,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    operations: Arc<RwLock<()>>,
    closed: AtomicBool,
    shutdown: watch::Sender<bool>,
    worker_monitor: watch::Sender<WorkerMonitorState>,
}

#[derive(Clone)]
enum WorkerMonitorState {
    Running,
    Failed(ErrorClassification),
    Closed,
}

/// Owns the embedded node runtime and its background workers.
///
/// Keep this owner alive for the lifetime of the application server. During planned shutdown,
/// first drain the server, then call [`Self::shutdown`]. Dropping the owner only signals its
/// workers and cannot report drain or durability errors.
pub struct Rhiza {
    inner: Option<Arc<Inner>>,
    workers: JoinSet<Result<(), Error>>,
}

#[derive(Clone)]
pub struct RhizaHandle {
    inner: Weak<Inner>,
}

impl Rhiza {
    pub async fn open(config: EmbeddedConfig) -> Result<Self, Error> {
        let EmbeddedConfig {
            identity,
            data_dir,
            execution_profile,
            members,
            recorders,
            log_peers,
            coordinator,
        } = config;
        require_embedded_profile(execution_profile)?;
        let node_config = NodeConfig::new_embedded(
            identity.cluster_id.clone(),
            identity.node_id.clone(),
            data_dir,
            identity.epoch,
            identity.config_id,
            members,
        )?
        .with_execution_profile(execution_profile)?;
        let effective_cluster_id = node_config.cluster_id().to_owned();
        let consensus = Arc::new(ThreeNodeConsensus::from_recorders_with_ids(
            effective_cluster_id,
            identity.node_id,
            identity.epoch,
            identity.config_id,
            recorders,
        )?);
        if node_config.membership() != consensus.membership() {
            return Err(ConfigError::PeerMembershipMismatch.into());
        }
        let peers: Vec<&dyn LogPeer> = log_peers.iter().map(Box::as_ref).collect();
        let runtime = Arc::new(NodeRuntime::open(node_config, consensus, &peers)?);

        if let Some(coordinator) = &coordinator {
            coordinator.note_recovered_committed(runtime.applied_index()?);
        }

        Ok(Self::from_open_runtime(runtime, coordinator))
    }

    fn from_open_runtime(
        runtime: Arc<NodeRuntime>,
        coordinator: Option<Arc<CheckpointCoordinator>>,
    ) -> Self {
        #[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
        let execution_profile = runtime.config().execution_profile();
        #[cfg(feature = "sql")]
        let service = NodeService::new(runtime.clone(), coordinator.clone());
        let (shutdown, _) = watch::channel(false);
        let (worker_monitor, _) = watch::channel(WorkerMonitorState::Running);
        let inner = Arc::new(Inner {
            runtime,
            #[cfg(feature = "sql")]
            service,
            #[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
            execution_profile,
            coordinator,
            operations: Arc::new(RwLock::new(())),
            closed: AtomicBool::new(false),
            shutdown,
            worker_monitor,
        });
        let mut workers = JoinSet::new();
        spawn_materializer(&inner, &mut workers);
        if inner.coordinator.is_some() {
            spawn_coordinator(&inner, &mut workers);
        }

        Self {
            inner: Some(inner),
            workers,
        }
    }

    pub fn handle(&self) -> RhizaHandle {
        RhizaHandle {
            inner: Arc::downgrade(self.inner.as_ref().expect("open owner has inner state")),
        }
    }

    /// Waits for the first background worker failure.
    ///
    /// `None` means graceful shutdown was requested. The returned classification is only a
    /// live-health signal; [`Self::shutdown`] retains and returns the original worker error.
    pub async fn wait_for_worker_failure(&self) -> Option<ErrorClassification> {
        let mut monitor = self
            .inner
            .as_ref()
            .expect("open owner has inner state")
            .worker_monitor
            .subscribe();
        loop {
            match monitor.borrow().clone() {
                WorkerMonitorState::Running => {}
                WorkerMonitorState::Failed(classification) => return Some(classification),
                WorkerMonitorState::Closed => return None,
            }
            if monitor.changed().await.is_err() {
                return None;
            }
        }
    }

    /// Drains embedded work within the default shutdown budget.
    pub async fn shutdown(self) -> Result<(), Error> {
        self.shutdown_with_timeout(SHUTDOWN_TIMEOUT).await
    }

    /// Drains embedded work within one timeout budget shared by every shutdown phase.
    pub async fn shutdown_with_timeout(self, timeout: Duration) -> Result<(), Error> {
        self.shutdown_with_deadline(Instant::now() + timeout).await
    }

    /// Drains embedded work before an absolute deadline shared by HA internals.
    pub(crate) async fn shutdown_with_deadline(mut self, deadline: Instant) -> Result<(), Error> {
        let inner = self.inner.take().expect("open owner has inner state");
        let mut errors = Vec::new();

        close_inner(&inner);
        let operations_drained =
            match tokio::time::timeout_at(deadline, inner.operations.write()).await {
                Ok(operations) => {
                    drop(operations);
                    true
                }
                Err(_) => {
                    errors.push(ShutdownError::new(
                        ShutdownPhase::InFlightOperations,
                        ShutdownCause::DeadlineExceeded,
                    ));
                    false
                }
            };

        signal_workers(&inner);
        let mut workers_stopped = true;
        while !self.workers.is_empty() {
            match tokio::time::timeout_at(deadline, self.workers.join_next()).await {
                Ok(Some(Ok(Ok(())))) => {}
                Ok(Some(Ok(Err(error)))) => errors.push(shutdown_error_from_source(
                    ShutdownPhase::BackgroundWorkers,
                    error,
                )),
                Ok(Some(Err(error))) => errors.push(ShutdownError::task_failure(
                    ShutdownPhase::BackgroundWorkers,
                    error,
                )),
                Ok(None) => break,
                Err(_) => {
                    errors.push(ShutdownError::new(
                        ShutdownPhase::BackgroundWorkers,
                        ShutdownCause::DeadlineExceeded,
                    ));
                    self.workers.abort_all();
                    workers_stopped = false;
                    break;
                }
            }
        }

        if operations_drained && workers_stopped {
            match tokio::time::timeout_at(deadline, flush_applied_tip(&inner)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(shutdown_error_from_source(
                    ShutdownPhase::AppliedTipFlush,
                    error,
                )),
                Err(_) => errors.push(ShutdownError::new(
                    ShutdownPhase::AppliedTipFlush,
                    ShutdownCause::DeadlineExceeded,
                )),
            }
        }

        drop(inner);
        combine_shutdown_errors(errors)
    }
}

impl Drop for Rhiza {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            stop_inner(inner);
        }
        self.workers.abort_all();
    }
}

impl RhizaHandle {
    pub(crate) fn close_admission(&self) {
        if let Some(inner) = self.inner.upgrade() {
            close_inner(&inner);
        }
    }

    #[cfg(feature = "sql")]
    pub async fn put(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
    ) -> Result<WriteResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Sqlite)?;
        Ok(inner.service.put(request_id, key, value).await?)
    }

    #[cfg(feature = "sql")]
    pub async fn write(&self, request: WriteRequest) -> Result<WriteResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Sqlite)?;
        Ok(inner.service.write(request).await?)
    }

    #[cfg(feature = "sql")]
    pub async fn execute_sql(&self, command: SqlCommand) -> Result<SqlExecuteResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Sqlite)?;
        Ok(inner.service.execute_sql(command).await?)
    }

    /// Executes an ordered, non-atomic SQL batch that may coalesce commands into fewer log entries.
    ///
    /// The returned vector has the same length and order as `commands`. An outer `NotAttempted`
    /// guarantees that no command was attempted. After `Indeterminate`, retry the entire unchanged
    /// vector with the same request IDs.
    #[cfg(feature = "sql")]
    pub async fn execute_sql_batch(
        &self,
        commands: Vec<SqlCommand>,
    ) -> Result<Vec<Result<SqlExecuteResponse, NodeError>>, BatchWriteError> {
        self.execute_typed_batch(
            ExecutionProfile::Sqlite,
            move |runtime| runtime.execute_sql_batch(commands),
            |response| response.applied_index,
        )
        .await
    }

    #[cfg(feature = "sql")]
    pub async fn read(
        &self,
        key: &str,
        consistency: ReadConsistency,
    ) -> Result<ReadResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Sqlite)?;
        Ok(inner.service.read(key, consistency).await?)
    }

    #[cfg(feature = "sql")]
    pub async fn query(
        &self,
        statement: SqlStatement,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<SqlQueryResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Sqlite)?;
        Ok(inner
            .service
            .query(statement, consistency, max_rows)
            .await?)
    }

    #[cfg(feature = "graph")]
    pub async fn mutate_graph(
        &self,
        command: GraphCommandV1,
    ) -> Result<GraphMutationOutcome, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Graph)?;
        embedded_write_allowed(&inner)?;
        let runtime = inner.runtime.clone();
        let outcome = tokio::task::spawn_blocking(move || runtime.mutate_graph(command))
            .await
            .map_err(|error| Error::Worker(WorkerError::from_join(error)))??;
        confirm_embedded_write(&inner, outcome.applied_index()).await?;
        Ok(outcome)
    }

    /// Executes an ordered, non-atomic graph batch that may coalesce commands into fewer log entries.
    ///
    /// The returned vector has the same length and order as `commands`. An outer `NotAttempted`
    /// guarantees that no command was attempted. After `Indeterminate`, retry the entire unchanged
    /// vector with the same request IDs.
    #[cfg(feature = "graph")]
    pub async fn mutate_graph_batch(
        &self,
        commands: Vec<GraphCommandV1>,
    ) -> Result<Vec<Result<GraphMutationOutcome, NodeError>>, BatchWriteError> {
        self.execute_typed_batch(
            ExecutionProfile::Graph,
            move |runtime| runtime.mutate_graph_batch(commands),
            GraphMutationOutcome::applied_index,
        )
        .await
    }

    #[cfg(feature = "graph")]
    pub async fn query_graph(
        &self,
        statement: impl Into<String>,
        parameters: BTreeMap<String, GraphParameterValue>,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<GraphQueryResult, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Graph)?;
        let runtime = inner.runtime.clone();
        let statement = statement.into();
        tokio::task::spawn_blocking(move || {
            runtime.query_graph(&statement, &parameters, consistency, max_rows)
        })
        .await
        .map_err(|error| Error::Worker(WorkerError::from_join(error)))?
        .map_err(Error::Node)
    }

    #[cfg(feature = "graph")]
    pub async fn get_graph_document(
        &self,
        id: impl Into<String>,
        consistency: ReadConsistency,
    ) -> Result<GraphReadResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Graph)?;
        let runtime = inner.runtime.clone();
        let id = id.into();
        tokio::task::spawn_blocking(move || runtime.get_graph_document(&id, consistency))
            .await
            .map_err(|error| Error::Worker(WorkerError::from_join(error)))?
            .map_err(Error::Node)
    }

    /// Stores a key-value pair. An exact retry with the same `request_id` replays the
    /// original result; reuse with different bytes is a conflict.
    #[cfg(feature = "kv")]
    pub async fn kv_put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        request_id: String,
    ) -> Result<KvMutationOutcome, Error> {
        let command = KvCommandV1::put(request_id, key, value)
            .map_err(|error| Error::Node(NodeError::InvalidRequest(error.to_string())))?;
        self.kv_mutate(command).await
    }

    /// Deletes a key. Returns `existed: true` if the key was present.
    #[cfg(feature = "kv")]
    pub async fn kv_delete(
        &self,
        key: Vec<u8>,
        request_id: String,
    ) -> Result<KvMutationOutcome, Error> {
        let command = KvCommandV1::delete(request_id, key)
            .map_err(|error| Error::Node(NodeError::InvalidRequest(error.to_string())))?;
        self.kv_mutate(command).await
    }

    /// Executes a single KV mutation command. Prefer [`Self::kv_put`] or
    /// [`Self::kv_delete`] for simple operations.
    #[cfg(feature = "kv")]
    pub async fn kv_mutate(&self, command: KvCommandV1) -> Result<KvMutationOutcome, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Kv)?;
        embedded_write_allowed(&inner)?;
        let runtime = inner.runtime.clone();
        let outcome = tokio::task::spawn_blocking(move || runtime.mutate_kv(command))
            .await
            .map_err(|error| Error::Worker(WorkerError::from_join(error)))??;
        confirm_embedded_write(&inner, outcome.applied_index()).await?;
        Ok(outcome)
    }

    /// Executes an ordered, non-atomic KV batch that may coalesce commands into fewer log entries.
    ///
    /// The returned vector has the same length and order as `commands`. An outer `NotAttempted`
    /// guarantees that no command was attempted. After `Indeterminate`, retry the entire unchanged
    /// vector with the same request IDs.
    #[cfg(feature = "kv")]
    pub async fn kv_batch(
        &self,
        commands: Vec<KvCommandV1>,
    ) -> Result<Vec<Result<KvMutationOutcome, NodeError>>, BatchWriteError> {
        self.execute_typed_batch(
            ExecutionProfile::Kv,
            move |runtime| runtime.mutate_kv_batch(commands),
            KvMutationOutcome::applied_index,
        )
        .await
    }

    /// Reads a single key. Returns `value: None` if the key does not exist.
    #[cfg(feature = "kv")]
    pub async fn kv_get(
        &self,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<KvReadResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Kv)?;
        let runtime = inner.runtime.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || runtime.get_kv(&key, consistency))
            .await
            .map_err(|error| Error::Worker(WorkerError::from_join(error)))?
            .map_err(Error::Node)
    }

    /// Scans keys in `[start, end)` range. Pass `end: None` for unbounded.
    /// Use `cursor` for pagination from a previous scan result.
    #[cfg(feature = "kv")]
    pub async fn kv_scan_range(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: usize,
        cursor: Option<Vec<u8>>,
        consistency: ReadConsistency,
    ) -> Result<KvScanResult, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Kv)?;
        let runtime = inner.runtime.clone();
        tokio::task::spawn_blocking(move || {
            runtime.scan_kv_range(
                &start,
                end.as_deref(),
                limit,
                cursor.as_deref(),
                consistency,
            )
        })
        .await
        .map_err(|error| Error::Worker(WorkerError::from_join(error)))?
        .map_err(Error::Node)
    }

    /// Scans keys with the given prefix. Use `cursor` for pagination from a previous scan.
    #[cfg(feature = "kv")]
    pub async fn kv_scan_prefix(
        &self,
        prefix: Vec<u8>,
        limit: usize,
        cursor: Option<Vec<u8>>,
        consistency: ReadConsistency,
    ) -> Result<KvScanResult, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Kv)?;
        let runtime = inner.runtime.clone();
        tokio::task::spawn_blocking(move || {
            runtime.scan_kv_prefix(&prefix, limit, cursor.as_deref(), consistency)
        })
        .await
        .map_err(|error| Error::Worker(WorkerError::from_join(error)))?
        .map_err(Error::Node)
    }

    pub async fn status(&self) -> Result<NodeStatus, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        let runtime = inner.runtime.clone();
        let mut status = tokio::task::spawn_blocking(move || runtime.status())
            .await
            .map_err(|error| Error::Worker(WorkerError::from_join(error)))??;
        if inner
            .coordinator
            .as_ref()
            .is_some_and(|coordinator| coordinator.health() == DurabilityHealth::Unavailable)
        {
            status.ready = false;
        }
        Ok(status)
    }

    async fn begin_operation(&self) -> Result<(Arc<Inner>, OwnedRwLockReadGuard<()>), Error> {
        let inner = self.inner.upgrade().ok_or(Error::Closed)?;
        let operation = inner.operations.clone().read_owned().await;
        if inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok((inner, operation))
    }

    #[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
    async fn execute_typed_batch<T, F, I>(
        &self,
        profile: ExecutionProfile,
        execute: F,
        applied_index: I,
    ) -> Result<Vec<Result<T, NodeError>>, BatchWriteError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<NodeRuntime>) -> Result<Vec<Result<T, NodeError>>, NodeError>
            + Send
            + 'static,
        I: Fn(&T) -> rhiza_core::LogIndex,
    {
        let (inner, _operation) = self
            .begin_operation()
            .await
            .map_err(BatchWriteError::NotAttempted)?;
        require_profile(&inner, profile).map_err(BatchWriteError::NotAttempted)?;
        embedded_write_allowed(&inner).map_err(BatchWriteError::NotAttempted)?;
        let runtime = inner.runtime.clone();
        let results = tokio::task::spawn_blocking(move || execute(runtime))
            .await
            .map_err(|error| {
                BatchWriteError::Indeterminate(Error::Worker(WorkerError::from_join(error)))
            })?
            .map_err(|error| BatchWriteError::NotAttempted(Error::Node(error)))?;
        if let Some(index) = results
            .iter()
            .filter_map(|result| result.as_ref().ok().map(&applied_index))
            .max()
        {
            confirm_embedded_write(&inner, index)
                .await
                .map_err(BatchWriteError::Indeterminate)?;
        }
        Ok(results)
    }
}

#[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
fn require_profile(inner: &Inner, expected: ExecutionProfile) -> Result<(), Error> {
    if inner.execution_profile == expected {
        Ok(())
    } else {
        Err(Error::ExecutionProfileMismatch {
            expected,
            actual: inner.execution_profile,
        })
    }
}

fn require_embedded_profile(execution_profile: ExecutionProfile) -> Result<(), Error> {
    if execution_profile_compiled(execution_profile) {
        Ok(())
    } else {
        let expected = [
            ExecutionProfile::Sqlite,
            ExecutionProfile::Graph,
            ExecutionProfile::Kv,
        ]
        .into_iter()
        .find(|profile| execution_profile_compiled(*profile))
        .unwrap_or(ExecutionProfile::Sqlite);
        Err(Error::ExecutionProfileMismatch {
            expected,
            actual: execution_profile,
        })
    }
}

#[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
fn embedded_write_allowed(inner: &Inner) -> Result<(), Error> {
    if let Some(coordinator) = &inner.coordinator {
        coordinator.write_allowed()?;
    }
    Ok(())
}

#[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
async fn confirm_embedded_write(
    inner: &Inner,
    applied_index: rhiza_core::LogIndex,
) -> Result<(), Error> {
    confirm_write_durability(
        inner.runtime.as_ref(),
        inner.coordinator.as_deref(),
        applied_index,
    )
    .await
    .map_err(Error::Durability)
}

fn spawn_materializer(inner: &Arc<Inner>, workers: &mut JoinSet<Result<(), Error>>) {
    let runtime = inner.runtime.clone();
    let shutdown = inner.shutdown.subscribe();
    let worker_monitor = inner.worker_monitor.clone();
    workers.spawn(supervise_worker(
        "materializer",
        shutdown.clone(),
        worker_monitor,
        async move {
            runtime
                .run_background_materializer(
                    MATERIALIZER_POLL_INTERVAL,
                    wait_for_shutdown(shutdown),
                )
                .await
                .map_err(Error::Node)
        },
    ));
}

fn spawn_coordinator(inner: &Arc<Inner>, workers: &mut JoinSet<Result<(), Error>>) {
    let coordinator = inner.coordinator.as_ref().unwrap().clone();
    let runtime = inner.runtime.clone();
    let shutdown = inner.shutdown.subscribe();
    let worker_monitor = inner.worker_monitor.clone();
    workers.spawn(supervise_worker(
        "checkpoint coordinator",
        shutdown.clone(),
        worker_monitor,
        async move {
            coordinator
                .run_background(runtime, wait_for_shutdown(shutdown))
                .await
                .map_err(Error::Durability)
        },
    ));
}

async fn supervise_worker<F>(
    worker_name: &'static str,
    shutdown: watch::Receiver<bool>,
    worker_monitor: watch::Sender<WorkerMonitorState>,
    worker: F,
) -> Result<(), Error>
where
    F: Future<Output = Result<(), Error>> + Send + 'static,
{
    let mut exit_monitor = WorkerExitMonitor {
        shutdown,
        worker_monitor,
        armed: true,
    };
    let result = worker.await;
    if *exit_monitor.shutdown.borrow() {
        exit_monitor.armed = false;
        return result;
    }

    let error = match result {
        Ok(()) => Error::WorkerExited {
            worker: worker_name,
        },
        Err(error) => error,
    };
    exit_monitor.publish(error.classification());
    Err(error)
}

struct WorkerExitMonitor {
    shutdown: watch::Receiver<bool>,
    worker_monitor: watch::Sender<WorkerMonitorState>,
    armed: bool,
}

impl WorkerExitMonitor {
    fn publish(&mut self, classification: ErrorClassification) {
        self.armed = false;
        self.worker_monitor.send_if_modified(|state| {
            if matches!(state, WorkerMonitorState::Running) {
                *state = WorkerMonitorState::Failed(classification);
                true
            } else {
                false
            }
        });
    }
}

impl Drop for WorkerExitMonitor {
    fn drop(&mut self) {
        if !self.armed || *self.shutdown.borrow() {
            return;
        }
        let classification =
            ErrorClassification::new("worker_error", ErrorCategory::Internal, false);
        self.worker_monitor.send_if_modified(|state| {
            if matches!(state, WorkerMonitorState::Running) {
                *state = WorkerMonitorState::Failed(classification);
                true
            } else {
                false
            }
        });
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
}

async fn flush_applied_tip(inner: &Inner) -> Result<(), Error> {
    let Some(coordinator) = &inner.coordinator else {
        return Ok(());
    };
    if !inner.runtime.configuration_state()?.is_active() {
        return Ok(());
    }
    let applied_tip = inner.runtime.applied_index()?;
    let observed = coordinator.refresh_durable_tip().await?;
    if observed.index() >= applied_tip {
        return Ok(());
    }
    coordinator.note_committed(applied_tip);
    coordinator
        .flush_runtime(&inner.runtime, applied_tip)
        .await?;
    Ok(())
}

fn combine_shutdown_errors(mut errors: Vec<ShutdownError>) -> Result<(), Error> {
    if errors.is_empty() {
        return Ok(());
    }
    let mut primary = errors.remove(0);
    if errors.is_empty() {
        Err(Error::Shutdown(primary))
    } else {
        primary.cleanup = errors;
        Err(Error::Shutdown(primary))
    }
}

fn shutdown_error_from_source(phase: ShutdownPhase, error: Error) -> ShutdownError {
    let cause = match error {
        Error::Consensus(ConsensusError::UnknownOutcome) => ShutdownCause::RecorderOutcomeUnknown,
        error => ShutdownCause::Source(Box::new(error)),
    };
    ShutdownError::new(phase, cause)
}

fn close_inner(inner: &Inner) {
    inner.closed.store(true, Ordering::Release);
    inner.runtime.cancel_operations();
}

fn signal_workers(inner: &Inner) {
    let _ = inner.shutdown.send(true);
    inner.worker_monitor.send_if_modified(|state| {
        if matches!(state, WorkerMonitorState::Running) {
            *state = WorkerMonitorState::Closed;
            true
        } else {
            false
        }
    });
}

fn stop_inner(inner: &Inner) {
    close_inner(inner);
    signal_workers(inner);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Default)]
    struct TestBlockingRelease(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);

    impl TestBlockingRelease {
        fn wait(&self) {
            let (released, condition) = &*self.0;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
        }

        fn release(&self) {
            let (released, condition) = &*self.0;
            *released.lock().unwrap() = true;
            condition.notify_all();
        }
    }

    impl Drop for TestBlockingRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    const TEST_SLOT_GATE_MAX_WAIT: Duration = Duration::from_secs(15);

    #[derive(Clone)]
    struct TestSlotGate(Arc<(std::sync::Mutex<TestSlotGateState>, std::sync::Condvar)>);

    #[derive(Default)]
    struct TestSlotGateState {
        entered: usize,
        released: bool,
    }

    struct TestSlotGateRelease(TestSlotGate);

    impl TestSlotGate {
        fn new() -> (Self, TestSlotGateRelease) {
            let gate = Self(Arc::new((
                std::sync::Mutex::new(TestSlotGateState::default()),
                std::sync::Condvar::new(),
            )));
            (gate.clone(), TestSlotGateRelease(gate))
        }

        /// Blocks one recorder RPC until the test releases this gate. The
        /// bound turns a lost test release into an attributable recorder
        /// error instead of an indefinitely stuck worker thread.
        fn wait(&self) -> rhiza_quepaxa::Result<()> {
            let started = std::time::Instant::now();
            let (state, changed) = &*self.0;
            let mut state = state.lock().unwrap();
            state.entered += 1;
            changed.notify_all();
            while !state.released {
                let Some(remaining) = TEST_SLOT_GATE_MAX_WAIT.checked_sub(started.elapsed()) else {
                    return Err(ConsensusError::Io(
                        "test slot gate timed out before release".into(),
                    ));
                };
                let (next, timed_out) = changed.wait_timeout(state, remaining).unwrap();
                state = next;
                if timed_out.timed_out() && !state.released {
                    return Err(ConsensusError::Io(
                        "test slot gate timed out before release".into(),
                    ));
                }
            }
            Ok(())
        }

        fn wait_for_entered(&self, required: usize, timeout: Duration) -> bool {
            let started = std::time::Instant::now();
            let (state, changed) = &*self.0;
            let mut state = state.lock().unwrap();
            while state.entered < required {
                let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                    return false;
                };
                let (next, timed_out) = changed.wait_timeout(state, remaining).unwrap();
                state = next;
                if timed_out.timed_out() && state.entered < required {
                    return false;
                }
            }
            true
        }

        fn release(&self) {
            let (state, changed) = &*self.0;
            state.lock().unwrap().released = true;
            changed.notify_all();
        }
    }

    impl TestSlotGateRelease {
        fn release(&self) {
            self.0.release();
        }
    }

    impl Drop for TestSlotGateRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    #[derive(Clone, Default)]
    struct TestOperationProbes(
        Arc<std::sync::Mutex<std::collections::BTreeMap<u64, Arc<std::sync::atomic::AtomicUsize>>>>,
    );

    impl TestOperationProbes {
        fn register(&self, slot: u64) -> Arc<std::sync::atomic::AtomicUsize> {
            let probe = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            self.0.lock().unwrap().insert(slot, Arc::clone(&probe));
            probe
        }

        fn enter(&self, slot: u64) -> Option<TestOperationProbeLease> {
            let probe = self.0.lock().unwrap().get(&slot).cloned()?;
            probe.fetch_add(1, Ordering::AcqRel);
            Some(TestOperationProbeLease { probe })
        }
    }

    struct TestOperationProbeLease {
        probe: Arc<std::sync::atomic::AtomicUsize>,
    }

    static CONSENSUS_GROUP_STRESS_SERIALIZER: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();

    impl Drop for TestOperationProbeLease {
        fn drop(&mut self) {
            self.probe.fetch_sub(1, Ordering::AcqRel);
        }
    }

    struct TestBlockingRecorder {
        inner: RecorderFileStore,
        started: std::sync::mpsc::Sender<()>,
        release: TestBlockingRelease,
        probes: TestOperationProbes,
    }

    impl RecorderRpc for TestBlockingRecorder {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            self.inner.recorder_id()
        }

        fn store_command_for(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            cluster_id: String,
            epoch: u64,
            config_id: u64,
            config_digest: rhiza_core::LogHash,
            command_hash: rhiza_core::LogHash,
            command: rhiza_core::StoredCommand,
        ) -> rhiza_quepaxa::Result<()> {
            self.inner.store_command_for(
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                command,
            )
        }

        fn fetch_command_for(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            cluster_id: String,
            epoch: u64,
            config_id: u64,
            config_digest: rhiza_core::LogHash,
            command_hash: rhiza_core::LogHash,
        ) -> rhiza_quepaxa::Result<Option<rhiza_core::StoredCommand>> {
            self.inner
                .fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash)
        }

        fn stage_effect_bundle_chunk(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            binding: rhiza_quepaxa::EffectBundleBinding,
            manifest_command: rhiza_core::StoredCommand,
            ordinal: u16,
            chunk: Vec<u8>,
        ) -> rhiza_quepaxa::Result<()> {
            RecorderRpc::stage_effect_bundle_chunk(
                &self.inner,
                context,
                binding,
                manifest_command,
                ordinal,
                chunk,
            )
        }

        fn finalize_staged_effect_bundle(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            binding: rhiza_quepaxa::EffectBundleBinding,
            manifest_command: rhiza_core::StoredCommand,
        ) -> rhiza_quepaxa::Result<()> {
            RecorderRpc::finalize_staged_effect_bundle(
                &self.inner,
                context,
                binding,
                manifest_command,
            )
        }

        fn fetch_effect_bundle_manifest(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            binding: rhiza_quepaxa::EffectBundleBinding,
        ) -> rhiza_quepaxa::Result<Option<rhiza_core::StoredCommand>> {
            RecorderRpc::fetch_effect_bundle_manifest(&self.inner, context, binding)
        }

        fn fetch_effect_bundle_chunk(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            binding: rhiza_quepaxa::EffectBundleBinding,
            ordinal: u16,
        ) -> rhiza_quepaxa::Result<Option<Vec<u8>>> {
            RecorderRpc::fetch_effect_bundle_chunk(&self.inner, context, binding, ordinal)
        }

        fn record(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            request: rhiza_quepaxa::RecordRequest,
        ) -> rhiza_quepaxa::Result<rhiza_quepaxa::RecordSummary> {
            let _probe = self.probes.enter(request.slot);
            let _ = self.started.send(());
            self.release.wait();
            self.inner.record(request)
        }

        fn install_decision_proof(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            proof: rhiza_quepaxa::DecisionProof,
            membership: &rhiza_quepaxa::Membership,
        ) -> rhiza_quepaxa::Result<()> {
            self.inner.install_decision_proof(proof, membership)
        }

        fn inspect_decision_proof(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<rhiza_quepaxa::DecisionProof>> {
            self.inner.inspect_decision_proof(slot)
        }

        fn inspect_record_summary(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<rhiza_quepaxa::RecordSummary>> {
            self.inner.inspect_record_summary(slot)
        }
    }

    /// Test-only recorder wrapper. Slot gates are instance-owned and scoped
    /// to this exact test configuration; no global hook can affect another
    /// consensus instance or later stress iteration.
    struct TestSlotGatedRecorder {
        inner: RecorderFileStore,
        gates: std::collections::BTreeMap<u64, TestSlotGate>,
        probes: TestOperationProbes,
    }

    impl RecorderRpc for TestSlotGatedRecorder {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            self.inner.recorder_id()
        }

        fn store_command_for(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            cluster_id: String,
            epoch: u64,
            config_id: u64,
            config_digest: rhiza_core::LogHash,
            command_hash: rhiza_core::LogHash,
            command: rhiza_core::StoredCommand,
        ) -> rhiza_quepaxa::Result<()> {
            self.inner.store_command_for(
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                command,
            )
        }

        fn fetch_command_for(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            cluster_id: String,
            epoch: u64,
            config_id: u64,
            config_digest: rhiza_core::LogHash,
            command_hash: rhiza_core::LogHash,
        ) -> rhiza_quepaxa::Result<Option<rhiza_core::StoredCommand>> {
            self.inner
                .fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash)
        }

        fn stage_effect_bundle_chunk(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            binding: rhiza_quepaxa::EffectBundleBinding,
            manifest_command: rhiza_core::StoredCommand,
            ordinal: u16,
            chunk: Vec<u8>,
        ) -> rhiza_quepaxa::Result<()> {
            RecorderRpc::stage_effect_bundle_chunk(
                &self.inner,
                context,
                binding,
                manifest_command,
                ordinal,
                chunk,
            )
        }

        fn finalize_staged_effect_bundle(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            binding: rhiza_quepaxa::EffectBundleBinding,
            manifest_command: rhiza_core::StoredCommand,
        ) -> rhiza_quepaxa::Result<()> {
            RecorderRpc::finalize_staged_effect_bundle(
                &self.inner,
                context,
                binding,
                manifest_command,
            )
        }

        fn fetch_effect_bundle_manifest(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            binding: rhiza_quepaxa::EffectBundleBinding,
        ) -> rhiza_quepaxa::Result<Option<rhiza_core::StoredCommand>> {
            RecorderRpc::fetch_effect_bundle_manifest(&self.inner, context, binding)
        }

        fn fetch_effect_bundle_chunk(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            binding: rhiza_quepaxa::EffectBundleBinding,
            ordinal: u16,
        ) -> rhiza_quepaxa::Result<Option<Vec<u8>>> {
            RecorderRpc::fetch_effect_bundle_chunk(&self.inner, context, binding, ordinal)
        }

        fn record(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            request: rhiza_quepaxa::RecordRequest,
        ) -> rhiza_quepaxa::Result<rhiza_quepaxa::RecordSummary> {
            let _probe = self.probes.enter(request.slot);
            if let Some(gate) = self.gates.get(&request.slot) {
                gate.wait()?;
            }
            self.inner.record(request)
        }

        fn install_decision_proof(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            proof: rhiza_quepaxa::DecisionProof,
            membership: &rhiza_quepaxa::Membership,
        ) -> rhiza_quepaxa::Result<()> {
            self.inner.install_decision_proof(proof, membership)
        }

        fn inspect_decision_proof(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<rhiza_quepaxa::DecisionProof>> {
            self.inner.inspect_decision_proof(slot)
        }

        fn inspect_record_summary(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<rhiza_quepaxa::RecordSummary>> {
            self.inner.inspect_record_summary(slot)
        }
    }

    fn test_config_with_blocked_recorder(
        root: &std::path::Path,
    ) -> (
        EmbeddedConfig,
        std::sync::mpsc::Receiver<()>,
        TestBlockingRelease,
        TestOperationProbes,
    ) {
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let cluster_id = effective_cluster_id(ExecutionProfile::Sqlite, "cluster-a").unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let release = TestBlockingRelease::default();
        let probes = TestOperationProbes::default();
        let recorders = membership
            .members()
            .iter()
            .enumerate()
            .map(|(index, id)| {
                let recorder = RecorderFileStore::new_with_membership(
                    root.join("recorders").join(id),
                    id.clone(),
                    &cluster_id,
                    1,
                    1,
                    membership.clone(),
                )?;
                let recorder: Box<dyn RecorderRpc> = if index == 2 {
                    Box::new(TestBlockingRecorder {
                        inner: recorder,
                        started: started_tx.clone(),
                        release: release.clone(),
                        probes: probes.clone(),
                    })
                } else {
                    Box::new(recorder)
                };
                Ok((id.clone(), recorder))
            })
            .collect::<Result<Vec<_>, ConsensusError>>()
            .unwrap();
        (
            EmbeddedConfig::new(
                EmbeddedIdentity::new("cluster-a", "node-1", 1, 1),
                root.join("node"),
                ExecutionProfile::Sqlite,
                membership.members().to_vec(),
                recorders,
                vec![],
                None,
            ),
            started_rx,
            release,
            probes,
        )
    }

    fn test_config_with_slot_gated_recorder_quorums(
        root: &std::path::Path,
        owned_slot: u64,
        unowned_slot: u64,
    ) -> (
        EmbeddedConfig,
        TestSlotGate,
        TestSlotGateRelease,
        TestSlotGate,
        TestSlotGateRelease,
        TestOperationProbes,
    ) {
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let cluster_id = effective_cluster_id(ExecutionProfile::Sqlite, "cluster-a").unwrap();
        let (owned_gate, owned_release) = TestSlotGate::new();
        let (unowned_gate, unowned_release) = TestSlotGate::new();
        let probes = TestOperationProbes::default();
        let recorders = membership
            .members()
            .iter()
            .enumerate()
            .map(|(index, id)| {
                let recorder = RecorderFileStore::new_with_membership(
                    root.join("recorders").join(id),
                    id.clone(),
                    &cluster_id,
                    1,
                    1,
                    membership.clone(),
                )?;
                // The owned quorum occupies recorders 0/1. The unowned
                // quorum is recorders 1/2. Recorder 1's unowned RPC queues
                // behind its owned RPC, while recorder 2 is actively gated.
                // Both unowned quorum jobs therefore remain admitted at the
                // shutdown boundary without inventing a global test hook.
                let mut gates = std::collections::BTreeMap::new();
                if index <= 1 {
                    gates.insert(owned_slot, owned_gate.clone());
                }
                // Recorder 1's unowned RPC queues behind its owned RPC;
                // recorder 2 is the one actively held at the unowned
                // recorder boundary. Keep this gate specific so the test can
                // prove an unowned record() attempt rather than merely a
                // proposal thread start.
                if index == 2 {
                    gates.insert(unowned_slot, unowned_gate.clone());
                }
                Ok((
                    id.clone(),
                    Box::new(TestSlotGatedRecorder {
                        inner: recorder,
                        gates,
                        probes: probes.clone(),
                    }) as Box<dyn RecorderRpc>,
                ))
            })
            .collect::<Result<Vec<_>, ConsensusError>>()
            .unwrap();
        (
            EmbeddedConfig::new(
                EmbeddedIdentity::new("cluster-a", "node-1", 1, 1),
                root.join("node"),
                ExecutionProfile::Sqlite,
                membership.members().to_vec(),
                recorders,
                vec![],
                None,
            ),
            owned_gate,
            owned_release,
            unowned_gate,
            unowned_release,
            probes,
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn open_rejects_recorder_membership_before_creating_runtime_storage() {
        let root = tempfile::tempdir().unwrap();
        let mut config =
            EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Sqlite)
                .unwrap();
        config.members = vec![
            "recorder-1".into(),
            "recorder-2".into(),
            "recorder-4".into(),
        ];

        assert!(matches!(
            Rhiza::open(config).await,
            Err(Error::Config(ConfigError::PeerMembershipMismatch))
        ));
        assert!(!root.path().join("node").exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_does_not_wait_for_an_unowned_shared_consensus_group() {
        let root = tempfile::tempdir().unwrap();
        let (config, started_rx, release, _probes) = test_config_with_blocked_recorder(root.path());
        let owner = Rhiza::open(config).await.unwrap();
        let consensus = owner.inner.as_ref().unwrap().runtime.consensus().clone();
        let unrelated = std::thread::spawn(move || {
            consensus.propose_at(
                rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                1,
                rhiza_core::LogHash::ZERO,
                rhiza_core::Command::new(
                    rhiza_core::CommandKind::Deterministic,
                    b"outside".to_vec(),
                ),
            )
        });
        tokio::task::spawn_blocking(move || started_rx.recv())
            .await
            .unwrap()
            .unwrap();

        tokio::time::timeout(
            Duration::from_millis(250),
            owner.shutdown_with_timeout(Duration::from_millis(100)),
        )
        .await
        .expect("shutdown must not wait for an unowned consensus group")
        .unwrap();
        assert!(
            !unrelated.is_finished(),
            "shutdown must not cancel or quarantine the unrelated group"
        );
        release.release();
        unrelated.join().unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_deadline_reports_only_the_owned_admitted_operation() {
        let root = tempfile::tempdir().unwrap();
        let owner = Rhiza::open(
            EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Sqlite)
                .unwrap(),
        )
        .await
        .unwrap();
        let handle = owner.handle();
        let (_, operation) = handle.begin_operation().await.unwrap();

        let error = owner
            .shutdown_with_timeout(Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::Shutdown(ShutdownError {
                phase: ShutdownPhase::InFlightOperations,
                cause: ShutdownCause::DeadlineExceeded,
                ..
            })
        ));
        drop(operation);
        assert!(matches!(
            handle.put("after-shutdown", "key", "value").await,
            Err(Error::Closed)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_deadline_bounds_a_blocked_owned_proposal_without_touching_other_groups() {
        let root = tempfile::tempdir().unwrap();
        let (config, started_rx, release, _probes) = test_config_with_blocked_recorder(root.path());
        let owner = Rhiza::open(config).await.unwrap();
        let handle = owner.handle();
        let owned_handle = handle.clone();
        let owned = tokio::spawn(async move { owned_handle.put("owned", "key", "value").await });
        tokio::task::spawn_blocking(move || started_rx.recv())
            .await
            .unwrap()
            .unwrap();

        let shutdown = owner.shutdown_with_timeout(Duration::from_millis(1)).await;
        assert!(matches!(
            shutdown,
            Err(Error::Shutdown(ShutdownError {
                phase: ShutdownPhase::InFlightOperations,
                cause: ShutdownCause::DeadlineExceeded,
                ..
            }))
        ));
        assert!(
            !owned.is_finished(),
            "the admitted proposal remains its caller's scope"
        );
        release.release();
        let outcome = owned.await.unwrap();
        assert!(
            outcome.is_ok()
                || matches!(
                    outcome,
                    Err(Error::Node(NodeError::OutcomeUnknown(ref message)))
                        if message == "QuePaxa recorder RPC outcome is unknown; recover recorder state"
                ),
            "the owned proposal must resolve to its exact success or mutation-unknown outcome: {outcome:?}"
        );
        assert!(matches!(
            handle.put("after-shutdown", "key", "value").await,
            Err(Error::Closed)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn owned_and_unowned_consensus_groups_do_not_cross_cancel_under_shutdown_races() {
        let _serial = CONSENSUS_GROUP_STRESS_SERIALIZER
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await;
        for iteration in 0..200 {
            let root = tempfile::tempdir().unwrap();
            let owned_slot = 1;
            let unowned_slot = 100 + iteration;
            let (config, owned_gate, owned_release, unowned_gate, unowned_release, probes) =
                test_config_with_slot_gated_recorder_quorums(root.path(), owned_slot, unowned_slot);
            let owner = Rhiza::open(config).await.unwrap();
            let handle = owner.handle();
            let consensus = owner.inner.as_ref().unwrap().runtime.consensus().clone();
            let owned_probe = probes.register(owned_slot);
            let unowned_probe = probes.register(unowned_slot);
            let owned_group_probe = Arc::new(rhiza_quepaxa::TestControlOperationProbe::default());
            let unowned_group_probe = Arc::new(rhiza_quepaxa::TestControlOperationProbe::default());
            let _owned_group_probe = consensus
                .install_test_record_operation_probe(owned_slot, Arc::clone(&owned_group_probe))
                .unwrap();
            let _unowned_group_probe = consensus
                .install_test_record_operation_probe(unowned_slot, Arc::clone(&unowned_group_probe))
                .unwrap();
            let owned_handle = handle.clone();
            let owned =
                tokio::spawn(async move { owned_handle.put("owned", "key", "value").await });
            let readiness_watchdog = Duration::from_secs(15);
            assert!(
                tokio::task::spawn_blocking({
                    let owned_gate = owned_gate.clone();
                    move || owned_gate.wait_for_entered(2, readiness_watchdog)
                })
                .await
                .expect("owned gate waiter must not panic"),
                "owned group {iteration} did not enter both slot-scoped quorum gates"
            );
            assert!(
                tokio::time::timeout(readiness_watchdog, async {
                    while owned_group_probe.outstanding() != 2 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .is_ok(),
                "owned recorder 2 did not drain before unowned dispatch: \
                 dispatched={} max_outstanding={} outstanding={} drained={} workers={:?}",
                owned_group_probe.dispatch_count(),
                owned_group_probe.observed_max_outstanding(),
                owned_group_probe.outstanding(),
                owned_group_probe.drained_count(),
                owned_group_probe.worker_transitions(),
            );
            let (unrelated_started_tx, unrelated_started_rx) = std::sync::mpsc::sync_channel(1);
            let unrelated_consensus = consensus.clone();
            let unrelated = std::thread::spawn(move || {
                unrelated_started_tx
                    .send(())
                    .expect("unowned proposal start receiver must stay alive");
                unrelated_consensus.propose_at(
                    // This is the caller-owned operation budget, not the
                    // intentionally short shutdown deadline below. Keep it
                    // generous so the test observes draining after its gate
                    // opens rather than an unrelated RPC budget expiry.
                    rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(30)),
                    unowned_slot,
                    rhiza_core::LogHash::ZERO,
                    rhiza_core::Command::new(
                        rhiza_core::CommandKind::Deterministic,
                        format!("unowned-{iteration}").into_bytes(),
                    ),
                )
            });
            assert!(
                tokio::task::spawn_blocking(move || {
                    unrelated_started_rx.recv_timeout(readiness_watchdog)
                })
                .await
                .expect("unowned proposal start waiter must not panic")
                .is_ok(),
                "unowned proposal thread {iteration} did not start before the dispatch watchdog"
            );
            assert!(
                tokio::task::spawn_blocking({
                    let unowned_group_probe = Arc::clone(&unowned_group_probe);
                    move || unowned_group_probe.wait_for_admitted_outstanding(readiness_watchdog)
                })
                .await
                .expect("unowned admission waiter must not panic"),
                "unowned proposal thread {iteration} started but did not dispatch an admitted \
                 recorder RPC group: dispatched={} max_outstanding={} outstanding={} drained={} workers={:?}",
                unowned_group_probe.dispatch_count(),
                unowned_group_probe.observed_max_outstanding(),
                unowned_group_probe.outstanding(),
                unowned_group_probe.drained_count(),
                unowned_group_probe.worker_transitions(),
            );
            assert!(
                tokio::task::spawn_blocking({
                    let unowned_gate = unowned_gate.clone();
                    move || unowned_gate.wait_for_entered(1, readiness_watchdog)
                })
                .await
                .expect("unowned gate waiter must not panic"),
                "unowned group {iteration} dispatched but recorder 2 did not enter its \
                 non-overlapping slot gate"
            );
            assert!(
                owned_group_probe.wait_for_admitted_outstanding(readiness_watchdog),
                "owned group {iteration} did not admit an outstanding lease: \
                 dispatched={} max_outstanding={} outstanding={} drained={} workers={:?}",
                owned_group_probe.dispatch_count(),
                owned_group_probe.observed_max_outstanding(),
                owned_group_probe.outstanding(),
                owned_group_probe.drained_count(),
                owned_group_probe.worker_transitions(),
            );
            assert!(
                owned_group_probe.outstanding() >= 2,
                "owned group {iteration} did not retain its gated quorum at shutdown: workers={:?}",
                owned_group_probe.worker_transitions(),
            );
            assert!(
                unowned_group_probe.outstanding() >= 2,
                "unowned group {iteration} did not retain its gated quorum at shutdown: workers={:?}",
                unowned_group_probe.worker_transitions(),
            );
            assert!(
                !unrelated.is_finished(),
                "unowned group {iteration} completed before the shutdown boundary"
            );

            assert!(matches!(
                owner.shutdown_with_timeout(Duration::from_millis(1)).await,
                Err(Error::Shutdown(ShutdownError {
                    phase: ShutdownPhase::InFlightOperations,
                    cause: ShutdownCause::DeadlineExceeded,
                    ..
                }))
            ));
            assert!(
                !unrelated.is_finished(),
                "shutdown iteration {iteration} cross-cancelled the unrelated consensus group"
            );
            assert_eq!(
                owned_group_probe.cancel_count(),
                0,
                "owned group {iteration} stays caller-owned and drains cooperatively after its gate releases"
            );
            assert_eq!(
                owned_group_probe.quarantine_count(),
                0,
                "owned group {iteration} must not quarantine a cooperative recorder"
            );
            assert_eq!(
                unowned_group_probe.cancel_count(),
                0,
                "shutdown iteration {iteration} cancelled the unrelated RPC group"
            );
            assert_eq!(
                unowned_group_probe.quarantine_count(),
                0,
                "shutdown iteration {iteration} quarantined the unrelated RPC group"
            );
            assert!(
                unowned_group_probe.outstanding() >= 2,
                "shutdown iteration {iteration} drained the unrelated gated quorum"
            );
            assert!(matches!(
                handle.put("after-shutdown", "key", "value").await,
                Err(Error::Closed)
            ));
            owned_release.release();
            unowned_release.release();
            let owned_outcome = owned.await.unwrap();
            assert!(
                matches!(
                    owned_outcome,
                    Err(Error::Node(NodeError::OutcomeUnknown(ref message)))
                        if message == "QuePaxa recorder RPC outcome is unknown; recover recorder state"
                ),
                "owned operation {iteration} must report the exact shutdown mutation-indeterminate result: {owned_outcome:?}"
            );
            let unrelated_outcome = unrelated.join().unwrap();
            assert!(
                unrelated_outcome.is_ok(),
                "unowned operation {iteration} must complete successfully after its gate releases: {unrelated_outcome:?}"
            );
            tokio::time::timeout(Duration::from_secs(15), async {
                while owned_group_probe.outstanding() != 0 || unowned_group_probe.outstanding() != 0
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "iteration {iteration} RPC groups did not drain: owned \
                     pending={} outstanding={} dispatched={} drained={} workers={:?}; unowned \
                     pending={} outstanding={} dispatched={} drained={} workers={:?}",
                    owned_group_probe.pending(),
                    owned_group_probe.outstanding(),
                    owned_group_probe.dispatch_count(),
                    owned_group_probe.drained_count(),
                    owned_group_probe.worker_transitions(),
                    unowned_group_probe.pending(),
                    unowned_group_probe.outstanding(),
                    unowned_group_probe.dispatch_count(),
                    unowned_group_probe.drained_count(),
                    unowned_group_probe.worker_transitions(),
                )
            });
            assert_eq!(
                owned_probe.load(Ordering::Acquire),
                0,
                "owned group {iteration} leaked a recorder lease"
            );
            assert_eq!(
                unowned_probe.load(Ordering::Acquire),
                0,
                "unowned group {iteration} leaked a recorder lease"
            );
            assert_eq!(
                owned_group_probe.outstanding(),
                0,
                "owned group {iteration} retained an admitted QuePaxa lease"
            );
            assert_eq!(
                unowned_group_probe.outstanding(),
                0,
                "unowned group {iteration} retained an admitted QuePaxa lease"
            );
            assert!(
                owned_group_probe.drained_count() > 0,
                "owned group {iteration} did not observe a completed lease"
            );
            assert!(
                unowned_group_probe.drained_count() > 0,
                "unowned group {iteration} did not observe a completed lease"
            );
            drop(_owned_group_probe);
            drop(_unowned_group_probe);
            assert_eq!(
                consensus.test_record_operation_probe_registration_count(),
                0,
                "iteration {iteration} leaked an instance-scoped record probe"
            );
            // Keep the iteration boundary real: no caller, consensus handle,
            // or operation-local probe escapes into the next race.
            drop(handle);
            drop(consensus);
            drop(owned_release);
            drop(unowned_release);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn record_probe_isolates_equal_slots_in_separate_consensus_instances() {
        let _serial = CONSENSUS_GROUP_STRESS_SERIALIZER
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await;
        for _ in 0..200 {
            let first_root = tempfile::tempdir().unwrap();
            let second_root = tempfile::tempdir().unwrap();
            let first_owner = Rhiza::open(
                EmbeddedConfig::local_file_backed(
                    "cluster-a",
                    first_root.path(),
                    ExecutionProfile::Sqlite,
                )
                .unwrap(),
            )
            .await
            .unwrap();
            let second_owner = Rhiza::open(
                EmbeddedConfig::local_file_backed(
                    "cluster-a",
                    second_root.path(),
                    ExecutionProfile::Sqlite,
                )
                .unwrap(),
            )
            .await
            .unwrap();
            let first_consensus = first_owner
                .inner
                .as_ref()
                .unwrap()
                .runtime
                .consensus()
                .clone();
            let second_consensus = second_owner
                .inner
                .as_ref()
                .unwrap()
                .runtime
                .consensus()
                .clone();
            let first_probe = Arc::new(rhiza_quepaxa::TestControlOperationProbe::default());
            let second_probe = Arc::new(rhiza_quepaxa::TestControlOperationProbe::default());
            let first_guard = first_consensus
                .install_test_record_operation_probe(7, Arc::clone(&first_probe))
                .unwrap();
            let second_guard = second_consensus
                .install_test_record_operation_probe(7, Arc::clone(&second_probe))
                .unwrap();

            first_consensus
                .propose_at(
                    rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                    7,
                    rhiza_core::LogHash::ZERO,
                    rhiza_core::Command::new(
                        rhiza_core::CommandKind::Deterministic,
                        b"first".to_vec(),
                    ),
                )
                .unwrap();
            assert!(first_consensus.finish_pending_rpcs(Duration::from_secs(1)));
            assert!(
                first_probe.wait_for_quiescence(Duration::from_secs(15)),
                "first probe did not quiesce after diagnostic finish: dispatched={} \
                 outstanding={} drained={} cancel={} quarantine={} workers={:?}",
                first_probe.dispatch_count(),
                first_probe.outstanding(),
                first_probe.drained_count(),
                first_probe.cancel_count(),
                first_probe.quarantine_count(),
                first_probe.worker_transitions(),
            );
            assert!(first_probe.dispatch_count() > 0);
            assert!(first_probe.observed_max_outstanding() > 0);
            assert_eq!(second_probe.dispatch_count(), 0);
            assert_eq!(second_probe.observed_max_outstanding(), 0);
            assert_eq!(second_probe.outstanding(), 0);
            assert_eq!(second_probe.cancel_count(), 0);
            assert_eq!(second_probe.quarantine_count(), 0);
            assert_eq!(second_probe.drained_count(), 0);
            let first_snapshot = (
                first_probe.dispatch_count(),
                first_probe.observed_max_outstanding(),
                first_probe.outstanding(),
                first_probe.cancel_count(),
                first_probe.quarantine_count(),
                first_probe.drained_count(),
            );

            second_consensus
                .propose_at(
                    rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                    7,
                    rhiza_core::LogHash::ZERO,
                    rhiza_core::Command::new(
                        rhiza_core::CommandKind::Deterministic,
                        b"second".to_vec(),
                    ),
                )
                .unwrap();
            assert!(second_consensus.finish_pending_rpcs(Duration::from_secs(1)));
            assert!(second_probe.dispatch_count() > 0);
            assert!(second_probe.observed_max_outstanding() > 0);
            assert_eq!(
                first_snapshot,
                (
                    first_probe.dispatch_count(),
                    first_probe.observed_max_outstanding(),
                    first_probe.outstanding(),
                    first_probe.cancel_count(),
                    first_probe.quarantine_count(),
                    first_probe.drained_count(),
                ),
                "same slot on a separate consensus instance mutated the first probe"
            );
            drop(first_guard);
            drop(second_guard);
            assert_eq!(
                first_consensus.test_record_operation_probe_registration_count(),
                0
            );
            assert_eq!(
                second_consensus.test_record_operation_probe_registration_count(),
                0
            );
        }
    }

    #[cfg(not(feature = "graph"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn open_rejects_non_sql_profile_before_creating_runtime_storage() {
        let root = tempfile::tempdir().unwrap();
        let config = EmbeddedConfig::new(
            EmbeddedIdentity::new("cluster-a", "node-1", 1, 1),
            root.path().join("node"),
            ExecutionProfile::Graph,
            vec![],
            vec![],
            vec![],
            None,
        );

        assert!(matches!(
            Rhiza::open(config).await,
            Err(Error::ExecutionProfileMismatch {
                expected: ExecutionProfile::Sqlite,
                actual: ExecutionProfile::Graph,
            })
        ));
        assert!(!root.path().join("node").exists());
    }
}
