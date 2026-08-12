//! High-availability node ownership and membership replacement.
//!
//! # Flow Overview
//!
//! ## Normal Node Startup
//!
//! ```text
//! HaStartupConfig::new(...)
//!     .start(HaServeConfig::new(recorder, service, transport, recorders, log_peers))
//!     -> HaNode
//!         .ready() -> RhizaHandle
//!         .monitor() -> watches for failures
//!         .shutdown() -> graceful drain
//! ```
//!
//! ## Membership Replacement (Stop-and-Replace)
//!
//! ```text
//! HaSuccessorPrestageConfig::new(...)
//!     .prepare() -> PreparedHaSuccessorPrestage
//!         .publish(...) -> PublishedHaSuccessorPrestage
//!             .apply_page(...)  // catch-up from predecessor
//!             .finalize(...) -> HaStartupConfig
//!                 .with_predecessor(predecessor)
//!                 .start(serve) -> HaSuccessorNode
//!                     .bind_predecessor(predecessor)
//!                     .ready() -> RhizaHandle
//!                     .shutdown()
//! ```
//!
//! # Key Types
//!
//! - [`HaStartupConfig`]: Configuration for a normal or successor node startup.
//! - [`HaServeConfig`]: Bound listeners, transport, and recorder/log-peer clients.
//! - [`HaNode`]: Running normal node owner. Call [`HaNode::ready`] then [`HaNode::monitor`].
//! - [`HaSuccessorNode`]: Running successor node owner after membership replacement.
//! - [`HaSuccessorPrestageConfig`]: Entry point for the successor prestage flow.
//!
//! # Shutdown
//!
//! Both [`HaNode`] and [`HaSuccessorNode`] implement the same shutdown contract:
//! call [`HaNode::shutdown`] (not just drop) to drain and report errors.

use std::{
    fmt, fs,
    future::Future,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[cfg(test)]
use std::collections::VecDeque;
#[cfg(test)]
use std::sync::{Condvar, OnceLock};

use hyper::{body::Incoming, server::conn::http1};
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use rhiza_archive::{CheckpointIdentity, CheckpointPublisherOptions, ObjectArchiveStore};
use rhiza_core::{
    ConfigChange, ErrorClassification, ExecutionProfile, LogAnchor, LogHash, StopBinding,
    StoredCommand,
};
use rhiza_log::LogStore;
use rhiza_node::{
    certified_tail_router_for_runtime,
    durability::{
        adopt_finalized_successor_prestage, complete_adopted_successor_prestage,
        inspect_successor_prestage, prestage_successor_checkpoint, publish_successor_prestage,
        DurabilityError, SuccessorPrestage, SuccessorPrestageIdentity, SuccessorPrestageState,
    },
    node_router_with_checkpoint, node_router_with_checkpoint_and_admin_tasks,
    recorder_router_for_generation, recover_successor_recorder_after_checkpoint,
    rehydrate_recorder_after_checkpoint, serve_recorder_tcp, serve_recorder_tcp_tls, AdminConfig,
    AdminTaskTracker, CertifiedTailRequest, CertifiedTailResponse, CheckpointCoordinator,
    DurabilityMode, LearnerProgress, LearnerStore, LogPeer, NodeConfig, NodeError, NodeRuntime,
    NodeStatus, RecorderIngressExit, RecorderIngressLifecycle,
    RecorderTaskDisposition as NodeRecorderTaskDisposition, RecorderTlsServerConfig,
    StartupCancellationAuthority, StartupCancellationToken, StartupIoContext, StopInformation,
    TailReaderConfig, DEFAULT_CERTIFIED_TAIL_ENTRIES, LIVEZ_PATH, MAX_CERTIFIED_TAIL_ENTRIES,
    READYZ_PATH,
};
#[cfg(feature = "recorder-postcard-rpc")]
use rhiza_node::{
    serve_recorder_postcard_rpc, serve_recorder_postcard_rpc_tls,
    RecorderPostcardRpcTlsServerConfig,
};
use rhiza_quepaxa::{
    AcceptedValue, DecisionProof, EffectBundleBinding, Membership, ReadFenceObservation,
    ReadFenceRequest, RecordRequest, RecordSummary, RecorderFileStore, RecorderPreflight,
    RecorderRpc, ThreeNodeConsensus,
};
use serde::{Deserialize, Serialize};
use tower::ServiceExt as _;

use crate::{Rhiza, RhizaHandle};

const LOCAL_CHECKPOINT_IDENTITY_FILE: &str = rhiza_node::durability::LOCAL_CHECKPOINT_IDENTITY_FILE;
const MAX_LOCAL_CHECKPOINT_IDENTITY_BYTES: u64 = 4 * 1024;
const SUCCESSOR_RESTORE_INTENT_FILE: &str = ".successor-restore.intent";
const SUCCESSOR_RESTORE_COMPLETE_FILE: &str = ".successor-restore.complete";
const MAX_SUCCESSOR_RESTORE_CONTROL_BYTES: u64 = 16 * 1024;
const STARTUP_RETRY_DELAY: Duration = Duration::from_millis(100);
const HA_STATUS_POLL_INTERVAL: Duration = Duration::from_millis(100);
const SUCCESSOR_TAIL_RETRY_DELAY: Duration = Duration::from_millis(250);
const ACCEPT_RESOURCE_BACKOFF: Duration = Duration::from_secs(1);
const HA_SERVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);
const HA_SERVER_ABORT_RECEIPT_RESERVE: Duration = Duration::from_millis(50);
// One prepared checkpoint can retain the full archive restore budget. Keep
// process-wide HA preparation bounded without adding a runtime/config knob.
const HA_CHECKPOINT_PREPARE_CONCURRENCY: usize = 2;
static HA_CHECKPOINT_PREPARE_SLOTS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(HA_CHECKPOINT_PREPARE_CONCURRENCY);

fn lock_unpoison<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HaStartupMode {
    Bootstrap,
    Rejoin,
    Disaster,
}

pub enum HaRecorderTransport {
    Http,
    TcpPostcard,
    TcpTlsPostcard(RecorderTlsServerConfig),
    #[cfg(feature = "recorder-postcard-rpc")]
    TcpPostcardRpc,
    #[cfg(feature = "recorder-postcard-rpc")]
    TcpTlsPostcardRpc(RecorderPostcardRpcTlsServerConfig),
}

pub struct HaServeConfig {
    recorder_listener: tokio::net::TcpListener,
    service_listener: ServiceListener,
    recorder_transport: HaRecorderTransport,
    recorders: Vec<(String, Box<dyn RecorderRpc>)>,
    log_peers: Vec<Arc<dyn LogPeer>>,
    admin: Option<AdminConfig>,
    tail_token: Option<String>,
    #[cfg(test)]
    recorder_start_error: Option<&'static str>,
    #[cfg(test)]
    recorder_shutdown_outcome: Option<TestRecorderShutdownOutcome>,
    #[cfg(test)]
    open_shutdown_token_observer: Option<Arc<TestCleanupTokenObserver>>,
}

impl HaServeConfig {
    pub fn new(
        recorder_listener: tokio::net::TcpListener,
        service_listener: tokio::net::TcpListener,
        recorder_transport: HaRecorderTransport,
        recorders: Vec<(String, Box<dyn RecorderRpc>)>,
        log_peers: Vec<Arc<dyn LogPeer>>,
    ) -> Self {
        Self {
            recorder_listener,
            service_listener: ServiceListener::Bound(service_listener),
            recorder_transport,
            recorders,
            log_peers,
            admin: None,
            tail_token: None,
            #[cfg(test)]
            recorder_start_error: None,
            #[cfg(test)]
            recorder_shutdown_outcome: None,
            #[cfg(test)]
            open_shutdown_token_observer: None,
        }
    }

    pub fn with_admin(mut self, admin: AdminConfig) -> Self {
        self.admin = Some(admin);
        self
    }

    pub fn with_tail_token(mut self, tail_token: impl Into<String>) -> Self {
        self.tail_token = Some(tail_token.into());
        self
    }
}

/// Internal-only service-listener state.  A live successor may complete all
/// recovery work while staging owns the public socket, then receive that same
/// socket exactly at service activation.  This is never exposed through the
/// public HA API.
enum ServiceListener {
    Bound(tokio::net::TcpListener),
    Deferred {
        ready: tokio::sync::oneshot::Sender<()>,
        listener: tokio::sync::oneshot::Receiver<tokio::net::TcpListener>,
    },
}

enum ServiceActivation {
    Listener(tokio::net::TcpListener),
    Shutdown {
        deadline: tokio::time::Instant,
        listener_closed: bool,
    },
}

async fn activate_service_listener(
    listener: ServiceListener,
    shutdown: &mut tokio::sync::watch::Receiver<ShutdownSignal>,
) -> Result<ServiceActivation, HaNodeError> {
    match listener {
        ServiceListener::Bound(listener) => match shutdown.borrow().clone() {
            Some(token) => {
                drop(listener);
                Ok(ServiceActivation::Shutdown {
                    deadline: token.deadline(),
                    listener_closed: true,
                })
            }
            None => Ok(ServiceActivation::Listener(listener)),
        },
        ServiceListener::Deferred {
            ready,
            mut listener,
        } => {
            // The activation gate may already have consumed the watch change
            // that delivered shutdown. Inspect retained state before sending
            // readiness; otherwise this child can wait forever for a second
            // change while its parent is already cleaning up a staging fault.
            if let Some(token) = shutdown.borrow().clone() {
                return Ok(ServiceActivation::Shutdown {
                    deadline: token.deadline(),
                    listener_closed: false,
                });
            }
            ready.send(()).map_err(|_| {
                HaNodeError::ServiceServer(
                    "live successor parent stopped before service listener handoff".into(),
                )
            })?;
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || shutdown.borrow().is_some() {
                        let deadline = shutdown_deadline(&shutdown.borrow());
                        Ok(ServiceActivation::Shutdown {
                            deadline,
                            listener_closed: false,
                        })
                    } else {
                        Err(HaNodeError::ServiceServer(
                            "live successor service activation shutdown changed without a token".into(),
                        ))
                    }
                }
                received = &mut listener => received
                    .map(ServiceActivation::Listener)
                    .map_err(|_| HaNodeError::ServiceServer(
                        "live successor parent dropped the service listener handoff".into(),
                    )),
            }
        }
    }
}

/// The live-successor supervisor temporarily owns the service listener while
/// staging is running.  Keeping it out of `HaServeConfig` makes the transfer
/// into the child explicit and one-way.
struct SuccessorServeConfig {
    recorder_listener: tokio::net::TcpListener,
    service_listener: Option<ListenerLease>,
    recorder_transport: HaRecorderTransport,
    recorders: Vec<(String, Box<dyn RecorderRpc>)>,
    log_peers: Vec<Arc<dyn LogPeer>>,
    admin: Option<AdminConfig>,
    tail_token: Option<String>,
    #[cfg(test)]
    staging_close_error: Option<&'static str>,
    #[cfg(test)]
    cleanup_token_observer: Option<Arc<TestCleanupTokenObserver>>,
    #[cfg(test)]
    recorder_start_error: Option<&'static str>,
    #[cfg(test)]
    recorder_shutdown_outcome: Option<TestRecorderShutdownOutcome>,
    #[cfg(test)]
    open_shutdown_token_observer: Option<Arc<TestCleanupTokenObserver>>,
    #[cfg(test)]
    staging_accept_faults: Option<Arc<TestStagingAcceptFaults>>,
}

impl SuccessorServeConfig {
    fn take_service_listener(&mut self) -> ListenerLease {
        self.service_listener
            .take()
            .expect("live successor service listener is owned until handoff")
    }

    fn spawn_staging_server(
        &mut self,
        ready: Arc<AtomicBool>,
        command: tokio::sync::watch::Receiver<StagingCommand>,
        started: tokio::sync::oneshot::Sender<()>,
    ) -> StagingServerTask {
        #[cfg(test)]
        {
            spawn_successor_staging_server_inner(
                self.take_service_listener(),
                ready,
                command,
                started,
                self.staging_close_error,
                self.staging_accept_faults.clone(),
            )
        }
        #[cfg(not(test))]
        {
            spawn_successor_staging_server(self.take_service_listener(), ready, command, started)
        }
    }

    fn into_deferred_child(
        self,
        ready: tokio::sync::oneshot::Sender<()>,
        listener: tokio::sync::oneshot::Receiver<tokio::net::TcpListener>,
    ) -> HaServeConfig {
        debug_assert!(self.service_listener.is_none());
        HaServeConfig {
            recorder_listener: self.recorder_listener,
            service_listener: ServiceListener::Deferred { ready, listener },
            recorder_transport: self.recorder_transport,
            recorders: self.recorders,
            log_peers: self.log_peers,
            admin: self.admin,
            tail_token: self.tail_token,
            #[cfg(test)]
            recorder_start_error: self.recorder_start_error,
            #[cfg(test)]
            recorder_shutdown_outcome: self.recorder_shutdown_outcome,
            #[cfg(test)]
            open_shutdown_token_observer: self.open_shutdown_token_observer,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HaNodeStatus {
    Starting,
    Restoring,
    CatchingUp,
    PreStopReady,
    Transitioning,
    AwaitingActivation,
    Ready,
    Degraded,
    ShuttingDown,
    Stopped,
    Failed,
}

/// Public, opaque shutdown authority. Its absolute deadline and raw task
/// receipts remain private so callers cannot forge or extend a shutdown.
#[derive(Clone)]
pub struct ShutdownToken {
    inner: Arc<ShutdownTokenInner>,
}

impl fmt::Debug for ShutdownToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ShutdownToken(..)")
    }
}

type ShutdownSignal = Option<Arc<ShutdownToken>>;

#[derive(Debug)]
struct ShutdownTokenInner {
    deadline: tokio::time::Instant,
    startup_token: StartupCancellationToken,
    authority: StartupCancellationAuthority,
}

impl ShutdownToken {
    fn new(timeout: Duration) -> Self {
        Self::with_authority(timeout, StartupCancellationAuthority::External)
    }

    fn new_internal(timeout: Duration) -> Self {
        Self::with_authority(timeout, StartupCancellationAuthority::Internal)
    }

    fn new_internal_at(deadline: tokio::time::Instant) -> Self {
        Self {
            inner: Arc::new(ShutdownTokenInner {
                deadline,
                startup_token: StartupIoContext::issue_cancellation_token(),
                authority: StartupCancellationAuthority::Internal,
            }),
        }
    }

    fn with_authority(timeout: Duration, authority: StartupCancellationAuthority) -> Self {
        Self {
            inner: Arc::new(ShutdownTokenInner {
                deadline: tokio::time::Instant::now() + timeout,
                startup_token: StartupIoContext::issue_cancellation_token(),
                authority,
            }),
        }
    }

    fn deadline(&self) -> tokio::time::Instant {
        self.inner.deadline
    }

    fn startup_token(&self) -> &StartupCancellationToken {
        &self.inner.startup_token
    }

    fn startup_authority(&self) -> StartupCancellationAuthority {
        self.inner.authority
    }

    fn replaces(&self, current: &Self) -> bool {
        self.startup_authority().replaces(
            current.startup_authority(),
            self.deadline().into_std(),
            current.deadline().into_std(),
        )
    }
}

fn shutdown_deadline(signal: &ShutdownSignal) -> tokio::time::Instant {
    signal
        .as_ref()
        .map(|token| token.deadline())
        .unwrap_or_else(tokio::time::Instant::now)
}

fn cancel_startup_for_token(startup: &StartupIoContext, token: &ShutdownToken) {
    startup.cancel_for_shutdown(
        token.startup_token().clone(),
        token.deadline().into_std(),
        token.startup_authority(),
    );
}

// Identity is deliberately test-only: production callers receive an opaque
// authority and cannot observe or forge its deadline.
#[cfg(test)]
fn shutdown_token_identity(token: &Arc<ShutdownToken>) -> usize {
    Arc::as_ptr(token) as usize
}

#[cfg(test)]
#[derive(Debug)]
struct TestCleanupTokenObserver {
    observed: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<(usize, tokio::time::Instant)>>>,
}

#[cfg(test)]
impl TestCleanupTokenObserver {
    fn new() -> (
        Arc<Self>,
        tokio::sync::oneshot::Receiver<(usize, tokio::time::Instant)>,
    ) {
        let (observed, observed_rx) = tokio::sync::oneshot::channel();
        (
            Arc::new(Self {
                observed: std::sync::Mutex::new(Some(observed)),
            }),
            observed_rx,
        )
    }

    fn observe(&self, token: &Arc<ShutdownToken>) {
        if let Ok(mut observed) = self.observed.lock() {
            if let Some(observed) = observed.take() {
                let _ = observed.send((shutdown_token_identity(token), token.deadline()));
            }
        }
    }
}

#[derive(Default)]
struct LiveSuccessorCleanupContext {
    #[cfg(test)]
    token_observer: Option<Arc<TestCleanupTokenObserver>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HaShutdownPhase {
    Ingress,
    Startup,
    Rehydrate,
    Activation,
    Service,
    Supervisor,
    InFlightOperations,
    BackgroundWorkers,
    AppliedTipFlush,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IngressDisposition {
    Uncertain,
    Closed,
    Open,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TaskDisposition {
    Uncertain,
    Quiesced,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MutationCertainty {
    Quiesced,
    Uncertain {
        local_io: bool,
        recorder_rpc: bool,
        activation: bool,
    },
}

#[derive(Clone, Debug)]
pub enum HaShutdownCause {
    RecorderOutcomeUnknown,
    Source(Arc<crate::Error>),
    TaskFailure(Arc<crate::WorkerError>),
}

#[derive(Clone, Debug)]
pub enum HaNodeError {
    Startup(HaStartupError),
    CertifiedTail(HaCertifiedTailError),
    RecorderServer(String),
    ServiceServer(String),
    Supervisor(String),
    WorkerFailure(ErrorClassification),
    StartupIoDeadlineExceeded {
        stage: String,
    },
    ShutdownDeadlineExceeded {
        phase: HaShutdownPhase,
        ingress: IngressDisposition,
        tasks: TaskDisposition,
        mutation: MutationCertainty,
    },
    ShutdownIncomplete {
        phase: HaShutdownPhase,
        cause: HaShutdownCause,
        ingress: IngressDisposition,
        tasks: TaskDisposition,
        mutation: MutationCertainty,
    },
    Cleanup {
        primary: Box<HaNodeError>,
        cleanup: Box<HaNodeError>,
    },
    Cancelled,
}

impl fmt::Display for HaNodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Startup(error) => write!(formatter, "HA startup failed: {error}"),
            Self::CertifiedTail(error) => write!(formatter, "certified tail failed: {error}"),
            Self::RecorderServer(error) => write!(formatter, "recorder server failed: {error}"),
            Self::ServiceServer(error) => write!(formatter, "service server failed: {error}"),
            Self::Supervisor(error) => write!(formatter, "HA supervisor failed: {error}"),
            Self::WorkerFailure(classification) => write!(
                formatter,
                "HA worker failed with code {}",
                classification.code()
            ),
            Self::StartupIoDeadlineExceeded { stage } => {
                write!(
                    formatter,
                    "HA startup I/O exceeded the shutdown deadline during {stage}"
                )
            }
            Self::ShutdownDeadlineExceeded { phase, .. } => {
                write!(formatter, "HA shutdown deadline exceeded during {phase:?}")
            }
            Self::ShutdownIncomplete { phase, cause, .. } => {
                write!(
                    formatter,
                    "HA shutdown incomplete during {phase:?}: {cause}"
                )
            }
            Self::Cleanup { primary, cleanup } => {
                write!(formatter, "{primary}; cleanup also failed: {cleanup}")
            }
            Self::Cancelled => formatter.write_str("HA node startup was cancelled"),
        }
    }
}

impl fmt::Display for HaShutdownCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecorderOutcomeUnknown => formatter.write_str("recorder outcome is unknown"),
            Self::Source(error) => write!(formatter, "{error}"),
            Self::TaskFailure(error) => write!(formatter, "task failure: {:?}", error.failure()),
        }
    }
}

impl std::error::Error for HaShutdownCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(error) => Some(error.as_ref()),
            Self::TaskFailure(error) => Some(error.as_ref()),
            Self::RecorderOutcomeUnknown => None,
        }
    }
}

impl std::error::Error for HaNodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Startup(error) => Some(error),
            Self::CertifiedTail(error) => Some(error),
            Self::ShutdownIncomplete { cause, .. } => Some(cause),
            Self::Cleanup { primary, .. } => Some(primary),
            Self::RecorderServer(_)
            | Self::ServiceServer(_)
            | Self::Supervisor(_)
            | Self::WorkerFailure(_)
            | Self::StartupIoDeadlineExceeded { .. }
            | Self::ShutdownDeadlineExceeded { .. }
            | Self::Cancelled => None,
        }
    }
}

const CONSERVATIVE_SHUTDOWN_MUTATION: MutationCertainty = MutationCertainty::Uncertain {
    local_io: true,
    recorder_rpc: true,
    activation: true,
};

fn conservative_shutdown_deadline_error(
    phase: HaShutdownPhase,
    evidence: ShutdownEvidence,
) -> HaNodeError {
    HaNodeError::ShutdownDeadlineExceeded {
        phase,
        ingress: evidence.ingress,
        tasks: evidence.tasks,
        mutation: CONSERVATIVE_SHUTDOWN_MUTATION,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ShutdownEvidence {
    ingress: IngressDisposition,
    tasks: TaskDisposition,
}

const UNCERTAIN_SHUTDOWN_EVIDENCE: ShutdownEvidence = ShutdownEvidence {
    ingress: IngressDisposition::Uncertain,
    tasks: TaskDisposition::Uncertain,
};

const PRE_SERVICE_SHUTDOWN_EVIDENCE: ShutdownEvidence = ShutdownEvidence {
    ingress: IngressDisposition::Closed,
    tasks: TaskDisposition::Quiesced,
};

fn ingress_after_service_wait(listener_ended: bool) -> IngressDisposition {
    if listener_ended {
        IngressDisposition::Closed
    } else {
        IngressDisposition::Uncertain
    }
}

/// A shutdown claim is only as strong as every listener owner covered by the
/// cleanup path. HTTP and non-HTTP recorder owners contribute only their
/// actual receipts; a missing owner receipt cannot be manufactured here.
fn merge_shutdown_evidence(
    primary: ShutdownEvidence,
    additional: Option<ShutdownEvidence>,
) -> ShutdownEvidence {
    let Some(additional) = additional else {
        return primary;
    };
    ShutdownEvidence {
        ingress: if primary.ingress == IngressDisposition::Closed
            && additional.ingress == IngressDisposition::Closed
        {
            IngressDisposition::Closed
        } else {
            IngressDisposition::Uncertain
        },
        tasks: if primary.tasks == TaskDisposition::Quiesced
            && additional.tasks == TaskDisposition::Quiesced
        {
            TaskDisposition::Quiesced
        } else {
            TaskDisposition::Uncertain
        },
    }
}

fn merge_optional_shutdown_evidence(
    primary: Option<ShutdownEvidence>,
    additional: Option<ShutdownEvidence>,
) -> Option<ShutdownEvidence> {
    match (primary, additional) {
        (Some(primary), additional) => Some(merge_shutdown_evidence(primary, additional)),
        (None, additional) => additional,
    }
}

/// A lower-level Rhiza owner failure may leave in-flight/background work
/// unresolved even when HTTP/admin and recorder drain receipts succeeded.
/// Keep this downgrade named so future cleanup paths cannot accidentally turn
/// a local ingress receipt into a global quiescence claim.
fn downgrade_tasks_for_owner_cleanup(mut evidence: ShutdownEvidence) -> ShutdownEvidence {
    evidence.tasks = TaskDisposition::Uncertain;
    evidence
}

fn shutdown_owner_error(error: crate::Error, evidence: ShutdownEvidence) -> HaNodeError {
    match error {
        crate::Error::Consensus(rhiza_quepaxa::Error::UnknownOutcome) => {
            HaNodeError::ShutdownIncomplete {
                phase: HaShutdownPhase::Service,
                cause: HaShutdownCause::RecorderOutcomeUnknown,
                ingress: evidence.ingress,
                tasks: evidence.tasks,
                mutation: MutationCertainty::Uncertain {
                    local_io: false,
                    recorder_rpc: true,
                    activation: false,
                },
            }
        }
        crate::Error::Shutdown(error) => shutdown_owner_shutdown_error(error, evidence),
        error => HaNodeError::ShutdownIncomplete {
            phase: HaShutdownPhase::Service,
            cause: HaShutdownCause::Source(Arc::new(error)),
            ingress: evidence.ingress,
            tasks: evidence.tasks,
            mutation: MutationCertainty::Uncertain {
                local_io: true,
                recorder_rpc: true,
                activation: false,
            },
        },
    }
}

fn shutdown_owner_shutdown_error(
    error: crate::ShutdownError,
    evidence: ShutdownEvidence,
) -> HaNodeError {
    let (lower_phase, cause, cleanup, task_source) = error.into_parts();
    let phase = match lower_phase {
        crate::ShutdownPhase::InFlightOperations => HaShutdownPhase::InFlightOperations,
        crate::ShutdownPhase::BackgroundWorkers => HaShutdownPhase::BackgroundWorkers,
        crate::ShutdownPhase::AppliedTipFlush => HaShutdownPhase::AppliedTipFlush,
    };
    let primary = match cause {
        crate::ShutdownCause::DeadlineExceeded => lower_shutdown_deadline_error(phase, evidence),
        crate::ShutdownCause::RecorderOutcomeUnknown => HaNodeError::ShutdownIncomplete {
            phase,
            cause: HaShutdownCause::RecorderOutcomeUnknown,
            ingress: evidence.ingress,
            tasks: evidence.tasks,
            mutation: MutationCertainty::Uncertain {
                local_io: phase == HaShutdownPhase::InFlightOperations
                    || phase == HaShutdownPhase::BackgroundWorkers
                    || phase == HaShutdownPhase::AppliedTipFlush,
                recorder_rpc: true,
                activation: false,
            },
        },
        crate::ShutdownCause::Source(error) => HaNodeError::ShutdownIncomplete {
            phase,
            cause: HaShutdownCause::Source(Arc::new(*error)),
            ingress: evidence.ingress,
            tasks: evidence.tasks,
            mutation: lower_shutdown_mutation(phase),
        },
        crate::ShutdownCause::TaskFailure(_) => HaNodeError::ShutdownIncomplete {
            phase,
            cause: HaShutdownCause::TaskFailure(
                task_source.expect("task-failure shutdown errors retain their worker receipt"),
            ),
            ingress: evidence.ingress,
            tasks: evidence.tasks,
            mutation: lower_shutdown_mutation(phase),
        },
    };
    cleanup
        .into_iter()
        .fold(primary, |primary, cleanup| HaNodeError::Cleanup {
            primary: Box::new(primary),
            cleanup: Box::new(shutdown_owner_shutdown_error(cleanup, evidence)),
        })
}

fn lower_shutdown_deadline_error(
    phase: HaShutdownPhase,
    evidence: ShutdownEvidence,
) -> HaNodeError {
    HaNodeError::ShutdownDeadlineExceeded {
        phase,
        ingress: evidence.ingress,
        tasks: evidence.tasks,
        mutation: lower_shutdown_mutation(phase),
    }
}

fn lower_shutdown_mutation(phase: HaShutdownPhase) -> MutationCertainty {
    MutationCertainty::Uncertain {
        local_io: matches!(
            phase,
            HaShutdownPhase::InFlightOperations
                | HaShutdownPhase::BackgroundWorkers
                | HaShutdownPhase::AppliedTipFlush
        ),
        recorder_rpc: phase == HaShutdownPhase::InFlightOperations,
        activation: false,
    }
}

impl From<HaStartupError> for HaNodeError {
    fn from(error: HaStartupError) -> Self {
        Self::Startup(error)
    }
}

#[derive(Clone)]
struct HaNodeSnapshot {
    status: HaNodeStatus,
    handle: Option<RhizaHandle>,
    terminal_error: Option<HaNodeError>,
}

/// Running high-availability node owner.
///
/// Obtain from [`HaStartupConfig::start`]. Call [`Self::ready`] to wait for
/// readiness and get a [`RhizaHandle`], then [`Self::monitor`] to watch for
/// background failures. During planned shutdown, call [`Self::shutdown`].
///
/// Dropping the owner starts deadline-bounded cleanup but cannot report errors.
pub struct HaNode {
    shutdown: tokio::sync::watch::Sender<ShutdownSignal>,
    recorder_shutdown: tokio::sync::watch::Sender<bool>,
    startup: StartupIoContext,
    state: tokio::sync::watch::Receiver<HaNodeSnapshot>,
    supervisor: Option<AbortOnDropTask<Result<(), HaNodeError>>>,
}

impl HaNode {
    pub async fn ready(&self) -> Result<RhizaHandle, HaNodeError> {
        let mut state = self.state.clone();
        loop {
            let (shutdown, snapshot) = {
                let shutdown_guard = self.shutdown.borrow();
                let shutdown = shutdown_guard.clone();
                let snapshot = state.borrow().clone();
                (shutdown, snapshot)
            };
            if let Some(error) = snapshot.terminal_error {
                return Err(error);
            }
            if shutdown.is_some() {
                return Err(HaNodeError::Cancelled);
            }
            if snapshot.status == HaNodeStatus::Ready {
                return snapshot.handle.ok_or_else(|| {
                    HaNodeError::Startup(fail("ready HA node has no application handle"))
                });
            }
            if matches!(
                snapshot.status,
                HaNodeStatus::ShuttingDown | HaNodeStatus::Stopped
            ) {
                return Err(HaNodeError::Cancelled);
            }
            state.changed().await.map_err(|_| HaNodeError::Cancelled)?;
        }
    }

    pub fn status(&self) -> HaNodeStatus {
        self.state.borrow().status
    }

    pub fn is_ready(&self) -> bool {
        let shutdown_guard = self.shutdown.borrow();
        shutdown_guard.is_none() && self.status() == HaNodeStatus::Ready
    }

    pub async fn monitor(&self) -> Result<(), HaNodeError> {
        let mut state = self.state.clone();
        loop {
            let snapshot = state.borrow().clone();
            if let Some(error) = snapshot.terminal_error {
                return Err(error);
            }
            if snapshot.status == HaNodeStatus::Stopped {
                return Ok(());
            }
            state.changed().await.map_err(|_| {
                HaNodeError::Supervisor("HA node monitor state channel closed".into())
            })?;
        }
    }

    pub async fn shutdown(self) -> Result<(), HaNodeError> {
        self.shutdown_with_timeout(HA_SERVER_SHUTDOWN_TIMEOUT).await
    }

    pub async fn shutdown_with_timeout(self, timeout: Duration) -> Result<(), HaNodeError> {
        self.shutdown_with_token(Arc::new(ShutdownToken::new(timeout)))
            .await
    }

    async fn shutdown_with_token(mut self, token: Arc<ShutdownToken>) -> Result<(), HaNodeError> {
        let token = request_ha_shutdown(&self.shutdown, token, || {
            if let Some(handle) = self.state.borrow().handle.as_ref() {
                handle.close_admission();
            }
        });
        self.recorder_shutdown.send_replace(true);
        cancel_startup_for_token(&self.startup, &token);
        let mut supervisor = self.supervisor.take().ok_or_else(|| {
            HaNodeError::Supervisor("HA node supervisor ownership missing during shutdown".into())
        })?;
        wait_for_ha_supervisor_before(&mut supervisor, token.deadline(), "HA node supervisor").await
    }
}

impl Drop for HaNode {
    fn drop(&mut self) {
        let token = request_ha_shutdown(
            &self.shutdown,
            Arc::new(ShutdownToken::new_internal(HA_SERVER_SHUTDOWN_TIMEOUT)),
            || {
                if let Some(handle) = self.state.borrow().handle.as_ref() {
                    handle.close_admission();
                }
            },
        );
        self.recorder_shutdown.send_replace(true);
        cancel_startup_for_token(&self.startup, &token);
        if let Some(supervisor) = self.supervisor.take() {
            // The public owner is gone, but its supervisor still gets the
            // exact shutdown authority already installed above. The reaper
            // stops waiting at D and intentionally makes no late-completion
            // or quiescence claim for blocking work.
            supervisor.reap_before(token.deadline());
        }
    }
}

/// One public owner for a live successor from prestage through active service.
pub struct HaSuccessorNode {
    shutdown: tokio::sync::watch::Sender<ShutdownSignal>,
    predecessor: tokio::sync::mpsc::UnboundedSender<HaPredecessor>,
    predecessor_binding: Mutex<Option<HaPredecessor>>,
    state: tokio::sync::watch::Receiver<HaNodeSnapshot>,
    supervisor: Option<AbortOnDropTask<Result<(), HaNodeError>>>,
}

impl HaSuccessorNode {
    fn start(
        prestage: HaSuccessorPrestageConfig,
        startup: HaStartupConfig,
        serve: HaServeConfig,
        tail_source: Arc<dyn HaCertifiedTailSource>,
    ) -> Result<Self, HaStartupError> {
        validate_live_successor_draft(&prestage, &startup)?;
        let HaServeConfig {
            recorder_listener,
            service_listener,
            recorder_transport,
            recorders,
            log_peers,
            admin,
            tail_token,
            #[cfg(test)]
            recorder_start_error,
            #[cfg(test)]
            recorder_shutdown_outcome,
            #[cfg(test)]
            open_shutdown_token_observer,
        } = serve;
        let service_listener = match service_listener {
            ServiceListener::Bound(listener) => listener,
            ServiceListener::Deferred { .. } => {
                return Err(fail(
                    "live successor cannot start from an internally deferred service listener",
                ));
            }
        };
        // A live successor has one public service port throughout staging and
        // activation.  Keep the actual bound FD in one linear lease: staging
        // consumes it first and the child receives the *same* FD only after a
        // successful handoff.  In particular, do not clone the descriptor --
        // a clone can continue accepting after staging has claimed closure.
        let serve = SuccessorServeConfig {
            recorder_listener,
            service_listener: Some(ListenerLease::new(service_listener)),
            recorder_transport,
            recorders,
            log_peers,
            admin,
            tail_token,
            #[cfg(test)]
            staging_close_error: None,
            #[cfg(test)]
            cleanup_token_observer: None,
            #[cfg(test)]
            recorder_start_error,
            #[cfg(test)]
            recorder_shutdown_outcome,
            #[cfg(test)]
            open_shutdown_token_observer,
            #[cfg(test)]
            staging_accept_faults: None,
        };
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel::<ShutdownSignal>(None);
        let (predecessor, predecessor_rx) = tokio::sync::mpsc::unbounded_channel();
        let (state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Restoring,
            handle: None,
            terminal_error: None,
        });
        let supervisor_state = state_tx.clone();
        let supervisor = AbortOnDropTask::spawn(async move {
            let result = supervise_live_successor(
                prestage,
                startup,
                serve,
                tail_source,
                shutdown_rx,
                predecessor_rx,
                state_tx,
            )
            .await;
            if let Err(error) = &result {
                publish_ha_failure(&supervisor_state, error.clone());
            }
            result
        });
        Ok(Self {
            shutdown,
            predecessor,
            predecessor_binding: Mutex::new(None),
            state,
            supervisor: Some(supervisor),
        })
    }

    /// Supplies the one immutable predecessor Stop proof observed by the embedding application.
    pub fn bind_predecessor(&self, predecessor: HaPredecessor) -> Result<(), HaNodeError> {
        let mut binding = self
            .predecessor_binding
            .lock()
            .map_err(|_| HaNodeError::Startup(fail("predecessor binding mutex is poisoned")))?;
        match binding.as_ref() {
            Some(bound) if bound == &predecessor => return Ok(()),
            Some(_) => {
                return Err(HaNodeError::Startup(fail(
                    "live successor predecessor binding changed after first observation",
                )));
            }
            None => {}
        }
        self.predecessor.send(predecessor.clone()).map_err(|_| {
            self.state
                .borrow()
                .terminal_error
                .clone()
                .unwrap_or(HaNodeError::Cancelled)
        })?;
        *binding = Some(predecessor);
        Ok(())
    }

    pub async fn ready(&self) -> Result<RhizaHandle, HaNodeError> {
        wait_for_ha_ready(&self.shutdown, self.state.clone()).await
    }

    pub fn status(&self) -> HaNodeStatus {
        self.state.borrow().status
    }

    pub fn is_prestop_ready(&self) -> bool {
        self.status() == HaNodeStatus::PreStopReady
    }

    pub fn is_ready(&self) -> bool {
        self.shutdown.borrow().is_none() && self.status() == HaNodeStatus::Ready
    }

    pub async fn monitor(&self) -> Result<(), HaNodeError> {
        monitor_ha_state(self.state.clone()).await
    }

    pub async fn shutdown(self) -> Result<(), HaNodeError> {
        self.shutdown_with_timeout(HA_SERVER_SHUTDOWN_TIMEOUT).await
    }

    pub async fn shutdown_with_timeout(self, timeout: Duration) -> Result<(), HaNodeError> {
        self.shutdown_with_token(Arc::new(ShutdownToken::new(timeout)))
            .await
    }

    async fn shutdown_with_token(mut self, token: Arc<ShutdownToken>) -> Result<(), HaNodeError> {
        let token = request_ha_shutdown(&self.shutdown, token, || {});
        let mut supervisor = self.supervisor.take().ok_or_else(|| {
            HaNodeError::Supervisor(
                "live successor supervisor ownership missing during shutdown".into(),
            )
        })?;
        wait_for_ha_supervisor_before(
            &mut supervisor,
            token.deadline(),
            "live successor supervisor",
        )
        .await
    }
}

impl Drop for HaSuccessorNode {
    fn drop(&mut self) {
        let token = request_ha_shutdown(
            &self.shutdown,
            Arc::new(ShutdownToken::new_internal(HA_SERVER_SHUTDOWN_TIMEOUT)),
            || {},
        );
        if let Some(supervisor) = self.supervisor.take() {
            // The cascade observes this same token and absolute deadline;
            // a successor drop must not mint a child-specific grace period.
            supervisor.reap_before(token.deadline());
        }
    }
}

async fn wait_for_ha_ready(
    shutdown: &tokio::sync::watch::Sender<ShutdownSignal>,
    mut state: tokio::sync::watch::Receiver<HaNodeSnapshot>,
) -> Result<RhizaHandle, HaNodeError> {
    loop {
        let (shutdown, snapshot) = {
            let shutdown = shutdown.borrow().clone();
            let snapshot = state.borrow().clone();
            (shutdown, snapshot)
        };
        if let Some(error) = snapshot.terminal_error {
            return Err(error);
        }
        if shutdown.is_some() {
            return Err(HaNodeError::Cancelled);
        }
        if snapshot.status == HaNodeStatus::Ready {
            return snapshot.handle.ok_or_else(|| {
                HaNodeError::Startup(fail("ready HA node has no application handle"))
            });
        }
        if matches!(
            snapshot.status,
            HaNodeStatus::ShuttingDown | HaNodeStatus::Stopped
        ) {
            return Err(HaNodeError::Cancelled);
        }
        state.changed().await.map_err(|_| HaNodeError::Cancelled)?;
    }
}

async fn monitor_ha_state(
    mut state: tokio::sync::watch::Receiver<HaNodeSnapshot>,
) -> Result<(), HaNodeError> {
    loop {
        let snapshot = state.borrow().clone();
        if let Some(error) = snapshot.terminal_error {
            return Err(error);
        }
        if snapshot.status == HaNodeStatus::Stopped {
            return Ok(());
        }
        state
            .changed()
            .await
            .map_err(|_| HaNodeError::Supervisor("HA node monitor state channel closed".into()))?;
    }
}

fn request_ha_shutdown<F>(
    shutdown: &tokio::sync::watch::Sender<ShutdownSignal>,
    requested: Arc<ShutdownToken>,
    close_admission: F,
) -> Arc<ShutdownToken>
where
    F: FnOnce(),
{
    shutdown.send_if_modified(|current| {
        if current.is_none()
            || current
                .as_ref()
                .is_some_and(|existing| requested.replaces(existing))
        {
            *current = Some(Arc::clone(&requested));
            close_admission();
            true
        } else {
            false
        }
    });
    shutdown.borrow().clone().unwrap_or(requested)
}

async fn wait_for_ha_supervisor_before(
    supervisor: &mut AbortOnDropTask<Result<(), HaNodeError>>,
    deadline: tokio::time::Instant,
    name: &str,
) -> Result<(), HaNodeError> {
    let result = tokio::select! {
        biased;
        result = &mut *supervisor => Some(result),
        () = tokio::time::sleep_until(deadline) => None,
    };
    match result {
        Some(Ok(result)) if tokio::time::Instant::now() < deadline => result,
        Some(Err(error)) if tokio::time::Instant::now() < deadline => Err(HaNodeError::Supervisor(
            format!("{name} task failed: {error}"),
        )),
        None if supervisor.is_finished() => Err(conservative_shutdown_deadline_error(
            HaShutdownPhase::Supervisor,
            UNCERTAIN_SHUTDOWN_EVIDENCE,
        )),
        None => {
            supervisor.abort();
            Err(conservative_shutdown_deadline_error(
                HaShutdownPhase::Supervisor,
                UNCERTAIN_SHUTDOWN_EVIDENCE,
            ))
        }
        Some(_) => Err(conservative_shutdown_deadline_error(
            HaShutdownPhase::Supervisor,
            UNCERTAIN_SHUTDOWN_EVIDENCE,
        )),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HaPredecessor {
    membership: Membership,
    stop: StopInformation,
}

impl HaPredecessor {
    pub fn new(membership: Membership, stop: StopInformation) -> Self {
        Self { membership, stop }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HaCertifiedTailError {
    Unavailable(String),
    RebaseRequired(LogAnchor),
    Rejected(String),
}

impl fmt::Display for HaCertifiedTailError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(message) => write!(formatter, "source unavailable: {message}"),
            Self::RebaseRequired(anchor) => write!(
                formatter,
                "source requires a newer checkpoint at index {} with hash {}",
                anchor.index(),
                anchor.hash().to_hex()
            ),
            Self::Rejected(message) => write!(formatter, "source rejected the request: {message}"),
        }
    }
}

impl std::error::Error for HaCertifiedTailError {}

/// Transport boundary for a certified predecessor tail.
///
/// The facade owns request cadence, validation, application, Stop matching, and retries.
/// Embedders only provide a transport for one immutable predecessor identity.
pub trait HaCertifiedTailSource: Send + Sync {
    fn fetch<'a>(
        &'a self,
        request: &'a CertifiedTailRequest,
    ) -> Pin<
        Box<dyn Future<Output = Result<CertifiedTailResponse, HaCertifiedTailError>> + Send + 'a>,
    >;
}

pub struct HaSuccessorPrestageConfig {
    archive: ObjectArchiveStore,
    prestage_dir: PathBuf,
    target_node_id: String,
    execution_profile: ExecutionProfile,
    predecessor_membership: Membership,
    target_membership: Membership,
    tail_token: String,
    effect_consensus: Option<Arc<ThreeNodeConsensus>>,
}

impl HaSuccessorPrestageConfig {
    pub fn new(
        archive: ObjectArchiveStore,
        prestage_dir: impl Into<PathBuf>,
        target_node_id: impl Into<String>,
        execution_profile: ExecutionProfile,
        predecessor_membership: Membership,
        target_membership: Membership,
        tail_token: impl Into<String>,
    ) -> Self {
        Self {
            archive,
            prestage_dir: prestage_dir.into(),
            target_node_id: target_node_id.into(),
            execution_profile,
            predecessor_membership,
            target_membership,
            tail_token: tail_token.into(),
            effect_consensus: None,
        }
    }

    /// Supplies the predecessor Recorder quorum used by a SQL successor to
    /// fetch and verify QEFX bytes before it persists or applies a tail entry.
    pub fn with_effect_consensus(mut self, consensus: Arc<ThreeNodeConsensus>) -> Self {
        self.effect_consensus = Some(consensus);
        self
    }

    /// Restores the current Active checkpoint into a detached successor stage.
    ///
    /// This does not open a [`NodeRuntime`], recorder, proposer, or client service. The target is
    /// bound to the next configuration and is not added to the predecessor membership.
    pub async fn prepare(self) -> Result<PreparedHaSuccessorPrestage, HaStartupError> {
        let checkpoint_identity = self.archive.checkpoint_identity().map_err(error)?;
        let cluster_id = checkpoint_identity.cluster_id().to_owned();
        let epoch = checkpoint_identity.epoch();
        let predecessor_config_id = checkpoint_identity.config_id();
        let recovery_generation = checkpoint_identity.recovery_generation();
        let target_config_id = predecessor_config_id
            .checked_add(1)
            .ok_or_else(|| fail("successor prestage target config_id cannot advance"))?;
        let predecessor_digest = self.predecessor_membership.digest();
        let predecessor_configuration =
            rhiza_core::ConfigurationState::active(predecessor_config_id, predecessor_digest);
        let tail_config = TailReaderConfig::new(
            cluster_id,
            epoch,
            predecessor_config_id,
            self.predecessor_membership,
            recovery_generation,
            self.tail_token,
        )
        .map_err(error)?;
        let prestage = prestage_successor_checkpoint(
            self.archive,
            &self.prestage_dir,
            predecessor_configuration,
            &self.target_node_id,
            self.execution_profile,
            target_config_id,
            self.target_membership.digest(),
        )
        .await
        .map_err(error)?;
        require_resumable_successor_prestage(&prestage)?;
        let identity = HaSuccessorPrestageIdentity::from(prestage.identity());
        Ok(PreparedHaSuccessorPrestage {
            prestage,
            identity,
            tail_config,
            effect_consensus: self.effect_consensus,
        })
    }

    /// Resumes an exact ready or published prestage without rereading the source checkpoint.
    pub fn resume(
        prestage_dir: impl AsRef<Path>,
        predecessor_config_id: u64,
        predecessor_membership: Membership,
        tail_token: impl Into<String>,
    ) -> Result<PreparedHaSuccessorPrestage, HaStartupError> {
        let prestage = inspect_successor_prestage(
            prestage_dir,
            rhiza_core::ConfigurationState::active(
                predecessor_config_id,
                predecessor_membership.digest(),
            ),
        )
        .map_err(error)?;
        require_resumable_successor_prestage(&prestage)?;
        let identity = HaSuccessorPrestageIdentity::from(prestage.identity());
        let tail_config = TailReaderConfig::new(
            identity.cluster_id(),
            identity.epoch(),
            identity.predecessor_config_id(),
            predecessor_membership,
            identity.predecessor_recovery_generation(),
            tail_token,
        )
        .map_err(error)?;
        Ok(PreparedHaSuccessorPrestage {
            prestage,
            identity,
            tail_config,
            effect_consensus: None,
        })
    }

    /// Starts a live successor owner that keeps the same process and listeners through cutover.
    ///
    /// The target startup must describe the unactivated successor draft. The returned owner
    /// restores and tails before Stop, accepts one exact predecessor binding, then starts and
    /// activates the successor without returning listener ownership to the embedding application.
    pub fn start_live(
        self,
        startup: HaStartupConfig,
        serve: HaServeConfig,
        tail_source: Arc<dyn HaCertifiedTailSource>,
    ) -> Result<HaSuccessorNode, HaStartupError> {
        HaSuccessorNode::start(self, startup, serve, tail_source)
    }
}

fn require_resumable_successor_prestage(
    prestage: &SuccessorPrestage,
) -> Result<(), HaStartupError> {
    if matches!(
        prestage.state(),
        SuccessorPrestageState::Ready | SuccessorPrestageState::Published
    ) {
        Ok(())
    } else {
        Err(fail(
            "successor prestage resume requires a ready or published stage",
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HaSuccessorPrestageIdentity {
    cluster_id: String,
    epoch: u64,
    predecessor_config_id: u64,
    predecessor_membership_digest: LogHash,
    predecessor_recovery_generation: u64,
    node_id: String,
    execution_profile: ExecutionProfile,
    target_config_id: u64,
    target_membership_digest: LogHash,
    seed_anchor: LogAnchor,
}

impl HaSuccessorPrestageIdentity {
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn predecessor_config_id(&self) -> u64 {
        self.predecessor_config_id
    }

    pub const fn predecessor_membership_digest(&self) -> LogHash {
        self.predecessor_membership_digest
    }

    pub const fn predecessor_recovery_generation(&self) -> u64 {
        self.predecessor_recovery_generation
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub const fn execution_profile(&self) -> ExecutionProfile {
        self.execution_profile
    }

    pub const fn target_config_id(&self) -> u64 {
        self.target_config_id
    }

    pub const fn target_membership_digest(&self) -> LogHash {
        self.target_membership_digest
    }

    pub const fn seed_anchor(&self) -> LogAnchor {
        self.seed_anchor
    }
}

impl From<&SuccessorPrestageIdentity> for HaSuccessorPrestageIdentity {
    fn from(identity: &SuccessorPrestageIdentity) -> Self {
        Self {
            cluster_id: identity.cluster_id().to_owned(),
            epoch: identity.epoch(),
            predecessor_config_id: identity.predecessor_config_id(),
            predecessor_membership_digest: identity.predecessor_membership_digest(),
            predecessor_recovery_generation: identity.predecessor_recovery_generation(),
            node_id: identity.node_id().to_owned(),
            execution_profile: identity.execution_profile(),
            target_config_id: identity.target_config_id(),
            target_membership_digest: identity.target_membership_digest(),
            seed_anchor: identity.seed_anchor(),
        }
    }
}

pub struct PreparedHaSuccessorPrestage {
    prestage: SuccessorPrestage,
    identity: HaSuccessorPrestageIdentity,
    tail_config: TailReaderConfig,
    effect_consensus: Option<Arc<ThreeNodeConsensus>>,
}

impl PreparedHaSuccessorPrestage {
    pub const fn identity(&self) -> &HaSuccessorPrestageIdentity {
        &self.identity
    }

    pub fn tail_request(&self, max_entries: u32) -> Result<CertifiedTailRequest, HaStartupError> {
        tail_request(self.identity.seed_anchor, max_entries)
    }

    /// Atomically moves the detached stage into its final data directory and opens only the
    /// recovery-only learner store.
    pub fn publish(
        self,
        data_dir: impl Into<PathBuf>,
    ) -> Result<PublishedHaSuccessorPrestage, HaStartupError> {
        let data_dir = data_dir.into();
        let published = publish_successor_prestage(self.prestage, &data_dir).map_err(error)?;
        drop(published);
        let learner = match self.effect_consensus {
            Some(consensus) => {
                LearnerStore::open_with_consensus(&data_dir, self.tail_config, consensus)
            }
            None => LearnerStore::open(&data_dir, self.tail_config),
        }
        .map_err(error)?;
        Ok(PublishedHaSuccessorPrestage {
            learner,
            identity: self.identity,
        })
    }
}

pub struct PublishedHaSuccessorPrestage {
    learner: LearnerStore,
    identity: HaSuccessorPrestageIdentity,
}

impl PublishedHaSuccessorPrestage {
    pub const fn identity(&self) -> &HaSuccessorPrestageIdentity {
        &self.identity
    }

    pub fn durable_anchor(&self) -> Result<LogAnchor, HaStartupError> {
        self.learner.durable_anchor().map_err(error)
    }

    pub fn applied_anchor(&self) -> Result<LogAnchor, HaStartupError> {
        self.learner.applied_anchor().map_err(error)
    }

    pub fn tail_request(&self, max_entries: u32) -> Result<CertifiedTailRequest, HaStartupError> {
        self.learner.tail_request(max_entries).map_err(error)
    }

    pub fn apply_page(
        &self,
        request: &CertifiedTailRequest,
        response: &CertifiedTailResponse,
    ) -> Result<LearnerProgress, HaStartupError> {
        self.learner.apply_page(request, response).map_err(error)
    }

    /// Requires the exact bound Stop and leaves a durable adoption intent for [`HaNode`].
    ///
    /// Recorder installation, archive initialization, and active runtime opening remain owned by
    /// the later [`HaStartupConfig::start`] lifecycle.
    pub fn finalize(self, startup: HaStartupConfig) -> Result<HaStartupConfig, HaStartupError> {
        if startup.mode != HaStartupMode::Rejoin {
            return Err(fail("successor prestage finalization requires rejoin mode"));
        }
        let predecessor = startup
            .predecessor
            .clone()
            .ok_or_else(|| fail("successor prestage finalization requires a predecessor"))?;
        let target_config_id = startup.target_config_id()?;
        validate_predecessor_binding(&startup.node_config, target_config_id, &predecessor)?;
        let restore = self
            .learner
            .finalize(&startup.node_config, &predecessor.stop)
            .map_err(error)?;
        drop(restore);
        Ok(startup)
    }
}

fn tail_request(from: LogAnchor, max_entries: u32) -> Result<CertifiedTailRequest, HaStartupError> {
    if max_entries == 0 || max_entries > MAX_CERTIFIED_TAIL_ENTRIES {
        return Err(fail(format!(
            "certified tail max_entries must be in 1..={MAX_CERTIFIED_TAIL_ENTRIES}"
        )));
    }
    Ok(CertifiedTailRequest { from, max_entries })
}

/// Configuration for starting a normal or successor HA node.
///
/// Build this configuration, then call [`Self::start`] with a [`HaServeConfig`]
/// to produce a running [`HaNode`]. For membership replacement, use
/// [`HaSuccessorPrestageConfig`] first and obtain a `HaStartupConfig` from
/// [`PublishedHaSuccessorPrestage::finalize`].
///
/// # Example
///
/// ```text
/// let startup = HaStartupConfig::new(
///     node_config,
///     archive,
///     HaStartupMode::Rejoin,
///     DurabilityMode::Sync,
///     5000,  // lease duration ms
/// );
/// let node = startup.start(serve_config);
/// let handle = node.ready().await?;
/// ```
pub struct HaStartupConfig {
    node_config: NodeConfig,
    archive: ObjectArchiveStore,
    durability: DurabilityMode,
    lease_duration_ms: u64,
    mode: HaStartupMode,
    predecessor: Option<HaPredecessor>,
    auto_activate_stop: Option<LogAnchor>,
    #[cfg(feature = "test-hooks")]
    service_activation_gate: Option<HaServiceActivationGate>,
    #[cfg(test)]
    open_phase_gate: Option<TestHaOpenPhaseGate>,
}

#[cfg(test)]
#[derive(Clone, Debug)]
struct TestHaOpenPhaseGate {
    entered: tokio::sync::watch::Sender<bool>,
    released: tokio::sync::watch::Sender<bool>,
}

#[cfg(test)]
impl TestHaOpenPhaseGate {
    fn new() -> Self {
        Self {
            entered: tokio::sync::watch::channel(false).0,
            released: tokio::sync::watch::channel(false).0,
        }
    }

    async fn entered(&self) {
        let mut entered = self.entered.subscribe();
        while !*entered.borrow() {
            entered
                .changed()
                .await
                .expect("HA open-phase gate entered sender closed");
        }
    }

    fn release_guard(&self) -> TestHaOpenPhaseRelease {
        TestHaOpenPhaseRelease(self.clone())
    }

    async fn wait(&self) {
        self.entered.send_replace(true);
        let mut released = self.released.subscribe();
        while !*released.borrow() {
            released
                .changed()
                .await
                .expect("HA open-phase gate release sender closed");
        }
    }
}

#[cfg(test)]
struct TestHaOpenPhaseRelease(TestHaOpenPhaseGate);

#[cfg(test)]
impl Drop for TestHaOpenPhaseRelease {
    fn drop(&mut self) {
        self.0.released.send_replace(true);
    }
}

/// Opt-in synchronization point immediately before service-listener activation.
/// Available only with the non-default `test-hooks` feature.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
#[derive(Clone)]
pub struct HaServiceActivationGate {
    entered: tokio::sync::watch::Sender<bool>,
    entered_rx: tokio::sync::watch::Receiver<bool>,
    released: tokio::sync::watch::Sender<bool>,
    released_rx: tokio::sync::watch::Receiver<bool>,
}

#[doc(hidden)]
#[cfg(feature = "test-hooks")]
pub struct HaServiceActivationRelease(HaServiceActivationGate);

#[cfg(feature = "test-hooks")]
impl HaServiceActivationGate {
    #[doc(hidden)]
    pub fn new() -> Self {
        let (entered, entered_rx) = tokio::sync::watch::channel(false);
        let (released, released_rx) = tokio::sync::watch::channel(false);
        Self {
            entered,
            entered_rx,
            released,
            released_rx,
        }
    }

    #[doc(hidden)]
    pub async fn entered(&self) {
        let mut entered = self.entered_rx.clone();
        while !*entered.borrow() {
            if entered.changed().await.is_err() {
                return;
            }
        }
    }

    #[doc(hidden)]
    pub fn release_guard(&self) -> HaServiceActivationRelease {
        HaServiceActivationRelease(self.clone())
    }

    async fn wait(&self) {
        self.entered.send_replace(true);
        let mut released = self.released_rx.clone();
        while !*released.borrow() {
            if released.changed().await.is_err() {
                return;
            }
        }
    }

    fn release(&self) {
        self.released.send_replace(true);
    }
}

#[cfg(feature = "test-hooks")]
impl Drop for HaServiceActivationRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl fmt::Debug for HaStartupConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HaStartupConfig")
            .field("node_id", &self.node_config.node_id())
            .field("mode", &self.mode)
            .field("predecessor", &self.predecessor)
            .field("auto_activate_stop", &self.auto_activate_stop)
            .finish_non_exhaustive()
    }
}

impl HaStartupConfig {
    pub fn new(
        node_config: NodeConfig,
        archive: ObjectArchiveStore,
        durability: DurabilityMode,
        lease_duration_ms: u64,
        mode: HaStartupMode,
    ) -> Self {
        Self {
            node_config,
            archive,
            durability,
            lease_duration_ms,
            mode,
            predecessor: None,
            auto_activate_stop: None,
            #[cfg(feature = "test-hooks")]
            service_activation_gate: None,
            #[cfg(test)]
            open_phase_gate: None,
        }
    }

    pub fn with_predecessor(mut self, predecessor: HaPredecessor) -> Self {
        self.predecessor = Some(predecessor);
        self
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn with_service_activation_gate(mut self, gate: HaServiceActivationGate) -> Self {
        self.service_activation_gate = Some(gate);
        self
    }

    fn bind_live_predecessor(mut self, predecessor: HaPredecessor) -> Result<Self, HaStartupError> {
        if self.predecessor.is_some() {
            return Err(fail(
                "live successor startup is already bound to a predecessor",
            ));
        }
        let stop_anchor = LogAnchor::new(predecessor.stop.entry.index, predecessor.stop.entry.hash);
        self.node_config = self
            .node_config
            .bind_predecessor_stop(&predecessor.membership, predecessor.stop.entry.clone())
            .map_err(error)?;
        let target_config_id = predecessor
            .stop
            .entry
            .config_id
            .checked_add(1)
            .ok_or_else(|| fail("successor config_id cannot advance"))?;
        validate_predecessor_binding(&self.node_config, target_config_id, &predecessor)?;
        self.predecessor = Some(predecessor);
        self.auto_activate_stop = Some(stop_anchor);
        Ok(self)
    }

    /// Starts the complete HA lifecycle under one public owner.
    ///
    /// Preparation, recorder ingress, runtime recovery, readiness, and shutdown all remain
    /// owned by the returned [`HaNode`].
    pub fn start(self, serve: HaServeConfig) -> HaNode {
        let startup = StartupIoContext::new();
        let supervisor_startup = startup.clone();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
        let (recorder_shutdown, recorder_shutdown_rx) = tokio::sync::watch::channel(false);
        let (state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Starting,
            handle: None,
            terminal_error: None,
        });
        let supervisor_shutdown = shutdown.clone();
        let supervisor_recorder_shutdown = recorder_shutdown.clone();
        let supervisor = AbortOnDropTask::spawn(async move {
            supervise_ha_node(
                self,
                serve,
                supervisor_startup,
                supervisor_shutdown,
                shutdown_rx,
                supervisor_recorder_shutdown,
                recorder_shutdown_rx,
                state_tx,
            )
            .await
        });
        HaNode {
            shutdown,
            recorder_shutdown,
            startup,
            state,
            supervisor: Some(supervisor),
        }
    }

    async fn prepare(
        self,
        startup: &StartupIoContext,
    ) -> Result<PreparedHaStartup, HaStartupError> {
        startup
            .check("HA startup configuration validation")
            .map_err(startup_error)?;
        if self.predecessor.is_none() && !self.node_config.configuration_state().is_active() {
            return Err(fail("stopped configuration requires a predecessor"));
        }
        startup
            .check("checkpoint identity inspection")
            .map_err(startup_error)?;
        let identity = self.archive.checkpoint_identity().map_err(error)?.clone();
        let target_config_id = self.target_config_id()?;
        validate_archive_identity(&self.node_config, &identity, target_config_id)?;
        let preparation = match &self.predecessor {
            Some(predecessor) => {
                prepare_successor(
                    &self.node_config,
                    &self.archive,
                    self.mode,
                    target_config_id,
                    predecessor,
                    startup,
                )
                .await?
            }
            None => {
                prepare_standard(
                    &self.node_config,
                    &self.archive,
                    self.mode,
                    self.node_config.membership(),
                    startup,
                )
                .await?
            }
        };
        startup
            .check("local recorder open")
            .map_err(startup_error)?;
        self.finish_prepare(identity, target_config_id, preparation, startup)
    }

    fn finish_prepare(
        self,
        identity: CheckpointIdentity,
        target_config_id: u64,
        preparation: StartupPreparation,
        startup: &StartupIoContext,
    ) -> Result<PreparedHaStartup, HaStartupError> {
        let recorder = open_recorder_for_preparation(
            &self.node_config,
            target_config_id,
            preparation.open_policy(),
            startup,
        )?;
        let recorder_hook = match preparation {
            StartupPreparation::RuntimeFirstWithPeerCatchup {
                checkpoint_root, ..
            } => HaRecorder::quarantined(recorder.clone(), checkpoint_root),
            StartupPreparation::RecorderFirst { .. }
            | StartupPreparation::VerifyLocalCheckpoint { .. } => {
                HaRecorder::active(recorder.clone())
            }
        };
        Ok(PreparedHaStartup {
            config: self,
            authoritative_identity: identity,
            target_config_id,
            preparation,
            recorder,
            recorder_hook,
        })
    }

    fn target_config_id(&self) -> Result<u64, HaStartupError> {
        self.predecessor
            .as_ref()
            .map(|predecessor| {
                predecessor
                    .stop
                    .entry
                    .config_id
                    .checked_add(1)
                    .ok_or_else(|| fail("successor config_id cannot advance"))
            })
            .unwrap_or_else(|| Ok(self.node_config.config_id()))
    }
}

struct PreparedHaStartup {
    config: HaStartupConfig,
    authoritative_identity: CheckpointIdentity,
    target_config_id: u64,
    preparation: StartupPreparation,
    recorder: RecorderFileStore,
    recorder_hook: HaRecorder,
}

impl fmt::Debug for PreparedHaStartup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedHaStartup")
            .field("node_id", &self.config.node_config.node_id())
            .field("mode", &self.config.mode)
            .finish_non_exhaustive()
    }
}

impl PreparedHaStartup {
    async fn open_cancellable(
        self,
        recorders: Vec<(String, Box<dyn RecorderRpc>)>,
        log_peers: Vec<Arc<dyn LogPeer>>,
        startup: StartupIoContext,
        shutdown: tokio::sync::watch::Receiver<ShutdownSignal>,
    ) -> Result<HaOpenNode, HaOpenError> {
        self.open_inner(recorders, log_peers, startup, shutdown)
            .await
    }

    async fn open_inner(
        self,
        recorders: Vec<(String, Box<dyn RecorderRpc>)>,
        log_peers: Vec<Arc<dyn LogPeer>>,
        startup: StartupIoContext,
        mut shutdown: tokio::sync::watch::Receiver<ShutdownSignal>,
    ) -> Result<HaOpenNode, HaOpenError> {
        #[cfg(test)]
        let open_phase_gate = self.config.open_phase_gate.clone();
        let direct_recorder = !matches!(
            self.preparation,
            StartupPreparation::RuntimeFirstWithPeerCatchup { .. }
        );
        let checkpoint_root = match self.preparation {
            StartupPreparation::RuntimeFirstWithPeerCatchup {
                checkpoint_root, ..
            } => Some(checkpoint_root),
            StartupPreparation::RecorderFirst { .. }
            | StartupPreparation::VerifyLocalCheckpoint { .. } => None,
        };
        let consensus = build_consensus(
            &self.config.node_config,
            self.target_config_id,
            recorders,
            direct_recorder.then_some(&self.recorder),
            checkpoint_root,
        )?;
        let runtime = open_runtime_with_retry(
            self.config.node_config.clone(),
            consensus,
            if checkpoint_root.is_some() {
                log_peers
            } else {
                Vec::new()
            },
            &startup,
            &mut shutdown,
        )
        .await?;
        let pending = PendingHaRuntime::new(runtime);

        match &self.preparation {
            StartupPreparation::VerifyLocalCheckpoint { identity, root } => {
                if let Err(error) =
                    verify_local_rejoin_checkpoint(pending.runtime(), identity, *root)
                {
                    return Err(pending.fail(error).await);
                }
            }
            StartupPreparation::RuntimeFirstWithPeerCatchup {
                checkpoint_root, ..
            } => {
                if let Err(error) = verify_local_rejoin_checkpoint(
                    pending.runtime(),
                    &self.authoritative_identity,
                    *checkpoint_root,
                ) {
                    return Err(pending.fail(error).await);
                }
                if let Err(error) = rehydrate_recorder_with_retry(
                    pending.runtime().clone(),
                    self.recorder.clone(),
                    checkpoint_root.index(),
                    &startup,
                    &mut shutdown,
                )
                .await
                {
                    return Err(pending.merge(error).await);
                }
                self.recorder_hook.activate();
            }
            StartupPreparation::RecorderFirst { .. } => {}
        }

        let auto_activate_stop = self.config.auto_activate_stop;
        let successor_stop = self.config.predecessor.as_ref().map(|predecessor| {
            LogAnchor::new(predecessor.stop.entry.index, predecessor.stop.entry.hash)
        });
        let target_config_id = self.target_config_id;
        let coordinator_open = CheckpointCoordinator::open_with_holder_options_local_state(
            self.config.archive,
            self.config.durability,
            self.config.node_config.node_id(),
            CheckpointPublisherOptions::new(self.config.lease_duration_ms),
            self.recorder.clone(),
            self.config.node_config.data_dir(),
        );
        tokio::pin!(coordinator_open);
        let coordinator = loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || shutdown.borrow().is_some() {
                        let deadline = shutdown_deadline(&shutdown.borrow());
                        return Err(pending.cancel(deadline, Ok(())).await);
                    }
                }
                result = &mut coordinator_open => {
                    match result {
                        Ok(coordinator) => {
                            let coordinator = Arc::new(coordinator);
                            if successor_stop.is_some() && coordinator.durable_tip().index() == 0 {
                                coordinator.require_successor_checkpoint_baseline();
                            }
                            break coordinator;
                        }
                        Err(cause) => return Err(pending.fail(error(cause)).await),
                    }
                }
            }
        };
        let post_coordinator_deadline = shutdown.borrow().clone();
        if let Some(token) = post_coordinator_deadline {
            let deadline = token.deadline();
            return Err(pending.cancel(deadline, Ok(())).await);
        }
        let applied_index = match pending.runtime().applied_index() {
            Ok(applied_index) => applied_index,
            Err(cause) => return Err(pending.fail(error(cause)).await),
        };
        coordinator.note_recovered_committed(applied_index);
        let runtime = pending.transfer();
        let opened = HaOpenNode {
            runtime,
            coordinator,
            recorder: self.recorder,
            recorder_hook: self.recorder_hook,
            auto_activate_stop,
            successor_stop,
            target_config_id,
        };
        #[cfg(test)]
        if let Some(gate) = open_phase_gate {
            gate.wait().await;
        }
        Ok(opened)
    }
}

struct HaOpenNode {
    runtime: Arc<NodeRuntime>,
    coordinator: Arc<CheckpointCoordinator>,
    recorder: RecorderFileStore,
    recorder_hook: HaRecorder,
    auto_activate_stop: Option<LogAnchor>,
    successor_stop: Option<LogAnchor>,
    target_config_id: u64,
}

enum HaOpenError {
    Startup {
        error: HaStartupError,
        cleanup: Result<(), HaNodeError>,
    },
    Cancelled {
        deadline: tokio::time::Instant,
        cleanup: Result<(), HaNodeError>,
    },
}

impl From<HaStartupError> for HaOpenError {
    fn from(error: HaStartupError) -> Self {
        Self::Startup {
            error,
            cleanup: Ok(()),
        }
    }
}

struct PendingHaRuntime {
    runtime: Option<Arc<NodeRuntime>>,
}

impl PendingHaRuntime {
    fn new(runtime: Arc<NodeRuntime>) -> Self {
        Self {
            runtime: Some(runtime),
        }
    }

    fn runtime(&self) -> &Arc<NodeRuntime> {
        self.runtime.as_ref().expect("pending runtime is present")
    }

    fn transfer(mut self) -> Arc<NodeRuntime> {
        self.runtime.take().expect("pending runtime is present")
    }

    async fn fail(mut self, error: HaStartupError) -> HaOpenError {
        let deadline = tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT;
        let cleanup = self.cleanup(deadline);
        HaOpenError::Startup { error, cleanup }
    }

    async fn cancel(
        mut self,
        deadline: tokio::time::Instant,
        prior_cleanup: Result<(), HaNodeError>,
    ) -> HaOpenError {
        let cleanup = combine_ha_errors(
            prior_cleanup
                .err()
                .into_iter()
                .chain(self.cleanup(deadline).err())
                .collect(),
        );
        HaOpenError::Cancelled { deadline, cleanup }
    }

    async fn merge(self, error: HaOpenError) -> HaOpenError {
        match error {
            HaOpenError::Startup {
                error,
                cleanup: prior_cleanup,
            } => {
                let mut pending = self;
                let deadline = tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT;
                let cleanup = combine_ha_errors(
                    prior_cleanup
                        .err()
                        .into_iter()
                        .chain(pending.cleanup(deadline).err())
                        .collect(),
                );
                HaOpenError::Startup { error, cleanup }
            }
            HaOpenError::Cancelled { deadline, cleanup } => self.cancel(deadline, cleanup).await,
        }
    }

    fn cleanup(&mut self, deadline: tokio::time::Instant) -> Result<(), HaNodeError> {
        let Some(runtime) = self.runtime.take() else {
            return Ok(());
        };
        runtime.cancel_operations();
        let _ = deadline;
        Ok(())
    }
}

impl Drop for PendingHaRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = &self.runtime {
            runtime.cancel_operations();
        }
    }
}

impl HaOpenNode {
    fn runtime(&self) -> Arc<NodeRuntime> {
        self.runtime.clone()
    }

    fn coordinator(&self) -> Arc<CheckpointCoordinator> {
        self.coordinator.clone()
    }

    fn certified_tail_router(
        &self,
        tail_token: impl Into<String>,
    ) -> Result<axum::Router, HaStartupError> {
        let config = self.runtime.config();
        let tail_config = TailReaderConfig::new(
            config.cluster_id(),
            config.epoch(),
            self.target_config_id,
            config.membership().clone(),
            config.recovery_generation(),
            tail_token,
        )
        .map_err(error)?;
        certified_tail_router_for_runtime(self.runtime.clone(), tail_config).map_err(error)
    }

    fn local_recorder(&self) -> RecorderFileStore {
        self.recorder.clone()
    }

    fn into_rhiza(self) -> Rhiza {
        Rhiza::from_open_runtime(self.runtime, Some(self.coordinator))
    }
}

type HaServerTask = AbortOnDropTask<Result<(), HaNodeError>>;

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum TestRecorderShutdownOutcome {
    Error(&'static str),
    Panic(&'static str),
}

#[cfg(test)]
fn spawn_ha_recorder_server(
    listener: tokio::net::TcpListener,
    recorder: HaRecorder,
    transport: HaRecorderTransport,
    peers: Vec<rhiza_node::PeerConfig>,
    recovery_generation: u64,
    shutdown: tokio::sync::watch::Receiver<bool>,
    started: tokio::sync::oneshot::Sender<()>,
) -> RecorderServerTask {
    #[cfg(test)]
    {
        spawn_ha_recorder_server_inner(
            listener,
            recorder,
            transport,
            peers,
            recovery_generation,
            shutdown,
            started,
            None,
            None,
        )
    }
    #[cfg(not(test))]
    {
        spawn_ha_recorder_server_inner(
            listener,
            recorder,
            transport,
            peers,
            recovery_generation,
            shutdown,
            started,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_ha_recorder_server_inner(
    listener: tokio::net::TcpListener,
    recorder: HaRecorder,
    transport: HaRecorderTransport,
    peers: Vec<rhiza_node::PeerConfig>,
    recovery_generation: u64,
    shutdown: tokio::sync::watch::Receiver<bool>,
    started: tokio::sync::oneshot::Sender<()>,
    #[cfg(test)] start_error: Option<&'static str>,
    #[cfg(test)] shutdown_outcome: Option<TestRecorderShutdownOutcome>,
) -> RecorderServerTask {
    #[cfg(test)]
    if let Some(message) = start_error {
        return RecorderServerTask::Tcp {
            ingress: spawn_failed_tracked_recorder_ingress(listener, started, message),
            completed_evidence: None,
        };
    }
    match transport {
        HaRecorderTransport::Http => {
            let router = recorder_router_for_generation(recorder, peers, recovery_generation);
            RecorderServerTask::Http {
                ingress: spawn_tracked_axum_ingress(
                    listener,
                    router,
                    shutdown,
                    started,
                    #[cfg(test)]
                    shutdown_outcome,
                ),
                completed_evidence: None,
            }
        }
        HaRecorderTransport::TcpPostcard => RecorderServerTask::Tcp {
            ingress: spawn_tracked_recorder_ingress(
                listener,
                shutdown,
                started,
                move |listener, lifecycle| {
                    serve_recorder_tcp(listener, recorder, peers, recovery_generation, lifecycle)
                },
            ),
            completed_evidence: None,
        },
        HaRecorderTransport::TcpTlsPostcard(tls) => RecorderServerTask::Tcp {
            ingress: spawn_tracked_recorder_ingress(
                listener,
                shutdown,
                started,
                move |listener, lifecycle| {
                    serve_recorder_tcp_tls(
                        listener,
                        recorder,
                        peers,
                        recovery_generation,
                        tls,
                        lifecycle,
                    )
                },
            ),
            completed_evidence: None,
        },
        #[cfg(feature = "recorder-postcard-rpc")]
        HaRecorderTransport::TcpPostcardRpc => RecorderServerTask::Tcp {
            ingress: spawn_tracked_recorder_ingress(
                listener,
                shutdown,
                started,
                move |listener, lifecycle| {
                    serve_recorder_postcard_rpc(
                        listener,
                        recorder,
                        peers,
                        recovery_generation,
                        lifecycle,
                    )
                },
            ),
            completed_evidence: None,
        },
        #[cfg(feature = "recorder-postcard-rpc")]
        HaRecorderTransport::TcpTlsPostcardRpc(tls) => RecorderServerTask::Tcp {
            ingress: spawn_tracked_recorder_ingress(
                listener,
                shutdown,
                started,
                move |listener, lifecycle| {
                    serve_recorder_postcard_rpc_tls(
                        listener,
                        recorder,
                        peers,
                        recovery_generation,
                        tls,
                        lifecycle,
                    )
                },
            ),
            completed_evidence: None,
        },
    }
}

/// Private HTTP ingress owner used by the service and HTTP recorder transports.
///
/// Its listener receipt is intentionally separate from the server task:
/// listener closure stops new admission, while accepted HTTP connections may
/// still be draining. Staging and admin task shutdown remain out of scope for
/// this HTTP-specific owner.
struct TrackedAxumIngress {
    task: HaServerTask,
    listener_dropped: tokio::sync::oneshot::Receiver<()>,
    listener_receipted: bool,
    force: tokio::sync::watch::Sender<bool>,
    forced: Arc<AtomicBool>,
}

/// Scoped owner for the four non-HTTP recorder listener transports.
///
/// The node transport owns every accepted connection and TLS handshake. This
/// outer owner retains its task, force authority, the actual listener-drop
/// receipt, and the transport's independently reported task disposition.
/// It makes no claim about service/admin/supervisor tasks outside recorder
/// ingress.
struct TrackedRecorderIngress {
    task: HaServerTask,
    listener_dropped: tokio::sync::oneshot::Receiver<()>,
    listener_receipted: bool,
    force: tokio::sync::watch::Sender<bool>,
    forced: Arc<AtomicBool>,
    node_tasks: Arc<Mutex<Option<NodeRecorderTaskDisposition>>>,
}

/// A scoped task owner which retains the runtime that created the task.
///
/// Dropping this value requests cancellation but deliberately does not join:
/// the sole place that detaches a public HA supervisor transfers ownership to
/// a runtime-managed reaper with the already-installed absolute deadline.
/// `poll` takes the handle on completion, so a completed task is never
/// aborted later by this owner's `Drop` implementation.
struct AbortOnDropTask<T> {
    task: Option<tokio::task::JoinHandle<T>>,
    runtime: tokio::runtime::Handle,
}

impl<T> AbortOnDropTask<T> {
    fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let runtime = tokio::runtime::Handle::current();
        let task = runtime.spawn(future);
        Self {
            task: Some(task),
            runtime,
        }
    }

    fn spawn_blocking<F>(function: F) -> Self
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let runtime = tokio::runtime::Handle::current();
        let task = runtime.spawn_blocking(function);
        Self {
            task: Some(task),
            runtime,
        }
    }

    fn abort(&self) {
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
    }

    fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    /// Transfer this owner to a runtime reaper bounded by the same absolute
    /// deadline. At D it only requests abort and intentionally detaches; it
    /// makes no claim that blocking work or its late effects have completed.
    fn reap_before(self, deadline: tokio::time::Instant)
    where
        T: Send + 'static,
    {
        let runtime = self.runtime.clone();
        runtime.spawn(async move {
            let mut task = self;
            let _ = await_task_before(&mut task, deadline).await;
        });
    }
}

impl<T> Future for AbortOnDropTask<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(
        self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let task = this
            .task
            .as_mut()
            .expect("AbortOnDropTask must not be polled after completion");
        match Pin::new(task).poll(context) {
            std::task::Poll::Ready(result) => {
                // A JoinHandle is consumed exactly when it reports Ready;
                // keeping it would let Drop issue a stale abort request.
                let _ = this.task.take();
                std::task::Poll::Ready(result)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        self.abort();
    }
}

/// Wait for a task through an absolute deadline without erasing a completion
/// that races with D. The task result wins when both branches are ready; once
/// D wins we inspect `is_finished` once more, then request abort and do not
/// wait again. This is intentionally conservative for non-preemptible
/// `spawn_blocking` work: cancellation is not a completion claim.
async fn await_task_before<T>(
    task: &mut AbortOnDropTask<T>,
    deadline: tokio::time::Instant,
) -> Option<Result<T, tokio::task::JoinError>> {
    let observed = tokio::select! {
        biased;
        result = &mut *task => Some(result),
        () = tokio::time::sleep_until(deadline) => None,
    };
    if observed.is_some() {
        return observed;
    }
    if task.is_finished() {
        Some(task.await)
    } else {
        task.abort();
        None
    }
}

/// Owns the one listener FD and emits a receipt only after that FD is gone.
/// Drop covers task cancellation/panic before the normal shutdown path.
struct ListenerOwner {
    listener: Option<tokio::net::TcpListener>,
    receipt: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ListenerOwner {
    fn new(listener: tokio::net::TcpListener, receipt: tokio::sync::oneshot::Sender<()>) -> Self {
        Self {
            listener: Some(listener),
            receipt: Some(receipt),
        }
    }

    fn listener(&self) -> &tokio::net::TcpListener {
        self.listener
            .as_ref()
            .expect("listener owner is only accepted while open")
    }

    fn close(&mut self) {
        drop(self.listener.take());
        if let Some(receipt) = self.receipt.take() {
            let _ = receipt.send(());
        }
    }
}

impl Drop for ListenerOwner {
    fn drop(&mut self) {
        self.close();
    }
}

impl std::future::Future for TrackedAxumIngress {
    type Output = Result<Result<(), HaNodeError>, tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.task).poll(context)
    }
}

impl TrackedAxumIngress {
    fn poll_listener_receipt(&mut self) -> bool {
        if !self.listener_receipted {
            self.listener_receipted = self.listener_dropped.try_recv().is_ok();
        }
        self.listener_receipted
    }
}

/// Admission/drain ownership for admin routes that share the service
/// listener. This is deliberately not a listener lease: admin has no
/// independent socket and therefore cannot contribute ingress-closed proof.
#[derive(Clone)]
enum AdminDrainLease {
    Disabled,
    Enabled(AdminTaskTracker),
}

impl AdminDrainLease {
    fn from_tracker(tracker: Option<AdminTaskTracker>) -> Self {
        match tracker {
            Some(tracker) => Self::Enabled(tracker),
            None => Self::Disabled,
        }
    }

    fn stop_admission(&self) {
        if let Self::Enabled(tracker) = self {
            tracker.stop_admission();
        }
    }

    async fn drain_before(&self, deadline: tokio::time::Instant) -> bool {
        match self {
            Self::Disabled => true,
            Self::Enabled(tracker) => tokio::time::timeout_at(deadline, tracker.wait_for_idle())
                .await
                .is_ok(),
        }
    }
}

/// The service task has exactly one monotonic owner state. A state transition
/// never recreates a completed/unstarted server task merely to fit an async
/// API: doing so previously made ownership and shutdown evidence ambiguous.
enum ServiceTaskState {
    Unstarted(ServiceTaskUnstarted),
    Running(TrackedAxumIngress),
    Completed {
        result: Option<Result<(), HaNodeError>>,
        evidence: ShutdownEvidence,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServiceTaskUnstarted {
    /// This child owned a bound service listener and dropped it before a
    /// service task was started. The actual FD is therefore known closed.
    OwnedListenerDropped,
    /// A live successor has not received the staging-owned listener. Its
    /// cleanup cannot manufacture a local listener-close receipt.
    DeferredOwnerUnknown,
}

impl ServiceTaskUnstarted {
    fn evidence(self) -> ShutdownEvidence {
        match self {
            Self::OwnedListenerDropped => PRE_SERVICE_SHUTDOWN_EVIDENCE,
            Self::DeferredOwnerUnknown => UNCERTAIN_SHUTDOWN_EVIDENCE,
        }
    }
}

struct ScopeShutdown {
    result: Result<(), HaNodeError>,
    evidence: ShutdownEvidence,
}

/// The sole owner of shared service/admin shutdown. Recorder ownership is
/// intentionally separate: it has a distinct listener and evidence channel.
struct HaTaskScope {
    service_shutdown: tokio::sync::watch::Sender<bool>,
    service: ServiceTaskState,
    admin: AdminDrainLease,
    shutdown_started: bool,
}

impl HaTaskScope {
    fn new(admin: AdminDrainLease, service: ServiceTaskUnstarted) -> Self {
        let (service_shutdown, _) = tokio::sync::watch::channel(false);
        Self {
            service_shutdown,
            service: ServiceTaskState::Unstarted(service),
            admin,
            shutdown_started: false,
        }
    }

    fn service_shutdown_receiver(&self) -> tokio::sync::watch::Receiver<bool> {
        self.service_shutdown.subscribe()
    }

    fn start_service(&mut self, ingress: TrackedAxumIngress) {
        debug_assert!(matches!(self.service, ServiceTaskState::Unstarted(_)));
        self.service = ServiceTaskState::Running(ingress);
    }

    fn set_unstarted_service(&mut self, service: ServiceTaskUnstarted) {
        debug_assert!(matches!(self.service, ServiceTaskState::Unstarted(_)));
        self.service = ServiceTaskState::Unstarted(service);
    }

    fn running_service_task(&mut self) -> Option<&mut TrackedAxumIngress> {
        match &mut self.service {
            ServiceTaskState::Running(ingress) => Some(ingress),
            ServiceTaskState::Unstarted(_) | ServiceTaskState::Completed { .. } => None,
        }
    }

    /// Cache a finished task's exact result before its owner can be dropped.
    /// The cache is taken exactly once by the caller that establishes the
    /// primary error; later shutdown uses only durable evidence.
    fn complete_running_service_task(
        &mut self,
        joined: Result<Result<(), HaNodeError>, tokio::task::JoinError>,
    ) -> Result<(), HaNodeError> {
        let ServiceTaskState::Running(mut ingress) = std::mem::replace(
            &mut self.service,
            ServiceTaskState::Unstarted(ServiceTaskUnstarted::DeferredOwnerUnknown),
        ) else {
            unreachable!("only a running service task may complete");
        };
        let joined_normally = joined.is_ok();
        let result = joined
            .map_err(|error| server_task_join_error("service server", error))
            .and_then(|result| result);
        let listener_closed = ingress.poll_listener_receipt();
        let evidence = ShutdownEvidence {
            ingress: ingress_after_service_wait(listener_closed),
            tasks: if joined_normally && result.is_ok() && !ingress.forced.load(Ordering::Acquire) {
                TaskDisposition::Quiesced
            } else {
                TaskDisposition::Uncertain
            },
        };
        self.service = ServiceTaskState::Completed {
            result: Some(result),
            evidence,
        };
        self.take_completed_service_result()
            .expect("service completion result is cached exactly once")
    }

    fn take_completed_service_result(&mut self) -> Option<Result<(), HaNodeError>> {
        match &mut self.service {
            ServiceTaskState::Completed { result, .. } => result.take(),
            ServiceTaskState::Unstarted(_) | ServiceTaskState::Running(_) => None,
        }
    }

    fn begin_shutdown(&mut self, runtime: &NodeRuntime) {
        // Admission closes before service ingress and before runtime work is
        // cancelled, so a request already routed after listener closure cannot
        // start a new admin mutation during drain.
        if !self.shutdown_started {
            self.admin.stop_admission();
            self.service_shutdown.send_replace(true);
            self.shutdown_started = true;
        }
        runtime.cancel_operations();
    }

    #[cfg(all(test, feature = "test-hooks"))]
    fn begin_shutdown_for_test(&mut self) {
        if !self.shutdown_started {
            self.admin.stop_admission();
            self.service_shutdown.send_replace(true);
            self.shutdown_started = true;
        }
    }

    async fn drain_before(&mut self, deadline: tokio::time::Instant) -> ScopeShutdown {
        let admin = self.admin.clone();
        let service = self.drain_service_before(deadline);
        let admin = admin.drain_before(deadline);
        let (mut service, admin_quiesced) = tokio::join!(service, admin);
        if !admin_quiesced {
            service.evidence.tasks = TaskDisposition::Uncertain;
            let admin_deadline =
                conservative_shutdown_deadline_error(HaShutdownPhase::Service, service.evidence);
            // Service completion is the authoritative terminal result.  An
            // admin drain that exhausts the same deadline is independent
            // cleanup evidence, so it must be retained even when the service
            // already supplied an exact source error.
            let terminal = std::mem::replace(&mut service.result, Ok(()));
            service.result = combine_ha_results(terminal.err(), Err(admin_deadline));
        }
        service
    }

    async fn drain_service_before(&mut self, deadline: tokio::time::Instant) -> ScopeShutdown {
        let service = std::mem::replace(
            &mut self.service,
            ServiceTaskState::Unstarted(ServiceTaskUnstarted::DeferredOwnerUnknown),
        );
        match service {
            ServiceTaskState::Unstarted(unstarted) => {
                let evidence = unstarted.evidence();
                self.service = ServiceTaskState::Unstarted(unstarted);
                ScopeShutdown {
                    result: Ok(()),
                    evidence,
                }
            }
            ServiceTaskState::Completed { result, evidence } => {
                self.service = ServiceTaskState::Completed { result, evidence };
                ScopeShutdown {
                    result: self.take_completed_service_result().unwrap_or(Ok(())),
                    evidence,
                }
            }
            ServiceTaskState::Running(mut ingress) => {
                let (result, ingress_disposition, tasks) =
                    wait_for_tracked_axum_ingress(&mut ingress, "service server", deadline).await;
                let evidence = ShutdownEvidence {
                    ingress: ingress_disposition,
                    tasks,
                };
                self.service = ServiceTaskState::Completed {
                    result: Some(result),
                    evidence,
                };
                ScopeShutdown {
                    result: self
                        .take_completed_service_result()
                        .expect("service drain caches its exact result"),
                    evidence,
                }
            }
        }
    }
}

impl Drop for HaTaskScope {
    fn drop(&mut self) {
        self.admin.stop_admission();
        self.service_shutdown.send_replace(true);
        // `TrackedAxumIngress` aborts its owned task on drop. This is merely
        // an ownership safety net; it does not join and must not claim either
        // listener closure or task quiescence.
    }
}

impl TrackedRecorderIngress {
    fn poll_listener_receipt(&mut self) -> bool {
        if !self.listener_receipted {
            self.listener_receipted = self.listener_dropped.try_recv().is_ok();
        }
        self.listener_receipted
    }

    fn reported_tasks(&self) -> TaskDisposition {
        match *lock_unpoison(&self.node_tasks) {
            Some(NodeRecorderTaskDisposition::Quiesced) => TaskDisposition::Quiesced,
            Some(NodeRecorderTaskDisposition::Uncertain) | None => TaskDisposition::Uncertain,
        }
    }
}

/// Recorder task ownership is transport-specific, but every listener variant
/// retains an exact listener receipt and a completed-task evidence cache.
enum RecorderServerTask {
    Http {
        ingress: TrackedAxumIngress,
        // A recorder HTTP task can win a supervisor select before cleanup
        // begins. Retain its conservative receipt so later cleanup cannot
        // treat that completed HTTP owner like an evidence-neutral TCP task.
        completed_evidence: Option<ShutdownEvidence>,
    },
    Tcp {
        ingress: TrackedRecorderIngress,
        completed_evidence: Option<ShutdownEvidence>,
    },
}

impl RecorderServerTask {
    fn completed_shutdown_evidence(&self) -> Option<ShutdownEvidence> {
        match self {
            Self::Http {
                completed_evidence, ..
            }
            | Self::Tcp {
                completed_evidence, ..
            } => *completed_evidence,
        }
    }
}

impl Future for RecorderServerTask {
    type Output = Result<Result<(), HaNodeError>, tokio::task::JoinError>;

    fn poll(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.as_mut().get_mut() {
            Self::Http {
                ingress,
                completed_evidence,
            } => match Pin::new(&mut ingress.task).poll(context) {
                std::task::Poll::Ready(result) => {
                    // Cache the receipt before the supervisor classifies the
                    // result as expected cooperative shutdown or an
                    // authoritative recorder failure. A listener receipt
                    // proves admission closure, but this wrapper alone does
                    // not upgrade the broader task evidence.
                    *completed_evidence = Some(ShutdownEvidence {
                        ingress: ingress_after_service_wait(ingress.poll_listener_receipt()),
                        tasks: TaskDisposition::Uncertain,
                    });
                    std::task::Poll::Ready(result)
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            Self::Tcp {
                ingress,
                completed_evidence,
            } => match Pin::new(&mut ingress.task).poll(context) {
                std::task::Poll::Ready(result) => {
                    let tasks = if result.is_ok() && !ingress.forced.load(Ordering::Acquire) {
                        ingress.reported_tasks()
                    } else {
                        TaskDisposition::Uncertain
                    };
                    *completed_evidence = Some(ShutdownEvidence {
                        ingress: ingress_after_service_wait(ingress.poll_listener_receipt()),
                        tasks,
                    });
                    std::task::Poll::Ready(result)
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
        }
    }
}

fn spawn_tracked_recorder_ingress<F, Fut>(
    listener: tokio::net::TcpListener,
    shutdown: tokio::sync::watch::Receiver<bool>,
    started: tokio::sync::oneshot::Sender<()>,
    serve: F,
) -> TrackedRecorderIngress
where
    F: FnOnce(tokio::net::TcpListener, RecorderIngressLifecycle) -> Fut + Send + 'static,
    Fut: Future<Output = RecorderIngressExit> + Send + 'static,
{
    let (listener_dropped, listener_dropped_rx) = tokio::sync::oneshot::channel();
    let (force, force_rx) = tokio::sync::watch::channel(false);
    let forced = Arc::new(AtomicBool::new(false));
    let node_tasks = Arc::new(Mutex::new(None));
    let task_node_tasks = Arc::clone(&node_tasks);
    let lifecycle = RecorderIngressLifecycle::new(shutdown, force_rx, started, listener_dropped);
    let task = HaServerTask::spawn(async move {
        let exit = serve(listener, lifecycle).await;
        *lock_unpoison(&task_node_tasks) = Some(exit.tasks);
        exit.result.map_err(HaNodeError::RecorderServer)
    });
    TrackedRecorderIngress {
        task,
        listener_dropped: listener_dropped_rx,
        listener_receipted: false,
        force,
        forced,
        node_tasks,
    }
}

#[cfg(test)]
fn spawn_failed_tracked_recorder_ingress(
    listener: tokio::net::TcpListener,
    started: tokio::sync::oneshot::Sender<()>,
    message: &'static str,
) -> TrackedRecorderIngress {
    let (listener_dropped, listener_dropped_rx) = tokio::sync::oneshot::channel();
    let (force, _) = tokio::sync::watch::channel(false);
    let forced = Arc::new(AtomicBool::new(false));
    let node_tasks = Arc::new(Mutex::new(None));
    let task_node_tasks = Arc::clone(&node_tasks);
    let task = HaServerTask::spawn(async move {
        let mut listener = ListenerOwner::new(listener, listener_dropped);
        drop(started);
        listener.close();
        *lock_unpoison(&task_node_tasks) = Some(NodeRecorderTaskDisposition::Quiesced);
        Err(HaNodeError::RecorderServer(message.into()))
    });
    TrackedRecorderIngress {
        task,
        listener_dropped: listener_dropped_rx,
        listener_receipted: false,
        force,
        forced,
        node_tasks,
    }
}

fn spawn_tracked_axum_ingress(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    shutdown: tokio::sync::watch::Receiver<bool>,
    started: tokio::sync::oneshot::Sender<()>,
    #[cfg(test)] recorder_shutdown_outcome: Option<TestRecorderShutdownOutcome>,
) -> TrackedAxumIngress {
    let (listener_dropped, listener_dropped_rx) = tokio::sync::oneshot::channel();
    let (force, force_rx) = tokio::sync::watch::channel(false);
    let forced = Arc::new(AtomicBool::new(false));
    let task = HaServerTask::spawn(async move {
        let result = run_tracked_axum_ingress(
            listener,
            router,
            shutdown,
            force_rx,
            started,
            listener_dropped,
        )
        .await;
        #[cfg(test)]
        if result.is_ok() {
            match recorder_shutdown_outcome {
                Some(TestRecorderShutdownOutcome::Error(message)) => {
                    return Err(HaNodeError::RecorderServer(message.into()));
                }
                Some(TestRecorderShutdownOutcome::Panic(message)) => panic!("{message}"),
                None => {}
            }
        }
        result
    });
    TrackedAxumIngress {
        task,
        listener_dropped: listener_dropped_rx,
        listener_receipted: false,
        force,
        forced,
    }
}

fn spawn_ha_service_server(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    shutdown: tokio::sync::watch::Receiver<bool>,
    started: tokio::sync::oneshot::Sender<()>,
) -> TrackedAxumIngress {
    spawn_tracked_axum_ingress(
        listener,
        router,
        shutdown,
        started,
        #[cfg(test)]
        None,
    )
}

async fn run_tracked_axum_ingress(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    mut force: tokio::sync::watch::Receiver<bool>,
    started: tokio::sync::oneshot::Sender<()>,
    listener_dropped: tokio::sync::oneshot::Sender<()>,
) -> Result<(), HaNodeError> {
    let (connection_shutdown, _) = tokio::sync::watch::channel(false);
    let mut connections = tokio::task::JoinSet::new();
    let mut listener = ListenerOwner::new(listener, listener_dropped);
    let _ = started.send(());

    loop {
        let accepted = listener.listener().accept();
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                // A connection failure is isolated to that peer. It must not
                // take down listener ownership or later healthy connections.
                let _ = result;
            }
            accepted = accepted => match accepted {
                Ok((stream, _)) => {
                    let router = router.clone();
                    let mut connection_shutdown = connection_shutdown.subscribe();
                    connections.spawn(async move {
                        let service = router.map_request(|request: hyper::Request<Incoming>| {
                            request.map(axum::body::Body::new)
                        });
                        let service = TowerToHyperService::new(service);
                        let io = TokioIo::new(stream);
                        let mut connection = std::pin::pin!(
                            http1::Builder::new()
                                .serve_connection(io, service)
                                .with_upgrades()
                        );
                        tokio::select! {
                            result = connection.as_mut() => { let _ = result; }
                            changed = connection_shutdown.changed() => {
                                if changed.is_err() || *connection_shutdown.borrow() {
                                    connection.as_mut().graceful_shutdown();
                                    let _ = connection.await;
                                }
                            }
                        }
                    });
                }
                Err(error) => match classify_accept_error(&error) {
                    AcceptErrorDisposition::ImmediateRetry => {}
                    AcceptErrorDisposition::ResourceBackoff => {
                        if wait_for_accept_backoff(&mut shutdown, ACCEPT_RESOURCE_BACKOFF).await {
                            break;
                        }
                    }
                    AcceptErrorDisposition::Terminal => {
                        return Err(HaNodeError::ServiceServer(format!(
                            "service server accept failed: {error}"
                        )));
                    }
                },
            }
        }
    }

    // This is the ingress boundary: no task retaining the listener survives
    // the receipt. Accepted connections get their own graceful-drain phase.
    listener.close();
    connection_shutdown.send_replace(true);
    while !connections.is_empty() {
        tokio::select! {
            Some(_) = connections.join_next() => {}
            changed = force.changed() => {
                if changed.is_err() || *force.borrow() {
                    connections.abort_all();
                    let reap_deadline = tokio::time::Instant::now()
                        + HA_SERVER_ABORT_RECEIPT_RESERVE;
                    while !connections.is_empty()
                        && tokio::time::timeout_at(reap_deadline, connections.join_next())
                            .await
                            .is_ok()
                    {}
                    break;
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptErrorDisposition {
    ImmediateRetry,
    ResourceBackoff,
    Terminal,
}

fn classify_accept_error(error: &io::Error) -> AcceptErrorDisposition {
    if matches!(
        error.kind(),
        io::ErrorKind::Interrupted
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::WouldBlock
    ) {
        return AcceptErrorDisposition::ImmediateRetry;
    }
    if error.kind() == io::ErrorKind::OutOfMemory
        || (cfg!(unix) && matches!(error.raw_os_error(), Some(23 | 24)))
    {
        return AcceptErrorDisposition::ResourceBackoff;
    }
    AcceptErrorDisposition::Terminal
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum TestStagingAcceptFailure {
    Kind(io::ErrorKind),
    RawOs(i32),
}

#[cfg(test)]
impl TestStagingAcceptFailure {
    fn into_error(self) -> io::Error {
        match self {
            Self::Kind(kind) => io::Error::from(kind),
            Self::RawOs(code) => io::Error::from_raw_os_error(code),
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
struct TestStagingAcceptFaults {
    failures: Mutex<VecDeque<TestStagingAcceptFailure>>,
    resource_backoff_delay: Duration,
    backoffs: tokio::sync::watch::Sender<u64>,
}

#[cfg(test)]
impl TestStagingAcceptFaults {
    fn new(
        failures: impl IntoIterator<Item = TestStagingAcceptFailure>,
        resource_backoff_delay: Duration,
    ) -> (Arc<Self>, tokio::sync::watch::Receiver<u64>) {
        let (backoffs, backoff_rx) = tokio::sync::watch::channel(0);
        (
            Arc::new(Self {
                failures: Mutex::new(failures.into_iter().collect()),
                resource_backoff_delay,
                backoffs,
            }),
            backoff_rx,
        )
    }

    fn next_error(&self) -> Option<io::Error> {
        self.failures
            .lock()
            .expect("staging accept fault queue poisoned")
            .pop_front()
            .map(TestStagingAcceptFailure::into_error)
    }

    fn observe_resource_backoff(&self) {
        self.backoffs.send_modify(|backoffs| *backoffs += 1);
    }
}

/// Returns true when shutdown interrupts the resource-error accept backoff.
async fn wait_for_accept_backoff(
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    delay: Duration,
) -> bool {
    tokio::select! {
        biased;
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        () = tokio::time::sleep(delay) => false,
    }
}

/// A non-cloneable lease for the service listener during live-successor
/// staging.  Moving the lease is the only way to hand the socket to the child;
/// dropping it closes the actual FD.
struct ListenerLease(Option<tokio::net::TcpListener>);

impl ListenerLease {
    fn new(listener: tokio::net::TcpListener) -> Self {
        Self(Some(listener))
    }

    fn listener(&self) -> &tokio::net::TcpListener {
        self.0
            .as_ref()
            .expect("listener lease is used only while it owns the socket")
    }

    fn into_listener(mut self) -> tokio::net::TcpListener {
        self.0.take().expect("listener lease is consumed only once")
    }
}

#[derive(Clone, Debug)]
enum StagingCommand {
    Running,
    Close,
    Handoff,
    /// A test-only pause after the production supervisor has sent Handoff but
    /// before the staging task moves its sole listener lease out.
    #[cfg(test)]
    BlockedHandoff(Arc<TestStagingHandoffGate>),
    #[cfg(test)]
    Fail(&'static str),
    #[cfg(all(test, feature = "test-hooks"))]
    Panic(&'static str),
}

#[cfg(test)]
#[derive(Debug)]
struct TestStagingHandoffGate {
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    released: tokio::sync::watch::Sender<bool>,
}

#[cfg(test)]
impl TestStagingHandoffGate {
    fn new() -> (Arc<Self>, tokio::sync::oneshot::Receiver<()>) {
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let (released, _released_rx) = tokio::sync::watch::channel(false);
        (
            Arc::new(Self {
                entered: std::sync::Mutex::new(Some(entered)),
                released,
            }),
            entered_rx,
        )
    }

    fn entered(&self) {
        if let Ok(mut entered) = self.entered.lock() {
            if let Some(entered) = entered.take() {
                let _ = entered.send(());
            }
        }
    }

    fn release_guard(self: &Arc<Self>) -> TestStagingHandoffRelease {
        TestStagingHandoffRelease(Arc::clone(self))
    }

    async fn wait_released(&self) {
        let mut released = self.released.subscribe();
        while !*released.borrow() {
            if released.changed().await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
struct TestStagingHandoffRelease(Arc<TestStagingHandoffGate>);

#[cfg(test)]
impl Drop for TestStagingHandoffRelease {
    fn drop(&mut self) {
        self.0.released.send_replace(true);
    }
}

enum StagingExit {
    Closed,
    Handoff(ListenerLease),
}

enum StagingCommandOutcome {
    Continue(ListenerLease),
    Exit(StagingExit),
}

type StagingServerTask = tokio::task::JoinHandle<Result<StagingExit, HaNodeError>>;

/// An unfinished staging task owns the listener.  Aborting or dropping it
/// therefore closes the unique FD; a completed handoff carries the lease out
/// through its join result instead of leaving a detached accept loop behind.
struct AbortStagingServerOnDrop(Option<StagingServerTask>);

impl AbortStagingServerOnDrop {
    fn new(task: StagingServerTask) -> Self {
        Self(Some(task))
    }
}

impl std::ops::Deref for AbortStagingServerOnDrop {
    type Target = StagingServerTask;

    fn deref(&self) -> &Self::Target {
        self.0
            .as_ref()
            .expect("staging task is present until it is consumed")
    }
}

impl std::ops::DerefMut for AbortStagingServerOnDrop {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
            .as_mut()
            .expect("staging task is present until it is consumed")
    }
}

impl Drop for AbortStagingServerOnDrop {
    fn drop(&mut self) {
        if let Some(task) = self.0.as_ref() {
            if !task.is_finished() {
                task.abort();
            }
        }
    }
}

#[derive(Clone)]
struct SuccessorStagingState {
    ready: Arc<AtomicBool>,
}

async fn successor_staging_livez() -> axum::http::StatusCode {
    axum::http::StatusCode::OK
}

async fn successor_staging_readyz(
    axum::extract::State(state): axum::extract::State<SuccessorStagingState>,
) -> axum::http::StatusCode {
    if state.ready.load(Ordering::Acquire) {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}

fn spawn_successor_staging_server(
    listener: ListenerLease,
    ready: Arc<AtomicBool>,
    command: tokio::sync::watch::Receiver<StagingCommand>,
    started: tokio::sync::oneshot::Sender<()>,
) -> StagingServerTask {
    #[cfg(test)]
    {
        spawn_successor_staging_server_inner(listener, ready, command, started, None, None)
    }
    #[cfg(not(test))]
    {
        spawn_successor_staging_server_inner(listener, ready, command, started)
    }
}

fn spawn_successor_staging_server_inner(
    listener: ListenerLease,
    ready: Arc<AtomicBool>,
    command: tokio::sync::watch::Receiver<StagingCommand>,
    started: tokio::sync::oneshot::Sender<()>,
    #[cfg(test)] close_error: Option<&'static str>,
    #[cfg(test)] accept_faults: Option<Arc<TestStagingAcceptFaults>>,
) -> StagingServerTask {
    tokio::spawn(async move {
        let router = axum::Router::new()
            .route(LIVEZ_PATH, axum::routing::get(successor_staging_livez))
            .route(READYZ_PATH, axum::routing::get(successor_staging_readyz))
            .with_state(SuccessorStagingState { ready });
        run_successor_staging_server(
            listener,
            router,
            command,
            started,
            #[cfg(test)]
            close_error,
            #[cfg(test)]
            accept_faults,
        )
        .await
    })
}

async fn accept_successor_staging_connection(
    listener: &tokio::net::TcpListener,
    #[cfg(test)] accept_faults: Option<&TestStagingAcceptFaults>,
) -> io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    #[cfg(test)]
    if let Some(error) = accept_faults.and_then(TestStagingAcceptFaults::next_error) {
        return Err(error);
    }
    listener.accept().await
}

async fn apply_staging_command(
    command: StagingCommand,
    listener: ListenerLease,
    connections: &mut tokio::task::JoinSet<()>,
    #[cfg(test)] close_error: Option<&'static str>,
) -> Result<StagingCommandOutcome, HaNodeError> {
    match command {
        StagingCommand::Running => Ok(StagingCommandOutcome::Continue(listener)),
        StagingCommand::Close => {
            drop(listener);
            // Health-check connections are disposable. An idle HTTP/1
            // keep-alive must not hold successor shutdown open indefinitely
            // after the sole listener is gone.
            connections.abort_all();
            while connections.join_next().await.is_some() {}
            #[cfg(test)]
            if let Some(message) = close_error {
                return Err(HaNodeError::ServiceServer(message.into()));
            }
            Ok(StagingCommandOutcome::Exit(StagingExit::Closed))
        }
        StagingCommand::Handoff => {
            // Existing health-check connections are aborted rather than
            // detached, so the child becomes the sole listener owner before
            // it is started.
            connections.abort_all();
            while connections.join_next().await.is_some() {}
            Ok(StagingCommandOutcome::Exit(StagingExit::Handoff(listener)))
        }
        #[cfg(test)]
        StagingCommand::BlockedHandoff(gate) => {
            // This is deliberately in the real staging task, after Handoff is
            // observed and before ListenerLease is moved. Aborting this await
            // drops the actual sole FD.
            gate.entered();
            gate.wait_released().await;
            connections.abort_all();
            while connections.join_next().await.is_some() {}
            Ok(StagingCommandOutcome::Exit(StagingExit::Handoff(listener)))
        }
        #[cfg(test)]
        StagingCommand::Fail(message) => Err(HaNodeError::ServiceServer(message.into())),
        #[cfg(all(test, feature = "test-hooks"))]
        StagingCommand::Panic(message) => panic!("{message}"),
    }
}

async fn wait_for_staging_accept_backoff(
    command: &mut tokio::sync::watch::Receiver<StagingCommand>,
    delay: Duration,
) -> Option<StagingCommand> {
    tokio::select! {
        biased;
        changed = command.changed() => Some(if changed.is_err() {
            StagingCommand::Close
        } else {
            command.borrow().clone()
        }),
        () = tokio::time::sleep(delay) => None,
    }
}

async fn run_successor_staging_server(
    listener: ListenerLease,
    router: axum::Router,
    mut command: tokio::sync::watch::Receiver<StagingCommand>,
    started: tokio::sync::oneshot::Sender<()>,
    #[cfg(test)] close_error: Option<&'static str>,
    #[cfg(test)] accept_faults: Option<Arc<TestStagingAcceptFaults>>,
) -> Result<StagingExit, HaNodeError> {
    let mut connections = tokio::task::JoinSet::new();
    let mut listener = listener;
    let _ = started.send(());

    loop {
        let accepted = accept_successor_staging_connection(
            listener.listener(),
            #[cfg(test)]
            accept_faults.as_deref(),
        );
        tokio::select! {
            biased;
            changed = command.changed() => {
                let next_command = if changed.is_err() {
                    StagingCommand::Close
                } else {
                    command.borrow().clone()
                };
                match apply_staging_command(
                    next_command,
                    listener,
                    &mut connections,
                    #[cfg(test)]
                    close_error,
                ).await? {
                    StagingCommandOutcome::Continue(next_listener) => listener = next_listener,
                    StagingCommandOutcome::Exit(exit) => return Ok(exit),
                }
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = accepted => match accepted {
                Ok((stream, _)) => {
                    let router = router.clone();
                    connections.spawn(async move {
                        let service = router.map_request(|request: hyper::Request<Incoming>| {
                            request.map(axum::body::Body::new)
                        });
                        let service = TowerToHyperService::new(service);
                        let io = TokioIo::new(stream);
                        let _ = http1::Builder::new()
                            .serve_connection(io, service)
                            .with_upgrades()
                            .await;
                    });
                }
                Err(error) => match classify_accept_error(&error) {
                    AcceptErrorDisposition::ImmediateRetry => {}
                    AcceptErrorDisposition::ResourceBackoff => {
                        #[cfg(test)]
                        let backoff_delay = accept_faults.as_ref().map_or(
                            ACCEPT_RESOURCE_BACKOFF,
                            |faults| {
                                faults.observe_resource_backoff();
                                faults.resource_backoff_delay
                            },
                        );
                        #[cfg(not(test))]
                        let backoff_delay = ACCEPT_RESOURCE_BACKOFF;
                        if let Some(next_command) =
                            wait_for_staging_accept_backoff(&mut command, backoff_delay).await
                        {
                            match apply_staging_command(
                                next_command,
                                listener,
                                &mut connections,
                                #[cfg(test)]
                                close_error,
                            ).await? {
                                StagingCommandOutcome::Continue(next_listener) => {
                                    listener = next_listener;
                                }
                                StagingCommandOutcome::Exit(exit) => return Ok(exit),
                            }
                        }
                    }
                    AcceptErrorDisposition::Terminal => {
                        return Err(HaNodeError::ServiceServer(format!(
                            "successor staging service accept failed: {error}"
                        )));
                    }
                },
            }
        }
    }
}

enum LiveSuccessorPrestage {
    Published(Box<PublishedHaSuccessorPrestage>),
    Finalized,
    TargetCheckpoint,
}

fn validate_live_successor_draft(
    prestage: &HaSuccessorPrestageConfig,
    startup: &HaStartupConfig,
) -> Result<(), HaStartupError> {
    if startup.mode != HaStartupMode::Rejoin
        || startup.predecessor.is_some()
        || !startup.node_config.configuration_state().is_active()
    {
        return Err(fail(
            "live successor requires an unbound active draft in rejoin mode",
        ));
    }
    let source = prestage.archive.checkpoint_identity().map_err(error)?;
    let target = startup.archive.checkpoint_identity().map_err(error)?;
    if source.cluster_id() != startup.node_config.cluster_id()
        || target.cluster_id() != startup.node_config.cluster_id()
        || source.epoch() != startup.node_config.epoch()
        || target.epoch() != startup.node_config.epoch()
        || source.config_id().checked_add(1) != Some(target.config_id())
        || target.config_id() != startup.node_config.config_id()
        || source.recovery_generation() != target.recovery_generation()
        || target.recovery_generation() != startup.node_config.recovery_generation()
        || prestage.target_node_id != startup.node_config.node_id()
        || prestage.execution_profile != startup.node_config.execution_profile()
        || prestage.target_membership != *startup.node_config.membership()
    {
        return Err(fail(
            "live successor draft, source checkpoint, and target checkpoint identities differ",
        ));
    }
    Ok(())
}

async fn prepare_live_successor(
    prestage: HaSuccessorPrestageConfig,
    startup: &HaStartupConfig,
) -> Result<LiveSuccessorPrestage, HaStartupError> {
    let target_checkpoint = startup
        .archive
        .initialize_checkpoint()
        .await
        .map_err(error)?;
    let complete = read_optional_bounded_regular_file_no_follow(
        &startup
            .node_config
            .data_dir()
            .join(SUCCESSOR_RESTORE_COMPLETE_FILE),
        MAX_SUCCESSOR_RESTORE_CONTROL_BYTES,
        "successor restore completion",
    )?;
    if complete.is_some() {
        // A completed live successor can restart before its first target checkpoint. The child
        // startup validates the exact receipt and predecessor Stop before it opens the runtime.
        return Ok(LiveSuccessorPrestage::Finalized);
    }
    let target_manifest = target_checkpoint.manifest();
    let target_checkpoint_empty =
        target_manifest.tip().index() == 0 && target_manifest.segments().is_empty();
    if !target_checkpoint_empty {
        let active_target_snapshot = target_manifest.base().snapshot().is_some_and(|snapshot| {
            let anchor = snapshot.anchor();
            anchor.configuration_state().is_active()
                && anchor.config_id() == startup.node_config.config_id()
        });
        if !active_target_snapshot {
            return Err(fail(
                "non-empty live successor checkpoint is not an active target snapshot",
            ));
        }
        return Ok(LiveSuccessorPrestage::TargetCheckpoint);
    }
    let data_dir = startup.node_config.data_dir().clone();
    let predecessor_config_id = prestage
        .archive
        .checkpoint_identity()
        .map_err(error)?
        .config_id();
    match HaSuccessorPrestageConfig::resume(
        &data_dir,
        predecessor_config_id,
        prestage.predecessor_membership.clone(),
        prestage.tail_token.clone(),
    ) {
        Ok(prepared) => prepared
            .publish(&data_dir)
            .map(Box::new)
            .map(LiveSuccessorPrestage::Published),
        Err(resume_error) => {
            let predecessor_configuration = rhiza_core::ConfigurationState::active(
                predecessor_config_id,
                prestage.predecessor_membership.digest(),
            );
            match inspect_successor_prestage(&data_dir, predecessor_configuration) {
                Ok(existing) if existing.state() == SuccessorPrestageState::Finalized => {
                    drop(existing);
                    Ok(LiveSuccessorPrestage::Finalized)
                }
                Ok(existing) => {
                    drop(existing);
                    Err(resume_error)
                }
                Err(DurabilityError::DataDirNotFresh(_)) if local_data_is_fresh(&data_dir)? => {
                    prestage
                        .prepare()
                        .await?
                        .publish(&data_dir)
                        .map(Box::new)
                        .map(LiveSuccessorPrestage::Published)
                }
                Err(DurabilityError::DataDirNotFresh(_)) => Err(fail(format!(
                    "cannot resume live successor prestage ({resume_error}); local data is not fresh"
                ))),
                Err(cause) => Err(error(cause)),
            }
        }
    }
}

fn accept_live_predecessor(
    bound: &mut Option<HaPredecessor>,
    predecessor: HaPredecessor,
    learner: Option<&PublishedHaSuccessorPrestage>,
    finalized: bool,
    staging_ready: &AtomicBool,
    state: &tokio::sync::watch::Sender<HaNodeSnapshot>,
) -> Result<(), HaNodeError> {
    match bound {
        None => {
            let reached = match learner {
                Some(learner) => live_successor_reached_stop(learner, &predecessor)?,
                None => finalized,
            };
            *bound = Some(predecessor);
            if !reached {
                // The first authoritative Stop immediately invalidates
                // pre-Stop readiness. Publish that fact before the loop can
                // issue another certified-tail fetch.
                staging_ready.store(false, Ordering::Release);
                publish_ha_state(state, HaNodeStatus::CatchingUp, None, None);
            }
            Ok(())
        }
        Some(existing) if existing == &predecessor => Ok(()),
        Some(_) => Err(HaNodeError::Startup(fail(
            "live successor predecessor binding changed after first observation",
        ))),
    }
}

fn live_successor_reached_stop(
    learner: &PublishedHaSuccessorPrestage,
    predecessor: &HaPredecessor,
) -> Result<bool, HaNodeError> {
    let durable = learner.durable_anchor().map_err(HaNodeError::Startup)?;
    let stop = LogAnchor::new(predecessor.stop.entry.index, predecessor.stop.entry.hash);
    if durable.index() > stop.index()
        || (durable.index() == stop.index() && durable.hash() != stop.hash())
    {
        return Err(HaNodeError::Startup(fail(
            "live successor diverged from the exact predecessor Stop",
        )));
    }
    Ok(durable == stop)
}

enum LiveSuccessorRetryEvent {
    Shutdown(ShutdownSignal),
    Staging(Result<Result<StagingExit, HaNodeError>, tokio::task::JoinError>),
    Predecessor(Option<Box<HaPredecessor>>),
    Elapsed,
}

/// Empty successful pages and transient source failures share exactly the
/// same interruptible retry boundary. Shutdown remains biased and retains its
/// original token/D; staging and predecessor changes are never hidden behind
/// the fixed retry delay.
async fn wait_live_successor_retry(
    shutdown: &mut tokio::sync::watch::Receiver<ShutdownSignal>,
    staging_task: &mut AbortStagingServerOnDrop,
    predecessor_rx: &mut tokio::sync::mpsc::UnboundedReceiver<HaPredecessor>,
) -> LiveSuccessorRetryEvent {
    tokio::select! {
        biased;
        changed = shutdown.changed() => {
            let _ = changed;
            LiveSuccessorRetryEvent::Shutdown(shutdown.borrow().clone())
        }
        result = &mut **staging_task => LiveSuccessorRetryEvent::Staging(result),
        predecessor = predecessor_rx.recv() => {
            LiveSuccessorRetryEvent::Predecessor(predecessor.map(Box::new))
        }
        () = tokio::time::sleep(SUCCESSOR_TAIL_RETRY_DELAY) => {
            LiveSuccessorRetryEvent::Elapsed
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn supervise_live_successor(
    prestage: HaSuccessorPrestageConfig,
    startup: HaStartupConfig,
    mut serve: SuccessorServeConfig,
    tail_source: Arc<dyn HaCertifiedTailSource>,
    mut shutdown: tokio::sync::watch::Receiver<ShutdownSignal>,
    mut predecessor_rx: tokio::sync::mpsc::UnboundedReceiver<HaPredecessor>,
    state: tokio::sync::watch::Sender<HaNodeSnapshot>,
) -> Result<(), HaNodeError> {
    let cleanup_context = LiveSuccessorCleanupContext {
        #[cfg(test)]
        token_observer: serve.cleanup_token_observer.clone(),
    };
    let staging_ready = Arc::new(AtomicBool::new(false));
    let (staging_shutdown, staging_shutdown_rx) =
        tokio::sync::watch::channel(StagingCommand::Running);
    let (staging_started, staging_started_rx) = tokio::sync::oneshot::channel();
    let mut staging_task = AbortStagingServerOnDrop::new(serve.spawn_staging_server(
        Arc::clone(&staging_ready),
        staging_shutdown_rx,
        staging_started,
    ));
    let mut staging_started_rx = staging_started_rx;
    tokio::select! {
        biased;
        _changed = shutdown.changed() => {
            let deadline = shutdown_deadline(&shutdown.borrow());
            let cleanup = stop_staging_server_before(
                &staging_shutdown,
                &mut staging_task,
                "successor staging service",
                deadline,
            ).await;
            return finish_cancelled_live_successor(&state, cleanup);
        }
        result = &mut *staging_task => {
            let error = unexpected_staging_server_exit(result);
            publish_ha_failure(&state, error.clone());
            return Err(error);
        }
        result = &mut staging_started_rx => {
            if result.is_err() {
                let error = HaNodeError::ServiceServer(
                    "successor staging service stopped before reporting startup".into(),
                );
                return fail_live_successor_catchup(
                    &state,
                    &shutdown,
                    &staging_shutdown,
                    &mut staging_task,
                    &cleanup_context,
                    error,
                )
                .await;
            }
        }
    }

    publish_ha_state(&state, HaNodeStatus::Restoring, None, None);
    let staged = {
        let preparation = prepare_live_successor(prestage, &startup);
        tokio::pin!(preparation);
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                let deadline = shutdown_deadline(&shutdown.borrow());
                let cleanup = stop_staging_server_before(
                    &staging_shutdown,
                    &mut staging_task,
                    "successor staging service",
                    deadline,
                ).await;
                return finish_cancelled_live_successor(&state, cleanup);
            }
            result = &mut *staging_task => {
                let error = unexpected_staging_server_exit(result);
                publish_ha_failure(&state, error.clone());
                return Err(error);
            }
            result = &mut preparation => match result {
                Ok(staged) => staged,
                Err(error) => {
                    let error = HaNodeError::Startup(error);
                    return fail_live_successor_catchup(
                        &state,
                        &shutdown,
                        &staging_shutdown,
                        &mut staging_task,
                        &cleanup_context,
                        error,
                    )
                    .await;
                }
            }
        }
    };

    if matches!(staged, LiveSuccessorPrestage::TargetCheckpoint) {
        publish_ha_state(&state, HaNodeStatus::CatchingUp, None, None);
        return supervise_live_successor_child_after_staging(
            startup,
            serve,
            &staging_shutdown,
            &mut staging_task,
            shutdown,
            state,
        )
        .await;
    }

    let mut learner = match staged {
        LiveSuccessorPrestage::Published(learner) => Some(*learner),
        LiveSuccessorPrestage::Finalized => None,
        LiveSuccessorPrestage::TargetCheckpoint => unreachable!("handled above"),
    };
    let finalized = learner.is_none();
    let mut startup = Some(startup);
    let mut bound = None;
    publish_ha_state(&state, HaNodeStatus::CatchingUp, None, None);

    macro_rules! catchup_try {
        ($result:expr) => {
            match $result {
                Ok(value) => value,
                Err(primary) => {
                    return fail_live_successor_catchup(
                        &state,
                        &shutdown,
                        &staging_shutdown,
                        &mut staging_task,
                        &cleanup_context,
                        primary,
                    )
                    .await;
                }
            }
        };
    }

    let startup = loop {
        if let Some(predecessor) = bound.as_ref() {
            let reached = match learner.as_ref() {
                Some(learner) => {
                    catchup_try!(live_successor_reached_stop(learner, predecessor))
                }
                None => finalized,
            };
            if reached {
                staging_ready.store(false, Ordering::Release);
                publish_ha_state(&state, HaNodeStatus::Transitioning, None, None);
                let configured = catchup_try!(startup
                    .take()
                    .expect("live successor startup is present")
                    .bind_live_predecessor(predecessor.clone())
                    .map_err(HaNodeError::Startup));
                let configured = match learner.take() {
                    Some(learner) => {
                        catchup_try!(learner.finalize(configured).map_err(HaNodeError::Startup))
                    }
                    None => configured,
                };
                break configured;
            }
        }

        if learner.is_none() {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    let _ = changed;
                    let deadline = shutdown_deadline(&shutdown.borrow());
                    let cleanup = stop_staging_server_before(
                        &staging_shutdown,
                        &mut staging_task,
                        "successor staging service",
                        deadline,
                    ).await;
                    return finish_cancelled_live_successor(&state, cleanup);
                }
                result = &mut *staging_task => {
                    let error = unexpected_staging_server_exit(result);
                    publish_ha_failure(&state, error.clone());
                    return Err(error);
                }
                predecessor = predecessor_rx.recv() => {
                    let predecessor = catchup_try!(predecessor.ok_or(HaNodeError::Cancelled));
                    catchup_try!(accept_live_predecessor(
                        &mut bound,
                        predecessor,
                        learner.as_ref(),
                        finalized,
                        &staging_ready,
                        &state,
                    ));
                }
            }
            continue;
        }

        let request = catchup_try!(learner
            .as_ref()
            .expect("published live successor learner is present")
            .tail_request(DEFAULT_CERTIFIED_TAIL_ENTRIES)
            .map_err(HaNodeError::Startup));
        let fetch = tail_source.fetch(&request);
        tokio::pin!(fetch);
        let response = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                let deadline = shutdown_deadline(&shutdown.borrow());
                let cleanup = stop_staging_server_before(
                    &staging_shutdown,
                    &mut staging_task,
                    "successor staging service",
                    deadline,
                ).await;
                return finish_cancelled_live_successor(&state, cleanup);
            }
            result = &mut *staging_task => {
                let error = unexpected_staging_server_exit(result);
                publish_ha_failure(&state, error.clone());
                return Err(error);
            }
            predecessor = predecessor_rx.recv() => {
                let predecessor = catchup_try!(predecessor.ok_or(HaNodeError::Cancelled));
                catchup_try!(accept_live_predecessor(
                    &mut bound,
                    predecessor,
                    learner.as_ref(),
                    finalized,
                    &staging_ready,
                    &state,
                ));
                continue;
            }
            result = &mut fetch => result,
        };
        match response {
            Ok(response) => {
                let progress = catchup_try!(learner
                    .as_ref()
                    .expect("published live successor learner is present")
                    .apply_page(&request, &response)
                    .map_err(HaNodeError::Startup));
                let caught_up = progress.durable == progress.observed_source_tip;
                staging_ready.store(caught_up && bound.is_none(), Ordering::Release);
                publish_ha_state(
                    &state,
                    if caught_up && bound.is_none() {
                        HaNodeStatus::PreStopReady
                    } else {
                        HaNodeStatus::CatchingUp
                    },
                    None,
                    None,
                );
                if response.records.is_empty() {
                    match wait_live_successor_retry(
                        &mut shutdown,
                        &mut staging_task,
                        &mut predecessor_rx,
                    )
                    .await
                    {
                        LiveSuccessorRetryEvent::Shutdown(token) => {
                            let deadline = shutdown_deadline(&token);
                            let cleanup = stop_staging_server_before(
                                &staging_shutdown,
                                &mut staging_task,
                                "successor staging service",
                                deadline,
                            )
                            .await;
                            return finish_cancelled_live_successor(&state, cleanup);
                        }
                        LiveSuccessorRetryEvent::Staging(result) => {
                            let error = unexpected_staging_server_exit(result);
                            publish_ha_failure(&state, error.clone());
                            return Err(error);
                        }
                        LiveSuccessorRetryEvent::Predecessor(predecessor) => {
                            let predecessor = catchup_try!(predecessor
                                .map(|value| *value)
                                .ok_or(HaNodeError::Cancelled));
                            catchup_try!(accept_live_predecessor(
                                &mut bound,
                                predecessor,
                                learner.as_ref(),
                                finalized,
                                &staging_ready,
                                &state,
                            ));
                        }
                        LiveSuccessorRetryEvent::Elapsed => {}
                    }
                }
            }
            Err(HaCertifiedTailError::Unavailable(_)) => {
                staging_ready.store(false, Ordering::Release);
                publish_ha_state(&state, HaNodeStatus::CatchingUp, None, None);
                match wait_live_successor_retry(
                    &mut shutdown,
                    &mut staging_task,
                    &mut predecessor_rx,
                )
                .await
                {
                    LiveSuccessorRetryEvent::Shutdown(token) => {
                        let deadline = shutdown_deadline(&token);
                        let cleanup = stop_staging_server_before(
                            &staging_shutdown,
                            &mut staging_task,
                            "successor staging service",
                            deadline,
                        )
                        .await;
                        return finish_cancelled_live_successor(&state, cleanup);
                    }
                    LiveSuccessorRetryEvent::Staging(result) => {
                        let error = unexpected_staging_server_exit(result);
                        publish_ha_failure(&state, error.clone());
                        return Err(error);
                    }
                    LiveSuccessorRetryEvent::Predecessor(predecessor) => {
                        let predecessor = catchup_try!(predecessor
                            .map(|value| *value)
                            .ok_or(HaNodeError::Cancelled));
                        catchup_try!(accept_live_predecessor(
                            &mut bound,
                            predecessor,
                            learner.as_ref(),
                            finalized,
                            &staging_ready,
                            &state,
                        ));
                    }
                    LiveSuccessorRetryEvent::Elapsed => {}
                }
            }
            Err(
                error @ (HaCertifiedTailError::RebaseRequired(_)
                | HaCertifiedTailError::Rejected(_)),
            ) => {
                let error = HaNodeError::CertifiedTail(error);
                return fail_live_successor_catchup(
                    &state,
                    &shutdown,
                    &staging_shutdown,
                    &mut staging_task,
                    &cleanup_context,
                    error,
                )
                .await;
            }
        }
    };

    supervise_live_successor_child_after_staging(
        startup,
        serve,
        &staging_shutdown,
        &mut staging_task,
        shutdown,
        state,
    )
    .await
}

fn finish_cancelled_live_successor(
    state: &tokio::sync::watch::Sender<HaNodeSnapshot>,
    cleanup: Result<(), HaNodeError>,
) -> Result<(), HaNodeError> {
    match cleanup {
        Ok(()) => {
            publish_ha_state(state, HaNodeStatus::Stopped, None, None);
            Ok(())
        }
        Err(error) => {
            publish_ha_failure(state, error.clone());
            Err(error)
        }
    }
}

async fn fail_live_successor_catchup(
    state: &tokio::sync::watch::Sender<HaNodeSnapshot>,
    shutdown: &tokio::sync::watch::Receiver<ShutdownSignal>,
    staging_command: &tokio::sync::watch::Sender<StagingCommand>,
    staging_task: &mut AbortStagingServerOnDrop,
    _cleanup_context: &LiveSuccessorCleanupContext,
    primary: HaNodeError,
) -> Result<(), HaNodeError> {
    let token = live_successor_cleanup_token(
        shutdown,
        tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT,
    );
    #[cfg(test)]
    if let Some(observer) = _cleanup_context.token_observer.as_ref() {
        observer.observe(&token);
    }
    let cleanup = stop_staging_server_before(
        staging_command,
        staging_task,
        "successor staging service",
        token.deadline(),
    )
    .await;
    let result = combine_ha_results(Some(primary), cleanup);
    if let Err(error) = &result {
        publish_ha_failure(state, error.clone());
    }
    result
}

/// Cleanup caused by a successor-side failure must not extend an already
/// requested outer shutdown. The exact external Arc carries both authority
/// and its absolute D; mint an internal fallback only when none exists.
fn live_successor_cleanup_token(
    shutdown: &tokio::sync::watch::Receiver<ShutdownSignal>,
    internal_deadline: tokio::time::Instant,
) -> Arc<ShutdownToken> {
    shutdown
        .borrow()
        .clone()
        .unwrap_or_else(|| Arc::new(ShutdownToken::new_internal_at(internal_deadline)))
}

/// A child's shutdown result is authoritative for its own terminal chain.
/// Avoid wrapping the same terminal primary twice when the child already
/// returned it (directly or as a completed Cleanup chain), while still
/// attaching shutdown receipt/deadline failures to the published primary.
fn normalize_live_successor_child_shutdown(
    primary: Option<HaNodeError>,
    child_result: Result<(), HaNodeError>,
) -> Result<(), HaNodeError> {
    match child_result {
        Ok(()) => primary.map_or(Ok(()), Err),
        Err(authoritative @ HaNodeError::Cleanup { .. }) => Err(authoritative),
        Err(
            cleanup @ (HaNodeError::ShutdownDeadlineExceeded { .. }
            | HaNodeError::ShutdownIncomplete { .. }),
        ) => combine_ha_results(primary, Err(cleanup)),
        // Other child failures are the child's authoritative terminal result,
        // including the same direct primary observed in its state snapshot.
        Err(authoritative) => Err(authoritative),
    }
}

fn combine_live_successor_child_and_staging(
    primary: Option<HaNodeError>,
    child_result: Result<(), HaNodeError>,
    staging_result: Result<(), HaNodeError>,
) -> Result<(), HaNodeError> {
    let child_result = normalize_live_successor_child_shutdown(primary, child_result);
    combine_ha_results(child_result.err(), staging_result)
}

#[allow(clippy::too_many_arguments)]
async fn supervise_live_successor_child_after_staging(
    startup: HaStartupConfig,
    serve: SuccessorServeConfig,
    staging_command: &tokio::sync::watch::Sender<StagingCommand>,
    staging_task: &mut AbortStagingServerOnDrop,
    shutdown: tokio::sync::watch::Receiver<ShutdownSignal>,
    state: tokio::sync::watch::Sender<HaNodeSnapshot>,
) -> Result<(), HaNodeError> {
    let (service_ready, mut service_ready_rx) = tokio::sync::oneshot::channel();
    let (service_listener, service_listener_rx) = tokio::sync::oneshot::channel();
    let child = startup.start(serve.into_deferred_child(service_ready, service_listener_rx));
    supervise_live_successor_child_after_staging_with_child(
        child,
        &mut service_ready_rx,
        service_listener,
        staging_command,
        staging_task,
        shutdown,
        state,
        #[cfg(test)]
        None,
    )
    .await
}

/// Supervises the exact post-staging child activation boundary.  Keeping the
/// child as an input lets tests drive the production select with a real
/// cancellable child task while staging retains the actual listener lease.
#[allow(clippy::too_many_arguments)]
async fn supervise_live_successor_child_after_staging_with_child(
    child: HaNode,
    service_ready_rx: &mut tokio::sync::oneshot::Receiver<()>,
    service_listener: tokio::sync::oneshot::Sender<tokio::net::TcpListener>,
    staging_command: &tokio::sync::watch::Sender<StagingCommand>,
    staging_task: &mut AbortStagingServerOnDrop,
    mut shutdown: tokio::sync::watch::Receiver<ShutdownSignal>,
    state: tokio::sync::watch::Sender<HaNodeSnapshot>,
    #[cfg(test)] mut test_handoff_command: Option<StagingCommand>,
) -> Result<(), HaNodeError> {
    let mut child_state = child.state.clone();

    loop {
        let snapshot = child_state.borrow().clone();
        if let Some(primary) = snapshot.terminal_error {
            let token = live_successor_cleanup_token(
                &shutdown,
                tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT,
            );
            let deadline = token.deadline();
            let (child_cleanup, staging_cleanup) = tokio::join!(
                child.shutdown_with_token(token),
                stop_staging_server_before(
                    staging_command,
                    staging_task,
                    "successor staging service",
                    deadline,
                ),
            );
            return combine_live_successor_child_and_staging(
                Some(primary),
                child_cleanup,
                staging_cleanup,
            );
        }
        if snapshot.status == HaNodeStatus::Stopped {
            let token = live_successor_cleanup_token(
                &shutdown,
                tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT,
            );
            let deadline = token.deadline();
            let (child_cleanup, staging_cleanup) = tokio::join!(
                child.shutdown_with_token(token),
                stop_staging_server_before(
                    staging_command,
                    staging_task,
                    "successor staging service",
                    deadline,
                ),
            );
            return finish_cancelled_live_successor(
                &state,
                combine_live_successor_child_and_staging(None, child_cleanup, staging_cleanup),
            );
        }

        tokio::select! {
            biased;
            result = &mut **staging_task => {
                // The staging task is still the sole listener owner during
                // child preparation. A normal return, error, or panic is
                // terminal immediately; do not wait for child readiness.
                let primary = unexpected_staging_server_exit(result);
                let token = live_successor_cleanup_token(
                    &shutdown,
                    tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT,
                );
                let cleanup = child
                    .shutdown_with_token(token)
                    .await;
                return combine_ha_results(Some(primary), cleanup);
            }
            changed = shutdown.changed() => {
                let _ = changed;
                let Some(token) = shutdown.borrow().clone() else {
                    let error = HaNodeError::ServiceServer(
                        "live successor shutdown signal closed without a token".into(),
                    );
                    publish_ha_failure(&state, error.clone());
                    return Err(error);
                };
                let deadline = token.deadline();
                publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
                let (child_cleanup, staging_cleanup) = tokio::join!(
                    child.shutdown_with_token(token),
                    stop_staging_server_before(
                        staging_command,
                        staging_task,
                        "successor staging service",
                        deadline,
                    ),
                );
                return finish_cancelled_live_successor(
                    &state,
                    combine_ha_errors(
                        child_cleanup
                            .err()
                            .into_iter()
                            .chain(staging_cleanup.err())
                            .collect(),
                    ),
                );
            }
            ready = &mut *service_ready_rx => {
                if ready.is_err() {
                    // A child terminal/stopped publication and closure of its
                    // ready sender can become visible in the same turn. The
                    // retained child snapshot is authoritative; the generic
                    // channel error is only a fallback when no terminal state
                    // explains the closure.
                    let snapshot = child_state.borrow().clone();
                    let stopped = snapshot.status == HaNodeStatus::Stopped;
                    let primary = snapshot.terminal_error.or_else(|| {
                        (!stopped).then(|| {
                            HaNodeError::ServiceServer(
                                "live successor child stopped before service activation".into(),
                            )
                        })
                    });
                    let token = live_successor_cleanup_token(
                        &shutdown,
                        tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT,
                    );
                    let deadline = token.deadline();
                    let (child_cleanup, staging_cleanup) = tokio::join!(
                        child.shutdown_with_token(token),
                        stop_staging_server_before(
                            staging_command,
                            staging_task,
                            "successor staging service",
                            deadline,
                        ),
                    );
                    let has_primary = primary.is_some();
                    let result = combine_live_successor_child_and_staging(
                        primary,
                        child_cleanup,
                        staging_cleanup,
                    );
                    return if has_primary {
                        result
                    } else {
                        finish_cancelled_live_successor(&state, result)
                    };
                }
                let deadline = tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT;
                enum HandoffWait {
                    Result(Result<ListenerLease, HaNodeError>),
                    Shutdown(Option<Arc<ShutdownToken>>),
                }
                let handoff = {
                    #[cfg(test)]
                    let handoff = async {
                        match test_handoff_command.take() {
                            Some(command) => {
                                handoff_staging_listener_with_command(
                                    staging_command,
                                    staging_task,
                                    deadline,
                                    command,
                                )
                                .await
                            }
                            None => {
                                handoff_staging_listener(staging_command, staging_task, deadline)
                                    .await
                            }
                        }
                    };
                    #[cfg(not(test))]
                    let handoff = handoff_staging_listener(staging_command, staging_task, deadline);
                    tokio::pin!(handoff);
                    tokio::select! {
                        biased;
                        changed = shutdown.changed() => {
                            let _ = changed;
                            HandoffWait::Shutdown(shutdown.borrow().clone())
                        }
                        result = &mut handoff => HandoffWait::Result(result),
                    }
                };
                let listener = match handoff {
                    HandoffWait::Shutdown(Some(token)) => {
                        // The handoff future has been dropped, so this is no
                        // longer borrowing the task. Abort and reap the sole
                        // staging owner within the caller's D; do not leave a
                        // detached FD owner behind or wait after that D.
                        let deadline = token.deadline();
                        let (child_cleanup, staging_cleanup) = tokio::join!(
                            child.shutdown_with_token(token),
                            abort_staging_server_before(staging_task, deadline),
                        );
                        let cleanup = combine_ha_errors(
                            child_cleanup
                                .err()
                                .into_iter()
                                .chain(staging_cleanup.err())
                                .collect(),
                        );
                        return finish_cancelled_live_successor(&state, cleanup);
                    }
                    HandoffWait::Shutdown(None) => {
                        let primary = HaNodeError::ServiceServer(
                            "live successor shutdown signal closed during listener handoff".into(),
                        );
                        let token = live_successor_cleanup_token(&shutdown, deadline);
                        let deadline = token.deadline();
                        let (child_cleanup, staging_cleanup) = tokio::join!(
                            child.shutdown_with_token(token),
                            abort_staging_server_before(staging_task, deadline),
                        );
                        let cleanup = combine_ha_errors(
                            child_cleanup
                                .err()
                                .into_iter()
                                .chain(staging_cleanup.err())
                                .collect(),
                        );
                        return combine_ha_results(Some(primary), cleanup);
                    }
                    HandoffWait::Result(Ok(listener)) => listener,
                    HandoffWait::Result(Err(primary)) => {
                        let token = live_successor_cleanup_token(&shutdown, deadline);
                        let child_cleanup = child.shutdown_with_token(token).await;
                        return combine_ha_results(Some(primary), child_cleanup);
                    }
                };
                if service_listener.send(listener.into_listener()).is_err() {
                    let primary = HaNodeError::ServiceServer(
                        "live successor child dropped the service listener handoff".into(),
                    );
                    let token = live_successor_cleanup_token(&shutdown, deadline);
                    let child_cleanup = child.shutdown_with_token(token).await;
                    return combine_ha_results(Some(primary), child_cleanup);
                }
                return supervise_live_successor_child(child, shutdown, state).await;
            }
            changed = child_state.changed() => {
                if changed.is_err() {
                    let primary = HaNodeError::ServiceServer(
                        "HA child supervisor state channel closed before service activation".into(),
                    );
                    let token = live_successor_cleanup_token(
                        &shutdown,
                        tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT,
                    );
                    let deadline = token.deadline();
                    let (child_cleanup, staging_cleanup) = tokio::join!(
                        child.shutdown_with_token(token),
                        stop_staging_server_before(
                            staging_command,
                            staging_task,
                            "successor staging service",
                            deadline,
                        ),
                    );
                    return combine_ha_results(
                        Some(primary),
                        combine_ha_errors(
                            child_cleanup
                                .err()
                                .into_iter()
                                .chain(staging_cleanup.err())
                                .collect(),
                        ),
                    );
                }
            }
        }
    }
}

async fn supervise_live_successor_child(
    child: HaNode,
    mut shutdown: tokio::sync::watch::Receiver<ShutdownSignal>,
    state: tokio::sync::watch::Sender<HaNodeSnapshot>,
) -> Result<(), HaNodeError> {
    let mut child_state = child.state.clone();
    loop {
        let snapshot = child_state.borrow().clone();
        publish_ha_state(
            &state,
            snapshot.status,
            snapshot.handle.clone(),
            snapshot.terminal_error.clone(),
        );
        if let Some(error) = snapshot.terminal_error {
            let token = live_successor_cleanup_token(
                &shutdown,
                tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT,
            );
            let result = normalize_live_successor_child_shutdown(
                Some(error),
                child.shutdown_with_token(token).await,
            );
            if let Err(error) = &result {
                publish_ha_failure(&state, error.clone());
            }
            return result;
        }
        if snapshot.status == HaNodeStatus::Stopped {
            let token = live_successor_cleanup_token(
                &shutdown,
                tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT,
            );
            let result = normalize_live_successor_child_shutdown(
                None,
                child.shutdown_with_token(token).await,
            );
            return match result {
                Ok(()) => {
                    publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
                    Ok(())
                }
                Err(error) => {
                    publish_ha_failure(&state, error.clone());
                    Err(error)
                }
            };
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
                let Some(token) = shutdown.borrow().clone() else {
                    return Err(HaNodeError::ServiceServer(
                        "live successor shutdown signal closed without a token".into(),
                    ));
                };
                let result = child.shutdown_with_token(token).await;
                return match result {
                    Ok(()) => {
                        publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
                        Ok(())
                    }
                    Err(error) => {
                        publish_ha_failure(&state, error.clone());
                        Err(error)
                    }
                };
            }
            changed = child_state.changed() => {
                if changed.is_err() {
                    let error = HaNodeError::ServiceServer(
                        "HA child supervisor state channel closed".into(),
                    );
                    let token = live_successor_cleanup_token(
                        &shutdown,
                        tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT,
                    );
                    let result = normalize_live_successor_child_shutdown(
                        Some(error),
                        child.shutdown_with_token(token).await,
                    );
                    if let Err(error) = &result {
                        publish_ha_failure(&state, error.clone());
                    }
                    return result;
                }
            }
        }
    }
}

fn completed_preparation_after_shutdown(
    result: Result<Result<PreparedHaStartup, HaStartupError>, tokio::task::JoinError>,
    startup: &StartupIoContext,
    token: Option<&Arc<ShutdownToken>>,
) -> Result<(), HaNodeError> {
    match result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) if is_requested_startup_cancellation(&error, startup, token) => Ok(()),
        Ok(Err(error)) => Err(HaNodeError::Startup(error)),
        Err(error) => Err(HaNodeError::Startup(fail(format!(
            "HA startup preparation task failed during shutdown: {error}"
        )))),
    }
}

#[allow(clippy::too_many_arguments)]
async fn supervise_ha_node(
    startup_config: HaStartupConfig,
    serve: HaServeConfig,
    startup_io: StartupIoContext,
    shutdown: tokio::sync::watch::Sender<ShutdownSignal>,
    mut shutdown_rx: tokio::sync::watch::Receiver<ShutdownSignal>,
    recorder_shutdown: tokio::sync::watch::Sender<bool>,
    recorder_shutdown_rx: tokio::sync::watch::Receiver<bool>,
    state: tokio::sync::watch::Sender<HaNodeSnapshot>,
) -> Result<(), HaNodeError> {
    #[cfg(feature = "test-hooks")]
    let service_activation_gate = startup_config.service_activation_gate.clone();
    let preparation_startup = startup_io.clone();
    let mut preparation =
        AbortOnDropTask::spawn(async move { startup_config.prepare(&preparation_startup).await });
    let prepared = loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || shutdown_rx.borrow().is_some() {
                    let token = shutdown_rx.borrow().clone();
                    let deadline = shutdown_deadline(&token);
                    if preparation.is_finished() {
                        let result = completed_preparation_after_shutdown(
                            preparation.await,
                            &startup_io,
                            token.as_ref(),
                        );
                        match &result {
                            Ok(()) => publish_ha_state(&state, HaNodeStatus::Stopped, None, None),
                            Err(error) => publish_ha_failure(&state, error.clone()),
                        }
                        return result;
                    }
                    if let Some(token) = token.as_ref() {
                        cancel_startup_for_token(&startup_io, token);
                    } else {
                        startup_io.cancel(deadline.into_std());
                    }
                    match await_task_before(&mut preparation, deadline).await {
                        Some(result) => {
                            let result = completed_preparation_after_shutdown(
                                result,
                                &startup_io,
                                token.as_ref(),
                            );
                            match &result {
                                Ok(()) => publish_ha_state(&state, HaNodeStatus::Stopped, None, None),
                                Err(error) => publish_ha_failure(&state, error.clone()),
                            }
                            return result;
                        }
                        None => {
                            let stage = startup_io.unfinished_stage().to_owned();
                            let error = HaNodeError::StartupIoDeadlineExceeded { stage };
                            publish_ha_failure(&state, error.clone());
                            return Err(error);
                        }
                    }
                }
            }
            result = &mut preparation => {
                break match result {
                    Ok(Ok(prepared)) => prepared,
                    Ok(Err(error)) => {
                        let error = HaNodeError::Startup(error);
                        publish_ha_failure(&state, error.clone());
                        return Err(error);
                    }
                    Err(error) => {
                        let error = HaNodeError::Startup(fail(format!(
                            "HA startup preparation task failed: {error}"
                        )));
                        publish_ha_failure(&state, error.clone());
                        return Err(error);
                    }
                };
            }
        }
    };

    let HaServeConfig {
        recorder_listener,
        service_listener,
        recorder_transport,
        recorders,
        log_peers,
        admin,
        tail_token,
        #[cfg(test)]
        recorder_start_error,
        #[cfg(test)]
        recorder_shutdown_outcome,
        #[cfg(test)]
        open_shutdown_token_observer,
    } = serve;
    if let Some(token) = shutdown_rx.borrow().clone() {
        let deadline = token.deadline();
        cancel_startup_for_token(&startup_io, &token);
        if tokio::time::Instant::now() >= deadline {
            let error = HaNodeError::StartupIoDeadlineExceeded {
                stage: startup_io.unfinished_stage().to_owned(),
            };
            publish_ha_failure(&state, error.clone());
            return Err(error);
        }
        publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
        return Ok(());
    }
    let peers = prepared.config.node_config.peers().to_vec();
    let recovery_generation = prepared.config.node_config.recovery_generation();
    let recorder = prepared.recorder_hook.clone();
    let (recorder_started, recorder_started_rx) = tokio::sync::oneshot::channel();
    let recorder_task = {
        let shutdown_guard = shutdown_rx.borrow();
        if let Some(token) = shutdown_guard.clone() {
            cancel_startup_for_token(&startup_io, &token);
            publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
            return Ok(());
        }
        spawn_ha_recorder_server_inner(
            recorder_listener,
            recorder,
            recorder_transport,
            peers,
            recovery_generation,
            recorder_shutdown_rx,
            recorder_started,
            #[cfg(test)]
            recorder_start_error,
            #[cfg(test)]
            recorder_shutdown_outcome,
        )
    };
    let mut recorder_task = recorder_task;
    let mut recorder_started_rx = recorder_started_rx;
    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || shutdown_rx.borrow().is_some() {
                    let token = shutdown_rx.borrow().clone();
                    let deadline = shutdown_deadline(&token);
                    if let Some(token) = token.as_ref() {
                        cancel_startup_for_token(&startup_io, token);
                    } else {
                        startup_io.cancel(deadline.into_std());
                    }
                    publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
                    let cleanup = stop_recorder_server_before(
                        &recorder_shutdown,
                        &mut recorder_task,
                        deadline,
                    )
                    .await;
                    return match cleanup {
                        Ok(()) => {
                            publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
                            Ok(())
                        }
                        Err(error) => {
                            publish_ha_failure(&state, error.clone());
                            Err(error)
                        }
                    };
                }
            }
            result = &mut recorder_task => {
                let error = unexpected_server_exit(result, "recorder server");
                publish_ha_failure(&state, error.clone());
                return Err(error);
            }
            result = &mut recorder_started_rx => {
                if result.is_ok() {
                    break;
                }
                let error = unexpected_server_exit((&mut recorder_task).await, "recorder server");
                publish_ha_failure(&state, error.clone());
                return Err(error);
            }
        }
    }

    supervise_prepared_ha_node(
        prepared,
        service_listener,
        recorders,
        log_peers,
        admin,
        tail_token,
        startup_io,
        shutdown,
        shutdown_rx,
        recorder_shutdown,
        recorder_task,
        #[cfg(feature = "test-hooks")]
        service_activation_gate,
        #[cfg(test)]
        open_shutdown_token_observer,
        state,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn supervise_prepared_ha_node(
    prepared: PreparedHaStartup,
    service_listener: ServiceListener,
    recorders: Vec<(String, Box<dyn RecorderRpc>)>,
    log_peers: Vec<Arc<dyn LogPeer>>,
    admin: Option<AdminConfig>,
    tail_token: Option<String>,
    startup_io: StartupIoContext,
    shutdown: tokio::sync::watch::Sender<ShutdownSignal>,
    mut shutdown_rx: tokio::sync::watch::Receiver<ShutdownSignal>,
    recorder_shutdown: tokio::sync::watch::Sender<bool>,
    mut recorder_task: RecorderServerTask,
    #[cfg(feature = "test-hooks")] service_activation_gate: Option<HaServiceActivationGate>,
    #[cfg(test)] open_shutdown_token_observer: Option<Arc<TestCleanupTokenObserver>>,
    state: tokio::sync::watch::Sender<HaNodeSnapshot>,
) -> Result<(), HaNodeError> {
    let opened = {
        let startup = prepared.open_cancellable(
            recorders,
            log_peers,
            startup_io.clone(),
            shutdown_rx.clone(),
        );
        tokio::pin!(startup);
        tokio::select! {
            result = &mut startup => match result {
                Ok(opened) => opened,
                Err(HaOpenError::Cancelled { deadline, cleanup: startup_cleanup })
                    if shutdown_rx.borrow().is_some() =>
                {
                    publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
                    let recorder_cleanup = stop_recorder_server_before(
                        &recorder_shutdown,
                        &mut recorder_task,
                        deadline,
                    )
                    .await;
                    let cleanup = match startup_cleanup {
                        Ok(()) => recorder_cleanup,
                        Err(primary) => combine_ha_results(Some(primary), recorder_cleanup),
                    };
                    if let Err(error) = cleanup {
                        publish_ha_failure(&state, error.clone());
                        return Err(error);
                    }
                    publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
                    return Ok(());
                }
                Err(HaOpenError::Startup { error, cleanup: startup_cleanup }) => {
                    let error = HaNodeError::Startup(error);
                    publish_ha_failure(&state, error.clone());
                    let recorder_cleanup = stop_recorder_server(
                        &recorder_shutdown,
                        &mut recorder_task,
                    )
                    .await;
                    let cleanup = combine_ha_errors(
                        startup_cleanup
                            .err()
                            .into_iter()
                            .chain(recorder_cleanup.err())
                            .collect(),
                    );
                    return combine_ha_results(Some(error), cleanup);
                }
                Err(HaOpenError::Cancelled { cleanup, .. }) => {
                    let error = HaNodeError::Cancelled;
                    publish_ha_failure(&state, error.clone());
                    return combine_ha_results(Some(error), cleanup);
                }
            },
            result = &mut recorder_task => {
                let completed_recorder_evidence = recorder_task.completed_shutdown_evidence();
                let requested_shutdown = shutdown_rx.borrow().clone();
                if let Some(token) = requested_shutdown {
                    let deadline = token.deadline();
                    #[cfg(test)]
                    if let Some(observer) = open_shutdown_token_observer.as_ref() {
                        observer.observe(&token);
                    }
                    // Once the retained shutdown request has also reached the
                    // recorder, a normal recorder return is the expected
                    // cooperative completion. Errors and panics remain the
                    // authoritative recorder primary.
                    let primary = match result {
                        Ok(Ok(())) => None,
                        result => Some(unexpected_server_exit(result, "recorder server")),
                    };
                    publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
                    let cleanup = match tokio::time::timeout_at(deadline, &mut startup).await {
                        Ok(Ok(opened)) => {
                            let mut scope = HaTaskScope::new(
                                AdminDrainLease::Disabled,
                                ServiceTaskUnstarted::DeferredOwnerUnknown,
                            );
                            shutdown_opened_ha_startup_before(
                                opened,
                                service_listener,
                                &mut scope,
                                &recorder_shutdown,
                                &mut recorder_task,
                                false,
                                completed_recorder_evidence,
                                deadline,
                            )
                            .await
                        }
                        Ok(Err(HaOpenError::Cancelled { cleanup, .. })) => cleanup,
                        Ok(Err(HaOpenError::Startup {
                            error: startup_error,
                            cleanup,
                        })) => combine_ha_results(
                            Some(HaNodeError::Startup(startup_error)),
                            cleanup,
                        ),
                        Err(_) => Err(HaNodeError::StartupIoDeadlineExceeded {
                            stage: startup_io.unfinished_stage().to_owned(),
                        }),
                    };
                    let result = combine_ha_results(primary, cleanup);
                    match &result {
                        Ok(()) => {
                            publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
                        }
                        Err(error) => publish_ha_failure(&state, error.clone()),
                    }
                    return result;
                }
                let error = unexpected_server_exit(result, "recorder server");
                let token = request_ha_shutdown(
                    &shutdown,
                    Arc::new(ShutdownToken::new_internal(HA_SERVER_SHUTDOWN_TIMEOUT)),
                    || {},
                );
                let deadline = token.deadline();
                cancel_startup_for_token(&startup_io, &token);
                publish_ha_failure(&state, error.clone());
                let cleanup = match tokio::time::timeout_at(deadline, &mut startup).await {
                    Ok(Ok(opened)) => {
                        let mut scope = HaTaskScope::new(
                            AdminDrainLease::Disabled,
                            ServiceTaskUnstarted::DeferredOwnerUnknown,
                        );
                        shutdown_opened_ha_startup_before(
                            opened,
                            service_listener,
                            &mut scope,
                            &recorder_shutdown,
                            &mut recorder_task,
                            false,
                            completed_recorder_evidence,
                            deadline,
                        )
                        .await
                    }
                    Ok(Err(HaOpenError::Cancelled { cleanup, .. })) => cleanup,
                    Ok(Err(HaOpenError::Startup {
                        error: startup_error,
                        cleanup,
                    })) => {
                        combine_ha_results(Some(HaNodeError::Startup(startup_error)), cleanup)
                    }
                    Err(_) => {
                        let stage = startup_io.unfinished_stage().to_owned();
                        combine_ha_results(
                            Some(HaNodeError::StartupIoDeadlineExceeded { stage }),
                            Ok(()),
                        )
                    }
                };
                return combine_ha_results(Some(error), cleanup);
            }
        }
    };

    let runtime = opened.runtime();
    let coordinator = opened.coordinator();
    let recorder = opened.local_recorder();
    let recorder_hook = opened.recorder_hook.clone();
    let auto_activate_stop = opened.auto_activate_stop;
    let successor_stop = opened.successor_stop;
    let target_config_id = opened.target_config_id;
    let router = match admin {
        Some(admin) => node_router_with_checkpoint_and_admin_tasks(
            runtime.clone(),
            recorder,
            coordinator.clone(),
            admin,
        )
        .map(|(router, tasks)| (router, Some(tasks)))
        .map_err(|error| HaNodeError::Startup(fail(error.to_string()))),
        None => Ok((
            node_router_with_checkpoint(runtime.clone(), recorder_hook, coordinator.clone()),
            None,
        )),
    };
    let router = router.and_then(|(mut router, admin_tasks)| {
        if let Some(tail_token) = tail_token {
            router = router.merge(
                opened
                    .certified_tail_router(tail_token)
                    .map_err(HaNodeError::Startup)?,
            );
        }
        Ok((router, admin_tasks))
    });
    let (router, admin_tasks) = match router {
        Ok(router) => router,
        Err(error) => {
            publish_ha_failure(&state, error.clone());
            let mut scope = HaTaskScope::new(
                AdminDrainLease::Disabled,
                ServiceTaskUnstarted::DeferredOwnerUnknown,
            );
            let cleanup = shutdown_opened_ha_startup(
                opened,
                service_listener,
                &mut scope,
                &recorder_shutdown,
                &mut recorder_task,
                true,
                None,
            )
            .await;
            return combine_ha_results(Some(error), cleanup);
        }
    };
    let mut scope = HaTaskScope::new(
        AdminDrainLease::from_tracker(admin_tasks),
        ServiceTaskUnstarted::DeferredOwnerUnknown,
    );
    let startup_shutdown_deadline = shutdown_rx.borrow().clone();
    if let Some(token) = startup_shutdown_deadline {
        let deadline = token.deadline();
        publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
        let cleanup = shutdown_opened_ha_startup_before(
            opened,
            service_listener,
            &mut scope,
            &recorder_shutdown,
            &mut recorder_task,
            true,
            None,
            deadline,
        )
        .await;
        return match cleanup {
            Ok(()) => {
                publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
                Ok(())
            }
            Err(cleanup) => {
                let error = HaNodeError::Cleanup {
                    primary: Box::new(HaNodeError::Cancelled),
                    cleanup: Box::new(cleanup),
                };
                publish_ha_failure(&state, error.clone());
                Err(error)
            }
        };
    }
    let owner = opened.into_rhiza();
    let handle = owner.handle();
    let (service_started, service_started_rx) = tokio::sync::oneshot::channel();
    #[cfg(feature = "test-hooks")]
    {
        if let Some(gate) = service_activation_gate {
            if shutdown_rx.borrow().is_none() {
                let wait = gate.wait();
                tokio::pin!(wait);
                tokio::select! {
                    biased;
                    changed = shutdown_rx.changed() => {
                        let _ = changed;
                    }
                    () = &mut wait => {}
                }
            }
        }
    }
    let activation = activate_service_listener(service_listener, &mut shutdown_rx).await;
    let pre_service_deadline = match activation {
        Ok(ServiceActivation::Listener(listener)) => {
            let service_shutdown = scope.service_shutdown_receiver();
            scope.start_service(spawn_ha_service_server(
                listener,
                router.clone(),
                service_shutdown,
                service_started,
            ));
            None
        }
        Ok(ServiceActivation::Shutdown {
            deadline,
            listener_closed,
        }) => {
            scope.set_unstarted_service(if listener_closed {
                ServiceTaskUnstarted::OwnedListenerDropped
            } else {
                ServiceTaskUnstarted::DeferredOwnerUnknown
            });
            Some(deadline)
        }
        Err(error) => {
            publish_ha_failure(&state, error.clone());
            scope.set_unstarted_service(ServiceTaskUnstarted::DeferredOwnerUnknown);
            let cleanup = shutdown_ha_runtime(
                owner,
                runtime,
                &mut scope,
                &recorder_shutdown,
                &mut recorder_task,
                true,
                None,
            )
            .await;
            return combine_ha_results(Some(error), cleanup);
        }
    };
    if let Some(deadline) = pre_service_deadline {
        publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
        let cleanup = shutdown_ha_runtime_before(
            owner,
            runtime,
            &mut scope,
            &recorder_shutdown,
            &mut recorder_task,
            true,
            None,
            deadline,
        )
        .await;
        return match cleanup {
            Ok(()) => {
                publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
                Ok(())
            }
            Err(cleanup) => {
                let error = HaNodeError::Cleanup {
                    primary: Box::new(HaNodeError::Cancelled),
                    cleanup: Box::new(cleanup),
                };
                publish_ha_failure(&state, error.clone());
                Err(error)
            }
        };
    }
    let mut service_start_shutdown = shutdown_rx.clone();
    match wait_for_service_start_or_shutdown(service_started_rx, &mut service_start_shutdown).await
    {
        Ok(ServiceStartup::Started) => {}
        Ok(ServiceStartup::Shutdown(deadline)) => {
            publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
            let cleanup = shutdown_ha_runtime_before(
                owner,
                runtime,
                &mut scope,
                &recorder_shutdown,
                &mut recorder_task,
                true,
                None,
                deadline,
            )
            .await;
            return match cleanup {
                Ok(()) => {
                    publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
                    Ok(())
                }
                Err(cleanup) => {
                    let error = HaNodeError::Cleanup {
                        primary: Box::new(HaNodeError::Cancelled),
                        cleanup: Box::new(cleanup),
                    };
                    publish_ha_failure(&state, error.clone());
                    Err(error)
                }
            };
        }
        Err(error) => {
            publish_ha_failure(&state, error.clone());
            let cleanup = shutdown_ha_runtime(
                owner,
                runtime,
                &mut scope,
                &recorder_shutdown,
                &mut recorder_task,
                true,
                None,
            )
            .await;
            return combine_ha_results(Some(error), cleanup);
        }
    }
    let post_service_start_deadline = shutdown_rx.borrow().clone();
    if let Some(token) = post_service_start_deadline {
        let deadline = token.deadline();
        publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
        let cleanup = shutdown_ha_runtime_before(
            owner,
            runtime,
            &mut scope,
            &recorder_shutdown,
            &mut recorder_task,
            true,
            None,
            deadline,
        )
        .await;
        return match cleanup {
            Ok(()) => {
                publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
                Ok(())
            }
            Err(cleanup) => {
                let error = HaNodeError::Cleanup {
                    primary: Box::new(HaNodeError::Cancelled),
                    cleanup: Box::new(cleanup),
                };
                publish_ha_failure(&state, error.clone());
                Err(error)
            }
        };
    }
    let initial_readiness = update_ha_readiness(
        &state,
        &handle,
        &runtime,
        &coordinator,
        auto_activate_stop,
        successor_stop,
        target_config_id,
        &startup_io,
        &shutdown_rx,
    )
    .await;

    let mut shutdown_rx = shutdown_rx;
    let mut status_tick = tokio::time::interval(HA_STATUS_POLL_INTERVAL);
    status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut recorder_running = true;
    let mut completed_recorder_evidence = None;
    let terminal = if let Err(error) = initial_readiness {
        Some(error)
    } else {
        let worker_failure = owner.wait_for_worker_failure();
        tokio::pin!(worker_failure);
        loop {
            tokio::select! {
                biased;
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || shutdown_rx.borrow().is_some() {
                        break None;
                    }
                }
                result = &mut recorder_task => {
                    recorder_running = false;
                    completed_recorder_evidence = recorder_task.completed_shutdown_evidence();
                    break Some(unexpected_server_exit(result, "recorder server"));
                }
                result = scope.running_service_task().expect("service task is running after startup") => {
                    let result = scope.complete_running_service_task(result);
                    break Some(unexpected_service_task_exit(result));
                }
                failure = &mut worker_failure => {
                    break Some(match failure {
                        Some(classification) => HaNodeError::WorkerFailure(classification),
                        None => HaNodeError::ServiceServer(
                            "HA workers stopped before node shutdown".into()
                        ),
                    });
                }
                _ = status_tick.tick() => {
                    if let Err(error) = update_ha_readiness(
                        &state,
                        &handle,
                        &runtime,
                        &coordinator,
                        auto_activate_stop,
                        successor_stop,
                        target_config_id,
                        &startup_io,
                        &shutdown_rx,
                    )
                    .await
                    {
                        break Some(error);
                    }
                }
            }
        }
    };

    let token = if let Some(error) = &terminal {
        let token = request_ha_shutdown(
            &shutdown,
            Arc::new(ShutdownToken::new_internal(HA_SERVER_SHUTDOWN_TIMEOUT)),
            || handle.close_admission(),
        );
        publish_ha_failure(&state, error.clone());
        token
    } else {
        publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
        shutdown_rx.borrow().clone().unwrap_or_else(|| {
            request_ha_shutdown(
                &shutdown,
                Arc::new(ShutdownToken::new_internal(HA_SERVER_SHUTDOWN_TIMEOUT)),
                || handle.close_admission(),
            )
        })
    };
    let deadline = token.deadline();
    cancel_startup_for_token(&startup_io, &token);
    let cleanup = shutdown_ha_runtime_before(
        owner,
        runtime,
        &mut scope,
        &recorder_shutdown,
        &mut recorder_task,
        recorder_running,
        completed_recorder_evidence,
        deadline,
    )
    .await;
    let result = combine_ha_results(terminal, cleanup);
    match result {
        Ok(()) => {
            publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
            Ok(())
        }
        Err(error) => {
            publish_ha_failure(&state, error.clone());
            Err(error)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServiceStartup {
    Started,
    Shutdown(tokio::time::Instant),
}

async fn wait_for_service_start_or_shutdown(
    mut started: tokio::sync::oneshot::Receiver<()>,
    shutdown: &mut tokio::sync::watch::Receiver<ShutdownSignal>,
) -> Result<ServiceStartup, HaNodeError> {
    loop {
        if let Some(token) = shutdown.borrow().clone() {
            let deadline = token.deadline();
            return Ok(ServiceStartup::Shutdown(deadline));
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || shutdown.borrow().is_some() {
                    let deadline = shutdown_deadline(&shutdown.borrow());
                    return Ok(ServiceStartup::Shutdown(deadline));
                }
            }
            result = &mut started => {
                return result
                    .map(|()| ServiceStartup::Started)
                    .map_err(|_| HaNodeError::ServiceServer(
                        "service ingress stopped before reporting startup".into()
                    ));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn update_ha_readiness(
    state: &tokio::sync::watch::Sender<HaNodeSnapshot>,
    handle: &RhizaHandle,
    runtime: &Arc<NodeRuntime>,
    coordinator: &CheckpointCoordinator,
    auto_activate_stop: Option<LogAnchor>,
    successor_stop: Option<LogAnchor>,
    target_config_id: u64,
    startup: &StartupIoContext,
    shutdown: &tokio::sync::watch::Receiver<ShutdownSignal>,
) -> Result<(), HaNodeError> {
    if let Err(error) = startup.check("activation status") {
        if let (
            Some(token),
            NodeError::StartupCancelled {
                token: cancellation,
                ..
            },
        ) = (shutdown.borrow().clone(), &error)
        {
            if token.startup_token() == cancellation && startup.is_cancelled_by(cancellation) {
                return Ok(());
            }
        }
        return Err(HaNodeError::Startup(startup_error(error)));
    }
    let attempt_runtime = Arc::clone(runtime);
    let mut attempt = AbortOnDropTask::spawn_blocking(move || {
        let status = attempt_runtime.status()?;
        let Some(expected_stop) = auto_activate_stop else {
            return Ok(status);
        };
        if status.configuration_state.is_active() {
            return Ok(status);
        }
        if status.configuration_state.stop().copied() != Some(expected_stop) {
            return Err(NodeError::PreconditionFailed(
                "live successor activation Stop anchor changed".into(),
            ));
        }
        match attempt_runtime.activate_successor_if(target_config_id) {
            Ok(_) => attempt_runtime.status(),
            Err(activation_error) => {
                let current = attempt_runtime.status()?;
                if current.configuration_state.is_active()
                    || activation_error.classification().retryable()
                {
                    Ok(current)
                } else {
                    Err(activation_error)
                }
            }
        }
    });
    let status = loop {
        let mut shutdown = shutdown.clone();
        tokio::select! {
            biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || shutdown.borrow().is_some() {
                        let token = shutdown.borrow().clone();
                        let deadline = shutdown_deadline(&token);
                        if attempt.is_finished() {
                            return completed_activation_after_shutdown(
                                attempt.await,
                                startup,
                                token.as_ref(),
                            );
                        }
                        if let Some(token) = token.as_ref() {
                            cancel_startup_for_token(startup, token);
                        } else {
                            startup.cancel(deadline.into_std());
                        }
                        match await_task_before(&mut attempt, deadline).await {
                            Some(result) => return completed_activation_after_shutdown(
                                result,
                                startup,
                                token.as_ref(),
                            ),
                        None => {
                            let stage = startup.unfinished_stage().to_owned();
                            runtime.cancel_operations();
                            return Err(HaNodeError::StartupIoDeadlineExceeded { stage });
                        }
                    }
                }
            }
            result = &mut attempt => {
                break result.map_err(|error| {
                    HaNodeError::Startup(fail(format!("activation status task failed: {error}")))
                })?;
            }
        }
    };
    if status
        .as_ref()
        .is_ok_and(|status| status.configuration_state.is_active())
        && coordinator.successor_checkpoint_baseline_required()
    {
        let predecessor_stop = successor_stop.ok_or_else(|| {
            HaNodeError::Startup(fail(
                "successor checkpoint baseline is missing its predecessor Stop capability",
            ))
        })?;
        let baseline =
            coordinator.establish_successor_checkpoint_baseline(runtime, predecessor_stop);
        tokio::pin!(baseline);
        let mut baseline_shutdown = shutdown.clone();
        let result = tokio::select! {
            biased;
            changed = baseline_shutdown.changed() => {
                if changed.is_err() || baseline_shutdown.borrow().is_some() {
                    return Ok(());
                }
                return Ok(());
            }
            result = &mut baseline => result,
        };
        if let Err(error) = result {
            if matches!(
                error,
                DurabilityError::Unavailable
                    | DurabilityError::Archive(
                        rhiza_archive::Error::ObjectStore(_)
                            | rhiza_archive::Error::CompareAndSwapRetriesExhausted { .. }
                            | rhiza_archive::Error::GcBarrierActive { .. }
                            | rhiza_archive::Error::GcBarrierBusy { .. }
                    )
            ) {
                publish_ha_state(state, HaNodeStatus::Degraded, Some(handle.clone()), None);
                return Ok(());
            }
            return Err(HaNodeError::Startup(fail(error.to_string())));
        }
    }
    let phase = match status {
        Ok(status) if !status.configuration_state.is_active() => HaNodeStatus::AwaitingActivation,
        Ok(status) if status.ready && coordinator.write_allowed().is_ok() => HaNodeStatus::Ready,
        Ok(_) => HaNodeStatus::Degraded,
        Err(error) if auto_activate_stop.is_none() || error.classification().retryable() => {
            HaNodeStatus::Degraded
        }
        Err(error) => return Err(HaNodeError::Startup(fail(error.to_string()))),
    };
    let shutdown_guard = shutdown.borrow();
    if shutdown_guard.is_some() {
        return Ok(());
    }
    publish_ha_state(state, phase, Some(handle.clone()), None);
    drop(shutdown_guard);
    Ok(())
}

fn completed_activation_after_shutdown(
    result: Result<Result<NodeStatus, NodeError>, tokio::task::JoinError>,
    startup: &StartupIoContext,
    token: Option<&Arc<ShutdownToken>>,
) -> Result<(), HaNodeError> {
    match result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) if is_requested_node_cancellation(&error, startup, token) => Ok(()),
        Ok(Err(error)) => Err(HaNodeError::Startup(startup_error(error))),
        Err(error) => Err(HaNodeError::Startup(fail(format!(
            "activation status task failed during shutdown: {error}"
        )))),
    }
}

fn publish_ha_state(
    state: &tokio::sync::watch::Sender<HaNodeSnapshot>,
    status: HaNodeStatus,
    handle: Option<RhizaHandle>,
    terminal_error: Option<HaNodeError>,
) {
    state.send_replace(HaNodeSnapshot {
        status,
        handle,
        terminal_error,
    });
}

fn publish_ha_failure(state: &tokio::sync::watch::Sender<HaNodeSnapshot>, error: HaNodeError) {
    publish_ha_state(state, HaNodeStatus::Failed, None, Some(error));
}

fn unexpected_server_exit(
    result: Result<Result<(), HaNodeError>, tokio::task::JoinError>,
    name: &str,
) -> HaNodeError {
    match result {
        Ok(Ok(())) => match name {
            "recorder server" => {
                HaNodeError::RecorderServer("recorder server stopped unexpectedly".into())
            }
            _ => HaNodeError::ServiceServer("service server stopped unexpectedly".into()),
        },
        Ok(Err(error)) => error,
        Err(error) => match name {
            "recorder server" => {
                HaNodeError::RecorderServer(format!("{name} task failed: {error}"))
            }
            _ => HaNodeError::ServiceServer(format!("{name} task failed: {error}")),
        },
    }
}

fn unexpected_service_task_exit(result: Result<(), HaNodeError>) -> HaNodeError {
    match result {
        Ok(()) => HaNodeError::ServiceServer("service server stopped unexpectedly".into()),
        Err(error) => error,
    }
}

fn unexpected_staging_server_exit(
    result: Result<Result<StagingExit, HaNodeError>, tokio::task::JoinError>,
) -> HaNodeError {
    match result {
        Ok(Ok(StagingExit::Closed)) => {
            HaNodeError::ServiceServer("successor staging service stopped unexpectedly".into())
        }
        Ok(Ok(StagingExit::Handoff(_))) => {
            HaNodeError::ServiceServer("successor staging service handed off unexpectedly".into())
        }
        Ok(Err(error)) => error,
        Err(error) => server_task_join_error("successor staging service", error),
    }
}

fn server_task_join_error(name: &str, error: tokio::task::JoinError) -> HaNodeError {
    match name {
        "recorder server" => HaNodeError::RecorderServer(format!("{name} task failed: {error}")),
        _ => HaNodeError::ServiceServer(format!("{name} task failed: {error}")),
    }
}

async fn shutdown_opened_ha_startup(
    opened: HaOpenNode,
    service_listener: ServiceListener,
    scope: &mut HaTaskScope,
    recorder_shutdown: &tokio::sync::watch::Sender<bool>,
    recorder_task: &mut RecorderServerTask,
    recorder_running: bool,
    completed_recorder_evidence: Option<ShutdownEvidence>,
) -> Result<(), HaNodeError> {
    let deadline = tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT;
    shutdown_opened_ha_startup_before(
        opened,
        service_listener,
        scope,
        recorder_shutdown,
        recorder_task,
        recorder_running,
        completed_recorder_evidence,
        deadline,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn shutdown_opened_ha_startup_before(
    opened: HaOpenNode,
    service_listener: ServiceListener,
    scope: &mut HaTaskScope,
    recorder_shutdown: &tokio::sync::watch::Sender<bool>,
    recorder_task: &mut RecorderServerTask,
    recorder_running: bool,
    completed_recorder_evidence: Option<ShutdownEvidence>,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    // A normal child owns its unstarted listener; a deferred live successor
    // does not.  Keep the latter conservative until its staging owner closes
    // the actual FD.
    let service = match service_listener {
        ServiceListener::Bound(listener) => {
            drop(listener);
            ServiceTaskUnstarted::OwnedListenerDropped
        }
        ServiceListener::Deferred { ready, listener } => {
            drop(ready);
            drop(listener);
            ServiceTaskUnstarted::DeferredOwnerUnknown
        }
    };
    let runtime = opened.runtime();
    let owner = opened.into_rhiza();
    scope.set_unstarted_service(service);
    shutdown_ha_runtime_before(
        owner,
        runtime,
        scope,
        recorder_shutdown,
        recorder_task,
        recorder_running,
        completed_recorder_evidence,
        deadline,
    )
    .await
}

async fn shutdown_ha_runtime(
    owner: Rhiza,
    runtime: Arc<NodeRuntime>,
    scope: &mut HaTaskScope,
    recorder_shutdown: &tokio::sync::watch::Sender<bool>,
    recorder_task: &mut RecorderServerTask,
    recorder_running: bool,
    completed_recorder_evidence: Option<ShutdownEvidence>,
) -> Result<(), HaNodeError> {
    let deadline = tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT;
    shutdown_ha_runtime_before(
        owner,
        runtime,
        scope,
        recorder_shutdown,
        recorder_task,
        recorder_running,
        completed_recorder_evidence,
        deadline,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn shutdown_ha_runtime_before(
    owner: Rhiza,
    runtime: Arc<NodeRuntime>,
    scope: &mut HaTaskScope,
    recorder_shutdown: &tokio::sync::watch::Sender<bool>,
    recorder_task: &mut RecorderServerTask,
    recorder_running: bool,
    completed_recorder_evidence: Option<ShutdownEvidence>,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    scope.begin_shutdown(runtime.as_ref());
    // Recorder has independent ownership, but its signal and waiter begin at
    // the same boundary as service/admin drain. A slow admin handler must not
    // consume the recorder's only chance to observe the common deadline.
    recorder_shutdown.send_replace(true);
    let scope_drain = scope.drain_before(deadline);
    let recorder_drain = async {
        let mut evidence = completed_recorder_evidence;
        let result = if recorder_running {
            let (result, observed) = wait_for_recorder_server(recorder_task, deadline).await;
            evidence = merge_optional_shutdown_evidence(evidence, observed);
            result
        } else {
            Ok(())
        };
        (result, evidence)
    };
    let (scope_shutdown, (recorder_result, recorder_evidence)) =
        tokio::join!(scope_drain, recorder_drain);
    let mut errors = Vec::new();
    if let Err(error) = scope_shutdown.result {
        errors.push(error);
    }
    if let Err(error) = recorder_result {
        errors.push(error);
    }
    if let Err(error) = owner.shutdown_with_deadline(deadline).await {
        errors.push(shutdown_owner_error(
            error,
            downgrade_tasks_for_owner_cleanup(merge_shutdown_evidence(
                scope_shutdown.evidence,
                recorder_evidence,
            )),
        ));
    }
    combine_ha_errors(errors)
}

async fn wait_for_tracked_axum_ingress(
    ingress: &mut TrackedAxumIngress,
    name: &str,
    deadline: tokio::time::Instant,
) -> (Result<(), HaNodeError>, IngressDisposition, TaskDisposition) {
    let inner_force_at = deadline
        .checked_sub(Duration::from_millis(100))
        .unwrap_or_else(tokio::time::Instant::now);
    let outer_force_at = deadline
        .checked_sub(HA_SERVER_ABORT_RECEIPT_RESERVE)
        .unwrap_or_else(tokio::time::Instant::now);
    // `poll_listener_receipt` may already have consumed the oneshot while a
    // supervisor observed a completed service task.  Carry that durable bit
    // into this waiter; polling a completed oneshot receiver a second time
    // panics and must never be required to establish Closed evidence.
    let mut listener_dropped = ingress.listener_receipted;
    loop {
        tokio::select! {
            receipt = &mut ingress.listener_dropped, if !listener_dropped => {
                listener_dropped = receipt.is_ok();
                ingress.listener_receipted |= listener_dropped;
            }
            result = &mut ingress.task => {
                let result = match result {
                    Ok(result) => result,
                    Err(error) => Err(server_task_join_error(name, error)),
                };
                let tasks = if ingress.forced.load(Ordering::Acquire) || result.is_err() {
                    TaskDisposition::Uncertain
                } else {
                    TaskDisposition::Quiesced
                };
                if !listener_dropped {
                    listener_dropped = ingress.poll_listener_receipt();
                }
                return (
                    result,
                    if listener_dropped {
                        IngressDisposition::Closed
                    } else {
                        IngressDisposition::Uncertain
                    },
                    tasks,
                );
            }
            () = tokio::time::sleep_until(inner_force_at) => {
                ingress.forced.store(true, Ordering::Release);
                ingress.force.send_replace(true);
                break;
            }
        }
    }
    // The inner owner gets its earlier force point to abort/reap connections.
    // At D-50ms the outer owner itself is aborted; it is never left running
    // beyond the public deadline.
    let outer = tokio::time::sleep_until(outer_force_at);
    tokio::pin!(outer);
    loop {
        tokio::select! {
            receipt = &mut ingress.listener_dropped, if !listener_dropped => {
                listener_dropped = receipt.is_ok();
                ingress.listener_receipted |= listener_dropped;
            }
            result = &mut ingress.task => {
                let result = result
                    .map_err(|error| server_task_join_error(name, error))
                    .and_then(|result| result);
                return (
                    result,
                    if listener_dropped { IngressDisposition::Closed } else { IngressDisposition::Uncertain },
                    TaskDisposition::Uncertain,
                );
            }
            () = &mut outer => {
                ingress.task.abort();
                break;
            }
        }
    }
    match tokio::time::timeout_at(deadline, &mut ingress.task).await {
        Ok(Ok(result)) => {
            listener_dropped |= ingress.poll_listener_receipt();
            (
                result,
                if listener_dropped {
                    IngressDisposition::Closed
                } else {
                    IngressDisposition::Uncertain
                },
                if ingress.forced.load(Ordering::Acquire) {
                    TaskDisposition::Uncertain
                } else {
                    TaskDisposition::Quiesced
                },
            )
        }
        Ok(Err(error)) => (
            Err(server_task_join_error(name, error)),
            if listener_dropped {
                IngressDisposition::Closed
            } else {
                IngressDisposition::Uncertain
            },
            TaskDisposition::Uncertain,
        ),
        Err(_) => {
            ingress.task.abort();
            (
                Err(conservative_shutdown_deadline_error(
                    HaShutdownPhase::Service,
                    ShutdownEvidence {
                        ingress: if listener_dropped {
                            IngressDisposition::Closed
                        } else {
                            IngressDisposition::Uncertain
                        },
                        tasks: TaskDisposition::Uncertain,
                    },
                )),
                if listener_dropped {
                    IngressDisposition::Closed
                } else {
                    IngressDisposition::Uncertain
                },
                TaskDisposition::Uncertain,
            )
        }
    }
}

async fn stop_recorder_server(
    shutdown: &tokio::sync::watch::Sender<bool>,
    task: &mut RecorderServerTask,
) -> Result<(), HaNodeError> {
    stop_recorder_server_before(
        shutdown,
        task,
        tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT,
    )
    .await
}

async fn stop_recorder_server_before(
    shutdown: &tokio::sync::watch::Sender<bool>,
    task: &mut RecorderServerTask,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    shutdown.send_replace(true);
    wait_for_recorder_server(task, deadline).await.0
}

async fn wait_for_recorder_server(
    task: &mut RecorderServerTask,
    deadline: tokio::time::Instant,
) -> (Result<(), HaNodeError>, Option<ShutdownEvidence>) {
    match task {
        RecorderServerTask::Http { ingress, .. } => {
            let (result, ingress, tasks) =
                wait_for_tracked_axum_ingress(ingress, "recorder server", deadline).await;
            (result, Some(ShutdownEvidence { ingress, tasks }))
        }
        RecorderServerTask::Tcp { ingress, .. } => {
            let (result, ingress, tasks) =
                wait_for_tracked_recorder_ingress(ingress, deadline).await;
            (result, Some(ShutdownEvidence { ingress, tasks }))
        }
    }
}

fn clamped_before_deadline(
    deadline: tokio::time::Instant,
    reserve: Duration,
) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    deadline
        .checked_sub(reserve)
        .filter(|candidate| *candidate > now)
        .unwrap_or(now)
}

async fn wait_for_tracked_recorder_ingress(
    ingress: &mut TrackedRecorderIngress,
    deadline: tokio::time::Instant,
) -> (Result<(), HaNodeError>, IngressDisposition, TaskDisposition) {
    let inner_force_at = clamped_before_deadline(deadline, Duration::from_millis(100));
    let outer_force_at = clamped_before_deadline(deadline, HA_SERVER_ABORT_RECEIPT_RESERVE);
    let mut listener_dropped = ingress.listener_receipted;
    let inner = tokio::time::sleep_until(inner_force_at);
    tokio::pin!(inner);
    loop {
        tokio::select! {
            receipt = &mut ingress.listener_dropped, if !listener_dropped => {
                listener_dropped = receipt.is_ok();
                ingress.listener_receipted |= listener_dropped;
            }
            result = &mut ingress.task => {
                let joined_normally = result.is_ok();
                let result = result
                    .map_err(|error| server_task_join_error("recorder server", error))
                    .and_then(|result| result);
                if !listener_dropped {
                    listener_dropped = ingress.poll_listener_receipt();
                }
                let tasks = if joined_normally && !ingress.forced.load(Ordering::Acquire) {
                    ingress.reported_tasks()
                } else {
                    TaskDisposition::Uncertain
                };
                return (
                    result,
                    ingress_after_service_wait(listener_dropped),
                    tasks,
                );
            }
            () = &mut inner => {
                ingress.forced.store(true, Ordering::Release);
                ingress.force.send_replace(true);
                break;
            }
        }
    }

    let outer = tokio::time::sleep_until(outer_force_at);
    tokio::pin!(outer);
    loop {
        tokio::select! {
            receipt = &mut ingress.listener_dropped, if !listener_dropped => {
                listener_dropped = receipt.is_ok();
                ingress.listener_receipted |= listener_dropped;
            }
            result = &mut ingress.task => {
                let result = result
                    .map_err(|error| server_task_join_error("recorder server", error))
                    .and_then(|result| result);
                if !listener_dropped {
                    listener_dropped = ingress.poll_listener_receipt();
                }
                return (
                    result,
                    ingress_after_service_wait(listener_dropped),
                    TaskDisposition::Uncertain,
                );
            }
            () = &mut outer => {
                ingress.task.abort();
                break;
            }
        }
    }

    match tokio::time::timeout_at(deadline, &mut ingress.task).await {
        Ok(Ok(result)) => {
            listener_dropped |= ingress.poll_listener_receipt();
            (
                result,
                ingress_after_service_wait(listener_dropped),
                TaskDisposition::Uncertain,
            )
        }
        Ok(Err(error)) => {
            listener_dropped |= ingress.poll_listener_receipt();
            (
                Err(server_task_join_error("recorder server", error)),
                ingress_after_service_wait(listener_dropped),
                TaskDisposition::Uncertain,
            )
        }
        Err(_) => {
            ingress.task.abort();
            (
                Err(conservative_shutdown_deadline_error(
                    HaShutdownPhase::Service,
                    ShutdownEvidence {
                        ingress: ingress_after_service_wait(listener_dropped),
                        tasks: TaskDisposition::Uncertain,
                    },
                )),
                ingress_after_service_wait(listener_dropped),
                TaskDisposition::Uncertain,
            )
        }
    }
}

#[cfg(test)]
async fn stop_ha_server_before(
    shutdown: &tokio::sync::watch::Sender<bool>,
    task: &mut HaServerTask,
    name: &str,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    shutdown.send_replace(true);
    wait_for_ha_server(task, name, deadline).await
}

async fn stop_staging_server_before(
    command: &tokio::sync::watch::Sender<StagingCommand>,
    task: &mut AbortStagingServerOnDrop,
    _name: &str,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    command.send_replace(StagingCommand::Close);
    match tokio::time::timeout_at(deadline, &mut **task).await {
        Ok(Ok(Ok(StagingExit::Closed))) => Ok(()),
        Ok(Ok(Ok(StagingExit::Handoff(_)))) => Err(HaNodeError::ServiceServer(
            "successor staging service handed off its listener while closing".into(),
        )),
        Ok(Ok(Err(error))) => Err(error),
        Ok(Err(error)) => Err(server_task_join_error("successor staging service", error)),
        Err(_) => {
            task.abort();
            Err(conservative_shutdown_deadline_error(
                HaShutdownPhase::Service,
                UNCERTAIN_SHUTDOWN_EVIDENCE,
            ))
        }
    }
}

async fn handoff_staging_listener(
    command: &tokio::sync::watch::Sender<StagingCommand>,
    task: &mut AbortStagingServerOnDrop,
    deadline: tokio::time::Instant,
) -> Result<ListenerLease, HaNodeError> {
    handoff_staging_listener_with_command(command, task, deadline, StagingCommand::Handoff).await
}

async fn handoff_staging_listener_with_command(
    command: &tokio::sync::watch::Sender<StagingCommand>,
    task: &mut AbortStagingServerOnDrop,
    deadline: tokio::time::Instant,
    handoff: StagingCommand,
) -> Result<ListenerLease, HaNodeError> {
    command.send_replace(handoff);
    match tokio::time::timeout_at(deadline, &mut **task).await {
        Ok(Ok(Ok(StagingExit::Handoff(listener)))) => Ok(listener),
        Ok(Ok(Ok(StagingExit::Closed))) => Err(HaNodeError::ServiceServer(
            "successor staging service closed before listener handoff".into(),
        )),
        Ok(Ok(Err(error))) => Err(error),
        Ok(Err(error)) => Err(server_task_join_error("successor staging service", error)),
        Err(_) => {
            task.abort();
            Err(conservative_shutdown_deadline_error(
                HaShutdownPhase::Service,
                UNCERTAIN_SHUTDOWN_EVIDENCE,
            ))
        }
    }
}

/// An interrupted handoff still has a staging-owned listener.  Reap the
/// aborted owner before returning whenever D permits, so a successful parent
/// shutdown is also proof that the unique listener lease was dropped.
async fn abort_staging_server_before(
    task: &mut AbortStagingServerOnDrop,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    task.abort();
    match tokio::time::timeout_at(deadline, &mut **task).await {
        Ok(Err(error)) if error.is_cancelled() => Ok(()),
        Ok(Err(error)) => Err(server_task_join_error("successor staging service", error)),
        Ok(Ok(Ok(StagingExit::Closed))) => Ok(()),
        Ok(Ok(Ok(StagingExit::Handoff(listener)))) => {
            // The handoff future was dropped before it could deliver this
            // lease to the child. The completed task result is therefore the
            // sole remaining owner; dropping it here closes ingress and is a
            // successful cleanup, not an ownership failure.
            drop(listener);
            Ok(())
        }
        Ok(Ok(Err(error))) => Err(error),
        Err(_) => Err(conservative_shutdown_deadline_error(
            HaShutdownPhase::Service,
            UNCERTAIN_SHUTDOWN_EVIDENCE,
        )),
    }
}

#[cfg(test)]
async fn wait_for_ha_server(
    task: &mut HaServerTask,
    name: &str,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    wait_for_ha_server_with_receipt(task, name, deadline)
        .await
        .0
}

#[cfg(test)]
async fn wait_for_ha_server_with_receipt(
    task: &mut HaServerTask,
    name: &str,
    deadline: tokio::time::Instant,
) -> (Result<(), HaNodeError>, bool) {
    match tokio::time::timeout_at(deadline, &mut *task).await {
        // This legacy helper has no listener ownership receipt. Joining a
        // task alone never proves that ingress was closed.
        Ok(Ok(result)) => (result, false),
        Ok(Err(error)) => (Err(server_task_join_error(name, error)), false),
        Err(_) => {
            task.abort();
            (
                Err(conservative_shutdown_deadline_error(
                    HaShutdownPhase::Service,
                    UNCERTAIN_SHUTDOWN_EVIDENCE,
                )),
                false,
            )
        }
    }
}

fn combine_ha_results(
    terminal: Option<HaNodeError>,
    cleanup: Result<(), HaNodeError>,
) -> Result<(), HaNodeError> {
    match (terminal, cleanup) {
        (None, cleanup) => cleanup,
        (Some(primary), Ok(())) => Err(primary),
        (Some(primary), Err(cleanup)) => Err(HaNodeError::Cleanup {
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

fn combine_ha_errors(mut errors: Vec<HaNodeError>) -> Result<(), HaNodeError> {
    if errors.is_empty() {
        return Ok(());
    }
    let primary = errors.remove(0);
    Err(errors
        .into_iter()
        .fold(primary, |primary, cleanup| HaNodeError::Cleanup {
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }))
}

#[derive(Clone)]
struct HaRecorder {
    recorder: RecorderFileStore,
    checkpoint_root: Option<LogAnchor>,
    active: Arc<AtomicBool>,
}

impl HaRecorder {
    fn active(recorder: RecorderFileStore) -> Self {
        Self {
            recorder,
            checkpoint_root: None,
            active: Arc::new(AtomicBool::new(true)),
        }
    }

    fn quarantined(recorder: RecorderFileStore, checkpoint_root: LogAnchor) -> Self {
        Self {
            recorder,
            checkpoint_root: Some(checkpoint_root),
            active: Arc::new(AtomicBool::new(false)),
        }
    }

    fn activate(&self) {
        self.active.store(true, Ordering::Release);
    }

    fn require_active(&self) -> rhiza_quepaxa::Result<()> {
        if self.active.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(rhiza_quepaxa::Error::Io(
                "recorder is quarantined during checkpoint recovery".into(),
            ))
        }
    }

    fn require_visible_slot(&self, slot: u64) -> rhiza_quepaxa::Result<()> {
        if self
            .checkpoint_root
            .is_some_and(|checkpoint_root| slot <= checkpoint_root.index())
        {
            return Err(rhiza_quepaxa::Error::Io(format!(
                "recorder checkpoint root {} does not expose historical slot {slot}",
                self.checkpoint_root.unwrap().index()
            )));
        }
        Ok(())
    }
}

impl RecorderRpc for HaRecorder {
    fn recorder_id(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        RecorderRpc::recorder_id(&self.recorder, context)
    }

    fn store_command_for(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        self.require_active()?;
        RecorderRpc::store_command_for(
            &self.recorder,
            context,
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
        context: &rhiza_quepaxa::RecorderRpcContext,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        RecorderRpc::fetch_command_for(
            &self.recorder,
            context,
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
        )
    }

    fn stage_effect_bundle_chunk(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
        ordinal: u16,
        chunk: Vec<u8>,
    ) -> rhiza_quepaxa::Result<()> {
        self.require_active()?;
        RecorderRpc::stage_effect_bundle_chunk(
            &self.recorder,
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
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        self.require_active()?;
        RecorderRpc::finalize_staged_effect_bundle(
            &self.recorder,
            context,
            binding,
            manifest_command,
        )
    }

    fn fetch_effect_bundle_manifest(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        RecorderRpc::fetch_effect_bundle_manifest(&self.recorder, context, binding)
    }

    fn fetch_effect_bundle_chunk(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
        ordinal: u16,
    ) -> rhiza_quepaxa::Result<Option<Vec<u8>>> {
        RecorderRpc::fetch_effect_bundle_chunk(&self.recorder, context, binding, ordinal)
    }

    fn record(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
        self.require_active()?;
        RecorderRpc::record(&self.recorder, context, request)
    }

    fn install_decision_proof(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.require_active()?;
        RecorderRpc::install_decision_proof(&self.recorder, context, proof, membership)
    }

    fn inspect_decision_proof(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        self.require_visible_slot(slot)?;
        RecorderRpc::inspect_decision_proof(&self.recorder, context, slot)
    }

    fn inspect_record_summary(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        self.require_visible_slot(slot)?;
        RecorderRpc::inspect_record_summary(&self.recorder, context, slot)
    }

    fn supports_context_read_fence(&self) -> bool {
        self.recorder.supports_context_read_fence()
    }

    fn observe_read_fence(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<ReadFenceObservation> {
        self.require_active()?;
        self.require_visible_slot(request.slot)?;
        RecorderRpc::observe_read_fence(&self.recorder, context, request)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HaStartupError {
    Source(String),
    Cancelled(StartupCancellationToken),
}

impl fmt::Display for HaStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(message) => formatter.write_str(message),
            Self::Cancelled { .. } => formatter.write_str("startup cancelled"),
        }
    }
}

impl std::error::Error for HaStartupError {}

fn error(error: impl fmt::Display) -> HaStartupError {
    HaStartupError::Source(error.to_string())
}

fn fail(message: impl Into<String>) -> HaStartupError {
    HaStartupError::Source(message.into())
}

fn startup_error(error: NodeError) -> HaStartupError {
    match error {
        NodeError::StartupCancelled { token, .. } => HaStartupError::Cancelled(token),
        error => HaStartupError::Source(error.to_string()),
    }
}

/// Admits one non-preemptible synchronous HA startup transaction.
///
/// The permit must span the complete local transaction, but never an `.await`
/// or object-store call.  In particular, do not add a second admission below
/// an already admitted composite transaction: close-first is the only path
/// that turns this boundary into `Cancelled`; later source failures retain
/// their original error.
fn admit_ha_startup_local_io(
    startup: &StartupIoContext,
    stage: &'static str,
) -> Result<rhiza_node::StartupLocalIoPermit, HaStartupError> {
    startup.admit_local_io(stage).map_err(startup_error)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestHaStartupTransaction {
    MarkerPublication,
    SuccessorPrestageAdoption,
    RejoinRecoveryViewCleanupAndOpen,
    /// The archive-only half of a standard checkpoint restore.  This must
    /// never own a startup local-I/O permit.
    CheckpointRestoreRemotePrepareEntry,
    CheckpointRestoreRemotePrepareComplete,
    /// Reached only after the synchronous installer has performed its local
    /// transaction, while that transaction's single permit is still live.
    CheckpointRestoreLocalInstallComplete,
}

/// A test-only, operation-local pause point.  Hooks are keyed by the exact
/// transaction and data directory, so another concurrent HA test cannot
/// observe or block this test's operation merely because it reaches the same
/// generic startup stage.
#[cfg(test)]
#[derive(Clone)]
struct TestHaStartupTransactionGate {
    transaction: TestHaStartupTransaction,
    data_dir: PathBuf,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

#[cfg(test)]
impl TestHaStartupTransactionGate {
    fn new(
        transaction: TestHaStartupTransaction,
        data_dir: impl Into<PathBuf>,
    ) -> (Self, std::sync::mpsc::Receiver<()>) {
        let (entered, receiver) = std::sync::mpsc::sync_channel(1);
        (
            Self {
                transaction,
                data_dir: data_dir.into(),
                entered,
                release: Arc::new((Mutex::new(false), Condvar::new())),
            },
            receiver,
        )
    }

    fn release_guard(&self) -> TestHaStartupTransactionRelease {
        TestHaStartupTransactionRelease(Arc::clone(&self.release))
    }

    fn wait(&self) {
        // A dropped receiver means its test has unwound.  Returning rather
        // than waiting leaves the real transaction able to drain.
        if self.entered.send(()).is_err() {
            return;
        }
        let (released, condition) = &*self.release;
        let mut released = lock_unpoison(released);
        while !*released {
            released = condition
                .wait(released)
                .unwrap_or_else(|error| error.into_inner());
        }
    }
}

#[cfg(test)]
struct TestHaStartupTransactionRelease(Arc<(Mutex<bool>, Condvar)>);

#[cfg(test)]
impl Drop for TestHaStartupTransactionRelease {
    fn drop(&mut self) {
        let (released, condition) = &*self.0;
        *lock_unpoison(released) = true;
        condition.notify_all();
    }
}

#[cfg(test)]
#[derive(Clone)]
struct InstalledTestHaStartupTransactionGate {
    id: u64,
}

#[cfg(test)]
type TestHaStartupTransactionGateRegistry = Vec<(u64, Arc<TestHaStartupTransactionGate>)>;

#[cfg(test)]
static TEST_HA_STARTUP_TRANSACTION_GATES: OnceLock<Mutex<TestHaStartupTransactionGateRegistry>> =
    OnceLock::new();

#[cfg(test)]
static NEXT_TEST_HA_STARTUP_TRANSACTION_GATE: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
fn test_ha_startup_transaction_gates() -> &'static Mutex<TestHaStartupTransactionGateRegistry> {
    TEST_HA_STARTUP_TRANSACTION_GATES.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
impl Drop for InstalledTestHaStartupTransactionGate {
    fn drop(&mut self) {
        lock_unpoison(test_ha_startup_transaction_gates()).retain(|(id, _)| *id != self.id);
    }
}

#[cfg(test)]
fn install_test_ha_startup_transaction_gate(
    gate: TestHaStartupTransactionGate,
) -> InstalledTestHaStartupTransactionGate {
    let id = NEXT_TEST_HA_STARTUP_TRANSACTION_GATE.fetch_add(1, Ordering::Relaxed);
    assert_ne!(id, 0, "HA startup transaction gate identity exhausted");
    let mut gates = lock_unpoison(test_ha_startup_transaction_gates());
    assert!(
        !gates.iter().any(|(_, existing)| {
            existing.transaction == gate.transaction && existing.data_dir == gate.data_dir
        }),
        "HA startup transaction gate already installed for this operation and data directory"
    );
    gates.push((id, Arc::new(gate)));
    InstalledTestHaStartupTransactionGate { id }
}

#[cfg(test)]
fn test_ha_startup_transaction_gate(transaction: TestHaStartupTransaction, data_dir: &Path) {
    let gate = lock_unpoison(test_ha_startup_transaction_gates())
        .iter()
        .find(|(_, gate)| gate.transaction == transaction && gate.data_dir == data_dir)
        .map(|(_, gate)| Arc::clone(gate));
    if let Some(gate) = gate {
        gate.wait();
    }
}

fn is_requested_startup_cancellation(
    error: &HaStartupError,
    startup: &StartupIoContext,
    token: Option<&Arc<ShutdownToken>>,
) -> bool {
    matches!(
        error,
        HaStartupError::Cancelled(cancellation)
            if token.is_some_and(|token| {
                token.startup_token() == cancellation && startup.is_cancelled_by(cancellation)
            })
    )
}

fn is_requested_node_cancellation(
    error: &NodeError,
    startup: &StartupIoContext,
    token: Option<&Arc<ShutdownToken>>,
) -> bool {
    matches!(
        error,
        NodeError::StartupCancelled { token: cancellation, .. }
            if token.is_some_and(|token| {
                token.startup_token() == cancellation && startup.is_cancelled_by(cancellation)
            })
    )
}

fn node_error_after_shutdown(
    error: NodeError,
    startup: &StartupIoContext,
    token: Option<&Arc<ShutdownToken>>,
) -> Result<(), HaStartupError> {
    if is_requested_node_cancellation(&error, startup, token) {
        Ok(())
    } else {
        Err(startup_error(error))
    }
}

#[derive(Clone, Debug)]
enum StartupPreparation {
    RecorderFirst {
        open_policy: RecorderOpenPolicy,
    },
    VerifyLocalCheckpoint {
        identity: CheckpointIdentity,
        root: LogAnchor,
    },
    RuntimeFirstWithPeerCatchup {
        checkpoint_root: LogAnchor,
        open_policy: RecorderOpenPolicy,
    },
}

impl StartupPreparation {
    fn open_policy(&self) -> RecorderOpenPolicy {
        match self {
            Self::RecorderFirst { open_policy }
            | Self::RuntimeFirstWithPeerCatchup { open_policy, .. } => *open_policy,
            Self::VerifyLocalCheckpoint { .. } => RecorderOpenPolicy::MustExist,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum RecorderOpenPolicy {
    MustExist,
    CreateAfterRehydration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalRecorderState {
    Missing,
    Valid,
    Recoverable,
}

fn recorder_open_policy_for_state(state: LocalRecorderState) -> RecorderOpenPolicy {
    match state {
        LocalRecorderState::Missing => RecorderOpenPolicy::CreateAfterRehydration,
        LocalRecorderState::Valid | LocalRecorderState::Recoverable => {
            RecorderOpenPolicy::MustExist
        }
    }
}

fn validate_archive_identity(
    config: &NodeConfig,
    identity: &CheckpointIdentity,
    target_config_id: u64,
) -> Result<(), HaStartupError> {
    if identity.cluster_id() != config.cluster_id()
        || identity.epoch() != config.epoch()
        || identity.config_id() != target_config_id
        || identity.recovery_generation() != config.recovery_generation()
    {
        return Err(fail(
            "checkpoint identity does not match target node configuration",
        ));
    }
    Ok(())
}

/// Loads and validates one immutable archive checkpoint before any HA local
/// startup admission.  Keeping this as a single boundary is deliberate: all
/// standard restore branches must consume the same prepared payload model and
/// tests can prove that no archive await is hidden under a local-I/O permit.
async fn prepare_checkpoint_restore_for_ha(
    archive: &ObjectArchiveStore,
    _data_dir: &Path,
    startup: &StartupIoContext,
) -> Result<rhiza_node::durability::PreparedCheckpointRestore, HaStartupError> {
    let prepare_permit =
        acquire_checkpoint_prepare_permit(&HA_CHECKPOINT_PREPARE_SLOTS, startup).await?;
    #[cfg(test)]
    test_ha_startup_transaction_gate(
        TestHaStartupTransaction::CheckpointRestoreRemotePrepareEntry,
        _data_dir,
    );

    let prepared = rhiza_node::durability::prepare_checkpoint_restore(archive)
        .await
        .map_err(error)?;

    #[cfg(test)]
    test_ha_startup_transaction_gate(
        TestHaStartupTransaction::CheckpointRestoreRemotePrepareComplete,
        _data_dir,
    );

    drop(prepare_permit);
    Ok(prepared)
}

async fn acquire_checkpoint_prepare_permit<'a>(
    slots: &'a tokio::sync::Semaphore,
    startup: &StartupIoContext,
) -> Result<tokio::sync::SemaphorePermit<'a>, HaStartupError> {
    const WAIT_STAGE: &str = "checkpoint remote prepare admission";
    startup.check(WAIT_STAGE).map_err(startup_error)?;
    let permit = tokio::select! {
        biased;
        cancellation = startup.wait_for_cancellation(WAIT_STAGE) => {
            return Err(startup_error(cancellation));
        }
        permit = slots.acquire() => permit.map_err(|_| {
            fail("checkpoint remote prepare admission closed")
        })?,
    };
    startup.check(WAIT_STAGE).map_err(startup_error)?;
    Ok(permit)
}

/// Captures the exact local restore epoch before any archive await. The token
/// is consumed by the one synchronous installer after HA admits local I/O;
/// this makes a prepared remote payload stale if another process installs or
/// advances the same data root while it is downloading.
fn capture_checkpoint_restore_state_for_ha(
    config: &NodeConfig,
    archive: &ObjectArchiveStore,
    mode: rhiza_node::durability::CheckpointInstallMode,
    completion_marker_name: Option<&str>,
) -> Result<rhiza_node::durability::ExpectedLocalRestoreState, HaStartupError> {
    let identity = archive.checkpoint_identity().map_err(error)?;
    rhiza_node::durability::capture_expected_local_restore_state(
        config.data_dir(),
        mode,
        config.node_id(),
        identity,
        config.execution_profile(),
        config.log_initial_configuration().clone(),
        completion_marker_name,
    )
    .map_err(error)
}

#[cfg(test)]
fn test_ha_checkpoint_restore_local_install_complete(data_dir: &Path) {
    test_ha_startup_transaction_gate(
        TestHaStartupTransaction::CheckpointRestoreLocalInstallComplete,
        data_dir,
    );
}

async fn prepare_standard(
    config: &NodeConfig,
    archive: &ObjectArchiveStore,
    mode: HaStartupMode,
    membership: &Membership,
    startup: &StartupIoContext,
) -> Result<StartupPreparation, HaStartupError> {
    let data_dir = config.data_dir();
    let node_id = config.node_id();
    let execution_profile = config.execution_profile();
    match mode {
        HaStartupMode::Bootstrap => {
            startup
                .check("bootstrap local data inspection")
                .map_err(startup_error)?;
            if !local_data_is_fresh(data_dir)? {
                return Err(fail("bootstrap requires a fresh local data directory"));
            }
            let loaded = archive
                .load_checkpoint()
                .await
                .map_err(error)?
                .ok_or_else(|| fail("bootstrap requires an initialized empty checkpoint"))?;
            startup
                .check("bootstrap checkpoint validation")
                .map_err(startup_error)?;
            if loaded.manifest().tip().index() != 0 || !loaded.manifest().segments().is_empty() {
                return Err(fail("bootstrap requires an initialized empty checkpoint"));
            }
            write_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                loaded.manifest().identity(),
                node_id,
                startup,
            )?;
            Ok(StartupPreparation::RecorderFirst {
                open_policy: RecorderOpenPolicy::CreateAfterRehydration,
            })
        }
        HaStartupMode::Rejoin if local_data_is_fresh(data_dir)? => {
            startup
                .check("rejoin checkpoint restore")
                .map_err(startup_error)?;
            let expected = capture_checkpoint_restore_state_for_ha(
                config,
                archive,
                rhiza_node::durability::CheckpointInstallMode::Fresh,
                Some(LOCAL_CHECKPOINT_IDENTITY_FILE),
            )?;
            let prepared = prepare_checkpoint_restore_for_ha(archive, data_dir, startup).await?;
            let identity = prepared.identity();
            let marker =
                encode_local_checkpoint_identity_marker(execution_profile, identity, node_id)?;
            let completion_marker = rhiza_node::durability::RestoreCompletionMarker::new(
                LOCAL_CHECKPOINT_IDENTITY_FILE,
                &marker,
            )
            .map_err(error)?;
            let tip = {
                let _permit =
                    admit_ha_startup_local_io(startup, "fresh rejoin checkpoint install")?;
                let tip = rhiza_node::durability::install_prepared_checkpoint_to_fresh_data_dir(
                    &prepared,
                    expected,
                    Some(completion_marker),
                )
                .map_err(error)?;
                #[cfg(test)]
                test_ha_checkpoint_restore_local_install_complete(data_dir);
                tip
            };
            startup
                .check("rejoin checkpoint marker validation")
                .map_err(startup_error)?;
            read_and_validate_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                identity,
                node_id,
            )?;
            Ok(StartupPreparation::RuntimeFirstWithPeerCatchup {
                checkpoint_root: LogAnchor::new(tip.index(), tip.hash()),
                open_policy: RecorderOpenPolicy::CreateAfterRehydration,
            })
        }
        HaStartupMode::Rejoin => {
            startup
                .check("rejoin checkpoint inspection")
                .map_err(startup_error)?;
            let loaded = archive
                .load_checkpoint()
                .await
                .map_err(error)?
                .ok_or_else(|| fail("rejoin requires an initialized checkpoint"))?;
            startup
                .check("recorder preflight and recovery")
                .map_err(startup_error)?;
            let identity = loaded.manifest().identity();
            let checkpoint_root = LogAnchor::new(
                loaded.manifest().tip().index(),
                loaded.manifest().tip().hash(),
            );
            let exact_marker_exists =
                exact_checkpoint_marker_exists(data_dir, execution_profile, identity, node_id)?;
            let restore_state = rhiza_node::durability::checkpoint_restore_in_progress(
                data_dir,
                identity,
                node_id,
                execution_profile,
                checkpoint_root,
            )
            .map_err(error)?;
            if !exact_marker_exists
                && restore_state == rhiza_node::durability::CheckpointRestoreState::None
            {
                return Err(fail("rejoin requires a local checkpoint identity marker"));
            }
            let recorder_state = recover_local_recorder_before_view_recovery(
                preflight_local_recorder(data_dir, identity, membership)?,
                data_dir,
                node_id,
                identity,
                membership,
                startup,
            )?;
            if restore_state != rhiza_node::durability::CheckpointRestoreState::None {
                startup
                    .check("rejoin interrupted checkpoint restore")
                    .map_err(startup_error)?;
                let expected = capture_checkpoint_restore_state_for_ha(
                    config,
                    archive,
                    if execution_profile == ExecutionProfile::Graph {
                        rhiza_node::durability::CheckpointInstallMode::Fresh
                    } else {
                        rhiza_node::durability::CheckpointInstallMode::RejoinPreservingRecorder
                    },
                    Some(LOCAL_CHECKPOINT_IDENTITY_FILE),
                )?;
                let prepared =
                    prepare_checkpoint_restore_for_ha(archive, data_dir, startup).await?;
                let marker = encode_local_checkpoint_identity_marker(
                    execution_profile,
                    prepared.identity(),
                    node_id,
                )?;
                let completion_marker = rhiza_node::durability::RestoreCompletionMarker::new(
                    LOCAL_CHECKPOINT_IDENTITY_FILE,
                    &marker,
                )
                .map_err(error)?;
                let tip = if execution_profile == ExecutionProfile::Graph {
                    let _permit = admit_ha_startup_local_io(
                        startup,
                        "interrupted graph rejoin checkpoint install",
                    )?;
                    let tip =
                        rhiza_node::durability::install_prepared_checkpoint_to_fresh_data_dir(
                            &prepared,
                            expected,
                            Some(completion_marker),
                        )
                        .map_err(error)?;
                    #[cfg(test)]
                    test_ha_checkpoint_restore_local_install_complete(data_dir);
                    tip
                } else {
                    let _permit = admit_ha_startup_local_io(
                        startup,
                        "interrupted rejoin checkpoint install",
                    )?;
                    let tip = rhiza_node::durability::install_prepared_checkpoint_for_rejoin_preserving_recorder(
                        &prepared,
                        expected,
                        completion_marker,
                    )
                    .map_err(error)?;
                    #[cfg(test)]
                    test_ha_checkpoint_restore_local_install_complete(data_dir);
                    tip
                };
                startup
                    .check("rejoin restored checkpoint validation")
                    .map_err(startup_error)?;
                read_and_validate_local_checkpoint_identity_marker(
                    data_dir,
                    execution_profile,
                    identity,
                    node_id,
                )?;
                return Ok(StartupPreparation::RuntimeFirstWithPeerCatchup {
                    checkpoint_root: LogAnchor::new(tip.index(), tip.hash()),
                    open_policy: recorder_open_policy_for_state(recorder_state),
                });
            }
            read_and_validate_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                identity,
                node_id,
            )?;
            let recovery_view = validate_rejoin_recovery_view_for_startup(
                data_dir,
                identity,
                node_id,
                execution_profile,
                checkpoint_root,
                startup,
            );
            if let Err(view_error) = recovery_view {
                startup
                    .check("rejoin recovery-view restore")
                    .map_err(startup_error)?;
                eprintln!(
                    "local recovery view is not trustworthy ({view_error}); attempting an identity-bound quarantine and verified checkpoint restore"
                );
                let expected = capture_checkpoint_restore_state_for_ha(
                    config,
                    archive,
                    rhiza_node::durability::CheckpointInstallMode::RejoinPreservingRecorder,
                    Some(LOCAL_CHECKPOINT_IDENTITY_FILE),
                )?;
                let prepared =
                    prepare_checkpoint_restore_for_ha(archive, data_dir, startup).await?;
                let marker = encode_local_checkpoint_identity_marker(
                    execution_profile,
                    prepared.identity(),
                    node_id,
                )?;
                let completion_marker = rhiza_node::durability::RestoreCompletionMarker::new(
                    LOCAL_CHECKPOINT_IDENTITY_FILE,
                    &marker,
                )
                .map_err(error)?;
                let tip = {
                    let _permit = admit_ha_startup_local_io(
                        startup,
                        "rebuildable rejoin checkpoint install",
                    )?;
                    let tip = rhiza_node::durability::install_prepared_checkpoint_for_rejoin_preserving_recorder(
                        &prepared,
                        expected,
                        completion_marker,
                    )
                    .map_err(|restore_error| {
                        fail(format!(
                            "rebuildable local recovery view could not be quarantined and restored from the verified checkpoint: {restore_error}"
                        ))
                    })?;
                    #[cfg(test)]
                    test_ha_checkpoint_restore_local_install_complete(data_dir);
                    tip
                };
                startup
                    .check("rejoin recovery-view validation")
                    .map_err(startup_error)?;
                read_and_validate_local_checkpoint_identity_marker(
                    data_dir,
                    execution_profile,
                    identity,
                    node_id,
                )?;
                return Ok(StartupPreparation::RuntimeFirstWithPeerCatchup {
                    checkpoint_root: LogAnchor::new(tip.index(), tip.hash()),
                    open_policy: recorder_open_policy_for_state(recorder_state),
                });
            }
            if recorder_state == LocalRecorderState::Missing {
                return Ok(StartupPreparation::RuntimeFirstWithPeerCatchup {
                    checkpoint_root,
                    open_policy: RecorderOpenPolicy::CreateAfterRehydration,
                });
            }
            Ok(StartupPreparation::VerifyLocalCheckpoint {
                identity: identity.clone(),
                root: checkpoint_root,
            })
        }
        HaStartupMode::Disaster => {
            startup
                .check("disaster checkpoint inspection")
                .map_err(startup_error)?;
            let expected = capture_checkpoint_restore_state_for_ha(
                config,
                archive,
                rhiza_node::durability::CheckpointInstallMode::Fresh,
                Some(LOCAL_CHECKPOINT_IDENTITY_FILE),
            )?;
            let prepared = prepare_checkpoint_restore_for_ha(archive, data_dir, startup).await?;
            let identity = prepared.identity();
            startup
                .check("disaster restore preparation")
                .map_err(startup_error)?;
            let checkpoint_root = prepared.checkpoint_root();
            let _exact_marker_exists =
                exact_checkpoint_marker_exists(data_dir, execution_profile, identity, node_id)?;
            let restore_state = rhiza_node::durability::checkpoint_restore_in_progress(
                data_dir,
                identity,
                node_id,
                execution_profile,
                checkpoint_root,
            )
            .map_err(error)?;
            if restore_state == rhiza_node::durability::CheckpointRestoreState::None
                && !local_data_is_fresh(data_dir)?
            {
                return Err(fail(
                    "disaster startup requires a fresh local data directory",
                ));
            }
            let marker =
                encode_local_checkpoint_identity_marker(execution_profile, identity, node_id)?;
            let completion_marker = rhiza_node::durability::RestoreCompletionMarker::new(
                LOCAL_CHECKPOINT_IDENTITY_FILE,
                &marker,
            )
            .map_err(error)?;
            {
                let _permit = admit_ha_startup_local_io(startup, "disaster checkpoint install")?;
                rhiza_node::durability::install_prepared_checkpoint_to_fresh_data_dir(
                    &prepared,
                    expected,
                    Some(completion_marker),
                )
                .map_err(error)?;
                #[cfg(test)]
                test_ha_checkpoint_restore_local_install_complete(data_dir);
            }
            startup
                .check("disaster checkpoint validation")
                .map_err(startup_error)?;
            read_and_validate_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                identity,
                node_id,
            )?;
            Ok(StartupPreparation::RecorderFirst {
                open_policy: RecorderOpenPolicy::CreateAfterRehydration,
            })
        }
    }
}

async fn prepare_successor(
    config: &NodeConfig,
    archive: &ObjectArchiveStore,
    mode: HaStartupMode,
    target_config_id: u64,
    predecessor: &HaPredecessor,
    startup: &StartupIoContext,
) -> Result<StartupPreparation, HaStartupError> {
    startup
        .check("successor binding validation")
        .map_err(startup_error)?;
    if mode != HaStartupMode::Rejoin {
        return Err(fail("successor startup requires rejoin mode"));
    }
    validate_predecessor_binding(config, target_config_id, predecessor)?;
    adopt_successor_prestage_for_startup(config, predecessor, startup)?;
    startup
        .check("successor checkpoint initialization")
        .map_err(startup_error)?;
    let initialized = archive.initialize_checkpoint().await.map_err(error)?;
    startup
        .check("successor restore inspection")
        .map_err(startup_error)?;
    let target_checkpoint_empty =
        initialized.manifest().tip().index() == 0 && initialized.manifest().segments().is_empty();
    let identity = archive.checkpoint_identity().map_err(error)?;
    let data_dir = config.data_dir();
    let exact_marker_exists = exact_checkpoint_marker_exists(
        data_dir,
        config.execution_profile(),
        identity,
        config.node_id(),
    )?;
    let expected = expected_successor_restore_receipt(config, target_config_id, predecessor)?;
    let controls = validate_successor_restore_controls(data_dir, &expected)?;
    if controls == SuccessorRestoreControlState::Fresh {
        return Err(fail(
            "successor startup requires a finalized local prestage receipt",
        ));
    }
    if !target_checkpoint_empty {
        let minimum_activation_index = predecessor
            .stop
            .entry
            .index
            .checked_add(1)
            .ok_or_else(|| fail("successor Activate index cannot advance"))?;
        let active_target_baseline =
            initialized
                .manifest()
                .base()
                .snapshot()
                .is_some_and(|snapshot| {
                    let anchor = snapshot.anchor();
                    anchor.configuration_state().is_active()
                        && anchor.config_id() == target_config_id
                        && anchor.compacted().index() >= minimum_activation_index
                });
        if !active_target_baseline
            || (controls == SuccessorRestoreControlState::Complete && !exact_marker_exists)
        {
            return Err(fail(
                "non-empty successor checkpoint requires an active target snapshot and exact completed local identity",
            ));
        }
    }
    startup
        .check("recorder preflight and recovery")
        .map_err(startup_error)?;
    let recorder_state = preflight_local_recorder(data_dir, identity, config.membership())?;
    let recorder_state = recover_local_recorder_before_view_recovery(
        recorder_state,
        data_dir,
        config.node_id(),
        identity,
        config.membership(),
        startup,
    )?;
    if recorder_state == LocalRecorderState::Missing {
        install_successor_recorder_for_startup(config, target_config_id, predecessor, startup)?;
    }
    if controls == SuccessorRestoreControlState::Intent {
        complete_adopted_successor_prestage_for_startup(data_dir, &expected, startup)?;
    }
    write_local_checkpoint_identity_marker(
        data_dir,
        config.execution_profile(),
        identity,
        config.node_id(),
        startup,
    )?;
    Ok(StartupPreparation::RecorderFirst {
        open_policy: RecorderOpenPolicy::MustExist,
    })
}

fn adopt_successor_prestage_for_startup(
    config: &NodeConfig,
    predecessor: &HaPredecessor,
    startup: &StartupIoContext,
) -> Result<(), HaStartupError> {
    let _permit = admit_ha_startup_local_io(startup, "successor prestage inspection and adoption")?;
    match inspect_successor_prestage(
        config.data_dir(),
        config.log_initial_configuration().clone(),
    ) {
        Ok(prestage) => {
            let restore = adopt_finalized_successor_prestage(
                prestage,
                config,
                &predecessor.stop,
                &predecessor.membership,
            )
            .map_err(error)?;
            // `restore` holds the exact successor lock after the finalized
            // prestage has been adopted and its restore intent published.
            // Keep it live across the test-only gate so the close proof
            // covers the real adoption transaction, not only admission.
            #[cfg(test)]
            test_ha_startup_transaction_gate(
                TestHaStartupTransaction::SuccessorPrestageAdoption,
                config.data_dir(),
            );
            drop(restore);
            Ok(())
        }
        Err(DurabilityError::DataDirNotFresh(_)) => Ok(()),
        Err(cause) => Err(error(cause)),
    }
}

fn validate_predecessor_binding(
    config: &NodeConfig,
    target_config_id: u64,
    predecessor: &HaPredecessor,
) -> Result<(), HaStartupError> {
    let stop = &predecessor.stop;
    if stop.entry.cluster_id != config.cluster_id()
        || stop.entry.epoch != config.epoch()
        || stop.entry.config_id.checked_add(1) != Some(target_config_id)
        || stop.entry.recompute_hash() != stop.entry.hash
    {
        return Err(fail(
            "predecessor Stop does not exactly bind the target node configuration",
        ));
    }
    let ConfigChange::BoundStop { successor } =
        ConfigChange::recognize_parts(stop.entry.entry_type, &stop.entry.payload)
            .map_err(|_| fail("predecessor Stop is not a bound configuration change"))?
    else {
        return Err(fail("predecessor entry is not a bound Stop"));
    };
    if successor.cluster_id() != config.cluster_id()
        || successor.predecessor_config_id() != stop.entry.config_id
        || successor.predecessor_config_digest() != predecessor.membership.digest()
        || successor.config_id() != target_config_id
        || successor.digest() != config.membership().digest()
        || successor.members() != config.membership().members()
    {
        return Err(fail(
            "predecessor membership or Stop binding conflicts with the prestage target",
        ));
    }
    let command = StoredCommand::new(stop.entry.entry_type, stop.entry.payload.clone());
    let stop_anchor = LogAnchor::new(stop.entry.index, stop.entry.hash);
    if config.log_initial_configuration()
        != &rhiza_core::ConfigurationState::active(
            stop.entry.config_id,
            predecessor.membership.digest(),
        )
        || config.configuration_state()
            != &rhiza_core::ConfigurationState::stopped(
                stop.entry.config_id,
                predecessor.membership.digest(),
                stop_anchor,
                StopBinding::Bound {
                    successor: successor.clone(),
                    stop_command_hash: command.hash(),
                },
            )
    {
        return Err(fail(
            "successor startup configuration does not durably encode the exact Stop binding",
        ));
    }
    stop.proof
        .validate_for_cluster(
            config.cluster_id(),
            stop.entry.index,
            config.epoch(),
            stop.entry.config_id,
            &predecessor.membership,
        )
        .map_err(|proof_error| {
            fail(format!(
                "predecessor Stop proof is not valid for its membership: {proof_error:?}"
            ))
        })?;
    let expected_value = AcceptedValue::from_command(
        config.cluster_id(),
        stop.entry.index,
        config.epoch(),
        stop.entry.config_id,
        stop.entry.prev_hash,
        &command,
    );
    if stop.proof.proposal().value.as_ref() != Some(&expected_value) {
        return Err(fail(
            "predecessor Stop proof does not certify the exact Stop entry",
        ));
    }
    Ok(())
}

fn install_successor_recorder_for_startup(
    config: &NodeConfig,
    target_config_id: u64,
    predecessor: &HaPredecessor,
    startup: &StartupIoContext,
) -> Result<(), HaStartupError> {
    let _permit = admit_ha_startup_local_io(
        startup,
        "successor recorder preflight, installation, and recovery",
    )?;
    let recorder = open_recorder_after_preflight_under_startup_permit(
        config.data_dir().join("recorder"),
        config.node_id().to_owned(),
        config.cluster_id().to_owned(),
        config.epoch(),
        predecessor.stop.entry.config_id,
        predecessor.membership.clone(),
    )?;
    recover_successor_recorder_after_checkpoint(
        &recorder,
        config,
        target_config_id,
        config.membership().clone(),
        &predecessor.stop,
    )
    .map(|_| ())
    .map_err(error)
}

fn complete_adopted_successor_prestage_for_startup(
    data_dir: &Path,
    expected_identity: &[u8],
    startup: &StartupIoContext,
) -> Result<(), HaStartupError> {
    let _permit =
        admit_ha_startup_local_io(startup, "successor restore completion receipt publication")?;
    complete_adopted_successor_prestage(data_dir, expected_identity).map_err(error)
}

fn open_recorder_for_preparation(
    config: &NodeConfig,
    target_config_id: u64,
    policy: RecorderOpenPolicy,
    startup: &StartupIoContext,
) -> Result<RecorderFileStore, HaStartupError> {
    let _permit = admit_ha_startup_local_io(startup, "final local recorder open or create")?;
    let root = config.data_dir().join("recorder");
    match policy {
        RecorderOpenPolicy::MustExist => RecorderFileStore::open_existing_with_membership(
            root,
            config.node_id(),
            config.cluster_id(),
            config.epoch(),
            target_config_id,
            config.membership().clone(),
        )
        .map_err(|open_error| {
            fail(format!(
                "required local recorder disappeared after startup preparation: {open_error}"
            ))
        }),
        RecorderOpenPolicy::CreateAfterRehydration => {
            match RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                config.cluster_id(),
                config.epoch(),
                target_config_id,
                config.membership(),
            )
            .map_err(|preflight_error| {
                fail(format!(
                    "local recorder is not trustworthy: {preflight_error}"
                ))
            })? {
                RecorderPreflight::Missing => RecorderFileStore::new_with_membership(
                    root,
                    config.node_id(),
                    config.cluster_id(),
                    config.epoch(),
                    target_config_id,
                    config.membership().clone(),
                )
                .map_err(error),
                RecorderPreflight::Valid | RecorderPreflight::Recoverable => Err(fail(
                    "local recorder appeared after startup preparation; refusing to replace it",
                )),
            }
        }
    }
}

/// Opens or creates the successor recorder while the caller owns the one
/// transaction-wide startup-local-I/O permit.
fn open_recorder_after_preflight_under_startup_permit(
    root: PathBuf,
    recorder_id: String,
    cluster_id: String,
    epoch: u64,
    config_id: u64,
    membership: Membership,
) -> Result<RecorderFileStore, HaStartupError> {
    let outcome = RecorderFileStore::preflight_existing_with_membership_outcome(
        &root,
        &cluster_id,
        epoch,
        config_id,
        &membership,
    )
    .map_err(|preflight_error| {
        fail(format!(
            "local recorder is not trustworthy: {preflight_error}"
        ))
    })?;
    match outcome {
        RecorderPreflight::Missing => RecorderFileStore::new_with_membership(
            root,
            recorder_id,
            cluster_id,
            epoch,
            config_id,
            membership,
        ),
        RecorderPreflight::Valid | RecorderPreflight::Recoverable => {
            RecorderFileStore::open_existing_with_membership(
                root,
                recorder_id,
                cluster_id,
                epoch,
                config_id,
                membership,
            )
        }
    }
    .map_err(error)
}

fn preflight_local_recorder(
    data_dir: &Path,
    identity: &CheckpointIdentity,
    membership: &Membership,
) -> Result<LocalRecorderState, HaStartupError> {
    RecorderFileStore::preflight_existing_with_membership_outcome(
        data_dir.join("recorder"),
        identity.cluster_id(),
        identity.epoch(),
        identity.config_id(),
        membership,
    )
    .map(|outcome| match outcome {
        RecorderPreflight::Missing => LocalRecorderState::Missing,
        RecorderPreflight::Valid => LocalRecorderState::Valid,
        RecorderPreflight::Recoverable => LocalRecorderState::Recoverable,
    })
    .map_err(|preflight_error| {
        fail(format!(
            "local recorder is not trustworthy: {preflight_error}"
        ))
    })
}

fn recover_local_recorder_before_view_recovery(
    state: LocalRecorderState,
    data_dir: &Path,
    node_id: &str,
    identity: &CheckpointIdentity,
    membership: &Membership,
    startup: &StartupIoContext,
) -> Result<LocalRecorderState, HaStartupError> {
    if state != LocalRecorderState::Recoverable {
        return Ok(state);
    }
    let _permit =
        admit_ha_startup_local_io(startup, "local recorder crash recovery and preflight")?;
    eprintln!(
        "local recorder has normal crash artifacts; completing locked recorder recovery before rebuilding local views"
    );
    RecorderFileStore::open_existing_with_membership(
        data_dir.join("recorder"),
        node_id,
        identity.cluster_id(),
        identity.epoch(),
        identity.config_id(),
        membership.clone(),
    )
    .map_err(|open_error| {
        fail(format!(
            "local recorder crash recovery failed: {open_error}"
        ))
    })?;
    match preflight_local_recorder(data_dir, identity, membership)? {
        LocalRecorderState::Valid => Ok(LocalRecorderState::Valid),
        state => Err(fail(format!(
            "local recorder crash recovery did not produce a valid recorder: {state:?}"
        ))),
    }
}

fn validate_rejoin_recovery_view_for_startup(
    data_dir: &Path,
    identity: &CheckpointIdentity,
    node_id: &str,
    execution_profile: ExecutionProfile,
    checkpoint_root: LogAnchor,
    startup: &StartupIoContext,
) -> Result<(), HaStartupError> {
    let _permit = admit_ha_startup_local_io(startup, "rejoin recovery-view cleanup and open")?;
    let result = rhiza_node::durability::validate_local_recovery_view(
        data_dir,
        identity,
        node_id,
        execution_profile,
        checkpoint_root,
    );
    // The recovery cleaner and local materializer/qlog open above have run,
    // but the sole startup permit remains live. This makes the test gate a
    // post-effect transaction boundary even when the real operation returns
    // a source error.
    #[cfg(test)]
    test_ha_startup_transaction_gate(
        TestHaStartupTransaction::RejoinRecoveryViewCleanupAndOpen,
        data_dir,
    );
    result.map_err(error)
}

fn build_consensus(
    config: &NodeConfig,
    target_config_id: u64,
    recorders: Vec<(String, Box<dyn RecorderRpc>)>,
    local_recorder: Option<&RecorderFileStore>,
    checkpoint_root: Option<LogAnchor>,
) -> Result<Arc<ThreeNodeConsensus>, HaStartupError> {
    if let Some(recorder) = local_recorder {
        let recorder_id = recorder.recorder_id().map_err(error)?;
        if recorder_id != config.node_id() {
            return Err(fail(format!(
                "local recorder identity mismatch: expected {}, got {recorder_id}",
                config.node_id()
            )));
        }
    }
    let recorders = recorders
        .into_iter()
        .map(|(id, network)| {
            let recorder = if id == config.node_id() {
                local_recorder
                    .map(|local| Box::new(local.clone()) as Box<dyn RecorderRpc>)
                    .unwrap_or(network)
            } else {
                network
            };
            (id, recorder)
        })
        .collect();
    let consensus = match checkpoint_root {
        Some(root) => ThreeNodeConsensus::from_recorders_with_ids_and_recovered_tip(
            config.cluster_id().to_owned(),
            config.node_id().to_owned(),
            config.epoch(),
            target_config_id,
            recorders,
            root.index()
                .checked_add(1)
                .ok_or_else(|| fail("checkpoint root index cannot advance"))?,
            root.hash(),
        ),
        None => ThreeNodeConsensus::from_recorders_with_ids(
            config.cluster_id().to_owned(),
            config.node_id().to_owned(),
            config.epoch(),
            target_config_id,
            recorders,
        ),
    }
    .map_err(error)?;
    if config.membership() != consensus.membership() {
        return Err(fail(
            "recorder membership does not match node configuration",
        ));
    }
    Ok(Arc::new(consensus))
}

async fn open_runtime_with_retry(
    config: NodeConfig,
    consensus: Arc<ThreeNodeConsensus>,
    peers: Vec<Arc<dyn LogPeer>>,
    startup: &StartupIoContext,
    shutdown: &mut tokio::sync::watch::Receiver<ShutdownSignal>,
) -> Result<Arc<NodeRuntime>, HaOpenError> {
    let mut last_retry_error = None;
    loop {
        if let Some(token) = shutdown.borrow().clone() {
            let deadline = token.deadline();
            return Err(HaOpenError::Cancelled {
                deadline,
                cleanup: Ok(()),
            });
        }
        let attempt_config = config.clone();
        let attempt_consensus = consensus.clone();
        let attempt_peers = peers.clone();
        let attempt_startup = startup.clone();
        let mut attempt = AbortOnDropTask::spawn_blocking(move || {
            let peer_refs = attempt_peers
                .iter()
                .map(|peer| peer.as_ref())
                .collect::<Vec<_>>();
            NodeRuntime::open_cancellable(
                attempt_config,
                attempt_consensus,
                &peer_refs,
                &attempt_startup,
            )
        });
        let result = loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || shutdown.borrow().is_some() {
                        let token = shutdown.borrow().clone();
                        let deadline = shutdown_deadline(&token);
                        if attempt.is_finished() {
                            return Err(completed_runtime_open_after_shutdown(
                                attempt.await,
                                startup,
                                token.as_ref(),
                                deadline,
                            ));
                        }
                        return Err(cancel_runtime_open_attempt(attempt, startup, token, deadline).await);
                    }
                }
                result = &mut attempt => break result,
            }
        };
        if let Some(token) = shutdown.borrow().clone() {
            let deadline = token.deadline();
            return Err(completed_runtime_open_after_shutdown(
                result,
                startup,
                Some(&token),
                deadline,
            ));
        }
        let result = result
            .map_err(|join_error| fail(format!("runtime startup task failed: {join_error}")))?;
        match result {
            Ok(runtime) => return Ok(Arc::new(runtime)),
            Err(node_error @ (NodeError::Unavailable(_) | NodeError::Contention(_))) => {
                let message = node_error.to_string();
                if last_retry_error.as_deref() != Some(message.as_str()) {
                    eprintln!("runtime startup waiting for recorder quorum: {message}");
                    last_retry_error = Some(message);
                }
                wait_for_startup_retry(shutdown).await?;
            }
            Err(node_error) => return Err(error(node_error).into()),
        }
    }
}

async fn cancel_runtime_open_attempt(
    mut attempt: AbortOnDropTask<Result<NodeRuntime, NodeError>>,
    startup: &StartupIoContext,
    token: ShutdownSignal,
    deadline: tokio::time::Instant,
) -> HaOpenError {
    if let Some(token) = token.as_ref() {
        cancel_startup_for_token(startup, token);
    } else {
        startup.cancel(deadline.into_std());
    }
    match await_task_before(&mut attempt, deadline).await {
        Some(result) => {
            completed_runtime_open_after_shutdown(result, startup, token.as_ref(), deadline)
        }
        None => {
            let stage = startup.unfinished_stage().to_owned();
            HaOpenError::Cancelled {
                deadline,
                cleanup: Err(HaNodeError::StartupIoDeadlineExceeded { stage }),
            }
        }
    }
}

fn completed_runtime_open_after_shutdown(
    result: Result<Result<NodeRuntime, NodeError>, tokio::task::JoinError>,
    startup: &StartupIoContext,
    token: Option<&Arc<ShutdownToken>>,
    deadline: tokio::time::Instant,
) -> HaOpenError {
    match result {
        Ok(Ok(runtime)) => {
            runtime.cancel_operations();
            HaOpenError::Cancelled {
                deadline,
                cleanup: Ok(()),
            }
        }
        Ok(Err(error)) => match node_error_after_shutdown(error, startup, token) {
            Ok(()) => HaOpenError::Cancelled {
                deadline,
                cleanup: Ok(()),
            },
            Err(error) => HaOpenError::Startup {
                error,
                cleanup: Ok(()),
            },
        },
        Err(error) => HaOpenError::Startup {
            error: fail(format!(
                "runtime startup task failed during shutdown: {error}"
            )),
            cleanup: Ok(()),
        },
    }
}

async fn rehydrate_recorder_with_retry(
    runtime: Arc<NodeRuntime>,
    recorder: RecorderFileStore,
    checkpoint_index: u64,
    startup: &StartupIoContext,
    shutdown: &mut tokio::sync::watch::Receiver<ShutdownSignal>,
) -> Result<(), HaOpenError> {
    loop {
        require_startup_active(shutdown)?;
        let attempt_runtime = runtime.clone();
        let attempt_recorder = recorder.clone();
        let attempt_startup = startup.clone();
        let mut attempt = AbortOnDropTask::spawn_blocking(move || {
            rehydrate_recorder_after_checkpoint(
                &attempt_runtime,
                &attempt_recorder,
                checkpoint_index,
                &attempt_startup,
            )
        });
        let result = loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || shutdown.borrow().is_some() {
                        let token = shutdown.borrow().clone();
                        let deadline = shutdown_deadline(&token);
                        if attempt.is_finished() {
                            return Err(completed_rehydrate_after_shutdown(
                                attempt.await,
                                &runtime,
                                startup,
                                token.as_ref(),
                                deadline,
                            ));
                        }
                        return Err(cancel_rehydrate_attempt(attempt, &runtime, startup, token, deadline).await);
                    }
                }
                result = &mut attempt => break result,
            }
        };
        if let Some(token) = shutdown.borrow().clone() {
            let deadline = token.deadline();
            return Err(completed_rehydrate_after_shutdown(
                result,
                &runtime,
                startup,
                Some(&token),
                deadline,
            ));
        }
        let result = result.map_err(|join_error| HaOpenError::Startup {
            error: fail(format!("recorder rehydration task failed: {join_error}")),
            cleanup: Ok(()),
        })?;
        match result {
            Ok(()) => return Ok(()),
            Err(NodeError::Unavailable(_) | NodeError::Contention(_)) => {
                wait_for_startup_retry(shutdown).await?;
            }
            Err(node_error) => return Err(error(node_error).into()),
        }
    }
}

async fn cancel_rehydrate_attempt(
    mut attempt: AbortOnDropTask<Result<(), NodeError>>,
    runtime: &Arc<NodeRuntime>,
    startup: &StartupIoContext,
    token: ShutdownSignal,
    deadline: tokio::time::Instant,
) -> HaOpenError {
    if let Some(token) = token.as_ref() {
        cancel_startup_for_token(startup, token);
    } else {
        startup.cancel(deadline.into_std());
    }
    runtime.cancel_operations();
    match await_task_before(&mut attempt, deadline).await {
        Some(result) => {
            completed_rehydrate_after_shutdown(result, runtime, startup, token.as_ref(), deadline)
        }
        None => {
            let stage = startup.unfinished_stage().to_owned();
            HaOpenError::Cancelled {
                deadline,
                cleanup: Err(HaNodeError::StartupIoDeadlineExceeded { stage }),
            }
        }
    }
}

fn completed_rehydrate_after_shutdown(
    result: Result<Result<(), NodeError>, tokio::task::JoinError>,
    runtime: &Arc<NodeRuntime>,
    startup: &StartupIoContext,
    token: Option<&Arc<ShutdownToken>>,
    deadline: tokio::time::Instant,
) -> HaOpenError {
    match result {
        Ok(Ok(())) => {
            runtime.cancel_operations();
            HaOpenError::Cancelled {
                deadline,
                cleanup: Ok(()),
            }
        }
        Ok(Err(error)) => match node_error_after_shutdown(error, startup, token) {
            Ok(()) => HaOpenError::Cancelled {
                deadline,
                cleanup: Ok(()),
            },
            Err(error) => HaOpenError::Startup {
                error,
                cleanup: Ok(()),
            },
        },
        Err(error) => HaOpenError::Startup {
            error: fail(format!(
                "recorder rehydration task failed during shutdown: {error}"
            )),
            cleanup: Ok(()),
        },
    }
}

fn require_startup_active(
    shutdown: &tokio::sync::watch::Receiver<ShutdownSignal>,
) -> Result<(), HaOpenError> {
    if let Some(token) = shutdown.borrow().clone() {
        let deadline = token.deadline();
        Err(HaOpenError::Cancelled {
            deadline,
            cleanup: Ok(()),
        })
    } else {
        Ok(())
    }
}

async fn wait_for_startup_retry(
    shutdown: &mut tokio::sync::watch::Receiver<ShutdownSignal>,
) -> Result<(), HaOpenError> {
    if let Some(token) = shutdown.borrow().clone() {
        let deadline = token.deadline();
        return Err(HaOpenError::Cancelled {
            deadline,
            cleanup: Ok(()),
        });
    }
    tokio::select! {
        () = tokio::time::sleep(STARTUP_RETRY_DELAY) => Ok(()),
        changed = shutdown.changed() => {
            if changed.is_err() || shutdown.borrow().is_some() {
                let deadline = shutdown_deadline(&shutdown.borrow());
                Err(HaOpenError::Cancelled {
                    deadline,
                    cleanup: Ok(()),
                })
            } else {
                Ok(())
            }
        }
    }
}

fn verify_local_rejoin_checkpoint(
    runtime: &NodeRuntime,
    identity: &CheckpointIdentity,
    authoritative_root: LogAnchor,
) -> Result<(), HaStartupError> {
    let config = runtime.config();
    let local_identity = CheckpointIdentity::new(
        config.cluster_id().to_owned(),
        config.epoch(),
        runtime.consensus().config_id(),
        runtime.configuration_state().map_err(error)?.digest(),
        config.recovery_generation(),
    );
    if &local_identity != identity {
        return Err(fail(
            "nonfresh rejoin local qlog identity does not match the authoritative checkpoint",
        ));
    }
    let expected_profile_prefix = format!("rhiza:{}:", config.execution_profile());
    if !identity.cluster_id().starts_with(&expected_profile_prefix) {
        return Err(fail(
            "nonfresh rejoin execution profile does not match the checkpoint identity",
        ));
    }
    let state = runtime.log_store().logical_state().map_err(error)?;
    let local_tip = state
        .tip
        .unwrap_or_else(|| LogAnchor::new(0, LogHash::ZERO));
    if local_tip.index() < authoritative_root.index() {
        return Err(fail(format!(
            "nonfresh rejoin local qlog tip {} is behind authoritative checkpoint {}",
            local_tip.index(),
            authoritative_root.index(),
        )));
    }
    if authoritative_root.index() == 0 {
        return if authoritative_root.hash() == LogHash::ZERO {
            Ok(())
        } else {
            Err(fail("authoritative checkpoint genesis hash is not zero"))
        };
    }
    let local_hash = if state
        .anchor
        .as_ref()
        .is_some_and(|anchor| anchor.compacted().index() == authoritative_root.index())
    {
        state
            .anchor
            .as_ref()
            .map(|anchor| anchor.compacted().hash())
    } else if state
        .anchor
        .as_ref()
        .is_some_and(|anchor| anchor.compacted().index() > authoritative_root.index())
    {
        return Err(fail(
            "nonfresh rejoin local qlog compacted past the authoritative checkpoint without exact inclusion evidence",
        ));
    } else {
        runtime
            .log_store()
            .read(authoritative_root.index())
            .map_err(error)?
            .map(|entry| entry.hash)
    };
    if local_hash != Some(authoritative_root.hash()) {
        return Err(fail(format!(
            "nonfresh rejoin local qlog hash at index {} does not match the authoritative checkpoint",
            authoritative_root.index(),
        )));
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LocalCheckpointIdentityMarker {
    cluster_id: String,
    node_id: String,
    execution_profile: ExecutionProfile,
    epoch: u64,
    config_id: u64,
    recovery_generation: u64,
}

fn marker_from_identity(
    execution_profile: ExecutionProfile,
    identity: &CheckpointIdentity,
    node_id: &str,
) -> LocalCheckpointIdentityMarker {
    LocalCheckpointIdentityMarker {
        cluster_id: identity.cluster_id().to_owned(),
        node_id: node_id.to_owned(),
        execution_profile,
        epoch: identity.epoch(),
        config_id: identity.config_id(),
        recovery_generation: identity.recovery_generation(),
    }
}

fn encode_local_checkpoint_identity_marker(
    execution_profile: ExecutionProfile,
    identity: &CheckpointIdentity,
    node_id: &str,
) -> Result<Vec<u8>, HaStartupError> {
    serde_json::to_vec(&marker_from_identity(execution_profile, identity, node_id)).map_err(
        |encode_error| {
            fail(format!(
                "cannot encode local checkpoint identity marker: {encode_error}"
            ))
        },
    )
}

fn validate_local_checkpoint_identity_marker(
    marker: &LocalCheckpointIdentityMarker,
    execution_profile: ExecutionProfile,
    identity: &CheckpointIdentity,
    node_id: &str,
) -> Result<(), HaStartupError> {
    if marker.cluster_id != identity.cluster_id()
        || marker.node_id != node_id
        || marker.execution_profile != execution_profile
        || marker.epoch != identity.epoch()
        || marker.config_id != identity.config_id()
        || marker.recovery_generation != identity.recovery_generation()
    {
        return Err(fail(
            "local checkpoint identity marker does not exactly match the authoritative checkpoint",
        ));
    }
    Ok(())
}

fn exact_checkpoint_marker_exists(
    data_dir: &Path,
    execution_profile: ExecutionProfile,
    identity: &CheckpointIdentity,
    node_id: &str,
) -> Result<bool, HaStartupError> {
    match fs::symlink_metadata(data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE)) {
        Ok(_) => {
            read_and_validate_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                identity,
                node_id,
            )?;
            Ok(true)
        }
        Err(metadata_error) if metadata_error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(metadata_error) => Err(fail(format!(
            "cannot inspect local checkpoint identity marker: {metadata_error}"
        ))),
    }
}

fn read_and_validate_local_checkpoint_identity_marker(
    data_dir: &Path,
    execution_profile: ExecutionProfile,
    identity: &CheckpointIdentity,
    node_id: &str,
) -> Result<(), HaStartupError> {
    let bytes = read_bounded_regular_file_no_follow(
        &data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE),
        MAX_LOCAL_CHECKPOINT_IDENTITY_BYTES,
        "local checkpoint identity marker",
    )?;
    let marker: LocalCheckpointIdentityMarker = serde_json::from_slice(&bytes)
        .map_err(|_| fail("local checkpoint identity marker is invalid"))?;
    validate_local_checkpoint_identity_marker(&marker, execution_profile, identity, node_id)
}

fn write_local_checkpoint_identity_marker(
    data_dir: &Path,
    execution_profile: ExecutionProfile,
    identity: &CheckpointIdentity,
    node_id: &str,
    startup: &StartupIoContext,
) -> Result<(), HaStartupError> {
    let _permit =
        admit_ha_startup_local_io(startup, "local checkpoint identity marker publication")?;
    fs::create_dir_all(data_dir).map_err(|create_error| {
        fail(format!(
            "cannot create local data directory: {create_error}"
        ))
    })?;
    let metadata = fs::symlink_metadata(data_dir).map_err(|metadata_error| {
        fail(format!(
            "cannot inspect local data directory: {metadata_error}"
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(fail("local data directory must be a real directory"));
    }
    let marker_path = data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE);
    match fs::symlink_metadata(&marker_path) {
        Ok(_) => {
            #[cfg(test)]
            test_ha_startup_transaction_gate(TestHaStartupTransaction::MarkerPublication, data_dir);
            return read_and_validate_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                identity,
                node_id,
            );
        }
        Err(metadata_error) if metadata_error.kind() == io::ErrorKind::NotFound => {}
        Err(metadata_error) => {
            return Err(fail(format!(
                "cannot inspect local checkpoint identity marker: {metadata_error}"
            )));
        }
    }
    let bytes = encode_local_checkpoint_identity_marker(execution_profile, identity, node_id)?;
    let nonce = LOCAL_MARKER_NONCE.fetch_add(1, Ordering::Relaxed);
    let temporary = data_dir.join(format!(
        ".rhiza-checkpoint-identity.tmp-{}-{nonce}",
        std::process::id()
    ));
    let result = (|| -> Result<(), HaStartupError> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|create_error| {
                fail(format!(
                    "cannot create checkpoint identity marker: {create_error}"
                ))
            })?;
        file.write_all(&bytes).map_err(|write_error| {
            fail(format!(
                "cannot write checkpoint identity marker: {write_error}"
            ))
        })?;
        file.sync_all().map_err(|sync_error| {
            fail(format!(
                "cannot sync checkpoint identity marker: {sync_error}"
            ))
        })?;
        // The durable staging file exists, but this permit still owns the
        // atomic publication below.  The test-only gate therefore pauses a
        // real mutation, rather than merely startup admission.
        #[cfg(test)]
        test_ha_startup_transaction_gate(TestHaStartupTransaction::MarkerPublication, data_dir);
        match fs::hard_link(&temporary, &marker_path) {
            Ok(()) => {}
            Err(link_error) if link_error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(link_error) => {
                return Err(fail(format!(
                    "cannot atomically publish checkpoint identity marker: {link_error}"
                )));
            }
        }
        fs::remove_file(&temporary).map_err(|remove_error| {
            fail(format!(
                "cannot remove checkpoint marker staging file: {remove_error}"
            ))
        })?;
        fs::File::open(data_dir)
            .and_then(|directory| directory.sync_all())
            .map_err(|sync_error| {
                fail(format!("cannot sync local data directory: {sync_error}"))
            })?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result?;
    read_and_validate_local_checkpoint_identity_marker(
        data_dir,
        execution_profile,
        identity,
        node_id,
    )
}

static LOCAL_MARKER_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Serialize)]
struct SuccessorRestoreReceipt<'a> {
    cluster_id: &'a str,
    epoch: u64,
    target_config_id: u64,
    recovery_generation: u64,
    node_id: &'a str,
    membership_digest: String,
    predecessor_config_id: u64,
    stop_index: u64,
    stop_hash: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SuccessorRestoreControlState {
    Fresh,
    Intent,
    Complete,
}

fn expected_successor_restore_receipt(
    config: &NodeConfig,
    target_config_id: u64,
    predecessor: &HaPredecessor,
) -> Result<Vec<u8>, HaStartupError> {
    validate_predecessor_binding(config, target_config_id, predecessor)?;
    serde_json::to_vec(&SuccessorRestoreReceipt {
        cluster_id: config.cluster_id(),
        epoch: config.epoch(),
        target_config_id,
        recovery_generation: config.recovery_generation(),
        node_id: config.node_id(),
        membership_digest: config.membership().digest().to_hex(),
        predecessor_config_id: predecessor.stop.entry.config_id,
        stop_index: predecessor.stop.entry.index,
        stop_hash: predecessor.stop.entry.hash.to_hex(),
    })
    .map_err(|encode_error| {
        fail(format!(
            "cannot encode successor restore receipt: {encode_error}"
        ))
    })
}

fn validate_successor_restore_controls(
    data_dir: &Path,
    expected: &[u8],
) -> Result<SuccessorRestoreControlState, HaStartupError> {
    let intent = read_optional_bounded_regular_file_no_follow(
        &data_dir.join(SUCCESSOR_RESTORE_INTENT_FILE),
        MAX_SUCCESSOR_RESTORE_CONTROL_BYTES,
        "successor restore intent",
    )?;
    let complete = read_optional_bounded_regular_file_no_follow(
        &data_dir.join(SUCCESSOR_RESTORE_COMPLETE_FILE),
        MAX_SUCCESSOR_RESTORE_CONTROL_BYTES,
        "successor restore completion",
    )?;
    match (intent, complete) {
        (None, None) => Ok(SuccessorRestoreControlState::Fresh),
        (Some(actual), None) if actual == expected => Ok(SuccessorRestoreControlState::Intent),
        (None, Some(actual)) if actual == expected => Ok(SuccessorRestoreControlState::Complete),
        _ => Err(fail(
            "successor restore receipt does not exactly match this node and checkpoint",
        )),
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const O_NOFOLLOW_FLAG: i32 = 0o400000;
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
const O_NOFOLLOW_FLAG: i32 = 0x0100;

fn open_read_no_follow(path: &Path) -> io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(O_NOFOLLOW_FLAG);
    }
    options.open(path)
}

fn read_bounded_regular_file_no_follow(
    path: &Path,
    max_bytes: u64,
    label: &str,
) -> Result<Vec<u8>, HaStartupError> {
    read_optional_bounded_regular_file_no_follow(path, max_bytes, label)?
        .ok_or_else(|| fail(format!("nonfresh rejoin requires a {label}")))
}

fn read_optional_bounded_regular_file_no_follow(
    path: &Path,
    max_bytes: u64,
    label: &str,
) -> Result<Option<Vec<u8>>, HaStartupError> {
    let mut file = match open_read_no_follow(path) {
        Ok(file) => file,
        Err(open_error) if open_error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(open_error) => {
            let is_symlink =
                fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink());
            return Err(
                if is_symlink || (cfg!(unix) && open_error.raw_os_error() == Some(40)) {
                    fail(format!("{label} must be a regular file"))
                } else {
                    fail(format!("cannot open {label}: {open_error}"))
                },
            );
        }
    };
    let before = file
        .metadata()
        .map_err(|metadata_error| fail(format!("cannot inspect open {label}: {metadata_error}")))?;
    if !before.is_file() {
        return Err(fail(format!("{label} must be a regular file")));
    }
    if before.len() == 0 || before.len() > max_bytes {
        return Err(fail(format!("{label} has an invalid size")));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(before.len()).unwrap_or(0));
    Read::by_ref(&mut file)
        .take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|read_error| fail(format!("cannot read {label}: {read_error}")))?;
    let after = file
        .metadata()
        .map_err(|metadata_error| fail(format!("cannot inspect open {label}: {metadata_error}")))?;
    if !after.is_file()
        || after.len() != before.len()
        || bytes.len() as u64 != before.len()
        || bytes.len() as u64 > max_bytes
    {
        return Err(fail(format!("{label} changed during bounded read")));
    }
    Ok(Some(bytes))
}

fn local_data_is_fresh(data_dir: &Path) -> Result<bool, HaStartupError> {
    for path in [
        data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE),
        data_dir.join("consensus/log"),
        data_dir.join("sqlite"),
        data_dir.join("ladybug"),
        data_dir.join("kv"),
        data_dir.join("recorder"),
        data_dir.join("consensus/recorder"),
    ] {
        if path_has_state(&path).map_err(error)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn path_has_state(path: &Path) -> io::Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(metadata_error) if metadata_error.kind() == io::ErrorKind::NotFound => {
            return Ok(false);
        }
        Err(metadata_error) => return Err(metadata_error),
    };
    if !metadata.is_dir() {
        return Ok(true);
    }
    fs::read_dir(path)?
        .next()
        .transpose()
        .map(|entry| entry.is_some())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::*;

    fn startup_fence_test_identity() -> CheckpointIdentity {
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"ha-test-config"]),
            1,
        )
    }

    async fn wait_for_ha_startup_transaction(entered: std::sync::mpsc::Receiver<()>) {
        tokio::time::timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || entered.recv()),
        )
        .await
        .expect("HA startup transaction did not reach its scoped gate")
        .expect("HA startup transaction receiver task must not panic")
        .expect("HA startup transaction gate dropped before entry");
    }

    async fn assert_startup_close_is_pending(
        startup: &StartupIoContext,
        receipt: &rhiza_node::StartupCloseReceipt,
    ) {
        let mut close = Box::pin(startup.await_close(receipt));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut close)
                .await
                .is_err(),
            "the same close receipt must remain pending while the real transaction is gated"
        );
    }

    #[tokio::test]
    async fn checkpoint_prepare_slots_cap_waiters_and_close_cancels_without_local_effect() {
        let slots = tokio::sync::Semaphore::new(2);
        let first_startup = StartupIoContext::new();
        let second_startup = StartupIoContext::new();
        let first = acquire_checkpoint_prepare_permit(&slots, &first_startup)
            .await
            .unwrap();
        let second = acquire_checkpoint_prepare_permit(&slots, &second_startup)
            .await
            .unwrap();
        assert_eq!(slots.available_permits(), 0);

        let waiting_startup = StartupIoContext::new();
        let mut waiting = Box::pin(acquire_checkpoint_prepare_permit(&slots, &waiting_startup));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(waiting.as_mut().poll(&mut context).is_pending());

        let receipt = waiting_startup.cancel(
            std::time::Instant::now()
                .checked_add(Duration::from_secs(1))
                .unwrap(),
        );
        assert!(matches!(waiting.await, Err(HaStartupError::Cancelled(_))));
        assert_eq!(
            waiting_startup.await_close(&receipt).await,
            rhiza_node::StartupCloseOutcome::Drained,
            "a cancelled prepare waiter must never own local startup I/O"
        );
        assert_eq!(slots.available_permits(), 0);

        drop(first);
        assert_eq!(slots.available_permits(), 1);
        drop(second);
        assert_eq!(slots.available_permits(), 2);
    }

    #[tokio::test]
    async fn checkpoint_prepare_slot_is_raii_on_success_error_and_panic() {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));

        {
            let startup = StartupIoContext::new();
            let _permit = acquire_checkpoint_prepare_permit(&slots, &startup)
                .await
                .unwrap();
        }
        assert_eq!(slots.available_permits(), 1);

        let error: Result<(), &'static str> = async {
            let startup = StartupIoContext::new();
            let _permit = acquire_checkpoint_prepare_permit(&slots, &startup)
                .await
                .unwrap();
            Err("injected prepare error")
        }
        .await;
        assert_eq!(error.unwrap_err(), "injected prepare error");
        assert_eq!(slots.available_permits(), 1);

        let panic_slots = Arc::clone(&slots);
        let panic = tokio::spawn(async move {
            let startup = StartupIoContext::new();
            let _permit = acquire_checkpoint_prepare_permit(&panic_slots, &startup)
                .await
                .unwrap();
            panic!("injected prepare panic");
        })
        .await
        .unwrap_err();
        assert!(panic.is_panic());
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn checkpoint_prepare_slot_fails_closed_when_admission_is_closed() {
        let slots = tokio::sync::Semaphore::new(1);
        slots.close();
        let error = acquire_checkpoint_prepare_permit(&slots, &StartupIoContext::new())
            .await
            .unwrap_err();
        assert!(
            matches!(error, HaStartupError::Source(message) if message.contains("admission closed"))
        );
    }

    #[tokio::test]
    async fn startup_close_before_marker_admission_leaves_data_dir_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let data_dir = root.path().join("data");
        let startup = StartupIoContext::new();
        let receipt = startup.cancel(std::time::Instant::now() + Duration::from_secs(1));

        let error = write_local_checkpoint_identity_marker(
            &data_dir,
            ExecutionProfile::Sqlite,
            &startup_fence_test_identity(),
            "node-1",
            &startup,
        )
        .unwrap_err();

        assert!(matches!(error, HaStartupError::Cancelled(_)), "{error}");
        assert!(
            !data_dir.exists(),
            "close-first marker admission must not create data"
        );
        assert_eq!(
            startup.await_close(&receipt).await,
            rhiza_node::StartupCloseOutcome::Drained
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_close_waits_for_admitted_marker_transaction() {
        let root = tempfile::tempdir().unwrap();
        let data_dir = root.path().join("data");
        let startup = StartupIoContext::new();
        let (gate, entered_rx) = TestHaStartupTransactionGate::new(
            TestHaStartupTransaction::MarkerPublication,
            &data_dir,
        );
        let _gate = install_test_ha_startup_transaction_gate(gate.clone());
        let release = gate.release_guard();
        let worker_startup = startup.clone();
        let worker_data_dir = data_dir.clone();
        let worker = tokio::task::spawn_blocking(move || {
            write_local_checkpoint_identity_marker(
                &worker_data_dir,
                ExecutionProfile::Sqlite,
                &startup_fence_test_identity(),
                "node-1",
                &worker_startup,
            )
        });
        wait_for_ha_startup_transaction(entered_rx).await;

        let receipt = startup.cancel(std::time::Instant::now() + Duration::from_secs(1));
        assert_startup_close_is_pending(&startup, &receipt).await;
        assert!(
            data_dir.is_dir() && !data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE).exists(),
            "the gate must hold the durable marker staging transaction before its atomic publication"
        );

        drop(release);
        worker
            .await
            .expect("marker worker must not panic")
            .expect("admitted marker transaction must finish normally");
        assert!(data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE).is_file());
        assert_eq!(
            startup.await_close(&receipt).await,
            rhiza_node::StartupCloseOutcome::Drained,
            "the completion receipt must observe the released marker publication"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admitted_local_effect_preserves_source_error_after_close() {
        let root = tempfile::tempdir().unwrap();
        let data_dir = root.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(
            data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE),
            b"invalid marker",
        )
        .unwrap();
        let startup = StartupIoContext::new();
        let (gate, entered_rx) = TestHaStartupTransactionGate::new(
            TestHaStartupTransaction::MarkerPublication,
            &data_dir,
        );
        let _gate = install_test_ha_startup_transaction_gate(gate.clone());
        let release = gate.release_guard();
        let worker_startup = startup.clone();
        let worker_data_dir = data_dir.clone();
        let worker = tokio::task::spawn_blocking(move || {
            write_local_checkpoint_identity_marker(
                &worker_data_dir,
                ExecutionProfile::Sqlite,
                &startup_fence_test_identity(),
                "node-1",
                &worker_startup,
            )
        });
        wait_for_ha_startup_transaction(entered_rx).await;

        let receipt = startup.cancel(std::time::Instant::now() + Duration::from_secs(1));
        assert_startup_close_is_pending(&startup, &receipt).await;
        drop(release);
        let error = worker
            .await
            .expect("marker worker must not panic")
            .unwrap_err();
        assert!(
            matches!(error, HaStartupError::Source(ref message) if message == "local checkpoint identity marker is invalid"),
            "post-admission source error must not be overwritten by cancellation: {error}"
        );
        assert_eq!(
            startup.await_close(&receipt).await,
            rhiza_node::StartupCloseOutcome::Drained
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_transaction_gate_does_not_block_the_same_operation_at_another_path() {
        let root = tempfile::tempdir().unwrap();
        let blocked_data_dir = root.path().join("blocked");
        let unrelated_data_dir = root.path().join("unrelated");
        let blocked_startup = StartupIoContext::new();
        let unrelated_startup = StartupIoContext::new();
        let (gate, entered_rx) = TestHaStartupTransactionGate::new(
            TestHaStartupTransaction::MarkerPublication,
            &blocked_data_dir,
        );
        // If an assertion unwinds, releasing in Drop leaves the blocked real
        // transaction able to complete before the hook is uninstalled.
        let _gate = install_test_ha_startup_transaction_gate(gate.clone());
        let release = gate.release_guard();
        let blocked_worker_startup = blocked_startup.clone();
        let blocked_worker_data_dir = blocked_data_dir.clone();
        let blocked_worker = tokio::task::spawn_blocking(move || {
            write_local_checkpoint_identity_marker(
                &blocked_worker_data_dir,
                ExecutionProfile::Sqlite,
                &startup_fence_test_identity(),
                "node-1",
                &blocked_worker_startup,
            )
        });
        wait_for_ha_startup_transaction(entered_rx).await;

        let unrelated_worker = tokio::task::spawn_blocking(move || {
            write_local_checkpoint_identity_marker(
                &unrelated_data_dir,
                ExecutionProfile::Sqlite,
                &startup_fence_test_identity(),
                "node-1",
                &unrelated_startup,
            )
        });
        tokio::time::timeout(Duration::from_millis(250), unrelated_worker)
            .await
            .expect("a path-scoped gate must not block an unrelated same-operation transaction")
            .expect("unrelated marker worker must not panic")
            .expect("unrelated marker transaction must finish normally");
        assert!(
            root.path()
                .join("unrelated")
                .join(LOCAL_CHECKPOINT_IDENTITY_FILE)
                .is_file(),
            "the unrelated transaction must publish while the scoped gate is still held"
        );

        drop(release);
        blocked_worker
            .await
            .expect("blocked marker worker must not panic")
            .expect("blocked marker transaction must finish after release");
        assert!(
            blocked_data_dir
                .join(LOCAL_CHECKPOINT_IDENTITY_FILE)
                .is_file(),
            "the blocked transaction must publish after its own release"
        );
    }

    #[tokio::test]
    async fn closed_startup_does_not_recover_or_create_recorder() {
        let root = tempfile::tempdir().unwrap();
        let startup = StartupIoContext::new();
        let receipt = startup.cancel(std::time::Instant::now() + Duration::from_secs(1));
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let recovery_data_dir = root.path().join("recoverable");
        let recovery = recover_local_recorder_before_view_recovery(
            LocalRecorderState::Recoverable,
            &recovery_data_dir,
            "node-1",
            &startup_fence_test_identity(),
            &membership,
            &startup,
        )
        .unwrap_err();
        assert!(
            matches!(recovery, HaStartupError::Cancelled(_)),
            "{recovery}"
        );
        assert!(
            !recovery_data_dir.join("recorder").exists(),
            "close-first recovery must not open or repair the recorder"
        );

        let config = actual_child_test_node_config(&root.path().join("missing"));
        let create = open_recorder_for_preparation(
            &config,
            config.config_id(),
            RecorderOpenPolicy::CreateAfterRehydration,
            &startup,
        )
        .unwrap_err();
        assert!(matches!(create, HaStartupError::Cancelled(_)), "{create}");
        assert!(
            !config.data_dir().join("recorder").exists(),
            "close-first missing-recorder startup must not create a recorder root"
        );
        assert_eq!(
            startup.await_close(&receipt).await,
            rhiza_node::StartupCloseOutcome::Drained
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successor_adoption_does_not_readmit_substeps_after_close() {
        let root = tempfile::tempdir().unwrap();
        let archive = actual_child_test_archive(&root.path().join("archive"));
        archive.initialize_checkpoint().await.unwrap();
        let source_config = actual_child_test_node_config(&root.path().join("source"));
        let source_cluster_id = source_config.cluster_id().to_owned();
        let source_epoch = source_config.epoch();
        let source_config_id = source_config.config_id();
        let predecessor_membership = source_config.membership().clone();
        let source_consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                source_config.cluster_id().to_owned(),
                source_config.node_id().to_owned(),
                source_config.epoch(),
                source_config.config_id(),
                actual_child_test_recorders(&root.path().join("source-recorders")),
            )
            .unwrap(),
        );
        let source = NodeRuntime::open(source_config, source_consensus, &[]).unwrap();
        let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
            .await
            .unwrap();
        source.write("startup-fence-seed", "key", "value").unwrap();
        source.checkpoint_compact(&coordinator).await.unwrap();
        let successor_membership = Membership::new(["node-4", "node-5", "node-6"]).unwrap();
        let stop = source
            .stop_current_configuration_for_successor(&successor_membership)
            .unwrap();
        let source_configuration = source.configuration_state().unwrap();
        let predecessor = HaPredecessor::new(predecessor_membership.clone(), stop.clone());
        drop(source);

        let target_data_dir = root.path().join("successor");
        let target_peers = successor_membership
            .members()
            .iter()
            .enumerate()
            .map(|(index, node_id)| {
                rhiza_node::PeerConfig::new(
                    node_id,
                    format!("http://127.0.0.1:{}", 39201 + index),
                    format!("successor-peer-token-{}", index + 1),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let config = NodeConfig::new_with_configuration(
            source_cluster_id,
            "node-4",
            target_data_dir,
            source_epoch,
            successor_membership.clone(),
            source_configuration,
            target_peers,
            "successor-client-token",
        )
        .unwrap()
        .with_log_initial_configuration(rhiza_core::ConfigurationState::active(
            source_config_id,
            predecessor_membership.digest(),
        ))
        .with_predecessor_stop_entry(stop.entry.clone());
        let learner = HaSuccessorPrestageConfig::new(
            archive,
            root.path().join("prestage"),
            "node-4",
            ExecutionProfile::Sqlite,
            predecessor_membership.clone(),
            successor_membership,
            "tail-token",
        )
        .prepare()
        .await
        .unwrap()
        .publish(config.data_dir())
        .unwrap();
        let stop_anchor = LogAnchor::new(stop.entry.index, stop.entry.hash);
        let request = learner.tail_request(8).unwrap();
        learner
            .apply_page(
                &request,
                &CertifiedTailResponse {
                    records: vec![rhiza_node::CertifiedTailRecord {
                        entry: stop.entry.clone(),
                        proof: stop.proof.clone(),
                    }],
                    observed_tip: stop_anchor,
                },
            )
            .unwrap();
        drop(learner);
        let prestage = inspect_successor_prestage(
            config.data_dir(),
            config.log_initial_configuration().clone(),
        )
        .unwrap();
        assert_eq!(prestage.state(), SuccessorPrestageState::Published);
        let finalized = rhiza_node::durability::finalize_successor_prestage_for_stop(
            prestage,
            &stop,
            &predecessor_membership,
        )
        .unwrap();
        assert_eq!(finalized.state(), SuccessorPrestageState::Finalized);
        drop(finalized);

        let startup = StartupIoContext::new();
        let (gate, entered_rx) = TestHaStartupTransactionGate::new(
            TestHaStartupTransaction::SuccessorPrestageAdoption,
            config.data_dir(),
        );
        let _gate = install_test_ha_startup_transaction_gate(gate.clone());
        let release = gate.release_guard();
        let worker_startup = startup.clone();
        let worker_config = config.clone();
        let worker = tokio::task::spawn_blocking(move || {
            adopt_successor_prestage_for_startup(&worker_config, &predecessor, &worker_startup)
        });
        wait_for_ha_startup_transaction(entered_rx).await;

        assert!(
            config
                .data_dir()
                .join(SUCCESSOR_RESTORE_INTENT_FILE)
                .is_file(),
            "the scoped gate must be reached only after the real finalized prestage is adopted"
        );
        let receipt = startup.cancel(std::time::Instant::now() + Duration::from_secs(1));
        assert_startup_close_is_pending(&startup, &receipt).await;
        assert!(
            startup
                .admit_local_io("forbidden nested successor-adoption substep")
                .is_err(),
            "close must reject a nested admission while the original successor transaction drains"
        );

        drop(release);
        worker
            .await
            .expect("successor-adoption worker must not panic")
            .expect("the real finalized successor prestage adoption must finish normally");
        assert!(
            config
                .data_dir()
                .join(SUCCESSOR_RESTORE_INTENT_FILE)
                .is_file(),
            "the adopted successor restore intent must survive transaction completion"
        );
        assert_eq!(
            startup.await_close(&receipt).await,
            rhiza_node::StartupCloseOutcome::Drained
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejoin_cleanup_is_owned_by_one_startup_permit() {
        let root = tempfile::tempdir().unwrap();
        let data_dir = root.path().join("rejoin");
        let config = actual_child_test_node_config(&data_dir);
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                config.cluster_id().to_owned(),
                config.node_id().to_owned(),
                config.epoch(),
                config.config_id(),
                actual_child_test_recorders(&root.path().join("recorders")),
            )
            .unwrap(),
        );
        let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();
        drop(runtime);
        assert!(
            data_dir.join("sqlite/db.sqlite").is_file(),
            "the test must exercise a real local materializer open, not an empty-directory error"
        );
        let startup = StartupIoContext::new();
        let (gate, entered_rx) = TestHaStartupTransactionGate::new(
            TestHaStartupTransaction::RejoinRecoveryViewCleanupAndOpen,
            &data_dir,
        );
        let _gate = install_test_ha_startup_transaction_gate(gate.clone());
        let release = gate.release_guard();
        let worker_startup = startup.clone();
        let worker_data_dir = data_dir.clone();
        let worker = tokio::task::spawn_blocking(move || {
            validate_rejoin_recovery_view_for_startup(
                &worker_data_dir,
                &startup_fence_test_identity(),
                "node-1",
                ExecutionProfile::Sqlite,
                LogAnchor::new(0, LogHash::ZERO),
                &worker_startup,
            )
        });
        wait_for_ha_startup_transaction(entered_rx).await;

        let receipt = startup.cancel(std::time::Instant::now() + Duration::from_secs(1));
        assert_startup_close_is_pending(&startup, &receipt).await;

        drop(release);
        worker
            .await
            .expect("recovery-view worker must not panic")
            .expect(
                "the real recovery-view cleanup and materializer/qlog open must finish normally",
            );
        assert!(
            data_dir.join("sqlite/db.sqlite").is_file(),
            "the successful recovery-view transaction must retain its validated local materializer"
        );
        assert_eq!(
            startup.await_close(&receipt).await,
            rhiza_node::StartupCloseOutcome::Drained
        );
    }

    #[derive(Clone)]
    struct UnusedCertifiedTailSource;

    impl HaCertifiedTailSource for UnusedCertifiedTailSource {
        fn fetch<'a>(
            &'a self,
            _request: &'a CertifiedTailRequest,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<CertifiedTailResponse, HaCertifiedTailError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async { panic!("prepare failure must not fetch a certified tail") })
        }
    }

    /// A production-shaped tail transport that proves the live successor has
    /// completed staging/prestage and is waiting before the first Stop.  It
    /// never returns, so a test can make public-owner Drop the sole cause of
    /// cancellation without racing child activation.
    #[derive(Clone)]
    struct EnteredPendingTailSource {
        entered: std::sync::mpsc::SyncSender<()>,
    }

    impl HaCertifiedTailSource for EnteredPendingTailSource {
        fn fetch<'a>(
            &'a self,
            _request: &'a CertifiedTailRequest,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<CertifiedTailResponse, HaCertifiedTailError>>
                    + Send
                    + 'a,
            >,
        > {
            let entered = self.entered.clone();
            Box::pin(async move {
                let _ = entered.send(());
                std::future::pending::<Result<CertifiedTailResponse, HaCertifiedTailError>>().await
            })
        }
    }

    enum TestServiceTaskExit {
        Error(&'static str),
        Panic(&'static str),
    }

    fn test_service_ingress_completion(
        receipt: bool,
        exit: TestServiceTaskExit,
    ) -> TrackedAxumIngress {
        let (listener_dropped, listener_dropped_rx) = tokio::sync::oneshot::channel();
        let (force, _) = tokio::sync::watch::channel(false);
        let task = HaServerTask::spawn(async move {
            if receipt {
                let _ = listener_dropped.send(());
            } else {
                drop(listener_dropped);
            }
            match exit {
                TestServiceTaskExit::Error(message) => {
                    Err(HaNodeError::ServiceServer(message.into()))
                }
                TestServiceTaskExit::Panic(message) => panic!("{message}"),
            }
        });
        TrackedAxumIngress {
            task,
            listener_dropped: listener_dropped_rx,
            listener_receipted: false,
            force,
            forced: Arc::new(AtomicBool::new(false)),
        }
    }

    #[cfg(feature = "test-hooks")]
    fn test_service_ingress_error_with_receipt_and_ready(
        message: &'static str,
    ) -> (TrackedAxumIngress, tokio::sync::oneshot::Receiver<()>) {
        let (listener_dropped, listener_dropped_rx) = tokio::sync::oneshot::channel();
        let (ready, ready_rx) = tokio::sync::oneshot::channel();
        let (force, _) = tokio::sync::watch::channel(false);
        let task = HaServerTask::spawn(async move {
            let _ = listener_dropped.send(());
            let _ = ready.send(());
            Err(HaNodeError::ServiceServer(message.into()))
        });
        (
            TrackedAxumIngress {
                task,
                listener_dropped: listener_dropped_rx,
                listener_receipted: false,
                force,
                forced: Arc::new(AtomicBool::new(false)),
            },
            ready_rx,
        )
    }

    fn actual_child_test_archive(root: &std::path::Path) -> ObjectArchiveStore {
        let config = actual_child_test_node_config(&root.join("identity-only"));
        actual_child_test_archive_for_cluster(
            root,
            config.cluster_id(),
            config.log_initial_configuration().digest(),
        )
    }

    fn actual_child_test_archive_for_cluster(
        root: &std::path::Path,
        cluster_id: impl Into<String>,
        config_digest: LogHash,
    ) -> ObjectArchiveStore {
        ObjectArchiveStore::new_checkpoint_for_single_process(
            rhiza_obj_store::ObjStore::new(rhiza_obj_store::ObjStoreConfig::Local {
                root: root.to_path_buf(),
            })
            .unwrap(),
            CheckpointIdentity::new(cluster_id, 1, 1, config_digest, 1),
        )
    }

    fn actual_child_test_node_config(data_dir: &std::path::Path) -> NodeConfig {
        actual_child_test_node_config_with_profile(data_dir, ExecutionProfile::Sqlite)
    }

    fn actual_child_test_node_config_with_profile(
        data_dir: &std::path::Path,
        profile: ExecutionProfile,
    ) -> NodeConfig {
        let peers = ["node-1", "node-2", "node-3"]
            .into_iter()
            .enumerate()
            .map(|(index, node_id)| {
                rhiza_node::PeerConfig::new(
                    node_id,
                    format!("http://127.0.0.1:{}", 39101 + index),
                    format!("peer-token-{}", index + 1),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        NodeConfig::new(
            "cluster-a",
            "node-1",
            data_dir.to_path_buf(),
            1,
            1,
            peers,
            "client-token",
        )
        .unwrap()
        .with_execution_profile(profile)
        .unwrap()
    }

    #[derive(Clone, Copy, Debug)]
    enum StandardCheckpointRestoreBranch {
        FreshRejoin,
        InterruptedRejoin,
        InterruptedGraphRejoin,
        RecoveryViewRebuildRejoin,
        Disaster,
    }

    impl StandardCheckpointRestoreBranch {
        fn mode(self) -> HaStartupMode {
            match self {
                Self::FreshRejoin
                | Self::InterruptedRejoin
                | Self::InterruptedGraphRejoin
                | Self::RecoveryViewRebuildRejoin => HaStartupMode::Rejoin,
                Self::Disaster => HaStartupMode::Disaster,
            }
        }

        fn execution_profile(self) -> ExecutionProfile {
            match self {
                Self::InterruptedGraphRejoin => ExecutionProfile::Graph,
                Self::FreshRejoin
                | Self::InterruptedRejoin
                | Self::RecoveryViewRebuildRejoin
                | Self::Disaster => ExecutionProfile::Sqlite,
            }
        }

        fn label(self) -> &'static str {
            match self {
                Self::FreshRejoin => "fresh rejoin",
                Self::InterruptedRejoin => "interrupted rejoin",
                Self::InterruptedGraphRejoin => "interrupted graph rejoin",
                Self::RecoveryViewRebuildRejoin => "recovery-view rebuild rejoin",
                Self::Disaster => "disaster",
            }
        }
    }

    async fn standard_checkpoint_restore_test_context(
        root: &std::path::Path,
        branch: StandardCheckpointRestoreBranch,
    ) -> (NodeConfig, ObjectArchiveStore) {
        let data_dir = root.join("data");
        let config =
            actual_child_test_node_config_with_profile(&data_dir, branch.execution_profile());
        let archive = actual_child_test_archive_for_cluster(
            &root.join("archive"),
            config.cluster_id().to_owned(),
            config.log_initial_configuration().digest(),
        );
        archive.initialize_checkpoint().await.unwrap();

        // Seed only the durable preconditions that select each standard
        // branch.  The actual checkpoint install remains exclusively owned by
        // `prepare_standard`, so the gates below observe the production path.
        match branch {
            StandardCheckpointRestoreBranch::FreshRejoin
            | StandardCheckpointRestoreBranch::Disaster => {}
            StandardCheckpointRestoreBranch::InterruptedRejoin
            | StandardCheckpointRestoreBranch::InterruptedGraphRejoin => {
                let prepared = rhiza_node::durability::prepare_checkpoint_restore(&archive)
                    .await
                    .unwrap();
                let identity = prepared.identity();
                let checkpoint_root = prepared.checkpoint_root();
                fs::create_dir_all(&data_dir).unwrap();
                // This is the production serializer, not a map-shaped test
                // approximation: recovery compares the persisted bytes
                // byte-for-byte before it permits local cleanup.
                let intent = rhiza_node::durability::checkpoint_restore_intent_bytes(
                    identity,
                    config.node_id(),
                    config.execution_profile(),
                    checkpoint_root,
                )
                .unwrap();
                fs::write(data_dir.join(".rhiza-restore.json"), intent).unwrap();
            }
            StandardCheckpointRestoreBranch::RecoveryViewRebuildRejoin => {
                let prepared = rhiza_node::durability::prepare_checkpoint_restore(&archive)
                    .await
                    .unwrap();
                write_local_checkpoint_identity_marker(
                    &data_dir,
                    config.execution_profile(),
                    prepared.identity(),
                    config.node_id(),
                    &StartupIoContext::new(),
                )
                .unwrap();
                // Make the actual SQLite recovery-view validation fail after
                // marker validation, while leaving a real rebuildable
                // component for the recorder-preserving installer to
                // quarantine. The post-install gate below must observe this
                // sentinel moved out of the live recovery view.
                fs::create_dir_all(data_dir.join("sqlite")).unwrap();
                fs::write(
                    data_dir.join("sqlite/db.sqlite"),
                    b"rebuildable sqlite sentinel",
                )
                .unwrap();
            }
        }
        (config, archive)
    }

    async fn run_standard_restore_through_local_install_gate(
        branch: StandardCheckpointRestoreBranch,
    ) {
        let root = tempfile::tempdir().unwrap();
        let (config, archive) = standard_checkpoint_restore_test_context(root.path(), branch).await;
        let data_dir = config.data_dir().to_path_buf();
        let startup = StartupIoContext::new();
        let (gate, entered_rx) = TestHaStartupTransactionGate::new(
            TestHaStartupTransaction::CheckpointRestoreLocalInstallComplete,
            &data_dir,
        );
        let _gate = install_test_ha_startup_transaction_gate(gate.clone());
        let release = gate.release_guard();
        let worker_config = config.clone();
        let worker_membership = config.membership().clone();
        let worker_startup = startup.clone();
        let mut worker = tokio::spawn(async move {
            prepare_standard(
                &worker_config,
                &archive,
                branch.mode(),
                &worker_membership,
                &worker_startup,
            )
            .await
        });
        tokio::select! {
            () = wait_for_ha_startup_transaction(entered_rx) => {}
            result = &mut worker => panic!(
                "{} restore ended before its post-install transaction gate: {result:?}",
                branch.label(),
            ),
        }

        let receipt = startup.cancel(std::time::Instant::now() + Duration::from_secs(1));
        assert_startup_close_is_pending(&startup, &receipt).await;

        // The fixture deliberately restores an initialized genesis checkpoint,
        // which has no materializer snapshot to promote. Its observable local
        // install effect is instead the durable caller marker after the
        // staging transaction has completed and the generic intent is gone.
        // This proves the gate is after the real installer rather than after
        // an impossible SQLite materializer assertion for genesis.
        assert!(
            data_dir.is_dir(),
            "{} must create the local data directory before the post-install gate",
            branch.label(),
        );
        assert!(
            data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE).is_file(),
            "{} must reach the local gate only after the completion marker is published",
            branch.label(),
        );
        assert!(
            !data_dir.join(".rhiza-restore.json").exists(),
            "{} must remove its restore intent before the local transaction is released",
            branch.label(),
        );
        assert!(
            std::fs::read_dir(&data_dir).unwrap().all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".restore-stage-")),
            "{} must remove its restore staging directory before the post-install gate",
            branch.label(),
        );

        if matches!(
            branch,
            StandardCheckpointRestoreBranch::RecoveryViewRebuildRejoin
        ) {
            assert!(
                !data_dir.join("sqlite/db.sqlite").exists(),
                "the rebuildable SQLite sentinel must leave the live view before the gated post-install point"
            );
            let quarantine = fs::read_dir(&data_dir)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .find(|path| {
                    path.file_name().is_some_and(|name| {
                        name.to_string_lossy()
                            .starts_with(".rebuildable-quarantine-")
                    })
                })
                .expect("rebuildable rejoin must quarantine the prior SQLite view");
            assert_eq!(
                fs::read(quarantine.join("sqlite/db.sqlite")).unwrap(),
                b"rebuildable sqlite sentinel",
                "the gate must observe the actual preexisting SQLite bytes quarantined while close remains pending"
            );
        }

        drop(release);

        let error = worker
            .await
            .expect("standard checkpoint restore worker must not panic")
            .expect_err(
                "close after local install must be observed at the following startup check",
            );
        assert!(
            matches!(error, HaStartupError::Cancelled(_)),
            "{} must preserve close-first cancellation after its admitted local install: {error}",
            branch.label(),
        );
        assert_eq!(
            startup.await_close(&receipt).await,
            rhiza_node::StartupCloseOutcome::Drained,
            "{} close receipt must drain only after the gated installer releases its permit",
            branch.label(),
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn standard_restore_remote_prepare_is_unadmitted_at_entry_and_completion() {
        for (transaction, mode, label) in [
            (
                TestHaStartupTransaction::CheckpointRestoreRemotePrepareEntry,
                HaStartupMode::Rejoin,
                "remote prepare entry",
            ),
            (
                TestHaStartupTransaction::CheckpointRestoreRemotePrepareComplete,
                HaStartupMode::Disaster,
                "remote prepare completion",
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let (config, archive) = standard_checkpoint_restore_test_context(
                root.path(),
                if mode == HaStartupMode::Rejoin {
                    StandardCheckpointRestoreBranch::FreshRejoin
                } else {
                    StandardCheckpointRestoreBranch::Disaster
                },
            )
            .await;
            let data_dir = config.data_dir().to_path_buf();
            let startup = StartupIoContext::new();
            let (gate, entered_rx) = TestHaStartupTransactionGate::new(transaction, &data_dir);
            let _gate = install_test_ha_startup_transaction_gate(gate.clone());
            let release = gate.release_guard();
            let worker_config = config.clone();
            let worker_membership = config.membership().clone();
            let worker_startup = startup.clone();
            let worker = tokio::spawn(async move {
                prepare_standard(
                    &worker_config,
                    &archive,
                    mode,
                    &worker_membership,
                    &worker_startup,
                )
                .await
            });
            wait_for_ha_startup_transaction(entered_rx).await;

            let receipt = startup.cancel(std::time::Instant::now() + Duration::from_secs(1));
            assert_eq!(
                startup.await_close(&receipt).await,
                rhiza_node::StartupCloseOutcome::Drained,
                "{label} must not own a local-I/O permit",
            );
            assert!(
                !data_dir.exists(),
                "close at {label} must leave the fresh local data directory untouched",
            );

            drop(release);
            let error = worker
                .await
                .expect("remote-prepare worker must not panic")
                .expect_err("close before local admission must reject the standard restore");
            assert!(
                matches!(error, HaStartupError::Cancelled(_)),
                "{label} must report close-first cancellation after release: {error}",
            );
            assert!(
                !data_dir.exists(),
                "close at {label} must not permit a later local mutation",
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_standard_restore_branch_keeps_one_local_installer_permit_until_close_drains() {
        for branch in [
            StandardCheckpointRestoreBranch::FreshRejoin,
            StandardCheckpointRestoreBranch::InterruptedRejoin,
            StandardCheckpointRestoreBranch::RecoveryViewRebuildRejoin,
            StandardCheckpointRestoreBranch::Disaster,
        ] {
            run_standard_restore_through_local_install_gate(branch).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn interrupted_graph_rejoin_fresh_installer_keeps_its_direct_local_permit_until_close_drains(
    ) {
        // Graph rejoin intentionally takes the fresh installer instead of
        // the recorder-preserving installer. Keep this fifth branch separate
        // from the SQLite branch matrix so a later branch merge cannot make
        // its permit, close, or local-effect proof vacuous.
        run_standard_restore_through_local_install_gate(
            StandardCheckpointRestoreBranch::InterruptedGraphRejoin,
        )
        .await;
    }

    fn actual_child_test_recorders(root: &std::path::Path) -> Vec<(String, Box<dyn RecorderRpc>)> {
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        membership
            .members()
            .iter()
            .map(|node_id| {
                let recorder = RecorderFileStore::new_with_membership(
                    root.join(node_id),
                    node_id.clone(),
                    "rhiza:sql:cluster-a",
                    1,
                    1,
                    membership.clone(),
                )
                .unwrap();
                (node_id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
            })
            .collect()
    }

    async fn exercise_open_phase_requested_recorder_shutdown(
        shutdown_outcome: Option<TestRecorderShutdownOutcome>,
    ) -> (Result<(), HaNodeError>, HaNodeSnapshot) {
        let root = tempfile::tempdir().unwrap();
        let archive = actual_child_test_archive(&root.path().join("archive"));
        archive.initialize_checkpoint().await.unwrap();
        let gate = TestHaOpenPhaseGate::new();
        let release = gate.release_guard();
        let mut startup = HaStartupConfig::new(
            actual_child_test_node_config(&root.path().join("node")),
            archive,
            DurabilityMode::Sync,
            60_000,
            HaStartupMode::Bootstrap,
        );
        startup.open_phase_gate = Some(gate.clone());

        let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let recorder_address = recorder_listener.local_addr().unwrap();
        let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let service_address = service_listener.local_addr().unwrap();
        let (token_observer, token_observer_rx) = TestCleanupTokenObserver::new();
        let mut serve = HaServeConfig::new(
            recorder_listener,
            service_listener,
            HaRecorderTransport::Http,
            actual_child_test_recorders(&root.path().join("recorders")),
            Vec::new(),
        );
        serve.recorder_shutdown_outcome = shutdown_outcome;
        serve.open_shutdown_token_observer = Some(token_observer);
        let node = startup.start(serve);
        let mut state = node.state.clone();

        tokio::time::timeout(Duration::from_secs(5), gate.entered())
            .await
            .expect("actual HA open must reach the post-owner gate");
        let token = Arc::new(ShutdownToken::new(Duration::from_secs(5)));
        let token_identity = shutdown_token_identity(&token);
        let deadline = token.deadline();
        let shutdown = node.shutdown_with_token(Arc::clone(&token));
        tokio::pin!(shutdown);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if state.borrow().status == HaNodeStatus::ShuttingDown {
                    break;
                }
                tokio::select! {
                    result = &mut shutdown => {
                        panic!("open-phase shutdown returned before gated owner cleanup: {result:?}");
                    }
                    changed = state.changed() => {
                        changed.expect("HA state sender closed before ShuttingDown");
                    }
                }
            }
        })
        .await
        .expect("normal recorder completion must win while open remains gated");
        let (observed_identity, observed_deadline) = token_observer_rx
            .await
            .expect("requested-shutdown recorder branch must observe its token");
        assert_eq!(observed_identity, token_identity);
        assert_eq!(observed_deadline, deadline);

        drop(release);
        let result = tokio::time::timeout_at(deadline, &mut shutdown)
            .await
            .expect("opened owner cleanup must finish before the original deadline");
        let snapshot = state.borrow().clone();
        assert!(tokio::net::TcpStream::connect(recorder_address)
            .await
            .is_err());
        assert!(tokio::net::TcpStream::connect(service_address)
            .await
            .is_err());
        (result, snapshot)
    }

    fn test_http_recorder(root: &std::path::Path) -> (HaRecorder, Vec<rhiza_node::PeerConfig>) {
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let recorder = RecorderFileStore::new_with_membership(
            root.join("recorder"),
            "node-1",
            "cluster-a",
            1,
            1,
            membership,
        )
        .unwrap();
        (
            HaRecorder::active(recorder),
            vec![
                rhiza_node::PeerConfig::new("node-2", "http://node-2:8081", "peer-token-2")
                    .unwrap(),
            ],
        )
    }

    #[test]
    fn ha_recorder_forwards_external_effect_operations() {
        let root = tempfile::tempdir().unwrap();
        let (recorder, _) = test_http_recorder(root.path());
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let chunks = vec![b"ha-effect".to_vec()];
        let qefx = rhiza_core::ExternalEffectCommand::from_profile_bytes_and_chunks(
            "cluster-a",
            1,
            1,
            membership.digest(),
            1,
            LogHash::ZERO,
            rhiza_core::ExternalEffectProfile::sql(vec![1]),
            &chunks,
        )
        .unwrap();
        let manifest = StoredCommand::new(rhiza_core::EntryType::Command, qefx.encode().unwrap());
        let binding = EffectBundleBinding {
            cluster_id: qefx.cluster_id().into(),
            epoch: qefx.epoch(),
            config_id: qefx.config_id(),
            config_digest: qefx.config_digest(),
            intended_slot: qefx.intended_slot(),
            prev_hash: qefx.prev_hash(),
            manifest_command_hash: manifest.hash(),
            effect_digest: qefx.effect_digest_value(),
        };
        let context = rhiza_quepaxa::RecorderRpcContext::default_timeout();
        RecorderRpc::stage_effect_bundle_chunk(
            &recorder,
            &context,
            binding.clone(),
            manifest.clone(),
            0,
            chunks[0].clone(),
        )
        .unwrap();
        RecorderRpc::finalize_staged_effect_bundle(
            &recorder,
            &context,
            binding.clone(),
            manifest.clone(),
        )
        .unwrap();
        assert_eq!(
            RecorderRpc::fetch_effect_bundle_manifest(&recorder, &context, binding.clone(),)
                .unwrap(),
            Some(manifest)
        );
        assert_eq!(
            RecorderRpc::fetch_effect_bundle_chunk(&recorder, &context, binding, 0).unwrap(),
            Some(chunks[0].clone())
        );
    }

    fn test_recorder_tls_material() -> (String, String) {
        // `generate_simple_self_signed` puts this DNS name in subjectAltName;
        // TLS identity verification must not rely on a legacy CN fallback.
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["recorder.test".into()]).unwrap();
        (cert.pem(), signing_key.serialize_pem())
    }

    #[cfg(feature = "recorder-postcard-rpc")]
    #[derive(Clone)]
    struct HangingPostcardFetch {
        started: std::sync::mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, std::sync::Condvar)>,
        completed: Arc<AtomicUsize>,
    }

    #[cfg(feature = "recorder-postcard-rpc")]
    impl RecorderRpc for HangingPostcardFetch {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn fetch_command_for(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
            self.started.send(()).unwrap();
            let (released, ready) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }

    async fn recorder_identity_request(
        address: std::net::SocketAddr,
        authorization: Option<&str>,
    ) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let authorization = authorization
            .map(|token| format!("Authorization: Bearer {token}\r\n"))
            .unwrap_or_default();
        let body = r#"{"version":5,"remaining_deadline_ms":1000,"body":null}"#;
        let request = format!(
            "POST /v2/quepaxa/recorder/identity HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nx-rhiza-version: 5\r\nx-rhiza-node-id: node-2\r\nx-rhiza-recovery-generation: 1\r\n{authorization}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len(),
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }

    async fn staging_livez_request(address: std::net::SocketAddr) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /livez HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }

    async fn wait_for_staging_resource_backoff(backoffs: &mut tokio::sync::watch::Receiver<u64>) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while *backoffs.borrow() == 0 {
                backoffs
                    .changed()
                    .await
                    .expect("staging accept backoff observer closed");
            }
        })
        .await
        .expect("staging must enter resource accept backoff");
    }

    #[tokio::test]
    async fn recorder_http_ingress_keeps_peer_gate_and_receipts_listener_close() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let root = tempfile::tempdir().unwrap();
        let (recorder, peers) = test_http_recorder(root.path());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut task = spawn_ha_recorder_server(
            listener,
            recorder,
            HaRecorderTransport::Http,
            peers,
            1,
            shutdown_rx,
            started,
        );
        started_rx.await.unwrap();

        let rejected = recorder_identity_request(address, None).await;
        assert!(rejected.starts_with("HTTP/1.1 401"));
        let accepted = recorder_identity_request(address, Some("peer-token-2")).await;
        assert!(accepted.starts_with("HTTP/1.1 200"));
        assert!(accepted.contains("node-1"));

        // The same recorder Router must retain HTTP/1 keep-alive semantics;
        // the tracked owner only changes accept/drain ownership.
        let body = r#"{"version":5,"remaining_deadline_ms":1000,"body":null}"#;
        let request = |connection: &str| {
            format!(
                "POST /v2/quepaxa/recorder/identity HTTP/1.1\r\nHost: localhost\r\nConnection: {connection}\r\nx-rhiza-version: 5\r\nx-rhiza-node-id: node-2\r\nx-rhiza-recovery-generation: 1\r\nAuthorization: Bearer peer-token-2\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len(),
            )
        };
        let mut keepalive = tokio::net::TcpStream::connect(address).await.unwrap();
        keepalive
            .write_all(request("keep-alive").as_bytes())
            .await
            .unwrap();
        let mut first = [0_u8; 4096];
        let read = tokio::time::timeout(Duration::from_secs(1), keepalive.read(&mut first))
            .await
            .unwrap()
            .unwrap();
        assert!(std::str::from_utf8(&first[..read])
            .unwrap()
            .starts_with("HTTP/1.1 200"));
        keepalive
            .write_all(request("close").as_bytes())
            .await
            .unwrap();
        let mut second = Vec::new();
        keepalive.read_to_end(&mut second).await.unwrap();
        assert!(std::str::from_utf8(&second).unwrap().contains("200"));

        shutdown.send_replace(true);
        let (result, evidence) = wait_for_recorder_server(
            &mut task,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(
            evidence,
            Some(ShutdownEvidence {
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Quiesced,
            })
        );
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }

    #[tokio::test]
    async fn recorder_tcp_ingress_receipts_actual_close_and_quiesces_partial_reader() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let root = tempfile::tempdir().unwrap();
        let (recorder, peers) = test_http_recorder(root.path());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut task = spawn_ha_recorder_server(
            listener,
            recorder,
            HaRecorderTransport::TcpPostcard,
            peers,
            1,
            shutdown_rx,
            started,
        );
        started_rx.await.unwrap();

        // A partial length prefix is intentionally not resumable once the
        // shared shutdown closes this socket.  It exercises the concrete
        // node lifecycle through the HA owner rather than a synthetic receipt.
        let mut partial = tokio::net::TcpStream::connect(address).await.unwrap();
        partial.write_all(&[0, 0]).await.unwrap();
        shutdown.send_replace(true);
        let (result, evidence) = wait_for_recorder_server(
            &mut task,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(
            evidence,
            Some(ShutdownEvidence {
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Quiesced,
            })
        );
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
        let mut byte = [0_u8; 1];
        match partial.read(&mut byte).await {
            Ok(0) => {}
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
            result => panic!("partial recorder reader remained usable: {result:?}"),
        }
    }

    async fn assert_non_http_ha_transport_lifecycle<F, Fut>(
        recorder: HaRecorder,
        peers: Vec<rhiza_node::PeerConfig>,
        transport: HaRecorderTransport,
        recorder_id: F,
    ) where
        F: FnOnce(std::net::SocketAddr) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut task = spawn_ha_recorder_server(
            listener,
            recorder,
            transport,
            peers,
            1,
            shutdown_rx,
            started,
        );
        assert!(matches!(&task, RecorderServerTask::Tcp { .. }));

        // The node lifecycle sends this only after the listener's single FD
        // has entered the transport's RAII owner.
        started_rx.await.unwrap();
        recorder_id(address).await;

        shutdown.send_replace(true);
        let (result, evidence) = wait_for_recorder_server(
            &mut task,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert!(
            result.is_ok(),
            "non-HTTP recorder shutdown failed: {result:?}"
        );
        assert_eq!(
            evidence,
            Some(ShutdownEvidence {
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Quiesced,
            })
        );

        // A closed-and-quiesced result has to release the actual socket now,
        // not only after the outer task is subsequently dropped.
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }

    #[tokio::test]
    async fn every_non_http_ha_transport_dispatches_to_a_tracked_tcp_ingress_owner() {
        let root = tempfile::tempdir().unwrap();
        let (recorder, peers) = test_http_recorder(root.path());

        assert_non_http_ha_transport_lifecycle(
            recorder.clone(),
            peers.clone(),
            HaRecorderTransport::TcpPostcard,
            |address| async move {
                let client = rhiza_node::TcpPostcardRecorderClient::new(
                    address,
                    "node-1",
                    "node-2",
                    "peer-token-2",
                    1,
                )
                .unwrap();
                let recorder_id = tokio::task::spawn_blocking(move || {
                    client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
                })
                .await
                .unwrap()
                .unwrap();
                assert_eq!(recorder_id, "node-1");
            },
        )
        .await;

        let (cert_pem, key_pem) = test_recorder_tls_material();
        let tls =
            rhiza_node::RecorderTlsClientConfig::from_ca_pem(cert_pem.as_bytes(), "recorder.test")
                .unwrap();
        assert_non_http_ha_transport_lifecycle(
            recorder.clone(),
            peers.clone(),
            HaRecorderTransport::TcpTlsPostcard(
                RecorderTlsServerConfig::from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap(),
            ),
            move |address| async move {
                let client = rhiza_node::TcpPostcardRecorderClient::new_tls(
                    address,
                    "node-1",
                    "node-2",
                    "peer-token-2",
                    1,
                    tls,
                )
                .unwrap();
                // The WebPKI/SAN-validated framed client offers the framed
                // ALPN.  A successful authenticated identity proves that the
                // framed TLS handler, not merely the TLS handshake, won.
                let recorder_id = tokio::task::spawn_blocking(move || {
                    client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
                })
                .await
                .unwrap()
                .unwrap();
                assert_eq!(recorder_id, "node-1");
            },
        )
        .await;

        #[cfg(feature = "recorder-postcard-rpc")]
        assert_non_http_ha_transport_lifecycle(
            recorder.clone(),
            peers.clone(),
            HaRecorderTransport::TcpPostcardRpc,
            |address| async move {
                let client = rhiza_node::TcpPostcardRpcRecorderClient::new(
                    address,
                    "node-1",
                    "node-2",
                    "peer-token-2",
                    1,
                )
                .unwrap();
                let recorder_id = tokio::task::spawn_blocking(move || {
                    client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
                })
                .await
                .unwrap()
                .unwrap();
                assert_eq!(recorder_id, "node-1");
            },
        )
        .await;

        #[cfg(feature = "recorder-postcard-rpc")]
        {
            let (cert_pem, key_pem) = test_recorder_tls_material();
            let tls = rhiza_node::RecorderPostcardRpcTlsClientConfig::from_ca_pem(
                cert_pem.as_bytes(),
                "recorder.test",
            )
            .unwrap();
            assert_non_http_ha_transport_lifecycle(
                recorder,
                peers,
                HaRecorderTransport::TcpTlsPostcardRpc(
                    RecorderPostcardRpcTlsServerConfig::from_pem(
                        cert_pem.as_bytes(),
                        key_pem.as_bytes(),
                    )
                    .unwrap(),
                ),
                move |address| async move {
                    let client = rhiza_node::TcpPostcardRpcRecorderClient::new_tls(
                        address,
                        "node-1",
                        "node-2",
                        "peer-token-2",
                        1,
                        tls,
                    )
                    .unwrap();
                    // This client has a distinct postcard-RPC ALPN. Its
                    // authenticated identity establishes correct ALPN and
                    // postcard-RPC handler selection end to end.
                    let recorder_id = tokio::task::spawn_blocking(move || {
                        client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
                    })
                    .await
                    .unwrap()
                    .unwrap();
                    assert_eq!(recorder_id, "node-1");
                },
            )
            .await;
        }
    }

    #[tokio::test]
    async fn tcp_recorder_immediate_owner_drop_releases_the_actual_listener() {
        let root = tempfile::tempdir().unwrap();
        let (recorder, peers) = test_http_recorder(root.path());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let task = spawn_ha_recorder_server(
            listener,
            recorder,
            HaRecorderTransport::TcpPostcard,
            peers,
            1,
            shutdown_rx,
            started,
        );
        // This acknowledgement is sent only after the node lifecycle has
        // placed the real FD under its RAII listener owner.
        started_rx.await.unwrap();
        drop(task);
        let rebound = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match tokio::net::TcpListener::bind(address).await {
                    Ok(listener) => break listener,
                    Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("dropped TCP recorder listener rebind failed: {error}"),
                }
            }
        })
        .await
        .expect("dropped TCP recorder owner left its listener bound");
        drop(rebound);
    }

    #[test]
    fn recorder_shutdown_force_points_clamp_short_deadlines_and_preserve_order() {
        let now = tokio::time::Instant::now();
        let short = now + Duration::from_millis(10);
        let short_inner = clamped_before_deadline(short, Duration::from_millis(100));
        let short_outer = clamped_before_deadline(short, HA_SERVER_ABORT_RECEIPT_RESERVE);
        assert!(short_inner <= short_outer);
        assert!(short_outer <= short);

        let ordinary = now + Duration::from_millis(250);
        let ordinary_inner = clamped_before_deadline(ordinary, Duration::from_millis(100));
        let ordinary_outer = clamped_before_deadline(ordinary, HA_SERVER_ABORT_RECEIPT_RESERVE);
        assert!(ordinary_inner <= ordinary_outer);
        assert!(ordinary_outer <= ordinary);
    }

    #[tokio::test]
    async fn task_scope_keeps_pre_service_listener_evidence_explicit_without_placeholder_tasks() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut bound = HaTaskScope::new(
            AdminDrainLease::Disabled,
            ServiceTaskUnstarted::OwnedListenerDropped,
        );
        let bound_shutdown = bound.drain_before(deadline).await;
        assert!(bound_shutdown.result.is_ok());
        assert_eq!(bound_shutdown.evidence, PRE_SERVICE_SHUTDOWN_EVIDENCE);

        let mut deferred = HaTaskScope::new(
            AdminDrainLease::Disabled,
            ServiceTaskUnstarted::DeferredOwnerUnknown,
        );
        let deferred_shutdown = deferred.drain_before(deadline).await;
        assert!(deferred_shutdown.result.is_ok());
        assert_eq!(deferred_shutdown.evidence, UNCERTAIN_SHUTDOWN_EVIDENCE);
    }

    #[tokio::test]
    async fn task_scope_caches_service_completion_once_and_preserves_listener_receipt() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut scope = HaTaskScope::new(
            AdminDrainLease::Disabled,
            ServiceTaskUnstarted::OwnedListenerDropped,
        );
        scope.start_service(spawn_ha_service_server(
            listener,
            axum::Router::new(),
            scope.service_shutdown_receiver(),
            started,
        ));
        started_rx.await.unwrap();
        scope.service_shutdown.send_replace(true);
        let joined = tokio::time::timeout(
            Duration::from_secs(1),
            scope
                .running_service_task()
                .expect("service task is running before its completion"),
        )
        .await
        .expect("service task did not observe its scoped shutdown");
        assert!(scope.complete_running_service_task(joined).is_ok());
        assert!(scope.take_completed_service_result().is_none());

        let shutdown = scope
            .drain_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        assert!(shutdown.result.is_ok());
        assert_eq!(shutdown.evidence, PRE_SERVICE_SHUTDOWN_EVIDENCE);
    }

    #[tokio::test]
    async fn task_scope_drop_signals_service_shutdown_without_claiming_quiescence() {
        let scope = HaTaskScope::new(
            AdminDrainLease::Disabled,
            ServiceTaskUnstarted::DeferredOwnerUnknown,
        );
        let mut shutdown = scope.service_shutdown_receiver();
        drop(scope);
        shutdown.changed().await.unwrap();
        assert!(*shutdown.borrow());
    }

    #[tokio::test]
    async fn task_scope_preserves_exact_service_error_once_with_receipted_ingress() {
        let mut scope = HaTaskScope::new(
            AdminDrainLease::Disabled,
            ServiceTaskUnstarted::OwnedListenerDropped,
        );
        scope.start_service(test_service_ingress_completion(
            true,
            TestServiceTaskExit::Error("exact service source"),
        ));
        let joined = scope
            .running_service_task()
            .expect("test service is running")
            .await;
        assert!(matches!(
            scope.complete_running_service_task(joined),
            Err(HaNodeError::ServiceServer(message)) if message == "exact service source"
        ));
        assert!(scope.take_completed_service_result().is_none());

        let shutdown = scope
            .drain_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        assert!(
            shutdown.result.is_ok(),
            "the primary was already taken once"
        );
        assert_eq!(shutdown.evidence.ingress, IngressDisposition::Closed);
        assert_eq!(shutdown.evidence.tasks, TaskDisposition::Uncertain);
    }

    #[tokio::test]
    async fn task_scope_panic_without_listener_receipt_stays_uncertain() {
        let mut scope = HaTaskScope::new(
            AdminDrainLease::Disabled,
            ServiceTaskUnstarted::OwnedListenerDropped,
        );
        scope.start_service(test_service_ingress_completion(
            false,
            TestServiceTaskExit::Panic("test service panic"),
        ));
        let joined = scope
            .running_service_task()
            .expect("test service is running")
            .await;
        assert!(matches!(
            scope.complete_running_service_task(joined),
            Err(HaNodeError::ServiceServer(message)) if message.contains("task failed")
        ));
        let shutdown = scope
            .drain_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        assert!(shutdown.result.is_ok());
        assert_eq!(shutdown.evidence, UNCERTAIN_SHUTDOWN_EVIDENCE);
    }

    #[tokio::test]
    async fn global_shutdown_bias_does_not_swallow_ready_service_error() {
        let mut scope = HaTaskScope::new(
            AdminDrainLease::Disabled,
            ServiceTaskUnstarted::OwnedListenerDropped,
        );
        scope.start_service(test_service_ingress_completion(
            true,
            TestServiceTaskExit::Error("simultaneous service error"),
        ));
        let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
        shutdown.send_replace(true);
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => changed.unwrap(),
            result = scope.running_service_task().expect("test service is running") => {
                panic!("global shutdown must win the deliberately biased branch: {result:?}");
            }
        }

        let drained = scope
            .drain_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        assert!(matches!(
            drained.result,
            Err(HaNodeError::ServiceServer(message)) if message == "simultaneous service error"
        ));
        assert_eq!(drained.evidence.ingress, IngressDisposition::Closed);
        assert_eq!(drained.evidence.tasks, TaskDisposition::Uncertain);
    }

    #[tokio::test]
    async fn task_scope_short_deadline_force_keeps_receipted_ingress_and_uncertain_tasks() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let router = {
            let entered = Arc::clone(&entered);
            axum::Router::new().route(
                "/stuck",
                axum::routing::get(move || {
                    let entered = Arc::clone(&entered);
                    async move {
                        entered.notify_one();
                        std::future::pending::<()>().await;
                        axum::http::StatusCode::OK
                    }
                }),
            )
        };
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut scope = HaTaskScope::new(
            AdminDrainLease::Disabled,
            ServiceTaskUnstarted::OwnedListenerDropped,
        );
        scope.start_service(spawn_ha_service_server(
            listener,
            router,
            scope.service_shutdown_receiver(),
            started,
        ));
        started_rx.await.unwrap();

        let entered_wait = entered.notified();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /stuck HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .unwrap();

        scope.service_shutdown.send_replace(true);
        let shutdown = scope
            .drain_before(tokio::time::Instant::now() + Duration::from_millis(150))
            .await;
        assert!(shutdown.result.is_ok());
        assert_eq!(shutdown.evidence.ingress, IngressDisposition::Closed);
        assert_eq!(shutdown.evidence.tasks, TaskDisposition::Uncertain);
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
        drop(stream);
    }

    #[cfg(feature = "test-hooks")]
    #[tokio::test]
    async fn task_scope_stops_admin_admission_before_known_listener_drain() {
        let tracker = AdminTaskTracker::test_tracker();
        let admitted = tracker
            .test_start_admitted()
            .expect("operation is admitted before shutdown begins");
        let mut scope = HaTaskScope::new(
            AdminDrainLease::Enabled(tracker.clone()),
            ServiceTaskUnstarted::OwnedListenerDropped,
        );

        scope.begin_shutdown_for_test();
        assert!(
            tracker.test_start_admitted().is_none(),
            "shutdown closes admission before it begins draining the service"
        );
        let timed_out = scope
            .drain_before(tokio::time::Instant::now() + Duration::from_millis(20))
            .await;
        assert!(matches!(
            timed_out.result,
            Err(HaNodeError::ShutdownDeadlineExceeded {
                phase: HaShutdownPhase::Service,
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Uncertain,
                ..
            })
        ));
        assert_eq!(timed_out.evidence.ingress, IngressDisposition::Closed);
        assert_eq!(timed_out.evidence.tasks, TaskDisposition::Uncertain);

        drop(admitted);
        let drained = scope
            .drain_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        assert!(drained.result.is_ok());
        assert_eq!(drained.evidence, PRE_SERVICE_SHUTDOWN_EVIDENCE);
    }

    #[tokio::test]
    async fn tcp_accept_error_retains_exact_source_with_closed_quiesced_evidence() {
        let root = tempfile::tempdir().unwrap();
        let (recorder, peers) = test_http_recorder(root.path());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, _started_rx) = tokio::sync::oneshot::channel();
        let mut task = spawn_ha_recorder_server_inner(
            listener,
            recorder,
            HaRecorderTransport::TcpPostcard,
            peers,
            1,
            shutdown_rx,
            started,
            Some("recorder TCP accept failed: injected permission denied"),
            None,
        );
        let (result, evidence) = wait_for_recorder_server(
            &mut task,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert!(matches!(
            result,
            Err(HaNodeError::RecorderServer(message))
                if message == "recorder TCP accept failed: injected permission denied"
        ));
        assert_eq!(
            evidence,
            Some(ShutdownEvidence {
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Quiesced,
            })
        );
    }

    #[tokio::test]
    async fn tcp_outer_task_panic_or_abort_is_always_uncertain() {
        for aborted in [false, true] {
            let (receipt, receipt_rx) = tokio::sync::oneshot::channel();
            let _ = receipt.send(());
            let (force, _) = tokio::sync::watch::channel(false);
            let task = if aborted {
                HaServerTask::spawn(async {
                    std::future::pending::<Result<(), HaNodeError>>().await
                })
            } else {
                HaServerTask::spawn(async {
                    panic!("injected TCP recorder owner panic");
                    #[allow(unreachable_code)]
                    Ok::<(), HaNodeError>(())
                })
            };
            if aborted {
                task.abort();
            }
            let mut ingress = TrackedRecorderIngress {
                task,
                listener_dropped: receipt_rx,
                listener_receipted: false,
                force,
                forced: Arc::new(AtomicBool::new(false)),
                node_tasks: Arc::new(Mutex::new(Some(NodeRecorderTaskDisposition::Quiesced))),
            };
            let (result, ingress_evidence, tasks) = wait_for_tracked_recorder_ingress(
                &mut ingress,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await;
            assert!(result.is_err());
            assert_eq!(ingress_evidence, IngressDisposition::Closed);
            assert_eq!(tasks, TaskDisposition::Uncertain);
        }
    }

    #[cfg(feature = "recorder-postcard-rpc")]
    #[tokio::test(flavor = "multi_thread")]
    async fn ha_force_reaps_hung_postcard_call_joinset_without_claiming_quiescence() {
        let (backend_started, backend_started_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let completed = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let peers =
            vec![
                rhiza_node::PeerConfig::new("node-2", "http://node-2:8081", "peer-token-2")
                    .unwrap(),
            ];
        let recorder = HangingPostcardFetch {
            started: backend_started,
            release: Arc::clone(&release),
            completed: Arc::clone(&completed),
        };
        let mut ingress = spawn_tracked_recorder_ingress(
            listener,
            shutdown_rx,
            started,
            move |listener, lifecycle| {
                rhiza_node::serve_recorder_postcard_rpc(listener, recorder, peers, 1, lifecycle)
            },
        );
        started_rx.await.unwrap();
        let client = rhiza_node::TcpPostcardRpcRecorderClient::new(
            address,
            "node-1",
            "node-2",
            "peer-token-2",
            1,
        )
        .unwrap();
        let call = tokio::task::spawn_blocking(move || {
            client.fetch_command_for(
                &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2)),
                "cluster-a".into(),
                1,
                1,
                LogHash::ZERO,
                LogHash::ZERO,
            )
        });
        backend_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let (result, ingress_evidence, tasks) = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_tracked_recorder_ingress(
                &mut ingress,
                tokio::time::Instant::now() + Duration::from_millis(200),
            ),
        )
        .await
        .expect("HA force did not reap the hung postcard connection");
        assert!(result.is_ok());
        assert_eq!(ingress_evidence, IngressDisposition::Closed);
        assert_eq!(tasks, TaskDisposition::Uncertain);
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);

        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
        assert!(call.await.unwrap().is_err());
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn recorder_http_ingress_drop_never_detaches_the_listener_owner() {
        let root = tempfile::tempdir().unwrap();
        let (recorder, peers) = test_http_recorder(root.path());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let task = spawn_ha_recorder_server(
            listener,
            recorder,
            HaRecorderTransport::Http,
            peers,
            1,
            shutdown_rx,
            started,
        );
        started_rx.await.unwrap();
        drop(task);
        let rebound = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match tokio::net::TcpListener::bind(address).await {
                    Ok(listener) => break listener,
                    Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("recorder listener rebind failed: {error}"),
                }
            }
        })
        .await
        .expect("dropped recorder owner left its listener bound");
        drop(rebound);
    }

    #[tokio::test]
    async fn recorder_http_ingress_force_keeps_listener_closed_and_tasks_uncertain() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let entered = Arc::new(AtomicBool::new(false));
        let router = {
            let entered = Arc::clone(&entered);
            axum::Router::new().route(
                "/stuck",
                axum::routing::get(move || {
                    let entered = Arc::clone(&entered);
                    async move {
                        entered.store(true, Ordering::Release);
                        std::future::pending::<()>().await;
                        axum::http::StatusCode::OK
                    }
                }),
            )
        };
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut task = RecorderServerTask::Http {
            ingress: spawn_tracked_axum_ingress(listener, router, shutdown_rx, started, None),
            completed_evidence: None,
        };
        started_rx.await.unwrap();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /stuck HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        shutdown.send_replace(true);
        let (result, evidence) = wait_for_recorder_server(
            &mut task,
            tokio::time::Instant::now() + Duration::from_millis(200),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(
            evidence,
            Some(ShutdownEvidence {
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Uncertain,
            })
        );
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
        drop(stream);
    }

    fn completed_http_recorder_task(
        receipt_present: bool,
        result: Result<(), HaNodeError>,
    ) -> RecorderServerTask {
        let (receipt, receipt_rx) = tokio::sync::oneshot::channel();
        if receipt_present {
            let _ = receipt.send(());
        }
        let (force, _) = tokio::sync::watch::channel(false);
        RecorderServerTask::Http {
            ingress: TrackedAxumIngress {
                task: HaServerTask::spawn(async move { result }),
                listener_dropped: receipt_rx,
                listener_receipted: false,
                force,
                forced: Arc::new(AtomicBool::new(false)),
            },
            completed_evidence: None,
        }
    }

    #[tokio::test]
    async fn completed_http_recorder_error_caches_closed_but_uncertain_evidence() {
        let mut task = completed_http_recorder_task(
            true,
            Err(HaNodeError::RecorderServer(
                "unexpected recorder exit".into(),
            )),
        );
        assert!(matches!(
            (&mut task).await,
            Ok(Err(HaNodeError::RecorderServer(message))) if message == "unexpected recorder exit"
        ));
        let evidence = task.completed_shutdown_evidence();
        assert_eq!(
            evidence,
            Some(ShutdownEvidence {
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Uncertain,
            })
        );
        assert_eq!(
            merge_shutdown_evidence(PRE_SERVICE_SHUTDOWN_EVIDENCE, evidence),
            ShutdownEvidence {
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Uncertain,
            }
        );
    }

    #[tokio::test]
    async fn completed_http_recorder_panic_without_receipt_stays_fully_uncertain() {
        let (receipt, receipt_rx) = tokio::sync::oneshot::channel::<()>();
        drop(receipt);
        let (force, _) = tokio::sync::watch::channel(false);
        let mut task = RecorderServerTask::Http {
            ingress: TrackedAxumIngress {
                task: HaServerTask::spawn(async {
                    panic!("recorder test panic");
                    #[allow(unreachable_code)]
                    Ok::<(), HaNodeError>(())
                }),
                listener_dropped: receipt_rx,
                listener_receipted: false,
                force,
                forced: Arc::new(AtomicBool::new(false)),
            },
            completed_evidence: None,
        };
        assert!((&mut task).await.is_err());
        let evidence = task.completed_shutdown_evidence();
        assert_eq!(
            evidence,
            Some(ShutdownEvidence {
                ingress: IngressDisposition::Uncertain,
                tasks: TaskDisposition::Uncertain,
            })
        );
        assert_eq!(
            merge_shutdown_evidence(PRE_SERVICE_SHUTDOWN_EVIDENCE, evidence),
            ShutdownEvidence {
                ingress: IngressDisposition::Uncertain,
                tasks: TaskDisposition::Uncertain,
            }
        );
    }

    #[tokio::test]
    async fn tracked_axum_ingress_drops_listener_before_a_cooperative_connection_drains() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let release = Arc::new(tokio::sync::Notify::new());
        let entered = Arc::new(AtomicBool::new(false));
        let router = {
            let release = Arc::clone(&release);
            let entered = Arc::clone(&entered);
            axum::Router::new().route(
                "/slow",
                axum::routing::get(move || {
                    let release = Arc::clone(&release);
                    let entered = Arc::clone(&entered);
                    async move {
                        entered.store(true, Ordering::Release);
                        release.notified().await;
                        axum::http::StatusCode::OK
                    }
                }),
            )
        };
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut ingress = spawn_ha_service_server(listener, router, shutdown_rx, started);
        started_rx.await.unwrap();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /slow HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        shutdown.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), &mut ingress.listener_dropped)
            .await
            .unwrap()
            .unwrap();
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
        assert!(!ingress.task.is_finished());

        release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), &mut ingress.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(stream);
    }

    #[tokio::test]
    async fn tracked_axum_ingress_preserves_router_extensions_and_body_limits() {
        use axum::response::IntoResponse as _;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn require_test_token(
            request: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            if request
                .headers()
                .get(axum::http::header::AUTHORIZATION)
                .is_some_and(|value| value == "Bearer test-token")
            {
                next.run(request).await
            } else {
                axum::http::StatusCode::UNAUTHORIZED.into_response()
            }
        }

        async fn raw_request(address: std::net::SocketAddr, request: String) -> String {
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            String::from_utf8(response).unwrap()
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router =
            axum::Router::new()
                .route(
                    "/extension",
                    axum::routing::get(
                        |axum::extract::Extension(value): axum::extract::Extension<
                            &'static str,
                        >| async move { value },
                    ),
                )
                .route(
                    "/tiny",
                    axum::routing::post(|_: axum::body::Bytes| async {
                        axum::http::StatusCode::NO_CONTENT
                    }),
                )
                .route(
                    "/protected",
                    axum::routing::get(|| async { axum::http::StatusCode::OK })
                        .route_layer(axum::middleware::from_fn(require_test_token)),
                )
                .layer(axum::extract::DefaultBodyLimit::max(1))
                .layer(axum::Extension("preserved"));
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut ingress = spawn_ha_service_server(listener, router, shutdown_rx, started);
        started_rx.await.unwrap();

        let mut extension = tokio::net::TcpStream::connect(address).await.unwrap();
        extension
            .write_all(b"GET /extension HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        extension.read_to_end(&mut response).await.unwrap();
        assert!(std::str::from_utf8(&response)
            .unwrap()
            .contains("200 OK\r\n"));
        assert!(std::str::from_utf8(&response)
            .unwrap()
            .ends_with("preserved"));

        let mut too_large = tokio::net::TcpStream::connect(address).await.unwrap();
        too_large
            .write_all(b"POST /tiny HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 2\r\n\r\nxx")
            .await
            .unwrap();
        let mut response = Vec::new();
        too_large.read_to_end(&mut response).await.unwrap();
        assert!(std::str::from_utf8(&response)
            .unwrap()
            .contains("413 Payload Too Large\r\n"));
        for authorization in [
            "",
            "Authorization: Bearer wrong-token\r\n",
            "Authorization: Bearer test-token\r\n",
        ] {
            let response = raw_request(
                address,
                format!(
                    "GET /protected HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{authorization}\r\n"
                ),
            )
            .await;
            let expected = if authorization == "Authorization: Bearer test-token\r\n" {
                "200 OK\r\n"
            } else {
                "401 Unauthorized\r\n"
            };
            assert!(response.contains(expected), "response: {response}");
        }

        shutdown.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), &mut ingress.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn tracked_axum_ingress_isolates_connection_errors() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = axum::Router::new().route("/ok", axum::routing::get(|| async { "ok" }));
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut ingress = spawn_ha_service_server(listener, router, shutdown_rx, started);
        started_rx.await.unwrap();

        let mut broken = tokio::net::TcpStream::connect(address).await.unwrap();
        broken.write_all(b"not an HTTP request\r\n").await.unwrap();
        drop(broken);
        tokio::task::yield_now().await;

        let mut healthy = tokio::net::TcpStream::connect(address).await.unwrap();
        healthy
            .write_all(b"GET /ok HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        healthy.read_to_end(&mut response).await.unwrap();
        assert!(std::str::from_utf8(&response)
            .unwrap()
            .contains("200 OK\r\n"));

        shutdown.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), &mut ingress.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn tracked_axum_ingress_forces_stubborn_connection_before_deadline() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let entered = Arc::new(AtomicBool::new(false));
        let router = {
            let entered = Arc::clone(&entered);
            axum::Router::new().route(
                "/stuck",
                axum::routing::get(move || {
                    let entered = Arc::clone(&entered);
                    async move {
                        entered.store(true, Ordering::Release);
                        std::future::pending::<()>().await;
                        axum::http::StatusCode::OK
                    }
                }),
            )
        };
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut ingress = spawn_ha_service_server(listener, router, shutdown_rx, started);
        started_rx.await.unwrap();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /stuck HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        shutdown.send_replace(true);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let (result, listener, tasks) =
            wait_for_tracked_axum_ingress(&mut ingress, "service server", deadline).await;
        assert!(result.is_ok());
        assert_eq!(listener, IngressDisposition::Closed);
        assert_eq!(tasks, TaskDisposition::Uncertain);
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
        drop(stream);
    }

    #[tokio::test]
    async fn tracked_axum_ingress_bounds_blocking_handler_but_keeps_task_evidence_uncertain() {
        use tokio::io::AsyncWriteExt;

        struct ActiveGuard {
            active: Arc<AtomicUsize>,
            drained: Arc<tokio::sync::Notify>,
        }

        impl Drop for ActiveGuard {
            fn drop(&mut self) {
                self.active.fetch_sub(1, Ordering::AcqRel);
                self.drained.notify_one();
            }
        }

        struct ReleaseGuard(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);

        impl ReleaseGuard {
            fn release(&self) {
                let (released, condition) = &*self.0;
                *released.lock().unwrap() = true;
                condition.notify_all();
            }
        }

        impl Drop for ReleaseGuard {
            fn drop(&mut self) {
                self.release();
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let active = Arc::new(AtomicUsize::new(0));
        let active_drained = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let release_guard = ReleaseGuard(Arc::clone(&gate));
        let router = {
            let entered = Arc::clone(&entered);
            let active = Arc::clone(&active);
            let active_drained = Arc::clone(&active_drained);
            let gate = Arc::clone(&gate);
            axum::Router::new().route(
                "/blocking",
                axum::routing::get(move || {
                    let entered = Arc::clone(&entered);
                    let active = Arc::clone(&active);
                    let active_drained = Arc::clone(&active_drained);
                    let gate = Arc::clone(&gate);
                    async move {
                        tokio::task::spawn_blocking(move || {
                            // Signal only after this blocking worker has
                            // established ownership, so shutdown cannot race
                            // ahead of the task this test is exercising.
                            active.fetch_add(1, Ordering::AcqRel);
                            let _active = ActiveGuard {
                                active,
                                drained: active_drained,
                            };
                            entered.notify_one();
                            {
                                let (released, condition) = &*gate;
                                let mut released = released.lock().unwrap();
                                while !*released {
                                    released = condition.wait(released).unwrap();
                                }
                            }
                        })
                        .await
                        .unwrap();
                        axum::http::StatusCode::OK
                    }
                }),
            )
        };
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut ingress = spawn_ha_service_server(listener, router, shutdown_rx, started);
        started_rx.await.unwrap();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /blocking HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .unwrap();

        shutdown.send_replace(true);
        let (result, listener, tasks) = wait_for_tracked_axum_ingress(
            &mut ingress,
            "service server",
            tokio::time::Instant::now() + Duration::from_millis(200),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(listener, IngressDisposition::Closed);
        assert_eq!(tasks, TaskDisposition::Uncertain);
        assert_eq!(active.load(Ordering::Acquire), 1);
        let active_released = active_drained.notified();
        tokio::pin!(active_released);
        release_guard.release();
        tokio::time::timeout(Duration::from_secs(1), &mut active_released)
            .await
            .unwrap();
        assert_eq!(active.load(Ordering::Acquire), 0);
        drop(stream);
    }

    #[tokio::test]
    async fn tracked_axum_ingress_shutdown_rebind_race_is_receipted() {
        for _ in 0..200 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut ingress =
                spawn_ha_service_server(listener, axum::Router::new(), shutdown_rx, started);
            started_rx.await.unwrap();
            shutdown.send_replace(true);
            tokio::time::timeout(Duration::from_secs(1), &mut ingress.listener_dropped)
                .await
                .unwrap()
                .unwrap();
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
            tokio::time::timeout(Duration::from_secs(1), &mut ingress.task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn preconsumed_service_listener_receipt_is_idempotent_for_cleanup() {
        for _ in 0..25 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut ingress =
                spawn_ha_service_server(listener, axum::Router::new(), shutdown_rx, started);
            started_rx.await.unwrap();
            shutdown.send_replace(true);
            tokio::time::timeout(Duration::from_secs(1), async {
                while !ingress.poll_listener_receipt() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();

            let (result, listener, tasks) = wait_for_tracked_axum_ingress(
                &mut ingress,
                "service server",
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await;
            assert!(result.is_ok());
            assert_eq!(listener, IngressDisposition::Closed);
            assert_eq!(tasks, TaskDisposition::Quiesced);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn tracked_axum_ingress_drop_aborts_owner_and_releases_listener() {
        for _ in 0..200 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let ingress =
                spawn_ha_service_server(listener, axum::Router::new(), shutdown_rx, started);
            started_rx.await.unwrap();
            drop(ingress);
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if let Ok(rebound) = tokio::net::TcpListener::bind(address).await {
                        drop(rebound);
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
        }
    }

    #[tokio::test]
    async fn staging_listener_close_drops_the_unique_lease_before_rebind() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
            ListenerLease::new(listener),
            Arc::new(AtomicBool::new(false)),
            command_rx,
            started,
        ));
        started_rx.await.unwrap();

        stop_staging_server_before(
            &command,
            &mut staging,
            "successor staging service",
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }

    #[tokio::test]
    async fn staging_close_aborts_idle_http1_keepalive_before_deadline() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        for _ in 0..25 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
            ));
            started_rx.await.unwrap();

            // The stream itself is the RAII cleanup guard on every assertion
            // path. Keep it deliberately idle after the first response.
            let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
            client
                .write_all(
                    b"GET /livez HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n",
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            tokio::time::timeout(Duration::from_secs(1), async {
                let mut byte = [0_u8; 1];
                while !response.ends_with(b"\r\n\r\n") {
                    client.read_exact(&mut byte).await.unwrap();
                    response.push(byte[0]);
                }
            })
            .await
            .unwrap();
            assert!(String::from_utf8(response).unwrap().contains("200 OK"));

            let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
            tokio::time::timeout_at(
                deadline,
                stop_staging_server_before(
                    &command,
                    &mut staging,
                    "successor staging service",
                    deadline,
                ),
            )
            .await
            .expect("idle health keepalive must not hold staging shutdown through D")
            .unwrap();
            let mut closed = [0_u8; 1];
            assert_eq!(client.read(&mut closed).await.unwrap(), 0);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn staging_resource_accept_error_backs_off_then_recovers() {
        for _ in 0..20 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (faults, backoffs) =
                TestStagingAcceptFaults::new([TestStagingAcceptFailure::RawOs(24)], Duration::ZERO);
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server_inner(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
                None,
                Some(faults),
            ));
            started_rx.await.unwrap();

            let response =
                tokio::time::timeout(Duration::from_secs(1), staging_livez_request(address))
                    .await
                    .expect("staging must recover after the resource backoff");
            assert!(response.contains("200 OK"));
            assert_eq!(*backoffs.borrow(), 1);

            command.send_replace(StagingCommand::Close);
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(1), &mut *staging)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                StagingExit::Closed
            ));
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn staging_resource_accept_backoff_yields_to_close() {
        for _ in 0..20 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (faults, mut backoffs) = TestStagingAcceptFaults::new(
                [TestStagingAcceptFailure::RawOs(24)],
                Duration::from_secs(60 * 60),
            );
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server_inner(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
                None,
                Some(faults),
            ));
            started_rx.await.unwrap();
            wait_for_staging_resource_backoff(&mut backoffs).await;

            command.send_replace(StagingCommand::Close);
            assert!(matches!(
                tokio::time::timeout(Duration::from_millis(200), &mut *staging)
                    .await
                    .expect("Close must preempt resource accept backoff")
                    .unwrap()
                    .unwrap(),
                StagingExit::Closed
            ));
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn staging_resource_accept_backoff_yields_to_handoff() {
        for _ in 0..20 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (faults, mut backoffs) = TestStagingAcceptFaults::new(
                [TestStagingAcceptFailure::RawOs(24)],
                Duration::from_secs(60 * 60),
            );
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server_inner(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
                None,
                Some(faults),
            ));
            started_rx.await.unwrap();
            wait_for_staging_resource_backoff(&mut backoffs).await;

            command.send_replace(StagingCommand::Handoff);
            let lease = match tokio::time::timeout(Duration::from_millis(200), &mut *staging)
                .await
                .expect("Handoff must preempt resource accept backoff")
                .unwrap()
                .unwrap()
            {
                StagingExit::Handoff(lease) => lease,
                StagingExit::Closed => panic!("staging closed instead of handing off"),
            };
            assert_eq!(lease.listener().local_addr().unwrap(), address);
            assert!(tokio::net::TcpListener::bind(address).await.is_err());
            drop(lease);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn staging_transient_accept_error_retries_without_resource_backoff() {
        for _ in 0..20 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (faults, backoffs) = TestStagingAcceptFaults::new(
                [TestStagingAcceptFailure::Kind(
                    io::ErrorKind::ConnectionRefused,
                )],
                Duration::from_secs(60 * 60),
            );
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server_inner(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
                None,
                Some(faults),
            ));
            started_rx.await.unwrap();

            let response =
                tokio::time::timeout(Duration::from_secs(1), staging_livez_request(address))
                    .await
                    .expect("staging must immediately retry transient accept errors");
            assert!(response.contains("200 OK"));
            assert_eq!(*backoffs.borrow(), 0);

            command.send_replace(StagingCommand::Close);
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(1), &mut *staging)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                StagingExit::Closed
            ));
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn staging_terminal_accept_error_preserves_its_source() {
        for _ in 0..20 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let source = io::Error::from(io::ErrorKind::PermissionDenied);
            let expected = format!("successor staging service accept failed: {source}");
            let (faults, _backoffs) = TestStagingAcceptFaults::new(
                [TestStagingAcceptFailure::Kind(
                    io::ErrorKind::PermissionDenied,
                )],
                Duration::ZERO,
            );
            let (_command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server_inner(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
                None,
                Some(faults),
            ));
            started_rx.await.unwrap();

            let result = tokio::time::timeout(Duration::from_secs(1), &mut *staging)
                .await
                .expect("terminal staging accept error must stop the server")
                .unwrap();
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("terminal staging accept error unexpectedly succeeded"),
            };
            assert!(matches!(
                error,
                HaNodeError::ServiceServer(message) if message == expected
            ));
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn successor_retry_wait_surfaces_actual_staging_fault_before_timer() {
        for _ in 0..25 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
            ));
            started_rx.await.unwrap();
            let (_shutdown, mut shutdown_rx) = tokio::sync::watch::channel(None);
            let (_predecessor, mut predecessor_rx) = tokio::sync::mpsc::unbounded_channel();
            command.send_replace(StagingCommand::Fail("retry staging failure"));

            let event = tokio::time::timeout(
                Duration::from_millis(100),
                wait_live_successor_retry(&mut shutdown_rx, &mut staging, &mut predecessor_rx),
            )
            .await
            .expect("actual staging fault must preempt the 250ms retry");
            let LiveSuccessorRetryEvent::Staging(result) = event else {
                panic!("retry returned without the staging fault");
            };
            assert!(matches!(
                unexpected_staging_server_exit(result),
                HaNodeError::ServiceServer(message) if message == "retry staging failure"
            ));
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn staging_handoff_keeps_the_same_bound_listener_for_the_child() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
            ListenerLease::new(listener),
            Arc::new(AtomicBool::new(false)),
            command_rx,
            started,
        ));
        started_rx.await.unwrap();

        let lease = handoff_staging_listener(
            &command,
            &mut staging,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(lease.listener().local_addr().unwrap(), address);
        assert!(tokio::net::TcpListener::bind(address).await.is_err());

        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (child_started, child_started_rx) = tokio::sync::oneshot::channel();
        let mut child = spawn_ha_service_server(
            lease.into_listener(),
            axum::Router::new().route("/child", axum::routing::get(|| async { "child" })),
            shutdown_rx,
            child_started,
        );
        child_started_rx.await.unwrap();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /child HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8(response).unwrap().contains("child"));

        shutdown.send_replace(true);
        let (result, ingress, _) = wait_for_tracked_axum_ingress(
            &mut child,
            "service server",
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(ingress, IngressDisposition::Closed);
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }

    #[tokio::test]
    async fn staging_handoff_gate_retains_release_before_wait() {
        for _ in 0..100 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
            ));
            started_rx.await.unwrap();

            let (gate, entered) = TestStagingHandoffGate::new();
            let release = gate.release_guard();
            drop(release);
            command.send_replace(StagingCommand::BlockedHandoff(gate));
            tokio::time::timeout(Duration::from_millis(200), entered)
                .await
                .expect("staging must enter the released handoff gate")
                .expect("staging handoff gate dropped before entry");
            let lease = match tokio::time::timeout(Duration::from_millis(200), &mut *staging)
                .await
                .expect("a release sent before wait must remain observable")
                .unwrap()
                .unwrap()
            {
                StagingExit::Handoff(lease) => lease,
                StagingExit::Closed => panic!("staging closed instead of handing off"),
            };
            assert_eq!(lease.listener().local_addr().unwrap(), address);
            assert!(tokio::net::TcpListener::bind(address).await.is_err());
            drop(lease);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn staging_close_before_handoff_is_a_conservative_service_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
            ListenerLease::new(listener),
            Arc::new(AtomicBool::new(false)),
            command_rx,
            started,
        ));
        started_rx.await.unwrap();
        command.send_replace(StagingCommand::Close);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !staging.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert!(matches!(
            handoff_staging_listener(
                &command,
                &mut staging,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await,
            Err(HaNodeError::ServiceServer(message))
                if message == "successor staging service closed before listener handoff"
        ));
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }

    #[tokio::test]
    async fn deferred_child_cancellation_leaves_staging_as_the_only_listener_owner() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
        let (staging_started, staging_started_rx) = tokio::sync::oneshot::channel();
        let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
            ListenerLease::new(listener),
            Arc::new(AtomicBool::new(false)),
            command_rx,
            staging_started,
        ));
        staging_started_rx.await.unwrap();
        let (ready, ready_rx) = tokio::sync::oneshot::channel();
        let (_handoff, handoff_rx) = tokio::sync::oneshot::channel();
        let (_shutdown, mut shutdown_rx) = tokio::sync::watch::channel(None);
        let activation = tokio::spawn(async move {
            activate_service_listener(
                ServiceListener::Deferred {
                    ready,
                    listener: handoff_rx,
                },
                &mut shutdown_rx,
            )
            .await
        });
        ready_rx.await.unwrap();
        assert!(tokio::net::TcpListener::bind(address).await.is_err());
        let shutdown = _shutdown;
        shutdown.send_replace(Some(Arc::new(ShutdownToken::new(Duration::from_secs(1)))));
        assert!(matches!(
            activation.await.unwrap().unwrap(),
            ServiceActivation::Shutdown {
                listener_closed: false,
                ..
            }
        ));
        stop_staging_server_before(
            &command,
            &mut staging,
            "successor staging service",
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }

    fn blocked_pre_service_test_child(
        status: HaNodeStatus,
        terminal_error: Option<HaNodeError>,
        cancelled: tokio::sync::oneshot::Sender<(usize, tokio::time::Instant)>,
    ) -> HaNode {
        let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(None);
        let (state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status,
            handle: None,
            terminal_error,
        });
        let supervisor = AbortOnDropTask::spawn(async move {
            loop {
                if let Some(token) = shutdown_rx.borrow().clone() {
                    let _ = cancelled.send((shutdown_token_identity(&token), token.deadline()));
                    drop(state_tx);
                    return Ok(());
                }
                if shutdown_rx.changed().await.is_err() {
                    return Ok(());
                }
            }
        });
        HaNode {
            shutdown,
            recorder_shutdown: tokio::sync::watch::channel(false).0,
            startup: StartupIoContext::new(),
            state,
            supervisor: Some(supervisor),
        }
    }

    fn controllable_pre_service_test_child(
        cancelled: tokio::sync::oneshot::Sender<(usize, tokio::time::Instant)>,
    ) -> (HaNode, tokio::sync::watch::Sender<HaNodeSnapshot>) {
        let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(None);
        let (state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Starting,
            handle: None,
            terminal_error: None,
        });
        let held_state = state_tx.clone();
        let supervisor = AbortOnDropTask::spawn(async move {
            let _held_state = held_state;
            loop {
                if let Some(token) = shutdown_rx.borrow().clone() {
                    let _ = cancelled.send((shutdown_token_identity(&token), token.deadline()));
                    return Ok(());
                }
                if shutdown_rx.changed().await.is_err() {
                    return Ok(());
                }
            }
        });
        (
            HaNode {
                shutdown,
                recorder_shutdown: tokio::sync::watch::channel(false).0,
                startup: StartupIoContext::new(),
                state,
                supervisor: Some(supervisor),
            },
            state_tx,
        )
    }

    fn post_handoff_test_child(
        status: HaNodeStatus,
        terminal_error: Option<HaNodeError>,
        close_state_channel: bool,
        listener: tokio::net::TcpListener,
        cancelled: tokio::sync::oneshot::Sender<(usize, tokio::time::Instant)>,
    ) -> HaNode {
        let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(None);
        let (state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status,
            handle: None,
            terminal_error,
        });
        let held_state = (!close_state_channel).then_some(state_tx);
        let supervisor = AbortOnDropTask::spawn(async move {
            let _held_state = held_state;
            loop {
                if let Some(token) = shutdown_rx.borrow().clone() {
                    let _ = cancelled.send((shutdown_token_identity(&token), token.deadline()));
                    drop(listener);
                    return Ok(());
                }
                if shutdown_rx.changed().await.is_err() {
                    drop(listener);
                    return Ok(());
                }
            }
        });
        HaNode {
            shutdown,
            recorder_shutdown: tokio::sync::watch::channel(false).0,
            startup: StartupIoContext::new(),
            state,
            supervisor: Some(supervisor),
        }
    }

    fn post_handoff_error_child(
        status: HaNodeStatus,
        terminal_error: Option<HaNodeError>,
        close_state_channel: bool,
        listener: tokio::net::TcpListener,
        cancelled: tokio::sync::oneshot::Sender<(usize, tokio::time::Instant)>,
        shutdown_error: HaNodeError,
    ) -> HaNode {
        let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(None);
        let (state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status,
            handle: None,
            terminal_error,
        });
        let held_state = (!close_state_channel).then_some(state_tx);
        let supervisor = AbortOnDropTask::spawn(async move {
            let _held_state = held_state;
            loop {
                if let Some(token) = shutdown_rx.borrow().clone() {
                    let _ = cancelled.send((shutdown_token_identity(&token), token.deadline()));
                    drop(listener);
                    return Err(shutdown_error);
                }
                if shutdown_rx.changed().await.is_err() {
                    drop(listener);
                    return Err(shutdown_error);
                }
            }
        });
        HaNode {
            shutdown,
            recorder_shutdown: tokio::sync::watch::channel(false).0,
            startup: StartupIoContext::new(),
            state,
            supervisor: Some(supervisor),
        }
    }

    #[tokio::test]
    async fn prepare_failure_reuses_retained_shutdown_and_reaps_staging_before_publish() {
        const PREPARE_ERROR: &str = "successor restore completion must be a regular file";
        const CLEANUP_ERROR: &str = "injected staging close failure after listener cleanup";

        fn assert_combined_prepare_failure(error: &HaNodeError) {
            let HaNodeError::Cleanup { primary, cleanup } = error else {
                panic!("prepare failure did not retain staging cleanup evidence: {error}");
            };
            assert!(matches!(
                primary.as_ref(),
                HaNodeError::Startup(HaStartupError::Source(message))
                    if message == PREPARE_ERROR
            ));
            assert!(matches!(
                cleanup.as_ref(),
                HaNodeError::ServiceServer(message) if message == CLEANUP_ERROR
            ));
        }

        for _ in 0..20 {
            let root = tempfile::tempdir().unwrap();
            let store = rhiza_obj_store::ObjStore::new(rhiza_obj_store::ObjStoreConfig::Local {
                root: root.path().join("archive"),
            })
            .unwrap();
            let source_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
                store.clone(),
                CheckpointIdentity::new(
                    "rhiza:sql:cluster-a",
                    1,
                    1,
                    LogHash::digest(&[b"ha-test-config"]),
                    1,
                ),
            );
            source_archive.initialize_checkpoint().await.unwrap();
            let target_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
                store,
                CheckpointIdentity::new(
                    "rhiza:sql:cluster-a",
                    1,
                    2,
                    LogHash::digest(&[b"ha-test-config"]),
                    1,
                ),
            );
            let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
            let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();
            let data_dir = root.path().join("successor");
            std::fs::create_dir_all(data_dir.join(SUCCESSOR_RESTORE_COMPLETE_FILE)).unwrap();
            let peers = successor
                .members()
                .iter()
                .enumerate()
                .map(|(index, node_id)| {
                    rhiza_node::PeerConfig::new(
                        node_id,
                        format!("http://127.0.0.1:{}", 39501 + index),
                        format!("prepare-failure-peer-token-{}", index + 1),
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            let startup = HaStartupConfig::new(
                NodeConfig::new_with_configuration(
                    "rhiza:sql:cluster-a",
                    "node-4",
                    data_dir,
                    1,
                    successor.clone(),
                    rhiza_core::ConfigurationState::active(2, successor.digest()),
                    peers,
                    "successor-client-token",
                )
                .unwrap(),
                target_archive,
                DurabilityMode::Sync,
                60_000,
                HaStartupMode::Rejoin,
            );
            let prestage = HaSuccessorPrestageConfig::new(
                source_archive,
                root.path().join("prestage"),
                "node-4",
                ExecutionProfile::Sqlite,
                predecessor,
                successor,
                "tail-token",
            );
            validate_live_successor_draft(&prestage, &startup).unwrap();

            let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let recorder_address = recorder_listener.local_addr().unwrap();
            let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let service_address = service_listener.local_addr().unwrap();
            let (token_observer, token_observer_rx) = TestCleanupTokenObserver::new();
            let serve = SuccessorServeConfig {
                recorder_listener,
                service_listener: Some(ListenerLease::new(service_listener)),
                recorder_transport: HaRecorderTransport::Http,
                recorders: Vec::new(),
                log_peers: Vec::new(),
                admin: None,
                tail_token: None,
                staging_close_error: Some(CLEANUP_ERROR),
                cleanup_token_observer: Some(token_observer),
                recorder_start_error: None,
                recorder_shutdown_outcome: None,
                open_shutdown_token_observer: None,
                staging_accept_faults: None,
            };
            let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel::<ShutdownSignal>(None);
            let external = Arc::new(ShutdownToken::new(Duration::from_millis(500)));
            let external_identity = shutdown_token_identity(&external);
            let external_deadline = external.deadline();
            shutdown.send_replace(Some(Arc::clone(&external)));
            let retained = shutdown_rx.borrow_and_update().clone().unwrap();
            assert!(Arc::ptr_eq(&retained, &external));
            let (_predecessor, predecessor_rx) = tokio::sync::mpsc::unbounded_channel();
            let (state, state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Restoring,
                handle: None,
                terminal_error: None,
            });

            let error = tokio::time::timeout_at(
                external_deadline,
                supervise_live_successor(
                    prestage,
                    startup,
                    serve,
                    Arc::new(UnusedCertifiedTailSource),
                    shutdown_rx,
                    predecessor_rx,
                    state,
                ),
            )
            .await
            .expect("prepare failure cleanup must finish before the retained outer D")
            .unwrap_err();
            assert_combined_prepare_failure(&error);
            let (observed_identity, observed_deadline) = token_observer_rx.await.unwrap();
            assert_eq!(observed_identity, external_identity);
            assert_eq!(observed_deadline, external_deadline);

            let snapshot = state_rx.borrow().clone();
            assert_eq!(snapshot.status, HaNodeStatus::Failed);
            let terminal = snapshot
                .terminal_error
                .as_ref()
                .expect("combined prepare failure must be the terminal snapshot");
            assert_combined_prepare_failure(terminal);
            let recorder_rebound = tokio::net::TcpListener::bind(recorder_address)
                .await
                .unwrap();
            let service_rebound = tokio::net::TcpListener::bind(service_address)
                .await
                .unwrap();
            drop((recorder_rebound, service_rebound, shutdown));
        }
    }

    #[test]
    fn live_successor_child_shutdown_normalization_keeps_one_authoritative_chain() {
        let snapshot_message = "snapshot failure";
        let direct = normalize_live_successor_child_shutdown(
            Some(HaNodeError::ServiceServer(snapshot_message.into())),
            Err(HaNodeError::ServiceServer(snapshot_message.into())),
        )
        .unwrap_err();
        assert!(matches!(
            direct,
            HaNodeError::ServiceServer(message) if message == snapshot_message
        ));

        let deadline = HaNodeError::ShutdownDeadlineExceeded {
            phase: HaShutdownPhase::Service,
            ingress: IngressDisposition::Closed,
            tasks: TaskDisposition::Uncertain,
            mutation: CONSERVATIVE_SHUTDOWN_MUTATION,
        };
        let authoritative_cleanup = normalize_live_successor_child_shutdown(
            Some(HaNodeError::ServiceServer(snapshot_message.into())),
            Err(HaNodeError::Cleanup {
                primary: Box::new(HaNodeError::ServiceServer(snapshot_message.into())),
                cleanup: Box::new(deadline),
            }),
        )
        .unwrap_err();
        let HaNodeError::Cleanup { primary, cleanup } = authoritative_cleanup else {
            panic!("child Cleanup must remain authoritative without another wrapper");
        };
        assert!(matches!(
            primary.as_ref(),
            HaNodeError::ServiceServer(message) if message == snapshot_message
        ));
        assert!(matches!(
            cleanup.as_ref(),
            HaNodeError::ShutdownDeadlineExceeded { .. }
        ));

        let recorder = combine_live_successor_child_and_staging(
            None,
            Err(HaNodeError::RecorderServer("recorder child failure".into())),
            Ok(()),
        )
        .unwrap_err();
        assert!(matches!(
            recorder,
            HaNodeError::RecorderServer(message) if message == "recorder child failure"
        ));

        let startup = combine_live_successor_child_and_staging(
            None,
            Err(HaNodeError::Startup(fail("startup child failure"))),
            Err(HaNodeError::ServiceServer("staging cleanup failure".into())),
        )
        .unwrap_err();
        let HaNodeError::Cleanup { primary, cleanup } = startup else {
            panic!("staging cleanup must be attached once to the authoritative child error");
        };
        assert!(matches!(
            primary.as_ref(),
            HaNodeError::Startup(HaStartupError::Source(message))
                if message == "startup child failure"
        ));
        assert!(matches!(
            cleanup.as_ref(),
            HaNodeError::ServiceServer(message) if message == "staging cleanup failure"
        ));
    }

    #[tokio::test]
    async fn pre_handoff_terminal_direct_child_error_is_not_duplicated() {
        for _ in 0..20 {
            let staging_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let staging_address = staging_listener.local_addr().unwrap();
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                ListenerLease::new(staging_listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
            ));
            started_rx.await.unwrap();

            let child_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let child_address = child_listener.local_addr().unwrap();
            let message = "same direct child terminal";
            let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
            let child = post_handoff_error_child(
                HaNodeStatus::Failed,
                Some(HaNodeError::ServiceServer(message.into())),
                false,
                child_listener,
                cancelled,
                HaNodeError::ServiceServer(message.into()),
            );
            let (_service_ready, mut service_ready_rx) = tokio::sync::oneshot::channel();
            let (service_listener, _service_listener_rx) = tokio::sync::oneshot::channel();
            let (shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
            let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
            let deadline = token.deadline();
            let identity = shutdown_token_identity(&token);
            shutdown.send_replace(Some(token));
            let (state, _state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Transitioning,
                handle: None,
                terminal_error: None,
            });

            let error = tokio::time::timeout_at(
                deadline,
                supervise_live_successor_child_after_staging_with_child(
                    child,
                    &mut service_ready_rx,
                    service_listener,
                    &command,
                    &mut staging,
                    shutdown_rx,
                    state,
                    None,
                ),
            )
            .await
            .expect("pre-handoff child cleanup must finish before retained D")
            .unwrap_err();
            assert!(matches!(
                error,
                HaNodeError::ServiceServer(actual) if actual == message
            ));
            let (observed_identity, observed_deadline) = cancelled_rx.await.unwrap();
            assert_eq!(observed_identity, identity);
            assert_eq!(observed_deadline, deadline);
            let staging_rebound = tokio::net::TcpListener::bind(staging_address)
                .await
                .unwrap();
            let child_rebound = tokio::net::TcpListener::bind(child_address).await.unwrap();
            drop((staging_rebound, child_rebound));
        }
    }

    #[tokio::test]
    async fn pre_handoff_ready_close_prefers_child_source_then_attaches_staging_once() {
        for staging_error in [None, Some("ready-close staging cleanup failure")] {
            for _ in 0..20 {
                let staging_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let staging_address = staging_listener.local_addr().unwrap();
                let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
                let (started, started_rx) = tokio::sync::oneshot::channel();
                let mut staging =
                    AbortStagingServerOnDrop::new(spawn_successor_staging_server_inner(
                        ListenerLease::new(staging_listener),
                        Arc::new(AtomicBool::new(false)),
                        command_rx,
                        started,
                        staging_error,
                        None,
                    ));
                started_rx.await.unwrap();

                let child_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let child_address = child_listener.local_addr().unwrap();
                let child_message = "authoritative recorder child failure";
                let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
                let child = post_handoff_error_child(
                    HaNodeStatus::Starting,
                    None,
                    false,
                    child_listener,
                    cancelled,
                    HaNodeError::RecorderServer(child_message.into()),
                );
                let (service_ready, mut service_ready_rx) = tokio::sync::oneshot::channel();
                drop(service_ready);
                let (service_listener, _service_listener_rx) = tokio::sync::oneshot::channel();
                let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(None);
                let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
                let deadline = token.deadline();
                let identity = shutdown_token_identity(&token);
                shutdown.send_replace(Some(token));
                let _ = shutdown_rx.borrow_and_update();
                let (state, _state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                    status: HaNodeStatus::Transitioning,
                    handle: None,
                    terminal_error: None,
                });

                let error = tokio::time::timeout_at(
                    deadline,
                    supervise_live_successor_child_after_staging_with_child(
                        child,
                        &mut service_ready_rx,
                        service_listener,
                        &command,
                        &mut staging,
                        shutdown_rx,
                        state,
                        None,
                    ),
                )
                .await
                .expect("ready-close cleanup must finish before retained D")
                .unwrap_err();
                match staging_error {
                    None => assert!(matches!(
                        error,
                        HaNodeError::RecorderServer(message) if message == child_message
                    )),
                    Some(staging_message) => {
                        let HaNodeError::Cleanup { primary, cleanup } = error else {
                            panic!("staging evidence must wrap the child source exactly once");
                        };
                        assert!(matches!(
                            primary.as_ref(),
                            HaNodeError::RecorderServer(message) if message == child_message
                        ));
                        assert!(matches!(
                            cleanup.as_ref(),
                            HaNodeError::ServiceServer(message) if message == staging_message
                        ));
                    }
                }
                let (observed_identity, observed_deadline) = cancelled_rx.await.unwrap();
                assert_eq!(observed_identity, identity);
                assert_eq!(observed_deadline, deadline);
                let staging_rebound = tokio::net::TcpListener::bind(staging_address)
                    .await
                    .unwrap();
                let child_rebound = tokio::net::TcpListener::bind(child_address).await.unwrap();
                drop((staging_rebound, child_rebound));
            }
        }
    }

    #[tokio::test]
    async fn staging_exit_cancels_the_blocked_actual_child_before_activation_and_releases_listener()
    {
        for _ in 0..25 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
            ));
            started_rx.await.unwrap();

            let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
            let child = blocked_pre_service_test_child(HaNodeStatus::Starting, None, cancelled);
            let (_service_ready, mut service_ready_rx) = tokio::sync::oneshot::channel();
            let (service_listener, _service_listener_rx) = tokio::sync::oneshot::channel();
            let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
            let (state, _state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Transitioning,
                handle: None,
                terminal_error: None,
            });

            let supervisor = supervise_live_successor_child_after_staging_with_child(
                child,
                &mut service_ready_rx,
                service_listener,
                &command,
                &mut staging,
                shutdown_rx,
                state,
                None,
            );
            tokio::pin!(supervisor);
            command.send_replace(StagingCommand::Close);
            let result = tokio::time::timeout(Duration::from_secs(1), &mut supervisor)
                .await
                .expect("production supervisor must not wait for blocked activation")
                .unwrap_err();
            assert!(matches!(
                result,
                HaNodeError::ServiceServer(message)
                    if message == "successor staging service stopped unexpectedly"
            ));
            tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
                .await
                .expect("staging exit must cancel the actual child")
                .unwrap();
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn simultaneous_staging_failure_keeps_primary_but_reuses_exact_outer_shutdown() {
        for _ in 0..25 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
            ));
            started_rx.await.unwrap();
            command.send_replace(StagingCommand::Close);
            tokio::time::timeout(Duration::from_secs(1), async {
                while !staging.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();

            let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
            let child = blocked_pre_service_test_child(HaNodeStatus::Starting, None, cancelled);
            let (_service_ready, mut service_ready_rx) = tokio::sync::oneshot::channel();
            let (service_listener, _service_listener_rx) = tokio::sync::oneshot::channel();
            let (shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
            let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
            let deadline = token.deadline();
            let token_identity = shutdown_token_identity(&token);
            shutdown.send_replace(Some(token));
            let (state, _state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Transitioning,
                handle: None,
                terminal_error: None,
            });

            let result = tokio::time::timeout_at(
                deadline,
                supervise_live_successor_child_after_staging_with_child(
                    child,
                    &mut service_ready_rx,
                    service_listener,
                    &command,
                    &mut staging,
                    shutdown_rx,
                    state,
                    None,
                ),
            )
            .await
            .expect("simultaneous staging failure cleanup must finish before the external D")
            .unwrap_err();
            assert!(matches!(
                result,
                HaNodeError::ServiceServer(message)
                    if message == "successor staging service stopped unexpectedly"
            ));
            let (cancelled_identity, cancelled_deadline) = cancelled_rx.await.unwrap();
            assert_eq!(cancelled_identity, token_identity);
            assert_eq!(cancelled_deadline, deadline);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn simultaneous_ready_close_preserves_authoritative_child_terminal_error() {
        for _ in 0..25 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
            ));
            started_rx.await.unwrap();

            let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
            let (child, child_state) = controllable_pre_service_test_child(cancelled);
            let (service_ready, mut service_ready_rx) = tokio::sync::oneshot::channel();
            let (service_listener, _service_listener_rx) = tokio::sync::oneshot::channel();
            let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(None);
            let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
            let deadline = token.deadline();
            let token_identity = shutdown_token_identity(&token);
            shutdown.send_replace(Some(token));
            let _ = shutdown_rx.borrow_and_update();
            let (state, _state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Transitioning,
                handle: None,
                terminal_error: None,
            });

            let supervisor = supervise_live_successor_child_after_staging_with_child(
                child,
                &mut service_ready_rx,
                service_listener,
                &command,
                &mut staging,
                shutdown_rx,
                state,
                None,
            );
            tokio::pin!(supervisor);
            let first_poll = std::future::poll_fn(|context| {
                std::task::Poll::Ready(std::future::Future::poll(supervisor.as_mut(), context))
            })
            .await;
            assert!(first_poll.is_pending());

            let primary_message = "authoritative child startup failure";
            child_state.send_replace(HaNodeSnapshot {
                status: HaNodeStatus::Failed,
                handle: None,
                terminal_error: Some(HaNodeError::ServiceServer(primary_message.into())),
            });
            drop(service_ready);
            let error = tokio::time::timeout_at(deadline, &mut supervisor)
                .await
                .expect("ready-close cleanup must finish before retained external D")
                .unwrap_err();
            assert!(matches!(
                error,
                HaNodeError::ServiceServer(message) if message == primary_message
            ));
            let (cancelled_identity, cancelled_deadline) = cancelled_rx.await.unwrap();
            assert_eq!(cancelled_identity, token_identity);
            assert_eq!(cancelled_deadline, deadline);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn top_level_child_terminal_cleanup_reuses_exact_outer_shutdown() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
            ListenerLease::new(listener),
            Arc::new(AtomicBool::new(false)),
            command_rx,
            started,
        ));
        started_rx.await.unwrap();

        let primary_message = "actual child terminal before activation";
        let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
        let child = blocked_pre_service_test_child(
            HaNodeStatus::Failed,
            Some(HaNodeError::ServiceServer(primary_message.into())),
            cancelled,
        );
        let (_service_ready, mut service_ready_rx) = tokio::sync::oneshot::channel();
        let (service_listener, _service_listener_rx) = tokio::sync::oneshot::channel();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
        let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
        let deadline = token.deadline();
        let token_identity = shutdown_token_identity(&token);
        shutdown.send_replace(Some(token));
        let (state, _state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Transitioning,
            handle: None,
            terminal_error: None,
        });

        let result = tokio::time::timeout_at(
            deadline,
            supervise_live_successor_child_after_staging_with_child(
                child,
                &mut service_ready_rx,
                service_listener,
                &command,
                &mut staging,
                shutdown_rx,
                state,
                None,
            ),
        )
        .await
        .expect("top-level terminal cleanup must finish before the external D")
        .unwrap_err();
        assert!(matches!(
            result,
            HaNodeError::ServiceServer(message) if message == primary_message
        ));
        let (cancelled_identity, cancelled_deadline) = cancelled_rx.await.unwrap();
        assert_eq!(cancelled_identity, token_identity);
        assert_eq!(cancelled_deadline, deadline);
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }

    #[tokio::test]
    async fn top_level_child_stopped_cleanup_reuses_exact_outer_shutdown() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
            ListenerLease::new(listener),
            Arc::new(AtomicBool::new(false)),
            command_rx,
            started,
        ));
        started_rx.await.unwrap();

        let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
        let child = blocked_pre_service_test_child(HaNodeStatus::Stopped, None, cancelled);
        let (_service_ready, mut service_ready_rx) = tokio::sync::oneshot::channel();
        let (service_listener, _service_listener_rx) = tokio::sync::oneshot::channel();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
        let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
        let deadline = token.deadline();
        let token_identity = shutdown_token_identity(&token);
        shutdown.send_replace(Some(token));
        let (state, state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Transitioning,
            handle: None,
            terminal_error: None,
        });

        tokio::time::timeout_at(
            deadline,
            supervise_live_successor_child_after_staging_with_child(
                child,
                &mut service_ready_rx,
                service_listener,
                &command,
                &mut staging,
                shutdown_rx,
                state,
                None,
            ),
        )
        .await
        .expect("top-level stopped cleanup must finish before the external D")
        .unwrap();
        let (cancelled_identity, cancelled_deadline) = cancelled_rx.await.unwrap();
        assert_eq!(cancelled_identity, token_identity);
        assert_eq!(cancelled_deadline, deadline);
        assert_eq!(state_rx.borrow().status, HaNodeStatus::Stopped);
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }

    #[tokio::test]
    async fn post_handoff_terminal_keeps_primary_and_reuses_exact_outer_shutdown() {
        for _ in 0..25 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let primary_message = "post-handoff child terminal";
            let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
            let child = post_handoff_test_child(
                HaNodeStatus::Failed,
                Some(HaNodeError::ServiceServer(primary_message.into())),
                false,
                listener,
                cancelled,
            );
            let (shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
            let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
            let deadline = token.deadline();
            let token_identity = shutdown_token_identity(&token);
            shutdown.send_replace(Some(token));
            let (state, _state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Ready,
                handle: None,
                terminal_error: None,
            });

            let error = tokio::time::timeout_at(
                deadline,
                supervise_live_successor_child(child, shutdown_rx, state),
            )
            .await
            .expect("post-handoff terminal cleanup must finish before external D")
            .unwrap_err();
            assert!(matches!(
                error,
                HaNodeError::ServiceServer(message) if message == primary_message
            ));
            let (cancelled_identity, cancelled_deadline) = cancelled_rx.await.unwrap();
            assert_eq!(cancelled_identity, token_identity);
            assert_eq!(cancelled_deadline, deadline);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn post_handoff_stopped_reuses_exact_outer_shutdown() {
        for _ in 0..25 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
            let child =
                post_handoff_test_child(HaNodeStatus::Stopped, None, false, listener, cancelled);
            let (shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
            let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
            let deadline = token.deadline();
            let token_identity = shutdown_token_identity(&token);
            shutdown.send_replace(Some(token));
            let (state, state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Ready,
                handle: None,
                terminal_error: None,
            });

            tokio::time::timeout_at(
                deadline,
                supervise_live_successor_child(child, shutdown_rx, state),
            )
            .await
            .expect("post-handoff stopped cleanup must finish before external D")
            .unwrap();
            let (cancelled_identity, cancelled_deadline) = cancelled_rx.await.unwrap();
            assert_eq!(cancelled_identity, token_identity);
            assert_eq!(cancelled_deadline, deadline);
            assert_eq!(state_rx.borrow().status, HaNodeStatus::Stopped);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn post_handoff_state_close_keeps_primary_and_reuses_retained_outer_shutdown() {
        for _ in 0..25 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
            let child =
                post_handoff_test_child(HaNodeStatus::Ready, None, true, listener, cancelled);
            let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(None);
            let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
            let deadline = token.deadline();
            let token_identity = shutdown_token_identity(&token);
            shutdown.send_replace(Some(token));
            // Retain the external token while making changed() pending, so the
            // distinct state-channel-close branch wins deterministically.
            let _ = shutdown_rx.borrow_and_update();
            let (state, _state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Ready,
                handle: None,
                terminal_error: None,
            });

            let error = tokio::time::timeout_at(
                deadline,
                supervise_live_successor_child(child, shutdown_rx, state),
            )
            .await
            .expect("post-handoff state-close cleanup must finish before external D")
            .unwrap_err();
            assert!(matches!(
                error,
                HaNodeError::ServiceServer(message)
                    if message == "HA child supervisor state channel closed"
            ));
            let (cancelled_identity, cancelled_deadline) = cancelled_rx.await.unwrap();
            assert_eq!(cancelled_identity, token_identity);
            assert_eq!(cancelled_deadline, deadline);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn post_handoff_terminal_preserves_authoritative_child_cleanup_without_duplication() {
        for _ in 0..20 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let primary_message = "post-handoff terminal with cleanup";
            let deadline_error = HaNodeError::ShutdownDeadlineExceeded {
                phase: HaShutdownPhase::Service,
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Uncertain,
                mutation: CONSERVATIVE_SHUTDOWN_MUTATION,
            };
            let authoritative = HaNodeError::Cleanup {
                primary: Box::new(HaNodeError::ServiceServer(primary_message.into())),
                cleanup: Box::new(deadline_error),
            };
            let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
            let child = post_handoff_error_child(
                HaNodeStatus::Failed,
                Some(HaNodeError::ServiceServer(primary_message.into())),
                false,
                listener,
                cancelled,
                authoritative,
            );
            let (shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
            let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
            let deadline = token.deadline();
            let token_identity = shutdown_token_identity(&token);
            shutdown.send_replace(Some(token));
            let (state, _state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Ready,
                handle: None,
                terminal_error: None,
            });

            let error = tokio::time::timeout_at(
                deadline,
                supervise_live_successor_child(child, shutdown_rx, state),
            )
            .await
            .expect("authoritative terminal cleanup must finish before external D")
            .unwrap_err();
            let HaNodeError::Cleanup { primary, cleanup } = error else {
                panic!("authoritative child Cleanup was discarded");
            };
            assert!(matches!(
                *primary,
                HaNodeError::ServiceServer(message) if message == primary_message
            ));
            assert!(matches!(
                *cleanup,
                HaNodeError::ShutdownDeadlineExceeded {
                    phase: HaShutdownPhase::Service,
                    ingress: IngressDisposition::Closed,
                    tasks: TaskDisposition::Uncertain,
                    ..
                }
            ));
            let (cancelled_identity, cancelled_deadline) = cancelled_rx.await.unwrap();
            assert_eq!(cancelled_identity, token_identity);
            assert_eq!(cancelled_deadline, deadline);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn post_handoff_state_close_keeps_primary_and_attaches_shutdown_evidence() {
        for _ in 0..20 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let shutdown_error = HaNodeError::ShutdownDeadlineExceeded {
                phase: HaShutdownPhase::Service,
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Uncertain,
                mutation: CONSERVATIVE_SHUTDOWN_MUTATION,
            };
            let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
            let child = post_handoff_error_child(
                HaNodeStatus::Ready,
                None,
                true,
                listener,
                cancelled,
                shutdown_error,
            );
            let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(None);
            let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
            let deadline = token.deadline();
            let token_identity = shutdown_token_identity(&token);
            shutdown.send_replace(Some(token));
            let _ = shutdown_rx.borrow_and_update();
            let (state, _state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Ready,
                handle: None,
                terminal_error: None,
            });

            let error = tokio::time::timeout_at(
                deadline,
                supervise_live_successor_child(child, shutdown_rx, state),
            )
            .await
            .expect("state-close cleanup must finish before external D")
            .unwrap_err();
            let HaNodeError::Cleanup { primary, cleanup } = error else {
                panic!("state-channel primary did not retain cleanup evidence");
            };
            assert!(matches!(
                *primary,
                HaNodeError::ServiceServer(message)
                    if message == "HA child supervisor state channel closed"
            ));
            assert!(matches!(
                *cleanup,
                HaNodeError::ShutdownDeadlineExceeded {
                    phase: HaShutdownPhase::Service,
                    ingress: IngressDisposition::Closed,
                    tasks: TaskDisposition::Uncertain,
                    ..
                }
            ));
            let (cancelled_identity, cancelled_deadline) = cancelled_rx.await.unwrap();
            assert_eq!(cancelled_identity, token_identity);
            assert_eq!(cancelled_deadline, deadline);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn open_phase_requested_shutdown_accepts_normal_recorder_completion() {
        for _ in 0..20 {
            let (result, snapshot) = exercise_open_phase_requested_recorder_shutdown(None).await;
            assert!(result.is_ok());
            assert_eq!(snapshot.status, HaNodeStatus::Stopped);
            assert!(snapshot.terminal_error.is_none());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn open_phase_requested_shutdown_keeps_recorder_error_and_panic_authoritative() {
        for outcome in [
            TestRecorderShutdownOutcome::Error("injected recorder shutdown failure"),
            TestRecorderShutdownOutcome::Panic("injected recorder shutdown panic"),
        ] {
            for _ in 0..10 {
                let (result, snapshot) =
                    exercise_open_phase_requested_recorder_shutdown(Some(outcome)).await;
                let error = result.expect_err("recorder error/panic must remain terminal");
                assert!(matches!(
                    &error,
                    HaNodeError::RecorderServer(message)
                        if message.contains(match outcome {
                            TestRecorderShutdownOutcome::Error(message)
                            | TestRecorderShutdownOutcome::Panic(message) => message,
                        })
                ));
                assert_eq!(snapshot.status, HaNodeStatus::Failed);
                assert!(matches!(
                    snapshot.terminal_error,
                    Some(HaNodeError::RecorderServer(message))
                        if message.contains(match outcome {
                            TestRecorderShutdownOutcome::Error(message)
                            | TestRecorderShutdownOutcome::Panic(message) => message,
                        })
                ));
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recorder_start_close_preserves_actual_child_source_through_parent_ready_close() {
        const RECORDER_ERROR: &str = "injected recorder start failure";

        for _ in 0..10 {
            let root = tempfile::tempdir().unwrap();
            let archive = actual_child_test_archive(&root.path().join("archive"));
            archive.initialize_checkpoint().await.unwrap();
            let startup = HaStartupConfig::new(
                actual_child_test_node_config(&root.path().join("node")),
                archive,
                DurabilityMode::Sync,
                60_000,
                HaStartupMode::Bootstrap,
            );
            let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let recorder_address = recorder_listener.local_addr().unwrap();
            let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let service_address = service_listener.local_addr().unwrap();
            let serve = SuccessorServeConfig {
                recorder_listener,
                service_listener: None,
                recorder_transport: HaRecorderTransport::Http,
                recorders: actual_child_test_recorders(&root.path().join("recorders")),
                log_peers: Vec::new(),
                admin: None,
                tail_token: None,
                staging_close_error: None,
                cleanup_token_observer: None,
                recorder_start_error: Some(RECORDER_ERROR),
                recorder_shutdown_outcome: None,
                open_shutdown_token_observer: None,
                staging_accept_faults: None,
            };
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                ListenerLease::new(service_listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
            ));
            started_rx.await.unwrap();
            let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(None);
            let token = Arc::new(ShutdownToken::new(Duration::from_secs(5)));
            let deadline = token.deadline();
            shutdown.send_replace(Some(token));
            let _ = shutdown_rx.borrow_and_update();
            let (state, state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Transitioning,
                handle: None,
                terminal_error: None,
            });
            let supervisor_state = state.clone();

            let result = tokio::time::timeout_at(deadline, async move {
                let result = supervise_live_successor_child_after_staging(
                    startup,
                    serve,
                    &command,
                    &mut staging,
                    shutdown_rx,
                    state,
                )
                .await;
                if let Err(error) = &result {
                    publish_ha_failure(&supervisor_state, error.clone());
                }
                result
            })
            .await
            .expect("actual recorder-start failure cleanup must finish before retained D")
            .unwrap_err();
            assert!(matches!(
                &result,
                HaNodeError::RecorderServer(message) if message == RECORDER_ERROR
            ));
            let snapshot = state_rx.borrow().clone();
            assert_eq!(snapshot.status, HaNodeStatus::Failed);
            assert!(matches!(
                snapshot.terminal_error.as_ref(),
                Some(HaNodeError::RecorderServer(message)) if message == RECORDER_ERROR
            ));
            let recorder_rebound = tokio::net::TcpListener::bind(recorder_address)
                .await
                .unwrap();
            let service_rebound = tokio::net::TcpListener::bind(service_address)
                .await
                .unwrap();
            drop((recorder_rebound, service_rebound, shutdown));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[cfg(feature = "test-hooks")]
    async fn actual_deferred_ha_child_is_cancelled_when_staging_errors_or_panics_at_activation() {
        for _ in 0..3 {
            for (fault, expected) in [
                (
                    StagingCommand::Fail("injected successor staging failure"),
                    "injected successor staging failure",
                ),
                (
                    StagingCommand::Panic("injected successor staging panic"),
                    "injected successor staging panic",
                ),
            ] {
                let root = tempfile::tempdir().unwrap();
                let archive = actual_child_test_archive(&root.path().join("archive"));
                archive.initialize_checkpoint().await.unwrap();
                let activation_gate = HaServiceActivationGate::new();
                // If the assertion path unwinds, release the real child instead of
                // stranding it at the private activation barrier.
                let _activation_release = activation_gate.release_guard();
                let startup = HaStartupConfig::new(
                    actual_child_test_node_config(&root.path().join("node")),
                    archive,
                    DurabilityMode::Sync,
                    60_000,
                    HaStartupMode::Bootstrap,
                )
                .with_service_activation_gate(activation_gate.clone());

                let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let recorder_address = recorder_listener.local_addr().unwrap();
                let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let service_address = service_listener.local_addr().unwrap();
                let serve = SuccessorServeConfig {
                    recorder_listener,
                    service_listener: None,
                    recorder_transport: HaRecorderTransport::Http,
                    recorders: actual_child_test_recorders(&root.path().join("recorders")),
                    log_peers: Vec::new(),
                    admin: None,
                    tail_token: None,
                    staging_close_error: None,
                    cleanup_token_observer: None,
                    recorder_start_error: None,
                    recorder_shutdown_outcome: None,
                    open_shutdown_token_observer: None,
                    staging_accept_faults: None,
                };
                let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
                let (started, started_rx) = tokio::sync::oneshot::channel();
                let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                    ListenerLease::new(service_listener),
                    Arc::new(AtomicBool::new(false)),
                    command_rx,
                    started,
                ));
                started_rx.await.unwrap();
                let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
                let (state, _state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                    status: HaNodeStatus::Transitioning,
                    handle: None,
                    terminal_error: None,
                });

                let supervisor = supervise_live_successor_child_after_staging(
                    startup,
                    serve,
                    &command,
                    &mut staging,
                    shutdown_rx,
                    state,
                );
                tokio::pin!(supervisor);
                tokio::time::timeout(Duration::from_secs(10), async {
                    tokio::select! {
                        () = activation_gate.entered() => {},
                        result = &mut supervisor => panic!("actual HA child stopped before activation gate: {result:?}"),
                    }
                })
                .await
                .expect("actual HA child must complete prepare/open/router before the fault");

                command.send_replace(fault);
                let error = tokio::time::timeout(Duration::from_secs(5), &mut supervisor)
                    .await
                    .expect("staging fault must promptly cancel the actual HA child")
                    .unwrap_err();
                assert!(
                    matches!(&error, HaNodeError::ServiceServer(message) if message.contains(expected)),
                    "staging role/source was not preserved: {error:?}"
                );
                let recorder_rebound = tokio::net::TcpListener::bind(recorder_address)
                    .await
                    .unwrap();
                let service_rebound = tokio::net::TcpListener::bind(service_address)
                    .await
                    .unwrap();
                drop((recorder_rebound, service_rebound));
            }
        }
    }

    #[tokio::test]
    async fn outer_shutdown_aborts_and_reaps_blocked_actual_handoff_before_its_same_deadline() {
        for _ in 0..25 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
            ));
            started_rx.await.unwrap();

            let (gate, entered) = TestStagingHandoffGate::new();
            let _release = gate.release_guard();
            let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
            let child = blocked_pre_service_test_child(HaNodeStatus::Starting, None, cancelled);
            let (service_ready, mut service_ready_rx) = tokio::sync::oneshot::channel();
            service_ready.send(()).unwrap();
            let (service_listener, _service_listener_rx) = tokio::sync::oneshot::channel();
            let (shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
            let (state, state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Transitioning,
                handle: None,
                terminal_error: None,
            });

            let supervisor = supervise_live_successor_child_after_staging_with_child(
                child,
                &mut service_ready_rx,
                service_listener,
                &command,
                &mut staging,
                shutdown_rx,
                state,
                Some(StagingCommand::BlockedHandoff(Arc::clone(&gate))),
            );
            tokio::pin!(supervisor);
            tokio::time::timeout(Duration::from_secs(1), async {
                tokio::select! {
                    result = &mut supervisor => panic!("supervisor returned before reaching handoff: {result:?}"),
                    result = entered => result.expect("actual staging handoff gate dropped"),
                }
            })
            .await
            .expect("production supervisor must enter its actual handoff wait");

            let token = Arc::new(ShutdownToken::new(Duration::from_millis(300)));
            let deadline = token.deadline();
            let token_identity = shutdown_token_identity(&token);
            shutdown.send_replace(Some(token));
            tokio::time::timeout_at(deadline, &mut supervisor)
                .await
                .expect("handoff cancellation must finish before the original D")
                .unwrap();
            let (cancelled_identity, cancelled_deadline) =
                tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(
                cancelled_identity, token_identity,
                "must not mint a fresh shutdown token"
            );
            assert_eq!(
                cancelled_deadline, deadline,
                "child must retain the outer absolute D"
            );
            assert_eq!(state_rx.borrow().status, HaNodeStatus::Stopped);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn completed_handoff_and_outer_shutdown_both_ready_drop_undelivered_lease_cleanly() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        for _ in 0..25 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let mut staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
            ));
            started_rx.await.unwrap();
            let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
            client
                .write_all(
                    b"GET /livez HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n",
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            tokio::time::timeout(Duration::from_secs(1), async {
                let mut byte = [0_u8; 1];
                while !response.ends_with(b"\r\n\r\n") {
                    client.read_exact(&mut byte).await.unwrap();
                    response.push(byte[0]);
                }
            })
            .await
            .unwrap();

            let (gate, entered) = TestStagingHandoffGate::new();
            let release = gate.release_guard();
            let (cancelled, cancelled_rx) = tokio::sync::oneshot::channel();
            let child = blocked_pre_service_test_child(HaNodeStatus::Starting, None, cancelled);
            let (service_ready, mut service_ready_rx) = tokio::sync::oneshot::channel();
            service_ready.send(()).unwrap();
            let (service_listener, _service_listener_rx) = tokio::sync::oneshot::channel();
            let (shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
            let (state, state_rx) = tokio::sync::watch::channel(HaNodeSnapshot {
                status: HaNodeStatus::Transitioning,
                handle: None,
                terminal_error: None,
            });
            let staging_status = staging.abort_handle();
            let supervisor = supervise_live_successor_child_after_staging_with_child(
                child,
                &mut service_ready_rx,
                service_listener,
                &command,
                &mut staging,
                shutdown_rx,
                state,
                Some(StagingCommand::BlockedHandoff(Arc::clone(&gate))),
            );
            tokio::pin!(supervisor);
            tokio::time::timeout(Duration::from_secs(1), async {
                tokio::select! {
                    result = &mut supervisor => panic!("supervisor returned before handoff: {result:?}"),
                    result = entered => result.expect("handoff gate dropped"),
                }
            })
            .await
            .unwrap();
            drop(release);
            tokio::time::timeout(Duration::from_secs(1), async {
                while !staging_status.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("staging must complete Handoff while its result owns the lease");
            let mut closed = [0_u8; 1];
            assert_eq!(client.read(&mut closed).await.unwrap(), 0);

            let token = Arc::new(ShutdownToken::new(Duration::from_millis(250)));
            let deadline = token.deadline();
            let token_identity = shutdown_token_identity(&token);
            shutdown.send_replace(Some(token));
            tokio::time::timeout_at(deadline, &mut supervisor)
                .await
                .expect("both-ready handoff cleanup must finish before external D")
                .unwrap();
            let (cancelled_identity, cancelled_deadline) = cancelled_rx.await.unwrap();
            assert_eq!(cancelled_identity, token_identity);
            assert_eq!(cancelled_deadline, deadline);
            assert_eq!(state_rx.borrow().status, HaNodeStatus::Stopped);
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    #[cfg(feature = "test-hooks")]
    async fn service_activation_gate_keeps_a_release_between_checks() {
        for _ in 0..100 {
            let gate = HaServiceActivationGate::new();
            // Release before the waiter registers: watch retains the value,
            // so this cannot lose the only wakeup.
            drop(gate.release_guard());
            tokio::time::timeout(Duration::from_secs(1), gate.wait())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), gate.entered())
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn staging_owner_drop_and_panic_release_the_actual_listener() {
        for _ in 0..100 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (_command, command_rx) = tokio::sync::watch::channel(StagingCommand::Running);
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let staging = AbortStagingServerOnDrop::new(spawn_successor_staging_server(
                ListenerLease::new(listener),
                Arc::new(AtomicBool::new(false)),
                command_rx,
                started,
            ));
            started_rx.await.unwrap();
            drop(staging);
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if let Ok(rebound) = tokio::net::TcpListener::bind(address).await {
                        drop(rebound);
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let panic = tokio::spawn(async move {
                let _lease = ListenerLease::new(listener);
                panic!("staging lease panic");
            });
            assert!(panic.await.unwrap_err().is_panic());
            let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
            drop(rebound);
        }
    }

    #[tokio::test]
    async fn listener_owner_receipts_after_panic_only_after_listener_drop() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (receipt, receipt_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _owner = ListenerOwner::new(listener, receipt);
            panic!("owner panic before normal receipt");
        });
        assert!(task.await.unwrap_err().is_panic());
        receipt_rx.await.unwrap();
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }

    #[tokio::test]
    async fn listener_owner_receipts_after_task_error_only_after_listener_drop() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (receipt, receipt_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _owner = ListenerOwner::new(listener, receipt);
            Err::<(), HaNodeError>(HaNodeError::ServiceServer("test task error".into()))
        });
        assert!(matches!(
            task.await.unwrap(),
            Err(HaNodeError::ServiceServer(_))
        ));
        receipt_rx.await.unwrap();
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }

    #[tokio::test]
    async fn accept_backoff_is_interrupted_by_shutdown() {
        let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
        shutdown.send_replace(true);
        assert!(tokio::time::timeout(
            Duration::from_millis(100),
            wait_for_accept_backoff(&mut shutdown_rx, Duration::from_secs(1)),
        )
        .await
        .unwrap());
    }

    #[test]
    fn quarantined_recorders_do_not_contribute_empty_read_fence_votes() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let recorders = membership
            .members()
            .iter()
            .enumerate()
            .map(|(index, node_id)| {
                let recorder = RecorderFileStore::new_with_membership(
                    root.path().join(node_id),
                    node_id.clone(),
                    "cluster-a",
                    1,
                    1,
                    membership.clone(),
                )
                .unwrap();
                let recorder = if index == 0 {
                    HaRecorder::active(recorder)
                } else {
                    HaRecorder::quarantined(recorder, LogAnchor::new(0, LogHash::ZERO))
                };
                (node_id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster-a", "node-1", 1, 1, recorders)
                .unwrap();

        assert_eq!(
            consensus
                .inspect_context_read_fence_at(
                    &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                    1,
                    LogHash::ZERO,
                )
                .unwrap(),
            rhiza_quepaxa::CertifiedDecisionInspection::Unavailable
        );
    }

    #[tokio::test]
    async fn ready_rejects_a_ready_snapshot_after_shutdown_is_requested() {
        let (shutdown, _shutdown_rx) = tokio::sync::watch::channel(None);
        let (_state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Ready,
            handle: Some(RhizaHandle {
                inner: std::sync::Weak::new(),
            }),
            terminal_error: None,
        });
        let node = HaNode {
            shutdown,
            recorder_shutdown: tokio::sync::watch::channel(false).0,
            startup: StartupIoContext::new(),
            state,
            supervisor: None,
        };
        fn assert_send<T: Send>(_: T) {}
        assert_send(node.ready());

        request_ha_shutdown(
            &node.shutdown,
            Arc::new(ShutdownToken::new(HA_SERVER_SHUTDOWN_TIMEOUT)),
            || {},
        );

        assert!(matches!(node.ready().await, Err(HaNodeError::Cancelled)));
    }

    #[tokio::test]
    async fn service_start_prefers_shutdown_when_both_signals_are_ready() {
        let token = Arc::new(ShutdownToken::new(Duration::from_secs(1)));
        let deadline = token.deadline();
        let (_shutdown, mut shutdown_rx) = tokio::sync::watch::channel(Some(token));
        let (started, started_rx) = tokio::sync::oneshot::channel();
        started.send(()).unwrap();

        assert_eq!(
            wait_for_service_start_or_shutdown(started_rx, &mut shutdown_rx)
                .await
                .unwrap(),
            ServiceStartup::Shutdown(deadline)
        );
    }

    #[test]
    fn earlier_shutdown_request_replaces_the_prior_token() {
        let (shutdown, _receiver) = tokio::sync::watch::channel::<ShutdownSignal>(None);
        let admissions_closed = Arc::new(AtomicUsize::new(0));
        let first_closed = Arc::clone(&admissions_closed);
        let first = request_ha_shutdown(
            &shutdown,
            Arc::new(ShutdownToken::new(Duration::from_secs(1))),
            move || {
                first_closed.fetch_add(1, Ordering::AcqRel);
            },
        );
        let second_closed = Arc::clone(&admissions_closed);
        let second = request_ha_shutdown(
            &shutdown,
            Arc::new(ShutdownToken::new(Duration::ZERO)),
            move || {
                second_closed.fetch_add(1, Ordering::AcqRel);
            },
        );

        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(admissions_closed.load(Ordering::Acquire), 2);
    }

    #[test]
    fn shutdown_authority_precedence_matches_startup_cancellation_under_stress() {
        for _ in 0..1_000 {
            let (shutdown, shutdown_rx) = tokio::sync::watch::channel::<ShutdownSignal>(None);
            let startup = StartupIoContext::new();
            let internal = Arc::new(ShutdownToken::new_internal(Duration::ZERO));
            let external = Arc::new(ShutdownToken::new(Duration::from_secs(1)));
            request_ha_shutdown(&shutdown, Arc::clone(&internal), || {});

            let barrier = Arc::new(std::sync::Barrier::new(2));
            let external_shutdown = shutdown.clone();
            let external_token = Arc::clone(&external);
            let external_barrier = Arc::clone(&barrier);
            let external_request = std::thread::spawn(move || {
                external_barrier.wait();
                request_ha_shutdown(&external_shutdown, external_token, || {});
            });
            let internal_shutdown = shutdown.clone();
            let internal_token = Arc::clone(&internal);
            let internal_request = std::thread::spawn(move || {
                barrier.wait();
                request_ha_shutdown(&internal_shutdown, internal_token, || {});
            });
            external_request.join().unwrap();
            internal_request.join().unwrap();

            let observed = shutdown_rx.borrow().clone().unwrap();
            assert!(Arc::ptr_eq(&observed, &external));
            assert_eq!(observed.deadline(), external.deadline());
            cancel_startup_for_token(&startup, &internal);
            cancel_startup_for_token(&startup, &external);
            assert!(startup.is_cancelled_by(external.startup_token()));
            assert!(!startup.is_cancelled_by(internal.startup_token()));
        }

        let (shutdown, shutdown_rx) = tokio::sync::watch::channel::<ShutdownSignal>(None);
        let external = Arc::new(ShutdownToken::new(Duration::from_secs(1)));
        let later_internal = Arc::new(ShutdownToken::new_internal(Duration::ZERO));
        request_ha_shutdown(&shutdown, Arc::clone(&external), || {});
        request_ha_shutdown(&shutdown, later_internal, || {});
        let observed = shutdown_rx.borrow().clone().unwrap();
        assert!(Arc::ptr_eq(&observed, &external));
    }

    #[cfg(feature = "sql")]
    async fn shutdown_test_owner() -> (Rhiza, tempfile::TempDir) {
        let root = tempfile::tempdir().unwrap();
        let config = crate::EmbeddedConfig::local_file_backed(
            "shutdown-producer-test",
            root.path(),
            ExecutionProfile::Sqlite,
        )
        .unwrap();
        let owner = Rhiza::open(config).await.unwrap();
        (owner, root)
    }

    #[cfg(feature = "sql")]
    async fn clear_shutdown_test_workers(owner: &mut Rhiza) {
        owner.workers.abort_all();
        while owner.workers.join_next().await.is_some() {}
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn shutdown_owner_error_maps_actual_shutdown_gate_deadline_and_cleanup() {
        let (mut owner, _root) = shutdown_test_owner().await;
        clear_shutdown_test_workers(&mut owner).await;
        let operations = owner.inner.as_ref().unwrap().operations.clone();
        let operation = operations.read_owned().await;
        owner.workers.spawn(async { Err(crate::Error::Closed) });

        let lower = owner
            .shutdown_with_timeout(Duration::from_millis(1))
            .await
            .unwrap_err();
        drop(operation);

        assert!(matches!(
            shutdown_owner_error(lower, UNCERTAIN_SHUTDOWN_EVIDENCE),
            HaNodeError::Cleanup { primary, cleanup }
                if matches!(
                    primary.as_ref(),
                    HaNodeError::ShutdownDeadlineExceeded {
                        phase: HaShutdownPhase::InFlightOperations,
                        mutation: MutationCertainty::Uncertain {
                            local_io: true,
                            recorder_rpc: true,
                            ..
                        },
                        ..
                    }
                ) && matches!(
                    cleanup.as_ref(),
                    HaNodeError::ShutdownIncomplete {
                        phase: HaShutdownPhase::BackgroundWorkers,
                        cause: HaShutdownCause::Source(error),
                        mutation: MutationCertainty::Uncertain { local_io: true, .. },
                        ..
                    } if matches!(error.as_ref(), crate::Error::Closed)
                )
        ));
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn shutdown_owner_error_maps_actual_worker_unknown_outcome_and_panic() {
        let (mut owner, _root) = shutdown_test_owner().await;
        clear_shutdown_test_workers(&mut owner).await;
        owner.workers.spawn(async {
            Err(crate::Error::Consensus(
                rhiza_quepaxa::Error::UnknownOutcome,
            ))
        });
        let unknown = owner
            .shutdown_with_timeout(Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(
            shutdown_owner_error(unknown, UNCERTAIN_SHUTDOWN_EVIDENCE),
            HaNodeError::ShutdownIncomplete {
                phase: HaShutdownPhase::BackgroundWorkers,
                cause: HaShutdownCause::RecorderOutcomeUnknown,
                mutation: MutationCertainty::Uncertain { local_io: true, .. },
                ..
            }
        ));

        let (mut owner, _root) = shutdown_test_owner().await;
        clear_shutdown_test_workers(&mut owner).await;
        owner
            .workers
            .spawn(async { panic!("shutdown worker test panic") });
        let panic = owner
            .shutdown_with_timeout(Duration::from_secs(1))
            .await
            .unwrap_err();
        let mapped = shutdown_owner_error(panic, UNCERTAIN_SHUTDOWN_EVIDENCE);
        assert!(matches!(
            mapped,
            HaNodeError::ShutdownIncomplete {
                phase: HaShutdownPhase::BackgroundWorkers,
                cause: HaShutdownCause::TaskFailure(ref error),
                mutation: MutationCertainty::Uncertain { local_io: true, .. },
                ..
            } if error.failure() == crate::ShutdownTaskFailure::Panicked
        ));
        let cause = std::error::Error::source(&mapped).unwrap();
        assert!(cause.downcast_ref::<HaShutdownCause>().is_some());
        let worker = cause.source().unwrap();
        assert!(worker.downcast_ref::<crate::WorkerError>().is_some());
        let join = worker.source().unwrap();
        assert!(join.downcast_ref::<tokio::task::JoinError>().is_some());
    }

    #[tokio::test]
    async fn legacy_service_task_completion_without_a_receipt_keeps_ingress_uncertain() {
        for _ in 0..200 {
            let mut task = HaServerTask::spawn(async {
                Err::<(), HaNodeError>(HaNodeError::ServiceServer("test service error".into()))
            });
            let (result, listener_ended) = wait_for_ha_server_with_receipt(
                &mut task,
                "service server",
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await;
            assert!(!listener_ended);
            assert!(matches!(result, Err(HaNodeError::ServiceServer(_))));
            assert_eq!(
                ingress_after_service_wait(listener_ended),
                IngressDisposition::Uncertain
            );
        }

        let mut task = HaServerTask::spawn(async {
            panic!("service join failure");
            #[allow(unreachable_code)]
            Ok::<(), HaNodeError>(())
        });
        let (result, listener_ended) = wait_for_ha_server_with_receipt(
            &mut task,
            "service server",
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert!(!listener_ended);
        assert!(matches!(result, Err(HaNodeError::ServiceServer(_))));
        assert_eq!(
            ingress_after_service_wait(listener_ended),
            IngressDisposition::Uncertain
        );
    }

    #[cfg(feature = "test-hooks")]
    #[tokio::test]
    async fn service_error_and_admin_deadline_preserve_primary_and_cleanup_evidence() {
        let tracker = AdminTaskTracker::test_tracker();
        let admitted = tracker
            .test_start_admitted()
            .expect("admin operation is admitted before shutdown starts");
        let mut scope = HaTaskScope::new(
            AdminDrainLease::Enabled(tracker.clone()),
            ServiceTaskUnstarted::OwnedListenerDropped,
        );
        let (service, service_error_ready) =
            test_service_ingress_error_with_receipt_and_ready("exact service source failure");
        scope.start_service(service);
        service_error_ready
            .await
            .expect("the exact service source error is ready before the admin deadline");

        scope.begin_shutdown_for_test();
        assert!(
            tracker.test_start_admitted().is_none(),
            "shutdown closes admission before the shared deadline begins"
        );
        let shutdown = scope
            .drain_before(tokio::time::Instant::now() + Duration::from_millis(150))
            .await;

        let Err(HaNodeError::Cleanup { primary, cleanup }) = shutdown.result else {
            panic!("service source error and admin deadline must both be retained");
        };
        assert!(matches!(
            *primary,
            HaNodeError::ServiceServer(ref message) if message == "exact service source failure"
        ));
        assert!(matches!(
            *cleanup,
            HaNodeError::ShutdownDeadlineExceeded {
                phase: HaShutdownPhase::Service,
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Uncertain,
                mutation: CONSERVATIVE_SHUTDOWN_MUTATION,
            }
        ));
        assert_eq!(
            shutdown.evidence,
            ShutdownEvidence {
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Uncertain,
            }
        );

        drop(admitted);
    }

    #[tokio::test]
    async fn timeout_deadlines_never_understate_possible_mutation() {
        let mut server = HaServerTask::spawn(std::future::pending::<Result<(), HaNodeError>>());
        let (server_error, listener_ended) = wait_for_ha_server_with_receipt(
            &mut server,
            "service server",
            tokio::time::Instant::now(),
        )
        .await;
        assert!(!listener_ended);
        assert!(matches!(
            server_error,
            Err(HaNodeError::ShutdownDeadlineExceeded {
                phase: HaShutdownPhase::Service,
                mutation: CONSERVATIVE_SHUTDOWN_MUTATION,
                ..
            })
        ));

        let mut supervisor =
            AbortOnDropTask::spawn(std::future::pending::<Result<(), HaNodeError>>());
        assert!(matches!(
            wait_for_ha_supervisor_before(
                &mut supervisor,
                tokio::time::Instant::now(),
                "test supervisor",
            )
            .await,
            Err(HaNodeError::ShutdownDeadlineExceeded {
                phase: HaShutdownPhase::Supervisor,
                mutation: CONSERVATIVE_SHUTDOWN_MUTATION,
                ..
            })
        ));

        assert!(matches!(
            conservative_shutdown_deadline_error(
                HaShutdownPhase::Service,
                ShutdownEvidence {
                    ingress: IngressDisposition::Closed,
                    tasks: TaskDisposition::Uncertain,
                },
            ),
            HaNodeError::ShutdownDeadlineExceeded {
                phase: HaShutdownPhase::Service,
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Uncertain,
                mutation: CONSERVATIVE_SHUTDOWN_MUTATION,
            }
        ));
    }

    #[test]
    fn lower_shutdown_deadlines_keep_phase_specific_mutation_flags() {
        assert_eq!(
            lower_shutdown_mutation(HaShutdownPhase::InFlightOperations),
            MutationCertainty::Uncertain {
                local_io: true,
                recorder_rpc: true,
                activation: false,
            }
        );
        assert_eq!(
            lower_shutdown_mutation(HaShutdownPhase::BackgroundWorkers),
            MutationCertainty::Uncertain {
                local_io: true,
                recorder_rpc: false,
                activation: false,
            }
        );
        assert_eq!(
            lower_shutdown_mutation(HaShutdownPhase::AppliedTipFlush),
            MutationCertainty::Uncertain {
                local_io: true,
                recorder_rpc: false,
                activation: false,
            }
        );
    }

    #[tokio::test]
    async fn missing_supervisor_is_an_immediate_ownership_failure_not_a_deadline() {
        let node = HaNode {
            shutdown: tokio::sync::watch::channel(None).0,
            recorder_shutdown: tokio::sync::watch::channel(false).0,
            startup: StartupIoContext::new(),
            state: test_state(),
            supervisor: None,
        };
        let node_error = tokio::time::timeout(
            Duration::from_millis(100),
            node.shutdown_with_timeout(Duration::from_secs(60)),
        )
        .await
        .expect("missing ownership must not wait for the deadline")
        .unwrap_err();
        assert!(matches!(
            node_error,
            HaNodeError::Supervisor(message)
                if message == "HA node supervisor ownership missing during shutdown"
        ));

        let (shutdown, _) = tokio::sync::watch::channel(None);
        let (predecessor, _) = tokio::sync::mpsc::unbounded_channel();
        let successor = HaSuccessorNode {
            shutdown,
            predecessor,
            predecessor_binding: Mutex::new(None),
            state: test_state(),
            supervisor: None,
        };
        let successor_error = tokio::time::timeout(
            Duration::from_millis(100),
            successor.shutdown_with_timeout(Duration::from_secs(60)),
        )
        .await
        .expect("missing ownership must not wait for the deadline")
        .unwrap_err();
        assert!(matches!(
            successor_error,
            HaNodeError::Supervisor(message)
                if message == "live successor supervisor ownership missing during shutdown"
        ));
    }

    #[test]
    fn shutdown_error_types_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<crate::Error>();
        assert_send_sync::<crate::ShutdownError>();
        assert_send_sync::<crate::ShutdownCause>();
        assert_send_sync::<crate::WorkerError>();
        assert_send_sync::<HaShutdownCause>();
        assert_send_sync::<HaNodeError>();
    }

    #[test]
    fn opened_startup_cleanup_drops_the_owned_listener_before_closed_evidence() {
        let lower = crate::shutdown_error_from_source(
            crate::ShutdownPhase::AppliedTipFlush,
            crate::Error::Closed,
        );
        assert!(matches!(
            shutdown_owner_error(
                crate::combine_shutdown_errors(vec![lower]).unwrap_err(),
                PRE_SERVICE_SHUTDOWN_EVIDENCE,
            ),
            HaNodeError::ShutdownIncomplete {
                phase: HaShutdownPhase::AppliedTipFlush,
                cause: HaShutdownCause::Source(error),
                mutation: MutationCertainty::Uncertain { local_io: true, .. },
                ingress: IngressDisposition::Closed,
                tasks: TaskDisposition::Quiesced,
                ..
            } if matches!(error.as_ref(), crate::Error::Closed)
        ));
    }

    #[test]
    fn shutdown_cancellation_requires_the_exact_token_and_preserves_sources() {
        for _ in 0..200 {
            let startup = StartupIoContext::new();
            let token = Arc::new(ShutdownToken::new(Duration::from_secs(1)));
            let other = Arc::new(ShutdownToken::new(Duration::from_secs(1)));
            cancel_startup_for_token(&startup, &token);

            let preparation_cancellation = HaStartupError::Cancelled(token.startup_token().clone());
            assert!(is_requested_startup_cancellation(
                &preparation_cancellation,
                &startup,
                Some(&token),
            ));
            assert!(!is_requested_startup_cancellation(
                &preparation_cancellation,
                &startup,
                Some(&other),
            ));
            assert!(!is_requested_startup_cancellation(
                &HaStartupError::Source("preparation source failure".into()),
                &startup,
                Some(&token),
            ));
            assert!(matches!(
                completed_preparation_after_shutdown(
                    Ok(Err(HaStartupError::Source("preparation source failure".into()))),
                    &startup,
                    Some(&token),
                ),
                Err(HaNodeError::Startup(HaStartupError::Source(message)))
                    if message == "preparation source failure"
            ));
            assert!(completed_preparation_after_shutdown(
                Ok(Err(preparation_cancellation.clone())),
                &startup,
                Some(&token),
            )
            .is_ok());

            let node_cancellation = NodeError::StartupCancelled {
                token: token.startup_token().clone(),
                stage: "test stage".into(),
            };
            assert!(is_requested_node_cancellation(
                &node_cancellation,
                &startup,
                Some(&token),
            ));
            assert!(!is_requested_node_cancellation(
                &node_cancellation,
                &startup,
                Some(&other),
            ));
            assert!(!is_requested_node_cancellation(
                &NodeError::Fatal("activation/open/rehydrate source failure".into()),
                &startup,
                Some(&token),
            ));
            assert!(matches!(
                completed_activation_after_shutdown(
                    Ok(Err(NodeError::Fatal("activation source failure".into()))),
                    &startup,
                    Some(&token),
                ),
                Err(HaNodeError::Startup(HaStartupError::Source(message)))
                    if message.contains("activation source failure")
            ));
            assert!(matches!(
                node_error_after_shutdown(
                    NodeError::Fatal("activation source failure".into()),
                    &startup,
                    Some(&token),
                ),
                Err(HaStartupError::Source(message)) if message.contains("activation source failure")
            ));
            assert!(matches!(
                node_error_after_shutdown(
                    NodeError::Fatal("open source failure".into()),
                    &startup,
                    Some(&token),
                ),
                Err(HaStartupError::Source(message)) if message.contains("open source failure")
            ));
            assert!(matches!(
                node_error_after_shutdown(
                    NodeError::Fatal("rehydrate source failure".into()),
                    &startup,
                    Some(&token),
                ),
                Err(HaStartupError::Source(message)) if message.contains("rehydrate source failure")
            ));
        }
    }

    fn test_state() -> tokio::sync::watch::Receiver<HaNodeSnapshot> {
        tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Starting,
            handle: None,
            terminal_error: None,
        })
        .1
    }

    #[tokio::test]
    async fn node_shutdown_passes_the_exact_token_identity() {
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel::<ShutdownSignal>(None);
        let token = Arc::new(ShutdownToken::new(Duration::from_secs(1)));
        let startup = StartupIoContext::new();
        let node = HaNode {
            shutdown,
            recorder_shutdown: tokio::sync::watch::channel(false).0,
            startup: startup.clone(),
            state: test_state(),
            supervisor: Some(AbortOnDropTask::spawn(async { Ok(()) })),
        };

        node.shutdown_with_token(Arc::clone(&token)).await.unwrap();
        let observed = shutdown_rx.borrow().clone().unwrap();
        assert_eq!(
            shutdown_token_identity(&observed),
            shutdown_token_identity(&token)
        );
        assert_eq!(observed.deadline(), token.deadline());
        assert!(startup.is_cancelled_by(token.startup_token()));
    }

    #[tokio::test]
    async fn successor_shutdown_passes_the_exact_token_identity() {
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel::<ShutdownSignal>(None);
        let (predecessor, _predecessor_rx) = tokio::sync::mpsc::unbounded_channel();
        let token = Arc::new(ShutdownToken::new(Duration::from_secs(1)));
        let successor = HaSuccessorNode {
            shutdown,
            predecessor,
            predecessor_binding: Mutex::new(None),
            state: test_state(),
            supervisor: Some(AbortOnDropTask::spawn(async { Ok(()) })),
        };

        successor
            .shutdown_with_token(Arc::clone(&token))
            .await
            .unwrap();
        let observed = shutdown_rx.borrow().clone().unwrap();
        assert_eq!(
            shutdown_token_identity(&observed),
            shutdown_token_identity(&token)
        );
        assert_eq!(observed.deadline(), token.deadline());
    }

    #[tokio::test]
    async fn external_deadline_replaces_an_earlier_internal_cleanup_deadline() {
        let (shutdown, _shutdown_rx) = tokio::sync::watch::channel::<ShutdownSignal>(None);
        request_ha_shutdown(
            &shutdown,
            Arc::new(ShutdownToken::new(HA_SERVER_SHUTDOWN_TIMEOUT)),
            || {},
        );
        let node = HaNode {
            shutdown,
            recorder_shutdown: tokio::sync::watch::channel(false).0,
            startup: StartupIoContext::new(),
            state: test_state(),
            supervisor: Some(AbortOnDropTask::spawn(async {
                std::future::pending::<Result<(), HaNodeError>>().await
            })),
        };

        let result = tokio::time::timeout(
            Duration::from_millis(100),
            node.shutdown_with_timeout(Duration::from_millis(5)),
        )
        .await
        .expect("external deadline must remain a hard bound")
        .unwrap_err();
        assert!(matches!(
            result,
            HaNodeError::ShutdownDeadlineExceeded {
                phase: HaShutdownPhase::Supervisor,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn supervisor_panic_before_deadline_is_typed() {
        let mut supervisor = AbortOnDropTask::spawn(async {
            panic!("supervisor test panic");
            #[allow(unreachable_code)]
            Ok::<(), HaNodeError>(())
        });
        let error = wait_for_ha_supervisor_before(
            &mut supervisor,
            tokio::time::Instant::now() + Duration::from_secs(1),
            "test supervisor",
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            HaNodeError::Supervisor(message) if message.contains("test supervisor task failed")
        ));
    }

    #[tokio::test]
    async fn supervisor_result_mapping_is_monotonic_at_the_shutdown_deadline() {
        let mut before_deadline = AbortOnDropTask::spawn(async {
            Err(HaNodeError::StartupIoDeadlineExceeded {
                stage: "before deadline".into(),
            })
        });
        assert!(matches!(
            wait_for_ha_supervisor_before(
                &mut before_deadline,
                tokio::time::Instant::now() + Duration::from_secs(1),
                "test supervisor",
            )
            .await,
            Err(HaNodeError::StartupIoDeadlineExceeded { stage }) if stage == "before deadline"
        ));

        let mut at_deadline = AbortOnDropTask::spawn(async {
            Err(HaNodeError::StartupIoDeadlineExceeded {
                stage: "at deadline".into(),
            })
        });
        tokio::task::yield_now().await;
        assert!(matches!(
            wait_for_ha_supervisor_before(
                &mut at_deadline,
                tokio::time::Instant::now(),
                "test supervisor",
            )
            .await,
            Err(HaNodeError::ShutdownDeadlineExceeded {
                phase: HaShutdownPhase::Supervisor,
                mutation: CONSERVATIVE_SHUTDOWN_MUTATION,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn server_panics_before_deadline_keep_their_server_role() {
        for (name, recorder) in [
            ("recorder server", true),
            ("service server", false),
            ("successor staging service", false),
        ] {
            let mut task = HaServerTask::spawn(async {
                panic!("server test panic");
                #[allow(unreachable_code)]
                Ok::<(), HaNodeError>(())
            });
            let error = wait_for_ha_server(
                &mut task,
                name,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap_err();
            assert!(match error {
                HaNodeError::RecorderServer(message) if recorder => {
                    message.contains("recorder server task failed")
                }
                HaNodeError::ServiceServer(message) if !recorder => {
                    message.contains(&format!("{name} task failed"))
                }
                _ => false,
            });
        }
    }

    #[tokio::test]
    async fn monitor_channel_close_is_a_supervisor_failure() {
        let (state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Starting,
            handle: None,
            terminal_error: None,
        });
        drop(state_tx);
        assert!(matches!(
            monitor_ha_state(state).await,
            Err(HaNodeError::Supervisor(message)) if message.contains("monitor state channel closed")
        ));

        let (state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Starting,
            handle: None,
            terminal_error: None,
        });
        let node = HaNode {
            shutdown: tokio::sync::watch::channel(None).0,
            recorder_shutdown: tokio::sync::watch::channel(false).0,
            startup: StartupIoContext::new(),
            state,
            supervisor: None,
        };
        drop(state_tx);
        assert!(matches!(
            node.monitor().await,
            Err(HaNodeError::Supervisor(message)) if message.contains("monitor state channel closed")
        ));
    }

    #[tokio::test]
    async fn shutdown_server_exit_races_preserve_the_server_error() {
        for _ in 0..200 {
            let (shutdown, _shutdown_rx) = tokio::sync::watch::channel(false);
            let mut already_finished = HaServerTask::spawn(async {
                Err::<(), HaNodeError>(HaNodeError::ServiceServer("before stop".into()))
            });
            while !already_finished.is_finished() {
                tokio::task::yield_now().await;
            }
            assert!(matches!(
                stop_ha_server_before(
                    &shutdown,
                    &mut already_finished,
                    "service server",
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await,
                Err(HaNodeError::ServiceServer(message)) if message == "before stop"
            ));

            let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
            let mut after_stop = HaServerTask::spawn(async move {
                shutdown_rx.changed().await.unwrap();
                Err::<(), HaNodeError>(HaNodeError::ServiceServer("after stop".into()))
            });
            assert!(matches!(
                stop_ha_server_before(
                    &shutdown,
                    &mut after_stop,
                    "service server",
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await,
                Err(HaNodeError::ServiceServer(message)) if message == "after stop"
            ));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deadline_aborts_and_detaches_supervisor_without_leaking_tasks_under_stress() {
        struct ActiveGuard(Arc<AtomicUsize>);

        impl Drop for ActiveGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }

        for _ in 0..200 {
            let active = Arc::new(AtomicUsize::new(0));
            let worker_active = Arc::clone(&active);
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let mut supervisor = AbortOnDropTask::spawn(async move {
                worker_active.fetch_add(1, Ordering::AcqRel);
                let _active = ActiveGuard(worker_active);
                entered_tx.send(()).unwrap();
                std::future::pending::<Result<(), HaNodeError>>().await
            });
            entered_rx.await.unwrap();

            assert!(matches!(
                wait_for_ha_supervisor_before(
                    &mut supervisor,
                    tokio::time::Instant::now(),
                    "test supervisor",
                )
                .await,
                Err(HaNodeError::ShutdownDeadlineExceeded {
                    phase: HaShutdownPhase::Supervisor,
                    ..
                })
            ));
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            while active.load(Ordering::Acquire) != 0 {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "aborted supervisor task must release its owned work"
                );
                tokio::task::yield_now().await;
            }
        }
    }

    #[tokio::test]
    async fn owned_task_keeps_a_ready_result_at_the_exact_deadline() {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed_polls = Arc::clone(&polls);
        let mut task = AbortOnDropTask::spawn(async move {
            observed_polls.fetch_add(1, Ordering::AcqRel);
            Err::<(), HaNodeError>(HaNodeError::ServiceServer(
                "exact-deadline source result".into(),
            ))
        });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            await_task_before(&mut task, tokio::time::Instant::now()).await,
            Some(Ok(Err(HaNodeError::ServiceServer(message))))
                if message == "exact-deadline source result"
        ));
        assert_eq!(polls.load(Ordering::Acquire), 1);
        assert!(
            task.is_finished(),
            "a Ready join handle is taken exactly once"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blocking_task_abort_is_not_a_late_effect_completion_claim() {
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let task_started = Arc::clone(&started);
        let task_release = Arc::clone(&release);
        let mut task = AbortOnDropTask::spawn_blocking(move || {
            task_started.store(true, Ordering::Release);
            while !task_release.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            7usize
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking task did not start");
        assert!(
            await_task_before(
                &mut task,
                tokio::time::Instant::now() + Duration::from_millis(5),
            )
            .await
            .is_none(),
            "D only requests cancellation for already-running blocking work"
        );
        release.store(true, Ordering::Release);
        assert_eq!(task.await.unwrap(), 7);
    }

    #[tokio::test]
    async fn drop_installs_one_token_before_recorder_startup_and_reaper() {
        let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel::<ShutdownSignal>(None);
        let (recorder_shutdown, recorder_shutdown_rx) = tokio::sync::watch::channel(false);
        let startup = StartupIoContext::new();
        let observer_startup = startup.clone();
        let (state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Starting,
            handle: None,
            terminal_error: None,
        });
        let (observed, observed_rx) = tokio::sync::oneshot::channel();
        let supervisor = AbortOnDropTask::spawn(async move {
            let _state = state_tx;
            shutdown_rx.changed().await.unwrap();
            let token = shutdown_rx.borrow().clone().unwrap();
            let _ = observed.send((
                shutdown_token_identity(&token),
                token.deadline(),
                *recorder_shutdown_rx.borrow(),
                observer_startup.is_cancelled_by(token.startup_token()),
            ));
            Ok(())
        });
        let node = HaNode {
            shutdown,
            recorder_shutdown,
            startup,
            state,
            supervisor: Some(supervisor),
        };
        drop(node);
        let (identity, deadline, recorder_closed, startup_cancelled) =
            tokio::time::timeout(Duration::from_secs(1), observed_rx)
                .await
                .expect("Drop reaper did not run")
                .unwrap();
        assert_ne!(identity, 0);
        assert!(deadline >= tokio::time::Instant::now());
        assert!(recorder_closed);
        assert!(startup_cancelled);
    }

    #[tokio::test]
    async fn successor_drop_cascades_its_identical_token_and_deadline() {
        let (child_shutdown, mut child_shutdown_rx) =
            tokio::sync::watch::channel::<ShutdownSignal>(None);
        let (child_seen, child_seen_rx) = tokio::sync::oneshot::channel();
        let child = HaNode {
            shutdown: child_shutdown,
            recorder_shutdown: tokio::sync::watch::channel(false).0,
            startup: StartupIoContext::new(),
            state: test_state(),
            supervisor: Some(AbortOnDropTask::spawn(async move {
                child_shutdown_rx.changed().await.unwrap();
                let token = child_shutdown_rx.borrow().clone().unwrap();
                let _ = child_seen.send((shutdown_token_identity(&token), token.deadline()));
                Ok(())
            })),
        };
        let (parent_shutdown, mut parent_shutdown_rx) =
            tokio::sync::watch::channel::<ShutdownSignal>(None);
        let (parent_seen, parent_seen_rx) = tokio::sync::oneshot::channel();
        let supervisor = AbortOnDropTask::spawn(async move {
            parent_shutdown_rx.changed().await.unwrap();
            let token = parent_shutdown_rx.borrow().clone().unwrap();
            let _ = parent_seen.send((shutdown_token_identity(&token), token.deadline()));
            child.shutdown_with_token(token).await
        });
        let (predecessor, _predecessor_rx) = tokio::sync::mpsc::unbounded_channel();
        let successor = HaSuccessorNode {
            shutdown: parent_shutdown,
            predecessor,
            predecessor_binding: Mutex::new(None),
            state: test_state(),
            supervisor: Some(supervisor),
        };
        drop(successor);
        let parent = tokio::time::timeout(Duration::from_secs(1), parent_seen_rx)
            .await
            .expect("successor Drop did not signal its supervisor")
            .unwrap();
        let child = tokio::time::timeout(Duration::from_secs(1), child_seen_rx)
            .await
            .expect("successor shutdown did not cascade to child")
            .unwrap();
        assert_eq!(child, parent);
    }

    /// The public owner may move between runtimes, but the reaper must remain
    /// on the runtime that created its JoinHandle.  B is deliberately fully
    /// destroyed before A is allowed to complete: if `reap_before` used the
    /// current/drop runtime, dropping B would drop the reaper and abort this
    /// A-owned supervisor instead of delivering `completed` below.
    #[test]
    fn node_drop_on_foreign_runtime_reaps_on_its_creator_runtime_before_original_deadline() {
        let (node_tx, node_rx) = std::sync::mpsc::sync_channel(1);
        let (shutdown_seen_tx, shutdown_seen_rx) = std::sync::mpsc::sync_channel(1);
        let (completion_tx, completion_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (creator_stop_tx, creator_stop_rx) = tokio::sync::oneshot::channel();

        let creator = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("creator runtime must build");
            runtime.block_on(async move {
                let (shutdown, mut shutdown_rx) =
                    tokio::sync::watch::channel::<ShutdownSignal>(None);
                let supervisor = AbortOnDropTask::spawn(async move {
                    shutdown_rx
                        .changed()
                        .await
                        .expect("foreign Drop must signal the creator-owned supervisor");
                    let token = shutdown_rx
                        .borrow()
                        .clone()
                        .expect("foreign Drop must install one shutdown token");
                    shutdown_seen_tx
                        .send((shutdown_token_identity(&token), token.deadline()))
                        .expect("test must still observe creator shutdown");
                    release_rx
                        .await
                        .expect("test must release the creator-owned supervisor after B exits");
                    completion_tx
                        .send(())
                        .expect("creator-owned reaper must retain the supervisor through B exit");
                    Ok::<(), HaNodeError>(())
                });
                let node = HaNode {
                    shutdown,
                    recorder_shutdown: tokio::sync::watch::channel(false).0,
                    startup: StartupIoContext::new(),
                    state: test_state(),
                    supervisor: Some(supervisor),
                };
                node_tx
                    .send(node)
                    .expect("test must receive the creator-owned node");
                creator_stop_rx
                    .await
                    .expect("test must stop the creator runtime after orderly reap");
            });
        });

        let node = node_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("creator runtime did not construct its node");
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("foreign runtime must build");
            runtime.block_on(async move {
                drop(node);
            });
            // `runtime` is dropped here before A is allowed to complete.
        })
        .join()
        .expect("foreign runtime must not panic while dropping the node");

        let (identity, deadline) = shutdown_seen_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("creator runtime did not observe the Drop token");
        assert_ne!(identity, 0);
        release_tx
            .send(())
            .expect("creator-owned supervisor must still be waiting after B shutdown");
        completion_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("creator reaper did not complete after B shutdown");
        assert!(
            tokio::time::Instant::now() < deadline,
            "reaping must complete before the original Drop deadline"
        );
        creator_stop_tx
            .send(())
            .expect("creator runtime must still be alive for its reaper");
        creator
            .join()
            .expect("creator runtime must stop after orderly reap");
    }

    /// A live creator runtime is the only place that can prove an externally
    /// installed deadline remains authoritative after the public owner moves
    /// to another runtime.  A's supervisor is deliberately pending: B drops
    /// the public owner, then the guard receipt proves A's reaper aborted the
    /// original task by the already-installed D.  The observation gate runs
    /// only after B has been destroyed, so a Drop implementation that replaces
    /// the external authority is observable as well.
    #[test]
    fn node_drop_on_foreign_runtime_keeps_preinstalled_external_deadline() {
        const EXTERNAL_DEADLINE: Duration = Duration::from_millis(500);
        // Timers are driven by A's current-thread runtime.  A 250ms allowance
        // covers scheduling latency without admitting the 25s default grace.
        const DEADLINE_TOLERANCE: Duration = Duration::from_millis(250);

        struct SupervisorDropGuard(std::sync::mpsc::SyncSender<tokio::time::Instant>);

        impl Drop for SupervisorDropGuard {
            fn drop(&mut self) {
                let _ = self.0.send(tokio::time::Instant::now());
            }
        }

        struct CreatorRuntimeGuard {
            stop: Option<tokio::sync::oneshot::Sender<()>>,
            thread: Option<std::thread::JoinHandle<()>>,
        }

        impl CreatorRuntimeGuard {
            fn stop_and_join(&mut self) -> std::thread::Result<()> {
                if let Some(stop) = self.stop.take() {
                    let _ = stop.send(());
                }
                match self.thread.take() {
                    Some(thread) => thread.join(),
                    None => Ok(()),
                }
            }
        }

        impl Drop for CreatorRuntimeGuard {
            fn drop(&mut self) {
                let _ = self.stop_and_join();
            }
        }

        fn receive_before<T>(
            receiver: &std::sync::mpsc::Receiver<T>,
            deadline: tokio::time::Instant,
        ) -> Result<T, std::sync::mpsc::RecvTimeoutError> {
            receiver.recv_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()))
        }

        let (node_tx, node_rx) = std::sync::mpsc::sync_channel(1);
        let (observed_tx, observed_rx) = std::sync::mpsc::sync_channel(1);
        let (dropped_tx, dropped_rx) = std::sync::mpsc::sync_channel(1);
        let (observe_after_drop_tx, observe_after_drop_rx) = tokio::sync::oneshot::channel();
        let (creator_stop_tx, creator_stop_rx) = tokio::sync::oneshot::channel();

        let mut creator = CreatorRuntimeGuard {
            stop: Some(creator_stop_tx),
            thread: Some(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("creator runtime must build");
                runtime.block_on(async move {
                    let (shutdown, shutdown_rx) =
                        tokio::sync::watch::channel::<ShutdownSignal>(None);
                    let token = Arc::new(ShutdownToken::new(EXTERNAL_DEADLINE));
                    let token_identity = shutdown_token_identity(&token);
                    let deadline = token.deadline();
                    shutdown.send_replace(Some(Arc::clone(&token)));

                    let (armed_tx, armed_rx) = tokio::sync::oneshot::channel();
                    let supervisor = AbortOnDropTask::spawn(async move {
                        let _drop_guard = SupervisorDropGuard(dropped_tx);
                        let _ = armed_tx.send(());
                        if observe_after_drop_rx.await.is_ok() {
                            if let Some(token) = shutdown_rx.borrow().clone() {
                                let _ = observed_tx
                                    .send((shutdown_token_identity(&token), token.deadline()));
                            }
                        }
                        std::future::pending::<Result<(), HaNodeError>>().await
                    });

                    if armed_rx.await.is_err() {
                        return;
                    }
                    let node = HaNode {
                        shutdown,
                        recorder_shutdown: tokio::sync::watch::channel(false).0,
                        startup: StartupIoContext::new(),
                        state: test_state(),
                        supervisor: Some(supervisor),
                    };
                    let _ = node_tx.send((node, token_identity, deadline));
                    let _ = creator_stop_rx.await;
                });
            })),
        };

        // Record all outcomes before asserting so the creator runtime is
        // stopped and joined even if a watchdog reports a regression.
        let node = node_rx.recv_timeout(Duration::from_secs(2));
        let mut foreign_drop = Err("creator did not construct the public node".to_owned());
        let mut observed = Err(std::sync::mpsc::RecvTimeoutError::Disconnected);
        let mut dropped = Err(std::sync::mpsc::RecvTimeoutError::Disconnected);
        let mut expected = None;

        if let Ok((node, token_identity, deadline)) = node {
            expected = Some((token_identity, deadline));
            foreign_drop = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("foreign runtime must build");
                runtime.block_on(async move {
                    drop(node);
                });
            }))
            .map_err(|_| "foreign runtime must not panic while dropping the node".to_owned());
            let _ = observe_after_drop_tx.send(());
            let watchdog = deadline + DEADLINE_TOLERANCE;
            observed = receive_before(&observed_rx, watchdog);
            dropped = receive_before(&dropped_rx, watchdog);
        }

        let creator_join = creator.stop_and_join();

        if let Err(message) = foreign_drop {
            panic!("{message}");
        }
        creator_join.expect("creator runtime must stop after the deadline proof");
        let (expected_identity, expected_deadline) =
            expected.expect("creator runtime did not construct the public node");
        let (observed_identity, observed_deadline) =
            observed.expect("Drop must preserve the preinstalled external shutdown token");
        assert_eq!(
            observed_identity, expected_identity,
            "foreign Drop must not replace the preinstalled external shutdown token"
        );
        assert_eq!(
            observed_deadline, expected_deadline,
            "foreign Drop must not extend the preinstalled external deadline"
        );
        let dropped_at = dropped.expect("creator reaper did not abort the pending supervisor by D");
        assert!(
            dropped_at <= expected_deadline + DEADLINE_TOLERANCE,
            "creator reaper exceeded the preinstalled external D plus the bounded scheduling allowance"
        );
    }

    /// A closed creator runtime cannot drive a detached reaper. Tokio drops
    /// its cancelled task at runtime shutdown; this narrowly verifies that a
    /// later public-owner Drop does not panic and that task cancellation drops
    /// the supervisor. Deadline/no-new-grace behavior is proved only while
    /// the creator runtime remains alive in the preceding test.
    #[test]
    fn node_drop_with_closed_creator_runtime_is_nonpanicking_and_aborts_supervisor() {
        struct SupervisorDropGuard(std::sync::mpsc::SyncSender<()>);

        impl Drop for SupervisorDropGuard {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }

        let (dropped_tx, dropped_rx) = std::sync::mpsc::sync_channel(1);
        let node = {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("creator runtime must build");
            let node = runtime.block_on(async move {
                let (shutdown, _shutdown_rx) = tokio::sync::watch::channel::<ShutdownSignal>(None);
                let supervisor = AbortOnDropTask::spawn(async move {
                    let _drop_guard = SupervisorDropGuard(dropped_tx);
                    std::future::pending::<Result<(), HaNodeError>>().await
                });
                tokio::task::yield_now().await;
                HaNode {
                    shutdown,
                    recorder_shutdown: tokio::sync::watch::channel(false).0,
                    startup: StartupIoContext::new(),
                    state: test_state(),
                    supervisor: Some(supervisor),
                }
            });
            runtime.shutdown_background();
            node
        };

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("foreign runtime must build");
            runtime.block_on(async move {
                drop(node);
            });
        }));
        assert!(
            result.is_ok(),
            "dropping through a closed creator handle must not panic"
        );
        dropped_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("closed creator runtime must abort/drop its supervisor");
    }

    /// This runs the public live-successor owner through the real staging and
    /// prestage path.  It is still before handoff/activation: the tail source
    /// is intentionally pending after the first request.  We move the owner
    /// to B only after that event, destroy B immediately after Drop, and let
    /// A prove the exact shutdown token, terminal state, and service-FD
    /// release before D.
    #[test]
    fn live_successor_drop_on_foreign_runtime_reaps_actual_staging_before_original_deadline() {
        let root = tempfile::tempdir().expect("test root must exist");
        let root_path = root.path().to_path_buf();
        let (tail_entered_tx, tail_entered_rx) = std::sync::mpsc::sync_channel(1);
        let (node_tx, node_rx) = std::sync::mpsc::sync_channel(1);
        let (shutdown_seen_tx, shutdown_seen_rx) = std::sync::mpsc::sync_channel(1);
        let (stopped_tx, stopped_rx) = std::sync::mpsc::sync_channel(1);
        let (creator_stop_tx, creator_stop_rx) = tokio::sync::oneshot::channel();
        let activated = Arc::new(AtomicBool::new(false));
        let creator_activated = Arc::clone(&activated);

        let creator = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("creator runtime must build");
            runtime.block_on(async move {
                let store =
                    rhiza_obj_store::ObjStore::new(rhiza_obj_store::ObjStoreConfig::Local {
                        root: root_path.join("archive"),
                    })
                    .expect("archive store must open");
                let predecessor = Membership::new(["node-1", "node-2", "node-3"])
                    .expect("predecessor membership must be valid");
                let successor = Membership::new(["node-4", "node-5", "node-6"])
                    .expect("successor membership must be valid");
                let source_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
                    store.clone(),
                    CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, predecessor.digest(), 1),
                );
                source_archive
                    .initialize_checkpoint()
                    .await
                    .expect("source checkpoint must initialize");
                let source_recorders = predecessor
                    .members()
                    .iter()
                    .map(|node_id| {
                        let recorder = RecorderFileStore::new_with_membership(
                            root_path.join("source-recorders").join(node_id),
                            node_id.clone(),
                            "rhiza:sql:cluster-a",
                            1,
                            1,
                            predecessor.clone(),
                        )
                        .expect("source recorder must initialize");
                        (node_id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
                    })
                    .collect::<Vec<_>>();
                let source_consensus = Arc::new(
                    ThreeNodeConsensus::from_recorders_with_ids(
                        "rhiza:sql:cluster-a",
                        "node-1",
                        1,
                        1,
                        source_recorders,
                    )
                    .expect("source consensus must initialize"),
                );
                let source_peers = predecessor
                    .members()
                    .iter()
                    .enumerate()
                    .map(|(index, node_id)| {
                        rhiza_node::PeerConfig::new(
                            node_id,
                            format!("http://127.0.0.1:{}", 39601 + index),
                            format!("foreign-drop-source-token-{}", index + 1),
                        )
                        .expect("source peer config must be valid")
                    })
                    .collect::<Vec<_>>();
                let source_config = NodeConfig::new(
                    "rhiza:sql:cluster-a",
                    "node-1",
                    root_path.join("source"),
                    1,
                    1,
                    source_peers,
                    "source-client-token",
                )
                .expect("source config must be valid");
                let source = NodeRuntime::open(source_config, source_consensus, &[])
                    .expect("source runtime must open");
                source
                    .write("foreign-drop-seed", "key", "value")
                    .expect("source write must commit before prestage");
                let coordinator =
                    CheckpointCoordinator::open(source_archive.clone(), DurabilityMode::Sync)
                        .await
                        .expect("source checkpoint coordinator must open");
                source
                    .checkpoint_compact(&coordinator)
                    .await
                    .expect("source checkpoint must compact before prestage");
                source
                    .stop_current_configuration_for_successor(&successor)
                    .expect("source must stop for the actual successor draft");
                assert!(
                    source
                        .consensus()
                        .finish_pending_rpcs(Duration::from_secs(1)),
                    "source Stop must settle before the successor prestage"
                );
                drop(source);
                let target_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
                    store,
                    CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 2, successor.digest(), 1),
                );
                let peers = successor
                    .members()
                    .iter()
                    .enumerate()
                    .map(|(index, node_id)| {
                        rhiza_node::PeerConfig::new(
                            node_id,
                            format!("http://127.0.0.1:{}", 39701 + index),
                            format!("foreign-drop-peer-token-{}", index + 1),
                        )
                        .expect("successor peer config must be valid")
                    })
                    .collect::<Vec<_>>();
                let target_config = NodeConfig::new_with_configuration(
                    "rhiza:sql:cluster-a",
                    "node-4",
                    root_path.join("successor"),
                    1,
                    successor.clone(),
                    rhiza_core::ConfigurationState::active(2, successor.digest()),
                    peers,
                    "successor-client-token",
                )
                .expect("successor draft config must be valid");
                let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("recorder listener must bind");
                let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("service listener must bind");
                let service_address = service_listener
                    .local_addr()
                    .expect("service listener must expose its address");
                let node = HaSuccessorPrestageConfig::new(
                    source_archive,
                    root_path.join("prestage"),
                    "node-4",
                    ExecutionProfile::Sqlite,
                    predecessor,
                    successor,
                    "tail-token",
                )
                .start_live(
                    HaStartupConfig::new(
                        target_config,
                        target_archive,
                        DurabilityMode::Sync,
                        60_000,
                        HaStartupMode::Rejoin,
                    ),
                    HaServeConfig::new(
                        recorder_listener,
                        service_listener,
                        HaRecorderTransport::Http,
                        Vec::new(),
                        Vec::new(),
                    ),
                    Arc::new(EnteredPendingTailSource {
                        entered: tail_entered_tx,
                    }),
                )
                .expect("public live successor must start");

                let mut shutdown = node.shutdown.subscribe();
                tokio::spawn(async move {
                    shutdown
                        .changed()
                        .await
                        .expect("Drop must signal the actual live successor");
                    let token = shutdown
                        .borrow()
                        .clone()
                        .expect("Drop must install one shutdown token");
                    shutdown_seen_tx
                        .send((shutdown_token_identity(&token), token.deadline()))
                        .expect("test must observe the successor shutdown token");
                });
                let mut state = node.state.clone();
                tokio::spawn(async move {
                    loop {
                        let snapshot = state.borrow().clone();
                        if snapshot.status == HaNodeStatus::Ready {
                            creator_activated.store(true, Ordering::Release);
                        }
                        if snapshot.status == HaNodeStatus::Stopped {
                            stopped_tx
                                .send(())
                                .expect("test must observe successor stop");
                            return;
                        }
                        if state.changed().await.is_err() {
                            return;
                        }
                    }
                });
                node_tx
                    .send((node, service_address))
                    .expect("test must receive the public successor owner");
                creator_stop_rx
                    .await
                    .expect("test must stop creator only after actual cleanup");
            });
        });

        let (node, service_address) = node_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("creator runtime did not return the public successor owner");
        tail_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("actual prestage path did not reach the pending tail before Drop");

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("foreign runtime must build");
            runtime.block_on(async move {
                let livez = staging_livez_request(service_address).await;
                assert!(
                    livez.contains(" 200 "),
                    "the public successor must still be served by its actual staging listener"
                );
                assert_ne!(node.status(), HaNodeStatus::Ready);
                drop(node);
            });
            // B is fully gone before A reports terminal cleanup.
        })
        .join()
        .expect("foreign runtime must not panic while dropping successor");

        let (identity, deadline) = shutdown_seen_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("actual successor supervisor did not observe its Drop token");
        assert_ne!(identity, 0);
        stopped_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("actual successor staging cleanup did not stop before its original deadline");
        assert!(
            tokio::time::Instant::now() < deadline,
            "actual staging cleanup must retain the public owner's original deadline"
        );
        assert!(
            !activated.load(Ordering::Acquire),
            "pre-handoff Drop must not activate the child service"
        );
        let rebound = std::net::TcpListener::bind(service_address)
            .expect("staging listener must be released before the original deadline");
        drop(rebound);
        creator_stop_tx
            .send(())
            .expect("creator runtime must remain alive until staging cleanup is observed");
        creator
            .join()
            .expect("creator runtime must stop after successor cleanup");
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn ready_node_drop_closes_actual_handle_admission_before_supervisor_observes_shutdown() {
        let (owner, _root) = shutdown_test_owner().await;
        let handle = owner.handle();
        let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel::<ShutdownSignal>(None);
        let (state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Ready,
            handle: Some(handle.clone()),
            terminal_error: None,
        });
        let (admission_result_tx, admission_result_rx) = tokio::sync::oneshot::channel();
        let supervisor_handle = handle.clone();
        let supervisor = AbortOnDropTask::spawn(async move {
            let _state = state_tx;
            shutdown_rx
                .changed()
                .await
                .expect("Drop must signal the ready-node supervisor");
            let result = supervisor_handle
                .put("ready-drop-order", "key", "value")
                .await;
            admission_result_tx
                .send(result)
                .expect("test must observe admission after Drop");
            Ok::<(), HaNodeError>(())
        });
        let node = HaNode {
            shutdown,
            recorder_shutdown: tokio::sync::watch::channel(false).0,
            startup: StartupIoContext::new(),
            state,
            supervisor: Some(supervisor),
        };

        drop(node);
        let admission = tokio::time::timeout(Duration::from_secs(2), admission_result_rx)
            .await
            .expect("ready-node Drop reaper did not observe shutdown ordering")
            .expect("ready-node supervisor dropped before reporting admission");
        assert!(
            matches!(admission, Err(crate::Error::Closed)),
            "Drop must close real RhizaHandle admission before supervisor shutdown is visible"
        );
        owner
            .shutdown()
            .await
            .expect("closed-admission owner must still shut down cleanly");
    }
}
