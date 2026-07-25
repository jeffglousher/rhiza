use std::{
    fmt, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use rhiza_archive::{CheckpointIdentity, CheckpointPublisherOptions, ObjectArchiveStore};
use rhiza_core::{
    ConfigChange, ErrorClassification, ExecutionProfile, LogAnchor, LogHash, StoredCommand,
};
use rhiza_log::LogStore;
use rhiza_node::{
    certified_tail_router_for_runtime,
    durability::{
        adopt_finalized_successor_prestage, complete_adopted_successor_prestage,
        inspect_successor_prestage, prestage_successor_checkpoint, publish_successor_prestage,
        DurabilityError, SuccessorPrestage, SuccessorPrestageIdentity, SuccessorPrestageState,
        SuccessorRestorePreparation,
    },
    node_router_with_checkpoint, node_router_with_checkpoint_and_admin_tasks,
    recorder_router_for_generation, recover_successor_recorder_after_checkpoint,
    rehydrate_recorder_after_checkpoint, serve_recorder_tcp, serve_recorder_tcp_tls, AdminConfig,
    AdminTaskTracker, CertifiedTailRequest, CertifiedTailResponse, CheckpointCoordinator,
    DurabilityMode, LearnerProgress, LearnerStore, LogPeer, NodeConfig, NodeError, NodeRuntime,
    RecorderTlsServerConfig, StopInformation, TailReaderConfig, MAX_CERTIFIED_TAIL_ENTRIES,
};
#[cfg(feature = "recorder-postcard-rpc")]
use rhiza_node::{
    serve_recorder_postcard_rpc, serve_recorder_postcard_rpc_tls,
    RecorderPostcardRpcTlsServerConfig,
};
use rhiza_quepaxa::{
    AcceptedValue, DecisionProof, Membership, ReadFenceObservation, ReadFenceRequest,
    RecordRequest, RecordSummary, RecorderFileStore, RecorderPreflight, RecorderRpc,
    ThreeNodeConsensus,
};
use serde::{Deserialize, Serialize};

use crate::{Rhiza, RhizaHandle};

const LOCAL_CHECKPOINT_IDENTITY_FILE: &str = rhiza_node::durability::LOCAL_CHECKPOINT_IDENTITY_FILE;
const MAX_LOCAL_CHECKPOINT_IDENTITY_BYTES: u64 = 4 * 1024;
const SUCCESSOR_RESTORE_INTENT_FILE: &str = ".successor-restore.intent";
const SUCCESSOR_RESTORE_COMPLETE_FILE: &str = ".successor-restore.complete";
const MAX_SUCCESSOR_RESTORE_CONTROL_BYTES: u64 = 16 * 1024;
const STARTUP_RETRY_DELAY: Duration = Duration::from_millis(100);
const HA_STATUS_POLL_INTERVAL: Duration = Duration::from_millis(100);
const HA_SERVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);
const HA_STARTUP_CLEANUP_GRACE: Duration = Duration::from_millis(100);

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
    service_listener: tokio::net::TcpListener,
    recorder_transport: HaRecorderTransport,
    recorders: Vec<(String, Box<dyn RecorderRpc>)>,
    log_peers: Vec<Arc<dyn LogPeer>>,
    admin: Option<AdminConfig>,
    tail_token: Option<String>,
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
            service_listener,
            recorder_transport,
            recorders,
            log_peers,
            admin: None,
            tail_token: None,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HaNodeStatus {
    Starting,
    AwaitingActivation,
    Ready,
    Degraded,
    ShuttingDown,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HaNodeError {
    Startup(HaStartupError),
    RecorderServer(String),
    ServiceServer(String),
    WorkerFailure(ErrorClassification),
    Shutdown(String),
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
            Self::RecorderServer(error) => write!(formatter, "recorder server failed: {error}"),
            Self::ServiceServer(error) => write!(formatter, "service server failed: {error}"),
            Self::WorkerFailure(classification) => write!(
                formatter,
                "HA worker failed with code {}",
                classification.code()
            ),
            Self::Shutdown(error) => write!(formatter, "HA shutdown failed: {error}"),
            Self::Cleanup { primary, cleanup } => {
                write!(formatter, "{primary}; cleanup also failed: {cleanup}")
            }
            Self::Cancelled => formatter.write_str("HA node startup was cancelled"),
        }
    }
}

impl std::error::Error for HaNodeError {}

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

pub struct HaNode {
    shutdown: tokio::sync::watch::Sender<Option<tokio::time::Instant>>,
    state: tokio::sync::watch::Receiver<HaNodeSnapshot>,
    supervisor: Option<tokio::task::JoinHandle<Result<(), HaNodeError>>>,
}

impl HaNode {
    pub async fn ready(&self) -> Result<RhizaHandle, HaNodeError> {
        let mut state = self.state.clone();
        loop {
            let snapshot = state.borrow().clone();
            if snapshot.status == HaNodeStatus::Ready {
                return snapshot.handle.ok_or_else(|| {
                    HaNodeError::Startup(fail("ready HA node has no application handle"))
                });
            }
            if let Some(error) = snapshot.terminal_error {
                return Err(error);
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
        self.status() == HaNodeStatus::Ready
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
                HaNodeError::Shutdown("HA node supervisor state channel closed".into())
            })?;
        }
    }

    pub async fn shutdown(mut self) -> Result<(), HaNodeError> {
        request_ha_shutdown(&self.shutdown);
        let supervisor = self
            .supervisor
            .take()
            .ok_or_else(|| HaNodeError::Shutdown("HA node supervisor is missing".into()))?;
        match supervisor.await {
            Ok(result) => result,
            Err(error) => Err(HaNodeError::Shutdown(format!(
                "HA node supervisor task failed: {error}"
            ))),
        }
    }
}

impl Drop for HaNode {
    fn drop(&mut self) {
        request_ha_shutdown(&self.shutdown);
    }
}

fn request_ha_shutdown(
    shutdown: &tokio::sync::watch::Sender<Option<tokio::time::Instant>>,
) -> tokio::time::Instant {
    let requested = tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT;
    shutdown.send_if_modified(|deadline| {
        if deadline.is_none() {
            *deadline = Some(requested);
            true
        } else {
            false
        }
    });
    shutdown.borrow().unwrap_or(requested)
}

#[derive(Clone, Debug)]
pub struct HaPredecessor {
    membership: Membership,
    stop: StopInformation,
}

impl HaPredecessor {
    pub fn new(membership: Membership, stop: StopInformation) -> Self {
        Self { membership, stop }
    }
}

pub struct HaSuccessorPrestageConfig {
    archive: ObjectArchiveStore,
    prestage_dir: PathBuf,
    target_node_id: String,
    execution_profile: ExecutionProfile,
    predecessor_membership: Membership,
    target_membership: Membership,
    tail_token: String,
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
        }
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
        })
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
        let learner = LearnerStore::open(&data_dir, self.tail_config).map_err(error)?;
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

    /// Requires the exact bound Stop, adopts the detached stage without recopying its
    /// checkpoint, and hands the result to the existing successor prepare/open lifecycle.
    pub async fn finalize(
        self,
        startup: HaStartupConfig,
    ) -> Result<PreparedHaStartup, HaStartupError> {
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
        finish_successor_prestage_adoption(startup, predecessor, target_config_id, restore).await
    }
}

async fn finish_successor_prestage_adoption(
    startup: HaStartupConfig,
    predecessor: HaPredecessor,
    target_config_id: u64,
    restore: SuccessorRestorePreparation,
) -> Result<PreparedHaStartup, HaStartupError> {
    if restore.requires_recorder_install() {
        install_successor_recorder_for_startup(
            &startup.node_config,
            target_config_id,
            &predecessor,
        )?;
        restore.complete().map_err(error)?;
    }
    let initialized = startup
        .archive
        .initialize_checkpoint()
        .await
        .map_err(error)?;
    if initialized.manifest().tip().index() != 0 || !initialized.manifest().segments().is_empty() {
        return Err(fail(
            "successor target checkpoint namespace must be empty before activation",
        ));
    }
    let identity = startup
        .archive
        .checkpoint_identity()
        .map_err(error)?
        .clone();
    validate_archive_identity(&startup.node_config, &identity, target_config_id)?;
    write_local_checkpoint_identity_marker(
        startup.node_config.data_dir(),
        startup.node_config.execution_profile(),
        &identity,
        startup.node_config.node_id(),
    )?;
    startup.finish_prepare(
        identity,
        target_config_id,
        StartupPreparation::RecorderFirst {
            open_policy: RecorderOpenPolicy::MustExist,
        },
    )
}

fn tail_request(from: LogAnchor, max_entries: u32) -> Result<CertifiedTailRequest, HaStartupError> {
    if max_entries == 0 || max_entries > MAX_CERTIFIED_TAIL_ENTRIES {
        return Err(fail(format!(
            "certified tail max_entries must be in 1..={MAX_CERTIFIED_TAIL_ENTRIES}"
        )));
    }
    Ok(CertifiedTailRequest { from, max_entries })
}

pub struct HaStartupConfig {
    node_config: NodeConfig,
    archive: ObjectArchiveStore,
    durability: DurabilityMode,
    lease_duration_ms: u64,
    mode: HaStartupMode,
    predecessor: Option<HaPredecessor>,
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
        }
    }

    pub fn with_predecessor(mut self, predecessor: HaPredecessor) -> Self {
        self.predecessor = Some(predecessor);
        self
    }

    /// Resumes exact-Stop successor adoption across finalized-marker, intent, and completion
    /// crash windows.
    pub async fn resume_finalized_successor_prestage(
        self,
    ) -> Result<PreparedHaStartup, HaStartupError> {
        if self.mode != HaStartupMode::Rejoin {
            return Err(fail(
                "finalized successor prestage resume requires rejoin mode",
            ));
        }
        let predecessor = self
            .predecessor
            .clone()
            .ok_or_else(|| fail("finalized successor prestage resume requires a predecessor"))?;
        let target_config_id = self.target_config_id()?;
        validate_predecessor_binding(&self.node_config, target_config_id, &predecessor)?;
        let prestage = match inspect_successor_prestage(
            self.node_config.data_dir(),
            self.node_config.log_initial_configuration().clone(),
        ) {
            Ok(prestage) => prestage,
            Err(DurabilityError::DataDirNotFresh(_)) => return self.prepare().await,
            Err(cause) => return Err(error(cause)),
        };
        let restore = adopt_finalized_successor_prestage(
            prestage,
            &self.node_config,
            &predecessor.stop,
            &predecessor.membership,
        )
        .map_err(error)?;
        finish_successor_prestage_adoption(self, predecessor, target_config_id, restore).await
    }

    pub async fn prepare(self) -> Result<PreparedHaStartup, HaStartupError> {
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
                )
                .await?
            }
            None => {
                prepare_standard(
                    &self.node_config,
                    &self.archive,
                    self.mode,
                    self.node_config.membership(),
                )
                .await?
            }
        };
        self.finish_prepare(identity, target_config_id, preparation)
    }

    fn finish_prepare(
        self,
        identity: CheckpointIdentity,
        target_config_id: u64,
        preparation: StartupPreparation,
    ) -> Result<PreparedHaStartup, HaStartupError> {
        let recorder = open_recorder_for_preparation(
            &self.node_config,
            target_config_id,
            preparation.open_policy(),
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

pub struct PreparedHaStartup {
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
    pub async fn start(self, serve: HaServeConfig) -> Result<HaNode, HaNodeError> {
        let HaServeConfig {
            recorder_listener,
            service_listener,
            recorder_transport,
            recorders,
            log_peers,
            admin,
            tail_token,
        } = serve;
        let peers = self.config.node_config.peers().to_vec();
        let recovery_generation = self.config.node_config.recovery_generation();
        let recorder = self.recorder_hook.clone();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(None);
        let (recorder_shutdown, recorder_shutdown_rx) = tokio::sync::watch::channel(false);
        let (recorder_started, recorder_started_rx) = tokio::sync::oneshot::channel();
        let recorder_task = spawn_ha_recorder_server(
            recorder_listener,
            recorder,
            recorder_transport,
            peers,
            recovery_generation,
            recorder_shutdown_rx,
            recorder_started,
        );
        recorder_started_rx.await.map_err(|_| {
            HaNodeError::RecorderServer(
                "recorder ingress stopped before reporting startup".to_string(),
            )
        })?;

        let (state_tx, state) = tokio::sync::watch::channel(HaNodeSnapshot {
            status: HaNodeStatus::Starting,
            handle: None,
            terminal_error: None,
        });
        let supervisor_shutdown = shutdown.clone();
        let supervisor = tokio::spawn(async move {
            supervise_ha_node(
                self,
                service_listener,
                recorders,
                log_peers,
                admin,
                tail_token,
                supervisor_shutdown,
                shutdown_rx,
                recorder_shutdown,
                recorder_task,
                state_tx,
            )
            .await
        });
        Ok(HaNode {
            shutdown,
            state,
            supervisor: Some(supervisor),
        })
    }

    async fn open_cancellable(
        self,
        recorders: Vec<(String, Box<dyn RecorderRpc>)>,
        log_peers: Vec<Arc<dyn LogPeer>>,
        shutdown: tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
    ) -> Result<HaOpenNode, HaOpenError> {
        self.open_inner(recorders, log_peers, shutdown).await
    }

    async fn open_inner(
        self,
        recorders: Vec<(String, Box<dyn RecorderRpc>)>,
        log_peers: Vec<Arc<dyn LogPeer>>,
        mut shutdown: tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
    ) -> Result<HaOpenNode, HaOpenError> {
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

        let coordinator_open = CheckpointCoordinator::open_with_holder_and_options(
            self.config.archive,
            self.config.durability,
            self.config.node_config.node_id(),
            CheckpointPublisherOptions::new(self.config.lease_duration_ms),
        );
        tokio::pin!(coordinator_open);
        let coordinator = loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || shutdown.borrow().is_some() {
                        let deadline = shutdown
                            .borrow()
                            .unwrap_or_else(tokio::time::Instant::now);
                        return Err(pending.cancel(deadline, Ok(())).await);
                    }
                }
                result = &mut coordinator_open => {
                    match result {
                        Ok(coordinator) => break Arc::new(coordinator),
                        Err(cause) => return Err(pending.fail(error(cause)).await),
                    }
                }
            }
        };
        let post_coordinator_deadline = *shutdown.borrow();
        if let Some(deadline) = post_coordinator_deadline {
            return Err(pending.cancel(deadline, Ok(())).await);
        }
        let applied_index = match pending.runtime().applied_index() {
            Ok(applied_index) => applied_index,
            Err(cause) => return Err(pending.fail(error(cause)).await),
        };
        coordinator.note_recovered_committed(applied_index);
        let runtime = pending.transfer();
        Ok(HaOpenNode {
            runtime,
            coordinator,
            recorder: self.recorder,
            recorder_hook: self.recorder_hook,
        })
    }
}

struct HaOpenNode {
    runtime: Arc<NodeRuntime>,
    coordinator: Arc<CheckpointCoordinator>,
    recorder: RecorderFileStore,
    recorder_hook: HaRecorder,
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
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if finish_ha_pending_consensus_rpcs(runtime.consensus(), remaining) {
            Ok(())
        } else {
            Err(HaNodeError::Shutdown(
                "pending consensus RPCs did not drain before the shutdown deadline".into(),
            ))
        }
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
            config.config_id(),
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

type HaServerTask = tokio::task::JoinHandle<Result<(), HaNodeError>>;

fn spawn_ha_recorder_server(
    listener: tokio::net::TcpListener,
    recorder: HaRecorder,
    transport: HaRecorderTransport,
    peers: Vec<rhiza_node::PeerConfig>,
    recovery_generation: u64,
    shutdown: tokio::sync::watch::Receiver<bool>,
    started: tokio::sync::oneshot::Sender<()>,
) -> HaServerTask {
    tokio::spawn(async move {
        let _ = started.send(());
        match transport {
            HaRecorderTransport::Http => {
                let router = recorder_router_for_generation(recorder, peers, recovery_generation);
                axum::serve(listener, router)
                    .with_graceful_shutdown(wait_for_ha_shutdown(shutdown))
                    .await
                    .map_err(|error| HaNodeError::RecorderServer(error.to_string()))
            }
            HaRecorderTransport::TcpPostcard => serve_recorder_tcp(
                listener,
                recorder,
                peers,
                recovery_generation,
                wait_for_ha_shutdown(shutdown),
            )
            .await
            .map_err(HaNodeError::RecorderServer),
            HaRecorderTransport::TcpTlsPostcard(tls) => serve_recorder_tcp_tls(
                listener,
                recorder,
                peers,
                recovery_generation,
                tls,
                wait_for_ha_shutdown(shutdown),
            )
            .await
            .map_err(HaNodeError::RecorderServer),
            #[cfg(feature = "recorder-postcard-rpc")]
            HaRecorderTransport::TcpPostcardRpc => serve_recorder_postcard_rpc(
                listener,
                recorder,
                peers,
                recovery_generation,
                wait_for_ha_shutdown(shutdown),
            )
            .await
            .map_err(HaNodeError::RecorderServer),
            #[cfg(feature = "recorder-postcard-rpc")]
            HaRecorderTransport::TcpTlsPostcardRpc(tls) => serve_recorder_postcard_rpc_tls(
                listener,
                recorder,
                peers,
                recovery_generation,
                tls,
                wait_for_ha_shutdown(shutdown),
            )
            .await
            .map_err(HaNodeError::RecorderServer),
        }
    })
}

fn spawn_ha_service_server(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    shutdown: tokio::sync::watch::Receiver<bool>,
    started: tokio::sync::oneshot::Sender<()>,
) -> HaServerTask {
    tokio::spawn(async move {
        let _ = started.send(());
        axum::serve(listener, router)
            .with_graceful_shutdown(wait_for_ha_shutdown(shutdown))
            .await
            .map_err(|error| HaNodeError::ServiceServer(error.to_string()))
    })
}

async fn wait_for_ha_shutdown(mut shutdown: tokio::sync::watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn supervise_ha_node(
    prepared: PreparedHaStartup,
    service_listener: tokio::net::TcpListener,
    recorders: Vec<(String, Box<dyn RecorderRpc>)>,
    log_peers: Vec<Arc<dyn LogPeer>>,
    admin: Option<AdminConfig>,
    tail_token: Option<String>,
    shutdown: tokio::sync::watch::Sender<Option<tokio::time::Instant>>,
    shutdown_rx: tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
    recorder_shutdown: tokio::sync::watch::Sender<bool>,
    mut recorder_task: HaServerTask,
    state: tokio::sync::watch::Sender<HaNodeSnapshot>,
) -> Result<(), HaNodeError> {
    let opened = {
        let startup = prepared.open_cancellable(recorders, log_peers, shutdown_rx.clone());
        tokio::pin!(startup);
        tokio::select! {
            result = &mut startup => match result {
                Ok(opened) => opened,
                Err(HaOpenError::Cancelled { deadline, cleanup: startup_cleanup })
                    if shutdown_rx.borrow().is_some() =>
                {
                    publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
                    let recorder_cleanup = stop_ha_server_before(
                        &recorder_shutdown,
                        &mut recorder_task,
                        "recorder server",
                        deadline,
                    )
                    .await;
                    let cleanup = combine_ha_errors(
                        startup_cleanup
                            .err()
                            .into_iter()
                            .chain(recorder_cleanup.err())
                            .collect(),
                    );
                    if let Err(error) = cleanup {
                        let error = HaNodeError::Cleanup {
                            primary: Box::new(HaNodeError::Cancelled),
                            cleanup: Box::new(error),
                        };
                        publish_ha_failure(&state, error.clone());
                        return Err(error);
                    }
                    publish_ha_state(&state, HaNodeStatus::Stopped, None, None);
                    return Ok(());
                }
                Err(HaOpenError::Startup { error, cleanup: startup_cleanup }) => {
                    let error = HaNodeError::Startup(error);
                    publish_ha_failure(&state, error.clone());
                    let recorder_cleanup = stop_ha_server(
                        &recorder_shutdown,
                        &mut recorder_task,
                        "recorder server",
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
                let error = unexpected_server_exit(result, "recorder server");
                let deadline = request_ha_shutdown(&shutdown);
                publish_ha_failure(&state, error.clone());
                let cleanup = match tokio::time::timeout_at(deadline, &mut startup).await {
                    Ok(Ok(opened)) => {
                        shutdown_opened_ha_startup_before(
                            opened,
                            &recorder_shutdown,
                            &mut recorder_task,
                            false,
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
                    Err(_) => Err(HaNodeError::Shutdown(
                        "HA startup did not stop before the shutdown deadline".into(),
                    )),
                };
                return combine_ha_results(Some(error), cleanup);
            }
        }
    };

    let runtime = opened.runtime();
    let coordinator = opened.coordinator();
    let recorder = opened.local_recorder();
    let recorder_hook = opened.recorder_hook.clone();
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
            let cleanup =
                shutdown_opened_ha_startup(opened, &recorder_shutdown, &mut recorder_task, true)
                    .await;
            return combine_ha_results(Some(error), cleanup);
        }
    };
    let startup_shutdown_deadline = *shutdown_rx.borrow();
    if let Some(deadline) = startup_shutdown_deadline {
        publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
        let cleanup = shutdown_opened_ha_startup_before(
            opened,
            &recorder_shutdown,
            &mut recorder_task,
            true,
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
    let (service_shutdown, service_shutdown_rx) = tokio::sync::watch::channel(false);
    let (service_started, service_started_rx) = tokio::sync::oneshot::channel();
    let mut service_task = spawn_ha_service_server(
        service_listener,
        router.clone(),
        service_shutdown_rx,
        service_started,
    );
    let mut service_start_shutdown = shutdown_rx.clone();
    match wait_for_service_start_or_shutdown(service_started_rx, &mut service_start_shutdown).await
    {
        Ok(ServiceStartup::Started) => {}
        Ok(ServiceStartup::Shutdown(deadline)) => {
            publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
            let cleanup = shutdown_ha_runtime_before(
                owner,
                runtime,
                admin_tasks.as_ref(),
                &service_shutdown,
                &mut service_task,
                true,
                &recorder_shutdown,
                &mut recorder_task,
                true,
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
                admin_tasks.as_ref(),
                &service_shutdown,
                &mut service_task,
                true,
                &recorder_shutdown,
                &mut recorder_task,
                true,
            )
            .await;
            return combine_ha_results(Some(error), cleanup);
        }
    }
    let post_service_start_deadline = *shutdown_rx.borrow();
    if let Some(deadline) = post_service_start_deadline {
        publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
        let cleanup = shutdown_ha_runtime_before(
            owner,
            runtime,
            admin_tasks.as_ref(),
            &service_shutdown,
            &mut service_task,
            true,
            &recorder_shutdown,
            &mut recorder_task,
            true,
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
    update_ha_readiness(&state, &handle, &coordinator, &shutdown_rx).await;

    let mut shutdown_rx = shutdown_rx;
    let mut status_tick = tokio::time::interval(HA_STATUS_POLL_INTERVAL);
    status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut recorder_running = true;
    let mut service_running = true;
    let terminal = {
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
                    break Some(unexpected_server_exit(result, "recorder server"));
                }
                result = &mut service_task => {
                    service_running = false;
                    break Some(unexpected_server_exit(result, "service server"));
                }
                failure = &mut worker_failure => {
                    break Some(match failure {
                        Some(classification) => HaNodeError::WorkerFailure(classification),
                        None => HaNodeError::Shutdown(
                            "HA workers stopped before node shutdown".into()
                        ),
                    });
                }
                _ = status_tick.tick() => {
                    update_ha_readiness(&state, &handle, &coordinator, &shutdown_rx).await;
                }
            }
        }
    };

    let deadline = if let Some(error) = &terminal {
        let deadline = request_ha_shutdown(&shutdown);
        publish_ha_failure(&state, error.clone());
        deadline
    } else {
        publish_ha_state(&state, HaNodeStatus::ShuttingDown, None, None);
        shutdown_rx
            .borrow()
            .unwrap_or_else(|| request_ha_shutdown(&shutdown))
    };
    let cleanup = shutdown_ha_runtime_before(
        owner,
        runtime,
        admin_tasks.as_ref(),
        &service_shutdown,
        &mut service_task,
        service_running,
        &recorder_shutdown,
        &mut recorder_task,
        recorder_running,
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
    shutdown: &mut tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
) -> Result<ServiceStartup, HaNodeError> {
    loop {
        if let Some(deadline) = *shutdown.borrow() {
            return Ok(ServiceStartup::Shutdown(deadline));
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || shutdown.borrow().is_some() {
                    let deadline = shutdown
                        .borrow()
                        .unwrap_or_else(tokio::time::Instant::now);
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

async fn update_ha_readiness(
    state: &tokio::sync::watch::Sender<HaNodeSnapshot>,
    handle: &RhizaHandle,
    coordinator: &CheckpointCoordinator,
    shutdown: &tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
) {
    let status = handle.status().await;
    let phase = match status {
        Ok(status) if !status.configuration_state.is_active() => HaNodeStatus::AwaitingActivation,
        Ok(status) if status.ready && coordinator.write_allowed().is_ok() => HaNodeStatus::Ready,
        Ok(_) | Err(_) => HaNodeStatus::Degraded,
    };
    let shutdown_guard = shutdown.borrow();
    if shutdown_guard.is_some() {
        return;
    }
    publish_ha_state(
        state,
        phase,
        (phase == HaNodeStatus::Ready).then(|| handle.clone()),
        None,
    );
    drop(shutdown_guard);
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

async fn shutdown_opened_ha_startup(
    opened: HaOpenNode,
    recorder_shutdown: &tokio::sync::watch::Sender<bool>,
    recorder_task: &mut HaServerTask,
    recorder_running: bool,
) -> Result<(), HaNodeError> {
    let deadline = tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT;
    shutdown_opened_ha_startup_before(
        opened,
        recorder_shutdown,
        recorder_task,
        recorder_running,
        deadline,
    )
    .await
}

async fn shutdown_opened_ha_startup_before(
    opened: HaOpenNode,
    recorder_shutdown: &tokio::sync::watch::Sender<bool>,
    recorder_task: &mut HaServerTask,
    recorder_running: bool,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    let runtime = opened.runtime();
    runtime.cancel_operations();
    let owner = opened.into_rhiza();
    let mut errors = Vec::new();
    if let Err(error) = owner.shutdown_with_deadline(deadline).await {
        errors.push(HaNodeError::Shutdown(error.to_string()));
    }
    recorder_shutdown.send_replace(true);
    if recorder_running {
        if let Err(error) = wait_for_ha_server(recorder_task, "recorder server", deadline).await {
            errors.push(error);
        }
    }
    combine_ha_errors(errors)
}

#[allow(clippy::too_many_arguments)]
async fn shutdown_ha_runtime(
    owner: Rhiza,
    runtime: Arc<NodeRuntime>,
    admin_tasks: Option<&AdminTaskTracker>,
    service_shutdown: &tokio::sync::watch::Sender<bool>,
    service_task: &mut HaServerTask,
    service_running: bool,
    recorder_shutdown: &tokio::sync::watch::Sender<bool>,
    recorder_task: &mut HaServerTask,
    recorder_running: bool,
) -> Result<(), HaNodeError> {
    let deadline = tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT;
    shutdown_ha_runtime_before(
        owner,
        runtime,
        admin_tasks,
        service_shutdown,
        service_task,
        service_running,
        recorder_shutdown,
        recorder_task,
        recorder_running,
        deadline,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn shutdown_ha_runtime_before(
    owner: Rhiza,
    runtime: Arc<NodeRuntime>,
    admin_tasks: Option<&AdminTaskTracker>,
    service_shutdown: &tokio::sync::watch::Sender<bool>,
    service_task: &mut HaServerTask,
    service_running: bool,
    recorder_shutdown: &tokio::sync::watch::Sender<bool>,
    recorder_task: &mut HaServerTask,
    recorder_running: bool,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    if let Some(tasks) = admin_tasks {
        tasks.stop_admission();
    }
    service_shutdown.send_replace(true);
    runtime.cancel_operations();
    let mut errors = Vec::new();
    if service_running {
        if let Err(error) = wait_for_ha_server(service_task, "service server", deadline).await {
            errors.push(error);
        }
    }
    if let Some(tasks) = admin_tasks {
        if tokio::time::timeout_at(deadline, tasks.wait_for_idle())
            .await
            .is_err()
        {
            errors.push(HaNodeError::Shutdown(
                "admin tasks did not drain before the shutdown deadline".into(),
            ));
        }
    }
    if let Err(error) = owner.shutdown_with_deadline(deadline).await {
        errors.push(HaNodeError::Shutdown(error.to_string()));
    }
    recorder_shutdown.send_replace(true);
    if recorder_running {
        if let Err(error) = wait_for_ha_server(recorder_task, "recorder server", deadline).await {
            errors.push(error);
        }
    }
    combine_ha_errors(errors)
}

async fn stop_ha_server(
    shutdown: &tokio::sync::watch::Sender<bool>,
    task: &mut HaServerTask,
    name: &str,
) -> Result<(), HaNodeError> {
    stop_ha_server_before(
        shutdown,
        task,
        name,
        tokio::time::Instant::now() + HA_SERVER_SHUTDOWN_TIMEOUT,
    )
    .await
}

async fn stop_ha_server_before(
    shutdown: &tokio::sync::watch::Sender<bool>,
    task: &mut HaServerTask,
    name: &str,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    shutdown.send_replace(true);
    wait_for_ha_server(task, name, deadline).await
}

async fn wait_for_ha_server(
    task: &mut HaServerTask,
    name: &str,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    match tokio::time::timeout_at(deadline, &mut *task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(HaNodeError::Shutdown(format!(
            "{name} task failed during shutdown: {error}"
        ))),
        Err(_) => {
            task.abort();
            Err(HaNodeError::Shutdown(format!(
                "{name} did not stop before the shutdown deadline"
            )))
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
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        self.recorder.recorder_id()
    }

    fn store_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        self.require_active()?;
        self.recorder.store_command_for(
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
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        self.recorder
            .fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash)
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        self.require_active()?;
        self.recorder.record(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.require_active()?;
        self.recorder.install_decision_proof(proof, membership)
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        self.require_visible_slot(slot)?;
        self.recorder.inspect_decision_proof(slot)
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        self.require_visible_slot(slot)?;
        self.recorder.inspect_record_summary(slot)
    }

    fn supports_context_read_fence(&self) -> bool {
        self.recorder.supports_context_read_fence()
    }

    fn observe_read_fence(
        &self,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<ReadFenceObservation> {
        self.require_visible_slot(request.slot)?;
        self.recorder.observe_read_fence(request)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HaStartupError(String);

impl fmt::Display for HaStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for HaStartupError {}

fn error(error: impl fmt::Display) -> HaStartupError {
    HaStartupError(error.to_string())
}

fn fail(message: impl Into<String>) -> HaStartupError {
    HaStartupError(message.into())
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

async fn prepare_standard(
    config: &NodeConfig,
    archive: &ObjectArchiveStore,
    mode: HaStartupMode,
    membership: &Membership,
) -> Result<StartupPreparation, HaStartupError> {
    let data_dir = config.data_dir();
    let node_id = config.node_id();
    let execution_profile = config.execution_profile();
    match mode {
        HaStartupMode::Bootstrap => {
            if !local_data_is_fresh(data_dir)? {
                return Err(fail("bootstrap requires a fresh local data directory"));
            }
            let loaded = archive
                .load_checkpoint()
                .await
                .map_err(error)?
                .ok_or_else(|| fail("bootstrap requires an initialized empty checkpoint"))?;
            if loaded.manifest().tip().index() != 0 || !loaded.manifest().segments().is_empty() {
                return Err(fail("bootstrap requires an initialized empty checkpoint"));
            }
            write_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                loaded.manifest().identity(),
                node_id,
            )?;
            Ok(StartupPreparation::RecorderFirst {
                open_policy: RecorderOpenPolicy::CreateAfterRehydration,
            })
        }
        HaStartupMode::Rejoin if local_data_is_fresh(data_dir)? => {
            let identity = archive.checkpoint_identity().map_err(error)?;
            let marker =
                encode_local_checkpoint_identity_marker(execution_profile, identity, node_id)?;
            let tip =
                rhiza_node::durability::restore_checkpoint_to_fresh_data_dir_for_node_with_marker(
                    archive.clone(),
                    data_dir,
                    node_id,
                    LOCAL_CHECKPOINT_IDENTITY_FILE,
                    &marker,
                )
                .await
                .map_err(error)?;
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
            let loaded = archive
                .load_checkpoint()
                .await
                .map_err(error)?
                .ok_or_else(|| fail("rejoin requires an initialized checkpoint"))?;
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
            )?;
            if restore_state != rhiza_node::durability::CheckpointRestoreState::None {
                let marker =
                    encode_local_checkpoint_identity_marker(execution_profile, identity, node_id)?;
                let tip = if execution_profile == ExecutionProfile::Graph {
                    rhiza_node::durability::restore_checkpoint_to_fresh_data_dir_for_node_with_marker(
                        archive.clone(),
                        data_dir,
                        node_id,
                        LOCAL_CHECKPOINT_IDENTITY_FILE,
                        &marker,
                    )
                    .await
                } else {
                    rhiza_node::durability::restore_checkpoint_for_rejoin_preserving_recorder(
                        archive.clone(),
                        data_dir,
                        node_id,
                        execution_profile,
                        LOCAL_CHECKPOINT_IDENTITY_FILE,
                        &marker,
                    )
                    .await
                }
                .map_err(error)?;
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
            if let Err(view_error) = rhiza_node::durability::validate_local_recovery_view(
                data_dir,
                identity,
                node_id,
                execution_profile,
                checkpoint_root,
            ) {
                eprintln!(
                    "local recovery view is not trustworthy ({view_error}); quarantining rebuildable state and restoring the verified checkpoint"
                );
                let marker =
                    encode_local_checkpoint_identity_marker(execution_profile, identity, node_id)?;
                let tip = rhiza_node::durability::restore_checkpoint_for_rejoin_preserving_recorder(
                    archive.clone(),
                    data_dir,
                    node_id,
                    execution_profile,
                    LOCAL_CHECKPOINT_IDENTITY_FILE,
                    &marker,
                )
                .await
                .map_err(|restore_error| {
                    fail(format!(
                        "rebuildable local recovery view was quarantined but verified checkpoint restore failed: {restore_error}"
                    ))
                })?;
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
            let identity = archive.checkpoint_identity().map_err(error)?;
            let checkpoint = archive
                .load_checkpoint()
                .await
                .map_err(error)?
                .ok_or_else(|| fail("disaster startup requires an initialized checkpoint"))?;
            let checkpoint_root = LogAnchor::new(
                checkpoint.manifest().tip().index(),
                checkpoint.manifest().tip().hash(),
            );
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
            rhiza_node::durability::restore_checkpoint_to_fresh_data_dir_for_node_with_marker(
                archive.clone(),
                data_dir,
                node_id,
                LOCAL_CHECKPOINT_IDENTITY_FILE,
                &marker,
            )
            .await
            .map_err(error)?;
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
) -> Result<StartupPreparation, HaStartupError> {
    if mode != HaStartupMode::Rejoin {
        return Err(fail("successor startup requires rejoin mode"));
    }
    validate_predecessor_binding(config, target_config_id, predecessor)?;
    let initialized = archive.initialize_checkpoint().await.map_err(error)?;
    if initialized.manifest().tip().index() != 0 || !initialized.manifest().segments().is_empty() {
        return Err(fail(
            "successor target checkpoint namespace must be empty before activation",
        ));
    }
    let identity = archive.checkpoint_identity().map_err(error)?;
    let data_dir = config.data_dir();
    let _exact_marker_exists = exact_checkpoint_marker_exists(
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
    let recorder_state = preflight_local_recorder(data_dir, identity, config.membership())?;
    let recorder_state = recover_local_recorder_before_view_recovery(
        recorder_state,
        data_dir,
        config.node_id(),
        identity,
        config.membership(),
    )?;
    if recorder_state == LocalRecorderState::Missing {
        install_successor_recorder_for_startup(config, target_config_id, predecessor)?;
    }
    if controls == SuccessorRestoreControlState::Intent {
        complete_adopted_successor_prestage(data_dir, &expected).map_err(error)?;
    }
    write_local_checkpoint_identity_marker(
        data_dir,
        config.execution_profile(),
        identity,
        config.node_id(),
    )?;
    Ok(StartupPreparation::RecorderFirst {
        open_policy: RecorderOpenPolicy::MustExist,
    })
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
    let ConfigChange::Stop { successor } =
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
                successor.clone(),
                command.hash(),
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
) -> Result<(), HaStartupError> {
    let recorder = open_recorder_after_preflight(
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

fn open_recorder_for_preparation(
    config: &NodeConfig,
    target_config_id: u64,
    policy: RecorderOpenPolicy,
) -> Result<RecorderFileStore, HaStartupError> {
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

fn open_recorder_after_preflight(
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
) -> Result<LocalRecorderState, HaStartupError> {
    if state != LocalRecorderState::Recoverable {
        return Ok(state);
    }
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
    shutdown: &mut tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
) -> Result<Arc<NodeRuntime>, HaOpenError> {
    let mut last_retry_error = None;
    loop {
        if let Some(deadline) = *shutdown.borrow() {
            return Err(HaOpenError::Cancelled {
                deadline,
                cleanup: Ok(()),
            });
        }
        let attempt_config = config.clone();
        let attempt_consensus = consensus.clone();
        let attempt_peers = peers.clone();
        let mut attempt = tokio::task::spawn_blocking(move || {
            let peer_refs = attempt_peers
                .iter()
                .map(|peer| peer.as_ref())
                .collect::<Vec<_>>();
            NodeRuntime::open(attempt_config, attempt_consensus, &peer_refs)
        });
        let result = loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || shutdown.borrow().is_some() {
                        let deadline = shutdown
                            .borrow()
                            .unwrap_or_else(tokio::time::Instant::now);
                        let cleanup = cancel_runtime_open_attempt(
                            &mut attempt,
                            &consensus,
                            deadline,
                        )
                        .await;
                        return Err(HaOpenError::Cancelled { deadline, cleanup });
                    }
                }
                result = &mut attempt => break result,
            }
        };
        if let Some(deadline) = *shutdown.borrow() {
            let cleanup = cleanup_completed_runtime_open(result, &consensus, deadline);
            return Err(HaOpenError::Cancelled { deadline, cleanup });
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
    attempt: &mut tokio::task::JoinHandle<Result<NodeRuntime, NodeError>>,
    consensus: &Arc<ThreeNodeConsensus>,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    let attempt_deadline = deadline
        .checked_sub(HA_STARTUP_CLEANUP_GRACE)
        .unwrap_or(deadline);
    match tokio::time::timeout_at(attempt_deadline, &mut *attempt).await {
        Ok(result) => cleanup_completed_runtime_open(result, consensus, deadline),
        Err(_) => {
            attempt.abort();
            let mut errors = vec![HaNodeError::Shutdown(
                "runtime startup task did not stop before the shutdown deadline".into(),
            )];
            if !finish_ha_pending_consensus_rpcs(consensus, Duration::ZERO) {
                errors.push(HaNodeError::Shutdown(
                    "pending consensus RPCs did not drain before the shutdown deadline".into(),
                ));
            }
            combine_ha_errors(errors)
        }
    }
}

fn cleanup_completed_runtime_open(
    result: Result<Result<NodeRuntime, NodeError>, tokio::task::JoinError>,
    consensus: &Arc<ThreeNodeConsensus>,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    let mut errors = Vec::new();
    match result {
        Ok(Ok(runtime)) => runtime.cancel_operations(),
        Ok(Err(_)) => {}
        Err(error) => errors.push(HaNodeError::Shutdown(format!(
            "runtime startup task failed during cancellation: {error}"
        ))),
    }
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if !finish_ha_pending_consensus_rpcs(consensus, remaining) {
        errors.push(HaNodeError::Shutdown(
            "pending consensus RPCs did not drain before the shutdown deadline".into(),
        ));
    }
    combine_ha_errors(errors)
}

fn finish_ha_pending_consensus_rpcs(consensus: &ThreeNodeConsensus, timeout: Duration) -> bool {
    if matches!(
        tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor()),
        Ok(tokio::runtime::RuntimeFlavor::MultiThread)
    ) {
        tokio::task::block_in_place(|| consensus.finish_pending_rpcs(timeout))
    } else {
        consensus.finish_pending_rpcs(timeout)
    }
}

async fn rehydrate_recorder_with_retry(
    runtime: Arc<NodeRuntime>,
    recorder: RecorderFileStore,
    checkpoint_index: u64,
    shutdown: &mut tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
) -> Result<(), HaOpenError> {
    loop {
        require_startup_active(shutdown)?;
        let attempt_runtime = runtime.clone();
        let attempt_recorder = recorder.clone();
        let mut attempt = tokio::task::spawn_blocking(move || {
            rehydrate_recorder_after_checkpoint(
                &attempt_runtime,
                &attempt_recorder,
                checkpoint_index,
            )
        });
        let result = loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || shutdown.borrow().is_some() {
                        let deadline = shutdown
                            .borrow()
                            .unwrap_or_else(tokio::time::Instant::now);
                        let cleanup = cancel_rehydrate_attempt(
                            &mut attempt,
                            &runtime,
                            deadline,
                        )
                        .await;
                        return Err(HaOpenError::Cancelled { deadline, cleanup });
                    }
                }
                result = &mut attempt => break result,
            }
        };
        if let Some(deadline) = *shutdown.borrow() {
            runtime.cancel_operations();
            let cleanup = cleanup_completed_rehydrate(result, &runtime, deadline);
            return Err(HaOpenError::Cancelled { deadline, cleanup });
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
    attempt: &mut tokio::task::JoinHandle<Result<(), NodeError>>,
    runtime: &Arc<NodeRuntime>,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    runtime.cancel_operations();
    let attempt_deadline = deadline
        .checked_sub(HA_STARTUP_CLEANUP_GRACE)
        .unwrap_or(deadline);
    match tokio::time::timeout_at(attempt_deadline, &mut *attempt).await {
        Ok(result) => cleanup_completed_rehydrate(result, runtime, deadline),
        Err(_) => {
            attempt.abort();
            let mut errors = vec![HaNodeError::Shutdown(
                "recorder rehydration task did not stop before the shutdown deadline".into(),
            )];
            if !finish_ha_pending_consensus_rpcs(runtime.consensus(), Duration::ZERO) {
                errors.push(HaNodeError::Shutdown(
                    "pending consensus RPCs did not drain before the shutdown deadline".into(),
                ));
            }
            combine_ha_errors(errors)
        }
    }
}

fn cleanup_completed_rehydrate(
    result: Result<Result<(), NodeError>, tokio::task::JoinError>,
    runtime: &Arc<NodeRuntime>,
    deadline: tokio::time::Instant,
) -> Result<(), HaNodeError> {
    let mut errors = Vec::new();
    if let Err(error) = result {
        errors.push(HaNodeError::Shutdown(format!(
            "recorder rehydration task failed during cancellation: {error}"
        )));
    }
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if !finish_ha_pending_consensus_rpcs(runtime.consensus(), remaining) {
        errors.push(HaNodeError::Shutdown(
            "pending consensus RPCs did not drain before the shutdown deadline".into(),
        ));
    }
    combine_ha_errors(errors)
}

fn require_startup_active(
    shutdown: &tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
) -> Result<(), HaOpenError> {
    if let Some(deadline) = *shutdown.borrow() {
        Err(HaOpenError::Cancelled {
            deadline,
            cleanup: Ok(()),
        })
    } else {
        Ok(())
    }
}

async fn wait_for_startup_retry(
    shutdown: &mut tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
) -> Result<(), HaOpenError> {
    if let Some(deadline) = *shutdown.borrow() {
        return Err(HaOpenError::Cancelled {
            deadline,
            cleanup: Ok(()),
        });
    }
    tokio::select! {
        () = tokio::time::sleep(STARTUP_RETRY_DELAY) => Ok(()),
        changed = shutdown.changed() => {
            if changed.is_err() || shutdown.borrow().is_some() {
                let deadline = shutdown
                    .borrow()
                    .unwrap_or_else(tokio::time::Instant::now);
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
) -> Result<(), HaStartupError> {
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
            return read_and_validate_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                identity,
                node_id,
            )
        }
        Err(metadata_error) if metadata_error.kind() == io::ErrorKind::NotFound => {}
        Err(metadata_error) => {
            return Err(fail(format!(
                "cannot inspect local checkpoint identity marker: {metadata_error}"
            )))
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
        match fs::hard_link(&temporary, &marker_path) {
            Ok(()) => {}
            Err(link_error) if link_error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(link_error) => {
                return Err(fail(format!(
                    "cannot atomically publish checkpoint identity marker: {link_error}"
                )))
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
            return Ok(false)
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
    use super::*;

    #[tokio::test]
    async fn service_start_prefers_shutdown_when_both_signals_are_ready() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let (_shutdown, mut shutdown_rx) = tokio::sync::watch::channel(Some(deadline));
        let (started, started_rx) = tokio::sync::oneshot::channel();
        started.send(()).unwrap();

        assert_eq!(
            wait_for_service_start_or_shutdown(started_rx, &mut shutdown_rx)
                .await
                .unwrap(),
            ServiceStartup::Shutdown(deadline)
        );
    }
}
