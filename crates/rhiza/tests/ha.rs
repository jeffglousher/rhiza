use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_core::{ConfigurationState, LogAnchor, LogHash};
use rhiza_node::{
    durability::{finalize_successor_prestage_for_stop, inspect_successor_prestage},
    install_successor_recorder, NodeRuntime,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{Membership, RecorderFileStore, RecorderRpc, ThreeNodeConsensus};
#[cfg(feature = "test-hooks")]
use rhizadb::HaServiceActivationGate;
use rhizadb::{
    CertifiedTailRecord, CertifiedTailRequest, CertifiedTailResponse, CheckpointCoordinator,
    DurabilityMode, ExecutionProfile, HaCertifiedTailError, HaCertifiedTailSource, HaNodeError,
    HaNodeStatus, HaPredecessor, HaRecorderTransport, HaServeConfig, HaStartupConfig,
    HaStartupError, HaStartupMode, HaSuccessorNode, HaSuccessorPrestageConfig, NodeConfig,
    PeerConfig, StopInformation,
};
#[cfg(feature = "test-hooks")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(feature = "test-hooks")]
async fn readiness_is_ok(address: std::net::SocketAddr) -> bool {
    readiness_status(address).await.contains(" 200 ")
}

#[cfg(feature = "test-hooks")]
async fn readiness_status(address: std::net::SocketAddr) -> String {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_owned()
}

#[cfg(feature = "test-hooks")]
async fn staging_health_keepalive(address: std::net::SocketAddr) -> String {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            b"GET /livez HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\nGET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}

#[derive(Clone)]
struct ControlledTailSource {
    seed: LogAnchor,
    stop: Arc<Mutex<Option<CertifiedTailRecord>>>,
    stop_page_blocked: Arc<AtomicBool>,
    stop_page_entered: tokio::sync::watch::Sender<bool>,
    stop_page_released: tokio::sync::watch::Sender<bool>,
}

impl ControlledTailSource {
    fn new(seed: LogAnchor) -> Self {
        Self {
            seed,
            stop: Arc::new(Mutex::new(None)),
            stop_page_blocked: Arc::new(AtomicBool::new(false)),
            stop_page_entered: tokio::sync::watch::channel(false).0,
            stop_page_released: tokio::sync::watch::channel(false).0,
        }
    }

    fn publish_stop(&self, stop: &StopInformation) {
        *self.stop.lock().unwrap() = Some(CertifiedTailRecord {
            entry: stop.entry.clone(),
            proof: stop.proof.clone(),
        });
    }

    #[cfg(feature = "test-hooks")]
    async fn stop_page_entered(&self) {
        let mut entered = self.stop_page_entered.subscribe();
        while !*entered.borrow() {
            entered.changed().await.unwrap();
        }
    }

    #[cfg(feature = "test-hooks")]
    fn stop_page_release_guard(&self) -> ControlledTailPageRelease {
        self.stop_page_blocked.store(true, Ordering::Release);
        ControlledTailPageRelease(self.stop_page_released.clone())
    }
}

#[cfg(feature = "test-hooks")]
struct ControlledTailPageRelease(tokio::sync::watch::Sender<bool>);

#[cfg(feature = "test-hooks")]
impl Drop for ControlledTailPageRelease {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

impl HaCertifiedTailSource for ControlledTailSource {
    fn fetch<'a>(
        &'a self,
        request: &'a CertifiedTailRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<CertifiedTailResponse, HaCertifiedTailError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let stop = self.stop.lock().unwrap().clone();
            match stop {
                None if request.from == self.seed => Ok(CertifiedTailResponse {
                    records: Vec::new(),
                    observed_tip: self.seed,
                }),
                Some(stop) if request.from == self.seed => {
                    if self.stop_page_blocked.load(Ordering::Acquire) {
                        self.stop_page_entered.send_replace(true);
                        let mut released = self.stop_page_released.subscribe();
                        while !*released.borrow() {
                            released.changed().await.map_err(|_| {
                                HaCertifiedTailError::Unavailable(
                                    "test Stop-page release sender closed".into(),
                                )
                            })?;
                        }
                    }
                    let observed_tip = LogAnchor::new(stop.entry.index, stop.entry.hash);
                    Ok(CertifiedTailResponse {
                        records: vec![stop],
                        observed_tip,
                    })
                }
                Some(stop) if request.from == LogAnchor::new(stop.entry.index, stop.entry.hash) => {
                    Ok(CertifiedTailResponse {
                        records: Vec::new(),
                        observed_tip: request.from,
                    })
                }
                _ => Err(HaCertifiedTailError::Rejected(format!(
                    "unexpected test tail anchor {}",
                    request.from.index()
                ))),
            }
        })
    }
}

#[derive(Clone)]
struct UnavailableTailSource {
    attempts: tokio::sync::watch::Sender<u64>,
}

impl UnavailableTailSource {
    fn new() -> Self {
        Self {
            attempts: tokio::sync::watch::channel(0).0,
        }
    }

    async fn entered_retry(&self) {
        let mut attempts = self.attempts.subscribe();
        while *attempts.borrow() == 0 {
            attempts.changed().await.unwrap();
        }
    }
}

impl HaCertifiedTailSource for UnavailableTailSource {
    fn fetch<'a>(
        &'a self,
        _request: &'a CertifiedTailRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<CertifiedTailResponse, HaCertifiedTailError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let next = self.attempts.borrow().saturating_add(1);
            self.attempts.send_replace(next);
            Err(HaCertifiedTailError::Unavailable(
                "injected transient tail outage".into(),
            ))
        })
    }
}

#[derive(Clone)]
struct InvalidPageTailSource {
    attempts: tokio::sync::watch::Sender<u64>,
    record: tokio::sync::watch::Sender<Option<CertifiedTailRecord>>,
}

impl InvalidPageTailSource {
    fn new() -> Self {
        Self {
            attempts: tokio::sync::watch::channel(0).0,
            record: tokio::sync::watch::channel(None).0,
        }
    }

    fn publish_stop(&self, stop: &StopInformation) {
        self.record.send_replace(Some(CertifiedTailRecord {
            entry: stop.entry.clone(),
            proof: stop.proof.clone(),
        }));
    }

    async fn entered(&self) {
        let mut attempts = self.attempts.subscribe();
        while *attempts.borrow() == 0 {
            attempts.changed().await.unwrap();
        }
    }
}

impl HaCertifiedTailSource for InvalidPageTailSource {
    fn fetch<'a>(
        &'a self,
        request: &'a CertifiedTailRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<CertifiedTailResponse, HaCertifiedTailError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mut record = self.record.subscribe();
            while record.borrow().is_none() {
                record.changed().await.unwrap();
            }
            let record = record.borrow().clone().unwrap();
            let next = self.attempts.borrow().saturating_add(1);
            self.attempts.send_replace(next);
            Ok(CertifiedTailResponse {
                records: vec![record],
                // Deliberately contradict the returned record tip so the real
                // learner apply path rejects this certified page.
                observed_tip: request.from,
            })
        })
    }
}

fn archive(root: &Path) -> ObjectArchiveStore {
    ObjectArchiveStore::new_checkpoint_for_single_process(
        ObjStore::new(ObjStoreConfig::Local {
            root: root.to_path_buf(),
        })
        .unwrap(),
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    )
}

fn node_config(data_dir: &Path) -> NodeConfig {
    let peers = ["node-1", "node-2", "node-3"]
        .into_iter()
        .enumerate()
        .map(|(index, node_id)| {
            PeerConfig::new(
                node_id,
                format!("http://127.0.0.1:{}", 9101 + index),
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
    .with_execution_profile(ExecutionProfile::Sqlite)
    .unwrap()
}

fn recorder_clients(root: &Path) -> Vec<(String, Box<dyn RecorderRpc>)> {
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

fn successor_recorder_clients(
    root: &Path,
    predecessor: &Membership,
    successor: &Membership,
    stop: &StopInformation,
) -> Vec<(String, Box<dyn RecorderRpc>)> {
    successor
        .members()
        .iter()
        .map(|node_id| {
            let recorder = RecorderFileStore::new_with_membership(
                root.join(node_id),
                node_id.clone(),
                "rhiza:sql:cluster-a",
                1,
                1,
                predecessor.clone(),
            )
            .unwrap();
            install_successor_recorder(&recorder, 2, successor.clone(), stop).unwrap();
            (node_id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
        })
        .collect()
}

fn active_successor_recorder_clients(
    root: &Path,
    successor: &Membership,
) -> Vec<(String, Box<dyn RecorderRpc>)> {
    successor
        .members()
        .iter()
        .map(|node_id| {
            let recorder = RecorderFileStore::open_existing_with_membership(
                root.join(node_id),
                node_id.clone(),
                "rhiza:sql:cluster-a",
                1,
                2,
                successor.clone(),
            )
            .unwrap();
            (node_id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
        })
        .collect()
}

#[tokio::test]
async fn ha_bootstrap_rejects_nonfresh_local_state_before_opening_a_recorder() {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let data_dir = root.path().join("node");
    std::fs::create_dir_all(data_dir.join("sqlite")).unwrap();
    std::fs::write(data_dir.join("sqlite/existing"), b"state").unwrap();

    let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = HaStartupConfig::new(
        node_config(&data_dir),
        archive,
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Bootstrap,
    )
    .start(HaServeConfig::new(
        recorder_listener,
        service_listener,
        HaRecorderTransport::Http,
        recorder_clients(&root.path().join("recorders")),
        Vec::new(),
    ));
    let error = node.monitor().await.unwrap_err();

    assert!(error.to_string().contains("fresh local data directory"));
    let _ = node.shutdown().await;
    assert!(!data_dir.join("recorder").exists());
}

#[tokio::test]
async fn ha_standard_startup_rejects_bound_stopped_successor_before_creating_local_state() {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            recorder_clients(&root.path().join("source-recorders")),
        )
        .unwrap(),
    );
    let source =
        NodeRuntime::open(node_config(&root.path().join("source")), consensus, &[]).unwrap();
    let stop = source
        .stop_current_configuration_for_successor(&successor)
        .unwrap();
    assert!(source
        .consensus()
        .finish_pending_rpcs(Duration::from_secs(1)));
    let peers = successor
        .members()
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            PeerConfig::new(
                node_id,
                format!("http://127.0.0.1:{}", 9201 + index),
                format!("successor-peer-token-{}", index + 1),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let data_dir = root.path().join("successor");
    let successor_config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-4",
        data_dir.clone(),
        1,
        successor,
        source.configuration_state().unwrap(),
        peers,
        "successor-client-token",
    )
    .unwrap()
    .with_log_initial_configuration(ConfigurationState::active(1, predecessor.digest()))
    .with_predecessor_stop_entry(stop.entry);

    let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = HaStartupConfig::new(
        successor_config,
        archive,
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Bootstrap,
    )
    .start(HaServeConfig::new(
        recorder_listener,
        service_listener,
        HaRecorderTransport::Http,
        recorder_clients(&root.path().join("recorders")),
        Vec::new(),
    ));
    let error = node.monitor().await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("stopped configuration requires a predecessor"),
        "{error}"
    );
    let _ = node.shutdown().await;
    assert!(!data_dir.exists());
}

#[tokio::test]
async fn successor_prestage_exposes_the_exact_seed_and_target_without_opening_a_runtime() {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();

    let prepared = HaSuccessorPrestageConfig::new(
        archive,
        root.path().join("prestage"),
        "node-4",
        ExecutionProfile::Sqlite,
        predecessor,
        successor.clone(),
        "tail-token",
    )
    .prepare()
    .await
    .unwrap();

    assert_eq!(prepared.identity().cluster_id(), "rhiza:sql:cluster-a");
    assert_eq!(prepared.identity().epoch(), 1);
    assert_eq!(prepared.identity().predecessor_config_id(), 1);
    assert_eq!(prepared.identity().predecessor_recovery_generation(), 1);
    assert_eq!(prepared.identity().target_config_id(), 2);
    assert_eq!(
        prepared.identity().target_membership_digest(),
        successor.digest()
    );
    assert_eq!(
        prepared.identity().seed_anchor(),
        LogAnchor::new(0, rhiza_core::LogHash::ZERO)
    );
    assert_eq!(
        prepared.tail_request(8).unwrap().from,
        prepared.identity().seed_anchor()
    );
    assert!(!root.path().join("prestage/recorder").exists());
}

#[tokio::test]
async fn finalized_successor_restart_handles_empty_target_archive_before_activation() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.path().join("archive"),
    })
    .unwrap();
    let source_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store.clone(),
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    );
    source_archive.initialize_checkpoint().await.unwrap();
    let coordinator = CheckpointCoordinator::open(source_archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            recorder_clients(&root.path().join("source-recorders")),
        )
        .unwrap(),
    );
    let source =
        NodeRuntime::open(node_config(&root.path().join("source")), consensus, &[]).unwrap();
    source.write("seed-write", "key", "seed").unwrap();
    let seed = source.checkpoint_compact(&coordinator).await.unwrap();
    let target_data_dir = root.path().join("successor");
    let learner = HaSuccessorPrestageConfig::new(
        source_archive.clone(),
        root.path().join("prestage"),
        "node-4",
        ExecutionProfile::Sqlite,
        predecessor.clone(),
        successor.clone(),
        "tail-token",
    )
    .prepare()
    .await
    .unwrap();
    assert_eq!(learner.identity().seed_anchor(), *seed.compacted());
    let learner = learner.publish(&target_data_dir).unwrap();

    let stop = source
        .stop_current_configuration_for_successor(&successor)
        .unwrap();
    assert!(source
        .consensus()
        .finish_pending_rpcs(Duration::from_secs(1)));
    let request = learner.tail_request(8).unwrap();
    let stop_anchor = LogAnchor::new(stop.entry.index, stop.entry.hash);
    let progress = learner
        .apply_page(
            &request,
            &CertifiedTailResponse {
                records: vec![CertifiedTailRecord {
                    entry: stop.entry.clone(),
                    proof: stop.proof.clone(),
                }],
                observed_tip: stop_anchor,
            },
        )
        .unwrap();
    assert_eq!(progress.durable, stop_anchor);
    assert_eq!(progress.applied, stop_anchor);

    let target_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            2,
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    );
    assert!(target_archive.load_checkpoint().await.unwrap().is_none());
    let peers = successor
        .members()
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            PeerConfig::new(
                node_id,
                format!("http://127.0.0.1:{}", 9201 + index),
                format!("successor-peer-token-{}", index + 1),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let successor_config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-4",
        target_data_dir,
        1,
        successor.clone(),
        source.configuration_state().unwrap(),
        peers,
        "successor-client-token",
    )
    .unwrap()
    .with_log_initial_configuration(ConfigurationState::active(1, predecessor.digest()))
    .with_predecessor_stop_entry(stop.entry.clone());
    let wrong_predecessor = Membership::new(["node-1", "node-2", "node-9"]).unwrap();
    let error = learner
        .finalize(
            HaStartupConfig::new(
                successor_config.clone(),
                target_archive.clone(),
                DurabilityMode::Sync,
                60_000,
                HaStartupMode::Rejoin,
            )
            .with_predecessor(HaPredecessor::new(wrong_predecessor.clone(), stop.clone())),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("InvalidTransition")
            || error
                .to_string()
                .contains("Stop binding conflicts with the prestage target")
            || error
                .to_string()
                .contains("predecessor membership or Stop binding"),
        "{error}"
    );
    let learner = HaSuccessorPrestageConfig::resume(
        successor_config.data_dir(),
        1,
        predecessor.clone(),
        "tail-token",
    )
    .unwrap()
    .publish(successor_config.data_dir())
    .unwrap();
    assert_eq!(learner.durable_anchor().unwrap(), stop_anchor);
    assert_eq!(learner.applied_anchor().unwrap(), stop_anchor);
    drop(learner);
    let prestage = inspect_successor_prestage(
        successor_config.data_dir(),
        ConfigurationState::active(1, predecessor.digest()),
    )
    .unwrap();
    let finalized = finalize_successor_prestage_for_stop(prestage, &stop, &predecessor).unwrap();
    drop(finalized);
    let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wrong_node = HaStartupConfig::new(
        successor_config.clone(),
        target_archive.clone(),
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Rejoin,
    )
    .with_predecessor(HaPredecessor::new(wrong_predecessor, stop.clone()))
    .start(HaServeConfig::new(
        recorder_listener,
        service_listener,
        HaRecorderTransport::Http,
        Vec::new(),
        Vec::new(),
    ));
    let error = wrong_node.monitor().await.unwrap_err();
    assert!(
        error.to_string().contains("InvalidTransition")
            || error
                .to_string()
                .contains("Stop binding conflicts with the prestage target")
            || error
                .to_string()
                .contains("predecessor membership or Stop binding"),
        "{error}"
    );
    let _ = wrong_node.shutdown().await;
    assert!(HaSuccessorPrestageConfig::resume(
        successor_config.data_dir(),
        1,
        predecessor.clone(),
        "tail-token",
    )
    .is_err());
    let startup = HaStartupConfig::new(
        successor_config.clone(),
        target_archive.clone(),
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Rejoin,
    )
    .with_predecessor(HaPredecessor::new(predecessor.clone(), stop.clone()));
    let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = startup.start(HaServeConfig::new(
        recorder_listener,
        service_listener,
        HaRecorderTransport::Http,
        successor_recorder_clients(
            &root.path().join("successor-recorders"),
            &predecessor,
            &successor,
            &stop,
        ),
        Vec::new(),
    ));

    tokio::time::timeout(Duration::from_secs(10), async {
        while node.status() == rhizadb::HaNodeStatus::Starting {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(node.status(), rhizadb::HaNodeStatus::AwaitingActivation);
    let initialized = target_archive.load_checkpoint().await.unwrap().unwrap();
    assert_eq!(initialized.manifest().tip().index(), 0);
    assert!(initialized.manifest().segments().is_empty());
    tokio::select! {
        biased;
        _ = node.ready() => panic!("AwaitingActivation must not become ready"),
        () = std::future::ready(()) => {}
    }
    node.shutdown().await.unwrap();

    let active_draft = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-4",
        successor_config.data_dir().clone(),
        1,
        successor.clone(),
        ConfigurationState::active(2, successor.digest()),
        successor
            .members()
            .iter()
            .enumerate()
            .map(|(index, node_id)| {
                PeerConfig::new(
                    node_id,
                    format!("http://127.0.0.1:{}", 9201 + index),
                    format!("successor-peer-token-{}", index + 1),
                )
                .unwrap()
            })
            .collect::<Vec<_>>(),
        "successor-client-token",
    )
    .unwrap();
    let tail = ControlledTailSource::new(*seed.compacted());
    tail.publish_stop(&stop);
    let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live = HaSuccessorPrestageConfig::new(
        source_archive,
        root.path().join("prestage"),
        "node-4",
        ExecutionProfile::Sqlite,
        predecessor.clone(),
        successor.clone(),
        "tail-token",
    )
    .start_live(
        HaStartupConfig::new(
            active_draft,
            target_archive,
            DurabilityMode::Sync,
            60_000,
            HaStartupMode::Rejoin,
        ),
        HaServeConfig::new(
            recorder_listener,
            service_listener,
            HaRecorderTransport::Http,
            active_successor_recorder_clients(&root.path().join("successor-recorders"), &successor),
            Vec::new(),
        ),
        Arc::new(tail),
    )
    .unwrap();
    live.bind_predecessor(HaPredecessor::new(predecessor, stop))
        .unwrap();
    let handle = tokio::time::timeout(Duration::from_secs(15), live.ready())
        .await
        .expect("finalized live successor must resume before its first target checkpoint")
        .unwrap();
    handle
        .put("empty-target-restart", "key-after-restart", "value")
        .await
        .unwrap();
    live.shutdown().await.unwrap();
}

async fn start_faulting_live_successor(
    root: &Path,
    tail_source: Arc<dyn HaCertifiedTailSource>,
) -> (
    HaSuccessorNode,
    std::net::SocketAddr,
    std::net::SocketAddr,
    StopInformation,
) {
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.join("archive"),
    })
    .unwrap();
    let source_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store.clone(),
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    );
    source_archive.initialize_checkpoint().await.unwrap();
    let coordinator = CheckpointCoordinator::open(source_archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            recorder_clients(&root.join("source-recorders")),
        )
        .unwrap(),
    );
    let source = NodeRuntime::open(node_config(&root.join("source")), consensus, &[]).unwrap();
    source.write("seed-write", "key", "seed").unwrap();
    source.checkpoint_compact(&coordinator).await.unwrap();
    let stop = source
        .stop_current_configuration_for_successor(&successor)
        .unwrap();
    assert!(source
        .consensus()
        .finish_pending_rpcs(Duration::from_secs(1)));

    let target_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            2,
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    );
    let peers = successor
        .members()
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            PeerConfig::new(
                node_id,
                format!("http://127.0.0.1:{}", 9401 + index),
                format!("fault-peer-token-{}", index + 1),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let target_config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-4",
        root.join("successor"),
        1,
        successor.clone(),
        ConfigurationState::active(2, successor.digest()),
        peers,
        "successor-client-token",
    )
    .unwrap();
    let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let recorder_address = recorder_listener.local_addr().unwrap();
    let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service_address = service_listener.local_addr().unwrap();
    let node = HaSuccessorPrestageConfig::new(
        source_archive,
        root.join("prestage"),
        "node-4",
        ExecutionProfile::Sqlite,
        predecessor.clone(),
        successor.clone(),
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
            successor_recorder_clients(
                &root.join("successor-recorders"),
                &predecessor,
                &successor,
                &stop,
            ),
            Vec::new(),
        ),
        tail_source,
    )
    .unwrap();
    (node, recorder_address, service_address, stop)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unavailable_tail_retry_is_interrupted_by_short_successor_shutdown_deadline() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.path().join("archive"),
    })
    .unwrap();
    let source_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store.clone(),
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    );
    source_archive.initialize_checkpoint().await.unwrap();
    let coordinator = CheckpointCoordinator::open(source_archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            recorder_clients(&root.path().join("source-recorders")),
        )
        .unwrap(),
    );
    let source =
        NodeRuntime::open(node_config(&root.path().join("source")), consensus, &[]).unwrap();
    source.write("seed-write", "key", "seed").unwrap();
    source.checkpoint_compact(&coordinator).await.unwrap();
    let stop = source
        .stop_current_configuration_for_successor(&successor)
        .unwrap();
    assert!(source
        .consensus()
        .finish_pending_rpcs(Duration::from_secs(1)));

    let target_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            2,
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    );
    let peers = successor
        .members()
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            PeerConfig::new(
                node_id,
                format!("http://127.0.0.1:{}", 9401 + index),
                format!("unavailable-peer-token-{}", index + 1),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let target_config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-4",
        root.path().join("successor"),
        1,
        successor.clone(),
        ConfigurationState::active(2, successor.digest()),
        peers,
        "successor-client-token",
    )
    .unwrap();
    let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let recorder_address = recorder_listener.local_addr().unwrap();
    let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service_address = service_listener.local_addr().unwrap();
    let tail = UnavailableTailSource::new();
    let node = HaSuccessorPrestageConfig::new(
        source_archive,
        root.path().join("prestage"),
        "node-4",
        ExecutionProfile::Sqlite,
        predecessor.clone(),
        successor.clone(),
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
            successor_recorder_clients(
                &root.path().join("successor-recorders"),
                &predecessor,
                &successor,
                &stop,
            ),
            Vec::new(),
        ),
        Arc::new(tail.clone()),
    )
    .unwrap();

    tokio::time::timeout(Duration::from_secs(10), tail.entered_retry())
        .await
        .expect("live successor must enter the transient unavailable retry");
    assert_eq!(node.status(), HaNodeStatus::CatchingUp);
    tokio::time::timeout(
        Duration::from_millis(150),
        node.shutdown_with_timeout(Duration::from_millis(100)),
    )
    .await
    .expect("shutdown must interrupt the 250ms retry before its 100ms D")
    .unwrap();
    let recorder_rebound = tokio::net::TcpListener::bind(recorder_address)
        .await
        .unwrap();
    let service_rebound = tokio::net::TcpListener::bind(service_address)
        .await
        .unwrap();
    drop((recorder_rebound, service_rebound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_certified_page_cleans_up_live_successor_staging_before_returning_primary() {
    let root = tempfile::tempdir().unwrap();
    let tail = InvalidPageTailSource::new();
    let (node, recorder_address, service_address, stop) =
        start_faulting_live_successor(root.path(), Arc::new(tail.clone())).await;
    tail.publish_stop(&stop);
    tokio::time::timeout(Duration::from_secs(10), tail.entered())
        .await
        .expect("live successor must request the invalid certified page");

    let error = tokio::time::timeout(Duration::from_secs(5), node.monitor())
        .await
        .expect("invalid certified page must terminate the live successor")
        .unwrap_err();
    assert!(
        matches!(
            &error,
            HaNodeError::Startup(HaStartupError::Source(message))
                if message
                    == "learner page is invalid: certified tail records are not consecutive from the request"
        ),
        "invalid-page primary was lost: {error}"
    );
    let shutdown = tokio::time::timeout(
        Duration::from_secs(1),
        node.shutdown_with_timeout(Duration::from_millis(500)),
    )
    .await
    .expect("terminal live successor shutdown must complete");
    let shutdown_error = shutdown.expect_err("terminal primary must remain observable");
    assert!(matches!(
        &shutdown_error,
        HaNodeError::Startup(HaStartupError::Source(message))
            if message
                == "learner page is invalid: certified tail records are not consecutive from the request"
    ));
    let recorder_rebound = tokio::net::TcpListener::bind(recorder_address)
        .await
        .unwrap();
    let service_rebound = tokio::net::TcpListener::bind(service_address)
        .await
        .unwrap();
    drop((recorder_rebound, service_rebound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "test-hooks")]
async fn live_successor_keeps_one_owner_and_listener_from_prestop_ready_through_active() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.path().join("archive"),
    })
    .unwrap();
    let source_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store.clone(),
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    );
    source_archive.initialize_checkpoint().await.unwrap();
    let coordinator = CheckpointCoordinator::open(source_archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            recorder_clients(&root.path().join("source-recorders")),
        )
        .unwrap(),
    );
    let source =
        NodeRuntime::open(node_config(&root.path().join("source")), consensus, &[]).unwrap();
    source.write("seed-write", "key", "seed").unwrap();
    let seed = source.checkpoint_compact(&coordinator).await.unwrap();
    let stop = source
        .stop_current_configuration_for_successor(&successor)
        .unwrap();
    assert!(source
        .consensus()
        .finish_pending_rpcs(Duration::from_secs(1)));

    let target_data_dir = root.path().join("successor");
    let target_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            2,
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    );
    let peers = successor
        .members()
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            PeerConfig::new(
                node_id,
                format!("http://127.0.0.1:{}", 9201 + index),
                format!("successor-peer-token-{}", index + 1),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let target_config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-4",
        target_data_dir,
        1,
        successor.clone(),
        ConfigurationState::active(2, successor.digest()),
        peers,
        "successor-client-token",
    )
    .unwrap();
    let restart_config = target_config.clone();
    let activation_gate = HaServiceActivationGate::new();
    let startup = HaStartupConfig::new(
        target_config,
        target_archive.clone(),
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Rejoin,
    )
    .with_service_activation_gate(activation_gate.clone());
    let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let recorder_address = recorder_listener.local_addr().unwrap();
    let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service_address = service_listener.local_addr().unwrap();
    let tail = ControlledTailSource::new(*seed.compacted());
    let restart_source_archive = source_archive.clone();
    let node = HaSuccessorPrestageConfig::new(
        source_archive,
        root.path().join("prestage"),
        "node-4",
        ExecutionProfile::Sqlite,
        predecessor.clone(),
        successor.clone(),
        "tail-token",
    )
    .start_live(
        startup,
        HaServeConfig::new(
            recorder_listener,
            service_listener,
            HaRecorderTransport::Http,
            successor_recorder_clients(
                &root.path().join("successor-recorders"),
                &predecessor,
                &successor,
                &stop,
            ),
            Vec::new(),
        )
        .with_tail_token("successor-tail-token"),
        Arc::new(tail.clone()),
    )
    .unwrap();

    tokio::time::timeout(Duration::from_secs(10), async {
        while !readiness_is_ok(service_address).await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("staging readyz must become 200 after pre-Stop catchup");
    assert_eq!(node.status(), HaNodeStatus::PreStopReady);
    assert!(readiness_status(service_address).await.contains(" 200 "));
    let initialized = target_archive.load_checkpoint().await.unwrap().unwrap();
    assert_eq!(initialized.manifest().tip().index(), 0);

    let stop_page_release = tail.stop_page_release_guard();
    tail.publish_stop(&stop);
    let bound = HaPredecessor::new(predecessor.clone(), stop.clone());
    node.bind_predecessor(bound.clone()).unwrap();
    node.bind_predecessor(bound).unwrap();
    assert!(node
        .bind_predecessor(HaPredecessor::new(successor.clone(), stop.clone()))
        .unwrap_err()
        .to_string()
        .contains("binding changed"));
    tokio::time::timeout(Duration::from_secs(10), tail.stop_page_entered())
        .await
        .expect("successor must request the Stop page after the first binding");
    assert_eq!(node.status(), HaNodeStatus::CatchingUp);
    assert!(readiness_status(service_address).await.contains(" 503 "));
    let blocked_response = staging_health_keepalive(service_address).await;
    assert_eq!(blocked_response.matches("HTTP/1.1 200").count(), 1);
    assert_eq!(blocked_response.matches("HTTP/1.1 503").count(), 1);

    drop(stop_page_release);
    tokio::time::timeout(Duration::from_secs(10), activation_gate.entered())
        .await
        .expect("child must reach the pre-service activation gate");
    // The child has finished preparation/open/router construction but must
    // not own the service FD yet.  Keep probing the actual staging socket,
    // including HTTP/1 keep-alive, to prove there is no health outage before
    // the late same-FD handoff.
    for _ in 0..8 {
        let response = staging_health_keepalive(service_address).await;
        assert!(
            response.contains("HTTP/1.1 200"),
            "livez lost before handoff: {response}"
        );
        assert_eq!(
            response.matches("HTTP/1.1 200").count(),
            1,
            "only livez may be ready before handoff: {response}"
        );
        assert_eq!(
            response.matches("HTTP/1.1 503").count(),
            1,
            "readyz must remain 503 before handoff: {response}"
        );
    }
    let release = activation_gate.release_guard();
    drop(release);
    let handle = tokio::time::timeout(Duration::from_secs(15), node.ready())
        .await
        .unwrap_or_else(|_| panic!("same live owner stalled in {:?}", node.status()))
        .unwrap();
    assert_eq!(node.status(), HaNodeStatus::Ready);
    handle
        .put("post-cutover", "active-key", "active-value")
        .await
        .unwrap();
    assert!(readiness_is_ok(service_address).await);

    node.shutdown().await.unwrap();
    let recorder_listener = tokio::net::TcpListener::bind(recorder_address)
        .await
        .unwrap();
    let service_listener = tokio::net::TcpListener::bind(service_address)
        .await
        .unwrap();
    let restarted = HaSuccessorPrestageConfig::new(
        restart_source_archive,
        root.path().join("prestage"),
        "node-4",
        ExecutionProfile::Sqlite,
        predecessor.clone(),
        successor.clone(),
        "tail-token",
    )
    .start_live(
        HaStartupConfig::new(
            restart_config,
            target_archive,
            DurabilityMode::Sync,
            60_000,
            HaStartupMode::Rejoin,
        ),
        HaServeConfig::new(
            recorder_listener,
            service_listener,
            HaRecorderTransport::Http,
            active_successor_recorder_clients(&root.path().join("successor-recorders"), &successor),
            Vec::new(),
        ),
        Arc::new(tail),
    )
    .unwrap();
    restarted
        .bind_predecessor(HaPredecessor::new(predecessor, stop))
        .unwrap();
    let restarted_handle = tokio::time::timeout(Duration::from_secs(15), restarted.ready())
        .await
        .expect("active successor must rejoin through the same public owner")
        .unwrap();
    restarted_handle
        .put("post-restart", "restart-key", "restart-value")
        .await
        .unwrap();
    restarted.shutdown().await.unwrap();
    let recorder = tokio::net::TcpListener::bind(recorder_address)
        .await
        .unwrap();
    let service = tokio::net::TcpListener::bind(service_address)
        .await
        .unwrap();
    drop((recorder, service));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_live_successor_rejoins_the_active_target_checkpoint_without_predecessor() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.path().join("archive"),
    })
    .unwrap();
    let source_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store.clone(),
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"rhiza-test-config"]),
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
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    );
    target_archive.initialize_checkpoint().await.unwrap();

    let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();
    let recorder_root = root.path().join("target-recorders");
    let target_recorders = successor
        .members()
        .iter()
        .map(|node_id| {
            let recorder = RecorderFileStore::new_with_membership(
                recorder_root.join(node_id),
                node_id.clone(),
                "rhiza:sql:cluster-a",
                1,
                2,
                successor.clone(),
            )
            .unwrap();
            (node_id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
        })
        .collect();
    let peers = successor
        .members()
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            PeerConfig::new(
                node_id,
                format!("http://127.0.0.1:{}", 9301 + index),
                format!("target-peer-token-{}", index + 1),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let checkpoint_config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-4",
        root.path().join("checkpoint-source"),
        1,
        successor.clone(),
        ConfigurationState::active(2, successor.digest()),
        peers.clone(),
        "successor-client-token",
    )
    .unwrap();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-4",
            1,
            2,
            target_recorders,
        )
        .unwrap(),
    );
    let runtime = NodeRuntime::open(checkpoint_config, consensus, &[]).unwrap();
    runtime.write("target-seed", "key", "value").unwrap();
    let coordinator = CheckpointCoordinator::open(target_archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    runtime.checkpoint_compact(&coordinator).await.unwrap();
    runtime
        .write("target-tail", "tail-key", "tail-value")
        .unwrap();
    let tail_index = runtime.applied_index().unwrap();
    coordinator.note_committed(tail_index);
    coordinator
        .flush_runtime(&runtime, tail_index)
        .await
        .unwrap();
    drop((runtime, coordinator));

    let fresh_config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-4",
        root.path().join("fresh-target"),
        1,
        successor.clone(),
        ConfigurationState::active(2, successor.digest()),
        peers,
        "successor-client-token",
    )
    .unwrap();
    let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = HaSuccessorPrestageConfig::new(
        source_archive,
        root.path().join("unused-prestage"),
        "node-4",
        ExecutionProfile::Sqlite,
        predecessor,
        successor.clone(),
        "tail-token",
    )
    .start_live(
        HaStartupConfig::new(
            fresh_config,
            target_archive,
            DurabilityMode::Sync,
            60_000,
            HaStartupMode::Rejoin,
        ),
        HaServeConfig::new(
            recorder_listener,
            service_listener,
            HaRecorderTransport::Http,
            active_successor_recorder_clients(&recorder_root, &successor),
            Vec::new(),
        ),
        Arc::new(ControlledTailSource::new(LogAnchor::new(
            0,
            rhiza_core::LogHash::ZERO,
        ))),
    )
    .unwrap();

    let handle = tokio::time::timeout(Duration::from_secs(15), node.ready())
        .await
        .expect("fresh successor must restore the active target checkpoint")
        .unwrap();
    handle
        .put("after-target-rejoin", "restored-key", "restored-value")
        .await
        .unwrap();
    node.shutdown().await.unwrap();
}
