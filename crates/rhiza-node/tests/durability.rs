use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

use rhiza_archive::{
    CheckpointIdentity, CheckpointPublisherOptions, ObjectArchiveStore, RestoredCheckpoint,
};
#[cfg(any(feature = "graph", feature = "kv"))]
use rhiza_core::ExecutionProfile;
use rhiza_core::{ConfigurationState, LogHash};
use rhiza_log::{FileLogStore, LogStore};
#[cfg(any(feature = "graph", feature = "kv"))]
use rhiza_node::effective_cluster_id;
#[cfg(feature = "kv")]
use rhiza_node::KvCommandV1;
use rhiza_node::{
    capture_expected_local_restore_state,
    install_prepared_checkpoint_for_rejoin_preserving_recorder,
    install_prepared_checkpoint_to_fresh_data_dir, prepare_checkpoint_restore,
    rehydrate_recorder_after_checkpoint, CheckpointCoordinator, CheckpointInstallMode,
    DurabilityError, DurabilityHealth, DurabilityMode, NodeConfig, NodeRuntime, PeerConfig,
    ReadConsistency, RestoreCompletionMarker, StartupIoContext,
};
#[cfg(feature = "graph")]
use rhiza_node::{GraphCommandV1, GraphValueV1};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{
    install_test_control_operation_probe, DecisionProof, EffectBundleBinding, Membership,
    ReadFenceObservation, ReadFenceRequest, RecordRequest, RecordSummary, RecorderFileStore,
    RecorderRpc, RecorderRpcContext, TestControlOperationProbe, ThreeNodeConsensus,
};
use rhiza_sql::SqliteStateMachine;

async fn load_restored_for_test(archive: &ObjectArchiveStore) -> RestoredCheckpoint {
    archive
        .load_checkpoint_restore()
        .await
        .unwrap()
        .unwrap()
        .into_parts()
        .1
}

async fn install_fresh_for_test(
    archive: &ObjectArchiveStore,
    data_dir: &Path,
    node_id: &str,
) -> Result<rhiza_archive::CheckpointTip, DurabilityError> {
    let prepared = prepare_checkpoint_restore(archive).await?;
    install_prepared_checkpoint_to_fresh_data_dir(
        &prepared,
        expected_restore_state_for_test(
            &prepared,
            data_dir,
            node_id,
            CheckpointInstallMode::Fresh,
            None,
        )?,
        None,
    )
}

fn expected_restore_state_for_test(
    prepared: &rhiza_node::PreparedCheckpointRestore,
    data_dir: &Path,
    node_id: &str,
    mode: CheckpointInstallMode,
    completion_marker_name: Option<&str>,
) -> Result<rhiza_node::ExpectedLocalRestoreState, DurabilityError> {
    capture_expected_local_restore_state(
        data_dir,
        mode,
        node_id,
        prepared.identity(),
        prepared.execution_profile(),
        ConfigurationState::active(
            prepared.identity().config_id(),
            prepared.identity().config_digest(),
        ),
        completion_marker_name,
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RestoreTreeEntry {
    kind: &'static str,
    bytes: Vec<u8>,
    len: u64,
    #[cfg(unix)]
    mode: u32,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}

fn restore_tree_snapshot(root: &Path) -> BTreeMap<std::path::PathBuf, RestoreTreeEntry> {
    fn visit(
        root: &Path,
        path: &Path,
        entries: &mut BTreeMap<std::path::PathBuf, RestoreTreeEntry>,
    ) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let relative = entry.path().strip_prefix(root).unwrap().to_path_buf();
            // Opening or locking the shared runtime/restore lock can update
            // platform-specific access metadata. Every mutable restore object
            // remains in the comparison; only this synchronization inode is
            // deliberately normalized out.
            if relative == Path::new(".node.lock") {
                continue;
            }
            let metadata = std::fs::symlink_metadata(entry.path()).unwrap();
            let kind = if metadata.file_type().is_symlink() {
                "symlink"
            } else if metadata.is_dir() {
                "directory"
            } else if metadata.is_file() {
                "file"
            } else {
                "other"
            };
            entries.insert(
                relative.clone(),
                RestoreTreeEntry {
                    kind,
                    bytes: if metadata.is_file() {
                        std::fs::read(entry.path()).unwrap()
                    } else {
                        Vec::new()
                    },
                    len: metadata.len(),
                    #[cfg(unix)]
                    mode: {
                        use std::os::unix::fs::MetadataExt;
                        metadata.mode()
                    },
                    #[cfg(unix)]
                    dev: {
                        use std::os::unix::fs::MetadataExt;
                        metadata.dev()
                    },
                    #[cfg(unix)]
                    ino: {
                        use std::os::unix::fs::MetadataExt;
                        metadata.ino()
                    },
                },
            );
            if metadata.is_dir() {
                visit(root, &entry.path(), entries);
            }
        }
    }

    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

#[test]
fn durability_mode_rejects_zero_intervals() {
    assert!(DurabilityMode::Sync.validate().is_ok());
    assert!(matches!(
        DurabilityMode::Bounded {
            max_lag: Duration::ZERO
        }
        .validate(),
        Err(DurabilityError::InvalidDuration { mode: "bounded" })
    ));
    assert!(matches!(
        DurabilityMode::Periodic {
            interval: Duration::ZERO
        }
        .validate(),
        Err(DurabilityError::InvalidDuration { mode: "periodic" })
    ));
}

#[tokio::test]
async fn coordinator_open_fails_closed_when_checkpoint_is_missing() {
    let archive_root = tempfile::tempdir().unwrap();
    let archive = checkpoint_store(archive_root.path());

    assert!(matches!(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync).await,
        Err(DurabilityError::MissingCheckpoint)
    ));
}

#[tokio::test]
async fn coordinator_open_rejects_tampered_segment_checksum_metadata() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.path().join("archive"),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store.clone(),
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            runtime_config(PathBuf::from("unused"))
                .configuration_state()
                .digest(),
            1,
        ),
    );
    archive.initialize_checkpoint().await.unwrap();
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let runtime = runtime(root.path().join("node"));
    let committed = runtime.write("request-1", "alpha", "one").unwrap();
    coordinator.note_committed(committed.applied_index);
    coordinator
        .flush_runtime(&runtime, committed.applied_index)
        .await
        .unwrap();

    let loaded = archive.load_checkpoint().await.unwrap().unwrap();
    let checksum = loaded.manifest().segments()[0].sha256();
    let replacement = LogHash::digest(&[b"different valid checksum"]).to_hex();
    assert_ne!(checksum, replacement);
    let manifest_key = archive.checkpoint_manifest_key().unwrap();
    let manifest = String::from_utf8(store.get(&manifest_key).await.unwrap()).unwrap();
    assert_eq!(manifest.matches(checksum).count(), 1);
    store
        .put(&manifest_key, manifest.replacen(checksum, &replacement, 1))
        .await
        .unwrap();

    assert!(matches!(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync).await,
        Err(DurabilityError::Archive(_))
    ));
}

#[tokio::test]
async fn sync_health_recovers_only_after_the_committed_tip_reaches_object_storage() {
    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive_backup = root.path().join("archive-backup");
    let archive = initialized_checkpoint(&archive_root).await;
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let runtime = bound_runtime(root.path().join("node"));
    let committed = runtime.write("request-1", "alpha", "one").unwrap();
    coordinator.note_committed(committed.applied_index);
    std::fs::rename(&archive_root, &archive_backup).unwrap();
    std::fs::write(&archive_root, b"archive unavailable").unwrap();

    assert!(coordinator
        .flush_runtime(&runtime, committed.applied_index)
        .await
        .is_err());
    assert_eq!(coordinator.health(), DurabilityHealth::Unavailable);
    assert_eq!(coordinator.durable_tip().index(), 0);

    std::fs::remove_file(&archive_root).unwrap();
    std::fs::rename(&archive_backup, &archive_root).unwrap();
    coordinator
        .flush_runtime(&runtime, committed.applied_index)
        .await
        .unwrap();

    assert_eq!(coordinator.health(), DurabilityHealth::Available);
    assert_eq!(coordinator.durable_tip().index(), committed.applied_index);
    assert_eq!(
        archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .tip()
            .index(),
        committed.applied_index
    );
}

#[test]
fn recorder_rehydration_restores_command_bytes_before_installing_decision_proof() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime(root.path().join("source"));
    let committed = runtime.write("request-1", "alpha", "one").unwrap();
    let membership = runtime.consensus().membership().clone();
    let recorder = RecorderFileStore::new_with_membership(
        root.path().join("fresh-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
        membership,
    )
    .unwrap();

    rehydrate_recorder_after_checkpoint(&runtime, &recorder, 0, &StartupIoContext::new()).unwrap();

    let entry = runtime
        .log_store()
        .read(committed.applied_index)
        .unwrap()
        .unwrap();
    let command = rhiza_core::StoredCommand::new(entry.entry_type, entry.payload);
    assert_eq!(
        recorder.fetch_command(command.hash()).unwrap(),
        Some(command)
    );
}

#[test]
fn cancelled_recorder_rehydration_is_joined_without_late_persistence() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let blocked = Arc::new(AtomicBool::new(false));
    let gates = [
        ("node-1", Arc::new((Mutex::new(false), Condvar::new()))),
        ("node-2", Arc::new((Mutex::new(false), Condvar::new()))),
        ("node-3", Arc::new((Mutex::new(false), Condvar::new()))),
    ];
    let (started, entered) = mpsc::sync_channel(3);
    let (finished, drained) = mpsc::sync_channel(3);
    let active = Arc::new(AtomicUsize::new(0));
    let recorders = membership
        .members()
        .iter()
        .map(|node_id| {
            let gate = gates
                .iter()
                .find(|(candidate, _)| *candidate == node_id.as_str())
                .map(|(_, gate)| Arc::clone(gate))
                .expect("every recorder has a deterministic gate");
            let recorder = RecorderFileStore::new_with_membership(
                root.path().join("recorders").join(node_id),
                node_id.clone(),
                "rhiza:sql:cluster-a",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (
                node_id.clone(),
                Box::new(BlockingRehydrateRecorder {
                    recorder_id: node_id.clone(),
                    recorder,
                    blocked: Arc::clone(&blocked),
                    started: started.clone(),
                    finished: finished.clone(),
                    active: Arc::clone(&active),
                    gate,
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            recorders,
        )
        .unwrap(),
    );
    let runtime = Arc::new(
        NodeRuntime::open(
            NodeConfig::new(
                "rhiza:sql:cluster-a",
                "node-1",
                root.path().join("node"),
                1,
                1,
                [
                    PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
                    PeerConfig::new("node-2", "http://node-2", "peer-token-2").unwrap(),
                    PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
                ],
                "client-token",
            )
            .unwrap(),
            consensus,
            &[],
        )
        .unwrap(),
    );
    let committed = runtime.write("request-1", "alpha", "one").unwrap();
    let recorder = RecorderFileStore::new_with_membership(
        root.path().join("fresh-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
        membership,
    )
    .unwrap();
    blocked.store(true, Ordering::Release);
    let startup = StartupIoContext::new();
    let probe = Arc::new(TestControlOperationProbe::default());
    let _probe_guard =
        install_test_control_operation_probe(&startup.recorder_context(), Arc::clone(&probe));
    let unrelated_probe = Arc::new(TestControlOperationProbe::default());
    let _unrelated_probe_guard = install_test_control_operation_probe(
        &RecorderRpcContext::default_timeout(),
        Arc::clone(&unrelated_probe),
    );
    let attempt_startup = startup.clone();
    let attempt_runtime = Arc::clone(&runtime);
    let attempt_recorder = recorder.clone();
    let (completion_tx, completion) = mpsc::sync_channel(1);
    let mut attempt = RehydrationAttempt::spawn(
        gates.iter().map(|(_, gate)| Arc::clone(gate)).collect(),
        move || {
            let result = rehydrate_recorder_after_checkpoint(
                &attempt_runtime,
                &attempt_recorder,
                0,
                &attempt_startup,
            );
            // The parent may already be unwinding when this finishes.  A
            // disconnected observer must not turn cleanup into a second
            // panic in this worker.
            let _ = completion_tx.send(result);
        },
    );

    let mut admitted = (0..3)
        .map(|_| entered.recv_timeout(Duration::from_secs(1)).unwrap())
        .collect::<Vec<_>>();
    admitted.sort();
    assert_eq!(
        admitted,
        vec![
            "node-1".to_owned(),
            "node-2".to_owned(),
            "node-3".to_owned(),
        ]
    );
    assert_eq!(probe.pending(), 3);
    assert_eq!(probe.outstanding(), 3);
    assert_eq!(probe.dispatch_count(), 3);
    assert_eq!(probe.observed_max_outstanding(), 3);
    startup.cancel(Instant::now() + Duration::from_secs(1));
    for (_, gate) in gates.iter().take(2) {
        release_gate(gate);
    }
    let mut first_two_drained = (0..2)
        .map(|_| drained.recv_timeout(Duration::from_secs(1)).unwrap())
        .collect::<Vec<_>>();
    first_two_drained.sort();
    assert_eq!(
        first_two_drained,
        vec!["node-1".to_owned(), "node-2".to_owned()]
    );
    assert!(
        matches!(
            completion.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "rehydration must retain the third admitted recorder inspection"
    );
    let (_, gate) = &gates[2];
    release_gate(gate);

    let error = completion
        .recv_timeout(Duration::from_secs(1))
        .expect("rehydration did not complete after the admitted inspection drained")
        .unwrap_err();
    assert!(
        error.to_string().contains("QuePaxa recorder RPC cancelled"),
        "{error}"
    );
    assert_eq!(
        drained.recv_timeout(Duration::from_secs(1)),
        Ok("node-3".to_owned())
    );
    attempt
        .join_after_completion(Duration::from_secs(1))
        .expect("rehydration worker must terminate after the final gate opens");
    assert_eq!(active.load(Ordering::Acquire), 0);
    assert_eq!(probe.pending(), 0);
    assert_eq!(probe.outstanding(), 0);
    assert!(probe.cancel_count() >= 1);
    assert_eq!(probe.quarantine_count(), 0);
    assert_eq!(probe.drained_count(), 3);
    assert_eq!(unrelated_probe.dispatch_count(), 0);
    assert_eq!(unrelated_probe.observed_max_outstanding(), 0);
    assert_eq!(unrelated_probe.outstanding(), 0);
    assert_eq!(unrelated_probe.cancel_count(), 0);
    assert_eq!(unrelated_probe.quarantine_count(), 0);
    assert_eq!(unrelated_probe.drained_count(), 0);
    let entry = runtime
        .log_store()
        .read(committed.applied_index)
        .unwrap()
        .unwrap();
    let command = rhiza_core::StoredCommand::new(entry.entry_type, entry.payload);
    assert_eq!(recorder.fetch_command(command.hash()).unwrap(), None);
    blocked.store(false, Ordering::Release);
    rehydrate_recorder_after_checkpoint(&runtime, &recorder, 0, &StartupIoContext::new()).unwrap();
    assert_eq!(
        recorder.fetch_command(command.hash()).unwrap(),
        Some(command)
    );
}

struct GateRelease {
    gates: Vec<Arc<(Mutex<bool>, Condvar)>>,
}

impl GateRelease {
    fn new(gates: Vec<Arc<(Mutex<bool>, Condvar)>>) -> Self {
        Self { gates }
    }

    fn release_all(&self) {
        for gate in &self.gates {
            let (released, changed) = &**gate;
            // A panic while a test was inspecting a gate must not prevent the
            // remaining workers from being released during unwinding.
            *released
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
            changed.notify_all();
        }
    }
}

impl Drop for GateRelease {
    fn drop(&mut self) {
        self.release_all();
    }
}

struct RehydrationAttempt {
    handle: Option<std::thread::JoinHandle<()>>,
    gates: GateRelease,
    cleanup_timeout: Duration,
    cleanup_events: Option<mpsc::Sender<CleanupEvent>>,
}

#[derive(Debug, Eq, PartialEq)]
enum RehydrationAttemptJoinError {
    TimedOut,
    Panicked,
}

#[derive(Debug, Eq, PartialEq)]
enum CleanupEvent {
    ExternalGateReleased,
    AttemptCleanup { joined: bool, detached: bool },
}

struct ObservedGateRelease {
    gates: GateRelease,
    events: mpsc::Sender<CleanupEvent>,
}

impl ObservedGateRelease {
    fn new(gates: Vec<Arc<(Mutex<bool>, Condvar)>>, events: mpsc::Sender<CleanupEvent>) -> Self {
        Self {
            gates: GateRelease::new(gates),
            events,
        }
    }
}

impl Drop for ObservedGateRelease {
    fn drop(&mut self) {
        self.gates.release_all();
        let _ = self.events.send(CleanupEvent::ExternalGateReleased);
    }
}

impl RehydrationAttempt {
    fn spawn(gates: Vec<Arc<(Mutex<bool>, Condvar)>>, run: impl FnOnce() + Send + 'static) -> Self {
        Self::spawn_with_cleanup_timeout(gates, Duration::from_millis(250), run)
    }

    fn spawn_with_cleanup_timeout(
        gates: Vec<Arc<(Mutex<bool>, Condvar)>>,
        cleanup_timeout: Duration,
        run: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            handle: Some(std::thread::spawn(run)),
            gates: GateRelease::new(gates),
            cleanup_timeout,
            cleanup_events: None,
        }
    }

    fn with_cleanup_events(mut self, events: mpsc::Sender<CleanupEvent>) -> Self {
        self.cleanup_events = Some(events);
        self
    }

    fn join_after_completion(
        &mut self,
        timeout: Duration,
    ) -> Result<(), RehydrationAttemptJoinError> {
        // Take ownership before waiting.  On timeout this deliberately drops
        // the JoinHandle, so Drop cannot perform a second, potentially
        // unbounded join while an assertion is unwinding.
        let handle = self
            .handle
            .take()
            .ok_or(RehydrationAttemptJoinError::TimedOut)?;
        let deadline = Instant::now() + timeout;
        while !handle.is_finished() {
            if Instant::now() >= deadline {
                drop(handle);
                return Err(RehydrationAttemptJoinError::TimedOut);
            }
            std::thread::yield_now();
        }
        handle
            .join()
            .map_err(|_| RehydrationAttemptJoinError::Panicked)
    }
}

impl Drop for RehydrationAttempt {
    fn drop(&mut self) {
        // Cleanup must open every backend gate before it waits for the caller
        // thread; this path runs during assertion unwinding as well.  Never
        // block or panic indefinitely from Drop: a non-cooperative external
        // dependency is detached after the bounded grace period.
        self.gates.release_all();
        let mut joined = false;
        let mut detached = self.handle.is_none();
        if let Some(handle) = self.handle.take() {
            let deadline = Instant::now() + self.cleanup_timeout;
            while !handle.is_finished() && Instant::now() < deadline {
                std::thread::yield_now();
            }
            if handle.is_finished() {
                let _ = handle.join();
                joined = true;
            } else {
                drop(handle);
                detached = true;
            }
        }
        if let Some(events) = &self.cleanup_events {
            let _ = events.send(CleanupEvent::AttemptCleanup { joined, detached });
        }
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

fn release_gate(gate: &Arc<(Mutex<bool>, Condvar)>) {
    let (released, changed) = &**gate;
    *released
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
    changed.notify_all();
}

#[test]
fn rehydration_attempt_unwind_releases_all_gates_and_returns_bounded() {
    let gates = [
        Arc::new((Mutex::new(false), Condvar::new())),
        Arc::new((Mutex::new(false), Condvar::new())),
        Arc::new((Mutex::new(false), Condvar::new())),
    ];
    let worker_gates = gates.clone();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (exited_tx, exited_rx) = mpsc::sync_channel(1);
    let started = Instant::now();

    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _attempt = RehydrationAttempt::spawn(worker_gates.to_vec(), move || {
            entered_tx.send(()).unwrap();
            for gate in &worker_gates {
                await_gate(gate);
            }
            exited_tx.send(()).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        panic!("exercise assertion unwinding through RehydrationAttempt::drop");
    }));

    assert!(unwound.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "unwinding cleanup exceeded its bounded ceiling"
    );
    for gate in &gates {
        assert!(
            *gate
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            "Drop must release every registered gate"
        );
    }
    assert_eq!(exited_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
}

#[test]
fn rehydration_attempt_drop_and_timeout_detach_stubborn_workers_then_recover_out_of_band() {
    // Keep the attempt binding before the external guard.  Should this scope
    // unwind before the explicit release below, reverse-drop opens the
    // external dependency before attempt cleanup starts polling its worker.
    let attempt: RehydrationAttempt;
    let registered_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let external_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let _external_gate_release = GateRelease::new(vec![Arc::clone(&external_gate)]);
    let worker_registered_gate = Arc::clone(&registered_gate);
    let worker_external_gate = Arc::clone(&external_gate);
    let active = Arc::new(AtomicUsize::new(0));
    let worker_active = Arc::clone(&active);
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (external_wait_tx, external_wait_rx) = mpsc::sync_channel(1);
    let (exited_tx, exited_rx) = mpsc::sync_channel(1);
    attempt = RehydrationAttempt::spawn_with_cleanup_timeout(
        vec![registered_gate],
        Duration::from_millis(50),
        move || {
            worker_active.fetch_add(1, Ordering::AcqRel);
            entered_tx.send(()).unwrap();
            await_gate(&worker_registered_gate);
            external_wait_tx.send(()).unwrap();
            await_gate(&worker_external_gate);
            worker_active.fetch_sub(1, Ordering::AcqRel);
            exited_tx.send(()).unwrap();
        },
    );
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    // Drop itself must release the owned gate, see the worker reach its
    // unowned stubborn dependency, and detach rather than joining forever.
    let drop_started = Instant::now();
    drop(attempt);
    assert!(
        drop_started.elapsed() < Duration::from_secs(2),
        "Drop waited forever on an unowned stubborn dependency"
    );
    external_wait_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("registered gate was not released by Drop");
    assert_eq!(active.load(Ordering::Acquire), 1);

    release_gate(&external_gate);
    assert_eq!(exited_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
    assert_eq!(active.load(Ordering::Acquire), 0);

    // Exercise the explicit bounded join path too.  It takes the handle
    // before waiting, so the following Drop must not retry the timed-out
    // thread; the out-of-band completion proves it can still drain cleanly.
    {
        // This is a separate scope so its external guard has the same
        // reverse-drop relationship to its pre-declared attempt binding.
        let mut attempt: RehydrationAttempt;
        let registered_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let external_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _external_gate_release = GateRelease::new(vec![Arc::clone(&external_gate)]);
        let worker_registered_gate = Arc::clone(&registered_gate);
        let worker_external_gate = Arc::clone(&external_gate);
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (external_wait_tx, external_wait_rx) = mpsc::sync_channel(1);
        let (exited_tx, exited_rx) = mpsc::sync_channel(1);
        attempt = RehydrationAttempt::spawn(vec![Arc::clone(&registered_gate)], move || {
            entered_tx.send(()).unwrap();
            await_gate(&worker_registered_gate);
            external_wait_tx.send(()).unwrap();
            await_gate(&worker_external_gate);
            exited_tx.send(()).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_gate(&registered_gate);
        external_wait_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker did not reach its external stubborn dependency");

        let timeout_started = Instant::now();
        assert_eq!(
            attempt.join_after_completion(Duration::from_millis(50)),
            Err(RehydrationAttemptJoinError::TimedOut)
        );
        assert!(
            timeout_started.elapsed() < Duration::from_secs(2),
            "bounded completion timeout did not return promptly"
        );
        let detached_drop_started = Instant::now();
        drop(attempt);
        assert!(
            detached_drop_started.elapsed() < Duration::from_secs(2),
            "Drop retried the handle consumed by the timeout path"
        );
        release_gate(&external_gate);
        assert_eq!(exited_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
    }
}

fn assert_stubborn_stage_unwind_releases_external_gate_before_attempt_cleanup(stage: &str) {
    let active = Arc::new(AtomicUsize::new(0));
    let worker_active = Arc::clone(&active);
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (external_wait_tx, external_wait_rx) = mpsc::sync_channel(1);
    let (exited_tx, exited_rx) = mpsc::sync_channel(1);
    let (events_tx, events_rx) = mpsc::channel();
    let started = Instant::now();

    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // This declaration intentionally precedes the external gate guard.
        // Locals drop in reverse declaration order, so the guard opens the
        // external dependency before RehydrationAttempt starts its own bounded
        // cleanup poll.
        let _attempt: RehydrationAttempt;
        let registered_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let external_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _external_gate_release =
            ObservedGateRelease::new(vec![Arc::clone(&external_gate)], events_tx.clone());
        let worker_registered_gate = Arc::clone(&registered_gate);
        let worker_external_gate = Arc::clone(&external_gate);
        _attempt = RehydrationAttempt::spawn_with_cleanup_timeout(
            vec![registered_gate],
            Duration::from_millis(50),
            move || {
                worker_active.fetch_add(1, Ordering::AcqRel);
                entered_tx.send(()).unwrap();
                await_gate(&worker_registered_gate);
                external_wait_tx.send(()).unwrap();
                await_gate(&worker_external_gate);
                worker_active.fetch_sub(1, Ordering::AcqRel);
                exited_tx.send(()).unwrap();
            },
        )
        .with_cleanup_events(events_tx.clone());
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        panic!("exercise {stage} external gate RAII before manual release");
    }));

    assert!(unwound.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "unwinding with an external dependency exceeded its bounded ceiling"
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)),
        Ok(CleanupEvent::ExternalGateReleased),
        "external gate release must precede attempt cleanup"
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)),
        Ok(CleanupEvent::AttemptCleanup {
            joined: true,
            detached: false,
        }),
        "attempt must join after the external RAII release rather than detach"
    );
    external_wait_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("attempt cleanup did not release its registered gate");
    assert_eq!(exited_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
    assert_eq!(active.load(Ordering::Acquire), 0);
}

#[test]
fn rehydration_attempt_stubborn_stages_release_external_gate_before_cleanup() {
    assert_stubborn_stage_unwind_releases_external_gate_before_attempt_cleanup("drop stage");
    assert_stubborn_stage_unwind_releases_external_gate_before_attempt_cleanup("timeout stage");
}

#[test]
fn rehydration_attempt_receiver_disconnect_is_bounded_and_does_not_panic_worker() {
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let worker_gate = Arc::clone(&gate);
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (completion_tx, completion_rx) = mpsc::sync_channel::<()>(1);
    let (exited_tx, exited_rx) = mpsc::sync_channel(1);
    drop(completion_rx);
    let attempt = RehydrationAttempt::spawn(vec![gate], move || {
        entered_tx.send(()).unwrap();
        await_gate(&worker_gate);
        assert!(
            completion_tx.send(()).is_err(),
            "the completion observer is intentionally gone"
        );
        exited_tx.send(()).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let started = Instant::now();
    drop(attempt);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "receiver-disconnect cleanup exceeded its bounded ceiling"
    );
    assert_eq!(exited_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
}

struct BlockingRehydrateRecorder {
    recorder_id: String,
    recorder: RecorderFileStore,
    blocked: Arc<AtomicBool>,
    started: mpsc::SyncSender<String>,
    finished: mpsc::SyncSender<String>,
    active: Arc<AtomicUsize>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}

impl RecorderRpc for BlockingRehydrateRecorder {
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        self.recorder.recorder_id()
    }

    fn record(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
        self.recorder.record(request)
    }

    fn install_decision_proof(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.recorder.install_decision_proof(proof, membership)
    }

    fn inspect_decision_proof(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        self.recorder.inspect_decision_proof(slot)
    }

    fn inspect_record_summary(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        if self.blocked.load(Ordering::Acquire) {
            self.active.fetch_add(1, Ordering::AcqRel);
            self.started.send(self.recorder_id.clone()).unwrap();
            let (released, changed) = &*self.gate;
            let mut released = released.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
            self.active.fetch_sub(1, Ordering::AcqRel);
            self.finished.send(self.recorder_id.clone()).unwrap();
        }
        self.recorder.inspect_record_summary(slot)
    }

    fn supports_context_read_fence(&self) -> bool {
        self.recorder.supports_context_read_fence()
    }

    fn observe_read_fence(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<ReadFenceObservation> {
        RecorderRpc::observe_read_fence(&self.recorder, context, request)
    }

    fn store_command_for(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: rhiza_core::StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
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
        _context: &rhiza_quepaxa::RecorderRpcContext,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<rhiza_core::StoredCommand>> {
        self.recorder
            .fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash)
    }

    fn stage_effect_bundle_chunk(
        &self,
        context: &RecorderRpcContext,
        binding: EffectBundleBinding,
        manifest_command: rhiza_core::StoredCommand,
        ordinal: u16,
        chunk: Vec<u8>,
    ) -> rhiza_quepaxa::Result<()> {
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
        context: &RecorderRpcContext,
        binding: EffectBundleBinding,
        manifest_command: rhiza_core::StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        RecorderRpc::finalize_staged_effect_bundle(
            &self.recorder,
            context,
            binding,
            manifest_command,
        )
    }

    fn fetch_effect_bundle_manifest(
        &self,
        context: &RecorderRpcContext,
        binding: EffectBundleBinding,
    ) -> rhiza_quepaxa::Result<Option<rhiza_core::StoredCommand>> {
        RecorderRpc::fetch_effect_bundle_manifest(&self.recorder, context, binding)
    }

    fn fetch_effect_bundle_chunk(
        &self,
        context: &RecorderRpcContext,
        binding: EffectBundleBinding,
        ordinal: u16,
    ) -> rhiza_quepaxa::Result<Option<Vec<u8>>> {
        RecorderRpc::fetch_effect_bundle_chunk(&self.recorder, context, binding, ordinal)
    }
}

#[tokio::test]
async fn bounded_mode_blocks_after_lag_limit_and_flush_unblocks_writes() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(root.path()).await;
    let coordinator = CheckpointCoordinator::open(
        archive,
        DurabilityMode::Bounded {
            max_lag: Duration::from_millis(10),
        },
    )
    .await
    .unwrap();
    let runtime = runtime(root.path().join("node"));
    let committed = runtime.write("request-1", "alpha", "one").unwrap();

    coordinator.note_committed(committed.applied_index);
    assert!(coordinator.write_allowed().is_ok());
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(matches!(
        coordinator.write_allowed(),
        Err(DurabilityError::LagExceeded {
            committed_index: 1,
            durable_index: 0,
            ..
        })
    ));

    let tip = coordinator
        .flush_runtime(&runtime, committed.applied_index)
        .await
        .unwrap();
    assert_eq!(tip.index(), 1);
    assert!(coordinator.write_allowed().is_ok());
}

#[tokio::test]
async fn flush_resumes_after_anchor_when_checkpoint_is_durable_through_snapshot_tip() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("checkpoint")).await;
    let coordinator = CheckpointCoordinator::open(archive, DurabilityMode::Sync)
        .await
        .unwrap();
    let runtime = runtime(root.path().join("node"));
    let first = runtime.write("request-1", "alpha", "one").unwrap();
    coordinator
        .flush_runtime(&runtime, first.applied_index)
        .await
        .unwrap();

    let snapshot = runtime.create_recovery_snapshot().unwrap();
    let snapshot_store = ObjStore::new(ObjStoreConfig::Local {
        root: root.path().join("snapshots"),
    })
    .unwrap();
    let publication =
        ObjectArchiveStore::new_for_single_process(snapshot_store, "rhiza:sql:cluster-a")
            .publish_snapshot(snapshot.snapshot())
            .await
            .unwrap();
    let verified = runtime
        .verify_snapshot_publication(&snapshot, &publication)
        .unwrap();
    runtime.compact_log(&verified).unwrap();

    let second = runtime.write("request-2", "beta", "two").unwrap();
    assert_eq!(
        coordinator
            .flush_runtime(&runtime, second.applied_index)
            .await
            .unwrap()
            .index(),
        second.applied_index
    );
}

#[tokio::test]
async fn flush_fails_with_snapshot_requirement_when_checkpoint_is_below_anchor() {
    let root = tempfile::tempdir().unwrap();
    let coordinator = CheckpointCoordinator::open(
        initialized_checkpoint(&root.path().join("checkpoint")).await,
        DurabilityMode::Sync,
    )
    .await
    .unwrap();
    let runtime = runtime(root.path().join("node"));
    runtime.write("request-1", "alpha", "one").unwrap();
    let snapshot = runtime.create_recovery_snapshot().unwrap();
    let snapshot_store = ObjStore::new(ObjStoreConfig::Local {
        root: root.path().join("snapshots"),
    })
    .unwrap();
    let publication =
        ObjectArchiveStore::new_for_single_process(snapshot_store, "rhiza:sql:cluster-a")
            .publish_snapshot(snapshot.snapshot())
            .await
            .unwrap();
    let verified = runtime
        .verify_snapshot_publication(&snapshot, &publication)
        .unwrap();
    runtime.compact_log(&verified).unwrap();

    assert!(matches!(
        coordinator.flush_runtime(&runtime, u64::MAX).await,
        Err(DurabilityError::SnapshotRequired { anchor }) if *anchor == *snapshot.anchor()
    ));
}

#[tokio::test]
async fn bounded_mode_blocks_recovered_lag_immediately_but_gives_new_commits_the_window() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(root.path()).await;
    let coordinator = CheckpointCoordinator::open(
        archive,
        DurabilityMode::Bounded {
            max_lag: Duration::from_secs(1),
        },
    )
    .await
    .unwrap();
    let runtime = runtime(root.path().join("node"));
    let recovered = runtime.write("request-1", "alpha", "one").unwrap();

    coordinator.note_recovered_committed(recovered.applied_index);
    assert!(matches!(
        coordinator.write_allowed(),
        Err(DurabilityError::LagExceeded {
            committed_index: 1,
            durable_index: 0,
            ..
        })
    ));
    coordinator
        .flush_runtime(&runtime, recovered.applied_index)
        .await
        .unwrap();
    assert!(coordinator.write_allowed().is_ok());

    let fresh = runtime.write("request-2", "beta", "two").unwrap();
    coordinator.note_committed(fresh.applied_index);
    assert!(coordinator.write_allowed().is_ok());
}

#[tokio::test]
async fn concurrent_flushes_are_serialized_idempotent_and_clamped_to_local_qlog() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(root.path()).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let runtime = Arc::new(runtime(root.path().join("node")));
    for index in 1..=6 {
        let committed = runtime
            .write(
                &format!("request-{index}"),
                &format!("key-{index}"),
                &format!("value-{index}"),
            )
            .unwrap();
        coordinator.note_committed(committed.applied_index);
    }

    let (first, second) = tokio::join!(
        coordinator.flush_runtime(&runtime, 4),
        coordinator.flush_runtime(&runtime, u64::MAX)
    );
    first.unwrap();
    second.unwrap();
    coordinator.flush_runtime(&runtime, u64::MAX).await.unwrap();

    assert_eq!(coordinator.durable_tip().index(), 6);
    assert_eq!(load_restored_for_test(&archive).await.suffix().len(), 6);
    assert_eq!(
        archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .tip()
            .index(),
        6
    );
}

#[tokio::test]
async fn periodic_background_flushes_in_bounded_batches() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(root.path()).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(
            archive.clone(),
            DurabilityMode::Periodic {
                interval: Duration::from_millis(5),
            },
        )
        .await
        .unwrap(),
    );
    let runtime = Arc::new(runtime(root.path().join("node")));
    for index in 1..=40 {
        let committed = runtime
            .write(
                &format!("request-{index}"),
                &format!("key-{index}"),
                "value",
            )
            .unwrap();
        coordinator.note_committed(committed.applied_index);
    }

    coordinator
        .clone()
        .run_background(runtime, tokio::time::sleep(Duration::from_millis(40)))
        .await
        .unwrap();

    let loaded = archive.load_checkpoint().await.unwrap().unwrap();
    assert_eq!(loaded.manifest().tip().index(), 40);
    assert!(loaded.manifest().segments().len() > 1);

    let restored_dir = root.path().join("restored-batches");
    let tip = install_fresh_for_test(&archive, &restored_dir, "node-1")
        .await
        .unwrap();
    assert_eq!(tip.index(), 40);
    let restored_log = FileLogStore::open(
        restored_dir.join("consensus/log"),
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    assert_eq!(restored_log.last_index().unwrap(), Some(40));
}

#[tokio::test]
async fn background_checkpoint_recovers_after_transient_storage_failure() {
    for (name, mode) in [
        (
            "periodic",
            DurabilityMode::Periodic {
                interval: Duration::from_millis(5),
            },
        ),
        (
            "bounded",
            DurabilityMode::Bounded {
                max_lag: Duration::from_millis(20),
            },
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let archive_root = root.path().join(format!("{name}-archive"));
        let archive = initialized_checkpoint(&archive_root).await;
        let coordinator = Arc::new(
            CheckpointCoordinator::open(archive.clone(), mode)
                .await
                .unwrap(),
        );
        let runtime = Arc::new(runtime(root.path().join(format!("{name}-node"))));
        let committed = runtime.write("request-1", "alpha", "one").unwrap();
        coordinator.note_committed(committed.applied_index);
        let archive_backup = root.path().join(format!("{name}-archive-backup"));
        std::fs::rename(&archive_root, &archive_backup).unwrap();
        std::fs::write(&archive_root, b"archive unavailable").unwrap();

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(coordinator.clone().run_background(runtime, async move {
            if !*shutdown_rx.borrow() {
                let _ = shutdown_rx.changed().await;
            }
        }));
        tokio::time::timeout(Duration::from_secs(1), async {
            while coordinator.health() != DurabilityHealth::Unavailable {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        std::fs::remove_file(&archive_root).unwrap();
        std::fs::rename(&archive_backup, &archive_root).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while coordinator.durable_tip().index() < committed.applied_index {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(coordinator.health(), DurabilityHealth::Available);
        assert!(coordinator.write_allowed().is_ok());
        assert_eq!(
            archive
                .load_checkpoint()
                .await
                .unwrap()
                .unwrap()
                .manifest()
                .tip()
                .index(),
            committed.applied_index
        );
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn bounded_background_flushes_at_half_lag_and_sync_only_compacts() {
    let root = tempfile::tempdir().unwrap();
    let bounded_archive = initialized_checkpoint(&root.path().join("bounded-archive")).await;
    let bounded = Arc::new(
        CheckpointCoordinator::open(
            bounded_archive.clone(),
            DurabilityMode::Bounded {
                max_lag: Duration::from_millis(20),
            },
        )
        .await
        .unwrap(),
    );
    let bounded_runtime = Arc::new(runtime(root.path().join("bounded-node")));
    let committed = bounded_runtime.write("request-1", "alpha", "one").unwrap();
    bounded.note_committed(committed.applied_index);
    let bounded_shutdown = {
        let bounded = bounded.clone();
        async move {
            while bounded.durable_tip().index() < committed.applied_index {
                tokio::task::yield_now().await;
            }
        }
    };
    tokio::time::timeout(
        Duration::from_secs(1),
        bounded.run_background(bounded_runtime, bounded_shutdown),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        bounded_archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .tip()
            .index(),
        1
    );

    let sync_archive = initialized_checkpoint(&root.path().join("sync-archive")).await;
    let sync = Arc::new(
        CheckpointCoordinator::open(sync_archive.clone(), DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let sync_runtime = Arc::new(runtime(root.path().join("sync-node")));
    let committed = sync_runtime.write("request-1", "alpha", "one").unwrap();
    sync.note_committed(committed.applied_index);
    sync.run_background(sync_runtime, tokio::time::sleep(Duration::from_millis(10)))
        .await
        .unwrap();
    assert_eq!(
        sync_archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .tip()
            .index(),
        0
    );
}

#[tokio::test]
async fn sync_background_compacts_at_the_publisher_segment_limit() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("sync-compact-archive")).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open_with_holder_and_options(
            archive.clone(),
            DurabilityMode::Sync,
            "sync-compact",
            CheckpointPublisherOptions::default().with_compaction_segment_limit(2),
        )
        .await
        .unwrap(),
    );
    let runtime = Arc::new(runtime(root.path().join("sync-compact-node")));
    for index in 1..=2 {
        let committed = runtime
            .write(
                &format!("request-{index}"),
                &format!("key-{index}"),
                "value",
            )
            .unwrap();
        coordinator.note_committed(committed.applied_index);
        coordinator
            .flush_runtime(&runtime, committed.applied_index)
            .await
            .unwrap();
    }
    assert_eq!(
        archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .segments()
            .len(),
        2
    );

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let worker = tokio::spawn(coordinator.run_background(runtime, async move {
        if !*shutdown_rx.borrow() {
            let _ = shutdown_rx.changed().await;
        }
    }));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if archive
                .load_checkpoint()
                .await
                .unwrap()
                .unwrap()
                .manifest()
                .segments()
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    shutdown_tx.send(true).unwrap();
    worker.await.unwrap().unwrap();
}

#[tokio::test]
async fn restore_requires_an_existing_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let archive = checkpoint_store(&root.path().join("archive"));
    let data_dir = root.path().join("data");

    assert!(matches!(
        prepare_checkpoint_restore(&archive).await,
        Err(DurabilityError::MissingCheckpoint)
    ));
    assert!(!data_dir.exists());
}

#[tokio::test]
async fn prepared_fresh_install_rejects_hostile_inputs_without_local_mutation() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let prepared = prepare_checkpoint_restore(&archive).await.unwrap();
    let untouched = root.path().join("untouched");

    assert!(matches!(
        expected_restore_state_for_test(
            &prepared,
            &untouched,
            "",
            CheckpointInstallMode::Fresh,
            None,
        ),
        Err(DurabilityError::SnapshotVerification(_))
    ));
    assert!(!untouched.exists());

    for name in [
        "../escape",
        "/absolute",
        "C:relative",
        "C:/absolute",
        r"C:\\absolute",
        r"\\\\server\\share",
        r"\\rooted",
        "back\\slash",
        "nul\0name",
        ".rhiza-restore.json",
        ".successor-restore.lock",
        ".successor-restore.intent",
        ".successor-restore.complete",
        ".successor-prestage.lock",
        ".successor-prestage.intent",
        ".successor-prestage.ready",
        ".successor-prestage.published",
        ".successor-prestage.finalized",
        ".restore-marker-tmp-1-1",
        "sqlite",
        "ladybug",
        "kv",
        "consensus",
        ".restore-stage-1-1",
        ".rebuildable-quarantine-1-1",
        ".RhIzA-ReStOrE.JsOn",
        ".SuCcEsSoR-ReStOrE.LoCk",
        ".SuCcEsSoR-PrEsTaGe.FiNaLiZeD",
        ".RhIzA-ReCoVeRy-OwNeR.JsOn",
        "SqLiTe",
        "LaDyBuG",
        "CoNsEnSuS",
        ".ReStOrE-StAgE-1-1",
        ".ReStOrE-MaRkEr-TmP-1-1",
        ".ReBuIlDaBlE-QuArAnTiNe-1-1",
    ] {
        assert!(
            RestoreCompletionMarker::new(name, b"marker").is_err(),
            "{name:?}"
        );
        assert!(!untouched.exists());
    }

    let regular_file = root.path().join("not-a-directory");
    std::fs::write(&regular_file, b"sentinel").unwrap();
    assert!(matches!(
        expected_restore_state_for_test(
            &prepared,
            &regular_file,
            "node-1",
            CheckpointInstallMode::Fresh,
            None,
        ),
        Err(DurabilityError::DataDirNotFresh(_))
    ));
    assert_eq!(std::fs::read(&regular_file).unwrap(), b"sentinel");

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let symlink_target = root.path().join("symlink-target");
        std::fs::create_dir(&symlink_target).unwrap();
        let symlink_path = root.path().join("symlink-data");
        symlink(&symlink_target, &symlink_path).unwrap();
        assert!(matches!(
            expected_restore_state_for_test(
                &prepared,
                &symlink_path,
                "node-1",
                CheckpointInstallMode::Fresh,
                None,
            ),
            Err(DurabilityError::DataDirNotFresh(_))
        ));
        assert!(std::fs::symlink_metadata(&symlink_path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(std::fs::read_dir(&symlink_target).unwrap().next().is_none());
    }
}

#[tokio::test]
async fn prepared_rejoin_installer_preserves_recorder_bytes_across_retry() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let prepared = prepare_checkpoint_restore(&archive).await.unwrap();
    let data_dir = root.path().join("data");
    install_prepared_checkpoint_to_fresh_data_dir(
        &prepared,
        expected_restore_state_for_test(
            &prepared,
            &data_dir,
            "node-1",
            CheckpointInstallMode::Fresh,
            None,
        )
        .unwrap(),
        None,
    )
    .unwrap();
    let recorder = data_dir.join("recorder/sentinel");
    std::fs::create_dir_all(recorder.parent().unwrap()).unwrap();
    std::fs::write(&recorder, b"keep-recorder-bytes").unwrap();

    for _ in 0..2 {
        install_prepared_checkpoint_for_rejoin_preserving_recorder(
            &prepared,
            expected_restore_state_for_test(
                &prepared,
                &data_dir,
                "node-1",
                CheckpointInstallMode::RejoinPreservingRecorder,
                Some("identity.json"),
            )
            .unwrap(),
            RestoreCompletionMarker::new("identity.json", b"identity").unwrap(),
        )
        .unwrap();
        assert_eq!(std::fs::read(&recorder).unwrap(), b"keep-recorder-bytes");
        assert_eq!(
            std::fs::read(data_dir.join("identity.json")).unwrap(),
            b"identity"
        );
        assert!(!data_dir.join(".rhiza-restore.json").exists());
    }
}

#[tokio::test]
async fn stale_rejoin_restore_cannot_mutate_a_newer_installed_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let source = runtime(root.path().join("source"));

    let r10 = source.write("request-r10", "key-r10", "value-r10").unwrap();
    coordinator.note_committed(r10.applied_index);
    coordinator
        .flush_runtime(&source, r10.applied_index)
        .await
        .unwrap();
    let prepared_a = prepare_checkpoint_restore(&archive).await.unwrap();

    let target = root.path().join("target");
    install_prepared_checkpoint_to_fresh_data_dir(
        &prepared_a,
        expected_restore_state_for_test(
            &prepared_a,
            &target,
            "node-1",
            CheckpointInstallMode::Fresh,
            Some("identity.json"),
        )
        .unwrap(),
        Some(RestoreCompletionMarker::new("identity.json", b"r10").unwrap()),
    )
    .unwrap();
    let recorder = target.join("recorder/sentinel");
    std::fs::create_dir_all(recorder.parent().unwrap()).unwrap();
    std::fs::write(&recorder, b"must-survive").unwrap();

    // A captures R10's exact local epoch before waiting on the remote archive.
    let expected_a = expected_restore_state_for_test(
        &prepared_a,
        &target,
        "node-1",
        CheckpointInstallMode::RejoinPreservingRecorder,
        Some("identity.json"),
    )
    .unwrap();

    let r11 = source.write("request-r11", "key-r11", "value-r11").unwrap();
    coordinator.note_committed(r11.applied_index);
    coordinator
        .flush_runtime(&source, r11.applied_index)
        .await
        .unwrap();
    let prepared_b = prepare_checkpoint_restore(&archive).await.unwrap();
    assert_eq!(prepared_a.restored().tip().index(), r10.applied_index);
    assert_eq!(prepared_b.restored().tip().index(), r11.applied_index);

    let expected_b = expected_restore_state_for_test(
        &prepared_b,
        &target,
        "node-1",
        CheckpointInstallMode::RejoinPreservingRecorder,
        Some("identity.json"),
    )
    .unwrap();
    install_prepared_checkpoint_for_rejoin_preserving_recorder(
        &prepared_b,
        expected_b,
        RestoreCompletionMarker::new("identity.json", b"r11").unwrap(),
    )
    .unwrap();

    let before_a = restore_tree_snapshot(&target);
    let recorder_before = std::fs::read(&recorder).unwrap();
    let stale = install_prepared_checkpoint_for_rejoin_preserving_recorder(
        &prepared_a,
        expected_a,
        RestoreCompletionMarker::new("identity.json", b"r10").unwrap(),
    )
    .unwrap_err();
    assert!(
        matches!(&stale, DurabilityError::SnapshotVerification(message) if message.contains("changed after expected state capture")),
        "stale restore must fail in token revalidation before any restore mutation: {stale}"
    );
    assert_eq!(restore_tree_snapshot(&target), before_a);
    assert_eq!(std::fs::read(&recorder).unwrap(), recorder_before);
    assert_eq!(std::fs::read(target.join("identity.json")).unwrap(), b"r11");
    assert!(target.join(".rhiza-checkpoint-install.json").is_file());
    assert!(!target.join(".rhiza-restore.json").exists());
    // The B install may retain its own crash-retriable quarantine. Equality
    // with the pre-A tree proves A added no staging/quarantine artifact.
}

#[tokio::test]
async fn runtime_qlog_advance_after_capture_rejects_restore_without_mutation() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let prepared = prepare_checkpoint_restore(&archive).await.unwrap();
    let target = root.path().join("target");
    install_prepared_checkpoint_to_fresh_data_dir(
        &prepared,
        expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::Fresh,
            Some("identity.json"),
        )
        .unwrap(),
        Some(RestoreCompletionMarker::new("identity.json", b"identity").unwrap()),
    )
    .unwrap();
    let expected = expected_restore_state_for_test(
        &prepared,
        &target,
        "node-1",
        CheckpointInstallMode::RejoinPreservingRecorder,
        Some("identity.json"),
    )
    .unwrap();

    let runtime = runtime(&target);
    runtime.write("runtime-advance", "key", "value").unwrap();
    drop(runtime);

    let before = restore_tree_snapshot(&target);
    let error = install_prepared_checkpoint_for_rejoin_preserving_recorder(
        &prepared,
        expected,
        RestoreCompletionMarker::new("identity.json", b"identity").unwrap(),
    )
    .unwrap_err();
    assert!(
        matches!(&error, DurabilityError::SnapshotVerification(message) if message.contains("local qlog state changed after expected state capture")),
        "qlog advance must fail before restore mutation: {error}"
    );
    assert_eq!(restore_tree_snapshot(&target), before);
}

#[tokio::test]
async fn durable_fresh_receipt_retries_exactly_and_finalizes_pending_intent_without_reinstall() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let prepared = prepare_checkpoint_restore(&archive).await.unwrap();
    let target = root.path().join("target");
    let marker = RestoreCompletionMarker::new("identity.json", b"identity").unwrap();
    let tip = install_prepared_checkpoint_to_fresh_data_dir(
        &prepared,
        expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::Fresh,
            Some("identity.json"),
        )
        .unwrap(),
        Some(marker),
    )
    .unwrap();

    let committed = restore_tree_snapshot(&target);
    assert_eq!(
        install_prepared_checkpoint_to_fresh_data_dir(
            &prepared,
            expected_restore_state_for_test(
                &prepared,
                &target,
                "node-1",
                CheckpointInstallMode::Fresh,
                Some("identity.json"),
            )
            .unwrap(),
            Some(RestoreCompletionMarker::new("identity.json", b"identity").unwrap()),
        )
        .unwrap(),
        tip
    );
    assert_eq!(restore_tree_snapshot(&target), committed);

    // A valid Fresh receipt is explicitly idempotent only for the exact
    // prepared checkpoint. A newer remote checkpoint cannot repurpose this
    // token to overwrite the completed fresh data root.
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let source = runtime(root.path().join("source"));
    let newer_entry = source.write("newer", "key", "value").unwrap();
    coordinator.note_committed(newer_entry.applied_index);
    coordinator
        .flush_runtime(&source, newer_entry.applied_index)
        .await
        .unwrap();
    let newer = prepare_checkpoint_restore(&archive).await.unwrap();
    let before_mismatch = restore_tree_snapshot(&target);
    let error = install_prepared_checkpoint_to_fresh_data_dir(
        &newer,
        expected_restore_state_for_test(
            &newer,
            &target,
            "node-1",
            CheckpointInstallMode::Fresh,
            Some("identity.json"),
        )
        .unwrap(),
        Some(RestoreCompletionMarker::new("identity.json", b"identity").unwrap()),
    )
    .unwrap_err();
    assert!(
        matches!(&error, DurabilityError::SnapshotVerification(message) if message.contains("receipt does not match"))
    );
    assert_eq!(restore_tree_snapshot(&target), before_mismatch);

    // Model a crash after durable receipt publication and before generic
    // intent cleanup. An exact retry is permitted to remove only the intent;
    // it must not stage, quarantine, or rewrite the completed checkpoint.
    let intent = rhiza_node::durability::checkpoint_restore_intent_bytes(
        prepared.identity(),
        "node-1",
        prepared.execution_profile(),
        prepared.checkpoint_root(),
    )
    .unwrap();
    std::fs::write(target.join(".rhiza-restore.json"), intent).unwrap();
    let mut expected_after_finalize = committed.clone();
    assert!(expected_after_finalize
        .remove(&std::path::PathBuf::from(".rhiza-restore.json"))
        .is_none());
    assert_eq!(
        install_prepared_checkpoint_to_fresh_data_dir(
            &prepared,
            expected_restore_state_for_test(
                &prepared,
                &target,
                "node-1",
                CheckpointInstallMode::Fresh,
                Some("identity.json"),
            )
            .unwrap(),
            Some(RestoreCompletionMarker::new("identity.json", b"identity").unwrap()),
        )
        .unwrap(),
        tip
    );
    assert!(!target.join(".rhiza-restore.json").exists());
    assert_eq!(restore_tree_snapshot(&target), expected_after_finalize);
}

#[tokio::test]
async fn live_runtime_lock_rejects_installer_without_restore_mutation() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let prepared = prepare_checkpoint_restore(&archive).await.unwrap();
    let target = root.path().join("target");
    install_prepared_checkpoint_to_fresh_data_dir(
        &prepared,
        expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::Fresh,
            Some("identity.json"),
        )
        .unwrap(),
        Some(RestoreCompletionMarker::new("identity.json", b"identity").unwrap()),
    )
    .unwrap();
    let expected = expected_restore_state_for_test(
        &prepared,
        &target,
        "node-1",
        CheckpointInstallMode::RejoinPreservingRecorder,
        Some("identity.json"),
    )
    .unwrap();
    let runtime = runtime(&target);
    let before = restore_tree_snapshot(&target);
    let error = install_prepared_checkpoint_for_rejoin_preserving_recorder(
        &prepared,
        expected,
        RestoreCompletionMarker::new("identity.json", b"identity").unwrap(),
    )
    .unwrap_err();
    assert!(
        matches!(&error, DurabilityError::SnapshotVerification(message) if message.contains("data-root restore lock")),
        "live runtime lock must fence installer: {error}"
    );
    assert_eq!(restore_tree_snapshot(&target), before);
    drop(runtime);
}

#[tokio::test]
async fn expected_restore_capture_rejects_hostile_node_lock_without_mutation() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let prepared = prepare_checkpoint_restore(&archive).await.unwrap();
    for kind in ["nonzero-file", "directory"] {
        let target = root.path().join(kind);
        std::fs::create_dir(&target).unwrap();
        let lock = target.join(".node.lock");
        match kind {
            "nonzero-file" => std::fs::write(&lock, b"not-empty").unwrap(),
            "directory" => std::fs::create_dir(&lock).unwrap(),
            _ => unreachable!(),
        }
        let before = restore_tree_snapshot(&target);
        assert!(matches!(
            expected_restore_state_for_test(
                &prepared,
                &target,
                "node-1",
                CheckpointInstallMode::Fresh,
                None,
            ),
            Err(DurabilityError::SnapshotVerification(_))
        ));
        assert_eq!(restore_tree_snapshot(&target), before);
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let target = root.path().join("symlink");
        let victim = root.path().join("victim");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(&victim, b"do-not-follow").unwrap();
        symlink(&victim, target.join(".node.lock")).unwrap();
        let before = restore_tree_snapshot(&target);
        assert!(expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::Fresh,
            None,
        )
        .is_err());
        assert_eq!(restore_tree_snapshot(&target), before);
        assert_eq!(std::fs::read(victim).unwrap(), b"do-not-follow");
    }
}

#[tokio::test]
async fn restore_rejects_existing_state_without_mutation() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let data_dir = root.path().join("data");
    let recorder = data_dir.join("consensus/recorder/node-1");
    std::fs::create_dir_all(&recorder).unwrap();
    let sentinel = recorder.join("state.bin");
    std::fs::write(&sentinel, b"keep-me").unwrap();

    assert!(matches!(
        install_fresh_for_test(&archive, &data_dir, "node-1").await,
        Err(DurabilityError::DataDirNotFresh(_))
    ));
    assert_eq!(std::fs::read(&sentinel).unwrap(), b"keep-me");
    assert!(!data_dir.join("consensus/log").exists());
    assert!(!data_dir.join("sqlite").exists());
}

#[tokio::test]
async fn restore_roundtrip_replays_normally_through_node_runtime() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let source = runtime(root.path().join("source"));
    let first = source.write("request-1", "alpha", "one").unwrap();
    let second = source.write("request-2", "beta", "two").unwrap();
    coordinator.note_committed(second.applied_index);
    coordinator
        .flush_runtime(&source, second.applied_index)
        .await
        .unwrap();
    source.checkpoint_compact(&coordinator).await.unwrap();
    drop(source);

    let restored_dir = root.path().join("restored");
    let tip = install_fresh_for_test(&archive, &restored_dir, "node-1")
        .await
        .unwrap();
    assert_eq!(tip.index(), second.applied_index);
    assert_eq!(tip.hash(), second.hash);
    assert_ne!(tip.hash(), first.hash);

    let restored = runtime(&restored_dir);
    assert_eq!(restored.applied_index().unwrap(), 2);
    assert_eq!(
        restored
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
    assert_eq!(
        restored
            .read("beta", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("two")
    );

    let other_node_dir = root.path().join("restored-node-2");
    install_fresh_for_test(&archive, &other_node_dir, "node-2")
        .await
        .unwrap();
    let other = SqliteStateMachine::open_with_configuration(
        other_node_dir.join("sqlite/db.sqlite"),
        "rhiza:sql:cluster-a",
        "node-2",
        1,
        ConfigurationState::active(
            1,
            Membership::new(["node-1", "node-2", "node-3"])
                .unwrap()
                .digest(),
        ),
    )
    .unwrap();
    assert_eq!(other.applied_index_value().unwrap(), second.applied_index);
}

#[tokio::test]
async fn empty_initialized_checkpoint_restores_as_genesis() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let data_dir = root.path().join("data");

    let tip = install_fresh_for_test(&archive, &data_dir, "node-1")
        .await
        .unwrap();

    assert_eq!(tip.index(), 0);
    assert_eq!(tip.hash(), LogHash::ZERO);
    assert!(!data_dir.join("consensus/log").exists());
}

#[tokio::test]
async fn restore_preserves_an_existing_empty_data_directory() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let data_dir = root.path().join("mounted-data");
    std::fs::create_dir(&data_dir).unwrap();

    let before = std::fs::metadata(&data_dir).unwrap();
    install_fresh_for_test(&archive, &data_dir, "node-1")
        .await
        .unwrap();
    let after = std::fs::metadata(&data_dir).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(before.dev(), after.dev());
        assert_eq!(before.ino(), after.ino());
    }
    let entries = std::fs::read_dir(&data_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(
        entries.len(),
        2,
        "genesis restore may retain only its shared lock and durable receipt"
    );
    assert!(entries.iter().any(|name| name == ".node.lock"));
    assert!(entries
        .iter()
        .any(|name| name == ".rhiza-checkpoint-install.json"));
}

#[tokio::test]
async fn checkpoint_compact_publishes_canonical_snapshot_with_exact_suffix() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let source = runtime(root.path().join("node"));
    let first = source.write("request-1", "alpha", "one").unwrap();
    coordinator
        .flush_runtime(&source, first.applied_index)
        .await
        .unwrap();
    let anchor = source.checkpoint_compact(&coordinator).await.unwrap();
    let local = source.log_store().logical_state().unwrap();
    assert_eq!(local.anchor, Some(anchor.clone()));
    assert!(source
        .log_store()
        .read(first.applied_index)
        .unwrap()
        .is_none());

    let second = source.write("request-2", "beta", "two").unwrap();
    coordinator
        .flush_runtime(&source, second.applied_index)
        .await
        .unwrap();
    let restored_dir = root.path().join("restored");
    let tip = install_fresh_for_test(&archive, &restored_dir, "node-1")
        .await
        .unwrap();
    assert_eq!(tip.index(), second.applied_index);
    let restored_checkpoint = load_restored_for_test(&archive).await;
    assert_eq!(restored_checkpoint.snapshot().unwrap().anchor(), &anchor);
    assert_eq!(restored_checkpoint.suffix().len(), 1);
    assert_eq!(restored_checkpoint.suffix()[0].index, second.applied_index);

    let restored = runtime(&restored_dir);
    assert_eq!(
        restored
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
    assert_eq!(
        restored
            .read("beta", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("two")
    );
    let recorder = RecorderFileStore::new_with_membership(
        root.path().join("restored-handoff-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
        restored.consensus().membership().clone(),
    )
    .unwrap();
    rehydrate_recorder_after_checkpoint(
        &restored,
        &recorder,
        second.applied_index,
        &StartupIoContext::new(),
    )
    .unwrap();
    assert!(
        !restored_dir.join("consensus/qefx-restore").exists(),
        "the verified local QEFX handoff is consumed before normal recorder rehydration"
    );
}

#[cfg(feature = "graph")]
#[tokio::test]
async fn graph_checkpoint_restores_snapshot_and_exact_suffix_to_a_fresh_other_node() {
    let root = tempfile::tempdir().unwrap();
    let archive =
        initialized_profile_checkpoint(&root.path().join("archive"), ExecutionProfile::Graph).await;
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let source = profile_runtime(root.path(), "source", "n1", ExecutionProfile::Graph);
    let first = source
        .mutate_graph(
            GraphCommandV1::put_document("request-1", "first", GraphValueV1::U64(1)).unwrap(),
        )
        .unwrap();
    coordinator
        .flush_runtime(&source, first.applied_index())
        .await
        .unwrap();
    let anchor = source.checkpoint_compact(&coordinator).await.unwrap();
    let second = source
        .mutate_graph(
            GraphCommandV1::put_document("request-2", "second", GraphValueV1::U64(2)).unwrap(),
        )
        .unwrap();
    coordinator
        .flush_runtime(&source, second.applied_index())
        .await
        .unwrap();

    let remote = load_restored_for_test(&archive).await;
    assert_eq!(remote.snapshot().unwrap().anchor(), &anchor);
    assert_eq!(remote.suffix().len(), 1);
    assert_eq!(remote.suffix()[0].index, second.applied_index());

    let restored_dir = root.path().join("restored");
    install_fresh_for_test(&archive, &restored_dir, "n2")
        .await
        .unwrap();
    assert!(restored_dir.join("ladybug/graph.lbug").is_file());
    let restored = profile_runtime_at(
        root.path(),
        "restored-recorders",
        restored_dir.clone(),
        "n2",
        ExecutionProfile::Graph,
    );
    assert_eq!(
        restored
            .get_graph_document("first", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(GraphValueV1::U64(1))
    );
    assert_eq!(
        restored
            .get_graph_document("second", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(GraphValueV1::U64(2))
    );
    drop(restored);
    let committed = restore_tree_snapshot(&restored_dir);
    assert_eq!(
        install_fresh_for_test(&archive, &restored_dir, "n2")
            .await
            .unwrap(),
        *remote.tip()
    );
    assert_eq!(restore_tree_snapshot(&restored_dir), committed);
}

#[cfg(feature = "kv")]
#[tokio::test]
async fn kv_checkpoint_restores_snapshot_and_exact_suffix_to_a_fresh_other_node() {
    let root = tempfile::tempdir().unwrap();
    let archive =
        initialized_profile_checkpoint(&root.path().join("archive"), ExecutionProfile::Kv).await;
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let source = profile_runtime(root.path(), "source", "n1", ExecutionProfile::Kv);
    let first = source
        .mutate_kv(KvCommandV1::put("request-1", b"first".to_vec(), b"one".to_vec()).unwrap())
        .unwrap();
    coordinator
        .flush_runtime(&source, first.applied_index())
        .await
        .unwrap();
    let anchor = source.checkpoint_compact(&coordinator).await.unwrap();
    let second = source
        .mutate_kv(KvCommandV1::put("request-2", b"second".to_vec(), b"two".to_vec()).unwrap())
        .unwrap();
    coordinator
        .flush_runtime(&source, second.applied_index())
        .await
        .unwrap();

    let remote = load_restored_for_test(&archive).await;
    assert_eq!(remote.snapshot().unwrap().anchor(), &anchor);
    assert_eq!(remote.suffix().len(), 1);
    assert_eq!(remote.suffix()[0].index, second.applied_index());

    let restored_dir = root.path().join("restored");
    install_fresh_for_test(&archive, &restored_dir, "n2")
        .await
        .unwrap();
    assert!(restored_dir.join("kv/data.redb").is_file());
    let restored = profile_runtime_at(
        root.path(),
        "restored-recorders",
        restored_dir.clone(),
        "n2",
        ExecutionProfile::Kv,
    );
    assert_eq!(
        restored
            .get_kv(b"first", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(b"one".to_vec())
    );
    assert_eq!(
        restored
            .get_kv(b"second", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(b"two".to_vec())
    );
    drop(restored);
    let committed = restore_tree_snapshot(&restored_dir);
    assert_eq!(
        install_fresh_for_test(&archive, &restored_dir, "n2")
            .await
            .unwrap(),
        *remote.tip()
    );
    assert_eq!(restore_tree_snapshot(&restored_dir), committed);
}

#[tokio::test]
async fn failed_snapshot_publication_leaves_local_qlog_prefix_intact() {
    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive = initialized_checkpoint(&archive_root).await;
    let coordinator = CheckpointCoordinator::open(archive, DurabilityMode::Sync)
        .await
        .unwrap();
    let runtime = runtime(root.path().join("node"));
    let committed = runtime.write("request-1", "alpha", "one").unwrap();
    coordinator
        .flush_runtime(&runtime, committed.applied_index)
        .await
        .unwrap();
    std::fs::remove_dir_all(&archive_root).unwrap();
    std::fs::write(&archive_root, b"publication blocked").unwrap();

    assert!(runtime.checkpoint_compact(&coordinator).await.is_err());
    let local = runtime.log_store().logical_state().unwrap();
    assert!(local.anchor.is_none());
    assert!(runtime
        .log_store()
        .read(committed.applied_index)
        .unwrap()
        .is_some());
}

async fn initialized_checkpoint(root: &Path) -> ObjectArchiveStore {
    let archive = checkpoint_store(root);
    archive.initialize_checkpoint().await.unwrap();
    archive
}

#[cfg(any(feature = "graph", feature = "kv"))]
async fn initialized_profile_checkpoint(
    root: &Path,
    profile: ExecutionProfile,
) -> ObjectArchiveStore {
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.to_path_buf(),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(
            effective_cluster_id(profile, "cluster-a").unwrap(),
            1,
            1,
            NodeConfig::new_embedded(
                "cluster-a",
                "n1",
                root.join("checkpoint-identity"),
                1,
                1,
                ["n1", "n2", "n3"],
            )
            .unwrap()
            .with_execution_profile(profile)
            .unwrap()
            .configuration_state()
            .digest(),
            1,
        ),
    );
    archive.initialize_checkpoint().await.unwrap();
    archive
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn profile_runtime(
    root: &Path,
    name: &str,
    node_id: &str,
    profile: ExecutionProfile,
) -> NodeRuntime {
    profile_runtime_at(
        root,
        &format!("{name}-recorders"),
        root.join(name),
        node_id,
        profile,
    )
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn profile_runtime_at(
    root: &Path,
    recorder_name: &str,
    data_dir: std::path::PathBuf,
    node_id: &str,
    profile: ExecutionProfile,
) -> NodeRuntime {
    let cluster_id = effective_cluster_id(profile, "cluster-a").unwrap();
    let config = NodeConfig::new_embedded("cluster-a", node_id, data_dir, 1, 1, ["n1", "n2", "n3"])
        .unwrap()
        .with_execution_profile(profile)
        .unwrap();
    let recorder_root = root.join(recorder_name);
    NodeRuntime::open(
        config,
        Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                cluster_id,
                node_id,
                1,
                1,
                [
                    recorder_root.join("n1"),
                    recorder_root.join("n2"),
                    recorder_root.join("n3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        ),
        &[],
    )
    .unwrap()
}

fn checkpoint_store(root: &Path) -> ObjectArchiveStore {
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.to_path_buf(),
    })
    .unwrap();
    ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            runtime_config(PathBuf::from("unused"))
                .configuration_state()
                .digest(),
            1,
        ),
    )
}

fn runtime_config(data_dir: PathBuf) -> NodeConfig {
    NodeConfig::new(
        "rhiza:sql:cluster-a",
        "node-1",
        data_dir,
        1,
        1,
        [
            PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
            PeerConfig::new("node-2", "http://node-2", "peer-token-2").unwrap(),
            PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
        ],
        "client-token",
    )
    .unwrap()
}

fn runtime(data_dir: impl AsRef<Path>) -> NodeRuntime {
    let data_dir = data_dir.as_ref().to_path_buf();
    let consensus_root = data_dir.parent().unwrap_or(&data_dir).join(format!(
        "{}-recorders",
        data_dir.file_name().unwrap().to_string_lossy()
    ));
    NodeRuntime::open(
        runtime_config(data_dir),
        Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:sql:cluster-a",
                "node-1",
                1,
                1,
                [
                    consensus_root.join("node-1"),
                    consensus_root.join("node-2"),
                    consensus_root.join("node-3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        ),
        &[],
    )
    .unwrap()
}

fn bound_runtime(data_dir: impl AsRef<Path>) -> NodeRuntime {
    let data_dir = data_dir.as_ref().to_path_buf();
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let recorder_root = data_dir.parent().unwrap().join("bound-recorders");
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            let recorder = RecorderFileStore::new_with_membership(
                recorder_root.join(id),
                id.clone(),
                "rhiza:sql:cluster-a",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
        })
        .collect();
    NodeRuntime::open(
        NodeConfig::new(
            "rhiza:sql:cluster-a",
            "node-1",
            data_dir,
            1,
            1,
            [
                PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
                PeerConfig::new("node-2", "http://node-2", "peer-token-2").unwrap(),
                PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
            ],
            "client-token",
        )
        .unwrap(),
        Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "rhiza:sql:cluster-a",
                "node-1",
                1,
                1,
                recorders,
            )
            .unwrap(),
        ),
        &[],
    )
    .unwrap()
}
