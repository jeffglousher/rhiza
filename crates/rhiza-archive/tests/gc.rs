use rhiza_archive::{
    CheckpointIdentity, CheckpointPublisherOptions, Error, GcLeaseKind, GcPolicy,
    ObjectArchiveStore,
};
use rhiza_core::{
    ConfigurationState, EntryType, LogAnchor, LogEntry, LogHash, RecoveryAnchor, SnapshotIdentity,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};

const NOW: u64 = 2_000_000_000_000;

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::test]
async fn gc_plan_keeps_root_retained_generations_and_unknown_layouts() {
    let (_dir, store, root) = fixture();
    let old = checkpoint(&store, 1);
    let retained = checkpoint(&store, 2);
    publish(&old).await;
    publish(&retained).await;
    publish(&root).await;
    root.set_gc_root(identity(3), NOW - 1).await.unwrap();
    age_generation(&store, 1).await;
    age_generation(&store, 2).await;
    store
        .put(&format!("{}/mystery.bin", generation_prefix(1)), b"unknown")
        .await
        .unwrap();

    let plan = root
        .plan_gc(GcPolicy::new("gc-1", identity(3), 1, 1, 1), NOW)
        .await
        .unwrap();

    assert_eq!(plan.root(), &identity(3));
    assert!(plan.candidates().iter().all(|item| item
        .key()
        .contains("generation-00000000000000000001/segments/")));
    assert!(plan
        .candidates()
        .iter()
        .all(|item| item.reason().as_str() == "superseded_recovery_generation"));
    assert!(store
        .get(&format!("{}/manifest.json", generation_prefix(1)))
        .await
        .is_ok());
    assert!(store
        .get(&format!("{}/mystery.bin", generation_prefix(1)))
        .await
        .is_ok());
}

#[tokio::test]
async fn publisher_and_reader_leases_block_gc_until_expiry() {
    for kind in [GcLeaseKind::Publisher, GcLeaseKind::Reader] {
        let (_dir, store, root) = fixture();
        let old = checkpoint(&store, 1);
        publish(&old).await;
        publish(&root).await;
        root.set_gc_root(identity(3), NOW - 1).await.unwrap();
        age_generation(&store, 1).await;
        let plan = root
            .plan_gc(GcPolicy::new("gc-lease", identity(3), 0, 1, 1), NOW)
            .await
            .unwrap();
        root.acquire_gc_lease(kind, "holder", NOW, 100)
            .await
            .unwrap();
        assert!(matches!(
            root.execute_gc(plan.plan_hash(), NOW + 1).await,
            Err(Error::GcBarrierBusy { .. })
        ));
        root.execute_gc(plan.plan_hash(), NOW + 101).await.unwrap();
    }
}

#[tokio::test]
async fn crashed_publisher_session_blocks_gc_only_until_its_lease_expires() {
    let (_dir, store, root) = fixture();
    let old = checkpoint(&store, 1);
    publish(&old).await;
    publish(&root).await;
    let now = current_time_ms();
    root.set_gc_root(identity(3), now - 1).await.unwrap();
    age_generation(&store, 1).await;
    let plan = root
        .plan_gc(
            GcPolicy::new("gc-publisher-crash", identity(3), 0, 1, 1),
            now,
        )
        .await
        .unwrap();
    let publisher = root
        .open_checkpoint_publisher("publisher-crash", CheckpointPublisherOptions::new(10_000))
        .await
        .unwrap();
    let after_open = current_time_ms();

    drop(publisher);
    assert!(matches!(
        root.execute_gc(plan.plan_hash(), after_open + 1).await,
        Err(Error::GcBarrierBusy { .. })
    ));
    root.execute_gc(plan.plan_hash(), after_open + 10_001)
        .await
        .unwrap();
}

#[tokio::test]
async fn planning_does_not_block_publish_or_restore() {
    let (_dir, store, root) = fixture();
    let old = checkpoint(&store, 1);
    publish(&old).await;
    publish(&root).await;
    root.set_gc_root(identity(3), NOW - 1).await.unwrap();
    age_generation(&store, 1).await;
    let plan = root
        .plan_gc(GcPolicy::new("gc-race", identity(3), 0, 100, 1), NOW)
        .await
        .unwrap();

    publish_result(&root).await.unwrap();
    assert_eq!(root.restore_checkpoint().await.unwrap(), entries());
    root.execute_gc(plan.plan_hash(), NOW + 101).await.unwrap();
}

#[tokio::test]
async fn stale_or_tampered_plan_never_deletes() {
    let (_dir, store, root) = fixture();
    let old = checkpoint(&store, 1);
    publish(&old).await;
    publish(&root).await;
    root.set_gc_root(identity(3), NOW - 1).await.unwrap();
    age_generation(&store, 1).await;
    let plan = root
        .plan_gc(GcPolicy::new("gc-stale", identity(3), 0, 1, 1), NOW)
        .await
        .unwrap();
    let plan_key = root.gc_plan_key(plan.plan_hash());
    let mut json: serde_json::Value =
        serde_json::from_slice(&store.get(&plan_key).await.unwrap()).unwrap();
    json["candidates"][0]["size_bytes"] = 999_u64.into();
    store
        .put(&plan_key, serde_json::to_vec(&json).unwrap())
        .await
        .unwrap();

    assert!(matches!(
        root.execute_gc(plan.plan_hash(), NOW + 2).await,
        Err(Error::GcPlanHashMismatch { .. })
    ));
    assert!(store.get(plan.candidates()[0].key()).await.is_ok());
}

#[tokio::test]
async fn partial_execution_retries_idempotently_and_keeps_evidence() {
    let (_dir, store, root) = fixture();
    let old = checkpoint(&store, 1);
    publish(&old).await;
    publish(&root).await;
    root.set_gc_root(identity(3), NOW - 1).await.unwrap();
    age_generation(&store, 1).await;
    let plan = root
        .plan_gc(GcPolicy::new("gc-retry", identity(3), 0, 1, 1), NOW)
        .await
        .unwrap();
    store
        .delete_exact(plan.candidates()[0].key(), plan.candidates()[0].version())
        .await
        .unwrap();

    let report = root.execute_gc(plan.plan_hash(), NOW + 2).await.unwrap();
    let retried = root.execute_gc(plan.plan_hash(), NOW + 3).await.unwrap();

    assert_eq!(report, retried);
    assert_eq!(report.plan_hash(), plan.plan_hash());
    assert_eq!(report.results().len(), plan.candidates().len());
    assert!(
        store
            .list(&root.gc_evidence_prefix(plan.plan_hash()))
            .await
            .unwrap()
            .len()
            >= plan.candidates().len()
    );
}

#[tokio::test]
async fn changed_root_rejects_an_existing_plan() {
    let (_dir, store, root) = fixture();
    let old = checkpoint(&store, 1);
    publish(&old).await;
    publish(&root).await;
    root.set_gc_root(identity(3), NOW - 1).await.unwrap();
    age_generation(&store, 1).await;
    let plan = root
        .plan_gc(GcPolicy::new("gc-root", identity(3), 0, 1, 1), NOW)
        .await
        .unwrap();
    let newer = checkpoint(&store, 4);
    publish(&newer).await;
    newer.set_gc_root(identity(4), NOW + 2).await.unwrap();
    assert!(matches!(
        root.execute_gc(plan.plan_hash(), NOW + 3).await,
        Err(Error::GcPlanStale { .. })
    ));
}

#[tokio::test]
async fn two_executes_converge_on_one_report() {
    let (_dir, store, root) = fixture();
    let old = checkpoint(&store, 1);
    publish(&old).await;
    publish(&root).await;
    root.set_gc_root(identity(3), NOW - 1).await.unwrap();
    age_generation(&store, 1).await;
    let plan = root
        .plan_gc(GcPolicy::new("gc-concurrent", identity(3), 0, 1, 1), NOW)
        .await
        .unwrap();

    let (first, second) = tokio::join!(
        root.execute_gc(plan.plan_hash(), NOW + 2),
        root.execute_gc(plan.plan_hash(), NOW + 2)
    );

    assert_eq!(first.unwrap(), second.unwrap());
}

#[tokio::test]
async fn retained_generation_restores_while_retired_generation_fails_before_object_reads() {
    let (_dir, store, root) = fixture();
    let retired = checkpoint(&store, 1);
    let retained = checkpoint(&store, 2);
    publish(&retired).await;
    publish(&retained).await;
    publish(&root).await;
    root.set_gc_root(identity(3), NOW - 1).await.unwrap();
    age_generation(&store, 1).await;
    age_generation(&store, 2).await;
    let plan = root
        .plan_gc(GcPolicy::new("gc-retire", identity(3), 1, 1, 1), NOW)
        .await
        .unwrap();
    root.execute_gc(plan.plan_hash(), NOW + 2).await.unwrap();

    assert_eq!(root.restore_checkpoint().await.unwrap(), entries());
    assert_eq!(retained.restore_checkpoint().await.unwrap(), entries());
    store
        .put(
            &retired.checkpoint_manifest_key().unwrap(),
            b"unreadable-retired-manifest",
        )
        .await
        .unwrap();
    assert!(matches!(
        retired.restore_checkpoint().await,
        Err(Error::GenerationRetired { .. })
    ));
    assert!(matches!(
        publish_result(&retired).await,
        Err(Error::GenerationRetired { .. })
    ));
}

#[tokio::test]
async fn gc_preserves_active_snapshot_and_collects_orphans_after_grace() {
    let (_dir, store, root) = fixture();
    let first = entries();
    let second = next_entry(&first[0]);
    root.publish_committed(&first).await.unwrap();
    root.publish_committed(std::slice::from_ref(&second))
        .await
        .unwrap();
    let old_bytes = b"snapshot-one";
    root.publish_checkpoint_snapshot(anchor(&first[0], old_bytes), old_bytes)
        .await
        .unwrap();
    let new_bytes = b"snapshot-two";
    let current = root
        .publish_checkpoint_snapshot(
            anchor_for_fingerprint(&second, new_bytes, LogHash::from_bytes([6; 32])),
            new_bytes,
        )
        .await
        .unwrap();
    let active_key = current
        .manifest()
        .base()
        .snapshot()
        .unwrap()
        .object_key()
        .to_string();
    root.set_gc_root(identity(3), NOW - 1).await.unwrap();

    let plan = root
        .plan_gc(GcPolicy::new("gc-orphans", identity(3), 0, 10, 1), NOW)
        .await
        .unwrap();
    assert!(!plan.candidates().is_empty());
    assert!(plan
        .candidates()
        .iter()
        .all(|item| item.key() != active_key));
    assert!(plan
        .candidates()
        .iter()
        .any(|item| item.key().contains("/snapshots/")));
    assert!(plan
        .candidates()
        .iter()
        .any(|item| item.key().contains("/segments/")));
    assert!(matches!(
        root.execute_gc(plan.plan_hash(), NOW + 1).await,
        Err(Error::GcBarrierBusy { .. })
    ));
    root.execute_gc(plan.plan_hash(), NOW + 11).await.unwrap();
    assert!(store.get(&active_key).await.is_ok());
    assert_eq!(
        root.restore_checkpoint_state()
            .await
            .unwrap()
            .snapshot()
            .unwrap()
            .bytes(),
        new_bytes
    );
    for candidate in plan.candidates() {
        assert!(store.get(candidate.key()).await.is_err());
    }
}

fn fixture() -> (tempfile::TempDir, ObjStore, ObjectArchiveStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: dir.path().to_path_buf(),
    })
    .unwrap();
    let root = checkpoint(&store, 3);
    (dir, store, root)
}

fn checkpoint(store: &ObjStore, generation: u64) -> ObjectArchiveStore {
    ObjectArchiveStore::new_checkpoint_for_single_process(store.clone(), identity(generation))
}

fn identity(generation: u64) -> CheckpointIdentity {
    CheckpointIdentity::new("cluster-a", 7, 3, generation)
}

async fn publish(archive: &ObjectArchiveStore) {
    publish_result(archive).await.unwrap();
}

async fn publish_result(archive: &ObjectArchiveStore) -> rhiza_archive::Result<()> {
    archive.publish_committed(&entries()).await.map(|_| ())
}

fn entries() -> Vec<LogEntry> {
    let payload = b"entry".to_vec();
    let hash = LogEntry::calculate_hash(
        "cluster-a",
        1,
        7,
        3,
        EntryType::Command,
        LogHash::ZERO,
        &payload,
    );
    vec![LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 7,
        config_id: 3,
        index: 1,
        entry_type: EntryType::Command,
        payload,
        prev_hash: LogHash::ZERO,
        hash,
    }]
}

fn next_entry(previous: &LogEntry) -> LogEntry {
    let payload = b"entry-two".to_vec();
    let hash = LogEntry::calculate_hash(
        "cluster-a",
        2,
        7,
        3,
        EntryType::Command,
        previous.hash,
        &payload,
    );
    LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 7,
        config_id: 3,
        index: 2,
        entry_type: EntryType::Command,
        payload,
        prev_hash: previous.hash,
        hash,
    }
}

fn anchor(entry: &LogEntry, bytes: &[u8]) -> RecoveryAnchor {
    RecoveryAnchor::new(
        "cluster-a",
        7,
        ConfigurationState::active(3, LogHash::ZERO),
        3,
        LogAnchor::new(entry.index, entry.hash),
        SnapshotIdentity::new(
            format!("snapshot-{}", entry.index),
            LogHash::digest(&[bytes]),
            bytes.len() as u64,
            LogHash::from_bytes([6; 32]),
        ),
    )
}

fn anchor_for_fingerprint(
    entry: &LogEntry,
    bytes: &[u8],
    executor_fingerprint: LogHash,
) -> RecoveryAnchor {
    RecoveryAnchor::new(
        "cluster-a",
        7,
        ConfigurationState::active(3, LogHash::ZERO),
        3,
        LogAnchor::new(entry.index, entry.hash),
        SnapshotIdentity::new(
            format!("snapshot-{}", entry.index),
            LogHash::digest(&[bytes]),
            bytes.len() as u64,
            executor_fingerprint,
        ),
    )
}

async fn age_generation(store: &ObjStore, generation: u64) {
    let prefix = generation_prefix(generation);
    let metadata = store
        .list_metadata(&format!("{prefix}/segments/"))
        .await
        .unwrap();
    assert!(!metadata.is_empty());
}

fn generation_prefix(generation: u64) -> String {
    format!("rhiza/cluster-a/checkpoints/epoch-00000000000000000007/config-00000000000000000003/generation-{generation:020}")
}
