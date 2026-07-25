use std::{path::Path, sync::Arc};

use rhiza_archive::{ObjectArchiveStore, SnapshotRecord};
use rhiza_core::{ConfigurationState, LogAnchor, LogHash, RecoveryAnchor, SnapshotIdentity};
use rhiza_log::LogStore;
use rhiza_node::{
    FetchLogError, FetchLogRequest, LogPeer, NodeConfig, NodeError, NodeRuntime, PeerConfig,
    ReadConsistency,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::ThreeNodeConsensus;
use rhiza_sql::{restore_snapshot_file, sql_executor_fingerprint};

#[tokio::test]
async fn restart_accepts_sqlite_exactly_at_compacted_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let consensus = consensus(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus.clone(), &[]).unwrap();
    let applied = runtime.write("request-1", "alpha", "one").unwrap();
    let snapshot = runtime.create_recovery_snapshot().unwrap();
    let publication = publish(dir.path(), &snapshot).await;
    let verified = runtime
        .verify_snapshot_publication(&snapshot, &publication)
        .unwrap();
    runtime.compact_log(&verified).unwrap();
    drop(runtime);

    let reopened = NodeRuntime::open(config, consensus, &[]).unwrap();
    assert_eq!(reopened.applied_index().unwrap(), applied.applied_index);
    assert_eq!(reopened.applied_hash().unwrap(), applied.hash);
    assert_eq!(
        reopened
            .log_store()
            .logical_state()
            .unwrap()
            .tip
            .unwrap()
            .index(),
        1
    );
}

#[tokio::test]
async fn corrupt_materializer_after_compaction_requires_snapshot_restore() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let consensus = consensus(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus.clone(), &[]).unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();
    let snapshot = runtime.create_recovery_snapshot().unwrap();
    let publication = publish(dir.path(), &snapshot).await;
    let verified = runtime
        .verify_snapshot_publication(&snapshot, &publication)
        .unwrap();
    runtime.compact_log(&verified).unwrap();
    drop(runtime);

    std::fs::write(config.data_dir().join("sqlite/db.sqlite"), b"corrupt cache").unwrap();

    assert_eq!(
        NodeRuntime::open(config.clone(), consensus, &[]).unwrap_err(),
        NodeError::SnapshotRequired(Box::new(snapshot.anchor().clone()))
    );
    assert!(config.data_dir().join("sqlite").is_dir());
    assert!(!std::fs::read_dir(config.data_dir())
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("sqlite.quarantine-")));
}

#[tokio::test]
async fn restart_accepts_sqlite_at_anchor_and_replays_retained_tail() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let consensus = consensus(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus.clone(), &[]).unwrap();
    let first = runtime.write("request-1", "alpha", "one").unwrap();
    let snapshot = runtime.create_recovery_snapshot().unwrap();
    let publication = publish(dir.path(), &snapshot).await;
    let verified = runtime
        .verify_snapshot_publication(&snapshot, &publication)
        .unwrap();
    runtime.compact_log(&verified).unwrap();
    let second = runtime.write("request-2", "beta", "two").unwrap();
    drop(runtime);

    std::fs::rename(
        config.data_dir().join("sqlite"),
        config.data_dir().join("sqlite-before-restore"),
    )
    .unwrap();
    restore_snapshot_file(
        config.data_dir().join("sqlite/db.sqlite"),
        snapshot.snapshot(),
        config.node_id(),
    )
    .unwrap();

    let reopened = NodeRuntime::open(config, consensus, &[]).unwrap();
    assert_eq!(reopened.applied_index().unwrap(), second.applied_index);
    assert_eq!(reopened.applied_hash().unwrap(), second.hash);
    assert_eq!(
        reopened
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
    assert_eq!(
        reopened
            .read("beta", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("two")
    );
    assert_eq!(first.applied_index, snapshot.anchor().compacted().index());
}

#[tokio::test]
async fn peer_fetch_at_anchor_returns_full_snapshot_requirement() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();
    let snapshot = runtime.create_recovery_snapshot().unwrap();
    let publication = publish(dir.path(), &snapshot).await;
    let verified = runtime
        .verify_snapshot_publication(&snapshot, &publication)
        .unwrap();
    runtime.compact_log(&verified).unwrap();
    let tail = runtime.write("request-2", "beta", "two").unwrap();

    assert_eq!(
        runtime.fetch_log(FetchLogRequest {
            from_index: snapshot.anchor().compacted().index(),
            max_entries: 1,
        }),
        Err(FetchLogError::SnapshotRequired {
            anchor: Box::new(snapshot.anchor().clone()),
        })
    );
    assert_eq!(
        runtime
            .fetch_log(FetchLogRequest {
                from_index: snapshot.anchor().compacted().index() + 1,
                max_entries: 1,
            })
            .unwrap()
            .entries[0]
            .hash,
        tail.hash
    );
}

#[test]
fn fresh_catch_up_surfaces_snapshot_restore_requirement() {
    let dir = tempfile::tempdir().unwrap();
    let anchor = test_anchor(3, LogHash::digest(&[b"entry-3"]), 1);
    let peer = SnapshotPeer(anchor.clone());

    assert_eq!(
        NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[&peer]).unwrap_err(),
        NodeError::SnapshotRequired(Box::new(anchor))
    );
}

#[test]
fn nonfresh_lagging_catch_up_fails_closed_with_snapshot_requirement() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let consensus = consensus(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus.clone(), &[]).unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();
    drop(runtime);
    let anchor = test_anchor(3, LogHash::digest(&[b"entry-3"]), 1);
    let peer = SnapshotPeer(anchor.clone());

    assert_eq!(
        NodeRuntime::open(config, consensus, &[&peer]).unwrap_err(),
        NodeError::SnapshotRequired(Box::new(anchor))
    );
}

#[test]
fn restart_rejects_anchor_with_wrong_recovery_generation() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let consensus = consensus(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus.clone(), &[]).unwrap();
    let applied = runtime.write("request-1", "alpha", "one").unwrap();
    let anchor = test_anchor(applied.applied_index, applied.hash, 2);
    runtime.log_store().compact_prefix(&anchor).unwrap();
    drop(runtime);

    assert!(matches!(
        NodeRuntime::open(config, consensus, &[]),
        Err(NodeError::Reconciliation(message))
            if message.contains("recovery generation")
    ));
}

#[tokio::test]
async fn compaction_requires_matching_remote_snapshot_publication() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();
    let first = runtime.create_recovery_snapshot().unwrap();
    runtime.write("request-2", "beta", "two").unwrap();
    let second = runtime.create_recovery_snapshot().unwrap();
    let wrong_publication = publish(dir.path(), &second).await;

    assert!(matches!(
        runtime.verify_snapshot_publication(&first, &wrong_publication),
        Err(NodeError::Reconciliation(_))
    ));
    assert!(runtime
        .log_store()
        .logical_state()
        .unwrap()
        .anchor
        .is_none());
}

#[tokio::test]
async fn snapshot_publication_is_rejected_when_executor_fingerprint_differs_from_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();
    let snapshot = runtime.create_recovery_snapshot().unwrap();
    let publication = publication_with_executor_fingerprint(
        publish(dir.path(), &snapshot).await,
        LogHash::digest(&[b"other-executor"]),
    );
    let before = runtime.log_store().logical_state().unwrap().anchor;

    assert!(matches!(
        runtime.verify_snapshot_publication(&snapshot, &publication),
        Err(NodeError::Reconciliation(_))
    ));
    assert_eq!(runtime.log_store().logical_state().unwrap().anchor, before);
}

#[tokio::test]
async fn snapshot_publication_is_rejected_when_configuration_state_differs_from_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();
    let snapshot = runtime.create_recovery_snapshot().unwrap();
    let publication = publication_with_configuration_state(
        publish(dir.path(), &snapshot).await,
        ConfigurationState::active(1, LogHash::digest(&[b"other-configuration"])),
    );
    let before = runtime.log_store().logical_state().unwrap().anchor;

    assert!(matches!(
        runtime.verify_snapshot_publication(&snapshot, &publication),
        Err(NodeError::Reconciliation(_))
    ));
    assert_eq!(runtime.log_store().logical_state().unwrap().anchor, before);
}

#[derive(Clone)]
struct SnapshotPeer(RecoveryAnchor);

impl LogPeer for SnapshotPeer {
    fn fetch_log(
        &self,
        _request: FetchLogRequest,
    ) -> Result<rhiza_node::FetchLogResponse, FetchLogError> {
        Err(FetchLogError::SnapshotRequired {
            anchor: Box::new(self.0.clone()),
        })
    }
}

async fn publish(
    root: &Path,
    snapshot: &rhiza_sql::RecoverySnapshot,
) -> rhiza_archive::SnapshotRecord {
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.join("objects"),
    })
    .unwrap();
    ObjectArchiveStore::new_for_single_process(store, "rhiza:sql:cluster-a")
        .publish_snapshot(snapshot.snapshot())
        .await
        .unwrap()
}

fn publication_with_executor_fingerprint(
    publication: SnapshotRecord,
    executor_fingerprint: LogHash,
) -> SnapshotRecord {
    let mut value = serde_json::to_value(publication).unwrap();
    value["manifest"]["executor_fingerprint"] = serde_json::to_value(executor_fingerprint).unwrap();
    serde_json::from_value(value).unwrap()
}

fn publication_with_configuration_state(
    publication: SnapshotRecord,
    configuration_state: ConfigurationState,
) -> SnapshotRecord {
    let mut value = serde_json::to_value(publication).unwrap();
    value["manifest"]["configuration_state"] = serde_json::to_value(configuration_state).unwrap();
    serde_json::from_value(value).unwrap()
}

fn test_anchor(index: u64, hash: LogHash, recovery_generation: u64) -> RecoveryAnchor {
    let digest = rhiza_quepaxa::Membership::new(["node-1", "node-2", "node-3"])
        .unwrap()
        .digest();
    RecoveryAnchor::new(
        "rhiza:sql:cluster-a",
        1,
        ConfigurationState::active(1, digest),
        recovery_generation,
        LogAnchor::new(index, hash),
        SnapshotIdentity::new(
            format!("snapshot-{index:015}"),
            LogHash::digest(&[b"snapshot"]),
            8,
            sql_executor_fingerprint().unwrap(),
        ),
    )
}

fn node_config(root: &Path) -> NodeConfig {
    NodeConfig::new(
        "rhiza:sql:cluster-a",
        "node-1",
        root.to_path_buf(),
        1,
        1,
        [
            PeerConfig::new("node-1", "http://127.0.0.1:3101", "token-1").unwrap(),
            PeerConfig::new("node-2", "http://127.0.0.1:3102", "token-2").unwrap(),
            PeerConfig::new("node-3", "http://127.0.0.1:3103", "token-3").unwrap(),
        ],
        "client-token",
    )
    .unwrap()
}

fn consensus(root: &Path) -> Arc<ThreeNodeConsensus> {
    Arc::new(
        ThreeNodeConsensus::new(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            [
                root.join("recorders/node-1"),
                root.join("recorders/node-2"),
                root.join("recorders/node-3"),
            ],
        )
        .unwrap(),
    )
}
