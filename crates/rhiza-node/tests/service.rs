use std::{path::Path, sync::Arc, time::Duration};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_node::{
    CheckpointCoordinator, DurabilityMode, NodeConfig, NodeError, NodeRuntime, NodeService,
    PeerConfig, ReadConsistency, WriteRequest,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{Membership, RecorderFileStore, RecorderRpc, ThreeNodeConsensus};
use rhiza_sql::{SqlCommand, SqlStatement, SqlValue};

#[tokio::test(flavor = "multi_thread")]
async fn direct_sync_write_reaches_checkpoint_before_return() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let runtime = runtime(&root.path().join("node"));
    let service = NodeService::new(runtime, Some(coordinator.clone()));

    let committed = service
        .write(WriteRequest {
            request_id: "sync-1".into(),
            key: "alpha".into(),
            value: "one".into(),
        })
        .await
        .unwrap();

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
    assert_eq!(
        service
            .read("alpha", ReadConsistency::Local)
            .await
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_bounded_write_rejects_after_lag_limit() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(
            archive,
            DurabilityMode::Bounded {
                max_lag: Duration::from_millis(10),
            },
        )
        .await
        .unwrap(),
    );
    let runtime = runtime(&root.path().join("node"));
    let service = NodeService::new(runtime.clone(), Some(coordinator));

    service.put("bounded-1", "alpha", "one").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let error = service.put("bounded-2", "beta", "two").await.unwrap_err();

    assert!(
        matches!(error, NodeError::Unavailable(message) if message.contains("checkpoint lag exceeded"))
    );
    assert_eq!(runtime.applied_index().unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_sync_write_stops_retrying_when_runtime_is_cancelled() {
    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive = initialized_checkpoint(&archive_root).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let runtime = runtime(&root.path().join("node"));
    let service = NodeService::new(runtime.clone(), Some(coordinator));
    std::fs::remove_dir_all(&archive_root).unwrap();
    std::fs::write(&archive_root, b"archive unavailable").unwrap();

    let write = tokio::spawn(async move { service.put("sync-1", "alpha", "one").await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while runtime.applied_index().unwrap() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    runtime.cancel_operations();
    let error = tokio::time::timeout(Duration::from_secs(1), write)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();

    assert!(
        matches!(error, NodeError::Unavailable(message) if message.contains("sync durability is unavailable"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_sql_retry_preserves_returning_results() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let service = NodeService::new(runtime(&root.path().join("node")), Some(coordinator));
    service
        .execute_sql(SqlCommand {
            request_id: "schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        })
        .await
        .unwrap();
    let command = SqlCommand {
        request_id: "insert-returning".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(id, name) VALUES (?1, ?2) RETURNING id, name".into(),
            parameters: vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())],
        }],
    };

    let first = service.execute_sql(command.clone()).await.unwrap();
    let replay = service.execute_sql(command).await.unwrap();

    let serialized = serde_json::to_value(&first).unwrap();
    assert!(serialized.get("version").is_none());
    assert!(serialized.get("results").is_some());
    assert_eq!(replay, first);
    assert_eq!(first.results[0].rows_affected, 1);
    assert_eq!(
        first.results[0].returning.as_ref().unwrap().rows,
        [vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())]]
    );
    assert_eq!(
        service
            .query(
                SqlStatement {
                    sql: "SELECT id, name FROM items".into(),
                    parameters: vec![],
                },
                ReadConsistency::Local,
                10,
            )
            .await
            .unwrap()
            .rows,
        [vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())]]
    );
}

async fn initialized_checkpoint(root: &Path) -> ObjectArchiveStore {
    let config = NodeConfig::new(
        "rhiza:sql:cluster-a",
        "node-1",
        root.join("identity-only"),
        1,
        1,
        peers(),
        "client-token",
    )
    .unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.to_path_buf(),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            config.log_initial_configuration().digest(),
            1,
        ),
    );
    archive.initialize_checkpoint().await.unwrap();
    archive
}

fn runtime(data_dir: &Path) -> Arc<NodeRuntime> {
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let recorder_root = data_dir.parent().unwrap().join("service-recorders");
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
    Arc::new(
        NodeRuntime::open(
            NodeConfig::new(
                "rhiza:sql:cluster-a",
                "node-1",
                data_dir.to_path_buf(),
                1,
                1,
                peers(),
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
        .unwrap(),
    )
}

fn peers() -> [PeerConfig; 3] {
    [
        PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
        PeerConfig::new("node-2", "http://node-2", "peer-token-2").unwrap(),
        PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
    ]
}
