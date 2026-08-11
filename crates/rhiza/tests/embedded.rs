use std::{
    sync::{mpsc, Arc, Condvar, Mutex},
    time::Duration,
};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_core::LogHash;
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{
    DecisionProof, EffectBundleBinding, Membership, RecordRequest, RecordSummary, RecorderFileStore,
};
use rhizadb::{
    effective_cluster_id, BatchWriteError, CheckpointCoordinator, DurabilityHealth, DurabilityMode,
    EmbeddedConfig, EmbeddedIdentity, Error, ExecutionProfile, NodeError, ReadConsistency,
    RecorderRpc, Rhiza, SqlCommand, SqlStatement, SqlValue,
};

#[tokio::test(flavor = "multi_thread")]
async fn executes_and_queries_sql_with_in_process_recorders() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config(root.path())).await.unwrap();
    let handle = rhiza.handle();

    handle
        .execute_sql(SqlCommand {
            request_id: "schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        })
        .await
        .unwrap();
    let insert = SqlCommand {
        request_id: "insert".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(id, name) VALUES (?1, ?2) RETURNING id, name".into(),
            parameters: vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())],
        }],
    };
    let first = handle.execute_sql(insert.clone()).await.unwrap();
    let replay = handle.execute_sql(insert).await.unwrap();

    assert_eq!(replay, first);
    assert_eq!(
        first.results[0].returning.as_ref().unwrap().rows,
        [vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())]]
    );

    let result = handle
        .query(
            SqlStatement {
                sql: "SELECT id, name FROM items".into(),
                parameters: vec![],
            },
            ReadConsistency::Local,
            10,
        )
        .await
        .unwrap();

    assert_eq!(result.columns, ["id", "name"]);
    assert_eq!(
        result.rows,
        [vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())]]
    );
    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_sql_batch_shares_one_qwal_anchor_and_retries_unchanged_vector() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config(root.path())).await.unwrap();
    let handle = rhiza.handle();
    handle
        .execute_sql(SqlCommand {
            request_id: "batch-schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE batch_items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        })
        .await
        .unwrap();
    let commands = (1..=3)
        .map(|id| SqlCommand {
            request_id: format!("batch-insert-{id}"),
            statements: vec![SqlStatement {
                sql: "INSERT INTO batch_items(id, name) VALUES (?1, ?2) RETURNING id".into(),
                parameters: vec![SqlValue::Integer(id), SqlValue::Text(format!("name-{id}"))],
            }],
        })
        .collect::<Vec<_>>();

    let first = handle.execute_sql_batch(commands.clone()).await.unwrap();
    let replay = handle.execute_sql_batch(commands).await.unwrap();

    assert_eq!(first, replay);
    assert!(first.iter().all(Result::is_ok));
    let anchors = first
        .iter()
        .map(|result| {
            let result = result.as_ref().unwrap();
            (result.applied_index, result.hash)
        })
        .collect::<Vec<_>>();
    assert_eq!(anchors[0].0, 2);
    assert!(anchors.iter().all(|anchor| *anchor == anchors[0]));
    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_sql_batch_preflight_failure_is_not_attempted() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config(root.path())).await.unwrap();
    let handle = rhiza.handle();
    let statement = SqlStatement {
        sql: "CREATE TABLE batch_preflight(id INTEGER PRIMARY KEY)".into(),
        parameters: vec![],
    };

    let error = handle
        .execute_sql_batch(vec![
            SqlCommand {
                request_id: "would-be-valid".into(),
                statements: vec![statement.clone()],
            },
            SqlCommand {
                request_id: String::new(),
                statements: vec![statement.clone()],
            },
        ])
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BatchWriteError::NotAttempted(Error::Node(NodeError::InvalidRequest(_)))
    ));
    handle
        .execute_sql(SqlCommand {
            request_id: "after-preflight".into(),
            statements: vec![statement],
        })
        .await
        .unwrap();
    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_is_closed_after_shutdown() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config(root.path())).await.unwrap();
    let handle = rhiza.handle();

    rhiza.shutdown().await.unwrap();

    assert!(matches!(
        handle.put("request", "key", "value").await,
        Err(Error::Closed)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_preserves_sql_and_idempotent_returning_results() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config(root.path())).await.unwrap();
    let handle = rhiza.handle();
    handle
        .execute_sql(SqlCommand {
            request_id: "schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        })
        .await
        .unwrap();
    let insert = SqlCommand {
        request_id: "insert".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(id, name) VALUES (?1, ?2) RETURNING id, name".into(),
            parameters: vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())],
        }],
    };
    let first = handle.execute_sql(insert.clone()).await.unwrap();
    rhiza.shutdown().await.unwrap();

    let reopened = Rhiza::open(config(root.path())).await.unwrap();
    let handle = reopened.handle();
    let replay = handle.execute_sql(insert).await.unwrap();

    assert_eq!(replay, first);
    assert_eq!(
        handle
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
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_cancels_a_sync_write_blocked_on_checkpoint_storage() {
    const OUTER_HANG_GUARD: Duration = Duration::from_secs(10);
    const BEHAVIOR_DEADLINE: Duration = Duration::from_secs(1);

    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive = initialized_checkpoint(&archive_root).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let config = config(root.path()).with_coordinator(coordinator.clone());
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();
    let status_handle = handle.clone();
    std::fs::remove_dir_all(&archive_root).unwrap();
    std::fs::write(&archive_root, b"archive unavailable").unwrap();

    let write = tokio::spawn(async move { handle.put("request", "key", "value").await });
    tokio::time::timeout(OUTER_HANG_GUARD, async {
        while coordinator.health() != DurabilityHealth::Unavailable {
            assert!(
                !write.is_finished(),
                "the sync write finished before checkpoint storage became unavailable"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("checkpoint storage failure must make durability unavailable");
    assert!(!status_handle.status().await.unwrap().ready);

    let shutdown = tokio::time::timeout(BEHAVIOR_DEADLINE, rhiza.shutdown())
        .await
        .expect("shutdown must not wait forever for the blocked write");
    assert!(shutdown.is_err());
    assert!(write.await.unwrap().is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn proposal_drains_a_minority_rpc_before_shutdown() {
    let root = tempfile::tempdir().unwrap();
    let (blocked_config, started, release) = config_with_blocked_minority(root.path());
    let rhiza = Rhiza::open(blocked_config).await.unwrap();
    let handle = rhiza.handle();

    let write_handle = handle.clone();
    let write = tokio::spawn(async move { write_handle.put("request", "key", "value").await });
    tokio::task::spawn_blocking(move || started.recv().unwrap())
        .await
        .unwrap();
    assert!(
        !write.is_finished(),
        "a proposal must not report success while an accepted recorder RPC is still running"
    );
    release.release();
    write.await.unwrap().unwrap();
    rhiza.shutdown().await.unwrap();

    let (reopen_config, _started, reopen_release) = config_with_blocked_minority(root.path());
    reopen_release.release();
    let reopened = Rhiza::open(reopen_config).await.unwrap();
    reopened.shutdown().await.unwrap();
    root.close().unwrap();
}

fn config(root: &std::path::Path) -> EmbeddedConfig {
    config_for_profile(root, ExecutionProfile::Sqlite)
}

fn config_for_profile(
    root: &std::path::Path,
    execution_profile: ExecutionProfile,
) -> EmbeddedConfig {
    EmbeddedConfig::local_file_backed("cluster-a", root, execution_profile).unwrap()
}

#[test]
fn local_file_backed_rejects_wrong_canonical_cluster_id_before_creating_state() {
    let root = tempfile::tempdir().unwrap();

    assert!(matches!(
        EmbeddedConfig::local_file_backed(
            "rhiza:graph:cluster-a",
            root.path(),
            ExecutionProfile::Sqlite,
        ),
        Err(Error::Config(
            rhiza_node::ConfigError::ClusterIdProfileMismatch { .. }
        ))
    ));
    assert!(root.path().read_dir().unwrap().next().is_none());
}

#[test]
fn local_file_backed_rejects_uncompiled_profiles_before_creating_state() {
    let expected = [
        ExecutionProfile::Sqlite,
        ExecutionProfile::Graph,
        ExecutionProfile::Kv,
    ]
    .into_iter()
    .find(|profile| rhiza_node::execution_profile_compiled(*profile))
    .expect("the test build must compile at least one execution profile");
    let mut rejected = 0;

    for execution_profile in [ExecutionProfile::Graph, ExecutionProfile::Kv]
        .into_iter()
        .filter(|profile| !rhiza_node::execution_profile_compiled(*profile))
    {
        let root = tempfile::tempdir().unwrap();
        assert!(root.path().read_dir().unwrap().next().is_none());

        assert!(matches!(
            EmbeddedConfig::local_file_backed("cluster-a", root.path(), execution_profile),
            Err(Error::ExecutionProfileMismatch {
                expected: actual_expected,
                actual,
            }) if actual_expected == expected && actual == execution_profile
        ));
        assert!(root.path().read_dir().unwrap().next().is_none());
        rejected += 1;
    }

    if rejected == 0 {
        // All profiles are compiled; the test cannot exercise an uncompiled case.
    }
}

fn config_with_blocked_minority(
    root: &std::path::Path,
) -> (EmbeddedConfig, mpsc::Receiver<()>, BlockingRelease) {
    let identity = EmbeddedIdentity::new("cluster-a", "node-1", 1, 1);
    let recorder_cluster_id = effective_cluster_id(ExecutionProfile::Sqlite, "cluster-a").unwrap();
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let release = BlockingRelease::default();
    let recorders = membership
        .members()
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let recorder = RecorderFileStore::new_with_membership(
                root.join("recorders").join(id),
                id.clone(),
                &recorder_cluster_id,
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            let recorder: Box<dyn RecorderRpc> = if index == 2 {
                Box::new(BlockingRecorder {
                    inner: recorder,
                    started: started_tx.clone(),
                    release: release.clone(),
                })
            } else {
                Box::new(recorder)
            };
            (id.clone(), recorder)
        })
        .collect();
    (
        EmbeddedConfig::new(
            identity,
            root.join("node"),
            ExecutionProfile::Sqlite,
            membership.members().to_vec(),
            recorders,
            vec![],
            None,
        ),
        started_rx,
        release,
    )
}

#[derive(Clone, Default)]
struct BlockingRelease(Arc<(Mutex<bool>, Condvar)>);

impl BlockingRelease {
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

impl Drop for BlockingRelease {
    fn drop(&mut self) {
        self.release();
    }
}

struct BlockingRecorder {
    inner: RecorderFileStore,
    started: mpsc::Sender<()>,
    release: BlockingRelease,
}

impl RecorderRpc for BlockingRecorder {
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
        binding: EffectBundleBinding,
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
        binding: EffectBundleBinding,
        manifest_command: rhiza_core::StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        RecorderRpc::finalize_staged_effect_bundle(&self.inner, context, binding, manifest_command)
    }

    fn fetch_effect_bundle_manifest(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
    ) -> rhiza_quepaxa::Result<Option<rhiza_core::StoredCommand>> {
        RecorderRpc::fetch_effect_bundle_manifest(&self.inner, context, binding)
    }

    fn fetch_effect_bundle_chunk(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
        ordinal: u16,
    ) -> rhiza_quepaxa::Result<Option<Vec<u8>>> {
        RecorderRpc::fetch_effect_bundle_chunk(&self.inner, context, binding, ordinal)
    }

    fn record(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
        let _ = self.started.send(());
        self.release.wait();
        self.inner.record(request)
    }

    fn install_decision_proof(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.inner.install_decision_proof(proof, membership)
    }

    fn inspect_decision_proof(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        self.inner.inspect_decision_proof(slot)
    }

    fn inspect_record_summary(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        self.inner.inspect_record_summary(slot)
    }
}

async fn initialized_checkpoint(root: &std::path::Path) -> ObjectArchiveStore {
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.to_path_buf(),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(
            "cluster-a",
            1,
            1,
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    );
    archive.initialize_checkpoint().await.unwrap();
    archive
}
