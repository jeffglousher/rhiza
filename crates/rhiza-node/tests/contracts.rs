use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpStream,
    path::Path,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use rhiza_core::{
    Command, CommandKind, ConfigChange, ConfigurationState, EntryType, LogEntry, LogHash,
    StoredCommand,
};
use rhiza_log::{FileLogStore, LogStore};
use rhiza_node::{
    catch_up_missing_entries, log_peer_router, node_router, node_router_with_limits,
    node_rpc_router_for_generation, node_rpc_router_with_limits, recorder_router, AckMode,
    ClientErrorResponse, ConfigError, FetchLogError, FetchLogRequest, FetchLogResponse,
    HttpLogPeer, HttpRecorderClient, InMemoryLogPeer, LogPeer, NodeConfig, NodeError, NodeRuntime,
    PeerConfig, ReadConsistency, ReadRequest, SqlExecuteRequest, SqlExecuteResponse,
    SqlQueryRequest, SqlQueryResponse, StartupIoContext, WriteRequest, DEFAULT_WRITER_BATCH_MAX,
    DEFAULT_WRITER_BATCH_WINDOW, LIVEZ_PATH, MAX_COMMAND_BYTES, MAX_FETCH_ENTRIES,
    MAX_HTTP_BODY_BYTES, NODE_ID_HEADER, PROTOCOL_VERSION, READYZ_PATH, RECORDER_IDENTITY_PATH,
    RECORDER_PROTOCOL_VERSION, RECOVERY_GENERATION_HEADER, SQL_EXECUTE_PATH, SQL_QUERY_PATH,
    VERSION_HEADER,
};
use rhiza_quepaxa::{
    CertifiedDecisionInspection, DecisionProof, FixedMembership, IsrState, Membership,
    ReadFenceRequest, ReadFenceSlotState, RecordRequest, RecordSummary, RecorderFileStore,
    RecorderRpc, ThreeNodeConsensus,
};
use rhiza_sql::{SqlCommand, SqlStatement, SqlValue, SqliteStateMachine, QWAL_V3_MAGIC};

fn test_config_digest() -> LogHash {
    FixedMembership::new(["node-1", "node-2", "node-3"])
        .unwrap()
        .digest()
}

fn assert_test_recorder_context(
    cluster_id: &str,
    epoch: u64,
    config_id: u64,
    config_digest: LogHash,
) {
    assert_eq!(cluster_id, "rhiza:sql:cluster-a");
    assert_eq!(epoch, 1);
    assert_eq!(config_id, 1);
    assert_eq!(config_digest, test_config_digest());
}

#[test]
fn recorder_rpc_piggybacks_command_before_recording_isr_state() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorders = (1..=3)
        .map(|index| {
            Box::new(OrderingRecorder::new(
                format!("node-{index}"),
                calls.clone(),
            )) as Box<dyn RecorderRpc>
        })
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders("rhiza:sql:cluster-a", "node-1", 1, 1, recorders)
            .unwrap();

    let entry = consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(
                CommandKind::Deterministic,
                b"put\trequest-1\talpha\tone".to_vec(),
            ),
        )
        .unwrap();
    assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    let CertifiedDecisionInspection::Committed(certified) = consensus
        .inspect_certified_decision_at(1, LogHash::ZERO)
        .unwrap()
    else {
        panic!("typed ISR quorum did not reconstruct the ordinary decision");
    };
    assert_eq!(certified.entry, entry);
    assert!(matches!(certified.proof, DecisionProof::FastPath { .. }));

    let calls = calls.lock().unwrap();
    let mut piggybacks = 0;
    let mut records = 0;
    for call in calls.iter() {
        match *call {
            "piggyback" => piggybacks += 1,
            "record" => {
                records += 1;
                assert!(piggybacks >= records, "Record ran before its piggyback");
            }
            _ => {}
        }
    }
    assert_eq!(calls.iter().filter(|call| **call == "piggyback").count(), 3);
    assert_eq!(calls.iter().filter(|call| **call == "record").count(), 3);
    assert_eq!(calls.iter().filter(|call| **call == "store").count(), 0);
}

#[test]
fn catch_up_rejects_wrong_first_prev_hash() {
    let local_hash = LogHash::digest(&[b"local"]);
    let fetched = entry(2, LogHash::digest(&[b"wrong"]), b"put\tr2\tk\tv");
    let peer = OneResponsePeer::new(vec![fetched]);

    assert!(matches!(
        catch_up_missing_entries(1, local_hash, "rhiza:sql:cluster-a", 1, 1, &peer, 10),
        Err(FetchLogError::InvalidAnchor { .. })
    ));
}

#[test]
fn catch_up_rejects_tampered_hash() {
    let mut fetched = entry(1, LogHash::ZERO, b"put\tr1\tk\tv");
    fetched.hash = LogHash::ZERO;
    let peer = OneResponsePeer::new(vec![fetched]);

    assert!(matches!(
        catch_up_missing_entries(0, LogHash::ZERO, "rhiza:sql:cluster-a", 1, 1, &peer, 10),
        Err(FetchLogError::InvalidEntry { index: 1, .. })
    ));
}

#[test]
fn catch_up_rejects_foreign_identity() {
    let mut fetched = entry(1, LogHash::ZERO, b"put\tr1\tk\tv");
    fetched.cluster_id = "cluster-b".into();
    fetched.hash = fetched.recompute_hash();
    let peer = OneResponsePeer::new(vec![fetched]);

    assert!(matches!(
        catch_up_missing_entries(0, LogHash::ZERO, "rhiza:sql:cluster-a", 1, 1, &peer, 10),
        Err(FetchLogError::ForeignIdentity { index: 1 })
    ));
}

#[test]
fn runtime_commit_persists_qlog_and_sqlite_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus(dir.path()), &[]).unwrap();

    let committed = runtime.write("request-1", "alpha", "one").unwrap();

    assert_eq!(committed.applied_index, 1);
    assert_eq!(
        runtime
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
    assert_eq!(
        runtime.log_store().read(1).unwrap().unwrap().hash,
        committed.hash
    );
    drop(runtime);

    let reopened = NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap();
    assert_eq!(reopened.applied_index().unwrap(), 1);
    assert_eq!(
        reopened
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
}

#[test]
fn sql_strict_commit_rehydrates_qlog_when_buffered_mirror_is_lost() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus(dir.path()), &[]).unwrap();

    let committed = runtime.write("request-1", "alpha", "one").unwrap();
    drop(runtime);

    std::fs::remove_dir_all(dir.path().join("consensus/log")).unwrap();
    let reopened = NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap();

    assert_eq!(reopened.log_store().last_index().unwrap(), Some(1));
    let read = reopened.read("alpha", ReadConsistency::Local).unwrap();
    assert_eq!(read.value.as_deref(), Some("one"));
    assert_eq!((read.applied_index, read.hash), (1, committed.hash));
}

#[test]
fn startup_rebuilds_corrupt_sql_materializer_from_recorder_quorum() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus(dir.path()), &[]).unwrap();

    let committed = runtime.write("request-1", "alpha", "one").unwrap();
    drop(runtime);

    std::fs::remove_dir_all(dir.path().join("consensus/log")).unwrap();
    std::fs::write(dir.path().join("sqlite/db.sqlite"), b"corrupt local cache").unwrap();

    let reopened = NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap();
    let read = reopened.read("alpha", ReadConsistency::Local).unwrap();
    assert_eq!(read.value.as_deref(), Some("one"));
    assert_eq!((read.applied_index, read.hash), (1, committed.hash));
    assert_eq!(reopened.log_store().last_index().unwrap(), Some(1));
}

#[test]
fn startup_rebuilds_every_torn_sql_cache_pair_from_recorder_quorum() {
    #[derive(Clone, Copy, Debug)]
    enum TornCache {
        DatabaseBehind,
        ControlBehind,
        DatabaseCorrupt,
        ControlCorrupt,
        BothMissing,
    }

    for fault in [
        TornCache::DatabaseBehind,
        TornCache::ControlBehind,
        TornCache::DatabaseCorrupt,
        TornCache::ControlCorrupt,
        TornCache::BothMissing,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let config = node_config(dir.path());
        let runtime = NodeRuntime::open(config.clone(), consensus(dir.path()), &[]).unwrap();
        let db_path = dir.path().join("sqlite/db.sqlite");
        let control_path = dir.path().join("sqlite/db.sqlite.control");
        let base_db = std::fs::read(&db_path).unwrap();
        let base_control = std::fs::read(&control_path).unwrap();
        let committed = runtime.write("request-1", "alpha", "one").unwrap();
        drop(runtime);

        std::fs::remove_dir_all(dir.path().join("consensus/log")).unwrap();
        for suffix in ["-wal", "-shm"] {
            let path = dir.path().join(format!("sqlite/db.sqlite{suffix}"));
            if path.exists() {
                std::fs::remove_file(path).unwrap();
            }
        }
        match fault {
            TornCache::DatabaseBehind => std::fs::write(&db_path, &base_db).unwrap(),
            TornCache::ControlBehind => std::fs::write(&control_path, &base_control).unwrap(),
            TornCache::DatabaseCorrupt => std::fs::write(&db_path, b"corrupt database").unwrap(),
            TornCache::ControlCorrupt => std::fs::write(&control_path, b"corrupt control").unwrap(),
            TornCache::BothMissing => std::fs::remove_dir_all(dir.path().join("sqlite")).unwrap(),
        }

        let reopened = NodeRuntime::open(config, consensus(dir.path()), &[])
            .unwrap_or_else(|error| panic!("{fault:?} did not rebuild: {error}"));
        let read = reopened.read("alpha", ReadConsistency::Local).unwrap();
        assert_eq!(read.value.as_deref(), Some("one"), "fault={fault:?}");
        assert_eq!(
            (read.applied_index, read.hash),
            (committed.applied_index, committed.hash),
            "fault={fault:?}"
        );
        assert_eq!(
            reopened.write("request-1", "alpha", "one").unwrap(),
            committed,
            "fault={fault:?}"
        );
        assert!(matches!(
            reopened.write("request-1", "alpha", "different"),
            Err(NodeError::RequestConflict(_))
        ));
    }
}

#[test]
fn startup_recovers_after_recorder_rotation_and_one_permanent_voter_loss() {
    if std::env::var_os("RUN_EXPENSIVE").is_none() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus(dir.path()), &[]).unwrap();
    let mut last = None;

    for index in 1..=1_100 {
        let request_id = format!("rotation-request-{index}");
        let key = format!("rotation-key-{index}");
        let value = format!("rotation-value-{index}");
        last = Some((
            request_id.clone(),
            key.clone(),
            value.clone(),
            runtime.write(&request_id, &key, &value).unwrap(),
        ));
        if index >= 40
            && ["node-1", "node-2"].into_iter().all(|node| {
                std::fs::read_dir(dir.path().join("recorders").join(node))
                    .unwrap()
                    .filter_map(Result::ok)
                    .any(|entry| entry.file_name().to_string_lossy().starts_with("command-"))
            })
        {
            break;
        }
    }
    let (request_id, key, value, committed) = last.unwrap();
    assert!(["node-1", "node-2"].into_iter().all(|node| {
        std::fs::read_dir(dir.path().join("recorders").join(node))
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().starts_with("command-"))
    }));
    drop(runtime);

    std::fs::remove_dir_all(dir.path().join("recorders/node-3")).unwrap();
    std::fs::remove_dir_all(dir.path().join("consensus/log")).unwrap();
    std::fs::remove_dir_all(dir.path().join("sqlite")).unwrap();

    let reopened = NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap();
    let read = reopened.read(&key, ReadConsistency::Local).unwrap();
    assert_eq!(read.value.as_deref(), Some(value.as_str()));
    assert_eq!(
        (read.applied_index, read.hash),
        (committed.applied_index, committed.hash)
    );
    assert_eq!(
        reopened.write(&request_id, &key, &value).unwrap(),
        committed
    );
}

#[test]
fn startup_fails_closed_when_only_one_recorder_remembers_the_lost_local_tail() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus(dir.path()), &[]).unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();
    drop(runtime);

    std::fs::remove_dir_all(dir.path().join("recorders/node-2")).unwrap();
    std::fs::remove_dir_all(dir.path().join("recorders/node-3")).unwrap();
    std::fs::remove_dir_all(dir.path().join("consensus/log")).unwrap();
    std::fs::remove_dir_all(dir.path().join("sqlite")).unwrap();

    assert!(matches!(
        NodeRuntime::open(config, consensus(dir.path()), &[]),
        Err(NodeError::Unavailable(_))
    ));
}

#[test]
fn runtime_regenerates_sql_effect_after_a_foreign_slot_winner() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let shared_consensus = consensus(dir.path());
    let runtime = NodeRuntime::open(config, Arc::clone(&shared_consensus), &[]).unwrap();
    let schema = runtime
        .execute_sql(SqlCommand {
            request_id: "effect-schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    assert_eq!(schema.applied_index, 1);

    let winner = shared_consensus
        .propose_at(
            2,
            schema.hash,
            Command::new(CommandKind::ReadBarrier, Vec::new()),
        )
        .unwrap();
    let committed = runtime
        .execute_sql(SqlCommand {
            request_id: "effect-after-winner".into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO items(value) VALUES ('effect') RETURNING id, value".into(),
                parameters: vec![],
            }],
        })
        .unwrap();

    assert_eq!(committed.applied_index, 3);
    assert_eq!(runtime.log_store().read(2).unwrap(), Some(winner));
    assert!(runtime
        .log_store()
        .read(3)
        .unwrap()
        .unwrap()
        .payload
        .starts_with(QWAL_V3_MAGIC));
    assert_eq!(
        runtime
            .query_sql(
                &SqlStatement {
                    sql: "SELECT id, value FROM items".into(),
                    parameters: vec![],
                },
                ReadConsistency::Local,
                10,
            )
            .unwrap()
            .rows,
        [vec![SqlValue::Integer(1), SqlValue::Text("effect".into())]]
    );
}

#[test]
fn sql_batch_reprepares_all_members_after_a_foreign_slot_winner() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let shared_consensus = consensus(dir.path());
    let runtime = NodeRuntime::open(config, Arc::clone(&shared_consensus), &[]).unwrap();
    let schema = runtime
        .execute_sql(SqlCommand {
            request_id: "batch-winner-schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE batch_winner(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                    .into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    let winner = shared_consensus
        .propose_at(
            2,
            schema.hash,
            Command::new(CommandKind::ReadBarrier, Vec::new()),
        )
        .unwrap();
    let commands = (1..=2)
        .map(|id| SqlCommand {
            request_id: format!("batch-winner-{id}"),
            statements: vec![SqlStatement {
                sql: "INSERT INTO batch_winner(id, value) VALUES (?1, ?2)".into(),
                parameters: vec![SqlValue::Integer(id), SqlValue::Text(format!("value-{id}"))],
            }],
        })
        .collect::<Vec<_>>();

    let results = runtime.execute_sql_batch(commands).unwrap();

    assert_eq!(runtime.log_store().read(2).unwrap(), Some(winner));
    assert!(results.iter().all(Result::is_ok));
    assert!(results
        .iter()
        .all(|result| result.as_ref().unwrap().applied_index == 3));
    assert_eq!(
        results
            .iter()
            .map(|result| result.as_ref().unwrap().hash)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        1
    );
    assert!(runtime
        .log_store()
        .read(3)
        .unwrap()
        .unwrap()
        .payload
        .starts_with(QWAL_V3_MAGIC));
}

#[test]
fn node_config_rejects_duplicate_peer_ids() {
    let dir = tempfile::tempdir().unwrap();
    let peers = [
        PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
        PeerConfig::new("node-1", "http://node-2", "peer-token-2").unwrap(),
        PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
    ];

    assert!(matches!(
        NodeConfig::new(
            "rhiza:sql:cluster-a",
            "node-1",
            dir.path().to_path_buf(),
            1,
            1,
            peers,
            "client-token",
        ),
        Err(ConfigError::DuplicatePeerNodeId(node_id)) if node_id == "node-1"
    ));
}

#[test]
fn node_config_defaults_generation_to_one_and_rejects_zero() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());

    assert_eq!(config.recovery_generation(), 1);
    let debug = format!("{config:?}");
    assert!(debug.contains("recovery_generation: 1"));
    assert!(!debug.contains("client-token"));
    assert!(!debug.contains("peer-token"));
    assert_eq!(
        config.with_recovery_generation(0),
        Err(ConfigError::InvalidRecoveryGeneration)
    );
}

#[test]
fn node_config_uses_conservative_configurable_writer_batch_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());

    assert_eq!(config.writer_batch_max(), DEFAULT_WRITER_BATCH_MAX);
    assert_eq!(config.writer_batch_window(), DEFAULT_WRITER_BATCH_WINDOW);
    assert_eq!(DEFAULT_WRITER_BATCH_MAX, 8);
    assert!(DEFAULT_WRITER_BATCH_WINDOW < Duration::from_millis(1));

    let configured = config
        .with_writer_batching(4, Duration::from_millis(10))
        .unwrap();
    assert_eq!(configured.writer_batch_max(), 4);
    assert_eq!(configured.writer_batch_window(), Duration::from_millis(10));
}

#[test]
fn dr_strong_open_is_rejected_until_synchronous_archive_is_injected() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path()).with_ack_mode(AckMode::DrStrong);

    assert!(matches!(
        NodeRuntime::open(config, consensus(dir.path()), &[]),
        Err(NodeError::UnsupportedAckMode(AckMode::DrStrong))
    ));
}

#[test]
fn node_runtime_holds_exclusive_data_root_lock() {
    let dir = tempfile::tempdir().unwrap();
    let first_consensus = consensus_named(dir.path(), "first-recorders");
    let second_consensus = consensus_named(dir.path(), "second-recorders");
    let first = NodeRuntime::open(node_config(dir.path()), first_consensus, &[]).unwrap();

    assert!(matches!(
        NodeRuntime::open(node_config(dir.path()), second_consensus, &[]),
        Err(NodeError::DataRootLocked(_))
    ));

    drop(first);
}

#[test]
fn repeated_request_returns_original_outcome_and_conflict_is_typed() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap();

    let first = runtime.write("request-1", "alpha", "one").unwrap();
    let repeated = runtime.write("request-1", "alpha", "one").unwrap();

    assert_eq!(repeated, first);
    assert!(matches!(
        runtime.write("request-1", "alpha", "two"),
        Err(NodeError::RequestConflict(_))
    ));
}

#[test]
fn startup_replays_qlog_ahead_of_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let log = FileLogStore::open(
        dir.path().join("consensus/log"),
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let entry = first_qwal_put_entry(
        &dir.path().join("prepared-qwal"),
        "request-1",
        "alpha",
        "from-qlog",
    );
    log.append(&entry).unwrap();

    let runtime = NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap();

    assert_eq!(runtime.applied_index().unwrap(), 1);
    assert_eq!(
        runtime
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("from-qlog")
    );
}

#[test]
fn startup_recovers_quorum_decision_without_qlog_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let decided_consensus = consensus(dir.path());
    let prepared = first_qwal_put_entry(
        &dir.path().join("prepared-qwal"),
        "request-1",
        "alpha",
        "recovered",
    );
    let decided = decided_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, prepared.payload),
        )
        .unwrap();

    let runtime = NodeRuntime::open(config.clone(), decided_consensus, &[]).unwrap();
    assert_eq!(runtime.log_store().read(1).unwrap(), Some(decided.clone()));
    assert_eq!(runtime.applied_index().unwrap(), 1);
    drop(runtime);

    let reopened = NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap();
    assert_eq!(reopened.applied_index().unwrap(), 1);
    assert_eq!(reopened.log_store().last_index().unwrap(), Some(1));
}

#[test]
fn startup_rejects_non_qwal_sql_decision_before_mutating_qlog() {
    let dir = tempfile::tempdir().unwrap();
    let decided_consensus = consensus(dir.path());
    decided_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(
                CommandKind::Deterministic,
                b"put\tlegacy-request\talpha\tlegacy".to_vec(),
            ),
        )
        .unwrap();

    assert!(matches!(
        NodeRuntime::open(node_config(dir.path()), decided_consensus, &[]),
        Err(NodeError::Invariant(message)) if message.contains("QWAL")
    ));
    let log = FileLogStore::open_with_configuration(
        dir.path().join("consensus/log"),
        "rhiza:sql:cluster-a",
        1,
        ConfigurationState::active(1, test_config_digest()),
    )
    .unwrap();
    assert_eq!(log.last_index().unwrap(), None);
    assert_eq!(log.read(1).unwrap(), None);
}

#[test]
fn startup_rejects_non_qwal_peer_winner_before_mutating_qlog() {
    let dir = tempfile::tempdir().unwrap();
    let decided_consensus = consensus(dir.path());
    let decided = decided_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(
                CommandKind::Deterministic,
                b"put\tlegacy-request\talpha\tlegacy".to_vec(),
            ),
        )
        .unwrap();
    let peer = OneResponsePeer::new(vec![decided]);

    assert!(matches!(
        NodeRuntime::open(node_config(dir.path()), decided_consensus, &[&peer]),
        Err(NodeError::Invariant(message)) if message.contains("QWAL")
    ));
    let log = FileLogStore::open_with_configuration(
        dir.path().join("consensus/log"),
        "rhiza:sql:cluster-a",
        1,
        ConfigurationState::active(1, test_config_digest()),
    )
    .unwrap();
    assert_eq!(log.last_index().unwrap(), None);
    assert_eq!(log.read(1).unwrap(), None);
}

#[test]
fn startup_replays_qlog_config_change_ahead_of_sqlite_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    drop(NodeRuntime::open(config.clone(), consensus(dir.path()), &[]).unwrap());

    let log = FileLogStore::open_with_configuration(
        dir.path().join("consensus/log"),
        "rhiza:sql:cluster-a",
        1,
        ConfigurationState::active(1, test_config_digest()),
    )
    .unwrap();
    let command = ConfigChange::bound_stop(
        "rhiza:sql:cluster-a",
        1,
        test_config_digest(),
        2,
        vec!["node-1".into(), "node-2".into(), "node-3".into()],
    )
    .unwrap()
    .to_stored_command();
    let stop = LogEntry {
        cluster_id: "rhiza:sql:cluster-a".into(),
        epoch: 1,
        config_id: 1,
        index: 1,
        entry_type: command.entry_type,
        payload: command.payload,
        prev_hash: LogHash::ZERO,
        hash: LogHash::ZERO,
    };
    let stop = LogEntry {
        hash: stop.recompute_hash(),
        ..stop
    };
    log.append(&stop).unwrap();
    drop(log);

    let runtime = NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap();
    assert_eq!(runtime.applied_index().unwrap(), 1);
    assert_eq!(
        runtime
            .configuration_state()
            .unwrap()
            .stop()
            .map(|anchor| (anchor.index(), anchor.hash())),
        Some((1, stop.hash))
    );
}

#[test]
fn startup_accepts_peer_candidate_only_when_consensus_committed_exact_entry() {
    let dir = tempfile::tempdir().unwrap();
    let decided_consensus = consensus(dir.path());
    let prepared = first_qwal_put_entry(
        &dir.path().join("prepared-qwal"),
        "request-candidate",
        "alpha",
        "verified",
    );
    let decided = decided_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, prepared.payload),
        )
        .unwrap();
    let peer = OneResponsePeer::new(vec![decided.clone()]);

    let runtime = NodeRuntime::open(node_config(dir.path()), decided_consensus, &[&peer]).unwrap();

    assert_eq!(runtime.log_store().read(1).unwrap(), Some(decided));
    assert_eq!(
        runtime
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("verified")
    );
}

#[test]
fn startup_rejects_peer_candidate_that_differs_from_committed_decision() {
    let dir = tempfile::tempdir().unwrap();
    let decided_consensus = consensus(dir.path());
    decided_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(
                CommandKind::Deterministic,
                b"put\trequest-committed\talpha\tcommitted".to_vec(),
            ),
        )
        .unwrap();
    let peer = OneResponsePeer::new(vec![entry(
        1,
        LogHash::ZERO,
        b"put\trequest-candidate\talpha\tuncommitted",
    )]);

    assert!(matches!(
        NodeRuntime::open(
            node_config(dir.path()),
            decided_consensus,
            &[&peer],
        ),
        Err(NodeError::Reconciliation(message)) if message.contains("candidate")
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_three_http_recorders_commit() {
    let dir = tempfile::tempdir().unwrap();
    let peers = peer_configs(["http://unused"; 3]);
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let mut servers = Vec::new();
    let mut base_urls = Vec::new();
    for index in 1..=3 {
        let store = RecorderFileStore::new_with_membership(
            dir.path().join(format!("http-recorder-{index}")),
            format!("node-{index}"),
            "rhiza:sql:cluster-a",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = recorder_router(store, peers.clone());
        servers.push(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }));
        base_urls.push(format!("http://{addr}"));
    }

    let recorders = base_urls
        .into_iter()
        .map(|base_url| {
            Box::new(HttpRecorderClient::new(base_url, "node-1", "peer-token-1").unwrap())
                as Box<dyn RecorderRpc>
        })
        .collect();
    let committed = tokio::task::spawn_blocking(move || {
        ThreeNodeConsensus::from_recorders("rhiza:sql:cluster-a", "node-1", 1, 1, recorders)
            .unwrap()
            .propose_at(
                1,
                LogHash::ZERO,
                Command::new(
                    CommandKind::Deterministic,
                    b"put\trequest-1\talpha\tone".to_vec(),
                ),
            )
    })
    .await
    .unwrap()
    .unwrap();

    assert_eq!(committed.index, 1);
    for server in servers {
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_generation_http_clients_reach_recorder_and_log_routes() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("generation-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let log_peer = InMemoryLogPeer::new(Vec::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            node_rpc_router_for_generation(
                TypedOnlyRecorder(recorder),
                log_peer,
                peer_configs(["http://unused"; 3]),
                7,
            ),
        )
        .await
        .unwrap();
    });
    let base_url = format!("http://{addr}");
    let recorder =
        HttpRecorderClient::new_with_recovery_generation(&base_url, "node-1", "peer-token-1", 7)
            .unwrap();
    let (reply, summary, fence) = tokio::task::spawn_blocking(move || {
        (
            recorder.recorder_id(),
            recorder.inspect_record_summary(1),
            recorder.observe_read_fence(ReadFenceRequest {
                cluster_id: "rhiza:sql:cluster-a".into(),
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                slot: 1,
            }),
        )
    })
    .await
    .unwrap();
    let reply = reply.unwrap();
    assert_eq!(reply, "node-1");
    assert!(summary.unwrap().is_none());
    assert_eq!(fence.unwrap().slot_state, ReadFenceSlotState::Empty);

    let log_peer = HttpLogPeer::new(base_url, "node-1", "peer-token-1")
        .unwrap()
        .with_recovery_generation(7)
        .unwrap();
    let fetched = tokio::task::spawn_blocking(move || {
        log_peer.fetch_log(FetchLogRequest {
            from_index: 1,
            max_entries: 1,
        })
    })
    .await
    .unwrap()
    .unwrap();
    assert!(fetched.entries.is_empty());
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn http_read_fence_fails_before_the_general_recorder_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let client =
        HttpRecorderClient::new(format!("http://{address}"), "node-1", "peer-token-1").unwrap();

    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        client.observe_read_fence(ReadFenceRequest {
            cluster_id: "rhiza:sql:cluster-a".into(),
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            slot: 1,
        })
    })
    .await
    .unwrap();

    assert!(matches!(result, Err(rhiza_quepaxa::Error::Io(_))));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "read fence inherited the 10 second recorder deadline"
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn http_record_transport_failure_releases_the_quorum_attempt_promptly() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let client =
        HttpRecorderClient::new(format!("http://{address}"), "node-1", "peer-token-1").unwrap();

    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        client.record(RecordRequest {
            cluster_id: "rhiza:sql:cluster-a".into(),
            epoch: 1,
            config_id: 1,
            config_digest: test_config_digest(),
            slot: 1,
            step: 1,
            proposal: rhiza_quepaxa::Proposal::nil(),
            command: None,
        })
    })
    .await
    .unwrap();

    assert!(matches!(result, Err(rhiza_quepaxa::Error::ProposeFailed)));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "record inherited the 10 second recorder deadline"
    );
    server.abort();
}

#[derive(Clone)]
struct TypedOnlyRecorder(RecorderFileStore);

impl RecorderRpc for TypedOnlyRecorder {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        self.0.recorder_id()
    }

    fn store_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        self.0.store_command(command_hash, command)
    }

    fn fetch_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        self.0.fetch_command(command_hash)
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        self.0.record_proposal(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.0.install_decision_proof_record(proof, membership)
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        RecorderRpc::inspect_decision_proof(&self.0, slot)
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        RecorderRpc::inspect_record_summary(&self.0, slot)
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<rhiza_quepaxa::ReadFenceObservation> {
        RecorderRpc::observe_read_fence(&self.0, request)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_or_missing_generation_is_rejected_before_body_and_peer_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path()).with_recovery_generation(7).unwrap();
    let runtime = Arc::new(NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("generation-order-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router_with_limits(runtime, recorder, 0, 0))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();

    for (path, version) in [
        (RECORDER_IDENTITY_PATH, RECORDER_PROTOCOL_VERSION),
        ("/v1/log/fetch", PROTOCOL_VERSION),
    ] {
        for generation in [None, Some("6")] {
            let mut request = client
                .post(format!("http://{addr}{path}"))
                .header(VERSION_HEADER, version)
                .header(NODE_ID_HEADER, "node-1")
                .bearer_auth("peer-token-1")
                .header("content-type", "application/json")
                .body("{");
            if let Some(generation) = generation {
                request = request.header(RECOVERY_GENERATION_HEADER, generation);
            }
            assert_eq!(
                request.send().await.unwrap().status(),
                reqwest::StatusCode::UNAUTHORIZED
            );
        }

        let at_capacity = client
            .post(format!("http://{addr}{path}"))
            .header(VERSION_HEADER, version)
            .header(NODE_ID_HEADER, "node-1")
            .header(RECOVERY_GENERATION_HEADER, "7")
            .bearer_auth("peer-token-1")
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await
            .unwrap();
        assert_eq!(at_capacity.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    }

    for path in ["/v1/write", "/v1/read", "/v1/sql/execute", "/v1/sql/query"] {
        let response = client
            .post(format!("http://{addr}{path}"))
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .bearer_auth("client-token")
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await
            .unwrap();
        assert_client_error(
            response,
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "overloaded",
            true,
            None,
        )
        .await;
    }
    server.abort();
}

#[tokio::test]
async fn unauthorized_request_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = RecorderFileStore::new_with_id(
        dir.path().join("recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            recorder_router(store, peer_configs(["http://unused"; 3])),
        )
        .await
        .unwrap();
    });

    let status = reqwest::Client::new()
        .post(format!("http://{addr}{RECORDER_IDENTITY_PATH}"))
        .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("wrong-token")
        .json(&serde_json::json!({"version": 3, "body": null}))
        .send()
        .await
        .unwrap()
        .status();

    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn health_routes_distinguish_process_liveness_from_runtime_readiness() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("health-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .get(format!("http://{addr}{LIVEZ_PATH}"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("http://{addr}{READYZ_PATH}"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );

    runtime
        .log_store()
        .append(&entry(1, LogHash::ZERO, b"put\texternal\talpha\tone"))
        .unwrap();
    assert!(runtime.read("alpha", ReadConsistency::ReadBarrier).is_err());
    assert!(runtime.is_fatal());

    assert_eq!(
        client
            .get(format!("http://{addr}{LIVEZ_PATH}"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("http://{addr}{READYZ_PATH}"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn log_fetch_is_authenticated_versioned_and_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    runtime.write("request-log", "alpha", "one").unwrap();
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("log-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let base_url = format!("http://{addr}");
    let peer = HttpLogPeer::new(&base_url, "node-1", "peer-token-1").unwrap();
    let fetched = tokio::task::spawn_blocking(move || {
        peer.fetch_log(FetchLogRequest {
            from_index: 1,
            max_entries: 10,
        })
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fetched.entries.len(), 1);

    let client = reqwest::Client::new();
    let request = || FetchLogRequest {
        from_index: 1,
        max_entries: 1,
    };
    let missing_version = client
        .post(format!("{base_url}/v1/log/fetch"))
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&request())
        .send()
        .await
        .unwrap();
    let wrong_version = client
        .post(format!("{base_url}/v1/log/fetch"))
        .header(VERSION_HEADER, "wrong")
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&request())
        .send()
        .await
        .unwrap();
    let missing_caller = client
        .post(format!("{base_url}/v1/log/fetch"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&request())
        .send()
        .await
        .unwrap();
    let wrong_bearer = client
        .post(format!("{base_url}/v1/log/fetch"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("wrong")
        .json(&request())
        .send()
        .await
        .unwrap();
    for response in [missing_version, wrong_version, missing_caller, wrong_bearer] {
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    }

    let oversized_batch = client
        .post(format!("{base_url}/v1/log/fetch"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&FetchLogRequest {
            from_index: 1,
            max_entries: MAX_FETCH_ENTRIES + 1,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(oversized_batch.status(), reqwest::StatusCode::BAD_REQUEST);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn client_routes_use_stable_status_mapping_and_body_limit() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("status-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/v1/write");
    let write = |request_id: &str, value: &str| WriteRequest {
        request_id: request_id.into(),
        key: "alpha".into(),
        value: value.into(),
    };

    let unauthorized = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .json(&write("unauthorized", "one"))
        .send()
        .await
        .unwrap();
    assert_client_error(
        unauthorized,
        reqwest::StatusCode::UNAUTHORIZED,
        "unauthorized",
        false,
        None,
    )
    .await;

    let invalid = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&WriteRequest {
            request_id: "invalid".into(),
            key: "bad\tkey".into(),
            value: "one".into(),
        })
        .send()
        .await
        .unwrap();
    assert_client_error(
        invalid,
        reqwest::StatusCode::BAD_REQUEST,
        "invalid_request",
        false,
        None,
    )
    .await;

    let first = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&write("conflict", "one"))
        .send()
        .await
        .unwrap();
    assert!(first.status().is_success());
    let conflict = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&write("conflict", "two"))
        .send()
        .await
        .unwrap();
    assert_client_error(
        conflict,
        reqwest::StatusCode::CONFLICT,
        "request_conflict",
        false,
        None,
    )
    .await;

    let unavailable = client
        .post(format!("http://{addr}/v1/read"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&ReadRequest {
            key: "alpha".into(),
            consistency: Some(ReadConsistency::AppliedIndex(100)),
        })
        .send()
        .await
        .unwrap();
    assert_client_error(
        unavailable,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        true,
        None,
    )
    .await;

    let too_large = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .header("content-type", "application/json")
        .body("x".repeat(MAX_HTTP_BODY_BYTES + 1))
        .send()
        .await
        .unwrap();
    assert_client_error(
        too_large,
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
        false,
        None,
    )
    .await;

    let last_index = runtime.log_store().last_index().unwrap().unwrap();
    let last_hash = runtime.log_store().read(last_index).unwrap().unwrap().hash;
    runtime
        .log_store()
        .append(&entry(
            last_index + 1,
            last_hash,
            b"put\texternal\talpha\tfatal",
        ))
        .unwrap();
    assert!(runtime.read("alpha", ReadConsistency::ReadBarrier).is_err());
    let fatal = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&write("fatal", "three"))
        .send()
        .await
        .unwrap();
    assert_client_error(
        fatal,
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "fatal",
        false,
        None,
    )
    .await;
    server.abort();
}

#[test]
fn read_consistency_accepts_only_canonical_snake_case_values() {
    assert_eq!(
        serde_json::to_value(ReadConsistency::Local).unwrap(),
        "local"
    );
    assert_eq!(
        serde_json::to_value(ReadConsistency::ReadBarrier).unwrap(),
        "read_barrier"
    );
    assert_eq!(
        serde_json::to_value(ReadConsistency::AppliedIndex(7)).unwrap(),
        serde_json::json!({"applied_index": 7})
    );

    assert_eq!(
        serde_json::from_value::<ReadConsistency>(serde_json::json!("local")).unwrap(),
        ReadConsistency::Local
    );
    assert_eq!(
        serde_json::from_value::<ReadConsistency>(serde_json::json!("read_barrier")).unwrap(),
        ReadConsistency::ReadBarrier
    );
    assert_eq!(
        serde_json::from_value::<ReadConsistency>(serde_json::json!({"applied_index": 7})).unwrap(),
        ReadConsistency::AppliedIndex(7)
    );
    for legacy in [
        serde_json::json!("Local"),
        serde_json::json!("ReadBarrier"),
        serde_json::json!({"AppliedIndex": 7}),
    ] {
        assert!(serde_json::from_value::<ReadConsistency>(legacy).is_err());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnected_client_holds_its_slot_without_starving_peer_rpc() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(Gate::default());
    let recorders = (1..=3)
        .map(|index| {
            Box::new(GatedRecorder::new(format!("node-{index}"), gate.clone()))
                as Box<dyn RecorderRpc>
        })
        .collect();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders("rhiza:sql:cluster-a", "node-1", 1, 1, recorders)
            .unwrap(),
    );
    let runtime = Arc::new(NodeRuntime::open(node_config(dir.path()), consensus, &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("saturation-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router_with_limits(runtime, recorder, 1, 1))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/v1/write");
    let slow_client = client.clone();
    let slow_url = url.clone();
    let slow = tokio::spawn(async move {
        slow_client
            .post(slow_url)
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .bearer_auth("client-token")
            .json(&WriteRequest {
                request_id: "slow".into(),
                key: "alpha".into(),
                value: "one".into(),
            })
            .send()
            .await
    });

    let deadline = Instant::now() + Duration::from_secs(3);
    while !gate.started() {
        assert!(Instant::now() < deadline, "blocking write did not start");
        tokio::task::yield_now().await;
    }
    slow.abort();

    let saturated = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&WriteRequest {
            request_id: "saturated".into(),
            key: "alpha".into(),
            value: "two".into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(saturated.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    let peer = client
        .post(format!("http://{addr}{RECORDER_IDENTITY_PATH}"))
        .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&serde_json::json!({"version": 3, "body": null}))
        .send()
        .await
        .unwrap();
    assert!(peer.status().is_success());

    gate.release();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let response = client
            .post(&url)
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .bearer_auth("client-token")
            .json(&WriteRequest {
                request_id: "after-release".into(),
                key: "alpha".into(),
                value: "three".into(),
            })
            .send()
            .await
            .unwrap();
        if response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS {
            assert!(response.status().is_success());
            break;
        }
        assert!(Instant::now() < deadline, "client slot was not released");
        tokio::task::yield_now().await;
    }
    server.abort();
}

#[test]
fn read_barrier_empty_quorum_returns_current_anchor_without_advancing_qlog() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();

    let read = runtime.read("alpha", ReadConsistency::ReadBarrier).unwrap();

    assert_eq!(read.value.as_deref(), Some("one"));
    assert_eq!(read.applied_index, 1);
    assert_eq!(runtime.log_store().read(2).unwrap(), None);
}

#[test]
fn read_barrier_on_fresh_node_keeps_qlog_at_zero() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap();

    let read = runtime
        .read("missing", ReadConsistency::ReadBarrier)
        .unwrap();

    assert_eq!(read.value, None);
    assert_eq!(read.applied_index, 0);
    assert_eq!(read.hash, LogHash::ZERO);
    assert_eq!(runtime.log_store().last_index().unwrap(), None);
}

#[test]
fn read_barrier_inspects_after_an_identical_historical_noop() {
    let dir = tempfile::tempdir().unwrap();
    let shared_consensus = consensus(dir.path());
    let runtime =
        NodeRuntime::open(node_config(dir.path()), Arc::clone(&shared_consensus), &[]).unwrap();
    let written = runtime.write("request-1", "alpha", "one").unwrap();
    let historical = shared_consensus
        .propose_at(
            written.applied_index + 1,
            written.hash,
            Command::new(CommandKind::ReadBarrier, Vec::new()),
        )
        .unwrap();

    let read = runtime.read("alpha", ReadConsistency::ReadBarrier).unwrap();

    assert_eq!(historical.entry_type, EntryType::Noop);
    assert!(historical.payload.is_empty());
    assert_eq!(read.value.as_deref(), Some("one"));
    assert_eq!(read.applied_index, historical.index);
    assert_eq!(read.hash, historical.hash);
    assert_eq!(
        runtime.log_store().read(historical.index).unwrap(),
        Some(historical.clone())
    );
    assert_eq!(
        runtime.log_store().read(historical.index + 1).unwrap(),
        None
    );
    assert_eq!(runtime.applied_index().unwrap(), read.applied_index);
    assert_eq!(runtime.applied_hash().unwrap(), read.hash);
    for index in 1..=read.applied_index {
        assert!(
            runtime.log_store().read(index).unwrap().is_some(),
            "read barrier left a gap at qlog index {index}"
        );
    }
}

#[derive(Clone)]
struct FenceFaultRecorder {
    inner: RecorderFileStore,
    record_failure: bool,
    read_fence_failure: Option<rhiza_quepaxa::Error>,
}

impl RecorderRpc for FenceFaultRecorder {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        self.inner.recorder_id()
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
        RecorderRpc::store_command_for(
            &self.inner,
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
        RecorderRpc::fetch_command_for(
            &self.inner,
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
        )
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        if self.record_failure {
            return Err(rhiza_quepaxa::Error::ProposeFailed);
        }
        RecorderRpc::record(&self.inner, request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        RecorderRpc::install_decision_proof(&self.inner, proof, membership)
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        RecorderRpc::inspect_decision_proof(&self.inner, slot)
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        RecorderRpc::inspect_record_summary(&self.inner, slot)
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<rhiza_quepaxa::ReadFenceObservation> {
        if let Some(error) = self.read_fence_failure.clone() {
            return Err(error);
        }
        RecorderRpc::observe_read_fence(&self.inner, request)
    }
}

fn consensus_with_fence_failures(root: &Path, unavailable: usize) -> Arc<ThreeNodeConsensus> {
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let recorders = ["node-1", "node-2", "node-3"]
        .into_iter()
        .enumerate()
        .map(|(index, recorder_id)| {
            let inner = RecorderFileStore::new_with_membership(
                root.join("fence-fault-recorders").join(recorder_id),
                recorder_id,
                "rhiza:sql:cluster-a",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (
                recorder_id.to_string(),
                Box::new(FenceFaultRecorder {
                    inner,
                    record_failure: false,
                    read_fence_failure: (index < unavailable)
                        .then(|| rhiza_quepaxa::Error::Io("injected read-fence outage".into())),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            recorders,
        )
        .unwrap(),
    )
}

fn consensus_with_explicit_quorum_failures(
    root: &Path,
    failed: usize,
    fail_records: bool,
) -> Arc<ThreeNodeConsensus> {
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let recorders = ["node-1", "node-2", "node-3"]
        .into_iter()
        .enumerate()
        .map(|(index, recorder_id)| {
            let inner = RecorderFileStore::new_with_membership(
                root.join("explicit-quorum-fault-recorders")
                    .join(recorder_id),
                recorder_id,
                "rhiza:sql:cluster-a",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            let unavailable = index < failed;
            (
                recorder_id.to_string(),
                Box::new(FenceFaultRecorder {
                    inner,
                    record_failure: unavailable && fail_records,
                    read_fence_failure: unavailable.then_some(rhiza_quepaxa::Error::ProposeFailed),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            recorders,
        )
        .unwrap(),
    )
}

#[test]
fn read_barrier_returns_current_value_with_one_unavailable_voter() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(
        node_config(dir.path()),
        consensus_with_fence_failures(dir.path(), 1),
        &[],
    )
    .unwrap();
    let written = runtime.write("request-1", "alpha", "one").unwrap();

    let read = runtime.read("alpha", ReadConsistency::ReadBarrier).unwrap();

    assert_eq!(read.value.as_deref(), Some("one"));
    assert_eq!(
        (read.applied_index, read.hash),
        (written.applied_index, written.hash)
    );
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));
}

#[test]
fn read_barrier_returns_retryable_unavailable_without_quorum() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(
        node_config(dir.path()),
        consensus_with_fence_failures(dir.path(), 2),
        &[],
    )
    .unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();

    assert!(matches!(
        runtime.read("alpha", ReadConsistency::ReadBarrier),
        Err(NodeError::Unavailable(_))
    ));
    assert_eq!(
        runtime
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
    assert!(runtime.is_ready());
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));
}

#[test]
fn explicit_read_fence_quorum_failure_is_retryable_and_does_not_latch_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(
        node_config(dir.path()),
        consensus_with_explicit_quorum_failures(dir.path(), 2, false),
        &[],
    )
    .unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();

    assert!(matches!(
        runtime.read("alpha", ReadConsistency::ReadBarrier),
        Err(NodeError::Unavailable(_))
    ));
    assert!(runtime.is_ready());
    assert!(!runtime.is_fatal());
}

#[test]
fn explicit_record_quorum_failure_is_retryable_and_does_not_latch_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(
        node_config(dir.path()),
        consensus_with_explicit_quorum_failures(dir.path(), 2, true),
        &[],
    )
    .unwrap();

    assert!(matches!(
        runtime.write("request-1", "alpha", "one"),
        Err(NodeError::Unavailable(_))
    ));
    assert!(runtime.is_ready());
    assert!(!runtime.is_fatal());
}

#[test]
fn reopened_runtime_preserves_strong_read_value_and_tip() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus(dir.path()), &[]).unwrap();
    let written = runtime.write("request-1", "alpha", "one").unwrap();
    drop(runtime);

    let reopened = NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap();
    let read = reopened
        .read("alpha", ReadConsistency::ReadBarrier)
        .unwrap();

    assert_eq!(read.value.as_deref(), Some("one"));
    assert_eq!(
        (read.applied_index, read.hash),
        (written.applied_index, written.hash)
    );
    assert_eq!(reopened.log_store().last_index().unwrap(), Some(1));
}

#[test]
fn stopped_configuration_rejects_read_barrier_without_advancing_qlog() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();
    let before = runtime.read("alpha", ReadConsistency::ReadBarrier).unwrap();
    let successor =
        Membership::from_voters(["node-1".into(), "node-2".into(), "node-3".into()]).unwrap();
    let stop = runtime
        .stop_current_configuration_for_successor(&successor)
        .unwrap();

    assert_eq!(before.value.as_deref(), Some("one"));
    assert!(matches!(
        runtime.read("alpha", ReadConsistency::ReadBarrier),
        Err(NodeError::ConfigurationTransition { .. })
    ));
    assert_eq!(
        runtime.log_store().last_index().unwrap(),
        Some(stop.entry.index)
    );
}

#[derive(Clone)]
struct ProofDroppingRecorder(RecorderFileStore);

impl RecorderRpc for ProofDroppingRecorder {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        self.0.recorder_id()
    }

    fn store_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        self.0.store_command(command_hash, command)
    }

    fn fetch_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        self.0.fetch_command(command_hash)
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        self.0.record_proposal(request)
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        // Simulate a proposer process that returned a phase-2 decision before
        // its asynchronous proof dissemination survived a crash.
        Ok(())
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        RecorderRpc::inspect_decision_proof(&self.0, slot)
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        RecorderRpc::inspect_record_summary(&self.0, slot)
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<rhiza_quepaxa::ReadFenceObservation> {
        RecorderRpc::observe_read_fence(&self.0, request)
    }
}

#[test]
fn read_barrier_catches_up_past_a_historical_phase2_noop_without_an_installed_proof() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let recorders = ["node-1", "node-2", "node-3"].map(|recorder_id| {
        ProofDroppingRecorder(
            RecorderFileStore::new_with_membership(
                dir.path().join("phase2-recorders").join(recorder_id),
                recorder_id,
                "rhiza:sql:cluster-a",
                1,
                1,
                membership.clone(),
            )
            .unwrap(),
        )
    });
    let consensus_for = |proposer_id: &str| {
        Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "rhiza:sql:cluster-a",
                proposer_id,
                1,
                1,
                ["node-1", "node-2", "node-3"]
                    .into_iter()
                    .zip(recorders.iter().cloned())
                    .map(|(recorder_id, recorder)| {
                        (
                            recorder_id.to_string(),
                            Box::new(recorder) as Box<dyn RecorderRpc>,
                        )
                    })
                    .collect(),
            )
            .unwrap(),
        )
    };
    let writer_consensus = consensus_for("node-2");
    let reader_consensus = consensus_for("node-1");
    let writer_root = dir.path().join("writer");
    let reader_root = dir.path().join("reader");
    let writer = NodeRuntime::open(
        node_config(&writer_root),
        Arc::clone(&writer_consensus),
        &[],
    )
    .unwrap();
    let reader = NodeRuntime::open(node_config(&reader_root), reader_consensus, &[]).unwrap();

    let historical = writer_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::ReadBarrier, Vec::new()),
        )
        .unwrap();
    assert!(matches!(
        &historical,
        LogEntry {
            entry_type: EntryType::Noop,
            payload,
            ..
        } if payload.is_empty()
    ));
    assert!(recorders
        .iter()
        .all(|recorder| recorder.inspect_decision_proof(1).unwrap().is_none()));

    let written = writer.write("request-1", "alpha", "one").unwrap();
    assert!(written.applied_index > historical.index);

    let read = reader.read("alpha", ReadConsistency::ReadBarrier).unwrap();

    assert_eq!(read.value.as_deref(), Some("one"));
    assert!(read.applied_index >= written.applied_index);
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_http_write_and_read_use_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let runtime = Arc::new(NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("served-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();

    let write = client
        .post(format!("http://{addr}/v1/write"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&WriteRequest {
            request_id: "request-http".into(),
            key: "alpha".into(),
            value: "http".into(),
        })
        .send()
        .await
        .unwrap();
    assert!(write.status().is_success());

    let read = client
        .post(format!("http://{addr}/v1/read"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&ReadRequest {
            key: "alpha".into(),
            consistency: Some(ReadConsistency::Local),
        })
        .send()
        .await
        .unwrap();

    assert!(read.status().is_success());
    assert!(read.text().await.unwrap().contains("http"));
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_http_replays_fts5_write_and_read_barrier_exposes_match_results() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("sql-http-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let command = SqlExecuteRequest {
        request_id: "fts-http-write".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE VIRTUAL TABLE documents USING fts5(body)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO documents(body) VALUES (?1)".into(),
                parameters: vec![SqlValue::Text("leaderless sqlite search".into())],
            },
        ],
    };

    let execute = || {
        client
            .post(format!("http://{addr}{SQL_EXECUTE_PATH}"))
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .bearer_auth("client-token")
            .json(&command)
            .send()
    };
    let first = execute().await.unwrap();
    assert!(first.status().is_success());
    let first = first.json::<SqlExecuteResponse>().await.unwrap();

    let replay = execute().await.unwrap();
    assert!(replay.status().is_success());
    assert_eq!(replay.json::<SqlExecuteResponse>().await.unwrap(), first);

    let query = client
        .post(format!("http://{addr}{SQL_QUERY_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&SqlQueryRequest {
            statement: SqlStatement {
                sql: "SELECT rowid, body FROM documents WHERE documents MATCH ?1".into(),
                parameters: vec![SqlValue::Text("sqlite".into())],
            },
            consistency: Some(ReadConsistency::ReadBarrier),
            max_rows: Some(10),
        })
        .send()
        .await
        .unwrap();
    assert!(query.status().is_success());
    let query = query.json::<SqlQueryResponse>().await.unwrap();

    assert_eq!(query.columns, ["rowid", "body"]);
    assert_eq!(
        query.rows,
        [vec![
            SqlValue::Integer(1),
            SqlValue::Text("leaderless sqlite search".into())
        ]]
    );
    assert!(query.applied_index >= first.applied_index);
    server.abort();
}

#[test]
fn sql_batch_commits_one_qwal_entry_and_replays_results() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    runtime
        .execute_sql(SqlCommand {
            request_id: "batch-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE batched(id INTEGER PRIMARY KEY, value TEXT UNIQUE)".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    let request = |request_id: &str, value: &str| SqlCommand {
        request_id: request_id.into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO batched(value) VALUES (?1) RETURNING id, value".into(),
            parameters: vec![SqlValue::Text(value.into())],
        }],
    };

    let commands = [
        request("sql-batch-a", "alpha"),
        request("sql-batch-b", "beta"),
    ];
    let results = runtime.execute_sql_batch(commands.to_vec()).unwrap();
    let first = results[0].as_ref().unwrap();
    let second = results[1].as_ref().unwrap();

    assert_eq!(first.applied_index, 2);
    assert_eq!(second.applied_index, 2);
    assert_eq!(first.hash, second.hash);
    assert!(runtime
        .log_store()
        .read(2)
        .unwrap()
        .unwrap()
        .payload
        .starts_with(QWAL_V3_MAGIC));
    assert_eq!(first.results.len(), 1);
    assert_eq!(second.results.len(), 1);
    assert_ne!(first.results[0].returning, second.results[0].returning);

    let last_index = runtime.log_store().last_index().unwrap();
    let replay = runtime
        .execute_sql_batch(vec![commands[0].clone()])
        .unwrap()
        .remove(0)
        .unwrap();
    assert_eq!(&replay, first);
    assert_eq!(runtime.log_store().last_index().unwrap(), last_index);

    let duplicate = request("sql-batch-duplicate", "gamma");
    let duplicate_results = runtime
        .execute_sql_batch(vec![duplicate.clone(), duplicate])
        .unwrap();
    let duplicate_first = duplicate_results[0].as_ref().unwrap();
    let duplicate_second = duplicate_results[1].as_ref().unwrap();
    assert_eq!(duplicate_first, duplicate_second);
    assert_eq!(
        runtime.log_store().last_index().unwrap(),
        last_index.map(|index| index + 1)
    );
}

#[test]
fn four_member_sql_batch_commits_one_exact_base_qwal_entry() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    runtime
        .execute_sql(SqlCommand {
            request_id: "byte-cap-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE byte_cap(id INTEGER PRIMARY KEY, ordinal INTEGER UNIQUE, value TEXT)"
                    .into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    let value = "x".repeat(1024);
    let commands = (0..4)
        .map(|ordinal| SqlCommand {
            request_id: format!("byte-cap-{ordinal}"),
            statements: vec![SqlStatement {
                sql: "INSERT INTO byte_cap(ordinal, value) VALUES (?1, ?2)".into(),
                parameters: vec![SqlValue::Integer(ordinal), SqlValue::Text(value.clone())],
            }],
        })
        .collect::<Vec<_>>();
    let results = runtime.execute_sql_batch(commands.clone()).unwrap();
    let responses = commands
        .iter()
        .map(|command| command.request_id.clone())
        .zip(results)
        .map(|(request_id, result)| (request_id, result.unwrap()))
        .collect::<HashMap<_, _>>();

    let indices = responses
        .values()
        .map(|response| response.applied_index)
        .collect::<Vec<_>>();
    assert_eq!(indices, vec![2; 4]);
    let hashes = responses
        .values()
        .map(|response| response.hash)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(hashes.len(), 1);
    assert!(runtime
        .log_store()
        .read(2)
        .unwrap()
        .unwrap()
        .payload
        .starts_with(QWAL_V3_MAGIC));
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(2));

    for command in commands {
        let original = responses.get(&command.request_id).unwrap();
        assert_eq!(original.results[0].rows_affected, 1);
        assert_eq!(original.results[0].returning, None);
        let replay = runtime
            .execute_sql_batch(vec![command])
            .unwrap()
            .remove(0)
            .unwrap();
        assert_eq!(&replay, original);
    }
    let ordered = runtime
        .query_sql(
            &SqlStatement {
                sql: "SELECT ordinal FROM byte_cap ORDER BY ordinal".into(),
                parameters: vec![],
            },
            ReadConsistency::Local,
            10,
        )
        .unwrap();
    assert_eq!(
        ordered.rows,
        (0..4)
            .map(|ordinal| vec![SqlValue::Integer(ordinal)])
            .collect::<Vec<_>>()
    );
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(2));
}

#[test]
fn oversized_sql_batch_halves_until_each_qwal_effect_fits() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    runtime
        .execute_sql(SqlCommand {
            request_id: "split-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE split_batch(id INTEGER PRIMARY KEY, value BLOB NOT NULL)".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    let commands = (0..4)
        .map(|id| SqlCommand {
            request_id: format!("split-{id}"),
            statements: vec![SqlStatement {
                sql: "INSERT INTO split_batch(id, value) VALUES (?1, randomblob(140000))".into(),
                parameters: vec![SqlValue::Integer(id)],
            }],
        })
        .collect::<Vec<_>>();

    let results = runtime.execute_sql_batch(commands).unwrap();
    let indices = results
        .iter()
        .map(|result| result.as_ref().unwrap().applied_index)
        .collect::<Vec<_>>();

    assert_eq!(indices, vec![2, 2, 3, 3]);
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(3));
    assert!(runtime.log_store().read(2).unwrap().unwrap().payload.len() <= MAX_COMMAND_BYTES);
    assert!(runtime.log_store().read(3).unwrap().unwrap().payload.len() <= MAX_COMMAND_BYTES);
}

#[test]
fn sql_typed_batch_accepts_256_members_and_rejects_257_before_writing() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    runtime
        .execute_sql(SqlCommand {
            request_id: "limit-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE batch_limit(id INTEGER PRIMARY KEY)".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    let command = |id| SqlCommand {
        request_id: format!("limit-{id}"),
        statements: vec![SqlStatement {
            sql: "INSERT INTO batch_limit(id) VALUES (?1)".into(),
            parameters: vec![SqlValue::Integer(id)],
        }],
    };

    let results = runtime
        .execute_sql_batch((0..256).map(command).collect())
        .unwrap();

    assert_eq!(results.len(), 256);
    assert!(results.iter().all(Result::is_ok));
    assert!(results
        .iter()
        .all(|result| result.as_ref().unwrap().applied_index == 2));
    let last_index = runtime.log_store().last_index().unwrap();
    let error = runtime
        .execute_sql_batch((256..513).map(command).collect())
        .unwrap_err();
    assert!(matches!(error, NodeError::InvalidRequest(_)));
    assert_eq!(runtime.log_store().last_index().unwrap(), last_index);
}

#[test]
fn failed_sql_member_does_not_prevent_a_valid_request_in_the_same_batch() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    runtime
        .execute_sql(SqlCommand {
            request_id: "partial-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE partial(value TEXT UNIQUE)".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    runtime
        .execute_sql(SqlCommand {
            request_id: "partial-existing".into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO partial(value) VALUES ('existing')".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    let request = |request_id: &str, value: &str| SqlCommand {
        request_id: request_id.into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO partial(value) VALUES (?1)".into(),
            parameters: vec![SqlValue::Text(value.into())],
        }],
    };

    let results = runtime
        .execute_sql_batch(vec![
            request("partial-invalid", "existing"),
            request("partial-valid", "fresh"),
        ])
        .unwrap();

    assert!(matches!(
        results[0],
        Err(NodeError::InvalidSqlStatement { .. }) | Err(NodeError::InvalidRequest(_))
    ));
    assert_eq!(results[1].as_ref().unwrap().applied_index, 3);
}

#[test]
fn sql_exact_winner_fast_completion_preserves_retry_conflict_and_failure_results() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    runtime
        .execute_sql(SqlCommand {
            request_id: "fast-complete-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE fast_complete(id INTEGER PRIMARY KEY, value TEXT UNIQUE)".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    let stored_command = SqlCommand {
        request_id: "fast-complete-stored".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO fast_complete(id, value) VALUES (1, 'stored')".into(),
            parameters: vec![],
        }],
    };
    let stored = runtime.execute_sql(stored_command.clone()).unwrap();
    let valid = SqlCommand {
        request_id: "fast-complete-valid".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO fast_complete(id, value) VALUES (2, 'valid') RETURNING id, value"
                .into(),
            parameters: vec![],
        }],
    };
    let conflicting = SqlCommand {
        request_id: stored_command.request_id.clone(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO fast_complete(id, value) VALUES (3, 'conflict')".into(),
            parameters: vec![],
        }],
    };
    let failed = SqlCommand {
        request_id: "fast-complete-failed".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO fast_complete(id, value) VALUES (4, 'stored')".into(),
            parameters: vec![],
        }],
    };

    let results = runtime
        .execute_sql_batch(vec![
            stored_command,
            conflicting,
            failed,
            valid.clone(),
            valid,
        ])
        .unwrap();

    assert_eq!(
        results[0].as_ref().unwrap().applied_index,
        stored.applied_index
    );
    assert_eq!(results[0].as_ref().unwrap().hash, stored.hash);
    assert!(matches!(results[1], Err(NodeError::RequestConflict(_))));
    assert!(matches!(
        results[2],
        Err(NodeError::InvalidSqlStatement { .. })
    ));
    let exact = results[3].as_ref().unwrap();
    assert_eq!(exact.applied_index, 3);
    assert_eq!(results[4].as_ref().unwrap(), exact);
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(3));
}

#[test]
fn sql_bulk_precheck_aligns_reverse_conflict_retry_absent_failure_and_alias() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    runtime
        .execute_sql(SqlCommand {
            request_id: "bulk-aligned-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE bulk_aligned(id INTEGER PRIMARY KEY, value TEXT UNIQUE)".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    let stored_command = SqlCommand {
        request_id: "bulk-aligned-stored".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO bulk_aligned(id, value) VALUES (1, 'stored')".into(),
            parameters: vec![],
        }],
    };
    let stored = runtime.execute_sql(stored_command.clone()).unwrap();
    let conflicting = SqlCommand {
        request_id: stored_command.request_id.clone(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO bulk_aligned(id, value) VALUES (2, 'conflict')".into(),
            parameters: vec![],
        }],
    };
    let valid = SqlCommand {
        request_id: "bulk-aligned-valid".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO bulk_aligned(id, value) VALUES (3, 'valid') RETURNING id, value"
                .into(),
            parameters: vec![],
        }],
    };
    let failed = SqlCommand {
        request_id: "bulk-aligned-failed".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO bulk_aligned(id, value) VALUES (4, 'stored')".into(),
            parameters: vec![],
        }],
    };

    let results = runtime
        .execute_sql_batch(vec![
            conflicting,
            stored_command,
            valid.clone(),
            failed,
            valid,
        ])
        .unwrap();

    assert!(matches!(results[0], Err(NodeError::RequestConflict(_))));
    assert_eq!(
        results[1].as_ref().unwrap().applied_index,
        stored.applied_index
    );
    assert_eq!(results[1].as_ref().unwrap().hash, stored.hash);
    assert_eq!(results[2].as_ref().unwrap().applied_index, 3);
    assert!(matches!(
        results[3],
        Err(NodeError::InvalidSqlStatement { .. })
    ));
    assert_eq!(results[4], results[2]);
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(3));
}

#[cfg(unix)]
#[test]
fn sql_exact_winner_is_not_returned_when_materializer_apply_fails() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(Gate::default());
    let recorders = (1..=3)
        .map(|index| {
            Box::new(GatedRecorder::new(format!("node-{index}"), gate.clone()))
                as Box<dyn RecorderRpc>
        })
        .collect();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders("rhiza:sql:cluster-a", "node-1", 1, 1, recorders)
            .unwrap(),
    );
    let runtime = Arc::new(NodeRuntime::open(node_config(dir.path()), consensus, &[]).unwrap());
    let worker_runtime = runtime.clone();
    let worker = std::thread::spawn(move || {
        worker_runtime.execute_sql_batch(vec![SqlCommand {
            request_id: "apply-must-succeed".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE apply_must_succeed(id INTEGER PRIMARY KEY)".into(),
                parameters: vec![],
            }],
        }])
    });
    gate.wait_until_started();
    let sqlite_dir = dir.path().join("sqlite");
    let original_permissions = std::fs::metadata(&sqlite_dir).unwrap().permissions();
    std::fs::set_permissions(&sqlite_dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    gate.release();
    let outcome = worker.join();
    std::fs::set_permissions(&sqlite_dir, original_permissions).unwrap();

    let error = outcome.unwrap().unwrap_err();
    assert!(matches!(error, NodeError::Fatal(_)));
    assert!(runtime.is_fatal());
}

#[tokio::test(flavor = "multi_thread")]
async fn log_fetch_saturation_does_not_consume_recorder_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(Gate::default());
    let log_peer = BlockingLogPeer { gate: gate.clone() };
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("separate-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let peers = peer_configs(["http://unused"; 3]);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            node_rpc_router_with_limits(recorder, log_peer, peers, 1, 1),
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::new();
    let fetch_url = format!("http://{addr}/v1/log/fetch");
    let blocking_client = client.clone();
    let blocking_fetch = tokio::spawn(async move {
        blocking_client
            .post(fetch_url)
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .header(NODE_ID_HEADER, "node-1")
            .header(RECOVERY_GENERATION_HEADER, "1")
            .bearer_auth("peer-token-1")
            .json(&FetchLogRequest {
                from_index: 1,
                max_entries: 1,
            })
            .send()
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(3);
    while !gate.started() {
        assert!(
            Instant::now() < deadline,
            "blocking log fetch did not start"
        );
        tokio::task::yield_now().await;
    }

    let saturated_fetch = client
        .post(format!("http://{addr}/v1/log/fetch"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&FetchLogRequest {
            from_index: 1,
            max_entries: 1,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(
        saturated_fetch.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS
    );

    let recorder_response = client
        .post(format!("http://{addr}{RECORDER_IDENTITY_PATH}"))
        .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&serde_json::json!({"version": 3, "body": null}))
        .send()
        .await
        .unwrap();
    assert!(recorder_response.status().is_success());

    gate.release();
    assert!(blocking_fetch.await.unwrap().unwrap().status().is_success());
    server.abort();
}

#[test]
fn cancelled_peer_recovery_is_joined_before_open_returns_without_persisting() {
    let root = tempfile::tempdir().unwrap();
    let config = node_config(&root.path().join("node"));
    let consensus = consensus(&root.path().join("consensus"));
    let gate = Arc::new(Gate::default());
    let peer = BlockingLogPeer {
        gate: Arc::clone(&gate),
    };
    let startup = StartupIoContext::new();
    let attempt_startup = startup.clone();
    let attempt = std::thread::spawn(move || {
        NodeRuntime::open_cancellable(config, consensus, &[&peer], &attempt_startup)
    });

    gate.wait_until_started();
    startup.cancel(Instant::now() + Duration::from_millis(10));
    std::thread::sleep(Duration::from_millis(20));
    assert!(
        !attempt.is_finished(),
        "noncooperative peer recovery must remain owned until the call exits"
    );
    gate.release();

    let error = attempt.join().unwrap().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("startup cancelled during peer log fetch"),
        "{error}"
    );
    let log = FileLogStore::open_with_configuration(
        root.path().join("node/consensus/log"),
        "rhiza:sql:cluster-a",
        1,
        ConfigurationState::active(1, test_config_digest()),
    )
    .unwrap();
    assert_eq!(log.last_index().unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn recorder_backend_errors_return_non_success_statuses() {
    let failures = [
        (
            rhiza_quepaxa::Error::Io("recorder unavailable".into()),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            rhiza_quepaxa::Error::ConflictingCertificates,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ];
    for (error, expected_status) in failures {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                recorder_router(
                    FailingRecorder { error },
                    peer_configs(["http://unused"; 3]),
                ),
            )
            .await
            .unwrap();
        });
        let response = reqwest::Client::new()
            .post(format!("http://{addr}{RECORDER_IDENTITY_PATH}"))
            .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
            .header(NODE_ID_HEADER, "node-1")
            .header(RECOVERY_GENERATION_HEADER, "1")
            .bearer_auth("peer-token-1")
            .json(&serde_json::json!({"version": 3, "body": null}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected_status);
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn log_backend_errors_are_non_success_and_preserved_by_http_peer() {
    let failures = [
        (
            FetchLogError::Transport {
                message: "log unavailable".into(),
            },
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            FetchLogError::InvalidEntry {
                index: 1,
                message: "corrupt committed file".into(),
            },
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ];
    for (expected_error, expected_status) in failures {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_error = expected_error.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                log_peer_router(
                    FailingLogPeer {
                        error: server_error,
                    },
                    peer_configs(["http://unused"; 3]),
                ),
            )
            .await
            .unwrap();
        });
        let base_url = format!("http://{addr}");
        let direct = reqwest::Client::new()
            .post(format!("{base_url}/v1/log/fetch"))
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .header(NODE_ID_HEADER, "node-1")
            .header(RECOVERY_GENERATION_HEADER, "1")
            .bearer_auth("peer-token-1")
            .json(&FetchLogRequest {
                from_index: 1,
                max_entries: 1,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(direct.status(), expected_status);

        let peer = HttpLogPeer::new(base_url, "node-1", "peer-token-1").unwrap();
        let returned = tokio::task::spawn_blocking(move || {
            peer.fetch_log(FetchLogRequest {
                from_index: 1,
                max_entries: 1,
            })
        })
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(returned, expected_error);
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn non_json_non_success_log_response_is_transient_transport() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().fallback(|| async {
                (axum::http::StatusCode::NOT_FOUND, "peer is still starting")
            }),
        )
        .await
        .unwrap();
    });
    let peer = HttpLogPeer::new(format!("http://{addr}"), "node-1", "peer-token-1").unwrap();
    let returned = tokio::task::spawn_blocking(move || {
        peer.fetch_log(FetchLogRequest {
            from_index: 1,
            max_entries: 1,
        })
    })
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(
        returned,
        FetchLogError::Transport { message } if message.contains("HTTP 404")
    ));
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn unauthenticated_invalid_bodies_are_rejected_before_body_or_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("auth-order-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router_with_limits(runtime, recorder, 0, 0))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();

    for path in ["/v1/write", RECORDER_IDENTITY_PATH, "/v1/log/fetch"] {
        let malformed = client
            .post(format!("http://{addr}{path}"))
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await
            .unwrap();
        assert_eq!(malformed.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    let oversized_status = tokio::task::spawn_blocking(move || {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        write!(
            stream,
            "POST /v1/write HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            MAX_HTTP_BODY_BYTES + 1
        )
        .unwrap();
        stream.flush().unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response.lines().next().unwrap_or_default().to_string()
    })
    .await
    .unwrap();
    assert!(oversized_status.contains(" 401 "), "{oversized_status}");

    let authenticated_malformed = client
        .post(format!("http://{addr}/v1/write"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(
        authenticated_malformed.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS
    );
    let valid_recorder = client
        .post(format!("http://{addr}{RECORDER_IDENTITY_PATH}"))
        .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&serde_json::json!({"version": 3, "body": null}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        valid_recorder.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS
    );
    let valid_fetch = client
        .post(format!("http://{addr}/v1/log/fetch"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&FetchLogRequest {
            from_index: 1,
            max_entries: 1,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(valid_fetch.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    server.abort();
}

fn node_config(root: &Path) -> NodeConfig {
    NodeConfig::new(
        "rhiza:sql:cluster-a",
        "node-1",
        root.to_path_buf(),
        1,
        1,
        peer_configs(["http://node-1", "http://node-2", "http://node-3"]),
        "client-token",
    )
    .unwrap()
}

fn peer_configs(urls: [&str; 3]) -> [PeerConfig; 3] {
    [
        PeerConfig::new("node-1", urls[0], "peer-token-1").unwrap(),
        PeerConfig::new("node-2", urls[1], "peer-token-2").unwrap(),
        PeerConfig::new("node-3", urls[2], "peer-token-3").unwrap(),
    ]
}

fn consensus(root: &Path) -> Arc<ThreeNodeConsensus> {
    consensus_named(root, "recorders")
}

async fn assert_client_error(
    response: reqwest::Response,
    status: reqwest::StatusCode,
    code: &str,
    retryable: bool,
    statement_index: Option<usize>,
) {
    assert_eq!(response.status(), status);
    assert_eq!(
        response.headers()[reqwest::header::CONTENT_TYPE],
        "application/json"
    );
    let body = response.json::<ClientErrorResponse>().await.unwrap();
    assert_eq!(body.code, code);
    assert_eq!(body.retryable, retryable);
    assert!(!body.message.is_empty());
    assert_eq!(body.statement_index, statement_index);
}

fn consensus_named(root: &Path, recorder_dir: &str) -> Arc<ThreeNodeConsensus> {
    Arc::new(
        ThreeNodeConsensus::from_recovered_tip(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            [
                root.join(recorder_dir).join("node-1"),
                root.join(recorder_dir).join("node-2"),
                root.join(recorder_dir).join("node-3"),
            ],
            1,
            LogHash::ZERO,
        )
        .unwrap(),
    )
}

fn first_qwal_put_entry(root: &Path, request_id: &str, key: &str, value: &str) -> LogEntry {
    let state = SqliteStateMachine::open(
        root.join("db.sqlite"),
        "rhiza:sql:cluster-a",
        "qwal-preparer",
        1,
        1,
    )
    .unwrap();
    let request_payload = format!("put\t{request_id}\t{key}\t{value}").into_bytes();
    let payload = state
        .prepare_put_effect(request_id, key, value, &request_payload, 0, LogHash::ZERO)
        .unwrap();
    assert!(payload.starts_with(QWAL_V3_MAGIC));
    entry(1, LogHash::ZERO, &payload)
}

fn entry(index: u64, prev_hash: LogHash, payload: &[u8]) -> LogEntry {
    LogEntry {
        cluster_id: "rhiza:sql:cluster-a".into(),
        epoch: 1,
        config_id: 1,
        index,
        entry_type: EntryType::Command,
        payload: payload.to_vec(),
        prev_hash,
        hash: LogEntry::calculate_hash(
            "rhiza:sql:cluster-a",
            index,
            1,
            1,
            EntryType::Command,
            prev_hash,
            payload,
        ),
    }
}

struct OneResponsePeer {
    entries: Vec<LogEntry>,
}

impl OneResponsePeer {
    fn new(entries: Vec<LogEntry>) -> Self {
        Self { entries }
    }
}

impl LogPeer for OneResponsePeer {
    fn fetch_log(&self, _request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        Ok(FetchLogResponse {
            last_index: self.entries.last().map_or(0, |entry| entry.index),
            entries: self.entries.clone(),
        })
    }
}

struct OrderingRecorder {
    id: String,
    commands: Mutex<HashMap<LogHash, StoredCommand>>,
    isr: Mutex<HashMap<u64, IsrState>>,
    proofs: Mutex<HashMap<u64, DecisionProof>>,
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl OrderingRecorder {
    fn new(id: String, calls: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            id,
            commands: Mutex::new(HashMap::new()),
            isr: Mutex::new(HashMap::new()),
            proofs: Mutex::new(HashMap::new()),
            calls,
        }
    }
}

impl RecorderRpc for OrderingRecorder {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        Ok(self.id.clone())
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
        assert_test_recorder_context(&cluster_id, epoch, config_id, config_digest);
        assert_eq!(command.hash(), command_hash);
        self.commands.lock().unwrap().insert(command_hash, command);
        self.calls.lock().unwrap().push("store");
        Ok(())
    }

    fn fetch_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        assert_test_recorder_context(&cluster_id, epoch, config_id, config_digest);
        Ok(self.commands.lock().unwrap().get(&command_hash).cloned())
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        assert_test_recorder_context(
            &request.cluster_id,
            request.epoch,
            request.config_id,
            request.config_digest,
        );
        if let Some(command) = request.command.as_ref() {
            let value = request.proposal.value.as_ref().unwrap();
            assert_eq!(command.hash(), value.command_hash);
            self.commands
                .lock()
                .unwrap()
                .insert(command.hash(), command.clone());
            self.calls.lock().unwrap().push("piggyback");
        }
        assert!(request.proposal.value.as_ref().is_none_or(|value| self
            .commands
            .lock()
            .unwrap()
            .contains_key(&value.command_hash)));
        self.calls.lock().unwrap().push("record");
        let mut states = self.isr.lock().unwrap();
        let current = states.entry(request.slot).or_default().clone();
        let (next, reply) = current.record(request.step, request.proposal);
        states.insert(request.slot, next);
        Ok(RecordSummary {
            recorder_id: self.id.clone(),
            slot: request.slot,
            config_id: 1,
            config_digest: test_config_digest(),
            step: reply.step,
            first_current: reply.first_current,
            aggregate_prior: reply.aggregate_prior,
            decided: self.proofs.lock().unwrap().get(&request.slot).cloned(),
        })
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        _membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        let slot = match &proof {
            DecisionProof::FastPath { slot, .. } | DecisionProof::Phase2 { slot, .. } => *slot,
        };
        self.proofs.lock().unwrap().insert(slot, proof);
        Ok(())
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        Ok(self.proofs.lock().unwrap().get(&slot).cloned())
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        let states = self.isr.lock().unwrap();
        let Some(state) = states.get(&slot) else {
            return Ok(None);
        };
        Ok(Some(RecordSummary {
            recorder_id: self.id.clone(),
            slot,
            config_id: 1,
            config_digest: test_config_digest(),
            step: state.step(),
            first_current: state.first_current().cloned(),
            aggregate_prior: state.aggregate_prior().cloned(),
            decided: self.proofs.lock().unwrap().get(&slot).cloned(),
        }))
    }
}

#[derive(Clone)]
struct BlockingLogPeer {
    gate: Arc<Gate>,
}

impl LogPeer for BlockingLogPeer {
    fn fetch_log(&self, _request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        self.gate.wait();
        Ok(FetchLogResponse {
            entries: Vec::new(),
            last_index: 0,
        })
    }
}

#[derive(Clone)]
struct FailingRecorder {
    error: rhiza_quepaxa::Error,
}

impl RecorderRpc for FailingRecorder {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        Err(self.error.clone())
    }
}

#[derive(Clone)]
struct FailingLogPeer {
    error: FetchLogError,
}

impl LogPeer for FailingLogPeer {
    fn fetch_log(&self, _request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        Err(self.error.clone())
    }
}

#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Default)]
struct GateState {
    started: bool,
    released: bool,
}

impl Gate {
    fn wait(&self) {
        let mut state = self.state.lock().unwrap();
        state.started = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn started(&self) -> bool {
        self.state.lock().unwrap().started
    }

    fn wait_until_started(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut state = self.state.lock().unwrap();
        while !state.started {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .expect("recorder call did not reach the test gate");
            let (next, timeout) = self.changed.wait_timeout(state, remaining).unwrap();
            assert!(
                !timeout.timed_out(),
                "recorder call did not reach the test gate"
            );
            state = next;
        }
    }

    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}

struct GatedRecorder {
    id: String,
    commands: Mutex<HashMap<LogHash, StoredCommand>>,
    isr: Mutex<HashMap<u64, IsrState>>,
    proofs: Mutex<HashMap<u64, DecisionProof>>,
    gate: Arc<Gate>,
}

impl GatedRecorder {
    fn new(id: String, gate: Arc<Gate>) -> Self {
        Self {
            id,
            commands: Mutex::new(HashMap::new()),
            isr: Mutex::new(HashMap::new()),
            proofs: Mutex::new(HashMap::new()),
            gate,
        }
    }
}

impl RecorderRpc for GatedRecorder {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        Ok(self.id.clone())
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
        assert_test_recorder_context(&cluster_id, epoch, config_id, config_digest);
        self.gate.wait();
        self.commands.lock().unwrap().insert(command_hash, command);
        Ok(())
    }

    fn fetch_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        assert_test_recorder_context(&cluster_id, epoch, config_id, config_digest);
        Ok(self.commands.lock().unwrap().get(&command_hash).cloned())
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        assert_test_recorder_context(
            &request.cluster_id,
            request.epoch,
            request.config_id,
            request.config_digest,
        );
        if let Some(command) = request.command.as_ref() {
            let value = request.proposal.value.as_ref().unwrap();
            assert_eq!(command.hash(), value.command_hash);
            self.commands
                .lock()
                .unwrap()
                .insert(command.hash(), command.clone());
        }
        self.gate.wait();
        let mut states = self.isr.lock().unwrap();
        let current = states.entry(request.slot).or_default().clone();
        let (next, reply) = current.record(request.step, request.proposal);
        states.insert(request.slot, next);
        Ok(RecordSummary {
            recorder_id: self.id.clone(),
            slot: request.slot,
            config_id: 1,
            config_digest: test_config_digest(),
            step: reply.step,
            first_current: reply.first_current,
            aggregate_prior: reply.aggregate_prior,
            decided: self.proofs.lock().unwrap().get(&request.slot).cloned(),
        })
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        _membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        let slot = match &proof {
            DecisionProof::FastPath { slot, .. } | DecisionProof::Phase2 { slot, .. } => *slot,
        };
        self.proofs.lock().unwrap().insert(slot, proof);
        Ok(())
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        Ok(self.proofs.lock().unwrap().get(&slot).cloned())
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        let states = self.isr.lock().unwrap();
        let Some(state) = states.get(&slot) else {
            return Ok(None);
        };
        Ok(Some(RecordSummary {
            recorder_id: self.id.clone(),
            slot,
            config_id: 1,
            config_digest: test_config_digest(),
            step: state.step(),
            first_current: state.first_current().cloned(),
            aggregate_prior: state.aggregate_prior().cloned(),
            decided: self.proofs.lock().unwrap().get(&slot).cloned(),
        }))
    }
}

#[test]
fn in_memory_peer_still_supports_bounded_happy_path() {
    let first = entry(1, LogHash::ZERO, b"put\tr1\talpha\tone");
    let second = entry(2, first.hash, b"put\tr2\talpha\ttwo");
    let peer = InMemoryLogPeer::new(vec![first.clone(), second.clone()]);

    assert_eq!(
        catch_up_missing_entries(1, first.hash, "rhiza:sql:cluster-a", 1, 1, &peer, 10).unwrap(),
        vec![second]
    );
}
