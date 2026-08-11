#![cfg(feature = "kv")]

use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_core::{ExecutionProfile, LogHash};
use rhiza_kv::{encode_replicated_kv_command, RedbStateMachine, MAX_KV_VALUE_BYTES};
use rhiza_log::LogStore;
use rhiza_node::{
    node_router, node_router_with_checkpoint_and_limits, CheckpointCoordinator,
    ClientErrorResponse, DurabilityMode, KvCommandResultV1, KvCommandV1, KvGetResponse,
    KvMutationResponse, KvScanResponse, NodeConfig, NodeError, NodeRuntime, PeerConfig,
    ReadConsistency, KV_GET_PATH, KV_PUT_PATH, KV_SCAN_PATH, MAX_COMMAND_BYTES, MAX_KV_SCAN_ROWS,
    PROTOCOL_VERSION, READYZ_PATH, VERSION_HEADER,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{RecorderFileStore, ThreeNodeConsensus};

const CLUSTER_ID: &str = "rhiza:kv:cluster-a";

static KV_RUNTIME_TEST_LOCK: Mutex<()> = Mutex::new(());

fn kv_runtime_test_lock() -> MutexGuard<'static, ()> {
    KV_RUNTIME_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn kv_profile_reuses_node_runtime_commit_and_reopen_lifecycle() {
    let _lock = kv_runtime_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = kv_config(dir.path());
    let runtime =
        NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]).unwrap();

    let written = runtime
        .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
        .unwrap();
    let read = runtime.get_kv(b"key", ReadConsistency::Local).unwrap();

    assert_eq!(written.applied_index(), 1);
    assert_eq!(
        written.result(),
        &KvCommandResultV1::Put { replaced: false }
    );
    assert_eq!(read.value, Some(b"value".to_vec()));
    assert_eq!(
        (read.applied_index, read.hash),
        (written.applied_index(), written.hash())
    );
    assert_eq!(runtime.config().cluster_id(), CLUSTER_ID);
    drop(runtime);

    let reopened = NodeRuntime::open(config, consensus(dir.path(), "recorders"), &[]).unwrap();
    let reopened_read = reopened
        .get_kv(b"key", ReadConsistency::ReadBarrier)
        .unwrap();
    assert_eq!(reopened_read.value, Some(b"value".to_vec()));
    assert_eq!(
        (reopened_read.applied_index, reopened_read.hash),
        (written.applied_index(), written.hash())
    );
}

#[test]
fn kv_strict_commit_rehydrates_qlog_when_buffered_mirror_is_lost() {
    let _lock = kv_runtime_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = kv_config(dir.path());
    let runtime =
        NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]).unwrap();

    let written = runtime
        .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
        .unwrap();
    drop(runtime);

    std::fs::remove_dir_all(dir.path().join("node/consensus/log")).unwrap();
    let reopened = NodeRuntime::open(config, consensus(dir.path(), "recorders"), &[]).unwrap();

    assert_eq!(reopened.log_store().last_index().unwrap(), Some(1));
    let read = reopened.get_kv(b"key", ReadConsistency::Local).unwrap();
    assert_eq!(read.value, Some(b"value".to_vec()));
    assert_eq!(
        (read.applied_index, read.hash),
        (written.applied_index(), written.hash())
    );
}

#[test]
fn corrupt_unanchored_kv_rebuilds_exact_state_and_receipts_from_two_recorders() {
    let _lock = kv_runtime_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = kv_config(dir.path());
    let runtime =
        NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]).unwrap();
    let first = runtime
        .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
        .unwrap();
    let second = runtime
        .mutate_kv(KvCommandV1::put("request-2", b"other".to_vec(), b"second".to_vec()).unwrap())
        .unwrap();
    drop(runtime);

    std::fs::remove_dir_all(dir.path().join("recorders/n3")).unwrap();
    std::fs::remove_dir_all(dir.path().join("node/consensus/log")).unwrap();
    std::fs::write(dir.path().join("node/kv/data.redb"), b"corrupt local cache").unwrap();

    let reopened = NodeRuntime::open(config, consensus(dir.path(), "recorders"), &[]).unwrap();
    let key = reopened.get_kv(b"key", ReadConsistency::Local).unwrap();
    let other = reopened.get_kv(b"other", ReadConsistency::Local).unwrap();
    assert_eq!(key.value, Some(b"value".to_vec()));
    assert_eq!(other.value, Some(b"second".to_vec()));
    assert_eq!((key.applied_index, key.hash), (2, second.hash()));
    assert_eq!((other.applied_index, other.hash), (2, second.hash()));
    assert_eq!(
        reopened
            .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
            .unwrap(),
        first
    );

    let next = reopened
        .mutate_kv(KvCommandV1::put("request-3", b"next".to_vec(), b"third".to_vec()).unwrap())
        .unwrap();
    assert_eq!(next.applied_index(), 3);
    assert_eq!(
        reopened
            .get_kv(b"next", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(b"third".to_vec())
    );
}

#[test]
fn corrupt_unanchored_kv_recovery_stably_restores_two_slots_from_two_recorders() {
    let _lock = kv_runtime_test_lock();
    for attempt in 0..20 {
        let dir = tempfile::tempdir().unwrap();
        let config = kv_config(dir.path());
        let runtime =
            NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]).unwrap();
        runtime
            .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
            .unwrap();
        let second = runtime
            .mutate_kv(
                KvCommandV1::put("request-2", b"other".to_vec(), b"second".to_vec()).unwrap(),
            )
            .unwrap();
        drop(runtime);

        std::fs::remove_dir_all(dir.path().join("recorders/n3")).unwrap();
        std::fs::remove_dir_all(dir.path().join("node/consensus/log")).unwrap();
        std::fs::write(dir.path().join("node/kv/data.redb"), b"corrupt local cache").unwrap();

        let reopened = NodeRuntime::open(config, consensus(dir.path(), "recorders"), &[])
            .unwrap_or_else(|error| panic!("recovery attempt {attempt} failed: {error}"));
        let key = reopened.get_kv(b"key", ReadConsistency::Local).unwrap();
        let other = reopened.get_kv(b"other", ReadConsistency::Local).unwrap();
        assert_eq!(key.value, Some(b"value".to_vec()), "attempt {attempt}");
        assert_eq!(other.value, Some(b"second".to_vec()), "attempt {attempt}");
        assert_eq!(key.applied_index, 2, "attempt {attempt}");
        assert_eq!(other.applied_index, 2, "attempt {attempt}");
        assert_eq!(key.hash, second.hash(), "attempt {attempt}");
        assert_eq!(other.hash, second.hash(), "attempt {attempt}");
    }
}

#[test]
fn missing_partial_and_identity_invalid_unanchored_kv_rebuild_from_qlog() {
    let _lock = kv_runtime_test_lock();
    #[derive(Clone, Copy, Debug)]
    enum Fault {
        Missing,
        Partial,
        IdentityInvalid,
    }

    for fault in [Fault::Missing, Fault::Partial, Fault::IdentityInvalid] {
        let dir = tempfile::tempdir().unwrap();
        let config = kv_config(dir.path());
        let runtime =
            NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]).unwrap();
        let committed = runtime
            .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
            .unwrap();
        drop(runtime);

        let kv_path = config.data_dir().join("kv/data.redb");
        match fault {
            Fault::Missing => std::fs::remove_file(&kv_path).unwrap(),
            Fault::Partial => std::fs::write(&kv_path, b"partial redb").unwrap(),
            Fault::IdentityInvalid => {
                std::fs::remove_dir_all(config.data_dir().join("kv")).unwrap();
                drop(
                    RedbStateMachine::open(&kv_path, "foreign-cluster", "foreign-node", 1, 1)
                        .unwrap(),
                );
            }
        }

        let reopened = NodeRuntime::open(config, consensus(dir.path(), "recorders"), &[])
            .unwrap_or_else(|error| panic!("{fault:?} did not rebuild: {error}"));
        let read = reopened.get_kv(b"key", ReadConsistency::Local).unwrap();
        assert_eq!(read.value, Some(b"value".to_vec()), "fault={fault:?}");
        assert_eq!(
            (read.applied_index, read.hash),
            (committed.applied_index(), committed.hash()),
            "fault={fault:?}"
        );
        assert_eq!(
            reopened
                .mutate_kv(
                    KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap()
                )
                .unwrap(),
            committed,
            "fault={fault:?}"
        );
        let next = reopened
            .mutate_kv(KvCommandV1::put("request-2", b"next".to_vec(), b"second".to_vec()).unwrap())
            .unwrap();
        assert_eq!(next.applied_index(), 2, "fault={fault:?}");
    }
}

#[test]
fn corrupt_unanchored_kv_recovers_from_one_certified_recorder_copy() {
    let _lock = kv_runtime_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = kv_config(dir.path());
    let runtime =
        NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]).unwrap();
    let committed = runtime
        .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
        .unwrap();
    drop(runtime);

    std::fs::remove_dir_all(dir.path().join("recorders/n2")).unwrap();
    std::fs::remove_dir_all(dir.path().join("recorders/n3")).unwrap();
    std::fs::remove_dir_all(dir.path().join("node/consensus/log")).unwrap();
    std::fs::write(dir.path().join("node/kv/data.redb"), b"corrupt local cache").unwrap();

    let reopened = NodeRuntime::open(config, consensus(dir.path(), "recorders"), &[]).unwrap();
    let read = reopened.get_kv(b"key", ReadConsistency::Local).unwrap();
    assert_eq!(read.value, Some(b"value".to_vec()));
    assert_eq!(
        (read.applied_index, read.hash),
        (committed.applied_index(), committed.hash())
    );
    assert_eq!(reopened.log_store().last_index().unwrap(), Some(1));
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn corrupt_anchored_kv_requires_snapshot_without_quarantining_the_view() {
    let _lock = kv_runtime_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = kv_config(dir.path());
    let archive = initialized_checkpoint(&dir.path().join("archive")).await;
    let coordinator = CheckpointCoordinator::open(archive, DurabilityMode::Sync)
        .await
        .unwrap();
    let runtime =
        NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]).unwrap();
    let written = runtime
        .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
        .unwrap();
    coordinator
        .flush_runtime(&runtime, written.applied_index())
        .await
        .unwrap();
    let anchor = runtime.checkpoint_compact(&coordinator).await.unwrap();
    drop(runtime);

    std::fs::write(
        config.data_dir().join("kv/data.redb"),
        b"corrupt local cache",
    )
    .unwrap();

    assert_eq!(
        NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]).unwrap_err(),
        NodeError::SnapshotRequired(Box::new(anchor))
    );
    assert!(config.data_dir().join("kv").is_dir());
    assert!(!std::fs::read_dir(config.data_dir()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("kv.quarantine-")
    }));
}

#[test]
fn corrupt_qlog_fails_before_kv_quarantine_and_preserves_both_views() {
    let _lock = kv_runtime_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = kv_config(dir.path());
    let runtime =
        NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]).unwrap();
    runtime
        .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
        .unwrap();
    drop(runtime);

    let qlog_path = std::fs::read_dir(config.data_dir().join("consensus/log"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with("-open.qlog")
        })
        .unwrap();
    let mut corrupt_qlog = std::fs::read(&qlog_path).unwrap();
    *corrupt_qlog.last_mut().unwrap() ^= 1;
    std::fs::write(&qlog_path, &corrupt_qlog).unwrap();
    let corrupt_kv = b"corrupt local cache";
    std::fs::write(config.data_dir().join("kv/data.redb"), corrupt_kv).unwrap();

    assert!(matches!(
        NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]),
        Err(NodeError::Storage(_))
    ));
    assert_eq!(std::fs::read(&qlog_path).unwrap(), corrupt_qlog);
    assert_eq!(
        std::fs::read(config.data_dir().join("kv/data.redb")).unwrap(),
        corrupt_kv
    );
    assert!(!std::fs::read_dir(config.data_dir()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("kv.quarantine-")
    }));
}

#[test]
fn kv_read_barrier_returns_value_and_tip_from_one_materializer_boundary() {
    let _lock = kv_runtime_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(
        kv_config(dir.path()),
        consensus(dir.path(), "recorders"),
        &[],
    )
    .unwrap();
    let written = runtime
        .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
        .unwrap();

    let read = runtime
        .get_kv(b"key", ReadConsistency::ReadBarrier)
        .unwrap();

    assert_eq!(read.value, Some(b"value".to_vec()));
    assert_eq!(
        (read.applied_index, read.hash),
        (written.applied_index(), written.hash())
    );
}

#[test]
fn kv_scan_pages_ranges_and_prefixes_with_the_exact_snapshot_tip() {
    let _lock = kv_runtime_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(
        kv_config(dir.path()),
        consensus(dir.path(), "recorders"),
        &[],
    )
    .unwrap();
    for (request, key, value) in [
        ("put-a", b"a".as_slice(), b"one".as_slice()),
        ("put-aa", b"aa".as_slice(), b"two".as_slice()),
        ("put-b", b"b".as_slice(), b"three".as_slice()),
    ] {
        runtime
            .mutate_kv(KvCommandV1::put(request, key.to_vec(), value.to_vec()).unwrap())
            .unwrap();
    }

    let first = runtime
        .scan_kv_range(b"a", Some(b"b"), 1, None, ReadConsistency::Local)
        .unwrap();
    assert_eq!(first.rows()[0].key(), b"a");
    assert_eq!(first.next_cursor(), Some(b"a".as_slice()));
    assert_eq!(first.tip().applied_index(), 3);
    assert_eq!(first.tip().applied_hash(), runtime.applied_hash().unwrap());

    let second = runtime
        .scan_kv_range(
            b"a",
            Some(b"b"),
            1,
            first.next_cursor(),
            ReadConsistency::AppliedIndex(first.tip().applied_index()),
        )
        .unwrap();
    assert_eq!(second.rows()[0].key(), b"aa");
    assert_eq!(second.next_cursor(), None);

    let prefix = runtime
        .scan_kv_prefix(b"a", 10, None, ReadConsistency::ReadBarrier)
        .unwrap();
    assert_eq!(
        prefix
            .rows()
            .iter()
            .map(|entry| entry.key())
            .collect::<Vec<_>>(),
        vec![b"a".as_slice(), b"aa".as_slice()]
    );
    assert_eq!(prefix.tip().applied_index(), 3);
    assert_eq!(prefix.tip().applied_hash(), runtime.applied_hash().unwrap());
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn kv_http_routes_use_base64_and_map_invalid_input_without_mutating_state() {
    let _lock = kv_runtime_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(
            kv_http_config(dir.path()),
            consensus(dir.path(), "recorders"),
            &[],
        )
        .unwrap(),
    );
    let recorder =
        RecorderFileStore::new_with_id(dir.path().join("http-recorder"), "n1", CLUSTER_ID, 1, 1)
            .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let put_url = format!("http://{addr}{KV_PUT_PATH}");

    let invalid = client
        .post(&put_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "request_id": "invalid",
            "key": "***",
            "value": "dmFsdWU="
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid.json::<ClientErrorResponse>().await.unwrap().code,
        "invalid_request"
    );

    let put = client
        .post(&put_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "request_id": "request-1",
            "key": "a2V5",
            "value": "dmFsdWU="
        }))
        .send()
        .await
        .unwrap();
    assert!(put.status().is_success());
    let put = put.json::<KvMutationResponse>().await.unwrap();
    assert_eq!(put.applied_index, 1);

    let get = client
        .post(format!("http://{addr}{KV_GET_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "key": "a2V5",
            "consistency": "read_barrier"
        }))
        .send()
        .await
        .unwrap();
    assert!(get.status().is_success());
    let get = get.json::<KvGetResponse>().await.unwrap();
    assert_eq!(get.value.as_deref(), Some("dmFsdWU="));
    assert_eq!(get.applied_index, 1);
    assert_eq!(get.hash, put.hash);

    let second_put = client
        .post(&put_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "request_id": "request-2",
            "key": "a2V5Mg==",
            "value": "dmFsdWUy"
        }))
        .send()
        .await
        .unwrap();
    assert!(second_put.status().is_success());

    let scan_url = format!("http://{addr}{KV_SCAN_PATH}");
    let scan = client
        .post(&scan_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "prefix": "a2V5",
            "limit": 1,
            "consistency": "local"
        }))
        .send()
        .await
        .unwrap();
    assert!(scan.status().is_success());
    let scan = scan.json::<KvScanResponse>().await.unwrap();
    assert_eq!(scan.entries[0].key, "a2V5");
    assert_eq!(scan.entries[0].value, "dmFsdWU=");
    assert_eq!(scan.next_cursor.as_deref(), Some("a2V5"));
    assert_eq!(scan.applied_index, 2);

    for invalid in [
        serde_json::json!({"prefix":"a2V5", "start":"aQ=="}),
        serde_json::json!({"start":"aQ==", "cursor":"***"}),
        serde_json::json!({"prefix":"a2V5", "limit":0}),
        serde_json::json!({"prefix":"a2V5", "limit":MAX_KV_SCAN_ROWS + 1}),
    ] {
        let response = client
            .post(&scan_url)
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .bearer_auth("client-token")
            .json(&invalid)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(
            response.json::<ClientErrorResponse>().await.unwrap().code,
            "invalid_request"
        );
    }
    server.abort();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn largest_node_valid_kv_record_scans_without_latching_readiness() {
    let _lock = kv_runtime_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(
            kv_http_config(dir.path()),
            consensus(dir.path(), "largest-recorders"),
            &[],
        )
        .unwrap(),
    );
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("largest-http-recorder"),
        "n1",
        CLUSTER_ID,
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = Arc::clone(&runtime);
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let request_id = "largest-http";
    let key = b"k";
    let value = vec![b'v'; MAX_KV_VALUE_BYTES];
    let command = KvCommandV1::put(request_id, key.to_vec(), value.clone()).unwrap();
    assert_eq!(value.len(), MAX_KV_VALUE_BYTES);
    assert!(encode_replicated_kv_command(&command).unwrap().len() <= MAX_COMMAND_BYTES);
    let client = reqwest::Client::new();

    let put = post_kv_put(
        &client,
        addr,
        &serde_json::json!({
            "request_id": request_id,
            "key": encode_base64(key),
            "value": encode_base64(&value)
        }),
    )
    .await;
    assert!(
        put.status().is_success(),
        "put failed: {}",
        put.text().await.unwrap()
    );

    let scan = client
        .post(format!("http://{addr}{KV_SCAN_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({"prefix": "", "limit": 1}))
        .send()
        .await
        .unwrap();
    assert!(
        scan.status().is_success(),
        "scan failed: {}",
        scan.text().await.unwrap()
    );
    let ready = client
        .get(format!("http://{addr}{READYZ_PATH}"))
        .send()
        .await
        .unwrap();
    assert!(ready.status().is_success());
    assert!(runtime.is_ready());
    assert!(!runtime.is_fatal());
    server.abort();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_kv_writes_share_one_entry_and_retry_distinct_outcomes() {
    let _lock = kv_runtime_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = kv_http_config(dir.path())
        .with_writer_batching(8, Duration::from_millis(20))
        .unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(config, consensus(dir.path(), "batch-recorders"), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("batch-http-recorder"),
        "n1",
        CLUSTER_ID,
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = Arc::clone(&runtime);
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let first_body = serde_json::json!({
        "request_id": "batch-a",
        "key": "c2hhcmVk",
        "value": "Zmlyc3Q="
    });
    let second_body = serde_json::json!({
        "request_id": "batch-b",
        "key": "c2hhcmVk",
        "value": "c2Vjb25k"
    });

    let (first, second) = tokio::join!(
        post_kv_put(&client, addr, &first_body),
        post_kv_put(&client, addr, &second_body)
    );
    let first = kv_mutation_response(first, "first concurrent write").await;
    let second = kv_mutation_response(second, "second concurrent write").await;

    assert_eq!(first.applied_index, second.applied_index);
    assert_eq!(first.hash, second.hash);
    assert_ne!(first.result, second.result);
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));

    let first_retry = kv_mutation_response(
        post_kv_put(&client, addr, &first_body).await,
        "first write retry",
    )
    .await;
    let second_retry = kv_mutation_response(
        post_kv_put(&client, addr, &second_body).await,
        "second write retry",
    )
    .await;
    assert_eq!(first_retry, first);
    assert_eq!(second_retry, second);
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));

    let conflict = post_kv_put(
        &client,
        addr,
        &serde_json::json!({
            "request_id": "batch-a",
            "key": "c2hhcmVk",
            "value": "Y29uZmxpY3Q="
        }),
    )
    .await;
    assert_eq!(conflict.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        conflict.json::<ClientErrorResponse>().await.unwrap().code,
        "invalid_request"
    );
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));
    server.abort();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn kv_sync_checkpoint_outage_reports_unknown_and_retries_original_outcome() {
    let _lock = kv_runtime_test_lock();
    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive_backup = root.path().join("archive-backup");
    let archive = initialized_checkpoint(&archive_root).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let runtime = Arc::new(
        NodeRuntime::open(
            kv_http_config(root.path()),
            consensus(root.path(), "recorders"),
            &[],
        )
        .unwrap(),
    );
    let recorder =
        RecorderFileStore::new_with_id(root.path().join("http-recorder"), "n1", CLUSTER_ID, 1, 1)
            .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            node_router_with_checkpoint_and_limits(runtime, recorder, coordinator, 1, 8),
        )
        .await
        .unwrap();
    });
    std::fs::rename(&archive_root, &archive_backup).unwrap();
    std::fs::write(&archive_root, b"archive unavailable").unwrap();
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "request_id": "request-1",
        "key": "a2V5",
        "value": "dmFsdWU="
    });

    let first = post_kv_put(&client, addr, &body).await;

    assert_eq!(first.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        first.json::<ClientErrorResponse>().await.unwrap().code,
        "write_outcome_unknown"
    );
    let read = client
        .post(format!("http://{addr}{KV_GET_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({"key": "a2V5", "consistency": "local"}))
        .send()
        .await
        .unwrap();
    assert!(read.status().is_success());
    let original = read.json::<KvGetResponse>().await.unwrap();
    assert_eq!(original.value.as_deref(), Some("dmFsdWU="));

    restore_archive(&archive_root, &archive_backup);
    wait_ready(&client, addr).await;
    let retry = post_kv_put(&client, addr, &body).await;
    assert!(retry.status().is_success());
    let retry = retry.json::<KvMutationResponse>().await.unwrap();
    assert_eq!(
        (retry.applied_index, retry.hash),
        (original.applied_index, original.hash)
    );
    assert_eq!(
        retry.result,
        rhiza_node::KvMutationResultDto::Put { replaced: false }
    );
    server.abort();
}

fn kv_config(root: &Path) -> NodeConfig {
    NodeConfig::new_embedded(
        "cluster-a",
        "n1",
        root.join("node"),
        1,
        1,
        ["n1", "n2", "n3"],
    )
    .unwrap()
    .with_execution_profile(ExecutionProfile::Kv)
    .unwrap()
}

fn kv_http_config(root: &Path) -> NodeConfig {
    NodeConfig::new(
        "cluster-a",
        "n1",
        root.join("node"),
        1,
        1,
        [
            PeerConfig::new("n1", "http://n1", "peer-1").unwrap(),
            PeerConfig::new("n2", "http://n2", "peer-2").unwrap(),
            PeerConfig::new("n3", "http://n3", "peer-3").unwrap(),
        ],
        "client-token",
    )
    .unwrap()
    .with_execution_profile(ExecutionProfile::Kv)
    .unwrap()
}

async fn initialized_checkpoint(root: &Path) -> ObjectArchiveStore {
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.to_path_buf(),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(
            CLUSTER_ID,
            1,
            1,
            kv_config(root).configuration_state().digest(),
            1,
        ),
    );
    archive.initialize_checkpoint().await.unwrap();
    archive
}

async fn post_kv_put(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    body: &serde_json::Value,
) -> reqwest::Response {
    client
        .post(format!("http://{addr}{KV_PUT_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(body)
        .send()
        .await
        .unwrap()
}

async fn kv_mutation_response(response: reqwest::Response, request: &str) -> KvMutationResponse {
    const MAX_ERROR_BODY_BYTES: usize = 1024;

    let status = response.status();
    if status.is_success() {
        return response.json::<KvMutationResponse>().await.unwrap_or_else(|error| {
            panic!("{request} returned success status {status} but invalid mutation response: {error}")
        });
    }

    let content_length = response.content_length();
    let mut response = response;
    let mut body = Vec::new();
    while body.len() < MAX_ERROR_BODY_BYTES {
        let Some(chunk) = response.chunk().await.unwrap_or_else(|error| {
            panic!("{request} response body read failed after status {status}: {error}")
        }) else {
            break;
        };
        let remaining = MAX_ERROR_BODY_BYTES - body.len();
        body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }

    panic!(
        "{request} returned status {status} (content-length {content_length:?}): {}",
        String::from_utf8_lossy(&body),
    );
}

fn restore_archive(archive_root: &Path, archive_backup: &Path) {
    std::fs::remove_file(archive_root).unwrap();
    let link = archive_root.with_extension("restore-link");
    std::os::unix::fs::symlink(archive_backup, &link).unwrap();
    std::fs::rename(link, archive_root).unwrap();
}

async fn wait_ready(client: &reqwest::Client, addr: std::net::SocketAddr) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client
                .get(format!("http://{addr}{READYZ_PATH}"))
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

fn consensus(root: &Path, recorder_dir: &str) -> Arc<ThreeNodeConsensus> {
    Arc::new(
        ThreeNodeConsensus::from_recovered_tip(
            CLUSTER_ID,
            "n1",
            1,
            1,
            [
                root.join(recorder_dir).join("n1"),
                root.join(recorder_dir).join("n2"),
                root.join(recorder_dir).join("n3"),
            ],
            1,
            LogHash::ZERO,
        )
        .unwrap(),
    )
}

fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        encoded.push(ALPHABET[usize::from(first >> 2)] as char);
        encoded.push(ALPHABET[usize::from(((first & 0x03) << 4) | (second >> 4))] as char);
        encoded.push(if chunk.len() > 1 {
            ALPHABET[usize::from(((second & 0x0f) << 2) | (third >> 6))] as char
        } else {
            '='
        });
        encoded.push(if chunk.len() > 2 {
            ALPHABET[usize::from(third & 0x3f)] as char
        } else {
            '='
        });
    }
    encoded
}
