use rhizadb::{
    EmbeddedConfig, ExecutionProfile, KvCommandResultV1, KvCommandV1, ReadConsistency, Rhiza,
};

#[tokio::test(flavor = "multi_thread")]
async fn kv_profile_executes_semantic_writes_and_reads_in_process() {
    let root = tempfile::tempdir().unwrap();
    let config =
        EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Kv).unwrap();
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();

    let put = handle
        .kv_put(b"key-1".to_vec(), b"value-1".to_vec(), "put-1".into())
        .await
        .unwrap();
    assert_eq!(put.result(), &KvCommandResultV1::Put { replaced: false });

    let get = handle
        .kv_get(b"key-1", ReadConsistency::Local)
        .await
        .unwrap();
    assert_eq!(get.value.as_deref(), Some(b"value-1".as_ref()));

    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_profile_returns_none_for_missing_key() {
    let root = tempfile::tempdir().unwrap();
    let config =
        EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Kv).unwrap();
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();

    let get = handle
        .kv_get(b"missing", ReadConsistency::Local)
        .await
        .unwrap();
    assert_eq!(get.value, None);

    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_profile_rejects_idempotent_conflict() {
    let root = tempfile::tempdir().unwrap();
    let config =
        EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Kv).unwrap();
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();

    let first = handle
        .kv_put(b"key-1".to_vec(), b"value-1".to_vec(), "same-req".into())
        .await
        .unwrap();
    assert_eq!(first.result(), &KvCommandResultV1::Put { replaced: false });

    let second = handle
        .kv_put(b"key-1".to_vec(), b"value-2".to_vec(), "same-req".into())
        .await;
    assert!(
        second.is_err(),
        "reusing request_id with different payload must fail"
    );

    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_profile_put_then_delete() {
    let root = tempfile::tempdir().unwrap();
    let config =
        EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Kv).unwrap();
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();

    handle
        .kv_put(b"key-1".to_vec(), b"value-1".to_vec(), "put-1".into())
        .await
        .unwrap();

    let delete = handle
        .kv_delete(b"key-1".to_vec(), "delete-1".into())
        .await
        .unwrap();
    assert_eq!(
        delete.result(),
        &KvCommandResultV1::Delete { existed: true }
    );

    let get = handle
        .kv_get(b"key-1", ReadConsistency::Local)
        .await
        .unwrap();
    assert_eq!(get.value, None);

    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_profile_batch_writes() {
    let root = tempfile::tempdir().unwrap();
    let config =
        EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Kv).unwrap();
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();

    let commands = vec![
        KvCommandV1::put("batch-a", b"a".to_vec(), b"1".to_vec()).unwrap(),
        KvCommandV1::put("batch-b", b"b".to_vec(), b"2".to_vec()).unwrap(),
        KvCommandV1::put("batch-c", b"c".to_vec(), b"3".to_vec()).unwrap(),
    ];

    let results = handle.kv_batch(commands).await.unwrap();
    assert_eq!(results.len(), 3);
    for result in &results {
        let outcome = result.as_ref().unwrap();
        assert_eq!(
            outcome.result(),
            &KvCommandResultV1::Put { replaced: false }
        );
    }

    let get = handle.kv_get(b"b", ReadConsistency::Local).await.unwrap();
    assert_eq!(get.value.as_deref(), Some(b"2".as_ref()));

    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_profile_scan_prefix() {
    let root = tempfile::tempdir().unwrap();
    let config =
        EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Kv).unwrap();
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();

    handle
        .kv_put(b"/users/1".to_vec(), b"Alice".to_vec(), "u1".into())
        .await
        .unwrap();
    handle
        .kv_put(b"/users/2".to_vec(), b"Bob".to_vec(), "u2".into())
        .await
        .unwrap();
    handle
        .kv_put(b"/items/1".to_vec(), b"Widget".to_vec(), "i1".into())
        .await
        .unwrap();

    let scan = handle
        .kv_scan_prefix(b"/users/".to_vec(), 100, None, ReadConsistency::Local)
        .await
        .unwrap();
    assert_eq!(scan.rows().len(), 2);

    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_profile_put_reports_replaced_on_update() {
    let root = tempfile::tempdir().unwrap();
    let config =
        EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Kv).unwrap();
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();

    let first = handle
        .kv_put(b"key-1".to_vec(), b"value-1".to_vec(), "put-1".into())
        .await
        .unwrap();
    assert_eq!(first.result(), &KvCommandResultV1::Put { replaced: false });

    let second = handle
        .kv_put(b"key-1".to_vec(), b"value-2".to_vec(), "put-2".into())
        .await
        .unwrap();
    assert_eq!(second.result(), &KvCommandResultV1::Put { replaced: true });

    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_profile_delete_reports_existed_false_for_missing_key() {
    let root = tempfile::tempdir().unwrap();
    let config =
        EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Kv).unwrap();
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();

    let delete = handle
        .kv_delete(b"missing".to_vec(), "delete-missing".into())
        .await
        .unwrap();
    assert_eq!(
        delete.result(),
        &KvCommandResultV1::Delete { existed: false }
    );

    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_profile_scan_range() {
    let root = tempfile::tempdir().unwrap();
    let config =
        EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Kv).unwrap();
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();

    handle
        .kv_put(b"a".to_vec(), b"1".to_vec(), "r1".into())
        .await
        .unwrap();
    handle
        .kv_put(b"b".to_vec(), b"2".to_vec(), "r2".into())
        .await
        .unwrap();
    handle
        .kv_put(b"c".to_vec(), b"3".to_vec(), "r3".into())
        .await
        .unwrap();
    handle
        .kv_put(b"d".to_vec(), b"4".to_vec(), "r4".into())
        .await
        .unwrap();

    let scan = handle
        .kv_scan_range(
            b"b".to_vec(),
            Some(b"d".to_vec()),
            100,
            None,
            ReadConsistency::Local,
        )
        .await
        .unwrap();
    assert_eq!(scan.rows().len(), 2);
    assert_eq!(scan.rows()[0].key(), b"b");
    assert_eq!(scan.rows()[1].key(), b"c");

    rhiza.shutdown().await.unwrap();
}
