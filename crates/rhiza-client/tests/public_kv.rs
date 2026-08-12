#![cfg(feature = "kv")]

use std::sync::Arc;

use axum::{routing::post, Json, Router};
use rhiza_client::{
    wire::{KvGetRequest, KvMutationResultDto, KvPutRequest, ReadConsistency},
    RhizaClient,
};
use rhiza_core::{ExecutionProfile, LogHash};
use rhiza_node::{node_router, NodeConfig, NodeRuntime, PeerConfig};
use rhiza_quepaxa::{Membership, RecorderFileStore, ThreeNodeConsensus};

#[tokio::test]
async fn kv_put_decodes_a_typed_success_for_an_external_consumer() {
    let app = Router::new().route(
        "/v1/kv/put",
        post(|| async {
            Json(serde_json::json!({
                "applied_index": 7,
                "hash": vec![0_u8; 32],
                "result": {"operation": "put", "replaced": false},
            }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = RhizaClient::new([endpoint], "client-token")
        .unwrap()
        .kv_put(KvPutRequest {
            request_id: "request-1".into(),
            key: "a2V5".into(),
            value: "dmFsdWU=".into(),
        })
        .await
        .unwrap();

    server.abort();
    assert_eq!(response.applied_index, 7);
    assert_eq!(
        response.result,
        KvMutationResultDto::Put { replaced: false }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_public_client_puts_and_gets_through_a_real_node() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_id = "rhiza:kv:client-e2e";
    let config = NodeConfig::new(
        cluster_id,
        "n1",
        dir.path().join("node"),
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
    .unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(
            config,
            Arc::new(
                ThreeNodeConsensus::from_recovered_tip(
                    cluster_id,
                    "n1",
                    1,
                    1,
                    [
                        dir.path().join("recorders/n1"),
                        dir.path().join("recorders/n2"),
                        dir.path().join("recorders/n3"),
                    ],
                    1,
                    LogHash::ZERO,
                )
                .unwrap(),
            ),
            &[],
        )
        .unwrap(),
    );
    let recorder = RecorderFileStore::new_with_membership(
        dir.path().join("http-recorder"),
        "n1",
        cluster_id,
        1,
        1,
        membership,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server_runtime = Arc::clone(&runtime);
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(server_runtime, recorder))
            .await
            .unwrap();
    });
    let client = RhizaClient::new([endpoint], "client-token").unwrap();

    let put = client
        .kv_put(KvPutRequest {
            request_id: "request-1".into(),
            key: "a2V5".into(),
            value: "dmFsdWU=".into(),
        })
        .await
        .unwrap();
    assert_eq!(put.result, KvMutationResultDto::Put { replaced: false });
    let read = client
        .kv_get(KvGetRequest {
            key: "a2V5".into(),
            consistency: Some(ReadConsistency::ReadBarrier),
        })
        .await
        .unwrap();
    assert_eq!(read.value.as_deref(), Some("dmFsdWU="));
    assert_eq!(read.applied_index, put.applied_index);
    assert_eq!(read.hash, put.hash);

    drop(client);
    server.abort();
    let _ = server.await;
    drop(runtime);
}
