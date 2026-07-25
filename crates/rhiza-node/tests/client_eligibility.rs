use std::{net::SocketAddr, path::Path, sync::Arc};

use axum::Router;
use rhiza_core::ConfigurationState;
use rhiza_node::{
    install_successor_recorder, node_router, ClientErrorResponse, NodeConfig, NodeRuntime,
    PeerConfig, ReadConsistency, ReadRequest, ReadResponse, RuntimeConfigurationStatus,
    SqlQueryRequest, PROTOCOL_VERSION, READYZ_PATH, READ_PATH, SQL_QUERY_PATH, VERSION_HEADER,
};
use rhiza_quepaxa::{Membership, RecorderFileStore, RecorderRpc, ThreeNodeConsensus};
use rhiza_sql::SqlStatement;

#[tokio::test(flavor = "multi_thread")]
async fn active_configuration_serves_every_read_consistency_and_is_ready() {
    let root = tempfile::tempdir().unwrap();
    let (runtime, recorder) = active_runtime(root.path());
    runtime.write("seed", "key", "value").unwrap();
    let (addr, server) = serve(node_router(runtime, recorder)).await;
    let client = reqwest::Client::new();

    assert_eq!(readyz(&client, addr).await, reqwest::StatusCode::OK);
    for consistency in read_consistencies() {
        let response = read(&client, addr, consistency).await;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(
            response
                .json::<ReadResponse>()
                .await
                .unwrap()
                .value
                .as_deref(),
            Some("value")
        );
        assert_eq!(
            query(&client, addr, consistency).await.status(),
            reqwest::StatusCode::OK
        );
    }

    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stopped_configuration_rejects_every_read_consistency_and_is_not_ready() {
    let root = tempfile::tempdir().unwrap();
    let (runtime, recorder) = active_runtime(root.path());
    runtime.write("seed", "key", "value").unwrap();
    let (addr, server) = serve(node_router(runtime.clone(), recorder)).await;
    let client = reqwest::Client::new();
    assert_eq!(
        read(&client, addr, ReadConsistency::Local).await.status(),
        reqwest::StatusCode::OK
    );

    runtime
        .stop_current_configuration_for_successor(&membership())
        .unwrap();
    assert_ineligible(&client, addr).await;

    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn awaiting_activation_rejects_every_read_consistency_and_is_not_ready() {
    let root = tempfile::tempdir().unwrap();
    let (runtime, _) = active_runtime(root.path());
    runtime.write("seed", "key", "value").unwrap();
    let membership = membership();
    let stop = runtime
        .stop_current_configuration_for_successor(&membership)
        .unwrap();
    let stopped = runtime.configuration_state().unwrap();
    drop(runtime);

    let successor_recorders = recorders(root.path(), "successor", 1, &membership);
    for recorder in &successor_recorders {
        install_successor_recorder(recorder, 2, membership.clone(), &stop).unwrap();
    }
    let local_recorder = successor_recorders[0].clone();
    let consensus = consensus(2, &successor_recorders);
    let config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-1",
        root.path().join("node"),
        1,
        membership.clone(),
        stopped,
        peers(),
        "client-token",
    )
    .unwrap()
    .with_log_initial_configuration(ConfigurationState::active(1, membership.digest()));
    let runtime = Arc::new(NodeRuntime::open(config, Arc::new(consensus), &[]).unwrap());
    assert_eq!(
        runtime.status().unwrap().configuration_status,
        RuntimeConfigurationStatus::AwaitingActivation
    );
    let (addr, server) = serve(node_router(runtime, local_recorder)).await;
    let client = reqwest::Client::new();

    assert_ineligible(&client, addr).await;

    server.abort();
}

async fn assert_ineligible(client: &reqwest::Client, addr: SocketAddr) {
    assert_eq!(
        readyz(client, addr).await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    for consistency in read_consistencies() {
        for response in [
            read(client, addr, consistency).await,
            query(client, addr, consistency).await,
        ] {
            assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
            let error = response.json::<ClientErrorResponse>().await.unwrap();
            assert_eq!(error.code, "configuration_transition");
            assert!(error.retryable);
        }
    }
}

fn read_consistencies() -> [ReadConsistency; 3] {
    [
        ReadConsistency::Local,
        ReadConsistency::AppliedIndex(0),
        ReadConsistency::ReadBarrier,
    ]
}

async fn readyz(client: &reqwest::Client, addr: SocketAddr) -> reqwest::StatusCode {
    client
        .get(format!("http://{addr}{READYZ_PATH}"))
        .send()
        .await
        .unwrap()
        .status()
}

async fn read(
    client: &reqwest::Client,
    addr: SocketAddr,
    consistency: ReadConsistency,
) -> reqwest::Response {
    client
        .post(format!("http://{addr}{READ_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&ReadRequest {
            key: "key".into(),
            consistency: Some(consistency),
        })
        .send()
        .await
        .unwrap()
}

async fn query(
    client: &reqwest::Client,
    addr: SocketAddr,
    consistency: ReadConsistency,
) -> reqwest::Response {
    client
        .post(format!("http://{addr}{SQL_QUERY_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&SqlQueryRequest {
            statement: SqlStatement {
                sql: "SELECT ?1 AS value".into(),
                parameters: vec![rhiza_sql::SqlValue::Text("value".into())],
            },
            consistency: Some(consistency),
            max_rows: Some(1),
        })
        .send()
        .await
        .unwrap()
}

async fn serve(router: Router) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (addr, server)
}

fn active_runtime(root: &Path) -> (Arc<NodeRuntime>, RecorderFileStore) {
    let membership = membership();
    let recorders = recorders(root, "active", 1, &membership);
    let local_recorder = recorders[0].clone();
    let runtime = NodeRuntime::open(
        NodeConfig::new(
            "rhiza:sql:cluster-a",
            "node-1",
            root.join("node"),
            1,
            1,
            peers(),
            "client-token",
        )
        .unwrap(),
        Arc::new(consensus(1, &recorders)),
        &[],
    )
    .unwrap();
    (Arc::new(runtime), local_recorder)
}

fn membership() -> Membership {
    Membership::new(["node-1", "node-2", "node-3"]).unwrap()
}

fn peers() -> Vec<PeerConfig> {
    (1..=3)
        .map(|id| {
            PeerConfig::new(
                format!("node-{id}"),
                format!("http://node-{id}"),
                format!("peer-token-{id}"),
            )
            .unwrap()
        })
        .collect()
}

fn recorders(
    root: &Path,
    prefix: &str,
    config_id: u64,
    membership: &Membership,
) -> Vec<RecorderFileStore> {
    membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.join(format!("{prefix}-{id}")),
                id.clone(),
                "rhiza:sql:cluster-a",
                1,
                config_id,
                membership.clone(),
            )
            .unwrap()
        })
        .collect()
}

fn consensus(config_id: u64, recorders: &[RecorderFileStore]) -> ThreeNodeConsensus {
    let recorders = recorders
        .iter()
        .map(|recorder| {
            (
                recorder.recorder_id().unwrap(),
                Box::new(recorder.clone()) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    ThreeNodeConsensus::from_recorders_with_ids(
        "rhiza:sql:cluster-a",
        "node-1",
        1,
        config_id,
        recorders,
    )
    .unwrap()
}
