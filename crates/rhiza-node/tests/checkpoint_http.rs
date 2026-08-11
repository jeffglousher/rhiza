use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};

use axum::Router;
use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_core::LogHash;
use rhiza_node::{
    node_router, node_router_with_checkpoint, CheckpointCoordinator, ClientErrorResponse,
    DurabilityHealth, DurabilityMode, NodeConfig, NodeRuntime, PeerConfig, ReadConsistency,
    SqlExecuteRequest, SqlExecuteResponse, SqlQueryRequest, SqlQueryResponse, WriteRequest,
    WriteResponse, PROTOCOL_VERSION, READYZ_PATH, SQL_EXECUTE_PATH, SQL_QUERY_PATH, VERSION_HEADER,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{Membership, RecorderFileStore, RecorderRpc, ThreeNodeConsensus};
use rhiza_sql::{SqlStatement, SqlValue};
use tokio::io::AsyncWriteExt;

#[tokio::test(flavor = "multi_thread")]
async fn sync_write_returns_success_after_checkpoint_reaches_applied_index() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let runtime = runtime(&root.path().join("node"));
    let (addr, server) = serve(node_router_with_checkpoint(
        runtime,
        recorder(root.path()),
        coordinator,
    ))
    .await;

    let response = post_write(addr, write("request-1", "alpha", "one")).await;

    assert!(response.status().is_success());
    let committed = response.json::<WriteResponse>().await.unwrap();
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
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_retry_returns_original_outcome_after_archive_recovers() {
    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive_backup = root.path().join("archive-backup");
    let archive = initialized_checkpoint(&archive_root).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let runtime = runtime(&root.path().join("node"));
    let (addr, server) = serve(node_router_with_checkpoint(
        runtime.clone(),
        recorder(root.path()),
        coordinator.clone(),
    ))
    .await;
    std::fs::rename(&archive_root, &archive_backup).unwrap();
    std::fs::write(&archive_root, b"archive unavailable").unwrap();

    let first = post_write(addr, write("request-1", "alpha", "one")).await;

    assert_client_error(
        first,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "write_outcome_unknown",
        true,
        None,
    )
    .await;
    assert_eq!(runtime.applied_index().unwrap(), 1);
    let original_hash = runtime.applied_hash().unwrap();
    assert_eq!(coordinator.health(), DurabilityHealth::Unavailable);
    assert_eq!(readyz(addr).await, reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(coordinator.durable_tip().index(), 0);
    let blocked = post_write(addr, write("request-2", "beta", "two")).await;
    assert_client_error(
        blocked,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "writes_unavailable",
        true,
        None,
    )
    .await;
    assert_eq!(runtime.applied_index().unwrap(), 1);

    let restore_link = root.path().join("archive-restore-link");
    std::os::unix::fs::symlink(&archive_backup, &restore_link).unwrap();
    std::fs::rename(&restore_link, &archive_root).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while readyz(addr).await != reqwest::StatusCode::OK {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(coordinator.health(), DurabilityHealth::Available);
    let retry = post_write(addr, write("request-1", "alpha", "one")).await;

    assert!(retry.status().is_success());
    let committed = retry.json::<WriteResponse>().await.unwrap();
    assert_eq!(committed.applied_index, 1);
    assert_eq!(committed.hash, original_hash);
    assert_eq!(runtime.applied_index().unwrap(), 1);
    let conflict = post_write(addr, write("request-1", "alpha", "changed")).await;
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(runtime.applied_index().unwrap(), 1);
    assert_eq!(
        runtime
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
    assert_eq!(
        archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .tip()
            .index(),
        1
    );
    server.abort();
}

async fn readyz(addr: SocketAddr) -> reqwest::StatusCode {
    reqwest::Client::new()
        .get(format!("http://{addr}{READYZ_PATH}"))
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnected_sync_write_finishes_checkpoint_after_archive_recovers() {
    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive_data = root.path().join("archive-data");
    std::fs::create_dir_all(&archive_root).unwrap();
    std::fs::create_dir_all(&archive_data).unwrap();
    let archive_namespace = archive_root.join("rhiza");
    std::os::unix::fs::symlink(&archive_data, &archive_namespace).unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: archive_root.clone(),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"checkpoint-http-test"]),
            1,
        ),
    );
    archive.initialize_checkpoint().await.unwrap();
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let runtime = runtime(&root.path().join("node"));
    let (addr, server) = serve(node_router_with_checkpoint(
        runtime.clone(),
        recorder(root.path()),
        coordinator,
    ))
    .await;
    let archive_unavailable = root.path().join("archive-unavailable");
    std::fs::write(&archive_unavailable, b"archive unavailable").unwrap();
    swap_symlink_target(&archive_namespace, &archive_unavailable);

    let body = serde_json::to_vec(&write("request-detached", "alpha", "one")).unwrap();
    let mut connection = tokio::net::TcpStream::connect(addr).await.unwrap();
    connection
        .write_all(
            format!(
                "POST /v1/write HTTP/1.1\r\nHost: {addr}\r\nx-rhiza-version: 1\r\nAuthorization: Bearer client-token\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    connection.write_all(&body).await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while runtime.applied_index().unwrap() != 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    drop(connection);
    assert_eq!(
        runtime
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );

    swap_symlink_target(&archive_namespace, &archive_data);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let tip = archive
                .load_checkpoint()
                .await
                .unwrap()
                .unwrap()
                .manifest()
                .tip()
                .index();
            if tip == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn bounded_write_is_rejected_without_mutation_when_checkpoint_lag_expires() {
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
    let (addr, server) = serve(node_router_with_checkpoint(
        runtime.clone(),
        recorder(root.path()),
        coordinator,
    ))
    .await;

    let first = post_write(addr, write("request-1", "alpha", "one")).await;
    assert!(first.status().is_success());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let rejected = post_write(addr, write("request-2", "beta", "two")).await;

    assert_eq!(rejected.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(runtime.applied_index().unwrap(), 1);
    assert_eq!(
        runtime.read("beta", ReadConsistency::Local).unwrap().value,
        None
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn existing_router_write_behavior_is_unchanged_without_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime(&root.path().join("node"));
    let (addr, server) = serve(node_router(runtime.clone(), recorder(root.path()))).await;

    let response = post_write(addr, write("request-1", "alpha", "one")).await;

    assert!(response.status().is_success());
    assert_eq!(runtime.applied_index().unwrap(), 1);
    assert_eq!(
        runtime
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn client_routes_return_json_for_malformed_request_bodies() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime(&root.path().join("node"));
    let (addr, server) = serve(node_router(runtime, recorder(root.path()))).await;
    let client = reqwest::Client::new();

    for path in ["/v1/write", "/v1/read", SQL_EXECUTE_PATH, SQL_QUERY_PATH] {
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
            reqwest::StatusCode::BAD_REQUEST,
            "invalid_json",
            false,
            None,
        )
        .await;
    }
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_http_executes_atomic_ddl_dml_and_queries_typed_rows_with_barrier() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime(&root.path().join("node"));
    let (addr, server) = serve(node_router(runtime.clone(), recorder(root.path()))).await;
    let client = reqwest::Client::new();
    let execute = SqlExecuteRequest {
        request_id: "sql-http-1".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO users(id, name) VALUES (?1, ?2)".into(),
                parameters: vec![SqlValue::Integer(1), SqlValue::Text("Ada".into())],
            },
        ],
    };

    let first = client
        .post(format!("http://{addr}{SQL_EXECUTE_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&execute)
        .send()
        .await
        .unwrap();
    assert!(first.status().is_success());
    let first = first.json::<SqlExecuteResponse>().await.unwrap();
    let serialized = serde_json::to_value(&first).unwrap();
    assert!(serialized.get("version").is_none());
    assert!(serialized.get("results").is_some());
    runtime
        .write("between-sql-retries", "other", "value")
        .unwrap();
    let replay = client
        .post(format!("http://{addr}{SQL_EXECUTE_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&execute)
        .send()
        .await
        .unwrap()
        .json::<SqlExecuteResponse>()
        .await
        .unwrap();
    assert_eq!(replay, first);

    let query = client
        .post(format!("http://{addr}{SQL_QUERY_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&SqlQueryRequest {
            statement: SqlStatement {
                sql: "SELECT id, name FROM users WHERE id = ?1".into(),
                parameters: vec![SqlValue::Integer(1)],
            },
            consistency: Some(ReadConsistency::ReadBarrier),
            max_rows: Some(10),
        })
        .send()
        .await
        .unwrap();
    assert!(query.status().is_success());
    let query = query.json::<SqlQueryResponse>().await.unwrap();
    assert_eq!(query.columns, ["id", "name"]);
    assert_eq!(
        query.rows,
        [vec![SqlValue::Integer(1), SqlValue::Text("Ada".into())]]
    );
    assert!(query.applied_index > first.applied_index);

    let wrong_mode = client
        .post(format!("http://{addr}{SQL_QUERY_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&SqlQueryRequest {
            statement: SqlStatement {
                sql: "DELETE FROM users".into(),
                parameters: vec![],
            },
            consistency: None,
            max_rows: None,
        })
        .send()
        .await
        .unwrap();
    assert_client_error(
        wrong_mode,
        reqwest::StatusCode::BAD_REQUEST,
        "invalid_request",
        false,
        Some(0),
    )
    .await;

    let readonly = SqlExecuteRequest {
        request_id: "sql-readonly-execute".into(),
        statements: vec![SqlStatement {
            sql: "SELECT name FROM users".into(),
            parameters: vec![],
        }],
    };
    let readonly_first = post_sql_execute(&client, addr, &readonly).await;
    assert!(readonly_first.status().is_success());
    let readonly_first = readonly_first.json::<SqlExecuteResponse>().await.unwrap();
    assert_eq!(readonly_first.results[0].rows_affected, 0);
    let returning = readonly_first.results[0].returning.as_ref().unwrap();
    assert_eq!(returning.columns, ["name"]);
    assert_eq!(returning.rows, [[SqlValue::Text("Ada".into())]]);
    runtime
        .write("between-readonly-retries", "receipt", "preserved")
        .unwrap();
    let readonly_replay = post_sql_execute(&client, addr, &readonly)
        .await
        .json::<SqlExecuteResponse>()
        .await
        .unwrap();
    assert_eq!(readonly_replay, readonly_first);

    let constraint_failure = post_sql_execute(
        &client,
        addr,
        &SqlExecuteRequest {
            request_id: "sql-constraint-failure".into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO users(id, name) VALUES (?1, ?2)".into(),
                parameters: vec![SqlValue::Integer(1), SqlValue::Text("Grace".into())],
            }],
        },
    )
    .await;
    assert_client_error(
        constraint_failure,
        reqwest::StatusCode::BAD_REQUEST,
        "invalid_request",
        false,
        Some(0),
    )
    .await;
    assert!(runtime.is_ready());

    let second_statement_failure = client
        .post(format!("http://{addr}{SQL_EXECUTE_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&SqlExecuteRequest {
            request_id: "sql-second-statement-failure".into(),
            statements: vec![
                SqlStatement {
                    sql: "INSERT INTO users(id, name) VALUES (2, 'Grace')".into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "INSERT INTO users(id, name) VALUES (1, 'duplicate')".into(),
                    parameters: vec![],
                },
            ],
        })
        .send()
        .await
        .unwrap();
    assert_client_error(
        second_statement_failure,
        reqwest::StatusCode::BAD_REQUEST,
        "invalid_request",
        false,
        Some(1),
    )
    .await;
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_http_returning_replay_preserves_typed_result_without_duplicate_row() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime(&root.path().join("node"));
    let (addr, server) = serve(node_router(runtime, recorder(root.path()))).await;
    let client = reqwest::Client::new();
    let schema = SqlExecuteRequest {
        request_id: "returning-schema".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
            parameters: vec![],
        }],
    };
    assert!(post_sql_execute(&client, addr, &schema)
        .await
        .status()
        .is_success());

    let original = SqlExecuteRequest {
        request_id: "returning-original".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(id, name) VALUES (?1, ?2) RETURNING id, name".into(),
            parameters: vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())],
        }],
    };
    let first_response = post_sql_execute(&client, addr, &original).await;
    assert!(first_response.status().is_success());
    let first = first_response.json::<SqlExecuteResponse>().await.unwrap();
    assert_eq!(first.results.len(), 1);
    assert_eq!(first.results[0].statement_index, 0);
    assert_eq!(first.results[0].rows_affected, 1);
    let returning = first.results[0].returning.as_ref().unwrap();
    assert_eq!(returning.columns, ["id", "name"]);
    assert_eq!(
        returning.rows,
        [vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())]]
    );

    let later = SqlExecuteRequest {
        request_id: "returning-later".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(id, name) VALUES (?1, ?2)".into(),
            parameters: vec![SqlValue::Integer(8), SqlValue::Text("Grace".into())],
        }],
    };
    assert!(post_sql_execute(&client, addr, &later)
        .await
        .status()
        .is_success());

    let replay = post_sql_execute(&client, addr, &original)
        .await
        .json::<SqlExecuteResponse>()
        .await
        .unwrap();
    assert_eq!(replay, first);

    let query = client
        .post(format!("http://{addr}{SQL_QUERY_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&SqlQueryRequest {
            statement: SqlStatement {
                sql: "SELECT id, name FROM items ORDER BY id".into(),
                parameters: vec![],
            },
            consistency: Some(ReadConsistency::ReadBarrier),
            max_rows: Some(10),
        })
        .send()
        .await
        .unwrap()
        .json::<SqlQueryResponse>()
        .await
        .unwrap();
    assert_eq!(query.columns, ["id", "name"]);
    assert_eq!(
        query.rows,
        [
            vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())],
            vec![SqlValue::Integer(8), SqlValue::Text("Grace".into())],
        ]
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_http_qwal_commits_ddl_and_returning_as_one_atomic_command() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime(&root.path().join("node"));
    let (addr, server) = serve(node_router(runtime.clone(), recorder(root.path()))).await;
    let client = reqwest::Client::new();
    let response = post_sql_execute(
        &client,
        addr,
        &SqlExecuteRequest {
            request_id: "returning-ddl".into(),
            statements: vec![
                SqlStatement {
                    sql: "CREATE TABLE rejected(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                        .into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "INSERT INTO rejected(value) VALUES ('x') RETURNING id".into(),
                    parameters: vec![],
                },
            ],
        },
    )
    .await;
    assert!(response.status().is_success());
    let response = response.json::<SqlExecuteResponse>().await.unwrap();
    assert_eq!(response.applied_index, 1);
    assert_eq!(response.results.len(), 2);
    assert_eq!(response.results[1].rows_affected, 1);
    assert_eq!(
        response.results[1].returning.as_ref().unwrap().rows,
        [vec![SqlValue::Integer(1)]]
    );
    assert_eq!(runtime.applied_index().unwrap(), 1);
    assert_eq!(
        runtime
            .query_sql(
                &SqlStatement {
                    sql: "SELECT id, value FROM rejected".into(),
                    parameters: vec![],
                },
                ReadConsistency::Local,
                1,
            )
            .unwrap()
            .rows,
        [vec![SqlValue::Integer(1), SqlValue::Text("x".into())]]
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_sql_retries_coalesce_identical_payloads_and_conflict_on_reuse() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime(&root.path().join("node"));
    let (addr, server) = serve(node_router(runtime.clone(), recorder(root.path()))).await;
    let client = reqwest::Client::new();
    let schema = SqlExecuteRequest {
        request_id: "schema".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE events(value TEXT)".into(),
            parameters: vec![],
        }],
    };
    assert!(post_sql_execute(&client, addr, &schema)
        .await
        .status()
        .is_success());

    let identical = SqlExecuteRequest {
        request_id: "same".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO events(value) VALUES (?1)".into(),
            parameters: vec![SqlValue::Text("once".into())],
        }],
    };
    let (left, right) = tokio::join!(
        post_sql_execute(&client, addr, &identical),
        post_sql_execute(&client, addr, &identical)
    );
    assert!(left.status().is_success());
    assert!(right.status().is_success());
    assert_eq!(
        left.json::<WriteResponse>().await.unwrap(),
        right.json::<WriteResponse>().await.unwrap()
    );

    let conflict_a = SqlExecuteRequest {
        request_id: "conflict".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO events(value) VALUES ('a')".into(),
            parameters: vec![],
        }],
    };
    let conflict_b = SqlExecuteRequest {
        request_id: "conflict".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO events(value) VALUES ('b')".into(),
            parameters: vec![],
        }],
    };
    let (left, right) = tokio::join!(
        post_sql_execute(&client, addr, &conflict_a),
        post_sql_execute(&client, addr, &conflict_b)
    );
    let (success, conflict) = if left.status().is_success() {
        (left, right)
    } else {
        (right, left)
    };
    assert!(success.status().is_success());
    assert_client_error(
        conflict,
        reqwest::StatusCode::CONFLICT,
        "request_conflict",
        false,
        None,
    )
    .await;

    let result = runtime
        .query_sql(
            &SqlStatement {
                sql: "SELECT count(*) FROM events".into(),
                parameters: vec![],
            },
            ReadConsistency::Local,
            10,
        )
        .unwrap();
    assert_eq!(result.rows, [vec![SqlValue::Integer(2)]]);
    server.abort();
}

async fn serve(router: Router) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (addr, server)
}

async fn post_write(addr: SocketAddr, request: WriteRequest) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}/v1/write"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&request)
        .send()
        .await
        .unwrap()
}

async fn post_sql_execute(
    client: &reqwest::Client,
    addr: SocketAddr,
    request: &SqlExecuteRequest,
) -> reqwest::Response {
    client
        .post(format!("http://{addr}{SQL_EXECUTE_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(request)
        .send()
        .await
        .unwrap()
}

fn swap_symlink_target(path: &Path, target: &Path) {
    let next = path.with_extension("next");
    std::os::unix::fs::symlink(target, &next).unwrap();
    std::fs::rename(next, path).unwrap();
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

fn write(request_id: &str, key: &str, value: &str) -> WriteRequest {
    WriteRequest {
        request_id: request_id.into(),
        key: key.into(),
        value: value.into(),
    }
}

async fn initialized_checkpoint(root: &Path) -> ObjectArchiveStore {
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
            LogHash::digest(&[b"checkpoint-http-test"]),
            1,
        ),
    );
    archive.initialize_checkpoint().await.unwrap();
    archive
}

fn runtime(data_dir: &Path) -> Arc<NodeRuntime> {
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let recorder_root = data_dir.parent().unwrap().join("consensus-recorders");
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

fn recorder(root: &Path) -> RecorderFileStore {
    RecorderFileStore::new_with_id(
        root.join("http-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap()
}

fn peers() -> [PeerConfig; 3] {
    [
        PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
        PeerConfig::new("node-2", "http://node-2", "peer-token-2").unwrap(),
        PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
    ]
}
