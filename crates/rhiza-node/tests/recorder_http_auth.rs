use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use rhiza_core::{EntryType, LogHash, StoredCommand};
use rhiza_node::{
    recorder_router, HttpRecorderClient, PeerConfig, DEFAULT_PEER_CONCURRENCY, NODE_ID_HEADER,
    RECORDER_FETCH_COMMAND_PATH, RECORDER_IDENTITY_PATH, RECORDER_PROTOCOL_VERSION,
    RECORDER_RECORD_PATH, RECOVERY_GENERATION_HEADER, VERSION_HEADER,
};
use rhiza_quepaxa::{
    AcceptedValue, DecisionProof, Error, Membership, Proposal, ProposalPriority, RecordRequest,
    RecordSummary, RecorderRpc, RejectReason,
};
use socket2::Socket;

fn peers() -> Vec<PeerConfig> {
    (1..=3)
        .map(|index| {
            PeerConfig::new(
                format!("node-{index}"),
                format!("http://node-{index}:8081"),
                format!("peer-token-{index}"),
            )
            .unwrap()
        })
        .collect()
}

#[derive(Clone, Default)]
struct CountingRecorder {
    records: Arc<AtomicUsize>,
    proofs: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct LargeFetchRecorder {
    payload: Arc<StoredCommand>,
    completed: tokio::sync::mpsc::UnboundedSender<()>,
    calls: Arc<AtomicUsize>,
}

impl RecorderRpc for LargeFetchRecorder {
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn fetch_command_for(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        assert_eq!(command_hash, self.payload.hash());
        // Complete the full copy before acknowledging the backend call.  The
        // slow-reader test below uses this signal as its proof that response
        // serialization, rather than recorder work, is retaining admission.
        let response = Some((*self.payload).clone());
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _ = self.completed.send(());
        Ok(response)
    }
}

fn authenticated_http_request(
    address: std::net::SocketAddr,
    path: &str,
    body: serde_json::Value,
) -> Vec<u8> {
    let body = serde_json::to_vec(&body).unwrap();
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{VERSION_HEADER}: {RECORDER_PROTOCOL_VERSION}\r\n{NODE_ID_HEADER}: node-2\r\n{RECOVERY_GENERATION_HEADER}: 1\r\nAuthorization: Bearer peer-token-2\r\n\r\n",
        body.len(),
    )
    .into_bytes();
    request.extend_from_slice(&body);
    request
}

fn open_authenticated_slow_reader(address: std::net::SocketAddr, request: &[u8]) -> TcpStream {
    let stream = TcpStream::connect(address).unwrap();
    let socket = Socket::from(stream);
    socket.set_recv_buffer_size(1024).unwrap();
    socket.set_linger(Some(Duration::ZERO)).unwrap();
    let mut stream: TcpStream = socket.into();
    stream.set_nodelay(true).unwrap();
    stream.write_all(request).unwrap();
    stream.flush().unwrap();
    stream
}

/// Starts an authenticated response, proving that its complete headers and a
/// payload byte have reached the slow client, then deliberately stops reading.
///
/// Reading a byte after the headers matters: merely writing the request can
/// leave the handler in flight, which would not distinguish the response-body
/// permit from the handler's permit. This accepts either a content-length body
/// or HTTP/1.1 chunked encoding without consuming more than the first payload
/// byte.
fn read_response_body_start(stream: &mut TcpStream) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut headers = Vec::new();
    loop {
        assert!(
            headers.len() < 64 * 1024,
            "HTTP response headers exceeded the test bound"
        );
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).unwrap();
        headers.push(byte[0]);
        if headers.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let headers = std::str::from_utf8(&headers).unwrap();
    assert!(
        headers.starts_with("HTTP/1.1 200 "),
        "slow-reader response status: {headers:?}"
    );
    let mut chunked = false;
    let mut content_length = None;
    for line in headers.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("transfer-encoding")
            && value.trim().eq_ignore_ascii_case("chunked")
        {
            chunked = true;
        }
        if name.eq_ignore_ascii_case("content-length") {
            content_length = Some(value.trim().parse::<usize>().unwrap());
        }
    }
    if chunked {
        let mut chunk_size = Vec::new();
        loop {
            assert!(
                chunk_size.len() < 64,
                "HTTP chunk-size line exceeded the test bound"
            );
            let mut byte = [0_u8; 1];
            stream.read_exact(&mut byte).unwrap();
            chunk_size.push(byte[0]);
            if chunk_size.ends_with(b"\r\n") {
                break;
            }
        }
        let chunk_size = std::str::from_utf8(&chunk_size[..chunk_size.len() - 2])
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .trim();
        assert!(
            usize::from_str_radix(chunk_size, 16).unwrap() > 0,
            "slow-reader response ended before a payload byte"
        );
    } else {
        assert!(
            content_length.is_some_and(|length| length > 0),
            "slow-reader response has no payload: {headers:?}"
        );
    }
    let mut body_byte = [0_u8; 1];
    stream.read_exact(&mut body_byte).unwrap();
}

fn authenticated_http_response(
    address: std::net::SocketAddr,
    path: &str,
    body: serde_json::Value,
) -> (String, serde_json::Value) {
    let request = authenticated_http_request(address, path, body);
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream.write_all(&request).unwrap();
    stream.flush().unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    let body_start = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP response must include headers")
        + 4;
    let status = String::from_utf8_lossy(&response[..body_start])
        .lines()
        .next()
        .unwrap()
        .to_owned();
    (
        status,
        serde_json::from_slice(&response[body_start..]).unwrap(),
    )
}

#[derive(Clone)]
struct SafetyErrorRecorder {
    error: Error,
}

impl RecorderRpc for SafetyErrorRecorder {
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn fetch_command_for(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        _command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        Err(self.error.clone())
    }
}

#[derive(Clone, Default)]
struct PanicRecorder {
    mutation_started: Arc<AtomicUsize>,
}

impl RecorderRpc for PanicRecorder {
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn fetch_command_for(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        _command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        panic!("injected read-only recorder panic")
    }

    fn record(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
        self.mutation_started.fetch_add(1, Ordering::SeqCst);
        panic!("injected mutating recorder panic")
    }
}

impl RecorderRpc for CountingRecorder {
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn record(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
        self.records.fetch_add(1, Ordering::Relaxed);
        Ok(RecordSummary {
            recorder_id: "node-1".into(),
            slot: request.slot,
            config_id: request.config_id,
            config_digest: request.config_digest,
            step: request.step,
            first_current: Some(request.proposal),
            aggregate_prior: None,
            decided: None,
        })
    }

    fn install_decision_proof(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.proofs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn record_request(proposer_id: &str, slot: u64) -> RecordRequest {
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let command = StoredCommand::new(EntryType::Command, format!("command-{slot}").into_bytes());
    RecordRequest {
        cluster_id: "rhiza:sql:cluster-a".into(),
        epoch: 1,
        config_id: 1,
        config_digest: membership.digest(),
        slot,
        step: 4,
        proposal: Proposal::new(
            ProposalPriority::MAX,
            proposer_id,
            slot,
            AcceptedValue::from_command("rhiza:sql:cluster-a", slot, 1, 1, LogHash::ZERO, &command),
        ),
        command: Some(command),
    }
}

fn decision_proof(proposer_id: &str, slot: u64) -> DecisionProof {
    let request = record_request(proposer_id, slot);
    DecisionProof::FastPath {
        cluster_id: request.cluster_id,
        slot: request.slot,
        epoch: request.epoch,
        config_id: request.config_id,
        config_digest: request.config_digest,
        proposal: request.proposal,
        summaries: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn http_recorder_accepts_member_relay_and_rejects_non_member_without_backend_call() {
    let recorder = CountingRecorder::default();
    let records = Arc::clone(&recorder.records);
    let proofs = Arc::clone(&recorder.proofs);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, recorder_router(recorder, peers()))
            .await
            .unwrap();
    });

    tokio::task::spawn_blocking(move || {
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        let context = rhiza_quepaxa::RecorderRpcContext::default_timeout();
        assert_eq!(
            client
                .record(&context, record_request("node-1", 1))
                .unwrap()
                .slot,
            1
        );
        assert!(matches!(
            client.record(&context, record_request("node-9", 2)),
            Err(Error::Rejected(RejectReason::InvalidRequest))
        ));
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        client
            .install_decision_proof(&context, decision_proof("node-1", 3), &membership)
            .unwrap();
        assert!(matches!(
            client.install_decision_proof(&context, decision_proof("node-9", 4), &membership),
            Err(Error::Rejected(RejectReason::InvalidRequest))
        ));
        assert_eq!(client.recorder_id(&context).unwrap(), "node-1");
    })
    .await
    .unwrap();

    assert_eq!(records.load(Ordering::Relaxed), 1);
    assert_eq!(proofs.load(Ordering::Relaxed), 1);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn http_recorder_round_trips_safety_errors_exactly() {
    let chain = Error::ChainConflict {
        slot: 9,
        expected_prev_hash: LogHash::digest(&[b"expected"]),
        actual_prev_hash: LogHash::digest(&[b"actual"]),
    };
    for expected in [chain.clone(), Error::ConflictingCertificates] {
        let server_error = expected.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                recorder_router(
                    SafetyErrorRecorder {
                        error: server_error,
                    },
                    peers(),
                ),
            )
            .await
            .unwrap();
        });
        let actual = tokio::task::spawn_blocking(move || {
            let client =
                HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2")
                    .unwrap();
            client.fetch_command_for(
                &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                "rhiza:sql:cluster-a".into(),
                1,
                1,
                LogHash::ZERO,
                LogHash::ZERO,
            )
        })
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(actual, expected);
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn http_recorder_panic_keeps_mutation_ambiguous_and_read_definite() {
    let recorder = PanicRecorder::default();
    let mutation_started = Arc::clone(&recorder.mutation_started);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, recorder_router(recorder, peers()))
            .await
            .unwrap();
    });

    let (mutation, read) = tokio::task::spawn_blocking(move || {
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        let context = rhiza_quepaxa::RecorderRpcContext::default_timeout();
        let mutation = client.record(&context, record_request("node-1", 11));
        let read = client.fetch_command_for(
            &context,
            "rhiza:sql:cluster-a".into(),
            1,
            1,
            LogHash::ZERO,
            LogHash::ZERO,
        );
        (mutation, read)
    })
    .await
    .unwrap();

    assert_eq!(mutation_started.load(Ordering::SeqCst), 1);
    assert!(matches!(mutation, Err(Error::UnknownOutcome)));
    assert!(matches!(read, Err(Error::ProposeFailed)));
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_readers_hold_authenticated_http_peer_permits_through_response_writes() {
    let payload = Arc::new(StoredCommand::new(
        EntryType::Command,
        vec![0x5a; rhiza_node::MAX_COMMAND_BYTES],
    ));
    let command_hash = payload.hash();
    let calls = Arc::new(AtomicUsize::new(0));
    let (completed_tx, mut completed_rx) = tokio::sync::mpsc::unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_payload = Arc::clone(&payload);
    let server_calls = Arc::clone(&calls);
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            recorder_router(
                LargeFetchRecorder {
                    payload: server_payload,
                    completed: completed_tx,
                    calls: server_calls,
                },
                peers(),
            ),
        )
        .await
        .unwrap();
    });
    let fetch_request = authenticated_http_request(
        address,
        RECORDER_FETCH_COMMAND_PATH,
        serde_json::json!({
            "version": 5,
            "remaining_deadline_ms": 1_000,
            "body": {
                "cluster_id": "rhiza:sql:cluster-a",
                "epoch": 1,
                "config_id": 1,
                "config_digest": LogHash::ZERO,
                "command_hash": command_hash,
            },
        }),
    );
    let slow_readers = tokio::task::spawn_blocking(move || {
        (0..DEFAULT_PEER_CONCURRENCY)
            .map(|_| {
                let mut stream = open_authenticated_slow_reader(address, &fetch_request);
                read_response_body_start(&mut stream);
                stream
            })
            .collect::<Vec<_>>()
    })
    .await
    .unwrap();
    for _ in 0..DEFAULT_PEER_CONCURRENCY {
        tokio::time::timeout(Duration::from_secs(2), completed_rx.recv())
            .await
            .expect("slow reader request did not reach the backend")
            .expect("slow reader completion channel closed unexpectedly");
    }
    assert_eq!(calls.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);

    let overloaded = tokio::task::spawn_blocking(move || {
        authenticated_http_response(
            address,
            RECORDER_IDENTITY_PATH,
            serde_json::json!({
                "version": 5,
                "remaining_deadline_ms": 1_000,
                "body": null,
            }),
        )
    })
    .await
    .unwrap();
    assert!(overloaded.0.contains(" 429 "), "status: {}", overloaded.0);
    assert_eq!(overloaded.1["body"]["status"], "Error");
    assert_eq!(overloaded.1["body"]["body"]["code"], "Io");
    assert_eq!(
        overloaded.1["body"]["body"]["detail"],
        serde_json::json!({"Message": "recorder RPC overloaded"})
    );
    assert_eq!(calls.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);

    drop(slow_readers);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let result = tokio::task::spawn_blocking(move || {
                let client =
                    HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2")
                        .unwrap();
                client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
                    Duration::from_millis(100),
                ))
            })
            .await
            .unwrap();
            if result.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("peer permits did not recover after slow-reader connections closed");
    let recovered = tokio::task::spawn_blocking(move || {
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        client.fetch_command_for(
            &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            "rhiza:sql:cluster-a".into(),
            1,
            1,
            LogHash::ZERO,
            command_hash,
        )
    })
    .await
    .unwrap()
    .expect("fresh fetch must return the large command");
    assert_eq!(recovered, Some((*payload).clone()));
    assert_eq!(calls.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY + 1);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn http_recorder_raw_auth_version_validation_and_deadline_responses_are_typed() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            recorder_router(CountingRecorder::default(), peers()),
        )
        .await
        .unwrap();
    });
    let record = serde_json::to_value(record_request("node-1", 20)).unwrap();
    let fetch = serde_json::json!({
        "cluster_id": "rhiza:sql:cluster-a",
        "epoch": 1,
        "config_id": 1,
        "config_digest": LogHash::ZERO,
        "command_hash": LogHash::ZERO,
    });

    let (unauthenticated, wrong_version, invalid_record, expired) =
        tokio::task::spawn_blocking(move || {
            let http = reqwest::blocking::Client::new();
            let base = format!("http://{address}");
            let authenticated = |path: &str, body: serde_json::Value| {
                http.post(format!("{base}{path}"))
                    .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
                    .header(NODE_ID_HEADER, "node-2")
                    .header(RECOVERY_GENERATION_HEADER, "1")
                    .bearer_auth("peer-token-2")
                    .json(&body)
                    .send()
                    .unwrap()
            };
            let unauthenticated = http
                .post(format!("{base}{RECORDER_FETCH_COMMAND_PATH}"))
                .json(&serde_json::json!({
                    "version": 5,
                    "remaining_deadline_ms": 1000,
                    "body": fetch.clone(),
                }))
                .send()
                .unwrap()
                .status();
            let wrong_version = authenticated(
                RECORDER_FETCH_COMMAND_PATH,
                serde_json::json!({
                    "version": 4,
                    "remaining_deadline_ms": 1000,
                    "body": fetch.clone(),
                }),
            )
            .json::<serde_json::Value>()
            .unwrap();
            let mut invalid_body = record;
            invalid_body["cluster_id"] = serde_json::json!("");
            let invalid_record = authenticated(
                RECORDER_RECORD_PATH,
                serde_json::json!({
                    "version": 5,
                    "remaining_deadline_ms": 1000,
                    "body": invalid_body,
                }),
            )
            .json::<serde_json::Value>()
            .unwrap();
            let expired = authenticated(
                RECORDER_FETCH_COMMAND_PATH,
                serde_json::json!({
                    "version": 5,
                    "remaining_deadline_ms": 0,
                    "body": fetch,
                }),
            )
            .json::<serde_json::Value>()
            .unwrap();
            (unauthenticated, wrong_version, invalid_record, expired)
        })
        .await
        .unwrap();

    assert_eq!(unauthenticated, reqwest::StatusCode::UNAUTHORIZED);
    for body in [wrong_version, invalid_record] {
        assert_eq!(body["body"]["status"], "Error");
        assert_eq!(body["body"]["body"]["code"], "Decode");
    }
    assert_eq!(expired["body"]["status"], "Error");
    assert_eq!(expired["body"]["body"]["code"], "RpcDeadlineExceeded");
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn http_recorder_content_type_and_malformed_json_are_post_auth_typed_decode_errors() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            recorder_router(CountingRecorder::default(), peers()),
        )
        .await
        .unwrap();
    });

    let (unauthenticated, wrong_content_type, malformed) = tokio::task::spawn_blocking(move || {
        let http = reqwest::blocking::Client::new();
        let url = format!("http://{address}{RECORDER_IDENTITY_PATH}");
        let authenticate = |request: reqwest::blocking::RequestBuilder| {
            request
                .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
                .header(NODE_ID_HEADER, "node-2")
                .header(RECOVERY_GENERATION_HEADER, "1")
                .bearer_auth("peer-token-2")
        };
        let decode = |response: reqwest::blocking::Response| {
            let status = response.status();
            let body = response.json::<serde_json::Value>().unwrap();
            (status, body)
        };

        let unauthenticated = http
            .post(&url)
            .header("Content-Type", "text/plain")
            .body("{}")
            .send()
            .unwrap()
            .status();
        let wrong_content_type = decode(
            authenticate(
                http.post(&url)
                    .header("Content-Type", "text/plain")
                    .body(r#"{"version":5,"remaining_deadline_ms":1000,"body":null}"#),
            )
            .send()
            .unwrap(),
        );
        let malformed = decode(
            authenticate(
                http.post(&url)
                    .header("Content-Type", "application/json; charset=utf-8")
                    .body("{"),
            )
            .send()
            .unwrap(),
        );
        (unauthenticated, wrong_content_type, malformed)
    })
    .await
    .unwrap();

    assert_eq!(unauthenticated, reqwest::StatusCode::UNAUTHORIZED);
    for (status, body) in [wrong_content_type, malformed] {
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(body["body"]["status"], "Error");
        assert_eq!(body["body"]["body"]["code"], "Decode");
    }
    server.abort();
}
