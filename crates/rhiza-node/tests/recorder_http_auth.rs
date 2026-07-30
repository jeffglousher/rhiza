use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use rhiza_core::{EntryType, LogHash, StoredCommand};
use rhiza_node::{
    recorder_router, HttpRecorderClient, PeerConfig, NODE_ID_HEADER, RECORDER_FETCH_COMMAND_PATH,
    RECORDER_IDENTITY_PATH, RECORDER_PROTOCOL_VERSION, RECORDER_RECORD_PATH,
    RECOVERY_GENERATION_HEADER, VERSION_HEADER,
};
use rhiza_quepaxa::{
    AcceptedValue, DecisionProof, Error, Membership, Proposal, ProposalPriority, RecordRequest,
    RecordSummary, RecorderRpc, RejectReason,
};

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
