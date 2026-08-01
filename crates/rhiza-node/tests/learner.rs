use std::{sync::Arc, time::Duration};

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use rhiza_core::{
    Command, CommandKind, ConfigurationState, EntryType, LogAnchor, LogEntry, LogHash,
    RecoveryAnchor, SnapshotIdentity,
};
use rhiza_log::{FileLogStore, LogStore};
use rhiza_node::{
    certified_tail_router, recorder_router, validate_certified_tail_response,
    CertifiedTailErrorResponse, CertifiedTailRequest, CertifiedTailResponse,
    CertifiedTailValidationError, PeerConfig, TailReaderConfig, CERTIFIED_TAIL_PATH,
    DEFAULT_CERTIFIED_TAIL_ENTRIES, MAX_CERTIFIED_TAIL_ENCODED_BYTES,
    MAX_CERTIFIED_TAIL_ENTRY_PAYLOAD_BYTES, NODE_ID_HEADER, RECORDER_IDENTITY_PATH,
    RECORDER_PROTOCOL_VERSION, RECOVERY_GENERATION_HEADER, TAIL_CLUSTER_ID_HEADER,
    TAIL_CONFIG_ID_HEADER, TAIL_EPOCH_HEADER, TAIL_MEMBERSHIP_DIGEST_HEADER, TAIL_PROTOCOL_VERSION,
    TAIL_VERSION_HEADER, VERSION_HEADER,
};
use rhiza_quepaxa::{Membership, RecorderFileStore, ThreeNodeConsensus};
use tower::ServiceExt;

const CLUSTER_ID: &str = "rhiza:sql:cluster-a";
const TOKEN: &str = "tail-secret";

struct Fixture {
    consensus: Arc<ThreeNodeConsensus>,
    log: Arc<FileLogStore>,
    membership: Membership,
    entries: Vec<LogEntry>,
    _root: tempfile::TempDir,
}

impl Fixture {
    fn new(entry_count: u64) -> Self {
        Self::with_payloads(
            (1..=entry_count)
                .map(|index| format!("command-{index}").into_bytes())
                .collect(),
        )
    }

    fn with_payloads(payloads: Vec<Vec<u8>>) -> Self {
        let root = tempfile::tempdir().unwrap();
        let recorder_roots = [
            root.path().join("node-1"),
            root.path().join("node-2"),
            root.path().join("node-3"),
        ];
        let consensus =
            Arc::new(ThreeNodeConsensus::new(CLUSTER_ID, "node-1", 1, 1, recorder_roots).unwrap());
        let log = Arc::new(FileLogStore::open(root.path().join("qlog"), CLUSTER_ID, 1, 1).unwrap());
        let mut entries = Vec::new();
        let mut previous = LogHash::ZERO;
        for (offset, payload) in payloads.into_iter().enumerate() {
            let index = u64::try_from(offset).unwrap() + 1;
            let entry = consensus
                .propose_at(
                    rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                    index,
                    previous,
                    Command::new(CommandKind::Deterministic, payload),
                )
                .unwrap();
            log.append(&entry).unwrap();
            previous = entry.hash;
            entries.push(entry);
        }
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        Self {
            consensus,
            log,
            membership: Membership::new(["node-1", "node-2", "node-3"]).unwrap(),
            entries,
            _root: root,
        }
    }

    fn router(&self) -> axum::Router {
        certified_tail_router(
            Arc::clone(&self.consensus),
            Arc::clone(&self.log),
            self.config(),
        )
        .unwrap()
    }

    fn config(&self) -> TailReaderConfig {
        TailReaderConfig::new(CLUSTER_ID, 1, 1, self.membership.clone(), 7, TOKEN).unwrap()
    }

    fn compact_through(&self, entry_offset: usize) -> RecoveryAnchor {
        let entry = &self.entries[entry_offset];
        let anchor = RecoveryAnchor::new(
            CLUSTER_ID,
            1,
            ConfigurationState::active(1, LogHash::ZERO),
            7,
            LogAnchor::new(entry.index, entry.hash),
            SnapshotIdentity::new(
                format!("snapshot-{:015}", entry.index),
                LogHash::digest(&[b"snapshot", &entry.index.to_be_bytes()]),
                4096,
                LogHash::from_bytes([10; 32]),
            ),
        );
        self.log.compact_prefix(&anchor).unwrap();
        anchor
    }

    fn request(&self, from: LogAnchor, max_entries: u32) -> Request<Body> {
        Request::post(CERTIFIED_TAIL_PATH)
            .header("authorization", format!("Rhiza-Tail {TOKEN}"))
            .header(TAIL_VERSION_HEADER, TAIL_PROTOCOL_VERSION)
            .header(TAIL_CLUSTER_ID_HEADER, CLUSTER_ID)
            .header(TAIL_EPOCH_HEADER, "1")
            .header(TAIL_CONFIG_ID_HEADER, "1")
            .header(
                TAIL_MEMBERSHIP_DIGEST_HEADER,
                self.membership.digest().to_hex(),
            )
            .header(RECOVERY_GENERATION_HEADER, "7")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&CertifiedTailRequest { from, max_entries }).unwrap(),
            ))
            .unwrap()
    }
}

#[test]
fn certified_tail_request_uses_a_conservative_default_page_size() {
    let from = LogAnchor::new(0, LogHash::ZERO);
    let request = CertifiedTailRequest::from_anchor(from);

    assert_eq!(request.from, from);
    assert_eq!(request.max_entries, DEFAULT_CERTIFIED_TAIL_ENTRIES);
    assert!(request.max_entries <= 64);
}

#[tokio::test]
async fn certified_tail_returns_only_the_requested_bounded_consecutive_prefix() {
    let fixture = Fixture::new(3);
    let request = CertifiedTailRequest {
        from: LogAnchor::new(1, fixture.entries[0].hash),
        max_entries: 1,
    };
    let response = fixture
        .router()
        .oneshot(fixture.request(request.from, request.max_entries))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let response: CertifiedTailResponse = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(response.records.len(), 1);
    assert_eq!(
        response.observed_tip,
        LogAnchor::new(3, fixture.entries[2].hash)
    );
    assert_eq!(
        response
            .records
            .iter()
            .map(|record| record.entry.clone())
            .collect::<Vec<_>>(),
        fixture.entries[1..2]
    );
    validate_certified_tail_response(&fixture.config(), &request, &response).unwrap();
    let mut tampered = response.clone();
    tampered.records[0].entry.payload.push(0);
    assert_eq!(
        validate_certified_tail_response(&fixture.config(), &request, &tampered),
        Err(CertifiedTailValidationError::InvalidEntryHash)
    );
    for record in response.records {
        record
            .proof
            .validate_for_cluster(CLUSTER_ID, record.entry.index, 1, 1, &fixture.membership)
            .unwrap();
    }
}

#[tokio::test]
async fn certified_tail_requires_a_new_checkpoint_when_the_next_entry_was_compacted() {
    let fixture = Fixture::new(3);
    let checkpoint = fixture.compact_through(1);

    let response = fixture
        .router()
        .oneshot(fixture.request(
            LogAnchor::new(1, fixture.entries[0].hash),
            DEFAULT_CERTIFIED_TAIL_ENTRIES,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<CertifiedTailErrorResponse>(&bytes).unwrap(),
        CertifiedTailErrorResponse::RebaseRequired {
            checkpoint: *checkpoint.compacted(),
        }
    );
}

#[tokio::test]
async fn certified_tail_serves_the_retained_suffix_from_the_exact_compacted_anchor() {
    let fixture = Fixture::new(3);
    let checkpoint = fixture.compact_through(1);
    let request = CertifiedTailRequest {
        from: *checkpoint.compacted(),
        max_entries: 1,
    };

    let response = fixture
        .router()
        .oneshot(fixture.request(request.from, request.max_entries))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let response: CertifiedTailResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].entry, fixture.entries[2]);
    validate_certified_tail_response(&fixture.config(), &request, &response).unwrap();
}

#[tokio::test]
async fn certified_tail_rejects_a_wrong_hash_at_the_compacted_anchor() {
    let fixture = Fixture::new(3);
    let checkpoint = fixture.compact_through(1);
    let wrong_anchor = LogAnchor::new(
        checkpoint.compacted().index(),
        LogHash::digest(&[b"wrong compacted hash"]),
    );

    let response = fixture
        .router()
        .oneshot(fixture.request(wrong_anchor, 1))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn certified_tail_stops_before_the_encoded_page_limit() {
    let fixture = Fixture::with_payloads(vec![vec![255; 256 * 1024]; 12]);
    let response = fixture
        .router()
        .oneshot(fixture.request(
            LogAnchor::new(0, LogHash::ZERO),
            DEFAULT_CERTIFIED_TAIL_ENTRIES,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), MAX_CERTIFIED_TAIL_ENCODED_BYTES + 1)
        .await
        .unwrap();
    let response: CertifiedTailResponse = serde_json::from_slice(&bytes).unwrap();

    assert!(bytes.len() <= MAX_CERTIFIED_TAIL_ENCODED_BYTES);
    assert!(!response.records.is_empty());
    assert!(response.records.len() < fixture.entries.len());
}

#[tokio::test]
async fn certified_tail_rejects_an_oversized_first_entry_before_certification() {
    let fixture = Fixture::new(0);
    let payload = vec![255; MAX_CERTIFIED_TAIL_ENTRY_PAYLOAD_BYTES + 1];
    let local = LogEntry {
        cluster_id: CLUSTER_ID.into(),
        epoch: 1,
        config_id: 1,
        index: 1,
        entry_type: EntryType::Command,
        payload: payload.clone(),
        prev_hash: LogHash::ZERO,
        hash: LogEntry::calculate_hash(
            CLUSTER_ID,
            1,
            1,
            1,
            EntryType::Command,
            LogHash::ZERO,
            &payload,
        ),
    };
    fixture.log.append(&local).unwrap();

    let response = fixture
        .router()
        .oneshot(fixture.request(LogAnchor::new(0, LogHash::ZERO), 1))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn certified_tail_returns_nothing_until_the_local_entry_is_certified() {
    let fixture = Fixture::new(0);
    let local = LogEntry {
        cluster_id: CLUSTER_ID.into(),
        epoch: 1,
        config_id: 1,
        index: 1,
        entry_type: EntryType::Command,
        payload: b"local-only".to_vec(),
        prev_hash: LogHash::ZERO,
        hash: LogEntry::calculate_hash(
            CLUSTER_ID,
            1,
            1,
            1,
            EntryType::Command,
            LogHash::ZERO,
            b"local-only",
        ),
    };
    fixture.log.append(&local).unwrap();

    let response = fixture
        .router()
        .oneshot(fixture.request(LogAnchor::new(0, LogHash::ZERO), 1))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let response: CertifiedTailResponse = serde_json::from_slice(&bytes).unwrap();
    assert!(response.records.is_empty());
    assert_eq!(response.observed_tip, LogAnchor::new(1, local.hash));
}

#[tokio::test]
async fn certified_tail_fails_closed_when_the_certified_decision_differs_from_qlog() {
    let fixture = Fixture::new(1);
    let divergent_log = Arc::new(
        FileLogStore::open(
            fixture._root.path().join("divergent-qlog"),
            CLUSTER_ID,
            1,
            1,
        )
        .unwrap(),
    );
    let divergent = LogEntry {
        cluster_id: CLUSTER_ID.into(),
        epoch: 1,
        config_id: 1,
        index: 1,
        entry_type: EntryType::Command,
        payload: b"divergent".to_vec(),
        prev_hash: LogHash::ZERO,
        hash: LogEntry::calculate_hash(
            CLUSTER_ID,
            1,
            1,
            1,
            EntryType::Command,
            LogHash::ZERO,
            b"divergent",
        ),
    };
    divergent_log.append(&divergent).unwrap();
    let router = certified_tail_router(
        Arc::clone(&fixture.consensus),
        divergent_log,
        TailReaderConfig::new(CLUSTER_ID, 1, 1, fixture.membership.clone(), 7, TOKEN).unwrap(),
    )
    .unwrap();

    let response = router
        .oneshot(fixture.request(LogAnchor::new(0, LogHash::ZERO), 1))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn tail_credentials_require_the_exact_bound_context_and_are_not_bearer_credentials() {
    let fixture = Fixture::new(1);
    let headers = [
        "authorization",
        TAIL_VERSION_HEADER,
        TAIL_CLUSTER_ID_HEADER,
        TAIL_EPOCH_HEADER,
        TAIL_CONFIG_ID_HEADER,
        TAIL_MEMBERSHIP_DIGEST_HEADER,
        RECOVERY_GENERATION_HEADER,
    ];
    for missing in headers {
        let mut request = fixture.request(LogAnchor::new(0, LogHash::ZERO), 1);
        request.headers_mut().remove(missing);
        assert_eq!(
            fixture.router().oneshot(request).await.unwrap().status(),
            StatusCode::UNAUTHORIZED,
            "missing {missing} must fail"
        );
    }
    let wrong_digest = LogHash::ZERO.to_hex();
    for (header, wrong) in [
        (TAIL_VERSION_HEADER, "2"),
        (TAIL_CLUSTER_ID_HEADER, "rhiza:sql:cluster-b"),
        (TAIL_EPOCH_HEADER, "2"),
        (TAIL_CONFIG_ID_HEADER, "2"),
        (TAIL_MEMBERSHIP_DIGEST_HEADER, wrong_digest.as_str()),
        (RECOVERY_GENERATION_HEADER, "8"),
    ] {
        let mut request = fixture.request(LogAnchor::new(0, LogHash::ZERO), 1);
        request.headers_mut().insert(header, wrong.parse().unwrap());
        assert_eq!(
            fixture.router().oneshot(request).await.unwrap().status(),
            StatusCode::UNAUTHORIZED,
            "wrong {header} must fail"
        );
    }

    let mut bearer = fixture.request(LogAnchor::new(0, LogHash::ZERO), 1);
    bearer
        .headers_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    assert_eq!(
        fixture.router().oneshot(bearer).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );

    let peer = PeerConfig::new("node-1", "http://node-1:8081", TOKEN).unwrap();
    assert_ne!(
        format!("Rhiza-Tail {TOKEN}"),
        format!("Bearer {}", peer.token())
    );

    let recorder = RecorderFileStore::new_with_membership(
        fixture._root.path().join("auth-recorder"),
        "node-1",
        CLUSTER_ID,
        1,
        1,
        fixture.membership.clone(),
    )
    .unwrap();
    let recorder_request = Request::post(RECORDER_IDENTITY_PATH)
        .header("authorization", format!("Rhiza-Tail {TOKEN}"))
        .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    assert_eq!(
        recorder_router(recorder, vec![peer])
            .oneshot(recorder_request)
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[test]
fn tail_config_redacts_its_dedicated_token() {
    let config = TailReaderConfig::new(
        CLUSTER_ID,
        1,
        1,
        Membership::new(["node-1", "node-2", "node-3"]).unwrap(),
        7,
        TOKEN,
    )
    .unwrap();
    let debug = format!("{config:?}");
    assert!(!debug.contains(TOKEN));
    assert!(debug.contains("[redacted]"));
}
