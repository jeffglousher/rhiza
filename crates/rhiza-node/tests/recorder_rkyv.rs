use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use rhiza_core::{EntryType, LogHash, StoredCommand};
use rhiza_node::{
    serve_recorder_rkyv_tcp, serve_recorder_tcp, PeerConfig, TcpPostcardRecorderClient,
    TcpRkyvRecorderClient,
};
use rhiza_quepaxa::{
    AcceptedValue, DecisionProof, Membership, Proposal, ProposalPriority, ReadFenceObservation,
    ReadFenceRequest, ReadFenceSlotState, RecordRequest, RecordSummary, RecorderRpc,
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

fn request(slot: u64) -> RecordRequest {
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
            "node-1",
            slot,
            AcceptedValue::from_command("rhiza:sql:cluster-a", slot, 1, 1, LogHash::ZERO, &command),
        ),
        command: Some(command),
    }
}

fn proof(slot: u64) -> DecisionProof {
    let request = request(slot);
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

#[derive(Clone, Default)]
struct Probe {
    commands: Arc<Mutex<HashMap<LogHash, StoredCommand>>>,
    records: Arc<Mutex<HashMap<u64, RecordSummary>>>,
    proof: Arc<Mutex<Option<DecisionProof>>>,
}

impl RecorderRpc for Probe {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
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
        self.commands.lock().unwrap().insert(command_hash, command);
        Ok(())
    }

    fn fetch_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        Ok(self.commands.lock().unwrap().get(&command_hash).cloned())
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        let summary = RecordSummary {
            recorder_id: "node-1".into(),
            slot: request.slot,
            config_id: request.config_id,
            config_digest: request.config_digest,
            step: request.step,
            first_current: Some(request.proposal),
            aggregate_prior: None,
            decided: None,
        };
        self.records
            .lock()
            .unwrap()
            .insert(summary.slot, summary.clone());
        Ok(summary)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        _membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        *self.proof.lock().unwrap() = Some(proof);
        Ok(())
    }

    fn inspect_decision_proof(&self, _slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        Ok(self.proof.lock().unwrap().clone())
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        Ok(self.records.lock().unwrap().get(&slot).cloned())
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<ReadFenceObservation> {
        let records = self.records.lock().unwrap();
        let max_head = records.keys().copied().max();
        let summary = records.get(&request.slot).cloned().map(Box::new);
        Ok(ReadFenceObservation {
            recorder_id: "node-1".into(),
            cluster_id: request.cluster_id,
            epoch: request.epoch,
            config_id: request.config_id,
            config_digest: request.config_digest,
            slot: request.slot,
            max_head,
            slot_state: if summary.is_some() {
                ReadFenceSlotState::Occupied { summary }
            } else {
                ReadFenceSlotState::Empty
            },
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rkyv_round_trips_all_recorder_operations() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_recorder_rkyv_tcp(
        listener,
        Probe::default(),
        peers(),
        7,
        std::future::pending(),
    ));

    tokio::task::spawn_blocking(move || {
        let client =
            TcpRkyvRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7).unwrap();
        assert_eq!(client.recorder_id().unwrap(), "node-1");

        let command = StoredCommand::new(EntryType::Command, b"payload".to_vec());
        let command_hash = command.hash();
        let digest = Membership::new(["node-1", "node-2", "node-3"])
            .unwrap()
            .digest();
        client
            .store_command_for(
                "rhiza:sql:cluster-a".into(),
                1,
                1,
                digest,
                command_hash,
                command.clone(),
            )
            .unwrap();
        assert_eq!(
            client
                .fetch_command_for("rhiza:sql:cluster-a".into(), 1, 1, digest, command_hash,)
                .unwrap(),
            Some(command)
        );

        assert_eq!(client.record(request(4)).unwrap().slot, 4);
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        client
            .install_decision_proof(proof(4), &membership)
            .unwrap();
        assert!(client.inspect_decision_proof(4).unwrap().is_some());
        assert!(client.inspect_record_summary(4).unwrap().is_some());
        assert!(matches!(
            client
                .observe_read_fence(ReadFenceRequest {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: digest,
                    slot: 4,
                })
                .unwrap()
                .slot_state,
            ReadFenceSlotState::Occupied { .. }
        ));
    })
    .await
    .unwrap();

    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_and_rkyv_plaintext_fail_closed_when_misconfigured() {
    let rkyv_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rkyv_address = rkyv_listener.local_addr().unwrap();
    let rkyv_server = tokio::spawn(serve_recorder_rkyv_tcp(
        rkyv_listener,
        Probe::default(),
        peers(),
        7,
        std::future::pending(),
    ));

    let postcard_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let postcard_address = postcard_listener.local_addr().unwrap();
    let postcard_server = tokio::spawn(serve_recorder_tcp(
        postcard_listener,
        Probe::default(),
        peers(),
        7,
        std::future::pending(),
    ));

    tokio::task::spawn_blocking(move || {
        let postcard_to_rkyv =
            TcpPostcardRecorderClient::new(rkyv_address, "node-1", "node-2", "peer-token-2", 7)
                .unwrap();
        assert!(postcard_to_rkyv.recorder_id().is_err());

        let rkyv_to_postcard =
            TcpRkyvRecorderClient::new(postcard_address, "node-1", "node-2", "peer-token-2", 7)
                .unwrap();
        assert!(rkyv_to_postcard.recorder_id().is_err());
    })
    .await
    .unwrap();

    rkyv_server.abort();
    postcard_server.abort();
}
