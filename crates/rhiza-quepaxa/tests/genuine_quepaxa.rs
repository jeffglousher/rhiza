use proptest::prelude::*;
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
    time::Duration,
};

use rhiza_core::{Command, CommandKind, EntryType, LogHash, StoredCommand};
use rhiza_quepaxa::{
    AcceptedValue, CertifiedDecisionInspection, ConfigChange, DecisionProof, DriveOutcome, Error,
    IsrState, Membership, Proposal, ProposalPriority, ProposerProgress, ReadFenceObservation,
    ReadFenceRequest, ReadFenceSlotState, RecordRequest, RecordSummary, RecorderFileStore,
    RecorderRpc, RecorderSummary, RejectReason, ThreeNodeConsensus,
};

fn value(byte: u8) -> AcceptedValue {
    let command = StoredCommand::new(EntryType::Command, vec![byte]);
    AcceptedValue::from_command("cluster", 7, 2, 3, LogHash::ZERO, &command)
}

fn proposal(priority: u64, proposer: &str, id: u64, byte: u8) -> Proposal {
    Proposal::new(
        ProposalPriority::from_u64(priority),
        proposer,
        id,
        value(byte),
    )
}

#[test]
fn proposal_order_binds_value_across_proposer_restart_id_collision() {
    let left = proposal(9, "p", 4, 1);
    let right = proposal(9, "p", 4, 2);
    assert_ne!(left, right);
    assert_ne!(left.cmp(&right), std::cmp::Ordering::Equal);
    assert_eq!(left.cmp(&right), right.cmp(&left).reverse());
    assert_eq!(Proposal::nil().cmp(&left), std::cmp::Ordering::Less);
}

#[test]
fn constant_space_isr_matches_algorithm_three() {
    let p1 = proposal(1, "a", 1, 1);
    let p2 = proposal(2, "b", 1, 2);
    let p3 = proposal(3, "c", 1, 3);

    let (s1, r1) = IsrState::default().record(4, p1.clone());
    assert_eq!(
        (r1.step, r1.first_current, r1.aggregate_prior),
        (4, Some(p1.clone()), None)
    );

    let (s2, r2) = s1.record(4, p2.clone());
    assert_eq!(r2.first_current, Some(p1));
    assert_eq!(s2.aggregate_current(), Some(&p2));

    let (s3, r3) = s2.record(5, p3.clone());
    assert_eq!(r3.aggregate_prior, Some(p2));
    assert_eq!(r3.first_current, Some(p3.clone()));

    let (skipped, reply) = s3.record(8, p3);
    assert_eq!(reply.step, 8);
    assert_eq!(reply.aggregate_prior, None);
    assert_eq!(skipped.step(), 8);
}

proptest! {
    #[test]
    fn isr_stale_duplicate_reorder_and_skip_invariants(
        start in 1_u64..100,
        priorities in prop::collection::vec(1_u64..10_000, 1..40),
        deltas in prop::collection::vec(0_u64..5, 1..40),
    ) {
        let mut state = IsrState::default();
        for (index, (&priority, &delta)) in priorities.iter().zip(&deltas).enumerate() {
            let requested = start.saturating_add(delta);
            let before = state.clone();
            let p = proposal(priority, "p", index as u64 + 1, index as u8);
            let (next, reply) = state.record(requested, p);
            prop_assert!(next.step() >= before.step());
            prop_assert_eq!(reply.step, next.step());
            if requested < before.step() {
                prop_assert_eq!(&next, &before);
            }
            if requested > before.step().saturating_add(1) {
                prop_assert_eq!(next.aggregate_prior(), None);
            }
            state = next;
        }
    }
}

fn consensus(root: &Path, proposer: &str) -> ThreeNodeConsensus {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (id.clone(), Box::new(store) as Box<dyn RecorderRpc>)
        })
        .collect();
    ThreeNodeConsensus::from_recorders_with_ids("cluster", proposer, 1, 1, recorders).unwrap()
}

#[test]
fn command_registration_surfaces_a_hash_mismatch() {
    let root = tempfile::tempdir().unwrap();
    let consensus = consensus(root.path(), "n1");

    assert_eq!(
        consensus.register_command(LogHash::ZERO, b"different hash".to_vec()),
        Err(Error::CommandHashMismatch)
    );
}

#[test]
fn preferred_proposer_decides_in_step_four_with_fast_proof() {
    let root = tempfile::tempdir().unwrap();
    let consensus = consensus(root.path(), "n1");
    let command = StoredCommand::new(EntryType::Command, b"fast".to_vec());
    consensus
        .register_command(command.hash(), command.payload.clone())
        .unwrap();
    let outcome = consensus
        .drive(ProposerProgress::new(
            1,
            Proposal::new(
                ProposalPriority::MAX,
                "n1",
                1,
                AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
            ),
        ))
        .unwrap();
    assert!(matches!(
        outcome,
        DriveOutcome::Decision(DecisionProof::FastPath { .. })
    ));
    assert!(matches!(
        consensus
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Committed(_)
    ));
    for id in ["n1", "n2", "n3"] {
        let store = RecorderFileStore::new_with_membership(
            root.path().join(id),
            id,
            "cluster",
            1,
            1,
            Membership::new(["n1", "n2", "n3"]).unwrap(),
        );
        assert!(store.is_err(), "open recorder ownership remains exclusive");
    }
}

#[test]
fn unsorted_recorder_pairs_preserve_identity_and_reach_quorum() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let recorders = ["n3", "n1", "n2"]
        .into_iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.path().join(id),
                id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (id.to_owned(), Box::new(store) as Box<dyn RecorderRpc>)
        })
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    let entry = consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"unsorted-recorders".to_vec()),
        )
        .unwrap();

    assert_eq!(entry.payload, b"unsorted-recorders");
}

#[test]
fn root_constructor_installs_membership_before_proof_installation() {
    let root = tempfile::tempdir().unwrap();
    let recorder_roots = ["n1", "n2", "n3"].map(|id| root.path().join(id));
    let consensus = ThreeNodeConsensus::new("cluster", "n1", 1, 1, recorder_roots).unwrap();

    let entry = consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"configured-roots".to_vec()),
        )
        .unwrap();

    assert_eq!(entry.payload, b"configured-roots");
}

#[derive(Debug)]
struct FixedPriority;

impl rhiza_quepaxa::PrioritySource for FixedPriority {
    fn sample(
        &self,
        _slot: u64,
        round: u64,
        proposer: &str,
        recorder: &str,
    ) -> Result<ProposalPriority, Error> {
        let seed = round + proposer.as_bytes()[0] as u64 + recorder.as_bytes()[1] as u64;
        Ok(ProposalPriority::from_u64(seed.max(1)))
    }
}

#[derive(Debug)]
struct FailingPriority;

impl rhiza_quepaxa::PrioritySource for FailingPriority {
    fn sample(
        &self,
        _slot: u64,
        _round: u64,
        _proposer: &str,
        _recorder: &str,
    ) -> Result<ProposalPriority, Error> {
        Err(Error::RandomnessUnavailable(
            "injected entropy failure".into(),
        ))
    }
}

#[test]
fn priority_randomness_failure_is_typed_and_fail_stop() {
    let root = tempfile::tempdir().unwrap();
    let consensus = consensus(root.path(), "n2").with_priority_source(Arc::new(FailingPriority));
    let command = StoredCommand::new(EntryType::Command, b"rng-failure".to_vec());
    consensus
        .register_command(command.hash(), command.payload.clone())
        .unwrap();
    let progress = ProposerProgress::new(
        1,
        Proposal::new(
            ProposalPriority::MAX,
            "n2",
            1,
            AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
        ),
    );

    assert!(matches!(
        consensus.drive(progress),
        Err(Error::RandomnessUnavailable(message)) if message.contains("entropy")
    ));
}

#[test]
fn non_preferred_proposer_uses_leaderless_four_phase_path() {
    let root = tempfile::tempdir().unwrap();
    let consensus = consensus(root.path(), "n2").with_priority_source(Arc::new(FixedPriority));
    let command = StoredCommand::new(EntryType::Command, b"slow".to_vec());
    consensus
        .register_command(command.hash(), command.payload.clone())
        .unwrap();
    let mut progress = ProposerProgress::new(
        1,
        Proposal::new(
            ProposalPriority::MAX,
            "n2",
            1,
            AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
        ),
    );
    let proof = loop {
        match consensus.drive(progress).unwrap() {
            DriveOutcome::Progress(next) | DriveOutcome::Pending(next) => progress = next,
            DriveOutcome::Decision(proof) => break proof,
        }
    };
    assert!(matches!(proof, DecisionProof::Phase2 { step, .. } if step % 4 == 2));
    assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    assert!(matches!(
        consensus
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Committed(_)
    ));
    assert!(matches!(
        consensus.recover_decision_at(1, LogHash::ZERO).unwrap(),
        rhiza_quepaxa::DecisionInspection::Committed(entry) if entry.payload == b"slow"
    ));
}

#[test]
fn proof_validation_rejects_tampering_quorum_config_and_step() {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let p = proposal(u64::MAX, "n1", 1, 1);
    let summaries = ["n1", "n2"]
        .into_iter()
        .map(|id| RecorderSummary {
            recorder_id: id.to_string(),
            slot: 7,
            step: 4,
            first_current: Some(Proposal::new(ProposalPriority::MAX, "n1", 1, value(1))),
            aggregate_prior: None,
        })
        .collect();
    let proof = DecisionProof::FastPath {
        cluster_id: "cluster".into(),
        slot: 7,
        epoch: 2,
        config_id: 3,
        config_digest: membership.digest(),
        proposal: Proposal::new(ProposalPriority::MAX, "n1", 1, value(1)),
        summaries,
    };
    proof.validate_for(7, 2, 3, &membership).unwrap();
    assert_eq!(
        proof.validate_for_cluster("other-cluster", 7, 2, 3, &membership),
        Err(RejectReason::WrongCluster)
    );

    let mut tampered = proof.clone();
    let DecisionProof::FastPath { summaries, .. } = &mut tampered else {
        unreachable!()
    };
    summaries[1].recorder_id = "n1".into();
    assert_eq!(
        tampered.validate_for(7, 2, 3, &membership),
        Err(RejectReason::InvalidCertificate)
    );
    assert_eq!(
        proof.validate_for(8, 2, 3, &membership),
        Err(RejectReason::MalformedDecision)
    );
    assert_eq!(
        proof.validate_for(7, 2, 4, &membership),
        Err(RejectReason::WrongConfig)
    );

    let DecisionProof::FastPath { summaries, .. } = proof.clone() else {
        unreachable!()
    };
    let phase2 = DecisionProof::Phase2 {
        cluster_id: "cluster".into(),
        slot: 7,
        epoch: 2,
        config_id: 3,
        config_digest: membership.digest(),
        step: 5,
        proposal: p,
        summaries,
    };
    assert_eq!(
        phase2.validate_for(7, 2, 3, &membership),
        Err(RejectReason::InvalidCertificate)
    );
}

#[derive(Clone)]
struct FailingRecord {
    store: RecorderFileStore,
    failures: Arc<AtomicUsize>,
}

impl RecorderRpc for FailingRecord {
    fn recorder_id(&self) -> Result<String, Error> {
        self.store.recorder_id()
    }

    fn store_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> Result<(), Error> {
        self.store.store_command_for(
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
    ) -> Result<Option<StoredCommand>, Error> {
        self.store
            .fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash)
    }

    fn record(&self, request: RecordRequest) -> Result<RecordSummary, Error> {
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return Err(Error::Io("scripted deadline".into()));
        }
        self.store.record(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<(), Error> {
        self.store.install_decision_proof(proof, membership)
    }
}

#[test]
fn drive_has_no_fixed_retry_cap_and_eventually_decides() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (
                id.clone(),
                Box::new(FailingRecord {
                    store,
                    failures: Arc::new(AtomicUsize::new(12)),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"eventual".to_vec());
    consensus
        .register_command(command.hash(), command.payload.clone())
        .unwrap();
    let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
    let mut progress =
        ProposerProgress::new(1, Proposal::new(ProposalPriority::MAX, "n1", 1, value));
    let mut pending = 0;
    loop {
        match consensus.drive(progress).unwrap() {
            DriveOutcome::Pending(next) => {
                pending += 1;
                progress = next;
            }
            DriveOutcome::Progress(next) => progress = next,
            DriveOutcome::Decision(_) => break,
        }
        assert!(pending < 30);
    }
    assert!(pending > 8, "the run exceeded the removed legacy retry cap");
}

#[test]
fn three_interleaved_proposers_cooperate_on_one_value() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let stores: Vec<_> = membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    let engines: Vec<_> = ["n1", "n2", "n3"]
        .into_iter()
        .map(|proposer| {
            let recorders = membership
                .members()
                .iter()
                .zip(&stores)
                .map(|(id, store)| (id.clone(), Box::new(store.clone()) as Box<dyn RecorderRpc>))
                .collect();
            Arc::new(
                ThreeNodeConsensus::from_recorders_with_ids("cluster", proposer, 1, 1, recorders)
                    .unwrap(),
            )
        })
        .collect();
    let mut proposers: Vec<_> = engines
        .into_iter()
        .enumerate()
        .map(|(index, engine)| {
            let command =
                StoredCommand::new(EntryType::Command, format!("value-{index}").into_bytes());
            engine
                .register_command(command.hash(), command.payload.clone())
                .unwrap();
            let accepted = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
            let proposer = format!("n{}", index + 1);
            (
                engine,
                Some(ProposerProgress::new(
                    1,
                    Proposal::new(ProposalPriority::MAX, proposer, 1, accepted),
                )),
                None,
            )
        })
        .collect();
    for _ in 0..1_000 {
        for (engine, progress, decision) in &mut proposers {
            let Some(current) = progress.take() else {
                continue;
            };
            match engine.drive(current).unwrap() {
                DriveOutcome::Pending(next) | DriveOutcome::Progress(next) => {
                    *progress = Some(next);
                }
                DriveOutcome::Decision(proof) => {
                    *decision = proof.proposal().value.clone();
                }
            }
        }
        if proposers.iter().all(|(_, _, decision)| decision.is_some()) {
            break;
        }
    }
    let values: Vec<_> = proposers
        .into_iter()
        .filter_map(|(_, _, decision)| decision)
        .collect();
    assert_eq!(values.len(), 3);
    assert!(values.windows(2).all(|pair| pair[0] == pair[1]));
}

#[test]
fn recorder_crash_reopen_reconstructs_decision_from_phase_state() {
    let root = tempfile::tempdir().unwrap();
    let engine = consensus(root.path(), "n1");
    let before = engine
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"reopen".to_vec()),
        )
        .unwrap();
    assert!(engine.finish_pending_rpcs(Duration::from_secs(1)));
    assert!(engine.inspect_decision_proof_at(1).unwrap().is_none());
    assert!(engine.finish_pending_rpcs(Duration::from_secs(1)));
    drop(engine);

    let reopened = consensus(root.path(), "n3").with_priority_source(Arc::new(FixedPriority));
    assert!(matches!(
        reopened
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Committed(_)
    ));
    let after = reopened
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"different".to_vec()),
        )
        .unwrap();
    assert_eq!(before, after);
}

#[test]
fn proof_cache_absent_restart_recovers_after_one_recorder_is_lost() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let producer = consensus(root.path(), "n1");
    let before = producer
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(
                CommandKind::Deterministic,
                b"restart-minority-loss".to_vec(),
            ),
        )
        .unwrap();
    assert!(producer.finish_pending_rpcs(Duration::from_secs(1)));
    assert!(producer.inspect_decision_proof_at(1).unwrap().is_none());
    drop(producer);

    let recorders = ["n1", "n2"]
        .into_iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.path().join(id),
                id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (id.to_string(), Box::new(store) as Box<dyn RecorderRpc>)
        })
        .chain(std::iter::once((
            "n3".into(),
            Box::new(DeadRecorder) as Box<dyn RecorderRpc>,
        )))
        .collect();
    let restarted =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n2", 1, 1, recorders).unwrap();

    let inspection = restarted
        .inspect_certified_decision_at(1, LogHash::ZERO)
        .unwrap();
    let CertifiedDecisionInspection::Committed(certified) = inspection else {
        panic!("surviving typed quorum did not reconstruct the decision: {inspection:?}");
    };
    assert_eq!(certified.entry, before);
    assert!(matches!(certified.proof, DecisionProof::FastPath { .. }));
}

struct StaleSummaryRecorder {
    id: String,
    summary: RecordSummary,
}

#[derive(Clone)]
struct TypedInspectionRecorder {
    store: RecorderFileStore,
    proof_inspections: Arc<AtomicUsize>,
    summary_inspections: Arc<AtomicUsize>,
}

impl RecorderRpc for TypedInspectionRecorder {
    fn recorder_id(&self) -> Result<String, Error> {
        self.store.recorder_id()
    }

    fn fetch_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> Result<Option<StoredCommand>, Error> {
        self.store
            .fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash)
    }

    fn inspect_decision_proof(&self, _slot: u64) -> Result<Option<DecisionProof>, Error> {
        self.proof_inspections.fetch_add(1, Ordering::SeqCst);
        Err(Error::Io(
            "typed inspection must not issue a separate proof RPC".into(),
        ))
    }

    fn inspect_record_summary(&self, slot: u64) -> Result<Option<RecordSummary>, Error> {
        self.summary_inspections.fetch_add(1, Ordering::SeqCst);
        self.store.inspect_record_summary(slot)
    }
}

fn recorder_stores(root: &Path) -> (Membership, Vec<(String, RecorderFileStore)>) {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let stores = membership
        .members()
        .iter()
        .map(|id| {
            (
                id.clone(),
                RecorderFileStore::new_with_membership(
                    root.join(id),
                    id,
                    "cluster",
                    1,
                    1,
                    membership.clone(),
                )
                .unwrap(),
            )
        })
        .collect();
    (membership, stores)
}

fn typed_inspector(
    stores: &[(String, RecorderFileStore)],
    proof_inspections: Arc<AtomicUsize>,
    summary_inspections: Arc<AtomicUsize>,
) -> ThreeNodeConsensus {
    let recorders = stores
        .iter()
        .map(|(id, store)| {
            (
                id.clone(),
                Box::new(TypedInspectionRecorder {
                    store: store.clone(),
                    proof_inspections: Arc::clone(&proof_inspections),
                    summary_inspections: Arc::clone(&summary_inspections),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap()
}

#[derive(Clone)]
enum SummaryGate {
    Immediate,
    WaitForStart(Arc<(Mutex<bool>, Condvar)>),
    SignalAndBlock {
        started: Arc<(Mutex<bool>, Condvar)>,
        release: Arc<(Mutex<bool>, Condvar)>,
    },
}

#[derive(Clone)]
struct QueuedFetchInspectionRecorder {
    id: String,
    membership: Membership,
    commands: Vec<(u64, StoredCommand)>,
    summary_gate: SummaryGate,
    command_slots: Vec<u64>,
    fetches: Option<Arc<AtomicUsize>>,
}

impl QueuedFetchInspectionRecorder {
    fn summary(&self, slot: u64) -> RecordSummary {
        let command = self
            .commands
            .iter()
            .find_map(|(command_slot, command)| (*command_slot == slot).then_some(command))
            .expect("requested slot has a scripted command");
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            slot,
            AcceptedValue::from_command("cluster", slot, 1, 1, LogHash::ZERO, command),
        );
        let proof = DecisionProof::FastPath {
            cluster_id: "cluster".into(),
            slot,
            epoch: 1,
            config_id: 1,
            config_digest: self.membership.digest(),
            proposal: proposal.clone(),
            summaries: ["n1", "n2"]
                .into_iter()
                .map(|recorder_id| RecorderSummary {
                    recorder_id: recorder_id.into(),
                    slot,
                    step: 4,
                    first_current: Some(proposal.clone()),
                    aggregate_prior: None,
                })
                .collect(),
        };
        RecordSummary {
            recorder_id: self.id.clone(),
            slot,
            config_id: 1,
            config_digest: self.membership.digest(),
            step: 5,
            first_current: None,
            aggregate_prior: None,
            decided: Some(proof),
        }
    }
}

impl RecorderRpc for QueuedFetchInspectionRecorder {
    fn recorder_id(&self) -> Result<String, Error> {
        Ok(self.id.clone())
    }

    fn fetch_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        command_hash: LogHash,
    ) -> Result<Option<StoredCommand>, Error> {
        if let Some(fetches) = &self.fetches {
            fetches.fetch_add(1, Ordering::SeqCst);
        }
        Ok(self.commands.iter().find_map(|(slot, command)| {
            (self.command_slots.contains(slot) && command.hash() == command_hash)
                .then_some(command.clone())
        }))
    }

    fn inspect_record_summary(&self, slot: u64) -> Result<Option<RecordSummary>, Error> {
        match &self.summary_gate {
            SummaryGate::Immediate => {}
            SummaryGate::WaitForStart(started) => {
                let (started, condition) = &**started;
                drop(
                    condition
                        .wait_while(started.lock().unwrap(), |started| !*started)
                        .unwrap(),
                );
            }
            SummaryGate::SignalAndBlock { started, release } => {
                let (started, condition) = &**started;
                *started.lock().unwrap() = true;
                condition.notify_all();
                let (released, condition) = &**release;
                drop(
                    condition
                        .wait_while(released.lock().unwrap(), |released| !*released)
                        .unwrap(),
                );
            }
        }
        if self.id == "n3" {
            Ok(None)
        } else {
            Ok(Some(self.summary(slot)))
        }
    }
}

#[test]
fn certified_inspection_discards_queued_minority_fetch_before_the_next_slot() {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let commands = vec![
        (1, StoredCommand::new(EntryType::Command, b"first".to_vec())),
        (
            2,
            StoredCommand::new(EntryType::Command, b"second".to_vec()),
        ),
    ];
    let summary_started = Arc::new((Mutex::new(false), Condvar::new()));
    let summary_release = Arc::new((Mutex::new(false), Condvar::new()));
    let minority_fetches = Arc::new(AtomicUsize::new(0));
    let recorders = vec![
        (
            "n1".into(),
            Box::new(QueuedFetchInspectionRecorder {
                id: "n1".into(),
                membership: membership.clone(),
                commands: commands.clone(),
                summary_gate: SummaryGate::WaitForStart(Arc::clone(&summary_started)),
                command_slots: vec![1],
                fetches: None,
            }) as Box<dyn RecorderRpc>,
        ),
        (
            "n2".into(),
            Box::new(QueuedFetchInspectionRecorder {
                id: "n2".into(),
                membership: membership.clone(),
                commands: commands.clone(),
                summary_gate: SummaryGate::Immediate,
                command_slots: Vec::new(),
                fetches: None,
            }) as Box<dyn RecorderRpc>,
        ),
        (
            "n3".into(),
            Box::new(QueuedFetchInspectionRecorder {
                id: "n3".into(),
                membership,
                commands,
                summary_gate: SummaryGate::SignalAndBlock {
                    started: summary_started,
                    release: Arc::clone(&summary_release),
                },
                command_slots: vec![1, 2],
                fetches: Some(Arc::clone(&minority_fetches)),
            }) as Box<dyn RecorderRpc>,
        ),
    ];
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    assert!(matches!(
        consensus
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Committed(_)
    ));

    let (released, condition) = &*summary_release;
    *released.lock().unwrap() = true;
    condition.notify_all();
    assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    assert_eq!(
        minority_fetches.load(Ordering::SeqCst),
        0,
        "the completed inspection must not run its queued minority command fetch"
    );

    assert!(matches!(
        consensus
            .inspect_certified_decision_at(2, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Committed(_)
    ));
    assert_eq!(minority_fetches.load(Ordering::SeqCst), 1);
}

#[test]
fn typed_inspection_reconstructs_fast_proof_without_a_proof_cache_or_proof_rpc() {
    let root = tempfile::tempdir().unwrap();
    let (_, stores) = recorder_stores(root.path());
    let recorders = stores
        .iter()
        .map(|(id, store)| (id.clone(), Box::new(store.clone()) as Box<dyn RecorderRpc>))
        .collect();
    let producer =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
    let committed = producer
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"fast-summary-proof".to_vec()),
        )
        .unwrap();
    assert!(producer.finish_pending_rpcs(Duration::from_secs(1)));
    assert!(stores
        .iter()
        .all(|(_, store)| store.inspect_decision_proof(1).unwrap().is_none()));

    let proof_inspections = Arc::new(AtomicUsize::new(0));
    let summary_inspections = Arc::new(AtomicUsize::new(0));
    let inspector = typed_inspector(
        &stores,
        Arc::clone(&proof_inspections),
        Arc::clone(&summary_inspections),
    );
    let inspected = inspector
        .inspect_certified_decision_at(1, LogHash::ZERO)
        .unwrap();

    let CertifiedDecisionInspection::Committed(certified) = inspected else {
        panic!("expected committed fast-path decision, got {inspected:?}");
    };
    assert_eq!(certified.entry, committed);
    assert!(matches!(certified.proof, DecisionProof::FastPath { .. }));
    assert_eq!(proof_inspections.load(Ordering::SeqCst), 0);
    assert!(summary_inspections.load(Ordering::SeqCst) >= 2);
}

#[test]
fn typed_inspection_reconstructs_phase2_proof_without_a_proof_cache_or_proof_rpc() {
    let root = tempfile::tempdir().unwrap();
    let (_, stores) = recorder_stores(root.path());
    let recorders = stores
        .iter()
        .map(|(id, store)| (id.clone(), Box::new(store.clone()) as Box<dyn RecorderRpc>))
        .collect();
    let producer = ThreeNodeConsensus::from_recorders_with_ids("cluster", "n2", 1, 1, recorders)
        .unwrap()
        .with_priority_source(Arc::new(FixedPriority));
    let committed = producer
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"phase2-summary-proof".to_vec()),
        )
        .unwrap();
    assert!(producer.finish_pending_rpcs(Duration::from_secs(1)));
    assert!(stores
        .iter()
        .all(|(_, store)| store.inspect_decision_proof(1).unwrap().is_none()));

    let proof_inspections = Arc::new(AtomicUsize::new(0));
    let summary_inspections = Arc::new(AtomicUsize::new(0));
    let inspector = typed_inspector(
        &stores,
        Arc::clone(&proof_inspections),
        Arc::clone(&summary_inspections),
    );
    let inspected = inspector
        .inspect_certified_decision_at(1, LogHash::ZERO)
        .unwrap();

    let CertifiedDecisionInspection::Committed(certified) = inspected else {
        panic!("expected committed phase-2 decision, got {inspected:?}");
    };
    assert_eq!(certified.entry, committed);
    assert!(matches!(certified.proof, DecisionProof::Phase2 { .. }));
    assert_eq!(proof_inspections.load(Ordering::SeqCst), 0);
    assert!(summary_inspections.load(Ordering::SeqCst) >= 2);
}

#[derive(Clone)]
struct ScriptedTypedInspectionRecorder {
    id: String,
    summary: Option<RecordSummary>,
    proof_inspections: Arc<AtomicUsize>,
    summary_inspections: Arc<AtomicUsize>,
}

impl RecorderRpc for ScriptedTypedInspectionRecorder {
    fn recorder_id(&self) -> Result<String, Error> {
        Ok(self.id.clone())
    }

    fn inspect_decision_proof(&self, _slot: u64) -> Result<Option<DecisionProof>, Error> {
        self.proof_inspections.fetch_add(1, Ordering::SeqCst);
        Err(Error::Io(
            "typed inspection must not issue a separate proof RPC".into(),
        ))
    }

    fn inspect_record_summary(&self, _slot: u64) -> Result<Option<RecordSummary>, Error> {
        self.summary_inspections.fetch_add(1, Ordering::SeqCst);
        Ok(self.summary.clone())
    }
}

fn scripted_typed_inspector(
    summaries: Vec<Option<RecordSummary>>,
    proof_inspections: Arc<AtomicUsize>,
    summary_inspections: Arc<AtomicUsize>,
) -> ThreeNodeConsensus {
    let recorders = ["n1", "n2", "n3"]
        .into_iter()
        .zip(summaries)
        .map(|(id, summary)| {
            (
                id.to_string(),
                Box::new(ScriptedTypedInspectionRecorder {
                    id: id.to_string(),
                    summary,
                    proof_inspections: Arc::clone(&proof_inspections),
                    summary_inspections: Arc::clone(&summary_inspections),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap()
}

#[derive(Clone)]
struct ScriptedReadFenceRecorder {
    id: String,
    observation: Result<ReadFenceObservation, Error>,
    delay: Duration,
    summary_inspections: Arc<AtomicUsize>,
}

impl RecorderRpc for ScriptedReadFenceRecorder {
    fn recorder_id(&self) -> Result<String, Error> {
        Ok(self.id.clone())
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        _request: ReadFenceRequest,
    ) -> Result<ReadFenceObservation, Error> {
        thread::sleep(self.delay);
        self.observation.clone()
    }

    fn inspect_record_summary(&self, _slot: u64) -> Result<Option<RecordSummary>, Error> {
        self.summary_inspections.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}

fn read_fence_observation(id: &str, slot_state: ReadFenceSlotState) -> ReadFenceObservation {
    ReadFenceObservation {
        recorder_id: id.into(),
        cluster_id: "cluster".into(),
        epoch: 1,
        config_id: 1,
        config_digest: Membership::new(["n1", "n2", "n3"]).unwrap().digest(),
        slot: 1,
        max_head: match &slot_state {
            ReadFenceSlotState::Empty => None,
            ReadFenceSlotState::Occupied { .. } => Some(1),
        },
        slot_state,
    }
}

fn scripted_read_fence_consensus(
    observations: Vec<ReadFenceObservation>,
    summary_inspections: Arc<AtomicUsize>,
) -> ThreeNodeConsensus {
    scripted_read_fence_consensus_with_results(
        observations.into_iter().map(Ok).collect(),
        summary_inspections,
    )
}

fn scripted_read_fence_consensus_with_results(
    observations: Vec<Result<ReadFenceObservation, Error>>,
    summary_inspections: Arc<AtomicUsize>,
) -> ThreeNodeConsensus {
    let recorders = ["n1", "n2", "n3"]
        .into_iter()
        .zip(observations)
        .map(|(id, observation)| {
            (
                id.to_string(),
                Box::new(ScriptedReadFenceRecorder {
                    id: id.to_string(),
                    observation,
                    delay: Duration::ZERO,
                    summary_inspections: Arc::clone(&summary_inspections),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap()
}

#[test]
fn context_read_fence_returns_before_a_slow_voter_when_quorum_is_impossible() {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let recorders = [
        ("n1", Duration::ZERO),
        ("n2", Duration::ZERO),
        ("n3", Duration::from_secs(2)),
    ]
    .into_iter()
    .map(|(id, delay)| {
        (
            id.to_string(),
            Box::new(ScriptedReadFenceRecorder {
                id: id.to_string(),
                observation: Err(Error::Io(format!("{id} unavailable"))),
                delay,
                summary_inspections: Arc::new(AtomicUsize::new(0)),
            }) as Box<dyn RecorderRpc>,
        )
    })
    .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    let started = std::time::Instant::now();
    assert_eq!(
        consensus
            .inspect_context_read_fence_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Unavailable
    );
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "an impossible quorum waited for the slow voter"
    );
    assert_eq!(membership.quorum_size(), 2);
}

#[test]
fn context_read_fence_succeeds_with_one_unavailable_voter() {
    let consensus = scripted_read_fence_consensus_with_results(
        vec![
            Ok(read_fence_observation("n1", ReadFenceSlotState::Empty)),
            Err(Error::Io("n2 unavailable".into())),
            Ok(read_fence_observation("n3", ReadFenceSlotState::Empty)),
        ],
        Arc::new(AtomicUsize::new(0)),
    );

    assert_eq!(
        consensus
            .inspect_context_read_fence_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Empty
    );
}

#[test]
fn context_read_fence_is_unavailable_with_two_unavailable_voters() {
    let consensus = scripted_read_fence_consensus_with_results(
        vec![
            Ok(read_fence_observation("n1", ReadFenceSlotState::Empty)),
            Err(Error::Io("n2 unavailable".into())),
            Err(Error::Io("n3 unavailable".into())),
        ],
        Arc::new(AtomicUsize::new(0)),
    );

    assert_eq!(
        consensus
            .inspect_context_read_fence_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Unavailable
    );
}

#[test]
fn context_read_fence_returns_empty_from_an_exact_empty_quorum_without_summary_rpc() {
    let summary_inspections = Arc::new(AtomicUsize::new(0));
    let consensus = scripted_read_fence_consensus(
        vec![
            read_fence_observation("n1", ReadFenceSlotState::Empty),
            read_fence_observation("n2", ReadFenceSlotState::Empty),
            read_fence_observation("n3", ReadFenceSlotState::Occupied { summary: None }),
        ],
        Arc::clone(&summary_inspections),
    );

    assert_eq!(
        consensus
            .inspect_context_read_fence_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Empty
    );
    assert_eq!(summary_inspections.load(Ordering::SeqCst), 0);
}

#[test]
fn context_read_fence_maps_a_crossed_slot_to_pending() {
    let summary_inspections = Arc::new(AtomicUsize::new(0));
    let consensus = scripted_read_fence_consensus(
        vec![
            read_fence_observation("n1", ReadFenceSlotState::Occupied { summary: None }),
            read_fence_observation("n2", ReadFenceSlotState::Occupied { summary: None }),
            read_fence_observation("n3", ReadFenceSlotState::Empty),
        ],
        Arc::clone(&summary_inspections),
    );

    assert_eq!(
        consensus
            .inspect_context_read_fence_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Pending
    );
    assert!(summary_inspections.load(Ordering::SeqCst) >= 2);
}

#[test]
fn context_read_fence_rejects_wrong_identity_or_context_from_the_empty_quorum() {
    let summary_inspections = Arc::new(AtomicUsize::new(0));
    let mut wrong_identity = read_fence_observation("n2", ReadFenceSlotState::Empty);
    wrong_identity.recorder_id = "n1".into();
    let mut wrong_context = read_fence_observation("n3", ReadFenceSlotState::Empty);
    wrong_context.config_id = 9;
    let consensus = scripted_read_fence_consensus(
        vec![
            read_fence_observation("n1", ReadFenceSlotState::Empty),
            wrong_identity,
            wrong_context,
        ],
        summary_inspections,
    );

    assert_eq!(
        consensus
            .inspect_context_read_fence_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Unavailable
    );
}

fn certified_fast_proof(byte: u8) -> DecisionProof {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let command = StoredCommand::new(EntryType::Command, vec![byte]);
    let proposal = Proposal::new(
        ProposalPriority::MAX,
        "n1",
        u64::from(byte),
        AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
    );
    DecisionProof::FastPath {
        cluster_id: "cluster".into(),
        slot: 1,
        epoch: 1,
        config_id: 1,
        config_digest: membership.digest(),
        proposal: proposal.clone(),
        summaries: ["n1", "n2"]
            .into_iter()
            .map(|id| RecorderSummary {
                recorder_id: id.into(),
                slot: 1,
                step: 4,
                first_current: Some(proposal.clone()),
                aggregate_prior: None,
            })
            .collect(),
    }
}

fn inspection_summary(id: &str, decided: Option<DecisionProof>) -> RecordSummary {
    RecordSummary {
        recorder_id: id.into(),
        slot: 1,
        config_id: 1,
        config_digest: Membership::new(["n1", "n2", "n3"]).unwrap().digest(),
        step: 5,
        first_current: None,
        aggregate_prior: None,
        decided,
    }
}

#[test]
fn typed_inspection_classifies_quorum_none_as_empty_without_proof_rpc() {
    let proof_inspections = Arc::new(AtomicUsize::new(0));
    let summary_inspections = Arc::new(AtomicUsize::new(0));
    let inspector = scripted_typed_inspector(
        vec![None, None, None],
        Arc::clone(&proof_inspections),
        Arc::clone(&summary_inspections),
    );

    assert_eq!(
        inspector
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Empty
    );
    assert_eq!(proof_inspections.load(Ordering::SeqCst), 0);
    assert!(summary_inspections.load(Ordering::SeqCst) >= 2);
}

#[test]
fn typed_inspection_classifies_uncertified_accepted_state_as_pending() {
    let proof_inspections = Arc::new(AtomicUsize::new(0));
    let summary_inspections = Arc::new(AtomicUsize::new(0));
    let summaries = ["n1", "n2", "n3"]
        .into_iter()
        .map(|id| Some(inspection_summary(id, None)))
        .collect();
    let inspector = scripted_typed_inspector(
        summaries,
        Arc::clone(&proof_inspections),
        summary_inspections,
    );

    assert_eq!(
        inspector
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Pending
    );
    assert_eq!(proof_inspections.load(Ordering::SeqCst), 0);
}

#[test]
fn typed_inspection_rejects_conflicting_embedded_certificates() {
    let proof_inspections = Arc::new(AtomicUsize::new(0));
    let summary_inspections = Arc::new(AtomicUsize::new(0));
    let summaries = ["n1", "n2", "n3"]
        .into_iter()
        .zip(1_u8..=3)
        .map(|(id, byte)| Some(inspection_summary(id, Some(certified_fast_proof(byte)))))
        .collect();
    let inspector = scripted_typed_inspector(
        summaries,
        Arc::clone(&proof_inspections),
        summary_inspections,
    );

    assert_eq!(
        inspector.inspect_certified_decision_at(1, LogHash::ZERO),
        Err(Error::ConflictingCertificates)
    );
    assert_eq!(proof_inspections.load(Ordering::SeqCst), 0);
}

#[test]
fn typed_inspection_rejects_embedded_proof_for_another_configuration() {
    let proof_inspections = Arc::new(AtomicUsize::new(0));
    let summary_inspections = Arc::new(AtomicUsize::new(0));
    let mut proof = certified_fast_proof(1);
    let DecisionProof::FastPath { config_id, .. } = &mut proof else {
        unreachable!()
    };
    *config_id = 2;
    let summaries = ["n1", "n2", "n3"]
        .into_iter()
        .map(|id| Some(inspection_summary(id, Some(proof.clone()))))
        .collect();
    let inspector = scripted_typed_inspector(
        summaries,
        Arc::clone(&proof_inspections),
        summary_inspections,
    );

    assert_eq!(
        inspector.inspect_certified_decision_at(1, LogHash::ZERO),
        Err(Error::Rejected(RejectReason::WrongConfig))
    );
    assert_eq!(proof_inspections.load(Ordering::SeqCst), 0);
}

#[test]
fn typed_inspection_rejects_embedded_proof_for_another_cluster() {
    let proof_inspections = Arc::new(AtomicUsize::new(0));
    let summary_inspections = Arc::new(AtomicUsize::new(0));
    let mut proof = certified_fast_proof(1);
    let DecisionProof::FastPath { cluster_id, .. } = &mut proof else {
        unreachable!()
    };
    *cluster_id = "other-cluster".into();
    let summaries = ["n1", "n2", "n3"]
        .into_iter()
        .map(|id| Some(inspection_summary(id, Some(proof.clone()))))
        .collect();
    let inspector = scripted_typed_inspector(
        summaries,
        Arc::clone(&proof_inspections),
        summary_inspections,
    );

    assert_eq!(
        inspector.inspect_certified_decision_at(1, LogHash::ZERO),
        Err(Error::Rejected(RejectReason::WrongCluster))
    );
    assert_eq!(proof_inspections.load(Ordering::SeqCst), 0);
}

impl RecorderRpc for StaleSummaryRecorder {
    fn recorder_id(&self) -> Result<String, Error> {
        Ok(self.id.clone())
    }

    fn inspect_decision_proof(&self, _slot: u64) -> Result<Option<DecisionProof>, Error> {
        Ok(None)
    }

    fn inspect_record_summary(&self, _slot: u64) -> Result<Option<RecordSummary>, Error> {
        Ok(Some(self.summary.clone()))
    }
}

struct MissingTypedRecord;

impl RecorderRpc for MissingTypedRecord {}

#[test]
fn recorder_without_typed_record_fails_closed() {
    let request = RecordRequest {
        cluster_id: "cluster".into(),
        epoch: 1,
        config_id: 1,
        config_digest: LogHash::ZERO,
        slot: 1,
        step: 4,
        proposal: proposal(1, "n1", 1, 1),
        command: None,
    };
    assert_eq!(
        MissingTypedRecord.record(request),
        Err(Error::TypedRecordRequired)
    );
}

#[test]
fn typed_summary_inspection_rejects_stale_configuration_evidence() {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"stale".to_vec());
    let proposal = Proposal::new(
        ProposalPriority::MAX,
        "n1",
        1,
        AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
    );
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            (
                id.clone(),
                Box::new(StaleSummaryRecorder {
                    id: id.clone(),
                    summary: RecordSummary {
                        recorder_id: id.clone(),
                        slot: 1,
                        config_id: 2,
                        config_digest: LogHash::ZERO,
                        step: 4,
                        first_current: Some(proposal.clone()),
                        aggregate_prior: None,
                        decided: None,
                    },
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    assert!(matches!(
        consensus
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Unavailable
    ));
}

#[test]
fn hedged_proposer_finishes_another_proposers_exact_h_quorum() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let stores: Vec<_> = membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    let n1_command = StoredCommand::new(EntryType::Command, b"n1".to_vec());
    let n2_command = StoredCommand::new(EntryType::Command, b"n2".to_vec());
    let n1_value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &n1_command);
    let n2_value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &n2_command);
    for store in &stores {
        store
            .store_command(n1_command.hash(), n1_command.clone())
            .unwrap();
        store
            .store_command(n2_command.hash(), n2_command.clone())
            .unwrap();
        store
            .record(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 1,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "n1", 1, n1_value.clone()),
                command: None,
            })
            .unwrap();
    }
    let recorders = membership
        .members()
        .iter()
        .zip(&stores)
        .map(|(id, store)| (id.clone(), Box::new(store.clone()) as Box<dyn RecorderRpc>))
        .collect();
    let n2 = ThreeNodeConsensus::from_recorders_with_ids("cluster", "n2", 1, 1, recorders).unwrap();

    let outcome = n2
        .drive(ProposerProgress::new(
            1,
            Proposal::new(ProposalPriority::MAX, "n2", 1, n2_value),
        ))
        .unwrap();
    let DriveOutcome::Decision(proof) = outcome else {
        panic!("hedged proposer did not finish the observed H proof");
    };
    assert_eq!(proof.proposal().proposer_id, "n1");
    assert_eq!(proof.proposal().value.as_ref(), Some(&n1_value));
}

#[test]
fn hedged_proposer_installs_an_adopted_config_change_on_a_quorum() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let config_command = ConfigChange::stop(
        "cluster",
        1,
        membership.digest(),
        2,
        membership.members().to_vec(),
    )
    .unwrap()
    .to_stored_command();
    let config_value =
        AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &config_command);
    let stores: Vec<_> = membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    for store in &stores {
        store
            .record(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 1,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "n1", 1, config_value.clone()),
                command: Some(config_command.clone()),
            })
            .unwrap();
    }
    let recorders = membership
        .members()
        .iter()
        .zip(&stores)
        .map(|(id, store)| (id.clone(), Box::new(store.clone()) as Box<dyn RecorderRpc>))
        .collect();
    let n2 = ThreeNodeConsensus::from_recorders_with_ids("cluster", "n2", 1, 1, recorders).unwrap();
    let local = StoredCommand::new(EntryType::Command, b"ordinary".to_vec());
    n2.register_command(local.hash(), local.payload.clone())
        .unwrap();

    let outcome = n2
        .drive(ProposerProgress::new(
            1,
            Proposal::new(
                ProposalPriority::MAX,
                "n2",
                1,
                AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &local),
            ),
        ))
        .unwrap();

    assert!(matches!(outcome, DriveOutcome::Decision(_)));
    assert!(
        stores
            .iter()
            .filter(|store| store.configuration_state().unwrap().seal().is_some())
            .count()
            >= membership.quorum_size()
    );
}

#[test]
fn noncurrent_recorder_state_fails_closed_without_rewrite() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("slot-00000000000000000001.rec");
    let mut bytes = b"QREC".to_vec();
    bytes.extend_from_slice(&3_u16.to_be_bytes());
    let digest = LogHash::digest(&[&bytes]);
    bytes.extend_from_slice(digest.as_bytes());
    std::fs::write(&path, &bytes).unwrap();
    assert!(matches!(
        RecorderFileStore::new_with_id(root.path(), "n1", "cluster", 1, 1),
        Err(Error::Decode(message))
            if message == "recorder directory is not a current durable layout"
    ));
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[derive(Default)]
struct ProtocolCounts {
    fetches: AtomicUsize,
    piggybacks: AtomicUsize,
    proof_installs: AtomicUsize,
}

#[derive(Clone)]
struct ObservedRecorder {
    store: RecorderFileStore,
    counts: Arc<ProtocolCounts>,
}

impl RecorderRpc for ObservedRecorder {
    fn record(&self, request: RecordRequest) -> Result<RecordSummary, Error> {
        if request.command.is_some() {
            self.counts.piggybacks.fetch_add(1, Ordering::SeqCst);
        }
        self.store.record(request)
    }

    fn fetch_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> Result<Option<StoredCommand>, Error> {
        self.counts.fetches.fetch_add(1, Ordering::SeqCst);
        self.store
            .fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<(), Error> {
        self.store.install_decision_proof(proof, membership)?;
        self.counts.proof_installs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn inspect_decision_proof(&self, slot: u64) -> Result<Option<DecisionProof>, Error> {
        self.store.inspect_decision_proof(slot)
    }

    fn inspect_record_summary(&self, slot: u64) -> Result<Option<RecordSummary>, Error> {
        self.store.inspect_record_summary(slot)
    }
}

struct DeadRecorder;

impl RecorderRpc for DeadRecorder {
    fn record(&self, _request: RecordRequest) -> Result<RecordSummary, Error> {
        Err(Error::Io("recorder unavailable".into()))
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> Result<(), Error> {
        Err(Error::Io("recorder unavailable".into()))
    }
}

#[derive(Clone)]
struct RejectingRecordRecorder {
    rejected: Option<Arc<(Mutex<bool>, Condvar)>>,
    reason: RejectReason,
}

impl RecorderRpc for RejectingRecordRecorder {
    fn record(&self, _request: RecordRequest) -> Result<RecordSummary, Error> {
        if let Some(rejected) = &self.rejected {
            let (state, condition) = &**rejected;
            *state.lock().unwrap() = true;
            condition.notify_all();
        }
        Err(Error::Rejected(self.reason.clone()))
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> Result<(), Error> {
        Err(Error::Io("recorder unavailable".into()))
    }
}

#[derive(Clone)]
struct WaitForRejectionRecorder {
    store: RecorderFileStore,
    rejected: Arc<(Mutex<bool>, Condvar)>,
}

impl RecorderRpc for WaitForRejectionRecorder {
    fn record(&self, request: RecordRequest) -> Result<RecordSummary, Error> {
        let (state, condition) = &*self.rejected;
        let rejected = condition
            .wait_while(state.lock().unwrap(), |rejected| !*rejected)
            .unwrap();
        drop(rejected);
        self.store.record(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<(), Error> {
        self.store.install_decision_proof(proof, membership)
    }
}

#[derive(Clone)]
struct RecordThroughStepThenDead {
    store: RecorderFileStore,
    last_live_step: u64,
    crashed: Arc<AtomicBool>,
}

impl RecorderRpc for RecordThroughStepThenDead {
    fn record(&self, request: RecordRequest) -> Result<RecordSummary, Error> {
        if request.step > self.last_live_step {
            self.crashed.store(true, Ordering::Release);
            Err(Error::Io("recorder crashed".into()))
        } else {
            self.store.record(request)
        }
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<(), Error> {
        if self.crashed.load(Ordering::Acquire) {
            Err(Error::Io("recorder crashed".into()))
        } else {
            self.store.install_decision_proof(proof, membership)
        }
    }
}

#[derive(Clone)]
struct UnavailableBeforeStep {
    store: RecorderFileStore,
    first_live_step: u64,
}

impl RecorderRpc for UnavailableBeforeStep {
    fn record(&self, request: RecordRequest) -> Result<RecordSummary, Error> {
        if request.step < self.first_live_step {
            return Err(Error::Io("recorder unavailable".into()));
        }
        if request.command.is_none() {
            return Err(Error::Rejected(RejectReason::InvalidRequest));
        }
        self.store.record(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<(), Error> {
        self.store.install_decision_proof(proof, membership)
    }
}

struct BlockingRecordRecorder {
    started: Arc<(Mutex<bool>, Condvar)>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl RecorderRpc for BlockingRecordRecorder {
    fn record(&self, _request: RecordRequest) -> Result<RecordSummary, Error> {
        let (started, started_condition) = &*self.started;
        *started.lock().unwrap() = true;
        started_condition.notify_all();

        let (release, release_condition) = &*self.release;
        let mut released = release.lock().unwrap();
        while !*released {
            released = release_condition.wait(released).unwrap();
        }
        Err(Error::Io("recorder unavailable".into()))
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> Result<(), Error> {
        Err(Error::Io("recorder unavailable".into()))
    }
}

struct CountingProofRecorder {
    proof_installs: Arc<AtomicUsize>,
}

impl RecorderRpc for CountingProofRecorder {
    fn record(&self, _request: RecordRequest) -> Result<RecordSummary, Error> {
        Err(Error::Io("recorder unavailable".into()))
    }

    fn store_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        _command_hash: LogHash,
        _command: StoredCommand,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> Result<(), Error> {
        self.proof_installs.fetch_add(1, Ordering::SeqCst);
        Err(Error::Io("recorder unavailable".into()))
    }
}

#[test]
fn preferred_fast_path_piggybacks_command_without_post_ack_proof_writes() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let counts = Arc::new(ProtocolCounts::default());
    let stores: Vec<_> = ["n1", "n2"]
        .into_iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    let recorders = vec![
        (
            "n1".into(),
            Box::new(ObservedRecorder {
                store: stores[0].clone(),
                counts: Arc::clone(&counts),
            }) as Box<dyn RecorderRpc>,
        ),
        (
            "n2".into(),
            Box::new(ObservedRecorder {
                store: stores[1].clone(),
                counts: Arc::clone(&counts),
            }) as Box<dyn RecorderRpc>,
        ),
        ("n3".into(), Box::new(DeadRecorder) as Box<dyn RecorderRpc>),
    ];
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"one-round-trip".to_vec());

    let entry = consensus
        .propose_stored_at(1, LogHash::ZERO, command.clone())
        .unwrap();
    assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));

    assert_eq!(entry.payload, command.payload);
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 0);
    assert!(counts.piggybacks.load(Ordering::SeqCst) >= membership.quorum_size());
    assert_eq!(counts.proof_installs.load(Ordering::SeqCst), 0);
    for store in stores {
        assert_eq!(
            store.fetch_command(command.hash()).unwrap(),
            Some(command.clone())
        );
    }
}

#[test]
fn non_preferred_path_piggybacks_command_without_post_ack_proof_writes() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let counts = Arc::new(ProtocolCounts::default());
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (
                id.clone(),
                Box::new(ObservedRecorder {
                    store,
                    counts: Arc::clone(&counts),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n2", 1, 1, recorders).unwrap();

    consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"slow-path".to_vec()),
        )
        .unwrap();
    assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));

    assert_eq!(counts.fetches.load(Ordering::SeqCst), 0);
    assert!(counts.piggybacks.load(Ordering::SeqCst) >= membership.quorum_size());
    assert!(counts.piggybacks.load(Ordering::SeqCst) <= 6);
    assert_eq!(counts.proof_installs.load(Ordering::SeqCst), 0);
}

#[test]
fn stop_and_activation_transitions_still_install_proofs_on_a_quorum() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let counts = Arc::new(ProtocolCounts::default());
    let stores: Vec<_> = membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    let recorders = membership
        .members()
        .iter()
        .zip(&stores)
        .map(|(id, store)| {
            (
                id.clone(),
                Box::new(ObservedRecorder {
                    store: store.clone(),
                    counts: Arc::clone(&counts),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let predecessor =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
    let stop = predecessor
        .propose_stop_for_successor_at(1, LogHash::ZERO, &membership)
        .unwrap();
    assert!(predecessor.finish_pending_rpcs(Duration::from_secs(1)));
    let stop_proof = predecessor.inspect_decision_proof_at(1).unwrap().unwrap();
    assert!(counts.proof_installs.load(Ordering::SeqCst) >= membership.quorum_size());
    drop(predecessor);

    for store in &stores {
        let installed = store
            .install_successor_from_proof(membership.clone(), &stop_proof)
            .unwrap();
        assert!(!installed.is_activated());
    }
    let proof_installs_after_stop = counts.proof_installs.load(Ordering::SeqCst);
    let recorders = membership
        .members()
        .iter()
        .zip(&stores)
        .map(|(id, store)| {
            (
                id.clone(),
                Box::new(ObservedRecorder {
                    store: store.clone(),
                    counts: Arc::clone(&counts),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let successor =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 2, recorders).unwrap();
    let activation = successor
        .propose_activation_for_stop_at(&stop_proof)
        .unwrap();
    assert_eq!(activation.index, 2);
    assert!(successor.finish_pending_rpcs(Duration::from_secs(1)));
    assert!(
        counts.proof_installs.load(Ordering::SeqCst)
            >= proof_installs_after_stop + membership.quorum_size()
    );
    assert_eq!(activation.prev_hash, stop.hash);
}

#[test]
fn adopted_command_is_redistributed_after_a_holder_crashes_in_a_later_phase() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let adopted = StoredCommand::new(EntryType::Command, b"adopted".to_vec());
    let adopted_proposal = Proposal::new(
        ProposalPriority::from_u64(10_000),
        "other",
        1,
        AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &adopted),
    );
    let stores: Vec<_> = membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    for store in &stores[..2] {
        store
            .record(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 1,
                step: 4,
                proposal: adopted_proposal.clone(),
                command: Some(adopted.clone()),
            })
            .unwrap();
    }
    let crashed = Arc::new(AtomicBool::new(false));
    let recorders = vec![
        (
            "n1".into(),
            Box::new(RecordThroughStepThenDead {
                store: stores[0].clone(),
                last_live_step: 5,
                crashed: Arc::clone(&crashed),
            }) as Box<dyn RecorderRpc>,
        ),
        (
            "n2".into(),
            Box::new(stores[1].clone()) as Box<dyn RecorderRpc>,
        ),
        (
            "n3".into(),
            Box::new(UnavailableBeforeStep {
                store: stores[2].clone(),
                first_live_step: 6,
            }) as Box<dyn RecorderRpc>,
        ),
    ];
    let consensus = ThreeNodeConsensus::from_recorders_with_ids("cluster", "n2", 1, 1, recorders)
        .unwrap()
        .with_priority_source(Arc::new(FixedPriority));

    let entry = consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"offered".to_vec()),
        )
        .unwrap();

    assert_eq!(entry.payload, adopted.payload);
    assert!(crashed.load(Ordering::Acquire));
    assert_eq!(
        stores[2].fetch_command(adopted.hash()).unwrap(),
        Some(adopted)
    );
}

#[test]
fn record_broadcast_ignores_a_minority_typed_rejection() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let rejected = Arc::new((Mutex::new(false), Condvar::new()));
    let mut recorders = Vec::new();
    for id in ["n1", "n2"] {
        let store = RecorderFileStore::new_with_membership(
            root.path().join(id),
            id,
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        recorders.push((
            id.to_string(),
            Box::new(WaitForRejectionRecorder {
                store,
                rejected: Arc::clone(&rejected),
            }) as Box<dyn RecorderRpc>,
        ));
    }
    recorders.push((
        "n3".into(),
        Box::new(RejectingRecordRecorder {
            rejected: Some(rejected),
            reason: RejectReason::WrongSlot,
        }) as Box<dyn RecorderRpc>,
    ));
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    let entry = consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"minority-rejection".to_vec()),
        )
        .unwrap();

    assert_eq!(entry.payload, b"minority-rejection");
}

#[test]
fn record_broadcast_returns_typed_rejection_when_quorum_is_impossible() {
    let recorders = vec![
        (
            "n1".into(),
            Box::new(RejectingRecordRecorder {
                rejected: None,
                reason: RejectReason::WrongSlot,
            }) as Box<dyn RecorderRpc>,
        ),
        ("n2".into(), Box::new(DeadRecorder) as Box<dyn RecorderRpc>),
        ("n3".into(), Box::new(DeadRecorder) as Box<dyn RecorderRpc>),
    ];
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    assert_eq!(
        consensus.propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"rejected".to_vec()),
        ),
        Err(Error::Rejected(RejectReason::WrongSlot))
    );
}

#[test]
fn consensus_drop_does_not_wait_for_a_blocked_minority_rpc() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let started = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let mut recorders = Vec::new();
    for id in ["n1", "n2"] {
        let store = RecorderFileStore::new_with_membership(
            root.path().join(id),
            id,
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        recorders.push((id.to_string(), Box::new(store) as Box<dyn RecorderRpc>));
    }
    recorders.push((
        "n3".into(),
        Box::new(BlockingRecordRecorder {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        }) as Box<dyn RecorderRpc>,
    ));
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
    consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"background-record".to_vec()),
        )
        .unwrap();

    let (started_lock, started_condition) = &*started;
    let (record_started, _) = started_condition
        .wait_timeout_while(
            started_lock.lock().unwrap(),
            Duration::from_secs(1),
            |started| !*started,
        )
        .unwrap();
    assert!(*record_started);

    let (drop_started_sender, drop_started_receiver) = std::sync::mpsc::channel();
    let (dropped_sender, dropped_receiver) = std::sync::mpsc::channel();
    let drop_thread = thread::spawn(move || {
        drop_started_sender.send(()).unwrap();
        drop(consensus);
        dropped_sender.send(()).unwrap();
    });
    drop_started_receiver.recv().unwrap();
    dropped_receiver
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    let (release_lock, release_condition) = &*release;
    *release_lock.lock().unwrap() = true;
    release_condition.notify_all();

    drop_thread.join().unwrap();
}

#[test]
fn proof_install_rejects_membership_other_than_the_recorder_configuration() {
    let root = tempfile::tempdir().unwrap();
    let current = Membership::new(["n1", "n2", "n3"]).unwrap();
    let supplied = Membership::new(["n1", "n2", "n4"]).unwrap();
    let store = RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, current)
        .unwrap();
    let command = StoredCommand::new(EntryType::Command, b"membership-proof".to_vec());
    store
        .store_command(command.hash(), command.clone())
        .unwrap();
    let proposal = Proposal::new(
        ProposalPriority::MAX,
        "n1",
        1,
        AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
    );
    let proof = DecisionProof::FastPath {
        cluster_id: "cluster".into(),
        slot: 1,
        epoch: 1,
        config_id: 1,
        config_digest: supplied.digest(),
        proposal: proposal.clone(),
        summaries: supplied.members()[..supplied.quorum_size()]
            .iter()
            .map(|id| RecorderSummary {
                recorder_id: id.clone(),
                slot: 1,
                step: 4,
                first_current: Some(proposal.clone()),
                aggregate_prior: None,
            })
            .collect(),
    };

    assert_eq!(
        store.install_decision_proof_record(proof, &supplied),
        Err(Error::Rejected(RejectReason::WrongConfig))
    );
    assert!(store.load(1).unwrap().decision_proof().is_none());
}

#[test]
fn proof_cache_accepts_different_metadata_for_the_same_decided_value() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let store = RecorderFileStore::new_with_membership(
        root.path(),
        "n1",
        "cluster",
        1,
        1,
        membership.clone(),
    )
    .unwrap();
    let command = StoredCommand::new(EntryType::Command, b"same-value".to_vec());
    store
        .store_command(command.hash(), command.clone())
        .unwrap();
    let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
    let proof = |proposer: &str, proposal_id: u64| {
        let proposal = Proposal::new(ProposalPriority::MAX, proposer, proposal_id, value.clone());
        DecisionProof::FastPath {
            cluster_id: "cluster".into(),
            slot: 1,
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            proposal: proposal.clone(),
            summaries: ["n1", "n2"]
                .into_iter()
                .map(|id| RecorderSummary {
                    recorder_id: id.into(),
                    slot: 1,
                    step: 4,
                    first_current: Some(proposal.clone()),
                    aggregate_prior: None,
                })
                .collect(),
        }
    };

    store
        .install_decision_proof_record(proof("n1", 1), &membership)
        .unwrap();
    store
        .install_decision_proof_record(proof("n2", 2), &membership)
        .unwrap();
}

#[test]
fn ordinary_fast_path_never_installs_a_proof_cache() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let counts = Arc::new(ProtocolCounts::default());
    let minority_proof_installs = Arc::new(AtomicUsize::new(0));
    let recorders = ["n1", "n2"]
        .into_iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.path().join(id),
                id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (
                id.to_string(),
                Box::new(ObservedRecorder {
                    store,
                    counts: Arc::clone(&counts),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .chain(std::iter::once((
            "n3".into(),
            Box::new(CountingProofRecorder {
                proof_installs: Arc::clone(&minority_proof_installs),
            }) as Box<dyn RecorderRpc>,
        )))
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"fast".to_vec()),
        )
        .unwrap();
    assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));

    assert_eq!(counts.proof_installs.load(Ordering::SeqCst), 0);
    assert_eq!(minority_proof_installs.load(Ordering::SeqCst), 0);
}

#[test]
fn consecutive_ordinary_decisions_remain_reconstructable_without_proof_caches() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let counts = Arc::new(ProtocolCounts::default());
    let stores: Vec<_> = membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    let recorders = membership
        .members()
        .iter()
        .zip(&stores)
        .map(|(id, store)| {
            (
                id.clone(),
                Box::new(ObservedRecorder {
                    store: store.clone(),
                    counts: Arc::clone(&counts),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    let first = consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"first".to_vec()),
        )
        .unwrap();
    assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    let second = consensus
        .propose_at(
            2,
            first.hash,
            Command::new(CommandKind::Deterministic, b"second".to_vec()),
        )
        .unwrap();
    assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    let third = consensus
        .propose_at(
            3,
            second.hash,
            Command::new(CommandKind::Deterministic, b"third".to_vec()),
        )
        .unwrap();
    assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    assert_eq!(counts.proof_installs.load(Ordering::SeqCst), 0);
    for slot in 1..=3 {
        assert!(stores
            .iter()
            .all(|store| store.inspect_decision_proof(slot).unwrap().is_none()));
    }
    for payload in [
        b"first".as_slice(),
        b"second".as_slice(),
        b"third".as_slice(),
    ] {
        let command = StoredCommand::new(EntryType::Command, payload.to_vec());
        assert!(stores
            .iter()
            .all(|store| store.fetch_command(command.hash()).unwrap() == Some(command.clone())));
    }
    let first_hash = first.hash;
    let second_hash = second.hash;
    for (entry, prev_hash) in [
        (first, LogHash::ZERO),
        (second, first_hash),
        (third, second_hash),
    ] {
        let inspection = consensus
            .inspect_certified_decision_at(entry.index, prev_hash)
            .unwrap();
        assert!(
            matches!(
                inspection,
            CertifiedDecisionInspection::Committed(ref certified) if certified.entry == entry
            ),
            "slot {} was not reconstructable: {inspection:?}",
            entry.index
        );
    }
}

thread_local! {
    static PIGGYBACK_PROPERTY_ROOT: tempfile::TempDir = tempfile::tempdir().unwrap();
}

static PIGGYBACK_PROPERTY_CASE: AtomicUsize = AtomicUsize::new(0);

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn mismatched_piggyback_never_persists_or_advances(
        offered in prop::collection::vec(any::<u8>(), 1..64),
        other in prop::collection::vec(any::<u8>(), 1..64),
    ) {
        prop_assume!(offered != other);
        let case = PIGGYBACK_PROPERTY_CASE.fetch_add(1, Ordering::Relaxed);
        let root = PIGGYBACK_PROPERTY_ROOT.with(|root| root.path().join(case.to_string()));
        std::fs::create_dir(&root).unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root, "n1", "cluster", 1, 1, membership.clone(),
        ).unwrap();
        let expected = StoredCommand::new(EntryType::Command, offered);
        let mismatched = StoredCommand::new(EntryType::Command, other);
        let request = RecordRequest {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            slot: 1,
            step: 4,
            proposal: Proposal::new(
                ProposalPriority::MAX,
                "n1",
                1,
                AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &expected),
            ),
            command: Some(mismatched.clone()),
        };

        prop_assert_eq!(
            store.record(request),
            Err(Error::Rejected(RejectReason::InvalidValue)),
        );
        prop_assert_eq!(store.fetch_command(mismatched.hash()).unwrap(), None);
        let state = store.load(1).unwrap();
        prop_assert_eq!(state.isr(), &IsrState::default());
    }
}

#[test]
fn proposer_failure_after_piggyback_recovers_with_restarted_quorum_and_one_dead_recorder() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"recover-after-proposer-crash".to_vec());
    let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
    let proposal = Proposal::new(ProposalPriority::MAX, "n1", 1, value.clone());

    for id in ["n1", "n2"] {
        let store = RecorderFileStore::new_with_membership(
            root.path().join(id),
            id,
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        store
            .record(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 1,
                step: 4,
                proposal: proposal.clone(),
                command: Some(command.clone()),
            })
            .unwrap();
    }

    let recorders = ["n1", "n2"]
        .into_iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.path().join(id),
                id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (id.to_string(), Box::new(store) as Box<dyn RecorderRpc>)
        })
        .chain(std::iter::once((
            "n3".into(),
            Box::new(DeadRecorder) as Box<dyn RecorderRpc>,
        )))
        .collect();
    let replacement =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    let outcome = replacement
        .drive(ProposerProgress::new(1, proposal))
        .unwrap();
    assert!(matches!(
        outcome,
        DriveOutcome::Decision(DecisionProof::FastPath { .. })
    ));
}
