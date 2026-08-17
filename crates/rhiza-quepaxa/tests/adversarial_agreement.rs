use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Barrier, Condvar, Mutex,
    },
    thread,
    time::Duration,
};

use rhiza_core::{Command, CommandKind, EntryType, LogHash, StoredCommand};
use rhiza_quepaxa::{
    AcceptedValue, CertifiedDecisionInspection, DecisionProof, Error, Membership, PrioritySource,
    Proposal, ProposalPriority, RecordRequest, RecordSummary, RecorderFileStore, RecorderRpc,
    ThreeNodeConsensus,
};

const CLUSTER_ID: &str = "adversarial-cluster";
const EPOCH: u64 = 1;
const CONFIG_ID: u64 = 1;
const SLOT: u64 = 1;
const REORDER_PROBE_SLOT: u64 = 2;
const PROPOSER_A: &str = "n0";
const PROPOSER_B: &str = "conflicting-proposer-b";

#[derive(Debug)]
struct SeededPriority(u64);

impl PrioritySource for SeededPriority {
    fn sample(
        &self,
        slot: u64,
        round: u64,
        proposer: &str,
        recorder: &str,
    ) -> Result<ProposalPriority, Error> {
        let mut value = self.0 ^ slot.rotate_left(7) ^ round.rotate_left(17);
        for byte in proposer.bytes().chain(recorder.bytes()) {
            value = value
                .wrapping_mul(0x100_0000_01b3)
                .wrapping_add(u64::from(byte) + 1);
        }
        Ok(ProposalPriority::from_u64(value.max(1)))
    }
}

#[derive(Default)]
struct FaultCounts {
    dropped: AtomicUsize,
    duplicated: AtomicUsize,
    reordered_stale: AtomicUsize,
}

struct CollisionGate {
    expected: [&'static str; 2],
    seen: Mutex<BTreeSet<String>>,
    changed: Condvar,
}

impl CollisionGate {
    fn new(expected: [&'static str; 2]) -> Self {
        Self {
            expected,
            seen: Mutex::new(BTreeSet::new()),
            changed: Condvar::new(),
        }
    }

    fn wait_for_pair(&self, request: &RecordRequest) {
        if request.slot != SLOT
            || !self
                .expected
                .contains(&request.proposal.proposer_id.as_str())
        {
            return;
        }
        let mut seen = self.seen.lock().expect("collision gate lock poisoned");
        seen.insert(request.proposal.proposer_id.clone());
        if self.observed_pair(&seen) {
            self.changed.notify_all();
            return;
        }
        while !self.observed_pair(&seen) {
            seen = self
                .changed
                .wait(seen)
                .expect("collision gate wait poisoned");
        }
    }

    fn observed(&self) -> bool {
        self.observed_pair(&self.seen.lock().expect("collision gate lock poisoned"))
    }

    fn observed_pair(&self, seen: &BTreeSet<String>) -> bool {
        self.expected
            .iter()
            .all(|proposer| seen.contains(*proposer))
    }
}

#[derive(Clone)]
struct AdversarialRecorder {
    store: RecorderFileStore,
    previous: Arc<Mutex<BTreeMap<u64, RecordRequest>>>,
    broadcast: Arc<Barrier>,
    collision: Arc<CollisionGate>,
    counts: Arc<FaultCounts>,
}

impl AdversarialRecorder {
    fn deliver(&self, request: RecordRequest) -> Result<RecordSummary, Error> {
        let mut previous = self
            .previous
            .lock()
            .expect("adversarial schedule lock poisoned");
        let current = self.store.record(request.clone())?;
        let duplicate = self.store.record(request.clone())?;
        assert_eq!(
            (
                &duplicate.recorder_id,
                duplicate.slot,
                duplicate.config_id,
                duplicate.config_digest,
                duplicate.step,
                &duplicate.first_current,
                &duplicate.aggregate_prior,
            ),
            (
                &current.recorder_id,
                current.slot,
                current.config_id,
                current.config_digest,
                current.step,
                &current.first_current,
                &current.aggregate_prior,
            ),
            "duplicate delivery changed accepted recorder state"
        );
        match (&current.decided, &duplicate.decided) {
            (Some(current), Some(duplicate)) => assert_eq!(
                duplicate, current,
                "duplicate delivery changed decided evidence"
            ),
            (Some(_), None) => panic!("duplicate delivery lost decided evidence"),
            (None, Some(decided)) => assert!(
                current.first_current.as_ref() == Some(decided.proposal())
                    || current.aggregate_prior.as_ref() == Some(decided.proposal()),
                "new decided evidence does not match accepted state"
            ),
            (None, None) => {}
        }
        self.counts.duplicated.fetch_add(1, Ordering::Relaxed);

        if let Some(stale) = previous
            .insert(request.slot, request.clone())
            .filter(|stale| stale.step < request.step)
        {
            let replayed = self.store.record(stale)?;
            assert_eq!(
                replayed.step, current.step,
                "a stale delivery moved recorder state backward"
            );
            self.counts.reordered_stale.fetch_add(1, Ordering::Relaxed);
        }
        Ok(current)
    }
}

impl RecorderRpc for AdversarialRecorder {
    fn recorder_id(&self, _context: &rhiza_quepaxa::RecorderRpcContext) -> Result<String, Error> {
        self.store.recorder_id()
    }

    fn store_command_for(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
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
        _context: &rhiza_quepaxa::RecorderRpcContext,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> Result<Option<StoredCommand>, Error> {
        self.store
            .fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash)
    }

    fn record(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecordRequest,
    ) -> Result<RecordSummary, Error> {
        self.collision.wait_for_pair(&request);
        self.broadcast.wait();
        self.deliver(request)
    }

    fn install_decision_proof(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<(), Error> {
        self.store.install_decision_proof(proof, membership)
    }

    fn inspect_decision_proof(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> Result<Option<DecisionProof>, Error> {
        self.store.inspect_decision_proof(slot)
    }

    fn inspect_record_summary(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> Result<Option<RecordSummary>, Error> {
        self.store.inspect_record_summary(slot)
    }
}

#[derive(Clone)]
struct DroppedRecorder {
    broadcast: Option<Arc<Barrier>>,
    counts: Arc<FaultCounts>,
}

impl RecorderRpc for DroppedRecorder {
    fn record(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _request: RecordRequest,
    ) -> Result<RecordSummary, Error> {
        self.counts.dropped.fetch_add(1, Ordering::Relaxed);
        if let Some(broadcast) = &self.broadcast {
            broadcast.wait();
        }
        Err(Error::Io("scripted dropped delivery".into()))
    }

    fn inspect_decision_proof(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _slot: u64,
    ) -> Result<Option<DecisionProof>, Error> {
        Err(Error::Io("scripted dropped delivery".into()))
    }

    fn inspect_record_summary(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _slot: u64,
    ) -> Result<Option<RecordSummary>, Error> {
        Err(Error::Io("scripted dropped delivery".into()))
    }
}

#[test]
fn seeded_fault_schedules_preserve_agreement_and_certified_recovery() {
    run_membership_cases([PROPOSER_A, PROPOSER_B], false);
}

#[test]
fn concurrent_nonpreferred_proposals_recover_phase2_proofs() {
    run_membership_cases(["n1", PROPOSER_B], true);
}

fn run_membership_cases(proposers: [&'static str; 2], expect_phase2: bool) {
    for members in [3, 5, 7] {
        let (completed, watchdog) = mpsc::channel();
        let run = thread::spawn(move || {
            run_membership_case(members, proposers, expect_phase2);
            completed.send(()).expect("watchdog receiver dropped");
        });

        match watchdog.recv_timeout(Duration::from_secs(15)) {
            Ok(()) => run.join().expect("adversarial case panicked"),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                run.join().expect("adversarial case panicked")
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("{members}-recorder adversarial case deadlocked")
            }
        }
    }
}

fn run_membership_case(member_count: usize, proposers: [&'static str; 2], expect_phase2: bool) {
    let root = tempfile::tempdir().expect("temporary recorder root");
    let ids: Vec<_> = (0..member_count).map(|index| format!("n{index}")).collect();
    if expect_phase2 {
        assert_ne!(proposers[0], ids[0]);
        assert_ne!(proposers[1], ids[0]);
        assert_ne!(proposers[0], proposers[1]);
    }
    let membership = Membership::from_voters(ids.iter().cloned()).expect("valid membership");
    let quorum = membership.quorum_size();
    let broadcast = Arc::new(Barrier::new(member_count));
    let collision = Arc::new(CollisionGate::new(proposers));
    let counts = Arc::new(FaultCounts::default());
    let stores: Vec<_> = ids
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id,
                CLUSTER_ID,
                EPOCH,
                CONFIG_ID,
                membership.clone(),
            )
            .expect("open recorder")
        })
        .collect();
    let live: Vec<_> = stores
        .iter()
        .take(quorum)
        .cloned()
        .map(|store| AdversarialRecorder {
            store,
            previous: Arc::new(Mutex::new(BTreeMap::new())),
            broadcast: Arc::clone(&broadcast),
            collision: Arc::clone(&collision),
            counts: Arc::clone(&counts),
        })
        .collect();

    for (index, recorder) in live.iter().enumerate() {
        exercise_reordered_stale_delivery(recorder, index);
    }

    let proposer_a = consensus(
        &ids,
        &live,
        quorum,
        Arc::clone(&broadcast),
        Arc::clone(&counts),
        proposers[0],
    );
    let proposer_b = consensus(
        &ids,
        &live,
        quorum,
        Arc::clone(&broadcast),
        Arc::clone(&counts),
        proposers[1],
    );

    let proposer_a = Arc::new(proposer_a);
    let proposer_b = Arc::new(proposer_b);
    let start = Arc::new(Barrier::new(3));
    let caller_a = {
        let proposer = Arc::clone(&proposer_a);
        let start = Arc::clone(&start);
        thread::spawn(move || {
            start.wait();
            proposer.propose_at(
                rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                SLOT,
                LogHash::ZERO,
                Command::new(CommandKind::Deterministic, b"command-a".to_vec()),
            )
        })
    };
    let caller_b = {
        let proposer = Arc::clone(&proposer_b);
        let start = Arc::clone(&start);
        thread::spawn(move || {
            start.wait();
            proposer.propose_at(
                rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                SLOT,
                LogHash::ZERO,
                Command::new(
                    CommandKind::Deterministic,
                    b"conflicting-command-b".to_vec(),
                ),
            )
        })
    };
    start.wait();
    let outcomes = [
        caller_a.join().expect("proposer A caller panicked"),
        caller_b.join().expect("proposer B caller panicked"),
    ];
    assert!(
        collision.observed(),
        "both conflicting proposers must reach Record before either may proceed"
    );

    assert!(
        proposer_a.finish_pending_rpcs(Duration::from_secs(5)),
        "proposer A proof workers did not drain"
    );
    assert!(
        proposer_b.finish_pending_rpcs(Duration::from_secs(5)),
        "proposer B proof workers did not drain"
    );
    drop(proposer_a);
    drop(proposer_b);
    drop(live);
    drop(stores);

    let recovery_recorders = ids
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let recorder: Box<dyn RecorderRpc> = if index < quorum {
                Box::new(
                    RecorderFileStore::new_with_membership(
                        root.path().join(id),
                        id,
                        CLUSTER_ID,
                        EPOCH,
                        CONFIG_ID,
                        membership.clone(),
                    )
                    .expect("reopen live recorder"),
                )
            } else {
                Box::new(DroppedRecorder {
                    broadcast: None,
                    counts: Arc::clone(&counts),
                })
            };
            (id.clone(), recorder)
        })
        .collect();
    let recovery = ThreeNodeConsensus::from_recorders_with_ids(
        CLUSTER_ID,
        "recovery-proposer",
        EPOCH,
        CONFIG_ID,
        recovery_recorders,
    )
    .expect("construct recovery proposer");
    let recovered = match recovery
        .inspect_certified_decision_at(
            &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            SLOT,
            LogHash::ZERO,
        )
        .expect("inspect reopened recorders")
    {
        CertifiedDecisionInspection::Committed(certified) => {
            if expect_phase2 {
                assert!(matches!(
                    certified.proof,
                    DecisionProof::Phase2 { step, .. } if step % 4 == 2
                ));
            }
            certified.entry
        }
        other => panic!("expected certified recovery, got {other:?}"),
    };
    for outcome in outcomes {
        match outcome {
            Ok(entry) => assert_eq!(entry, recovered),
            Err(Error::UnknownOutcome) => {}
            Err(error) => panic!("concurrent conflicting proposal failed: {error:?}"),
        }
    }

    assert!(
        counts.duplicated.load(Ordering::Relaxed) >= quorum * 4,
        "every live recorder must observe duplicate deliveries"
    );
    assert!(
        counts.reordered_stale.load(Ordering::Relaxed) >= quorum,
        "every live recorder must observe a lower-step replay"
    );
    assert!(
        counts.dropped.load(Ordering::Relaxed) >= (member_count - quorum) * 2,
        "every proposer broadcast must exercise the dropped minority"
    );
}

fn consensus(
    ids: &[String],
    live: &[AdversarialRecorder],
    quorum: usize,
    broadcast: Arc<Barrier>,
    counts: Arc<FaultCounts>,
    proposer: &str,
) -> ThreeNodeConsensus {
    let recorders = ids
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let recorder: Box<dyn RecorderRpc> = if index < quorum {
                Box::new(live[index].clone())
            } else {
                Box::new(DroppedRecorder {
                    broadcast: Some(Arc::clone(&broadcast)),
                    counts: Arc::clone(&counts),
                })
            };
            (id.clone(), recorder)
        })
        .collect();
    ThreeNodeConsensus::from_recorders_with_ids(CLUSTER_ID, proposer, EPOCH, CONFIG_ID, recorders)
        .expect("construct proposer")
        .with_priority_source(Arc::new(SeededPriority(0x5eed_cafe_d15c_a11e)))
}

fn exercise_reordered_stale_delivery(recorder: &AdversarialRecorder, recorder_index: usize) {
    let command = StoredCommand::new(
        EntryType::Command,
        format!("reorder-probe-{recorder_index}").into_bytes(),
    );
    let proposal = Proposal::new(
        ProposalPriority::from_u64(recorder_index as u64 + 1),
        "reorder-probe",
        1,
        AcceptedValue::from_command(
            CLUSTER_ID,
            REORDER_PROBE_SLOT,
            EPOCH,
            CONFIG_ID,
            LogHash::ZERO,
            &command,
        ),
    );
    let mut request = RecordRequest {
        cluster_id: CLUSTER_ID.into(),
        epoch: EPOCH,
        config_id: CONFIG_ID,
        config_digest: recorder
            .store
            .configuration_state()
            .unwrap()
            .config_digest(),
        slot: REORDER_PROBE_SLOT,
        step: 4,
        proposal,
        command: Some(command),
    };
    recorder
        .deliver(request.clone())
        .expect("deliver initial probe request");
    request.step = 5;
    recorder
        .deliver(request)
        .expect("deliver newer request before stale replay");
}
