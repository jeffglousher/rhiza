use rhiza_core::{Command, CommandKind, EntryType, LogHash, StoredCommand};
use rhiza_quepaxa::{
    AcceptedValue, Ballot, ConfigChange, DecisionCertificate, DecisionProof, Error, Membership,
    Proposal, ProposalPriority, RecordRequest, RecorderFileStore, RecorderRequest, RecorderSummary,
    RejectReason, SealFaultPoint, ThreeNodeConsensus,
};
use std::{fs, path::Path};

fn same_membership_stop(cluster_id: &str, config_id: u64, membership: &Membership) -> ConfigChange {
    ConfigChange::bound_stop(
        cluster_id,
        config_id,
        membership.digest(),
        config_id + 1,
        membership.members().to_vec(),
    )
    .unwrap()
}

#[test]
fn membership_is_canonical_for_three_through_seven_unique_voters() {
    for count in 3..=7 {
        let voters: Vec<_> = (0..count).rev().map(|id| format!("node-{id}")).collect();
        let membership = Membership::from_voters(voters).unwrap();

        assert!(membership
            .members()
            .windows(2)
            .all(|pair| pair[0] < pair[1]));
        assert_eq!(membership.quorum_size(), count / 2 + 1);
        assert_eq!(
            membership.digest(),
            Membership::from_voters(membership.members().to_vec())
                .unwrap()
                .digest()
        );
    }
}

#[test]
fn decision_certificate_requires_sorted_exact_membership_quorum_and_digest() {
    let membership = Membership::new(["r3", "r1", "r2", "r4"]).unwrap();
    let mut certificate = DecisionCertificate {
        slot: 9,
        epoch: 2,
        config_id: 7,
        config_digest: membership.digest(),
        ballot: Ballot::new(1, 1, "writer"),
        value: AcceptedValue {
            command_hash: LogHash::from_bytes([1; 32]),
            prev_hash: LogHash::from_bytes([2; 32]),
            entry_hash: LogHash::from_bytes([3; 32]),
        },
        recorder_ids: vec!["r1".into(), "r2".into(), "r3".into()],
    };
    assert_eq!(certificate.validate_for(7, &membership), Ok(()));

    certificate.recorder_ids.swap(0, 1);
    assert_eq!(
        certificate.validate_for(7, &membership),
        Err(RejectReason::InvalidCertificate)
    );
    certificate.recorder_ids.sort();
    certificate.config_digest = LogHash::ZERO;
    assert_eq!(
        certificate.validate_for(7, &membership),
        Err(RejectReason::WrongConfig)
    );
}

#[test]
fn config_change_commands_are_opaque_and_strictly_recognized() {
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let stop_change = same_membership_stop("cluster-a", 4, &membership);
    let stop = stop_change.to_stored_command();
    assert_eq!(stop.entry_type, EntryType::ConfigChange);
    assert_eq!(ConfigChange::recognize(&stop).unwrap(), stop_change);

    let activation_change = ConfigChange::bound_activation_barrier(
        stop_change.successor().expect("bound stop").clone(),
        10,
        LogHash::from_bytes([9; 32]),
        stop.hash(),
    );
    let activation = activation_change.to_stored_command();
    assert_eq!(
        ConfigChange::recognize(&activation).unwrap(),
        activation_change
    );

    let unknown = StoredCommand::new(EntryType::ConfigChange, b"not-a-quepaxa-change".to_vec());
    assert!(ConfigChange::recognize(&unknown).is_err());
}

#[test]
fn stop_seal_is_durable_before_ack_and_blocks_later_old_slots() {
    for fault in [
        SealFaultPoint::AfterIntent,
        SealFaultPoint::AfterSlot,
        SealFaultPoint::AfterConfiguration,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
        let recorder = store(&dir, "r1", 4, membership.clone());
        let stop = same_membership_stop("cluster-a", 4, &membership).to_stored_command();
        let value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &stop);
        recorder.store_command(stop.hash(), stop).unwrap();
        recorder.set_seal_fault(Some(fault)).unwrap();

        assert!(matches!(
            record(&recorder, 7, 4, proposal("writer", 1, 1, value.clone())),
            Err(Error::Io(message)) if message.contains(&format!("{fault:?}"))
        ));
        drop(recorder);

        let recorder = store(&dir, "r1", 4, Membership::new(["r1", "r2", "r3"]).unwrap());
        let state = recorder.configuration_state().unwrap();
        assert_eq!(state.seal().unwrap().stop_slot, 7);
        assert_eq!(state.max_accepted_or_decided_slot(), Some(7));
        assert_eq!(
            record(&recorder, 8, 4, proposal("writer", 1, 1, value)),
            Err(Error::Rejected(RejectReason::ConfigurationSealed {
                stop_slot: 7
            }))
        );
    }
}

#[test]
fn higher_normal_record_does_not_clear_provisional_stop_seal() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(&dir, "r1", 4, membership.clone());
    let stop = same_membership_stop("cluster-a", 4, &membership).to_stored_command();
    let stop_value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &stop);
    recorder.store_command(stop.hash(), stop).unwrap();
    record(&recorder, 7, 4, proposal("stopper", 1, 1, stop_value)).unwrap();
    assert!(recorder.configuration_state().unwrap().seal().is_some());

    let normal = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let normal_value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &normal);
    recorder.store_command(normal.hash(), normal).unwrap();
    record(&recorder, 7, 4, proposal("writer", 2, 9, normal_value)).unwrap();
    assert!(recorder.configuration_state().unwrap().seal().is_some());
    drop(recorder);

    let reopened = store(&dir, "r1", 4, membership);
    assert!(reopened.configuration_state().unwrap().seal().is_some());
    let later = StoredCommand::new(EntryType::Command, b"later".to_vec());
    let later_value = AcceptedValue::from_command("cluster-a", 8, 2, 4, LogHash::ZERO, &later);
    reopened.store_command(later.hash(), later).unwrap();
    assert!(matches!(
        record(&reopened, 8, 4, proposal("writer", 3, 10, later_value)),
        Err(Error::Rejected(RejectReason::ConfigurationSealed {
            stop_slot: 7
        }))
    ));
}

#[test]
fn stop_rejects_a_recorder_that_already_accepted_a_later_old_slot() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(&dir, "r1", 4, membership.clone());
    let normal = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let normal_value = AcceptedValue::from_command("cluster-a", 8, 2, 4, LogHash::ZERO, &normal);
    recorder.store_command(normal.hash(), normal).unwrap();
    record(&recorder, 8, 4, proposal("writer", 1, 1, normal_value)).unwrap();

    let stop = same_membership_stop("cluster-a", 4, &membership).to_stored_command();
    let stop_value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &stop);
    recorder.store_command(stop.hash(), stop).unwrap();
    assert_eq!(
        record(&recorder, 7, 4, proposal("stopper", 1, 1, stop_value)),
        Err(Error::Rejected(RejectReason::InvalidTransition))
    );
    assert_eq!(
        recorder
            .configuration_state()
            .unwrap()
            .max_accepted_or_decided_slot(),
        Some(8)
    );
}

#[test]
fn validated_stop_proof_seals_a_recorder_that_did_not_record_the_stop() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(&dir, "r3", 4, membership.clone());
    let stop = same_membership_stop("cluster-a", 4, &membership).to_stored_command();
    let value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &stop);
    recorder.store_command(stop.hash(), stop).unwrap();
    let stop_proposal = proposal("writer", 1, 1, value);
    recorder
        .install_decision_proof_record(
            phase2_proof("cluster-a", &membership, 7, stop_proposal),
            &membership,
        )
        .unwrap();

    assert_eq!(
        recorder
            .configuration_state()
            .unwrap()
            .seal()
            .unwrap()
            .stop_slot,
        7
    );
    let higher = StoredCommand::new(EntryType::Command, b"higher-normal".to_vec());
    let higher_value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &higher);
    recorder.store_command(higher.hash(), higher).unwrap();
    record(
        &recorder,
        7,
        8,
        proposal("higher-writer", 2, 9, higher_value),
    )
    .unwrap();
    assert!(recorder.configuration_state().unwrap().seal().is_some());
    assert_eq!(
        {
            let later = StoredCommand::new(EntryType::Command, b"later".to_vec());
            let later_value =
                AcceptedValue::from_command("cluster-a", 8, 2, 4, LogHash::ZERO, &later);
            recorder.store_command(later.hash(), later).unwrap();
            record(&recorder, 8, 4, proposal("writer", 2, 1, later_value))
        },
        Err(Error::Rejected(RejectReason::ConfigurationSealed {
            stop_slot: 7
        }))
    );
}

#[test]
fn successor_is_bound_by_stop_proof_and_activation_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let old = Membership::new(["r1", "r2", "r3"]).unwrap();
    let stores = [
        store(&dir, "r1", 4, old.clone()),
        store(&dir, "r2", 4, old.clone()),
        store(&dir, "r3", 4, old.clone()),
    ];
    let consensus = ThreeNodeConsensus::from_recorders_with_ids(
        "cluster-a",
        "writer",
        2,
        4,
        stores
            .iter()
            .zip(["r1", "r2", "r3"])
            .map(|(store, id)| (id.into(), Box::new(store.clone()) as _))
            .collect(),
    )
    .unwrap();
    let next = Membership::new(["r1", "r2", "r3", "r4"]).unwrap();
    let stop = consensus
        .propose_stop_for_successor_at(
            rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            7,
            LogHash::ZERO,
            &next,
        )
        .unwrap();
    let proof = consensus
        .inspect_decision_proof_at(&rhiza_quepaxa::RecorderRpcContext::default_timeout(), 7)
        .unwrap()
        .unwrap();
    drop(consensus);

    let joining = store(&dir, "r4", 4, old.clone());
    for store in stores.iter().chain(std::iter::once(&joining)) {
        let installed = store
            .install_successor_from_proof(next.clone(), &proof)
            .unwrap();
        assert!(!installed.is_activated());
    }

    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let normal = AcceptedValue::from_command("cluster-a", 8, 2, 5, stop.hash, &command);
    joining.store_command(command.hash(), command).unwrap();
    assert_eq!(
        record(&joining, 8, 4, proposal("writer", 1, 1, normal)),
        Err(Error::Rejected(RejectReason::ActivationRequired))
    );

    let successor = ThreeNodeConsensus::from_recorders_with_ids(
        "cluster-a",
        "writer",
        2,
        5,
        stores
            .iter()
            .chain(std::iter::once(&joining))
            .zip(["r1", "r2", "r3", "r4"])
            .map(|(store, id)| (id.into(), Box::new(store.clone()) as _))
            .collect(),
    )
    .unwrap();
    let barrier = successor
        .propose_activation_for_stop_at(
            rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            &proof,
        )
        .unwrap();
    assert_eq!(barrier.index, 8);
    assert_eq!(
        successor
            .propose_activation_for_stop_at(
                rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                &proof
            )
            .unwrap(),
        barrier
    );
    assert!(
        stores
            .iter()
            .chain(std::iter::once(&joining))
            .filter(|store| store.configuration_state().unwrap().is_activated())
            .count()
            >= next.quorum_size()
    );
    assert_eq!(
        joining.apply(RecorderRequest::Inspect {
            cluster_id: "cluster-a".into(),
            epoch: 2,
            config_id: 5,
            config_digest: next.digest(),
            slot: 7,
        }),
        Err(Error::Rejected(RejectReason::ConfigurationNotInstalled))
    );
    successor
        .propose_at(
            rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            9,
            barrier.hash,
            Command::new(CommandKind::Deterministic, b"normal".to_vec()),
        )
        .unwrap();
}

#[test]
fn activation_proof_reinstall_accepts_same_value_and_rejects_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(&dir, "r1", 4, membership.clone());
    let stop_change = ConfigChange::bound_stop(
        "cluster-a",
        4,
        membership.digest(),
        5,
        membership.members().to_vec(),
    )
    .unwrap();
    let stop = stop_change.to_stored_command();
    let stop_value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &stop);
    let stop_proof = phase2_proof(
        "cluster-a",
        &membership,
        7,
        proposal("writer", 1, 1, stop_value.clone()),
    );
    recorder
        .install_successor_from_proof(membership.clone(), &stop_proof)
        .unwrap();

    let activation = ConfigChange::bound_activation_barrier(
        stop_change.successor().expect("bound stop").clone(),
        7,
        stop_value.entry_hash,
        stop_value.command_hash,
    )
    .to_stored_command();
    let activation_value =
        AcceptedValue::from_command("cluster-a", 8, 2, 5, stop_value.entry_hash, &activation);
    recorder
        .store_command(activation.hash(), activation)
        .unwrap();
    let activation_proof = phase2_proof_for_config(
        "cluster-a",
        &membership,
        5,
        8,
        proposal("writer", 2, 1, activation_value),
    );

    recorder
        .install_decision_proof_record(activation_proof.clone(), &membership)
        .unwrap();
    assert!(recorder.configuration_state().unwrap().is_activated());
    recorder
        .install_decision_proof_record(activation_proof, &membership)
        .unwrap();

    let conflicting = StoredCommand::new(EntryType::Command, b"conflict".to_vec());
    let conflicting_value =
        AcceptedValue::from_command("cluster-a", 8, 2, 5, stop_value.entry_hash, &conflicting);
    recorder
        .store_command(conflicting.hash(), conflicting)
        .unwrap();
    assert_eq!(
        recorder.install_decision_proof_record(
            phase2_proof_for_config(
                "cluster-a",
                &membership,
                5,
                8,
                proposal("writer", 3, 2, conflicting_value),
            ),
            &membership,
        ),
        Err(Error::Rejected(RejectReason::AlreadyDecided))
    );
}

#[test]
fn verified_checkpoint_recovery_reactivates_fresh_successor_recorders() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let stores = [
        store(&dir, "r1", 4, membership.clone()),
        store(&dir, "r2", 4, membership.clone()),
        store(&dir, "r3", 4, membership.clone()),
    ];
    let old = ThreeNodeConsensus::from_recorders_with_ids(
        "cluster-a",
        "writer",
        2,
        4,
        stores
            .iter()
            .zip(["r1", "r2", "r3"])
            .map(|(store, id)| (id.into(), Box::new(store.clone()) as _))
            .collect(),
    )
    .unwrap();
    let stop = old
        .propose_stop_for_successor_at(
            rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            7,
            LogHash::ZERO,
            &membership,
        )
        .unwrap();
    let proof = old
        .inspect_decision_proof_at(&rhiza_quepaxa::RecorderRpcContext::default_timeout(), 7)
        .unwrap()
        .unwrap();
    drop(old);

    let checkpoint_hash = LogHash::digest(&[b"checkpoint-tip"]);
    for recorder in &stores {
        recorder
            .install_successor_from_proof(membership.clone(), &proof)
            .unwrap();
        let recovered = recorder
            .recover_successor_activation_from_checkpoint(7, stop.hash, 9, checkpoint_hash)
            .unwrap();
        assert!(recovered.is_activated());
        assert_eq!(recovered.max_accepted_or_decided_slot(), Some(9));
    }
    drop(stores);
    let stores = [
        store(&dir, "r1", 5, membership.clone()),
        store(&dir, "r2", 5, membership.clone()),
        store(&dir, "r3", 5, membership.clone()),
    ];
    for recorder in &stores {
        assert_eq!(
            recorder
                .configuration_state()
                .unwrap()
                .max_accepted_or_decided_slot(),
            Some(9)
        );
    }

    let recovered = ThreeNodeConsensus::from_recorders_with_ids(
        "cluster-a",
        "writer",
        2,
        5,
        stores
            .iter()
            .zip(["r1", "r2", "r3"])
            .map(|(store, id)| (id.into(), Box::new(store.clone()) as _))
            .collect(),
    )
    .unwrap();
    assert_eq!(
        recovered
            .propose_at(
                rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                10,
                checkpoint_hash,
                Command::new(CommandKind::ReadBarrier, Vec::new()),
            )
            .unwrap()
            .prev_hash,
        checkpoint_hash
    );
}

#[test]
fn checkpoint_recovery_rejects_zero_hash_without_mutating_pending_successor() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let stores = [
        store(&dir, "r1", 4, membership.clone()),
        store(&dir, "r2", 4, membership.clone()),
        store(&dir, "r3", 4, membership.clone()),
    ];
    let old = ThreeNodeConsensus::from_recorders_with_ids(
        "cluster-a",
        "writer",
        2,
        4,
        stores
            .iter()
            .zip(["r1", "r2", "r3"])
            .map(|(store, id)| (id.into(), Box::new(store.clone()) as _))
            .collect(),
    )
    .unwrap();
    let stop = old
        .propose_stop_for_successor_at(
            rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            7,
            LogHash::ZERO,
            &membership,
        )
        .unwrap();
    let proof = old
        .inspect_decision_proof_at(&rhiza_quepaxa::RecorderRpcContext::default_timeout(), 7)
        .unwrap()
        .unwrap();
    drop(old);

    let recorder = &stores[0];
    recorder
        .install_successor_from_proof(membership.clone(), &proof)
        .unwrap();
    let before_state = recorder.configuration_state().unwrap();
    let root = dir.path().join("r1");
    let before_files = direct_file_snapshot(&root);

    assert_eq!(
        recorder.recover_successor_activation_from_checkpoint(7, stop.hash, 9, LogHash::ZERO),
        Err(Error::Rejected(RejectReason::InvalidTransition))
    );
    assert_eq!(recorder.configuration_state().unwrap(), before_state);
    assert_eq!(direct_file_snapshot(&root), before_files);
    assert!(!recorder.configuration_state().unwrap().is_activated());

    drop(stores);
    let reopened = store(&dir, "r1", 5, membership);
    assert_eq!(reopened.configuration_state().unwrap(), before_state);
    assert!(!reopened.configuration_state().unwrap().is_activated());
}

#[test]
fn checkpoint_head_intent_recovers_every_config_and_head_fault_phase() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let stores = [
        store(&dir, "r1", 4, membership.clone()),
        store(&dir, "r2", 4, membership.clone()),
        store(&dir, "r3", 4, membership.clone()),
    ];
    let old = ThreeNodeConsensus::from_recorders_with_ids(
        "cluster-a",
        "writer",
        2,
        4,
        stores
            .iter()
            .zip(["r1", "r2", "r3"])
            .map(|(store, id)| (id.into(), Box::new(store.clone()) as _))
            .collect(),
    )
    .unwrap();
    let stop = old
        .propose_stop_for_successor_at(
            rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            7,
            LogHash::ZERO,
            &membership,
        )
        .unwrap();
    let proof = old
        .inspect_decision_proof_at(&rhiza_quepaxa::RecorderRpcContext::default_timeout(), 7)
        .unwrap()
        .unwrap();
    drop(old);
    let checkpoint_hash = LogHash::digest(&[b"checkpoint-fault-tip"]);

    for (recorder, fault) in stores.iter().zip([
        SealFaultPoint::AfterHeadIntent,
        SealFaultPoint::AfterHeadConfiguration,
        SealFaultPoint::AfterHead,
    ]) {
        recorder
            .install_successor_from_proof(membership.clone(), &proof)
            .unwrap();
        recorder.set_seal_fault(Some(fault)).unwrap();
        assert!(matches!(
            recorder.recover_successor_activation_from_checkpoint(
                7,
                stop.hash,
                9,
                checkpoint_hash,
            ),
            Err(Error::Io(message)) if message.contains(&format!("{fault:?}"))
        ));
    }
    drop(stores);

    for id in ["r1", "r2", "r3"] {
        let reopened = store(&dir, id, 5, membership.clone());
        let state = reopened.configuration_state().unwrap();
        assert!(state.is_activated());
        assert_eq!(state.max_accepted_or_decided_slot(), Some(9));
    }
}

#[test]
fn one_stop_proof_cannot_install_two_disjoint_successors() {
    let old = Membership::new(["r1", "r2", "r3"]).unwrap();
    let next_a = Membership::new(["a1", "a2", "a3"]).unwrap();
    let next_b = Membership::new(["b1", "b2", "b3"]).unwrap();
    let stop = ConfigChange::bound_stop("cluster-a", 4, old.digest(), 5, next_a.members().to_vec())
        .unwrap()
        .to_stored_command();
    let value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &stop);
    let proof = phase2_proof("cluster-a", &old, 7, proposal("writer", 1, 1, value));

    let dir_a = tempfile::tempdir().unwrap();
    let store_a = store(&dir_a, "a1", 4, old.clone());
    assert!(store_a.install_successor_from_proof(next_a, &proof).is_ok());
    let dir_b = tempfile::tempdir().unwrap();
    let store_b = store(&dir_b, "b1", 4, old);
    assert_eq!(
        store_b.install_successor_from_proof(next_b, &proof),
        Err(Error::Rejected(RejectReason::InvalidTransition))
    );
}

#[test]
fn cross_cluster_stop_proof_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let old = Membership::new(["r1", "r2", "r3"]).unwrap();
    let next = Membership::new(["r1", "r2", "r3"]).unwrap();
    let stop = ConfigChange::bound_stop("cluster-b", 4, old.digest(), 5, next.members().to_vec())
        .unwrap()
        .to_stored_command();
    let value = AcceptedValue::from_command("cluster-b", 7, 2, 4, LogHash::ZERO, &stop);
    let proof = phase2_proof("cluster-b", &old, 7, proposal("writer", 1, 1, value));
    let recorder = store(&dir, "r1", 4, old);
    assert_eq!(
        recorder.install_successor_from_proof(next, &proof),
        Err(Error::Rejected(RejectReason::WrongCluster))
    );
}

#[test]
fn same_voter_successor_is_legal() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let stop = ConfigChange::bound_stop(
        "cluster-a",
        4,
        membership.digest(),
        5,
        membership.members().to_vec(),
    )
    .unwrap()
    .to_stored_command();
    let value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &stop);
    let proof = phase2_proof("cluster-a", &membership, 7, proposal("writer", 1, 1, value));
    let recorder = store(&dir, "r1", 4, membership.clone());
    let installed = recorder
        .install_successor_from_proof(membership, &proof)
        .unwrap();
    assert_eq!(installed.config_id(), 5);
}

#[test]
fn losing_stop_never_seals_with_the_aggregate_winner_hash() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(&dir, "r1", 4, membership.clone());
    let normal = StoredCommand::new(EntryType::Command, b"winner".to_vec());
    let normal_value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &normal);
    recorder.store_command(normal.hash(), normal).unwrap();
    record(
        &recorder,
        7,
        4,
        proposal("winner", 1, 9, normal_value.clone()),
    )
    .unwrap();
    let stop = same_membership_stop("cluster-a", 4, &membership).to_stored_command();
    let stop_value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &stop);
    recorder.store_command(stop.hash(), stop).unwrap();
    record(&recorder, 7, 4, proposal("loser", 2, 1, stop_value.clone())).unwrap();

    let seal = recorder
        .configuration_state()
        .unwrap()
        .seal()
        .unwrap()
        .clone();
    assert_eq!(seal.command_hash, stop_value.command_hash);
    assert_ne!(seal.command_hash, normal_value.command_hash);
}

#[test]
fn validated_non_stop_proof_clears_provisional_seal() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(&dir, "r1", 4, membership.clone());
    let stop = same_membership_stop("cluster-a", 4, &membership).to_stored_command();
    let stop_value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &stop);
    recorder.store_command(stop.hash(), stop).unwrap();
    record(&recorder, 7, 4, proposal("stopper", 1, 1, stop_value)).unwrap();
    let normal = StoredCommand::new(EntryType::Command, b"winner".to_vec());
    let normal_value = AcceptedValue::from_command("cluster-a", 7, 2, 4, LogHash::ZERO, &normal);
    recorder.store_command(normal.hash(), normal).unwrap();
    let proof = phase2_proof(
        "cluster-a",
        &membership,
        7,
        proposal("writer", 2, 9, normal_value),
    );
    recorder
        .install_decision_proof_record(proof, &membership)
        .unwrap();
    assert!(recorder.configuration_state().unwrap().seal().is_none());
}

fn store(
    dir: &tempfile::TempDir,
    id: &str,
    config_id: u64,
    membership: Membership,
) -> RecorderFileStore {
    RecorderFileStore::new_with_membership(
        dir.path().join(id),
        id,
        "cluster-a",
        2,
        config_id,
        membership,
    )
    .unwrap()
}

fn direct_file_snapshot(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files = fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_type().unwrap().is_file())
        .map(|entry| {
            (
                entry.file_name().to_string_lossy().into_owned(),
                fs::read(entry.path()).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn record(
    store: &RecorderFileStore,
    slot: u64,
    step: u64,
    proposal: Proposal,
) -> rhiza_quepaxa::Result<rhiza_quepaxa::RecordSummary> {
    let configuration = store.configuration_state()?;
    store.record_proposal(RecordRequest {
        cluster_id: "cluster-a".into(),
        epoch: 2,
        config_id: configuration.config_id(),
        config_digest: configuration.config_digest(),
        slot,
        step,
        proposal,
        command: None,
    })
}

fn proposal(proposer: &str, id: u64, priority: u64, value: AcceptedValue) -> Proposal {
    Proposal::new(ProposalPriority::from_u64(priority), proposer, id, value)
}

fn phase2_proof(
    cluster_id: &str,
    membership: &Membership,
    slot: u64,
    proposal: Proposal,
) -> DecisionProof {
    phase2_proof_for_config(cluster_id, membership, 4, slot, proposal)
}

fn phase2_proof_for_config(
    cluster_id: &str,
    membership: &Membership,
    config_id: u64,
    slot: u64,
    proposal: Proposal,
) -> DecisionProof {
    DecisionProof::Phase2 {
        cluster_id: cluster_id.into(),
        slot,
        epoch: 2,
        config_id,
        config_digest: membership.digest(),
        step: 6,
        summaries: membership.members()[..membership.quorum_size()]
            .iter()
            .map(|id| RecorderSummary {
                recorder_id: id.clone(),
                slot,
                step: 6,
                first_current: None,
                aggregate_prior: Some(proposal.clone()),
            })
            .collect(),
        proposal,
    }
}
