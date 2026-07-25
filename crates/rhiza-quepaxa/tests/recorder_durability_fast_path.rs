use std::fs;

use rhiza_core::{EntryType, LogHash, StoredCommand};
use rhiza_quepaxa::{
    AcceptedValue, ConfigChange, Error, Membership, Proposal, ProposalPriority, ReadFenceRequest,
    ReadFenceSlotState, RecordRequest, RecorderFileStore, RecorderRpc, RejectReason,
    SealFaultPoint,
};

const CLUSTER_ID: &str = "cluster-a";
const EPOCH: u64 = 2;
const CONFIG_ID: u64 = 4;

#[test]
fn read_fence_observation_is_empty_only_beyond_the_durable_head() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let request = |slot| ReadFenceRequest {
        cluster_id: CLUSTER_ID.into(),
        epoch: EPOCH,
        config_id: CONFIG_ID,
        config_digest: membership.digest(),
        slot,
    };

    let fresh = recorder.observe_read_fence(request(8)).unwrap();
    assert_eq!(fresh.max_head, None);
    assert_eq!(fresh.slot_state, ReadFenceSlotState::Empty);

    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let value =
        AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);
    recorder
        .record_proposal(record(8, proposal("writer", 1, 1, value), Some(command)))
        .unwrap();

    let exact = recorder.observe_read_fence(request(8)).unwrap();
    assert_eq!(exact.max_head, Some(8));
    assert!(matches!(
        exact.slot_state,
        ReadFenceSlotState::Occupied {
            summary: Some(summary)
        } if summary.slot == 8 && summary.recorder_id == "r1"
    ));

    let gap_below_head = recorder.observe_read_fence(request(7)).unwrap();
    assert_eq!(gap_below_head.max_head, Some(8));
    assert_eq!(
        gap_below_head.slot_state,
        ReadFenceSlotState::Occupied { summary: None }
    );

    let beyond_head = recorder.observe_read_fence(request(9)).unwrap();
    assert_eq!(beyond_head.max_head, Some(8));
    assert_eq!(beyond_head.slot_state, ReadFenceSlotState::Empty);

    drop(recorder);
    let reopened = store(dir.path(), membership.clone()).unwrap();
    let after_reopen = reopened.observe_read_fence(request(8)).unwrap();
    assert_eq!(after_reopen.max_head, Some(8));
    assert!(matches!(
        after_reopen.slot_state,
        ReadFenceSlotState::Occupied {
            summary: Some(summary)
        } if summary.slot == 8 && summary.recorder_id == "r1"
    ));
}

#[test]
fn read_fence_observation_rejects_a_mismatched_context() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();

    let result = recorder.observe_read_fence(ReadFenceRequest {
        cluster_id: "wrong-cluster".into(),
        epoch: EPOCH,
        config_id: CONFIG_ID,
        config_digest: membership.digest(),
        slot: 8,
    });

    assert_eq!(result, Err(Error::Rejected(RejectReason::WrongCluster)));
}

#[test]
fn lock_only_partial_initialization_retries_as_fresh() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join(".recorder.lock"), b"").unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();

    let recorder = store(dir.path(), membership.clone()).unwrap();

    assert_eq!(
        recorder.configuration_state().unwrap().membership(),
        Some(&membership)
    );
    assert!(dir.path().join("configuration.rec").exists());
    assert!(dir.path().join("recorded-head.rec").exists());
}

#[test]
fn normal_record_reopens_with_command_and_max_without_rewriting_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let configuration_path = dir.path().join("configuration.rec");
    let configuration_before = fs::read(&configuration_path).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let value =
        AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);

    recorder
        .record_proposal(record(
            8,
            proposal("writer", 1, 1, value),
            Some(command.clone()),
        ))
        .unwrap();

    assert_eq!(fs::read(&configuration_path).unwrap(), configuration_before);
    assert_eq!(
        recorder.fetch_command(command.hash()).unwrap(),
        Some(command.clone())
    );
    drop(recorder);

    let reopened = store(dir.path(), membership.clone()).unwrap();
    assert_eq!(
        reopened
            .configuration_state()
            .unwrap()
            .max_accepted_or_decided_slot(),
        Some(8)
    );
    assert_eq!(
        reopened.fetch_command(command.hash()).unwrap(),
        Some(command)
    );

    let stop = ConfigChange::stop(
        CLUSTER_ID,
        CONFIG_ID,
        membership.digest(),
        CONFIG_ID + 1,
        membership.members().to_vec(),
    )
    .unwrap()
    .to_stored_command();
    let stop_value =
        AcceptedValue::from_command(CLUSTER_ID, 7, EPOCH, CONFIG_ID, LogHash::ZERO, &stop);
    assert_eq!(
        reopened.record_proposal(record(7, proposal("stopper", 1, 1, stop_value), Some(stop),)),
        Err(Error::Rejected(RejectReason::InvalidTransition))
    );
}

#[test]
fn reopen_reads_only_the_durable_head_and_ignores_non_head_history() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let value =
        AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);
    recorder
        .record_proposal(record(8, proposal("writer", 1, 1, value), Some(command)))
        .unwrap();
    drop(recorder);
    for slot in 0..1_000 {
        if slot != 8 {
            fs::write(
                dir.path().join(format!("slot-{slot:020}.rec")),
                b"corrupt non-head history",
            )
            .unwrap();
        }
    }

    let reopened = store(dir.path(), membership).unwrap();
    assert_eq!(
        reopened
            .configuration_state()
            .unwrap()
            .max_accepted_or_decided_slot(),
        Some(8)
    );
}

#[test]
fn reopen_repairs_a_corrupt_head_slot_cache_from_the_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let value =
        AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);
    recorder
        .record_proposal(record(8, proposal("writer", 1, 1, value), Some(command)))
        .unwrap();
    drop(recorder);
    fs::write(dir.path().join("slot-00000000000000000008.rec"), b"corrupt").unwrap();

    let reopened = store(dir.path(), membership).unwrap();
    assert_eq!(
        reopened
            .configuration_state()
            .unwrap()
            .max_accepted_or_decided_slot(),
        Some(8)
    );
    assert!(reopened.load(8).is_ok());
}

#[test]
fn reopen_fails_closed_when_the_authoritative_manifest_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    drop(store(dir.path(), membership.clone()).unwrap());
    let manifest_path = dir.path().join("recorded-head.rec");
    let mut manifest = fs::read(&manifest_path).unwrap();
    manifest[10] ^= 1;
    fs::write(manifest_path, manifest).unwrap();

    assert!(matches!(
        store(dir.path(), membership),
        Err(Error::Decode(_))
    ));
}

#[test]
fn noncurrent_recorder_directories_fail_without_mutation() {
    for (name, file) in [
        ("sentinel", "sentinel"),
        ("partial", "recorded-head.rec"),
        ("old-slot", "slot-00000000000000000008.rec"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(file), b"not a current recorder layout").unwrap();
        let before = directory_contents(dir.path());

        assert!(
            matches!(
                store(dir.path(), Membership::new(["r1", "r2", "r3"]).unwrap()),
                Err(Error::Decode(message))
                    if message == "recorder directory is not a current durable layout"
            ),
            "{name}"
        );
        assert_eq!(directory_contents(dir.path()), before, "{name}");
    }
}

#[test]
fn incomplete_current_layout_fails_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    drop(store(dir.path(), membership.clone()).unwrap());
    fs::remove_file(dir.path().join("recorded-head.rec")).unwrap();
    let before = directory_contents(dir.path());

    assert!(matches!(
        store(dir.path(), membership),
        Err(Error::Decode(message))
            if message == "recorder directory is not a current durable layout"
    ));
    assert_eq!(directory_contents(dir.path()), before);
}

#[test]
fn noncurrent_recorder_head_fails_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    drop(store(dir.path(), membership.clone()).unwrap());
    fs::write(
        dir.path().join("recorded-head.rec"),
        [b'Q', b'R', b'H', b'D', 0, 1],
    )
    .unwrap();
    let before = directory_contents(dir.path());

    assert!(matches!(
        store(dir.path(), membership),
        Err(Error::Decode(message))
            if message == "recorder directory is not a current durable layout"
    ));
    assert_eq!(directory_contents(dir.path()), before);
}

#[test]
fn reopen_fails_closed_when_an_acknowledged_slot_loses_its_command() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let value =
        AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);
    recorder
        .store_command(command.hash(), command.clone())
        .unwrap();
    recorder
        .record_proposal(record(8, proposal("writer", 1, 1, value), None))
        .unwrap();
    drop(recorder);
    fs::remove_file(
        dir.path()
            .join(format!("command-{}.cmd", command.hash().to_hex())),
    )
    .unwrap();

    assert!(matches!(
        store(dir.path(), membership),
        Err(Error::CommandUnavailable)
    ));
}

#[test]
fn interrupted_wal_record_replays_a_complete_unacknowledged_frame() {
    for fault in [SealFaultPoint::AfterWalWrite, SealFaultPoint::AfterWalSync] {
        let dir = tempfile::tempdir().unwrap();
        let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
        let recorder = store(dir.path(), membership.clone()).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
        let value =
            AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);
        let proposal = proposal("writer", 1, 1, value);
        recorder.set_seal_fault(Some(fault)).unwrap();

        assert!(matches!(
            recorder.record_proposal(record(8, proposal.clone(), Some(command.clone()))),
            Err(Error::Io(message)) if message.contains(&format!("{fault:?}"))
        ));
        drop(recorder);

        let reopened = store(dir.path(), membership).unwrap();
        assert_eq!(
            reopened
                .configuration_state()
                .unwrap()
                .max_accepted_or_decided_slot(),
            Some(8)
        );
        assert_eq!(
            reopened.load(8).unwrap().isr().first_current(),
            Some(&proposal)
        );
        assert_eq!(
            reopened.fetch_command(command.hash()).unwrap(),
            Some(command)
        );
        assert!(dir.path().join("recorder.wal").exists());
    }
}

#[test]
fn interrupted_record_before_wal_sync_never_acknowledges() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let value =
        AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);
    recorder
        .set_seal_fault(Some(SealFaultPoint::AfterWalWrite))
        .unwrap();

    assert!(matches!(
        recorder.record_proposal(record(
            8,
            proposal("writer", 1, 1, value),
            Some(command),
        )),
        Err(Error::Io(message)) if message.contains("AfterWalWrite")
    ));
    assert_eq!(
        recorder
            .configuration_state()
            .unwrap()
            .max_accepted_or_decided_slot(),
        None
    );
    assert_eq!(recorder.load(8).unwrap().isr().first_current(), None);
    assert!(matches!(
        recorder.record_proposal(record(
            8,
            proposal(
                "writer",
                2,
                1,
                AcceptedValue::from_command(
                    CLUSTER_ID,
                    8,
                    EPOCH,
                    CONFIG_ID,
                    LogHash::ZERO,
                    &StoredCommand::new(EntryType::Command, b"retry".to_vec()),
                ),
            ),
            Some(StoredCommand::new(EntryType::Command, b"retry".to_vec())),
        )),
        Err(Error::Io(_))
    ));
}

#[test]
fn wal_replays_recent_slots_before_cache_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let mut proposals = Vec::new();

    for slot in [8, 9] {
        let command = StoredCommand::new(EntryType::Command, format!("slot-{slot}").into_bytes());
        let value = AcceptedValue::from_command(
            CLUSTER_ID,
            slot,
            EPOCH,
            CONFIG_ID,
            LogHash::ZERO,
            &command,
        );
        let proposal = proposal("writer", slot, 1, value);
        recorder
            .record_proposal(record(slot, proposal.clone(), Some(command)))
            .unwrap();
        proposals.push((slot, proposal));
    }
    drop(recorder);

    assert!(proposals
        .iter()
        .all(|(slot, _)| !dir.path().join(format!("slot-{slot:020}.rec")).exists()));

    let reopened = store(dir.path(), membership).unwrap();
    for (slot, proposal) in proposals {
        assert_eq!(
            reopened.load(slot).unwrap().isr().first_current(),
            Some(&proposal)
        );
    }
    assert_eq!(
        reopened
            .configuration_state()
            .unwrap()
            .max_accepted_or_decided_slot(),
        Some(9)
    );
}

fn store(root: &std::path::Path, membership: Membership) -> Result<RecorderFileStore, Error> {
    RecorderFileStore::new_with_membership(root, "r1", CLUSTER_ID, EPOCH, CONFIG_ID, membership)
}

fn directory_contents(root: &std::path::Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut contents = fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), fs::read(entry.path()).unwrap())
        })
        .collect::<Vec<_>>();
    contents.sort_by(|left, right| left.0.cmp(&right.0));
    contents
}

fn record(slot: u64, proposal: Proposal, command: Option<StoredCommand>) -> RecordRequest {
    RecordRequest {
        cluster_id: CLUSTER_ID.into(),
        epoch: EPOCH,
        config_id: CONFIG_ID,
        config_digest: Membership::new(["r1", "r2", "r3"]).unwrap().digest(),
        slot,
        step: 4,
        proposal,
        command,
    }
}

fn proposal(proposer_id: &str, proposal_id: u64, priority: u64, value: AcceptedValue) -> Proposal {
    Proposal::new(
        ProposalPriority::from_u64(priority),
        proposer_id,
        proposal_id,
        value,
    )
}
