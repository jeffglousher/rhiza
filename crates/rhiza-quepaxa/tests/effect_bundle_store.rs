#![cfg(feature = "archive-gc")]
use rhiza_archive::{CheckpointIdentity, CheckpointReadbackCertificate, ObjectArchiveStore};
use rhiza_core::{
    EntryType, ExternalEffectCommand, ExternalEffectProfile, LogEntry, LogHash, StoredCommand,
    MAX_EXTERNAL_EFFECT_COMMAND_BYTES,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{
    AcceptedValue, DecisionProof, EffectBundleBinding, EffectBundleFinalizeRequest,
    EffectBundleGcPin, Error, Membership, Proposal, ProposalPriority, RecorderEffectBundle,
    RecorderFileStore, RecorderRpcContext, RecorderSummary, ThreeNodeConsensus,
};
use rhiza_sql::{QwalEffectManifestV4, QwalReceiptReferenceV4, StateIdentityV3};

const CLUSTER_ID: &str = "bundle-cluster";
const EPOCH: u64 = 7;
const CONFIG_ID: u64 = 9;

fn open_store(root: &std::path::Path) -> (RecorderFileStore, Membership) {
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    (
        RecorderFileStore::new_with_membership(
            root,
            "r1",
            CLUSTER_ID,
            EPOCH,
            CONFIG_ID,
            membership.clone(),
        )
        .unwrap(),
        membership,
    )
}

fn sql_qefx(
    membership: &Membership,
    chunks: Vec<Vec<u8>>,
    intended_slot: u64,
) -> (RecorderEffectBundle, EffectBundleFinalizeRequest) {
    let state = StateIdentityV3 {
        page_size: 512,
        page_count: 1,
        state_root: LogHash::digest(&[b"state"]),
    };
    let profile = QwalEffectManifestV4 {
        recovery_generation: 1,
        base_state: state,
        target_state: state,
        materializer_fingerprint: "cross-crate-qefx".into(),
        receipts: vec![QwalReceiptReferenceV4 {
            request_id: "request-1".into(),
            request_digest: LogHash::digest(&[b"request"]),
            result_offset: 0,
            result_len: 1,
            result_digest: LogHash::digest(&[b"profile-only-receipt"]),
        }],
    };
    let qefx = profile
        .external_command(
            CLUSTER_ID,
            EPOCH,
            CONFIG_ID,
            membership.digest(),
            intended_slot,
            LogHash::digest(&[b"previous"]),
            &chunks,
        )
        .unwrap();
    let stored = StoredCommand::new(EntryType::Command, qefx.encode().unwrap());
    let binding = EffectBundleBinding {
        cluster_id: qefx.cluster_id().into(),
        epoch: qefx.epoch(),
        config_id: qefx.config_id(),
        config_digest: qefx.config_digest(),
        intended_slot: qefx.intended_slot(),
        prev_hash: qefx.prev_hash(),
        manifest_command_hash: stored.hash(),
        effect_digest: qefx.effect_digest_value(),
    };
    let bundle = RecorderEffectBundle::new(binding, chunks).unwrap();
    let request = EffectBundleFinalizeRequest::new(bundle.clone(), stored).unwrap();
    (bundle, request)
}

fn large_qefx(
    membership: &Membership,
    profile_bytes: usize,
    intended_slot: u64,
) -> (RecorderEffectBundle, EffectBundleFinalizeRequest) {
    let chunks = vec![vec![0xa5]];
    let qefx = ExternalEffectCommand::from_profile_bytes_and_chunks(
        CLUSTER_ID,
        EPOCH,
        CONFIG_ID,
        membership.digest(),
        intended_slot,
        LogHash::digest(&[b"large-previous"]),
        ExternalEffectProfile::sql(vec![0x5a; profile_bytes]),
        &chunks,
    )
    .unwrap();
    let stored = StoredCommand::new(EntryType::Command, qefx.encode().unwrap());
    let binding = EffectBundleBinding {
        cluster_id: qefx.cluster_id().into(),
        epoch: qefx.epoch(),
        config_id: qefx.config_id(),
        config_digest: qefx.config_digest(),
        intended_slot: qefx.intended_slot(),
        prev_hash: qefx.prev_hash(),
        manifest_command_hash: stored.hash(),
        effect_digest: qefx.effect_digest_value(),
    };
    let bundle = RecorderEffectBundle::new(binding, chunks).unwrap();
    let request = EffectBundleFinalizeRequest::new(bundle.clone(), stored).unwrap();
    (bundle, request)
}

fn gc_certificate(
    root: &std::path::Path,
    cluster_id: &str,
    config_id: u64,
    config_digest: LogHash,
    through_slot: u64,
    recovery_generation: u64,
) -> CheckpointReadbackCertificate {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let archive_root = root.join(format!(
                "archive-{cluster_id}-{config_id}-{through_slot}-{recovery_generation}"
            ));
            let object_store = ObjStore::new(ObjStoreConfig::Local { root: archive_root }).unwrap();
            let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
                object_store,
                CheckpointIdentity::new(
                    cluster_id,
                    EPOCH,
                    config_id,
                    config_digest,
                    recovery_generation,
                ),
            );
            let mut previous = LogHash::ZERO;
            let entries = (1..=through_slot)
                .map(|index| {
                    let payload = format!("checkpoint-{index}").into_bytes();
                    let hash = LogEntry::calculate_hash(
                        cluster_id,
                        index,
                        EPOCH,
                        config_id,
                        EntryType::Command,
                        previous,
                        &payload,
                    );
                    let entry = LogEntry {
                        cluster_id: cluster_id.into(),
                        epoch: EPOCH,
                        config_id,
                        index,
                        entry_type: EntryType::Command,
                        payload,
                        prev_hash: previous,
                        hash,
                    };
                    previous = hash;
                    entry
                })
                .collect::<Vec<_>>();
            let loaded = archive.publish_committed(&entries).await.unwrap();
            archive
                .checkpoint_readback_certificate(&loaded)
                .await
                .unwrap()
        })
}

fn install_successor(store: &RecorderFileStore, current: &Membership, next: Membership) {
    let stop_slot = 200;
    let stop = rhiza_core::ConfigChange::bound_stop(
        CLUSTER_ID,
        CONFIG_ID,
        current.digest(),
        CONFIG_ID + 1,
        next.members().to_vec(),
    )
    .unwrap()
    .to_stored_command();
    let value = AcceptedValue::from_command(
        CLUSTER_ID,
        stop_slot,
        EPOCH,
        CONFIG_ID,
        LogHash::ZERO,
        &stop,
    );
    let proposal = Proposal::new(ProposalPriority::MAX, "r1", 1, value);
    let proof = DecisionProof::Phase2 {
        cluster_id: CLUSTER_ID.into(),
        slot: stop_slot,
        epoch: EPOCH,
        config_id: CONFIG_ID,
        config_digest: current.digest(),
        step: 6,
        summaries: current.members()[..current.quorum_size()]
            .iter()
            .map(|recorder_id| RecorderSummary {
                recorder_id: recorder_id.clone(),
                slot: stop_slot,
                step: 6,
                first_current: None,
                aggregate_prior: Some(proposal.clone()),
            })
            .collect(),
        proposal,
    };
    store.install_successor_from_proof(next, &proof).unwrap();
}

#[test]
fn sql_produced_qefx_finalizes_and_loads_at_the_recorder() {
    let root = tempfile::tempdir().unwrap();
    let (store, membership) = open_store(root.path());
    let (bundle, request) = sql_qefx(&membership, vec![b"x".to_vec(), b"y".to_vec()], 44);

    store.finalize_effect_bundle(&request).unwrap();
    store.finalize_effect_bundle(&request).unwrap();
    assert_eq!(
        store.load_effect_bundle(bundle.binding()).unwrap(),
        Some(bundle)
    );
}

#[test]
fn identical_effect_bytes_use_distinct_full_binding_manifest_keys() {
    let root = tempfile::tempdir().unwrap();
    let (store, membership) = open_store(root.path());
    let chunks = vec![b"shared-effect".to_vec(), b"shared-second-chunk".to_vec()];
    let (first, first_request) = sql_qefx(&membership, chunks.clone(), 44);
    let (second, second_request) = sql_qefx(&membership, chunks, 45);

    assert_eq!(
        first.binding().effect_digest,
        second.binding().effect_digest
    );
    assert_ne!(first.binding(), second.binding());

    store.finalize_effect_bundle(&first_request).unwrap();
    store.finalize_effect_bundle(&second_request).unwrap();
    // Exact retries remain idempotent even when the chunks are shared CAS.
    store.finalize_effect_bundle(&first_request).unwrap();
    store.finalize_effect_bundle(&second_request).unwrap();

    assert_eq!(
        store.load_effect_bundle(first.binding()).unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        store.load_effect_bundle(second.binding()).unwrap(),
        Some(second.clone())
    );

    let mut wrong_binding = first.binding().clone();
    wrong_binding.intended_slot = 46;
    assert_eq!(
        store.fetch_effect_bundle_manifest(&wrong_binding).unwrap(),
        None
    );
    assert_eq!(
        store.fetch_effect_bundle_chunk(&wrong_binding, 0).unwrap(),
        None
    );
    assert_eq!(store.load_effect_bundle(&wrong_binding).unwrap(), None);

    let names = std::fs::read_dir(root.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("effect-bundle-"))
        .collect::<Vec<_>>();
    assert_eq!(names.len(), 2);
    assert!(
        names
            .iter()
            .all(|name| !name.contains(&first.binding().effect_digest.to_hex())),
        "the clean-break store must not fall back to the legacy effect-digest filename"
    );
}

#[test]
fn qefx_tampering_and_context_mismatch_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let (_store, membership) = open_store(root.path());
    let (bundle, request) = sql_qefx(&membership, vec![b"payload".to_vec()], 44);

    let mut tampered = request.manifest_command.clone();
    tampered.payload[0] ^= 1;
    assert!(matches!(
        EffectBundleFinalizeRequest::new(bundle.clone(), tampered),
        Err(Error::EffectBundleInvalid(_))
    ));

    let (_, wrong_context) = sql_qefx(&membership, vec![b"payload".to_vec()], 45);
    assert!(matches!(
        EffectBundleFinalizeRequest::new(bundle, wrong_context.manifest_command),
        Err(Error::EffectBundleInvalid(_))
    ));
}

#[test]
fn recorder_finalizes_reopens_and_retries_manifest_larger_than_legacy_limit() {
    let root = tempfile::tempdir().unwrap();
    let (store, membership) = open_store(root.path());
    let (bundle, request) = large_qefx(&membership, 20 * 1024, 61);
    assert!(request.manifest_command.payload.len() > 16 * 1024);

    store.finalize_effect_bundle(&request).unwrap();
    store.finalize_effect_bundle(&request).unwrap();
    drop(store);

    let (reopened, _) = open_store(root.path());
    assert_eq!(
        reopened.load_effect_bundle(bundle.binding()).unwrap(),
        Some(bundle)
    );
}

#[test]
fn recorder_finalizes_reopens_and_retries_near_cap_manifest() {
    let root = tempfile::tempdir().unwrap();
    let (store, membership) = open_store(root.path());
    let (bundle, request) = large_qefx(&membership, MAX_EXTERNAL_EFFECT_COMMAND_BYTES - 4096, 62);
    assert!(request.manifest_command.payload.len() > MAX_EXTERNAL_EFFECT_COMMAND_BYTES - 8192);
    assert!(request.manifest_command.payload.len() <= MAX_EXTERNAL_EFFECT_COMMAND_BYTES);

    store.finalize_effect_bundle(&request).unwrap();
    store.finalize_effect_bundle(&request).unwrap();
    drop(store);

    let (reopened, _) = open_store(root.path());
    assert_eq!(
        reopened.load_effect_bundle(bundle.binding()).unwrap(),
        Some(bundle)
    );
}

#[test]
fn oversized_manifest_is_rejected_before_effect_chunk_mutation() {
    let root = tempfile::tempdir().unwrap();
    let (store, membership) = open_store(root.path());
    let (bundle, _) = large_qefx(&membership, 20 * 1024, 63);
    let request = EffectBundleFinalizeRequest {
        bundle,
        manifest_command: StoredCommand::new(
            EntryType::Command,
            vec![0_u8; MAX_EXTERNAL_EFFECT_COMMAND_BYTES + 1],
        ),
    };

    assert!(matches!(
        store.finalize_effect_bundle(&request),
        Err(Error::EffectBundleInvalid(_))
    ));
    assert!(std::fs::read_dir(root.path()).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with("effect-chunk-")));
}

#[test]
fn consensus_finalizes_and_resolves_qefx_from_recorder_quorum() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let consensus = ThreeNodeConsensus::new(
        CLUSTER_ID,
        "n1",
        EPOCH,
        CONFIG_ID,
        ["n1", "n2", "n3"].map(|id| root.path().join(id)),
    )
    .unwrap();
    let (bundle, request) = sql_qefx(
        &membership,
        vec![b"chunk-a".to_vec(), b"chunk-b".to_vec()],
        70,
    );
    let context = RecorderRpcContext::default_timeout();

    consensus
        .finalize_effect_bundle_on_quorum(&context, &request)
        .unwrap();
    consensus
        .finalize_effect_bundle_on_quorum(&context, &request)
        .unwrap();
    assert_eq!(
        consensus
            .resolve_effect_bundle_from_quorum(
                &context,
                bundle.binding(),
                &request.manifest_command
            )
            .unwrap(),
        Some(bundle)
    );
}

#[test]
fn consensus_resolves_same_effect_at_distinct_bindings_without_conflict() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let consensus = ThreeNodeConsensus::new(
        CLUSTER_ID,
        "n1",
        EPOCH,
        CONFIG_ID,
        ["n1", "n2", "n3"].map(|id| root.path().join(id)),
    )
    .unwrap();
    let chunks = vec![b"shared-quorum-chunk".to_vec()];
    let (first, first_request) = sql_qefx(&membership, chunks.clone(), 71);
    let (second, second_request) = sql_qefx(&membership, chunks, 72);
    let context = RecorderRpcContext::default_timeout();

    assert_eq!(
        first.binding().effect_digest,
        second.binding().effect_digest
    );
    consensus
        .finalize_effect_bundle_on_quorum(&context, &first_request)
        .unwrap();
    consensus
        .finalize_effect_bundle_on_quorum(&context, &second_request)
        .unwrap();

    assert_eq!(
        consensus
            .resolve_effect_bundle_from_quorum(
                &context,
                first.binding(),
                &first_request.manifest_command,
            )
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        consensus
            .resolve_effect_bundle_from_quorum(
                &context,
                second.binding(),
                &second_request.manifest_command,
            )
            .unwrap(),
        Some(second)
    );
}

#[test]
fn certified_gc_persists_monotonically_and_preserves_newer_effects_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let (store, membership) = open_store(root.path());
    let (old_bundle, old_request) = sql_qefx(&membership, vec![b"old-effect".to_vec()], 80);
    let (new_bundle, new_request) = sql_qefx(&membership, vec![b"new-effect".to_vec()], 81);
    store.finalize_effect_bundle(&old_request).unwrap();
    store.finalize_effect_bundle(&new_request).unwrap();

    let certificate = gc_certificate(
        root.path(),
        CLUSTER_ID,
        CONFIG_ID,
        membership.digest(),
        80,
        1,
    );
    let outcome = store
        .advance_effect_bundle_gc_anchor(&certificate, &[])
        .unwrap();
    assert_eq!(outcome.previous_anchor, None);
    assert_eq!(outcome.current_anchor, 80);
    assert_eq!(outcome.removed_manifests, 1);
    assert_eq!(
        store.load_effect_bundle(old_bundle.binding()).unwrap(),
        None
    );
    assert_eq!(
        store.load_effect_bundle(new_bundle.binding()).unwrap(),
        Some(new_bundle.clone())
    );
    assert_eq!(
        store
            .advance_effect_bundle_gc_anchor(&certificate, &[])
            .unwrap()
            .previous_anchor,
        Some(80),
        "exact certificate retry is idempotent"
    );
    drop(store);

    let (reopened, _) = open_store(root.path());
    assert_eq!(reopened.effect_bundle_gc_anchor().unwrap(), Some(80));
    assert_eq!(
        reopened.load_effect_bundle(old_bundle.binding()).unwrap(),
        None
    );
    assert_eq!(
        reopened.load_effect_bundle(new_bundle.binding()).unwrap(),
        Some(new_bundle)
    );
}

#[test]
fn certified_gc_bounded_sweep_requires_exact_retries_and_caps_each_slice() {
    let root = tempfile::tempdir().unwrap();
    let (store, membership) = open_store(root.path());
    let (first, first_request) = sql_qefx(&membership, vec![b"first-effect".to_vec()], 79);
    let (second, second_request) = sql_qefx(&membership, vec![b"second-effect".to_vec()], 80);
    store.finalize_effect_bundle(&first_request).unwrap();
    store.finalize_effect_bundle(&second_request).unwrap();
    let certificate = gc_certificate(
        root.path(),
        CLUSTER_ID,
        CONFIG_ID,
        membership.digest(),
        80,
        1,
    );

    let first_slice = store
        .advance_effect_bundle_gc_anchor_bounded(&certificate, &[], 1)
        .unwrap();
    assert_eq!(first_slice.removed_manifests, 1);
    assert!(first_slice.removed_chunks <= 1);
    assert!(!first_slice.sweep_complete);

    let second_slice = store
        .advance_effect_bundle_gc_anchor_bounded(&certificate, &[], 1)
        .unwrap();
    assert_eq!(second_slice.removed_manifests, 1);
    assert!(second_slice.removed_chunks <= 1);

    let final_slice = store
        .advance_effect_bundle_gc_anchor_bounded(&certificate, &[], 1)
        .unwrap();
    assert!(final_slice.sweep_complete);
    assert_eq!(store.load_effect_bundle(first.binding()).unwrap(), None);
    assert_eq!(store.load_effect_bundle(second.binding()).unwrap(), None);
}

#[test]
fn certified_gc_does_not_reap_chunks_between_stage_and_finalize() {
    let root = tempfile::tempdir().unwrap();
    let (store, membership) = open_store(root.path());
    let (bundle, request) = sql_qefx(&membership, vec![b"inflight-stage".to_vec()], 81);
    store
        .stage_effect_bundle_chunk(
            bundle.binding(),
            &request.manifest_command,
            0,
            &bundle.chunks()[0],
        )
        .unwrap();

    let certificate = gc_certificate(
        root.path(),
        CLUSTER_ID,
        CONFIG_ID,
        membership.digest(),
        80,
        1,
    );
    store
        .advance_effect_bundle_gc_anchor(&certificate, &[])
        .unwrap();

    store
        .finalize_staged_effect_bundle(bundle.binding(), request.manifest_command.clone())
        .unwrap();
    assert_eq!(
        store.load_effect_bundle(bundle.binding()).unwrap(),
        Some(bundle)
    );
}

#[test]
fn certified_gc_rejects_uncertified_rollback_and_foreign_identity() {
    let root = tempfile::tempdir().unwrap();
    let (store, membership) = open_store(root.path());
    let (_bundle, request) = sql_qefx(&membership, vec![b"retain-on-error".to_vec()], 90);
    store.finalize_effect_bundle(&request).unwrap();

    let certificate = gc_certificate(
        root.path(),
        CLUSTER_ID,
        CONFIG_ID,
        membership.digest(),
        90,
        1,
    );
    store
        .advance_effect_bundle_gc_anchor(&certificate, &[])
        .unwrap();
    let rollback = gc_certificate(
        root.path(),
        CLUSTER_ID,
        CONFIG_ID,
        membership.digest(),
        89,
        1,
    );
    assert!(matches!(
        store.advance_effect_bundle_gc_anchor(&rollback, &[]),
        Err(Error::EffectBundleInvalid(_))
    ));
    let foreign = gc_certificate(
        root.path(),
        "foreign-cluster",
        CONFIG_ID,
        membership.digest(),
        91,
        1,
    );
    assert!(matches!(
        store.advance_effect_bundle_gc_anchor(&foreign, &[]),
        Err(Error::EffectBundleInvalid(_))
    ));
}

#[test]
fn certified_gc_rejects_same_slot_conflicts_and_allows_real_configuration_transition() {
    let root = tempfile::tempdir().unwrap();
    let (store, membership) = open_store(root.path());

    let first = gc_certificate(
        root.path(),
        CLUSTER_ID,
        CONFIG_ID,
        membership.digest(),
        102,
        1,
    );
    store.advance_effect_bundle_gc_anchor(&first, &[]).unwrap();
    let conflicting = gc_certificate(
        root.path(),
        CLUSTER_ID,
        CONFIG_ID,
        membership.digest(),
        102,
        2,
    );
    assert!(matches!(
        store.advance_effect_bundle_gc_anchor(&conflicting, &[]),
        Err(Error::EffectBundleInvalid(_))
    ));

    let next = Membership::new(["r1", "r2", "r4"]).unwrap();
    install_successor(&store, &membership, next.clone());
    let configuration = store.configuration_state().unwrap();
    assert_eq!(configuration.config_id(), CONFIG_ID + 1);
    assert_eq!(configuration.config_digest(), next.digest());

    let after_transition = gc_certificate(
        root.path(),
        CLUSTER_ID,
        CONFIG_ID + 1,
        next.digest(),
        103,
        1,
    );
    assert_eq!(
        store
            .advance_effect_bundle_gc_anchor(&after_transition, &[])
            .unwrap()
            .previous_anchor,
        Some(102)
    );
}

#[test]
fn certified_gc_preserves_active_and_reconfiguration_pins_under_anchor() {
    let root = tempfile::tempdir().unwrap();
    let (store, membership) = open_store(root.path());
    let (active, active_request) = sql_qefx(&membership, vec![b"pinned-effect".to_vec()], 100);
    let (swept, swept_request) = sql_qefx(&membership, vec![b"swept-effect".to_vec()], 101);
    store.finalize_effect_bundle(&active_request).unwrap();
    store.finalize_effect_bundle(&swept_request).unwrap();
    let certificate = gc_certificate(
        root.path(),
        CLUSTER_ID,
        CONFIG_ID,
        membership.digest(),
        101,
        1,
    );
    let pins = vec![EffectBundleGcPin {
        binding: active.binding().clone(),
        manifest_command: active_request.manifest_command.clone(),
    }];

    let outcome = store
        .advance_effect_bundle_gc_anchor(&certificate, &pins)
        .unwrap();
    assert_eq!(outcome.removed_manifests, 1);
    assert_eq!(
        store.load_effect_bundle(active.binding()).unwrap(),
        Some(active)
    );
    assert_eq!(store.load_effect_bundle(swept.binding()).unwrap(), None);
}
