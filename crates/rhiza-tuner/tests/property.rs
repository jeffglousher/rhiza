use proptest::prelude::*;
use rhiza_tuner::types::*;
use rhiza_tuner::*;

fn identity_strategy() -> impl Strategy<Value = Identity> {
    (".*", 0u64..100, 0u64..100, any::<[u8; 32]>(), 0u64..100).prop_map(
        |(cluster_id, epoch, config_id, membership_digest, recovery_generation)| Identity {
            cluster_id,
            epoch,
            config_id,
            membership_digest,
            recovery_generation,
        },
    )
}

fn node_id_strategy() -> impl Strategy<Value = NodeId> {
    "[a-z0-9-]{1,10}".prop_map(NodeId::from)
}

fn hedge_delay_strategy() -> impl Strategy<Value = HedgeDelayBucket> {
    prop_oneof![
        Just(HedgeDelayBucket::Ms5),
        Just(HedgeDelayBucket::Ms10),
        Just(HedgeDelayBucket::Ms25),
        Just(HedgeDelayBucket::Ms50),
        Just(HedgeDelayBucket::Ms100),
        Just(HedgeDelayBucket::Static),
    ]
}

fn candidate_set_strategy() -> impl Strategy<Value = CandidateSet> {
    (
        identity_strategy(),
        prop::collection::vec(node_id_strategy(), 1..5),
        prop::collection::vec(hedge_delay_strategy(), 1..4),
    )
        .prop_map(|(identity, voters, delays)| {
            // Deduplicate voters
            let mut voters = voters;
            voters.sort();
            voters.dedup();
            if voters.is_empty() {
                voters.push("node-0".into());
            }
            CandidateSet {
                identity,
                eligible_voters: voters,
                hedge_delay_allowlist: delays,
                static_hedge_delay_ms: 100,
            }
        })
}

proptest! {
    #[test]
    fn safety_boundary_never_accepts_ineligible_proposer(
        candidate_set in candidate_set_strategy(),
        bad_proposer in "[a-z]{1,5}".prop_map(NodeId::from),
    ) {
        // Ensure bad_proposer is not in the eligible set
        prop_assume!(!candidate_set.eligible_voters.contains(&bad_proposer));

        let safety = SafetyBoundary::new(SafetyConfig::default());
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let output = ActionOutput {
            action: Action {
                first_request_target: bad_proposer,
                hedge_delay: HedgeDelayBucket::Static,
            },
            identity: candidate_set.identity.clone(),
            valid_from_slot: 1,
            expiry_us: now_us + 10_000_000,
            policy_version: 1,
            model_version: 1,
            exploration: false,
            confidence: 0.9,
            fallback_reason: None,
        };

        let result = safety.validate(&output, &candidate_set, &candidate_set.identity);
        prop_assert!(result.is_err());
    }

    #[test]
    fn static_fallback_always_uses_eligible_voter(
        candidate_set in candidate_set_strategy(),
    ) {
        let safety = SafetyBoundary::new(SafetyConfig::default());
        let fallback = safety.static_fallback(&candidate_set);
        prop_assert!(candidate_set.eligible_voters.contains(&fallback.action.first_request_target));
    }

    #[test]
    fn reward_pipeline_produces_finite_values(
        latency_us in 0u64..10_000_000u64,
        rpcs in 0u32..100u32,
        dup_work in any::<bool>(),
        contention in any::<bool>(),
    ) {
        let mut pipeline = RewardPipeline::new(RewardConfig::default());
        let outcome = TerminalOutcome::Success {
            decision_latency_us: latency_us,
            additional_rpcs: rpcs,
            duplicate_proposer_work: dup_work,
            contention_or_round_escalation: contention,
        };
        let reward = pipeline.compute(&outcome);
        prop_assert!(reward.total.is_finite());
        prop_assert!(reward.normalized_latency.is_finite());
    }

    #[test]
    fn bandit_output_references_eligible_voters(
        candidate_set in candidate_set_strategy(),
    ) {
        let mut bandit = ContextualBandit::new(BanditConfig::default());
        let features = FeatureVector {
            identity: candidate_set.identity.clone(),
            epoch: candidate_set.identity.epoch,
            voter_count: candidate_set.eligible_voters.len() as u32,
            eligible_proposers: candidate_set.eligible_voters.clone(),
            proposer_stats: candidate_set.eligible_voters.iter().map(|p| (p.clone(), ProposerStats::default())).collect(),
            rpc_stats: RpcStats::default(),
            node_pressure: NodePressure::default(),
            request_class: RequestClass {
                durability: DurabilityMode::Sync,
                size: SizeBucket::Small,
            },
            feature_age_us: 100,
            sample_count: 100,
            missingness_flags: MissingnessFlags::default(),
        };

        let output = bandit.select_action(&features, &candidate_set, false);
        prop_assert!(candidate_set.eligible_voters.contains(&output.action.first_request_target));
        prop_assert!(candidate_set.hedge_delay_allowlist.contains(&output.action.hedge_delay));
    }

    #[test]
    fn bandit_output_references_eligible_hedge_delays(
        candidate_set in candidate_set_strategy(),
    ) {
        let mut bandit = ContextualBandit::new(BanditConfig::default());
        let features = FeatureVector {
            identity: candidate_set.identity.clone(),
            epoch: candidate_set.identity.epoch,
            voter_count: candidate_set.eligible_voters.len() as u32,
            eligible_proposers: candidate_set.eligible_voters.clone(),
            proposer_stats: candidate_set.eligible_voters.iter().map(|p| (p.clone(), ProposerStats::default())).collect(),
            rpc_stats: RpcStats::default(),
            node_pressure: NodePressure::default(),
            request_class: RequestClass {
                durability: DurabilityMode::Sync,
                size: SizeBucket::Small,
            },
            feature_age_us: 100,
            sample_count: 100,
            missingness_flags: MissingnessFlags::default(),
        };

        let output = bandit.select_action(&features, &candidate_set, false);
        prop_assert!(candidate_set.hedge_delay_allowlist.contains(&output.action.hedge_delay));
    }

    #[test]
    fn identity_digest_is_deterministic(
        identity in identity_strategy(),
    ) {
        let d1 = identity.digest();
        let d2 = identity.digest();
        prop_assert_eq!(d1, d2);
    }

    #[test]
    fn different_identities_produce_different_digests(
        identity1 in identity_strategy(),
        identity2 in identity_strategy(),
    ) {
        prop_assume!(identity1 != identity2);
        prop_assert_ne!(identity1.digest(), identity2.digest());
    }
}
