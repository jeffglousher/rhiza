//! Caller-owned QuePaxa drive (no per-recorder OS worker threads).
//!
//! Record, install, fetch, and inspect RPCs run on the calling thread through
//! [`crate::RecorderRpc`]. This is the Taldra lab path and the intended
//! upstream-shaped alternative to [`crate::ThreeNodeConsensus`]. Shared error
//! and predecessor helpers live in [`crate::proposer_drive`].

use std::{
    collections::BTreeSet,
    fmt,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
};

use crate::{
    check_operation_context, check_proposal_operation_context, proof_cluster_id, proof_context,
    proposal_exact, proposer_drive, stored_command, AcceptedValue, ClusterId, Command, ConfigId,
    Consensus, ControlCallBudget, DecisionProof, DriveOutcome, EntryType, Epoch, Error, LogEntry,
    LogHash, LogIndex, Membership, NodeId, OsPrioritySource, PrioritySource, Proposal,
    ProposalPriority, ProposerProgress, RecordRequest, RecordSummary, RecorderFileStore,
    RecorderRpc, RecorderRpcContext, RecorderSummary, RejectReason, Result, RpcCallBudget,
    SingleNodeState, Slot, StoredCommand,
};
/// QuePaxa consensus driven entirely on the calling thread.
///
/// Unlike [`crate::ThreeNodeConsensus`], construction does **not** spawn
/// recorder or control OS threads. Record, install, fetch, and
/// inspect RPCs are dispatched synchronously through [`RecorderRpc`] on the
/// caller. Ambiguous cancel/deadline after a mutation may have started maps to
/// [`Error::UnknownOutcome`], matching the worker-path contract.
pub struct CallerOwnedConsensus {
    cluster_id: ClusterId,
    proposer_id: NodeId,
    epoch: Epoch,
    config_id: ConfigId,
    config_digest: LogHash,
    membership: Membership,
    recorders: Vec<Arc<dyn RecorderRpc>>,
    priority_source: Arc<dyn PrioritySource>,
    proposal_sequence: AtomicU64,
    sequential_tip: Mutex<SingleNodeState>,
}

impl fmt::Debug for CallerOwnedConsensus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallerOwnedConsensus")
            .field("cluster_id", &self.cluster_id)
            .field("proposer_id", &self.proposer_id)
            .field("epoch", &self.epoch)
            .field("config_id", &self.config_id)
            .field("recorders", &self.membership.members())
            .finish_non_exhaustive()
    }
}

impl CallerOwnedConsensus {
    pub const fn membership(&self) -> &Membership {
        &self.membership
    }

    pub fn new(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorder_roots: [PathBuf; 3],
    ) -> Result<Self> {
        Self::from_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorder_roots,
            1,
            LogHash::ZERO,
        )
    }

    fn from_recovered_tip(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorder_roots: [PathBuf; 3],
        next_index: LogIndex,
        last_hash: LogHash,
    ) -> Result<Self> {
        let cluster_id = cluster_id.into();
        let recorder_roots: Vec<_> = recorder_roots.into_iter().collect();
        let recorder_ids: Vec<_> = recorder_roots
            .iter()
            .map(|root| {
                root.file_name()
                    .and_then(|name| name.to_str())
                    .filter(|name| !name.is_empty())
                    .unwrap_or("recorder")
                    .to_owned()
            })
            .collect();
        let membership = Membership::from_voters(recorder_ids.iter().cloned())?;
        let recorders = recorder_roots
            .into_iter()
            .zip(recorder_ids)
            .map(|(root, recorder_id)| -> Result<(NodeId, Box<dyn RecorderRpc>)> {
                Ok((
                    recorder_id.clone(),
                    Box::new(RecorderFileStore::new_with_membership(
                        root,
                        recorder_id,
                        cluster_id.clone(),
                        epoch,
                        config_id,
                        membership.clone(),
                    )?) as Box<dyn RecorderRpc>,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Self::from_recorders_with_ids_and_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorders,
            next_index,
            last_hash,
        )
    }

    /// Constructs a caller-owned proposer from expected recorder identities.
    ///
    /// This path does not spawn recorder or control workers and does not issue
    /// `Identity` RPCs. Reply identities are still checked against the
    /// corresponding expected identity on every call that returns one.
    pub fn from_recorders_with_ids_and_recovered_tip(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        mut recorders: Vec<(NodeId, Box<dyn RecorderRpc>)>,
        next_index: LogIndex,
        last_hash: LogHash,
    ) -> Result<Self> {
        if next_index == 0 {
            return Err(Error::InvalidRecoveredTip);
        }
        recorders.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let (recorder_ids, recorders): (Vec<_>, Vec<_>) = recorders.into_iter().unzip();
        let recorders: Vec<Arc<dyn RecorderRpc>> = recorders.into_iter().map(Arc::from).collect();
        let membership = Membership::from_members(recorder_ids)?;
        let config_digest = membership.digest();
        Ok(Self {
            cluster_id: cluster_id.into(),
            proposer_id: proposer_id.into(),
            epoch,
            config_id,
            config_digest,
            membership,
            recorders,
            priority_source: Arc::new(OsPrioritySource),
            proposal_sequence: AtomicU64::new(1),
            sequential_tip: Mutex::new(SingleNodeState {
                next_index,
                last_hash,
            }),
        })
    }

    /// Proposes with a caller-owned RPC deadline and cancellation signal.
    ///
    /// Tip advancement is serialized by an internal mutex, matching
    /// [`ThreeNodeConsensus::propose`]. A deadline after any recorder may have
    /// accepted the value returns [`Error::UnknownOutcome`].
    pub fn propose(&self, context: RecorderRpcContext, command: Command) -> Result<LogEntry> {
        self.propose_sequential(context, command)
    }

    fn propose_sequential(&self, context: RecorderRpcContext, command: Command) -> Result<LogEntry> {
        context.check()?;
        let mut tip = self
            .sequential_tip
            .lock()
            .map_err(|_| Error::ProposeFailed)?;
        let entry = self.propose_stored_at_until(
            tip.next_index,
            tip.last_hash,
            stored_command(command)?,
            &context,
            || context.check(),
        )?;
        tip.next_index = entry.index + 1;
        tip.last_hash = entry.hash;
        Ok(entry)
    }

    pub fn drive(
        &self,
        context: &RecorderRpcContext,
        progress: ProposerProgress,
    ) -> Result<DriveOutcome> {
        let mutation_started = AtomicBool::new(false);
        self.drive_inner(progress, context, &mutation_started)
    }

    pub fn inspect_decision_proof_at(
        &self,
        context: &RecorderRpcContext,
        slot: Slot,
    ) -> Result<Option<DecisionProof>> {
        let budget = ControlCallBudget::new(context)?;
        self.inspect_decision_proof_with_budget(&budget, slot)
    }

    fn propose_stored_at_until<F>(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        offered_command: StoredCommand,
        context: &RecorderRpcContext,
        cancelled: F,
    ) -> Result<LogEntry>
    where
        F: Fn() -> Result<()>,
    {
        let mutation_started = AtomicBool::new(false);
        check_proposal_operation_context(context, &mutation_started, &cancelled)?;
        let offered_value = AcceptedValue::from_command(
            &self.cluster_id,
            slot,
            self.epoch,
            self.config_id,
            prev_hash,
            &offered_command,
        );
        let proposal_id = self.proposal_sequence.fetch_add(1, Ordering::Relaxed);
        let mut progress = ProposerProgress::new(
            slot,
            Proposal::new(
                ProposalPriority::MAX,
                self.proposer_id.clone(),
                proposal_id,
                offered_value,
            ),
        )
        .with_command(offered_command.clone());
        loop {
            check_proposal_operation_context(context, &mutation_started, &cancelled)?;
            match self.drive_inner(progress, context, &mutation_started)? {
                DriveOutcome::Progress(next) => progress = next,
                DriveOutcome::Pending(next) => {
                    progress = next;
                    thread::sleep(std::time::Duration::from_millis(10));
                }
                DriveOutcome::Decision(proof) => {
                    let value = proof
                        .proposal()
                        .value
                        .as_ref()
                        .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
                    self.ensure_predecessor(slot, prev_hash, value.prev_hash)?;
                    let command = if self.command_matches_value(slot, value, &offered_command) {
                        offered_command.clone()
                    } else {
                        self.fetch_verified_value(slot, value, context, &mutation_started)?
                            .ok_or(Error::CommandUnavailable)?
                    };
                    return self.log_entry_from_value(slot, command, value);
                }
            }
        }
    }

    fn drive_inner(
        &self,
        mut progress: ProposerProgress,
        context: &RecorderRpcContext,
        mutation_started: &AtomicBool,
    ) -> Result<DriveOutcome> {
        check_operation_context(context, mutation_started)?;
        self.ensure_progress_command(&mut progress, context, mutation_started)?;
        let round = progress.step / 4;
        let phase = progress.step % 4;
        if phase == 0 {
            progress
                .phase_zero_priorities
                .retain(|(cached_round, _), _| *cached_round == round);
        } else {
            progress.phase_zero_priorities.clear();
        }
        let command_targets: BTreeSet<_> = self
            .membership
            .members()
            .iter()
            .filter(|recorder_id| !progress.command_holders.contains(*recorder_id))
            .cloned()
            .collect();
        let requests: Vec<_> = self
            .membership
            .members()
            .iter()
            .map(|recorder_id| -> Result<RecordRequest> {
                let mut proposal = progress.proposal.clone();
                if phase == 0 {
                    proposal.priority =
                        if progress.step == 4 && self.proposer_id == self.membership.members()[0] {
                            ProposalPriority::MAX
                        } else {
                            match progress
                                .phase_zero_priorities
                                .entry((round, recorder_id.clone()))
                            {
                                std::collections::btree_map::Entry::Occupied(entry) => *entry.get(),
                                std::collections::btree_map::Entry::Vacant(entry) => {
                                    *entry.insert(self.priority_source.sample(
                                        progress.slot,
                                        round,
                                        &self.proposer_id,
                                        recorder_id,
                                    )?)
                                }
                            }
                        };
                }
                Ok(RecordRequest {
                    cluster_id: self.cluster_id.clone(),
                    epoch: self.epoch,
                    config_id: self.config_id,
                    config_digest: self.config_digest,
                    slot: progress.slot,
                    step: progress.step,
                    proposal,
                    command: command_targets
                        .contains(recorder_id)
                        .then(|| progress.command.clone())
                        .flatten(),
                })
            })
            .collect::<Result<_>>()?;
        let mut replies =
            self.record_broadcast_with_context(requests, context.clone(), mutation_started)?;
        progress.command_holders.extend(
            replies
                .iter()
                .filter(|reply| command_targets.contains(&reply.recorder_id))
                .map(|reply| reply.recorder_id.clone()),
        );
        for reply in &replies {
            if let Some(proof) = &reply.decided {
                if proof_cluster_id(proof) != self.cluster_id {
                    return Err(Error::Rejected(RejectReason::WrongCluster));
                }
                proof
                    .validate_for_cluster(
                        &self.cluster_id,
                        progress.slot,
                        self.epoch,
                        self.config_id,
                        &self.membership,
                    )
                    .map_err(Error::Rejected)?;
                return self.finish_decision_with_context(
                    proof.clone(),
                    progress.command.as_ref(),
                    context,
                    mutation_started,
                );
            }
        }
        if let Some(highest) = replies.iter().map(|reply| reply.step).max() {
            if highest > progress.step {
                let caught_up = replies
                    .iter()
                    .filter(|reply| reply.step == highest)
                    .min_by(|left, right| left.recorder_id.cmp(&right.recorder_id))
                    .expect("highest reply exists");
                progress.step = highest;
                if let Some(proposal) = &caught_up.first_current {
                    progress.proposal = proposal.clone();
                }
                self.ensure_progress_command(&mut progress, context, mutation_started)?;
                progress.phase_zero_priorities.clear();
                return Ok(DriveOutcome::Progress(progress));
            }
        }
        replies.retain(|reply| reply.step == progress.step);
        replies.sort_by(|left, right| left.recorder_id.cmp(&right.recorder_id));
        replies.dedup_by(|left, right| left.recorder_id == right.recorder_id);
        if replies.len() < self.membership.quorum_size() {
            return Ok(DriveOutcome::Pending(progress));
        }
        replies.truncate(self.membership.quorum_size());
        let summaries: Vec<_> = replies
            .iter()
            .map(|reply| RecorderSummary {
                recorder_id: reply.recorder_id.clone(),
                slot: reply.slot,
                step: reply.step,
                first_current: reply.first_current.clone(),
                aggregate_prior: reply.aggregate_prior.clone(),
            })
            .collect();
        match phase {
            0 => {
                let fast_proposal = summaries
                    .first()
                    .and_then(|summary| summary.first_current.as_ref())
                    .filter(|proposal| proposal.priority == ProposalPriority::MAX)
                    .filter(|proposal| {
                        progress.step == 4
                            && summaries.iter().all(|summary| {
                                summary
                                    .first_current
                                    .as_ref()
                                    .is_some_and(|candidate| proposal_exact(candidate, proposal))
                            })
                    })
                    .cloned();
                if let Some(proposal) = fast_proposal {
                    let proof = DecisionProof::FastPath {
                        cluster_id: self.cluster_id.clone(),
                        slot: progress.slot,
                        epoch: self.epoch,
                        config_id: self.config_id,
                        config_digest: self.config_digest,
                        proposal,
                        summaries,
                    };
                    return self.finish_decision_with_context(
                        proof,
                        progress.command.as_ref(),
                        context,
                        mutation_started,
                    );
                }
                progress.proposal = replies
                    .iter()
                    .filter_map(|reply| reply.first_current.clone())
                    .max()
                    .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
            }
            1 => {}
            2 => {
                let maximum = replies
                    .iter()
                    .filter_map(|reply| reply.aggregate_prior.clone())
                    .max();
                if maximum.as_ref() == Some(&progress.proposal) {
                    let proof = DecisionProof::Phase2 {
                        cluster_id: self.cluster_id.clone(),
                        slot: progress.slot,
                        epoch: self.epoch,
                        config_id: self.config_id,
                        config_digest: self.config_digest,
                        step: progress.step,
                        proposal: progress.proposal.clone(),
                        summaries,
                    };
                    return self.finish_decision_with_context(
                        proof,
                        progress.command.as_ref(),
                        context,
                        mutation_started,
                    );
                }
            }
            3 => {
                progress.proposal = replies
                    .iter()
                    .filter_map(|reply| reply.aggregate_prior.clone())
                    .max()
                    .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
            }
            _ => unreachable!("phase is step modulo four"),
        }
        self.ensure_progress_command(&mut progress, context, mutation_started)?;
        progress.step = progress.step.checked_add(1).ok_or(Error::ProposeFailed)?;
        progress.phase_zero_priorities.clear();
        Ok(DriveOutcome::Progress(progress))
    }

    fn finish_decision_with_context(
        &self,
        proof: DecisionProof,
        known_command: Option<&StoredCommand>,
        context: &RecorderRpcContext,
        mutation_started: &AtomicBool,
    ) -> Result<DriveOutcome> {
        proof
            .validate_for_cluster(
                &self.cluster_id,
                proof_context(&proof).0,
                self.epoch,
                self.config_id,
                &self.membership,
            )
            .map_err(Error::Rejected)?;
        let value = proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        let mut control_budget = None;
        let command = match known_command {
            Some(command)
                if self.command_matches_value(proof_context(&proof).0, value, command) =>
            {
                command.clone()
            }
            _ => {
                let budget = Self::finish_control_budget(&mut control_budget, context, mutation_started)?;
                self.fetch_verified_value_with_budget(
                    budget,
                    proof_context(&proof).0,
                    value,
                    mutation_started,
                )?
                .ok_or(Error::CommandUnavailable)?
            }
        };
        let budget = Self::finish_control_budget(&mut control_budget, context, mutation_started)?;
        if let Err(error) =
            self.install_decision_proof_quorum_with_budget(budget, proof.clone(), mutation_started)
        {
            if Self::is_control_safety_error(&error)
                || matches!(
                    error,
                    Error::TypedProofInstallRequired | Error::TypedRecordRequired
                )
            {
                return Err(error);
            }
            return self.reconcile_post_decision_unknown_outcome(
                budget,
                mutation_started,
                &proof,
                &command,
            );
        }
        Ok(DriveOutcome::Decision(proof))
    }

    fn reconcile_post_decision_unknown_outcome(
        &self,
        budget: &ControlCallBudget,
        mutation_started: &AtomicBool,
        proof: &DecisionProof,
        offered_command: &StoredCommand,
    ) -> Result<DriveOutcome> {
        let slot = proof_context(proof).0;
        let value = proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        match self.inspect_decision_proof_with_budget(budget, slot) {
            Ok(Some(found))
                if found.proposal().value.as_ref() == Some(value)
                    && self.command_matches_value(slot, value, offered_command) =>
            {
                Ok(DriveOutcome::Decision(proof.clone()))
            }
            Ok(Some(_)) => Err(Error::ConflictingCertificates),
            Ok(None) => Err(Error::UnknownOutcome),
            Err(error) if Self::is_control_safety_error(&error) => Err(error),
            Err(_) => {
                let _ = mutation_started;
                Err(Error::UnknownOutcome)
            }
        }
    }

    fn finish_control_budget<'a>(
        control_budget: &'a mut Option<ControlCallBudget>,
        context: &RecorderRpcContext,
        mutation_started: &AtomicBool,
    ) -> Result<&'a ControlCallBudget> {
        if control_budget.is_none() {
            *control_budget = Some(
                ControlCallBudget::new(context)
                    .map_err(|error| Self::store_context_error(error, mutation_started))?,
            );
        }
        Ok(control_budget
            .as_ref()
            .expect("the finish-decision control budget is initialized above"))
    }

    fn record_broadcast_with_context(
        &self,
        requests: Vec<RecordRequest>,
        context: RecorderRpcContext,
        mutation_started: &AtomicBool,
    ) -> Result<Vec<RecordSummary>> {
        check_operation_context(&context, mutation_started)?;
        let budget = RpcCallBudget::new(&context)
            .map_err(|error| Self::store_context_error(error, mutation_started))?;
        let quorum = self.membership.quorum_size();
        let total = self.recorders.len().min(requests.len());
        let mut replies = Vec::with_capacity(quorum);
        let mut observed_unknown = false;
        let mut safety_error = None;
        let mut typed_error = None;
        let mut soft_failures = 0;
        for (index, request) in requests.into_iter().take(total).enumerate() {
            if replies.len() >= quorum {
                break;
            }
            if let Err(error) = budget.check_admission() {
                if replies.len() >= quorum {
                    break;
                }
                return Err(Self::store_context_error(error, mutation_started));
            }
            mutation_started.store(true, Ordering::Release);
            match self.recorders[index].record(&budget.caller, request) {
                Ok(reply) => {
                    if !replies
                        .iter()
                        .any(|seen: &RecordSummary| seen.recorder_id == reply.recorder_id)
                    {
                        replies.push(reply);
                    }
                }
                Err(Error::UnknownOutcome) => observed_unknown = true,
                Err(Error::RpcCancelled | Error::RpcDeadlineExceeded) => {
                    observed_unknown = true;
                }
                Err(error) if Self::is_record_safety_error(&error) => {
                    safety_error.get_or_insert(error);
                }
                Err(error @ (Error::TypedRecordRequired | Error::Rejected(_))) => {
                    typed_error.get_or_insert(error);
                }
                Err(Error::ProposeFailed) => soft_failures += 1,
                Err(_) => soft_failures += 1,
            }
            if let Some(error) = safety_error.clone() {
                return Err(error);
            }
        }
        if let Some(error) = safety_error {
            return Err(error);
        }
        if replies.len() >= quorum {
            return Ok(replies);
        }
        if observed_unknown {
            return Err(Error::UnknownOutcome);
        }
        if let Err(error) = context.check() {
            return Err(Self::store_context_error(error, mutation_started));
        }
        if let Some(error) = typed_error {
            // Reachable quorum may still be impossible after typed failures.
            if replies.len() + (total.saturating_sub(replies.len() + soft_failures)) < quorum {
                return Err(error);
            }
        }
        // Partial success is returned so `drive_inner` can emit Pending.
        Ok(replies)
    }

    fn install_decision_proof_quorum_with_budget(
        &self,
        budget: &ControlCallBudget,
        proof: DecisionProof,
        mutation_started: &AtomicBool,
    ) -> Result<()> {
        check_operation_context(&budget.caller, mutation_started)?;
        let membership = self.membership.clone();
        let quorum = membership.quorum_size();
        let mut installed = 0;
        let mut observed_unknown = false;
        let mut safety_error = None;
        let mut typed_error = None;
        let mut soft_failures = 0;
        for recorder in &self.recorders {
            if installed >= quorum {
                break;
            }
            if let Err(error) = budget.check_admission() {
                if installed >= quorum {
                    break;
                }
                return Err(Self::store_context_error(error, mutation_started));
            }
            mutation_started.store(true, Ordering::Release);
            match recorder.install_decision_proof(&budget.caller, proof.clone(), &membership) {
                Ok(()) => installed += 1,
                Err(Error::UnknownOutcome) => observed_unknown = true,
                Err(Error::RpcCancelled | Error::RpcDeadlineExceeded) => {
                    observed_unknown = true;
                }
                Err(error) if Self::is_control_safety_error(&error) => {
                    safety_error.get_or_insert(error);
                }
                Err(error @ (Error::TypedProofInstallRequired | Error::TypedRecordRequired)) => {
                    typed_error.get_or_insert(error);
                }
                Err(Error::ProposeFailed) => soft_failures += 1,
                Err(_) => soft_failures += 1,
            }
            if let Some(error) = safety_error.clone() {
                return Err(error);
            }
        }
        if let Some(error) = safety_error {
            return Err(error);
        }
        if installed >= quorum {
            return Ok(());
        }
        if let Some(error) = typed_error {
            return Err(error);
        }
        if observed_unknown {
            return Err(Error::UnknownOutcome);
        }
        if let Err(error) = budget.check_admission() {
            return Err(Self::store_context_error(error, mutation_started));
        }
        let _ = soft_failures;
        Err(Error::NoQuorum)
    }

    fn inspect_decision_proof_with_budget(
        &self,
        budget: &ControlCallBudget,
        slot: Slot,
    ) -> Result<Option<DecisionProof>> {
        let quorum = self.membership.quorum_size();
        let mut successful = 0;
        let mut proofs = Vec::new();
        let mut observed_unknown = false;
        let mut safety_error = None;
        let mut soft_failures = 0;
        for recorder in &self.recorders {
            if successful >= quorum {
                break;
            }
            if let Err(error) = budget.check_admission() {
                if successful >= quorum {
                    break;
                }
                return Err(error);
            }
            match recorder.inspect_decision_proof(&budget.caller, slot) {
                Ok(proof) => {
                    successful += 1;
                    proofs.extend(proof);
                }
                Err(Error::UnknownOutcome) => {
                    observed_unknown = true;
                }
                Err(error) if Self::is_control_safety_error(&error) => {
                    safety_error.get_or_insert(error);
                }
                Err(Error::ProposeFailed) => soft_failures += 1,
                Err(_) => soft_failures += 1,
            }
            if let Some(error) = safety_error.clone() {
                return Err(error);
            }
        }
        if let Some(error) = safety_error {
            return Err(error);
        }
        if observed_unknown {
            return Err(Error::UnknownOutcome);
        }
        if successful < quorum {
            let _ = soft_failures;
            return Err(Error::NoQuorum);
        }
        budget.check_admission()?;
        self.select_decision_proof(slot, proofs)
    }

    fn select_decision_proof(
        &self,
        slot: Slot,
        mut proofs: Vec<DecisionProof>,
    ) -> Result<Option<DecisionProof>> {
        for proof in &proofs {
            if proof_cluster_id(proof) != self.cluster_id {
                return Err(Error::Rejected(RejectReason::WrongCluster));
            }
            proof
                .validate_for_cluster(
                    &self.cluster_id,
                    slot,
                    self.epoch,
                    self.config_id,
                    &self.membership,
                )
                .map_err(Error::Rejected)?;
        }
        let Some(first) = proofs.first() else {
            return Ok(None);
        };
        if proofs
            .iter()
            .skip(1)
            .any(|proof| proof.proposal().value != first.proposal().value)
        {
            return Err(Error::ConflictingCertificates);
        }
        proofs.sort_by_key(|proof| match proof {
            DecisionProof::FastPath { .. } => 4,
            DecisionProof::Phase2 { step, .. } => *step,
        });
        Ok(proofs.pop())
    }

    fn fetch_verified_value(
        &self,
        slot: Slot,
        value: &AcceptedValue,
        context: &RecorderRpcContext,
        mutation_started: &AtomicBool,
    ) -> Result<Option<StoredCommand>> {
        check_operation_context(context, mutation_started)?;
        let budget = ControlCallBudget::new(context)
            .map_err(|error| Self::store_context_error(error, mutation_started))?;
        self.fetch_verified_value_with_budget(&budget, slot, value, mutation_started)
    }

    fn fetch_verified_value_with_budget(
        &self,
        budget: &ControlCallBudget,
        slot: Slot,
        value: &AcceptedValue,
        mutation_started: &AtomicBool,
    ) -> Result<Option<StoredCommand>> {
        check_operation_context(&budget.caller, mutation_started)?;
        let quorum = self.membership.quorum_size();
        let mut successful = 0;
        let mut candidate = None;
        let mut observed_unknown = false;
        let mut safety_error = None;
        let mut soft_failures = 0;
        for recorder in &self.recorders {
            if candidate.is_some() || successful >= quorum {
                break;
            }
            if let Err(error) = budget.check_admission() {
                return Err(Self::store_context_error(error, mutation_started));
            }
            match recorder.fetch_command_for(
                &budget.caller,
                self.cluster_id.clone(),
                self.epoch,
                self.config_id,
                self.config_digest,
                value.command_hash,
            ) {
                Ok(command) => {
                    successful += 1;
                    if let Some(command) = command {
                        if command.hash() != value.command_hash {
                            safety_error.get_or_insert(Error::CommandHashMismatch);
                        } else {
                            let expected = AcceptedValue::from_command(
                                &self.cluster_id,
                                slot,
                                self.epoch,
                                self.config_id,
                                value.prev_hash,
                                &command,
                            );
                            if expected == *value {
                                candidate.get_or_insert(command);
                            } else {
                                safety_error
                                    .get_or_insert(Error::Rejected(RejectReason::InvalidValue));
                            }
                        }
                    }
                }
                Err(Error::UnknownOutcome) => observed_unknown = true,
                Err(Error::RpcCancelled | Error::RpcDeadlineExceeded)
                    if mutation_started.load(Ordering::Acquire) =>
                {
                    observed_unknown = true;
                }
                Err(Error::ProposeFailed) => soft_failures += 1,
                Err(_) => soft_failures += 1,
            }
            if let Some(error) = safety_error.clone() {
                return Err(error);
            }
        }
        if let Some(error) = safety_error {
            return Err(error);
        }
        if observed_unknown {
            return Err(Error::UnknownOutcome);
        }
        if let Err(error) = budget.check_admission() {
            return Err(Self::store_context_error(error, mutation_started));
        }
        if candidate.is_some() || successful >= quorum {
            return Ok(candidate);
        }
        let _ = soft_failures;
        Err(Error::NoQuorum)
    }

    fn ensure_progress_command(
        &self,
        progress: &mut ProposerProgress,
        context: &RecorderRpcContext,
        mutation_started: &AtomicBool,
    ) -> Result<()> {
        let value = progress
            .proposal
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
        if progress
            .command
            .as_ref()
            .is_some_and(|command| self.command_matches_value(progress.slot, value, command))
        {
            return Ok(());
        }
        progress.command_holders.clear();
        progress.command =
            self.fetch_verified_value(progress.slot, value, context, mutation_started)?;
        if let Some(command) = &progress.command {
            progress.transition_involved |= command.entry_type == EntryType::ConfigChange;
            Ok(())
        } else {
            Err(Error::CommandUnavailable)
        }
    }

    fn command_matches_value(
        &self,
        slot: Slot,
        value: &AcceptedValue,
        command: &StoredCommand,
    ) -> bool {
        AcceptedValue::from_command(
            &self.cluster_id,
            slot,
            self.epoch,
            self.config_id,
            value.prev_hash,
            command,
        ) == *value
    }

    fn log_entry_from_value(
        &self,
        slot: Slot,
        command: StoredCommand,
        value: &AcceptedValue,
    ) -> Result<LogEntry> {
        let entry = LogEntry {
            cluster_id: self.cluster_id.clone(),
            epoch: self.epoch,
            config_id: self.config_id,
            index: slot,
            entry_type: command.entry_type,
            payload: command.payload,
            prev_hash: value.prev_hash,
            hash: value.entry_hash,
        };
        if entry.recompute_hash() != entry.hash {
            return Err(Error::Rejected(RejectReason::InvalidValue));
        }
        Ok(entry)
    }

    fn ensure_predecessor(
        &self,
        slot: Slot,
        actual_prev_hash: LogHash,
        expected_prev_hash: LogHash,
    ) -> Result<()> {
        proposer_drive::ensure_predecessor(slot, actual_prev_hash, expected_prev_hash)
    }

    fn store_context_error(error: Error, mutation_started: &AtomicBool) -> Error {
        proposer_drive::store_context_error(error, mutation_started)
    }

    fn is_control_safety_error(error: &Error) -> bool {
        proposer_drive::is_control_safety_error(error)
    }

    fn is_record_safety_error(error: &Error) -> bool {
        proposer_drive::is_record_safety_error(error)
    }
}

impl Consensus for CallerOwnedConsensus {
    fn propose(&self, context: RecorderRpcContext, command: Command) -> Result<LogEntry> {
        self.propose_sequential(context, command)
    }
}


