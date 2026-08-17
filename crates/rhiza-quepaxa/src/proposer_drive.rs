//! Shared proposer-drive loop used by [`crate::ThreeNodeConsensus`] and
//! [`crate::CallerOwnedConsensus`].
//!
//! Hosts keep their own recorder transport (`record_broadcast`) and proof
//! installation (`finish_decision`). The QuePaxa phase/step loop lives here
//! once so the two proposers cannot drift.

use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;

use crate::{
    check_operation_context, proof_cluster_id, proposal_exact, ClusterId, ConfigId, DecisionProof,
    DriveOutcome, Epoch, Error, LogHash, Membership, NodeId, PrioritySource, ProposalPriority,
    ProposerProgress, RecordRequest, RecordSummary, RecorderRpcContext, RecorderSummary,
    RejectReason, Result, StoredCommand,
};

/// Identity and recorder-transport hooks required by [`drive_inner`].
pub(crate) trait ProposerDriveHost {
    fn drive_cluster_id(&self) -> &ClusterId;
    fn drive_proposer_id(&self) -> &NodeId;
    fn drive_epoch(&self) -> Epoch;
    fn drive_config_id(&self) -> ConfigId;
    fn drive_config_digest(&self) -> LogHash;
    fn drive_membership(&self) -> &Membership;
    fn drive_priority_source(&self) -> &dyn PrioritySource;

    fn drive_ensure_progress_command(
        &self,
        progress: &mut ProposerProgress,
        context: &RecorderRpcContext,
        mutation_started: &AtomicBool,
    ) -> Result<()>;

    fn drive_record_broadcast(
        &self,
        requests: Vec<RecordRequest>,
        context: RecorderRpcContext,
        mutation_started: &AtomicBool,
    ) -> Result<Vec<RecordSummary>>;

    fn drive_finish_decision(
        &self,
        proof: DecisionProof,
        known_command: Option<&StoredCommand>,
        context: &RecorderRpcContext,
        mutation_started: &AtomicBool,
    ) -> Result<DriveOutcome>;
}

pub(crate) fn store_context_error(error: Error, mutation_started: &AtomicBool) -> Error {
    match error {
        Error::RpcCancelled | Error::RpcDeadlineExceeded
            if mutation_started.load(std::sync::atomic::Ordering::Acquire) =>
        {
            Error::UnknownOutcome
        }
        error => error,
    }
}

pub(crate) fn is_control_safety_error(error: &Error) -> bool {
    matches!(
        error,
        Error::ChainConflict { .. }
            | Error::CommandHashMismatch
            | Error::ConflictingCertificates
            | Error::Rejected(_)
    )
}

pub(crate) fn is_record_safety_error(error: &Error) -> bool {
    matches!(
        error,
        Error::ChainConflict { .. }
            | Error::CommandHashMismatch
            | Error::ConflictingCertificates
    )
}

pub(crate) fn ensure_predecessor(
    slot: crate::Slot,
    actual_prev_hash: LogHash,
    expected_prev_hash: LogHash,
) -> Result<()> {
    if actual_prev_hash != expected_prev_hash {
        return Err(Error::ChainConflict {
            slot,
            expected_prev_hash,
            actual_prev_hash,
        });
    }
    Ok(())
}

/// One QuePaxa drive step. Both proposer types call this.
pub(crate) fn drive_inner<H: ProposerDriveHost>(
    host: &H,
    mut progress: ProposerProgress,
    context: &RecorderRpcContext,
    mutation_started: &AtomicBool,
) -> Result<DriveOutcome> {
    check_operation_context(context, mutation_started)?;
    host.drive_ensure_progress_command(&mut progress, context, mutation_started)?;
    let round = progress.step / 4;
    let phase = progress.step % 4;
    if phase == 0 {
        progress
            .phase_zero_priorities
            .retain(|(cached_round, _), _| *cached_round == round);
    } else {
        progress.phase_zero_priorities.clear();
    }
    let members = host.drive_membership().members();
    let preferred = members
        .first()
        .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
    let command_targets: BTreeSet<_> = members
        .iter()
        .filter(|recorder_id| !progress.command_holders.contains(*recorder_id))
        .cloned()
        .collect();
    let requests: Vec<_> = members
        .iter()
        .map(|recorder_id| -> Result<RecordRequest> {
            let mut proposal = progress.proposal.clone();
            if phase == 0 {
                proposal.priority = if progress.step == 4 && host.drive_proposer_id() == preferred {
                    ProposalPriority::MAX
                } else {
                    match progress
                        .phase_zero_priorities
                        .entry((round, recorder_id.clone()))
                    {
                        std::collections::btree_map::Entry::Occupied(entry) => *entry.get(),
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            *entry.insert(host.drive_priority_source().sample(
                                progress.slot,
                                round,
                                host.drive_proposer_id(),
                                recorder_id,
                            )?)
                        }
                    }
                };
            }
            Ok(RecordRequest {
                cluster_id: host.drive_cluster_id().clone(),
                epoch: host.drive_epoch(),
                config_id: host.drive_config_id(),
                config_digest: host.drive_config_digest(),
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
    let mut replies = host.drive_record_broadcast(requests, context.clone(), mutation_started)?;
    progress.command_holders.extend(
        replies
            .iter()
            .filter(|reply| command_targets.contains(&reply.recorder_id))
            .map(|reply| reply.recorder_id.clone()),
    );
    for reply in &replies {
        if let Some(proof) = &reply.decided {
            if proof_cluster_id(proof) != host.drive_cluster_id() {
                return Err(Error::Rejected(RejectReason::WrongCluster));
            }
            proof
                .validate_for_cluster(
                    host.drive_cluster_id(),
                    progress.slot,
                    host.drive_epoch(),
                    host.drive_config_id(),
                    host.drive_membership(),
                )
                .map_err(Error::Rejected)?;
            return host.drive_finish_decision(
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
                .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
            progress.step = highest;
            if let Some(proposal) = &caught_up.first_current {
                progress.proposal = proposal.clone();
            }
            host.drive_ensure_progress_command(&mut progress, context, mutation_started)?;
            progress.phase_zero_priorities.clear();
            return Ok(DriveOutcome::Progress(progress));
        }
    }
    replies.retain(|reply| reply.step == progress.step);
    replies.sort_by(|left, right| left.recorder_id.cmp(&right.recorder_id));
    replies.dedup_by(|left, right| left.recorder_id == right.recorder_id);
    if replies.len() < host.drive_membership().quorum_size() {
        return Ok(DriveOutcome::Pending(progress));
    }
    replies.truncate(host.drive_membership().quorum_size());
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
                    cluster_id: host.drive_cluster_id().clone(),
                    slot: progress.slot,
                    epoch: host.drive_epoch(),
                    config_id: host.drive_config_id(),
                    config_digest: host.drive_config_digest(),
                    proposal,
                    summaries,
                };
                return host.drive_finish_decision(
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
                    cluster_id: host.drive_cluster_id().clone(),
                    slot: progress.slot,
                    epoch: host.drive_epoch(),
                    config_id: host.drive_config_id(),
                    config_digest: host.drive_config_digest(),
                    step: progress.step,
                    proposal: progress.proposal.clone(),
                    summaries,
                };
                return host.drive_finish_decision(
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
    host.drive_ensure_progress_command(&mut progress, context, mutation_started)?;
    progress.step = progress.step.checked_add(1).ok_or(Error::ProposeFailed)?;
    progress.phase_zero_priorities.clear();
    Ok(DriveOutcome::Progress(progress))
}
