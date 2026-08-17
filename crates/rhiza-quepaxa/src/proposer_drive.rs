//! Shared proposer-drive helpers used by [`crate::ThreeNodeConsensus`] and
//! [`crate::CallerOwnedConsensus`].
//!
//! These classify recorder errors and predecessor hashes. The protocol loop
//! (`drive_inner`) still has two copies until a later extract introduces a
//! shared drive trait; this module is the first reviewable share.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::{Error, LogHash, Result, Slot};

pub(crate) fn store_context_error(error: Error, mutation_started: &AtomicBool) -> Error {
    match error {
        Error::RpcCancelled | Error::RpcDeadlineExceeded
            if mutation_started.load(Ordering::Acquire) =>
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
    slot: Slot,
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
