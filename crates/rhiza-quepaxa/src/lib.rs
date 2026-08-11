#![doc = include_str!("../README.md")]

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    cmp::Ordering as CmpOrdering,
    collections::{hash_map, BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Condvar, Mutex, Weak,
    },
    thread,
    time::{Duration, Instant},
};

use rhiza_archive::CheckpointReadbackCertificate;
use rhiza_core::canonical_membership_digest;

pub use rhiza_core::{
    ClusterId, Command, CommandKind, ConfigChange, ConfigId, EntryType, Epoch, ExternalEffectChunk,
    ExternalEffectCommand, LogEntry, LogHash, LogIndex, NodeId, StoredCommand,
};

mod anchored_fs;

pub type Result<T> = std::result::Result<T, Error>;
pub type Slot = u64;
pub type Round = u64;
pub type Phase = u8;
pub type Step = u64;
pub type Priority = u128;

/// Per-operation deadline and cancellation signal carried by every Recorder
/// RPC. A deadline is deliberately absolute so queueing time counts against
/// the operation's budget.
#[derive(Clone, Debug)]
pub struct RecorderRpcContext {
    deadline: Instant,
    cancellations: Vec<Arc<AtomicBool>>,
}

impl RecorderRpcContext {
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::with_deadline(
            Instant::now()
                .checked_add(timeout)
                .unwrap_or_else(Instant::now),
        )
    }

    /// Uses a caller-owned cancellation token. This is the only supported
    /// bridge from a host runtime's shutdown signal into a consensus call.
    pub fn with_timeout_and_cancellation(timeout: Duration, cancellation: Arc<AtomicBool>) -> Self {
        let mut context = Self::with_timeout(timeout);
        context.cancellations.push(cancellation);
        context
    }

    pub fn with_deadline(deadline: Instant) -> Self {
        Self {
            deadline,
            cancellations: vec![Arc::new(AtomicBool::new(false))],
        }
    }

    pub fn default_timeout() -> Self {
        Self::with_timeout(DEFAULT_RECORDER_RPC_TIMEOUT)
    }

    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn cancel(&self) {
        for cancellation in &self.cancellations {
            cancellation.store(true, Ordering::Release);
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellations
            .iter()
            .any(|cancellation| cancellation.load(Ordering::Acquire))
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.deadline.checked_duration_since(Instant::now())
    }

    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(Error::RpcCancelled);
        }
        if self.remaining().is_none() {
            return Err(Error::RpcDeadlineExceeded);
        }
        Ok(())
    }

    fn with_cancellation(&self, cancellation: Arc<AtomicBool>) -> Self {
        let mut cancellations = self.cancellations.clone();
        cancellations.push(cancellation);
        Self {
            deadline: self.deadline,
            cancellations,
        }
    }

    fn with_deadline_and_cancellation(
        &self,
        deadline: Instant,
        cancellation: Arc<AtomicBool>,
    ) -> Self {
        let mut cancellations = self.cancellations.clone();
        cancellations.push(cancellation);
        Self {
            deadline,
            cancellations,
        }
    }
}

/// An RPC invocation captures the caller deadline once.  `work_deadline`
/// is never recomputed by nested work: the final reserve is exclusively for
/// cancelling and draining already admitted jobs.
#[derive(Clone)]
struct RpcCallBudget {
    caller: RecorderRpcContext,
    deadline: Instant,
    work_deadline: Instant,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ControlWorkDeadlineCheckpoint {
    Constructor,
    Admission,
}

impl RpcCallBudget {
    fn new(caller: &RecorderRpcContext) -> Result<Self> {
        #[cfg(test)]
        record_control_budget_constructor(caller);
        let deadline = caller.deadline();
        let Some(work_deadline) = deadline.checked_sub(CONTROL_DRAIN_RESERVE) else {
            caller.check()?;
            return Err(Error::RpcDeadlineExceeded);
        };
        check_control_work_deadline(
            caller,
            work_deadline,
            ControlWorkDeadlineCheckpoint::Constructor,
        )?;
        Ok(Self {
            caller: caller.clone(),
            deadline,
            work_deadline,
        })
    }

    fn check_admission(&self) -> Result<()> {
        check_control_work_deadline(
            &self.caller,
            self.work_deadline,
            ControlWorkDeadlineCheckpoint::Admission,
        )
    }

    fn child_context(&self, group: &RpcCallGroup) -> RecorderRpcContext {
        self.caller
            .with_deadline_and_cancellation(self.work_deadline, group.token())
    }
}

// Control collectors were the first users.  Keep the private alias while
// record collectors migrate to the same caller-owned D/W budget.
type ControlCallBudget = RpcCallBudget;

#[cfg(test)]
struct ControlBudgetConstructorHook {
    cancellation: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
}

#[cfg(test)]
static CONTROL_BUDGET_CONSTRUCTOR_HOOK: std::sync::OnceLock<
    Mutex<Option<ControlBudgetConstructorHook>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
struct ControlBudgetConstructorHookGuard;

#[cfg(test)]
impl Drop for ControlBudgetConstructorHookGuard {
    fn drop(&mut self) {
        let hook = CONTROL_BUDGET_CONSTRUCTOR_HOOK.get_or_init(|| Mutex::new(None));
        *lock_unpoison(hook) = None;
    }
}

#[cfg(test)]
fn count_control_budget_constructors_for(
    cancellation: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
) -> ControlBudgetConstructorHookGuard {
    let hook = CONTROL_BUDGET_CONSTRUCTOR_HOOK.get_or_init(|| Mutex::new(None));
    let mut hook = lock_unpoison(hook);
    assert!(
        hook.is_none(),
        "only one control-budget constructor hook may be armed"
    );
    *hook = Some(ControlBudgetConstructorHook {
        cancellation,
        calls,
    });
    ControlBudgetConstructorHookGuard
}

#[cfg(test)]
fn record_control_budget_constructor(context: &RecorderRpcContext) {
    let hook = CONTROL_BUDGET_CONSTRUCTOR_HOOK.get_or_init(|| Mutex::new(None));
    let hook = lock_unpoison(hook);
    if hook.as_ref().is_some_and(|candidate| {
        context
            .cancellations
            .iter()
            .any(|token| Arc::ptr_eq(token, &candidate.cancellation))
    }) {
        hook.as_ref().unwrap().calls.fetch_add(1, Ordering::AcqRel);
    }
}

fn check_control_work_deadline(
    caller: &RecorderRpcContext,
    work_deadline: Instant,
    _checkpoint: ControlWorkDeadlineCheckpoint,
) -> Result<()> {
    caller.check()?;
    #[cfg(test)]
    pause_before_control_work_deadline_check(caller, _checkpoint);
    if Instant::now() >= work_deadline {
        caller.check()?;
        return Err(Error::RpcDeadlineExceeded);
    }
    Ok(())
}

#[cfg(test)]
struct ControlWorkDeadlineHook {
    cancellation: Arc<AtomicBool>,
    checkpoint: ControlWorkDeadlineCheckpoint,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

#[cfg(test)]
static CONTROL_WORK_DEADLINE_HOOK: std::sync::OnceLock<Mutex<Option<ControlWorkDeadlineHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
struct ControlWorkDeadlineHookGuard;

#[cfg(test)]
impl Drop for ControlWorkDeadlineHookGuard {
    fn drop(&mut self) {
        let hook = CONTROL_WORK_DEADLINE_HOOK.get_or_init(|| Mutex::new(None));
        *lock_unpoison(hook) = None;
    }
}

#[cfg(test)]
fn pause_next_control_work_deadline_check(
    cancellation: Arc<AtomicBool>,
    checkpoint: ControlWorkDeadlineCheckpoint,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
) -> ControlWorkDeadlineHookGuard {
    let hook = CONTROL_WORK_DEADLINE_HOOK.get_or_init(|| Mutex::new(None));
    let mut hook = lock_unpoison(hook);
    assert!(
        hook.is_none(),
        "only one control deadline hook may be armed"
    );
    *hook = Some(ControlWorkDeadlineHook {
        cancellation,
        checkpoint,
        entered,
        release,
    });
    ControlWorkDeadlineHookGuard
}

#[cfg(test)]
fn pause_before_control_work_deadline_check(
    context: &RecorderRpcContext,
    checkpoint: ControlWorkDeadlineCheckpoint,
) {
    let hook = CONTROL_WORK_DEADLINE_HOOK.get_or_init(|| Mutex::new(None));
    let hook = {
        let mut hook = lock_unpoison(hook);
        match hook.as_ref() {
            Some(candidate)
                if candidate.checkpoint == checkpoint
                    && context
                        .cancellations
                        .iter()
                        .any(|token| Arc::ptr_eq(token, &candidate.cancellation)) =>
            {
                hook.take()
            }
            _ => None,
        }
    };
    let Some(hook) = hook else {
        return;
    };
    hook.entered.send(()).unwrap();
    let (released, condition) = &*hook.release;
    let mut released = lock_unpoison(released);
    while !*released {
        released = condition
            .wait(released)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}

#[cfg(test)]
struct SummaryDispatchHook {
    cancellation: Arc<AtomicBool>,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

#[cfg(test)]
static SUMMARY_DISPATCH_HOOK: std::sync::OnceLock<Mutex<Option<SummaryDispatchHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
struct SummaryDispatchHookGuard;

#[cfg(test)]
impl Drop for SummaryDispatchHookGuard {
    fn drop(&mut self) {
        let hook = SUMMARY_DISPATCH_HOOK.get_or_init(|| Mutex::new(None));
        *lock_unpoison(hook) = None;
    }
}

#[cfg(test)]
fn pause_after_next_summary_dispatch(
    cancellation: Arc<AtomicBool>,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
) -> SummaryDispatchHookGuard {
    let hook = SUMMARY_DISPATCH_HOOK.get_or_init(|| Mutex::new(None));
    let mut hook = lock_unpoison(hook);
    assert!(
        hook.is_none(),
        "only one summary dispatch hook may be armed"
    );
    *hook = Some(SummaryDispatchHook {
        cancellation,
        entered,
        release,
    });
    SummaryDispatchHookGuard
}

#[cfg(test)]
fn pause_after_summary_dispatch(context: &RecorderRpcContext) {
    let hook = SUMMARY_DISPATCH_HOOK.get_or_init(|| Mutex::new(None));
    let hook = {
        let mut hook = lock_unpoison(hook);
        match hook.as_ref() {
            Some(candidate)
                if context
                    .cancellations
                    .iter()
                    .any(|token| Arc::ptr_eq(token, &candidate.cancellation)) =>
            {
                hook.take()
            }
            _ => None,
        }
    };
    let Some(hook) = hook else {
        return;
    };
    hook.entered.send(()).unwrap();
    let (released, condition) = &*hook.release;
    let mut released = lock_unpoison(released);
    while !*released {
        released = condition
            .wait(released)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}

#[cfg(test)]
struct SummaryProvisionalNoneHook {
    cancellation: Arc<AtomicBool>,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

#[cfg(test)]
static SUMMARY_PROVISIONAL_NONE_HOOK: std::sync::OnceLock<
    Mutex<Option<SummaryProvisionalNoneHook>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
struct SummaryProvisionalNoneHookGuard;

#[cfg(test)]
impl Drop for SummaryProvisionalNoneHookGuard {
    fn drop(&mut self) {
        let hook = SUMMARY_PROVISIONAL_NONE_HOOK.get_or_init(|| Mutex::new(None));
        *lock_unpoison(hook) = None;
    }
}

#[cfg(test)]
fn pause_after_next_summary_provisional_none(
    cancellation: Arc<AtomicBool>,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
) -> SummaryProvisionalNoneHookGuard {
    let hook = SUMMARY_PROVISIONAL_NONE_HOOK.get_or_init(|| Mutex::new(None));
    let mut hook = lock_unpoison(hook);
    assert!(
        hook.is_none(),
        "only one summary provisional-none hook may be armed"
    );
    *hook = Some(SummaryProvisionalNoneHook {
        cancellation,
        entered,
        release,
    });
    SummaryProvisionalNoneHookGuard
}

#[cfg(test)]
fn pause_after_summary_provisional_none(context: &RecorderRpcContext) {
    let hook = SUMMARY_PROVISIONAL_NONE_HOOK.get_or_init(|| Mutex::new(None));
    let hook = {
        let mut hook = lock_unpoison(hook);
        match hook.as_ref() {
            Some(candidate)
                if context
                    .cancellations
                    .iter()
                    .any(|token| Arc::ptr_eq(token, &candidate.cancellation)) =>
            {
                hook.take()
            }
            _ => None,
        }
    };
    let Some(hook) = hook else {
        return;
    };
    hook.entered.send(()).unwrap();
    let (released, condition) = &*hook.release;
    let mut released = lock_unpoison(released);
    while !*released {
        released = condition
            .wait(released)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}

#[cfg(test)]
struct FetchDispatchHook {
    cancellation: Arc<AtomicBool>,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

#[cfg(test)]
static FETCH_DISPATCH_HOOK: std::sync::OnceLock<Mutex<Option<FetchDispatchHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
struct FetchDispatchHookGuard;

#[cfg(test)]
impl Drop for FetchDispatchHookGuard {
    fn drop(&mut self) {
        let hook = FETCH_DISPATCH_HOOK.get_or_init(|| Mutex::new(None));
        *lock_unpoison(hook) = None;
    }
}

#[cfg(test)]
fn pause_after_next_fetch_dispatch(
    cancellation: Arc<AtomicBool>,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
) -> FetchDispatchHookGuard {
    let hook = FETCH_DISPATCH_HOOK.get_or_init(|| Mutex::new(None));
    let mut hook = lock_unpoison(hook);
    assert!(hook.is_none(), "only one fetch dispatch hook may be armed");
    *hook = Some(FetchDispatchHook {
        cancellation,
        entered,
        release,
    });
    FetchDispatchHookGuard
}

#[cfg(test)]
fn pause_after_fetch_dispatch(context: &RecorderRpcContext) {
    let hook = FETCH_DISPATCH_HOOK.get_or_init(|| Mutex::new(None));
    let hook = {
        let mut hook = lock_unpoison(hook);
        match hook.as_ref() {
            Some(candidate)
                if context
                    .cancellations
                    .iter()
                    .any(|token| Arc::ptr_eq(token, &candidate.cancellation)) =>
            {
                hook.take()
            }
            _ => None,
        }
    };
    let Some(hook) = hook else {
        return;
    };
    hook.entered.send(()).unwrap();
    let (released, condition) = &*hook.release;
    let mut released = lock_unpoison(released);
    while !*released {
        released = condition
            .wait(released)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}

#[cfg(test)]
#[derive(Debug)]
enum BudgetIdentityEvent {
    ReadFenceHandoff {
        deadline: Instant,
        work_deadline: Instant,
        outstanding: usize,
    },
    SummaryHandoff {
        deadline: Instant,
        work_deadline: Instant,
        outstanding: usize,
    },
    FetchDispatch {
        deadline: Instant,
        work_deadline: Instant,
    },
    FetchHandoff {
        deadline: Instant,
        work_deadline: Instant,
        outstanding: usize,
        mutation_started: usize,
    },
    FinishFetchHandoff {
        deadline: Instant,
        work_deadline: Instant,
        outstanding: usize,
        mutation_started: usize,
    },
    InstallDispatch {
        deadline: Instant,
        work_deadline: Instant,
        mutation_started: usize,
        mutation_started_set: bool,
    },
}

#[cfg(test)]
struct BudgetIdentityHook {
    cancellation: Arc<AtomicBool>,
    events: std::sync::mpsc::SyncSender<BudgetIdentityEvent>,
}

#[cfg(test)]
static BUDGET_IDENTITY_HOOK: std::sync::OnceLock<Mutex<Option<BudgetIdentityHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
struct BudgetIdentityHookGuard;

#[cfg(test)]
impl Drop for BudgetIdentityHookGuard {
    fn drop(&mut self) {
        let hook = BUDGET_IDENTITY_HOOK.get_or_init(|| Mutex::new(None));
        *lock_unpoison(hook) = None;
    }
}

#[cfg(test)]
fn record_budget_identity_for(
    cancellation: Arc<AtomicBool>,
    events: std::sync::mpsc::SyncSender<BudgetIdentityEvent>,
) -> BudgetIdentityHookGuard {
    let hook = BUDGET_IDENTITY_HOOK.get_or_init(|| Mutex::new(None));
    let mut hook = lock_unpoison(hook);
    assert!(hook.is_none(), "only one budget identity hook may be armed");
    *hook = Some(BudgetIdentityHook {
        cancellation,
        events,
    });
    BudgetIdentityHookGuard
}

#[cfg(test)]
fn record_budget_identity(context: &RecorderRpcContext, event: BudgetIdentityEvent) {
    let hook = BUDGET_IDENTITY_HOOK.get_or_init(|| Mutex::new(None));
    let hook = lock_unpoison(hook);
    if hook.as_ref().is_some_and(|candidate| {
        context
            .cancellations
            .iter()
            .any(|token| Arc::ptr_eq(token, &candidate.cancellation))
    }) {
        hook.as_ref().unwrap().events.send(event).unwrap();
    }
}

#[cfg(test)]
struct FetchGroupTokenHook {
    cancellation: Arc<AtomicBool>,
    token: std::sync::mpsc::SyncSender<Arc<AtomicBool>>,
}

#[cfg(test)]
static FETCH_GROUP_TOKEN_HOOK: std::sync::OnceLock<Mutex<Option<FetchGroupTokenHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
struct FetchGroupTokenHookGuard;

#[cfg(test)]
impl Drop for FetchGroupTokenHookGuard {
    fn drop(&mut self) {
        let hook = FETCH_GROUP_TOKEN_HOOK.get_or_init(|| Mutex::new(None));
        *lock_unpoison(hook) = None;
    }
}

#[cfg(test)]
fn capture_next_fetch_group_token(
    cancellation: Arc<AtomicBool>,
    token: std::sync::mpsc::SyncSender<Arc<AtomicBool>>,
) -> FetchGroupTokenHookGuard {
    let hook = FETCH_GROUP_TOKEN_HOOK.get_or_init(|| Mutex::new(None));
    let mut hook = lock_unpoison(hook);
    assert!(
        hook.is_none(),
        "only one fetch group-token hook may be armed"
    );
    *hook = Some(FetchGroupTokenHook {
        cancellation,
        token,
    });
    FetchGroupTokenHookGuard
}

#[cfg(test)]
fn capture_fetch_group_token(context: &RecorderRpcContext, group: &ControlCallGroup) {
    let hook = FETCH_GROUP_TOKEN_HOOK.get_or_init(|| Mutex::new(None));
    let hook = {
        let mut hook = lock_unpoison(hook);
        if hook.as_ref().is_some_and(|candidate| {
            context
                .cancellations
                .iter()
                .any(|token| Arc::ptr_eq(token, &candidate.cancellation))
        }) {
            hook.take()
        } else {
            None
        }
    };
    if let Some(hook) = hook {
        hook.token.send(group.token()).unwrap();
    }
}

/// Read-only classification of a recorder root before startup recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecorderPreflight {
    Missing,
    Valid,
    Recoverable,
}

const RECORDER_STATE_VERSION: u16 = 4;
const CONFIGURATION_STATE_VERSION: u16 = 3;
const RECORD_WORKER_QUEUE_CAPACITY: usize = 1;
const CONTROL_WORKER_QUEUE_CAPACITY: usize = 1;
/// A blocked worker result channel must periodically re-check the caller's
/// cancellation token instead of sleeping until its absolute deadline.
const CONTEXT_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// The final ten control polls are reserved for cancellation and drain.
const CONTROL_DRAIN_RESERVE: Duration = Duration::from_millis(100);
/// The maximum time the consensus runtime will wait for a Recorder RPC unless
/// its caller supplies a shorter explicit deadline.
pub const DEFAULT_RECORDER_RPC_TIMEOUT: Duration = Duration::from_secs(5);

// The largest replicated command supported by the bounded WAL rotation policy below. The
// remaining limits follow directly from the on-disk envelopes: seven membership entries,
// two durable slot snapshots, and at most one oversized WAL frame past the soft limit.
const MAX_REPLICATED_COMMAND_BYTES: usize = 512 * 1024;
/// Large SQL effects are deliberately not replicated as `StoredCommand`s.
/// Their immutable data lives in the Recorder effect-bundle namespace and a
/// small manifest is what will eventually be replicated by the SQL layer.
pub const MAX_EFFECT_BUNDLE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_EFFECT_BUNDLE_CHUNK_BYTES: usize = 256 * 1024;
pub const MAX_EFFECT_BUNDLE_CHUNKS: usize = 256;
pub const DEFAULT_EFFECT_BUNDLE_STORE_QUOTA_BYTES: u64 = 256 * 1024 * 1024;
const MAX_COMMAND_CACHE_BYTES: usize = 4 + 2 + 1 + 8 + MAX_REPLICATED_COMMAND_BYTES + 32;
const MAX_CONFIGURATION_BYTES: usize = 512 * 1024;
const MAX_RECORDER_STATE_BYTES: usize = 1024 * 1024;
const MAX_RECORDED_HEAD_BYTES: usize = 2 * MAX_RECORDER_STATE_BYTES + MAX_CONFIGURATION_BYTES;
const MAX_TRANSITION_INTENT_BYTES: usize =
    MAX_RECORDER_STATE_BYTES + MAX_CONFIGURATION_BYTES + 4 + 2 + 8 + 2 + 2;
const MAX_CONFIGURATION_HEAD_INTENT_BYTES: usize =
    MAX_CONFIGURATION_BYTES + MAX_RECORDED_HEAD_BYTES + 4 + 2 + 8 + 8;
const MAX_RECORDER_WAL_BYTES: usize = 64 * 1024 * 1024 + 2 * 1024 * 1024;

fn validate_replicated_command_size(command: &StoredCommand) -> Result<()> {
    let actual = command.payload.len();
    if actual > MAX_REPLICATED_COMMAND_BYTES {
        return Err(Error::CommandTooLarge {
            actual,
            limit: MAX_REPLICATED_COMMAND_BYTES,
        });
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const O_NOFOLLOW_FLAG: i32 = 0o400000;
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
const O_NOFOLLOW_FLAG: i32 = 0x0100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    ChainConflict {
        slot: Slot,
        expected_prev_hash: LogHash,
        actual_prev_hash: LogHash,
    },
    CommandHashMismatch,
    CommandTooLarge {
        actual: usize,
        limit: usize,
    },
    CommandUnavailable,
    EffectBundleConflict,
    EffectBundleInvalid(String),
    EffectBundleQuotaExceeded {
        actual: u64,
        limit: u64,
    },
    EffectBundleUnavailable,
    Cancelled,
    /// The caller cancelled an RPC before a recorder acknowledged it.
    RpcCancelled,
    /// The RPC deadline elapsed before a recorder acknowledged it. The
    /// recorder may still have durably performed the operation.
    RpcDeadlineExceeded,
    ConflictingCertificates,
    Decode(String),
    DuplicateRecorderIdentity,
    EmptyRecorderIdentity,
    EmptyFixedMembership,
    InvalidFixedMembershipSize,
    InvalidRecoveredTip,
    Io(String),
    NoQuorum,
    ProposeFailed,
    RandomnessUnavailable(String),
    RecorderRootLocked(PathBuf),
    Rejected(RejectReason),
    ReadFenceUnsupported,
    TypedProofInstallRequired,
    TypedRecordRequired,
    /// The result of a mutating Recorder RPC is unknown. Callers must recover
    /// from Recorder state rather than retrying it as a new operation.
    UnknownOutcome,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChainConflict { slot, .. } => {
                write!(f, "QuePaxa predecessor conflicts at slot {slot}")
            }
            Self::CommandHashMismatch => write!(f, "QuePaxa command hash mismatch"),
            Self::CommandTooLarge { actual, limit } => {
                write!(
                    f,
                    "QuePaxa command payload of {actual} bytes exceeds the {limit}-byte limit"
                )
            }
            Self::CommandUnavailable => write!(f, "QuePaxa command bytes unavailable"),
            Self::EffectBundleConflict => write!(f, "Recorder effect bundle conflicts with existing durable data"),
            Self::EffectBundleInvalid(message) => write!(f, "Recorder effect bundle is invalid: {message}"),
            Self::EffectBundleQuotaExceeded { actual, limit } => write!(
                f,
                "Recorder effect-bundle store needs {actual} bytes but its {limit}-byte quota is exhausted"
            ),
            Self::EffectBundleUnavailable => write!(f, "Recorder effect bundle bytes unavailable"),
            Self::Cancelled => write!(f, "QuePaxa proposal cancelled"),
            Self::RpcCancelled => write!(f, "QuePaxa recorder RPC cancelled"),
            Self::RpcDeadlineExceeded => write!(f, "QuePaxa recorder RPC deadline elapsed"),
            Self::ConflictingCertificates => {
                write!(f, "QuePaxa recovered conflicting decision certificates")
            }
            Self::Decode(message) => write!(f, "QuePaxa decode failed: {message}"),
            Self::DuplicateRecorderIdentity => write!(f, "recorder identities must be unique"),
            Self::EmptyRecorderIdentity => write!(f, "recorder identity must not be empty"),
            Self::EmptyFixedMembership => {
                write!(f, "fixed membership must include at least one node")
            }
            Self::InvalidFixedMembershipSize => {
                write!(f, "membership requires between three and seven recorders")
            }
            Self::InvalidRecoveredTip => write!(f, "recovered qlog next index must be positive"),
            Self::Io(message) => write!(f, "QuePaxa io failed: {message}"),
            Self::NoQuorum => write!(f, "QuePaxa quorum was not reached"),
            Self::ProposeFailed => write!(f, "QuePaxa propose failed"),
            Self::RandomnessUnavailable(message) => {
                write!(f, "QuePaxa OS randomness unavailable: {message}")
            }
            Self::RecorderRootLocked(root) => {
                write!(f, "recorder root is already owned: {}", root.display())
            }
            Self::Rejected(reason) => write!(f, "QuePaxa recorder rejected request: {reason:?}"),
            Self::ReadFenceUnsupported => {
                write!(f, "recorder does not implement context-bound read fences")
            }
            Self::TypedProofInstallRequired => {
                write!(
                    f,
                    "recorder does not implement typed decision-proof installation"
                )
            }
            Self::TypedRecordRequired => {
                write!(f, "recorder does not implement the typed Record operation")
            }
            Self::UnknownOutcome => {
                write!(
                    f,
                    "QuePaxa recorder RPC outcome is unknown; recover recorder state"
                )
            }
        }
    }
}

impl std::error::Error for Error {}

fn check_operation_context(
    context: &RecorderRpcContext,
    mutation_started: &AtomicBool,
) -> Result<()> {
    match context.check() {
        Err(Error::RpcCancelled | Error::RpcDeadlineExceeded)
            if mutation_started.load(Ordering::Acquire) =>
        {
            Err(Error::UnknownOutcome)
        }
        result => result,
    }
}

fn check_proposal_operation_context<F>(
    context: &RecorderRpcContext,
    mutation_started: &AtomicBool,
    cancelled: &F,
) -> Result<()>
where
    F: Fn() -> Result<()>,
{
    check_operation_context(context, mutation_started)?;
    match cancelled() {
        Err(Error::RpcCancelled | Error::RpcDeadlineExceeded)
            if mutation_started.load(Ordering::Acquire) =>
        {
            Err(Error::UnknownOutcome)
        }
        result => result,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Membership {
    members: Vec<NodeId>,
    digest: LogHash,
}

impl Membership {
    pub fn new<const N: usize>(members: [&str; N]) -> Result<Self> {
        Self::from_voters(members.into_iter().map(String::from))
    }

    pub fn from_voters(voters: impl IntoIterator<Item = NodeId>) -> Result<Self> {
        Self::from_members(voters.into_iter().collect())
    }

    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    pub fn contains(&self, recorder_id: &str) -> bool {
        self.members
            .binary_search_by(|member| member.as_str().cmp(recorder_id))
            .is_ok()
    }

    pub const fn digest(&self) -> LogHash {
        self.digest
    }

    pub fn quorum_size(&self) -> usize {
        quorum_size(self.members.len())
    }

    fn from_members(members: Vec<NodeId>) -> Result<Self> {
        if members.is_empty() {
            return Err(Error::EmptyFixedMembership);
        }
        if !(3..=7).contains(&members.len()) {
            return Err(Error::InvalidFixedMembershipSize);
        }
        if members.iter().any(String::is_empty) {
            return Err(Error::EmptyRecorderIdentity);
        }
        let member_count = members.len();
        let members: Vec<_> = members
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if members.len() != member_count {
            return Err(Error::DuplicateRecorderIdentity);
        }
        Ok(Self {
            digest: canonical_membership_digest(&members)
                .map_err(|_| Error::InvalidFixedMembershipSize)?,
            members,
        })
    }
}

pub type FixedMembership = Membership;

pub trait Consensus {
    fn propose(&self, context: RecorderRpcContext, command: Command) -> Result<LogEntry>;
}

pub fn quorum_size(n: usize) -> usize {
    n / 2 + 1
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
pub struct Ballot {
    pub round: Round,
    pub priority: Priority,
    pub proposer_id: NodeId,
}

impl Ballot {
    pub fn new(round: Round, priority: Priority, proposer_id: impl Into<NodeId>) -> Self {
        Self {
            round,
            priority,
            proposer_id: proposer_id.into(),
        }
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct LogicalStep {
    pub round: Round,
    pub phase: Phase,
}

impl LogicalStep {
    pub const fn as_u64(&self) -> Step {
        self.round * 4 + self.phase as u64
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct ProposalPriority(pub [u8; 32]);

impl ProposalPriority {
    pub const ZERO: Self = Self([0; 32]);
    pub const MAX: Self = Self([u8::MAX; 32]);

    pub const fn from_u64(value: u64) -> Self {
        let mut bytes = [0; 32];
        let encoded = value.to_be_bytes();
        let mut index = 0;
        while index < encoded.len() {
            bytes[24 + index] = encoded[index];
            index += 1;
        }
        Self(bytes)
    }

    fn low_u128(self) -> u128 {
        u128::from_be_bytes(self.0[16..].try_into().expect("fixed priority suffix"))
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Proposal {
    pub priority: ProposalPriority,
    pub proposer_id: NodeId,
    pub proposal_id: u64,
    pub value: Option<AcceptedValue>,
}

impl Proposal {
    pub fn new(
        priority: ProposalPriority,
        proposer_id: impl Into<NodeId>,
        proposal_id: u64,
        value: AcceptedValue,
    ) -> Self {
        Self {
            priority,
            proposer_id: proposer_id.into(),
            proposal_id,
            value: Some(value),
        }
    }

    pub fn nil() -> Self {
        Self {
            priority: ProposalPriority::ZERO,
            proposer_id: String::new(),
            proposal_id: 0,
            value: None,
        }
    }

    fn identity(&self) -> (ProposalPriority, &str, u64, Option<&AcceptedValue>) {
        (
            self.priority,
            &self.proposer_id,
            self.proposal_id,
            self.value.as_ref(),
        )
    }

    fn is_nil(&self) -> bool {
        self.value.is_none()
    }
}

impl PartialEq for Proposal {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for Proposal {}

impl PartialOrd for Proposal {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for Proposal {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match (self.is_nil(), other.is_nil()) {
            (true, true) => CmpOrdering::Equal,
            (true, false) => CmpOrdering::Less,
            (false, true) => CmpOrdering::Greater,
            (false, false) => self.identity().cmp(&other.identity()),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IsrState {
    step: Step,
    first_current: Option<Proposal>,
    aggregate_current: Option<Proposal>,
    aggregate_prior: Option<Proposal>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IsrReply {
    pub step: Step,
    pub first_current: Option<Proposal>,
    pub aggregate_prior: Option<Proposal>,
}

impl IsrState {
    pub const fn step(&self) -> Step {
        self.step
    }

    pub const fn first_current(&self) -> Option<&Proposal> {
        self.first_current.as_ref()
    }

    pub const fn aggregate_current(&self) -> Option<&Proposal> {
        self.aggregate_current.as_ref()
    }

    pub const fn aggregate_prior(&self) -> Option<&Proposal> {
        self.aggregate_prior.as_ref()
    }

    /// Pure Algorithm 3 transition. Stale inputs return an unchanged state.
    pub fn record(&self, step: Step, proposal: Proposal) -> (Self, IsrReply) {
        let mut next = self.clone();
        if step == next.step {
            if next.first_current.is_none() {
                next.first_current = Some(proposal.clone());
            }
            if next.aggregate_current.as_ref() < Some(&proposal) {
                next.aggregate_current = Some(proposal);
            }
        } else if step > next.step {
            next.aggregate_prior = if step == next.step.saturating_add(1) {
                next.aggregate_current.take()
            } else {
                None
            };
            next.step = step;
            next.first_current = Some(proposal.clone());
            next.aggregate_current = Some(proposal);
        }
        let reply = IsrReply {
            step: next.step,
            first_current: next.first_current.clone(),
            aggregate_prior: next.aggregate_prior.clone(),
        };
        (next, reply)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct AcceptedValue {
    pub command_hash: LogHash,
    pub prev_hash: LogHash,
    pub entry_hash: LogHash,
}

impl PartialOrd for AcceptedValue {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for AcceptedValue {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (
            self.entry_hash.as_bytes(),
            self.command_hash.as_bytes(),
            self.prev_hash.as_bytes(),
        )
            .cmp(&(
                other.entry_hash.as_bytes(),
                other.command_hash.as_bytes(),
                other.prev_hash.as_bytes(),
            ))
    }
}

impl AcceptedValue {
    pub fn from_command(
        cluster_id: &str,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        prev_hash: LogHash,
        command: &StoredCommand,
    ) -> Self {
        Self {
            command_hash: command.hash(),
            prev_hash,
            entry_hash: LogEntry::calculate_hash(
                cluster_id,
                slot,
                epoch,
                config_id,
                command.entry_type,
                prev_hash,
                &command.payload,
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct AcceptedSummary {
    pub ballot: Ballot,
    pub value: AcceptedValue,
}

pub type ProposalSummary = AcceptedSummary;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DecisionCertificate {
    pub slot: Slot,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub ballot: Ballot,
    pub value: AcceptedValue,
    pub recorder_ids: Vec<NodeId>,
}

impl DecisionCertificate {
    pub fn cluster_id(&self) -> Option<&str> {
        decode_certificate_proposer(&self.ballot.proposer_id).map(|(cluster_id, _)| cluster_id)
    }

    pub fn validate_for_cluster(
        &self,
        cluster_id: &str,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        if self.cluster_id() != Some(cluster_id) {
            return Err(RejectReason::WrongCluster);
        }
        self.validate_for(config_id, membership)
    }

    pub fn validate(&self, membership: &FixedMembership) -> std::result::Result<(), RejectReason> {
        if self.config_digest != membership.digest() {
            return Err(RejectReason::WrongConfig);
        }
        if self.recorder_ids.len() != membership.quorum_size()
            || !self.recorder_ids.windows(2).all(|pair| pair[0] < pair[1])
            || self
                .recorder_ids
                .iter()
                .any(|recorder_id| !membership.contains(recorder_id))
        {
            return Err(RejectReason::InvalidCertificate);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        if self.config_id != config_id {
            return Err(RejectReason::WrongConfig);
        }
        self.validate(membership)
    }

    fn validate_context(
        &self,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
    ) -> std::result::Result<(), RejectReason> {
        if self.slot != slot || self.epoch != epoch {
            return Err(RejectReason::MalformedDecision);
        }
        if self.config_id != config_id || self.config_digest != config_digest {
            return Err(RejectReason::WrongConfig);
        }
        Ok(())
    }
}

pub type DecisionRecord = DecisionCertificate;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecorderSummary {
    pub recorder_id: NodeId,
    pub slot: Slot,
    pub step: Step,
    pub first_current: Option<Proposal>,
    pub aggregate_prior: Option<Proposal>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum DecisionProof {
    FastPath {
        cluster_id: ClusterId,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        proposal: Proposal,
        summaries: Vec<RecorderSummary>,
    },
    Phase2 {
        cluster_id: ClusterId,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        step: Step,
        proposal: Proposal,
        summaries: Vec<RecorderSummary>,
    },
}

impl DecisionProof {
    pub fn proposal(&self) -> &Proposal {
        match self {
            Self::FastPath { proposal, .. } | Self::Phase2 { proposal, .. } => proposal,
        }
    }

    pub fn validate_for(
        &self,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        let (proof_slot, proof_epoch, proof_config, digest, step, proposal, summaries, fast) =
            match self {
                Self::FastPath {
                    slot,
                    epoch,
                    config_id,
                    config_digest,
                    proposal,
                    summaries,
                    ..
                } => (
                    *slot,
                    *epoch,
                    *config_id,
                    *config_digest,
                    4,
                    proposal,
                    summaries,
                    true,
                ),
                Self::Phase2 {
                    slot,
                    epoch,
                    config_id,
                    config_digest,
                    step,
                    proposal,
                    summaries,
                    ..
                } => (
                    *slot,
                    *epoch,
                    *config_id,
                    *config_digest,
                    *step,
                    proposal,
                    summaries,
                    false,
                ),
            };
        if proof_slot != slot || proof_epoch != epoch {
            return Err(RejectReason::MalformedDecision);
        }
        if proof_config != config_id || digest != membership.digest() {
            return Err(RejectReason::WrongConfig);
        }
        if proposal.is_nil() || proposal.value.is_none() {
            return Err(RejectReason::InvalidCertificate);
        }
        if summaries.len() != membership.quorum_size()
            || !summaries
                .windows(2)
                .all(|pair| pair[0].recorder_id < pair[1].recorder_id)
            || summaries.iter().any(|summary| {
                !membership.contains(&summary.recorder_id)
                    || summary.slot != slot
                    || summary.step != step
            })
        {
            return Err(RejectReason::InvalidCertificate);
        }
        if fast {
            if step != 4
                || proposal.priority != ProposalPriority::MAX
                || summaries.iter().any(|summary| {
                    !summary
                        .first_current
                        .as_ref()
                        .is_some_and(|candidate| proposal_exact(candidate, proposal))
                })
            {
                return Err(RejectReason::InvalidCertificate);
            }
        } else {
            if step % 4 != 2 {
                return Err(RejectReason::InvalidCertificate);
            }
            let maximum = summaries
                .iter()
                .filter_map(|summary| summary.aggregate_prior.as_ref())
                .max();
            if maximum != Some(proposal)
                || !maximum.is_some_and(|candidate| proposal_exact(candidate, proposal))
            {
                return Err(RejectReason::InvalidCertificate);
            }
        }
        Ok(())
    }

    pub fn validate_for_cluster(
        &self,
        cluster_id: &str,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        if proof_cluster_id(self) != cluster_id {
            return Err(RejectReason::WrongCluster);
        }
        self.validate_for(slot, epoch, config_id, membership)
    }
}

fn proposal_exact(left: &Proposal, right: &Proposal) -> bool {
    left == right && left.value == right.value
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecordRequest {
    pub cluster_id: ClusterId,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub slot: Slot,
    pub step: Step,
    pub proposal: Proposal,
    #[serde(deserialize_with = "deserialize_required_command")]
    pub command: Option<StoredCommand>,
}

fn deserialize_required_command<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<StoredCommand>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    serde::Deserialize::deserialize(deserializer)
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecordSummary {
    pub recorder_id: NodeId,
    pub slot: Slot,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub step: Step,
    pub first_current: Option<Proposal>,
    pub aggregate_prior: Option<Proposal>,
    pub decided: Option<DecisionProof>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReadFenceRequest {
    pub cluster_id: ClusterId,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub slot: Slot,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum ReadFenceSlotState {
    Empty,
    /// The exact slot is present, or a durable later slot proves that this
    /// recorder has already crossed the requested position. A crossed gap has
    /// no exact summary and must therefore fail closed as pending.
    Occupied {
        summary: Option<Box<RecordSummary>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReadFenceObservation {
    pub recorder_id: NodeId,
    pub cluster_id: ClusterId,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub slot: Slot,
    pub max_head: Option<Slot>,
    pub slot_state: ReadFenceSlotState,
}

fn valid_read_fence_observation(
    observation: &ReadFenceObservation,
    expected_recorder_id: &str,
    request: &ReadFenceRequest,
) -> bool {
    if observation.recorder_id != expected_recorder_id
        || observation.cluster_id != request.cluster_id
        || observation.epoch != request.epoch
        || observation.config_id != request.config_id
        || observation.config_digest != request.config_digest
        || observation.slot != request.slot
    {
        return false;
    }
    match &observation.slot_state {
        ReadFenceSlotState::Empty => observation
            .max_head
            .is_none_or(|max_head| max_head < request.slot),
        ReadFenceSlotState::Occupied { summary } => {
            observation
                .max_head
                .is_some_and(|max_head| max_head >= request.slot)
                && summary.as_ref().is_none_or(|summary| {
                    summary.recorder_id == observation.recorder_id
                        && summary.slot == request.slot
                        && summary.config_id == request.config_id
                        && summary.config_digest == request.config_digest
                })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecordResponse {
    pub from: NodeId,
    pub slot: Slot,
    pub step: Step,
    pub highest_promised: Option<Ballot>,
    pub accepted: Option<AcceptedSummary>,
    pub recorder_epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecorderReply {
    pub recorder_id: NodeId,
    pub slot: Slot,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub step: Step,
    pub highest_promised: Option<Ballot>,
    pub accepted: Option<AcceptedSummary>,
    pub decided: Option<DecisionCertificate>,
    pub command: Option<StoredCommand>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RecorderRequest {
    Identity,
    StoreCommand {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    },
    FetchCommand {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
    },
    Inspect {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RejectReason {
    StaleEpoch,
    FutureEpoch,
    WrongCluster,
    WrongConfig,
    WrongSlot,
    AlreadyDecided,
    MalformedDecision,
    BallotPromised { promised: Ballot },
    ConflictingValue,
    InvalidValue,
    InvalidCertificate,
    ConfigurationSealed { stop_slot: Slot },
    ConfigurationNotInstalled,
    ActivationRequired,
    TransitionInProgress,
    InvalidTransition,
    LocalVoterRequired,
    StepRegression,
    InvalidRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationSeal {
    pub stop_slot: Slot,
    pub command_hash: LogHash,
    pub prefix_hash: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationState {
    config_id: ConfigId,
    config_digest: LogHash,
    membership: Option<Membership>,
    predecessor: Option<ConfigurationSeal>,
    seal: Option<ConfigurationSeal>,
    max_accepted_or_decided_slot: Option<Slot>,
    activated: bool,
}

impl ConfigurationState {
    pub const fn config_id(&self) -> ConfigId {
        self.config_id
    }

    pub const fn config_digest(&self) -> LogHash {
        self.config_digest
    }

    pub const fn membership(&self) -> Option<&Membership> {
        self.membership.as_ref()
    }

    pub const fn predecessor(&self) -> Option<&ConfigurationSeal> {
        self.predecessor.as_ref()
    }

    pub const fn seal(&self) -> Option<&ConfigurationSeal> {
        self.seal.as_ref()
    }

    pub const fn is_activated(&self) -> bool {
        self.activated
    }

    pub const fn max_accepted_or_decided_slot(&self) -> Option<Slot> {
        self.max_accepted_or_decided_slot
    }

    fn initial(
        config_id: ConfigId,
        config_digest: LogHash,
        membership: Option<Membership>,
    ) -> Self {
        Self {
            config_id,
            config_digest,
            membership,
            predecessor: None,
            seal: None,
            max_accepted_or_decided_slot: None,
            activated: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SealFaultPoint {
    AfterIntent,
    AfterSlot,
    AfterConfiguration,
    BeforeRecordManifest,
    AfterRecordManifest,
    AfterRecordCache,
    AfterHeadIntent,
    AfterHeadConfiguration,
    AfterHead,
    AfterWalWrite,
    AfterWalSync,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecorderSlotState {
    slot: Slot,
    cluster_id: ClusterId,
    epoch: Epoch,
    config_id: ConfigId,
    config_digest: LogHash,
    highest_promised: Option<Ballot>,
    accepted: Option<AcceptedSummary>,
    decided: Option<DecisionCertificate>,
    isr: IsrState,
    decided_proof: Option<DecisionProof>,
}

impl RecorderSlotState {
    pub fn new(
        slot: Slot,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Self {
        Self::new_with_digest(slot, cluster_id, epoch, config_id, LogHash::ZERO)
    }

    pub fn new_with_digest(
        slot: Slot,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
    ) -> Self {
        Self {
            slot,
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            config_digest,
            highest_promised: None,
            accepted: None,
            decided: None,
            isr: IsrState::default(),
            decided_proof: None,
        }
    }

    pub fn apply(
        &mut self,
        request: RecorderRequest,
    ) -> std::result::Result<RecorderReply, RejectReason> {
        match request {
            RecorderRequest::Inspect {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                slot,
            } => self.inspect(cluster_id, epoch, config_id, config_digest, slot),
            RecorderRequest::Identity
            | RecorderRequest::StoreCommand { .. }
            | RecorderRequest::FetchCommand { .. } => Err(RejectReason::InvalidRequest),
        }
    }

    pub fn decided(&self) -> Option<&DecisionCertificate> {
        self.decided.as_ref()
    }

    pub fn decision_proof(&self) -> Option<&DecisionProof> {
        self.decided_proof.as_ref()
    }

    pub fn isr(&self) -> &IsrState {
        &self.isr
    }

    pub fn record(
        &self,
        request: &RecordRequest,
    ) -> std::result::Result<(Self, IsrReply), RejectReason> {
        self.validate(
            request.cluster_id.clone(),
            request.epoch,
            request.config_id,
            request.config_digest,
            request.slot,
        )?;
        let mut next = self.clone();
        let (isr, reply) = self.isr.record(request.step, request.proposal.clone());
        next.isr = isr;
        Ok((next, reply))
    }

    fn install_proof(&mut self, proof: DecisionProof) -> std::result::Result<(), RejectReason> {
        if let Some(existing) = &self.decided_proof {
            if existing.proposal().value != proof.proposal().value {
                return Err(RejectReason::AlreadyDecided);
            }
            return Ok(());
        }
        self.decided_proof = Some(proof);
        Ok(())
    }

    pub const fn slot(&self) -> Slot {
        self.slot
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub const fn config_id(&self) -> ConfigId {
        self.config_id
    }

    pub const fn config_digest(&self) -> LogHash {
        self.config_digest
    }

    pub fn highest_promised(&self) -> Option<&Ballot> {
        self.highest_promised.as_ref()
    }

    pub fn accepted(&self) -> Option<&AcceptedSummary> {
        self.accepted.as_ref()
    }

    pub fn max_step_seen(&self) -> Step {
        self.highest_promised
            .as_ref()
            .map_or(0, |ballot| ballot.round)
    }

    fn inspect(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
    ) -> std::result::Result<RecorderReply, RejectReason> {
        self.validate(cluster_id, epoch, config_id, config_digest, slot)?;
        Ok(self.reply())
    }

    fn validate(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
    ) -> std::result::Result<(), RejectReason> {
        if cluster_id != self.cluster_id {
            return Err(RejectReason::WrongCluster);
        }
        if slot != self.slot {
            return Err(RejectReason::WrongSlot);
        }
        if epoch < self.epoch {
            return Err(RejectReason::StaleEpoch);
        }
        if epoch > self.epoch {
            return Err(RejectReason::FutureEpoch);
        }
        if config_id != self.config_id {
            return Err(RejectReason::WrongConfig);
        }
        if config_digest != self.config_digest {
            return Err(RejectReason::WrongConfig);
        }
        Ok(())
    }

    fn reply(&self) -> RecorderReply {
        RecorderReply {
            recorder_id: String::new(),
            slot: self.slot,
            config_id: self.config_id,
            config_digest: self.config_digest,
            step: self.max_step_seen(),
            highest_promised: self.highest_promised.clone(),
            accepted: self.accepted.clone(),
            decided: self.decided.clone(),
            command: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RecorderFileStore {
    root: PathBuf,
    recorder_id: NodeId,
    cluster_id: ClusterId,
    epoch: Epoch,
    config_id: ConfigId,
    config_digest: LogHash,
    configuration: Arc<Mutex<ConfigurationState>>,
    recorded_head: Arc<Mutex<RecordedHeadProvenance>>,
    recent_slots: Arc<Mutex<Vec<DurableSlotSnapshot>>>,
    wal: Arc<Mutex<RecorderWal>>,
    seal_fault: Arc<Mutex<Option<SealFaultPoint>>>,
    _root_lock: Arc<fs::File>,
    effect_root_anchor: Arc<anchored_fs::AnchoredDir>,
    staged_effect_pins: Arc<Mutex<HashMap<LogHash, EffectBundleGcPin>>>,
    sync: Arc<Mutex<()>>,
}

const RECORDED_HEAD_MAGIC: &[u8; 4] = b"QRHD";
const RECORDED_HEAD_VERSION: u16 = 3;
const RECORDER_WAL_MAGIC: &[u8; 4] = b"QWAL";
const RECORDER_WAL_VERSION: u16 = 1;
// Keep rotation bounded while allowing 512 KiB replicated commands to amortize
// quorum fsyncs without forcing a synchronous checkpoint every few dozen slots.
const RECORDER_WAL_SOFT_BYTE_LIMIT: u64 = 64 * 1024 * 1024;
#[cfg(not(test))]
const RECORDER_WAL_HARD_FRAME_LIMIT: u64 = 1_024;
#[cfg(test)]
const RECORDER_WAL_HARD_FRAME_LIMIT: u64 = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WalCheckpoint {
    generation: u64,
    through_sequence: u64,
}

impl Default for WalCheckpoint {
    fn default() -> Self {
        Self {
            generation: 1,
            through_sequence: 0,
        }
    }
}

#[derive(Debug)]
struct RecorderWal {
    checkpoint: WalCheckpoint,
    next_sequence: u64,
    last_digest: LogHash,
    frame_count: u64,
    byte_count: u64,
    slots: BTreeMap<Slot, Vec<u8>>,
    commands: HashMap<LogHash, StoredCommand>,
    file: Option<fs::File>,
    failed: bool,
}

// The persisted manifest is the canonical QEFX command itself.  Keep this
// read bound identical to the command decoder's bound so an accepted command
// can always be recovered and an oversized command is rejected before any
// effect chunk is written.
const MAX_EFFECT_BUNDLE_MANIFEST_BYTES: usize = rhiza_core::MAX_EXTERNAL_EFFECT_COMMAND_BYTES;
const EFFECT_CHUNK_PREFIX: &str = "effect-chunk-";
const EFFECT_BUNDLE_PREFIX: &str = "effect-bundle-";
const EFFECT_BUNDLE_GC_ANCHOR_FILE: &str = ".effect-bundle-gc-anchor.rec";
const EFFECT_BUNDLE_GC_ANCHOR_MAGIC: &[u8; 4] = b"QEGC";
const EFFECT_BUNDLE_GC_ANCHOR_VERSION: u16 = 1;
const MAX_EFFECT_BUNDLE_GC_ANCHOR_BYTES: usize = 4 * 1024;
const STORAGE_GENERATION_FILE: &str = ".rhiza-storage-generation";
const STORAGE_GENERATION_FINGERPRINT: &[u8] = b"rhiza:recorder:storage-generation:clean-v1\n";

#[cfg(test)]
struct RecorderPostPreflightHook {
    root: PathBuf,
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static RECORDER_POST_PREFLIGHT_HOOK: std::sync::OnceLock<Mutex<Option<RecorderPostPreflightHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
fn pause_after_recorder_anchor_open(root: &Path) {
    let hook = RECORDER_POST_PREFLIGHT_HOOK.get_or_init(|| Mutex::new(None));
    let selected = {
        let mut hook = lock_unpoison(hook);
        if hook
            .as_ref()
            .is_some_and(|candidate| candidate.root == root)
        {
            hook.take()
        } else {
            None
        }
    };
    if let Some(hook) = selected {
        let _ = hook.entered.send(());
        let _ = hook.release.recv();
    }
}

/// The consensus context an immutable SQL page-effect is allowed to serve.
///
/// The binding makes an otherwise reusable content-addressed blob specific to
/// exactly one proposed log position and its small QWAL manifest command.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct EffectBundleBinding {
    pub cluster_id: ClusterId,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub intended_slot: Slot,
    pub prev_hash: LogHash,
    pub manifest_command_hash: LogHash,
    pub effect_digest: LogHash,
}

/// Stable filename key for a finalized QEFX manifest.
///
/// Chunks are content-addressed by their own digest and may intentionally be
/// shared. A manifest is instead bound to one exact consensus position, so
/// naming it by `effect_digest` alone would make equal bytes at two slots
/// collide. Length-prefix the only variable-width field to keep this encoding
/// unambiguous without depending on a serializer's wire-version policy.
fn effect_bundle_binding_digest(binding: &EffectBundleBinding) -> LogHash {
    let cluster_len = u64::try_from(binding.cluster_id.len())
        .expect("cluster identifier length fits u64")
        .to_be_bytes();
    let epoch = binding.epoch.to_be_bytes();
    let config_id = binding.config_id.to_be_bytes();
    let intended_slot = binding.intended_slot.to_be_bytes();
    LogHash::digest(&[
        b"rhiza.qefx.effect-bundle-binding.v1\0",
        &cluster_len,
        binding.cluster_id.as_bytes(),
        &epoch,
        &config_id,
        binding.config_digest.as_bytes(),
        &intended_slot,
        binding.prev_hash.as_bytes(),
        binding.manifest_command_hash.as_bytes(),
        binding.effect_digest.as_bytes(),
    ])
}

/// A protected QEFX registration. Pins cover work that is active/inflight as
/// well as successor or reconfiguration hand-off work, whose chunks may not
/// yet be reachable from a finalized manifest.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct EffectBundleGcPin {
    pub binding: EffectBundleBinding,
    pub manifest_command: StoredCommand,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct EffectBundleGcOutcome {
    pub previous_anchor: Option<Slot>,
    pub current_anchor: Slot,
    pub removed_manifests: usize,
    pub removed_chunks: usize,
    /// True when no additional object eligible under `current_anchor` remains.
    pub sweep_complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EffectBundleGcAnchor {
    cluster_id: ClusterId,
    epoch: Epoch,
    through_slot: Slot,
    tip_hash: LogHash,
    manifest_digest: LogHash,
}

#[derive(Default)]
struct EffectBundleGcProtected {
    bindings: HashSet<LogHash>,
    chunks: HashSet<LogHash>,
}

/// An immutable, ordered set of content-addressed effect chunks.
///
/// This type is intentionally separate from [`StoredCommand`]. Its eventual
/// consensus representation is a bounded manifest; the bytes themselves are
/// finalized through the Recorder effect-bundle namespace first.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecorderEffectBundle {
    binding: EffectBundleBinding,
    chunks: Vec<Vec<u8>>,
    chunk_hashes: Vec<LogHash>,
    chunk_lengths: Vec<u32>,
    total_len: usize,
}

/// Exact small command that commits the large bundle's ordered chunk sequence.
/// A Recorder may acknowledge finalization only when this command's hash is
/// the binding's `manifest_command_hash`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectBundleFinalizeRequest {
    pub bundle: RecorderEffectBundle,
    pub manifest_command: StoredCommand,
}

impl RecorderEffectBundle {
    pub fn effect_digest_for_chunks(chunks: &[Vec<u8>]) -> Result<LogHash> {
        validate_effect_chunks(chunks)?;
        let hashes = chunks
            .iter()
            .map(|chunk| effect_chunk_digest(chunk))
            .collect::<Vec<_>>();
        let lengths = chunks
            .iter()
            .map(|chunk| u32::try_from(chunk.len()).expect("effect chunk limit fits u32"))
            .collect::<Vec<_>>();
        Ok(effect_digest(
            &hashes,
            &lengths,
            chunks.iter().map(Vec::len).sum(),
        ))
    }

    pub fn new(binding: EffectBundleBinding, chunks: Vec<Vec<u8>>) -> Result<Self> {
        validate_effect_chunks(&chunks)?;
        let chunk_hashes = chunks
            .iter()
            .map(|chunk| effect_chunk_digest(chunk))
            .collect::<Vec<_>>();
        let chunk_lengths = chunks
            .iter()
            .map(|chunk| u32::try_from(chunk.len()).expect("effect chunk limit fits u32"))
            .collect::<Vec<_>>();
        let total_len = chunks.iter().map(Vec::len).sum::<usize>();
        let effect_digest = effect_digest(&chunk_hashes, &chunk_lengths, total_len);
        if effect_digest != binding.effect_digest {
            return Err(Error::EffectBundleInvalid(
                "full effect digest does not match its binding".into(),
            ));
        }
        Ok(Self {
            binding,
            chunks,
            chunk_hashes,
            chunk_lengths,
            total_len,
        })
    }

    pub fn binding(&self) -> &EffectBundleBinding {
        &self.binding
    }

    pub fn chunk_hashes(&self) -> &[LogHash] {
        &self.chunk_hashes
    }

    pub const fn total_len(&self) -> usize {
        self.total_len
    }

    pub fn chunks(&self) -> &[Vec<u8>] {
        &self.chunks
    }
}

impl EffectBundleFinalizeRequest {
    pub fn new(bundle: RecorderEffectBundle, manifest_command: StoredCommand) -> Result<Self> {
        let qefx = ExternalEffectCommand::decode(&manifest_command.payload).map_err(|error| {
            Error::EffectBundleInvalid(format!("manifest command is not canonical QEFX: {error}"))
        })?;
        let expected_chunks = bundle
            .chunk_hashes
            .iter()
            .zip(&bundle.chunk_lengths)
            .map(|(digest, encoded_len)| ExternalEffectChunk::new(*digest, *encoded_len))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| Error::EffectBundleInvalid(error.to_string()))?;
        if manifest_command.entry_type != EntryType::Command
            || manifest_command.hash() != bundle.binding.manifest_command_hash
            || qefx.cluster_id() != bundle.binding.cluster_id
            || qefx.epoch() != bundle.binding.epoch
            || qefx.config_id() != bundle.binding.config_id
            || qefx.config_digest() != bundle.binding.config_digest
            || qefx.intended_slot() != bundle.binding.intended_slot
            || qefx.prev_hash() != bundle.binding.prev_hash
            || qefx.effect_digest_value() != bundle.binding.effect_digest
            || qefx.chunks() != expected_chunks
        {
            return Err(Error::EffectBundleInvalid(
                "manifest command does not exactly commit this effect bundle".into(),
            ));
        }
        Ok(Self {
            bundle,
            manifest_command,
        })
    }
}

fn verified_effect_bundle_command(
    binding: &EffectBundleBinding,
    manifest_command: &StoredCommand,
) -> Result<ExternalEffectCommand> {
    let qefx = ExternalEffectCommand::decode(&manifest_command.payload).map_err(|error| {
        Error::EffectBundleInvalid(format!("manifest command is not canonical QEFX: {error}"))
    })?;
    if manifest_command.entry_type != EntryType::Command
        || manifest_command.hash() != binding.manifest_command_hash
        || qefx.cluster_id() != binding.cluster_id
        || qefx.epoch() != binding.epoch
        || qefx.config_id() != binding.config_id
        || qefx.config_digest() != binding.config_digest
        || qefx.intended_slot() != binding.intended_slot
        || qefx.prev_hash() != binding.prev_hash
        || qefx.effect_digest_value() != binding.effect_digest
    {
        return Err(Error::EffectBundleInvalid(
            "manifest command does not exactly commit this effect bundle".into(),
        ));
    }
    Ok(qefx)
}

fn validate_effect_chunks(chunks: &[Vec<u8>]) -> Result<()> {
    if chunks.is_empty() {
        return Err(Error::EffectBundleInvalid("bundle has no chunks".into()));
    }
    if chunks.len() > MAX_EFFECT_BUNDLE_CHUNKS {
        return Err(Error::EffectBundleInvalid(format!(
            "bundle has {} chunks; limit is {MAX_EFFECT_BUNDLE_CHUNKS}",
            chunks.len()
        )));
    }
    let mut total = 0usize;
    for (index, chunk) in chunks.iter().enumerate() {
        if chunk.is_empty() || chunk.len() > MAX_EFFECT_BUNDLE_CHUNK_BYTES {
            return Err(Error::EffectBundleInvalid(format!(
                "chunk {index} has {} bytes; each chunk must be 1..={MAX_EFFECT_BUNDLE_CHUNK_BYTES} bytes",
                chunk.len()
            )));
        }
        total = total
            .checked_add(chunk.len())
            .ok_or_else(|| Error::EffectBundleInvalid("bundle length overflow".into()))?;
    }
    if total > MAX_EFFECT_BUNDLE_BYTES {
        return Err(Error::EffectBundleInvalid(format!(
            "bundle has {total} bytes; limit is {MAX_EFFECT_BUNDLE_BYTES}"
        )));
    }
    Ok(())
}

fn effect_chunk_digest(chunk: &[u8]) -> LogHash {
    ExternalEffectCommand::chunk_digest(chunk)
}

fn effect_digest(chunk_hashes: &[LogHash], chunk_lengths: &[u32], total_len: usize) -> LogHash {
    let chunks = chunk_hashes
        .iter()
        .zip(chunk_lengths)
        .map(|(digest, length)| ExternalEffectChunk::new(*digest, *length))
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("validated effect chunks fit the core envelope");
    let (actual_len, digest) = ExternalEffectCommand::effect_digest(&chunks)
        .expect("validated effect chunks fit the core envelope");
    assert_eq!(actual_len as usize, total_len);
    digest
}

fn encode_effect_bundle_gc_anchor(anchor: &EffectBundleGcAnchor) -> Result<Vec<u8>> {
    let cluster = anchor.cluster_id.as_bytes();
    let cluster_len = u16::try_from(cluster.len())
        .map_err(|_| Error::EffectBundleInvalid("GC anchor cluster id is too long".into()))?;
    let mut out = Vec::with_capacity(4 + 2 + 2 + cluster.len() + 8 * 2 + 32 * 2);
    out.extend_from_slice(EFFECT_BUNDLE_GC_ANCHOR_MAGIC);
    out.extend_from_slice(&EFFECT_BUNDLE_GC_ANCHOR_VERSION.to_be_bytes());
    out.extend_from_slice(&cluster_len.to_be_bytes());
    out.extend_from_slice(cluster);
    out.extend_from_slice(&anchor.epoch.to_be_bytes());
    out.extend_from_slice(&anchor.through_slot.to_be_bytes());
    out.extend_from_slice(anchor.tip_hash.as_bytes());
    out.extend_from_slice(anchor.manifest_digest.as_bytes());
    Ok(out)
}

fn decode_effect_bundle_gc_anchor(bytes: &[u8]) -> Result<EffectBundleGcAnchor> {
    let minimum = 4 + 2 + 2 + 8 + 8 + 32 + 32;
    if bytes.len() < minimum || &bytes[..4] != EFFECT_BUNDLE_GC_ANCHOR_MAGIC {
        return Err(Error::EffectBundleInvalid(
            "invalid effect GC anchor".into(),
        ));
    }
    let version = u16::from_be_bytes([bytes[4], bytes[5]]);
    if version != EFFECT_BUNDLE_GC_ANCHOR_VERSION {
        return Err(Error::EffectBundleInvalid(
            "unsupported effect GC anchor version".into(),
        ));
    }
    let cluster_len = usize::from(u16::from_be_bytes([bytes[6], bytes[7]]));
    let expected = minimum
        .checked_add(cluster_len)
        .ok_or_else(|| Error::EffectBundleInvalid("effect GC anchor length overflow".into()))?;
    if bytes.len() != expected {
        return Err(Error::EffectBundleInvalid(
            "invalid effect GC anchor length".into(),
        ));
    }
    let mut cursor = 8;
    let cluster_end = cursor + cluster_len;
    let cluster_id = std::str::from_utf8(&bytes[cursor..cluster_end])
        .map_err(|_| Error::EffectBundleInvalid("invalid effect GC anchor cluster id".into()))?
        .to_owned();
    cursor = cluster_end;
    let read_u64 = |bytes: &[u8], cursor: &mut usize| {
        let end = *cursor + 8;
        let value = u64::from_be_bytes(bytes[*cursor..end].try_into().expect("bounded anchor"));
        *cursor = end;
        value
    };
    let epoch = read_u64(bytes, &mut cursor);
    let through_slot = read_u64(bytes, &mut cursor);
    let tip_hash = LogHash::from_bytes(
        bytes[cursor..cursor + 32]
            .try_into()
            .expect("bounded anchor"),
    );
    cursor += 32;
    let manifest_digest = LogHash::from_bytes(
        bytes[cursor..cursor + 32]
            .try_into()
            .expect("bounded anchor"),
    );
    Ok(EffectBundleGcAnchor {
        cluster_id,
        epoch,
        through_slot,
        tip_hash,
        manifest_digest,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EffectBundleManifest {
    binding: EffectBundleBinding,
    chunk_hashes: Vec<LogHash>,
    chunk_lengths: Vec<u32>,
    total_len: usize,
}

fn encode_effect_bundle(command: &StoredCommand) -> Result<Vec<u8>> {
    ExternalEffectCommand::decode(&command.payload)
        .map_err(|error| Error::EffectBundleInvalid(format!("invalid QEFX command: {error}")))?;
    Ok(command.payload.clone())
}

fn decode_effect_bundle(bytes: &[u8]) -> Result<EffectBundleManifest> {
    let command = ExternalEffectCommand::decode(bytes).map_err(|error| {
        Error::EffectBundleInvalid(format!("invalid persisted QEFX command: {error}"))
    })?;
    let stored = StoredCommand::new(EntryType::Command, bytes.to_vec());
    Ok(EffectBundleManifest {
        binding: EffectBundleBinding {
            cluster_id: command.cluster_id().into(),
            epoch: command.epoch(),
            config_id: command.config_id(),
            config_digest: command.config_digest(),
            intended_slot: command.intended_slot(),
            prev_hash: command.prev_hash(),
            manifest_command_hash: stored.hash(),
            effect_digest: command.effect_digest_value(),
        },
        chunk_hashes: command
            .chunks()
            .iter()
            .map(|chunk| chunk.digest())
            .collect(),
        chunk_lengths: command
            .chunks()
            .iter()
            .map(|chunk| chunk.encoded_len())
            .collect(),
        total_len: command.total_effect_bytes() as usize,
    })
}

impl Default for RecorderWal {
    fn default() -> Self {
        Self {
            checkpoint: WalCheckpoint::default(),
            next_sequence: 1,
            last_digest: LogHash::ZERO,
            frame_count: 0,
            byte_count: 0,
            slots: BTreeMap::new(),
            commands: HashMap::new(),
            file: None,
            failed: false,
        }
    }
}

#[derive(Debug)]
struct WalFrame {
    generation: u64,
    sequence: u64,
    prev_digest: LogHash,
    digest: LogHash,
    slot: Slot,
    slot_bytes: Vec<u8>,
    configuration_bytes: Vec<u8>,
    head: RecordedHeadProvenance,
    command: Option<(LogHash, StoredCommand)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableSlotSnapshot {
    slot: Slot,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RecordedHeadProvenance {
    Empty,
    SlotBacked {
        slot: Slot,
    },
    CheckpointBacked {
        stop_slot: Slot,
        prefix_hash: LogHash,
        recovered_tip: Slot,
        recovered_hash: LogHash,
    },
}

pub trait RecorderRpc: Send + Sync {
    /// Performs one genuine QuePaxa Record operation.
    ///
    /// Implementations must enforce `context.deadline()` and stop I/O when
    /// `context.is_cancelled()` becomes true.
    fn record(
        &self,
        _context: &RecorderRpcContext,
        _request: RecordRequest,
    ) -> Result<RecordSummary> {
        Err(Error::TypedRecordRequired)
    }

    /// Installs a verified decision proof durably.
    ///
    /// An `Ok(())` must mean the proof survives recorder recovery; ordinary
    /// FastPath and Phase2 acknowledgements rely on a quorum of these durable
    /// successes.
    fn install_decision_proof(
        &self,
        _context: &RecorderRpcContext,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> Result<()> {
        Err(Error::TypedProofInstallRequired)
    }

    fn inspect_decision_proof(
        &self,
        _context: &RecorderRpcContext,
        _slot: Slot,
    ) -> Result<Option<DecisionProof>> {
        Ok(None)
    }

    fn inspect_record_summary(
        &self,
        _context: &RecorderRpcContext,
        _slot: Slot,
    ) -> Result<Option<RecordSummary>> {
        Err(Error::TypedRecordRequired)
    }

    /// Whether this recorder can atomically bind an exact slot observation to
    /// the durable maximum accepted-or-decided head and the requested config.
    fn supports_context_read_fence(&self) -> bool {
        false
    }

    fn observe_read_fence(
        &self,
        _context: &RecorderRpcContext,
        _request: ReadFenceRequest,
    ) -> Result<ReadFenceObservation> {
        Err(Error::ReadFenceUnsupported)
    }

    fn recorder_id(&self, _context: &RecorderRpcContext) -> Result<NodeId> {
        Err(Error::TypedRecordRequired)
    }

    #[allow(clippy::too_many_arguments)] // mirrors the durable recorder identity tuple
    /// Stores the exact command bytes durably under their verified content hash.
    ///
    /// An `Ok(())` must mean the same `(command_hash, command)` survives recorder
    /// recovery. Repeating the same pair must be idempotent, while a mismatched
    /// hash or different bytes under an existing hash must fail closed.
    fn store_command_for(
        &self,
        context: &RecorderRpcContext,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> Result<()> {
        let _ = (
            context,
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        );
        Err(Error::TypedRecordRequired)
    }

    fn fetch_command_for(
        &self,
        context: &RecorderRpcContext,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> Result<Option<StoredCommand>> {
        let _ = (
            context,
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
        );
        Err(Error::TypedRecordRequired)
    }

    /// Durably stages one bounded QEFX effect chunk. This is not a final ACK:
    /// callers must receive a subsequent `finalize_staged_effect_bundle` ACK
    /// before considering the effect available for proposal.
    fn stage_effect_bundle_chunk(
        &self,
        _context: &RecorderRpcContext,
        _binding: EffectBundleBinding,
        _manifest_command: StoredCommand,
        _ordinal: u16,
        _chunk: Vec<u8>,
    ) -> Result<()> {
        Err(Error::TypedRecordRequired)
    }

    /// Makes previously staged chunks reachable only after full QEFX
    /// verification and durable manifest installation.
    fn finalize_staged_effect_bundle(
        &self,
        _context: &RecorderRpcContext,
        _binding: EffectBundleBinding,
        _manifest_command: StoredCommand,
    ) -> Result<()> {
        Err(Error::TypedRecordRequired)
    }

    fn fetch_effect_bundle_manifest(
        &self,
        _context: &RecorderRpcContext,
        _binding: EffectBundleBinding,
    ) -> Result<Option<StoredCommand>> {
        Err(Error::TypedRecordRequired)
    }

    fn fetch_effect_bundle_chunk(
        &self,
        _context: &RecorderRpcContext,
        _binding: EffectBundleBinding,
        _ordinal: u16,
    ) -> Result<Option<Vec<u8>>> {
        Err(Error::TypedRecordRequired)
    }
}

impl RecorderFileStore {
    /// Direct local-store access for embedding and diagnostic code. Distributed
    /// callers must use the context-bearing [`RecorderRpc`] interface.
    pub fn recorder_id(&self) -> Result<NodeId> {
        Ok(self.recorder_id.clone())
    }

    pub fn record(&self, request: RecordRequest) -> Result<RecordSummary> {
        self.record_proposal(request)
    }

    pub fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<()> {
        self.install_decision_proof_record(proof, membership)
    }

    pub fn inspect_decision_proof(&self, slot: Slot) -> Result<Option<DecisionProof>> {
        Ok(self.load(slot)?.decision_proof().cloned())
    }

    pub fn inspect_record_summary(&self, slot: Slot) -> Result<Option<RecordSummary>> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        let exists_in_wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?
            .slots
            .contains_key(&slot);
        if !exists_in_wal && !self.effect_root_anchor.exists(&Self::slot_name(slot))? {
            return Ok(None);
        }
        let state = self.load_unlocked(slot, configuration.config_digest)?;
        Ok(Some(record_summary(
            &self.recorder_id,
            &state,
            state.decision_proof().cloned(),
        )))
    }

    pub fn store_command_for(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> Result<()> {
        self.apply(RecorderRequest::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        })?;
        Ok(())
    }

    pub fn fetch_command_for(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> Result<Option<StoredCommand>> {
        Ok(self
            .apply(RecorderRequest::FetchCommand {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            })?
            .command)
    }

    /// Classifies an existing recorder root without creating, recovering, truncating, or
    /// rewriting local state. `Recoverable` is limited to durable normal-crash artifacts that a
    /// subsequent locked open knows how to finish.
    #[doc(hidden)]
    pub fn preflight_existing_with_membership_outcome(
        root: impl AsRef<Path>,
        cluster_id: &str,
        epoch: Epoch,
        config_id: ConfigId,
        membership: &Membership,
    ) -> Result<RecorderPreflight> {
        let root = root.as_ref();
        let metadata = match fs::symlink_metadata(root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(RecorderPreflight::Missing);
            }
            Err(error) => return Err(Error::Io(error.to_string())),
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Error::Decode(
                "recorder root must be a real directory".into(),
            ));
        }
        let entries = fs::read_dir(root)
            .map_err(|error| Error::Io(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| Error::Io(error.to_string()))?;
        if entries.is_empty()
            || entries.iter().all(|entry| {
                entry.file_name() == ".recorder.lock"
                    || entry.file_name() == STORAGE_GENERATION_FILE
            })
        {
            return Ok(RecorderPreflight::Missing);
        }
        let transition_intent =
            read_preflight_intent(root, "configuration.intent", MAX_TRANSITION_INTENT_BYTES)?;
        let configuration_head_intent = read_preflight_intent(
            root,
            "configuration-head.intent",
            MAX_CONFIGURATION_HEAD_INTENT_BYTES,
        )?;
        match (transition_intent, configuration_head_intent) {
            (Some(_), Some(_)) => {
                return Err(Error::Decode(
                    "recorder has conflicting recovery intents".into(),
                ));
            }
            (Some(bytes), None) => {
                validate_recoverable_transition_intent(
                    root, &bytes, cluster_id, epoch, config_id, membership,
                )?;
                validate_empty_recovery_wal(root)?;
                return Ok(RecorderPreflight::Recoverable);
            }
            (None, Some(bytes)) => {
                validate_recoverable_configuration_head_intent(
                    root, &bytes, cluster_id, epoch, config_id, membership,
                )?;
                validate_empty_recovery_wal(root)?;
                return Ok(RecorderPreflight::Recoverable);
            }
            (None, None) => {}
        }
        validate_current_recorder_layout(root)?;
        let configuration = decode_configuration_state(&read_regular_file_bounded(
            &root.join("configuration.rec"),
            MAX_CONFIGURATION_BYTES,
            "configuration.rec",
        )?)?;
        validate_preflight_configuration(&configuration, config_id, membership)?;
        let (head, recent_slots, checkpoint) = decode_recorded_head(
            &read_regular_file_bounded(
                &root.join("recorded-head.rec"),
                MAX_RECORDED_HEAD_BYTES,
                "recorded-head.rec",
            )?,
            cluster_id,
            epoch,
            &configuration,
        )?;
        validate_existing_snapshots(root, &recent_slots, cluster_id, epoch, &configuration)?;
        let torn_wal = validate_existing_wal(
            root,
            &read_regular_file_bounded(
                &root.join("recorder.wal"),
                MAX_RECORDER_WAL_BYTES,
                "recorder.wal",
            )?,
            cluster_id,
            epoch,
            &configuration,
            &head,
            checkpoint,
        )?;
        Ok(if torn_wal {
            RecorderPreflight::Recoverable
        } else {
            RecorderPreflight::Valid
        })
    }

    pub fn new(
        root: impl Into<PathBuf>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Result<Self> {
        let root = root.into();
        let recorder_id = root
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("recorder")
            .to_string();
        Self::new_with_id(root, recorder_id, cluster_id, epoch, config_id)
    }

    pub fn new_with_id(
        root: impl Into<PathBuf>,
        recorder_id: impl Into<NodeId>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Result<Self> {
        let root = root.into();
        let (store, existing_format) =
            Self::open_root(root, recorder_id, cluster_id, epoch, config_id)?;
        store.effect_root_anchor.verify_path(&store.root)?;
        if current_recorder_layout(&store.root)? != existing_format {
            return Err(Error::Decode(
                "recorder layout changed while opening".into(),
            ));
        }
        store.open_or_initialize_recorded_head(existing_format)?;
        store.open_or_replay_wal()?;
        Ok(store)
    }

    fn open_root(
        root: impl Into<PathBuf>,
        recorder_id: impl Into<NodeId>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Result<(Self, bool)> {
        let root = root.into();
        let recorder_id = recorder_id.into();
        if recorder_id.is_empty() {
            return Err(Error::EmptyRecorderIdentity);
        }
        let effect_root_anchor = Arc::new(prepare_fresh_recorder_root(&root)?);
        ensure_storage_generation(&effect_root_anchor)?;
        effect_root_anchor.verify_path(&root)?;
        if current_recorder_layout(&root)? {
            return Self::open_existing_root(
                root,
                recorder_id,
                cluster_id.into(),
                epoch,
                config_id,
                effect_root_anchor,
            );
        }
        let root_lock = effect_root_anchor.open_lock_or_create()?;
        if current_recorder_layout(&root)? {
            return Err(Error::Decode(
                "recorder layout changed while opening".into(),
            ));
        }
        match root_lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(Error::RecorderRootLocked(root));
            }
            Err(fs::TryLockError::Error(err)) => return Err(Error::Io(err.to_string())),
        }
        Ok((
            Self {
                root,
                recorder_id,
                cluster_id: cluster_id.into(),
                epoch,
                config_id,
                config_digest: LogHash::ZERO,
                configuration: Arc::new(Mutex::new(ConfigurationState::initial(
                    config_id,
                    LogHash::ZERO,
                    None,
                ))),
                recorded_head: Arc::new(Mutex::new(RecordedHeadProvenance::Empty)),
                recent_slots: Arc::new(Mutex::new(Vec::new())),
                wal: Arc::new(Mutex::new(RecorderWal::default())),
                seal_fault: Arc::new(Mutex::new(None)),
                _root_lock: Arc::new(root_lock),
                effect_root_anchor,
                staged_effect_pins: Arc::new(Mutex::new(HashMap::new())),
                sync: Arc::new(Mutex::new(())),
            },
            false,
        ))
    }

    fn open_existing_root(
        root: PathBuf,
        recorder_id: NodeId,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        effect_root_anchor: Arc<anchored_fs::AnchoredDir>,
    ) -> Result<(Self, bool)> {
        ensure_storage_generation(&effect_root_anchor)?;
        effect_root_anchor.verify_path(&root)?;
        let root_metadata = match fs::symlink_metadata(&root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(Error::Decode("recorder root does not exist".into()));
            }
            Err(error) => return Err(Error::Io(error.to_string())),
        };
        if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
            return Err(Error::Decode(
                "recorder root must be an existing real directory".into(),
            ));
        }
        if recorder_id.is_empty() {
            return Err(Error::EmptyRecorderIdentity);
        }
        let root_lock = effect_root_anchor.open_lock()?;
        match root_lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(Error::RecorderRootLocked(root));
            }
            Err(fs::TryLockError::Error(error)) => return Err(Error::Io(error.to_string())),
        }
        Ok((
            Self {
                root,
                recorder_id,
                cluster_id,
                epoch,
                config_id,
                config_digest: LogHash::ZERO,
                configuration: Arc::new(Mutex::new(ConfigurationState::initial(
                    config_id,
                    LogHash::ZERO,
                    None,
                ))),
                recorded_head: Arc::new(Mutex::new(RecordedHeadProvenance::Empty)),
                recent_slots: Arc::new(Mutex::new(Vec::new())),
                wal: Arc::new(Mutex::new(RecorderWal::default())),
                seal_fault: Arc::new(Mutex::new(None)),
                _root_lock: Arc::new(root_lock),
                effect_root_anchor,
                staged_effect_pins: Arc::new(Mutex::new(HashMap::new())),
                sync: Arc::new(Mutex::new(())),
            },
            true,
        ))
    }

    pub fn new_with_membership(
        root: impl Into<PathBuf>,
        recorder_id: impl Into<NodeId>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        membership: Membership,
    ) -> Result<Self> {
        Self::open_with_membership(
            root.into(),
            recorder_id.into(),
            cluster_id.into(),
            epoch,
            config_id,
            membership,
            false,
        )
    }

    /// Opens a recorder that must already exist. Unlike `new_with_membership`, this path never
    /// creates a directory, lock file, or fresh recorder state.
    #[doc(hidden)]
    pub fn open_existing_with_membership(
        root: impl Into<PathBuf>,
        recorder_id: impl Into<NodeId>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        membership: Membership,
    ) -> Result<Self> {
        Self::open_with_membership(
            root.into(),
            recorder_id.into(),
            cluster_id.into(),
            epoch,
            config_id,
            membership,
            true,
        )
    }

    fn open_with_membership(
        root: PathBuf,
        recorder_id: NodeId,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        membership: Membership,
        existing_only: bool,
    ) -> Result<Self> {
        let preflight = Self::preflight_existing_with_membership_outcome(
            &root,
            &cluster_id,
            epoch,
            config_id,
            &membership,
        )?;
        if existing_only && preflight == RecorderPreflight::Missing {
            return Err(Error::Decode(
                "recorder root is missing durable state".into(),
            ));
        }
        let existing_format = preflight != RecorderPreflight::Missing;
        let (mut store, _) = if existing_format {
            let effect_root_anchor = Arc::new(anchored_fs::AnchoredDir::open(&root)?);
            let opened = Self::open_existing_root(
                root,
                recorder_id,
                cluster_id,
                epoch,
                config_id,
                effect_root_anchor,
            )?;
            #[cfg(test)]
            pause_after_recorder_anchor_open(&opened.0.root);
            opened
        } else {
            Self::open_root(root, recorder_id, cluster_id, epoch, config_id)?
        };
        let revalidated = Self::preflight_existing_with_membership_outcome(
            &store.root,
            &store.cluster_id,
            store.epoch,
            config_id,
            &membership,
        )?;
        if revalidated != preflight {
            return Err(Error::Decode(
                "recorder layout changed while opening".into(),
            ));
        }
        store.recover_configuration_head_intent()?;
        let configured = match store.effect_root_anchor.read_optional(
            Self::CONFIGURATION_FILE,
            MAX_CONFIGURATION_BYTES,
            "configuration.rec",
        )? {
            Some(bytes) => decode_configuration_state(&bytes)?,
            None => {
                let configured =
                    ConfigurationState::initial(config_id, membership.digest(), Some(membership));
                store.commit_configuration_head_unlocked(
                    &configured,
                    &RecordedHeadProvenance::Empty,
                )?;
                configured
            }
        };
        if configured
            .membership
            .as_ref()
            .is_some_and(|current| current.digest() != configured.config_digest)
        {
            return Err(Error::Decode("installed membership digest mismatch".into()));
        }
        store.config_id = configured.config_id;
        store.config_digest = configured.config_digest;
        store.configuration = Arc::new(Mutex::new(configured));
        store.recover_intent()?;
        store.open_or_initialize_recorded_head(existing_format)?;
        store.open_or_replay_wal()?;
        Ok(store)
    }

    pub fn configuration_state(&self) -> Result<ConfigurationState> {
        self.configuration
            .lock()
            .map(|state| state.clone())
            .map_err(|_| Error::Io("configuration lock poisoned".into()))
    }

    #[doc(hidden)]
    pub fn set_seal_fault(&self, fault: Option<SealFaultPoint>) -> Result<()> {
        *self
            .seal_fault
            .lock()
            .map_err(|_| Error::Io("seal fault lock poisoned".into()))? = fault;
        Ok(())
    }

    pub fn install_successor(
        &self,
        _next_config_id: ConfigId,
        _membership: Membership,
        _stop_certificate: &DecisionCertificate,
        _stop_slot: Slot,
        _prefix_hash: LogHash,
    ) -> Result<ConfigurationState> {
        Err(Error::Rejected(RejectReason::InvalidTransition))
    }

    pub fn install_successor_from_proof(
        &self,
        membership: Membership,
        stop_proof: &DecisionProof,
    ) -> Result<ConfigurationState> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let current = self.configuration_state()?;
        let Some(old_membership) = current.membership.as_ref() else {
            return Err(Error::Rejected(RejectReason::ConfigurationNotInstalled));
        };
        let next_config_id = current
            .config_id
            .checked_add(1)
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?;
        if current.predecessor.is_some() && !current.activated {
            return Err(Error::Rejected(RejectReason::TransitionInProgress));
        }
        if !membership.contains(&self.recorder_id) {
            return Err(Error::Rejected(RejectReason::LocalVoterRequired));
        }
        if proof_cluster_id(stop_proof) != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        let (stop_slot, epoch, config_id, config_digest) = proof_context(stop_proof);
        if epoch != self.epoch
            || config_id != current.config_id
            || config_digest != current.config_digest
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        stop_proof
            .validate_for_cluster(
                &self.cluster_id,
                stop_slot,
                self.epoch,
                current.config_id,
                old_membership,
            )
            .map_err(Error::Rejected)?;
        let stop_command = ConfigChange::bound_stop(
            self.cluster_id.clone(),
            current.config_id,
            current.config_digest,
            next_config_id,
            membership.members().to_vec(),
        )
        .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?
        .to_stored_command();
        let stop_value = stop_proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        let expected_stop = AcceptedValue::from_command(
            &self.cluster_id,
            stop_slot,
            self.epoch,
            current.config_id,
            stop_value.prev_hash,
            &stop_command,
        );
        if *stop_value != expected_stop {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        let prefix_hash = expected_stop.entry_hash;
        let expected_seal = ConfigurationSeal {
            stop_slot,
            command_hash: expected_stop.command_hash,
            prefix_hash,
        };
        if current
            .seal
            .as_ref()
            .is_some_and(|seal| seal != &expected_seal)
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        self.checkpoint_wal_unlocked()?;
        self.store_command_unlocked(expected_stop.command_hash, &stop_command)?;
        let installed = ConfigurationState {
            config_id: next_config_id,
            config_digest: membership.digest(),
            membership: Some(membership),
            predecessor: Some(expected_seal),
            seal: None,
            max_accepted_or_decided_slot: None,
            activated: false,
        };
        let head = RecordedHeadProvenance::Empty;
        self.commit_configuration_head_unlocked(&installed, &head)?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = installed.clone();
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(installed)
    }

    pub fn recover_successor_activation_from_checkpoint(
        &self,
        stop_slot: Slot,
        prefix_hash: LogHash,
        recovered_tip: Slot,
        recovered_hash: LogHash,
    ) -> Result<ConfigurationState> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let current = self.configuration_state()?;
        let predecessor = current
            .predecessor
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?;
        if predecessor.stop_slot != stop_slot
            || predecessor.prefix_hash != prefix_hash
            || recovered_tip <= stop_slot
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        if current.activated {
            if current
                .max_accepted_or_decided_slot
                .is_some_and(|slot| slot > recovered_tip)
            {
                return Err(Error::Rejected(RejectReason::InvalidTransition));
            }
            return Ok(current);
        }
        let mut recovered = current;
        recovered.activated = true;
        recovered.max_accepted_or_decided_slot = Some(recovered_tip);
        let head = RecordedHeadProvenance::CheckpointBacked {
            stop_slot,
            prefix_hash,
            recovered_tip,
            recovered_hash,
        };
        self.checkpoint_wal_unlocked()?;
        self.commit_configuration_head_unlocked(&recovered, &head)?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = recovered.clone();
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(recovered)
    }

    pub fn apply(&self, request: RecorderRequest) -> Result<RecorderReply> {
        if !matches!(request, RecorderRequest::Identity) {
            self.validate_request_context(&request)?;
        }
        match request {
            RecorderRequest::Identity => Ok(self.reply(0, None)),
            RecorderRequest::StoreCommand {
                config_id,
                config_digest,
                command_hash,
                command,
                ..
            } => {
                let _guard = self
                    .sync
                    .lock()
                    .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
                self.recover_intent()?;
                let context = RecorderRequest::StoreCommand {
                    cluster_id: self.cluster_id.clone(),
                    epoch: self.epoch,
                    config_id,
                    config_digest,
                    command_hash,
                    command: command.clone(),
                };
                self.validate_request_context(&context)?;
                self.store_command_unlocked(command_hash, &command)?;
                let mut reply = self.reply(0, None);
                reply.config_id = config_id;
                reply.config_digest = config_digest;
                Ok(reply)
            }
            RecorderRequest::FetchCommand {
                config_id,
                config_digest,
                command_hash,
                ..
            } => {
                let _guard = self
                    .sync
                    .lock()
                    .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
                self.recover_intent()?;
                let command = self.fetch_command_unlocked(command_hash)?;
                let mut reply = self.reply(0, command);
                reply.config_id = config_id;
                reply.config_digest = config_digest;
                Ok(reply)
            }
            request => {
                let slot =
                    request_slot(&request).ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
                let request_digest = request_context(&request)
                    .ok_or(Error::Rejected(RejectReason::InvalidRequest))?
                    .3;
                let should_save = !matches!(request, RecorderRequest::Inspect { .. });
                let _guard = self
                    .sync
                    .lock()
                    .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
                self.recover_intent()?;
                self.validate_request_context(&request)?;
                let configuration = self.configuration_state()?;
                self.validate_slot_gate(&configuration, slot, None)?;
                let mut state = self.load_unlocked(slot, request_digest)?;
                let mut reply = state.apply(request).map_err(Error::Rejected)?;
                let next_configuration =
                    self.transition_after_apply(&configuration, &state, None, None)?;
                if should_save || next_configuration != configuration {
                    self.persist_state_transition_unlocked(
                        &state,
                        &configuration,
                        &next_configuration,
                    )?;
                }
                reply.recorder_id = self.recorder_id.clone();
                Ok(reply)
            }
        }
    }

    pub fn record_proposal(&self, request: RecordRequest) -> Result<RecordSummary> {
        self.validate_record_context(&request)?;
        let value = request
            .proposal
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        let command = if let Some(command) = request.command.as_ref() {
            self.validate_resolved_command_for_value(
                request.slot,
                configuration.config_id,
                value,
                command,
            )?;
            std::borrow::Cow::Borrowed(command)
        } else {
            std::borrow::Cow::Owned(self.command_for_value_unlocked(value)?)
        };
        let change = Self::change_for_command(&command)?;
        if !configuration.activated && change.is_none() {
            return Err(Error::Rejected(RejectReason::ActivationRequired));
        }
        let state = self.load_unlocked(request.slot, request.config_digest)?;
        if let Some(proof) = state.decision_proof() {
            if proof.proposal().value.as_ref() == Some(value) {
                return Ok(record_summary(
                    &self.recorder_id,
                    &state,
                    Some(proof.clone()),
                ));
            }
        }
        self.validate_slot_gate(&configuration, request.slot, change.as_ref())?;
        if request.command.is_none() {
            self.validate_resolved_command_for_value(
                request.slot,
                configuration.config_id,
                value,
                &command,
            )?;
        }
        if let Some(proof) = state.decision_proof() {
            return Ok(record_summary(
                &self.recorder_id,
                &state,
                Some(proof.clone()),
            ));
        }
        let (mut next, _) = state.record(&request).map_err(Error::Rejected)?;

        // Legacy fields are intentionally not synthesized from ISR state.
        next.highest_promised = next.isr.first_current().and_then(proposal_ballot);
        next.accepted = None;
        let next_configuration =
            self.transition_after_apply(&configuration, &next, change.as_ref(), Some(value))?;
        self.persist_state_transition_with_command_unlocked(
            &next,
            &configuration,
            &next_configuration,
            request
                .command
                .as_ref()
                .map(|command| (value.command_hash, command)),
        )?;
        Ok(record_summary(&self.recorder_id, &next, None))
    }

    pub fn install_decision_proof_record(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<()> {
        let (slot, epoch, config_id, digest) = proof_context(&proof);
        if proof_cluster_id(&proof) != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        if epoch != self.epoch
            || config_id != configuration.config_id
            || digest != configuration.config_digest
            || configuration.membership.as_ref() != Some(membership)
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        proof
            .validate_for_cluster(&self.cluster_id, slot, epoch, config_id, membership)
            .map_err(Error::Rejected)?;
        let value = proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        self.validate_value_unlocked(slot, value)?;
        let mut state = self.load_unlocked(slot, digest)?;
        if state.decision_proof().is_some() {
            state.install_proof(proof).map_err(Error::Rejected)?;
            return Ok(());
        }
        let change = self.change_for_value_unlocked(value)?;
        self.validate_slot_gate(&configuration, slot, change.as_ref())?;
        state
            .install_proof(proof.clone())
            .map_err(Error::Rejected)?;
        let certificate = certificate_from_proof(&proof)?;
        if let Some(existing) = &state.decided {
            if existing.value != certificate.value {
                return Err(Error::Rejected(RejectReason::AlreadyDecided));
            }
        } else {
            state.decided = Some(certificate);
        }
        let next =
            self.transition_after_apply(&configuration, &state, change.as_ref(), Some(value))?;
        self.persist_state_transition_unlocked(&state, &configuration, &next)
    }

    fn validate_record_context(&self, request: &RecordRequest) -> Result<()> {
        if request.cluster_id != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        if request.epoch != self.epoch {
            return Err(Error::Rejected(if request.epoch < self.epoch {
                RejectReason::StaleEpoch
            } else {
                RejectReason::FutureEpoch
            }));
        }
        let configuration = self.configuration_state()?;
        if request.config_id != configuration.config_id
            || (configuration.config_digest != LogHash::ZERO
                && request.config_digest != configuration.config_digest)
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        Ok(())
    }

    pub fn load(&self, slot: Slot) -> Result<RecorderSlotState> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.load_unlocked(slot, self.config_digest())
    }

    pub fn save(&self, state: &RecorderSlotState) -> Result<()> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        if state.cluster_id != self.cluster_id
            || state.epoch != self.epoch
            || state.config_id != configuration.config_id
            || (configuration.config_digest != LogHash::ZERO
                && state.config_digest != configuration.config_digest)
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        let change = state
            .decided()
            .map(|decision| &decision.value)
            .or_else(|| state.accepted().map(|accepted| &accepted.value))
            .map(|value| self.change_for_value_unlocked(value))
            .transpose()?
            .flatten();
        self.validate_slot_gate(&configuration, state.slot(), change.as_ref())?;
        let applied_value = state
            .decided()
            .map(|decision| &decision.value)
            .or_else(|| state.accepted().map(|accepted| &accepted.value));
        let next =
            self.transition_after_apply(&configuration, state, change.as_ref(), applied_value)?;
        self.persist_state_transition_unlocked(state, &configuration, &next)
    }

    pub fn store_command(&self, command_hash: LogHash, command: StoredCommand) -> Result<()> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.store_command_unlocked(command_hash, &command)
    }

    fn store_command_unlocked(&self, command_hash: LogHash, command: &StoredCommand) -> Result<()> {
        validate_replicated_command_size(command)?;
        if command.hash() != command_hash {
            return Err(Error::CommandHashMismatch);
        }
        {
            let wal = self
                .wal
                .lock()
                .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
            match wal.commands.get(&command_hash) {
                Some(existing) if existing == command => return Ok(()),
                Some(_) => return Err(Error::CommandHashMismatch),
                None => {}
            }
        }
        self.stage_command_unlocked(command_hash, command)?;
        self.sync_root()
    }

    fn stage_command_unlocked(&self, command_hash: LogHash, command: &StoredCommand) -> Result<()> {
        let name = Self::command_name(command_hash);
        if self.effect_root_anchor.exists(&name)? {
            return match self.fetch_command_cache_unlocked(command_hash)? {
                Some(existing) if existing == *command => Ok(()),
                _ => Err(Error::CommandHashMismatch),
            };
        }
        self.effect_root_anchor
            .atomic_write(&name, &encode_stored_command(command))?;
        Ok(())
    }

    pub fn fetch_command(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.fetch_command_unlocked(command_hash)
    }

    /// Durably finalizes one immutable SQL effect bundle before its small
    /// manifest can be proposed. This is deliberately a local foundation: no
    /// production proposal path calls it until the manifest/resolver protocol
    /// is wired end-to-end.
    pub fn finalize_effect_bundle(&self, request: &EffectBundleFinalizeRequest) -> Result<()> {
        self.finalize_effect_bundle_with_quota(request, DEFAULT_EFFECT_BUNDLE_STORE_QUOTA_BYTES)
    }

    /// Same as [`Self::finalize_effect_bundle`], with an explicit bounded
    /// admission quota for tests and future operator configuration.
    pub fn finalize_effect_bundle_with_quota(
        &self,
        request: &EffectBundleFinalizeRequest,
        quota_bytes: u64,
    ) -> Result<()> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        self.effect_root_anchor.verify_path(&self.root)?;
        let bundle = &request.bundle;
        EffectBundleFinalizeRequest::new(bundle.clone(), request.manifest_command.clone())?;
        self.validate_effect_bundle_binding(&bundle.binding)?;
        validate_effect_chunks(&bundle.chunks)?;
        if effect_digest(
            &bundle.chunk_hashes,
            &bundle.chunk_lengths,
            bundle.total_len,
        ) != bundle.binding.effect_digest
        {
            return Err(Error::EffectBundleInvalid(
                "full effect digest changed after construction".into(),
            ));
        }
        let anchor = &self.effect_root_anchor;
        let manifest_name = self.effect_bundle_name(&bundle.binding);
        if anchor
            .read_optional(
                &manifest_name,
                MAX_EFFECT_BUNDLE_MANIFEST_BYTES,
                "effect bundle manifest",
            )?
            .is_some()
        {
            let existing = self.load_effect_bundle_unlocked(&bundle.binding)?;
            return if existing == *bundle {
                self.clear_staged_effect_pin(&bundle.binding)?;
                Ok(())
            } else {
                Err(Error::EffectBundleConflict)
            };
        }

        let mut missing_bytes = 0u64;
        for (chunk, hash) in bundle.chunks.iter().zip(&bundle.chunk_hashes) {
            let name = self.effect_chunk_name(*hash);
            if let Some(existing) =
                anchor.read_optional(&name, MAX_EFFECT_BUNDLE_CHUNK_BYTES, "effect chunk")?
            {
                if existing != *chunk || effect_chunk_digest(&existing) != *hash {
                    return Err(Error::EffectBundleConflict);
                }
            } else {
                missing_bytes = missing_bytes
                    .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                        Error::EffectBundleInvalid("chunk length cannot fit u64".into())
                    })?)
                    .ok_or_else(|| {
                        Error::EffectBundleInvalid("quota accounting overflow".into())
                    })?;
            }
        }
        let actual = self
            .effect_chunk_usage_unlocked()?
            .checked_add(missing_bytes)
            .ok_or_else(|| Error::EffectBundleInvalid("quota accounting overflow".into()))?;
        if actual > quota_bytes {
            return Err(Error::EffectBundleQuotaExceeded {
                actual,
                limit: quota_bytes,
            });
        }

        // Chunks become durable before the manifest makes them reachable. A
        // crash before the final manifest leaves only harmless, unreachable
        // CAS data; a successful acknowledgement has both data and directory
        // entries fsynced.
        for (chunk, hash) in bundle.chunks.iter().zip(&bundle.chunk_hashes) {
            let name = self.effect_chunk_name(*hash);
            if anchor
                .read_optional(&name, MAX_EFFECT_BUNDLE_CHUNK_BYTES, "effect chunk")?
                .is_none()
            {
                anchor.atomic_write(&name, chunk)?;
            }
        }
        anchor.sync()?;
        anchor.atomic_write(
            &manifest_name,
            &encode_effect_bundle(&request.manifest_command)?,
        )?;
        anchor.sync()?;
        self.clear_staged_effect_pin(&bundle.binding)?;
        Ok(())
    }

    /// Persists one bounded chunk for an exact QEFX command. Staged chunks are
    /// deliberately unreachable until [`Self::finalize_staged_effect_bundle`]
    /// has verified the complete ordered bundle and durably installed its
    /// manifest.
    pub fn stage_effect_bundle_chunk(
        &self,
        binding: &EffectBundleBinding,
        manifest_command: &StoredCommand,
        ordinal: u16,
        chunk: &[u8],
    ) -> Result<()> {
        let qefx = verified_effect_bundle_command(binding, manifest_command)?;
        let expected = qefx.chunks().get(usize::from(ordinal)).ok_or_else(|| {
            Error::EffectBundleInvalid("effect chunk ordinal is out of range".into())
        })?;
        if expected.encoded_len() as usize != chunk.len()
            || expected.digest() != effect_chunk_digest(chunk)
        {
            return Err(Error::EffectBundleInvalid(
                "effect chunk does not match the QEFX commitment".into(),
            ));
        }
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        self.effect_root_anchor.verify_path(&self.root)?;
        self.validate_effect_bundle_binding(binding)?;
        self.staged_effect_pins
            .lock()
            .map_err(|_| Error::Io("staged effect pin lock poisoned".into()))?
            .insert(
                effect_bundle_binding_digest(binding),
                EffectBundleGcPin {
                    binding: binding.clone(),
                    manifest_command: manifest_command.clone(),
                },
            );
        let name = self.effect_chunk_name(expected.digest());
        if let Some(existing) = self.effect_root_anchor.read_optional(
            &name,
            MAX_EFFECT_BUNDLE_CHUNK_BYTES,
            "effect chunk",
        )? {
            if existing != chunk {
                return Err(Error::EffectBundleConflict);
            }
            return Ok(());
        }
        self.effect_root_anchor.atomic_write(&name, chunk)?;
        self.effect_root_anchor.sync()
    }

    /// Finalizes an exact QEFX bundle from its previously staged chunks.
    pub fn finalize_staged_effect_bundle(
        &self,
        binding: &EffectBundleBinding,
        manifest_command: StoredCommand,
    ) -> Result<()> {
        let qefx = verified_effect_bundle_command(binding, &manifest_command)?;
        let chunks = {
            let _guard = self
                .sync
                .lock()
                .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
            self.recover_intent()?;
            self.effect_root_anchor.verify_path(&self.root)?;
            self.validate_effect_bundle_binding(binding)?;
            qefx.chunks()
                .iter()
                .map(|expected| {
                    let chunk = self.effect_root_anchor.read(
                        &self.effect_chunk_name(expected.digest()),
                        MAX_EFFECT_BUNDLE_CHUNK_BYTES,
                        "effect chunk",
                    )?;
                    if chunk.len() != expected.encoded_len() as usize
                        || effect_chunk_digest(&chunk) != expected.digest()
                    {
                        return Err(Error::EffectBundleInvalid(
                            "staged effect chunk does not match QEFX".into(),
                        ));
                    }
                    Ok(chunk)
                })
                .collect::<Result<Vec<_>>>()?
        };
        let bundle = RecorderEffectBundle::new(binding.clone(), chunks)?;
        self.finalize_effect_bundle(&EffectBundleFinalizeRequest::new(bundle, manifest_command)?)
    }

    /// Returns only the bounded QEFX command for a finalized bundle.
    pub fn fetch_effect_bundle_manifest(
        &self,
        binding: &EffectBundleBinding,
    ) -> Result<Option<StoredCommand>> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        self.effect_root_anchor.verify_path(&self.root)?;
        self.validate_effect_bundle_binding(binding)?;
        let Some(bytes) = self.effect_root_anchor.read_optional(
            &self.effect_bundle_name(binding),
            MAX_EFFECT_BUNDLE_MANIFEST_BYTES,
            "effect bundle manifest",
        )?
        else {
            return Ok(None);
        };
        let command = StoredCommand::new(EntryType::Command, bytes);
        verified_effect_bundle_command(binding, &command)?;
        Ok(Some(command))
    }

    /// Reads exactly one bounded finalized chunk, preserving QEFX ordering.
    pub fn fetch_effect_bundle_chunk(
        &self,
        binding: &EffectBundleBinding,
        ordinal: u16,
    ) -> Result<Option<Vec<u8>>> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        self.effect_root_anchor.verify_path(&self.root)?;
        self.validate_effect_bundle_binding(binding)?;
        let Some(bytes) = self.effect_root_anchor.read_optional(
            &self.effect_bundle_name(binding),
            MAX_EFFECT_BUNDLE_MANIFEST_BYTES,
            "effect bundle manifest",
        )?
        else {
            return Ok(None);
        };
        let command = StoredCommand::new(EntryType::Command, bytes);
        let qefx = verified_effect_bundle_command(binding, &command)?;
        let expected = qefx.chunks().get(usize::from(ordinal)).ok_or_else(|| {
            Error::EffectBundleInvalid("effect chunk ordinal is out of range".into())
        })?;
        let chunk = self.effect_root_anchor.read(
            &self.effect_chunk_name(expected.digest()),
            MAX_EFFECT_BUNDLE_CHUNK_BYTES,
            "effect chunk",
        )?;
        if chunk.len() != expected.encoded_len() as usize
            || effect_chunk_digest(&chunk) != expected.digest()
        {
            return Err(Error::EffectBundleInvalid(
                "effect chunk digest mismatch".into(),
            ));
        }
        Ok(Some(chunk))
    }

    /// Loads and re-verifies a finalized effect bundle. Missing, truncated,
    /// reordered, or corrupted chunks fail before a caller can mutate SQLite.
    pub fn load_effect_bundle(
        &self,
        binding: &EffectBundleBinding,
    ) -> Result<Option<RecorderEffectBundle>> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.effect_root_anchor.verify_path(&self.root)?;
        self.validate_effect_bundle_binding(binding)?;
        let name = self.effect_bundle_name(binding);
        if self
            .effect_root_anchor
            .read_optional(
                &name,
                MAX_EFFECT_BUNDLE_MANIFEST_BYTES,
                "effect bundle manifest",
            )?
            .is_none()
        {
            return Ok(None);
        }
        self.load_effect_bundle_unlocked(binding).map(Some)
    }

    /// Persists a monotonic, archive-readback-certified GC anchor and then
    /// removes only finalized manifests at or below that anchor. The anchor is
    /// fsynced before any deletion, so a crash yields either the old complete
    /// set or a valid superset of the new swept set; it never creates a hole
    /// below an unpersisted certificate.
    pub fn advance_effect_bundle_gc_anchor(
        &self,
        certificate: &CheckpointReadbackCertificate,
        protected_pins: &[EffectBundleGcPin],
    ) -> Result<EffectBundleGcOutcome> {
        self.advance_effect_bundle_gc_anchor_bounded(certificate, protected_pins, usize::MAX)
    }

    /// Advances the durable anchor while limiting destructive maintenance work.
    ///
    /// The anchor is always published before deletion. Callers may retry the
    /// exact certificate until `sweep_complete`; each call removes at most
    /// `max_removals` manifests and chunks, respectively. This keeps online
    /// Recorder RPC admission from being monopolized by a directory-wide GC.
    pub fn advance_effect_bundle_gc_anchor_bounded(
        &self,
        certificate: &CheckpointReadbackCertificate,
        protected_pins: &[EffectBundleGcPin],
        max_removals: usize,
    ) -> Result<EffectBundleGcOutcome> {
        if max_removals == 0 {
            return Err(Error::EffectBundleInvalid(
                "effect GC removal budget must be positive".into(),
            ));
        }
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        self.effect_root_anchor.verify_path(&self.root)?;
        self.validate_effect_bundle_gc_certificate(certificate)?;
        let identity = certificate.identity();
        let tip = certificate.tip();
        let mut all_pins = protected_pins.to_vec();
        all_pins.extend(
            self.staged_effect_pins
                .lock()
                .map_err(|_| Error::Io("staged effect pin lock poisoned".into()))?
                .values()
                .filter(|pin| pin.binding.intended_slot > tip.index())
                .cloned(),
        );
        let protected = self.effect_bundle_gc_protected(&all_pins)?;
        let previous = self.load_effect_bundle_gc_anchor_unlocked()?;
        if let Some(previous) = &previous {
            if previous.cluster_id != identity.cluster_id() || previous.epoch != identity.epoch() {
                return Err(Error::EffectBundleInvalid(
                    "effect GC anchor cluster or epoch does not match this certificate".into(),
                ));
            }
            if tip.index() < previous.through_slot {
                return Err(Error::EffectBundleInvalid(
                    "effect GC anchor cannot move backwards".into(),
                ));
            }
            if tip.index() == previous.through_slot
                && (previous.tip_hash != tip.hash()
                    || previous.manifest_digest != certificate.manifest_digest())
            {
                return Err(Error::EffectBundleInvalid(
                    "effect GC anchor retry has different checkpoint evidence".into(),
                ));
            }
        }

        if previous.as_ref().is_none_or(|anchor| {
            anchor.through_slot != tip.index()
                || anchor.tip_hash != tip.hash()
                || anchor.manifest_digest != certificate.manifest_digest()
        }) {
            self.effect_root_anchor.atomic_write(
                EFFECT_BUNDLE_GC_ANCHOR_FILE,
                &encode_effect_bundle_gc_anchor(&EffectBundleGcAnchor {
                    cluster_id: identity.cluster_id().into(),
                    epoch: identity.epoch(),
                    through_slot: tip.index(),
                    tip_hash: tip.hash(),
                    manifest_digest: certificate.manifest_digest(),
                })?,
            )?;
            self.effect_root_anchor.sync()?;
        }

        let (removed_manifests, manifests_complete) =
            self.sweep_effect_bundle_manifests_unlocked(tip.index(), &protected, max_removals)?;
        let (removed_chunks, chunks_complete) =
            self.reap_unreachable_effect_chunks_unlocked(&protected, max_removals)?;
        Ok(EffectBundleGcOutcome {
            previous_anchor: previous.map(|anchor| anchor.through_slot),
            current_anchor: tip.index(),
            removed_manifests,
            removed_chunks,
            sweep_complete: manifests_complete && chunks_complete,
        })
    }

    pub fn effect_bundle_gc_anchor(&self) -> Result<Option<Slot>> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        self.effect_root_anchor.verify_path(&self.root)?;
        Ok(self
            .load_effect_bundle_gc_anchor_unlocked()?
            .map(|anchor| anchor.through_slot))
    }

    fn validate_effect_bundle_gc_certificate(
        &self,
        certificate: &CheckpointReadbackCertificate,
    ) -> Result<()> {
        let configuration = self.configuration_state()?;
        let identity = certificate.identity();
        let tip = certificate.tip();
        if identity.cluster_id() != self.cluster_id
            || identity.epoch() != self.epoch
            || identity.config_id() != configuration.config_id
            || identity.config_digest() != configuration.config_digest
            || tip.index() == 0
            || tip.hash() == LogHash::ZERO
            || certificate.manifest_digest() == LogHash::ZERO
        {
            return Err(Error::EffectBundleInvalid(
                "effect GC certificate is not a certified checkpoint for this recorder".into(),
            ));
        }
        Ok(())
    }

    fn effect_bundle_gc_protected(
        &self,
        pins: &[EffectBundleGcPin],
    ) -> Result<EffectBundleGcProtected> {
        let mut protected = EffectBundleGcProtected::default();
        for pin in pins {
            let qefx = verified_effect_bundle_command(&pin.binding, &pin.manifest_command)?;
            if pin.binding.cluster_id != self.cluster_id || pin.binding.epoch != self.epoch {
                return Err(Error::EffectBundleInvalid(
                    "effect GC pin belongs to a foreign cluster or epoch".into(),
                ));
            }
            protected
                .bindings
                .insert(effect_bundle_binding_digest(&pin.binding));
            protected
                .chunks
                .extend(qefx.chunks().iter().map(ExternalEffectChunk::digest));
        }
        Ok(protected)
    }

    fn load_effect_bundle_gc_anchor_unlocked(&self) -> Result<Option<EffectBundleGcAnchor>> {
        self.effect_root_anchor
            .read_optional(
                EFFECT_BUNDLE_GC_ANCHOR_FILE,
                MAX_EFFECT_BUNDLE_GC_ANCHOR_BYTES,
                "effect GC anchor",
            )?
            .map(|bytes| decode_effect_bundle_gc_anchor(&bytes))
            .transpose()
    }

    fn clear_staged_effect_pin(&self, binding: &EffectBundleBinding) -> Result<()> {
        self.staged_effect_pins
            .lock()
            .map_err(|_| Error::Io("staged effect pin lock poisoned".into()))?
            .remove(&effect_bundle_binding_digest(binding));
        Ok(())
    }

    fn sweep_effect_bundle_manifests_unlocked(
        &self,
        through_slot: Slot,
        protected: &EffectBundleGcProtected,
        max_removals: usize,
    ) -> Result<(usize, bool)> {
        let mut removed = 0;
        let mut complete = true;
        for name in self.effect_root_anchor.list()? {
            if !name.starts_with(EFFECT_BUNDLE_PREFIX) {
                continue;
            }
            if !self.is_effect_bundle_name(&name) {
                return Err(Error::EffectBundleInvalid(
                    "invalid effect bundle name".into(),
                ));
            }
            let bytes = self.effect_root_anchor.read(
                &name,
                MAX_EFFECT_BUNDLE_MANIFEST_BYTES,
                "effect bundle manifest",
            )?;
            let manifest = decode_effect_bundle(&bytes)?;
            let binding_digest = effect_bundle_binding_digest(&manifest.binding);
            if self.effect_bundle_name(&manifest.binding) != name {
                return Err(Error::EffectBundleInvalid(
                    "effect bundle filename/binding mismatch".into(),
                ));
            }
            if manifest.binding.intended_slot <= through_slot
                && !protected.bindings.contains(&binding_digest)
            {
                if removed == max_removals {
                    complete = false;
                    continue;
                }
                self.effect_root_anchor.remove(&name)?;
                removed += 1;
            }
        }
        self.effect_root_anchor.sync()?;
        Ok((removed, complete))
    }

    fn validate_effect_bundle_binding(&self, binding: &EffectBundleBinding) -> Result<()> {
        let configuration = self.configuration_state()?;
        if binding.cluster_id != self.cluster_id
            || binding.epoch != self.epoch
            || binding.config_id != configuration.config_id
            || binding.config_digest != configuration.config_digest
        {
            return Err(Error::EffectBundleInvalid(
                "binding does not match this recorder identity".into(),
            ));
        }
        if binding.intended_slot == 0 || binding.effect_digest == LogHash::ZERO {
            return Err(Error::EffectBundleInvalid(
                "binding has an invalid intended slot or effect digest".into(),
            ));
        }
        Ok(())
    }

    fn load_effect_bundle_unlocked(
        &self,
        expected: &EffectBundleBinding,
    ) -> Result<RecorderEffectBundle> {
        let name = self.effect_bundle_name(expected);
        let bytes = self.effect_root_anchor.read(
            &name,
            MAX_EFFECT_BUNDLE_MANIFEST_BYTES,
            "effect bundle manifest",
        )?;
        let manifest = decode_effect_bundle(&bytes)?;
        if manifest.binding != *expected {
            return Err(Error::EffectBundleConflict);
        }
        let mut chunks = Vec::with_capacity(manifest.chunk_hashes.len());
        for hash in &manifest.chunk_hashes {
            let chunk = self
                .effect_root_anchor
                .read(
                    &self.effect_chunk_name(*hash),
                    MAX_EFFECT_BUNDLE_CHUNK_BYTES,
                    "effect chunk",
                )
                .map_err(|_| Error::EffectBundleUnavailable)?;
            if effect_chunk_digest(&chunk) != *hash {
                return Err(Error::EffectBundleInvalid("chunk digest mismatch".into()));
            }
            chunks.push(chunk);
        }
        let bundle = RecorderEffectBundle::new(manifest.binding, chunks)?;
        if bundle.chunk_hashes != manifest.chunk_hashes
            || bundle.chunk_lengths != manifest.chunk_lengths
            || bundle.total_len != manifest.total_len
        {
            return Err(Error::EffectBundleInvalid(
                "manifest chunk order or total length mismatch".into(),
            ));
        }
        Ok(bundle)
    }

    fn effect_chunk_name(&self, hash: LogHash) -> String {
        format!("{EFFECT_CHUNK_PREFIX}{}.qefc", hash.to_hex())
    }

    fn effect_bundle_name(&self, binding: &EffectBundleBinding) -> String {
        format!(
            "{EFFECT_BUNDLE_PREFIX}{}.qefb",
            effect_bundle_binding_digest(binding).to_hex()
        )
    }

    fn effect_chunk_usage_unlocked(&self) -> Result<u64> {
        let mut total = 0u64;
        for name in self.effect_root_anchor.list()? {
            if !name.starts_with(EFFECT_CHUNK_PREFIX) {
                continue;
            }
            if !self.is_effect_chunk_name(&name) {
                return Err(Error::EffectBundleInvalid(
                    "invalid effect chunk name".into(),
                ));
            }
            let chunk = self.effect_root_anchor.read(
                &name,
                MAX_EFFECT_BUNDLE_CHUNK_BYTES,
                "effect chunk",
            )?;
            total = total
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| Error::EffectBundleInvalid("quota accounting overflow".into()))?;
        }
        Ok(total)
    }

    fn reap_unreachable_effect_chunks_unlocked(
        &self,
        protected: &EffectBundleGcProtected,
        max_removals: usize,
    ) -> Result<(usize, bool)> {
        let mut reachable = protected.chunks.clone();
        let names = self.effect_root_anchor.list()?;
        for name in &names {
            if !name.starts_with(EFFECT_BUNDLE_PREFIX) {
                continue;
            }
            if !self.is_effect_bundle_name(name) {
                return Err(Error::EffectBundleInvalid(
                    "invalid effect bundle name".into(),
                ));
            }
            let bytes = self.effect_root_anchor.read(
                name,
                MAX_EFFECT_BUNDLE_MANIFEST_BYTES,
                "effect bundle manifest",
            )?;
            let manifest = decode_effect_bundle(&bytes)?;
            if self.effect_bundle_name(&manifest.binding) != *name {
                return Err(Error::EffectBundleInvalid(
                    "effect bundle filename/binding mismatch".into(),
                ));
            }
            reachable.extend(manifest.chunk_hashes);
        }
        let mut removed = 0;
        let mut complete = true;
        for name in names {
            if !name.starts_with(EFFECT_CHUNK_PREFIX) {
                continue;
            }
            let Some(hash) = self.effect_chunk_hash_from_name(&name) else {
                return Err(Error::EffectBundleInvalid(
                    "invalid effect chunk name".into(),
                ));
            };
            if !reachable.contains(&hash) {
                if removed == max_removals {
                    complete = false;
                    continue;
                }
                self.effect_root_anchor.remove(&name)?;
                removed += 1;
            }
        }
        self.effect_root_anchor.sync()?;
        Ok((removed, complete))
    }

    fn is_effect_chunk_name(&self, name: &str) -> bool {
        self.effect_chunk_hash_from_name(name).is_some()
    }

    fn effect_chunk_hash_from_name(&self, name: &str) -> Option<LogHash> {
        name.strip_prefix(EFFECT_CHUNK_PREFIX)
            .and_then(|value| value.strip_suffix(".qefc"))
            .and_then(LogHash::from_hex)
    }

    fn is_effect_bundle_name(&self, name: &str) -> bool {
        name.strip_prefix(EFFECT_BUNDLE_PREFIX)
            .and_then(|value| value.strip_suffix(".qefb"))
            .and_then(LogHash::from_hex)
            .is_some()
    }

    fn load_unlocked(&self, slot: Slot, config_digest: LogHash) -> Result<RecorderSlotState> {
        let wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
        if let Some(bytes) = wal.slots.get(&slot) {
            let state = decode_recorder_state(bytes)?;
            drop(wal);
            if state.cluster_id != self.cluster_id
                || state.epoch != self.epoch
                || state.config_id != self.current_config_id()
                || (config_digest != LogHash::ZERO && state.config_digest != config_digest)
            {
                return Err(Error::Decode("recorder WAL state identity mismatch".into()));
            }
            return Ok(state);
        }
        drop(wal);
        let slot_name = Self::slot_name(slot);
        let bytes = match self.effect_root_anchor.read_optional(
            &slot_name,
            MAX_RECORDER_STATE_BYTES,
            "slot cache",
        )? {
            Some(bytes) => bytes,
            None => {
                return Ok(RecorderSlotState::new_with_digest(
                    slot,
                    self.cluster_id.clone(),
                    self.epoch,
                    self.current_config_id(),
                    config_digest,
                ));
            }
        };
        let state = decode_recorder_state(&bytes)?;
        if state.slot != slot
            || state.cluster_id != self.cluster_id
            || state.epoch != self.epoch
            || state.config_id != self.current_config_id()
            || (config_digest != LogHash::ZERO && state.config_digest != config_digest)
        {
            return Err(Error::Decode("recorder state identity mismatch".into()));
        }
        Ok(state)
    }

    fn open_or_initialize_recorded_head(&self, existing_format: bool) -> Result<()> {
        let configuration = self.configuration_state()?;
        let (head, recent_slots, wal_checkpoint) = match self.effect_root_anchor.read_optional(
            Self::RECORDED_HEAD_FILE,
            MAX_RECORDED_HEAD_BYTES,
            "recorded-head.rec",
        )? {
            Some(bytes) => {
                decode_recorded_head(&bytes, &self.cluster_id, self.epoch, &configuration)?
            }
            None => {
                if existing_format {
                    return Err(noncurrent_recorder_layout_error());
                }
                let head = RecordedHeadProvenance::Empty;
                let wal_checkpoint = WalCheckpoint::default();
                self.effect_root_anchor.atomic_write(
                    Self::RECORDED_HEAD_FILE,
                    &encode_recorded_head(
                        &self.cluster_id,
                        self.epoch,
                        &configuration,
                        &head,
                        &[],
                        wal_checkpoint,
                    )?,
                )?;
                (head, Vec::new(), wal_checkpoint)
            }
        };
        self.install_recorded_head(&configuration, head, recent_slots, wal_checkpoint)
    }

    fn open_or_replay_wal(&self) -> Result<()> {
        self.effect_root_anchor
            .create_empty_if_missing(Self::WAL_FILE)?;
        let bytes =
            self.effect_root_anchor
                .read(Self::WAL_FILE, MAX_RECORDER_WAL_BYTES, "recorder.wal")?;
        let checkpoint = self.wal_checkpoint()?;
        let mut replayed = RecorderWal {
            checkpoint,
            next_sequence: checkpoint
                .through_sequence
                .checked_add(1)
                .ok_or_else(|| Error::Decode("recorder WAL sequence exhausted".into()))?,
            ..RecorderWal::default()
        };
        let mut configuration = self.configuration_state()?;
        let mut head = self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))?
            .clone();
        let mut offset = 0usize;
        while offset < bytes.len() {
            let Some((frame, end)) = decode_wal_frame(&bytes, offset)? else {
                break;
            };
            if frame.generation < checkpoint.generation {
                offset = end;
                continue;
            }
            if frame.generation != checkpoint.generation
                || frame.sequence != replayed.next_sequence
                || frame.prev_digest != replayed.last_digest
            {
                return Err(Error::Decode(
                    "recorder WAL sequence or digest chain mismatch".into(),
                ));
            }
            let state = decode_recorder_state(&frame.slot_bytes)?;
            let next_configuration = decode_configuration_state(&frame.configuration_bytes)?;
            if state.slot() != frame.slot
                || state.cluster_id != self.cluster_id
                || state.epoch != self.epoch
                || state.config_id != next_configuration.config_id
                || state.config_digest != next_configuration.config_digest
                || configuration_structure_changed(&configuration, &next_configuration)
            {
                return Err(Error::Decode("recorder WAL state identity mismatch".into()));
            }
            if let Some((hash, command)) = &frame.command {
                if command.hash() != *hash {
                    return Err(Error::Decode(
                        "recorder WAL inline command hash mismatch".into(),
                    ));
                }
                upsert_wal_command(&mut replayed.commands, *hash, command)?;
            }
            for value in recorder_state_values(&state) {
                let cached_command;
                let command = match replayed.commands.get(&value.command_hash) {
                    Some(command) => command,
                    None => {
                        cached_command = self
                            .fetch_command_cache_unlocked(value.command_hash)
                            .ok()
                            .flatten()
                            .ok_or(Error::CommandUnavailable)?;
                        &cached_command
                    }
                };
                if AcceptedValue::from_command(
                    &self.cluster_id,
                    frame.slot,
                    self.epoch,
                    next_configuration.config_id,
                    value.prev_hash,
                    command,
                ) != *value
                {
                    return Err(Error::Decode("recorder WAL value mismatch".into()));
                }
            }
            let expected_head = if next_configuration.max_accepted_or_decided_slot
                == Some(frame.slot)
                && recorder_state_values(&state).next().is_some()
            {
                RecordedHeadProvenance::SlotBacked { slot: frame.slot }
            } else {
                head.clone()
            };
            if frame.head != expected_head {
                return Err(Error::Decode("recorder WAL head mismatch".into()));
            }
            replayed.slots.insert(frame.slot, frame.slot_bytes);
            replayed.next_sequence = replayed
                .next_sequence
                .checked_add(1)
                .ok_or_else(|| Error::Decode("recorder WAL sequence exhausted".into()))?;
            replayed.last_digest = frame.digest;
            replayed.frame_count += 1;
            configuration = next_configuration;
            head = frame.head;
            offset = end;
        }
        if offset != bytes.len() {
            self.effect_root_anchor
                .truncate(Self::WAL_FILE, offset as u64)?;
        }
        replayed.file = Some(self.effect_root_anchor.open_append(Self::WAL_FILE)?);
        replayed.byte_count = offset as u64;
        *self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))? = replayed;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = configuration;
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        Ok(())
    }

    fn install_recorded_head(
        &self,
        configuration: &ConfigurationState,
        head: RecordedHeadProvenance,
        recent_slots: Vec<DurableSlotSnapshot>,
        wal_checkpoint: WalCheckpoint,
    ) -> Result<()> {
        let mut recovered_cache = false;
        for snapshot in &recent_slots {
            let state = decode_recorder_state(&snapshot.bytes)?;
            if state.slot() != snapshot.slot
                || state.cluster_id != self.cluster_id
                || state.epoch != self.epoch
                || state.config_id != configuration.config_id
                || state.config_digest != configuration.config_digest
            {
                return Err(Error::Decode(
                    "durable recorder snapshot identity mismatch".into(),
                ));
            }
            for value in recorder_state_values(&state) {
                self.validate_value_unlocked(snapshot.slot, value)?;
            }
            let name = Self::slot_name(snapshot.slot);
            if self
                .effect_root_anchor
                .read_optional(&name, MAX_RECORDER_STATE_BYTES, "slot cache")?
                .as_deref()
                != Some(snapshot.bytes.as_slice())
            {
                self.effect_root_anchor
                    .atomic_write(&name, &snapshot.bytes)?;
                recovered_cache = true;
            }
        }
        if recovered_cache {
            self.sync_root()?;
        }
        let recovered_max = match &head {
            RecordedHeadProvenance::Empty => None,
            RecordedHeadProvenance::SlotBacked { slot } => {
                let state = self.load_unlocked(*slot, configuration.config_digest)?;
                let mut values = recorder_state_values(&state).peekable();
                if values.peek().is_none() {
                    return Err(Error::Decode(
                        "slot-backed recorder head references a state without a value".into(),
                    ));
                }
                for value in values {
                    self.validate_value_unlocked(*slot, value)?;
                }
                Some(*slot)
            }
            RecordedHeadProvenance::CheckpointBacked {
                stop_slot,
                prefix_hash,
                recovered_tip,
                recovered_hash,
            } => {
                let predecessor = configuration.predecessor.as_ref().ok_or_else(|| {
                    Error::Decode("checkpoint-backed head has no predecessor binding".into())
                })?;
                if !configuration.activated
                    || predecessor.stop_slot != *stop_slot
                    || predecessor.prefix_hash != *prefix_hash
                    || recovered_tip <= stop_slot
                    || *recovered_hash == LogHash::ZERO
                    || configuration.max_accepted_or_decided_slot != Some(*recovered_tip)
                {
                    return Err(Error::Decode(
                        "checkpoint-backed recorder head evidence is invalid".into(),
                    ));
                }
                Some(*recovered_tip)
            }
        };
        self.configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))?
            .max_accepted_or_decided_slot = recovered_max;
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        *self
            .recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))? = recent_slots;
        let mut wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
        wal.checkpoint = wal_checkpoint;
        wal.next_sequence = wal_checkpoint
            .through_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Decode("recorder WAL sequence exhausted".into()))?;
        Ok(())
    }

    fn fetch_command_unlocked(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        let wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
        if let Some(command) = wal.commands.get(&command_hash).cloned() {
            return Ok(Some(command));
        }
        drop(wal);
        self.fetch_command_cache_unlocked(command_hash)
    }

    fn fetch_command_cache_unlocked(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        #[cfg(test)]
        COMMAND_FILE_READS.with(|reads| reads.set(reads.get() + 1));
        let Some(bytes) = self.effect_root_anchor.read_optional(
            &Self::command_name(command_hash),
            MAX_COMMAND_CACHE_BYTES,
            "command cache entry",
        )?
        else {
            return Ok(None);
        };
        let command = decode_stored_command(&bytes)?;
        if command.hash() != command_hash {
            return Err(Error::CommandHashMismatch);
        }
        Ok(Some(command))
    }

    fn validate_value_unlocked(&self, slot: Slot, value: &AcceptedValue) -> Result<()> {
        let config_id = self.current_config_id();
        let command = self.command_for_value_unlocked(value)?;
        self.validate_resolved_command_for_value(slot, config_id, value, &command)
    }

    fn validate_resolved_command_for_value(
        &self,
        slot: Slot,
        config_id: ConfigId,
        value: &AcceptedValue,
        command: &StoredCommand,
    ) -> Result<()> {
        validate_replicated_command_size(command)?;
        let expected = AcceptedValue::from_command(
            &self.cluster_id,
            slot,
            self.epoch,
            config_id,
            value.prev_hash,
            command,
        );
        if expected != *value {
            return Err(Error::Rejected(RejectReason::InvalidValue));
        }
        Ok(())
    }

    fn change_for_value_unlocked(&self, value: &AcceptedValue) -> Result<Option<ConfigChange>> {
        let command = self.command_for_value_unlocked(value)?;
        Self::change_for_command(&command)
    }

    fn change_for_command(command: &StoredCommand) -> Result<Option<ConfigChange>> {
        if command.entry_type != EntryType::ConfigChange {
            return Ok(None);
        }
        ConfigChange::recognize(command)
            .map_err(|_| Error::Rejected(RejectReason::InvalidRequest))
            .map(Some)
    }

    fn command_for_value_unlocked(&self, value: &AcceptedValue) -> Result<StoredCommand> {
        self.fetch_command_unlocked(value.command_hash)?
            .ok_or(Error::CommandUnavailable)
    }

    fn validate_slot_gate(
        &self,
        configuration: &ConfigurationState,
        slot: Slot,
        change: Option<&ConfigChange>,
    ) -> Result<()> {
        if let Some(predecessor) = &configuration.predecessor {
            if slot <= predecessor.stop_slot {
                return Err(Error::Rejected(RejectReason::ConfigurationNotInstalled));
            }
        }
        if let Some(seal) = &configuration.seal {
            if slot > seal.stop_slot {
                return Err(Error::Rejected(RejectReason::ConfigurationSealed {
                    stop_slot: seal.stop_slot,
                }));
            }
            if matches!(
                change,
                Some(ConfigChange::Stop { .. } | ConfigChange::BoundStop { .. })
            ) && (slot != seal.stop_slot || seal.command_hash == LogHash::ZERO)
            {
                return Err(Error::Rejected(RejectReason::TransitionInProgress));
            }
        }
        if matches!(
            change,
            Some(ConfigChange::Stop { .. } | ConfigChange::BoundStop { .. })
        ) && configuration
            .max_accepted_or_decided_slot
            .is_some_and(|accepted_slot| accepted_slot > slot)
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        if !configuration.activated {
            let Some(predecessor) = &configuration.predecessor else {
                return Err(Error::Rejected(RejectReason::InvalidTransition));
            };
            match change {
                Some(ConfigChange::BoundActivationBarrier {
                    successor,
                    stop_slot,
                    prefix_hash,
                    stop_command_hash,
                }) if successor.cluster_id() == self.cluster_id
                    && successor.config_id() == configuration.config_id
                    && successor.digest() == configuration.config_digest
                    && successor.predecessor_config_id().checked_add(1)
                        == Some(configuration.config_id)
                    && *stop_slot == predecessor.stop_slot
                    && *prefix_hash == predecessor.prefix_hash
                    && *stop_command_hash == predecessor.command_hash
                    && slot == predecessor.stop_slot + 1 => {}
                None if slot == predecessor.stop_slot + 1 => {}
                _ => return Err(Error::Rejected(RejectReason::ActivationRequired)),
            }
        } else if matches!(
            change,
            Some(
                ConfigChange::ActivationBarrier { .. }
                    | ConfigChange::BoundActivationBarrier { .. }
            )
        ) {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        if let Some(change) = change {
            let (config_id, config_digest) = change.binding();
            if config_id != configuration.config_id || config_digest != configuration.config_digest
            {
                return Err(Error::Rejected(RejectReason::WrongConfig));
            }
        }
        Ok(())
    }

    fn transition_after_apply(
        &self,
        configuration: &ConfigurationState,
        state: &RecorderSlotState,
        change: Option<&ConfigChange>,
        applied_value: Option<&AcceptedValue>,
    ) -> Result<ConfigurationState> {
        let mut next = configuration.clone();
        if applied_value.is_some() {
            next.max_accepted_or_decided_slot = Some(
                next.max_accepted_or_decided_slot
                    .map_or(state.slot(), |current| current.max(state.slot())),
            );
        }
        if state.decision_proof().is_some()
            && next.seal.as_ref().is_some_and(|seal| {
                seal.stop_slot == state.slot()
                    && applied_value.is_some_and(|value| value.command_hash != seal.command_hash)
            })
        {
            next.seal = None;
        }
        match change {
            Some(ConfigChange::Stop { .. } | ConfigChange::BoundStop { .. })
                if applied_value.is_some() =>
            {
                let value = applied_value.expect("checked applied value");
                let proposed = ConfigurationSeal {
                    stop_slot: state.slot(),
                    command_hash: value.command_hash,
                    prefix_hash: value.entry_hash,
                };
                if let Some(existing) = &next.seal {
                    if existing != &proposed {
                        return Err(Error::Rejected(RejectReason::TransitionInProgress));
                    }
                } else {
                    next.seal = Some(proposed);
                }
            }
            Some(
                ConfigChange::ActivationBarrier { .. }
                | ConfigChange::BoundActivationBarrier { .. },
            ) if state.decision_proof().is_some() => {
                next.activated = true;
            }
            _ => {}
        }
        Ok(next)
    }

    fn validate_request_context(&self, request: &RecorderRequest) -> Result<()> {
        let (cluster_id, epoch, config_id, config_digest) =
            request_context(request).ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
        if cluster_id != &self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        if epoch < self.epoch {
            return Err(Error::Rejected(RejectReason::StaleEpoch));
        }
        if epoch > self.epoch {
            return Err(Error::Rejected(RejectReason::FutureEpoch));
        }
        let configuration = self.configuration_state()?;
        if config_id != configuration.config_id {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        if configuration.config_digest != LogHash::ZERO
            && config_digest != configuration.config_digest
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        Ok(())
    }

    fn reply(&self, slot: Slot, command: Option<StoredCommand>) -> RecorderReply {
        RecorderReply {
            recorder_id: self.recorder_id.clone(),
            slot,
            config_id: self.current_config_id(),
            config_digest: self.config_digest(),
            step: 0,
            highest_promised: None,
            accepted: None,
            decided: None,
            command,
        }
    }

    fn slot_name(slot: Slot) -> String {
        format!("slot-{slot:020}.rec")
    }

    fn command_name(command_hash: LogHash) -> String {
        format!("command-{}.cmd", command_hash.to_hex())
    }

    #[cfg(test)]
    fn command_path(&self, command_hash: LogHash) -> PathBuf {
        self.root.join(Self::command_name(command_hash))
    }

    const CONFIGURATION_FILE: &'static str = "configuration.rec";
    const TRANSITION_INTENT_FILE: &'static str = "configuration.intent";
    const CONFIGURATION_HEAD_INTENT_FILE: &'static str = "configuration-head.intent";
    const RECORDED_HEAD_FILE: &'static str = "recorded-head.rec";
    const WAL_FILE: &'static str = "recorder.wal";

    fn head_after_slot_state(
        &self,
        configuration: &ConfigurationState,
        slot_state: &RecorderSlotState,
    ) -> Result<RecordedHeadProvenance> {
        let current = self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))?
            .clone();
        if configuration.max_accepted_or_decided_slot == Some(slot_state.slot())
            && recorder_state_values(slot_state).next().is_some()
        {
            Ok(RecordedHeadProvenance::SlotBacked {
                slot: slot_state.slot(),
            })
        } else {
            Ok(current)
        }
    }

    fn recover_intent(&self) -> Result<()> {
        self.recover_configuration_head_intent()?;
        let Some(intent_bytes) = self.effect_root_anchor.read_optional(
            Self::TRANSITION_INTENT_FILE,
            MAX_TRANSITION_INTENT_BYTES,
            Self::TRANSITION_INTENT_FILE,
        )?
        else {
            return Ok(());
        };
        let (slot, slot_bytes, configuration_bytes) = decode_transition_intent(&intent_bytes)?;
        let configuration = decode_configuration_state(&configuration_bytes)?;
        let slot_state = decode_recorder_state(&slot_bytes)?;
        let head = self.head_after_slot_state(&configuration, &slot_state)?;
        self.effect_root_anchor
            .atomic_write(&Self::slot_name(slot), &slot_bytes)?;
        self.effect_root_anchor
            .atomic_write(Self::CONFIGURATION_FILE, &configuration_bytes)?;
        self.effect_root_anchor.atomic_write(
            Self::RECORDED_HEAD_FILE,
            &encode_recorded_head(
                &self.cluster_id,
                self.epoch,
                &configuration,
                &head,
                &[],
                self.wal_checkpoint()?,
            )?,
        )?;
        self.effect_root_anchor
            .remove(Self::TRANSITION_INTENT_FILE)?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = configuration;
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(())
    }

    fn recover_configuration_head_intent(&self) -> Result<()> {
        let Some(intent_bytes) = self.effect_root_anchor.read_optional(
            Self::CONFIGURATION_HEAD_INTENT_FILE,
            MAX_CONFIGURATION_HEAD_INTENT_BYTES,
            Self::CONFIGURATION_HEAD_INTENT_FILE,
        )?
        else {
            return Ok(());
        };
        let (configuration_bytes, head_bytes) = decode_configuration_head_intent(&intent_bytes)?;
        self.effect_root_anchor
            .atomic_write(Self::CONFIGURATION_FILE, configuration_bytes)?;
        self.effect_root_anchor
            .atomic_write(Self::RECORDED_HEAD_FILE, head_bytes)?;
        self.effect_root_anchor
            .remove(Self::CONFIGURATION_HEAD_INTENT_FILE)
    }

    fn commit_configuration_head_unlocked(
        &self,
        configuration: &ConfigurationState,
        head: &RecordedHeadProvenance,
    ) -> Result<()> {
        let configuration_bytes = encode_configuration_state(configuration)?;
        let head_bytes = encode_recorded_head(
            &self.cluster_id,
            self.epoch,
            configuration,
            head,
            &[],
            self.wal_checkpoint()?,
        )?;
        self.effect_root_anchor.atomic_write(
            Self::CONFIGURATION_HEAD_INTENT_FILE,
            &encode_configuration_head_intent(&configuration_bytes, &head_bytes),
        )?;
        self.fail_seal_at(SealFaultPoint::AfterHeadIntent)?;
        self.effect_root_anchor
            .atomic_write(Self::CONFIGURATION_FILE, &configuration_bytes)?;
        self.fail_seal_at(SealFaultPoint::AfterHeadConfiguration)?;
        self.effect_root_anchor
            .atomic_write(Self::RECORDED_HEAD_FILE, &head_bytes)?;
        self.fail_seal_at(SealFaultPoint::AfterHead)?;
        self.effect_root_anchor
            .remove(Self::CONFIGURATION_HEAD_INTENT_FILE)
    }

    fn persist_state_transition_unlocked(
        &self,
        slot_state: &RecorderSlotState,
        previous: &ConfigurationState,
        next: &ConfigurationState,
    ) -> Result<()> {
        self.persist_state_transition_with_command_unlocked(slot_state, previous, next, None)
    }

    fn persist_state_transition_with_command_unlocked(
        &self,
        slot_state: &RecorderSlotState,
        previous: &ConfigurationState,
        next: &ConfigurationState,
        command: Option<(LogHash, &StoredCommand)>,
    ) -> Result<()> {
        if configuration_structure_changed(previous, next) {
            self.checkpoint_wal_unlocked()?;
            if let Some((hash, command)) = command {
                self.store_command_unlocked(hash, command)?;
            }
            return self.commit_transition_unlocked(slot_state, next);
        }
        let head = self.head_after_slot_state(next, slot_state)?;
        self.append_wal_unlocked(slot_state, next, &head, command)?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = next.clone();
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(())
    }

    fn append_wal_unlocked(
        &self,
        slot_state: &RecorderSlotState,
        configuration: &ConfigurationState,
        head: &RecordedHeadProvenance,
        command: Option<(LogHash, &StoredCommand)>,
    ) -> Result<()> {
        let should_checkpoint = {
            let wal = self
                .wal
                .lock()
                .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
            if wal.failed {
                return Err(Error::Io(
                    "recorder WAL is unavailable after an I/O failure".into(),
                ));
            }
            wal.frame_count >= RECORDER_WAL_HARD_FRAME_LIMIT
                || wal.byte_count >= RECORDER_WAL_SOFT_BYTE_LIMIT
        };
        if should_checkpoint {
            self.checkpoint_wal_unlocked()?;
        }
        let (generation, sequence, prev_digest) = {
            let wal = self
                .wal
                .lock()
                .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
            (
                wal.checkpoint.generation,
                wal.next_sequence,
                wal.last_digest,
            )
        };
        let (frame, digest, slot_bytes) = encode_wal_frame(
            generation,
            sequence,
            prev_digest,
            slot_state,
            configuration,
            head,
            command,
        )?;
        let mut wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
        let append_result = (|| {
            let file = wal
                .file
                .as_mut()
                .ok_or_else(|| Error::Io("recorder WAL file is not open".into()))?;
            file.write_all(&frame)
                .map_err(|error| Error::Io(error.to_string()))?;
            self.fail_seal_at(SealFaultPoint::AfterWalWrite)?;
            sync_wal_append(file).map_err(|error| Error::Io(error.to_string()))?;
            self.fail_seal_at(SealFaultPoint::AfterWalSync)
        })();
        if let Err(error) = append_result {
            wal.failed = true;
            return Err(error);
        }
        wal.slots.insert(slot_state.slot(), slot_bytes);
        if let Some((hash, command)) = command {
            wal.commands.entry(hash).or_insert_with(|| command.clone());
        }
        wal.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Io("recorder WAL sequence exhausted".into()))?;
        wal.last_digest = digest;
        wal.frame_count += 1;
        wal.byte_count = wal
            .byte_count
            .checked_add(frame.len() as u64)
            .ok_or_else(|| Error::Io("recorder WAL byte count overflow".into()))?;
        Ok(())
    }

    fn checkpoint_wal_unlocked(&self) -> Result<()> {
        let (checkpoint, next_sequence, slots, commands) = {
            let mut wal = self
                .wal
                .lock()
                .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
            if wal.failed {
                return Err(Error::Io(
                    "recorder WAL is unavailable after an I/O failure".into(),
                ));
            }
            if wal.frame_count == 0 {
                return Ok(());
            }
            (
                wal.checkpoint,
                wal.next_sequence,
                std::mem::take(&mut wal.slots),
                std::mem::take(&mut wal.commands),
            )
        };
        let next_checkpoint = WalCheckpoint {
            generation: checkpoint
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::Io("recorder WAL generation exhausted".into()))?,
            through_sequence: next_sequence - 1,
        };
        for (hash, command) in &commands {
            self.effect_root_anchor
                .atomic_write(&Self::command_name(*hash), &encode_stored_command(command))?;
        }
        for (slot, bytes) in &slots {
            self.effect_root_anchor
                .atomic_write(&Self::slot_name(*slot), bytes)?;
        }
        let configuration = self.configuration_state()?;
        let head = self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))?
            .clone();
        self.effect_root_anchor.atomic_write(
            Self::CONFIGURATION_FILE,
            &encode_configuration_state(&configuration)?,
        )?;
        self.effect_root_anchor.atomic_write(
            Self::RECORDED_HEAD_FILE,
            &encode_recorded_head(
                &self.cluster_id,
                self.epoch,
                &configuration,
                &head,
                &[],
                next_checkpoint,
            )?,
        )?;
        if let Err(error) = self.effect_root_anchor.truncate(Self::WAL_FILE, 0) {
            if let Ok(mut wal) = self.wal.lock() {
                wal.failed = true;
            }
            return Err(error);
        }
        let mut wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
        wal.checkpoint = next_checkpoint;
        wal.last_digest = LogHash::ZERO;
        wal.frame_count = 0;
        wal.byte_count = 0;
        // slots and commands were already taken by std::mem::take() above.
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(())
    }

    fn sync_root(&self) -> Result<()> {
        self.effect_root_anchor.sync()
    }

    fn commit_transition_unlocked(
        &self,
        slot_state: &RecorderSlotState,
        configuration: &ConfigurationState,
    ) -> Result<()> {
        let slot_bytes = encode_recorder_state(slot_state)?;
        let configuration_bytes = encode_configuration_state(configuration)?;
        let head = self.head_after_slot_state(configuration, slot_state)?;
        let head_bytes = encode_recorded_head(
            &self.cluster_id,
            self.epoch,
            configuration,
            &head,
            &[],
            self.wal_checkpoint()?,
        )?;
        self.effect_root_anchor.atomic_write(
            Self::TRANSITION_INTENT_FILE,
            &encode_transition_intent(slot_state.slot(), &slot_bytes, &configuration_bytes)?,
        )?;
        self.fail_seal_at(SealFaultPoint::AfterIntent)?;
        self.effect_root_anchor
            .atomic_write(&Self::slot_name(slot_state.slot()), &slot_bytes)?;
        self.fail_seal_at(SealFaultPoint::AfterSlot)?;
        self.effect_root_anchor
            .atomic_write(Self::CONFIGURATION_FILE, &configuration_bytes)?;
        self.fail_seal_at(SealFaultPoint::AfterConfiguration)?;
        self.effect_root_anchor
            .atomic_write(Self::RECORDED_HEAD_FILE, &head_bytes)?;
        self.effect_root_anchor
            .remove(Self::TRANSITION_INTENT_FILE)?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = configuration.clone();
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(())
    }

    fn fail_seal_at(&self, point: SealFaultPoint) -> Result<()> {
        let mut fault = self
            .seal_fault
            .lock()
            .map_err(|_| Error::Io("seal fault lock poisoned".into()))?;
        if *fault == Some(point) {
            *fault = None;
            return Err(Error::Io(format!("injected seal fault at {point:?}")));
        }
        Ok(())
    }

    fn config_digest(&self) -> LogHash {
        self.configuration
            .lock()
            .map(|state| state.config_digest)
            .unwrap_or(self.config_digest)
    }

    fn current_config_id(&self) -> ConfigId {
        self.configuration
            .lock()
            .map(|state| state.config_id)
            .unwrap_or(self.config_id)
    }

    fn wal_checkpoint(&self) -> Result<WalCheckpoint> {
        self.wal
            .lock()
            .map(|wal| wal.checkpoint)
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))
    }

    #[cfg(test)]
    fn wal_stats(&self) -> Result<(u64, u64, u64)> {
        self.wal
            .lock()
            .map(|wal| {
                (
                    wal.checkpoint.generation,
                    wal.checkpoint.through_sequence,
                    wal.frame_count,
                )
            })
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))
    }
}

fn configuration_structure_changed(
    previous: &ConfigurationState,
    next: &ConfigurationState,
) -> bool {
    previous.config_id != next.config_id
        || previous.config_digest != next.config_digest
        || previous.membership != next.membership
        || previous.predecessor != next.predecessor
        || previous.seal != next.seal
        || previous.activated != next.activated
}

fn recorder_state_values(state: &RecorderSlotState) -> impl Iterator<Item = &AcceptedValue> {
    [
        state.accepted.as_ref().map(|accepted| &accepted.value),
        state.decided.as_ref().map(|decided| &decided.value),
        state
            .isr
            .first_current
            .as_ref()
            .and_then(|proposal| proposal.value.as_ref()),
        state
            .isr
            .aggregate_current
            .as_ref()
            .and_then(|proposal| proposal.value.as_ref()),
        state
            .isr
            .aggregate_prior
            .as_ref()
            .and_then(|proposal| proposal.value.as_ref()),
        state
            .decided_proof
            .as_ref()
            .and_then(|proof| proof.proposal().value.as_ref()),
    ]
    .into_iter()
    .flatten()
}

pub struct ThreeNodeConsensus {
    cluster_id: ClusterId,
    proposer_id: NodeId,
    epoch: Epoch,
    config_id: ConfigId,
    config_digest: LogHash,
    membership: FixedMembership,
    recorders: Vec<Arc<dyn RecorderRpc>>,
    record_workers: Vec<RecordWorker>,
    control_workers: Vec<ControlWorker>,
    // Read fences must not queue behind recovery/control RPCs whose network
    // deadline is intentionally longer. A lost majority can otherwise occupy
    // two control workers and turn a read-only quorum check into the caller's
    // HTTP timeout instead of a prompt Unavailable result.
    read_fence_workers: Vec<ControlWorker>,
    priority_source: Arc<dyn PrioritySource>,
    #[cfg(feature = "test-hooks")]
    test_instance_id: u64,
    proposal_sequence: AtomicU64,
    sequential_tip: Mutex<SingleNodeState>,
}

struct RecordJob {
    index: usize,
    context: RecorderRpcContext,
    request: RecordRequest,
    result: std::sync::mpsc::SyncSender<(usize, Result<RecordSummary>)>,
}

struct RecordWorker {
    state: Arc<RecordWorkerState>,
    handle: Option<thread::JoinHandle<()>>,
}

struct RecordWorkerState {
    queue: Arc<RecordQueue>,
    pending: Arc<AtomicUsize>,
    cancellation: Arc<AtomicBool>,
    quarantined: AtomicBool,
    #[cfg(feature = "test-hooks")]
    live_groups: Mutex<BTreeMap<usize, (RpcCallGroup, usize)>>,
}

struct RecordQueue {
    state: Mutex<RecordQueueState>,
    available: Condvar,
}

struct RecordQueueState {
    jobs: VecDeque<QueuedRecordJob>,
    closed: bool,
}

struct QueuedRecordJob {
    job: RecordJob,
    completion: ControlCompletionGuard,
}

#[cfg(test)]
struct RecordWorkerPanicAfterPopHook {
    worker_identity: usize,
    popped: std::sync::mpsc::SyncSender<()>,
}

#[cfg(test)]
static RECORD_WORKER_PANIC_AFTER_POP: std::sync::OnceLock<
    Mutex<Vec<RecordWorkerPanicAfterPopHook>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
struct RecordWorkerPanicAfterPopGuard {
    worker_identity: usize,
}

#[cfg(test)]
impl Drop for RecordWorkerPanicAfterPopGuard {
    fn drop(&mut self) {
        let hooks = RECORD_WORKER_PANIC_AFTER_POP.get_or_init(|| Mutex::new(Vec::new()));
        lock_unpoison(hooks).retain(|hook| hook.worker_identity != self.worker_identity);
    }
}

#[cfg(test)]
fn arm_record_worker_panic_after_pop(
    worker: &Arc<RecordWorkerState>,
    popped: std::sync::mpsc::SyncSender<()>,
) -> RecordWorkerPanicAfterPopGuard {
    let worker_identity = Arc::as_ptr(worker) as usize;
    let hooks = RECORD_WORKER_PANIC_AFTER_POP.get_or_init(|| Mutex::new(Vec::new()));
    let mut hooks = lock_unpoison(hooks);
    assert!(
        !hooks
            .iter()
            .any(|hook| hook.worker_identity == worker_identity),
        "only one record worker panic hook may be armed per worker"
    );
    hooks.push(RecordWorkerPanicAfterPopHook {
        worker_identity,
        popped,
    });
    RecordWorkerPanicAfterPopGuard { worker_identity }
}

#[cfg(test)]
fn take_record_worker_panic_after_pop(worker_identity: usize) -> bool {
    let hooks = RECORD_WORKER_PANIC_AFTER_POP.get_or_init(|| Mutex::new(Vec::new()));
    let mut hooks = lock_unpoison(hooks);
    let Some(index) = hooks
        .iter()
        .position(|hook| hook.worker_identity == worker_identity)
    else {
        return false;
    };
    let hook = hooks.swap_remove(index);
    let _ = hook.popped.send(());
    true
}

#[cfg(all(test, feature = "test-hooks"))]
struct RecordReplySentHook {
    worker_identity: usize,
    group_cancellation: Arc<AtomicBool>,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

#[cfg(all(test, feature = "test-hooks"))]
static RECORD_REPLY_SENT_HOOKS: std::sync::OnceLock<Mutex<Vec<RecordReplySentHook>>> =
    std::sync::OnceLock::new();

#[cfg(all(test, feature = "test-hooks"))]
struct RecordReplySentHookGuard {
    worker_identity: usize,
    group_cancellation: Arc<AtomicBool>,
}

#[cfg(all(test, feature = "test-hooks"))]
impl Drop for RecordReplySentHookGuard {
    fn drop(&mut self) {
        let hooks = RECORD_REPLY_SENT_HOOKS.get_or_init(|| Mutex::new(Vec::new()));
        lock_unpoison(hooks).retain(|hook| {
            hook.worker_identity != self.worker_identity
                || !Arc::ptr_eq(&hook.group_cancellation, &self.group_cancellation)
        });
    }
}

#[cfg(all(test, feature = "test-hooks"))]
fn pause_after_next_record_reply_sent(
    worker: &Arc<RecordWorkerState>,
    group_cancellation: Arc<AtomicBool>,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
) -> RecordReplySentHookGuard {
    let worker_identity = Arc::as_ptr(worker) as usize;
    let hooks = RECORD_REPLY_SENT_HOOKS.get_or_init(|| Mutex::new(Vec::new()));
    let mut hooks = lock_unpoison(hooks);
    assert!(
        !hooks.iter().any(|hook| {
            hook.worker_identity == worker_identity
                && Arc::ptr_eq(&hook.group_cancellation, &group_cancellation)
        }),
        "only one post-reply hook may be armed per record worker group"
    );
    hooks.push(RecordReplySentHook {
        worker_identity,
        group_cancellation: Arc::clone(&group_cancellation),
        entered,
        release,
    });
    RecordReplySentHookGuard {
        worker_identity,
        group_cancellation,
    }
}

#[cfg(all(test, feature = "test-hooks"))]
fn pause_after_record_reply_sent(
    worker_identity: usize,
    group_cancellation: Option<Arc<AtomicBool>>,
) {
    let Some(group_cancellation) = group_cancellation else {
        return;
    };
    let hooks = RECORD_REPLY_SENT_HOOKS.get_or_init(|| Mutex::new(Vec::new()));
    let hook = {
        let mut hooks = lock_unpoison(hooks);
        hooks
            .iter()
            .position(|hook| {
                hook.worker_identity == worker_identity
                    && Arc::ptr_eq(&hook.group_cancellation, &group_cancellation)
            })
            .map(|index| hooks.swap_remove(index))
    };
    let Some(hook) = hook else {
        return;
    };
    hook.entered.send(()).unwrap();
    let (released, condition) = &*hook.release;
    let mut released = lock_unpoison(released);
    while !*released {
        released = condition
            .wait(released)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}

impl RecordWorker {
    fn spawn(
        recorder_id: NodeId,
        recorder: Arc<dyn RecorderRpc>,
        config_id: ConfigId,
        config_digest: LogHash,
    ) -> Result<Self> {
        let expected_id = recorder_id;
        let queue = Arc::new(RecordQueue {
            state: Mutex::new(RecordQueueState {
                jobs: VecDeque::with_capacity(RECORD_WORKER_QUEUE_CAPACITY),
                closed: false,
            }),
            available: Condvar::new(),
        });
        let state = Arc::new(RecordWorkerState {
            queue,
            pending: Arc::new(AtomicUsize::new(0)),
            cancellation: Arc::new(AtomicBool::new(false)),
            quarantined: AtomicBool::new(false),
            #[cfg(feature = "test-hooks")]
            live_groups: Mutex::new(BTreeMap::new()),
        });
        let worker_state = Arc::clone(&state);
        let handle = thread::Builder::new()
            .spawn(move || {
                let abnormal = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    record_worker_loop(
                        &worker_state,
                        recorder.as_ref(),
                        &expected_id,
                        config_id,
                        config_digest,
                    );
                }))
                .is_err();
                if abnormal {
                    worker_state.quarantine();
                }
            })
            .map_err(|error| Error::Io(error.to_string()))?;
        Ok(Self {
            state,
            handle: Some(handle),
        })
    }

    #[cfg(test)]
    fn dispatch(&self, job: RecordJob) -> RecordDispatch {
        self.dispatch_inner(job, None, None)
    }

    fn dispatch_mutating_group(
        &self,
        job: RecordJob,
        group: &RpcCallGroup,
        mutation_started: &AtomicBool,
    ) -> RecordDispatch {
        self.dispatch_inner(job, Some(group), Some(mutation_started))
    }

    fn dispatch_inner(
        &self,
        job: RecordJob,
        group: Option<&RpcCallGroup>,
        mutation_started: Option<&AtomicBool>,
    ) -> RecordDispatch {
        let grouped = group.is_some();
        let mut queued_job = Some(QueuedRecordJob {
            job,
            completion: ControlCompletionGuard::new(Arc::clone(&self.state.pending)),
        });
        let (pruned, error, outcome) = {
            let mut queue = lock_unpoison(&self.state.queue.state);
            let mut pruned = Vec::new();
            let mut retained = VecDeque::with_capacity(queue.jobs.len());
            while let Some(job) = queue.jobs.pop_front() {
                if job.is_cancelled() {
                    pruned.push(job);
                } else {
                    retained.push_back(job);
                }
            }
            queue.jobs = retained;
            if queue.closed || self.state.quarantined.load(Ordering::Acquire) {
                (pruned, Some(Error::ProposeFailed), RecordDispatch::Failed)
            } else if queue.jobs.len() >= RECORD_WORKER_QUEUE_CAPACITY {
                (
                    pruned,
                    Some(Error::Io(
                        "recorder worker queue is temporarily full".into(),
                    )),
                    RecordDispatch::Saturated,
                )
            } else {
                let queued = queued_job.as_mut().expect("record job must be present");
                // Mark the mutating admission and register its lease while the
                // queue is locked, before the worker can observe the job.
                queued.completion.arm(group, &self.state);
                #[cfg(feature = "test-hooks")]
                queued.completion.attach_record_worker(&self.state, group);
                if let Some(mutation_started) = mutation_started {
                    mutation_started.store(true, Ordering::Release);
                }
                queue
                    .jobs
                    .push_back(queued_job.take().expect("record job must be present"));
                self.state.queue.available.notify_one();
                (pruned, None, RecordDispatch::Accepted)
            }
        };
        for job in pruned {
            if job.completion.group().is_some() {
                job.fail(Error::RpcCancelled);
            }
        }
        // Group collectors classify a pre-admission failure from
        // `RecordDispatch`; injecting a synthetic reply would make it look
        // like an un-attributable admitted result.  The direct test-only
        // dispatch path keeps the reply contract used by worker unit tests.
        if let Some(error) = error.filter(|_| !grouped) {
            queued_job.unwrap().fail(error);
        }
        outcome
    }

    fn is_idle(&self) -> bool {
        self.state.pending.load(Ordering::Acquire) == 0
    }

    fn shutdown(&mut self) {
        // Closing/draining is also the admission fence. Only the post-close
        // snapshot can prove that no recorder call is running: a pre-close
        // idle observation could race a newly admitted noncooperative RPC.
        let join_idle_worker = self.state.close_and_drain();
        let Some(handle) = self.handle.take() else {
            return;
        };
        if join_idle_worker || handle.is_finished() {
            let _ = handle.join();
            return;
        }
        // A running custom RecorderRpc may be stuck in a syscall or ignore
        // cancellation. Keep shutdown bounded in that case: detaching retains
        // only the worker-owned recorder/state Arcs, never a runtime borrow.
        drop(handle);
    }
}

impl QueuedRecordJob {
    #[cfg(feature = "test-hooks")]
    fn test_event(&self, worker: usize, event: TestWorkerEvent) {
        if let Some(group) = self.completion.group() {
            group.record_test_worker_event(worker, event);
        }
    }

    fn run(
        self,
        recorder: &dyn RecorderRpc,
        worker_cancellation: &Arc<AtomicBool>,
        _worker_identity: usize,
        expected_id: &NodeId,
        config_id: ConfigId,
        config_digest: LogHash,
    ) {
        #[cfg(feature = "test-hooks")]
        self.test_event(_worker_identity, TestWorkerEvent::RunningEntered);
        if self.is_cancelled() {
            #[cfg(feature = "test-hooks")]
            self.test_event(_worker_identity, TestWorkerEvent::RunningExited);
            self.fail(Error::RpcCancelled);
            return;
        }
        #[cfg(test)]
        if take_record_worker_panic_after_pop(_worker_identity) {
            panic!("injected record worker panic after queue pop");
        }
        let expected_slot = self.job.request.slot;
        let mut context = self
            .job
            .context
            .with_cancellation(Arc::clone(worker_cancellation));
        if let Some(group) = self.completion.group() {
            context = context.with_cancellation(group.token());
        }
        let reply = recorder_rpc(RecorderRpcOperation::Mutating, || {
            recorder.record(&context, self.job.request.clone())
        })
        .and_then(|reply| {
            if reply.recorder_id == *expected_id
                && reply.slot == expected_slot
                && reply.config_id == config_id
                && reply.config_digest == config_digest
            {
                Ok(reply)
            } else {
                Err(Error::Rejected(RejectReason::InvalidRequest))
            }
        });
        #[cfg(all(test, feature = "test-hooks"))]
        let group_cancellation = self.completion.group().map(RpcCallGroup::token);
        let _ = self.job.result.send((self.job.index, reply));
        #[cfg(all(test, feature = "test-hooks"))]
        pause_after_record_reply_sent(_worker_identity, group_cancellation);
        #[cfg(feature = "test-hooks")]
        self.test_event(_worker_identity, TestWorkerEvent::ReplySent);
        #[cfg(feature = "test-hooks")]
        self.test_event(_worker_identity, TestWorkerEvent::RunningExited);
    }

    fn fail(self, error: Error) {
        let _ = self.job.result.send((self.job.index, Err(error)));
    }

    fn fail_for_worker(self) {
        self.fail(Error::UnknownOutcome);
    }

    fn belongs_to(&self, group: &RpcCallGroup) -> bool {
        self.completion
            .group()
            .is_some_and(|candidate| candidate.is_same(group))
    }

    fn is_cancelled(&self) -> bool {
        self.completion
            .group()
            .is_some_and(RpcCallGroup::is_cancelled)
    }
}

fn record_worker_loop(
    state: &RecordWorkerState,
    recorder: &dyn RecorderRpc,
    expected_id: &NodeId,
    config_id: ConfigId,
    config_digest: LogHash,
) {
    loop {
        let job = {
            let mut queue = lock_unpoison(&state.queue.state);
            while queue.jobs.is_empty() && !queue.closed {
                queue = state
                    .queue
                    .available
                    .wait(queue)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            queue.jobs.pop_front()
        };
        let Some(job) = job else {
            break;
        };
        #[cfg(feature = "test-hooks")]
        job.test_event(
            state as *const RecordWorkerState as usize,
            TestWorkerEvent::Popped,
        );
        job.run(
            recorder,
            &state.cancellation,
            state as *const RecordWorkerState as usize,
            expected_id,
            config_id,
            config_digest,
        );
    }
}

impl RecordWorkerState {
    #[cfg(feature = "test-hooks")]
    fn test_register_group(&self, group: &RpcCallGroup) {
        let key = Arc::as_ptr(&group.state) as usize;
        let mut groups = lock_unpoison(&self.live_groups);
        let entry = groups.entry(key).or_insert_with(|| (group.clone(), 0));
        entry.1 += 1;
    }

    #[cfg(feature = "test-hooks")]
    fn test_complete_group(&self, group: &RpcCallGroup) {
        let key = Arc::as_ptr(&group.state) as usize;
        let mut groups = lock_unpoison(&self.live_groups);
        let (_, count) = groups
            .get_mut(&key)
            .expect("record worker completion must match an admitted group lease");
        assert!(
            *count > 0,
            "record worker group lease accounting must not underflow"
        );
        *count -= 1;
        if *count == 0 {
            groups.remove(&key);
        }
    }

    #[cfg(feature = "test-hooks")]
    fn test_record_worker_quarantined(&self) {
        for (group, _) in lock_unpoison(&self.live_groups).values() {
            group.record_test_worker_event(
                self as *const Self as usize,
                TestWorkerEvent::Quarantined,
            );
        }
    }

    fn prune_pending(&self, group: &RpcCallGroup) {
        let pruned = {
            let mut queue = lock_unpoison(&self.queue.state);
            let mut pruned = Vec::new();
            let mut retained = VecDeque::with_capacity(queue.jobs.len());
            while let Some(job) = queue.jobs.pop_front() {
                if job.belongs_to(group) {
                    pruned.push(job);
                } else {
                    retained.push_back(job);
                }
            }
            queue.jobs = retained;
            pruned
        };
        for job in pruned {
            #[cfg(feature = "test-hooks")]
            job.test_event(self as *const Self as usize, TestWorkerEvent::Pruned);
            job.fail(Error::RpcCancelled);
        }
    }

    fn quarantine(&self) {
        if self.quarantined.swap(true, Ordering::AcqRel) {
            return;
        }
        #[cfg(feature = "test-hooks")]
        self.test_record_worker_quarantined();
        self.close_and_drain();
    }

    /// Fences admission and returns whether no job remained running after
    /// queued jobs were drained. A true result permits a bounded worker join.
    fn close_and_drain(&self) -> bool {
        self.cancellation.store(true, Ordering::Release);
        let drained = {
            let mut queue = lock_unpoison(&self.queue.state);
            queue.closed = true;
            queue.jobs.drain(..).collect::<Vec<_>>()
        };
        self.queue.available.notify_all();
        for job in drained {
            #[cfg(feature = "test-hooks")]
            {
                let worker = self as *const Self as usize;
                job.test_event(worker, TestWorkerEvent::CloseDrained);
            }
            job.fail_for_worker();
        }
        self.pending.load(Ordering::Acquire) == 0
    }
}

impl RpcCallWorker for RecordWorkerState {
    fn prune_pending(&self, group: &RpcCallGroup) {
        RecordWorkerState::prune_pending(self, group);
    }

    fn quarantine(&self) {
        RecordWorkerState::quarantine(self);
    }

    fn worker_identity(&self) -> usize {
        self as *const Self as usize
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RecordDispatch {
    NotAttempted,
    Accepted,
    Saturated,
    Failed,
}

impl Drop for RecordWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

enum ControlJob {
    InstallProof {
        index: usize,
        context: RecorderRpcContext,
        proof: DecisionProof,
        membership: Membership,
        result: std::sync::mpsc::SyncSender<(usize, Result<()>)>,
    },
    InspectProof {
        index: usize,
        context: RecorderRpcContext,
        slot: Slot,
        result: std::sync::mpsc::SyncSender<(usize, Result<Option<DecisionProof>>)>,
    },
    InspectSummary {
        index: usize,
        context: RecorderRpcContext,
        slot: Slot,
        result: std::sync::mpsc::SyncSender<(usize, Result<Option<RecordSummary>>)>,
    },
    ObserveReadFence {
        index: usize,
        context: RecorderRpcContext,
        request: ReadFenceRequest,
        result: std::sync::mpsc::SyncSender<(usize, Result<ReadFenceObservation>)>,
    },
    StoreCommand {
        index: usize,
        context: RecorderRpcContext,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
        result: std::sync::mpsc::SyncSender<(usize, Result<()>)>,
    },
    FetchCommand {
        index: usize,
        context: RecorderRpcContext,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        result: std::sync::mpsc::SyncSender<(usize, Result<Option<StoredCommand>>)>,
    },
    StageEffectBundleChunk {
        index: usize,
        context: RecorderRpcContext,
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
        ordinal: u16,
        chunk: Vec<u8>,
        result: std::sync::mpsc::SyncSender<(usize, Result<()>)>,
    },
    FinalizeStagedEffectBundle {
        index: usize,
        context: RecorderRpcContext,
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
        result: std::sync::mpsc::SyncSender<(usize, Result<()>)>,
    },
    /// One logical effect fetch owns one reply stream and one cancellation
    /// group for both the manifest and every chunk.  Keeping those reads in
    /// the same group prevents a slow recorder from consuming the drain
    /// reserve between otherwise healthy fetch stages.
    FetchEffectBundle {
        index: usize,
        context: RecorderRpcContext,
        binding: EffectBundleBinding,
        kind: EffectFetchKind,
        result: std::sync::mpsc::SyncSender<(usize, EffectFetchReply)>,
    },
}

#[derive(Clone, Copy)]
enum EffectFetchKind {
    Manifest,
    Chunk { ordinal: u16 },
}

enum EffectFetchReply {
    Manifest(Result<Option<StoredCommand>>),
    Chunk {
        ordinal: u16,
        result: Result<Option<Vec<u8>>>,
    },
}

struct EffectFetchContext<'a> {
    budget: &'a ControlCallBudget,
    group: &'a ControlCallGroup,
    sender: &'a std::sync::mpsc::SyncSender<(usize, EffectFetchReply)>,
    receiver: &'a std::sync::mpsc::Receiver<(usize, EffectFetchReply)>,
    binding: &'a EffectBundleBinding,
}

/// Whether an admitted recorder RPC can have durably changed recorder state.
///
/// A panic only tells us that no response was observed.  For mutations, that
/// leaves the outcome indeterminate; for reads it remains a definite local
/// failure.  Keep this explicit at every catch boundary so new control RPCs
/// cannot silently inherit the wrong retry semantics.
#[derive(Clone, Copy)]
enum RecorderRpcOperation {
    Mutating,
    ReadOnly,
}

impl ControlJob {
    fn operation(&self) -> RecorderRpcOperation {
        match self {
            Self::InstallProof { .. }
            | Self::StoreCommand { .. }
            | Self::StageEffectBundleChunk { .. }
            | Self::FinalizeStagedEffectBundle { .. } => RecorderRpcOperation::Mutating,
            Self::InspectProof { .. }
            | Self::InspectSummary { .. }
            | Self::ObserveReadFence { .. }
            | Self::FetchCommand { .. }
            | Self::FetchEffectBundle { .. } => RecorderRpcOperation::ReadOnly,
        }
    }

    fn with_cancellation(&mut self, cancellation: Arc<AtomicBool>) {
        let context = match self {
            Self::InstallProof { context, .. }
            | Self::InspectProof { context, .. }
            | Self::InspectSummary { context, .. }
            | Self::ObserveReadFence { context, .. }
            | Self::StoreCommand { context, .. }
            | Self::FetchCommand { context, .. }
            | Self::StageEffectBundleChunk { context, .. }
            | Self::FinalizeStagedEffectBundle { context, .. }
            | Self::FetchEffectBundle { context, .. } => context,
        };
        *context = context.with_cancellation(cancellation);
    }

    fn run(self, recorder: &dyn RecorderRpc) {
        match self {
            Self::InstallProof {
                index,
                context,
                proof,
                membership,
                result,
            } => {
                let reply = recorder_rpc(RecorderRpcOperation::Mutating, || {
                    recorder.install_decision_proof(&context, proof, &membership)
                });
                let _ = result.send((index, reply));
            }
            Self::InspectProof {
                index,
                context,
                slot,
                result,
            } => {
                let reply = recorder_rpc(RecorderRpcOperation::ReadOnly, || {
                    recorder.inspect_decision_proof(&context, slot)
                });
                let _ = result.send((index, reply));
            }
            Self::InspectSummary {
                index,
                context,
                slot,
                result,
            } => {
                let reply = recorder_rpc(RecorderRpcOperation::ReadOnly, || {
                    recorder.inspect_record_summary(&context, slot)
                });
                let _ = result.send((index, reply));
            }
            Self::ObserveReadFence {
                index,
                context,
                request,
                result,
            } => {
                let reply = recorder_rpc(RecorderRpcOperation::ReadOnly, || {
                    recorder.observe_read_fence(&context, request)
                });
                let _ = result.send((index, reply));
            }
            Self::StoreCommand {
                index,
                context,
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                command,
                result,
            } => {
                let reply = recorder_rpc(RecorderRpcOperation::Mutating, || {
                    recorder.store_command_for(
                        &context,
                        cluster_id,
                        epoch,
                        config_id,
                        config_digest,
                        command_hash,
                        command,
                    )
                });
                let _ = result.send((index, reply));
            }
            Self::FetchCommand {
                index,
                context,
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                result,
            } => {
                let reply = recorder_rpc(RecorderRpcOperation::ReadOnly, || {
                    recorder.fetch_command_for(
                        &context,
                        cluster_id,
                        epoch,
                        config_id,
                        config_digest,
                        command_hash,
                    )
                });
                let _ = result.send((index, reply));
            }
            Self::StageEffectBundleChunk {
                index,
                context,
                binding,
                manifest_command,
                ordinal,
                chunk,
                result,
            } => {
                let reply = recorder_rpc(RecorderRpcOperation::Mutating, || {
                    recorder.stage_effect_bundle_chunk(
                        &context,
                        binding,
                        manifest_command,
                        ordinal,
                        chunk,
                    )
                });
                let _ = result.send((index, reply));
            }
            Self::FinalizeStagedEffectBundle {
                index,
                context,
                binding,
                manifest_command,
                result,
            } => {
                let reply = recorder_rpc(RecorderRpcOperation::Mutating, || {
                    recorder.finalize_staged_effect_bundle(&context, binding, manifest_command)
                });
                let _ = result.send((index, reply));
            }
            Self::FetchEffectBundle {
                index,
                context,
                binding,
                kind,
                result,
            } => {
                let reply = match kind {
                    EffectFetchKind::Manifest => EffectFetchReply::Manifest(recorder_rpc(
                        RecorderRpcOperation::ReadOnly,
                        || recorder.fetch_effect_bundle_manifest(&context, binding),
                    )),
                    EffectFetchKind::Chunk { ordinal } => EffectFetchReply::Chunk {
                        ordinal,
                        result: recorder_rpc(RecorderRpcOperation::ReadOnly, || {
                            recorder.fetch_effect_bundle_chunk(&context, binding, ordinal)
                        }),
                    },
                };
                let _ = result.send((index, reply));
            }
        }
    }

    fn fail(self, error: Error) {
        match self {
            Self::InstallProof { index, result, .. }
            | Self::StoreCommand { index, result, .. }
            | Self::StageEffectBundleChunk { index, result, .. }
            | Self::FinalizeStagedEffectBundle { index, result, .. } => {
                let _ = result.send((index, Err(error)));
            }
            Self::InspectProof { index, result, .. } => {
                let _ = result.send((index, Err(error)));
            }
            Self::InspectSummary { index, result, .. } => {
                let _ = result.send((index, Err(error)));
            }
            Self::ObserveReadFence { index, result, .. } => {
                let _ = result.send((index, Err(error)));
            }
            Self::FetchCommand { index, result, .. } => {
                let _ = result.send((index, Err(error)));
            }
            Self::FetchEffectBundle {
                index,
                kind,
                result,
                ..
            } => {
                let reply = match kind {
                    EffectFetchKind::Manifest => EffectFetchReply::Manifest(Err(error)),
                    EffectFetchKind::Chunk { ordinal } => EffectFetchReply::Chunk {
                        ordinal,
                        result: Err(error),
                    },
                };
                let _ = result.send((index, reply));
            }
        }
    }
}

fn recorder_rpc<T>(operation: RecorderRpcOperation, call: impl FnOnce() -> Result<T>) -> Result<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(call)).unwrap_or_else(|_| {
        Err(match operation {
            RecorderRpcOperation::Mutating => Error::UnknownOutcome,
            RecorderRpcOperation::ReadOnly => Error::ProposeFailed,
        })
    })
}

#[derive(Clone)]
struct RpcCallGroup {
    state: Arc<RpcCallGroupState>,
}

struct RpcCallGroupState {
    cancelled: Arc<AtomicBool>,
    outstanding: Mutex<Vec<Weak<RpcGroupLease>>>,
    drained: Condvar,
    #[cfg(feature = "test-hooks")]
    test_probes: Mutex<Vec<TestProbeAttachment>>,
}

// Existing control paths keep their internal name while record mutations use
// the same call-local accounting implementation.
type ControlCallGroup = RpcCallGroup;

trait RpcCallWorker: Send + Sync {
    /// Removes only jobs which are still queued for `group`. A running
    /// mutation is deliberately not interrupted here: its operation-local W
    /// remains authoritative until the caller-owned D.
    fn prune_pending(&self, group: &RpcCallGroup);
    fn quarantine(&self);
    fn worker_identity(&self) -> usize;
}

/// Operation-local RPC-group accounting exposed only to downstream test
/// harnesses. Registrations are keyed by either a caller-owned cancellation
/// token or a proposal slot scoped to one consensus instance. One probe Arc
/// may have only one live registration and can be reused only after every
/// admitted lease has drained and every captured logical RPC-group attachment
/// from its previous registration has dropped. Dropping a registration guard
/// alone does not revoke an attachment already captured by a group.
#[cfg(feature = "test-hooks")]
#[derive(Debug, Default)]
pub struct TestControlOperationProbe {
    lifecycle: Mutex<TestProbeLifecycle>,
    lifecycle_changed: Condvar,
    worker_transitions: Mutex<BTreeMap<usize, TestWorkerTransition>>,
}

/// One mutex-protected lifecycle snapshot backs every probe counter.  Tests
/// must never infer a lease state by comparing separately published atomics:
/// admission and completion are concurrent by design.
#[cfg(feature = "test-hooks")]
#[derive(Debug, Default)]
struct TestProbeLifecycle {
    generation: u64,
    attachments: Vec<Weak<TestProbeAttachmentLease>>,
    leases: usize,
    dispatch_count: usize,
    observed_max_outstanding: usize,
    cancel_count: usize,
    quarantine_count: usize,
    drained_count: usize,
}

#[cfg(feature = "test-hooks")]
impl TestProbeLifecycle {
    fn has_admitted_outstanding_lease(&self) -> bool {
        self.dispatch_count != 0 && self.observed_max_outstanding != 0 && self.leases != 0
    }

    fn has_drained_every_admitted_lease(&self) -> bool {
        self.dispatch_count != 0 && self.leases == 0 && self.drained_count == self.dispatch_count
    }
}

#[cfg(feature = "test-hooks")]
#[derive(Debug, Eq, PartialEq)]
enum TestProbeLifecycleWait {
    Ready,
    TimedOut,
    GenerationChanged,
}

/// A typed rejection from the test-only probe registration API.  Rejections
/// are normal test-harness outcomes and must not poison shared hook state.
#[cfg(feature = "test-hooks")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestProbeRegistrationError {
    DuplicateLiveRegistration,
    ActiveAttachments { attachments: usize },
    LiveLeases { leases: usize },
    GenerationExhausted,
}

#[cfg(feature = "test-hooks")]
#[derive(Clone)]
struct TestProbeAttachment {
    probe: Arc<TestControlOperationProbe>,
    generation: u64,
    // One Arc is captured per logical RpcCallGroup. Event-path clones share
    // this lease, so only the final group-state attachment drop releases the
    // generation for reuse.
    _lease: Arc<TestProbeAttachmentLease>,
}

/// Keeps a captured probe generation live for the lifetime of its owning
/// RpcCallGroupState attachment. The lifecycle keeps only a `Weak` reference,
/// so the final attachment drop is authoritative and cannot form a cycle.
/// Event-path clones share this Arc and therefore do not create more logical
/// group attachments.
#[cfg(feature = "test-hooks")]
struct TestProbeAttachmentLease;

/// Test-hook-only lifecycle counters for one record worker. They are kept off
/// production paths and make a leaked operation lease attributable to a
/// concrete worker rather than a global outstanding count.
#[cfg(feature = "test-hooks")]
#[derive(Clone, Debug, Default)]
pub struct TestWorkerTransition {
    pub worker_identity: usize,
    /// Leases still owned by this worker for the probed group. This follows
    /// the guard's admission/drop lifetime, so diagnostics can distinguish a
    /// real worker lease from a stale observation.
    pub live_leases: usize,
    pub enqueued: usize,
    pub popped: usize,
    pub running_entered: usize,
    pub running_exited: usize,
    pub reply_sent: usize,
    pub pruned: usize,
    pub close_drained: usize,
    pub quarantined: usize,
    pub completion_dropped: usize,
}

#[cfg(feature = "test-hooks")]
impl TestControlOperationProbe {
    pub fn pending(&self) -> usize {
        lock_unpoison(&self.lifecycle).leases
    }

    pub fn outstanding(&self) -> usize {
        lock_unpoison(&self.lifecycle).leases
    }

    pub fn dispatch_count(&self) -> usize {
        lock_unpoison(&self.lifecycle).dispatch_count
    }

    pub fn observed_max_outstanding(&self) -> usize {
        lock_unpoison(&self.lifecycle).observed_max_outstanding
    }

    pub fn cancel_count(&self) -> usize {
        lock_unpoison(&self.lifecycle).cancel_count
    }

    pub fn quarantine_count(&self) -> usize {
        lock_unpoison(&self.lifecycle).quarantine_count
    }

    pub fn drained_count(&self) -> usize {
        lock_unpoison(&self.lifecycle).drained_count
    }

    /// Waits for this exact probe's group to admit at least one lease that is
    /// still outstanding. The condition is checked while holding the same
    /// mutex used by lease admission, so an admission that happened before
    /// the waiter subscribes is observed rather than missed. Returns `false`
    /// if the timeout expires or this probe's generation changes while waiting;
    /// a waiter never consumes readiness from a later registration generation.
    pub fn wait_for_admitted_outstanding(&self, timeout: Duration) -> bool {
        matches!(
            self.wait_for_admitted_outstanding_after_generation_capture(timeout, || {}),
            TestProbeLifecycleWait::Ready
        )
    }

    /// Waits until every lease admitted by this exact probe generation has
    /// completed. The lifecycle condition and completion notification share
    /// one mutex/Condvar, so a completion published before the waiter starts
    /// remains observable. Returns `false` on timeout or generation reset.
    pub fn wait_for_quiescence(&self, timeout: Duration) -> bool {
        matches!(
            self.wait_for_quiescence_after_generation_capture(timeout, || {}),
            TestProbeLifecycleWait::Ready
        )
    }

    fn wait_for_admitted_outstanding_after_generation_capture<F>(
        &self,
        timeout: Duration,
        generation_captured: F,
    ) -> TestProbeLifecycleWait
    where
        F: FnOnce(),
    {
        self.wait_for_lifecycle_condition_after_generation_capture(
            timeout,
            generation_captured,
            TestProbeLifecycle::has_admitted_outstanding_lease,
        )
    }

    fn wait_for_quiescence_after_generation_capture<F>(
        &self,
        timeout: Duration,
        generation_captured: F,
    ) -> TestProbeLifecycleWait
    where
        F: FnOnce(),
    {
        self.wait_for_lifecycle_condition_after_generation_capture(
            timeout,
            generation_captured,
            TestProbeLifecycle::has_drained_every_admitted_lease,
        )
    }

    fn wait_for_lifecycle_condition_after_generation_capture<F, P>(
        &self,
        timeout: Duration,
        generation_captured: F,
        ready: P,
    ) -> TestProbeLifecycleWait
    where
        F: FnOnce(),
        P: Fn(&TestProbeLifecycle) -> bool,
    {
        let started = Instant::now();
        let mut lifecycle = lock_unpoison(&self.lifecycle);
        let generation = lifecycle.generation;
        generation_captured();
        loop {
            if lifecycle.generation != generation {
                return TestProbeLifecycleWait::GenerationChanged;
            }
            if ready(&lifecycle) {
                return TestProbeLifecycleWait::Ready;
            }
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return TestProbeLifecycleWait::TimedOut;
            };
            let (next_lifecycle, timeout_result) = self
                .lifecycle_changed
                .wait_timeout(lifecycle, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            lifecycle = next_lifecycle;
            if timeout_result.timed_out() {
                if lifecycle.generation != generation {
                    return TestProbeLifecycleWait::GenerationChanged;
                }
                if !ready(&lifecycle) {
                    return TestProbeLifecycleWait::TimedOut;
                }
            }
        }
    }

    #[cfg(test)]
    fn wait_for_admitted_outstanding_after_test_generation_capture(
        &self,
        timeout: Duration,
        captured: Arc<std::sync::Barrier>,
    ) -> TestProbeLifecycleWait {
        self.wait_for_admitted_outstanding_after_generation_capture(timeout, move || {
            captured.wait();
        })
    }

    #[cfg(test)]
    fn wait_for_quiescence_after_test_generation_capture(
        &self,
        timeout: Duration,
        captured: Arc<std::sync::Barrier>,
    ) -> TestProbeLifecycleWait {
        self.wait_for_quiescence_after_generation_capture(timeout, move || {
            captured.wait();
        })
    }

    pub fn worker_transitions(&self) -> Vec<TestWorkerTransition> {
        lock_unpoison(&self.worker_transitions)
            .values()
            .cloned()
            .collect()
    }

    #[cfg(test)]
    fn current_attachment(self: &Arc<Self>) -> TestProbeAttachment {
        let generation = lock_unpoison(&self.lifecycle).generation;
        self.capture_attachment(generation)
    }

    fn begin_generation(&self) -> std::result::Result<u64, TestProbeRegistrationError> {
        let mut lifecycle = lock_unpoison(&self.lifecycle);
        if lifecycle.leases != 0 {
            return Err(TestProbeRegistrationError::LiveLeases {
                leases: lifecycle.leases,
            });
        }
        lifecycle
            .attachments
            .retain(|attachment| attachment.strong_count() != 0);
        if !lifecycle.attachments.is_empty() {
            return Err(TestProbeRegistrationError::ActiveAttachments {
                attachments: lifecycle.attachments.len(),
            });
        }
        let generation = lifecycle
            .generation
            .checked_add(1)
            .ok_or(TestProbeRegistrationError::GenerationExhausted)?;
        *lifecycle = TestProbeLifecycle::default();
        // Keep generation monotonic while resetting all observable counters
        // for the next attachment generation.
        lifecycle.generation = generation;
        // Hold lifecycle through the transition clear. A stale attachment
        // that checked the old generation must not append an event after this
        // new generation starts.
        lock_unpoison(&self.worker_transitions).clear();
        self.lifecycle_changed.notify_all();
        drop(lifecycle);
        Ok(generation)
    }

    /// Captures one generation lease for a logical RPC group. Group capture
    /// and generation registration take the global hook lock before this
    /// lifecycle lock, so a live hook cannot cross a generation reset.
    fn capture_attachment(self: &Arc<Self>, generation: u64) -> TestProbeAttachment {
        let lease = Arc::new(TestProbeAttachmentLease);
        let mut lifecycle = lock_unpoison(&self.lifecycle);
        lifecycle
            .attachments
            .retain(|attachment| attachment.strong_count() != 0);
        lifecycle.attachments.push(Arc::downgrade(&lease));
        drop(lifecycle);
        TestProbeAttachment {
            probe: Arc::clone(self),
            generation,
            _lease: lease,
        }
    }

    fn record_lease_registered(&self, generation: u64) {
        let mut lifecycle = lock_unpoison(&self.lifecycle);
        if lifecycle.generation != generation {
            return;
        }
        lifecycle.leases = lifecycle
            .leases
            .checked_add(1)
            .expect("test probe lease counter overflow");
        lifecycle.dispatch_count = lifecycle
            .dispatch_count
            .checked_add(1)
            .expect("test probe dispatch counter overflow");
        lifecycle.observed_max_outstanding =
            lifecycle.observed_max_outstanding.max(lifecycle.leases);
        self.lifecycle_changed.notify_all();
    }

    fn record_lease_completed(&self, generation: u64) {
        let mut lifecycle = lock_unpoison(&self.lifecycle);
        if lifecycle.generation != generation {
            return;
        }
        lifecycle.leases = lifecycle
            .leases
            .checked_sub(1)
            .expect("test probe completion without a live lease");
        lifecycle.drained_count = lifecycle
            .drained_count
            .checked_add(1)
            .expect("test probe drain counter overflow");
        self.lifecycle_changed.notify_all();
    }

    fn record_cancel(&self, generation: u64) {
        let mut lifecycle = lock_unpoison(&self.lifecycle);
        if lifecycle.generation != generation {
            return;
        }
        lifecycle.cancel_count = lifecycle
            .cancel_count
            .checked_add(1)
            .expect("test probe cancellation counter overflow");
        self.lifecycle_changed.notify_all();
    }

    fn record_quarantine(&self, generation: u64) {
        let mut lifecycle = lock_unpoison(&self.lifecycle);
        if lifecycle.generation != generation {
            return;
        }
        lifecycle.quarantine_count = lifecycle
            .quarantine_count
            .checked_add(1)
            .expect("test probe quarantine counter overflow");
        self.lifecycle_changed.notify_all();
    }

    fn record_worker_event(&self, generation: u64, worker: usize, event: TestWorkerEvent) {
        let lifecycle = lock_unpoison(&self.lifecycle);
        if lifecycle.generation != generation {
            return;
        }
        let mut transitions = lock_unpoison(&self.worker_transitions);
        let entry = transitions
            .entry(worker)
            .or_insert_with(|| TestWorkerTransition {
                worker_identity: worker,
                ..Default::default()
            });
        match event {
            TestWorkerEvent::Enqueued => {
                entry.enqueued += 1;
                entry.live_leases += 1;
            }
            TestWorkerEvent::Popped => entry.popped += 1,
            TestWorkerEvent::RunningEntered => entry.running_entered += 1,
            TestWorkerEvent::RunningExited => entry.running_exited += 1,
            TestWorkerEvent::ReplySent => entry.reply_sent += 1,
            TestWorkerEvent::Pruned => entry.pruned += 1,
            TestWorkerEvent::CloseDrained => entry.close_drained += 1,
            TestWorkerEvent::Quarantined => entry.quarantined += 1,
            TestWorkerEvent::CompletionDropped => {
                entry.completion_dropped += 1;
                entry.live_leases = entry
                    .live_leases
                    .checked_sub(1)
                    .expect("test worker lease accounting underflow");
            }
        }
        drop(lifecycle);
    }
}

#[cfg(feature = "test-hooks")]
struct TestControlOperationHook {
    id: u64,
    generation: u64,
    root_cancellation: Option<Arc<AtomicBool>>,
    consensus_instance_id: Option<u64>,
    slot: Option<Slot>,
    probe: Arc<TestControlOperationProbe>,
}

#[cfg(feature = "test-hooks")]
static TEST_CONTROL_OPERATION_HOOKS: std::sync::OnceLock<Mutex<Vec<TestControlOperationHook>>> =
    std::sync::OnceLock::new();

/// Removes the test probe registration when its test scope ends.
#[cfg(feature = "test-hooks")]
pub struct TestControlOperationProbeGuard {
    id: u64,
}

#[cfg(feature = "test-hooks")]
impl Drop for TestControlOperationProbeGuard {
    fn drop(&mut self) {
        let hooks = TEST_CONTROL_OPERATION_HOOKS.get_or_init(|| Mutex::new(Vec::new()));
        lock_unpoison(hooks).retain(|hook| hook.id != self.id);
    }
}

#[cfg(feature = "test-hooks")]
fn register_test_operation_probe(
    mut hook: TestControlOperationHook,
) -> std::result::Result<(), TestProbeRegistrationError> {
    let hooks = TEST_CONTROL_OPERATION_HOOKS.get_or_init(|| Mutex::new(Vec::new()));
    let mut hooks = lock_unpoison(hooks);
    if hooks
        .iter()
        .any(|candidate| Arc::ptr_eq(&candidate.probe, &hook.probe))
    {
        return Err(TestProbeRegistrationError::DuplicateLiveRegistration);
    }
    // A dropped registration guard does not revoke a group attachment. Refuse
    // reuse until every captured group has gone away, including groups that
    // have not admitted a lease yet.
    hook.generation = hook.probe.begin_generation()?;
    hooks.push(hook);
    Ok(())
}

/// Registers an operation-local test probe for the externally owned
/// cancellation token carried by `context`. `with_timeout_and_cancellation`
/// appends that token last, which keeps independently running tests isolated.
#[cfg(feature = "test-hooks")]
pub fn install_test_control_operation_probe(
    context: &RecorderRpcContext,
    probe: Arc<TestControlOperationProbe>,
) -> std::result::Result<TestControlOperationProbeGuard, TestProbeRegistrationError> {
    let root_cancellation = context
        .cancellations
        .last()
        .cloned()
        .expect("recorder contexts always carry a cancellation token");
    let id = NEXT_TEST_CONTROL_OPERATION_HOOK_ID.fetch_add(1, Ordering::Relaxed);
    register_test_operation_probe(TestControlOperationHook {
        id,
        generation: 0,
        root_cancellation: Some(root_cancellation),
        consensus_instance_id: None,
        slot: None,
        probe,
    })?;
    Ok(TestControlOperationProbeGuard { id })
}

#[cfg(feature = "test-hooks")]
static NEXT_TEST_CONTROL_OPERATION_HOOK_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(feature = "test-hooks")]
static NEXT_TEST_CONSENSUS_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

/// Returns all live instance-scoped record-probe registrations. This is mainly
/// useful to diagnose a harness-wide leak; parallel tests should instead use
/// [`ThreeNodeConsensus::test_record_operation_probe_registration_count`].
#[cfg(feature = "test-hooks")]
pub fn test_record_operation_probe_registration_count() -> usize {
    TEST_CONTROL_OPERATION_HOOKS
        .get()
        .map(|hooks| {
            lock_unpoison(hooks)
                .iter()
                .filter(|hook| hook.consensus_instance_id.is_some())
                .count()
        })
        .unwrap_or(0)
}

struct RpcGroupLease {
    state: AtomicUsize,
    group: RpcCallGroup,
    // The group is allowed to outlive a worker during shutdown.  Keeping this
    // weak prevents a timed-out call from extending worker/queue lifetime.
    worker: Weak<dyn RpcCallWorker>,
    #[cfg(feature = "test-hooks")]
    worker_identity: usize,
}

#[cfg(test)]
struct ControlDrainTimeoutHook {
    group_cancellation: Arc<AtomicBool>,
    worker: Arc<ControlWorkerState>,
    fired: std::sync::mpsc::SyncSender<()>,
}

#[cfg(test)]
static CONTROL_DRAIN_TIMEOUT_HOOK: std::sync::OnceLock<Mutex<Option<ControlDrainTimeoutHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
struct ControlDrainTimeoutHookGuard;

#[cfg(test)]
impl Drop for ControlDrainTimeoutHookGuard {
    fn drop(&mut self) {
        let hook = CONTROL_DRAIN_TIMEOUT_HOOK.get_or_init(|| Mutex::new(None));
        *lock_unpoison(hook) = None;
    }
}

#[cfg(test)]
fn force_next_control_group_drain_timeout(
    group_cancellation: Arc<AtomicBool>,
    worker: Arc<ControlWorkerState>,
    fired: std::sync::mpsc::SyncSender<()>,
) -> ControlDrainTimeoutHookGuard {
    let hook = CONTROL_DRAIN_TIMEOUT_HOOK.get_or_init(|| Mutex::new(None));
    let mut hook = lock_unpoison(hook);
    assert!(hook.is_none(), "only one control drain hook may be armed");
    *hook = Some(ControlDrainTimeoutHook {
        group_cancellation,
        worker,
        fired,
    });
    ControlDrainTimeoutHookGuard
}

#[cfg(test)]
fn force_control_group_drain_timeout(
    group_cancellation: &Arc<AtomicBool>,
) -> Option<Arc<ControlWorkerState>> {
    let hook = CONTROL_DRAIN_TIMEOUT_HOOK.get_or_init(|| Mutex::new(None));
    let mut hook = lock_unpoison(hook);
    if hook
        .as_ref()
        .is_some_and(|candidate| Arc::ptr_eq(&candidate.group_cancellation, group_cancellation))
    {
        let hook = hook.take().unwrap();
        hook.fired.send(()).unwrap();
        Some(hook.worker)
    } else {
        None
    }
}

const CONTROL_LEASE_PENDING: usize = 0;
const CONTROL_LEASE_COMPLETED: usize = 1;
const CONTROL_LEASE_QUARANTINE_REQUESTED: usize = 2;

#[cfg(feature = "test-hooks")]
#[derive(Clone, Copy)]
enum TestWorkerEvent {
    Enqueued,
    Popped,
    RunningEntered,
    RunningExited,
    ReplySent,
    Pruned,
    CloseDrained,
    Quarantined,
    CompletionDropped,
}

impl RpcCallGroup {
    #[cfg(feature = "test-hooks")]
    fn record_test_worker_event(&self, worker: usize, event: TestWorkerEvent) {
        for attachment in self.test_probes() {
            attachment
                .probe
                .record_worker_event(attachment.generation, worker, event);
        }
    }
    fn new() -> Self {
        Self {
            state: Arc::new(RpcCallGroupState {
                cancelled: Arc::new(AtomicBool::new(false)),
                outstanding: Mutex::new(Vec::new()),
                drained: Condvar::new(),
                #[cfg(feature = "test-hooks")]
                test_probes: Mutex::new(Vec::new()),
            }),
        }
    }

    #[cfg(feature = "test-hooks")]
    fn attach_test_record_probe(&self, consensus_instance_id: u64, slot: Slot) {
        let hooks = TEST_CONTROL_OPERATION_HOOKS.get_or_init(|| Mutex::new(Vec::new()));
        let probes = lock_unpoison(hooks)
            .iter()
            .filter(|hook| {
                hook.consensus_instance_id == Some(consensus_instance_id) && hook.slot == Some(slot)
            })
            .map(|hook| hook.probe.capture_attachment(hook.generation))
            .collect();
        *lock_unpoison(&self.state.test_probes) = probes;
    }

    #[cfg(feature = "test-hooks")]
    fn attach_test_root_probe(&self, context: &RecorderRpcContext) {
        let hooks = TEST_CONTROL_OPERATION_HOOKS.get_or_init(|| Mutex::new(Vec::new()));
        let probes = lock_unpoison(hooks)
            .iter()
            .filter(|hook| {
                hook.root_cancellation.as_ref().is_some_and(|root| {
                    context
                        .cancellations
                        .iter()
                        .any(|token| Arc::ptr_eq(token, root))
                })
            })
            .map(|hook| hook.probe.capture_attachment(hook.generation))
            .collect();
        *lock_unpoison(&self.state.test_probes) = probes;
    }

    #[cfg(feature = "test-hooks")]
    fn test_probes(&self) -> Vec<TestProbeAttachment> {
        lock_unpoison(&self.state.test_probes).clone()
    }

    #[cfg(feature = "test-hooks")]
    /// Mirrors one lease registration into every attached probe. The probe
    /// owns one synchronized lifecycle state rather than publishing a group
    /// vector snapshot, so a completion cannot be overwritten by stale
    /// admission data.
    fn record_test_lease_registered(&self) {
        for probe in self.test_probes() {
            probe.probe.record_lease_registered(probe.generation);
        }
    }

    #[cfg(feature = "test-hooks")]
    fn record_test_lease_completed(&self) {
        for probe in self.test_probes() {
            probe.probe.record_lease_completed(probe.generation);
        }
    }

    fn token(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.state.cancelled)
    }

    fn register<W>(&self, worker: &Arc<W>) -> Arc<RpcGroupLease>
    where
        W: RpcCallWorker + 'static,
    {
        #[cfg(feature = "test-hooks")]
        let worker_identity = worker.worker_identity();
        let worker: Arc<dyn RpcCallWorker> = Arc::clone(worker) as Arc<dyn RpcCallWorker>;
        let lease = Arc::new(RpcGroupLease {
            state: AtomicUsize::new(CONTROL_LEASE_PENDING),
            group: self.clone(),
            worker: Arc::downgrade(&worker),
            #[cfg(feature = "test-hooks")]
            worker_identity,
        });
        lock_unpoison(&self.state.outstanding).push(Arc::downgrade(&lease));
        #[cfg(feature = "test-hooks")]
        self.record_test_lease_registered();
        lease
    }

    fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    #[cfg(test)]
    fn outstanding_len(&self) -> usize {
        let mut outstanding = lock_unpoison(&self.state.outstanding);
        outstanding.retain(|lease| lease.strong_count() != 0);
        outstanding.len()
    }

    fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        #[cfg(feature = "test-hooks")]
        for probe in self.test_probes() {
            probe.probe.record_cancel(probe.generation);
        }
    }

    fn prune_pending(&self) {
        let workers = self.outstanding_workers(false);
        for worker in workers {
            worker.prune_pending(self);
        }
    }

    fn cancel_and_prune(&self) {
        self.cancel();
        self.prune_pending();
    }

    fn complete(&self, lease: &Arc<RpcGroupLease>) {
        lease
            .state
            .store(CONTROL_LEASE_COMPLETED, Ordering::Release);
        let mut outstanding = lock_unpoison(&self.state.outstanding);
        outstanding.retain(|candidate| {
            candidate
                .upgrade()
                .is_some_and(|candidate| !Arc::ptr_eq(&candidate, lease))
        });
        drop(outstanding);
        #[cfg(feature = "test-hooks")]
        self.record_test_lease_completed();
        self.state.drained.notify_all();
    }

    fn outstanding_workers(&self, quarantine_only: bool) -> Vec<Arc<dyn RpcCallWorker>> {
        let outstanding = lock_unpoison(&self.state.outstanding);
        let mut workers: Vec<Arc<dyn RpcCallWorker>> = Vec::new();
        for lease in outstanding.iter().filter_map(Weak::upgrade) {
            if quarantine_only
                && lease
                    .state
                    .compare_exchange(
                        CONTROL_LEASE_PENDING,
                        CONTROL_LEASE_QUARANTINE_REQUESTED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
            {
                continue;
            }
            #[cfg(feature = "test-hooks")]
            if quarantine_only {
                for probe in self.test_probes() {
                    probe.probe.record_quarantine(probe.generation);
                }
            }
            let Some(worker) = lease.worker.upgrade() else {
                continue;
            };
            if !workers
                .iter()
                .any(|candidate| candidate.worker_identity() == worker.worker_identity())
            {
                workers.push(worker);
            }
        }
        workers
    }

    /// Returns the workers which still owned a pending lease at D.
    fn drain_to_deadline(&self, deadline: Instant) -> Vec<Arc<dyn RpcCallWorker>> {
        #[cfg(test)]
        if let Some(worker) = force_control_group_drain_timeout(&self.state.cancelled) {
            return self.outstanding_worker_forced_timeout(&worker);
        }
        let mut outstanding = lock_unpoison(&self.state.outstanding);
        loop {
            outstanding.retain(|lease| lease.strong_count() != 0);
            if outstanding.is_empty() {
                return Vec::new();
            }
            let Some(wait) = deadline.checked_duration_since(Instant::now()) else {
                drop(outstanding);
                return self.outstanding_workers(true);
            };
            let (next, _) = self
                .state
                .drained
                .wait_timeout(outstanding, wait)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            outstanding = next;
        }
    }

    #[cfg(test)]
    fn outstanding_worker_forced_timeout(
        &self,
        target: &Arc<ControlWorkerState>,
    ) -> Vec<Arc<dyn RpcCallWorker>> {
        let target_identity = target.worker_identity();
        let outstanding = lock_unpoison(&self.state.outstanding);
        for lease in outstanding.iter().filter_map(Weak::upgrade) {
            if lease
                .worker
                .upgrade()
                .is_some_and(|worker| worker.worker_identity() == target_identity)
                && lease
                    .state
                    .compare_exchange(
                        CONTROL_LEASE_PENDING,
                        CONTROL_LEASE_QUARANTINE_REQUESTED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                let target: Arc<dyn RpcCallWorker> = Arc::clone(target) as Arc<dyn RpcCallWorker>;
                return vec![target];
            }
        }
        Vec::new()
    }
}

/// Owns every admission-side accounting resource.  It is deliberately not
/// cloneable: whichever path consumes a queued job also consumes its guard.
struct ControlCompletionGuard {
    pending: Arc<AtomicUsize>,
    lease: Option<Arc<RpcGroupLease>>,
    armed: bool,
    #[cfg(feature = "test-hooks")]
    record_worker: Option<Weak<RecordWorkerState>>,
}

impl ControlCompletionGuard {
    fn new(pending: Arc<AtomicUsize>) -> Self {
        Self {
            pending,
            lease: None,
            armed: false,
            #[cfg(feature = "test-hooks")]
            record_worker: None,
        }
    }

    fn arm<W>(&mut self, group: Option<&RpcCallGroup>, worker: &Arc<W>)
    where
        W: RpcCallWorker + 'static,
    {
        debug_assert!(!self.armed);
        self.lease = group.map(|group| group.register(worker));
        #[cfg(feature = "test-hooks")]
        if let Some(group) = group {
            group.record_test_worker_event(worker.worker_identity(), TestWorkerEvent::Enqueued);
        }
        self.pending.fetch_add(1, Ordering::Release);
        self.armed = true;
    }

    fn group(&self) -> Option<&RpcCallGroup> {
        self.lease.as_ref().map(|lease| &lease.group)
    }

    #[cfg(feature = "test-hooks")]
    fn attach_record_worker(
        &mut self,
        worker: &Arc<RecordWorkerState>,
        group: Option<&RpcCallGroup>,
    ) {
        self.record_worker = Some(Arc::downgrade(worker));
        if let Some(group) = group {
            worker.test_register_group(group);
        }
    }
}

impl Drop for ControlCompletionGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let previous = self.pending.fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0);
        if let Some(lease) = self.lease.take() {
            #[cfg(feature = "test-hooks")]
            if let Some(worker) = self.record_worker.as_ref().and_then(Weak::upgrade) {
                worker.test_complete_group(&lease.group);
            }
            #[cfg(feature = "test-hooks")]
            lease.group.record_test_worker_event(
                lease.worker_identity,
                TestWorkerEvent::CompletionDropped,
            );
            lease.group.complete(&lease);
        }
    }
}

struct QueuedControlJob {
    job: ControlJob,
    cancelled: Option<Arc<AtomicBool>>,
    completion: ControlCompletionGuard,
}

impl QueuedControlJob {
    fn run(mut self, recorder: &dyn RecorderRpc, worker_cancellation: &Arc<AtomicBool>) {
        let cancelled = self
            .cancelled
            .as_ref()
            .is_some_and(|cancelled| cancelled.load(Ordering::Acquire))
            || self
                .completion
                .group()
                .is_some_and(ControlCallGroup::is_cancelled);
        if cancelled {
            if self.completion.group().is_some() {
                self.job.fail(Error::RpcCancelled);
            }
            return;
        }
        self.job.with_cancellation(Arc::clone(worker_cancellation));
        self.job.run(recorder);
    }

    fn fail(self, error: Error) {
        self.job.fail(error);
    }

    fn fail_for_worker(self) {
        let error = match self.job.operation() {
            RecorderRpcOperation::Mutating => Error::UnknownOutcome,
            RecorderRpcOperation::ReadOnly => Error::ProposeFailed,
        };
        self.fail(error);
    }

    fn belongs_to(&self, group: &ControlCallGroup) -> bool {
        self.completion
            .group()
            .is_some_and(|candidate| candidate.is_same(group))
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled
            .as_ref()
            .is_some_and(|cancelled| cancelled.load(Ordering::Acquire))
            || self
                .completion
                .group()
                .is_some_and(ControlCallGroup::is_cancelled)
    }
}

#[cfg(test)]
struct ControlJobCancellation {
    cancelled: Arc<AtomicBool>,
}

#[cfg(test)]
impl ControlJobCancellation {
    fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn token(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }
}

#[cfg(test)]
impl Drop for ControlJobCancellation {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

struct ControlWorker {
    state: Arc<ControlWorkerState>,
    handle: Option<thread::JoinHandle<()>>,
}

struct ControlWorkerState {
    queue: Arc<ControlQueue>,
    pending: Arc<AtomicUsize>,
    cancellation: Arc<AtomicBool>,
    quarantined: AtomicBool,
    #[cfg(test)]
    panic_after_pop: AtomicBool,
    #[cfg(test)]
    post_pop_hook: Mutex<Option<ControlWorkerPostPopHook>>,
}

#[cfg(test)]
struct ControlWorkerPostPopHook {
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

struct ControlQueue {
    state: Mutex<ControlQueueState>,
    available: Condvar,
}

struct ControlQueueState {
    jobs: VecDeque<QueuedControlJob>,
    closed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlDispatch {
    Accepted,
    Saturated,
    Failed,
}

impl ControlWorker {
    fn spawn(recorder: Arc<dyn RecorderRpc>) -> Result<Self> {
        let queue = Arc::new(ControlQueue {
            state: Mutex::new(ControlQueueState {
                jobs: VecDeque::with_capacity(CONTROL_WORKER_QUEUE_CAPACITY),
                closed: false,
            }),
            available: Condvar::new(),
        });
        let state = Arc::new(ControlWorkerState {
            queue,
            pending: Arc::new(AtomicUsize::new(0)),
            cancellation: Arc::new(AtomicBool::new(false)),
            quarantined: AtomicBool::new(false),
            #[cfg(test)]
            panic_after_pop: AtomicBool::new(false),
            #[cfg(test)]
            post_pop_hook: Mutex::new(None),
        });
        let worker_state = Arc::clone(&state);
        let handle = thread::Builder::new()
            .spawn(move || {
                let abnormal = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    control_worker_loop(&worker_state, recorder.as_ref());
                }))
                .is_err();
                if abnormal {
                    worker_state.close_and_drain();
                }
            })
            .map_err(|error| Error::Io(error.to_string()))?;
        Ok(Self {
            state,
            handle: Some(handle),
        })
    }

    #[cfg(test)]
    fn dispatch(&self, job: ControlJob) -> ControlDispatch {
        Self::dispatch_inner(&self.state, job, None, None, None, true)
    }

    #[cfg(test)]
    fn dispatch_cancellable(
        &self,
        job: ControlJob,
        cancellation: Arc<AtomicBool>,
    ) -> ControlDispatch {
        Self::dispatch_inner(&self.state, job, Some(cancellation), None, None, true)
    }

    fn dispatch_group(&self, job: ControlJob, group: &ControlCallGroup) -> ControlDispatch {
        Self::dispatch_inner(&self.state, job, None, Some(group), None, true)
    }

    fn dispatch_read_group_retryable(
        &self,
        job: ControlJob,
        group: &ControlCallGroup,
    ) -> ControlDispatch {
        // The caller retains saturated workers as retry candidates. Do not
        // inject a synthetic rejection into the shared response stream for a
        // job that was never admitted; repeated phase retries could otherwise
        // fill the bounded channel before the caller can drain it.
        Self::dispatch_inner(&self.state, job, None, Some(group), None, false)
    }

    /// Marks the caller's mutation certainty while the queue is still locked,
    /// before the worker can observe the admitted job.
    fn dispatch_mutating_group(
        &self,
        job: ControlJob,
        group: &ControlCallGroup,
        mutation_started: &AtomicBool,
    ) -> ControlDispatch {
        Self::dispatch_inner(
            &self.state,
            job,
            None,
            Some(group),
            Some(mutation_started),
            true,
        )
    }

    fn dispatch_inner(
        state: &Arc<ControlWorkerState>,
        job: ControlJob,
        cancelled: Option<Arc<AtomicBool>>,
        group: Option<&ControlCallGroup>,
        mutation_started: Option<&AtomicBool>,
        reply_on_rejection: bool,
    ) -> ControlDispatch {
        let mut queued_job = Some(QueuedControlJob {
            job,
            cancelled,
            completion: ControlCompletionGuard::new(Arc::clone(&state.pending)),
        });
        let (pruned, error, outcome) = {
            let mut queue = lock_unpoison(&state.queue.state);
            let mut pruned = Vec::new();
            let mut retained = VecDeque::with_capacity(queue.jobs.len());
            while let Some(job) = queue.jobs.pop_front() {
                if job.is_cancelled() {
                    pruned.push(job);
                } else {
                    retained.push_back(job);
                }
            }
            queue.jobs = retained;
            if queue.closed || state.quarantined.load(Ordering::Acquire) {
                (pruned, Some(Error::ProposeFailed), ControlDispatch::Failed)
            } else if queue.jobs.len() >= CONTROL_WORKER_QUEUE_CAPACITY {
                (
                    pruned,
                    Some(Error::Io(
                        "recorder control worker queue is temporarily full".into(),
                    )),
                    ControlDispatch::Saturated,
                )
            } else {
                let queued = queued_job.as_mut().expect("control job must be present");
                // The lease and pending increment happen while the queue is
                // still locked, before a worker can pop the job.
                queued.completion.arm(group, state);
                if let Some(mutation_started) = mutation_started {
                    mutation_started.store(true, Ordering::Release);
                }
                queue
                    .jobs
                    .push_back(queued_job.take().expect("control job must be present"));
                state.queue.available.notify_one();
                (pruned, None, ControlDispatch::Accepted)
            }
        };
        for job in pruned {
            // Legacy cancellable dispatch deliberately had no reply for a
            // skipped hedge.  Its guard still completes; group-owned jobs
            // receive the explicit cancellation reply required for draining.
            if job.completion.group().is_some() {
                job.fail(Error::RpcCancelled);
            }
        }
        if let Some(error) = error {
            let queued_job = queued_job.unwrap();
            if reply_on_rejection {
                queued_job.fail(error);
            }
        }
        outcome
    }

    fn is_idle(&self) -> bool {
        self.state.pending.load(Ordering::Acquire) == 0
    }

    fn shutdown(&mut self) {
        self.shutdown_after_before_close();
    }

    #[cfg(test)]
    fn shutdown_after_stale_idle_observation(
        &mut self,
        before_close: impl FnOnce(),
    ) -> (bool, bool) {
        // The seam is immediately before the admission fence. If an idle
        // snapshot were taken before this callback, a real dispatch below
        // could make that snapshot stale before close runs.
        let stale_idle = self.is_idle();
        before_close();
        let current_idle = self.is_idle();
        self.shutdown_after_before_close();
        (stale_idle, current_idle)
    }

    fn shutdown_after_before_close(&mut self) {
        // See RecordWorker::shutdown: closing first serializes the running
        // snapshot with admission, so a successful join never waits on an
        // RPC that raced a stale pre-close idle observation.
        let join_idle_worker = self.state.close_and_drain();
        let Some(handle) = self.handle.take() else {
            return;
        };
        if join_idle_worker || handle.is_finished() {
            let _ = handle.join();
            return;
        }
        // Preserve bounded shutdown for a noncooperative RecorderRpc. The
        // detached closure owns only the explicit recorder/state Arcs.
        drop(handle);
    }

    #[cfg(test)]
    fn panic_after_next_pop(&self) {
        self.state.panic_after_pop.store(true, Ordering::Release);
    }

    #[cfg(test)]
    fn pause_after_next_pop(
        &self,
        entered: std::sync::mpsc::SyncSender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
    ) {
        *lock_unpoison(&self.state.post_pop_hook) =
            Some(ControlWorkerPostPopHook { entered, release });
    }
}

fn lock_unpoison<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn control_worker_loop(state: &ControlWorkerState, recorder: &dyn RecorderRpc) {
    loop {
        let job = {
            let mut queue = lock_unpoison(&state.queue.state);
            while queue.jobs.is_empty() && !queue.closed {
                queue = state
                    .queue
                    .available
                    .wait(queue)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            queue.jobs.pop_front()
        };
        let Some(job) = job else {
            break;
        };
        #[cfg(test)]
        if let Some(hook) = lock_unpoison(&state.post_pop_hook).take() {
            hook.entered.send(()).unwrap();
            let (released, condition) = &*hook.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
        }
        #[cfg(test)]
        if state.panic_after_pop.swap(false, Ordering::AcqRel) {
            panic!("control worker test hook");
        }
        job.run(recorder, &state.cancellation);
    }
}

impl ControlWorkerState {
    fn prune_pending(&self, group: &ControlCallGroup) {
        let pruned = {
            let mut queue = lock_unpoison(&self.queue.state);
            let mut pruned = Vec::new();
            let mut retained = VecDeque::with_capacity(queue.jobs.len());
            while let Some(job) = queue.jobs.pop_front() {
                if job.belongs_to(group) {
                    pruned.push(job);
                } else {
                    retained.push_back(job);
                }
            }
            queue.jobs = retained;
            pruned
        };
        for job in pruned {
            job.fail(Error::RpcCancelled);
        }
    }

    fn quarantine(&self) {
        if self.quarantined.swap(true, Ordering::AcqRel) {
            return;
        }
        self.close_and_drain();
    }

    /// Fences admission and returns whether no job remained running after
    /// queued jobs were drained. A true result permits a bounded worker join.
    fn close_and_drain(&self) -> bool {
        self.cancellation.store(true, Ordering::Release);
        let drained = {
            let mut queue = lock_unpoison(&self.queue.state);
            queue.closed = true;
            queue.jobs.drain(..).collect::<Vec<_>>()
        };
        self.queue.available.notify_all();
        for job in drained {
            job.fail_for_worker();
        }
        self.pending.load(Ordering::Acquire) == 0
    }
}

impl RpcCallWorker for ControlWorkerState {
    fn prune_pending(&self, group: &RpcCallGroup) {
        ControlWorkerState::prune_pending(self, group);
    }

    fn quarantine(&self) {
        ControlWorkerState::quarantine(self);
    }

    fn worker_identity(&self) -> usize {
        self as *const Self as usize
    }
}

fn control_quorum_reachable(successful: usize, saturated: usize, quorum: usize) -> bool {
    successful.saturating_add(saturated) >= quorum
}

impl Drop for ControlWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub trait PrioritySource: Send + Sync {
    fn sample(
        &self,
        slot: Slot,
        round: Round,
        proposer_id: &str,
        recorder_id: &str,
    ) -> Result<ProposalPriority>;
}

#[derive(Debug, Default)]
pub struct OsPrioritySource;

impl PrioritySource for OsPrioritySource {
    fn sample(
        &self,
        slot: Slot,
        round: Round,
        proposer_id: &str,
        recorder_id: &str,
    ) -> Result<ProposalPriority> {
        let mut bytes = [0; 32];
        let _ = (slot, round, proposer_id, recorder_id);
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::RandomnessUnavailable(error.to_string()))?;
        if bytes == ProposalPriority::ZERO.0 || bytes == ProposalPriority::MAX.0 {
            bytes[31] = 1;
        }
        Ok(ProposalPriority(bytes))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposerProgress {
    pub slot: Slot,
    pub step: Step,
    pub proposal: Proposal,
    phase_zero_priorities: BTreeMap<(Round, NodeId), ProposalPriority>,
    command: Option<StoredCommand>,
    command_holders: BTreeSet<NodeId>,
    transition_involved: bool,
}

impl ProposerProgress {
    pub fn new(slot: Slot, proposal: Proposal) -> Self {
        Self {
            slot,
            step: 4,
            proposal,
            phase_zero_priorities: BTreeMap::new(),
            command: None,
            command_holders: BTreeSet::new(),
            transition_involved: false,
        }
    }

    fn with_command(mut self, command: StoredCommand) -> Self {
        self.transition_involved = command.entry_type == EntryType::ConfigChange;
        self.command = Some(command);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DriveOutcome {
    Progress(ProposerProgress),
    Pending(ProposerProgress),
    Decision(DecisionProof),
}

impl fmt::Debug for ThreeNodeConsensus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThreeNodeConsensus")
            .field("cluster_id", &self.cluster_id)
            .field("proposer_id", &self.proposer_id)
            .field("epoch", &self.epoch)
            .field("config_id", &self.config_id)
            .field("recorders", &self.membership.members())
            .finish_non_exhaustive()
    }
}

impl Drop for ThreeNodeConsensus {
    fn drop(&mut self) {
        for worker in &mut self.record_workers {
            worker.shutdown();
        }
        for worker in &mut self.control_workers {
            worker.shutdown();
        }
        for worker in &mut self.read_fence_workers {
            worker.shutdown();
        }
    }
}

impl ThreeNodeConsensus {
    /// Returns this consensus instance's live record-probe registrations.
    #[cfg(feature = "test-hooks")]
    pub fn test_record_operation_probe_registration_count(&self) -> usize {
        TEST_CONTROL_OPERATION_HOOKS
            .get()
            .map(|hooks| {
                lock_unpoison(hooks)
                    .iter()
                    .filter(|hook| hook.consensus_instance_id == Some(self.test_instance_id))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Registers an exact, instance-scoped record RPC-group probe for tests.
    ///
    /// The registration key is this consensus object's monotonically assigned
    /// identity plus `slot`; equal slots in separate consensus instances never
    /// share accounting.
    #[cfg(feature = "test-hooks")]
    pub fn install_test_record_operation_probe(
        &self,
        slot: Slot,
        probe: Arc<TestControlOperationProbe>,
    ) -> std::result::Result<TestControlOperationProbeGuard, TestProbeRegistrationError> {
        let id = NEXT_TEST_CONTROL_OPERATION_HOOK_ID.fetch_add(1, Ordering::Relaxed);
        register_test_operation_probe(TestControlOperationHook {
            id,
            generation: 0,
            root_cancellation: None,
            consensus_instance_id: Some(self.test_instance_id),
            slot: Some(slot),
            probe,
        })?;
        Ok(TestControlOperationProbeGuard { id })
    }

    /// Diagnostic wait for currently accepted recorder, control, and read RPC jobs.
    ///
    /// This is not shutdown or quiescence evidence: it observes every group
    /// sharing this consensus instance, including work owned by other callers.
    /// Shutdown owners must instead drain their own admitted operation/task
    /// scopes before reporting completion.
    pub fn finish_pending_rpcs(&self, timeout: Duration) -> bool {
        let started = Instant::now();
        loop {
            if self.record_workers.iter().all(RecordWorker::is_idle)
                && self.control_workers.iter().all(ControlWorker::is_idle)
                && self.read_fence_workers.iter().all(ControlWorker::is_idle)
            {
                return true;
            }
            if started.elapsed() >= timeout {
                return false;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    pub const fn config_id(&self) -> ConfigId {
        self.config_id
    }

    pub const fn membership(&self) -> &FixedMembership {
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

    pub fn from_recovered_tip(
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
            .map(|(root, recorder_id)| -> Result<Box<dyn RecorderRpc>> {
                Ok(Box::new(RecorderFileStore::new_with_membership(
                    root,
                    recorder_id,
                    cluster_id.clone(),
                    epoch,
                    config_id,
                    membership.clone(),
                )?) as Box<dyn RecorderRpc>)
            })
            .collect::<Result<Vec<_>>>()?;
        Self::from_recorders_with_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorders,
            next_index,
            last_hash,
        )
    }

    pub fn from_recorders(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorders: Vec<Box<dyn RecorderRpc>>,
    ) -> Result<Self> {
        Self::from_recorders_with_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorders,
            1,
            LogHash::ZERO,
        )
    }

    /// Constructs a proposer from expected recorder identities paired with RPC clients.
    ///
    /// This path does not issue `Identity` RPCs. Reply identities are still
    /// checked against the corresponding expected identity on every call.
    pub fn from_recorders_with_ids(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorders: Vec<(NodeId, Box<dyn RecorderRpc>)>,
    ) -> Result<Self> {
        Self::from_recorders_with_ids_and_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorders,
            1,
            LogHash::ZERO,
        )
    }

    pub fn from_recorders_with_recovered_tip(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorders: Vec<Box<dyn RecorderRpc>>,
        next_index: LogIndex,
        last_hash: LogHash,
    ) -> Result<Self> {
        let context = RecorderRpcContext::default_timeout();
        let recorder_ids = recorders
            .iter()
            .map(|recorder| recorder.recorder_id(&context))
            .collect::<Result<Vec<_>>>()?;
        Self::from_recorders_with_ids_and_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorder_ids.into_iter().zip(recorders).collect(),
            next_index,
            last_hash,
        )
    }

    /// Recovered-tip variant of [`Self::from_recorders_with_ids`].
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
        let membership = FixedMembership::from_members(recorder_ids)?;
        let config_digest = membership.digest();
        let record_workers = membership
            .members()
            .iter()
            .cloned()
            .zip(&recorders)
            .map(|(recorder_id, recorder)| {
                RecordWorker::spawn(recorder_id, Arc::clone(recorder), config_id, config_digest)
            })
            .collect::<Result<Vec<_>>>()?;
        let control_workers = recorders
            .iter()
            .cloned()
            .map(ControlWorker::spawn)
            .collect::<Result<Vec<_>>>()?;
        let read_fence_workers = recorders
            .iter()
            .cloned()
            .map(ControlWorker::spawn)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            cluster_id: cluster_id.into(),
            proposer_id: proposer_id.into(),
            epoch,
            config_id,
            config_digest,
            membership,
            recorders,
            record_workers,
            control_workers,
            read_fence_workers,
            priority_source: Arc::new(OsPrioritySource),
            #[cfg(feature = "test-hooks")]
            test_instance_id: NEXT_TEST_CONSENSUS_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
            proposal_sequence: AtomicU64::new(1),
            sequential_tip: Mutex::new(SingleNodeState {
                next_index,
                last_hash,
            }),
        })
    }

    pub fn with_priority_source(mut self, source: Arc<dyn PrioritySource>) -> Self {
        self.priority_source = source;
        self
    }

    /// Stores command bytes on a recorder quorum after verifying their hash.
    ///
    /// [`Error::NoQuorum`] is retryable, including when bounded control-worker
    /// queues are temporarily saturated.
    pub fn register_command(
        &self,
        context: &RecorderRpcContext,
        command_hash: LogHash,
        command_bytes: Vec<u8>,
    ) -> Result<()> {
        let command = StoredCommand::new(EntryType::Command, command_bytes);
        if command.hash() != command_hash {
            return Err(Error::CommandHashMismatch);
        }
        let mutation_started = AtomicBool::new(false);
        let budget = ControlCallBudget::new(context)
            .map_err(|error| Self::store_context_error(error, &mutation_started))?;
        self.store_command_on_quorum_with_budget(&budget, &mutation_started, command_hash, &command)
    }

    /// Stages every bounded chunk and then installs the exact QEFX manifest on
    /// a recorder quorum. A successful return is therefore a durable quorum
    /// acknowledgement, suitable as the precondition to proposing the small
    /// QEFX command. This deliberately does not invoke any SQL producer.
    pub fn finalize_effect_bundle_on_quorum(
        &self,
        context: &RecorderRpcContext,
        request: &EffectBundleFinalizeRequest,
    ) -> Result<()> {
        EffectBundleFinalizeRequest::new(request.bundle.clone(), request.manifest_command.clone())?;
        let mutation_started = AtomicBool::new(false);
        let budget = ControlCallBudget::new(context)
            .map_err(|error| Self::store_context_error(error, &mutation_started))?;
        for (ordinal, chunk) in request.bundle.chunks().iter().enumerate() {
            let ordinal = u16::try_from(ordinal).map_err(|_| {
                Error::EffectBundleInvalid("effect chunk ordinal exceeds wire limit".into())
            })?;
            self.mutation_on_quorum_with_budget(
                &budget,
                &mutation_started,
                |index, context, result| ControlJob::StageEffectBundleChunk {
                    index,
                    context,
                    binding: request.bundle.binding.clone(),
                    manifest_command: request.manifest_command.clone(),
                    ordinal,
                    chunk: chunk.clone(),
                    result,
                },
            )?;
        }
        self.mutation_on_quorum_with_budget(&budget, &mutation_started, |index, context, result| {
            ControlJob::FinalizeStagedEffectBundle {
                index,
                context,
                binding: request.bundle.binding.clone(),
                manifest_command: request.manifest_command.clone(),
                result,
            }
        })
    }

    /// Resolves a previously quorum-finalized QEFX bundle from any available
    /// recorder, verifying the exact command hash, binding, chunk order and
    /// digest before returning bytes to an executor.
    pub fn resolve_effect_bundle_from_quorum(
        &self,
        context: &RecorderRpcContext,
        binding: &EffectBundleBinding,
        manifest_command: &StoredCommand,
    ) -> Result<Option<RecorderEffectBundle>> {
        let qefx = verified_effect_bundle_command(binding, manifest_command)?;
        let budget = ControlCallBudget::new(context)?;
        let group = ControlCallGroup::new();
        #[cfg(feature = "test-hooks")]
        group.attach_test_root_probe(&budget.caller);
        let capacity = self
            .read_fence_workers
            .len()
            .saturating_mul(qefx.chunks().len().saturating_add(1))
            .max(1);
        let (sender, receiver) = std::sync::mpsc::sync_channel(capacity);
        let fetch = EffectFetchContext {
            budget: &budget,
            group: &group,
            sender: &sender,
            receiver: &receiver,
            binding,
        };
        let outcome = (|| {
            self.fetch_effect_manifest_in_group(&fetch, manifest_command)?;
            let mut chunks = Vec::with_capacity(qefx.chunks().len());
            for (ordinal, expected) in qefx.chunks().iter().enumerate() {
                let ordinal = u16::try_from(ordinal).map_err(|_| {
                    Error::EffectBundleInvalid("effect chunk ordinal exceeds wire limit".into())
                })?;
                chunks.push(self.fetch_effect_chunk_in_group(&fetch, ordinal, expected)?);
            }
            RecorderEffectBundle::new(binding.clone(), chunks).map(Some)
        })();
        drop(sender);
        // This is deliberately the only cancellation/drain point for the
        // entire bundle.  In particular, never drain after the manifest: a
        // non-cooperative recorder must not spend the caller's reserve before
        // healthy peers can supply the chunks.  A fully verified read has no
        // mutation uncertainty, so it may detach a stuck backend immediately
        // rather than making the healthy result wait for D. Quarantine closes
        // that worker's generation and fences all future reuse; its owned
        // thread releases the final attachment when it eventually returns.
        if outcome.is_ok() {
            group.cancel_and_prune();
            for worker in group.drain_to_deadline(Instant::now() + CONTEXT_POLL_INTERVAL) {
                worker.quarantine();
            }
        } else {
            group.cancel_and_prune();
            for worker in group.drain_to_deadline(budget.deadline) {
                worker.quarantine();
            }
        }
        outcome
    }

    fn dispatch_effect_fetch(
        &self,
        fetch: &EffectFetchContext<'_>,
        kind: EffectFetchKind,
        dispatched: &mut [bool],
    ) -> Result<usize> {
        if dispatched.len() != self.read_fence_workers.len() {
            return Err(Error::EffectBundleInvalid(
                "effect fetch dispatch state does not match Recorder membership".into(),
            ));
        }
        let mut admitted = 0;
        for (index, worker) in self.read_fence_workers.iter().enumerate() {
            if dispatched[index] {
                continue;
            }
            fetch.budget.check_admission()?;
            match worker.dispatch_read_group_retryable(
                ControlJob::FetchEffectBundle {
                    index,
                    context: fetch.budget.child_context(fetch.group),
                    binding: fetch.binding.clone(),
                    kind,
                    result: fetch.sender.clone(),
                },
                fetch.group,
            ) {
                ControlDispatch::Accepted => {
                    dispatched[index] = true;
                    admitted += 1;
                }
                ControlDispatch::Saturated => {
                    // A reply from the preceding manifest/chunk phase can be
                    // visible before that worker has dropped its queue lease.
                    // Keep this source eligible and retry it within the same
                    // caller budget instead of falsely concluding that only
                    // a missing hedge was available.
                }
                ControlDispatch::Failed => dispatched[index] = true,
            }
        }
        Ok(admitted)
    }

    fn fetch_effect_manifest_in_group(
        &self,
        fetch: &EffectFetchContext<'_>,
        expected: &StoredCommand,
    ) -> Result<()> {
        let mut dispatched = vec![false; self.read_fence_workers.len()];
        let mut outstanding = 0usize;
        let mut malformed = None;
        loop {
            outstanding = outstanding
                .checked_add(self.dispatch_effect_fetch(
                    fetch,
                    EffectFetchKind::Manifest,
                    &mut dispatched,
                )?)
                .ok_or_else(|| {
                    Error::EffectBundleInvalid("effect manifest dispatch count overflow".into())
                })?;
            if outstanding == 0 && dispatched.iter().all(|complete| *complete) {
                break;
            }
            fetch.budget.check_admission()?;
            let remaining = fetch
                .budget
                .work_deadline
                .saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(malformed.unwrap_or(Error::EffectBundleUnavailable));
            }
            match fetch
                .receiver
                .recv_timeout(remaining.min(CONTEXT_POLL_INTERVAL))
            {
                Ok((_, EffectFetchReply::Manifest(Ok(Some(command))))) if command == *expected => {
                    return Ok(());
                }
                Ok((index, EffectFetchReply::Manifest(Ok(Some(_))))) => {
                    outstanding -= 1;
                    self.read_fence_workers[index].state.quarantine();
                    malformed.get_or_insert_with(|| {
                        Error::EffectBundleInvalid(
                            "recorder returned a different QEFX manifest".into(),
                        )
                    });
                }
                Ok((_, EffectFetchReply::Manifest(Ok(None) | Err(_)))) => outstanding -= 1,
                // Late responses from an earlier phase cannot exist yet, but
                // retaining this arm makes the shared stream robust if a
                // recorder replies after its work was superseded.
                Ok((_, EffectFetchReply::Chunk { .. })) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        Err(malformed.unwrap_or(Error::EffectBundleUnavailable))
    }

    fn fetch_effect_chunk_in_group(
        &self,
        fetch: &EffectFetchContext<'_>,
        ordinal: u16,
        expected: &ExternalEffectChunk,
    ) -> Result<Vec<u8>> {
        let mut dispatched = vec![false; self.read_fence_workers.len()];
        let mut outstanding = 0usize;
        let mut malformed = None;
        loop {
            outstanding = outstanding
                .checked_add(self.dispatch_effect_fetch(
                    fetch,
                    EffectFetchKind::Chunk { ordinal },
                    &mut dispatched,
                )?)
                .ok_or_else(|| {
                    Error::EffectBundleInvalid("effect chunk dispatch count overflow".into())
                })?;
            if outstanding == 0 && dispatched.iter().all(|complete| *complete) {
                break;
            }
            fetch.budget.check_admission()?;
            let remaining = fetch
                .budget
                .work_deadline
                .saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(malformed.unwrap_or(Error::EffectBundleUnavailable));
            }
            match fetch
                .receiver
                .recv_timeout(remaining.min(CONTEXT_POLL_INTERVAL))
            {
                Ok((
                    _,
                    EffectFetchReply::Chunk {
                        ordinal: reply_ordinal,
                        result: Ok(Some(chunk)),
                    },
                )) if reply_ordinal == ordinal
                    && chunk.len() == expected.encoded_len() as usize
                    && effect_chunk_digest(&chunk) == expected.digest() =>
                {
                    return Ok(chunk);
                }
                Ok((
                    index,
                    EffectFetchReply::Chunk {
                        ordinal: reply_ordinal,
                        result: Ok(Some(_)),
                    },
                )) if reply_ordinal == ordinal => {
                    outstanding -= 1;
                    self.read_fence_workers[index].state.quarantine();
                    malformed.get_or_insert_with(|| {
                        Error::EffectBundleInvalid(
                            "recorder returned a corrupt effect chunk".into(),
                        )
                    });
                }
                Ok((
                    _,
                    EffectFetchReply::Chunk {
                        ordinal: reply_ordinal,
                        result: Ok(None) | Err(_),
                    },
                )) if reply_ordinal == ordinal => outstanding -= 1,
                // A manifest reply left in the shared stream, or an old
                // chunk reply, belongs to an earlier stage and must never be
                // counted as this ordinal's response.
                Ok((_, EffectFetchReply::Manifest(_) | EffectFetchReply::Chunk { .. })) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        Err(malformed.unwrap_or(Error::EffectBundleUnavailable))
    }

    fn propose_next(&self, context: RecorderRpcContext, command: Command) -> Result<LogEntry> {
        let mut tip = self
            .sequential_tip
            .lock()
            .map_err(|_| Error::ProposeFailed)?;
        let entry = self.propose_at(context, tip.next_index, tip.last_hash, command)?;
        tip.next_index = entry.index + 1;
        tip.last_hash = entry.hash;
        Ok(entry)
    }

    /// Proposes with a caller-owned RPC deadline and cancellation signal.
    /// A deadline after any recorder may have accepted the value returns
    /// [`Error::UnknownOutcome`]; recover the slot before issuing new work.
    pub fn propose(&self, context: RecorderRpcContext, command: Command) -> Result<LogEntry> {
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

    pub fn propose_at(
        &self,
        context: RecorderRpcContext,
        slot: Slot,
        prev_hash: LogHash,
        command: Command,
    ) -> Result<LogEntry> {
        self.propose_stored_at_until(slot, prev_hash, stored_command(command)?, &context, || {
            context.check()
        })
    }

    pub fn propose_stop_at(
        &self,
        context: RecorderRpcContext,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<LogEntry> {
        self.propose_stored_at(
            context,
            slot,
            prev_hash,
            ConfigChange::stop(self.config_id, self.config_digest).to_stored_command(),
        )
    }

    pub fn propose_stop_for_successor_at(
        &self,
        context: RecorderRpcContext,
        slot: Slot,
        prev_hash: LogHash,
        successor: &Membership,
    ) -> Result<LogEntry> {
        let next_config_id = self
            .config_id
            .checked_add(1)
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?;
        let stop = ConfigChange::bound_stop(
            self.cluster_id.clone(),
            self.config_id,
            self.config_digest,
            next_config_id,
            successor.members().to_vec(),
        )
        .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?;
        self.propose_stored_at(context, slot, prev_hash, stop.to_stored_command())
    }

    pub fn propose_activation_barrier_at(
        &self,
        context: RecorderRpcContext,
        stop_slot: Slot,
        prefix_hash: LogHash,
    ) -> Result<LogEntry> {
        self.propose_stored_at(
            context,
            stop_slot.checked_add(1).ok_or(Error::InvalidRecoveredTip)?,
            prefix_hash,
            ConfigChange::activation_barrier(
                self.config_id,
                self.config_digest,
                stop_slot,
                prefix_hash,
            )
            .to_stored_command(),
        )
    }

    pub fn propose_activation_for_stop_entry(
        &self,
        context: RecorderRpcContext,
        stop: &LogEntry,
    ) -> Result<LogEntry> {
        let command = StoredCommand::new(stop.entry_type, stop.payload.clone());
        let change = ConfigChange::recognize(&command)
            .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?;
        let successor = change
            .successor()
            .filter(|successor| {
                successor.cluster_id() == self.cluster_id
                    && successor.config_id() == self.config_id
                    && successor.digest() == self.config_digest
                    && successor.members() == self.membership.members()
            })
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?
            .clone();
        self.propose_stored_at(
            context,
            stop.index
                .checked_add(1)
                .ok_or(Error::InvalidRecoveredTip)?,
            stop.hash,
            ConfigChange::bound_activation_barrier(
                successor,
                stop.index,
                stop.hash,
                command.hash(),
            )
            .to_stored_command(),
        )
    }

    pub fn propose_activation_for_stop_at(
        &self,
        context: RecorderRpcContext,
        stop_proof: &DecisionProof,
    ) -> Result<LogEntry> {
        if proof_cluster_id(stop_proof) != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        let (stop_slot, epoch, predecessor_config_id, _) = proof_context(stop_proof);
        if epoch != self.epoch || predecessor_config_id.checked_add(1) != Some(self.config_id) {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        let value = stop_proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        let bound_stop = ConfigChange::bound_stop(
            self.cluster_id.clone(),
            predecessor_config_id,
            proof_context(stop_proof).3,
            self.config_id,
            self.membership.members().to_vec(),
        )
        .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?;
        let stop_command = bound_stop.to_stored_command();
        let expected = AcceptedValue::from_command(
            &self.cluster_id,
            stop_slot,
            self.epoch,
            predecessor_config_id,
            value.prev_hash,
            &stop_command,
        );
        if &expected != value {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        let successor = bound_stop
            .successor()
            .expect("bound stop has successor")
            .clone();
        self.propose_stored_at(
            context,
            stop_slot.checked_add(1).ok_or(Error::InvalidRecoveredTip)?,
            value.entry_hash,
            ConfigChange::bound_activation_barrier(
                successor,
                stop_slot,
                value.entry_hash,
                value.command_hash,
            )
            .to_stored_command(),
        )
    }

    pub fn propose_stored_at(
        &self,
        context: RecorderRpcContext,
        slot: Slot,
        prev_hash: LogHash,
        command: StoredCommand,
    ) -> Result<LogEntry> {
        self.propose_stored_at_until(slot, prev_hash, command, &context, || Ok(()))
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

    pub fn drive(
        &self,
        context: &RecorderRpcContext,
        progress: ProposerProgress,
    ) -> Result<DriveOutcome> {
        let mutation_started = AtomicBool::new(false);
        self.drive_inner(progress, context, &mutation_started)
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
                    progress.transition_involved,
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
                        progress.transition_involved,
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
                        progress.transition_involved,
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

    /// Completes Rhiza's public acknowledgement contract after QuePaxa has
    /// formed a decision proof. A FastPath proof is formed after one phase-0
    /// recorder round, but the normal public success path still requires the
    /// same durable proof-quorum installation used by Phase2. An ambiguous
    /// installer result is acknowledged only after exact committed-value
    /// reconciliation below.
    fn finish_decision_with_context(
        &self,
        proof: DecisionProof,
        known_command: Option<&StoredCommand>,
        _transition_involved: bool,
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
        let mut fetched_command = false;
        let _command = match known_command {
            Some(command)
                if self.command_matches_value(proof_context(&proof).0, value, command) =>
            {
                command.clone()
            }
            _ => {
                fetched_command = true;
                self.fetch_verified_value_with_budget(
                    Self::finish_control_budget(&mut control_budget, context, mutation_started)?,
                    proof_context(&proof).0,
                    value,
                    mutation_started,
                )?
                .ok_or(Error::CommandUnavailable)?
            }
        };
        let budget = Self::finish_control_budget(&mut control_budget, context, mutation_started)?;
        if fetched_command {
            #[cfg(test)]
            record_budget_identity(
                &budget.caller,
                BudgetIdentityEvent::FinishFetchHandoff {
                    deadline: budget.deadline,
                    work_deadline: budget.work_deadline,
                    // Fetch returns only after its group has been drained.
                    outstanding: 0,
                    mutation_started: mutation_started as *const AtomicBool as usize,
                },
            );
        }
        if let Err(error) =
            self.install_decision_proof_quorum_with_budget(budget, proof.clone(), mutation_started)
        {
            // Once a decision proof exists, any non-safety failure while
            // durably installing it may follow a partial install.  Returning
            // a retryable transport/quorum failure would permit a caller to
            // treat the decided slot as uncommitted.
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
                &_command,
            );
        }
        Ok(DriveOutcome::Decision(proof))
    }

    /// A failed durable-proof acknowledgement is ambiguous only after a
    /// decision exists. The installer has already drained its worker group
    /// before returning, so this bounded inspection can reuse the same
    /// caller deadline without nesting control-worker groups.
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
        match self.inspect_typed_record_summaries_with_budget(
            budget,
            mutation_started,
            slot,
            value.prev_hash,
        ) {
            Ok(CertifiedDecisionInspection::Committed(certified))
                if certified.certificate.value == *value
                    && certified.entry.entry_type == offered_command.entry_type
                    && certified.entry.payload == offered_command.payload
                    && self.command_matches_value(slot, value, offered_command) =>
            {
                Ok(DriveOutcome::Decision(proof.clone()))
            }
            // Both certificates were individually validated for this exact
            // slot and predecessor. A different decided value is therefore
            // safety evidence, never permission to acknowledge our offer.
            Ok(CertifiedDecisionInspection::Committed(_)) => Err(Error::ConflictingCertificates),
            Ok(
                CertifiedDecisionInspection::Empty
                | CertifiedDecisionInspection::Pending
                | CertifiedDecisionInspection::Unavailable,
            ) => Err(Error::UnknownOutcome),
            Err(error) if Self::is_control_safety_error(&error) => Err(error),
            Err(_) => Err(Error::UnknownOutcome),
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

    #[cfg(test)]
    fn install_decision_proof_quorum(
        &self,
        proof: DecisionProof,
        context: &RecorderRpcContext,
        mutation_started: &AtomicBool,
    ) -> Result<()> {
        let budget = ControlCallBudget::new(context)
            .map_err(|error| Self::store_context_error(error, mutation_started))?;
        self.install_decision_proof_quorum_with_budget(&budget, proof, mutation_started)
    }

    /// Installs a proof under an already captured control budget.  Nested
    /// fetch/install paths must use this entrypoint to share D/W and the root
    /// mutation certainty.
    fn install_decision_proof_quorum_with_budget(
        &self,
        budget: &ControlCallBudget,
        proof: DecisionProof,
        mutation_started: &AtomicBool,
    ) -> Result<()> {
        check_operation_context(&budget.caller, mutation_started)?;
        let membership = self.membership.clone();
        let quorum = membership.quorum_size();
        let total = self.control_workers.len();
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let group = ControlCallGroup::new();
        #[cfg(feature = "test-hooks")]
        group.attach_test_root_probe(&budget.caller);
        let mut dispatches = vec![None; total];
        let mut dispatch_error = None;
        let mut install_dispatch_paused = false;
        for (index, worker) in self.control_workers.iter().enumerate() {
            if let Err(error) = budget.check_admission() {
                dispatch_error = Some(Self::store_context_error(error, mutation_started));
                break;
            }
            let dispatch = worker.dispatch_mutating_group(
                ControlJob::InstallProof {
                    index,
                    context: budget.child_context(&group),
                    proof: proof.clone(),
                    membership: membership.clone(),
                    result: sender.clone(),
                },
                &group,
                mutation_started,
            );
            dispatches[index] = Some(dispatch);
            if dispatch == ControlDispatch::Accepted && !install_dispatch_paused {
                install_dispatch_paused = true;
                #[cfg(test)]
                pause_after_fetch_dispatch(&budget.caller);
                #[cfg(test)]
                capture_fetch_group_token(&budget.caller, &group);
                #[cfg(test)]
                record_budget_identity(
                    &budget.caller,
                    BudgetIdentityEvent::InstallDispatch {
                        deadline: budget.deadline,
                        work_deadline: budget.work_deadline,
                        mutation_started: mutation_started as *const AtomicBool as usize,
                        mutation_started_set: mutation_started.load(Ordering::Acquire),
                    },
                );
            }
        }
        drop(sender);
        let admitted: Vec<_> = dispatches
            .iter()
            .map(|dispatch| *dispatch == Some(ControlDispatch::Accepted))
            .collect();
        let accepted_count = admitted.iter().filter(|admitted| **admitted).count();
        let saturated = dispatches
            .iter()
            .filter(|dispatch| **dispatch == Some(ControlDispatch::Saturated))
            .count();
        let mut installed = 0;
        let mut received = 0;
        let mut replied = vec![false; total];
        let mut worker_failed = dispatches.contains(&Some(ControlDispatch::Failed));
        let mut observed_unknown = false;
        let mut safety_error = None;
        let mut typed_error = None;
        let mut frozen = dispatch_error.map(Err);
        let mut disconnected = false;
        while frozen.is_none() {
            if let Err(error) = budget.check_admission() {
                frozen = Some(Err(Self::store_context_error(error, mutation_started)));
                break;
            }
            let remaining = budget
                .work_deadline
                .checked_duration_since(Instant::now())
                .expect("admission check established the work deadline");
            let result = match receiver.recv_timeout(remaining.min(CONTEXT_POLL_INTERVAL)) {
                Ok(result) => Some(result),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    if mutation_started.load(Ordering::Acquire) {
                        frozen = Some(Err(Error::UnknownOutcome));
                    }
                    None
                }
            };
            let Some((index, result)) = result else {
                if frozen.is_some() || disconnected {
                    break;
                }
                continue;
            };
            let admitted_reply = if index >= total {
                observed_unknown = true;
                false
            } else if admitted[index] {
                if replied[index] {
                    observed_unknown = true;
                    false
                } else {
                    replied[index] = true;
                    received += 1;
                    true
                }
            } else {
                false
            };
            if admitted_reply {
                match result {
                    Ok(()) => installed += 1,
                    Err(Error::UnknownOutcome) => observed_unknown = true,
                    Err(Error::RpcCancelled | Error::RpcDeadlineExceeded) => {
                        observed_unknown = true;
                    }
                    Err(Error::ProposeFailed) => worker_failed = true,
                    Err(error) if Self::is_control_safety_error(&error) => {
                        safety_error.get_or_insert(error);
                    }
                    Err(
                        error @ (Error::TypedProofInstallRequired | Error::TypedRecordRequired),
                    ) => {
                        typed_error.get_or_insert(error);
                    }
                    Err(_) => {}
                }
            }
            if let Some(error) = safety_error.clone() {
                frozen = Some(Err(error));
            } else if installed >= quorum {
                frozen = Some(Ok(()));
            } else if received == accepted_count {
                break;
            }
        }
        group.cancel_and_prune();
        let timed_out = group.drain_to_deadline(budget.deadline);
        for worker in &timed_out {
            worker.quarantine();
        }
        while let Ok((index, result)) = receiver.try_recv() {
            let admitted_reply = if index >= total {
                observed_unknown = true;
                false
            } else if admitted[index] {
                if replied[index] {
                    observed_unknown = true;
                    false
                } else {
                    replied[index] = true;
                    received += 1;
                    true
                }
            } else {
                false
            };
            if admitted_reply {
                match result {
                    Ok(()) => installed += 1,
                    Err(Error::UnknownOutcome) => observed_unknown = true,
                    Err(Error::ProposeFailed) => worker_failed = true,
                    Err(error) if Self::is_control_safety_error(&error) => {
                        safety_error.get_or_insert(error);
                    }
                    Err(
                        error @ (Error::TypedProofInstallRequired | Error::TypedRecordRequired),
                    ) => {
                        typed_error.get_or_insert(error);
                    }
                    // Cleanup cancellation is delivery, not an independently
                    // ambiguous proof installation.
                    Err(Error::RpcCancelled | Error::RpcDeadlineExceeded) | Err(_) => {}
                }
            }
        }
        if let Some(error) = safety_error {
            return Err(error);
        }
        // Exact durable/idempotent installs from distinct admitted workers are
        // sufficient post-decision evidence.  An earlier ambiguous delivery
        // cannot erase that quorum; it remains UnknownOutcome only when no
        // exact quorum is subsequently observed.
        if installed >= quorum {
            return Ok(());
        }
        if let Some(error) = typed_error {
            return Err(error);
        }
        if received < accepted_count || observed_unknown || !timed_out.is_empty() {
            return Err(Error::UnknownOutcome);
        }
        if let Err(error) = budget.check_admission() {
            return Err(Self::store_context_error(error, mutation_started));
        }
        match frozen {
            Some(result) => result,
            None if worker_failed && !control_quorum_reachable(installed, saturated, quorum) => {
                Err(Error::ProposeFailed)
            }
            None => Err(Error::NoQuorum),
        }
    }

    #[cfg(test)]
    fn record_broadcast(&self, requests: Vec<RecordRequest>) -> Result<Vec<RecordSummary>> {
        let context = RecorderRpcContext::default_timeout();
        let mutation_started = AtomicBool::new(false);
        self.record_broadcast_with_context(requests, context, &mutation_started)
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
        let total = self.record_workers.len().min(requests.len());
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let group = RpcCallGroup::new();
        #[cfg(feature = "test-hooks")]
        if let Some(slot) = requests.first().map(|request| request.slot) {
            group.attach_test_record_probe(self.test_instance_id, slot);
        }
        let mut dispatches = vec![RecordDispatch::NotAttempted; total];
        let mut dispatch_error = None;
        for (index, (worker, request)) in self.record_workers.iter().zip(requests).enumerate() {
            let dispatch = match budget.check_admission() {
                Ok(()) => worker.dispatch_mutating_group(
                    RecordJob {
                        index,
                        context: budget.child_context(&group),
                        request,
                        result: sender.clone(),
                    },
                    &group,
                    mutation_started,
                ),
                Err(error) => {
                    dispatch_error = Some(Self::store_context_error(error, mutation_started));
                    break;
                }
            };
            dispatches[index] = dispatch;
        }
        drop(sender);
        let saturated = dispatches
            .iter()
            .filter(|dispatch| **dispatch == RecordDispatch::Saturated)
            .count();
        let accepted = dispatches
            .iter()
            .filter(|dispatch| **dispatch == RecordDispatch::Accepted)
            .count();
        let mut accepted_completed = 0;
        let mut replied = vec![false; total];
        let mut typed_errors = vec![None; total];
        // A preclosed or quarantined worker was actually contacted and
        // definitely failed admission.  It must affect the final quorum
        // classification, unlike workers the caller never attempted after a
        // context stop.
        let mut worker_failed = dispatches.contains(&RecordDispatch::Failed);
        let mut observed_unknown = false;
        let mut safety_error = None;
        let mut replies = Vec::with_capacity(quorum);
        let mut frozen: Option<Result<Vec<RecordSummary>>> = dispatch_error.map(Err);
        while frozen.is_none() && accepted_completed < accepted {
            if let Err(error) = budget.check_admission() {
                frozen = Some(Err(Self::store_context_error(error, mutation_started)));
                break;
            }
            let remaining = budget
                .work_deadline
                .checked_duration_since(Instant::now())
                .expect("record admission check established the work deadline");
            let (index, result) = match receiver.recv_timeout(remaining.min(CONTEXT_POLL_INTERVAL))
            {
                Ok(reply) => reply,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    frozen = Some(Err(Error::UnknownOutcome));
                    break;
                }
            };
            if index >= total || dispatches[index] != RecordDispatch::Accepted {
                observed_unknown = true;
                continue;
            }
            if replied[index] {
                observed_unknown = true;
                continue;
            }
            replied[index] = true;
            accepted_completed += 1;
            match result {
                Ok(reply) => {
                    if !replies
                        .iter()
                        .any(|seen: &RecordSummary| seen.recorder_id == reply.recorder_id)
                    {
                        replies.push(reply);
                    }
                    if replies.len() >= quorum {
                        frozen = Some(Ok(replies.clone()));
                    }
                }
                Err(Error::UnknownOutcome) => {
                    observed_unknown = true;
                }
                Err(error) if Self::is_record_safety_error(&error) => {
                    safety_error.get_or_insert(error);
                }
                Err(error @ (Error::TypedRecordRequired | Error::Rejected(_))) => {
                    typed_errors[index] = Some(error);
                }
                Err(Error::RpcCancelled | Error::RpcDeadlineExceeded) => {
                    observed_unknown |= mutation_started.load(Ordering::Acquire);
                }
                Err(Error::ProposeFailed) => worker_failed = true,
                Err(_) => {}
            }
            let accepted_remaining = accepted.saturating_sub(accepted_completed);
            if replies.len() + saturated + accepted_remaining < quorum {
                frozen = Some(match typed_errors.iter().flatten().next() {
                    Some(error) => Err(error.clone()),
                    None if worker_failed => Err(Error::ProposeFailed),
                    None => Ok(replies.clone()),
                });
            }
        }
        if frozen.is_none() {
            frozen = Some(if accepted_completed < accepted {
                Err(Error::UnknownOutcome)
            } else if replies.len() + saturated >= quorum {
                Ok(replies)
            } else {
                match typed_errors.into_iter().flatten().next() {
                    Some(error) => Err(error),
                    None if worker_failed => Err(Error::ProposeFailed),
                    None => Ok(replies),
                }
            });
        }

        let exact_quorum = matches!(&frozen, Some(Ok(replies)) if replies.len() >= quorum);
        // Record requests are immutable and idempotent for this exact proposer
        // operation. Once distinct current voters have returned an exact
        // quorum, cancel the minority hedge and spend only the reserved drain
        // budget reclaiming it; an ambiguous minority cannot erase the quorum.
        // Without that quorum, keep the original caller deadline and preserve
        // every admitted mutation ambiguity as UnknownOutcome.
        let drain_deadline = if exact_quorum {
            group.cancel_and_prune();
            Instant::now()
                .checked_add(CONTROL_DRAIN_RESERVE)
                .map_or(budget.deadline, |deadline| deadline.min(budget.deadline))
        } else {
            group.prune_pending();
            budget.deadline
        };
        let timed_out = group.drain_to_deadline(drain_deadline);
        if !timed_out.is_empty() {
            group.cancel();
            group.prune_pending();
            for worker in &timed_out {
                worker.quarantine();
            }
        }
        while let Ok((index, result)) = receiver.try_recv() {
            if index >= total || dispatches[index] != RecordDispatch::Accepted {
                observed_unknown = true;
                continue;
            }
            if replied[index] {
                observed_unknown = true;
                continue;
            }
            replied[index] = true;
            accepted_completed += 1;
            match result {
                Err(error) if Self::is_record_safety_error(&error) => {
                    safety_error.get_or_insert(error);
                }
                Err(Error::UnknownOutcome) => observed_unknown = true,
                _ => {}
            }
        }
        if let Some(error) = safety_error {
            return Err(error);
        }
        if exact_quorum {
            return frozen.expect("exact record quorum freezes a result");
        }
        if observed_unknown || !timed_out.is_empty() || accepted_completed < accepted {
            return Err(Error::UnknownOutcome);
        }
        if let Err(error) = context.check() {
            return Err(Self::store_context_error(error, mutation_started));
        }
        frozen.unwrap()
    }

    pub fn inspect_decision_at(
        &self,
        context: &RecorderRpcContext,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<DecisionInspection> {
        Ok(
            match self.inspect_certified_decision_at(context, slot, prev_hash)? {
                CertifiedDecisionInspection::Committed(certified) => {
                    DecisionInspection::Committed(certified.entry)
                }
                CertifiedDecisionInspection::Empty => DecisionInspection::Empty,
                CertifiedDecisionInspection::Pending => DecisionInspection::Pending,
                CertifiedDecisionInspection::Unavailable => DecisionInspection::Unavailable,
            },
        )
    }

    pub fn inspect_decision_proof_at(
        &self,
        context: &RecorderRpcContext,
        slot: Slot,
    ) -> Result<Option<DecisionProof>> {
        let budget = ControlCallBudget::new(context)?;
        self.inspect_decision_proof_with_budget(&budget, slot)
    }

    /// Reuses a top-level read budget so sequential inspection stages share
    /// the original D/W rather than subtracting the drain reserve again.
    fn inspect_decision_proof_with_budget(
        &self,
        budget: &ControlCallBudget,
        slot: Slot,
    ) -> Result<Option<DecisionProof>> {
        let group = ControlCallGroup::new();
        #[cfg(feature = "test-hooks")]
        group.attach_test_root_probe(&budget.caller);
        let quorum = self.membership.quorum_size();
        let total = self.control_workers.len();
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let mut saturated = 0;
        let mut candidate = None;
        for (index, worker) in self.control_workers.iter().enumerate() {
            if let Err(error) = budget.check_admission() {
                candidate = Some(Err(error));
                break;
            }
            match worker.dispatch_group(
                ControlJob::InspectProof {
                    index,
                    context: budget.child_context(&group),
                    slot,
                    result: sender.clone(),
                },
                &group,
            ) {
                ControlDispatch::Accepted => {}
                ControlDispatch::Saturated => saturated += 1,
                ControlDispatch::Failed => {}
            }
        }
        drop(sender);
        let mut successful = BTreeSet::new();
        let mut proofs = Vec::new();
        let mut worker_failed = false;
        let mut observed_unknown = false;
        while candidate.is_none() {
            if let Err(error) = budget.check_admission() {
                candidate = Some(Err(error));
                break;
            }
            let remaining = budget
                .work_deadline
                .checked_duration_since(Instant::now())
                .expect("admission check guaranteed a future work deadline");
            let result = match receiver.recv_timeout(remaining.min(CONTEXT_POLL_INTERVAL)) {
                Ok(result) => Some(result),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    candidate = Some(Err(
                        if worker_failed
                            && !control_quorum_reachable(successful.len(), saturated, quorum)
                        {
                            Error::ProposeFailed
                        } else {
                            Error::NoQuorum
                        },
                    ));
                    None
                }
            };
            let Some((index, result)) = result else {
                continue;
            };
            match result {
                Ok(proof) => {
                    successful.insert(self.membership.members()[index].clone());
                    proofs.extend(proof);
                }
                Err(Error::UnknownOutcome) => {
                    observed_unknown = true;
                    candidate = Some(Err(Error::UnknownOutcome));
                }
                Err(Error::ProposeFailed) => worker_failed = true,
                Err(_) => {}
            }
            if successful.len() >= quorum {
                candidate = Some(self.select_decision_proof(slot, proofs.clone()));
            }
        }

        let candidate = candidate.unwrap_or_else(|| {
            Err(
                if worker_failed && !control_quorum_reachable(successful.len(), saturated, quorum) {
                    Error::ProposeFailed
                } else {
                    Error::NoQuorum
                },
            )
        });
        group.cancel_and_prune();
        let timed_out_workers = group.drain_to_deadline(budget.deadline);

        // Quarantine is cleanup, not result selection.  Do it first even if
        // later evidence proves a safety error or an unknown outcome.
        for worker in &timed_out_workers {
            worker.quarantine();
        }

        let mut safety_error = match &candidate {
            Err(error) if Self::is_control_safety_error(error) => Some(error.clone()),
            _ => None,
        };
        let mut validated_proofs = Vec::new();
        for proof in proofs {
            self.fold_inspected_proof(slot, proof, &mut validated_proofs, &mut safety_error);
        }
        // `drain_to_deadline` proves every non-timed-out accepted job has
        // either sent its reply before completing or dropped its sender while
        // unwinding.  With a timed-out worker its sender may remain live, so
        // an Empty channel is likewise complete for every job that did drain.
        while let Ok((_, result)) = receiver.try_recv() {
            self.fold_late_inspect_proof_result(
                slot,
                result,
                &mut validated_proofs,
                &mut safety_error,
                &mut observed_unknown,
            );
        }
        if let Some(error) = safety_error {
            return Err(error);
        }
        if observed_unknown {
            return Err(Error::UnknownOutcome);
        }
        if !timed_out_workers.is_empty() {
            return Err(Error::RpcDeadlineExceeded);
        }
        budget.check_admission()?;
        candidate
    }

    fn fold_late_inspect_proof_result(
        &self,
        slot: Slot,
        result: Result<Option<DecisionProof>>,
        validated_proofs: &mut Vec<DecisionProof>,
        safety_error: &mut Option<Error>,
        observed_unknown: &mut bool,
    ) {
        match result {
            Ok(Some(proof)) => {
                self.fold_inspected_proof(slot, proof, validated_proofs, safety_error);
            }
            Err(Error::UnknownOutcome) => *observed_unknown = true,
            Ok(None) | Err(_) => {}
        }
    }

    /// Validates every proof in arrival-independent order.  This is used for
    /// both the replies observed before the frozen candidate and replies
    /// drained afterwards, so a valid but conflicting certificate can never
    /// be hidden by a quorum result or an intervening `UnknownOutcome`.
    fn fold_inspected_proof(
        &self,
        slot: Slot,
        proof: DecisionProof,
        validated_proofs: &mut Vec<DecisionProof>,
        safety_error: &mut Option<Error>,
    ) {
        let validation = self.validate_inspected_proof(slot, &proof).and_then(|()| {
            if validated_proofs
                .iter()
                .any(|existing| existing.proposal().value != proof.proposal().value)
            {
                Err(Error::ConflictingCertificates)
            } else {
                Ok(())
            }
        });
        match validation {
            Ok(()) => validated_proofs.push(proof),
            Err(error) if safety_error.is_none() => *safety_error = Some(error),
            Err(_) => {}
        }
    }

    fn validate_inspected_proof(&self, slot: Slot, proof: &DecisionProof) -> Result<()> {
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
            .map_err(Error::Rejected)
    }

    fn is_control_safety_error(error: &Error) -> bool {
        matches!(
            error,
            Error::ChainConflict { .. }
                | Error::CommandHashMismatch
                | Error::ConflictingCertificates
                // In this collector every retained Rejected candidate comes
                // from local proof validation; remote rejections are ignored
                // while collecting replies and cannot reach this precedence.
                | Error::Rejected(_)
        )
    }

    fn is_record_safety_error(error: &Error) -> bool {
        matches!(
            error,
            Error::ChainConflict { .. }
                | Error::CommandHashMismatch
                | Error::ConflictingCertificates
        )
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

    fn certified_inspection_from_proof(
        &self,
        budget: &ControlCallBudget,
        mutation_started: &AtomicBool,
        slot: Slot,
        prev_hash: LogHash,
        proof: DecisionProof,
    ) -> Result<CertifiedDecisionInspection> {
        let decision = certificate_from_proof(&proof)?;
        self.ensure_predecessor(slot, prev_hash, decision.value.prev_hash)?;
        let Some(command) =
            self.fetch_verified_value_with_budget(budget, slot, &decision.value, mutation_started)?
        else {
            return Ok(CertifiedDecisionInspection::Unavailable);
        };
        if command.entry_type == EntryType::ConfigChange {
            #[cfg(test)]
            record_budget_identity(
                &budget.caller,
                BudgetIdentityEvent::FetchHandoff {
                    deadline: budget.deadline,
                    work_deadline: budget.work_deadline,
                    // Fetch returns only after its group has been drained.
                    outstanding: 0,
                    mutation_started: mutation_started as *const AtomicBool as usize,
                },
            );
            self.install_decision_proof_quorum_with_budget(
                budget,
                proof.clone(),
                mutation_started,
            )?;
        }
        let entry = self.log_entry_from_value(slot, command, &decision.value)?;
        Ok(CertifiedDecisionInspection::Committed(Box::new(
            CertifiedDecision {
                entry,
                certificate: decision,
                proof,
            },
        )))
    }

    pub fn inspect_certified_decision_at(
        &self,
        context: &RecorderRpcContext,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<CertifiedDecisionInspection> {
        let budget = ControlCallBudget::new(context)?;
        let mutation_started = AtomicBool::new(false);
        self.inspect_typed_record_summaries_with_budget(&budget, &mutation_started, slot, prev_hash)
    }

    pub fn supports_context_read_fence(&self) -> bool {
        self.recorders
            .iter()
            .all(|recorder| recorder.supports_context_read_fence())
    }

    /// Observes whether `slot` is still empty at a quorum of recorders without
    /// mutating durable state. Any occupied or ambiguous quorum is delegated to
    /// the existing certified inspection path and can never become Empty.
    pub fn inspect_context_read_fence_at(
        &self,
        context: &RecorderRpcContext,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<CertifiedDecisionInspection> {
        if !self.supports_context_read_fence() {
            return Err(Error::ReadFenceUnsupported);
        }
        let budget = ControlCallBudget::new(context)?;
        let mutation_started = AtomicBool::new(false);
        self.inspect_context_read_fence_with_budget(&budget, &mutation_started, slot, prev_hash)
    }

    fn inspect_context_read_fence_with_budget(
        &self,
        budget: &ControlCallBudget,
        mutation_started: &AtomicBool,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<CertifiedDecisionInspection> {
        let quorum = self.membership.quorum_size();
        check_operation_context(&budget.caller, mutation_started)?;
        let total = self.read_fence_workers.len();
        let request = ReadFenceRequest {
            cluster_id: self.cluster_id.clone(),
            epoch: self.epoch,
            config_id: self.config_id,
            config_digest: self.config_digest,
            slot,
        };
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let group = ControlCallGroup::new();
        #[cfg(feature = "test-hooks")]
        group.attach_test_root_probe(&budget.caller);
        let mut saturated = 0;
        let mut dispatch_error = None;
        let mut read_fence_dispatch_paused = false;
        for (index, worker) in self.read_fence_workers.iter().enumerate() {
            if let Err(error) = budget.check_admission() {
                dispatch_error = Some(Self::fetch_context_error(error, mutation_started));
                break;
            }
            let dispatch = worker.dispatch_group(
                ControlJob::ObserveReadFence {
                    index,
                    context: budget.child_context(&group),
                    request: request.clone(),
                    result: sender.clone(),
                },
                &group,
            );
            if dispatch == ControlDispatch::Accepted && !read_fence_dispatch_paused {
                read_fence_dispatch_paused = true;
                #[cfg(test)]
                pause_after_fetch_dispatch(&budget.caller);
                #[cfg(test)]
                capture_fetch_group_token(&budget.caller, &group);
            }
            if dispatch == ControlDispatch::Saturated {
                saturated += 1;
            }
        }
        drop(sender);
        let mut successful = 0_usize;
        let mut empty = 0_usize;
        let mut worker_failed = false;
        let mut observed_unknown = false;
        let mut safety_error = None;
        let mut disconnected = false;
        let mut received = 0_usize;
        let mut frozen: Option<Result<bool>> = None;
        while frozen.is_none() && !observed_unknown {
            if let Err(error) = budget.check_admission() {
                frozen = Some(Err(Self::fetch_context_error(error, mutation_started)));
                break;
            }
            let remaining = budget
                .work_deadline
                .checked_duration_since(Instant::now())
                .unwrap();
            let result = match receiver.recv_timeout(remaining.min(CONTEXT_POLL_INTERVAL)) {
                Ok(result) => Some(result),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            };
            let Some((index, result)) = result else {
                continue;
            };
            received += 1;
            match result {
                Ok(observation)
                    if valid_read_fence_observation(
                        &observation,
                        &self.membership.members()[index],
                        &request,
                    ) =>
                {
                    successful += 1;
                    if observation.slot_state == ReadFenceSlotState::Empty {
                        empty += 1;
                    }
                }
                Err(Error::UnknownOutcome) => observed_unknown = true,
                Err(Error::ProposeFailed) => worker_failed = true,
                // A malformed identity/context observation is evidence of a
                // broken fence protocol, not an ordinary unavailable voter.
                Ok(_) => {
                    safety_error.get_or_insert(Error::Rejected(RejectReason::InvalidCertificate));
                }
                // Group cleanup can synthesize these replies for pruned
                // hedges. The root context is checked after drain.
                Err(Error::RpcCancelled | Error::RpcDeadlineExceeded) => {}
                Err(_) => {}
            }
            if empty >= quorum {
                // A quorum of Empty observations is a valid linearization
                // point. A late occupied/higher fence reply is later
                // ordinary evidence and cannot overturn that quorum; quorum
                // intersection makes the Empty result safe at that point.
                frozen = Some(Ok(true));
            } else if successful >= quorum
                && empty.saturating_add(total.saturating_sub(received)) < quorum
            {
                frozen = Some(Ok(false));
            }
        }
        let frozen = dispatch_error.map(Err).unwrap_or_else(|| {
            frozen.unwrap_or_else(|| {
                if disconnected && mutation_started.load(Ordering::Acquire) {
                    Err(Error::UnknownOutcome)
                } else if worker_failed && !control_quorum_reachable(successful, saturated, quorum)
                {
                    Err(Error::ProposeFailed)
                } else {
                    Ok(false)
                }
            })
        });
        if let Err(error) = &frozen {
            if Self::is_control_safety_error(error) {
                safety_error = Some(error.clone());
            }
        }
        group.cancel_and_prune();
        let timed_out = group.drain_to_deadline(budget.deadline);
        for worker in &timed_out {
            worker.quarantine();
        }
        while let Ok((index, result)) = receiver.try_recv() {
            match result {
                Ok(observation)
                    if valid_read_fence_observation(
                        &observation,
                        &self.membership.members()[index],
                        &request,
                    ) => {}
                Ok(_) => {
                    safety_error.get_or_insert(Error::Rejected(RejectReason::InvalidCertificate));
                }
                Err(Error::UnknownOutcome) => observed_unknown = true,
                _ => {}
            }
        }
        if let Some(error) = safety_error {
            return Err(error);
        }
        if observed_unknown {
            return Err(Error::UnknownOutcome);
        }
        if !timed_out.is_empty() {
            return Err(if mutation_started.load(Ordering::Acquire) {
                Error::UnknownOutcome
            } else {
                Error::RpcDeadlineExceeded
            });
        }
        if let Err(error) = budget.check_admission() {
            return Err(Self::fetch_context_error(error, mutation_started));
        }
        match frozen? {
            true => Ok(CertifiedDecisionInspection::Empty),
            false if successful < quorum => {
                if worker_failed && !control_quorum_reachable(successful, saturated, quorum) {
                    Err(Error::ProposeFailed)
                } else {
                    Ok(CertifiedDecisionInspection::Unavailable)
                }
            }
            false => {
                let inspection = {
                    #[cfg(test)]
                    record_budget_identity(
                        &budget.caller,
                        BudgetIdentityEvent::ReadFenceHandoff {
                            deadline: budget.deadline,
                            work_deadline: budget.work_deadline,
                            outstanding: group.outstanding_len(),
                        },
                    );
                    self.inspect_typed_record_summaries_with_budget(
                        budget,
                        mutation_started,
                        slot,
                        prev_hash,
                    )?
                };
                Ok(match inspection {
                    // An occupied fence quorum cannot be weakened by the
                    // typed summary path's context-free absence result.
                    CertifiedDecisionInspection::Empty => CertifiedDecisionInspection::Pending,
                    inspection => inspection,
                })
            }
        }
    }

    fn inspect_typed_record_summaries_with_budget(
        &self,
        budget: &ControlCallBudget,
        mutation_started: &AtomicBool,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<CertifiedDecisionInspection> {
        let quorum = self.membership.quorum_size();
        check_operation_context(&budget.caller, mutation_started)?;
        let config_id = self.config_id;
        let config_digest = self.config_digest;
        let total = self.control_workers.len();
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let group = ControlCallGroup::new();
        #[cfg(feature = "test-hooks")]
        group.attach_test_root_probe(&budget.caller);
        let mut saturated = 0;
        let mut dispatch_error = None;
        let mut summary_dispatch_paused = false;
        for (index, worker) in self.control_workers.iter().enumerate() {
            if let Err(error) = budget.check_admission() {
                dispatch_error = Some(match error {
                    Error::RpcCancelled | Error::RpcDeadlineExceeded
                        if mutation_started.load(Ordering::Acquire) =>
                    {
                        Error::UnknownOutcome
                    }
                    error => error,
                });
                break;
            }
            let dispatch = worker.dispatch_group(
                ControlJob::InspectSummary {
                    index,
                    context: budget.child_context(&group),
                    slot,
                    result: sender.clone(),
                },
                &group,
            );
            if dispatch == ControlDispatch::Accepted && !summary_dispatch_paused {
                summary_dispatch_paused = true;
                #[cfg(test)]
                pause_after_summary_dispatch(&budget.caller);
            }
            if dispatch == ControlDispatch::Saturated {
                saturated += 1;
            }
        }
        drop(sender);
        let mut successful = 0;
        let mut summaries = Vec::new();
        let mut worker_failed = false;
        let mut observed_unknown = false;
        let mut safety_error = None;
        let mut summary_error = None;
        let mut frozen: Option<Result<Option<DecisionProof>>> = None;
        let mut provisional_none_hook_fired = false;
        // An admitted summary RPC remains safety-relevant even after a
        // quorum has produced a candidate.  In particular, a quorum of
        // `None` responses cannot establish absence while another admitted
        // recorder can still report an occupied slot or a conflicting proof.
        // Keep the bounded original W/D until every admitted reply is
        // observed (or the caller deadline/cancellation makes that
        // impossible); only then may cleanup prune anything left queued.
        while !observed_unknown {
            if let Err(error) = budget.check_admission() {
                frozen = Some(Err(error));
                break;
            }
            let remaining = budget
                .work_deadline
                .checked_duration_since(Instant::now())
                .unwrap();
            let result = match receiver.recv_timeout(remaining.min(CONTEXT_POLL_INTERVAL)) {
                Ok(result) => Some(result),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            };
            let Some((index, result)) = result else {
                continue;
            };
            match result {
                Ok(summary)
                    if summary.as_ref().is_none_or(|summary| {
                        summary.recorder_id == self.membership.members()[index]
                            && summary.slot == slot
                            && summary.config_id == config_id
                            && summary.config_digest == config_digest
                    }) =>
                {
                    successful += 1;
                    summaries.extend(summary);
                }
                Err(Error::UnknownOutcome) => observed_unknown = true,
                Err(Error::ProposeFailed) => {
                    worker_failed = true;
                    summary_error.get_or_insert(Error::ProposeFailed);
                }
                Err(error) if Self::is_control_safety_error(&error) => {
                    safety_error.get_or_insert(error);
                }
                Ok(_) => {
                    safety_error.get_or_insert(Error::Rejected(RejectReason::InvalidCertificate));
                }
                Err(error) => {
                    summary_error.get_or_insert(error);
                }
            }
            if successful >= quorum {
                let proof = self.proof_from_record_summaries(slot, &summaries);
                if matches!(&proof, Ok(None))
                    && !summaries.is_empty()
                    && !provisional_none_hook_fired
                {
                    provisional_none_hook_fired = true;
                    #[cfg(test)]
                    pause_after_summary_provisional_none(&budget.caller);
                }
                frozen = Some(proof);
            }
        }
        let mut frozen = dispatch_error.map(Err).unwrap_or_else(|| {
            frozen.unwrap_or_else(|| {
                if successful < quorum {
                    Err(
                        if worker_failed && !control_quorum_reachable(successful, saturated, quorum)
                        {
                            Error::ProposeFailed
                        } else {
                            Error::NoQuorum
                        },
                    )
                } else {
                    self.proof_from_record_summaries(slot, &summaries)
                }
            })
        });
        if let Err(error) = &frozen {
            if Self::is_control_safety_error(error) {
                safety_error = Some(error.clone());
            }
        }
        group.cancel_and_prune();
        let timed_out = group.drain_to_deadline(budget.deadline);
        for worker in &timed_out {
            worker.quarantine();
        }
        while let Ok((index, result)) = receiver.try_recv() {
            match result {
                Ok(summary)
                    if summary.as_ref().is_none_or(|summary| {
                        summary.recorder_id == self.membership.members()[index]
                            && summary.slot == slot
                            && summary.config_id == config_id
                            && summary.config_digest == config_digest
                    }) =>
                {
                    summaries.extend(summary)
                }
                Ok(_) => {
                    safety_error.get_or_insert(Error::Rejected(RejectReason::InvalidCertificate));
                }
                Err(Error::UnknownOutcome) => observed_unknown = true,
                Err(Error::ProposeFailed) => {
                    worker_failed = true;
                    summary_error.get_or_insert(Error::ProposeFailed);
                }
                Err(error) if Self::is_control_safety_error(&error) => {
                    safety_error.get_or_insert(error);
                }
                Err(error) => {
                    summary_error.get_or_insert(error);
                }
            }
        }
        if safety_error.is_none() {
            match self.proof_from_record_summaries(slot, &summaries) {
                Err(error) => safety_error = Some(error),
                Ok(Some(late_proof)) => match &frozen {
                    // A quorum can first establish only that no proof has
                    // been observed. The compulsory drain may then deliver
                    // the missing valid evidence; preserve it for the
                    // certified inspection rather than returning Pending.
                    Ok(None) => frozen = Ok(Some(late_proof)),
                    Ok(Some(frozen_proof))
                        if frozen_proof.proposal().value != late_proof.proposal().value =>
                    {
                        safety_error = Some(Error::ConflictingCertificates);
                    }
                    _ => {}
                },
                _ => {}
            }
        }
        if let Some(error) = safety_error {
            return Err(error);
        }
        if observed_unknown {
            return Err(Error::UnknownOutcome);
        }
        if !timed_out.is_empty() {
            return Err(if mutation_started.load(Ordering::Acquire) {
                Error::UnknownOutcome
            } else {
                Error::RpcDeadlineExceeded
            });
        }
        if let Err(error) = budget.check_admission() {
            return Err(if mutation_started.load(Ordering::Acquire) {
                Error::UnknownOutcome
            } else {
                error
            });
        }
        let proof = match frozen {
            Ok(proof) => proof,
            Err(error) => return Err(summary_error.unwrap_or(error)),
        };
        if let Some(proof) = proof {
            #[cfg(test)]
            record_budget_identity(
                &budget.caller,
                BudgetIdentityEvent::SummaryHandoff {
                    deadline: budget.deadline,
                    work_deadline: budget.work_deadline,
                    outstanding: group.outstanding_len(),
                },
            );
            return self.certified_inspection_from_proof(
                budget,
                mutation_started,
                slot,
                prev_hash,
                proof,
            );
        }
        if let Some(error) = summary_error {
            return Err(error);
        }
        if successful < quorum {
            if worker_failed {
                return Err(Error::ProposeFailed);
            }
            return Ok(CertifiedDecisionInspection::Unavailable);
        }
        if summaries.is_empty() {
            return Ok(CertifiedDecisionInspection::Empty);
        }
        if summaries.len() < quorum {
            return Ok(CertifiedDecisionInspection::Unavailable);
        }
        Ok(CertifiedDecisionInspection::Pending)
    }

    fn proof_from_record_summaries(
        &self,
        slot: Slot,
        summaries: &[RecordSummary],
    ) -> Result<Option<DecisionProof>> {
        let quorum = self.membership.quorum_size();
        let mut summaries = summaries.to_vec();
        summaries.sort_by(|left, right| left.recorder_id.cmp(&right.recorder_id));
        summaries.dedup_by(|left, right| left.recorder_id == right.recorder_id);
        let installed_proofs = summaries
            .iter()
            .filter_map(|summary| summary.decided.clone())
            .collect();
        if let Some(proof) = self.select_decision_proof(slot, installed_proofs)? {
            return Ok(Some(proof));
        }
        for step in summaries
            .iter()
            .map(|summary| summary.step)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .rev()
        {
            let mut step_summaries: Vec<_> = summaries
                .iter()
                .filter(|summary| summary.step == step)
                .cloned()
                .collect();
            if step_summaries.len() < quorum {
                continue;
            }
            step_summaries.truncate(quorum);
            let proof_summaries: Vec<_> = step_summaries
                .iter()
                .map(|summary| RecorderSummary {
                    recorder_id: summary.recorder_id.clone(),
                    slot: summary.slot,
                    step: summary.step,
                    first_current: summary.first_current.clone(),
                    aggregate_prior: summary.aggregate_prior.clone(),
                })
                .collect();
            let proof = if step == 4 {
                step_summaries
                    .first()
                    .and_then(|summary| summary.first_current.clone())
                    .filter(|proposal| proposal.priority == ProposalPriority::MAX)
                    .filter(|proposal| {
                        step_summaries.iter().all(|summary| {
                            summary
                                .first_current
                                .as_ref()
                                .is_some_and(|candidate| proposal_exact(candidate, proposal))
                        })
                    })
                    .map(|proposal| DecisionProof::FastPath {
                        cluster_id: self.cluster_id.clone(),
                        slot,
                        epoch: self.epoch,
                        config_id: self.config_id,
                        config_digest: self.config_digest,
                        proposal,
                        summaries: proof_summaries.clone(),
                    })
            } else {
                None
            };
            let Some(proof) = proof else {
                continue;
            };
            let Ok(Some(proof)) = self.select_decision_proof(slot, vec![proof]) else {
                continue;
            };
            return Ok(Some(proof));
        }
        Ok(None)
    }

    pub fn recover_decision_at(
        &self,
        context: RecorderRpcContext,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<DecisionInspection> {
        match self.inspect_decision_at(&context, slot, prev_hash)? {
            DecisionInspection::Pending => self
                .propose_stored_at(
                    context,
                    slot,
                    prev_hash,
                    StoredCommand::new(EntryType::Noop, Vec::new()),
                )
                .map(DecisionInspection::Committed),
            inspection => Ok(inspection),
        }
    }

    pub fn recover_decided_at(
        &self,
        context: &RecorderRpcContext,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<Option<LogEntry>> {
        match self.inspect_decision_at(context, slot, prev_hash)? {
            DecisionInspection::Committed(entry) => Ok(Some(entry)),
            DecisionInspection::Empty | DecisionInspection::Pending => Ok(None),
            DecisionInspection::Unavailable => Err(Error::CommandUnavailable),
        }
    }

    pub fn recover_decided_next(&self, context: &RecorderRpcContext) -> Result<Option<LogEntry>> {
        let mut tip = self
            .sequential_tip
            .lock()
            .map_err(|_| Error::ProposeFailed)?;
        let Some(entry) = self.recover_decided_at(context, tip.next_index, tip.last_hash)? else {
            return Ok(None);
        };
        tip.next_index = entry
            .index
            .checked_add(1)
            .ok_or(Error::InvalidRecoveredTip)?;
        tip.last_hash = entry.hash;
        Ok(Some(entry))
    }

    fn ensure_predecessor(
        &self,
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

    fn store_context_error(error: Error, mutation_started: &AtomicBool) -> Error {
        match error {
            Error::RpcCancelled | Error::RpcDeadlineExceeded
                if mutation_started.load(Ordering::Acquire) =>
            {
                Error::UnknownOutcome
            }
            error => error,
        }
    }

    /// Runs a typed durable mutation under one caller-captured deadline. Every
    /// admitted mutation is group-owned so a terminal result is not returned
    /// until it has either finished by D or its worker was quarantined.
    fn mutation_on_quorum_with_budget(
        &self,
        budget: &ControlCallBudget,
        mutation_started: &AtomicBool,
        make_job: impl Fn(
            usize,
            RecorderRpcContext,
            std::sync::mpsc::SyncSender<(usize, Result<()>)>,
        ) -> ControlJob,
    ) -> Result<()> {
        check_operation_context(&budget.caller, mutation_started)?;
        let quorum = quorum_size(self.control_workers.len());
        let total = self.control_workers.len();
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let group = ControlCallGroup::new();
        #[cfg(feature = "test-hooks")]
        group.attach_test_root_probe(&budget.caller);
        let mut dispatches = vec![None; total];
        let mut dispatch_error = None;
        let mut store_dispatch_paused = false;
        for (index, worker) in self.control_workers.iter().enumerate() {
            if let Err(error) = budget.check_admission() {
                dispatch_error = Some(Self::store_context_error(error, mutation_started));
                break;
            }
            let dispatch = worker.dispatch_mutating_group(
                make_job(index, budget.child_context(&group), sender.clone()),
                &group,
                mutation_started,
            );
            dispatches[index] = Some(dispatch);
            if dispatch == ControlDispatch::Accepted && !store_dispatch_paused {
                store_dispatch_paused = true;
                #[cfg(test)]
                pause_after_fetch_dispatch(&budget.caller);
                #[cfg(test)]
                capture_fetch_group_token(&budget.caller, &group);
            }
        }
        drop(sender);
        let admitted: Vec<_> = dispatches
            .iter()
            .map(|dispatch| *dispatch == Some(ControlDispatch::Accepted))
            .collect();
        let accepted_count = admitted.iter().filter(|admitted| **admitted).count();
        let saturated = dispatches
            .iter()
            .filter(|dispatch| **dispatch == Some(ControlDispatch::Saturated))
            .count();
        let mut stored = 0;
        let mut received = 0;
        let mut replied = vec![false; total];
        // Direct dispatch failure is part of the final quorum classification,
        // but its synthetic channel reply is not an admitted worker reply.
        let mut worker_failed = dispatches.contains(&Some(ControlDispatch::Failed));
        let mut observed_unknown = false;
        let mut safety_error = None;
        let mut disconnected = false;
        let mut frozen = dispatch_error.map(Err);
        while frozen.is_none() {
            if let Err(error) = budget.check_admission() {
                frozen = Some(Err(Self::store_context_error(error, mutation_started)));
                break;
            }
            let remaining = budget
                .work_deadline
                .checked_duration_since(Instant::now())
                .expect("admission check established the work deadline");
            let result = match receiver.recv_timeout(remaining.min(CONTEXT_POLL_INTERVAL)) {
                Ok(result) => Some(result),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    None
                }
            };
            if disconnected {
                if mutation_started.load(Ordering::Acquire) {
                    frozen = Some(Err(Error::UnknownOutcome));
                }
                break;
            }
            let Some((index, result)) = result else {
                continue;
            };
            let admitted_reply = if index >= total {
                // A worker is bound to its collector index.  A reply which
                // cannot be attributed to that admission is indeterminate.
                observed_unknown = true;
                false
            } else if admitted[index] {
                if replied[index] {
                    // Duplicate replies could otherwise manufacture progress
                    // or conceal a missing admitted worker reply.
                    observed_unknown = true;
                    false
                } else {
                    replied[index] = true;
                    received += 1;
                    true
                }
            } else {
                // Saturated and pre-closed workers can still send a legacy
                // synthetic reply.  Dispatch state, never this arrival,
                // determines their contribution to quorum classification.
                false
            };
            if admitted_reply {
                match result {
                    Ok(()) => stored += 1,
                    Err(Error::UnknownOutcome) => observed_unknown = true,
                    Err(Error::RpcCancelled | Error::RpcDeadlineExceeded) => {
                        observed_unknown = true;
                    }
                    Err(Error::ProposeFailed) => worker_failed = true,
                    Err(error) if Self::is_control_safety_error(&error) => {
                        safety_error.get_or_insert(error);
                    }
                    Err(_) => {}
                }
            }
            if let Some(error) = safety_error.clone() {
                frozen = Some(Err(error));
            } else if stored >= quorum {
                frozen = Some(Ok(()));
            } else if received == accepted_count {
                break;
            }
        }
        group.cancel_and_prune();
        let timed_out = group.drain_to_deadline(budget.deadline);
        for worker in &timed_out {
            worker.quarantine();
        }
        while let Ok((index, result)) = receiver.try_recv() {
            let admitted_reply = if index >= total {
                observed_unknown = true;
                false
            } else if admitted[index] {
                if replied[index] {
                    observed_unknown = true;
                    false
                } else {
                    replied[index] = true;
                    received += 1;
                    true
                }
            } else {
                false
            };
            if admitted_reply {
                match result {
                    Ok(()) => stored += 1,
                    Err(Error::UnknownOutcome) => observed_unknown = true,
                    Err(Error::ProposeFailed) => worker_failed = true,
                    Err(error) if Self::is_control_safety_error(&error) => {
                        safety_error.get_or_insert(error);
                    }
                    // Group cleanup deliberately cancels hedges after the
                    // result is frozen.  It still counts as delivery, but is
                    // not an independently ambiguous mutation.
                    Err(Error::RpcCancelled | Error::RpcDeadlineExceeded) | Err(_) => {}
                }
            }
        }
        if let Some(error) = safety_error {
            return Err(error);
        }
        // StoreCommand verifies the supplied content hash and persists the
        // same content-addressed bytes durably and idempotently at its bound
        // worker.  Thus distinct exact ACKs establish this content-registration
        // quorum even if another admitted delivery is unknown.
        if stored >= quorum {
            return Ok(());
        }
        if received < accepted_count || observed_unknown || !timed_out.is_empty() {
            return Err(Error::UnknownOutcome);
        }
        if let Err(error) = budget.check_admission() {
            return Err(Self::store_context_error(error, mutation_started));
        }
        match frozen {
            Some(result) => result,
            None if stored >= quorum => Ok(()),
            None if worker_failed && !control_quorum_reachable(stored, saturated, quorum) => {
                Err(Error::ProposeFailed)
            }
            None => Err(Error::NoQuorum),
        }
    }

    fn store_command_on_quorum_with_budget(
        &self,
        budget: &ControlCallBudget,
        mutation_started: &AtomicBool,
        command_hash: LogHash,
        command: &StoredCommand,
    ) -> Result<()> {
        self.mutation_on_quorum_with_budget(budget, mutation_started, |index, context, result| {
            ControlJob::StoreCommand {
                index,
                context,
                cluster_id: self.cluster_id.clone(),
                epoch: self.epoch,
                config_id: self.config_id,
                config_digest: self.config_digest,
                command_hash,
                command: command.clone(),
                result,
            }
        })
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
            .map_err(|error| Self::fetch_context_error(error, mutation_started))?;
        self.fetch_verified_value_with_budget(&budget, slot, value, mutation_started)
    }

    fn fetch_context_error(error: Error, mutation_started: &AtomicBool) -> Error {
        match error {
            Error::RpcCancelled | Error::RpcDeadlineExceeded
                if mutation_started.load(Ordering::Acquire) =>
            {
                Error::UnknownOutcome
            }
            error => error,
        }
    }

    /// Collects command bytes under one already-captured control budget.  A
    /// certified-summary caller must use this entrypoint so its fetch cannot
    /// recapture a later deadline or spend a second drain reserve.
    fn fetch_verified_value_with_budget(
        &self,
        budget: &ControlCallBudget,
        slot: Slot,
        value: &AcceptedValue,
        mutation_started: &AtomicBool,
    ) -> Result<Option<StoredCommand>> {
        check_operation_context(&budget.caller, mutation_started)?;
        let quorum = quorum_size(self.control_workers.len());
        let total = self.control_workers.len();
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let group = ControlCallGroup::new();
        #[cfg(feature = "test-hooks")]
        group.attach_test_root_probe(&budget.caller);
        let mut saturated = 0;
        let mut dispatch_error = None;
        let mut fetch_dispatch_paused = false;
        for (index, worker) in self.control_workers.iter().enumerate() {
            if let Err(error) = budget.check_admission() {
                dispatch_error = Some(Self::fetch_context_error(error, mutation_started));
                break;
            }
            let dispatch = worker.dispatch_group(
                ControlJob::FetchCommand {
                    index,
                    context: budget.child_context(&group),
                    cluster_id: self.cluster_id.clone(),
                    epoch: self.epoch,
                    config_id: self.config_id,
                    config_digest: self.config_digest,
                    command_hash: value.command_hash,
                    result: sender.clone(),
                },
                &group,
            );
            if dispatch == ControlDispatch::Accepted && !fetch_dispatch_paused {
                fetch_dispatch_paused = true;
                #[cfg(test)]
                pause_after_fetch_dispatch(&budget.caller);
                #[cfg(test)]
                capture_fetch_group_token(&budget.caller, &group);
                #[cfg(test)]
                record_budget_identity(
                    &budget.caller,
                    BudgetIdentityEvent::FetchDispatch {
                        deadline: budget.deadline,
                        work_deadline: budget.work_deadline,
                    },
                );
            }
            if dispatch == ControlDispatch::Saturated {
                saturated += 1;
            }
        }
        drop(sender);
        let mut successful = 0;
        let mut worker_failed = false;
        let mut observed_unknown = false;
        let mut safety_error = None;
        let mut candidate = None;
        let mut disconnected = false;
        let mut frozen: Option<Result<Option<StoredCommand>>> = None;
        while frozen.is_none() && !observed_unknown {
            if let Err(error) = budget.check_admission() {
                frozen = Some(Err(Self::fetch_context_error(error, mutation_started)));
                break;
            }
            let remaining = budget
                .work_deadline
                .checked_duration_since(Instant::now())
                .unwrap();
            let result = match receiver.recv_timeout(remaining.min(CONTEXT_POLL_INTERVAL)) {
                Ok(result) => Some(result),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            };
            let Some((_, result)) = result else {
                continue;
            };
            match result {
                Ok(command) => {
                    successful += 1;
                    if let Some(command) = command {
                        if command.hash() != value.command_hash {
                            safety_error.get_or_insert(Error::CommandHashMismatch);
                        }
                        if safety_error.is_none() {
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
                // A group-owned hedge can report cancellation after the
                // collector begins its own cleanup.  The root context is
                // checked after drain, so a genuine caller cancellation or
                // deadline still upgrades a post-mutation call without
                // treating cleanup as an external UnknownOutcome.
                Err(Error::RpcCancelled | Error::RpcDeadlineExceeded) => {}
                Err(Error::ProposeFailed) => worker_failed = true,
                Err(_) => {}
            }
            // A locally verified command is already bound to the certified
            // value, so it is a complete fetch candidate.  Absence needs a
            // quorum before it can freeze as `None`.
            if candidate.is_some() || successful >= quorum {
                frozen = Some(Ok(candidate.take()));
            }
        }
        let frozen = dispatch_error.map(Err).unwrap_or_else(|| {
            frozen.unwrap_or_else(|| {
                if disconnected && mutation_started.load(Ordering::Acquire) {
                    Err(Error::UnknownOutcome)
                } else if successful < quorum && saturated > 0 {
                    Err(Error::NoQuorum)
                } else if worker_failed && !control_quorum_reachable(successful, saturated, quorum)
                {
                    Err(Error::ProposeFailed)
                } else {
                    Ok(candidate)
                }
            })
        });
        if let Err(error) = &frozen {
            if Self::is_control_safety_error(error) || matches!(error, Error::CommandHashMismatch) {
                safety_error = Some(error.clone());
            }
        }
        group.cancel_and_prune();
        let timed_out = group.drain_to_deadline(budget.deadline);
        for worker in &timed_out {
            worker.quarantine();
        }
        while let Ok((_, result)) = receiver.try_recv() {
            match result {
                Ok(Some(command)) if command.hash() != value.command_hash => {
                    safety_error.get_or_insert(Error::CommandHashMismatch);
                }
                Ok(Some(command)) => {
                    let expected = AcceptedValue::from_command(
                        &self.cluster_id,
                        slot,
                        self.epoch,
                        self.config_id,
                        value.prev_hash,
                        &command,
                    );
                    if expected != *value {
                        safety_error.get_or_insert(Error::Rejected(RejectReason::InvalidValue));
                    }
                }
                Err(Error::UnknownOutcome) => observed_unknown = true,
                _ => {}
            }
        }
        if let Some(error) = safety_error {
            return Err(error);
        }
        if observed_unknown {
            return Err(Error::UnknownOutcome);
        }
        if !timed_out.is_empty() {
            return Err(if mutation_started.load(Ordering::Acquire) {
                Error::UnknownOutcome
            } else {
                Error::RpcDeadlineExceeded
            });
        }
        if let Err(error) = budget.check_admission() {
            return Err(Self::fetch_context_error(error, mutation_started));
        }
        frozen
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecisionInspection {
    Committed(LogEntry),
    Empty,
    Pending,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CertifiedDecision {
    pub entry: LogEntry,
    pub certificate: DecisionCertificate,
    pub proof: DecisionProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CertifiedDecisionInspection {
    Empty,
    Pending,
    Committed(Box<CertifiedDecision>),
    Unavailable,
}

impl RecorderRpc for RecorderFileStore {
    fn recorder_id(&self, context: &RecorderRpcContext) -> Result<NodeId> {
        context.check()?;
        Ok(self.recorder_id.clone())
    }

    fn record(
        &self,
        context: &RecorderRpcContext,
        request: RecordRequest,
    ) -> Result<RecordSummary> {
        context.check()?;
        self.record_proposal(request)
    }

    fn install_decision_proof(
        &self,
        context: &RecorderRpcContext,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<()> {
        context.check()?;
        self.install_decision_proof_record(proof, membership)
    }

    fn inspect_decision_proof(
        &self,
        context: &RecorderRpcContext,
        slot: Slot,
    ) -> Result<Option<DecisionProof>> {
        context.check()?;
        Ok(self.load(slot)?.decision_proof().cloned())
    }

    fn inspect_record_summary(
        &self,
        context: &RecorderRpcContext,
        slot: Slot,
    ) -> Result<Option<RecordSummary>> {
        context.check()?;
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        let exists_in_wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?
            .slots
            .contains_key(&slot);
        if !exists_in_wal && !self.effect_root_anchor.exists(&Self::slot_name(slot))? {
            return Ok(None);
        }
        let state = self.load_unlocked(slot, configuration.config_digest)?;
        Ok(Some(record_summary(
            &self.recorder_id,
            &state,
            state.decision_proof().cloned(),
        )))
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        context: &RecorderRpcContext,
        request: ReadFenceRequest,
    ) -> Result<ReadFenceObservation> {
        context.check()?;
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        if request.cluster_id != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        if request.epoch != self.epoch {
            return Err(Error::Rejected(if request.epoch < self.epoch {
                RejectReason::StaleEpoch
            } else {
                RejectReason::FutureEpoch
            }));
        }
        let configuration = self.configuration_state()?;
        if request.config_id != configuration.config_id
            || request.config_digest != configuration.config_digest
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        let max_head = configuration.max_accepted_or_decided_slot;
        let exists_in_wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?
            .slots
            .contains_key(&request.slot);
        let exact_exists = exists_in_wal
            || self
                .effect_root_anchor
                .exists(&Self::slot_name(request.slot))?;
        let summary = if exact_exists {
            let state = self.load_unlocked(request.slot, configuration.config_digest)?;
            Some(Box::new(record_summary(
                &self.recorder_id,
                &state,
                state.decision_proof().cloned(),
            )))
        } else {
            None
        };
        let slot_state =
            if summary.is_none() && max_head.is_none_or(|max_head| max_head < request.slot) {
                ReadFenceSlotState::Empty
            } else {
                ReadFenceSlotState::Occupied { summary }
            };
        Ok(ReadFenceObservation {
            recorder_id: self.recorder_id.clone(),
            cluster_id: request.cluster_id,
            epoch: request.epoch,
            config_id: request.config_id,
            config_digest: request.config_digest,
            slot: request.slot,
            max_head,
            slot_state,
        })
    }

    fn store_command_for(
        &self,
        context: &RecorderRpcContext,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> Result<()> {
        context.check()?;
        self.apply(RecorderRequest::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        })?;
        Ok(())
    }

    fn fetch_command_for(
        &self,
        context: &RecorderRpcContext,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> Result<Option<StoredCommand>> {
        context.check()?;
        Ok(self
            .apply(RecorderRequest::FetchCommand {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            })?
            .command)
    }

    fn stage_effect_bundle_chunk(
        &self,
        context: &RecorderRpcContext,
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
        ordinal: u16,
        chunk: Vec<u8>,
    ) -> Result<()> {
        context.check()?;
        self.stage_effect_bundle_chunk(&binding, &manifest_command, ordinal, &chunk)
    }

    fn finalize_staged_effect_bundle(
        &self,
        context: &RecorderRpcContext,
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
    ) -> Result<()> {
        context.check()?;
        self.finalize_staged_effect_bundle(&binding, manifest_command)
    }

    fn fetch_effect_bundle_manifest(
        &self,
        context: &RecorderRpcContext,
        binding: EffectBundleBinding,
    ) -> Result<Option<StoredCommand>> {
        context.check()?;
        self.fetch_effect_bundle_manifest(&binding)
    }

    fn fetch_effect_bundle_chunk(
        &self,
        context: &RecorderRpcContext,
        binding: EffectBundleBinding,
        ordinal: u16,
    ) -> Result<Option<Vec<u8>>> {
        context.check()?;
        self.fetch_effect_bundle_chunk(&binding, ordinal)
    }
}

fn proposal_ballot(proposal: &Proposal) -> Option<Ballot> {
    proposal.value.as_ref()?;
    Some(Ballot::new(
        proposal.proposal_id,
        proposal.priority.low_u128(),
        proposal.proposer_id.clone(),
    ))
}

fn record_summary(
    recorder_id: &str,
    state: &RecorderSlotState,
    decided: Option<DecisionProof>,
) -> RecordSummary {
    RecordSummary {
        recorder_id: recorder_id.to_string(),
        slot: state.slot,
        config_id: state.config_id,
        config_digest: state.config_digest,
        step: state.isr.step(),
        first_current: state.isr.first_current().cloned(),
        aggregate_prior: state.isr.aggregate_prior().cloned(),
        decided,
    }
}

fn proof_context(proof: &DecisionProof) -> (Slot, Epoch, ConfigId, LogHash) {
    match proof {
        DecisionProof::FastPath {
            slot,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | DecisionProof::Phase2 {
            slot,
            epoch,
            config_id,
            config_digest,
            ..
        } => (*slot, *epoch, *config_id, *config_digest),
    }
}

fn proof_cluster_id(proof: &DecisionProof) -> &str {
    match proof {
        DecisionProof::FastPath { cluster_id, .. } | DecisionProof::Phase2 { cluster_id, .. } => {
            cluster_id
        }
    }
}

fn certificate_from_proof(proof: &DecisionProof) -> Result<DecisionCertificate> {
    let (slot, epoch, config_id, config_digest) = proof_context(proof);
    let proposal = proof.proposal();
    let value = proposal
        .value
        .clone()
        .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
    let recorder_ids = match proof {
        DecisionProof::FastPath { summaries, .. } | DecisionProof::Phase2 { summaries, .. } => {
            summaries
                .iter()
                .map(|summary| summary.recorder_id.clone())
                .collect()
        }
    };
    Ok(DecisionCertificate {
        slot,
        epoch,
        config_id,
        config_digest,
        ballot: Ballot::new(
            proposal.proposal_id,
            proposal.priority.low_u128(),
            encode_certificate_proposer(proof_cluster_id(proof), &proposal.proposer_id),
        ),
        value,
        recorder_ids,
    })
}

const CERTIFICATE_PROPOSER_PREFIX: &str = "QDC1:";

fn encode_certificate_proposer(cluster_id: &str, proposer_id: &str) -> String {
    format!(
        "{CERTIFICATE_PROPOSER_PREFIX}{}:{cluster_id}{proposer_id}",
        cluster_id.len()
    )
}

fn decode_certificate_proposer(encoded: &str) -> Option<(&str, &str)> {
    let rest = encoded.strip_prefix(CERTIFICATE_PROPOSER_PREFIX)?;
    let (length, joined) = rest.split_once(':')?;
    let length: usize = length.parse().ok()?;
    Some((joined.get(..length)?, joined.get(length..)?))
}

impl Consensus for ThreeNodeConsensus {
    fn propose(&self, context: RecorderRpcContext, command: Command) -> Result<LogEntry> {
        self.propose_next(context, command)
    }
}

fn request_slot(request: &RecorderRequest) -> Option<Slot> {
    match request {
        RecorderRequest::Inspect { slot, .. } => Some(*slot),
        RecorderRequest::Identity
        | RecorderRequest::StoreCommand { .. }
        | RecorderRequest::FetchCommand { .. } => None,
    }
}

fn request_context(request: &RecorderRequest) -> Option<(&ClusterId, Epoch, ConfigId, LogHash)> {
    match request {
        RecorderRequest::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::FetchCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::Inspect {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        } => Some((cluster_id, *epoch, *config_id, *config_digest)),
        _ => None,
    }
}

fn stored_command(command: Command) -> Result<StoredCommand> {
    let entry_type = match command.kind() {
        CommandKind::Deterministic => EntryType::Command,
        CommandKind::ReadBarrier => EntryType::Noop,
    };
    let command = StoredCommand::new(entry_type, command.payload().to_vec());
    validate_replicated_command_size(&command)?;
    Ok(command)
}

fn encode_configuration_state(state: &ConfigurationState) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QCON");
    put_u16(&mut out, CONFIGURATION_STATE_VERSION);
    put_u64(&mut out, state.config_id);
    out.extend_from_slice(state.config_digest.as_bytes());
    out.push(u8::from(state.activated));
    match state.max_accepted_or_decided_slot {
        Some(slot) => {
            out.push(1);
            put_u64(&mut out, slot);
        }
        None => out.push(0),
    }
    match &state.membership {
        Some(membership) => {
            out.push(membership.members().len() as u8);
            for member in membership.members() {
                put_bytes(&mut out, member.as_bytes())?;
            }
        }
        None => out.push(0),
    }
    encode_optional_seal(&mut out, state.predecessor.as_ref());
    encode_optional_seal(&mut out, state.seal.as_ref());
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    Ok(out)
}

fn decode_configuration_state(bytes: &[u8]) -> Result<ConfigurationState> {
    if bytes.len() < 4 + 2 + 32 || bytes.get(..4) != Some(b"QCON") {
        return Err(Error::Decode("invalid configuration state".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("configuration digest mismatch".into()));
    }
    let mut cursor = 4;
    if read_u16(body, &mut cursor)? != CONFIGURATION_STATE_VERSION {
        return Err(noncurrent_recorder_layout_error());
    }
    let config_id = read_u64(body, &mut cursor)?;
    let config_digest = read_hash(body, &mut cursor)?;
    let activated = match read_u8(body, &mut cursor)? {
        0 => false,
        1 => true,
        _ => return Err(Error::Decode("invalid activation flag".into())),
    };
    let max_accepted_or_decided_slot = match read_u8(body, &mut cursor)? {
        0 => None,
        1 => Some(read_u64(body, &mut cursor)?),
        _ => return Err(Error::Decode("invalid accepted-slot flag".into())),
    };
    let member_count = read_u8(body, &mut cursor)? as usize;
    let membership = if member_count == 0 {
        None
    } else {
        let members = (0..member_count)
            .map(|_| {
                String::from_utf8(read_bytes(body, &mut cursor)?)
                    .map_err(|err| Error::Decode(err.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let membership = Membership::from_voters(members)?;
        if membership.digest() != config_digest {
            return Err(Error::Decode("membership digest mismatch".into()));
        }
        Some(membership)
    };
    let predecessor = decode_optional_seal(body, &mut cursor)?;
    let seal = decode_optional_seal(body, &mut cursor)?;
    if cursor != body.len() {
        return Err(Error::Decode("trailing configuration bytes".into()));
    }
    Ok(ConfigurationState {
        config_id,
        config_digest,
        membership,
        predecessor,
        seal,
        max_accepted_or_decided_slot,
        activated,
    })
}

fn encode_optional_seal(out: &mut Vec<u8>, seal: Option<&ConfigurationSeal>) {
    match seal {
        Some(seal) => {
            out.push(1);
            put_u64(out, seal.stop_slot);
            out.extend_from_slice(seal.command_hash.as_bytes());
            out.extend_from_slice(seal.prefix_hash.as_bytes());
        }
        None => out.push(0),
    }
}

fn decode_optional_seal(bytes: &[u8], cursor: &mut usize) -> Result<Option<ConfigurationSeal>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(ConfigurationSeal {
            stop_slot: read_u64(bytes, cursor)?,
            command_hash: read_hash(bytes, cursor)?,
            prefix_hash: read_hash(bytes, cursor)?,
        })),
        _ => Err(Error::Decode("invalid configuration seal flag".into())),
    }
}

fn encode_transition_intent(
    slot: Slot,
    slot_bytes: &[u8],
    configuration_bytes: &[u8],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QINT");
    put_u16(&mut out, 1);
    put_u64(&mut out, slot);
    put_bytes(&mut out, slot_bytes)?;
    put_bytes(&mut out, configuration_bytes)?;
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    Ok(out)
}

fn decode_transition_intent(bytes: &[u8]) -> Result<(Slot, Vec<u8>, Vec<u8>)> {
    if bytes.len() < 4 + 2 + 32 || bytes.get(..4) != Some(b"QINT") {
        return Err(Error::Decode("invalid transition intent".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("transition intent digest mismatch".into()));
    }
    let mut cursor = 4;
    if read_u16(body, &mut cursor)? != 1 {
        return Err(Error::Decode(
            "unsupported transition intent version".into(),
        ));
    }
    let slot = read_u64(body, &mut cursor)?;
    let slot_bytes = read_bytes(body, &mut cursor)?;
    let configuration_bytes = read_bytes(body, &mut cursor)?;
    if cursor != body.len() {
        return Err(Error::Decode("trailing transition intent bytes".into()));
    }
    Ok((slot, slot_bytes, configuration_bytes))
}

fn encode_recorder_state(state: &RecorderSlotState) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QREC");
    put_u16(&mut out, RECORDER_STATE_VERSION);
    put_u64(&mut out, state.slot);
    put_u64(&mut out, state.epoch);
    put_u64(&mut out, state.config_id);
    out.extend_from_slice(state.config_digest.as_bytes());
    put_bytes(&mut out, state.cluster_id.as_bytes())?;
    encode_optional_ballot(&mut out, state.highest_promised.as_ref())?;
    match &state.accepted {
        Some(accepted) => {
            out.push(1);
            encode_ballot(&mut out, &accepted.ballot)?;
            encode_value(&mut out, &accepted.value);
        }
        None => out.push(0),
    }
    match &state.decided {
        Some(decided) => {
            out.push(1);
            encode_certificate(&mut out, decided)?;
        }
        None => out.push(0),
    }
    put_u64(&mut out, state.isr.step);
    encode_optional_proposal(&mut out, state.isr.first_current.as_ref())?;
    encode_optional_proposal(&mut out, state.isr.aggregate_current.as_ref())?;
    encode_optional_proposal(&mut out, state.isr.aggregate_prior.as_ref())?;
    encode_optional_proof(&mut out, state.decided_proof.as_ref())?;
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    Ok(out)
}

fn decode_recorder_state(bytes: &[u8]) -> Result<RecorderSlotState> {
    if bytes.len() < 4 + 2 + 32 || &bytes[..4] != b"QREC" {
        return Err(Error::Decode("invalid recorder magic".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("recorder digest mismatch".into()));
    }
    let mut cursor = 4;
    if read_u16(body, &mut cursor)? != RECORDER_STATE_VERSION {
        return Err(noncurrent_recorder_layout_error());
    }
    let slot = read_u64(body, &mut cursor)?;
    let epoch = read_u64(body, &mut cursor)?;
    let config_id = read_u64(body, &mut cursor)?;
    let config_digest = read_hash(body, &mut cursor)?;
    let cluster_id = String::from_utf8(read_bytes(body, &mut cursor)?)
        .map_err(|err| Error::Decode(err.to_string()))?;
    let highest_promised = decode_optional_ballot(body, &mut cursor)?;
    let accepted = match read_u8(body, &mut cursor)? {
        0 => None,
        1 => Some(AcceptedSummary {
            ballot: decode_ballot(body, &mut cursor)?,
            value: decode_value(body, &mut cursor)?,
        }),
        _ => return Err(Error::Decode("invalid accepted flag".into())),
    };
    let decided = match read_u8(body, &mut cursor)? {
        0 => None,
        1 => Some(decode_certificate(body, &mut cursor)?),
        _ => return Err(Error::Decode("invalid decided flag".into())),
    };
    let isr = IsrState {
        step: read_u64(body, &mut cursor)?,
        first_current: decode_optional_proposal(body, &mut cursor)?,
        aggregate_current: decode_optional_proposal(body, &mut cursor)?,
        aggregate_prior: decode_optional_proposal(body, &mut cursor)?,
    };
    let decided_proof = decode_optional_proof(body, &mut cursor)?;
    if cursor != body.len() {
        return Err(Error::Decode("trailing recorder bytes".into()));
    }
    if let Some(accepted) = &accepted {
        if highest_promised.as_ref() < Some(&accepted.ballot) {
            return Err(Error::Decode("accepted ballot exceeds promise".into()));
        }
    }
    if let Some(decided) = &decided {
        decided
            .validate_context(slot, epoch, config_id, config_digest)
            .map_err(|_| Error::Decode("invalid decision certificate".into()))?;
    }
    if let Some(proof) = &decided_proof {
        let proof_value = proof.proposal().value.as_ref();
        if proof_context(proof) != (slot, epoch, config_id, config_digest)
            || proof_cluster_id(proof) != cluster_id
            || !matches!((decided.as_ref(), proof_value), (Some(certificate), Some(value)) if &certificate.value == value)
        {
            return Err(Error::Decode("invalid persisted decision proof".into()));
        }
    }
    Ok(RecorderSlotState {
        slot,
        cluster_id,
        epoch,
        config_id,
        config_digest,
        highest_promised,
        accepted,
        decided,
        isr,
        decided_proof,
    })
}

fn encode_optional_proposal(out: &mut Vec<u8>, proposal: Option<&Proposal>) -> Result<()> {
    match proposal {
        None => out.push(0),
        Some(proposal) => {
            out.push(1);
            out.extend_from_slice(&proposal.priority.0);
            put_bytes(out, proposal.proposer_id.as_bytes())?;
            put_u64(out, proposal.proposal_id);
            match &proposal.value {
                None => out.push(0),
                Some(value) => {
                    out.push(1);
                    encode_value(out, value);
                }
            }
        }
    }
    Ok(())
}

fn decode_optional_proposal(bytes: &[u8], cursor: &mut usize) -> Result<Option<Proposal>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => {
            let end = cursor
                .checked_add(32)
                .ok_or_else(|| Error::Decode("priority overflow".into()))?;
            let priority = ProposalPriority(
                bytes
                    .get(*cursor..end)
                    .ok_or_else(|| Error::Decode("truncated priority".into()))?
                    .try_into()
                    .expect("checked priority length"),
            );
            *cursor = end;
            let proposer_id = String::from_utf8(read_bytes(bytes, cursor)?)
                .map_err(|error| Error::Decode(error.to_string()))?;
            let proposal_id = read_u64(bytes, cursor)?;
            let value = match read_u8(bytes, cursor)? {
                0 => None,
                1 => Some(decode_value(bytes, cursor)?),
                _ => return Err(Error::Decode("invalid proposal value flag".into())),
            };
            Ok(Some(Proposal {
                priority,
                proposer_id,
                proposal_id,
                value,
            }))
        }
        _ => Err(Error::Decode("invalid proposal flag".into())),
    }
}

fn encode_summary(out: &mut Vec<u8>, summary: &RecorderSummary) -> Result<()> {
    put_bytes(out, summary.recorder_id.as_bytes())?;
    put_u64(out, summary.slot);
    put_u64(out, summary.step);
    encode_optional_proposal(out, summary.first_current.as_ref())?;
    encode_optional_proposal(out, summary.aggregate_prior.as_ref())
}

fn decode_summary(bytes: &[u8], cursor: &mut usize) -> Result<RecorderSummary> {
    Ok(RecorderSummary {
        recorder_id: String::from_utf8(read_bytes(bytes, cursor)?)
            .map_err(|error| Error::Decode(error.to_string()))?,
        slot: read_u64(bytes, cursor)?,
        step: read_u64(bytes, cursor)?,
        first_current: decode_optional_proposal(bytes, cursor)?,
        aggregate_prior: decode_optional_proposal(bytes, cursor)?,
    })
}

fn encode_optional_proof(out: &mut Vec<u8>, proof: Option<&DecisionProof>) -> Result<()> {
    let Some(proof) = proof else {
        out.push(0);
        return Ok(());
    };
    let (tag, cluster_id, slot, epoch, config_id, digest, step, proposal, summaries) = match proof {
        DecisionProof::FastPath {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            proposal,
            summaries,
        } => (
            1,
            cluster_id,
            *slot,
            *epoch,
            *config_id,
            *config_digest,
            4,
            proposal,
            summaries,
        ),
        DecisionProof::Phase2 {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            step,
            proposal,
            summaries,
        } => (
            2,
            cluster_id,
            *slot,
            *epoch,
            *config_id,
            *config_digest,
            *step,
            proposal,
            summaries,
        ),
    };
    out.push(tag);
    put_bytes(out, cluster_id.as_bytes())?;
    put_u64(out, slot);
    put_u64(out, epoch);
    put_u64(out, config_id);
    out.extend_from_slice(digest.as_bytes());
    put_u64(out, step);
    encode_optional_proposal(out, Some(proposal))?;
    put_u16(
        out,
        u16::try_from(summaries.len())
            .map_err(|_| Error::Decode("too many proof summaries".into()))?,
    );
    for summary in summaries {
        encode_summary(out, summary)?;
    }
    Ok(())
}

fn decode_optional_proof(bytes: &[u8], cursor: &mut usize) -> Result<Option<DecisionProof>> {
    let tag = read_u8(bytes, cursor)?;
    if tag == 0 {
        return Ok(None);
    }
    let cluster_id = String::from_utf8(read_bytes(bytes, cursor)?)
        .map_err(|error| Error::Decode(error.to_string()))?;
    let slot = read_u64(bytes, cursor)?;
    let epoch = read_u64(bytes, cursor)?;
    let config_id = read_u64(bytes, cursor)?;
    let config_digest = read_hash(bytes, cursor)?;
    let step = read_u64(bytes, cursor)?;
    let proposal = decode_optional_proposal(bytes, cursor)?
        .ok_or_else(|| Error::Decode("nil decision proposal".into()))?;
    let summaries = (0..read_u16(bytes, cursor)? as usize)
        .map(|_| decode_summary(bytes, cursor))
        .collect::<Result<Vec<_>>>()?;
    match tag {
        1 if step == 4 => Ok(Some(DecisionProof::FastPath {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            proposal,
            summaries,
        })),
        2 => Ok(Some(DecisionProof::Phase2 {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            step,
            proposal,
            summaries,
        })),
        _ => Err(Error::Decode("invalid decision proof tag".into())),
    }
}

fn encode_optional_ballot(out: &mut Vec<u8>, ballot: Option<&Ballot>) -> Result<()> {
    match ballot {
        Some(ballot) => {
            out.push(1);
            encode_ballot(out, ballot)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn decode_optional_ballot(bytes: &[u8], cursor: &mut usize) -> Result<Option<Ballot>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(decode_ballot(bytes, cursor)?)),
        _ => Err(Error::Decode("invalid ballot flag".into())),
    }
}

fn encode_ballot(out: &mut Vec<u8>, ballot: &Ballot) -> Result<()> {
    put_u64(out, ballot.round);
    put_u128(out, ballot.priority);
    put_bytes(out, ballot.proposer_id.as_bytes())
}

fn decode_ballot(bytes: &[u8], cursor: &mut usize) -> Result<Ballot> {
    Ok(Ballot {
        round: read_u64(bytes, cursor)?,
        priority: read_u128(bytes, cursor)?,
        proposer_id: String::from_utf8(read_bytes(bytes, cursor)?)
            .map_err(|err| Error::Decode(err.to_string()))?,
    })
}

fn encode_value(out: &mut Vec<u8>, value: &AcceptedValue) {
    out.extend_from_slice(value.command_hash.as_bytes());
    out.extend_from_slice(value.prev_hash.as_bytes());
    out.extend_from_slice(value.entry_hash.as_bytes());
}

fn decode_value(bytes: &[u8], cursor: &mut usize) -> Result<AcceptedValue> {
    Ok(AcceptedValue {
        command_hash: read_hash(bytes, cursor)?,
        prev_hash: read_hash(bytes, cursor)?,
        entry_hash: read_hash(bytes, cursor)?,
    })
}

fn encode_certificate(out: &mut Vec<u8>, decision: &DecisionCertificate) -> Result<()> {
    put_u64(out, decision.slot);
    put_u64(out, decision.epoch);
    put_u64(out, decision.config_id);
    out.extend_from_slice(decision.config_digest.as_bytes());
    encode_ballot(out, &decision.ballot)?;
    encode_value(out, &decision.value);
    put_u16(
        out,
        u16::try_from(decision.recorder_ids.len())
            .map_err(|_| Error::Decode("too many certificate recorders".into()))?,
    );
    for recorder_id in &decision.recorder_ids {
        put_bytes(out, recorder_id.as_bytes())?;
    }
    Ok(())
}

fn decode_certificate(bytes: &[u8], cursor: &mut usize) -> Result<DecisionCertificate> {
    let slot = read_u64(bytes, cursor)?;
    let epoch = read_u64(bytes, cursor)?;
    let config_id = read_u64(bytes, cursor)?;
    let config_digest = read_hash(bytes, cursor)?;
    let ballot = decode_ballot(bytes, cursor)?;
    let value = decode_value(bytes, cursor)?;
    let recorder_count = read_u16(bytes, cursor)? as usize;
    let recorder_ids = (0..recorder_count)
        .map(|_| {
            String::from_utf8(read_bytes(bytes, cursor)?)
                .map_err(|err| Error::Decode(err.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DecisionCertificate {
        slot,
        epoch,
        config_id,
        config_digest,
        ballot,
        value,
        recorder_ids,
    })
}

fn encode_stored_command(command: &StoredCommand) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QCMD");
    put_u16(&mut out, 1);
    out.push(command.entry_type.as_u8());
    put_u64(&mut out, command.payload.len() as u64);
    out.extend_from_slice(&command.payload);
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    out
}

fn decode_stored_command(bytes: &[u8]) -> Result<StoredCommand> {
    if bytes.len() < 4 + 2 + 1 + 8 + 32 || &bytes[..4] != b"QCMD" {
        return Err(Error::Decode("invalid command magic".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("command digest mismatch".into()));
    }
    let mut cursor = 4;
    if read_u16(body, &mut cursor)? != 1 {
        return Err(Error::Decode("unsupported command version".into()));
    }
    let entry_type = EntryType::from_u8(read_u8(body, &mut cursor)?)
        .ok_or_else(|| Error::Decode("invalid command entry type".into()))?;
    let payload_len = usize::try_from(read_u64(body, &mut cursor)?)
        .map_err(|_| Error::Decode("command payload too large".into()))?;
    let end = cursor
        .checked_add(payload_len)
        .ok_or_else(|| Error::Decode("command payload length overflow".into()))?;
    let payload = body
        .get(cursor..end)
        .ok_or_else(|| Error::Decode("short command payload".into()))?
        .to_vec();
    validate_replicated_command_size(&StoredCommand::new(entry_type, payload.clone()))?;
    cursor = end;
    if cursor != body.len() {
        return Err(Error::Decode("trailing command bytes".into()));
    }
    Ok(StoredCommand::new(entry_type, payload))
}

#[cfg(test)]
std::thread_local! {
    static SYNC_COUNTS: std::cell::Cell<(usize, usize)> = const {
        std::cell::Cell::new((0, 0))
    };
    static LAST_FILE_SYNC_KIND: std::cell::Cell<Option<FileSyncKind>> = const {
        std::cell::Cell::new(None)
    };
    static COMMAND_FILE_READS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileSyncKind {
    #[cfg(target_os = "linux")]
    Data,
    All,
}

#[cfg(target_os = "linux")]
fn sync_wal_append(file: &fs::File) -> io::Result<()> {
    // Linux fdatasync (File::sync_data) also flushes metadata required for later data retrieval,
    // including the file size extended by this append, so a complete WAL frame remains replayable.
    file.sync_data()?;
    #[cfg(test)]
    record_file_sync_kind(FileSyncKind::Data);
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn sync_wal_append(file: &fs::File) -> io::Result<()> {
    // Keep non-Linux durability conservative because sync_data semantics vary by platform.
    file.sync_all()?;
    #[cfg(test)]
    record_file_sync_kind(FileSyncKind::All);
    Ok(())
}

#[cfg(test)]
fn sync_wal_metadata(file: &fs::File) -> io::Result<()> {
    file.sync_all()?;
    #[cfg(test)]
    record_file_sync_kind(FileSyncKind::All);
    Ok(())
}

#[cfg(test)]
fn record_file_sync() {
    SYNC_COUNTS.with(|counts| {
        let (file, directory) = counts.get();
        counts.set((file + 1, directory));
    });
}

#[cfg(test)]
fn record_file_sync_kind(kind: FileSyncKind) {
    record_file_sync();
    LAST_FILE_SYNC_KIND.with(|last| last.set(Some(kind)));
}

#[cfg(test)]
fn record_directory_sync() {
    SYNC_COUNTS.with(|counts| {
        let (file, directory) = counts.get();
        counts.set((file, directory + 1));
    });
}

#[cfg(test)]
fn reset_sync_counts() {
    SYNC_COUNTS.with(|counts| counts.set((0, 0)));
    LAST_FILE_SYNC_KIND.with(|last| last.set(None));
}

#[cfg(test)]
fn sync_counts() -> (usize, usize) {
    SYNC_COUNTS.with(std::cell::Cell::get)
}

#[cfg(test)]
fn last_file_sync_kind() -> Option<FileSyncKind> {
    LAST_FILE_SYNC_KIND.with(std::cell::Cell::get)
}

#[cfg(test)]
fn reset_command_file_reads() {
    COMMAND_FILE_READS.with(|reads| reads.set(0));
}

#[cfg(test)]
fn command_file_reads() -> usize {
    COMMAND_FILE_READS.with(std::cell::Cell::get)
}

const CONFIGURATION_HEAD_INTENT_MAGIC: &[u8; 4] = b"QCHI";

fn encode_configuration_head_intent(configuration: &[u8], head: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(CONFIGURATION_HEAD_INTENT_MAGIC);
    put_u16(&mut encoded, 1);
    put_u64(&mut encoded, configuration.len() as u64);
    encoded.extend_from_slice(configuration);
    put_u64(&mut encoded, head.len() as u64);
    encoded.extend_from_slice(head);
    encoded
}

fn decode_configuration_head_intent(bytes: &[u8]) -> Result<(&[u8], &[u8])> {
    let mut cursor = 0;
    if bytes.get(..CONFIGURATION_HEAD_INTENT_MAGIC.len()) != Some(CONFIGURATION_HEAD_INTENT_MAGIC) {
        return Err(Error::Decode(
            "invalid configuration-head intent magic".into(),
        ));
    }
    cursor += CONFIGURATION_HEAD_INTENT_MAGIC.len();
    if read_u16(bytes, &mut cursor)? != 1 {
        return Err(Error::Decode(
            "unsupported configuration-head intent version".into(),
        ));
    }
    let configuration_len = usize::try_from(read_u64(bytes, &mut cursor)?)
        .map_err(|_| Error::Decode("configuration-head intent length overflow".into()))?;
    let configuration_end = cursor
        .checked_add(configuration_len)
        .ok_or_else(|| Error::Decode("configuration-head intent length overflow".into()))?;
    let configuration = bytes
        .get(cursor..configuration_end)
        .ok_or_else(|| Error::Decode("truncated configuration-head intent".into()))?;
    cursor = configuration_end;
    let head_len = usize::try_from(read_u64(bytes, &mut cursor)?)
        .map_err(|_| Error::Decode("configuration-head intent length overflow".into()))?;
    let head_end = cursor
        .checked_add(head_len)
        .ok_or_else(|| Error::Decode("configuration-head intent length overflow".into()))?;
    let head = bytes
        .get(cursor..head_end)
        .ok_or_else(|| Error::Decode("truncated configuration-head intent".into()))?;
    if head_end != bytes.len() {
        return Err(Error::Decode(
            "trailing configuration-head intent bytes".into(),
        ));
    }
    Ok((configuration, head))
}

fn encode_wal_frame(
    generation: u64,
    sequence: u64,
    prev_digest: LogHash,
    slot_state: &RecorderSlotState,
    configuration: &ConfigurationState,
    head: &RecordedHeadProvenance,
    command: Option<(LogHash, &StoredCommand)>,
) -> Result<(Vec<u8>, LogHash, Vec<u8>)> {
    let slot_bytes = encode_recorder_state(slot_state)?;
    let configuration_bytes = encode_configuration_state(configuration)?;
    let mut payload = Vec::new();
    put_u64(&mut payload, generation);
    put_u64(&mut payload, sequence);
    payload.extend_from_slice(prev_digest.as_bytes());
    put_u64(&mut payload, slot_state.slot());
    put_blob(&mut payload, &slot_bytes)?;
    put_blob(&mut payload, &configuration_bytes)?;
    encode_head_provenance(&mut payload, head);
    match command {
        Some((hash, command)) => {
            payload.push(1);
            payload.extend_from_slice(hash.as_bytes());
            put_blob(&mut payload, &encode_stored_command(command))?;
        }
        None => payload.push(0),
    }
    let total_len = 4usize
        .checked_add(2)
        .and_then(|len| len.checked_add(8))
        .and_then(|len| len.checked_add(payload.len()))
        .and_then(|len| len.checked_add(32))
        .ok_or_else(|| Error::Io("recorder WAL frame length overflow".into()))?;
    let mut frame = Vec::with_capacity(total_len);
    frame.extend_from_slice(RECORDER_WAL_MAGIC);
    put_u16(&mut frame, RECORDER_WAL_VERSION);
    put_u64(&mut frame, total_len as u64);
    frame.extend_from_slice(&payload);
    let digest = LogHash::digest(&[&frame]);
    frame.extend_from_slice(digest.as_bytes());
    Ok((frame, digest, slot_bytes))
}

fn decode_wal_frame(bytes: &[u8], offset: usize) -> Result<Option<(WalFrame, usize)>> {
    const PREFIX_LEN: usize = 4 + 2 + 8;
    let remaining = bytes
        .get(offset..)
        .ok_or_else(|| Error::Decode("recorder WAL offset overflow".into()))?;
    if remaining.len() < PREFIX_LEN {
        return Ok(None);
    }
    if remaining.get(..4) != Some(RECORDER_WAL_MAGIC) {
        return Err(Error::Decode("recorder WAL frame magic mismatch".into()));
    }
    let mut cursor = offset + 4;
    if read_u16(bytes, &mut cursor)? != RECORDER_WAL_VERSION {
        return Err(Error::Decode("recorder WAL frame version mismatch".into()));
    }
    let frame_len = usize::try_from(read_u64(bytes, &mut cursor)?)
        .map_err(|_| Error::Decode("recorder WAL frame length overflow".into()))?;
    if frame_len < PREFIX_LEN + 32 {
        return Err(Error::Decode("recorder WAL frame length is invalid".into()));
    }
    let end = offset
        .checked_add(frame_len)
        .ok_or_else(|| Error::Decode("recorder WAL frame length overflow".into()))?;
    if end > bytes.len() {
        return Ok(None);
    }
    let digest_offset = end - 32;
    let digest = read_hash(bytes, &mut { digest_offset })?;
    let expected = LogHash::digest(&[&bytes[offset..digest_offset]]);
    if digest != expected {
        return Err(Error::Decode("recorder WAL frame checksum mismatch".into()));
    }
    let generation = read_u64(bytes, &mut cursor)?;
    let sequence = read_u64(bytes, &mut cursor)?;
    let prev_digest = read_hash(bytes, &mut cursor)?;
    let slot = read_u64(bytes, &mut cursor)?;
    let slot_bytes = read_blob(bytes, &mut cursor)?;
    let configuration_bytes = read_blob(bytes, &mut cursor)?;
    let head = decode_head_provenance(bytes, &mut cursor)?;
    let command = match read_u8(bytes, &mut cursor)? {
        0 => None,
        1 => {
            let hash = read_hash(bytes, &mut cursor)?;
            let command = decode_stored_command(&read_blob(bytes, &mut cursor)?)?;
            Some((hash, command))
        }
        _ => return Err(Error::Decode("recorder WAL command flag is invalid".into())),
    };
    if cursor != digest_offset {
        return Err(Error::Decode(
            "recorder WAL frame has trailing bytes".into(),
        ));
    }
    Ok(Some((
        WalFrame {
            generation,
            sequence,
            prev_digest,
            digest,
            slot,
            slot_bytes,
            configuration_bytes,
            head,
            command,
        },
        end,
    )))
}

fn read_regular_file_bounded(path: &Path, maximum: usize, name: &str) -> Result<Vec<u8>> {
    let before = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(Error::Decode(format!("recorder is missing {name}")));
        }
        Err(error) => return Err(Error::Io(error.to_string())),
    };
    if before.file_type().is_symlink() || !before.is_file() || before.len() > maximum as u64 {
        return Err(Error::Decode(format!(
            "recorder {name} must be a bounded regular file"
        )));
    }
    let mut file = open_regular_file_no_follow(path)?;
    let opened = file
        .metadata()
        .map_err(|error| Error::Io(error.to_string()))?;
    if !opened.is_file()
        || !same_opened_file(&before, &opened)
        || opened.len() != before.len()
        || opened.len() > maximum as u64
    {
        return Err(Error::Decode(format!(
            "recorder {name} changed before no-follow open"
        )));
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    Read::by_ref(&mut file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| Error::Io(error.to_string()))?;
    let after_opened = file
        .metadata()
        .map_err(|error| Error::Io(error.to_string()))?;
    let after_path = fs::symlink_metadata(path).map_err(|error| Error::Io(error.to_string()))?;
    if after_path.file_type().is_symlink()
        || !after_path.is_file()
        || !after_opened.is_file()
        || !same_opened_file(&opened, &after_opened)
        || !same_opened_file(&opened, &after_path)
        || after_opened.len() != opened.len()
        || after_path.len() != opened.len()
    {
        return Err(Error::Decode(format!(
            "recorder {name} changed during bounded read"
        )));
    }
    if bytes.len() > maximum || bytes.len() as u64 != opened.len() {
        return Err(Error::Decode(format!(
            "recorder {name} exceeds its size limit"
        )));
    }
    Ok(bytes)
}

fn open_regular_file_no_follow(path: &Path) -> Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    options.custom_flags(O_NOFOLLOW_FLAG);
    options
        .open(path)
        .map_err(|error| Error::Io(error.to_string()))
}

fn prepare_fresh_recorder_root(root: &Path) -> Result<anchored_fs::AnchoredDir> {
    if let Some(parent) = root
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_real_directory_if_missing(parent)?;
    }
    match fs::create_dir(root) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(Error::Io(error.to_string())),
    }
    anchored_fs::AnchoredDir::open(root)
}

/// Clean-install-only recorder generation gate. A nonempty root without this
/// exact immutable marker is legacy or partially initialized and must be
/// reset by the operator before any lock, WAL, or effect-store mutation.
fn ensure_storage_generation(root: &anchored_fs::AnchoredDir) -> Result<()> {
    match root.read_optional(
        STORAGE_GENERATION_FILE,
        STORAGE_GENERATION_FINGERPRINT.len(),
        STORAGE_GENERATION_FILE,
    )? {
        Some(bytes) if bytes == STORAGE_GENERATION_FINGERPRINT => return Ok(()),
        Some(_) => {
            return Err(Error::Decode(
                "recorder storage generation mismatch; reset required".into(),
            ))
        }
        None => {}
    }
    let has_durable = root
        .list()?
        .into_iter()
        .any(|name| name != ".recorder.lock");
    if has_durable {
        return Err(Error::Decode(
            "recorder storage generation missing; reset required".into(),
        ));
    }
    root.atomic_write(STORAGE_GENERATION_FILE, STORAGE_GENERATION_FINGERPRINT)
}

fn create_real_directory_if_missing(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => return ensure_real_directory(path, "recorder root parent"),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(Error::Io(error.to_string())),
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_real_directory_if_missing(parent)?;
    }
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(Error::Io(error.to_string())),
    }
    ensure_real_directory(path, "recorder root parent")
}

fn ensure_real_directory(path: &Path, name: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| Error::Io(error.to_string()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::Decode(format!("{name} must be a real directory")));
    }
    Ok(())
}

#[cfg(unix)]
fn same_opened_file(opened: &fs::Metadata, linked: &fs::Metadata) -> bool {
    opened.dev() == linked.dev() && opened.ino() == linked.ino()
}

#[cfg(not(unix))]
fn same_opened_file(opened: &fs::Metadata, linked: &fs::Metadata) -> bool {
    opened.file_type() == linked.file_type()
        && opened.len() == linked.len()
        && opened.modified().ok() == linked.modified().ok()
}

fn current_recorder_layout(root: &Path) -> Result<bool> {
    let metadata = match fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(Error::Io(error.to_string())),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::Decode(
            "recorder root must be a real directory".into(),
        ));
    }
    let entries = fs::read_dir(root)
        .map_err(|error| Error::Io(error.to_string()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::Io(error.to_string()))?;
    if entries.is_empty()
        || entries.iter().all(|entry| {
            entry.file_name() == ".recorder.lock" || entry.file_name() == STORAGE_GENERATION_FILE
        })
    {
        return Ok(false);
    }
    validate_current_recorder_layout(root)?;
    Ok(true)
}

fn noncurrent_recorder_layout_error() -> Error {
    Error::Decode("recorder directory is not a current durable layout".into())
}

fn validate_current_recorder_layout(root: &Path) -> Result<()> {
    for name in [
        ".recorder.lock",
        "configuration.rec",
        "recorded-head.rec",
        "recorder.wal",
    ] {
        let metadata = fs::symlink_metadata(root.join(name)).ok();
        if !metadata
            .is_some_and(|metadata| !metadata.file_type().is_symlink() && metadata.is_file())
        {
            return Err(noncurrent_recorder_layout_error());
        }
    }
    Ok(())
}

fn read_preflight_intent(root: &Path, name: &str, maximum: usize) -> Result<Option<Vec<u8>>> {
    let path = root.join(name);
    match read_regular_file_bounded(&path, maximum, name) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(Error::Decode(message)) if message == format!("recorder is missing {name}") => Ok(None),
        Err(error) => Err(error),
    }
}

fn validate_preflight_configuration(
    configuration: &ConfigurationState,
    config_id: ConfigId,
    membership: &Membership,
) -> Result<()> {
    if configuration.config_id != config_id
        || configuration.config_digest != membership.digest()
        || configuration.membership.as_ref() != Some(membership)
    {
        return Err(Error::Decode(
            "recorder configuration does not exactly match expected membership".into(),
        ));
    }
    Ok(())
}

fn validate_existing_snapshots(
    root: &Path,
    snapshots: &[DurableSlotSnapshot],
    cluster_id: &str,
    epoch: Epoch,
    configuration: &ConfigurationState,
) -> Result<()> {
    let no_inline_commands = HashMap::new();
    for snapshot in snapshots {
        let state = decode_recorder_state(&snapshot.bytes)?;
        if state.slot() != snapshot.slot
            || state.cluster_id != cluster_id
            || state.epoch != epoch
            || state.config_id != configuration.config_id
            || state.config_digest != configuration.config_digest
        {
            return Err(Error::Decode(
                "durable recorder snapshot identity mismatch".into(),
            ));
        }
        for value in recorder_state_values(&state) {
            validate_existing_value(
                root,
                cluster_id,
                snapshot.slot,
                epoch,
                configuration.config_id,
                value,
                &no_inline_commands,
            )?;
        }
    }
    Ok(())
}

fn validate_recoverable_transition_intent(
    root: &Path,
    bytes: &[u8],
    cluster_id: &str,
    epoch: Epoch,
    config_id: ConfigId,
    membership: &Membership,
) -> Result<()> {
    let (slot, slot_bytes, configuration_bytes) = decode_transition_intent(bytes)?;
    let configuration = decode_configuration_state(&configuration_bytes)?;
    validate_preflight_configuration(&configuration, config_id, membership)?;
    let state = decode_recorder_state(&slot_bytes)?;
    if state.slot() != slot
        || state.cluster_id != cluster_id
        || state.epoch != epoch
        || state.config_id != configuration.config_id
        || state.config_digest != configuration.config_digest
    {
        return Err(Error::Decode(
            "recorder transition intent state identity mismatch".into(),
        ));
    }
    let no_inline_commands = HashMap::new();
    for value in recorder_state_values(&state) {
        validate_existing_value(
            root,
            cluster_id,
            slot,
            epoch,
            configuration.config_id,
            value,
            &no_inline_commands,
        )?;
    }
    Ok(())
}

fn validate_recoverable_configuration_head_intent(
    root: &Path,
    bytes: &[u8],
    cluster_id: &str,
    epoch: Epoch,
    config_id: ConfigId,
    membership: &Membership,
) -> Result<()> {
    let (configuration_bytes, head_bytes) = decode_configuration_head_intent(bytes)?;
    let configuration = decode_configuration_state(configuration_bytes)?;
    validate_preflight_configuration(&configuration, config_id, membership)?;
    let (_, snapshots, _) = decode_recorded_head(head_bytes, cluster_id, epoch, &configuration)?;
    validate_existing_snapshots(root, &snapshots, cluster_id, epoch, &configuration)
}

fn validate_empty_recovery_wal(root: &Path) -> Result<()> {
    let path = root.join("recorder.wal");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Error::Io(error.to_string())),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::Decode(
            "recorder WAL must be a regular file during recovery".into(),
        ));
    }
    if metadata.len() != 0 {
        return Err(Error::Decode(
            "recorder recovery intent requires an empty WAL".into(),
        ));
    }
    Ok(())
}

fn validate_existing_wal(
    root: &Path,
    bytes: &[u8],
    cluster_id: &str,
    epoch: Epoch,
    initial_configuration: &ConfigurationState,
    initial_head: &RecordedHeadProvenance,
    checkpoint: WalCheckpoint,
) -> Result<bool> {
    let mut commands = HashMap::new();
    let mut next_sequence = checkpoint
        .through_sequence
        .checked_add(1)
        .ok_or_else(|| Error::Decode("recorder WAL sequence exhausted".into()))?;
    let mut last_digest = LogHash::ZERO;
    let mut configuration = initial_configuration.clone();
    let mut head = initial_head.clone();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let Some((frame, end)) = decode_wal_frame(bytes, offset)? else {
            return Ok(true);
        };
        if frame.generation < checkpoint.generation {
            offset = end;
            continue;
        }
        if frame.generation != checkpoint.generation
            || frame.sequence != next_sequence
            || frame.prev_digest != last_digest
        {
            return Err(Error::Decode(
                "recorder WAL sequence or digest chain mismatch".into(),
            ));
        }
        let state = decode_recorder_state(&frame.slot_bytes)?;
        let next_configuration = decode_configuration_state(&frame.configuration_bytes)?;
        if state.slot() != frame.slot
            || state.cluster_id != cluster_id
            || state.epoch != epoch
            || state.config_id != next_configuration.config_id
            || state.config_digest != next_configuration.config_digest
            || configuration_structure_changed(&configuration, &next_configuration)
        {
            return Err(Error::Decode("recorder WAL state identity mismatch".into()));
        }
        if let Some((hash, command)) = &frame.command {
            if command.hash() != *hash {
                return Err(Error::Decode(
                    "recorder WAL inline command hash mismatch".into(),
                ));
            }
            upsert_wal_command(&mut commands, *hash, command)?;
        }
        for value in recorder_state_values(&state) {
            validate_existing_value(
                root,
                cluster_id,
                frame.slot,
                epoch,
                next_configuration.config_id,
                value,
                &commands,
            )?;
        }
        let expected_head = if next_configuration.max_accepted_or_decided_slot == Some(frame.slot)
            && recorder_state_values(&state).next().is_some()
        {
            RecordedHeadProvenance::SlotBacked { slot: frame.slot }
        } else {
            head.clone()
        };
        if frame.head != expected_head {
            return Err(Error::Decode("recorder WAL head mismatch".into()));
        }
        next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Decode("recorder WAL sequence exhausted".into()))?;
        last_digest = frame.digest;
        configuration = next_configuration;
        head = frame.head;
        offset = end;
    }
    Ok(false)
}

fn validate_existing_value(
    root: &Path,
    cluster_id: &str,
    slot: Slot,
    epoch: Epoch,
    config_id: ConfigId,
    value: &AcceptedValue,
    inline_commands: &HashMap<LogHash, StoredCommand>,
) -> Result<()> {
    let cached_command;
    let command = match inline_commands.get(&value.command_hash) {
        Some(command) => command,
        None => {
            cached_command = read_existing_command_cache(root, value.command_hash)?;
            &cached_command
        }
    };
    if AcceptedValue::from_command(cluster_id, slot, epoch, config_id, value.prev_hash, command)
        != *value
    {
        return Err(Error::Decode("recorder WAL value mismatch".into()));
    }
    Ok(())
}

fn read_existing_command_cache(root: &Path, command_hash: LogHash) -> Result<StoredCommand> {
    let path = root.join(format!("command-{}.cmd", command_hash.to_hex()));
    let bytes =
        match read_regular_file_bounded(&path, MAX_COMMAND_CACHE_BYTES, "command cache entry") {
            Err(Error::Decode(message)) if message == "recorder is missing command cache entry" => {
                return Err(Error::CommandUnavailable);
            }
            result => result?,
        };
    let command = decode_stored_command(&bytes)?;
    if command.hash() != command_hash {
        return Err(Error::CommandHashMismatch);
    }
    Ok(command)
}

fn encode_head_provenance(out: &mut Vec<u8>, head: &RecordedHeadProvenance) {
    match head {
        RecordedHeadProvenance::Empty => out.push(0),
        RecordedHeadProvenance::SlotBacked { slot } => {
            out.push(1);
            put_u64(out, *slot);
        }
        RecordedHeadProvenance::CheckpointBacked {
            stop_slot,
            prefix_hash,
            recovered_tip,
            recovered_hash,
        } => {
            out.push(2);
            put_u64(out, *stop_slot);
            out.extend_from_slice(prefix_hash.as_bytes());
            put_u64(out, *recovered_tip);
            out.extend_from_slice(recovered_hash.as_bytes());
        }
    }
}

fn decode_head_provenance(bytes: &[u8], cursor: &mut usize) -> Result<RecordedHeadProvenance> {
    match read_u8(bytes, cursor)? {
        0 => Ok(RecordedHeadProvenance::Empty),
        1 => Ok(RecordedHeadProvenance::SlotBacked {
            slot: read_u64(bytes, cursor)?,
        }),
        2 => Ok(RecordedHeadProvenance::CheckpointBacked {
            stop_slot: read_u64(bytes, cursor)?,
            prefix_hash: read_hash(bytes, cursor)?,
            recovered_tip: read_u64(bytes, cursor)?,
            recovered_hash: read_hash(bytes, cursor)?,
        }),
        _ => Err(Error::Decode(
            "recorder WAL head provenance is invalid".into(),
        )),
    }
}

fn upsert_wal_command(
    commands: &mut HashMap<LogHash, StoredCommand>,
    hash: LogHash,
    command: &StoredCommand,
) -> Result<()> {
    match commands.entry(hash) {
        hash_map::Entry::Occupied(existing) if existing.get() != command => {
            Err(Error::CommandHashMismatch)
        }
        hash_map::Entry::Occupied(_) => Ok(()),
        hash_map::Entry::Vacant(vacant) => {
            vacant.insert(command.clone());
            Ok(())
        }
    }
}

fn encode_recorded_head(
    cluster_id: &str,
    epoch: Epoch,
    configuration: &ConfigurationState,
    provenance: &RecordedHeadProvenance,
    recent_slots: &[DurableSlotSnapshot],
    wal_checkpoint: WalCheckpoint,
) -> Result<Vec<u8>> {
    if recent_slots.len() > 2 {
        return Err(Error::Io(
            "recorder manifest can retain at most two slot snapshots".into(),
        ));
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(RECORDED_HEAD_MAGIC);
    put_u16(&mut encoded, RECORDED_HEAD_VERSION);
    put_bytes(&mut encoded, cluster_id.as_bytes())?;
    put_u64(&mut encoded, epoch);
    put_u64(&mut encoded, configuration.config_id);
    encoded.extend_from_slice(configuration.config_digest.as_bytes());
    match provenance {
        RecordedHeadProvenance::Empty => encoded.push(0),
        RecordedHeadProvenance::SlotBacked { slot } => {
            encoded.push(1);
            put_u64(&mut encoded, *slot);
        }
        RecordedHeadProvenance::CheckpointBacked {
            stop_slot,
            prefix_hash,
            recovered_tip,
            recovered_hash,
        } => {
            encoded.push(2);
            put_u64(&mut encoded, *stop_slot);
            encoded.extend_from_slice(prefix_hash.as_bytes());
            put_u64(&mut encoded, *recovered_tip);
            encoded.extend_from_slice(recovered_hash.as_bytes());
        }
    }
    put_u64(&mut encoded, wal_checkpoint.generation);
    put_u64(&mut encoded, wal_checkpoint.through_sequence);
    put_u16(&mut encoded, recent_slots.len() as u16);
    for snapshot in recent_slots {
        put_u64(&mut encoded, snapshot.slot);
        put_bytes(&mut encoded, &snapshot.bytes)?;
    }
    let digest = LogHash::digest(&[&encoded]);
    encoded.extend_from_slice(digest.as_bytes());
    Ok(encoded)
}

fn decode_recorded_head(
    bytes: &[u8],
    expected_cluster_id: &str,
    expected_epoch: Epoch,
    configuration: &ConfigurationState,
) -> Result<(
    RecordedHeadProvenance,
    Vec<DurableSlotSnapshot>,
    WalCheckpoint,
)> {
    if bytes.get(..RECORDED_HEAD_MAGIC.len()) != Some(RECORDED_HEAD_MAGIC) {
        return Err(Error::Decode("invalid recorder durable head magic".into()));
    }
    let mut version_cursor = RECORDED_HEAD_MAGIC.len();
    if read_u16(bytes, &mut version_cursor)? != RECORDED_HEAD_VERSION {
        return Err(noncurrent_recorder_layout_error());
    }
    if bytes.len() < 32 {
        return Err(Error::Decode("truncated recorder durable head".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode(
            "recorder durable head digest mismatch".into(),
        ));
    }
    let mut cursor = 0;
    cursor += RECORDED_HEAD_MAGIC.len();
    let _version = read_u16(body, &mut cursor)?;
    let cluster_id = String::from_utf8(read_bytes(body, &mut cursor)?)
        .map_err(|error| Error::Decode(error.to_string()))?;
    let epoch = read_u64(body, &mut cursor)?;
    let config_id = read_u64(body, &mut cursor)?;
    let config_digest = read_hash(body, &mut cursor)?;
    if cluster_id != expected_cluster_id
        || epoch != expected_epoch
        || config_id != configuration.config_id
        || config_digest != configuration.config_digest
    {
        return Err(Error::Decode(
            "recorder durable head identity mismatch".into(),
        ));
    }
    let provenance = match read_u8(body, &mut cursor)? {
        0 => RecordedHeadProvenance::Empty,
        1 => RecordedHeadProvenance::SlotBacked {
            slot: read_u64(body, &mut cursor)?,
        },
        2 => RecordedHeadProvenance::CheckpointBacked {
            stop_slot: read_u64(body, &mut cursor)?,
            prefix_hash: read_hash(body, &mut cursor)?,
            recovered_tip: read_u64(body, &mut cursor)?,
            recovered_hash: read_hash(body, &mut cursor)?,
        },
        value => {
            return Err(Error::Decode(format!(
                "invalid recorder durable head provenance {value}"
            )));
        }
    };
    let wal_checkpoint = WalCheckpoint {
        generation: read_u64(body, &mut cursor)?,
        through_sequence: read_u64(body, &mut cursor)?,
    };
    if wal_checkpoint.generation == 0 {
        return Err(Error::Decode(
            "recorder durable head has zero WAL generation".into(),
        ));
    }
    let recent_count = usize::from(read_u16(body, &mut cursor)?);
    if recent_count > 2 {
        return Err(Error::Decode(
            "recorder manifest contains too many slot snapshots".into(),
        ));
    }
    let mut recent_slots = Vec::with_capacity(recent_count);
    for _ in 0..recent_count {
        let slot = read_u64(body, &mut cursor)?;
        if recent_slots
            .iter()
            .any(|snapshot: &DurableSlotSnapshot| snapshot.slot == slot)
        {
            return Err(Error::Decode(
                "recorder manifest contains duplicate slot snapshots".into(),
            ));
        }
        recent_slots.push(DurableSlotSnapshot {
            slot,
            bytes: read_bytes(body, &mut cursor)?,
        });
    }
    if cursor != body.len() {
        return Err(Error::Decode("trailing recorder durable head bytes".into()));
    }
    Ok((provenance, recent_slots, wal_checkpoint))
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u128(out: &mut Vec<u8>, value: u128) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u16::try_from(value.len())
        .map_err(|_| Error::Decode("recorder string is too long".into()))?;
    put_u16(out, len);
    out.extend_from_slice(value);
    Ok(())
}

fn put_blob(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u64::try_from(value.len())
        .map_err(|_| Error::Decode("recorder blob is too long".into()))?;
    put_u64(out, len);
    out.extend_from_slice(value);
    Ok(())
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    let value = *bytes
        .get(*cursor)
        .ok_or_else(|| Error::Decode("short recorder u8".into()))?;
    *cursor += 1;
    Ok(value)
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
    let end = cursor
        .checked_add(2)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder u16".into()))?;
    *cursor = end;
    Ok(u16::from_be_bytes(slice.try_into().expect("u16 slice")))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder u64".into()))?;
    *cursor = end;
    Ok(u64::from_be_bytes(slice.try_into().expect("u64 slice")))
}

fn read_u128(bytes: &[u8], cursor: &mut usize) -> Result<u128> {
    let end = cursor
        .checked_add(16)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder u128".into()))?;
    *cursor = end;
    Ok(u128::from_be_bytes(slice.try_into().expect("u128 slice")))
}

fn read_hash(bytes: &[u8], cursor: &mut usize) -> Result<LogHash> {
    let end = cursor
        .checked_add(32)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder hash".into()))?;
    *cursor = end;
    let mut out = [0; 32];
    out.copy_from_slice(slice);
    Ok(LogHash::from_bytes(out))
}

fn read_bytes(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = read_u16(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder bytes".into()))?;
    *cursor = end;
    Ok(slice.to_vec())
}

fn read_blob(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = usize::try_from(read_u64(bytes, cursor)?)
        .map_err(|_| Error::Decode("recorder blob length overflow".into()))?;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::Decode("recorder blob length overflow".into()))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder blob".into()))?
        .to_vec();
    *cursor = end;
    Ok(value)
}

#[derive(Debug)]
pub struct SingleNodeConsensus {
    cluster_id: String,
    epoch: Epoch,
    config_id: ConfigId,
    state: Mutex<SingleNodeState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SingleNodeState {
    next_index: LogIndex,
    last_hash: LogHash,
}

impl SingleNodeConsensus {
    pub fn new(cluster_id: impl Into<String>, epoch: Epoch, config_id: ConfigId) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            state: Mutex::new(SingleNodeState {
                next_index: 1,
                last_hash: LogHash::ZERO,
            }),
        }
    }
}

impl Consensus for SingleNodeConsensus {
    fn propose(&self, context: RecorderRpcContext, command: Command) -> Result<LogEntry> {
        context.check()?;
        let mut state = self.state.lock().map_err(|_| Error::ProposeFailed)?;
        let command = stored_command(command)?;
        let prev_hash = state.last_hash;
        let hash = LogEntry::calculate_hash(
            &self.cluster_id,
            state.next_index,
            self.epoch,
            self.config_id,
            command.entry_type,
            prev_hash,
            &command.payload,
        );
        let entry = LogEntry {
            cluster_id: self.cluster_id.clone(),
            epoch: self.epoch,
            config_id: self.config_id,
            index: state.next_index,
            entry_type: command.entry_type,
            payload: command.payload,
            prev_hash,
            hash,
        };
        state.next_index += 1;
        state.last_hash = hash;
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        anchored_fs, capture_next_fetch_group_token, command_file_reads,
        count_control_budget_constructors_for, current_recorder_layout, decode_configuration_state,
        decode_recorder_state, decode_wal_frame, encode_stored_command, encode_wal_frame,
        ensure_storage_generation, force_next_control_group_drain_timeout, last_file_sync_kind,
        lock_unpoison, pause_after_next_fetch_dispatch, pause_after_next_summary_dispatch,
        pause_after_next_summary_provisional_none, prepare_fresh_recorder_root,
        record_budget_identity_for, reset_command_file_reads, reset_sync_counts, sync_counts,
        sync_wal_append, sync_wal_metadata, upsert_wal_command, AcceptedValue, BudgetIdentityEvent,
        CertifiedDecisionInspection, ClusterId, ConfigChange, ConfigId, ConfigurationState,
        Consensus, ControlCallBudget, ControlCallGroup, ControlDispatch, ControlJob,
        ControlJobCancellation, ControlWorker, DecisionInspection, DecisionProof, DriveOutcome,
        EffectBundleBinding, EffectBundleFinalizeRequest, Epoch, Error, ExternalEffectCommand,
        FileSyncKind, Membership, PrioritySource, Proposal, ProposalPriority, ProposerProgress,
        ReadFenceObservation, ReadFenceRequest, ReadFenceSlotState, RecordRequest, RecordSummary,
        RecordedHeadProvenance, RecorderEffectBundle, RecorderFileStore, RecorderPostPreflightHook,
        RecorderPreflight, RecorderRequest, RecorderRpc, RecorderRpcContext, RecorderSlotState,
        RecorderSummary, RejectReason, SealFaultPoint, SingleNodeConsensus, Slot,
        ThreeNodeConsensus, RECORDER_POST_PREFLIGHT_HOOK, STORAGE_GENERATION_FILE,
        STORAGE_GENERATION_FINGERPRINT,
    };
    #[cfg(feature = "test-hooks")]
    use super::{
        ControlCompletionGuard, RecordDispatch, RecordJob, RecordWorker, RpcCallGroup,
        RpcCallWorker, TestControlOperationProbe, TestProbeLifecycleWait,
        TestProbeRegistrationError,
    };
    use proptest::prelude::*;
    use rhiza_core::{
        Command, CommandKind, EntryType, ExternalEffectProfile, LogHash, StoredCommand,
    };
    #[cfg(feature = "test-hooks")]
    use std::sync::Barrier;
    use std::{
        collections::{BTreeMap, BTreeSet, HashMap, HashSet},
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc, Arc, Condvar, Mutex, OnceLock,
        },
        thread,
        time::{Duration, Instant},
    };

    fn directory_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        fn visit(base: &Path, current: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
            if !current.exists() {
                return;
            }
            for entry in std::fs::read_dir(current).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    visit(base, &entry.path(), files);
                } else {
                    files.insert(
                        entry.path().strip_prefix(base).unwrap().to_path_buf(),
                        std::fs::read(entry.path()).unwrap(),
                    );
                }
            }
        }
        let mut files = BTreeMap::new();
        visit(root, root, &mut files);
        files
    }

    fn blocking_control_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn lock_blocking_control_tests() -> std::sync::MutexGuard<'static, ()> {
        // A failed test must not turn every later serial test into an unrelated
        // `PoisonError`; the original assertion remains the only failure.
        blocking_control_test_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Ensures a test failure cannot strand a recorder worker behind a
    /// rendezvous-channel gate and contaminate later parallel tests.
    struct ChannelRelease(Option<mpsc::SyncSender<()>>);

    impl ChannelRelease {
        fn new(sender: mpsc::SyncSender<()>) -> Self {
            Self(Some(sender))
        }

        fn release(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    impl Drop for ChannelRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    fn cache_backed_recorder_for_preflight(
        root: &Path,
        membership: Membership,
    ) -> (LogHash, StoredCommand) {
        let store =
            RecorderFileStore::new_with_membership(root, "n1", "cluster", 1, 1, membership.clone())
                .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"preflight-cache".to_vec());
        store
            .store_command(command.hash(), command.clone())
            .unwrap();
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: None,
            })
            .unwrap();
        (command.hash(), command)
    }

    fn record_requests(consensus: &ThreeNodeConsensus, slot: u64) -> Vec<RecordRequest> {
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            slot,
            AcceptedValue {
                command_hash: LogHash::ZERO,
                prev_hash: LogHash::ZERO,
                entry_hash: LogHash::ZERO,
            },
        );
        consensus
            .membership()
            .members()
            .iter()
            .map(|_| RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: consensus.membership().digest(),
                slot,
                step: 4,
                proposal: proposal.clone(),
                command: None,
            })
            .collect()
    }

    #[cfg(feature = "test-hooks")]
    struct TestProbeWorker;

    #[cfg(feature = "test-hooks")]
    impl RpcCallWorker for TestProbeWorker {
        fn prune_pending(&self, _group: &RpcCallGroup) {}

        fn quarantine(&self) {}

        fn worker_identity(&self) -> usize {
            self as *const Self as usize
        }
    }

    #[cfg(feature = "test-hooks")]
    fn test_probe_consensus() -> ThreeNodeConsensus {
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(SlotRecorder {
                        recorder_id,
                        reject_slot: None,
                        observed: None,
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap()
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_probe_rejects_duplicate_registration() {
        let consensus = test_probe_consensus();
        let probe = Arc::new(TestControlOperationProbe::default());
        let guard = consensus
            .install_test_record_operation_probe(7, Arc::clone(&probe))
            .unwrap();

        assert!(matches!(
            consensus.install_test_record_operation_probe(7, Arc::clone(&probe)),
            Err(TestProbeRegistrationError::DuplicateLiveRegistration)
        ));
        assert_eq!(
            consensus.test_record_operation_probe_registration_count(),
            1
        );
        drop(guard);
        assert_eq!(
            consensus.test_record_operation_probe_registration_count(),
            0
        );
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_probe_capture_attaches_every_matching_live_probe() {
        let consensus = test_probe_consensus();
        let first_probe = Arc::new(TestControlOperationProbe::default());
        let second_probe = Arc::new(TestControlOperationProbe::default());
        let first_registration = consensus
            .install_test_record_operation_probe(11, Arc::clone(&first_probe))
            .unwrap();
        let second_registration = consensus
            .install_test_record_operation_probe(11, Arc::clone(&second_probe))
            .unwrap();
        let group = RpcCallGroup::new();
        group.attach_test_record_probe(consensus.test_instance_id, 11);
        let worker = Arc::new(TestProbeWorker);
        let mut completion = ControlCompletionGuard::new(Arc::new(AtomicUsize::new(0)));
        completion.arm(Some(&group), &worker);
        drop(completion);

        for probe in [&first_probe, &second_probe] {
            assert_eq!(probe.dispatch_count(), 1);
            assert_eq!(probe.drained_count(), 1);
            assert_eq!(probe.outstanding(), 0);
            assert!(probe.worker_transitions().iter().all(|transition| {
                transition.enqueued == 1
                    && transition.completion_dropped == 1
                    && transition.live_leases == 0
            }));
        }
        drop(group);
        drop(first_registration);
        drop(second_registration);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_probe_rejects_reinstall_while_a_dropped_guard_has_live_leases() {
        let consensus = test_probe_consensus();
        let probe = Arc::new(TestControlOperationProbe::default());
        let registration = consensus
            .install_test_record_operation_probe(8, Arc::clone(&probe))
            .unwrap();
        let group = RpcCallGroup::new();
        group.attach_test_record_probe(consensus.test_instance_id, 8);
        let worker = Arc::new(TestProbeWorker);
        let pending = Arc::new(AtomicUsize::new(0));
        let mut completion = ControlCompletionGuard::new(pending);
        completion.arm(Some(&group), &worker);
        drop(registration);
        assert_eq!(probe.outstanding(), 1);

        assert!(matches!(
            consensus.install_test_record_operation_probe(8, Arc::clone(&probe)),
            Err(TestProbeRegistrationError::LiveLeases { leases: 1 })
        ));
        assert_eq!(probe.outstanding(), 1);
        drop(completion);
        assert_eq!(probe.outstanding(), 0);
        assert_eq!(probe.drained_count(), 1);
        assert!(matches!(
            consensus.install_test_record_operation_probe(8, Arc::clone(&probe)),
            Err(TestProbeRegistrationError::ActiveAttachments { attachments: 1 })
        ));
        drop(group);
        let registration = consensus
            .install_test_record_operation_probe(8, Arc::clone(&probe))
            .unwrap();
        drop(registration);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_probe_reuses_only_after_the_last_group_attachment_drops() {
        let consensus = test_probe_consensus();
        let probe = Arc::new(TestControlOperationProbe::default());
        let registration = consensus
            .install_test_record_operation_probe(9, Arc::clone(&probe))
            .unwrap();
        let group = RpcCallGroup::new();
        group.attach_test_record_probe(consensus.test_instance_id, 9);
        let worker = Arc::new(TestProbeWorker);
        let mut completion = ControlCompletionGuard::new(Arc::new(AtomicUsize::new(0)));
        completion.arm(Some(&group), &worker);
        drop(registration);
        drop(completion);
        assert_eq!(probe.outstanding(), 0);
        assert!(matches!(
            consensus.install_test_record_operation_probe(9, Arc::clone(&probe)),
            Err(TestProbeRegistrationError::ActiveAttachments { attachments: 1 })
        ));
        drop(group);

        let registration = consensus
            .install_test_record_operation_probe(9, Arc::clone(&probe))
            .unwrap();
        assert_eq!(probe.dispatch_count(), 0);
        assert_eq!(probe.outstanding(), 0);
        drop(registration);
        assert_eq!(
            consensus.test_record_operation_probe_registration_count(),
            0
        );
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_probe_attachment_prevents_generation_reuse_until_group_drop() {
        let consensus = test_probe_consensus();
        let probe = Arc::new(TestControlOperationProbe::default());
        let old_registration = consensus
            .install_test_record_operation_probe(10, Arc::clone(&probe))
            .unwrap();
        let old_group = RpcCallGroup::new();
        old_group.attach_test_record_probe(consensus.test_instance_id, 10);
        let worker = Arc::new(TestProbeWorker);
        drop(old_registration);

        // A generation stays live from attachment, not only from an admitted
        // lease. This closes the attach-before-admit reuse window.
        assert!(matches!(
            consensus.install_test_record_operation_probe(10, Arc::clone(&probe)),
            Err(TestProbeRegistrationError::ActiveAttachments { attachments: 1 })
        ));

        let mut old_completion = ControlCompletionGuard::new(Arc::new(AtomicUsize::new(0)));
        old_completion.arm(Some(&old_group), &worker);
        drop(old_completion);
        assert_eq!(probe.dispatch_count(), 1);
        assert_eq!(probe.drained_count(), 1);
        assert_eq!(probe.outstanding(), 0);
        assert!(probe.worker_transitions().iter().all(|transition| {
            transition.enqueued == 1
                && transition.completion_dropped == 1
                && transition.live_leases == 0
        }));
        drop(old_group);

        let current_registration = consensus
            .install_test_record_operation_probe(10, Arc::clone(&probe))
            .unwrap();

        let current_group = RpcCallGroup::new();
        current_group.attach_test_record_probe(consensus.test_instance_id, 10);
        let mut current_completion = ControlCompletionGuard::new(Arc::new(AtomicUsize::new(0)));
        current_completion.arm(Some(&current_group), &worker);
        drop(current_completion);
        current_group.cancel_and_prune();
        assert_eq!(probe.dispatch_count(), 1);
        assert_eq!(probe.drained_count(), 1);
        assert_eq!(probe.outstanding(), 0);
        assert_eq!(probe.cancel_count(), 1);
        assert!(probe.worker_transitions().iter().all(|transition| {
            transition.enqueued == 1
                && transition.completion_dropped == 1
                && transition.pruned == 0
                && transition.live_leases == 0
        }));
        drop(current_registration);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_probe_readiness_observes_an_admission_before_waiting() {
        let probe = Arc::new(TestControlOperationProbe::default());
        probe.record_lease_registered(0);

        assert!(probe.wait_for_admitted_outstanding(Duration::ZERO));

        probe.record_lease_completed(0);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_probe_quiescence_observes_completion_before_waiting() {
        let probe = Arc::new(TestControlOperationProbe::default());
        probe.record_lease_registered(0);
        probe.record_lease_completed(0);

        assert!(probe.wait_for_quiescence(Duration::ZERO));
        assert_eq!(probe.dispatch_count(), 1);
        assert_eq!(probe.outstanding(), 0);
        assert_eq!(probe.drained_count(), 1);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_probe_readiness_rejects_a_later_generation() {
        let consensus = test_probe_consensus();
        let probe = Arc::new(TestControlOperationProbe::default());
        let old_registration = consensus
            .install_test_record_operation_probe(12, Arc::clone(&probe))
            .unwrap();
        let old_group = RpcCallGroup::new();
        old_group.attach_test_record_probe(consensus.test_instance_id, 12);

        let captured_generation = Arc::new(Barrier::new(2));
        let (waiter_result_tx, waiter_result_rx) = mpsc::sync_channel(1);
        let waiter = {
            let probe = Arc::clone(&probe);
            let captured_generation = Arc::clone(&captured_generation);
            thread::spawn(move || {
                waiter_result_tx
                    .send(
                        probe.wait_for_admitted_outstanding_after_test_generation_capture(
                            Duration::from_secs(2),
                            captured_generation,
                        ),
                    )
                    .unwrap();
            })
        };
        captured_generation.wait();

        drop(old_registration);
        drop(old_group);
        let current_registration = consensus
            .install_test_record_operation_probe(12, Arc::clone(&probe))
            .unwrap();
        assert_eq!(
            waiter_result_rx
                .recv_timeout(Duration::from_secs(3))
                .unwrap(),
            TestProbeLifecycleWait::GenerationChanged,
            "the stale waiter must wake on generation reset before new admission"
        );
        waiter.join().unwrap();

        let current_group = RpcCallGroup::new();
        current_group.attach_test_record_probe(consensus.test_instance_id, 12);
        let worker = Arc::new(TestProbeWorker);
        let mut completion = ControlCompletionGuard::new(Arc::new(AtomicUsize::new(0)));
        completion.arm(Some(&current_group), &worker);
        assert!(probe.wait_for_admitted_outstanding(Duration::ZERO));
        drop(completion);
        drop(current_group);
        drop(current_registration);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_probe_quiescence_rejects_a_later_generation_and_resets_false() {
        let consensus = test_probe_consensus();
        let probe = Arc::new(TestControlOperationProbe::default());
        let captured_generation = Arc::new(Barrier::new(2));
        let (waiter_result_tx, waiter_result_rx) = mpsc::sync_channel(1);
        let waiter = {
            let probe = Arc::clone(&probe);
            let captured_generation = Arc::clone(&captured_generation);
            thread::spawn(move || {
                waiter_result_tx
                    .send(probe.wait_for_quiescence_after_test_generation_capture(
                        Duration::from_secs(2),
                        captured_generation,
                    ))
                    .unwrap();
            })
        };
        captured_generation.wait();

        let registration = consensus
            .install_test_record_operation_probe(13, Arc::clone(&probe))
            .unwrap();
        assert_eq!(
            waiter_result_rx
                .recv_timeout(Duration::from_secs(3))
                .unwrap(),
            TestProbeLifecycleWait::GenerationChanged,
            "a stale quiescence waiter must not consume a later generation"
        );
        waiter.join().unwrap();
        assert!(!probe.wait_for_quiescence(Duration::ZERO));
        drop(registration);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn record_probe_counts_each_guard_lifetime_across_fast_groups() {
        // Force the original race with two threads: registration reads the
        // group's length, completion publishes zero, then delayed admission
        // overwrites that result with its stale nonzero snapshot.
        let legacy_published = Arc::new(AtomicUsize::new(1));
        let legacy_snapshot_taken = Arc::new(Barrier::new(2));
        let legacy_completion_published = Arc::new(Barrier::new(2));
        let legacy_registration = {
            let published = Arc::clone(&legacy_published);
            let snapshot_taken = Arc::clone(&legacy_snapshot_taken);
            let completion_published = Arc::clone(&legacy_completion_published);
            thread::spawn(move || {
                let stale_snapshot = published.load(Ordering::Acquire);
                snapshot_taken.wait();
                completion_published.wait();
                published.store(stale_snapshot, Ordering::Release);
            })
        };
        legacy_snapshot_taken.wait();
        legacy_published.store(0, Ordering::Release);
        legacy_completion_published.wait();
        legacy_registration.join().unwrap();
        assert_eq!(legacy_published.load(Ordering::Acquire), 1);

        // The authoritative lifecycle records admission in one mutex-held
        // transition. Its delayed registration suffix has no second counter
        // publication, so the analogous completion remains visible as zero.
        let authoritative = Arc::new(TestControlOperationProbe::default());
        let authoritative_registered = Arc::new(Barrier::new(2));
        let authoritative_completed = Arc::new(Barrier::new(2));
        let authoritative_registration = {
            let probe = Arc::clone(&authoritative);
            let registered = Arc::clone(&authoritative_registered);
            let completed = Arc::clone(&authoritative_completed);
            thread::spawn(move || {
                probe.record_lease_registered(0);
                registered.wait();
                completed.wait();
            })
        };
        authoritative_registered.wait();
        authoritative.record_lease_completed(0);
        authoritative_completed.wait();
        authoritative_registration.join().unwrap();
        assert_eq!(authoritative.outstanding(), 0);

        // The real probe has one mutex-protected lifecycle state. Two groups
        // share it and complete in reverse order; no delayed publication can
        // overwrite 1 or 0 after a completion.
        let probe = Arc::new(TestControlOperationProbe::default());
        let first = RpcCallGroup::new();
        let second = RpcCallGroup::new();
        *super::lock_unpoison(&first.state.test_probes) = vec![probe.current_attachment()];
        *super::lock_unpoison(&second.state.test_probes) = vec![probe.current_attachment()];
        let worker = Arc::new(TestProbeWorker);
        let pending = Arc::new(AtomicUsize::new(0));

        let mut first_guard = ControlCompletionGuard::new(Arc::clone(&pending));
        first_guard.arm(Some(&first), &worker);
        let mut second_guard = ControlCompletionGuard::new(Arc::clone(&pending));
        second_guard.arm(Some(&second), &worker);

        assert_eq!(first.outstanding_len(), 1);
        assert_eq!(second.outstanding_len(), 1);
        assert_eq!(pending.load(Ordering::Acquire), 2);
        assert_eq!(probe.dispatch_count(), 2);
        assert_eq!(probe.pending(), 2);
        assert_eq!(probe.outstanding(), 2);
        assert_eq!(probe.observed_max_outstanding(), 2);
        let transitions = probe.worker_transitions();
        assert_eq!(transitions.len(), 1);
        assert_eq!(transitions[0].enqueued, 2);
        assert_eq!(transitions[0].live_leases, 2);
        assert_eq!(transitions[0].completion_dropped, 0);

        // Completion can be immediate and out of admission order.
        drop(second_guard);
        assert_eq!(first.outstanding_len(), 1);
        assert_eq!(second.outstanding_len(), 0);
        assert_eq!(pending.load(Ordering::Acquire), 1);
        assert_eq!(probe.pending(), 1);
        assert_eq!(probe.outstanding(), 1);
        assert_eq!(probe.drained_count(), 1);
        let transitions = probe.worker_transitions();
        assert_eq!(transitions[0].live_leases, 1);
        assert_eq!(transitions[0].completion_dropped, 1);

        drop(first_guard);
        assert_eq!(first.outstanding_len(), 0);
        assert_eq!(pending.load(Ordering::Acquire), 0);
        assert_eq!(probe.pending(), 0);
        assert_eq!(probe.outstanding(), 0);
        assert_eq!(probe.drained_count(), 2);
        let transitions = probe.worker_transitions();
        assert_eq!(transitions[0].live_leases, 0);
        assert_eq!(transitions[0].completion_dropped, 2);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn record_probe_releases_queued_leases_on_prune_and_close() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let request = |slot| RecordRequest {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            slot,
            step: 4,
            proposal: Proposal::new(
                ProposalPriority::MAX,
                "n1",
                slot,
                AcceptedValue {
                    command_hash: LogHash::ZERO,
                    prev_hash: LogHash::ZERO,
                    entry_hash: LogHash::ZERO,
                },
            ),
            command: None,
        };
        let start_worker = |started, release| {
            RecordWorker::spawn(
                "n1".into(),
                Arc::new(BlockingRecorder {
                    recorder_id: "n1",
                    started,
                    release_first: Mutex::new(release),
                }),
                1,
                membership.digest(),
            )
            .unwrap()
        };

        // The running first job holds the worker while a second group queues
        // its hedge. `prune_pending` is the post-quorum path: it must consume
        // the queued guard rather than merely hide the job from the queue.
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let mut worker = start_worker(started_tx, release_rx);
        let background = RpcCallGroup::new();
        let pruned_group = RpcCallGroup::new();
        let pruned_probe = Arc::new(TestControlOperationProbe::default());
        *super::lock_unpoison(&pruned_group.state.test_probes) =
            vec![pruned_probe.current_attachment()];
        let started = AtomicBool::new(false);
        let (first_tx, first_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch_mutating_group(
                RecordJob {
                    index: 0,
                    context: RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    request: request(1),
                    result: first_tx,
                },
                &background,
                &started,
            ),
            RecordDispatch::Accepted
        ));
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        let (queued_tx, queued_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch_mutating_group(
                RecordJob {
                    index: 0,
                    context: RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    request: request(2),
                    result: queued_tx,
                },
                &pruned_group,
                &started,
            ),
            RecordDispatch::Accepted
        ));
        assert_eq!(pruned_probe.outstanding(), 1);
        pruned_group.prune_pending();
        assert_eq!(
            queued_rx.recv_timeout(Duration::from_secs(1)),
            Ok((0, Err(Error::RpcCancelled)))
        );
        assert_eq!(pruned_group.outstanding_len(), 0);
        assert_eq!(pruned_probe.pending(), 0);
        assert_eq!(pruned_probe.outstanding(), 0);
        assert_eq!(pruned_probe.drained_count(), 1);
        assert!(pruned_probe.worker_transitions().iter().all(|transition| {
            transition.pruned == 1
                && transition.live_leases == 0
                && transition.enqueued == transition.completion_dropped
        }));
        release_tx.send(()).unwrap();
        assert!(matches!(
            first_rx.recv_timeout(Duration::from_secs(1)),
            Ok((0, Ok(_)))
        ));
        worker.shutdown();

        // Shutdown's close-and-drain path has the same obligation for a
        // queued job, but returns mutation-unknown instead of cancellation.
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (_release_tx, release_rx) = mpsc::sync_channel(1);
        let mut worker = start_worker(started_tx, release_rx);
        let background = RpcCallGroup::new();
        let closed_group = RpcCallGroup::new();
        let closed_probe = Arc::new(TestControlOperationProbe::default());
        *super::lock_unpoison(&closed_group.state.test_probes) =
            vec![closed_probe.current_attachment()];
        let (first_tx, first_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch_mutating_group(
                RecordJob {
                    index: 0,
                    context: RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    request: request(1),
                    result: first_tx,
                },
                &background,
                &started,
            ),
            RecordDispatch::Accepted
        ));
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        let (queued_tx, queued_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch_mutating_group(
                RecordJob {
                    index: 0,
                    context: RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    request: request(2),
                    result: queued_tx,
                },
                &closed_group,
                &started,
            ),
            RecordDispatch::Accepted
        ));
        worker.state.close_and_drain();
        assert_eq!(
            queued_rx.recv_timeout(Duration::from_secs(1)),
            Ok((0, Err(Error::UnknownOutcome)))
        );
        assert!(matches!(
            first_rx.recv_timeout(Duration::from_secs(1)),
            Ok((0, Err(Error::RpcCancelled)))
        ));
        assert_eq!(closed_group.outstanding_len(), 0);
        assert_eq!(closed_probe.pending(), 0);
        assert_eq!(closed_probe.outstanding(), 0);
        assert_eq!(closed_probe.drained_count(), 1);
        assert!(closed_probe.worker_transitions().iter().all(|transition| {
            transition.close_drained == 1
                && transition.live_leases == 0
                && transition.enqueued == transition.completion_dropped
        }));
        worker.handle.take().unwrap().join().unwrap();
    }

    #[test]
    fn caller_context_reaches_record_and_control_workers_unchanged() {
        let event_timeout = Duration::from_secs(10);
        let record_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _record_gate = GateRelease::new(Arc::clone(&record_gate));
        let (record_tx, record_rx) = mpsc::sync_channel(3);
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(GatedContextRecorder {
                        recorder_id,
                        contexts: record_tx.clone(),
                        release: Arc::clone(&record_gate),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let context = RecorderRpcContext::with_timeout(event_timeout);
        let (record_done_tx, record_done_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            let context = context.clone();
            thread::spawn(move || {
                let mutation_started = AtomicBool::new(false);
                record_done_tx
                    .send(consensus.record_broadcast_with_context(
                        record_requests(&consensus, 1),
                        context,
                        &mutation_started,
                    ))
                    .unwrap();
            })
        };
        for _ in 0..3 {
            assert_eq!(
                record_rx.recv_timeout(event_timeout).unwrap(),
                context.deadline() - super::CONTROL_DRAIN_RESERVE
            );
        }
        release_gate(&record_gate);
        assert!(record_done_rx.recv_timeout(event_timeout).unwrap().is_ok());
        caller.join().unwrap();

        let (_unused_record_tx, _unused_record_rx) = mpsc::sync_channel(1);
        let (control_tx, control_rx) = mpsc::sync_channel(1);
        let worker = ControlWorker::spawn(Arc::new(ContextRecordingRecorder {
            recorder_id: "n1",
            record_contexts: _unused_record_tx,
            control_contexts: control_tx,
            record_error: None,
        }))
        .unwrap();
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch(ControlJob::InspectProof {
                index: 0,
                context: context.clone(),
                slot: 1,
                result: result_tx,
            }),
            ControlDispatch::Accepted
        ));
        assert_eq!(result_rx.recv_timeout(event_timeout).unwrap().1, Ok(None));
        assert_eq!(
            control_rx.recv_timeout(event_timeout).unwrap(),
            context.deadline()
        );
    }

    #[test]
    fn cancellation_before_record_dispatch_is_typed_and_side_effect_free() {
        let (record_tx, record_rx) = mpsc::sync_channel(3);
        let (control_tx, _control_rx) = mpsc::sync_channel(3);
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ContextRecordingRecorder {
                        recorder_id,
                        record_contexts: record_tx.clone(),
                        control_contexts: control_tx.clone(),
                        record_error: None,
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let context = RecorderRpcContext::with_timeout(Duration::from_secs(30));
        context.cancel();
        let mutation_started = AtomicBool::new(false);
        assert_eq!(
            consensus.record_broadcast_with_context(
                record_requests(&consensus, 1),
                context,
                &mutation_started,
            ),
            Err(Error::RpcCancelled)
        );
        assert!(!mutation_started.load(Ordering::Acquire));
        assert_eq!(record_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    }

    #[test]
    fn zero_deadline_record_broadcast_admits_no_record_worker() {
        let (record_tx, record_rx) = mpsc::sync_channel(3);
        let (control_tx, _control_rx) = mpsc::sync_channel(3);
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ContextRecordingRecorder {
                        recorder_id,
                        record_contexts: record_tx.clone(),
                        control_contexts: control_tx.clone(),
                        record_error: None,
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let mutation_started = AtomicBool::new(false);

        assert_eq!(
            consensus.record_broadcast_with_context(
                record_requests(&consensus, 1),
                RecorderRpcContext::with_timeout(Duration::ZERO),
                &mutation_started,
            ),
            Err(Error::RpcDeadlineExceeded)
        );
        assert!(!mutation_started.load(Ordering::Acquire));
        assert_eq!(record_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn record_partial_admission_is_independent_of_the_preclosed_worker_position() {
        for preclosed in [0, 2] {
            let recorders = ["n1", "n2", "n3"]
                .into_iter()
                .map(|recorder_id| {
                    (
                        recorder_id.into(),
                        Box::new(SlotRecorder {
                            recorder_id,
                            reject_slot: None,
                            observed: None,
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect();
            let consensus =
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap();
            consensus.record_workers[preclosed].state.close_and_drain();
            let mutation_started = AtomicBool::new(false);

            let replies = consensus
                .record_broadcast_with_context(
                    record_requests(&consensus, 1),
                    RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    &mutation_started,
                )
                .unwrap();
            assert_eq!(replies.len(), 2);
            assert!(replies.iter().all(|reply| reply.slot == 1));
            assert!(mutation_started.load(Ordering::Acquire));
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        }
    }

    #[test]
    fn record_preclosed_worker_contributes_to_impossible_quorum_classification() {
        for preclosed in 0..3 {
            let success = (preclosed + 1) % 3;
            let recorders = ["n1", "n2", "n3"]
                .into_iter()
                .enumerate()
                .map(|(index, recorder_id)| {
                    let recorder: Box<dyn RecorderRpc> = if index == success {
                        Box::new(SlotRecorder {
                            recorder_id,
                            reject_slot: None,
                            observed: None,
                        })
                    } else {
                        Box::new(AlwaysIoRecorder)
                    };
                    (recorder_id.into(), recorder)
                })
                .collect();
            let consensus =
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap();
            consensus.record_workers[preclosed].state.close_and_drain();
            assert_eq!(
                consensus.record_broadcast_with_context(
                    record_requests(&consensus, 1),
                    RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    &AtomicBool::new(false),
                ),
                Err(Error::ProposeFailed)
            );

            let typed = (preclosed + 2) % 3;
            let recorders = ["n1", "n2", "n3"]
                .into_iter()
                .enumerate()
                .map(|(index, recorder_id)| {
                    let recorder: Box<dyn RecorderRpc> = if index == success {
                        Box::new(SlotRecorder {
                            recorder_id,
                            reject_slot: None,
                            observed: None,
                        })
                    } else if index == typed {
                        Box::new(SlotRecorder {
                            recorder_id,
                            reject_slot: Some(1),
                            observed: None,
                        })
                    } else {
                        Box::new(AlwaysIoRecorder)
                    };
                    (recorder_id.into(), recorder)
                })
                .collect();
            let consensus =
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap();
            consensus.record_workers[preclosed].state.close_and_drain();
            assert_eq!(
                consensus.record_broadcast_with_context(
                    record_requests(&consensus, 1),
                    RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    &AtomicBool::new(false),
                ),
                Err(Error::Rejected(RejectReason::InvalidRequest))
            );
        }
    }

    #[test]
    fn cancellation_or_expiry_after_record_dispatch_is_unknown_outcome() {
        for record_error in [Error::RpcCancelled, Error::RpcDeadlineExceeded] {
            let (record_tx, record_rx) = mpsc::sync_channel(3);
            let (control_tx, _control_rx) = mpsc::sync_channel(3);
            let recorders = ["n1", "n2", "n3"]
                .into_iter()
                .map(|recorder_id| {
                    (
                        recorder_id.into(),
                        Box::new(ContextRecordingRecorder {
                            recorder_id,
                            record_contexts: record_tx.clone(),
                            control_contexts: control_tx.clone(),
                            record_error: Some(record_error.clone()),
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect();
            let consensus =
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap();
            let context = RecorderRpcContext::with_timeout(Duration::from_secs(30));
            let mutation_started = AtomicBool::new(false);
            assert_eq!(
                consensus.record_broadcast_with_context(
                    record_requests(&consensus, 1),
                    context,
                    &mutation_started,
                ),
                Err(Error::UnknownOutcome)
            );
            assert!(mutation_started.load(Ordering::Acquire));
            assert!(record_rx.recv().is_ok());
        }
    }

    #[test]
    fn exact_record_quorum_survives_an_earlier_unknown_outcome() {
        let (record_tx, record_rx) = mpsc::sync_channel(1);
        let (control_tx, _control_rx) = mpsc::sync_channel(1);
        let exact_release = Arc::new((Mutex::new(false), Condvar::new()));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ContextRecordingRecorder {
                    recorder_id: "n1",
                    record_contexts: record_tx,
                    control_contexts: control_tx,
                    record_error: Some(Error::UnknownOutcome),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(GatedRecordRecorder {
                    recorder_id: "n2",
                    release: Arc::clone(&exact_release),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(GatedRecordRecorder {
                    recorder_id: "n3",
                    release: Arc::clone(&exact_release),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let mutation_started = AtomicBool::new(false);

        let replies = thread::scope(|scope| {
            let proposal = scope.spawn(|| {
                consensus.record_broadcast_with_context(
                    record_requests(&consensus, 1),
                    RecorderRpcContext::with_timeout(Duration::from_secs(2)),
                    &mutation_started,
                )
            });
            assert!(record_rx.recv_timeout(Duration::from_secs(1)).is_ok());
            let (released, condition) = &*exact_release;
            *released.lock().unwrap() = true;
            condition.notify_all();
            proposal.join().unwrap().unwrap()
        });

        assert_eq!(
            replies
                .iter()
                .map(|reply| reply.recorder_id.as_str())
                .collect::<HashSet<_>>(),
            HashSet::from(["n2", "n3"])
        );
        assert!(mutation_started.load(Ordering::Acquire));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn record_no_quorum_preserves_unknown_over_a_definite_failure_in_every_position() {
        for exact_position in 0..3 {
            for unknown_position in 0..3 {
                if exact_position == unknown_position {
                    continue;
                }
                let (record_tx, _record_rx) = mpsc::sync_channel(3);
                let (control_tx, _control_rx) = mpsc::sync_channel(3);
                let recorders = ["n1", "n2", "n3"]
                    .into_iter()
                    .enumerate()
                    .map(|(position, recorder_id)| {
                        let recorder: Box<dyn RecorderRpc> = if position == exact_position {
                            Box::new(SlotRecorder {
                                recorder_id,
                                reject_slot: None,
                                observed: None,
                            })
                        } else {
                            Box::new(ContextRecordingRecorder {
                                recorder_id,
                                record_contexts: record_tx.clone(),
                                control_contexts: control_tx.clone(),
                                record_error: Some(if position == unknown_position {
                                    Error::UnknownOutcome
                                } else {
                                    Error::ProposeFailed
                                }),
                            })
                        };
                        (recorder_id.into(), recorder)
                    })
                    .collect();
                let consensus =
                    ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                        .unwrap();
                assert_eq!(
                    consensus.record_broadcast_with_context(
                        record_requests(&consensus, 1),
                        RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        &AtomicBool::new(false),
                    ),
                    Err(Error::UnknownOutcome)
                );
            }
        }
    }

    #[test]
    fn record_one_exact_and_two_definite_failures_is_retryable_in_every_position() {
        for exact_position in 0..3 {
            let (record_tx, _record_rx) = mpsc::sync_channel(3);
            let (control_tx, _control_rx) = mpsc::sync_channel(3);
            let recorders = ["n1", "n2", "n3"]
                .into_iter()
                .enumerate()
                .map(|(position, recorder_id)| {
                    let recorder: Box<dyn RecorderRpc> = if position == exact_position {
                        Box::new(SlotRecorder {
                            recorder_id,
                            reject_slot: None,
                            observed: None,
                        })
                    } else {
                        Box::new(ContextRecordingRecorder {
                            recorder_id,
                            record_contexts: record_tx.clone(),
                            control_contexts: control_tx.clone(),
                            record_error: Some(Error::ProposeFailed),
                        })
                    };
                    (recorder_id.into(), recorder)
                })
                .collect();
            let consensus =
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap();
            assert_eq!(
                consensus.record_broadcast_with_context(
                    record_requests(&consensus, 1),
                    RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    &AtomicBool::new(false),
                ),
                Err(Error::ProposeFailed)
            );
        }
    }

    #[test]
    fn record_quorum_returns_within_drain_reserve_and_quarantines_a_noncooperative_hedge() {
        let (n1_started_tx, n1_started_rx) = mpsc::sync_channel(1);
        let (n2_started_tx, n2_started_rx) = mpsc::sync_channel(1);
        let (n3_started_tx, n3_started_rx) = mpsc::sync_channel(1);
        let (n1_release_tx, n1_release_rx) = mpsc::sync_channel(1);
        let (n2_release_tx, n2_release_rx) = mpsc::sync_channel(1);
        let (n3_release_tx, n3_release_rx) = mpsc::sync_channel(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n1",
                    started: n1_started_tx,
                    release_first: Mutex::new(n1_release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n2",
                    started: n2_started_tx,
                    release_first: Mutex::new(n2_release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(NonCooperativeRecordRecorder {
                    recorder_id: "n3",
                    started: n3_started_tx,
                    release: Mutex::new(n3_release_rx),
                    calls: Arc::clone(&calls),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                let mutation_started = AtomicBool::new(false);
                let result = consensus.record_broadcast_with_context(
                    record_requests(&consensus, 1),
                    RecorderRpcContext::with_timeout(Duration::from_millis(300)),
                    &mutation_started,
                );
                done_tx
                    .send((result, mutation_started.load(Ordering::Acquire)))
                    .unwrap();
            })
        };
        let caller_guard = ChannelReleaseAndJoin::new(
            vec![
                n1_release_tx.clone(),
                n2_release_tx.clone(),
                n3_release_tx.clone(),
            ],
            caller,
        );

        assert_eq!(n1_started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        assert_eq!(n2_started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        assert_eq!(n3_started_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
        n1_release_tx.send(()).unwrap();
        n2_release_tx.send(()).unwrap();
        let (result, mutation_started) = done_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("an exact quorum must not wait through the root deadline");
        assert!(matches!(result, Ok(ref replies) if replies.len() == 2));
        assert!(mutation_started);

        let blocked = &consensus.record_workers[2].state;
        assert!(blocked.quarantined.load(Ordering::Acquire));
        assert!(!consensus.record_workers[0]
            .state
            .quarantined
            .load(Ordering::Acquire));
        assert!(!consensus.record_workers[1]
            .state
            .quarantined
            .load(Ordering::Acquire));
        assert_eq!(blocked.pending.load(Ordering::Acquire), 1);
        assert_eq!(calls.load(Ordering::Acquire), 1);

        // A quarantined worker accepts no new mutation, while the healthy
        // quorum recovers immediately without waiting for the stuck call.
        let follow_up = consensus.record_broadcast_with_context(
            record_requests(&consensus, 2),
            RecorderRpcContext::with_timeout(Duration::from_secs(1)),
            &AtomicBool::new(false),
        );
        assert!(
            matches!(follow_up, Ok(ref replies) if replies.len() == 2),
            "healthy quorum must recover after isolating the stuck worker: {follow_up:?}"
        );
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(blocked.pending.load(Ordering::Acquire), 1);

        caller_guard.finish();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        assert_eq!(blocked.pending.load(Ordering::Acquire), 0);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn ready_safety_error_beats_exact_record_quorum_while_completion_is_delayed() {
        let event_timeout = Duration::from_secs(2);
        let success_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _success_gate = GateRelease::new(Arc::clone(&success_gate));
        let post_send_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _post_send_gate = GateRelease::new(Arc::clone(&post_send_gate));
        let (n1_seen_tx, n1_seen_rx) = mpsc::sync_channel(1);
        let (n2_seen_tx, n2_seen_rx) = mpsc::sync_channel(1);
        let (group_tx, group_rx) = mpsc::sync_channel(1);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(GatedContextRecorder {
                    recorder_id: "n1",
                    contexts: n1_seen_tx,
                    release: Arc::clone(&success_gate),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(GatedContextRecorder {
                    recorder_id: "n2",
                    contexts: n2_seen_tx,
                    release: Arc::clone(&success_gate),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(CancellationSafetyRecorder {
                    group_cancellation: group_tx,
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let probe = Arc::new(TestControlOperationProbe::default());
        let registration = consensus
            .install_test_record_operation_probe(1, Arc::clone(&probe))
            .unwrap();
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                done_tx
                    .send(consensus.record_broadcast_with_context(
                        record_requests(&consensus, 1),
                        RecorderRpcContext::with_timeout(event_timeout),
                        &AtomicBool::new(false),
                    ))
                    .unwrap();
            })
        };
        assert!(n1_seen_rx.recv_timeout(event_timeout).is_ok());
        assert!(n2_seen_rx.recv_timeout(event_timeout).is_ok());
        let group_cancellation = group_rx.recv_timeout(event_timeout).unwrap();
        let (post_send_tx, post_send_rx) = mpsc::sync_channel(1);
        let _post_send_hook = super::pause_after_next_record_reply_sent(
            &consensus.record_workers[2].state,
            group_cancellation,
            post_send_tx,
            Arc::clone(&post_send_gate),
        );

        release_gate(&success_gate);
        assert_eq!(post_send_rx.recv_timeout(event_timeout), Ok(()));
        assert_eq!(
            done_rx.recv_timeout(Duration::from_millis(500)),
            Ok(Err(Error::ChainConflict {
                slot: 1,
                expected_prev_hash: LogHash::ZERO,
                actual_prev_hash: LogHash::ZERO,
            }))
        );
        assert!(consensus.record_workers[2]
            .state
            .quarantined
            .load(Ordering::Acquire));
        assert!(consensus.record_workers[..2]
            .iter()
            .all(|worker| !worker.state.quarantined.load(Ordering::Acquire)));
        assert_eq!(probe.quarantine_count(), 1);
        assert_eq!(probe.outstanding(), 1);
        assert!(!probe.wait_for_quiescence(Duration::ZERO));

        release_gate(&post_send_gate);
        caller.join().unwrap();
        assert!(probe.wait_for_quiescence(event_timeout));
        assert_eq!(probe.outstanding(), 0);
        assert!(probe.worker_transitions().iter().all(|transition| {
            transition.live_leases == 0 && transition.enqueued == transition.completion_dropped
        }));
        assert!(probe.worker_transitions().iter().any(|transition| {
            transition.worker_identity == Arc::as_ptr(&consensus.record_workers[2].state) as usize
                && transition.reply_sent == 1
                && transition.completion_dropped == 1
        }));
        drop(registration);
        assert_eq!(
            consensus.test_record_operation_probe_registration_count(),
            0
        );
        assert!(consensus.finish_pending_rpcs(event_timeout));
    }

    #[test]
    fn frozen_record_quorum_prunes_only_its_queued_hedge() {
        let event_timeout = Duration::from_secs(10);
        let (n1_started_tx, n1_started_rx) = mpsc::sync_channel(4);
        let (n1_release_tx, n1_release_rx) = mpsc::sync_channel(1);
        let (n2_seen_tx, n2_seen_rx) = mpsc::sync_channel(4);
        let (n3_seen_tx, n3_seen_rx) = mpsc::sync_channel(4);
        let record_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _record_gate = GateRelease::new(Arc::clone(&record_gate));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n1",
                    started: n1_started_tx,
                    release_first: Mutex::new(n1_release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(GatedObservedSlotRecorder {
                    recorder_id: "n2",
                    observed: n2_seen_tx,
                    release: Arc::clone(&record_gate),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(GatedObservedSlotRecorder {
                    recorder_id: "n3",
                    observed: n3_seen_tx,
                    release: Arc::clone(&record_gate),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        #[cfg(feature = "test-hooks")]
        let queued_hedge_probe = Arc::new(TestControlOperationProbe::default());
        #[cfg(feature = "test-hooks")]
        let _queued_hedge_probe = consensus
            .install_test_record_operation_probe(2, Arc::clone(&queued_hedge_probe))
            .unwrap();

        let (a_done_tx, a_done_rx) = mpsc::sync_channel(1);
        let a_consensus = Arc::clone(&consensus);
        let caller_a = thread::spawn(move || {
            a_done_tx
                .send(a_consensus.record_broadcast_with_context(
                    record_requests(&a_consensus, 1),
                    RecorderRpcContext::with_timeout(event_timeout),
                    &AtomicBool::new(false),
                ))
                .unwrap();
        });
        let caller_a_guard = ChannelReleaseAndJoin::new(vec![n1_release_tx], caller_a);
        #[cfg(feature = "test-hooks")]
        assert_eq!(
            n1_started_rx.recv_timeout(event_timeout),
            Ok(1),
            "A n1 start missing: a_done={:?}, probe_registrations={}, probe_pending={}, probe_outstanding={}, probe_dispatches={}, n1_pending={}, n1_quarantined={}",
            a_done_rx.try_recv(),
            consensus.test_record_operation_probe_registration_count(),
            queued_hedge_probe.pending(),
            queued_hedge_probe.outstanding(),
            queued_hedge_probe.dispatch_count(),
            consensus.record_workers[0]
                .state
                .pending
                .load(Ordering::Acquire),
            consensus.record_workers[0]
                .state
                .quarantined
                .load(Ordering::Acquire),
        );
        #[cfg(not(feature = "test-hooks"))]
        assert_eq!(n1_started_rx.recv_timeout(event_timeout), Ok(1));
        release_gate(&record_gate);
        assert_eq!(n2_seen_rx.recv_timeout(event_timeout), Ok(1));
        assert_eq!(n3_seen_rx.recv_timeout(event_timeout), Ok(1));

        let (b_done_tx, b_done_rx) = mpsc::sync_channel(1);
        let b_consensus = Arc::clone(&consensus);
        let caller_b = thread::spawn(move || {
            b_done_tx
                .send(b_consensus.record_broadcast_with_context(
                    record_requests(&b_consensus, 2),
                    RecorderRpcContext::with_timeout(event_timeout),
                    &AtomicBool::new(false),
                ))
                .unwrap();
        });
        assert_eq!(n2_seen_rx.recv_timeout(event_timeout), Ok(2));
        assert_eq!(n3_seen_rx.recv_timeout(event_timeout), Ok(2));
        let b_replies = b_done_rx
            .recv_timeout(event_timeout)
            .expect("B must reclaim its queued n1 hedge after the n2+n3 quorum")
            .expect("the frozen quorum must reclaim B's queued n1 job without waiting for B's W");
        let b_recorder_ids: HashSet<_> = b_replies
            .iter()
            .map(|reply| reply.recorder_id.clone())
            .collect();
        assert_eq!(b_recorder_ids, HashSet::from(["n2".into(), "n3".into()]));
        assert_eq!(
            a_done_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty),
            "A must remain blocked on its running n1 hedge while B reclaims only B's queued hedge"
        );
        caller_b.join().unwrap();
        #[cfg(feature = "test-hooks")]
        {
            assert_eq!(queued_hedge_probe.pending(), 0);
            assert_eq!(queued_hedge_probe.outstanding(), 0);
            assert_eq!(queued_hedge_probe.dispatch_count(), 3);
            assert_eq!(queued_hedge_probe.drained_count(), 3);
            let transitions = queued_hedge_probe.worker_transitions();
            assert!(
                transitions.iter().any(|transition| {
                    transition.pruned == 1
                        && transition.live_leases == 0
                        && transition.enqueued == transition.completion_dropped
                }),
                "the queued post-quorum hedge must be pruned and drop its exact lease: {transitions:?}"
            );
            assert!(transitions.iter().all(|transition| {
                transition.live_leases == 0 && transition.enqueued == transition.completion_dropped
            }));
        }
        assert_eq!(n1_started_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert_eq!(
            consensus.record_workers[0]
                .state
                .pending
                .load(Ordering::Acquire),
            1,
            "only A's running n1 job remains leased"
        );
        assert!(!consensus.record_workers[0]
            .state
            .quarantined
            .load(Ordering::Acquire));

        assert!(caller_a_guard.finish());
        let a_replies = a_done_rx
            .recv_timeout(event_timeout)
            .expect("A must finish after its running n1 hedge is released")
            .expect("A frozen quorum must succeed");
        let a_recorder_ids: HashSet<_> = a_replies
            .iter()
            .map(|reply| reply.recorder_id.clone())
            .collect();
        assert_eq!(a_recorder_ids, HashSet::from(["n2".into(), "n3".into()]));
        assert!(matches!(
            consensus.record_broadcast_with_context(
                record_requests(&consensus, 3),
                RecorderRpcContext::with_timeout(event_timeout),
                &AtomicBool::new(false),
            ),
            Ok(replies) if replies.len() == 2
        ));
        let recovery_request = record_requests(&consensus, 4).remove(0);
        let recovery_expected = record_summary("n1", recovery_request.clone());
        let (recovered_tx, recovered_rx) = mpsc::sync_channel(1);
        assert!(
            matches!(
                consensus.record_workers[0].dispatch(super::RecordJob {
                    index: 0,
                    context: RecorderRpcContext::with_timeout(event_timeout),
                    request: recovery_request,
                    result: recovered_tx,
                }),
                super::RecordDispatch::Accepted
            ),
            "released n1 must accept a later direct record"
        );
        assert_eq!(
            recovered_rx.recv_timeout(event_timeout),
            Ok((0, Ok(recovery_expected))),
            "released n1 must complete a later direct record",
        );
        assert!(consensus.finish_pending_rpcs(event_timeout));
    }

    #[test]
    fn record_work_deadline_cancels_all_admitted_workers_and_reuses_them_before_root_deadline() {
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let (cancelled_tx, cancelled_rx) = mpsc::sync_channel(3);
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(WorkDeadlineCancellationRecorder {
                        recorder_id,
                        started: started_tx.clone(),
                        cancelled: cancelled_tx.clone(),
                        calls: AtomicUsize::new(0),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                let mutation_started = AtomicBool::new(false);
                done_tx
                    .send(consensus.record_broadcast_with_context(
                        record_requests(&consensus, 1),
                        RecorderRpcContext::with_timeout(Duration::from_millis(300)),
                        &mutation_started,
                    ))
                    .unwrap();
            })
        };
        let mut started = HashSet::new();
        for _ in 0..3 {
            started.insert(started_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        }
        assert_eq!(started.len(), 3);
        let mut cancelled = HashSet::new();
        for _ in 0..3 {
            cancelled.insert(cancelled_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        }
        assert_eq!(cancelled, started);
        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(1)),
            Ok(Err(Error::UnknownOutcome))
        );
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        assert!(consensus.record_workers.iter().all(|worker| {
            worker.state.pending.load(Ordering::Acquire) == 0
                && !worker.state.quarantined.load(Ordering::Acquire)
        }));
        let replies = consensus
            .record_broadcast_with_context(
                record_requests(&consensus, 2),
                RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                &AtomicBool::new(false),
            )
            .unwrap();
        assert_eq!(replies.len(), 2);
    }

    #[test]
    fn record_worker_exit_after_pop_preserves_exact_quorum_and_quarantines_worker() {
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&release));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n1",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(GatedRecordRecorder {
                    recorder_id: "n2",
                    release: Arc::clone(&release),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(GatedRecordRecorder {
                    recorder_id: "n3",
                    release: Arc::clone(&release),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        #[cfg(feature = "test-hooks")]
        let worker_exit_probe = Arc::new(TestControlOperationProbe::default());
        #[cfg(feature = "test-hooks")]
        let _worker_exit_probe = consensus
            .install_test_record_operation_probe(1, Arc::clone(&worker_exit_probe))
            .unwrap();
        let (popped_tx, popped_rx) = mpsc::sync_channel(1);
        let _panic =
            super::arm_record_worker_panic_after_pop(&consensus.record_workers[0].state, popped_tx);
        let mutation_started = Arc::new(AtomicBool::new(false));
        let caller = {
            let consensus = Arc::new(consensus);
            let caller = Arc::clone(&consensus);
            let mutation_started = Arc::clone(&mutation_started);
            (
                consensus,
                thread::spawn(move || {
                    caller.record_broadcast_with_context(
                        record_requests(&caller, 1),
                        RecorderRpcContext::with_timeout(Duration::from_secs(10)),
                        &mutation_started,
                    )
                }),
            )
        };
        assert_eq!(popped_rx.recv_timeout(Duration::from_secs(5)), Ok(()));
        release_gate(&release);
        let replies = caller.1.join().unwrap().unwrap();
        assert_eq!(replies.len(), 2);
        assert!(replies.iter().all(|reply| reply.recorder_id != "n1"));
        assert!(mutation_started.load(Ordering::Acquire));
        // This observes a worker thread, so use the suite's hang-sized budget rather than the
        // RPC's one-second semantic deadline. A loaded test host may not schedule the worker
        // before that RPC deadline even though the resolver remains correct.
        let quarantine_deadline = Instant::now() + Duration::from_secs(10);
        while !caller
            .0
            .record_workers
            .iter()
            .any(|worker| worker.state.quarantined.load(Ordering::Acquire))
            && Instant::now() < quarantine_deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        let quarantined: Vec<_> = caller
            .0
            .record_workers
            .iter()
            .enumerate()
            .filter_map(|(index, worker)| {
                worker
                    .state
                    .quarantined
                    .load(Ordering::Acquire)
                    .then_some(index)
            })
            .collect();
        assert_eq!(quarantined.len(), 1);
        assert!(caller.0.finish_pending_rpcs(Duration::from_secs(1)));
        #[cfg(feature = "test-hooks")]
        {
            assert_eq!(worker_exit_probe.pending(), 0);
            assert_eq!(worker_exit_probe.outstanding(), 0);
            assert_eq!(worker_exit_probe.dispatch_count(), 3);
            assert_eq!(worker_exit_probe.drained_count(), 3);
            assert!(
                worker_exit_probe
                    .worker_transitions()
                    .iter()
                    .all(|transition| {
                        transition.live_leases == 0
                            && transition.enqueued == transition.completion_dropped
                            // The panic unwinds and drops its lease before
                            // the worker wrapper observes quarantine, so no
                            // group-scoped quarantine attribution exists.
                            && transition.quarantined == 0
                    }),
                "a popped worker panic must drop every group lease before quarantine is observable: {:?}",
                worker_exit_probe.worker_transitions(),
            );
        }

        let replies = caller
            .0
            .record_broadcast_with_context(
                record_requests(&caller.0, 2),
                RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                &AtomicBool::new(false),
            )
            .unwrap();
        assert_eq!(replies.len(), 2);
        assert!(replies
            .iter()
            .all(|reply| reply.recorder_id != caller.0.membership().members()[quarantined[0]]));
    }

    #[test]
    fn record_quorum_result_is_independent_of_the_first_two_reply_arrivals() {
        for first_two in [[0, 1], [1, 0]] {
            let (n1_started_tx, n1_started_rx) = mpsc::sync_channel(1);
            let (n2_started_tx, n2_started_rx) = mpsc::sync_channel(1);
            let (n3_started_tx, n3_started_rx) = mpsc::sync_channel(1);
            let (n1_release_tx, n1_release_rx) = mpsc::sync_channel(1);
            let (n2_release_tx, n2_release_rx) = mpsc::sync_channel(1);
            let (_n3_release_tx, n3_release_rx) = mpsc::sync_channel(1);
            let recorders = vec![
                (
                    "n1".into(),
                    Box::new(BlockingRecorder {
                        recorder_id: "n1",
                        started: n1_started_tx,
                        release_first: Mutex::new(n1_release_rx),
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n2".into(),
                    Box::new(BlockingRecorder {
                        recorder_id: "n2",
                        started: n2_started_tx,
                        release_first: Mutex::new(n2_release_rx),
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n3".into(),
                    Box::new(BlockingRecorder {
                        recorder_id: "n3",
                        started: n3_started_tx,
                        release_first: Mutex::new(n3_release_rx),
                    }) as Box<dyn RecorderRpc>,
                ),
            ];
            let consensus = Arc::new(
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap(),
            );
            let (done_tx, done_rx) = mpsc::sync_channel(1);
            let caller = {
                let consensus = Arc::clone(&consensus);
                thread::spawn(move || {
                    done_tx
                        .send(consensus.record_broadcast_with_context(
                            record_requests(&consensus, 1),
                            RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                            &AtomicBool::new(false),
                        ))
                        .unwrap();
                })
            };
            assert_eq!(n1_started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
            assert_eq!(n2_started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
            assert_eq!(n3_started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
            for worker in first_two {
                match worker {
                    0 => n1_release_tx.send(()).unwrap(),
                    1 => n2_release_tx.send(()).unwrap(),
                    _ => unreachable!(),
                }
            }
            let replies = done_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap();
            let mut ids: Vec<_> = replies
                .iter()
                .map(|reply| reply.recorder_id.as_str())
                .collect();
            ids.sort_unstable();
            assert_eq!(ids, ["n1", "n2"]);
            caller.join().unwrap();
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        }
    }

    #[test]
    fn caller_cancellation_while_proposal_is_pending_after_admission_is_unknown() {
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let mut releases = Vec::new();
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                let (release_tx, release_rx) = mpsc::sync_channel(1);
                releases.push(release_tx);
                (
                    recorder_id.into(),
                    Box::new(BlockingRecorder {
                        recorder_id,
                        started: started_tx.clone(),
                        release_first: Mutex::new(release_rx),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let context = RecorderRpcContext::with_timeout(Duration::from_secs(5));
        let proposer = {
            let consensus = Arc::clone(&consensus);
            let context = context.clone();
            thread::spawn(move || {
                consensus.propose_at(
                    context,
                    1,
                    LogHash::ZERO,
                    Command::new(CommandKind::Deterministic, b"pending".to_vec()),
                )
            })
        };
        for _ in 0..3 {
            assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        }
        context.cancel();
        assert_eq!(proposer.join().unwrap(), Err(Error::UnknownOutcome));
        for release in releases {
            let _ = release.send(());
        }
    }

    #[test]
    fn read_timeouts_never_become_unknown_outcomes() {
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ReadTimeoutRecorder) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        assert_eq!(
            consensus.inspect_decision_proof_at(&RecorderRpcContext::default_timeout(), 1),
            Err(Error::NoQuorum)
        );
        assert_eq!(
            consensus.inspect_context_read_fence_at(
                &RecorderRpcContext::default_timeout(),
                1,
                LogHash::ZERO
            ),
            Ok(CertifiedDecisionInspection::Unavailable)
        );
    }

    #[test]
    fn record_request_requires_the_current_command_field() {
        let request = RecordRequest {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            slot: 1,
            step: 4,
            proposal: Proposal::new(
                ProposalPriority::MAX,
                "n1",
                1,
                AcceptedValue {
                    command_hash: LogHash::ZERO,
                    prev_hash: LogHash::ZERO,
                    entry_hash: LogHash::ZERO,
                },
            ),
            command: None,
        };
        let mut encoded = serde_json::to_value(request).unwrap();
        encoded.as_object_mut().unwrap().remove("command");

        assert!(serde_json::from_value::<RecordRequest>(encoded).is_err());
    }

    fn record_summary(recorder_id: &str, request: RecordRequest) -> RecordSummary {
        RecordSummary {
            recorder_id: recorder_id.into(),
            slot: request.slot,
            config_id: request.config_id,
            config_digest: request.config_digest,
            step: request.step,
            first_current: Some(request.proposal),
            aggregate_prior: None,
            decided: None,
        }
    }

    struct ThreadRecordingRecorder {
        recorder_id: &'static str,
        threads: Arc<Mutex<HashSet<thread::ThreadId>>>,
    }

    impl RecorderRpc for ThreadRecordingRecorder {
        fn record(
            &self,
            _context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            self.threads.lock().unwrap().insert(thread::current().id());
            Ok(record_summary(self.recorder_id, request))
        }
    }

    struct ThreadRecordingControlRecorder {
        threads: Arc<Mutex<HashSet<thread::ThreadId>>>,
    }

    impl RecorderRpc for ThreadRecordingControlRecorder {
        fn inspect_decision_proof(
            &self,
            _context: &RecorderRpcContext,
            _slot: u64,
        ) -> super::Result<Option<super::DecisionProof>> {
            self.threads.lock().unwrap().insert(thread::current().id());
            Ok(None)
        }
    }

    struct ContextRecordingRecorder {
        recorder_id: &'static str,
        record_contexts: mpsc::SyncSender<Instant>,
        control_contexts: mpsc::SyncSender<Instant>,
        record_error: Option<Error>,
    }

    struct GatedContextRecorder {
        recorder_id: &'static str,
        contexts: mpsc::SyncSender<Instant>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    #[cfg(feature = "test-hooks")]
    struct CancellationSafetyRecorder {
        group_cancellation: mpsc::SyncSender<Arc<AtomicBool>>,
    }

    #[cfg(feature = "test-hooks")]
    impl RecorderRpc for CancellationSafetyRecorder {
        fn record(
            &self,
            context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            self.group_cancellation
                .send(
                    context
                        .cancellations
                        .last()
                        .cloned()
                        .expect("record worker context carries its group cancellation"),
                )
                .unwrap();
            while !context.is_cancelled() {
                thread::yield_now();
            }
            Err(Error::ChainConflict {
                slot: request.slot,
                expected_prev_hash: LogHash::ZERO,
                actual_prev_hash: LogHash::ZERO,
            })
        }
    }

    impl RecorderRpc for GatedContextRecorder {
        fn record(
            &self,
            context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            self.contexts.send(context.deadline()).unwrap();
            let (released, condition) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
            Ok(record_summary(self.recorder_id, request))
        }
    }

    impl RecorderRpc for ContextRecordingRecorder {
        fn record(
            &self,
            context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            self.record_contexts.send(context.deadline()).unwrap();
            match &self.record_error {
                Some(error) => Err(error.clone()),
                None => Ok(record_summary(self.recorder_id, request)),
            }
        }

        fn inspect_decision_proof(
            &self,
            context: &RecorderRpcContext,
            _slot: Slot,
        ) -> super::Result<Option<DecisionProof>> {
            self.control_contexts.send(context.deadline()).unwrap();
            Ok(None)
        }
    }

    struct ReadTimeoutRecorder;

    impl RecorderRpc for ReadTimeoutRecorder {
        fn inspect_decision_proof(
            &self,
            _context: &RecorderRpcContext,
            _slot: Slot,
        ) -> super::Result<Option<DecisionProof>> {
            Err(Error::RpcDeadlineExceeded)
        }

        fn supports_context_read_fence(&self) -> bool {
            true
        }

        fn observe_read_fence(
            &self,
            _context: &RecorderRpcContext,
            _request: ReadFenceRequest,
        ) -> super::Result<ReadFenceObservation> {
            Err(Error::RpcDeadlineExceeded)
        }
    }

    struct BlockingControlRecorder {
        recorder_id: &'static str,
        started: mpsc::SyncSender<u64>,
        release_first: Mutex<mpsc::Receiver<()>>,
    }

    struct GateProofRecorder {
        recorder_id: &'static str,
        started: mpsc::SyncSender<&'static str>,
        exited: Option<mpsc::SyncSender<&'static str>>,
        release: Option<Arc<(Mutex<bool>, Condvar)>>,
        calls: Arc<AtomicUsize>,
    }

    impl RecorderRpc for GateProofRecorder {
        fn inspect_decision_proof(
            &self,
            _context: &RecorderRpcContext,
            _slot: Slot,
        ) -> super::Result<Option<DecisionProof>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.started.send(self.recorder_id).unwrap();
            if let Some(release) = &self.release {
                let (released, condition) = &**release;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            if let Some(exited) = &self.exited {
                exited.send(self.recorder_id).unwrap();
            }
            Ok(None)
        }
    }

    struct GateRelease {
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl GateRelease {
        fn new(gate: Arc<(Mutex<bool>, Condvar)>) -> Self {
            Self { gate }
        }
    }

    impl Drop for GateRelease {
        fn drop(&mut self) {
            release_gate(&self.gate);
        }
    }

    /// Ensures channel-gated worker tests never leave a blocked caller or
    /// detached worker behind when an assertion fails midway through a test.
    struct ChannelReleaseAndJoin {
        releases: Vec<mpsc::SyncSender<()>>,
        caller: Option<thread::JoinHandle<()>>,
    }

    impl ChannelReleaseAndJoin {
        fn new(releases: Vec<mpsc::SyncSender<()>>, caller: thread::JoinHandle<()>) -> Self {
            Self {
                releases,
                caller: Some(caller),
            }
        }

        fn finish(mut self) -> bool {
            self.release_and_join()
        }

        fn release_and_join(&mut self) -> bool {
            for release in self.releases.drain(..) {
                let _ = release.send(());
            }
            let deadline = Instant::now() + Duration::from_secs(1);
            while self
                .caller
                .as_ref()
                .is_some_and(|caller| !caller.is_finished())
                && Instant::now() < deadline
            {
                thread::yield_now();
            }
            let Some(caller) = self.caller.take() else {
                return true;
            };
            if caller.is_finished() {
                caller.join().unwrap();
                true
            } else {
                // Drop detaches only after every test gate was released and
                // the bounded grace period elapsed; Drop itself never blocks.
                drop(caller);
                false
            }
        }
    }

    impl Drop for ChannelReleaseAndJoin {
        fn drop(&mut self) {
            let _ = self.release_and_join();
        }
    }

    #[test]
    fn channel_release_and_join_unwinds_without_leaking_a_gated_caller() {
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let (exited_tx, exited_rx) = mpsc::sync_channel(1);
        let active = Arc::new(AtomicUsize::new(1));
        let caller_active = Arc::clone(&active);
        let caller = thread::spawn(move || {
            let _ = release_rx.recv();
            caller_active.store(0, Ordering::Release);
            exited_tx.send(()).unwrap();
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = ChannelReleaseAndJoin::new(vec![release_tx], caller);
            panic!("injected assertion-path unwind");
        }));
        assert!(result.is_err());
        assert_eq!(exited_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
        assert_eq!(active.load(Ordering::Acquire), 0);
    }

    struct ScriptedProofRecorder {
        recorder_id: &'static str,
        started: mpsc::SyncSender<&'static str>,
        gate: Option<Arc<(Mutex<bool>, Condvar)>>,
        reply: super::Result<Option<DecisionProof>>,
    }

    impl RecorderRpc for ScriptedProofRecorder {
        fn inspect_decision_proof(
            &self,
            _context: &RecorderRpcContext,
            _slot: Slot,
        ) -> super::Result<Option<DecisionProof>> {
            self.started.send(self.recorder_id).unwrap();
            if let Some(gate) = &self.gate {
                let (released, condition) = &**gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            self.reply.clone()
        }
    }

    struct ScriptedSummaryRecorder {
        recorder_id: &'static str,
        entered: mpsc::SyncSender<&'static str>,
        gate: Option<Arc<(Mutex<bool>, Condvar)>>,
        reply: super::Result<Option<RecordSummary>>,
    }

    impl RecorderRpc for ScriptedSummaryRecorder {
        fn inspect_record_summary(
            &self,
            _context: &RecorderRpcContext,
            _slot: Slot,
        ) -> super::Result<Option<RecordSummary>> {
            self.entered.send(self.recorder_id).unwrap();
            if let Some(gate) = &self.gate {
                let (released, condition) = &**gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            self.reply.clone()
        }
    }

    struct ScriptedSummaryFetchRecorder {
        recorder_id: &'static str,
        entered: mpsc::SyncSender<&'static str>,
        gate: Option<Arc<(Mutex<bool>, Condvar)>>,
        cancellation_observed: Option<mpsc::SyncSender<bool>>,
        summary: RecordSummary,
        command: StoredCommand,
    }

    impl RecorderRpc for ScriptedSummaryFetchRecorder {
        fn inspect_record_summary(
            &self,
            _context: &RecorderRpcContext,
            _slot: Slot,
        ) -> super::Result<Option<RecordSummary>> {
            self.entered.send(self.recorder_id).unwrap();
            if let Some(gate) = &self.gate {
                let (released, condition) = &**gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            if let Some(cancellation_observed) = &self.cancellation_observed {
                cancellation_observed.send(_context.is_cancelled()).unwrap();
            }
            Ok(Some(self.summary.clone()))
        }

        fn fetch_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            Ok(Some(self.command.clone()))
        }
    }

    struct BlockingQueuedSummaryFetchRecorder {
        blocker_slot: Slot,
        blocker_started: mpsc::SyncSender<()>,
        blocker_gate: Arc<(Mutex<bool>, Condvar)>,
        summary_started: mpsc::SyncSender<()>,
        summary: RecordSummary,
        command: StoredCommand,
    }

    impl RecorderRpc for BlockingQueuedSummaryFetchRecorder {
        fn inspect_record_summary(
            &self,
            _context: &RecorderRpcContext,
            slot: Slot,
        ) -> super::Result<Option<RecordSummary>> {
            if slot == self.blocker_slot {
                self.blocker_started.send(()).unwrap();
                let (released, condition) = &*self.blocker_gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
                return Ok(None);
            }
            self.summary_started.send(()).unwrap();
            Ok(Some(self.summary.clone()))
        }

        fn fetch_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            Ok(Some(self.command.clone()))
        }
    }

    struct ScriptedFetchRecorder {
        recorder_id: &'static str,
        entered: mpsc::SyncSender<&'static str>,
        gate: Option<Arc<(Mutex<bool>, Condvar)>>,
        reply: super::Result<Option<StoredCommand>>,
    }

    impl RecorderRpc for ScriptedFetchRecorder {
        fn fetch_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            self.entered.send(self.recorder_id).unwrap();
            if let Some(gate) = &self.gate {
                let (released, condition) = &**gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            self.reply.clone()
        }
    }

    /// Deliberately does not inspect cancellation while gated.  It models a
    /// transport stuck below Rhiza's cancellation boundary, which is the case
    /// the effect-fetch group must contain without starving healthy peers.
    struct ScriptedEffectFetchRecorder {
        manifest: super::Result<Option<StoredCommand>>,
        chunks: Vec<super::Result<Option<Vec<u8>>>>,
        manifest_gate: Option<Arc<(Mutex<bool>, Condvar)>>,
        manifest_started: Option<mpsc::SyncSender<()>>,
        chunk_gate: Option<Arc<(Mutex<bool>, Condvar)>>,
    }

    impl RecorderRpc for ScriptedEffectFetchRecorder {
        fn fetch_effect_bundle_manifest(
            &self,
            _context: &RecorderRpcContext,
            _binding: EffectBundleBinding,
        ) -> super::Result<Option<StoredCommand>> {
            if let Some(started) = &self.manifest_started {
                let _ = started.send(());
            }
            if let Some(gate) = &self.manifest_gate {
                let (released, condition) = &**gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            self.manifest.clone()
        }

        fn fetch_effect_bundle_chunk(
            &self,
            _context: &RecorderRpcContext,
            _binding: EffectBundleBinding,
            ordinal: u16,
        ) -> super::Result<Option<Vec<u8>>> {
            if let Some(gate) = &self.chunk_gate {
                let (released, condition) = &**gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            self.chunks
                .get(ordinal as usize)
                .cloned()
                .unwrap_or(Ok(None))
        }
    }

    fn effect_fetch_fixture() -> (Membership, RecorderEffectBundle, StoredCommand) {
        effect_fetch_fixture_with_chunks(vec![
            b"first-effect-chunk".to_vec(),
            b"second-effect-chunk".to_vec(),
        ])
    }

    fn effect_fetch_fixture_with_chunks(
        chunks: Vec<Vec<u8>>,
    ) -> (Membership, RecorderEffectBundle, StoredCommand) {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let qefx = ExternalEffectCommand::from_profile_bytes_and_chunks(
            "effect-fetch-cluster",
            1,
            1,
            membership.digest(),
            9,
            LogHash::digest(&[b"effect-fetch-prev"]),
            ExternalEffectProfile::sql(vec![0x5a]),
            &chunks,
        )
        .unwrap();
        let manifest = StoredCommand::new(EntryType::Command, qefx.encode().unwrap());
        let binding = EffectBundleBinding {
            cluster_id: qefx.cluster_id().into(),
            epoch: qefx.epoch(),
            config_id: qefx.config_id(),
            config_digest: qefx.config_digest(),
            intended_slot: qefx.intended_slot(),
            prev_hash: qefx.prev_hash(),
            manifest_command_hash: manifest.hash(),
            effect_digest: qefx.effect_digest_value(),
        };
        let bundle = RecorderEffectBundle::new(binding, chunks).unwrap();
        EffectBundleFinalizeRequest::new(bundle.clone(), manifest.clone()).unwrap();
        (membership, bundle, manifest)
    }

    fn effect_fetch_consensus(recorders: [ScriptedEffectFetchRecorder; 3]) -> ThreeNodeConsensus {
        ThreeNodeConsensus::from_recorders_with_ids(
            "effect-fetch-cluster",
            "n1",
            1,
            1,
            ["n1", "n2", "n3"]
                .into_iter()
                .zip(recorders)
                .map(|(id, recorder)| (id.into(), Box::new(recorder) as Box<dyn RecorderRpc>))
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn effect_fetch_healthy_peers_finish_before_a_slow_manifest_is_drained() {
        let _blocking = lock_blocking_control_tests();
        let (_membership, bundle, manifest) =
            effect_fetch_fixture_with_chunks(vec![b"single-effect-chunk".to_vec()]);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&gate));
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let valid = || -> Vec<super::Result<Option<Vec<u8>>>> {
            bundle
                .chunks()
                .iter()
                .cloned()
                .map(|chunk| Ok(Some(chunk)))
                .collect()
        };
        let consensus = Arc::new(effect_fetch_consensus([
            ScriptedEffectFetchRecorder {
                manifest: Ok(Some(manifest.clone())),
                chunks: valid(),
                manifest_gate: Some(Arc::clone(&gate)),
                manifest_started: Some(started_tx),
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                manifest: Ok(Some(manifest.clone())),
                chunks: valid(),
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                manifest: Ok(Some(manifest.clone())),
                chunks: valid(),
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
        ]));
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            let binding = bundle.binding().clone();
            let caller_manifest = manifest.clone();
            thread::spawn(move || {
                result_tx
                    .send(consensus.resolve_effect_bundle_from_quorum(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        &binding,
                        &caller_manifest,
                    ))
                    .unwrap();
            })
        };
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let healthy_deadline = Instant::now() + Duration::from_millis(250);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Ok(Some(bundle.clone()))
        );
        assert!(
            Instant::now() < healthy_deadline,
            "a verified healthy effect bundle must not wait for the slow read's caller deadline"
        );
        caller.join().unwrap();
        assert!(consensus.read_fence_workers[0]
            .state
            .quarantined
            .load(Ordering::Acquire));
        release_gate(&gate);
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        // The detached worker's late reply belongs only to the old group's
        // private receiver.  A fresh fetch must obtain a clean bundle from
        // the surviving pair without inherited replies or queued work.
        assert_eq!(
            consensus.resolve_effect_bundle_from_quorum(
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                bundle.binding(),
                &manifest,
            ),
            Ok(Some(bundle))
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn effect_fetch_retries_sources_saturated_by_the_preceding_phase() {
        let _blocking = lock_blocking_control_tests();
        let (_membership, bundle, manifest) =
            effect_fetch_fixture_with_chunks(vec![b"single-effect-chunk".to_vec()]);
        let valid = || -> Vec<super::Result<Option<Vec<u8>>>> {
            bundle
                .chunks()
                .iter()
                .cloned()
                .map(|chunk| Ok(Some(chunk)))
                .collect()
        };
        let consensus = Arc::new(effect_fetch_consensus([
            ScriptedEffectFetchRecorder {
                manifest: Ok(None),
                chunks: vec![Ok(None)],
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                manifest: Ok(Some(manifest.clone())),
                chunks: valid(),
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                manifest: Ok(Some(manifest.clone())),
                chunks: valid(),
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
        ]));

        // Occupy both healthy workers and fill each one-entry queue. The
        // resolver initially reaches only the missing hedge. Once the prior
        // work drains it must retry the saturated healthy sources rather than
        // returning a false EffectBundleUnavailable.
        let pause = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&pause));
        let (entered_tx, entered_rx) = mpsc::sync_channel(2);
        let (dummy_tx, _dummy_rx) = mpsc::sync_channel(4);
        for index in 1..=2 {
            consensus.read_fence_workers[index]
                .pause_after_next_pop(entered_tx.clone(), Arc::clone(&pause));
            assert_eq!(
                consensus.read_fence_workers[index].dispatch(
                    super::ControlJob::FetchEffectBundle {
                        index,
                        context: RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        binding: bundle.binding().clone(),
                        kind: super::EffectFetchKind::Manifest,
                        result: dummy_tx.clone(),
                    }
                ),
                super::ControlDispatch::Accepted
            );
        }
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        for index in 1..=2 {
            assert_eq!(
                consensus.read_fence_workers[index].dispatch(
                    super::ControlJob::FetchEffectBundle {
                        index,
                        context: RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        binding: bundle.binding().clone(),
                        kind: super::EffectFetchKind::Manifest,
                        result: dummy_tx.clone(),
                    }
                ),
                super::ControlDispatch::Accepted
            );
        }

        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            let binding = bundle.binding().clone();
            let caller_manifest = manifest.clone();
            thread::spawn(move || {
                result_tx
                    .send(consensus.resolve_effect_bundle_from_quorum(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        &binding,
                        &caller_manifest,
                    ))
                    .unwrap();
            })
        };
        assert!(result_rx.recv_timeout(Duration::from_millis(50)).is_err());
        release_gate(&pause);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Ok(Some(bundle))
        );
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn effect_fetch_quarantines_corrupt_source_but_uses_valid_hedge() {
        let _blocking = lock_blocking_control_tests();
        // One chunk isolates corrupt-source quarantine from later-ordinal scheduling. Multi-chunk
        // sequencing is covered separately by the healthy/loss/cancellation fetch tests.
        let (_membership, bundle, manifest) =
            effect_fetch_fixture_with_chunks(vec![b"effect-fetch-chunk".to_vec()]);
        let valid_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&valid_gate));
        let valid = || -> Vec<super::Result<Option<Vec<u8>>>> {
            bundle
                .chunks()
                .iter()
                .cloned()
                .map(|chunk| Ok(Some(chunk)))
                .collect()
        };
        let mut corrupt = valid();
        corrupt[0] = Ok(Some(b"corrupt-effect-chunk".to_vec()));
        let consensus = effect_fetch_consensus([
            ScriptedEffectFetchRecorder {
                manifest: Ok(None),
                chunks: vec![Ok(None)],
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                manifest: Ok(Some(manifest.clone())),
                chunks: corrupt,
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                // The healthy hedge intentionally has only the chunk. This makes the corrupt
                // source the sole manifest source, so its chunk response must be observed and
                // quarantined before the gated healthy chunk can complete the fetch.
                manifest: Ok(None),
                chunks: valid(),
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: Some(Arc::clone(&valid_gate)),
            },
        ]);
        let consensus = Arc::new(consensus);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            let binding = bundle.binding().clone();
            thread::spawn(move || {
                result_tx
                    .send(consensus.resolve_effect_bundle_from_quorum(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        &binding,
                        &manifest,
                    ))
                    .unwrap();
            })
        };
        let quarantine_deadline = Instant::now() + Duration::from_secs(10);
        while !consensus.read_fence_workers[1]
            .state
            .quarantined
            .load(Ordering::Acquire)
        {
            assert!(Instant::now() < quarantine_deadline);
            thread::sleep(Duration::from_millis(1));
        }
        release_gate(&valid_gate);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Ok(Some(bundle))
        );
        caller.join().unwrap();
        assert!(consensus.read_fence_workers[1]
            .state
            .quarantined
            .load(Ordering::Acquire));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn effect_fetch_all_bad_sources_return_typed_safety_error() {
        let _blocking = lock_blocking_control_tests();
        let (_membership, bundle, manifest) = effect_fetch_fixture();
        let valid = || -> Vec<super::Result<Option<Vec<u8>>>> {
            bundle
                .chunks()
                .iter()
                .cloned()
                .map(|chunk| Ok(Some(chunk)))
                .collect()
        };
        let mut corrupt = valid();
        corrupt[0] = Ok(Some(b"corrupt-effect-chunk".to_vec()));
        let missing = bundle.chunks().iter().map(|_| Ok(None)).collect();
        let consensus = effect_fetch_consensus([
            ScriptedEffectFetchRecorder {
                manifest: Ok(None),
                chunks: missing,
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                manifest: Ok(Some(manifest.clone())),
                chunks: corrupt,
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                manifest: Ok(Some(manifest.clone())),
                chunks: bundle.chunks().iter().map(|_| Ok(None)).collect(),
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
        ]);
        assert!(matches!(
            consensus.resolve_effect_bundle_from_quorum(
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                bundle.binding(),
                &manifest,
            ),
            Err(Error::EffectBundleInvalid(_))
        ));
        assert!(consensus.read_fence_workers[1]
            .state
            .quarantined
            .load(Ordering::Acquire));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn effect_fetch_survives_one_recorder_loss() {
        let _blocking = lock_blocking_control_tests();
        let (_membership, bundle, manifest) = effect_fetch_fixture();
        let valid = || -> Vec<super::Result<Option<Vec<u8>>>> {
            bundle
                .chunks()
                .iter()
                .cloned()
                .map(|chunk| Ok(Some(chunk)))
                .collect()
        };
        let consensus = effect_fetch_consensus([
            ScriptedEffectFetchRecorder {
                manifest: Err(Error::Io("lost recorder".into())),
                chunks: valid(),
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                manifest: Ok(Some(manifest.clone())),
                chunks: valid(),
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                manifest: Ok(Some(manifest.clone())),
                chunks: valid(),
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
        ]);
        assert_eq!(
            consensus.resolve_effect_bundle_from_quorum(
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                bundle.binding(),
                &manifest,
            ),
            Ok(Some(bundle))
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn effect_fetch_cancellation_prunes_queued_chunks_and_drains_after_release() {
        let _blocking = lock_blocking_control_tests();
        let (_membership, bundle, manifest) = effect_fetch_fixture();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&gate));
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let missing = || bundle.chunks().iter().map(|_| Ok(None)).collect();
        let consensus = Arc::new(effect_fetch_consensus([
            ScriptedEffectFetchRecorder {
                manifest: Ok(Some(manifest.clone())),
                chunks: missing(),
                manifest_gate: Some(Arc::clone(&gate)),
                manifest_started: Some(started_tx),
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                manifest: Ok(None),
                chunks: missing(),
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
            ScriptedEffectFetchRecorder {
                manifest: Ok(None),
                chunks: missing(),
                manifest_gate: None,
                manifest_started: None,
                chunk_gate: None,
            },
        ]));
        let cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancellation),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            let binding = bundle.binding().clone();
            let caller_manifest = manifest.clone();
            thread::spawn(move || {
                result_tx
                    .send(consensus.resolve_effect_bundle_from_quorum(
                        &context,
                        &binding,
                        &caller_manifest,
                    ))
                    .unwrap();
            })
        };
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        cancellation.store(true, Ordering::Release);
        release_gate(&gate);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::RpcCancelled)
        );
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        assert!(consensus
            .read_fence_workers
            .iter()
            .all(ControlWorker::is_idle));
    }

    struct ScriptedCommandStoreRecorder {
        recorder_id: &'static str,
        entered: mpsc::SyncSender<&'static str>,
        gate: Option<Arc<(Mutex<bool>, Condvar)>>,
        reply: super::Result<()>,
        stored: Mutex<Vec<(LogHash, StoredCommand)>>,
    }

    impl RecorderRpc for ScriptedCommandStoreRecorder {
        fn store_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            command_hash: LogHash,
            command: StoredCommand,
        ) -> super::Result<()> {
            self.entered.send(self.recorder_id).unwrap();
            if let Some(gate) = &self.gate {
                let (released, condition) = &**gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            self.reply.clone()?;
            if command.hash() != command_hash {
                return Err(Error::CommandHashMismatch);
            }
            let mut stored = self.stored.lock().unwrap();
            match stored.iter().find(|(hash, _)| *hash == command_hash) {
                Some((_, existing_command)) if *existing_command != command => {
                    Err(Error::CommandHashMismatch)
                }
                Some(_) => Ok(()),
                None => {
                    stored.push((command_hash, command));
                    Ok(())
                }
            }
        }
    }

    struct ScriptedInstallProofRecorder {
        recorder_id: &'static str,
        entered: mpsc::SyncSender<&'static str>,
        gate: Option<Arc<(Mutex<bool>, Condvar)>>,
        reply: super::Result<()>,
    }

    impl RecorderRpc for ScriptedInstallProofRecorder {
        fn install_decision_proof(
            &self,
            _context: &RecorderRpcContext,
            _proof: DecisionProof,
            _membership: &Membership,
        ) -> super::Result<()> {
            self.entered.send(self.recorder_id).unwrap();
            if let Some(gate) = &self.gate {
                let (released, condition) = &**gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            self.reply.clone()
        }
    }

    struct ScriptedFenceRecorder {
        recorder_id: &'static str,
        entered: mpsc::SyncSender<&'static str>,
        gate: Option<Arc<(Mutex<bool>, Condvar)>>,
        reply: super::Result<ReadFenceSlotState>,
    }

    impl RecorderRpc for ScriptedFenceRecorder {
        fn supports_context_read_fence(&self) -> bool {
            true
        }

        fn observe_read_fence(
            &self,
            _context: &RecorderRpcContext,
            request: ReadFenceRequest,
        ) -> super::Result<ReadFenceObservation> {
            self.entered.send(self.recorder_id).unwrap();
            if let Some(gate) = &self.gate {
                let (released, condition) = &**gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            self.reply.clone().map(|slot_state| ReadFenceObservation {
                recorder_id: self.recorder_id.into(),
                cluster_id: request.cluster_id,
                epoch: request.epoch,
                config_id: request.config_id,
                config_digest: request.config_digest,
                slot: request.slot,
                max_head: match slot_state {
                    ReadFenceSlotState::Empty => None,
                    ReadFenceSlotState::Occupied { .. } => Some(request.slot),
                },
                slot_state,
            })
        }
    }

    struct SummaryFetchBudgetRecorder {
        summary: RecordSummary,
        command: StoredCommand,
    }

    impl RecorderRpc for SummaryFetchBudgetRecorder {
        fn supports_context_read_fence(&self) -> bool {
            true
        }

        fn observe_read_fence(
            &self,
            _context: &RecorderRpcContext,
            request: ReadFenceRequest,
        ) -> super::Result<ReadFenceObservation> {
            Ok(ReadFenceObservation {
                recorder_id: self.summary.recorder_id.clone(),
                cluster_id: request.cluster_id,
                epoch: request.epoch,
                config_id: request.config_id,
                config_digest: request.config_digest,
                slot: request.slot,
                max_head: Some(request.slot),
                slot_state: ReadFenceSlotState::Occupied { summary: None },
            })
        }

        fn inspect_record_summary(
            &self,
            _context: &RecorderRpcContext,
            _slot: Slot,
        ) -> super::Result<Option<RecordSummary>> {
            Ok(Some(self.summary.clone()))
        }

        fn fetch_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            Ok(Some(self.command.clone()))
        }

        fn install_decision_proof(
            &self,
            _context: &RecorderRpcContext,
            _proof: DecisionProof,
            _membership: &Membership,
        ) -> super::Result<()> {
            Ok(())
        }
    }

    struct ProofSummaryFetchRecorder {
        recorder_id: &'static str,
        entered: mpsc::SyncSender<&'static str>,
        gate: Arc<(Mutex<bool>, Condvar)>,
        summary: RecordSummary,
        fetch_entries: Arc<AtomicUsize>,
    }

    impl RecorderRpc for ProofSummaryFetchRecorder {
        fn inspect_record_summary(
            &self,
            _context: &RecorderRpcContext,
            _slot: Slot,
        ) -> super::Result<Option<RecordSummary>> {
            self.entered.send(self.recorder_id).unwrap();
            let (released, condition) = &*self.gate;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
            Ok(Some(self.summary.clone()))
        }

        fn fetch_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            self.fetch_entries.fetch_add(1, Ordering::AcqRel);
            Ok(None)
        }
    }

    struct TokenGateProofRecorder {
        recorder_id: &'static str,
        started: mpsc::SyncSender<&'static str>,
        group_token: mpsc::SyncSender<Arc<AtomicBool>>,
        gate: Arc<(Mutex<bool>, Condvar)>,
        exited: Option<mpsc::SyncSender<&'static str>>,
    }

    impl RecorderRpc for TokenGateProofRecorder {
        fn inspect_decision_proof(
            &self,
            context: &RecorderRpcContext,
            _slot: Slot,
        ) -> super::Result<Option<DecisionProof>> {
            self.group_token
                .send(Arc::clone(
                    &context.cancellations[context.cancellations.len() - 2],
                ))
                .unwrap();
            self.started.send(self.recorder_id).unwrap();
            let (released, condition) = &*self.gate;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
            if let Some(exited) = &self.exited {
                exited.send(self.recorder_id).unwrap();
            }
            Ok(None)
        }
    }

    struct BlockingInspectionReadFenceRecorder {
        recorder_id: &'static str,
        block_inspection: bool,
        started: mpsc::SyncSender<&'static str>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    struct BlockingCommandStoreRecorder {
        started: mpsc::SyncSender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    struct SuccessfulCommandStoreRecorder;

    struct UnknownCommandStoreRecorder;

    impl RecorderRpc for UnknownCommandStoreRecorder {
        fn store_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
            _command: StoredCommand,
        ) -> super::Result<()> {
            Err(Error::UnknownOutcome)
        }
    }

    impl RecorderRpc for SuccessfulCommandStoreRecorder {
        fn store_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
            _command: StoredCommand,
        ) -> super::Result<()> {
            Ok(())
        }
    }

    struct FailingCommandStoreRecorder;

    impl RecorderRpc for FailingCommandStoreRecorder {
        fn store_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
            _command: StoredCommand,
        ) -> super::Result<()> {
            Err(Error::ProposeFailed)
        }
    }

    impl RecorderRpc for BlockingCommandStoreRecorder {
        fn store_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
            _command: StoredCommand,
        ) -> super::Result<()> {
            self.started.send(()).unwrap();
            let (released, condition) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
            Ok(())
        }
    }

    impl RecorderRpc for BlockingControlRecorder {
        fn record(
            &self,
            _context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            Ok(record_summary(self.recorder_id, request))
        }

        fn inspect_decision_proof(
            &self,
            _context: &RecorderRpcContext,
            slot: u64,
        ) -> super::Result<Option<super::DecisionProof>> {
            self.started.send(slot).unwrap();
            if slot == 1 {
                self.release_first.lock().unwrap().recv().unwrap();
            }
            Ok(None)
        }
    }

    #[test]
    fn control_worker_reclaims_a_cancelled_queued_job_for_the_next_operation() {
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let recorder = Arc::new(BlockingControlRecorder {
            recorder_id: "n1",
            started: started_tx,
            release_first: Mutex::new(release_rx),
        });
        let worker = ControlWorker::spawn(recorder).unwrap();
        let (result_tx, result_rx) = mpsc::sync_channel(3);

        assert!(matches!(
            worker.dispatch(ControlJob::InspectProof {
                index: 1,
                context: RecorderRpcContext::default_timeout(),
                slot: 1,
                result: result_tx.clone(),
            }),
            ControlDispatch::Accepted
        ));
        assert_eq!(started_rx.recv().unwrap(), 1);

        let cancellation = ControlJobCancellation::new();
        assert!(matches!(
            worker.dispatch_cancellable(
                ControlJob::InspectProof {
                    index: 2,
                    context: RecorderRpcContext::default_timeout(),
                    slot: 2,
                    result: result_tx.clone(),
                },
                cancellation.token(),
            ),
            ControlDispatch::Accepted
        ));
        drop(cancellation);

        assert!(matches!(
            worker.dispatch(ControlJob::InspectProof {
                index: 3,
                context: RecorderRpcContext::default_timeout(),
                slot: 3,
                result: result_tx,
            }),
            ControlDispatch::Accepted
        ));

        release_tx.send(()).unwrap();
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 3);
        assert!(started_rx.try_recv().is_err());
        assert_eq!(result_rx.recv().unwrap().0, 1);
        assert_eq!(result_rx.recv().unwrap().0, 3);
    }

    fn release_gate(gate: &Arc<(Mutex<bool>, Condvar)>) {
        let (released, condition) = &**gate;
        *released.lock().unwrap() = true;
        condition.notify_all();
    }

    fn wait_for_control_worker_idle(worker: &ControlWorker, label: &str) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while !worker.is_idle() {
            assert!(
                Instant::now() < deadline,
                "{label} must send its reply and complete before the second gate opens"
            );
            thread::yield_now();
        }
    }

    fn valid_inspected_proof(membership: &Membership, command_hash: LogHash) -> DecisionProof {
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue {
                command_hash,
                prev_hash: LogHash::ZERO,
                entry_hash: LogHash::ZERO,
            },
        );
        let summaries = membership.members()[..membership.quorum_size()]
            .iter()
            .map(|recorder_id| RecorderSummary {
                recorder_id: recorder_id.clone(),
                slot: 1,
                step: 4,
                first_current: Some(proposal.clone()),
                aggregate_prior: None,
            })
            .collect();
        DecisionProof::FastPath {
            cluster_id: "cluster".into(),
            slot: 1,
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            proposal,
            summaries,
        }
    }

    #[test]
    fn proof_quorum_waits_for_every_admitted_group_job() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let (exited_tx, exited_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&release));
        let calls = Arc::new(AtomicUsize::new(0));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(GateProofRecorder {
                    recorder_id: "n1",
                    started: started_tx.clone(),
                    exited: None,
                    release: Some(Arc::clone(&release)),
                    calls: Arc::clone(&calls),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(GateProofRecorder {
                    recorder_id: "n2",
                    started: started_tx.clone(),
                    exited: None,
                    release: Some(Arc::clone(&release)),
                    calls: Arc::clone(&calls),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(GateProofRecorder {
                    recorder_id: "n3",
                    started: started_tx,
                    exited: Some(exited_tx),
                    release: Some(Arc::clone(&release)),
                    calls,
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_decision_proof_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        1,
                    ))
                    .unwrap();
            })
        };
        let mut admitted = BTreeSet::new();
        for _ in 0..3 {
            admitted.insert(started_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        }
        assert_eq!(admitted, BTreeSet::from(["n1", "n2", "n3"]));
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));

        release_gate(&release);
        assert_eq!(
            exited_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "n3"
        );
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Ok(None)
        );
        caller.join().unwrap();
        assert!(consensus.control_workers.iter().all(ControlWorker::is_idle));
    }

    #[test]
    fn proof_short_deadline_never_admits_and_cancellation_wins() {
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let calls = Arc::new(AtomicUsize::new(0));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(GateProofRecorder {
                        recorder_id,
                        started: started_tx.clone(),
                        exited: None,
                        release: None,
                        calls: Arc::clone(&calls),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert_eq!(
            consensus.inspect_decision_proof_at(
                &RecorderRpcContext::with_timeout(Duration::from_millis(1)),
                1,
            ),
            Err(Error::RpcDeadlineExceeded)
        );
        let cancelled = RecorderRpcContext::with_timeout(Duration::from_millis(1));
        cancelled.cancel();
        assert_eq!(
            consensus.inspect_decision_proof_at(&cancelled, 1),
            Err(Error::RpcCancelled)
        );
        assert_eq!(calls.load(Ordering::Acquire), 0);
        assert_eq!(started_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    }

    #[test]
    fn proof_work_deadline_admission_rechecks_cancellation_before_deadline_error() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let calls = Arc::new(AtomicUsize::new(0));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(GateProofRecorder {
                        recorder_id,
                        started: started_tx.clone(),
                        exited: None,
                        release: None,
                        calls: Arc::clone(&calls),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_millis(130),
            Arc::clone(&cancellation),
        );
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&release));
        let _hook = super::pause_next_control_work_deadline_check(
            Arc::clone(&cancellation),
            super::ControlWorkDeadlineCheckpoint::Admission,
            entered_tx,
            Arc::clone(&release),
        );
        let caller = {
            let consensus = Arc::clone(&consensus);
            let context = context.clone();
            thread::spawn(move || consensus.inspect_decision_proof_at(&context, 1))
        };
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        thread::sleep(Duration::from_millis(40));
        cancellation.store(true, Ordering::Release);
        release_gate(&release);
        assert_eq!(caller.join().unwrap(), Err(Error::RpcCancelled));
        assert_eq!(calls.load(Ordering::Acquire), 0);
        assert_eq!(started_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert!(consensus.control_workers.iter().all(ControlWorker::is_idle));
    }

    #[test]
    fn proof_work_deadline_constructor_rechecks_cancellation_before_deadline_error() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let calls = Arc::new(AtomicUsize::new(0));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(GateProofRecorder {
                        recorder_id,
                        started: started_tx.clone(),
                        exited: None,
                        release: None,
                        calls: Arc::clone(&calls),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_millis(130),
            Arc::clone(&cancellation),
        );
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&release));
        let _hook = super::pause_next_control_work_deadline_check(
            Arc::clone(&cancellation),
            super::ControlWorkDeadlineCheckpoint::Constructor,
            entered_tx,
            Arc::clone(&release),
        );
        let caller = {
            let consensus = Arc::clone(&consensus);
            let context = context.clone();
            thread::spawn(move || consensus.inspect_decision_proof_at(&context, 1))
        };
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        thread::sleep(Duration::from_millis(40));
        cancellation.store(true, Ordering::Release);
        release_gate(&release);
        assert_eq!(caller.join().unwrap(), Err(Error::RpcCancelled));
        assert_eq!(calls.load(Ordering::Acquire), 0);
        assert_eq!(started_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    }

    #[test]
    fn proof_late_unknown_overrides_a_frozen_quorum_candidate() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n1",
                    started: started_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n2",
                    started: started_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n3",
                    started: started_tx,
                    gate: Some(Arc::clone(&late)),
                    reply: Err(Error::UnknownOutcome),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_decision_proof_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        1,
                    ))
                    .unwrap();
            })
        };
        for _ in 0..3 {
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&late);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::UnknownOutcome)
        );
        caller.join().unwrap();
        assert!(consensus.control_workers.iter().all(ControlWorker::is_idle));
    }

    #[test]
    fn summary_quorum_drains_all_admitted_jobs_before_returning_empty() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n1",
                    entered: entered_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n2",
                    entered: entered_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n3",
                    entered: entered_tx,
                    gate: Some(Arc::clone(&late)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        1,
                        LogHash::ZERO,
                    ))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&late);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Ok(CertifiedDecisionInspection::Empty)
        );
        caller.join().unwrap();
        assert!(consensus.control_workers.iter().all(ControlWorker::is_idle));
    }

    #[test]
    fn summary_quorum_drain_preserves_cancellation_before_any_next_stage() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n1",
                    entered: entered_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n2",
                    entered: entered_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n3",
                    entered: entered_tx,
                    gate: Some(Arc::clone(&late)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancellation),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(&context, 1, LogHash::ZERO))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        cancellation.store(true, Ordering::Release);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&late);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::RpcCancelled)
        );
        caller.join().unwrap();
        assert!(consensus.control_workers.iter().all(ControlWorker::is_idle));
    }

    #[test]
    fn summary_late_unknown_overrides_a_frozen_empty_candidate() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n1",
                    entered: entered_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n2",
                    entered: entered_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n3",
                    entered: entered_tx,
                    gate: Some(Arc::clone(&late)),
                    reply: Err(Error::UnknownOutcome),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        1,
                        LogHash::ZERO,
                    ))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&late);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::UnknownOutcome)
        );
        caller.join().unwrap();
    }

    #[test]
    fn summary_error_never_turns_a_none_quorum_into_empty() {
        let run = |error: Error, expected: Error| {
            let (entered_tx, _entered_rx) = mpsc::sync_channel(3);
            let recorders = [("n1", Ok(None)), ("n2", Ok(None)), ("n3", Err(error))]
                .into_iter()
                .map(|(recorder_id, reply)| {
                    (
                        recorder_id.into(),
                        Box::new(ScriptedSummaryRecorder {
                            recorder_id,
                            entered: entered_tx.clone(),
                            gate: None,
                            reply,
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect();
            let consensus =
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap();
            assert_eq!(
                consensus.inspect_certified_decision_at(
                    &RecorderRpcContext::default_timeout(),
                    1,
                    LogHash::ZERO,
                ),
                Err(expected)
            );
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        };
        run(
            Error::Io("summary io".into()),
            Error::Io("summary io".into()),
        );
        run(
            Error::Decode("summary decode".into()),
            Error::Decode("summary decode".into()),
        );
        run(Error::TypedRecordRequired, Error::TypedRecordRequired);
        run(Error::ProposeFailed, Error::ProposeFailed);
        run(Error::RpcCancelled, Error::RpcCancelled);
    }

    #[test]
    fn summary_direct_safety_error_beats_a_valid_proof_quorum() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"direct-safety".to_vec());
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
        );
        let summary = |recorder_id: &str| RecordSummary {
            recorder_id: recorder_id.into(),
            slot: 1,
            config_id: 1,
            config_digest: membership.digest(),
            step: 4,
            first_current: Some(proposal.clone()),
            aggregate_prior: None,
            decided: None,
        };
        let (entered_tx, _entered_rx) = mpsc::sync_channel(3);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n1",
                    entered: entered_tx.clone(),
                    gate: None,
                    reply: Ok(Some(summary("n1"))),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n2",
                    entered: entered_tx.clone(),
                    gate: None,
                    reply: Ok(Some(summary("n2"))),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n3",
                    entered: entered_tx,
                    gate: None,
                    reply: Err(Error::Rejected(RejectReason::WrongConfig)),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        assert_eq!(
            consensus.inspect_certified_decision_at(
                &RecorderRpcContext::default_timeout(),
                1,
                LogHash::ZERO,
            ),
            Err(Error::Rejected(RejectReason::WrongConfig))
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn summary_drain_promotes_late_quorum_proof_after_pending_freeze() {
        let _blocking = lock_blocking_control_tests();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"late-proof".to_vec());
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
        );
        let summary = |recorder_id: &str, first_current: Option<Proposal>| RecordSummary {
            recorder_id: recorder_id.into(),
            slot: 1,
            config_id: 1,
            config_digest: membership.digest(),
            step: 4,
            first_current,
            aggregate_prior: None,
            decided: None,
        };
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let root_cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&root_cancellation),
        );
        let (provisional_tx, provisional_rx) = mpsc::sync_channel(1);
        let provisional_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_provisional = GateRelease::new(Arc::clone(&provisional_gate));
        let _provisional_hook = pause_after_next_summary_provisional_none(
            Arc::clone(&root_cancellation),
            provisional_tx,
            Arc::clone(&provisional_gate),
        );
        let (late_cancellation_tx, late_cancellation_rx) = mpsc::sync_channel(1);
        let recorder = |recorder_id, gate, summary, cancellation_observed| {
            Box::new(ScriptedSummaryFetchRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate,
                cancellation_observed,
                summary,
                command: command.clone(),
            }) as Box<dyn RecorderRpc>
        };
        let recorders = vec![
            (
                "n1".into(),
                recorder(
                    "n1",
                    Some(Arc::clone(&early)),
                    summary("n1", Some(proposal.clone())),
                    None,
                ),
            ),
            (
                "n2".into(),
                recorder(
                    "n2",
                    Some(Arc::clone(&late)),
                    summary("n2", Some(proposal.clone())),
                    Some(late_cancellation_tx),
                ),
            ),
            (
                "n3".into(),
                recorder("n3", Some(Arc::clone(&early)), summary("n3", None), None),
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(&context, 1, LogHash::ZERO))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        provisional_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_gate(&late);
        assert_eq!(
            late_cancellation_rx.recv_timeout(Duration::from_secs(1)),
            Ok(false),
            "late summary must remain live while the initial None is provisional"
        );
        release_gate(&provisional_gate);
        let inspection = result_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let Ok(CertifiedDecisionInspection::Committed(certified)) = inspection else {
            panic!("a valid late quorum proof must be promoted: {inspection:?}");
        };
        assert_eq!(certified.proof.proposal().value, proposal.value);
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn summary_none_quorum_waits_for_a_late_certified_single_copy() {
        let _blocking = lock_blocking_control_tests();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"late-single-proof".to_vec());
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
            config_digest: membership.digest(),
            proposal: proposal.clone(),
            summaries: membership.members()[..membership.quorum_size()]
                .iter()
                .map(|recorder_id| RecorderSummary {
                    recorder_id: recorder_id.clone(),
                    slot: 1,
                    step: 4,
                    first_current: Some(proposal.clone()),
                    aggregate_prior: None,
                })
                .collect(),
        };
        let late_summary = RecordSummary {
            recorder_id: "n1".into(),
            slot: 1,
            config_id: 1,
            config_digest: membership.digest(),
            step: 4,
            first_current: Some(proposal),
            aggregate_prior: None,
            decided: Some(proof.clone()),
        };
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedSummaryFetchRecorder {
                    recorder_id: "n1",
                    entered: entered_tx.clone(),
                    gate: Some(Arc::clone(&late)),
                    cancellation_observed: None,
                    summary: late_summary,
                    command: command.clone(),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n2",
                    entered: entered_tx.clone(),
                    gate: None,
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n3",
                    entered: entered_tx,
                    gate: None,
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        1,
                        LogHash::ZERO,
                    ))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        wait_for_control_worker_idle(&consensus.control_workers[1], "n2");
        wait_for_control_worker_idle(&consensus.control_workers[2], "n3");
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&late);
        let Ok(CertifiedDecisionInspection::Committed(certified)) =
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap()
        else {
            panic!("a late certified single-copy proof must prevent Empty");
        };
        assert_eq!(certified.proof, proof);
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn summary_provisional_none_keeps_queued_recorder_until_it_supplies_the_proof() {
        const BLOCKER_SLOT: Slot = 2;

        let _blocking = lock_blocking_control_tests();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"queued-late-proof".to_vec());
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
        );
        let summary = |recorder_id: &str| RecordSummary {
            recorder_id: recorder_id.into(),
            slot: 1,
            config_id: 1,
            config_digest: membership.digest(),
            step: 4,
            first_current: Some(proposal.clone()),
            aggregate_prior: None,
            decided: None,
        };
        let (blocker_started_tx, blocker_started_rx) = mpsc::sync_channel(1);
        let blocker_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_blocker = GateRelease::new(Arc::clone(&blocker_gate));
        let (queued_summary_started_tx, queued_summary_started_rx) = mpsc::sync_channel(1);
        let (n1_entered_tx, _n1_entered_rx) = mpsc::sync_channel(3);
        let (n3_entered_tx, _n3_entered_rx) = mpsc::sync_channel(3);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedSummaryFetchRecorder {
                    recorder_id: "n1",
                    entered: n1_entered_tx,
                    gate: None,
                    cancellation_observed: None,
                    summary: summary("n1"),
                    command: command.clone(),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(BlockingQueuedSummaryFetchRecorder {
                    blocker_slot: BLOCKER_SLOT,
                    blocker_started: blocker_started_tx,
                    blocker_gate: Arc::clone(&blocker_gate),
                    summary_started: queued_summary_started_tx,
                    summary: summary("n2"),
                    command: command.clone(),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n3",
                    entered: n3_entered_tx,
                    gate: None,
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );

        let (blocker_result_tx, blocker_result_rx) = mpsc::sync_channel(1);
        assert_eq!(
            consensus.control_workers[1].dispatch(ControlJob::InspectSummary {
                index: 1,
                context: RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                slot: BLOCKER_SLOT,
                result: blocker_result_tx,
            }),
            ControlDispatch::Accepted
        );
        blocker_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let root_cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&root_cancellation),
        );
        let (provisional_tx, provisional_rx) = mpsc::sync_channel(1);
        let provisional_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_provisional = GateRelease::new(Arc::clone(&provisional_gate));
        let _provisional_hook = pause_after_next_summary_provisional_none(
            Arc::clone(&root_cancellation),
            provisional_tx,
            Arc::clone(&provisional_gate),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(&context, 1, LogHash::ZERO))
                    .unwrap()
            })
        };

        provisional_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_gate(&blocker_gate);
        assert_eq!(
            queued_summary_started_rx.recv_timeout(Duration::from_secs(1)),
            Ok(()),
            "the queued n2 summary must run instead of being pruned"
        );
        release_gate(&provisional_gate);
        assert_eq!(
            blocker_result_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap(),
            (1, Ok(None))
        );
        let inspection = result_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(
            inspection,
            Ok(CertifiedDecisionInspection::Committed(_))
        ));
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn summary_empty_quorum_keeps_a_queued_occupied_recorder_until_inspected() {
        const BLOCKER_SLOT: Slot = 2;

        let _blocking = lock_blocking_control_tests();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let summary = RecordSummary {
            recorder_id: "n1".into(),
            slot: 1,
            config_id: 1,
            config_digest: membership.digest(),
            step: 1,
            first_current: None,
            aggregate_prior: None,
            decided: None,
        };
        let (blocker_started_tx, blocker_started_rx) = mpsc::sync_channel(1);
        let blocker_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_blocker = GateRelease::new(Arc::clone(&blocker_gate));
        let (queued_summary_started_tx, queued_summary_started_rx) = mpsc::sync_channel(1);
        let (empty_entered_tx, empty_entered_rx) = mpsc::sync_channel(2);
        let command = StoredCommand::new(EntryType::Command, Vec::new());
        let recorders = vec![
            (
                "n1".into(),
                Box::new(BlockingQueuedSummaryFetchRecorder {
                    blocker_slot: BLOCKER_SLOT,
                    blocker_started: blocker_started_tx,
                    blocker_gate: Arc::clone(&blocker_gate),
                    summary_started: queued_summary_started_tx,
                    summary,
                    command,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n2",
                    entered: empty_entered_tx.clone(),
                    gate: None,
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n3",
                    entered: empty_entered_tx,
                    gate: None,
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );

        let (blocker_result_tx, blocker_result_rx) = mpsc::sync_channel(1);
        assert_eq!(
            consensus.control_workers[0].dispatch(ControlJob::InspectSummary {
                index: 0,
                context: RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                slot: BLOCKER_SLOT,
                result: blocker_result_tx,
            }),
            ControlDispatch::Accepted
        );
        blocker_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        1,
                        LogHash::ZERO,
                    ))
                    .unwrap()
            })
        };

        for _ in 0..2 {
            empty_entered_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap();
        }
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&blocker_gate);
        assert_eq!(
            queued_summary_started_rx.recv_timeout(Duration::from_secs(1)),
            Ok(()),
            "an admitted occupied recorder must not be pruned after an empty quorum"
        );
        assert_eq!(
            blocker_result_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap(),
            (0, Ok(None))
        );
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Ok(CertifiedDecisionInspection::Unavailable)
        );
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn summary_drain_rejects_conflicting_valid_late_proofs_after_pending_freeze() {
        let _blocking = lock_blocking_control_tests();
        let membership = Membership::new(["n1", "n2", "n3", "n4", "n5"]).unwrap();
        let first_late = valid_inspected_proof(&membership, LogHash::digest(&[b"first-late"]));
        let conflicting_late =
            valid_inspected_proof(&membership, LogHash::digest(&[b"conflicting-late"]));
        let command = StoredCommand::new(EntryType::Command, Vec::new());
        let summary = |recorder_id: &str, decided: Option<DecisionProof>| RecordSummary {
            recorder_id: recorder_id.into(),
            slot: 1,
            config_id: 1,
            config_digest: membership.digest(),
            step: 4,
            first_current: None,
            aggregate_prior: None,
            decided,
        };
        let (entered_tx, entered_rx) = mpsc::sync_channel(5);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let root_cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&root_cancellation),
        );
        let (provisional_tx, provisional_rx) = mpsc::sync_channel(1);
        let provisional_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_provisional = GateRelease::new(Arc::clone(&provisional_gate));
        let _provisional_hook = pause_after_next_summary_provisional_none(
            Arc::clone(&root_cancellation),
            provisional_tx,
            Arc::clone(&provisional_gate),
        );
        let (late_cancellation_tx, late_cancellation_rx) = mpsc::sync_channel(2);
        let recorder = |recorder_id, gate, summary, cancellation_observed| {
            Box::new(ScriptedSummaryFetchRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate: Some(gate),
                cancellation_observed,
                summary,
                command: command.clone(),
            }) as Box<dyn RecorderRpc>
        };
        let recorders = vec![
            (
                "n1".into(),
                recorder("n1", Arc::clone(&early), summary("n1", None), None),
            ),
            (
                "n2".into(),
                recorder(
                    "n2",
                    Arc::clone(&late),
                    summary("n2", Some(first_late)),
                    Some(late_cancellation_tx.clone()),
                ),
            ),
            (
                "n3".into(),
                recorder(
                    "n3",
                    Arc::clone(&late),
                    summary("n3", Some(conflicting_late)),
                    Some(late_cancellation_tx),
                ),
            ),
            (
                "n4".into(),
                recorder("n4", Arc::clone(&early), summary("n4", None), None),
            ),
            (
                "n5".into(),
                recorder("n5", Arc::clone(&early), summary("n5", None), None),
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(&context, 1, LogHash::ZERO))
                    .unwrap()
            })
        };
        for _ in 0..5 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        provisional_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_gate(&late);
        assert_eq!(
            late_cancellation_rx.recv_timeout(Duration::from_secs(1)),
            Ok(false),
            "first late summary must remain live while the initial None is provisional"
        );
        assert_eq!(
            late_cancellation_rx.recv_timeout(Duration::from_secs(1)),
            Ok(false),
            "second late summary must remain live while the initial None is provisional"
        );
        release_gate(&provisional_gate);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::ConflictingCertificates)
        );
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn summary_proof_candidate_cancellation_blocks_legacy_fetch_stage() {
        let _blocking = lock_blocking_control_tests();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let proposal = valid_inspected_proof(&membership, LogHash::ZERO)
            .proposal()
            .clone();
        let summary = |recorder_id: &str| RecordSummary {
            recorder_id: recorder_id.into(),
            slot: 1,
            config_id: 1,
            config_digest: membership.digest(),
            step: 4,
            first_current: Some(proposal.clone()),
            aggregate_prior: None,
            decided: None,
        };
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let fetch_entries = Arc::new(AtomicUsize::new(0));
        let recorder = |recorder_id, gate, summary: RecordSummary| {
            Box::new(ProofSummaryFetchRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate,
                summary,
                fetch_entries: Arc::clone(&fetch_entries),
            }) as Box<dyn RecorderRpc>
        };
        let recorders = vec![
            (
                "n1".into(),
                recorder("n1", Arc::clone(&early), summary("n1")),
            ),
            (
                "n2".into(),
                recorder("n2", Arc::clone(&early), summary("n2")),
            ),
            (
                "n3".into(),
                recorder("n3", Arc::clone(&late), summary("n3")),
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancellation),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(&context, 1, LogHash::ZERO))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        cancellation.store(true, Ordering::Release);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&late);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::RpcCancelled)
        );
        caller.join().unwrap();
        assert_eq!(fetch_entries.load(Ordering::Acquire), 0);
    }

    #[test]
    fn summary_late_invalid_identity_evidence_beats_a_frozen_proof() {
        let _blocking = lock_blocking_control_tests();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let proposal = valid_inspected_proof(&membership, LogHash::ZERO)
            .proposal()
            .clone();
        let summary = |recorder_id: &str| RecordSummary {
            recorder_id: recorder_id.into(),
            slot: 1,
            config_id: 1,
            config_digest: membership.digest(),
            step: 4,
            first_current: Some(proposal.clone()),
            aggregate_prior: None,
            decided: None,
        };
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let fetch_entries = Arc::new(AtomicUsize::new(0));
        let proof_recorder = |recorder_id, summary: RecordSummary| {
            Box::new(ProofSummaryFetchRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate: Arc::clone(&early),
                summary,
                fetch_entries: Arc::clone(&fetch_entries),
            }) as Box<dyn RecorderRpc>
        };
        let invalid = RecordSummary {
            recorder_id: "wrong-member".into(),
            ..summary("n3")
        };
        let recorders = vec![
            ("n1".into(), proof_recorder("n1", summary("n1"))),
            ("n2".into(), proof_recorder("n2", summary("n2"))),
            (
                "n3".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n3",
                    entered: entered_tx,
                    gate: Some(Arc::clone(&late)),
                    reply: Ok(Some(invalid)),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        1,
                        LogHash::ZERO,
                    ))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&late);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::Rejected(RejectReason::InvalidCertificate))
        );
        caller.join().unwrap();
        assert_eq!(fetch_entries.load(Ordering::Acquire), 0);
    }

    #[test]
    fn summary_short_deadline_admits_no_backend_work() {
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedSummaryRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: None,
                        reply: Ok(None),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        assert_eq!(
            consensus.inspect_certified_decision_at(
                &RecorderRpcContext::with_timeout(Duration::from_millis(1)),
                1,
                LogHash::ZERO
            ),
            Err(Error::RpcDeadlineExceeded)
        );
        assert_eq!(entered_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    }

    #[test]
    fn summary_partial_admission_cancellation_drains_the_admitted_job() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let worker_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let dispatch_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_worker = GateRelease::new(Arc::clone(&worker_gate));
        let _release_dispatch = GateRelease::new(Arc::clone(&dispatch_gate));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedSummaryRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: (recorder_id == "n1").then(|| Arc::clone(&worker_gate)),
                        reply: Ok(None),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancellation),
        );
        let (hook_tx, hook_rx) = mpsc::sync_channel(1);
        let _hook = pause_after_next_summary_dispatch(
            Arc::clone(&cancellation),
            hook_tx,
            Arc::clone(&dispatch_gate),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(&context, 1, LogHash::ZERO))
                    .unwrap()
            })
        };

        hook_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "n1"
        );
        cancellation.store(true, Ordering::Release);
        release_gate(&dispatch_gate);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&worker_gate);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::RpcCancelled)
        );
        caller.join().unwrap();
        assert_eq!(entered_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn summary_frozen_safety_error_beats_late_unknown_outcome() {
        let _blocking = lock_blocking_control_tests();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let summary = |recorder_id: &str| RecordSummary {
            recorder_id: recorder_id.into(),
            slot: 1,
            config_id: 1,
            config_digest: membership.digest(),
            step: 5,
            first_current: None,
            aggregate_prior: None,
            // A two-recorder installed-proof quorum is selected before the
            // drain.  Its empty signer set is invalid, so this is a genuine
            // collector safety failure rather than a mocked finalizer.
            decided: Some(test_decision_proof(&membership)),
        };
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n1",
                    entered: entered_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(Some(summary("n1"))),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n2",
                    entered: entered_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(Some(summary("n2"))),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n3",
                    entered: entered_tx,
                    gate: Some(Arc::clone(&late)),
                    reply: Err(Error::UnknownOutcome),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        1,
                        LogHash::ZERO,
                    ))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&late);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::Rejected(RejectReason::InvalidCertificate))
        );
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn summary_late_direct_safety_error_beats_a_frozen_valid_proof() {
        let _blocking = lock_blocking_control_tests();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"late-direct-safety".to_vec());
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
        );
        let summary = |recorder_id: &str| RecordSummary {
            recorder_id: recorder_id.into(),
            slot: 1,
            config_id: 1,
            config_digest: membership.digest(),
            step: 4,
            first_current: Some(proposal.clone()),
            aggregate_prior: None,
            decided: None,
        };
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n1",
                    entered: entered_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(Some(summary("n1"))),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n2",
                    entered: entered_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(Some(summary("n2"))),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedSummaryRecorder {
                    recorder_id: "n3",
                    entered: entered_tx,
                    gate: Some(Arc::clone(&late)),
                    reply: Err(Error::Rejected(RejectReason::WrongConfig)),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_certified_decision_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        1,
                        LogHash::ZERO,
                    ))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&late);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::Rejected(RejectReason::WrongConfig))
        );
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn proof_late_conflict_or_invalid_evidence_overrides_a_frozen_candidate() {
        let _blocking = lock_blocking_control_tests();
        let run = |early_reply: super::Result<Option<DecisionProof>>,
                   late_replies: [super::Result<Option<DecisionProof>>; 2],
                   expected: Error| {
            let (started_tx, started_rx) = mpsc::sync_channel(3);
            let early = Arc::new((Mutex::new(false), Condvar::new()));
            let late = Arc::new((Mutex::new(false), Condvar::new()));
            let _release_early = GateRelease::new(Arc::clone(&early));
            let _release_late = GateRelease::new(Arc::clone(&late));
            let recorders = vec![
                (
                    "n1".into(),
                    Box::new(ScriptedProofRecorder {
                        recorder_id: "n1",
                        started: started_tx.clone(),
                        gate: Some(Arc::clone(&early)),
                        reply: early_reply,
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n2".into(),
                    Box::new(ScriptedProofRecorder {
                        recorder_id: "n2",
                        started: started_tx.clone(),
                        gate: Some(Arc::clone(&late)),
                        reply: late_replies[0].clone(),
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n3".into(),
                    Box::new(ScriptedProofRecorder {
                        recorder_id: "n3",
                        started: started_tx,
                        gate: Some(Arc::clone(&late)),
                        reply: late_replies[1].clone(),
                    }) as Box<dyn RecorderRpc>,
                ),
            ];
            let consensus = Arc::new(
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap(),
            );
            let (result_tx, result_rx) = mpsc::sync_channel(1);
            let caller = {
                let consensus = Arc::clone(&consensus);
                thread::spawn(move || {
                    result_tx
                        .send(consensus.inspect_decision_proof_at(
                            &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                            1,
                        ))
                        .unwrap();
                })
            };
            for _ in 0..3 {
                started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            }
            release_gate(&early);
            assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
            release_gate(&late);
            assert_eq!(
                result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                Err(expected)
            );
            caller.join().unwrap();
            assert!(consensus.control_workers.iter().all(ControlWorker::is_idle));
        };

        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        run(
            Err(Error::UnknownOutcome),
            [
                Ok(Some(valid_inspected_proof(&membership, LogHash::ZERO))),
                Ok(Some(valid_inspected_proof(
                    &membership,
                    LogHash::digest(&[b"late-conflict"]),
                ))),
            ],
            Error::ConflictingCertificates,
        );
    }

    #[test]
    fn proof_folds_pre_freeze_conflicts_even_when_unknown_freezes_the_response() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(5);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let unknown = Arc::new((Mutex::new(false), Condvar::new()));
        let drain = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_unknown = GateRelease::new(Arc::clone(&unknown));
        let _release_drain = GateRelease::new(Arc::clone(&drain));
        let membership = Membership::new(["n1", "n2", "n3", "n4", "n5"]).unwrap();
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n1",
                    started: started_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(Some(valid_inspected_proof(&membership, LogHash::ZERO))),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n2",
                    started: started_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(Some(valid_inspected_proof(
                        &membership,
                        LogHash::digest(&[b"pre-freeze-conflict"]),
                    ))),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n3",
                    started: started_tx.clone(),
                    gate: Some(Arc::clone(&unknown)),
                    reply: Err(Error::UnknownOutcome),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n4".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n4",
                    started: started_tx.clone(),
                    gate: Some(Arc::clone(&drain)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n5".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n5",
                    started: started_tx,
                    gate: Some(Arc::clone(&drain)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_decision_proof_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        1,
                    ))
                    .unwrap();
            })
        };
        for _ in 0..5 {
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        let early_deadline = Instant::now() + Duration::from_secs(1);
        while !consensus.control_workers[..2]
            .iter()
            .all(ControlWorker::is_idle)
        {
            assert!(
                Instant::now() < early_deadline,
                "both conflicting proofs must reach the production collector before Unknown"
            );
            thread::yield_now();
        }
        release_gate(&unknown);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&drain);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::ConflictingCertificates)
        );
        caller.join().unwrap();
        assert!(consensus.control_workers.iter().all(ControlWorker::is_idle));
    }

    #[test]
    fn proof_late_invalid_evidence_overrides_a_frozen_success() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n1",
                    started: started_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n2",
                    started: started_tx.clone(),
                    gate: Some(Arc::clone(&early)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n3",
                    started: started_tx,
                    gate: Some(Arc::clone(&late)),
                    reply: Ok(Some(test_decision_proof(&membership))),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_decision_proof_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        1,
                    ))
                    .unwrap();
            })
        };
        for _ in 0..3 {
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&early);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&late);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::Rejected(RejectReason::InvalidCertificate))
        );
        caller.join().unwrap();
        assert!(consensus.control_workers.iter().all(ControlWorker::is_idle));
    }

    #[test]
    fn proof_drain_timeout_quarantines_only_the_noncooperative_worker() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let (exited_tx, exited_rx) = mpsc::sync_channel(1);
        let (group_token_tx, group_token_rx) = mpsc::sync_channel(3);
        let early = Arc::new((Mutex::new(false), Condvar::new()));
        let slow = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_early = GateRelease::new(Arc::clone(&early));
        let _release_slow = GateRelease::new(Arc::clone(&slow));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(TokenGateProofRecorder {
                    recorder_id: "n1",
                    started: started_tx.clone(),
                    group_token: group_token_tx.clone(),
                    gate: Arc::clone(&early),
                    exited: None,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(TokenGateProofRecorder {
                    recorder_id: "n2",
                    started: started_tx.clone(),
                    group_token: group_token_tx.clone(),
                    gate: Arc::clone(&early),
                    exited: None,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(TokenGateProofRecorder {
                    recorder_id: "n3",
                    started: started_tx,
                    group_token: group_token_tx,
                    gate: Arc::clone(&slow),
                    exited: Some(exited_tx),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_decision_proof_at(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(5)),
                        1,
                    ))
                    .unwrap();
            })
        };
        let mut entered = BTreeSet::new();
        for _ in 0..3 {
            entered.insert(started_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        assert_eq!(entered, BTreeSet::from(["n1", "n2", "n3"]));
        let group_tokens = (0..3)
            .map(|_| group_token_rx.recv_timeout(Duration::from_secs(5)).unwrap())
            .collect::<Vec<_>>();
        assert!(
            group_tokens
                .iter()
                .all(|token| Arc::ptr_eq(token, &group_tokens[0])),
            "every control job must carry the exact same group cancellation token"
        );
        let (fired_tx, fired_rx) = mpsc::sync_channel(1);
        let _force_drain = super::force_next_control_group_drain_timeout(
            Arc::clone(&group_tokens[0]),
            Arc::clone(&consensus.control_workers[2].state),
            fired_tx,
        );
        release_gate(&early);
        assert_eq!(fired_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::RpcDeadlineExceeded)
        );
        caller.join().unwrap();
        assert!(consensus.control_workers[2]
            .state
            .quarantined
            .load(Ordering::Acquire));
        assert!(consensus.control_workers[..2]
            .iter()
            .all(|worker| !worker.state.quarantined.load(Ordering::Acquire)));

        // The closed third worker is isolated; the other quorum still serves
        // an unrelated call while the original backend remains blocked.
        assert_eq!(
            consensus.inspect_decision_proof_at(
                &RecorderRpcContext::with_timeout(Duration::from_secs(5)),
                2,
            ),
            Ok(None)
        );
        release_gate(&slow);
        assert_eq!(
            exited_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            "n3"
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(5)));
        assert!(consensus.control_workers.iter().all(ControlWorker::is_idle));
    }

    #[test]
    fn proof_timeout_quarantines_before_unknown_or_safety_precedence() {
        let _blocking = lock_blocking_control_tests();
        let run = |first: super::Result<Option<DecisionProof>>, expected: Error| {
            let (started_tx, started_rx) = mpsc::sync_channel(3);
            let early = Arc::new((Mutex::new(false), Condvar::new()));
            let blocked = Arc::new((Mutex::new(false), Condvar::new()));
            let _release_early = GateRelease::new(Arc::clone(&early));
            let _release_blocked = GateRelease::new(Arc::clone(&blocked));
            let recorders = vec![
                (
                    "n1".into(),
                    Box::new(ScriptedProofRecorder {
                        recorder_id: "n1",
                        started: started_tx.clone(),
                        gate: Some(Arc::clone(&early)),
                        reply: first,
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n2".into(),
                    Box::new(ScriptedProofRecorder {
                        recorder_id: "n2",
                        started: started_tx.clone(),
                        gate: Some(Arc::clone(&early)),
                        reply: Ok(None),
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n3".into(),
                    Box::new(ScriptedProofRecorder {
                        recorder_id: "n3",
                        started: started_tx,
                        gate: Some(Arc::clone(&blocked)),
                        reply: Ok(None),
                    }) as Box<dyn RecorderRpc>,
                ),
            ];
            let consensus = Arc::new(
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap(),
            );
            let (result_tx, result_rx) = mpsc::sync_channel(1);
            let caller = {
                let consensus = Arc::clone(&consensus);
                thread::spawn(move || {
                    result_tx
                        .send(consensus.inspect_decision_proof_at(
                            &RecorderRpcContext::with_timeout(Duration::from_millis(300)),
                            1,
                        ))
                        .unwrap();
                })
            };
            for _ in 0..3 {
                started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            }
            release_gate(&early);
            assert_eq!(
                result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                Err(expected)
            );
            caller.join().unwrap();
            assert!(consensus.control_workers[2]
                .state
                .quarantined
                .load(Ordering::Acquire));
            release_gate(&blocked);
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
            assert!(consensus.control_workers.iter().all(ControlWorker::is_idle));
        };

        run(Err(Error::UnknownOutcome), Error::UnknownOutcome);
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        run(
            Ok(Some(test_decision_proof(&membership))),
            Error::Rejected(RejectReason::InvalidCertificate),
        );
    }

    #[test]
    fn group_prune_does_not_cancel_an_unrelated_group() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let worker = ControlWorker::spawn(Arc::new(BlockingControlRecorder {
            recorder_id: "n1",
            started: started_tx,
            release_first: Mutex::new(release_rx),
        }))
        .unwrap();
        let group_a = ControlCallGroup::new();
        let group_b = ControlCallGroup::new();
        let (a_tx, a_rx) = mpsc::sync_channel(1);
        let (b_tx, b_rx) = mpsc::sync_channel(1);
        let context = RecorderRpcContext::default_timeout();

        assert_eq!(
            worker.dispatch_group(
                ControlJob::InspectProof {
                    index: 1,
                    context: context.clone(),
                    slot: 1,
                    result: a_tx.clone(),
                },
                &group_a,
            ),
            ControlDispatch::Accepted
        );
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
        assert_eq!(
            worker.dispatch_group(
                ControlJob::InspectProof {
                    index: 2,
                    context: context.clone(),
                    slot: 2,
                    result: b_tx,
                },
                &group_b,
            ),
            ControlDispatch::Accepted
        );
        group_b.cancel_and_prune();
        assert!(group_b
            .drain_to_deadline(Instant::now() + Duration::from_secs(1))
            .is_empty());
        assert_eq!(b_rx.recv_timeout(Duration::from_secs(1)).unwrap().0, 2);
        assert_eq!(a_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_tx.send(()).unwrap();
        assert_eq!(a_rx.recv_timeout(Duration::from_secs(1)).unwrap().0, 1);
        assert!(group_a
            .drain_to_deadline(Instant::now() + Duration::from_secs(1))
            .is_empty());
    }

    #[test]
    fn completion_guard_survives_receiver_drop_and_worker_exit_or_queue_poison() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, _started_rx) = mpsc::sync_channel(2);
        let worker = ControlWorker::spawn(Arc::new(GateProofRecorder {
            recorder_id: "n1",
            started: started_tx.clone(),
            exited: None,
            release: None,
            calls: Arc::new(AtomicUsize::new(0)),
        }))
        .unwrap();
        let group = ControlCallGroup::new();
        let (drop_tx, drop_rx) = mpsc::sync_channel(1);
        assert_eq!(
            worker.dispatch_group(
                ControlJob::InspectProof {
                    index: 1,
                    context: RecorderRpcContext::default_timeout(),
                    slot: 1,
                    result: drop_tx,
                },
                &group,
            ),
            ControlDispatch::Accepted
        );
        drop(drop_rx);
        assert!(group
            .drain_to_deadline(Instant::now() + Duration::from_secs(1))
            .is_empty());
        assert!(worker.is_idle());

        let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _queue = worker.state.queue.state.lock().unwrap();
            panic!("poison queue mutex");
        }));
        assert!(poison.is_err());
        let poisoned_group = ControlCallGroup::new();
        let (poisoned_tx, poisoned_rx) = mpsc::sync_channel(1);
        assert_eq!(
            worker.dispatch_group(
                ControlJob::InspectProof {
                    index: 2,
                    context: RecorderRpcContext::default_timeout(),
                    slot: 2,
                    result: poisoned_tx,
                },
                &poisoned_group,
            ),
            ControlDispatch::Accepted
        );
        assert_eq!(
            poisoned_rx.recv_timeout(Duration::from_secs(1)).unwrap().0,
            2
        );
        assert!(poisoned_group
            .drain_to_deadline(Instant::now() + Duration::from_secs(1))
            .is_empty());

        let exit_group = ControlCallGroup::new();
        let (exit_tx, exit_rx) = mpsc::sync_channel(1);
        worker.panic_after_next_pop();
        assert_eq!(
            worker.dispatch_group(
                ControlJob::InspectProof {
                    index: 3,
                    context: RecorderRpcContext::default_timeout(),
                    slot: 3,
                    result: exit_tx,
                },
                &exit_group,
            ),
            ControlDispatch::Accepted
        );
        assert!(exit_rx.recv_timeout(Duration::from_millis(50)).is_err());
        assert!(exit_group
            .drain_to_deadline(Instant::now() + Duration::from_secs(1))
            .is_empty());
        assert!(worker.is_idle());
        let mut queue = worker
            .state
            .queue
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !queue.closed {
            queue = worker
                .state
                .queue
                .available
                .wait(queue)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        drop(queue);
        assert_eq!(
            worker.dispatch(ControlJob::InspectProof {
                index: 4,
                context: RecorderRpcContext::default_timeout(),
                slot: 4,
                result: mpsc::sync_channel(1).0,
            }),
            ControlDispatch::Failed
        );
    }

    #[test]
    fn abnormal_worker_exit_fails_queued_jobs_by_operation_certainty() {
        let _blocking = lock_blocking_control_tests();
        let run_read = || {
            let (started_tx, _started_rx) = mpsc::sync_channel(1);
            let worker = ControlWorker::spawn(Arc::new(GateProofRecorder {
                recorder_id: "n1",
                started: started_tx,
                exited: None,
                release: None,
                calls: Arc::new(AtomicUsize::new(0)),
            }))
            .unwrap();
            let pause = Arc::new((Mutex::new(false), Condvar::new()));
            let _release_pause = GateRelease::new(Arc::clone(&pause));
            let (entered_tx, entered_rx) = mpsc::sync_channel(1);
            worker.pause_after_next_pop(entered_tx, Arc::clone(&pause));
            let first = ControlCallGroup::new();
            let second = ControlCallGroup::new();
            let (first_tx, _first_rx) = mpsc::sync_channel(1);
            let (second_tx, second_rx) = mpsc::sync_channel(1);
            assert_eq!(
                worker.dispatch_group(
                    ControlJob::InspectProof {
                        index: 1,
                        context: RecorderRpcContext::default_timeout(),
                        slot: 1,
                        result: first_tx,
                    },
                    &first,
                ),
                ControlDispatch::Accepted
            );
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(
                worker.dispatch_group(
                    ControlJob::InspectProof {
                        index: 2,
                        context: RecorderRpcContext::default_timeout(),
                        slot: 2,
                        result: second_tx,
                    },
                    &second,
                ),
                ControlDispatch::Accepted
            );
            worker.panic_after_next_pop();
            release_gate(&pause);
            assert_eq!(
                second_rx.recv_timeout(Duration::from_secs(1)).unwrap().1,
                Err(Error::ProposeFailed)
            );
            assert!(first
                .drain_to_deadline(Instant::now() + Duration::from_secs(1))
                .is_empty());
            assert!(second
                .drain_to_deadline(Instant::now() + Duration::from_secs(1))
                .is_empty());
            assert!(worker.is_idle());
        };
        let run_mutation = || {
            let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
            let (started_tx, _started_rx) = mpsc::sync_channel(1);
            let worker = ControlWorker::spawn(Arc::new(GateProofRecorder {
                recorder_id: "n1",
                started: started_tx,
                exited: None,
                release: None,
                calls: Arc::new(AtomicUsize::new(0)),
            }))
            .unwrap();
            let pause = Arc::new((Mutex::new(false), Condvar::new()));
            let _release_pause = GateRelease::new(Arc::clone(&pause));
            let (entered_tx, entered_rx) = mpsc::sync_channel(1);
            worker.pause_after_next_pop(entered_tx, Arc::clone(&pause));
            let first = ControlCallGroup::new();
            let second = ControlCallGroup::new();
            let (first_tx, _first_rx) = mpsc::sync_channel(1);
            let (second_tx, second_rx) = mpsc::sync_channel(1);
            assert_eq!(
                worker.dispatch_group(
                    ControlJob::InstallProof {
                        index: 1,
                        context: RecorderRpcContext::default_timeout(),
                        proof: test_decision_proof(&membership),
                        membership: membership.clone(),
                        result: first_tx,
                    },
                    &first,
                ),
                ControlDispatch::Accepted
            );
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(
                worker.dispatch_group(
                    ControlJob::InstallProof {
                        index: 2,
                        context: RecorderRpcContext::default_timeout(),
                        proof: test_decision_proof(&membership),
                        membership,
                        result: second_tx,
                    },
                    &second,
                ),
                ControlDispatch::Accepted
            );
            worker.panic_after_next_pop();
            release_gate(&pause);
            assert_eq!(
                second_rx.recv_timeout(Duration::from_secs(1)).unwrap().1,
                Err(Error::UnknownOutcome)
            );
            assert!(first
                .drain_to_deadline(Instant::now() + Duration::from_secs(1))
                .is_empty());
            assert!(second
                .drain_to_deadline(Instant::now() + Duration::from_secs(1))
                .is_empty());
            assert!(worker.is_idle());
        };
        run_read();
        run_mutation();
    }

    impl RecorderRpc for BlockingInspectionReadFenceRecorder {
        fn inspect_record_summary(
            &self,
            _context: &RecorderRpcContext,
            _slot: u64,
        ) -> super::Result<Option<RecordSummary>> {
            if self.block_inspection {
                self.started.send(self.recorder_id).unwrap();
                let (released, condition) = &*self.release;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            Ok(None)
        }

        fn supports_context_read_fence(&self) -> bool {
            true
        }

        fn observe_read_fence(
            &self,
            _context: &RecorderRpcContext,
            request: ReadFenceRequest,
        ) -> super::Result<ReadFenceObservation> {
            Ok(ReadFenceObservation {
                recorder_id: self.recorder_id.into(),
                cluster_id: request.cluster_id,
                epoch: request.epoch,
                config_id: request.config_id,
                config_digest: request.config_digest,
                slot: request.slot,
                max_head: None,
                slot_state: ReadFenceSlotState::Empty,
            })
        }
    }

    struct BlockingRecorder {
        recorder_id: &'static str,
        started: mpsc::SyncSender<u64>,
        release_first: Mutex<mpsc::Receiver<()>>,
    }

    impl RecorderRpc for BlockingRecorder {
        fn record(
            &self,
            context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            self.started.send(request.slot).unwrap();
            if request.slot == 1 {
                loop {
                    context.check()?;
                    match self
                        .release_first
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_millis(5))
                    {
                        Ok(()) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            panic!("blocking test recorder release channel disconnected")
                        }
                    }
                }
            }
            Ok(record_summary(self.recorder_id, request))
        }

        fn install_decision_proof(
            &self,
            _context: &RecorderRpcContext,
            _proof: DecisionProof,
            _membership: &Membership,
        ) -> super::Result<()> {
            Err(Error::ProposeFailed)
        }
    }

    struct FailInstallFileStore {
        inner: RecorderFileStore,
        fail_install: Arc<AtomicBool>,
    }

    impl RecorderRpc for FailInstallFileStore {
        fn record(
            &self,
            context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            context.check()?;
            self.inner.record(request)
        }

        fn install_decision_proof(
            &self,
            context: &RecorderRpcContext,
            proof: DecisionProof,
            membership: &Membership,
        ) -> super::Result<()> {
            if self.fail_install.load(Ordering::Acquire) {
                return Err(Error::ProposeFailed);
            }
            context.check()?;
            self.inner.install_decision_proof(proof, membership)
        }

        fn inspect_decision_proof(
            &self,
            context: &RecorderRpcContext,
            slot: Slot,
        ) -> super::Result<Option<DecisionProof>> {
            context.check()?;
            self.inner.inspect_decision_proof(slot)
        }
    }

    /// Models a recorder which made the proof durable but lost the response.
    /// The production caller must recover only from the independently
    /// certified durable state, not from this wrapper's acknowledgement.
    struct PersistThenUnknownInstallFileStore {
        inner: RecorderFileStore,
        persist: bool,
    }

    impl RecorderRpc for PersistThenUnknownInstallFileStore {
        fn record(
            &self,
            context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            context.check()?;
            self.inner.record(request)
        }

        fn install_decision_proof(
            &self,
            context: &RecorderRpcContext,
            proof: DecisionProof,
            membership: &Membership,
        ) -> super::Result<()> {
            context.check()?;
            if self.persist {
                self.inner.install_decision_proof(proof, membership)?;
            }
            Err(Error::UnknownOutcome)
        }

        fn inspect_record_summary(
            &self,
            context: &RecorderRpcContext,
            slot: Slot,
        ) -> super::Result<Option<RecordSummary>> {
            context.check()?;
            self.inner.inspect_record_summary(slot)
        }

        fn fetch_command_for(
            &self,
            context: &RecorderRpcContext,
            cluster_id: ClusterId,
            epoch: Epoch,
            config_id: ConfigId,
            config_digest: LogHash,
            command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            context.check()?;
            self.inner
                .fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash)
        }
    }

    struct GatedRecordRecorder {
        recorder_id: &'static str,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl RecorderRpc for GatedRecordRecorder {
        fn record(
            &self,
            _context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            let (released, condition) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
            Ok(record_summary(self.recorder_id, request))
        }
    }

    /// A cooperative, observable recorder gate used to freeze a quorum after
    /// its jobs are admitted, without relying on scheduler timing.
    struct GatedObservedSlotRecorder {
        recorder_id: &'static str,
        observed: mpsc::SyncSender<Slot>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl RecorderRpc for GatedObservedSlotRecorder {
        fn record(
            &self,
            context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            let (released, condition) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                context.check()?;
                let (next, _) = condition
                    .wait_timeout(released, Duration::from_millis(5))
                    .unwrap();
                released = next;
            }
            context.check()?;
            drop(released);
            self.observed
                .send(request.slot)
                .expect("observable test recorder receiver must remain live");
            Ok(record_summary(self.recorder_id, request))
        }
    }

    /// Deliberately ignores the call context.  It models a recorder stuck
    /// below the cooperative RPC layer, so the caller must wait through D and
    /// quarantine its worker rather than returning an early quorum success.
    struct NonCooperativeRecordRecorder {
        recorder_id: &'static str,
        started: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        calls: Arc<AtomicUsize>,
    }

    impl RecorderRpc for NonCooperativeRecordRecorder {
        fn record(
            &self,
            _context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.started.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            Ok(record_summary(self.recorder_id, request))
        }
    }

    struct WorkDeadlineCancellationRecorder {
        recorder_id: &'static str,
        started: mpsc::SyncSender<&'static str>,
        cancelled: mpsc::SyncSender<&'static str>,
        calls: AtomicUsize,
    }

    impl RecorderRpc for WorkDeadlineCancellationRecorder {
        fn record(
            &self,
            context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            if self.calls.fetch_add(1, Ordering::AcqRel) != 0 {
                return Ok(record_summary(self.recorder_id, request));
            }
            self.started.send(self.recorder_id).unwrap();
            loop {
                match context.check() {
                    Ok(()) => thread::sleep(Duration::from_millis(1)),
                    Err(error @ (Error::RpcCancelled | Error::RpcDeadlineExceeded)) => {
                        self.cancelled.send(self.recorder_id).unwrap();
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }

    struct SlotRecorder {
        recorder_id: &'static str,
        reject_slot: Option<u64>,
        observed: Option<mpsc::SyncSender<u64>>,
    }

    impl RecorderRpc for SlotRecorder {
        fn record(
            &self,
            _context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            if let Some(observed) = &self.observed {
                observed.send(request.slot).unwrap();
            }
            if self.reject_slot == Some(request.slot) {
                Err(Error::Rejected(RejectReason::InvalidRequest))
            } else {
                Ok(record_summary(self.recorder_id, request))
            }
        }
    }

    struct PanickingRecorder {
        mutated: Arc<AtomicBool>,
    }

    impl RecorderRpc for PanickingRecorder {
        fn record(
            &self,
            _context: &RecorderRpcContext,
            _request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            self.mutated.store(true, Ordering::Release);
            panic!("injected recorder panic")
        }
    }

    struct PanicThenSuccessfulRecordRecorder {
        recorder_id: &'static str,
        calls: AtomicUsize,
        mutated: Arc<AtomicBool>,
    }

    impl RecorderRpc for PanicThenSuccessfulRecordRecorder {
        fn record(
            &self,
            _context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
                self.mutated.store(true, Ordering::Release);
                panic!("injected record panic after mutation")
            }
            Ok(record_summary(self.recorder_id, request))
        }
    }

    struct PanicAfterMutationControlRecorder {
        mutations: Arc<AtomicUsize>,
    }

    impl RecorderRpc for PanicAfterMutationControlRecorder {
        fn install_decision_proof(
            &self,
            _context: &RecorderRpcContext,
            _proof: DecisionProof,
            _membership: &Membership,
        ) -> super::Result<()> {
            self.mutations.fetch_add(1, Ordering::Release);
            panic!("injected proof-install panic after mutation")
        }

        fn store_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
            _command: StoredCommand,
        ) -> super::Result<()> {
            self.mutations.fetch_add(1, Ordering::Release);
            panic!("injected command-store panic after mutation")
        }

        fn fetch_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            panic!("injected read-only fetch panic")
        }

        fn inspect_decision_proof(
            &self,
            _context: &RecorderRpcContext,
            _slot: Slot,
        ) -> super::Result<Option<DecisionProof>> {
            panic!("injected read-only inspection panic")
        }

        fn inspect_record_summary(
            &self,
            _context: &RecorderRpcContext,
            _slot: Slot,
        ) -> super::Result<Option<RecordSummary>> {
            panic!("injected read-only summary inspection panic")
        }

        fn observe_read_fence(
            &self,
            _context: &RecorderRpcContext,
            _request: ReadFenceRequest,
        ) -> super::Result<ReadFenceObservation> {
            panic!("injected read-only fence observation panic")
        }
    }

    fn test_decision_proof(membership: &Membership) -> DecisionProof {
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue {
                command_hash: LogHash::ZERO,
                prev_hash: LogHash::ZERO,
                entry_hash: LogHash::ZERO,
            },
        );
        DecisionProof::FastPath {
            cluster_id: "cluster".into(),
            slot: 1,
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            proposal,
            summaries: Vec::new(),
        }
    }

    #[test]
    fn installed_proof_is_idempotent_for_the_same_value_and_rejects_conflict() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let proof = test_decision_proof(&membership);
        let mut state = RecorderSlotState::new_with_digest(1, "cluster", 1, 1, membership.digest());
        assert_eq!(state.install_proof(proof.clone()), Ok(()));
        assert_eq!(state.install_proof(proof.clone()), Ok(()));

        let mut conflicting = proof;
        let DecisionProof::FastPath { proposal, .. } = &mut conflicting else {
            unreachable!("test decision proof is FastPath");
        };
        proposal
            .value
            .as_mut()
            .expect("test decision proof has a value")
            .entry_hash = LogHash::digest(&[b"conflicting"]);
        assert_eq!(
            state.install_proof(conflicting),
            Err(RejectReason::AlreadyDecided)
        );
    }

    #[test]
    fn one_record_summary_without_a_proof_is_unavailable_not_empty() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = |recorder_id| {
            RecorderFileStore::new_with_membership(
                root.path().join(recorder_id),
                recorder_id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        };
        let n1 = store("n1");
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "n1",
            1,
            1,
            vec![
                ("n1".into(), Box::new(n1.clone()) as Box<dyn RecorderRpc>),
                ("n2".into(), Box::new(store("n2")) as Box<dyn RecorderRpc>),
                ("n3".into(), Box::new(store("n3")) as Box<dyn RecorderRpc>),
            ],
        )
        .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"one-summary".to_vec());
        n1.record_proposal(RecordRequest {
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
                AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
            ),
            command: Some(command),
        })
        .unwrap();
        assert_eq!(n1.inspect_decision_proof(1).unwrap(), None);
        assert_eq!(
            consensus.inspect_certified_decision_at(
                &RecorderRpcContext::default_timeout(),
                1,
                LogHash::ZERO,
            ),
            Ok(CertifiedDecisionInspection::Unavailable)
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    struct FailingFromSlotRecorder {
        recorder_id: &'static str,
        fail_from: u64,
    }

    impl RecorderRpc for FailingFromSlotRecorder {
        fn record(
            &self,
            _context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            if request.slot >= self.fail_from {
                Err(Error::ProposeFailed)
            } else {
                Ok(record_summary(self.recorder_id, request))
            }
        }
    }

    struct AlwaysIoRecorder;

    impl RecorderRpc for AlwaysIoRecorder {
        fn record(
            &self,
            _context: &RecorderRpcContext,
            _request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            Err(Error::Io("injected recorder unavailable".into()))
        }
    }

    struct MissingCommandRecorder {
        observed: mpsc::SyncSender<()>,
    }

    impl RecorderRpc for MissingCommandRecorder {
        fn fetch_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            self.observed.send(()).unwrap();
            Ok(None)
        }
    }

    struct GatedMissingCommandRecorder {
        observed: mpsc::SyncSender<()>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl RecorderRpc for GatedMissingCommandRecorder {
        fn fetch_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            self.observed.send(()).unwrap();
            let (released, condition) = &*self.gate;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
            Ok(None)
        }
    }

    struct BlockingCommandRecorder {
        started: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        command: StoredCommand,
    }

    struct AvailableCommandRecorder {
        command: StoredCommand,
    }

    impl RecorderRpc for AvailableCommandRecorder {
        fn fetch_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            Ok(Some(self.command.clone()))
        }
    }

    struct FailingCommandFetchRecorder;

    impl RecorderRpc for FailingCommandFetchRecorder {
        fn fetch_command_for(
            &self,
            _context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            Err(Error::ProposeFailed)
        }
    }

    impl RecorderRpc for BlockingCommandRecorder {
        fn fetch_command_for(
            &self,
            context: &RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            self.started.send(()).unwrap();
            loop {
                context.check()?;
                match self
                    .release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_millis(5))
                {
                    Ok(()) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => return Err(Error::RpcCancelled),
                }
            }
            Ok(Some(self.command.clone()))
        }
    }

    struct FailingPrioritySource;

    impl PrioritySource for FailingPrioritySource {
        fn sample(
            &self,
            _slot: u64,
            _round: u64,
            _proposer_id: &str,
            _recorder_id: &str,
        ) -> super::Result<ProposalPriority> {
            Err(Error::RandomnessUnavailable("unexpected sample".into()))
        }
    }

    #[derive(Default)]
    struct CountingPrioritySource {
        samples: AtomicUsize,
    }

    impl PrioritySource for CountingPrioritySource {
        fn sample(
            &self,
            _slot: u64,
            _round: u64,
            _proposer_id: &str,
            _recorder_id: &str,
        ) -> super::Result<ProposalPriority> {
            let sample = self.samples.fetch_add(1, Ordering::Relaxed) + 1;
            Ok(ProposalPriority::from_u64(sample as u64))
        }
    }

    struct CatchUpRecorder {
        recorder_id: &'static str,
        step: u64,
    }

    impl RecorderRpc for CatchUpRecorder {
        fn record(
            &self,
            _context: &RecorderRpcContext,
            request: RecordRequest,
        ) -> super::Result<RecordSummary> {
            let mut summary = record_summary(self.recorder_id, request);
            summary.step = self.step;
            Ok(summary)
        }
    }

    #[test]
    fn single_node_consensus_commits_contiguous_hash_chain() {
        let consensus = SingleNodeConsensus::new("cluster-a", 1, 1);
        let first = consensus
            .propose(
                RecorderRpcContext::default_timeout(),
                Command::new(CommandKind::Deterministic, b"first".to_vec()),
            )
            .unwrap();
        let second = consensus
            .propose(
                RecorderRpcContext::default_timeout(),
                Command::new(CommandKind::Deterministic, b"second".to_vec()),
            )
            .unwrap();

        assert_eq!(first.index, 1);
        assert_eq!(first.prev_hash, LogHash::ZERO);
        assert_eq!(first.hash, first.recompute_hash());
        assert_eq!(second.index, 2);
        assert_eq!(second.prev_hash, first.hash);
        assert_eq!(second.hash, second.recompute_hash());
    }

    #[test]
    fn cached_phase_zero_priorities_do_not_resample_randomness() {
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "writer",
            1,
            1,
            ["n1", "n2", "n3"]
                .into_iter()
                .map(|recorder_id| {
                    (
                        recorder_id.into(),
                        Box::new(SlotRecorder {
                            recorder_id,
                            reject_slot: None,
                            observed: None,
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect(),
        )
        .unwrap()
        .with_priority_source(Arc::new(FailingPrioritySource));
        let command = StoredCommand::new(EntryType::Command, b"cached-priority".to_vec());
        let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
        let mut progress =
            ProposerProgress::new(1, Proposal::new(ProposalPriority::MAX, "writer", 1, value))
                .with_command(command);
        progress.step = 0;
        for recorder_id in ["n1", "n2", "n3"] {
            progress
                .phase_zero_priorities
                .insert((0, recorder_id.into()), ProposalPriority::from_u64(1));
        }

        let DriveOutcome::Progress(progress) = consensus
            .drive(&RecorderRpcContext::default_timeout(), progress)
            .unwrap()
        else {
            panic!("phase zero quorum should advance progress");
        };
        assert!(progress.phase_zero_priorities.is_empty());
    }

    #[test]
    fn phase_zero_priorities_are_stable_only_for_pending_retries_in_the_current_round() {
        let source = Arc::new(CountingPrioritySource::default());
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "writer",
            1,
            1,
            vec![
                (
                    "n1".into(),
                    Box::new(SlotRecorder {
                        recorder_id: "n1",
                        reject_slot: None,
                        observed: None,
                    }) as Box<dyn RecorderRpc>,
                ),
                ("n2".into(), Box::new(AlwaysIoRecorder)),
                ("n3".into(), Box::new(AlwaysIoRecorder)),
            ],
        )
        .unwrap()
        .with_priority_source(source.clone());
        let command = StoredCommand::new(EntryType::Command, b"bounded-priorities".to_vec());
        let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
        let mut progress =
            ProposerProgress::new(1, Proposal::new(ProposalPriority::MAX, "writer", 1, value))
                .with_command(command);
        progress.step = 0;

        let DriveOutcome::Pending(mut progress) = consensus
            .drive(&RecorderRpcContext::default_timeout(), progress)
            .unwrap()
        else {
            panic!("one recorder reply should leave progress pending");
        };
        assert_eq!(progress.phase_zero_priorities.len(), 3);
        assert_eq!(source.samples.load(Ordering::Relaxed), 3);

        let DriveOutcome::Pending(retry) = consensus
            .drive(&RecorderRpcContext::default_timeout(), progress.clone())
            .unwrap()
        else {
            panic!("same-round retry should remain pending");
        };
        assert_eq!(retry.phase_zero_priorities, progress.phase_zero_priorities);
        assert_eq!(source.samples.load(Ordering::Relaxed), 3);

        for round in 1..=64 {
            progress.step = round * 4;
            let DriveOutcome::Pending(next) = consensus
                .drive(&RecorderRpcContext::default_timeout(), progress)
                .unwrap()
            else {
                panic!("one recorder reply should leave progress pending");
            };
            assert_eq!(next.phase_zero_priorities.len(), 3);
            assert!(next
                .phase_zero_priorities
                .keys()
                .all(|(cached_round, _)| *cached_round == round));
            progress = next;
        }
        assert_eq!(source.samples.load(Ordering::Relaxed), 3 * 65);
    }

    #[test]
    fn phase_zero_priorities_are_cleared_when_progress_catches_up() {
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "writer",
            1,
            1,
            ["n1", "n2", "n3"]
                .into_iter()
                .map(|recorder_id| {
                    (
                        recorder_id.into(),
                        Box::new(CatchUpRecorder {
                            recorder_id,
                            step: 8,
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect(),
        )
        .unwrap()
        .with_priority_source(Arc::new(FailingPrioritySource));
        let command = StoredCommand::new(EntryType::Command, b"catch-up".to_vec());
        let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
        let mut progress =
            ProposerProgress::new(1, Proposal::new(ProposalPriority::MAX, "writer", 1, value))
                .with_command(command);
        progress.step = 0;
        for recorder_id in ["n1", "n2", "n3"] {
            progress
                .phase_zero_priorities
                .insert((0, recorder_id.into()), ProposalPriority::from_u64(1));
        }

        let DriveOutcome::Progress(progress) = consensus
            .drive(&RecorderRpcContext::default_timeout(), progress)
            .unwrap()
        else {
            panic!("higher recorder steps should catch progress up");
        };
        assert_eq!(progress.step, 8);
        assert!(progress.phase_zero_priorities.is_empty());
    }

    #[test]
    fn wal_command_cache_rejects_same_hash_with_different_payload() {
        let hash = LogHash::digest(&[b"forced-cache-key"]);
        let first = StoredCommand::new(EntryType::Command, b"first".to_vec());
        let second = StoredCommand::new(EntryType::Command, b"second".to_vec());
        let mut commands = HashMap::new();

        upsert_wal_command(&mut commands, hash, &first).unwrap();
        upsert_wal_command(&mut commands, hash, &first).unwrap();
        assert_eq!(
            upsert_wal_command(&mut commands, hash, &second),
            Err(Error::CommandHashMismatch)
        );
        assert_eq!(commands.len(), 1);
    }

    #[test]
    fn normal_record_uses_one_file_sync_and_no_directory_barrier() {
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
        let command = StoredCommand::new(EntryType::Command, b"barrier-count".to_vec());
        store
            .store_command(command.hash(), command.clone())
            .unwrap();
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        reset_sync_counts();

        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: None,
            })
            .unwrap();

        assert_eq!(sync_counts(), (1, 0));
        assert!(!root.path().join("slot-head.intent").exists());

        let inline = StoredCommand::new(EntryType::Command, b"inline-command".to_vec());
        let inline_value = AcceptedValue::from_command("cluster", 9, 1, 1, LogHash::ZERO, &inline);
        reset_sync_counts();
        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 9,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 2, inline_value),
                command: Some(inline),
            })
            .unwrap();

        assert_eq!(sync_counts(), (1, 0));
    }

    #[test]
    fn wal_durable_command_store_adds_no_sync_and_survives_proof_checkpoint_recovery() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"proof-worker-command".to_vec());
        let command_hash = command.hash();
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        let proposal = Proposal::new(ProposalPriority::MAX, "writer", 1, value);
        let proof = DecisionProof::FastPath {
            cluster_id: "cluster".into(),
            slot: 8,
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            proposal: proposal.clone(),
            summaries: ["n1", "n2"]
                .into_iter()
                .map(|recorder_id| RecorderSummary {
                    recorder_id: recorder_id.into(),
                    slot: 8,
                    step: 4,
                    first_current: Some(proposal.clone()),
                    aggregate_prior: None,
                })
                .collect(),
        };
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal,
                command: Some(command.clone()),
            })
            .unwrap();

        reset_sync_counts();
        RecorderRpc::store_command_for(
            &store,
            &RecorderRpcContext::default_timeout(),
            "cluster".into(),
            1,
            1,
            membership.digest(),
            command_hash,
            command.clone(),
        )
        .unwrap();
        assert_eq!(sync_counts(), (0, 0));
        assert!(!store.command_path(command_hash).exists());

        store
            .install_decision_proof_record(proof.clone(), &membership)
            .unwrap();
        drop(store);

        let reopened = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        assert_eq!(
            reopened.fetch_command(command_hash).unwrap(),
            Some(command.clone())
        );
        assert_eq!(reopened.load(8).unwrap().decision_proof(), Some(&proof));
        reopened.checkpoint_wal_unlocked().unwrap();
        assert!(reopened.command_path(command_hash).exists());
        drop(reopened);

        let checkpointed =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(
            checkpointed.fetch_command(command_hash).unwrap(),
            Some(command)
        );
        assert_eq!(checkpointed.load(8).unwrap().decision_proof(), Some(&proof));
    }

    #[test]
    fn duplicate_command_file_store_keeps_the_root_directory_barrier() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"durable-command".to_vec());
        store
            .store_command(command.hash(), command.clone())
            .unwrap();
        reset_sync_counts();

        store.store_command(command.hash(), command).unwrap();

        assert_eq!(sync_counts(), (0, 1));
    }

    #[test]
    fn direct_store_command_rejects_a_claimed_hash_without_creating_a_file() {
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
        let command = StoredCommand::new(EntryType::Command, b"mismatched-hash".to_vec());
        let claimed_hash = LogHash::digest(&[b"claimed-hash"]);

        assert_eq!(
            store.apply(RecorderRequest::StoreCommand {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                command_hash: claimed_hash,
                command,
            }),
            Err(Error::CommandHashMismatch)
        );
        assert!(!store.command_path(claimed_hash).exists());
    }

    #[test]
    fn recorder_rejects_commands_over_the_documented_payload_limit() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root.path().join("n1"),
            "n1",
            "cluster",
            1,
            1,
            membership,
        )
        .unwrap();
        let command = StoredCommand::new(
            EntryType::Command,
            vec![0_u8; super::MAX_REPLICATED_COMMAND_BYTES + 1],
        );
        assert_eq!(
            store.store_command(command.hash(), command),
            Err(Error::CommandTooLarge {
                actual: super::MAX_REPLICATED_COMMAND_BYTES + 1,
                limit: super::MAX_REPLICATED_COMMAND_BYTES,
            })
        );
    }

    #[test]
    fn recorder_rpc_context_distinguishes_cancellation_from_deadline_expiry() {
        let cancelled = RecorderRpcContext::with_timeout(Duration::from_secs(1));
        cancelled.cancel();
        assert_eq!(cancelled.check(), Err(Error::RpcCancelled));

        let expired = RecorderRpcContext::with_timeout(Duration::ZERO);
        assert_eq!(expired.check(), Err(Error::RpcDeadlineExceeded));
    }

    #[test]
    fn wal_command_store_rejects_conflicting_bytes_without_syncing() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"expected".to_vec());
        let conflicting = StoredCommand::new(EntryType::Command, b"conflicting".to_vec());
        store
            .wal
            .lock()
            .unwrap()
            .commands
            .insert(command.hash(), conflicting);
        reset_sync_counts();

        assert_eq!(
            store.store_command(command.hash(), command.clone()),
            Err(Error::CommandHashMismatch)
        );
        assert_eq!(sync_counts(), (0, 0));
        assert!(!store.command_path(command.hash()).exists());
    }

    #[test]
    fn prestored_command_resolution_revalidates_the_durable_file() {
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
        let command = StoredCommand::new(EntryType::Command, b"pre-stored-command".to_vec());
        store
            .store_command(command.hash(), command.clone())
            .unwrap();
        reset_command_file_reads();

        for slot in [8, 9] {
            let value = AcceptedValue::from_command("cluster", slot, 1, 1, LogHash::ZERO, &command);
            store
                .record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", slot, value),
                    command: None,
                })
                .unwrap();
        }

        assert_eq!(command_file_reads(), 2);
    }

    #[test]
    fn commandless_record_rejects_a_prestored_command_replaced_while_open() {
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
        let command = StoredCommand::new(EntryType::Command, b"pre-stored".to_vec());
        let replacement = StoredCommand::new(EntryType::Command, b"replacement".to_vec());
        let command_hash = command.hash();
        store.store_command(command_hash, command.clone()).unwrap();
        std::fs::write(
            store.command_path(command_hash),
            encode_stored_command(&replacement),
        )
        .unwrap();
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);

        assert_eq!(
            store.record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value.clone()),
                command: None,
            }),
            Err(Error::CommandHashMismatch)
        );
        assert_eq!(store.load(8).unwrap().isr.step(), 0);
        drop(store);

        let reopened = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        assert_eq!(
            reopened.record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: None,
            }),
            Err(Error::CommandHashMismatch)
        );
        assert_eq!(reopened.load(8).unwrap().isr.step(), 0);
    }

    #[test]
    fn duplicate_store_rejects_a_prestored_command_replaced_while_open() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"pre-stored".to_vec());
        let replacement = StoredCommand::new(EntryType::Command, b"replacement".to_vec());
        let command_hash = command.hash();
        store.store_command(command_hash, command.clone()).unwrap();
        std::fs::write(
            store.command_path(command_hash),
            encode_stored_command(&replacement),
        )
        .unwrap();

        assert_eq!(
            store.store_command(command_hash, command),
            Err(Error::CommandHashMismatch)
        );
    }

    #[test]
    fn record_rejects_mismatched_inline_command_independent_of_prestored_command_state() {
        for prestored in [false, true] {
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
            let expected = StoredCommand::new(EntryType::Command, b"expected".to_vec());
            if prestored {
                store
                    .store_command(expected.hash(), expected.clone())
                    .unwrap();
            }
            let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &expected);
            let mismatched = StoredCommand::new(EntryType::Command, b"mismatched".to_vec());

            assert_eq!(
                store.record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot: 8,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                    command: Some(mismatched),
                }),
                Err(Error::Rejected(RejectReason::InvalidValue)),
                "prestored={prestored}"
            );
        }
    }

    #[test]
    fn inline_record_uses_the_bound_command_without_reading_the_durable_command_file() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"inline-hot-path".to_vec());
        let command_hash = command.hash();
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        std::fs::write(store.command_path(command_hash), b"corrupt cache entry").unwrap();
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        reset_command_file_reads();

        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: Some(command.clone()),
            })
            .unwrap();

        assert_eq!(command_file_reads(), 0);
        assert_eq!(
            store.fetch_command(command_hash).unwrap(),
            Some(command.clone())
        );
        drop(store);

        reset_command_file_reads();
        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(command_file_reads(), 0);
        assert_eq!(reopened.fetch_command(command_hash).unwrap(), Some(command));
    }

    #[test]
    fn record_rejects_malformed_config_change_piggyback_as_invalid_value_before_parsing() {
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
        let expected = StoredCommand::new(EntryType::Command, b"expected".to_vec());
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &expected);
        let malformed = StoredCommand::new(EntryType::ConfigChange, b"malformed".to_vec());

        assert_eq!(
            store.record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: Some(malformed),
            }),
            Err(Error::Rejected(RejectReason::InvalidValue))
        );
    }

    #[test]
    fn wal_append_uses_the_platform_safe_file_sync() {
        let file = tempfile::tempfile().unwrap();
        reset_sync_counts();

        sync_wal_append(&file).unwrap();

        #[cfg(target_os = "linux")]
        assert_eq!(last_file_sync_kind(), Some(FileSyncKind::Data));
        #[cfg(not(target_os = "linux"))]
        assert_eq!(last_file_sync_kind(), Some(FileSyncKind::All));
    }

    #[test]
    fn wal_metadata_changes_keep_full_file_sync() {
        let file = tempfile::tempfile().unwrap();
        reset_sync_counts();

        sync_wal_metadata(&file).unwrap();

        assert_eq!(last_file_sync_kind(), Some(FileSyncKind::All));
    }

    #[test]
    fn wal_replays_acknowledged_records_after_reopen() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"wal-reopen".to_vec());
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            store
                .record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot: 8,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                    command: Some(command.clone()),
                })
                .unwrap();
        }

        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(
            reopened.fetch_command(command.hash()).unwrap(),
            Some(command)
        );
        assert_eq!(reopened.load(8).unwrap().isr.step(), 4);
    }

    #[test]
    fn wal_sync_fault_never_acknowledges_before_the_durable_frame_is_replayable() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"wal-sync-fault".to_vec());
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            store
                .set_seal_fault(Some(SealFaultPoint::AfterWalSync))
                .unwrap();
            assert!(matches!(
                store.record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot: 8,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                    command: Some(command.clone()),
                }),
                Err(Error::Io(message)) if message.contains("AfterWalSync")
            ));
            assert_eq!(
                store
                    .configuration_state()
                    .unwrap()
                    .max_accepted_or_decided_slot(),
                None
            );
            assert_eq!(store.load(8).unwrap().isr.step(), 0);
        }

        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(
            reopened.fetch_command(command.hash()).unwrap(),
            Some(command)
        );
        assert_eq!(reopened.load(8).unwrap().isr.step(), 4);
    }

    #[test]
    fn wal_ignores_a_torn_final_frame_but_replays_the_committed_prefix() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let first = StoredCommand::new(EntryType::Command, b"wal-first".to_vec());
        let second = StoredCommand::new(EntryType::Command, b"wal-second".to_vec());
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            for (slot, command) in [(8, first.clone()), (9, second.clone())] {
                let value =
                    AcceptedValue::from_command("cluster", slot, 1, 1, LogHash::ZERO, &command);
                store
                    .record_proposal(RecordRequest {
                        cluster_id: "cluster".into(),
                        epoch: 1,
                        config_id: 1,
                        config_digest: membership.digest(),
                        slot,
                        step: 4,
                        proposal: Proposal::new(ProposalPriority::MAX, "writer", slot, value),
                        command: Some(command),
                    })
                    .unwrap();
            }
        }
        let wal = root.path().join("recorder.wal");
        let len = std::fs::metadata(&wal).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&wal)
            .unwrap()
            .set_len(len - 7)
            .unwrap();

        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(reopened.fetch_command(first.hash()).unwrap(), Some(first));
        assert_eq!(reopened.load(8).unwrap().isr.step(), 4);
        assert_eq!(reopened.fetch_command(second.hash()).unwrap(), None);
        assert_eq!(reopened.load(9).unwrap().isr.step(), 0);
    }

    #[test]
    fn recorder_preflight_distinguishes_absent_from_valid_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let absent = root.path().join("absent");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &absent,
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Missing
        );
        assert!(!absent.exists());

        let valid = root.path().join("valid");
        drop(
            RecorderFileStore::new_with_membership(
                &valid,
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap(),
        );
        let before = directory_files(&valid);
        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &valid,
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Valid
        );
        assert_eq!(directory_files(&valid), before);
    }

    #[test]
    fn recorder_preflight_rejects_partial_or_foreign_state_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let partial = root.path().join("partial");
        std::fs::create_dir(&partial).unwrap();
        std::fs::write(partial.join("recorded-head.rec"), b"partial").unwrap();
        let partial_before = directory_files(&partial);
        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &partial,
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert_eq!(directory_files(&partial), partial_before);

        let valid = root.path().join("foreign");
        drop(
            RecorderFileStore::new_with_membership(
                &valid,
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap(),
        );
        let before = directory_files(&valid);
        let other_membership = Membership::new(["n1", "n2", "n4"]).unwrap();
        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &valid,
                "cluster",
                1,
                2,
                &membership,
            )
            .is_err()
        );
        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &valid,
                "cluster",
                1,
                1,
                &other_membership,
            )
            .is_err()
        );
        assert_eq!(directory_files(&valid), before);
    }

    #[test]
    fn recorder_preflight_validates_command_cache_backed_values_without_mutation() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        for case in ["valid", "missing", "corrupt", "foreign"] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join(case);
            let (command_hash, command) =
                cache_backed_recorder_for_preflight(&root, membership.clone());
            let command_path = root.join(format!("command-{}.cmd", command_hash.to_hex()));
            match case {
                "valid" => {}
                "missing" => std::fs::remove_file(&command_path).unwrap(),
                "corrupt" => std::fs::write(&command_path, b"corrupt command cache").unwrap(),
                "foreign" => std::fs::write(
                    &command_path,
                    encode_stored_command(&StoredCommand::new(
                        EntryType::Command,
                        b"different command".to_vec(),
                    )),
                )
                .unwrap(),
                _ => unreachable!(),
            }
            let before = directory_files(&root);
            let result = RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            );
            if case == "valid" {
                assert_eq!(result.unwrap(), RecorderPreflight::Valid, "{case}");
                assert_eq!(
                    std::fs::read(&command_path).unwrap(),
                    encode_stored_command(&command),
                    "{case}"
                );
            } else {
                assert!(result.is_err(), "{case}");
            }
            assert_eq!(directory_files(&root), before, "{case}");
        }
    }

    #[test]
    fn recorder_preflight_rejects_nonregular_command_cache_without_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let (command_hash, _) = cache_backed_recorder_for_preflight(&root, membership.clone());
        let command_path = root.join(format!("command-{}.cmd", command_hash.to_hex()));
        std::fs::remove_file(&command_path).unwrap();
        std::fs::create_dir(&command_path).unwrap();
        let before = directory_files(&root);

        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert_eq!(directory_files(&root), before);
    }

    #[cfg(unix)]
    #[test]
    fn recorder_preflight_rejects_symlinked_command_cache_without_mutation() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let (command_hash, command) =
            cache_backed_recorder_for_preflight(&root, membership.clone());
        let command_path = root.join(format!("command-{}.cmd", command_hash.to_hex()));
        let outside = temp.path().join("outside-command.cmd");
        std::fs::write(&outside, encode_stored_command(&command)).unwrap();
        std::fs::remove_file(&command_path).unwrap();
        symlink(&outside, &command_path).unwrap();

        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert!(std::fs::symlink_metadata(&command_path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::read(&outside).unwrap(),
            encode_stored_command(&command)
        );
    }

    #[test]
    fn recorder_preflight_rejects_command_cache_backed_value_mismatch_without_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        cache_backed_recorder_for_preflight(&root, membership.clone());
        let wal_path = root.join("recorder.wal");
        let original = std::fs::read(&wal_path).unwrap();
        let (frame, end) = decode_wal_frame(&original, 0).unwrap().unwrap();
        assert_eq!(end, original.len());
        let mut state = decode_recorder_state(&frame.slot_bytes).unwrap();
        state
            .isr
            .first_current
            .as_mut()
            .unwrap()
            .value
            .as_mut()
            .unwrap()
            .entry_hash = LogHash::ZERO;
        let configuration = decode_configuration_state(&frame.configuration_bytes).unwrap();
        let (tampered, _, _) = encode_wal_frame(
            frame.generation,
            frame.sequence,
            frame.prev_digest,
            &state,
            &configuration,
            &frame.head,
            None,
        )
        .unwrap();
        std::fs::write(&wal_path, tampered).unwrap();
        let before = directory_files(&root);

        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert_eq!(directory_files(&root), before);
    }

    #[test]
    fn recorder_preflight_allows_normal_configuration_intent_recovery_before_open() {
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
        let configuration = store.configuration_state().unwrap();
        let state = RecorderSlotState::new_with_digest(8, "cluster", 1, 1, membership.digest());
        store
            .set_seal_fault(Some(SealFaultPoint::AfterIntent))
            .unwrap();
        assert!(store
            .commit_transition_unlocked(&state, &configuration)
            .is_err());
        drop(store);
        let before = directory_files(root.path());

        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                root.path(),
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Recoverable,
        );
        assert_eq!(directory_files(root.path()), before);
        RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
            .unwrap();
        assert!(!root.path().join("configuration.intent").exists());
    }

    #[test]
    fn recorder_preflight_allows_torn_final_wal_recovery_before_open() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"torn-preflight".to_vec());
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
            store
                .record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot: 8,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                    command: Some(command.clone()),
                })
                .unwrap();
        }
        let wal = root.path().join("recorder.wal");
        let len = std::fs::metadata(&wal).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&wal)
            .unwrap()
            .set_len(len - 7)
            .unwrap();
        let before = directory_files(root.path());

        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                root.path(),
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Recoverable,
        );
        assert_eq!(directory_files(root.path()), before);
        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(reopened.load(8).unwrap().isr.step(), 0);
    }

    #[test]
    fn recorder_preflight_keeps_interior_wal_corruption_invalid_without_mutation() {
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
        let command = StoredCommand::new(EntryType::Command, b"interior-preflight".to_vec());
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: Some(command),
            })
            .unwrap();
        drop(store);
        let wal = root.path().join("recorder.wal");
        let mut bytes = std::fs::read(&wal).unwrap();
        bytes[100] ^= 0x80;
        std::fs::write(&wal, bytes).unwrap();
        let before = directory_files(root.path());

        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                root.path(),
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert_eq!(directory_files(root.path()), before);
    }

    #[test]
    fn recorder_preflight_rejects_oversized_wal_cache_and_intent_without_mutation() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        for (name, limit) in [
            ("recorder.wal", super::MAX_RECORDER_WAL_BYTES),
            ("configuration.intent", super::MAX_TRANSITION_INTENT_BYTES),
        ] {
            let root = tempfile::tempdir().unwrap();
            drop(
                RecorderFileStore::new_with_membership(
                    root.path(),
                    "n1",
                    "cluster",
                    1,
                    1,
                    membership.clone(),
                )
                .unwrap(),
            );
            let path = root.path().join(name);
            std::fs::File::create(&path)
                .unwrap()
                .set_len(limit as u64 + 1)
                .unwrap();
            let before_len = std::fs::symlink_metadata(&path).unwrap().len();
            assert!(
                RecorderFileStore::preflight_existing_with_membership_outcome(
                    root.path(),
                    "cluster",
                    1,
                    1,
                    &membership,
                )
                .is_err()
            );
            assert_eq!(std::fs::symlink_metadata(&path).unwrap().len(), before_len);
        }

        let root = tempfile::tempdir().unwrap();
        let (command_hash, _) =
            cache_backed_recorder_for_preflight(root.path(), membership.clone());
        let path = root
            .path()
            .join(format!("command-{}.cmd", command_hash.to_hex()));
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(super::MAX_COMMAND_CACHE_BYTES as u64 + 1)
            .unwrap();
        let before_len = std::fs::symlink_metadata(&path).unwrap().len();
        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                root.path(),
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert_eq!(std::fs::symlink_metadata(&path).unwrap().len(), before_len);
    }

    #[cfg(unix)]
    #[test]
    fn recorder_preflight_rejects_symlinked_authority_files_without_mutation() {
        use std::os::unix::fs::symlink;

        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        for name in [
            "configuration.rec",
            "recorded-head.rec",
            "recorder.wal",
            "configuration.intent",
            "configuration-head.intent",
        ] {
            let root = tempfile::tempdir().unwrap();
            drop(
                RecorderFileStore::new_with_membership(
                    root.path(),
                    "n1",
                    "cluster",
                    1,
                    1,
                    membership.clone(),
                )
                .unwrap(),
            );
            let path = root.path().join(name);
            let outside = root.path().join(format!("outside-{name}"));
            if path.exists() {
                std::fs::rename(&path, &outside).unwrap();
            } else {
                std::fs::write(&outside, b"intent").unwrap();
            }
            symlink(&outside, &path).unwrap();

            assert!(
                RecorderFileStore::preflight_existing_with_membership_outcome(
                    root.path(),
                    "cluster",
                    1,
                    1,
                    &membership,
                )
                .is_err(),
                "{name}"
            );
            assert!(
                std::fs::symlink_metadata(&path)
                    .unwrap()
                    .file_type()
                    .is_symlink(),
                "{name}"
            );
        }
    }

    #[test]
    fn recorder_open_revalidates_after_preflight_before_recovery_mutation() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        drop(
            RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap(),
        );
        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                root.path(),
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Valid,
        );
        let configuration = root.path().join("configuration.rec");
        std::fs::write(&configuration, b"replaced after preflight").unwrap();
        let before = directory_files(root.path());

        assert!(RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership,
        )
        .is_err());
        assert_eq!(directory_files(root.path()), before);
    }

    #[test]
    fn existing_open_never_recreates_a_deleted_or_replaced_valid_recorder_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        drop(
            RecorderFileStore::new_with_membership(
                &root,
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap(),
        );
        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Valid,
        );
        std::fs::remove_dir_all(&root).unwrap();
        let parent_before = directory_files(parent.path());
        assert!(RecorderFileStore::open_existing_with_membership(
            &root,
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .is_err());
        assert!(!root.exists());
        assert_eq!(directory_files(parent.path()), parent_before);

        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("replacement"), b"foreign").unwrap();
        let parent_before = directory_files(parent.path());
        assert!(RecorderFileStore::open_existing_with_membership(
            &root, "n1", "cluster", 1, 1, membership,
        )
        .is_err());
        assert!(!root.join(".recorder.lock").exists());
        assert_eq!(directory_files(parent.path()), parent_before);
    }

    #[cfg(unix)]
    #[test]
    fn post_preflight_root_swap_keeps_all_durable_io_and_lock_on_the_anchor() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("recorder");
        let retained = parent.path().join("retained-recorder");
        let staged = parent.path().join("replacement-recorder");
        let external = parent.path().join("external-target");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        drop(
            RecorderFileStore::new_with_membership(
                &root,
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap(),
        );
        std::fs::write(&external, b"external-bytes").unwrap();

        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        *lock_unpoison(RECORDER_POST_PREFLIGHT_HOOK.get_or_init(|| Mutex::new(None))) =
            Some(RecorderPostPreflightHook {
                root: root.clone(),
                entered: entered_tx,
                release: release_rx,
            });
        let open_root = root.clone();
        let open_membership = membership.clone();
        let opener = thread::spawn(move || {
            RecorderFileStore::open_existing_with_membership(
                open_root,
                "n1",
                "cluster",
                1,
                1,
                open_membership,
            )
        });
        entered_rx.recv().unwrap();

        std::fs::create_dir(&staged).unwrap();
        for entry in std::fs::read_dir(&root).unwrap() {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            std::fs::copy(entry.path(), staged.join(entry.file_name())).unwrap();
        }
        std::fs::rename(&root, &retained).unwrap();
        std::fs::rename(&staged, &root).unwrap();
        symlink(&external, root.join("external-target-link")).unwrap();
        let replacement_before = directory_files(&root);
        let external_before = std::fs::read(&external).unwrap();
        release_tx.send(()).unwrap();
        let store = opener.join().unwrap().unwrap();

        let command = StoredCommand::new(EntryType::Command, b"anchored-after-swap".to_vec());
        store
            .store_command(command.hash(), command.clone())
            .unwrap();
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: None,
            })
            .unwrap();
        store.checkpoint_wal_unlocked().unwrap();
        let configuration = store.configuration_state().unwrap();
        let head = store.recorded_head.lock().unwrap().clone();
        store
            .commit_configuration_head_unlocked(&configuration, &head)
            .unwrap();
        store
            .commit_transition_unlocked(
                &RecorderSlotState::new_with_digest(9, "cluster", 1, 1, membership.digest()),
                &configuration,
            )
            .unwrap();

        assert_eq!(directory_files(&root), replacement_before);
        assert_eq!(std::fs::read(&external).unwrap(), external_before);
        assert!(retained.join("slot-00000000000000000009.rec").exists());
        assert!(matches!(
            RecorderFileStore::open_existing_with_membership(
                &retained,
                "n1-second",
                "cluster",
                1,
                1,
                membership,
            ),
            Err(Error::RecorderRootLocked(path)) if path == retained
        ));
    }

    #[test]
    fn existing_open_never_recreates_a_deleted_or_replaced_recoverable_recorder_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            &root,
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let configuration = store.configuration_state().unwrap();
        let state = RecorderSlotState::new_with_digest(8, "cluster", 1, 1, membership.digest());
        store
            .set_seal_fault(Some(SealFaultPoint::AfterIntent))
            .unwrap();
        assert!(store
            .commit_transition_unlocked(&state, &configuration)
            .is_err());
        drop(store);
        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Recoverable,
        );
        std::fs::remove_dir_all(&root).unwrap();
        let parent_before = directory_files(parent.path());
        assert!(RecorderFileStore::open_existing_with_membership(
            &root,
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .is_err());
        assert!(!root.exists());
        assert_eq!(directory_files(parent.path()), parent_before);

        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("replacement"), b"foreign").unwrap();
        let parent_before = directory_files(parent.path());
        assert!(RecorderFileStore::open_existing_with_membership(
            &root, "n1", "cluster", 1, 1, membership,
        )
        .is_err());
        assert!(!root.join(".recorder.lock").exists());
        assert_eq!(directory_files(parent.path()), parent_before);
    }

    #[cfg(unix)]
    #[test]
    fn existing_open_rejects_a_recorder_root_symlink_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let root = parent.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        std::fs::write(target.path().join("sentinel"), b"untouched").unwrap();
        let before = directory_files(target.path());
        symlink(target.path(), &root).unwrap();

        assert!(RecorderFileStore::open_existing_with_membership(
            &root, "n1", "cluster", 1, 1, membership,
        )
        .is_err());
        assert!(std::fs::symlink_metadata(&root)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(directory_files(target.path()), before);
    }

    #[cfg(unix)]
    #[test]
    fn fresh_open_rejects_a_symlinked_parent_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let root = parent.path().join("linked-parent").join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        std::fs::write(target.path().join("sentinel"), b"untouched").unwrap();
        let before = directory_files(target.path());
        symlink(target.path(), parent.path().join("linked-parent")).unwrap();

        assert!(
            RecorderFileStore::new_with_membership(&root, "n1", "cluster", 1, 1, membership,)
                .is_err()
        );
        assert!(
            std::fs::symlink_metadata(parent.path().join("linked-parent"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(!target
            .path()
            .join("recorder")
            .join(".recorder.lock")
            .exists());
        assert_eq!(directory_files(target.path()), before);
    }

    #[test]
    fn storage_generation_marker_is_exact_and_marker_only_root_is_fresh() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("recorder");
        let anchor = prepare_fresh_recorder_root(&root).unwrap();
        ensure_storage_generation(&anchor).unwrap();
        assert_eq!(
            std::fs::read(root.join(STORAGE_GENERATION_FILE)).unwrap(),
            STORAGE_GENERATION_FINGERPRINT
        );
        assert!(!current_recorder_layout(&root).unwrap());
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        RecorderFileStore::new_with_membership(&root, "n1", "cluster", 1, 1, membership).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn storage_generation_marker_symlink_is_rejected_without_touching_target() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(outside.path(), b"untouched").unwrap();
        symlink(outside.path(), root.path().join(STORAGE_GENERATION_FILE)).unwrap();
        let anchor = anchored_fs::AnchoredDir::open(root.path()).unwrap();
        assert!(ensure_storage_generation(&anchor).is_err());
        assert_eq!(std::fs::read(outside.path()).unwrap(), b"untouched");
    }

    #[test]
    fn wal_fails_closed_on_interior_corruption() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            for slot in [8, 9] {
                let command = StoredCommand::new(
                    EntryType::Command,
                    format!("wal-corrupt-{slot}").into_bytes(),
                );
                let value =
                    AcceptedValue::from_command("cluster", slot, 1, 1, LogHash::ZERO, &command);
                store
                    .record_proposal(RecordRequest {
                        cluster_id: "cluster".into(),
                        epoch: 1,
                        config_id: 1,
                        config_digest: membership.digest(),
                        slot,
                        step: 4,
                        proposal: Proposal::new(ProposalPriority::MAX, "writer", slot, value),
                        command: Some(command),
                    })
                    .unwrap();
            }
        }
        let wal = root.path().join("recorder.wal");
        let mut bytes = std::fs::read(&wal).unwrap();
        bytes[100] ^= 0x80;
        std::fs::write(&wal, bytes).unwrap();

        assert!(matches!(
            RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership,
            ),
            Err(Error::Decode(message)) if message.contains("WAL")
        ));
    }

    #[test]
    fn wal_fails_closed_on_full_length_final_frame_corruption() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            let command = StoredCommand::new(EntryType::Command, b"wal-final-corrupt".to_vec());
            let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
            store
                .record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot: 8,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                    command: Some(command),
                })
                .unwrap();
        }
        let wal = root.path().join("recorder.wal");
        let mut bytes = std::fs::read(&wal).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x80;
        std::fs::write(&wal, bytes).unwrap();

        assert!(matches!(
            RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership,
            ),
            Err(Error::Decode(message)) if message.contains("WAL frame checksum")
        ));
    }

    #[test]
    fn wal_rotation_checkpoints_before_reusing_the_stable_file() {
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
        let last_slot = super::RECORDER_WAL_HARD_FRAME_LIMIT * 2 + 1;
        let mut commands = Vec::new();
        for slot in 1..=last_slot {
            let command = StoredCommand::new(
                EntryType::Command,
                format!("wal-rotate-{slot}").into_bytes(),
            );
            commands.push((slot, command.hash(), command.clone()));
            let value = AcceptedValue::from_command("cluster", slot, 1, 1, LogHash::ZERO, &command);
            store
                .record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", slot, value),
                    command: Some(command),
                })
                .unwrap();
        }
        let (generation, through_sequence, frames) = store.wal_stats().unwrap();
        assert!(generation > 2);
        assert!(through_sequence >= super::RECORDER_WAL_HARD_FRAME_LIMIT * 2);
        assert_eq!(frames, 1);
        drop(store);

        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        for (slot, hash, command) in commands {
            assert_eq!(reopened.load(slot).unwrap().isr.step(), 4);
            assert_eq!(reopened.fetch_command(hash).unwrap(), Some(command));
        }
        let (generation, through_sequence, frames) = reopened.wal_stats().unwrap();
        assert_eq!(generation, 3);
        assert_eq!(through_sequence, super::RECORDER_WAL_HARD_FRAME_LIMIT * 2);
        assert_eq!(frames, 1);
    }

    proptest! {
        #[test]
        fn wal_frame_round_trips_arbitrary_inline_commands(
            sequence in 1u64..u64::MAX,
            payload in proptest::collection::vec(any::<u8>(), 0..2048),
        ) {
            let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
            let configuration = ConfigurationState::initial(
                1,
                membership.digest(),
                Some(membership),
            );
            let state = RecorderSlotState::new_with_digest(
                8,
                "cluster",
                1,
                1,
                configuration.config_digest(),
            );
            let command = StoredCommand::new(EntryType::Command, payload);
            let (encoded, digest, slot_bytes) = encode_wal_frame(
                3,
                sequence,
                LogHash::ZERO,
                &state,
                &configuration,
                &RecordedHeadProvenance::Empty,
                Some((command.hash(), &command)),
            ).unwrap();
            let (decoded, end) = decode_wal_frame(&encoded, 0).unwrap().unwrap();
            prop_assert_eq!(end, encoded.len());
            prop_assert_eq!(decoded.generation, 3);
            prop_assert_eq!(decoded.sequence, sequence);
            prop_assert_eq!(decoded.digest, digest);
            prop_assert_eq!(decoded.slot_bytes, slot_bytes);
            prop_assert_eq!(decoded.command, Some((command.hash(), command)));
        }
    }

    #[test]
    fn fresh_initialization_recovers_when_configuration_was_published_before_head() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let (store, _) = RecorderFileStore::open_root(root.path(), "n1", "cluster", 1, 1).unwrap();
        let configuration =
            ConfigurationState::initial(1, membership.digest(), Some(membership.clone()));
        store
            .set_seal_fault(Some(SealFaultPoint::AfterHeadConfiguration))
            .unwrap();

        assert!(matches!(
            store.commit_configuration_head_unlocked(
                &configuration,
                &RecordedHeadProvenance::Empty,
            ),
            Err(Error::Io(message))
                if message.contains("AfterHeadConfiguration")
        ));
        assert!(root.path().join("configuration.rec").exists());
        assert!(!root.path().join("recorded-head.rec").exists());
        assert!(root.path().join("configuration-head.intent").exists());
        drop(store);

        let reopened = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        assert_eq!(
            reopened.configuration_state().unwrap().membership(),
            Some(&membership)
        );
        assert!(root.path().join("recorded-head.rec").exists());
        assert!(!root.path().join("configuration-head.intent").exists());
    }

    #[test]
    fn progress_remembers_config_change_after_later_normal_adoption() {
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
        let offered = StoredCommand::new(EntryType::Command, b"offered".to_vec());
        let transition = ConfigChange::bound_stop(
            "cluster",
            1,
            membership.digest(),
            2,
            membership.members().to_vec(),
        )
        .unwrap()
        .to_stored_command();
        let adopted = StoredCommand::new(EntryType::Command, b"adopted".to_vec());
        for store in &stores {
            for command in [&transition, &adopted] {
                store
                    .store_command(command.hash(), command.clone())
                    .unwrap();
            }
        }
        let recorders = membership
            .members()
            .iter()
            .zip(&stores)
            .map(|(id, store)| (id.clone(), Box::new(store.clone()) as Box<dyn RecorderRpc>))
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let proposal = |command: &StoredCommand| {
            Proposal::new(
                ProposalPriority::from_u64(1),
                "other",
                1,
                AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, command),
            )
        };
        let mut progress = ProposerProgress::new(1, proposal(&offered)).with_command(offered);

        progress.proposal = proposal(&transition);
        let context = RecorderRpcContext::default_timeout();
        let mutation_started = AtomicBool::new(false);
        consensus
            .ensure_progress_command(&mut progress, &context, &mutation_started)
            .unwrap();
        progress.proposal = proposal(&adopted);
        consensus
            .ensure_progress_command(&mut progress, &context, &mutation_started)
            .unwrap();

        assert_eq!(progress.command, Some(adopted));
        assert!(progress.transition_involved);
    }

    #[test]
    fn record_broadcast_reuses_one_worker_thread_per_recorder() {
        let seen: Vec<_> = (0..3)
            .map(|_| Arc::new(Mutex::new(HashSet::new())))
            .collect();
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .zip(&seen)
            .map(|(recorder_id, threads)| {
                (
                    recorder_id.into(),
                    Box::new(ThreadRecordingRecorder {
                        recorder_id,
                        threads: Arc::clone(threads),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        for slot in 1..=16 {
            assert_eq!(
                consensus
                    .record_broadcast(record_requests(&consensus, slot))
                    .unwrap()
                    .len(),
                2
            );
        }
        drop(consensus);

        assert!(seen
            .iter()
            .all(|threads| threads.lock().unwrap().len() == 1));
    }

    #[test]
    fn unsorted_explicit_recorder_ids_preserve_rpc_pairing_across_worker_paths() {
        let recorders = ["n3", "n1", "n2"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(SlotRecorder {
                        recorder_id,
                        reject_slot: None,
                        observed: None,
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert_eq!(
            consensus
                .record_broadcast(record_requests(&consensus, 1))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            consensus
                .inspect_decision_proof_at(&RecorderRpcContext::default_timeout(), 1)
                .unwrap(),
            None
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn repeated_control_operations_reuse_one_worker_thread_per_recorder() {
        let seen: Vec<_> = (0..3)
            .map(|_| Arc::new(Mutex::new(HashSet::new())))
            .collect();
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .zip(&seen)
            .map(|(recorder_id, threads)| {
                (
                    recorder_id.into(),
                    Box::new(ThreadRecordingControlRecorder {
                        threads: Arc::clone(threads),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        for slot in 1..=16 {
            assert_eq!(
                consensus
                    .inspect_decision_proof_at(&RecorderRpcContext::default_timeout(), slot)
                    .unwrap(),
                None
            );
        }
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));

        assert!(seen
            .iter()
            .all(|threads| threads.lock().unwrap().len() == 1));
    }

    #[test]
    fn blocked_control_minority_does_not_delay_record_quorum() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let mut release = ChannelRelease::new(release_tx);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n1",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(BlockingControlRecorder {
                    recorder_id: "n2",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n3",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        let (inspection_tx, inspection_rx) = mpsc::sync_channel(1);
        assert_eq!(
            consensus.control_workers[1].dispatch(ControlJob::InspectProof {
                index: 1,
                context: RecorderRpcContext::default_timeout(),
                slot: 1,
                result: inspection_tx,
            }),
            ControlDispatch::Accepted
        );
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(10)), Ok(1));
        let replies = consensus
            .record_broadcast(record_requests(&consensus, 1))
            .unwrap();
        assert_eq!(replies.len(), 2);

        release.release();
        assert_eq!(
            inspection_rx.recv_timeout(Duration::from_secs(5)),
            Ok((1, Ok(None)))
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn blocked_control_majority_does_not_head_of_line_block_read_fence() {
        let (started_tx, started_rx) = mpsc::sync_channel(2);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&release));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(BlockingInspectionReadFenceRecorder {
                        recorder_id,
                        block_inspection: recorder_id != "n3",
                        started: started_tx.clone(),
                        release: Arc::clone(&release),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let inspecting = Arc::clone(&consensus);
        let inspection = thread::spawn(move || {
            inspecting.inspect_decision_at(&RecorderRpcContext::default_timeout(), 1, LogHash::ZERO)
        });
        let mut started = BTreeSet::new();
        started.insert(started_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        started.insert(started_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        assert_eq!(started, BTreeSet::from(["n1", "n2"]));

        let before = Instant::now();
        assert_eq!(
            consensus
                .inspect_context_read_fence_at(
                    &RecorderRpcContext::default_timeout(),
                    1,
                    LogHash::ZERO
                )
                .unwrap(),
            CertifiedDecisionInspection::Empty
        );
        assert!(before.elapsed() < Duration::from_millis(250));

        let (released, condition) = &*release;
        *released.lock().unwrap() = true;
        condition.notify_all();
        assert_eq!(
            inspection.join().unwrap().unwrap(),
            DecisionInspection::Empty
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn saturated_control_queue_does_not_contaminate_later_requests() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (fast_started_tx, _fast_started_rx) = mpsc::sync_channel(8);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let fast_replies = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_fast_replies = GateRelease::new(Arc::clone(&fast_replies));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n1",
                    started: fast_started_tx.clone(),
                    gate: Some(Arc::clone(&fast_replies)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(BlockingControlRecorder {
                    recorder_id: "n2",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(ScriptedProofRecorder {
                    recorder_id: "n3",
                    started: fast_started_tx,
                    gate: Some(Arc::clone(&fast_replies)),
                    reply: Ok(None),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let first = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                consensus.inspect_decision_proof_at(&RecorderRpcContext::default_timeout(), 1)
            })
        };
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(5)), Ok(1));
        release_gate(&fast_replies);
        let fast_deadline = Instant::now() + Duration::from_secs(1);
        while ![0, 2]
            .into_iter()
            .all(|index| consensus.control_workers[index].is_idle())
        {
            assert!(
                Instant::now() < fast_deadline,
                "n1/n3 must finish slot 1 before their queue availability is tested at slot 3"
            );
            thread::yield_now();
        }
        let (queued_tx, queued_rx) = mpsc::sync_channel(1);
        assert_eq!(
            consensus.control_workers[1].dispatch(ControlJob::InspectProof {
                index: 1,
                context: RecorderRpcContext::default_timeout(),
                slot: 2,
                result: queued_tx,
            }),
            ControlDispatch::Accepted
        );
        assert_eq!(
            consensus.control_workers[1]
                .state
                .pending
                .load(Ordering::Acquire),
            2,
            "n2 has one running and one queued job, so slot 3 must saturate its dispatch"
        );
        assert_eq!(
            consensus
                .inspect_decision_proof_at(&RecorderRpcContext::default_timeout(), 3)
                .unwrap(),
            None
        );
        assert_eq!(
            consensus.control_workers[1]
                .state
                .pending
                .load(Ordering::Acquire),
            2,
            "the saturated slot-3 dispatch must not join n2's queue"
        );
        release_tx.send(()).unwrap();
        assert_eq!(first.join().unwrap(), Ok(None));
        assert_eq!(
            queued_rx.recv_timeout(Duration::from_secs(1)),
            Ok((1, Ok(None)))
        );
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(2));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        assert_eq!(
            consensus.inspect_decision_proof_at(&RecorderRpcContext::default_timeout(), 4),
            Ok(None)
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn command_registration_preserves_unknown_outcome_from_an_admitted_worker() {
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(UnknownCommandStoreRecorder) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"ambiguous".to_vec());
        assert_eq!(
            consensus.register_command(
                &RecorderRpcContext::default_timeout(),
                command.hash(),
                command.payload,
            ),
            Err(Error::UnknownOutcome)
        );
    }

    #[test]
    fn install_proof_quorum_drains_a_slow_admitted_hedge() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let fast = Arc::new((Mutex::new(false), Condvar::new()));
        let slow = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_fast = GateRelease::new(Arc::clone(&fast));
        let _release_slow = GateRelease::new(Arc::clone(&slow));
        let recorder = |recorder_id, gate| {
            Box::new(ScriptedInstallProofRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate,
                reply: Ok(()),
            }) as Box<dyn RecorderRpc>
        };
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "cluster",
                "n1",
                1,
                1,
                vec![
                    ("n1".into(), recorder("n1", Some(Arc::clone(&fast)))),
                    ("n2".into(), recorder("n2", Some(Arc::clone(&fast)))),
                    ("n3".into(), recorder("n3", Some(Arc::clone(&slow)))),
                ],
            )
            .unwrap(),
        );
        let proof = test_decision_proof(consensus.membership());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.install_decision_proof_quorum(
                        proof,
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        &AtomicBool::new(false),
                    ))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&fast);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&slow);
        assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)), Ok(Ok(())));
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn install_proof_exact_quorum_survives_missing_admitted_reply_after_worker_exit() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(2);
        let fast = Arc::new((Mutex::new(false), Condvar::new()));
        let pause = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_fast = GateRelease::new(Arc::clone(&fast));
        let _release_pause = GateRelease::new(Arc::clone(&pause));
        let recorder = |recorder_id| {
            Box::new(ScriptedInstallProofRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate: Some(Arc::clone(&fast)),
                reply: Ok(()),
            }) as Box<dyn RecorderRpc>
        };
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "cluster",
                "n1",
                1,
                1,
                vec![
                    ("n1".into(), recorder("n1")),
                    ("n2".into(), recorder("n2")),
                    (
                        "n3".into(),
                        Box::new(SuccessfulCommandStoreRecorder) as Box<dyn RecorderRpc>,
                    ),
                ],
            )
            .unwrap(),
        );
        let (paused_tx, paused_rx) = mpsc::sync_channel(1);
        consensus.control_workers[2].pause_after_next_pop(paused_tx, Arc::clone(&pause));
        consensus.control_workers[2].panic_after_next_pop();
        let proof = test_decision_proof(consensus.membership());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.install_decision_proof_quorum(
                        proof,
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        &AtomicBool::new(false),
                    ))
                    .unwrap()
            })
        };
        paused_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_gate(&fast);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&pause);
        assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)), Ok(Ok(())));
        caller.join().unwrap();
        assert!(consensus.control_workers[2].is_idle());
        let queue = &consensus.control_workers[2].state.queue;
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut state = queue
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !state.closed {
            let Some(wait) = deadline.checked_duration_since(Instant::now()) else {
                panic!("n3 must publish queue closure after its Install worker panic");
            };
            let (next, _) = queue
                .available
                .wait_timeout(state, wait)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
        }
        drop(state);
        let (failed_tx, _failed_rx) = mpsc::sync_channel(1);
        assert_eq!(
            consensus.control_workers[2].dispatch(ControlJob::InstallProof {
                index: 2,
                context: RecorderRpcContext::default_timeout(),
                proof: test_decision_proof(consensus.membership()),
                membership: consensus.membership().clone(),
                result: failed_tx,
            }),
            ControlDispatch::Failed
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        assert_eq!(
            consensus.install_decision_proof_quorum(
                test_decision_proof(consensus.membership()),
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                &AtomicBool::new(false),
            ),
            Ok(())
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn install_proof_exact_quorum_survives_a_quarantined_drain_miss() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let slow = Arc::new((Mutex::new(false), Condvar::new()));
        let fast = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_slow = GateRelease::new(Arc::clone(&slow));
        let _release_fast = GateRelease::new(Arc::clone(&fast));
        let recorder = |recorder_id, gate| {
            Box::new(ScriptedInstallProofRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate,
                reply: Ok(()),
            }) as Box<dyn RecorderRpc>
        };
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "cluster",
                "n1",
                1,
                1,
                vec![
                    ("n1".into(), recorder("n1", Some(Arc::clone(&slow)))),
                    ("n2".into(), recorder("n2", Some(Arc::clone(&fast)))),
                    ("n3".into(), recorder("n3", Some(Arc::clone(&fast)))),
                ],
            )
            .unwrap(),
        );
        let root = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&root),
        );
        let (token_tx, token_rx) = mpsc::sync_channel(1);
        let _token = capture_next_fetch_group_token(Arc::clone(&root), token_tx);
        let proof = test_decision_proof(consensus.membership());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.install_decision_proof_quorum(
                        proof,
                        &context,
                        &AtomicBool::new(false),
                    ))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        let token = token_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        let _timeout = force_next_control_group_drain_timeout(
            token,
            Arc::clone(&consensus.control_workers[0].state),
            ack_tx,
        );
        release_gate(&fast);
        ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)), Ok(Ok(())));
        assert!(consensus.control_workers[0]
            .state
            .quarantined
            .load(Ordering::Acquire));
        release_gate(&slow);
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        assert_eq!(
            consensus.install_decision_proof_quorum(
                test_decision_proof(consensus.membership()),
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                &AtomicBool::new(false),
            ),
            Ok(())
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn install_proof_group_cleanup_cancellation_counts_as_delivery() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let (entered_tx, _entered_rx) = mpsc::sync_channel(2);
        let successful = |recorder_id| {
            Box::new(ScriptedInstallProofRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate: None,
                reply: Ok(()),
            }) as Box<dyn RecorderRpc>
        };
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "n1",
            1,
            1,
            vec![
                ("n1".into(), successful("n1")),
                ("n2".into(), successful("n2")),
                (
                    "n3".into(),
                    Box::new(BlockingControlRecorder {
                        recorder_id: "n3",
                        started: started_tx,
                        release_first: Mutex::new(release_rx),
                    }) as Box<dyn RecorderRpc>,
                ),
            ],
        )
        .unwrap();
        let (inspection_tx, _inspection_rx) = mpsc::sync_channel(1);
        assert_eq!(
            consensus.control_workers[2].dispatch(ControlJob::InspectProof {
                index: 2,
                context: RecorderRpcContext::default_timeout(),
                slot: 1,
                result: inspection_tx,
            }),
            ControlDispatch::Accepted
        );
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        assert_eq!(
            consensus.install_decision_proof_quorum(
                test_decision_proof(consensus.membership()),
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                &AtomicBool::new(false),
            ),
            Ok(()),
            "the pruned admitted n3 RpcCancelled must be counted as delivery, not missing-reply Unknown"
        );
        release_tx.send(()).unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn install_proof_admission_shares_mutation_certainty_with_nested_fetch() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let worker_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let dispatch_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_worker = GateRelease::new(Arc::clone(&worker_gate));
        let _release_dispatch = GateRelease::new(Arc::clone(&dispatch_gate));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedInstallProofRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: (recorder_id == "n1").then(|| Arc::clone(&worker_gate)),
                        reply: Ok(()),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancellation),
        );
        let budget = ControlCallBudget::new(&context).unwrap();
        let mutation_started = Arc::new(AtomicBool::new(false));
        let (hook_tx, hook_rx) = mpsc::sync_channel(1);
        let _hook = pause_after_next_fetch_dispatch(
            Arc::clone(&cancellation),
            hook_tx,
            Arc::clone(&dispatch_gate),
        );
        let proof = test_decision_proof(consensus.membership());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            let mutation_started = Arc::clone(&mutation_started);
            thread::spawn(move || {
                result_tx
                    .send(consensus.install_decision_proof_quorum_with_budget(
                        &budget,
                        proof,
                        &mutation_started,
                    ))
                    .unwrap()
            })
        };
        hook_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(mutation_started.load(Ordering::Acquire));
        assert_eq!(entered_rx.recv_timeout(Duration::from_secs(1)), Ok("n1"));
        cancellation.store(true, Ordering::Release);
        let value = AcceptedValue::from_command(
            "cluster",
            7,
            1,
            1,
            LogHash::ZERO,
            &StoredCommand::new(EntryType::Command, b"install-nested-fetch-value".to_vec()),
        );
        assert_eq!(
            consensus.fetch_verified_value(7, &value, &context, &mutation_started),
            Err(Error::UnknownOutcome)
        );
        release_gate(&dispatch_gate);
        release_gate(&worker_gate);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)),
            Ok(Err(Error::UnknownOutcome))
        );
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn install_proof_partial_admission_cancellation_drains_before_unknown() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let worker_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let dispatch_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_worker = GateRelease::new(Arc::clone(&worker_gate));
        let _release_dispatch = GateRelease::new(Arc::clone(&dispatch_gate));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedInstallProofRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: (recorder_id == "n1").then(|| Arc::clone(&worker_gate)),
                        reply: Ok(()),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancellation),
        );
        let (hook_tx, hook_rx) = mpsc::sync_channel(1);
        let _hook = pause_after_next_fetch_dispatch(
            Arc::clone(&cancellation),
            hook_tx,
            Arc::clone(&dispatch_gate),
        );
        let proof = test_decision_proof(consensus.membership());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.install_decision_proof_quorum(
                        proof,
                        &context,
                        &AtomicBool::new(false),
                    ))
                    .unwrap()
            })
        };
        hook_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(entered_rx.recv_timeout(Duration::from_secs(1)), Ok("n1"));
        cancellation.store(true, Ordering::Release);
        release_gate(&dispatch_gate);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&worker_gate);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)),
            Ok(Err(Error::UnknownOutcome))
        );
        caller.join().unwrap();
        assert_eq!(entered_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn install_proof_unknown_reply_before_exact_quorum_does_not_freeze() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let fast = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_fast = GateRelease::new(Arc::clone(&fast));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let recorder = |recorder_id, gate, reply| {
            Box::new(ScriptedInstallProofRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate,
                reply,
            }) as Box<dyn RecorderRpc>
        };
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "cluster",
                "n1",
                1,
                1,
                vec![
                    ("n1".into(), recorder("n1", Some(Arc::clone(&fast)), Ok(()))),
                    ("n2".into(), recorder("n2", Some(Arc::clone(&fast)), Ok(()))),
                    (
                        "n3".into(),
                        recorder("n3", Some(Arc::clone(&late)), Err(Error::UnknownOutcome)),
                    ),
                ],
            )
            .unwrap(),
        );
        let proof = test_decision_proof(consensus.membership());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.install_decision_proof_quorum(
                        proof,
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        &AtomicBool::new(false),
                    ))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&late);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&fast);
        assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)), Ok(Ok(())));
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn install_proof_short_deadline_admits_no_backend_work() {
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedInstallProofRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: None,
                        reply: Ok(()),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        assert_eq!(
            consensus.install_decision_proof_quorum(
                test_decision_proof(consensus.membership()),
                &RecorderRpcContext::with_timeout(Duration::ZERO),
                &AtomicBool::new(false),
            ),
            Err(Error::RpcDeadlineExceeded)
        );
        assert_eq!(entered_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    }

    #[test]
    fn install_proof_saturated_and_admitted_failures_are_arrival_order_independent() {
        let _blocking = lock_blocking_control_tests();
        let run = |n2_reply: super::Result<()>, expected: Error| {
            for n2_first in [true, false] {
                let n1_gate = Arc::new((Mutex::new(false), Condvar::new()));
                let n2_gate = Arc::new((Mutex::new(false), Condvar::new()));
                let n3_gate = Arc::new((Mutex::new(false), Condvar::new()));
                let _release_n1 = GateRelease::new(Arc::clone(&n1_gate));
                let _release_n2 = GateRelease::new(Arc::clone(&n2_gate));
                let _release_n3 = GateRelease::new(Arc::clone(&n3_gate));
                let (n1_entered_tx, n1_entered_rx) = mpsc::sync_channel(2);
                let (entered_tx, entered_rx) = mpsc::sync_channel(2);
                let recorder = |recorder_id, gate, reply| {
                    Box::new(ScriptedInstallProofRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: Some(gate),
                        reply,
                    }) as Box<dyn RecorderRpc>
                };
                let consensus = Arc::new(
                    ThreeNodeConsensus::from_recorders_with_ids(
                        "cluster",
                        "n1",
                        1,
                        1,
                        vec![
                            (
                                "n1".into(),
                                Box::new(ScriptedInstallProofRecorder {
                                    recorder_id: "n1",
                                    entered: n1_entered_tx,
                                    gate: Some(Arc::clone(&n1_gate)),
                                    reply: Ok(()),
                                }) as Box<dyn RecorderRpc>,
                            ),
                            (
                                "n2".into(),
                                recorder("n2", Arc::clone(&n2_gate), n2_reply.clone()),
                            ),
                            (
                                "n3".into(),
                                recorder("n3", Arc::clone(&n3_gate), Err(Error::ProposeFailed)),
                            ),
                        ],
                    )
                    .unwrap(),
                );
                let occupied = test_decision_proof(consensus.membership());
                let (occupied_tx, _occupied_rx) = mpsc::sync_channel(2);
                assert_eq!(
                    consensus.control_workers[0].dispatch(ControlJob::InstallProof {
                        index: 0,
                        context: RecorderRpcContext::default_timeout(),
                        proof: occupied.clone(),
                        membership: consensus.membership().clone(),
                        result: occupied_tx.clone(),
                    }),
                    ControlDispatch::Accepted
                );
                assert_eq!(n1_entered_rx.recv_timeout(Duration::from_secs(1)), Ok("n1"));
                assert_eq!(
                    consensus.control_workers[0].dispatch(ControlJob::InstallProof {
                        index: 0,
                        context: RecorderRpcContext::default_timeout(),
                        proof: occupied,
                        membership: consensus.membership().clone(),
                        result: occupied_tx,
                    }),
                    ControlDispatch::Accepted,
                    "the second occupied n1 job must fill the sole queue slot"
                );

                let (result_tx, result_rx) = mpsc::sync_channel(1);
                let caller = {
                    let consensus = Arc::clone(&consensus);
                    thread::spawn(move || {
                        result_tx
                            .send(consensus.install_decision_proof_quorum(
                                test_decision_proof(consensus.membership()),
                                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                                &AtomicBool::new(false),
                            ))
                            .unwrap()
                    })
                };
                entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                assert!(
                    !consensus.control_workers[0].is_idle(),
                    "n1 must remain saturated while n2/n3 reply in either order"
                );
                if n2_first {
                    release_gate(&n2_gate);
                    wait_for_control_worker_idle(&consensus.control_workers[1], "n2");
                    assert!(!consensus.control_workers[2].is_idle());
                    release_gate(&n3_gate);
                } else {
                    release_gate(&n3_gate);
                    wait_for_control_worker_idle(&consensus.control_workers[2], "n3");
                    assert!(!consensus.control_workers[1].is_idle());
                    release_gate(&n2_gate);
                }
                assert_eq!(
                    result_rx.recv_timeout(Duration::from_secs(1)),
                    Ok(Err(expected.clone()))
                );
                caller.join().unwrap();
                release_gate(&n1_gate);
                assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
            }
        };
        run(Ok(()), Error::NoQuorum);
        run(
            Err(Error::Io("ordinary install failure".into())),
            Error::ProposeFailed,
        );
    }

    #[test]
    fn install_proof_typed_errors_follow_terminal_precedence() {
        let run = |replies: [super::Result<()>; 3], expected: super::Result<()>| {
            let (entered_tx, _entered_rx) = mpsc::sync_channel(3);
            let recorders = ["n1", "n2", "n3"]
                .into_iter()
                .zip(replies)
                .map(|(recorder_id, reply)| {
                    (
                        recorder_id.into(),
                        Box::new(ScriptedInstallProofRecorder {
                            recorder_id,
                            entered: entered_tx.clone(),
                            gate: None,
                            reply,
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect();
            let consensus =
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap();
            assert_eq!(
                consensus.install_decision_proof_quorum(
                    test_decision_proof(consensus.membership()),
                    &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    &AtomicBool::new(false),
                ),
                expected
            );
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        };

        run(
            [
                Err(Error::TypedProofInstallRequired),
                Err(Error::TypedProofInstallRequired),
                Err(Error::TypedProofInstallRequired),
            ],
            Err(Error::TypedProofInstallRequired),
        );
        run(
            [
                Err(Error::TypedProofInstallRequired),
                Ok(()),
                Err(Error::Io("transient install failure".into())),
            ],
            Err(Error::TypedProofInstallRequired),
        );
        run([Err(Error::TypedRecordRequired), Ok(()), Ok(())], Ok(()));
        run(
            [
                Err(Error::TypedProofInstallRequired),
                Err(Error::Rejected(RejectReason::AlreadyDecided)),
                Err(Error::ProposeFailed),
            ],
            Err(Error::Rejected(RejectReason::AlreadyDecided)),
        );
        run(
            [
                Err(Error::TypedProofInstallRequired),
                Err(Error::UnknownOutcome),
                Err(Error::ProposeFailed),
            ],
            Err(Error::TypedProofInstallRequired),
        );
    }

    #[test]
    fn install_proof_preclosed_worker_is_arrival_order_independent() {
        let _blocking = lock_blocking_control_tests();
        let run = |n2_reply: super::Result<()>| {
            for n2_first in [true, false] {
                let n2_gate = Arc::new((Mutex::new(false), Condvar::new()));
                let n3_gate = Arc::new((Mutex::new(false), Condvar::new()));
                let _release_n2 = GateRelease::new(Arc::clone(&n2_gate));
                let _release_n3 = GateRelease::new(Arc::clone(&n3_gate));
                let (n1_entered_tx, _n1_entered_rx) = mpsc::sync_channel(1);
                let (entered_tx, entered_rx) = mpsc::sync_channel(2);
                let recorder = |recorder_id, gate, reply| {
                    Box::new(ScriptedInstallProofRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: Some(gate),
                        reply,
                    }) as Box<dyn RecorderRpc>
                };
                let consensus = Arc::new(
                    ThreeNodeConsensus::from_recorders_with_ids(
                        "cluster",
                        "n1",
                        1,
                        1,
                        vec![
                            (
                                "n1".into(),
                                Box::new(ScriptedInstallProofRecorder {
                                    recorder_id: "n1",
                                    entered: n1_entered_tx,
                                    gate: None,
                                    reply: Ok(()),
                                }) as Box<dyn RecorderRpc>,
                            ),
                            (
                                "n2".into(),
                                recorder("n2", Arc::clone(&n2_gate), n2_reply.clone()),
                            ),
                            (
                                "n3".into(),
                                recorder("n3", Arc::clone(&n3_gate), Err(Error::ProposeFailed)),
                            ),
                        ],
                    )
                    .unwrap(),
                );
                consensus.control_workers[0].state.close_and_drain();

                let (result_tx, result_rx) = mpsc::sync_channel(1);
                let caller = {
                    let consensus = Arc::clone(&consensus);
                    thread::spawn(move || {
                        result_tx
                            .send(consensus.install_decision_proof_quorum(
                                test_decision_proof(consensus.membership()),
                                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                                &AtomicBool::new(false),
                            ))
                            .unwrap()
                    })
                };
                entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                if n2_first {
                    release_gate(&n2_gate);
                    wait_for_control_worker_idle(&consensus.control_workers[1], "n2");
                    assert!(!consensus.control_workers[2].is_idle());
                    release_gate(&n3_gate);
                } else {
                    release_gate(&n3_gate);
                    wait_for_control_worker_idle(&consensus.control_workers[2], "n3");
                    assert!(!consensus.control_workers[1].is_idle());
                    release_gate(&n2_gate);
                }
                assert_eq!(
                    result_rx.recv_timeout(Duration::from_secs(1)),
                    Ok(Err(Error::ProposeFailed))
                );
                caller.join().unwrap();
                assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
            }
        };
        run(Ok(()));
        run(Err(Error::Io("ordinary install failure".into())));
    }

    #[test]
    fn store_command_saturated_and_admitted_failures_are_arrival_order_independent() {
        let _blocking = lock_blocking_control_tests();
        let run = |n2_reply: super::Result<()>, expected: Error| {
            for n2_first in [true, false] {
                let (n1_started_tx, n1_started_rx) = mpsc::sync_channel(2);
                let n1_gate = Arc::new((Mutex::new(false), Condvar::new()));
                let n2_gate = Arc::new((Mutex::new(false), Condvar::new()));
                let n3_gate = Arc::new((Mutex::new(false), Condvar::new()));
                let _release_n1 = GateRelease::new(Arc::clone(&n1_gate));
                let _release_n2 = GateRelease::new(Arc::clone(&n2_gate));
                let _release_n3 = GateRelease::new(Arc::clone(&n3_gate));
                let (entered_tx, entered_rx) = mpsc::sync_channel(2);
                let recorder = |recorder_id, gate, reply| {
                    Box::new(ScriptedCommandStoreRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: Some(gate),
                        reply,
                        stored: Mutex::new(Vec::new()),
                    }) as Box<dyn RecorderRpc>
                };
                let consensus = Arc::new(
                    ThreeNodeConsensus::from_recorders_with_ids(
                        "cluster",
                        "n1",
                        1,
                        1,
                        vec![
                            (
                                "n1".into(),
                                Box::new(BlockingCommandStoreRecorder {
                                    started: n1_started_tx,
                                    release: Arc::clone(&n1_gate),
                                }) as Box<dyn RecorderRpc>,
                            ),
                            (
                                "n2".into(),
                                recorder("n2", Arc::clone(&n2_gate), n2_reply.clone()),
                            ),
                            (
                                "n3".into(),
                                recorder("n3", Arc::clone(&n3_gate), Err(Error::ProposeFailed)),
                            ),
                        ],
                    )
                    .unwrap(),
                );
                let occupied = StoredCommand::new(EntryType::Command, b"occupy-store".to_vec());
                let (occupied_tx, _occupied_rx) = mpsc::sync_channel(2);
                assert_eq!(
                    consensus.control_workers[0].dispatch(ControlJob::StoreCommand {
                        index: 0,
                        context: RecorderRpcContext::default_timeout(),
                        cluster_id: "cluster".into(),
                        epoch: 1,
                        config_id: 1,
                        config_digest: consensus.membership().digest(),
                        command_hash: occupied.hash(),
                        command: occupied.clone(),
                        result: occupied_tx.clone(),
                    }),
                    ControlDispatch::Accepted
                );
                n1_started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                assert_eq!(
                    consensus.control_workers[0].dispatch(ControlJob::StoreCommand {
                        index: 0,
                        context: RecorderRpcContext::default_timeout(),
                        cluster_id: "cluster".into(),
                        epoch: 1,
                        config_id: 1,
                        config_digest: consensus.membership().digest(),
                        command_hash: occupied.hash(),
                        command: occupied.clone(),
                        result: occupied_tx.clone(),
                    }),
                    ControlDispatch::Accepted
                );

                let command = StoredCommand::new(EntryType::Command, b"order-store".to_vec());
                let (result_tx, result_rx) = mpsc::sync_channel(1);
                let caller = {
                    let consensus = Arc::clone(&consensus);
                    thread::spawn(move || {
                        result_tx
                            .send(consensus.register_command(
                                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                                command.hash(),
                                command.payload,
                            ))
                            .unwrap()
                    })
                };
                entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                assert!(!consensus.control_workers[1].is_idle());
                assert!(!consensus.control_workers[2].is_idle());
                if n2_first {
                    release_gate(&n2_gate);
                    wait_for_control_worker_idle(&consensus.control_workers[1], "n2");
                    assert!(
                        !consensus.control_workers[2].is_idle(),
                        "n3 must remain blocked until its designated second release"
                    );
                    release_gate(&n3_gate);
                } else {
                    release_gate(&n3_gate);
                    wait_for_control_worker_idle(&consensus.control_workers[2], "n3");
                    assert!(
                        !consensus.control_workers[1].is_idle(),
                        "n2 must remain blocked until its designated second release"
                    );
                    release_gate(&n2_gate);
                }
                assert_eq!(
                    result_rx.recv_timeout(Duration::from_secs(1)),
                    Ok(Err(expected.clone()))
                );
                caller.join().unwrap();
                release_gate(&n1_gate);
                assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
            }
        };
        run(Ok(()), Error::NoQuorum);
        run(
            Err(Error::Io("ordinary store failure".into())),
            Error::ProposeFailed,
        );
    }

    #[test]
    fn store_command_preclosed_worker_is_arrival_order_independent() {
        let _blocking = lock_blocking_control_tests();
        for n2_first in [true, false] {
            let n2_gate = Arc::new((Mutex::new(false), Condvar::new()));
            let n3_gate = Arc::new((Mutex::new(false), Condvar::new()));
            let _release_n2 = GateRelease::new(Arc::clone(&n2_gate));
            let _release_n3 = GateRelease::new(Arc::clone(&n3_gate));
            let (entered_tx, entered_rx) = mpsc::sync_channel(2);
            let recorder = |recorder_id, gate, reply| {
                Box::new(ScriptedCommandStoreRecorder {
                    recorder_id,
                    entered: entered_tx.clone(),
                    gate: Some(gate),
                    reply,
                    stored: Mutex::new(Vec::new()),
                }) as Box<dyn RecorderRpc>
            };
            let consensus = Arc::new(
                ThreeNodeConsensus::from_recorders_with_ids(
                    "cluster",
                    "n1",
                    1,
                    1,
                    vec![
                        (
                            "n1".into(),
                            Box::new(SuccessfulCommandStoreRecorder) as Box<dyn RecorderRpc>,
                        ),
                        ("n2".into(), recorder("n2", Arc::clone(&n2_gate), Ok(()))),
                        (
                            "n3".into(),
                            recorder("n3", Arc::clone(&n3_gate), Err(Error::ProposeFailed)),
                        ),
                    ],
                )
                .unwrap(),
            );
            consensus.control_workers[0].state.close_and_drain();

            let command = StoredCommand::new(EntryType::Command, b"closed-order-store".to_vec());
            let (result_tx, result_rx) = mpsc::sync_channel(1);
            let caller = {
                let consensus = Arc::clone(&consensus);
                thread::spawn(move || {
                    result_tx
                        .send(consensus.register_command(
                            &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                            command.hash(),
                            command.payload,
                        ))
                        .unwrap()
                })
            };
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(!consensus.control_workers[1].is_idle());
            assert!(!consensus.control_workers[2].is_idle());
            if n2_first {
                release_gate(&n2_gate);
                wait_for_control_worker_idle(&consensus.control_workers[1], "n2");
                assert!(
                    !consensus.control_workers[2].is_idle(),
                    "n3 must remain blocked until its designated second release"
                );
                release_gate(&n3_gate);
            } else {
                release_gate(&n3_gate);
                wait_for_control_worker_idle(&consensus.control_workers[2], "n3");
                assert!(
                    !consensus.control_workers[1].is_idle(),
                    "n2 must remain blocked until its designated second release"
                );
                release_gate(&n2_gate);
            }
            assert_eq!(
                result_rx.recv_timeout(Duration::from_secs(1)),
                Ok(Err(Error::ProposeFailed))
            );
            caller.join().unwrap();
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        }
    }

    #[test]
    fn store_command_quorum_drains_a_slow_admitted_hedge() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let fast = Arc::new((Mutex::new(false), Condvar::new()));
        let slow = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_fast = GateRelease::new(Arc::clone(&fast));
        let _release_slow = GateRelease::new(Arc::clone(&slow));
        let recorder = |recorder_id, gate| {
            Box::new(ScriptedCommandStoreRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate,
                reply: Ok(()),
                stored: Mutex::new(Vec::new()),
            }) as Box<dyn RecorderRpc>
        };
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "cluster",
                "n1",
                1,
                1,
                vec![
                    ("n1".into(), recorder("n1", Some(Arc::clone(&fast)))),
                    ("n2".into(), recorder("n2", Some(Arc::clone(&fast)))),
                    ("n3".into(), recorder("n3", Some(Arc::clone(&slow)))),
                ],
            )
            .unwrap(),
        );
        let command = StoredCommand::new(EntryType::Command, b"store-drain".to_vec());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.register_command(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        command.hash(),
                        command.payload,
                    ))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&fast);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&slow);
        assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)), Ok(Ok(())));
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn store_command_exact_quorum_survives_missing_admitted_reply_after_worker_exit() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(2);
        let fast = Arc::new((Mutex::new(false), Condvar::new()));
        let pause = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_fast = GateRelease::new(Arc::clone(&fast));
        let _release_pause = GateRelease::new(Arc::clone(&pause));
        let fast_recorder = |recorder_id| {
            Box::new(ScriptedCommandStoreRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate: Some(Arc::clone(&fast)),
                reply: Ok(()),
                stored: Mutex::new(Vec::new()),
            }) as Box<dyn RecorderRpc>
        };
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "cluster",
                "n1",
                1,
                1,
                vec![
                    ("n1".into(), fast_recorder("n1")),
                    ("n2".into(), fast_recorder("n2")),
                    (
                        "n3".into(),
                        Box::new(SuccessfulCommandStoreRecorder) as Box<dyn RecorderRpc>,
                    ),
                ],
            )
            .unwrap(),
        );
        let (paused_tx, paused_rx) = mpsc::sync_channel(1);
        consensus.control_workers[2].pause_after_next_pop(paused_tx, Arc::clone(&pause));
        consensus.control_workers[2].panic_after_next_pop();

        let command = StoredCommand::new(EntryType::Command, b"store-missing-reply".to_vec());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.register_command(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        command.hash(),
                        command.payload,
                    ))
                    .unwrap()
            })
        };

        paused_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_gate(&fast);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&pause);
        assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)), Ok(Ok(())));
        caller.join().unwrap();
        assert!(consensus.control_workers[2].is_idle());
        let queue = &consensus.control_workers[2].state.queue;
        let closed_deadline = Instant::now() + Duration::from_secs(1);
        let mut queue_state = queue
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !queue_state.closed {
            let Some(wait) = closed_deadline.checked_duration_since(Instant::now()) else {
                panic!("n3 worker exit must publish its closed queue before dispatch is checked");
            };
            let (next, _) = queue
                .available
                .wait_timeout(queue_state, wait)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            queue_state = next;
        }
        drop(queue_state);
        let (failed_tx, _failed_rx) = mpsc::sync_channel(1);
        let rejected = StoredCommand::new(EntryType::Command, b"closed-worker".to_vec());
        assert_eq!(
            consensus.control_workers[2].dispatch(ControlJob::StoreCommand {
                index: 2,
                context: RecorderRpcContext::default_timeout(),
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: consensus.membership().digest(),
                command_hash: rejected.hash(),
                command: rejected,
                result: failed_tx,
            }),
            ControlDispatch::Failed
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));

        let retry = StoredCommand::new(EntryType::Command, b"store-survivor-retry".to_vec());
        assert_eq!(
            consensus.register_command(
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                retry.hash(),
                retry.payload,
            ),
            Ok(())
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn store_command_unknown_reply_before_exact_quorum_does_not_freeze() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let fast = Arc::new((Mutex::new(false), Condvar::new()));
        let late = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_fast = GateRelease::new(Arc::clone(&fast));
        let _release_late = GateRelease::new(Arc::clone(&late));
        let recorder = |recorder_id, gate, reply| {
            Box::new(ScriptedCommandStoreRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate,
                reply,
                stored: Mutex::new(Vec::new()),
            }) as Box<dyn RecorderRpc>
        };
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "cluster",
                "n1",
                1,
                1,
                vec![
                    ("n1".into(), recorder("n1", Some(Arc::clone(&fast)), Ok(()))),
                    ("n2".into(), recorder("n2", Some(Arc::clone(&fast)), Ok(()))),
                    (
                        "n3".into(),
                        recorder("n3", Some(Arc::clone(&late)), Err(Error::UnknownOutcome)),
                    ),
                ],
            )
            .unwrap(),
        );
        let command = StoredCommand::new(EntryType::Command, b"store-late-unknown".to_vec());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.register_command(
                        &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                        command.hash(),
                        command.payload,
                    ))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        release_gate(&late);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&fast);
        assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)), Ok(Ok(())));
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn store_command_group_cleanup_cancellation_does_not_overwrite_quorum_success() {
        let _blocking = lock_blocking_control_tests();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(SuccessfulCommandStoreRecorder) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(SuccessfulCommandStoreRecorder) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(BlockingControlRecorder {
                    recorder_id: "n3",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let (inspection_tx, _inspection_rx) = mpsc::sync_channel(1);
        assert_eq!(
            consensus.control_workers[2].dispatch(ControlJob::InspectProof {
                index: 2,
                context: RecorderRpcContext::default_timeout(),
                slot: 1,
                result: inspection_tx,
            }),
            ControlDispatch::Accepted
        );
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        let command = StoredCommand::new(EntryType::Command, b"store-cleanup-cancel".to_vec());
        assert_eq!(
            consensus.register_command(
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                command.hash(),
                command.payload,
            ),
            Ok(()),
            "the pruned admitted n3 cleanup cancellation must count as a reply, not leave a missing-reply Unknown"
        );
        release_tx.send(()).unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn store_command_partial_admission_cancellation_drains_before_unknown() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let worker_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let dispatch_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_worker = GateRelease::new(Arc::clone(&worker_gate));
        let _release_dispatch = GateRelease::new(Arc::clone(&dispatch_gate));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedCommandStoreRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: (recorder_id == "n1").then(|| Arc::clone(&worker_gate)),
                        reply: Ok(()),
                        stored: Mutex::new(Vec::new()),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancellation),
        );
        let (hook_tx, hook_rx) = mpsc::sync_channel(1);
        let _hook = pause_after_next_fetch_dispatch(
            Arc::clone(&cancellation),
            hook_tx,
            Arc::clone(&dispatch_gate),
        );
        let command = StoredCommand::new(EntryType::Command, b"store-cancel".to_vec());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.register_command(&context, command.hash(), command.payload))
                    .unwrap()
            })
        };
        hook_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(entered_rx.recv_timeout(Duration::from_secs(1)), Ok("n1"));
        cancellation.store(true, Ordering::Release);
        release_gate(&dispatch_gate);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&worker_gate);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)),
            Ok(Err(Error::UnknownOutcome))
        );
        caller.join().unwrap();
        assert_eq!(entered_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn store_command_admission_shares_mutation_certainty_with_nested_fetch() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let worker_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let dispatch_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_worker = GateRelease::new(Arc::clone(&worker_gate));
        let _release_dispatch = GateRelease::new(Arc::clone(&dispatch_gate));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedCommandStoreRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: (recorder_id == "n1").then(|| Arc::clone(&worker_gate)),
                        reply: Ok(()),
                        stored: Mutex::new(Vec::new()),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancellation),
        );
        let budget = ControlCallBudget::new(&context).unwrap();
        let mutation_started = Arc::new(AtomicBool::new(false));
        let (hook_tx, hook_rx) = mpsc::sync_channel(1);
        let _hook = pause_after_next_fetch_dispatch(
            Arc::clone(&cancellation),
            hook_tx,
            Arc::clone(&dispatch_gate),
        );
        let command = StoredCommand::new(EntryType::Command, b"store-nested-fetch".to_vec());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            let mutation_started = Arc::clone(&mutation_started);
            thread::spawn(move || {
                result_tx
                    .send(consensus.store_command_on_quorum_with_budget(
                        &budget,
                        &mutation_started,
                        command.hash(),
                        &command,
                    ))
                    .unwrap()
            })
        };
        hook_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(mutation_started.load(Ordering::Acquire));
        assert_eq!(entered_rx.recv_timeout(Duration::from_secs(1)), Ok("n1"));
        cancellation.store(true, Ordering::Release);
        let value = AcceptedValue::from_command(
            "cluster",
            7,
            1,
            1,
            LogHash::ZERO,
            &StoredCommand::new(EntryType::Command, b"nested-fetch-value".to_vec()),
        );
        assert_eq!(
            consensus.fetch_verified_value(7, &value, &context, &mutation_started),
            Err(Error::UnknownOutcome)
        );
        release_gate(&dispatch_gate);
        release_gate(&worker_gate);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)),
            Ok(Err(Error::UnknownOutcome))
        );
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn store_command_short_deadline_admits_no_backend_work() {
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedCommandStoreRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: None,
                        reply: Ok(()),
                        stored: Mutex::new(Vec::new()),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"store-short".to_vec());
        assert_eq!(
            consensus.register_command(
                &RecorderRpcContext::with_timeout(Duration::ZERO),
                command.hash(),
                command.payload,
            ),
            Err(Error::RpcDeadlineExceeded)
        );
        assert_eq!(entered_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    }

    #[test]
    fn store_command_exact_quorum_survives_drain_miss_and_quarantines_worker() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let slow = Arc::new((Mutex::new(false), Condvar::new()));
        let fast = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_slow = GateRelease::new(Arc::clone(&slow));
        let _release_fast = GateRelease::new(Arc::clone(&fast));
        let recorder = |recorder_id, gate| {
            Box::new(ScriptedCommandStoreRecorder {
                recorder_id,
                entered: entered_tx.clone(),
                gate,
                reply: Ok(()),
                stored: Mutex::new(Vec::new()),
            }) as Box<dyn RecorderRpc>
        };
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "cluster",
                "n1",
                1,
                1,
                vec![
                    ("n1".into(), recorder("n1", Some(Arc::clone(&slow)))),
                    ("n2".into(), recorder("n2", Some(Arc::clone(&fast)))),
                    ("n3".into(), recorder("n3", Some(Arc::clone(&fast)))),
                ],
            )
            .unwrap(),
        );
        let root = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&root),
        );
        let (token_tx, token_rx) = mpsc::sync_channel(1);
        let _token = capture_next_fetch_group_token(Arc::clone(&root), token_tx);
        let command = StoredCommand::new(EntryType::Command, b"store-drain-miss".to_vec());
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.register_command(&context, command.hash(), command.payload))
                    .unwrap()
            })
        };
        for _ in 0..3 {
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        let group_token = token_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        let _timeout = force_next_control_group_drain_timeout(
            group_token,
            Arc::clone(&consensus.control_workers[0].state),
            ack_tx,
        );
        release_gate(&fast);
        ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)), Ok(Ok(())));
        assert!(consensus.control_workers[0]
            .state
            .quarantined
            .load(Ordering::Acquire));
        release_gate(&slow);
        caller.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        let retry = StoredCommand::new(EntryType::Command, b"store-after-drain-miss".to_vec());
        assert_eq!(
            consensus.register_command(
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                retry.hash(),
                retry.payload,
            ),
            Ok(())
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn command_registration_returns_no_quorum_when_all_control_queues_are_full() {
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&release));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(BlockingCommandStoreRecorder {
                        started: started_tx.clone(),
                        release: Arc::clone(&release),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let first = StoredCommand::new(EntryType::Command, b"first".to_vec());
        let registering = Arc::clone(&consensus);
        let first_hash = first.hash();
        let first_payload = first.payload.clone();
        let registration = thread::spawn(move || {
            registering.register_command(
                &RecorderRpcContext::default_timeout(),
                first_hash,
                first_payload,
            )
        });

        for _ in 0..3 {
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }

        let queued = StoredCommand::new(EntryType::Command, b"queued".to_vec());
        let (queued_tx, _queued_rx) = mpsc::sync_channel(3);
        for (index, worker) in consensus.control_workers.iter().enumerate() {
            worker.dispatch(ControlJob::StoreCommand {
                index,
                context: RecorderRpcContext::default_timeout(),
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: consensus.membership().digest(),
                command_hash: queued.hash(),
                command: queued.clone(),
                result: queued_tx.clone(),
            });
        }

        let saturated = StoredCommand::new(EntryType::Command, b"saturated".to_vec());
        assert_eq!(
            consensus.register_command(
                &RecorderRpcContext::default_timeout(),
                saturated.hash(),
                saturated.payload,
            ),
            Err(Error::NoQuorum)
        );

        let (released, condition) = &*release;
        *released.lock().unwrap() = true;
        condition.notify_all();
        assert_eq!(registration.join().unwrap(), Ok(()));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn saturated_control_worker_keeps_command_quorum_retryable_after_worker_failure() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&release));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(BlockingCommandStoreRecorder {
                    started: started_tx,
                    release: Arc::clone(&release),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(SuccessfulCommandStoreRecorder) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(FailingCommandStoreRecorder) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let first = StoredCommand::new(EntryType::Command, b"first".to_vec());
        let registering = Arc::clone(&consensus);
        let registration = thread::spawn(move || {
            registering.register_command(
                &RecorderRpcContext::default_timeout(),
                first.hash(),
                first.payload,
            )
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let queued = StoredCommand::new(EntryType::Command, b"queued".to_vec());
        let (queued_tx, _queued_rx) = mpsc::sync_channel(1);
        consensus.control_workers[0].dispatch(ControlJob::StoreCommand {
            index: 0,
            context: RecorderRpcContext::default_timeout(),
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: consensus.membership().digest(),
            command_hash: queued.hash(),
            command: queued,
            result: queued_tx,
        });

        let retry = StoredCommand::new(EntryType::Command, b"retry".to_vec());
        assert_eq!(
            consensus.register_command(
                &RecorderRpcContext::default_timeout(),
                retry.hash(),
                retry.payload,
            ),
            Err(Error::NoQuorum)
        );

        let (released, condition) = &*release;
        *released.lock().unwrap() = true;
        condition.notify_all();
        assert_eq!(registration.join().unwrap(), Ok(()));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn control_worker_finish_and_drop_are_bounded() {
        let _blocking = lock_blocking_control_tests();
        let new_consensus = || {
            let (started_tx, started_rx) = mpsc::sync_channel(1);
            let (release_tx, release_rx) = mpsc::sync_channel(0);
            let recorders = vec![
                (
                    "n1".into(),
                    Box::new(SlotRecorder {
                        recorder_id: "n1",
                        reject_slot: None,
                        observed: None,
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n2".into(),
                    Box::new(BlockingControlRecorder {
                        recorder_id: "n2",
                        started: started_tx,
                        release_first: Mutex::new(release_rx),
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n3".into(),
                    Box::new(SlotRecorder {
                        recorder_id: "n3",
                        reject_slot: None,
                        observed: None,
                    }) as Box<dyn RecorderRpc>,
                ),
            ];
            (
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap(),
                started_rx,
                release_tx,
            )
        };

        let (consensus, started_rx, release_tx) = new_consensus();
        let mut release = ChannelRelease::new(release_tx);
        let consensus = Arc::new(consensus);
        let (inspection_tx, inspection_rx) = mpsc::sync_channel(1);
        assert_eq!(
            consensus.control_workers[1].dispatch(ControlJob::InspectProof {
                index: 1,
                context: RecorderRpcContext::default_timeout(),
                slot: 1,
                result: inspection_tx,
            }),
            ControlDispatch::Accepted
        );
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(10)), Ok(1));
        let (finished_tx, finished_rx) = mpsc::sync_channel(1);
        let finishing = Arc::clone(&consensus);
        let finisher = thread::spawn(move || {
            finished_tx
                .send(finishing.finish_pending_rpcs(Duration::from_millis(10)))
                .unwrap();
        });
        assert_eq!(finished_rx.recv_timeout(Duration::from_secs(5)), Ok(false));
        finisher.join().unwrap();
        release.release();
        assert_eq!(
            inspection_rx.recv_timeout(Duration::from_secs(5)),
            Ok((1, Ok(None)))
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));

        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let mut release = ChannelRelease::new(release_tx);
        let mut worker = ControlWorker::spawn(Arc::new(BlockingControlRecorder {
            recorder_id: "n2",
            started: started_tx,
            release_first: Mutex::new(release_rx),
        }))
        .unwrap();
        let state = Arc::clone(&worker.state);
        let (seam_entered_tx, seam_entered_rx) = mpsc::sync_channel(1);
        let (seam_release_tx, seam_release_rx) = mpsc::sync_channel(0);
        let (result_tx, _result_rx) = mpsc::sync_channel(1);
        let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
        let shutdown = thread::spawn(move || {
            let idle_transition = worker.shutdown_after_stale_idle_observation(|| {
                seam_entered_tx.send(()).unwrap();
                seam_release_rx.recv().unwrap();
            });
            shutdown_done_tx.send(idle_transition).unwrap();
        });
        assert_eq!(seam_entered_rx.recv_timeout(Duration::from_secs(1)), Ok(()));

        // This uses the exact admission function that normal worker dispatch
        // uses. It is now definitely between the shutdown seam and close.
        assert_eq!(
            ControlWorker::dispatch_inner(
                &state,
                ControlJob::InspectProof {
                    index: 1,
                    context: RecorderRpcContext::default_timeout(),
                    slot: 1,
                    result: result_tx,
                },
                None,
                None,
                None,
                true,
            ),
            ControlDispatch::Accepted
        );
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(10)), Ok(1));
        seam_release_tx.send(()).unwrap();
        // A stale pre-close idle snapshot would make shutdown join the blocked
        // RPC here. The post-close snapshot instead chooses bounded detach.
        assert_eq!(
            shutdown_done_rx.recv_timeout(Duration::from_secs(1)),
            Ok((true, false))
        );
        release.release();
        shutdown.join().unwrap();
    }

    #[test]
    fn idle_file_recorders_release_root_locks_before_consensus_drop_returns() {
        let root = tempfile::tempdir().unwrap();
        let roots = [
            root.path().join("recorders/n1"),
            root.path().join("recorders/n2"),
            root.path().join("recorders/n3"),
        ];
        let consensus = ThreeNodeConsensus::from_recovered_tip(
            "cluster",
            "n1",
            1,
            1,
            roots.clone(),
            1,
            LogHash::ZERO,
        )
        .unwrap();

        // Drop is the lifecycle boundary: reopening the exact same recorder
        // roots immediately proves that every idle record/control worker gave
        // up its RecorderFileStore ownership before it returned.
        drop(consensus);
        let reopened =
            ThreeNodeConsensus::from_recovered_tip("cluster", "n1", 1, 1, roots, 1, LogHash::ZERO);
        assert!(reopened.is_ok());
    }

    #[test]
    fn cooperative_record_hedge_is_reclaimed_without_contaminating_later_broadcasts() {
        let (started_tx, started_rx) = mpsc::sync_channel(2);
        let (_release_tx, release_rx) = mpsc::sync_channel(0);
        let (fast_observed_tx, _fast_observed_rx) = mpsc::sync_channel(6);
        let fast_release = Arc::new((Mutex::new(false), Condvar::new()));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(GatedObservedSlotRecorder {
                    recorder_id: "n1",
                    observed: fast_observed_tx.clone(),
                    release: Arc::clone(&fast_release),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n2",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(GatedObservedSlotRecorder {
                    recorder_id: "n3",
                    observed: fast_observed_tx,
                    release: Arc::clone(&fast_release),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );

        let first_consensus = Arc::clone(&consensus);
        let first = thread::spawn(move || {
            first_consensus
                .record_broadcast(record_requests(&first_consensus, 1))
                .unwrap()
        });
        // Hold both quorum replies until the minority hedge has definitely
        // entered its cooperative RPC. Scheduler order can no longer turn
        // this into a queued-job cancellation test by accident.
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        {
            let (released, condition) = &*fast_release;
            *released.lock().unwrap() = true;
            condition.notify_all();
        }
        let first = first.join().unwrap();
        assert_eq!(first.len(), 2);

        let second = consensus
            .record_broadcast(record_requests(&consensus, 2))
            .unwrap();
        assert_eq!(second.len(), 2);
        assert!(second.iter().all(|reply| reply.slot == 2));

        let third_replies = consensus
            .record_broadcast(record_requests(&consensus, 3))
            .unwrap();
        assert!(third_replies.len() >= 2);
        assert!(third_replies.iter().all(|reply| reply.slot == 3));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn fast_path_partial_proof_install_is_unknown_and_fences_conflict() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let n1 = RecorderFileStore::new_with_membership(
            root.path().join("n1"),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let n3 = RecorderFileStore::new_with_membership(
            root.path().join("n3"),
            "n3",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let fail_install = Arc::new(AtomicBool::new(true));
        let (n2_started_tx, n2_started_rx) = mpsc::sync_channel(1);
        let (n2_release_tx, n2_release_rx) = mpsc::sync_channel(1);
        let mut n2_release = ChannelRelease::new(n2_release_tx);
        let first = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "cluster",
                "n1",
                1,
                1,
                vec![
                    (
                        "n1".into(),
                        Box::new(FailInstallFileStore {
                            inner: n1.clone(),
                            fail_install: Arc::clone(&fail_install),
                        }) as Box<dyn RecorderRpc>,
                    ),
                    (
                        "n2".into(),
                        Box::new(BlockingRecorder {
                            recorder_id: "n2",
                            started: n2_started_tx,
                            release_first: Mutex::new(n2_release_rx),
                        }) as Box<dyn RecorderRpc>,
                    ),
                    (
                        "n3".into(),
                        Box::new(FailInstallFileStore {
                            inner: n3.clone(),
                            fail_install: Arc::clone(&fail_install),
                        }) as Box<dyn RecorderRpc>,
                    ),
                ],
            )
            .unwrap(),
        );

        // Hold n2's record worker on an unrelated request. The foreign
        // FastPath is therefore exactly n1+n3, while every proof installer
        // returns a bounded failure without a durable proof write.
        let (background_tx, background_rx) = mpsc::sync_channel(1);
        let background = record_requests(&first, 1).remove(1);
        assert!(matches!(
            first.record_workers[1].dispatch(super::RecordJob {
                index: 0,
                context: RecorderRpcContext::default_timeout(),
                request: background,
                result: background_tx,
            }),
            super::RecordDispatch::Accepted
        ));
        assert_eq!(n2_started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        assert_eq!(
            first.propose_at(
                RecorderRpcContext::default_timeout(),
                2,
                LogHash::ZERO,
                Command::new(CommandKind::ReadBarrier, Vec::new()),
            ),
            Err(Error::UnknownOutcome),
            "a decided FastPath with zero durable proof installs is not retryable"
        );
        assert_eq!(n1.inspect_decision_proof(2).unwrap(), None);
        assert_eq!(n3.inspect_decision_proof(2).unwrap(), None);
        assert_eq!(
            first
                .inspect_decision_proof_at(&RecorderRpcContext::default_timeout(), 2)
                .unwrap(),
            None,
            "an uninstalled FastPath proof is not a published proof"
        );
        fail_install.store(false, Ordering::Release);
        n2_release.release();
        assert!(background_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .1
            .is_ok());

        let n2 = RecorderFileStore::new_with_membership(
            root.path().join("n2"),
            "n2",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let second = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "n2",
            1,
            1,
            vec![
                ("n1".into(), Box::new(n1.clone()) as Box<dyn RecorderRpc>),
                ("n2".into(), Box::new(n2) as Box<dyn RecorderRpc>),
                ("n3".into(), Box::new(n3.clone()) as Box<dyn RecorderRpc>),
            ],
        )
        .unwrap();
        let recovered = second
            .inspect_certified_decision_at(&RecorderRpcContext::default_timeout(), 2, LogHash::ZERO)
            .unwrap();
        let CertifiedDecisionInspection::Committed(recovered) = recovered else {
            panic!("the n1+n3 FastPath summaries must reconstruct the foreign decision");
        };
        let foreign = recovered.entry.clone();
        assert!(matches!(recovered.proof, DecisionProof::FastPath { .. }));

        let conflicting = second
            .propose_at(
                RecorderRpcContext::default_timeout(),
                2,
                LogHash::ZERO,
                Command::new(CommandKind::Deterministic, b"conflicting".to_vec()),
            )
            .unwrap();
        assert_eq!(conflicting, foreign);
        let published = second
            .inspect_decision_proof_at(&RecorderRpcContext::default_timeout(), 2)
            .unwrap()
            .expect("the recovered foreign decision must be published");
        assert_eq!(
            published.proposal().value,
            recovered.proof.proposal().value,
            "a later Phase2 certificate may replace the FastPath shape, but never its decision"
        );
        assert!(first.finish_pending_rpcs(Duration::from_secs(1)));
        assert!(second.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn post_decision_unknown_recovers_only_from_a_certified_matching_entry() {
        let _blocking = lock_blocking_control_tests();
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let stores = ["n1", "n2", "n3"].map(|recorder_id| {
            RecorderFileStore::new_with_membership(
                root.path().join(recorder_id),
                recorder_id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        });
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "n1",
            1,
            1,
            ["n1", "n2", "n3"]
                .into_iter()
                .zip(stores.iter())
                .map(|(recorder_id, store)| {
                    (
                        recorder_id.into(),
                        Box::new(PersistThenUnknownInstallFileStore {
                            inner: store.clone(),
                            persist: true,
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect(),
        )
        .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"durable-unknown".to_vec());
        let entry = consensus
            .propose_stored_at(
                RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                1,
                LogHash::ZERO,
                command.clone(),
            )
            .expect("a quorum-certified durable matching proof resolves post-install ambiguity");
        assert_eq!(entry.entry_type, command.entry_type);
        assert_eq!(entry.payload, command.payload);
        let CertifiedDecisionInspection::Committed(decision) = consensus
            .inspect_certified_decision_at(
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                1,
                LogHash::ZERO,
            )
            .unwrap()
        else {
            panic!("the reconciliation acknowledgement requires a certified decision");
        };
        assert_eq!(decision.entry, entry);
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        assert!(
            consensus
                .control_workers
                .iter()
                .all(|worker| !worker.state.quarantined.load(Ordering::Acquire)),
            "an immediately drained lost response must not quarantine a reusable control worker"
        );
    }

    #[test]
    fn post_decision_unknown_never_acknowledges_a_certified_different_value() {
        let _blocking = lock_blocking_control_tests();
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let stores = ["n1", "n2", "n3"].map(|recorder_id| {
            RecorderFileStore::new_with_membership(
                root.path().join(recorder_id),
                recorder_id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        });
        let foreign = StoredCommand::new(EntryType::Command, b"foreign".to_vec());
        let seed = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "n1",
            1,
            1,
            ["n1", "n2", "n3"]
                .into_iter()
                .zip(stores.iter())
                .map(|(recorder_id, store)| {
                    (
                        recorder_id.into(),
                        Box::new(store.clone()) as Box<dyn RecorderRpc>,
                    )
                })
                .collect(),
        )
        .unwrap();
        seed.propose_stored_at(
            RecorderRpcContext::with_timeout(Duration::from_secs(1)),
            1,
            LogHash::ZERO,
            foreign,
        )
        .unwrap();
        drop(seed);

        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "n1",
            1,
            1,
            ["n1", "n2", "n3"]
                .into_iter()
                .zip(stores.iter())
                .map(|(recorder_id, store)| {
                    (
                        recorder_id.into(),
                        Box::new(PersistThenUnknownInstallFileStore {
                            inner: store.clone(),
                            persist: false,
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect(),
        )
        .unwrap();
        let offered = StoredCommand::new(EntryType::Command, b"offered".to_vec());
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &offered),
        );
        let proof = DecisionProof::FastPath {
            cluster_id: "cluster".into(),
            slot: 1,
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            proposal: proposal.clone(),
            summaries: membership.members()[..membership.quorum_size()]
                .iter()
                .map(|recorder_id| RecorderSummary {
                    recorder_id: recorder_id.clone(),
                    slot: 1,
                    step: 4,
                    first_current: Some(proposal.clone()),
                    aggregate_prior: None,
                })
                .collect(),
        };
        assert_eq!(
            consensus.finish_decision_with_context(
                proof,
                Some(&offered),
                false,
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                &AtomicBool::new(false),
            ),
            Err(Error::ConflictingCertificates),
            "a different certified value is safety evidence, never an acknowledgement of the offer"
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn post_decision_reconciliation_does_not_revive_a_cancelled_budget() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "n1",
            1,
            1,
            membership
                .members()
                .iter()
                .map(|recorder_id| {
                    (
                        recorder_id.clone(),
                        Box::new(SlotRecorder {
                            recorder_id: match recorder_id.as_str() {
                                "n1" => "n1",
                                "n2" => "n2",
                                "n3" => "n3",
                                _ => unreachable!(),
                            },
                            reject_slot: None,
                            observed: None,
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect(),
        )
        .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"cancelled".to_vec());
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
            config_digest: membership.digest(),
            proposal: proposal.clone(),
            summaries: membership.members()[..membership.quorum_size()]
                .iter()
                .map(|recorder_id| RecorderSummary {
                    recorder_id: recorder_id.clone(),
                    slot: 1,
                    step: 4,
                    first_current: Some(proposal.clone()),
                    aggregate_prior: None,
                })
                .collect(),
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancelled),
        );
        let budget = ControlCallBudget::new(&context).unwrap();
        cancelled.store(true, Ordering::Release);
        assert_eq!(
            consensus.reconcile_post_decision_unknown_outcome(
                &budget,
                &AtomicBool::new(true),
                &proof,
                &command,
            ),
            Err(Error::UnknownOutcome)
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn fast_path_decision_is_durable_before_a_conflicting_slot_proposal() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let n1 = RecorderFileStore::new_with_membership(
            root.path().join("n1"),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let n3 = RecorderFileStore::new_with_membership(
            root.path().join("n3"),
            "n3",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let (n2_started_tx, n2_started_rx) = mpsc::sync_channel(1);
        let (n2_release_tx, n2_release_rx) = mpsc::sync_channel(1);
        let mut n2_release = ChannelRelease::new(n2_release_tx);
        let recorders = vec![
            ("n1".into(), Box::new(n1.clone()) as Box<dyn RecorderRpc>),
            (
                "n2".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n2",
                    started: n2_started_tx,
                    release_first: Mutex::new(n2_release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            ("n3".into(), Box::new(n3.clone()) as Box<dyn RecorderRpc>),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );

        // Occupy n2's record worker before the foreign proposal. Its slot-2
        // hedge must remain queued, so n1+n3 are the exact FastPath quorum.
        let (background_tx, background_rx) = mpsc::sync_channel(1);
        let background = record_requests(&consensus, 1).remove(1);
        assert!(matches!(
            consensus.record_workers[1].dispatch(super::RecordJob {
                index: 0,
                context: RecorderRpcContext::default_timeout(),
                request: background,
                result: background_tx,
            }),
            super::RecordDispatch::Accepted
        ));
        assert_eq!(n2_started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));

        let foreign = consensus
            .propose_at(
                RecorderRpcContext::default_timeout(),
                2,
                LogHash::ZERO,
                Command::new(CommandKind::ReadBarrier, Vec::new()),
            )
            .unwrap();
        let foreign_proof = consensus
            .inspect_decision_proof_at(&RecorderRpcContext::default_timeout(), 2)
            .unwrap()
            .expect("a successful FastPath decision must be quorum-durable");
        assert!(matches!(foreign_proof, DecisionProof::FastPath { .. }));
        assert_eq!(
            n1.inspect_decision_proof(2).unwrap(),
            Some(foreign_proof.clone())
        );
        assert_eq!(
            n3.inspect_decision_proof(2).unwrap(),
            Some(foreign_proof.clone())
        );

        n2_release.release();
        assert!(background_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .1
            .is_ok());

        let conflicting = consensus
            .propose_at(
                RecorderRpcContext::default_timeout(),
                2,
                LogHash::ZERO,
                Command::new(CommandKind::Deterministic, b"conflicting".to_vec()),
            )
            .unwrap();
        assert_eq!(conflicting, foreign);
        assert_eq!(
            consensus
                .inspect_decision_proof_at(&RecorderRpcContext::default_timeout(), 2)
                .unwrap(),
            Some(foreign_proof)
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn concurrent_proposers_same_command_publish_one_durable_decision() {
        let _blocking = lock_blocking_control_tests();
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let stores = ["n1", "n2", "n3"].map(|recorder_id| {
            RecorderFileStore::new_with_membership(
                root.path().join(recorder_id),
                recorder_id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        });
        let proposer = |proposer_id| {
            ThreeNodeConsensus::from_recorders_with_ids(
                "cluster",
                proposer_id,
                1,
                1,
                ["n1", "n2", "n3"]
                    .into_iter()
                    .zip(stores.iter())
                    .map(|(recorder_id, store)| {
                        (
                            recorder_id.into(),
                            Box::new(store.clone()) as Box<dyn RecorderRpc>,
                        )
                    })
                    .collect(),
            )
            .unwrap()
        };
        let consensuses = ["p1", "p2", "p3"].map(|proposer_id| Arc::new(proposer(proposer_id)));
        let command = StoredCommand::new(EntryType::Command, b"same-concurrent-command".to_vec());
        consensuses[0]
            .register_command(
                &RecorderRpcContext::with_timeout(Duration::from_secs(5)),
                command.hash(),
                command.payload.clone(),
            )
            .unwrap();

        let contexts: [RecorderRpcContext; 3] = std::array::from_fn(|_| {
            RecorderRpcContext::with_timeout_and_cancellation(
                Duration::from_secs(5),
                Arc::new(AtomicBool::new(false)),
            )
        });
        let record_probes: [Arc<TestControlOperationProbe>; 3] =
            std::array::from_fn(|_| Arc::new(TestControlOperationProbe::default()));
        let control_probes: [Arc<TestControlOperationProbe>; 3] =
            std::array::from_fn(|_| Arc::new(TestControlOperationProbe::default()));
        let _record_guards: [super::TestControlOperationProbeGuard; 3] =
            std::array::from_fn(|index| {
                consensuses[index]
                    .install_test_record_operation_probe(1, Arc::clone(&record_probes[index]))
                    .unwrap()
            });
        let _control_guards: [super::TestControlOperationProbeGuard; 3] =
            std::array::from_fn(|index| {
                super::install_test_control_operation_probe(
                    &contexts[index],
                    Arc::clone(&control_probes[index]),
                )
                .unwrap()
            });
        let start = Arc::new(Barrier::new(consensuses.len() + 1));
        let callers: Vec<_> = consensuses
            .iter()
            .zip(contexts)
            .map(|(consensus, context)| {
                let consensus = Arc::clone(consensus);
                let start = Arc::clone(&start);
                let command = command.clone();
                thread::spawn(move || {
                    start.wait();
                    consensus.propose_stored_at(context, 1, LogHash::ZERO, command)
                })
            })
            .collect();
        start.wait();
        let results: Vec<_> = callers
            .into_iter()
            .map(|caller| caller.join().unwrap())
            .collect();

        for consensus in &consensuses {
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        }
        for (record, control) in record_probes.iter().zip(&control_probes) {
            assert!(
                record.dispatch_count() > 0,
                "record broadcast was not admitted"
            );
            assert_eq!(record.pending(), 0);
            assert!(
                record.cancel_count() > 0,
                "each exact record quorum must cancel its cooperative minority hedge"
            );
            assert_eq!(record.quarantine_count(), 0);
            assert!(
                control.dispatch_count() > 0,
                "decision proof install was not admitted"
            );
            assert_eq!(control.pending(), 0);
            assert_eq!(control.quarantine_count(), 0);
        }
        assert!(
            results.iter().all(Result::is_ok),
            "concurrent same-command proposal returned an ambiguous result; record/control dispatches: {:?}",
            record_probes
                .iter()
                .zip(&control_probes)
                .map(|(record, control)| (record.dispatch_count(), control.dispatch_count()))
                .collect::<Vec<_>>(),
        );
        let entries: Vec<_> = results.into_iter().map(Result::unwrap).collect();
        assert!(entries.iter().all(|entry| entry == &entries[0]));
        let CertifiedDecisionInspection::Committed(decision) = consensuses[0]
            .inspect_certified_decision_at(
                &RecorderRpcContext::with_timeout(Duration::from_secs(5)),
                1,
                LogHash::ZERO,
            )
            .unwrap()
        else {
            panic!("a completed concurrent proposal must be authoritatively discoverable");
        };
        assert_eq!(decision.entry, entries[0]);
    }

    #[test]
    fn reclaimed_record_hedge_stays_retryable_across_two_hundred_operations() {
        let (started_tx, started_rx) = mpsc::sync_channel(512);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let mut n2_release = ChannelRelease::new(release_tx);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n1",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n2",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n3",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        let background_request = record_requests(&consensus, 1).remove(1);
        let background_expected = record_summary("n2", background_request.clone());
        let (background_tx, background_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            consensus.record_workers[1].dispatch(super::RecordJob {
                index: 1,
                context: RecorderRpcContext::default_timeout(),
                request: background_request,
                result: background_tx,
            }),
            super::RecordDispatch::Accepted
        ));
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        assert_eq!(
            consensus
                .record_broadcast(record_requests(&consensus, 2))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            started_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty),
            "the slot-2 hedge must be reclaimed while n2 is occupied"
        );
        n2_release.release();
        assert_eq!(
            background_rx.recv_timeout(Duration::from_secs(1)),
            Ok((1, Ok(background_expected))),
            "the occupied n2 worker must drain before retry traffic"
        );
        assert_eq!(
            consensus
                .record_broadcast(record_requests(&consensus, 3))
                .unwrap()
                .len(),
            2
        );

        let failure = (4..=203).find_map(|slot| {
            let result = consensus.record_broadcast(record_requests(&consensus, slot));
            match &result {
                Ok(replies)
                    if replies.len() >= 2 && replies.iter().all(|reply| reply.slot == slot) =>
                {
                    None
                }
                _ => Some((slot, result)),
            }
        });
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        assert!(
            failure.is_none(),
            "a reclaimed recorder hedge must stay retryable: {failure:?}"
        );
    }

    #[test]
    fn saturated_recorder_keeps_quorum_reachable_when_another_worker_fails() {
        let (started_tx, started_rx) = mpsc::sync_channel(2);
        let (_release_tx, release_rx) = mpsc::sync_channel(0);
        let exact_release = Arc::new((Mutex::new(false), Condvar::new()));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n1",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(FailingFromSlotRecorder {
                    recorder_id: "n2",
                    fail_from: 3,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(GatedRecordRecorder {
                    recorder_id: "n3",
                    release: Arc::clone(&exact_release),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        let first = thread::scope(|scope| {
            let proposal =
                scope.spawn(|| consensus.record_broadcast(record_requests(&consensus, 1)));
            assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
            let (released, condition) = &*exact_release;
            *released.lock().unwrap() = true;
            condition.notify_all();
            proposal.join().unwrap().unwrap()
        });
        assert_eq!(first.len(), 2);
        assert_eq!(
            consensus
                .record_broadcast(record_requests(&consensus, 2))
                .unwrap()
                .len(),
            2
        );

        let third = consensus.record_broadcast(record_requests(&consensus, 3));
        assert!(
            matches!(third, Ok(ref replies) if replies.len() == 2
                && replies.iter().any(|reply| reply.recorder_id == "n1")
                && replies.iter().any(|reply| reply.recorder_id == "n3")),
            "a reclaimed healthy hedge must keep the quorum retryable after a worker failure: {third:?}"
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn command_lookup_drain_does_not_improve_a_frozen_missing_candidate() {
        let command = StoredCommand::new(EntryType::Command, b"available".to_vec());
        let value = AcceptedValue::from_command("cluster", 7, 1, 1, LogHash::ZERO, &command);
        let (observed_tx, observed_rx) = mpsc::sync_channel(2);
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (_release_tx, release_rx) = mpsc::sync_channel(0);
        let missing_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_missing = GateRelease::new(Arc::clone(&missing_gate));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(GatedMissingCommandRecorder {
                    observed: observed_tx.clone(),
                    gate: Arc::clone(&missing_gate),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(GatedMissingCommandRecorder {
                    observed: observed_tx,
                    gate: Arc::clone(&missing_gate),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(BlockingCommandRecorder {
                    started: started_tx,
                    release: Mutex::new(release_rx),
                    command: command.clone(),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let fetching = Arc::clone(&consensus);
        let fetch = thread::spawn(move || {
            done_tx
                .send(fetching.fetch_verified_value(
                    7,
                    &value,
                    &RecorderRpcContext::default_timeout(),
                    &AtomicBool::new(false),
                ))
                .unwrap();
        });

        assert_eq!(observed_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
        assert_eq!(observed_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
        release_gate(&missing_gate);
        assert!(matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));

        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Ok(None)
        );
        fetch.join().unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn fetch_partial_admission_cancellation_drains_the_admitted_job() {
        let _blocking = lock_blocking_control_tests();
        let command = StoredCommand::new(EntryType::Command, b"fetch-cancel".to_vec());
        let value = AcceptedValue::from_command("cluster", 7, 1, 1, LogHash::ZERO, &command);
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let worker_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let dispatch_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_worker = GateRelease::new(Arc::clone(&worker_gate));
        let _release_dispatch = GateRelease::new(Arc::clone(&dispatch_gate));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedFetchRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: (recorder_id == "n1").then(|| Arc::clone(&worker_gate)),
                        reply: Ok(None),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let cancellation = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancellation),
        );
        let (hook_tx, hook_rx) = mpsc::sync_channel(1);
        let _hook = pause_after_next_fetch_dispatch(
            Arc::clone(&cancellation),
            hook_tx,
            Arc::clone(&dispatch_gate),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.fetch_verified_value(
                        7,
                        &value,
                        &context,
                        &AtomicBool::new(false),
                    ))
                    .unwrap()
            })
        };

        hook_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "n1"
        );
        cancellation.store(true, Ordering::Release);
        release_gate(&dispatch_gate);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&worker_gate);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::RpcCancelled)
        );
        caller.join().unwrap();
        assert_eq!(entered_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn fetch_late_unknown_or_invalid_evidence_beats_a_frozen_command() {
        let _blocking = lock_blocking_control_tests();
        let run = |late_reply: super::Result<Option<StoredCommand>>, expected: Error| {
            let command = StoredCommand::new(EntryType::Command, b"fetch-proof".to_vec());
            let value = AcceptedValue::from_command("cluster", 7, 1, 1, LogHash::ZERO, &command);
            let (entered_tx, entered_rx) = mpsc::sync_channel(3);
            let early = Arc::new((Mutex::new(false), Condvar::new()));
            let late = Arc::new((Mutex::new(false), Condvar::new()));
            let _release_early = GateRelease::new(Arc::clone(&early));
            let _release_late = GateRelease::new(Arc::clone(&late));
            let recorders = vec![
                (
                    "n1".into(),
                    Box::new(ScriptedFetchRecorder {
                        recorder_id: "n1",
                        entered: entered_tx.clone(),
                        gate: Some(Arc::clone(&early)),
                        reply: Ok(Some(command.clone())),
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n2".into(),
                    Box::new(ScriptedFetchRecorder {
                        recorder_id: "n2",
                        entered: entered_tx.clone(),
                        gate: None,
                        reply: Ok(None),
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n3".into(),
                    Box::new(ScriptedFetchRecorder {
                        recorder_id: "n3",
                        entered: entered_tx,
                        gate: Some(Arc::clone(&late)),
                        reply: late_reply,
                    }) as Box<dyn RecorderRpc>,
                ),
            ];
            let consensus = Arc::new(
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap(),
            );
            let (result_tx, result_rx) = mpsc::sync_channel(1);
            let caller = {
                let consensus = Arc::clone(&consensus);
                thread::spawn(move || {
                    result_tx
                        .send(consensus.fetch_verified_value(
                            7,
                            &value,
                            &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                            &AtomicBool::new(false),
                        ))
                        .unwrap()
                })
            };
            for _ in 0..3 {
                entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            }
            release_gate(&early);
            assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
            release_gate(&late);
            assert_eq!(
                result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                Err(expected)
            );
            caller.join().unwrap();
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        };
        run(Err(Error::UnknownOutcome), Error::UnknownOutcome);
        run(
            Ok(Some(StoredCommand::new(
                EntryType::Command,
                b"wrong-hash".to_vec(),
            ))),
            Error::CommandHashMismatch,
        );
    }

    #[test]
    fn fetch_root_context_errors_are_exact_before_mutation_and_unknown_after() {
        let command = StoredCommand::new(EntryType::Command, b"fetch-context".to_vec());
        let value = AcceptedValue::from_command("cluster", 7, 1, 1, LogHash::ZERO, &command);
        let make_consensus = || {
            let (entered_tx, _entered_rx) = mpsc::sync_channel(3);
            let recorders = ["n1", "n2", "n3"]
                .into_iter()
                .map(|recorder_id| {
                    (
                        recorder_id.into(),
                        Box::new(ScriptedFetchRecorder {
                            recorder_id,
                            entered: entered_tx.clone(),
                            gate: None,
                            reply: Ok(None),
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect();
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap()
        };
        let cancellation = Arc::new(AtomicBool::new(true));
        let cancelled = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancellation),
        );
        assert_eq!(
            make_consensus().fetch_verified_value(7, &value, &cancelled, &AtomicBool::new(false)),
            Err(Error::RpcCancelled)
        );
        assert_eq!(
            make_consensus().fetch_verified_value(7, &value, &cancelled, &AtomicBool::new(true)),
            Err(Error::UnknownOutcome)
        );
        let expired = RecorderRpcContext::with_timeout(Duration::ZERO);
        assert_eq!(
            make_consensus().fetch_verified_value(7, &value, &expired, &AtomicBool::new(false)),
            Err(Error::RpcDeadlineExceeded)
        );
        assert_eq!(
            make_consensus().fetch_verified_value(7, &value, &expired, &AtomicBool::new(true)),
            Err(Error::UnknownOutcome)
        );
    }

    #[test]
    fn fetch_short_deadline_admits_no_backend_work() {
        let command = StoredCommand::new(EntryType::Command, b"fetch-short".to_vec());
        let value = AcceptedValue::from_command("cluster", 7, 1, 1, LogHash::ZERO, &command);
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedFetchRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: None,
                        reply: Ok(None),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        assert_eq!(
            consensus.fetch_verified_value(
                7,
                &value,
                &RecorderRpcContext::with_timeout(Duration::ZERO),
                &AtomicBool::new(false),
            ),
            Err(Error::RpcDeadlineExceeded)
        );
        assert_eq!(entered_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    }

    #[test]
    fn read_fence_quorum_drains_late_unknown_and_later_occupied_evidence() {
        let _blocking = lock_blocking_control_tests();
        let run = |late_reply: super::Result<ReadFenceSlotState>,
                   expected: super::Result<CertifiedDecisionInspection>| {
            let (entered_tx, entered_rx) = mpsc::sync_channel(3);
            let early = Arc::new((Mutex::new(false), Condvar::new()));
            let late = Arc::new((Mutex::new(false), Condvar::new()));
            let _release_early = GateRelease::new(Arc::clone(&early));
            let _release_late = GateRelease::new(Arc::clone(&late));
            let recorder = |recorder_id, gate, reply| {
                Box::new(ScriptedFenceRecorder {
                    recorder_id,
                    entered: entered_tx.clone(),
                    gate,
                    reply,
                }) as Box<dyn RecorderRpc>
            };
            let recorders = vec![
                (
                    "n1".into(),
                    recorder(
                        "n1",
                        Some(Arc::clone(&early)),
                        Ok(ReadFenceSlotState::Empty),
                    ),
                ),
                (
                    "n2".into(),
                    recorder(
                        "n2",
                        Some(Arc::clone(&early)),
                        Ok(ReadFenceSlotState::Empty),
                    ),
                ),
                (
                    "n3".into(),
                    recorder("n3", Some(Arc::clone(&late)), late_reply),
                ),
            ];
            let consensus = Arc::new(
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap(),
            );
            let (result_tx, result_rx) = mpsc::sync_channel(1);
            let caller = {
                let consensus = Arc::clone(&consensus);
                thread::spawn(move || {
                    result_tx
                        .send(consensus.inspect_context_read_fence_at(
                            &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                            1,
                            LogHash::ZERO,
                        ))
                        .unwrap()
                })
            };
            for _ in 0..3 {
                entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            }
            release_gate(&early);
            assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
            release_gate(&late);
            assert_eq!(
                result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                expected
            );
            caller.join().unwrap();
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        };
        run(
            Ok(ReadFenceSlotState::Occupied { summary: None }),
            Ok(CertifiedDecisionInspection::Empty),
        );
        run(Err(Error::UnknownOutcome), Err(Error::UnknownOutcome));
    }

    #[test]
    fn read_fence_partial_admission_cancellation_drains_the_admitted_job() {
        let _blocking = lock_blocking_control_tests();
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let worker_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let dispatch_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_worker = GateRelease::new(Arc::clone(&worker_gate));
        let _release_dispatch = GateRelease::new(Arc::clone(&dispatch_gate));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedFenceRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: (recorder_id == "n1").then(|| Arc::clone(&worker_gate)),
                        reply: Ok(ReadFenceSlotState::Empty),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let root = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&root),
        );
        let (hook_tx, hook_rx) = mpsc::sync_channel(1);
        let _hook =
            pause_after_next_fetch_dispatch(Arc::clone(&root), hook_tx, Arc::clone(&dispatch_gate));
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let caller = {
            let consensus = Arc::clone(&consensus);
            thread::spawn(move || {
                result_tx
                    .send(consensus.inspect_context_read_fence_at(&context, 1, LogHash::ZERO))
                    .unwrap()
            })
        };
        hook_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "n1"
        );
        root.store(true, Ordering::Release);
        release_gate(&dispatch_gate);
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_gate(&worker_gate);
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(Error::RpcCancelled)
        );
        caller.join().unwrap();
        assert_eq!(entered_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn read_fence_short_deadline_admits_no_backend_work() {
        let (entered_tx, entered_rx) = mpsc::sync_channel(3);
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedFenceRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: None,
                        reply: Ok(ReadFenceSlotState::Empty),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        assert_eq!(
            consensus.inspect_context_read_fence_at(
                &RecorderRpcContext::with_timeout(Duration::ZERO),
                1,
                LogHash::ZERO,
            ),
            Err(Error::RpcDeadlineExceeded)
        );
        assert_eq!(entered_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    }

    #[test]
    fn read_fence_drain_miss_is_deadline_for_reads_and_unknown_after_mutation() {
        let _blocking = lock_blocking_control_tests();
        let run = |mutation_started: bool, expected: Error| {
            let (slow_tx, slow_rx) = mpsc::sync_channel(1);
            let slow = Arc::new((Mutex::new(false), Condvar::new()));
            let fast = Arc::new((Mutex::new(false), Condvar::new()));
            let (fast_tx, fast_rx) = mpsc::sync_channel(2);
            let _release_slow = GateRelease::new(Arc::clone(&slow));
            let _release_fast = GateRelease::new(Arc::clone(&fast));
            let fast_recorder = |recorder_id| {
                Box::new(ScriptedFenceRecorder {
                    recorder_id,
                    entered: fast_tx.clone(),
                    gate: Some(Arc::clone(&fast)),
                    reply: Ok(ReadFenceSlotState::Empty),
                }) as Box<dyn RecorderRpc>
            };
            let recorders = vec![
                (
                    "n1".into(),
                    Box::new(ScriptedFenceRecorder {
                        recorder_id: "n1",
                        entered: slow_tx,
                        gate: Some(Arc::clone(&slow)),
                        reply: Ok(ReadFenceSlotState::Empty),
                    }) as Box<dyn RecorderRpc>,
                ),
                ("n2".into(), fast_recorder("n2")),
                ("n3".into(), fast_recorder("n3")),
            ];
            let consensus = Arc::new(
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap(),
            );
            let root = Arc::new(AtomicBool::new(false));
            let context = RecorderRpcContext::with_timeout_and_cancellation(
                Duration::from_secs(1),
                Arc::clone(&root),
            );
            let (token_tx, token_rx) = mpsc::sync_channel(1);
            let _token = capture_next_fetch_group_token(Arc::clone(&root), token_tx);
            let (result_tx, result_rx) = mpsc::sync_channel(1);
            let caller = {
                let consensus = Arc::clone(&consensus);
                thread::spawn(move || {
                    result_tx
                        .send(consensus.inspect_context_read_fence_with_budget(
                            &ControlCallBudget::new(&context).unwrap(),
                            &AtomicBool::new(mutation_started),
                            1,
                            LogHash::ZERO,
                        ))
                        .unwrap()
                })
            };
            slow_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            let group_token = token_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            let (ack_tx, ack_rx) = mpsc::sync_channel(1);
            let _timeout = force_next_control_group_drain_timeout(
                group_token,
                Arc::clone(&consensus.read_fence_workers[0].state),
                ack_tx,
            );
            release_gate(&fast);
            ack_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(
                result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                Err(expected)
            );
            assert!(consensus.read_fence_workers[0]
                .state
                .quarantined
                .load(Ordering::Acquire));
            release_gate(&slow);
            caller.join().unwrap();
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
            fast_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            fast_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(
                consensus.inspect_context_read_fence_at(
                    &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    1,
                    LogHash::ZERO,
                ),
                Ok(CertifiedDecisionInspection::Empty)
            );
        };
        run(false, Error::RpcDeadlineExceeded);
        run(true, Error::UnknownOutcome);
    }

    #[test]
    fn read_fence_empty_quorum_is_independent_of_observation_order() {
        let occupied = || ReadFenceSlotState::Occupied { summary: None };
        let empty = || ReadFenceSlotState::Empty;
        for states in [
            [occupied(), empty(), empty()],
            [empty(), occupied(), empty()],
            [empty(), empty(), occupied()],
        ] {
            let (entered_tx, _entered_rx) = mpsc::sync_channel(3);
            let recorders = ["n1", "n2", "n3"]
                .into_iter()
                .zip(states)
                .map(|(recorder_id, state)| {
                    (
                        recorder_id.into(),
                        Box::new(ScriptedFenceRecorder {
                            recorder_id,
                            entered: entered_tx.clone(),
                            gate: None,
                            reply: Ok(state),
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect();
            let consensus =
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap();
            assert_eq!(
                consensus.inspect_context_read_fence_at(
                    &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    1,
                    LogHash::ZERO,
                ),
                Ok(CertifiedDecisionInspection::Empty)
            );
        }
        let (entered_tx, _entered_rx) = mpsc::sync_channel(3);
        let recorders = [("n1", empty()), ("n2", occupied()), ("n3", occupied())]
            .into_iter()
            .map(|(recorder_id, state)| {
                (
                    recorder_id.into(),
                    Box::new(ScriptedFenceRecorder {
                        recorder_id,
                        entered: entered_tx.clone(),
                        gate: None,
                        reply: Ok(state),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        assert_eq!(
            consensus.inspect_context_read_fence_at(
                &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                1,
                LogHash::ZERO,
            ),
            Err(Error::TypedRecordRequired)
        );
    }

    #[test]
    fn read_fence_summary_fetch_reuses_one_budget_after_each_group_drains() {
        let _blocking = lock_blocking_control_tests();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"budget-handoff".to_vec());
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
        );
        let recorders = membership
            .members()
            .iter()
            .map(|recorder_id| {
                (
                    recorder_id.clone(),
                    Box::new(SummaryFetchBudgetRecorder {
                        summary: RecordSummary {
                            recorder_id: recorder_id.clone(),
                            slot: 1,
                            config_id: 1,
                            config_digest: membership.digest(),
                            step: 4,
                            first_current: Some(proposal.clone()),
                            aggregate_prior: None,
                            decided: None,
                        },
                        command: command.clone(),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let root = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&root),
        );
        let (event_tx, event_rx) = mpsc::sync_channel(3);
        let _hook = record_budget_identity_for(Arc::clone(&root), event_tx);
        assert!(matches!(
            consensus.inspect_context_read_fence_at(&context, 1, LogHash::ZERO),
            Ok(CertifiedDecisionInspection::Committed(_))
        ));
        let BudgetIdentityEvent::ReadFenceHandoff {
            deadline: fence_deadline,
            work_deadline: fence_work_deadline,
            outstanding: fence_outstanding,
        } = event_rx.recv_timeout(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected fence handoff event");
        };
        let BudgetIdentityEvent::SummaryHandoff {
            deadline,
            work_deadline,
            outstanding,
        } = event_rx.recv_timeout(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected summary handoff event");
        };
        let BudgetIdentityEvent::FetchDispatch {
            deadline: fetch_deadline,
            work_deadline: fetch_work_deadline,
        } = event_rx.recv_timeout(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected fetch dispatch event");
        };
        assert_eq!(
            outstanding, 0,
            "summary group must drain before fetch starts"
        );
        assert_eq!(fence_outstanding, 0);
        assert_eq!(fence_deadline, deadline);
        assert_eq!(fence_work_deadline, work_deadline);
        assert_eq!(fetch_deadline, deadline);
        assert_eq!(fetch_work_deadline, work_deadline);
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn certified_config_fetch_hands_the_same_budget_and_mutation_to_install() {
        let _blocking = lock_blocking_control_tests();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = ConfigChange::stop(1, membership.digest()).to_stored_command();
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
        );
        let recorders = membership
            .members()
            .iter()
            .map(|recorder_id| {
                (
                    recorder_id.clone(),
                    Box::new(SummaryFetchBudgetRecorder {
                        summary: RecordSummary {
                            recorder_id: recorder_id.clone(),
                            slot: 1,
                            config_id: 1,
                            config_digest: membership.digest(),
                            step: 4,
                            first_current: Some(proposal.clone()),
                            aggregate_prior: None,
                            decided: None,
                        },
                        command: command.clone(),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let root = Arc::new(AtomicBool::new(false));
        let context = RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&root),
        );
        let (event_tx, event_rx) = mpsc::sync_channel(5);
        let _hook = record_budget_identity_for(Arc::clone(&root), event_tx);
        assert!(matches!(
            consensus.inspect_context_read_fence_at(&context, 1, LogHash::ZERO),
            Ok(CertifiedDecisionInspection::Committed(_))
        ));
        let BudgetIdentityEvent::ReadFenceHandoff {
            deadline: fence_deadline,
            work_deadline: fence_work_deadline,
            outstanding: fence_outstanding,
        } = event_rx.recv_timeout(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected fence handoff event");
        };
        let BudgetIdentityEvent::SummaryHandoff {
            deadline,
            work_deadline,
            outstanding,
        } = event_rx.recv_timeout(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected summary handoff event");
        };
        let BudgetIdentityEvent::FetchDispatch {
            deadline: fetch_deadline,
            work_deadline: fetch_work_deadline,
        } = event_rx.recv_timeout(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected fetch dispatch event");
        };
        let BudgetIdentityEvent::FetchHandoff {
            deadline: handoff_deadline,
            work_deadline: handoff_work_deadline,
            outstanding: fetch_outstanding,
            mutation_started: fetch_mutation,
        } = event_rx.recv_timeout(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected fetch-to-install handoff event");
        };
        let BudgetIdentityEvent::InstallDispatch {
            deadline: install_deadline,
            work_deadline: install_work_deadline,
            mutation_started: install_mutation,
            mutation_started_set,
        } = event_rx.recv_timeout(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected install dispatch event");
        };
        assert_eq!(fence_outstanding, 0);
        assert_eq!(outstanding, 0);
        assert_eq!(
            fetch_outstanding, 0,
            "fetch must drain before Install starts"
        );
        assert_eq!(fence_deadline, deadline);
        assert_eq!(fence_work_deadline, work_deadline);
        assert_eq!(fetch_deadline, deadline);
        assert_eq!(fetch_work_deadline, work_deadline);
        assert_eq!(handoff_deadline, deadline);
        assert_eq!(handoff_work_deadline, work_deadline);
        assert_eq!(install_deadline, deadline);
        assert_eq!(install_work_deadline, work_deadline);
        assert_eq!(fetch_mutation, install_mutation);
        assert!(
            mutation_started_set,
            "Install admission must mark the shared root mutation state"
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn finish_decision_fetch_then_install_reuses_one_budget_without_recapture() {
        let _blocking = lock_blocking_control_tests();
        let run = |known_command: Option<StoredCommand>| {
            let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
            let command = ConfigChange::stop(1, membership.digest()).to_stored_command();
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
                config_digest: membership.digest(),
                proposal: proposal.clone(),
                summaries: membership.members()[..membership.quorum_size()]
                    .iter()
                    .map(|recorder_id| RecorderSummary {
                        recorder_id: recorder_id.clone(),
                        slot: 1,
                        step: 4,
                        first_current: Some(proposal.clone()),
                        aggregate_prior: None,
                    })
                    .collect(),
            };
            let recorders = membership
                .members()
                .iter()
                .map(|recorder_id| {
                    (
                        recorder_id.clone(),
                        Box::new(SummaryFetchBudgetRecorder {
                            summary: RecordSummary {
                                recorder_id: recorder_id.clone(),
                                slot: 1,
                                config_id: 1,
                                config_digest: membership.digest(),
                                step: 4,
                                first_current: None,
                                aggregate_prior: None,
                                decided: None,
                            },
                            command: command.clone(),
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect();
            let consensus =
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap();
            let root = Arc::new(AtomicBool::new(false));
            let context = RecorderRpcContext::with_timeout_and_cancellation(
                Duration::from_secs(1),
                Arc::clone(&root),
            );
            let mutation_started = Arc::new(AtomicBool::new(false));
            let mutation_identity = Arc::as_ptr(&mutation_started) as usize;
            let constructor_calls = Arc::new(AtomicUsize::new(0));
            let _constructor_hook = count_control_budget_constructors_for(
                Arc::clone(&root),
                Arc::clone(&constructor_calls),
            );
            let (event_tx, event_rx) = mpsc::sync_channel(3);
            let _identity_hook = record_budget_identity_for(Arc::clone(&root), event_tx);
            assert!(matches!(
                consensus.finish_decision_with_context(
                    proof.clone(),
                    known_command.as_ref(),
                    false,
                    &context,
                    &mutation_started,
                ),
                Ok(DriveOutcome::Decision(returned)) if returned == proof
            ));
            assert_eq!(
                constructor_calls.load(Ordering::Acquire),
                1,
                "the production finish path must capture its control budget once before fetch and reuse it for Install"
            );
            let BudgetIdentityEvent::FetchDispatch {
                deadline: fetch_deadline,
                work_deadline: fetch_work_deadline,
            } = event_rx.recv_timeout(Duration::from_secs(1)).unwrap()
            else {
                panic!("expected finish fetch dispatch event");
            };
            let BudgetIdentityEvent::FinishFetchHandoff {
                deadline: handoff_deadline,
                work_deadline: handoff_work_deadline,
                outstanding,
                mutation_started: fetch_mutation,
            } = event_rx.recv_timeout(Duration::from_secs(1)).unwrap()
            else {
                panic!("expected finish fetch-to-install handoff event");
            };
            let BudgetIdentityEvent::InstallDispatch {
                deadline: install_deadline,
                work_deadline: install_work_deadline,
                mutation_started: install_mutation,
                mutation_started_set,
            } = event_rx.recv_timeout(Duration::from_secs(1)).unwrap()
            else {
                panic!("expected finish install dispatch event");
            };
            assert_eq!(outstanding, 0, "fetch must drain before finish installs");
            assert_eq!(fetch_deadline, handoff_deadline);
            assert_eq!(fetch_work_deadline, handoff_work_deadline);
            assert_eq!(handoff_deadline, install_deadline);
            assert_eq!(handoff_work_deadline, install_work_deadline);
            assert_eq!(fetch_mutation, mutation_identity);
            assert_eq!(install_mutation, mutation_identity);
            assert!(mutation_started_set);
            assert!(mutation_started.load(Ordering::Acquire));
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        };
        run(None);
        run(Some(StoredCommand::new(
            EntryType::Command,
            b"mismatched-known-command".to_vec(),
        )));
    }

    #[test]
    fn fetch_drain_miss_is_deadline_for_reads_and_unknown_after_mutation() {
        let _blocking = lock_blocking_control_tests();
        let run = |mutation_started: bool, expected: Error| {
            let command = StoredCommand::new(EntryType::Command, b"drain-miss".to_vec());
            let value = AcceptedValue::from_command("cluster", 7, 1, 1, LogHash::ZERO, &command);
            let (entered_tx, entered_rx) = mpsc::sync_channel(1);
            let slow = Arc::new((Mutex::new(false), Condvar::new()));
            let fast = Arc::new((Mutex::new(false), Condvar::new()));
            let (fast_tx, fast_rx) = mpsc::sync_channel(2);
            let _release_slow = GateRelease::new(Arc::clone(&slow));
            let _release_fast = GateRelease::new(Arc::clone(&fast));
            let fast_recorder = |recorder_id| {
                Box::new(ScriptedFetchRecorder {
                    recorder_id,
                    entered: fast_tx.clone(),
                    gate: Some(Arc::clone(&fast)),
                    reply: Ok(Some(command.clone())),
                }) as Box<dyn RecorderRpc>
            };
            let recorders = vec![
                (
                    "n1".into(),
                    Box::new(ScriptedFetchRecorder {
                        recorder_id: "n1",
                        entered: entered_tx,
                        gate: Some(Arc::clone(&slow)),
                        reply: Ok(None),
                    }) as Box<dyn RecorderRpc>,
                ),
                ("n2".into(), fast_recorder("n2")),
                ("n3".into(), fast_recorder("n3")),
            ];
            let consensus = Arc::new(
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap(),
            );
            let root = Arc::new(AtomicBool::new(false));
            let context = RecorderRpcContext::with_timeout_and_cancellation(
                Duration::from_secs(1),
                Arc::clone(&root),
            );
            let (group_token_tx, group_token_rx) = mpsc::sync_channel(1);
            let _group_token = capture_next_fetch_group_token(Arc::clone(&root), group_token_tx);
            let (result_tx, result_rx) = mpsc::sync_channel(1);
            let caller = {
                let consensus = Arc::clone(&consensus);
                let fetch_value = value.clone();
                thread::spawn(move || {
                    result_tx
                        .send(consensus.fetch_verified_value(
                            7,
                            &fetch_value,
                            &context,
                            &AtomicBool::new(mutation_started),
                        ))
                        .unwrap()
                })
            };
            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            let group_token = group_token_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            let (timeout_tx, timeout_rx) = mpsc::sync_channel(1);
            let _timeout = force_next_control_group_drain_timeout(
                group_token,
                Arc::clone(&consensus.control_workers[0].state),
                timeout_tx,
            );
            fast_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            fast_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            release_gate(&fast);
            timeout_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(
                result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                Err(expected)
            );
            assert!(consensus.control_workers[0]
                .state
                .quarantined
                .load(Ordering::Acquire));
            release_gate(&slow);
            caller.join().unwrap();
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
            assert_eq!(
                consensus.fetch_verified_value(
                    7,
                    &value,
                    &RecorderRpcContext::with_timeout(Duration::from_secs(1)),
                    &AtomicBool::new(false),
                ),
                Ok(Some(command))
            );
        };
        run(false, Error::RpcDeadlineExceeded);
        run(true, Error::UnknownOutcome);
    }

    #[test]
    fn command_lookup_rejects_cryptographic_mismatch_despite_reachable_quorum() {
        let command = StoredCommand::new(EntryType::Command, b"mismatched".to_vec());
        let mut value = AcceptedValue::from_command("cluster", 7, 1, 1, LogHash::ZERO, &command);
        value.entry_hash = LogHash::ZERO;
        let (observed_tx, observed_rx) = mpsc::sync_channel(1);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(AvailableCommandRecorder {
                    command: command.clone(),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(MissingCommandRecorder {
                    observed: observed_tx,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(FailingCommandFetchRecorder) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert_eq!(
            consensus.fetch_verified_value(
                7,
                &value,
                &RecorderRpcContext::default_timeout(),
                &AtomicBool::new(false),
            ),
            Err(Error::Rejected(RejectReason::InvalidValue))
        );
        assert_eq!(observed_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
    }

    #[test]
    fn command_lookup_returns_no_quorum_when_a_control_worker_queue_is_full() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = GateRelease::new(Arc::clone(&release));
        let (observed_tx, observed_rx) = mpsc::sync_channel(1);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(BlockingCommandStoreRecorder {
                    started: started_tx,
                    release: Arc::clone(&release),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(MissingCommandRecorder {
                    observed: observed_tx,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(FailingCommandFetchRecorder) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let blocking = StoredCommand::new(EntryType::Command, b"blocking".to_vec());
        let (blocking_tx, _blocking_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            consensus.control_workers[0].dispatch(ControlJob::StoreCommand {
                index: 0,
                context: RecorderRpcContext::default_timeout(),
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: consensus.membership().digest(),
                command_hash: blocking.hash(),
                command: blocking,
                result: blocking_tx,
            }),
            ControlDispatch::Accepted
        ));
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let queued = StoredCommand::new(EntryType::Command, b"queued".to_vec());
        let (queued_tx, _queued_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            consensus.control_workers[0].dispatch(ControlJob::StoreCommand {
                index: 0,
                context: RecorderRpcContext::default_timeout(),
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: consensus.membership().digest(),
                command_hash: queued.hash(),
                command: queued,
                result: queued_tx,
            }),
            ControlDispatch::Accepted
        ));

        let command = StoredCommand::new(EntryType::Command, b"missing".to_vec());
        let value = AcceptedValue::from_command("cluster", 7, 1, 1, LogHash::ZERO, &command);
        assert_eq!(
            consensus.fetch_verified_value(
                7,
                &value,
                &RecorderRpcContext::default_timeout(),
                &AtomicBool::new(false),
            ),
            Err(Error::NoQuorum)
        );
        assert_eq!(observed_rx.recv_timeout(Duration::from_secs(1)), Ok(()));

        let (released, condition) = &*release;
        *released.lock().unwrap() = true;
        condition.notify_all();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn full_record_worker_queue_is_transient_unavailable_not_fatal() {
        // Every direct job emits a start event; leave room for the running,
        // queued, and recovery jobs so observability itself cannot block n1.
        let (started_tx, started_rx) = mpsc::sync_channel(4);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let mut release = ChannelRelease::new(release_tx);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n1",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n2",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(AlwaysIoRecorder) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        // Occupy n1's sole running slot before any consensus broadcast.  The
        // next direct admission fills its one queued slot, so the broadcast
        // below deterministically observes saturation rather than racing the
        // recorder's start signal.
        let (running_tx, running_rx) = mpsc::sync_channel(1);
        let running_request = record_requests(&consensus, 1).remove(0);
        assert!(matches!(
            consensus.record_workers[0].dispatch(super::RecordJob {
                index: 0,
                context: RecorderRpcContext::default_timeout(),
                request: running_request,
                result: running_tx,
            }),
            super::RecordDispatch::Accepted
        ));
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));

        let (queued_tx, queued_rx) = mpsc::sync_channel(1);
        let queued_request = record_requests(&consensus, 2).remove(0);
        assert!(matches!(
            consensus.record_workers[0].dispatch(super::RecordJob {
                index: 0,
                context: RecorderRpcContext::default_timeout(),
                request: queued_request,
                result: queued_tx,
            }),
            super::RecordDispatch::Accepted
        ));

        let saturated = consensus
            .record_broadcast(record_requests(&consensus, 3))
            .unwrap();
        assert!(
            saturated.len() == 1 && saturated[0].recorder_id == "n2",
            "a saturated n1 must leave the healthy n2 result retryable: {saturated:?}"
        );
        release.release();
        assert!(running_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .1
            .is_ok());
        assert!(queued_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .1
            .is_ok());

        let recovered = consensus
            .record_broadcast(record_requests(&consensus, 4))
            .unwrap();
        assert!(
            recovered.iter().any(|reply| reply.recorder_id == "n1"),
            "the worker must accept a later broadcast after running and queued work drain: {recovered:?}"
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn record_worker_queue_reports_saturation_after_one_running_and_one_queued_job() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let worker = super::RecordWorker::spawn(
            "n1".into(),
            Arc::new(BlockingRecorder {
                recorder_id: "n1",
                started: started_tx,
                release_first: Mutex::new(release_rx),
            }),
            1,
            membership.digest(),
        )
        .unwrap();
        let request = |slot| RecordRequest {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            slot,
            step: 4,
            proposal: Proposal::new(
                ProposalPriority::MAX,
                "n1",
                slot,
                AcceptedValue {
                    command_hash: LogHash::ZERO,
                    prev_hash: LogHash::ZERO,
                    entry_hash: LogHash::ZERO,
                },
            ),
            command: None,
        };
        let (first_tx, first_rx) = mpsc::sync_channel(1);
        let (second_tx, second_rx) = mpsc::sync_channel(1);
        let (third_tx, third_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch(super::RecordJob {
                index: 0,
                context: RecorderRpcContext::default_timeout(),
                request: request(1),
                result: first_tx,
            }),
            super::RecordDispatch::Accepted
        ));
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        assert!(matches!(
            worker.dispatch(super::RecordJob {
                index: 1,
                context: RecorderRpcContext::default_timeout(),
                request: request(2),
                result: second_tx,
            }),
            super::RecordDispatch::Accepted
        ));
        assert!(matches!(
            worker.dispatch(super::RecordJob {
                index: 2,
                context: RecorderRpcContext::default_timeout(),
                request: request(3),
                result: third_tx,
            }),
            super::RecordDispatch::Saturated
        ));
        assert!(matches!(
            third_rx.recv_timeout(Duration::from_secs(1)).unwrap().1,
            Err(Error::Io(message)) if message.contains("temporarily full")
        ));

        release_tx.send(()).unwrap();
        assert!(first_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .1
            .is_ok());
        assert!(second_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .1
            .is_ok());
        let idle_deadline = Instant::now() + Duration::from_secs(1);
        while !worker.is_idle() && Instant::now() < idle_deadline {
            thread::yield_now();
        }
        assert!(worker.is_idle());
    }

    #[test]
    fn disconnected_record_worker_is_fatal() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue {
                command_hash: LogHash::ZERO,
                prev_hash: LogHash::ZERO,
                entry_hash: LogHash::ZERO,
            },
        );
        let request = RecordRequest {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            slot: 1,
            step: 4,
            proposal,
            command: None,
        };
        let pending = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker = super::RecordWorker {
            state: Arc::new(super::RecordWorkerState {
                queue: Arc::new(super::RecordQueue {
                    state: Mutex::new(super::RecordQueueState {
                        jobs: std::collections::VecDeque::new(),
                        closed: true,
                    }),
                    available: Condvar::new(),
                }),
                pending: Arc::clone(&pending),
                cancellation: Arc::new(AtomicBool::new(false)),
                quarantined: AtomicBool::new(false),
                #[cfg(feature = "test-hooks")]
                live_groups: Mutex::new(BTreeMap::new()),
            }),
            handle: None,
        };
        let (result_tx, result_rx) = mpsc::sync_channel(1);

        worker.dispatch(super::RecordJob {
            index: 0,
            context: RecorderRpcContext::default_timeout(),
            request,
            result: result_tx,
        });

        assert_eq!(result_rx.recv().unwrap().1, Err(Error::ProposeFailed));
        assert_eq!(pending.load(std::sync::atomic::Ordering::Acquire), 0);
    }

    #[test]
    fn record_worker_survives_a_mutating_panic_and_processes_the_next_record() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let mutated = Arc::new(AtomicBool::new(false));
        let recorder = Arc::new(PanicThenSuccessfulRecordRecorder {
            recorder_id: "n1",
            calls: AtomicUsize::new(0),
            mutated: Arc::clone(&mutated),
        });
        let worker =
            super::RecordWorker::spawn("n1".into(), recorder.clone(), 1, membership.digest())
                .unwrap();
        let request = |slot| RecordRequest {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            slot,
            step: 4,
            proposal: Proposal::new(
                ProposalPriority::MAX,
                "n1",
                slot,
                AcceptedValue {
                    command_hash: LogHash::ZERO,
                    prev_hash: LogHash::ZERO,
                    entry_hash: LogHash::ZERO,
                },
            ),
            command: None,
        };

        let (first_tx, first_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch(super::RecordJob {
                index: 0,
                context: RecorderRpcContext::default_timeout(),
                request: request(1),
                result: first_tx,
            }),
            super::RecordDispatch::Accepted
        ));
        assert_eq!(
            first_rx.recv_timeout(Duration::from_secs(1)).unwrap().1,
            Err(Error::UnknownOutcome)
        );
        assert!(mutated.load(Ordering::Acquire));

        let (second_tx, second_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch(super::RecordJob {
                index: 1,
                context: RecorderRpcContext::default_timeout(),
                request: request(2),
                result: second_tx,
            }),
            super::RecordDispatch::Accepted
        ));
        let (_, reply) = second_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(reply, Ok(summary) if summary.recorder_id == "n1" && summary.slot == 2));
        assert_eq!(recorder.calls.load(Ordering::Acquire), 2);
    }

    #[test]
    fn recorder_panics_are_reported_without_panicking_the_proposer() {
        let n2_mutated = Arc::new(AtomicBool::new(false));
        let n3_mutated = Arc::new(AtomicBool::new(false));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n1",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(PanickingRecorder {
                    mutated: Arc::clone(&n2_mutated),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(PanickingRecorder {
                    mutated: Arc::clone(&n3_mutated),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert_eq!(
            consensus.record_broadcast(record_requests(&consensus, 1)),
            Err(Error::UnknownOutcome)
        );
        assert!(
            consensus.finish_pending_rpcs(Duration::from_secs(1)),
            "all admitted record workers must finish before their mutation state is asserted"
        );
        assert!(n2_mutated.load(Ordering::Acquire));
        assert!(n3_mutated.load(Ordering::Acquire));
    }

    #[test]
    fn control_worker_panic_classifies_mutations_as_unknown_and_reads_as_definite() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let mutations = Arc::new(AtomicUsize::new(0));
        let worker = ControlWorker::spawn(Arc::new(PanicAfterMutationControlRecorder {
            mutations: Arc::clone(&mutations),
        }))
        .unwrap();
        let context = RecorderRpcContext::default_timeout();

        let (install_tx, install_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch(ControlJob::InstallProof {
                index: 0,
                context: context.clone(),
                proof: test_decision_proof(&membership),
                membership: membership.clone(),
                result: install_tx,
            }),
            ControlDispatch::Accepted
        ));
        assert_eq!(
            install_rx.recv_timeout(Duration::from_secs(1)).unwrap().1,
            Err(Error::UnknownOutcome)
        );

        let command = StoredCommand::new(EntryType::Command, b"panic-store".to_vec());
        let (store_tx, store_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch(ControlJob::StoreCommand {
                index: 0,
                context: context.clone(),
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                command_hash: command.hash(),
                command,
                result: store_tx,
            }),
            ControlDispatch::Accepted
        ));
        assert_eq!(
            store_rx.recv_timeout(Duration::from_secs(1)).unwrap().1,
            Err(Error::UnknownOutcome)
        );
        assert_eq!(mutations.load(Ordering::Acquire), 2);

        let (fetch_tx, fetch_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch(ControlJob::FetchCommand {
                index: 0,
                context,
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                command_hash: LogHash::ZERO,
                result: fetch_tx,
            }),
            ControlDispatch::Accepted
        ));
        assert_eq!(
            fetch_rx.recv_timeout(Duration::from_secs(1)).unwrap().1,
            Err(Error::ProposeFailed)
        );

        let (inspect_tx, inspect_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch(ControlJob::InspectProof {
                index: 0,
                context: RecorderRpcContext::default_timeout(),
                slot: 1,
                result: inspect_tx,
            }),
            ControlDispatch::Accepted
        ));
        assert_eq!(
            inspect_rx.recv_timeout(Duration::from_secs(1)).unwrap().1,
            Err(Error::ProposeFailed)
        );

        let (summary_tx, summary_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch(ControlJob::InspectSummary {
                index: 0,
                context: RecorderRpcContext::default_timeout(),
                slot: 1,
                result: summary_tx,
            }),
            ControlDispatch::Accepted
        ));
        assert_eq!(
            summary_rx.recv_timeout(Duration::from_secs(1)).unwrap().1,
            Err(Error::ProposeFailed)
        );

        let (fence_tx, fence_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            worker.dispatch(ControlJob::ObserveReadFence {
                index: 0,
                context: RecorderRpcContext::default_timeout(),
                request: ReadFenceRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot: 1,
                },
                result: fence_tx,
            }),
            ControlDispatch::Accepted
        ));
        assert_eq!(
            fence_rx.recv_timeout(Duration::from_secs(1)).unwrap().1,
            Err(Error::ProposeFailed)
        );
    }

    #[test]
    fn mutating_control_quorum_collectors_preserve_panic_unknown_outcomes() {
        let mutations = Arc::new(AtomicUsize::new(0));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(PanicAfterMutationControlRecorder {
                        mutations: Arc::clone(&mutations),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let context = RecorderRpcContext::default_timeout();
        let command = StoredCommand::new(EntryType::Command, b"quorum-panic".to_vec());
        let mutation_started = AtomicBool::new(false);
        let budget = ControlCallBudget::new(&context).unwrap();
        assert_eq!(
            consensus.store_command_on_quorum_with_budget(
                &budget,
                &mutation_started,
                command.hash(),
                &command,
            ),
            Err(Error::UnknownOutcome)
        );
        assert!(mutation_started.load(Ordering::Acquire));

        let mutation_started = AtomicBool::new(false);
        assert_eq!(
            consensus.install_decision_proof_quorum(
                test_decision_proof(consensus.membership()),
                &context,
                &mutation_started,
            ),
            Err(Error::UnknownOutcome)
        );
        assert!(mutation_started.load(Ordering::Acquire));
        assert!(mutations.load(Ordering::Acquire) >= 2);
    }
}
