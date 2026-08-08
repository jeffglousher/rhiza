use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Runtime kill switch that immediately stops learning and action use.
/// No restart or consensus configuration change is required.
#[derive(Clone)]
pub struct KillSwitch {
    active: Arc<AtomicBool>,
    reason: Arc<std::sync::Mutex<Option<String>>>,
}

impl KillSwitch {
    pub fn new() -> Self {
        Self {
            active: Arc::new(AtomicBool::new(false)),
            reason: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Activate the kill switch with a reason.
    pub fn activate(&self, reason: impl Into<String>) {
        self.active.store(true, Ordering::Release);
        if let Ok(mut r) = self.reason.lock() {
            *r = Some(reason.into());
        }
    }

    /// Deactivate the kill switch.
    pub fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
        if let Ok(mut r) = self.reason.lock() {
            *r = None;
        }
    }

    /// Check if the kill switch is active.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Get the reason for activation, if any.
    pub fn reason(&self) -> Option<String> {
        self.reason.lock().ok()?.clone()
    }
}

impl Default for KillSwitch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kill_switch_starts_inactive() {
        let ks = KillSwitch::new();
        assert!(!ks.is_active());
        assert!(ks.reason().is_none());
    }

    #[test]
    fn kill_switch_activate_deactivate() {
        let ks = KillSwitch::new();
        ks.activate("guardrail breach");
        assert!(ks.is_active());
        assert_eq!(ks.reason().as_deref(), Some("guardrail breach"));

        ks.deactivate();
        assert!(!ks.is_active());
        assert!(ks.reason().is_none());
    }

    #[test]
    fn kill_switch_clone_shares_state() {
        let ks1 = KillSwitch::new();
        let ks2 = ks1.clone();
        ks1.activate("test");
        assert!(ks2.is_active());
    }
}
