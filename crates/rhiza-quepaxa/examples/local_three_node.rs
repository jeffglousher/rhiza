use std::{fs, time::SystemTime};

use rhiza_quepaxa::{Command, CommandKind, RecorderRpcContext, ThreeNodeConsensus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let base = std::env::temp_dir().join(format!("rhiza-quepaxa-{suffix}"));
    let roots = [base.join("n1"), base.join("n2"), base.join("n3")];

    let consensus = ThreeNodeConsensus::new("example", "n1", 1, 1, roots)?;
    let entry = consensus.propose(
        RecorderRpcContext::default_timeout(),
        Command::new(CommandKind::Deterministic, b"create user 42".to_vec()),
    )?;

    println!("decided slot {} with hash {:?}", entry.index, entry.hash);
    drop(consensus);
    fs::remove_dir_all(base)?;
    Ok(())
}
