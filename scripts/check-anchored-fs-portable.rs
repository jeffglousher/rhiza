#![allow(dead_code)]

#[derive(Debug)]
pub enum Error {
    Decode(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[path = "../crates/rhiza-quepaxa/src/anchored_fs.rs"]
mod anchored_fs;

pub fn portable_anchor_size() -> usize {
    std::mem::size_of::<anchored_fs::AnchoredDir>()
}
