//! Windows lab implementation of [`super::AnchoredDir`].
//!
//! Unix uses `openat` / `renameat` / `unlinkat` against a held directory
//! descriptor. Windows has no equivalent in stable `std`, so this path:
//!
//! - stores the canonical recorder root
//! - opens children only after a single-component name check
//! - refuses symlink children
//!
//! Child opens are path-relative. That is weaker than Unix `openat` against
//! rename races. It is enough for Taldra's single-process comparative lab.

use crate::{Error, Result};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(crate) struct AnchoredDir {
    root: PathBuf,
    canonical: PathBuf,
}

impl AnchoredDir {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        refuse_non_directory(path, "recorder root")?;
        let canonical = fs::canonicalize(path)
            .map_err(|error| Error::Io(format!("canonicalize recorder root: {error}")))?;
        Ok(Self {
            root: path.to_path_buf(),
            canonical,
        })
    }

    pub(crate) fn verify_path(&self, path: &Path) -> Result<()> {
        self.verify()?;
        refuse_non_directory(path, "recorder root path")?;
        let canonical = fs::canonicalize(path)
            .map_err(|error| Error::Io(format!("canonicalize recorder root path: {error}")))?;
        if canonical != self.canonical {
            return Err(Error::Decode(
                "recorder root path no longer names its anchored directory".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn read(&self, name: &str, maximum: usize, label: &str) -> Result<Vec<u8>> {
        self.verify()?;
        let mut file = self.open_file(name, Access::Read).map_err(|error| match error {
            Error::Io(message) if is_not_found_message(&message) => {
                Error::Decode(format!("recorder is missing {label}"))
            }
            other => other,
        })?;
        let before = file
            .metadata()
            .map_err(|error| Error::Io(error.to_string()))?;
        if !before.is_file() || before.len() > maximum as u64 {
            return Err(Error::Decode(format!(
                "recorder {label} must be a bounded regular file"
            )));
        }
        let mut bytes = Vec::with_capacity(before.len() as usize);
        Read::by_ref(&mut file)
            .take(maximum as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| Error::Io(error.to_string()))?;
        let after = file
            .metadata()
            .map_err(|error| Error::Io(error.to_string()))?;
        if before.len() != after.len()
            || bytes.len() as u64 != before.len()
            || bytes.len() > maximum
        {
            return Err(Error::Decode(format!(
                "recorder {label} changed during anchored read"
            )));
        }
        Ok(bytes)
    }

    pub(crate) fn read_optional(
        &self,
        name: &str,
        maximum: usize,
        label: &str,
    ) -> Result<Option<Vec<u8>>> {
        match self.open_file(name, Access::Read) {
            Ok(file) => {
                drop(file);
                self.read(name, maximum, label).map(Some)
            }
            Err(Error::Io(message)) if is_not_found_message(&message) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn atomic_write(&self, name: &str, bytes: &[u8]) -> Result<()> {
        self.verify()?;
        let (temp, mut file) = self.create_temp_file(name)?;
        let result = (|| -> Result<()> {
            file.write_all(bytes)
                .and_then(|_| file.sync_all())
                .map_err(|error| Error::Io(error.to_string()))?;
            #[cfg(test)]
            crate::record_file_sync();
            drop(file);
            self.rename(&temp, name)
        })();
        if result.is_err() {
            let _ = self.remove(&temp);
        }
        result
    }

    pub(crate) fn open_append(&self, name: &str) -> Result<fs::File> {
        let file = self.open_file(name, Access::Append)?;
        if !file
            .metadata()
            .map_err(|error| Error::Io(error.to_string()))?
            .is_file()
        {
            return Err(Error::Decode(
                "anchored append target must be a regular file".into(),
            ));
        }
        Ok(file)
    }

    pub(crate) fn truncate(&self, name: &str, len: u64) -> Result<()> {
        // Windows `SetEndOfFile` requires `FILE_WRITE_DATA`. An append-only
        // handle (`FILE_APPEND_DATA`) returns Access Denied. Unix `ftruncate`
        // on an `O_APPEND` fd does not have that restriction.
        let file = self.open_file(name, Access::ReadWrite)?;
        if !file
            .metadata()
            .map_err(|error| Error::Io(error.to_string()))?
            .is_file()
        {
            return Err(Error::Decode(
                "anchored truncate target must be a regular file".into(),
            ));
        }
        file.set_len(len)
            .and_then(|_| file.sync_all())
            .map_err(|error| Error::Io(error.to_string()))?;
        #[cfg(test)]
        crate::record_file_sync();
        drop(file);
        self.sync()
    }

    pub(crate) fn create_empty_if_missing(&self, name: &str) -> Result<bool> {
        match self.open_file(name, Access::CreateNew) {
            Ok(file) => {
                file.sync_all()
                    .map_err(|error| Error::Io(error.to_string()))?;
                #[cfg(test)]
                crate::record_file_sync();
                self.sync()?;
                Ok(true)
            }
            Err(Error::Io(message)) if is_already_exists_message(&message) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn exists(&self, name: &str) -> Result<bool> {
        match self.open_file(name, Access::Read) {
            Ok(file) => {
                if !file
                    .metadata()
                    .map_err(|error| Error::Io(error.to_string()))?
                    .is_file()
                {
                    return Err(Error::Decode(
                        "anchored target must be a regular file".into(),
                    ));
                }
                Ok(true)
            }
            Err(Error::Io(message)) if is_not_found_message(&message) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn open_lock_or_create(&self) -> Result<fs::File> {
        match self.open_file(".recorder.lock", Access::CreateNewReadWrite) {
            Ok(file) => Ok(file),
            Err(Error::Io(message)) if is_already_exists_message(&message) => self.open_lock(),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn open_lock(&self) -> Result<fs::File> {
        let file = self.open_file(".recorder.lock", Access::ReadWrite)?;
        if !file
            .metadata()
            .map_err(|error| Error::Io(error.to_string()))?
            .is_file()
        {
            return Err(Error::Decode(
                "recorder root lock must be an existing regular file".into(),
            ));
        }
        Ok(file)
    }

    pub(crate) fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.verify()?;
        let from_path = self.child_path(from)?;
        let to_path = self.child_path(to)?;
        refuse_symlink(&from_path)?;
        match fs::symlink_metadata(&to_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(Error::Decode(
                    "anchored rename destination must not be a symlink".into(),
                ));
            }
            Ok(_) | Err(_) => {}
        }
        fs::rename(&from_path, &to_path).map_err(|error| Error::Io(error.to_string()))?;
        self.sync()
    }

    pub(crate) fn remove(&self, name: &str) -> Result<()> {
        self.verify()?;
        let path = self.child_path(name)?;
        refuse_symlink(&path)?;
        fs::remove_file(&path).map_err(|error| Error::Io(error.to_string()))?;
        self.sync()
    }

    pub(crate) fn sync(&self) -> Result<()> {
        self.verify()?;
        #[cfg(test)]
        crate::record_directory_sync();
        Ok(())
    }

    pub(crate) fn list(&self) -> Result<Vec<String>> {
        self.verify()?;
        let mut names = Vec::new();
        let entries = fs::read_dir(&self.root).map_err(|error| Error::Io(error.to_string()))?;
        for entry in entries {
            let entry = entry.map_err(|error| Error::Io(error.to_string()))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(Error::Decode(
                    "anchored directory entry is not valid UTF-8".into(),
                ));
            };
            if name != "." && name != ".." {
                names.push(name.to_owned());
            }
        }
        names.sort();
        Ok(names)
    }

    fn open_file(&self, name: &str, access: Access) -> Result<fs::File> {
        self.verify()?;
        let path = self.child_path(name)?;
        if access != Access::CreateNew && access != Access::CreateNewReadWrite {
            refuse_symlink(&path)?;
        }
        let mut options = OpenOptions::new();
        match access {
            Access::Read => {
                options.read(true);
            }
            Access::ReadWrite => {
                options.read(true).write(true);
            }
            Access::Append => {
                options.write(true).append(true);
            }
            Access::CreateNew => {
                options.write(true).create_new(true);
            }
            Access::CreateNewReadWrite => {
                options.read(true).write(true).create_new(true);
            }
        }
        options
            .open(&path)
            .map_err(|error| Error::Io(error.to_string()))
    }

    fn create_temp_file(&self, name: &str) -> Result<(String, fs::File)> {
        component(name)?;
        for _ in 0..16 {
            let mut nonce = [0u8; 16];
            getrandom::fill(&mut nonce).map_err(|error| Error::Io(error.to_string()))?;
            let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let temp = format!(".{name}.tmp-{}-{counter:016x}", hex(&nonce));
            match self.open_file(&temp, Access::CreateNew) {
                Ok(file) => return Ok((temp, file)),
                Err(Error::Io(message)) if is_already_exists_message(&message) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::Io(
            "could not create unique anchored temporary file".into(),
        ))
    }

    fn child_path(&self, name: &str) -> Result<PathBuf> {
        Ok(self.root.join(component(name)?))
    }

    fn verify(&self) -> Result<()> {
        refuse_non_directory(&self.root, "recorder root directory anchor")?;
        let canonical = fs::canonicalize(&self.root).map_err(|error| {
            Error::Io(format!("canonicalize recorder root directory anchor: {error}"))
        })?;
        if canonical != self.canonical {
            return Err(Error::Decode(
                "recorder root directory anchor changed".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Access {
    Read,
    ReadWrite,
    Append,
    CreateNew,
    CreateNewReadWrite,
}

fn refuse_non_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| Error::Io(error.to_string()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::Decode(format!("{label} must be a real directory")));
    }
    Ok(())
}

fn component(name: &str) -> Result<&str> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
        || name.contains(':')
    {
        return Err(Error::Decode("invalid anchored path component".into()));
    }
    Ok(name)
}

fn refuse_symlink(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(Error::Decode(
            "anchored target must not be a symlink".into(),
        )),
        Ok(_) | Err(_) => Ok(()),
    }
}

fn is_not_found_message(message: &str) -> bool {
    message.contains("os error 2")
        || message.contains("cannot find the file")
        || message.contains("cannot find the path")
        || message.contains("No such file or directory")
}

fn is_already_exists_message(message: &str) -> bool {
    message.contains("os error 80")
        || message.contains("os error 183")
        || message.contains("already exists")
        || message.contains("File exists")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
