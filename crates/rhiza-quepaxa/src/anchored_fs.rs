#[cfg(any(
    target_os = "macos",
    all(
        any(target_os = "linux", target_os = "android"),
        target_pointer_width = "64"
    )
))]
mod anchored {
    use crate::{Error, Result};
    use std::{
        ffi::{CStr, CString},
        os::{
            fd::{AsRawFd, FromRawFd, RawFd},
            raw::{c_char, c_int, c_uchar, c_ushort, c_void},
            unix::fs::{MetadataExt, OpenOptionsExt},
        },
    };
    use std::{
        fs,
        io::{self, Read, Write},
        path::Path,
        sync::atomic::{AtomicU64, Ordering},
    };

    static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const O_DIRECTORY: c_int = 0o200000;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const O_CLOEXEC: c_int = 0o2000000;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const O_NOFOLLOW: c_int = 0o400000;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const O_APPEND: c_int = 0o2000;
    #[cfg(target_os = "macos")]
    const O_DIRECTORY: c_int = 0x0010_0000;
    #[cfg(target_os = "macos")]
    const O_CLOEXEC: c_int = 0x0100_0000;
    #[cfg(target_os = "macos")]
    const O_NOFOLLOW: c_int = 0x0000_0100;
    #[cfg(target_os = "macos")]
    const O_APPEND: c_int = 0x0000_0008;
    const O_RDONLY: c_int = 0;
    const O_WRONLY: c_int = 1;
    const O_RDWR: c_int = 2;
    const SEEK_SET: c_int = 0;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const O_CREAT: c_int = 0o100;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const O_EXCL: c_int = 0o200;
    #[cfg(target_os = "macos")]
    const O_CREAT: c_int = 0x0000_0200;
    #[cfg(target_os = "macos")]
    const O_EXCL: c_int = 0x0000_0800;

    unsafe extern "C" {
        fn openat(dirfd: c_int, path: *const c_char, flags: c_int, ...) -> c_int;
        fn renameat(
            olddirfd: c_int,
            oldpath: *const c_char,
            newdirfd: c_int,
            newpath: *const c_char,
        ) -> c_int;
        fn unlinkat(dirfd: c_int, path: *const c_char, flags: c_int) -> c_int;
        fn dup(fd: c_int) -> c_int;
        fn lseek(fd: c_int, offset: i64, whence: c_int) -> i64;
        fn fdopendir(fd: c_int) -> *mut c_void;
        fn readdir(dir: *mut c_void) -> *mut Dirent;
        fn closedir(dir: *mut c_void) -> c_int;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        fn __errno_location() -> *mut c_int;
        #[cfg(target_os = "macos")]
        fn __error() -> *mut c_int;
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[repr(C)]
    struct Dirent {
        d_ino: u64,
        d_off: i64,
        d_reclen: c_ushort,
        d_type: c_uchar,
        d_name: [c_char; 256],
    }

    #[cfg(target_os = "macos")]
    #[repr(C)]
    struct Dirent {
        d_ino: u64,
        d_seekoff: u64,
        d_reclen: c_ushort,
        d_namlen: c_ushort,
        d_type: c_uchar,
        d_name: [c_char; 1024],
    }

    #[derive(Debug)]
    pub(crate) struct AnchoredDir {
        directory: fs::File,
        device: u64,
        inode: u64,
    }

    impl AnchoredDir {
        pub(crate) fn open(path: &Path) -> Result<Self> {
            let mut options = fs::OpenOptions::new();
            options
                .read(true)
                .custom_flags(O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
            let directory = options
                .open(path)
                .map_err(|error| Error::Io(error.to_string()))?;
            let metadata = directory
                .metadata()
                .map_err(|error| Error::Io(error.to_string()))?;
            if !metadata.is_dir() {
                return Err(Error::Decode(
                    "recorder root must be a real directory".into(),
                ));
            }
            Ok(Self {
                directory,
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }

        pub(crate) fn verify_path(&self, path: &Path) -> Result<()> {
            self.verify()?;
            let metadata =
                fs::symlink_metadata(path).map_err(|error| Error::Io(error.to_string()))?;
            if metadata.file_type().is_symlink()
                || !metadata.is_dir()
                || metadata.dev() != self.device
                || metadata.ino() != self.inode
            {
                return Err(Error::Decode(
                    "recorder root path no longer names its anchored directory".into(),
                ));
            }
            Ok(())
        }

        pub(crate) fn read(&self, name: &str, maximum: usize, label: &str) -> Result<Vec<u8>> {
            self.verify()?;
            let mut file = self
                .open_file(name, O_RDONLY)
                .map_err(|error| match error {
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
            if before.dev() != after.dev()
                || before.ino() != after.ino()
                || before.len() != after.len()
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
            match self.open_file(name, O_RDONLY) {
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
            let file =
                self.open_file_flags(name, O_WRONLY | O_APPEND | O_CLOEXEC | O_NOFOLLOW, 0)?;
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
            let file = self.open_append(name)?;
            file.set_len(len)
                .and_then(|_| file.sync_all())
                .map_err(|error| Error::Io(error.to_string()))?;
            #[cfg(test)]
            crate::record_file_sync();
            self.sync()
        }

        pub(crate) fn create_empty_if_missing(&self, name: &str) -> Result<bool> {
            match self.open_file_flags(
                name,
                O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                0o600,
            ) {
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
            match self.open_file(name, O_RDONLY) {
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
            match self.open_file_flags(
                ".recorder.lock",
                O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                0o600,
            ) {
                Ok(file) => Ok(file),
                Err(Error::Io(message)) if is_already_exists_message(&message) => self.open_lock(),
                Err(error) => Err(error),
            }
        }

        pub(crate) fn open_lock(&self) -> Result<fs::File> {
            let file = self.open_file(".recorder.lock", O_RDWR)?;
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
            let from = component(from)?;
            let to = component(to)?;
            // SAFETY: names are NUL-terminated single components and both descriptors are held.
            let status = unsafe {
                renameat(
                    self.directory.as_raw_fd(),
                    from.as_ptr(),
                    self.directory.as_raw_fd(),
                    to.as_ptr(),
                )
            };
            if status != 0 {
                return Err(Error::Io(io::Error::last_os_error().to_string()));
            }
            self.sync()
        }

        pub(crate) fn remove(&self, name: &str) -> Result<()> {
            self.verify()?;
            let name = component(name)?;
            // SAFETY: name is a NUL-terminated single component and descriptor is held.
            let status = unsafe { unlinkat(self.directory.as_raw_fd(), name.as_ptr(), 0) };
            if status != 0 {
                return Err(Error::Io(io::Error::last_os_error().to_string()));
            }
            self.sync()
        }

        pub(crate) fn sync(&self) -> Result<()> {
            self.verify()?;
            self.directory
                .sync_all()
                .map_err(|error| Error::Io(error.to_string()))?;
            #[cfg(test)]
            crate::record_directory_sync();
            Ok(())
        }

        pub(crate) fn list(&self) -> Result<Vec<String>> {
            self.verify()?;
            // SAFETY: dup returns a separately-owned descriptor for fdopendir.
            let duplicate = unsafe { dup(self.directory.as_raw_fd()) };
            if duplicate < 0 {
                return Err(Error::Io(io::Error::last_os_error().to_string()));
            }
            // `dup` shares the directory offset. Rewind every scan so a
            // previous orphan-GC pass cannot hide later entries.
            if unsafe { lseek(duplicate, 0, SEEK_SET) } < 0 {
                drop(unsafe { fs::File::from_raw_fd(duplicate) });
                return Err(Error::Io(io::Error::last_os_error().to_string()));
            }
            // SAFETY: duplicate is valid and ownership transfers to the DIR stream.
            let stream = unsafe { fdopendir(duplicate) };
            if stream.is_null() {
                // SAFETY: fdopendir did not take ownership on failure.
                drop(unsafe { fs::File::from_raw_fd(duplicate) });
                return Err(Error::Io(io::Error::last_os_error().to_string()));
            }
            let mut names = Vec::new();
            loop {
                // SAFETY: the platform errno accessor returns thread-local writable storage.
                unsafe { *errno_pointer() = 0 };
                // SAFETY: stream remains valid until closed below.
                let entry = unsafe { readdir(stream) };
                if entry.is_null() {
                    // SAFETY: errno storage remains valid for this thread.
                    let error = unsafe { *errno_pointer() };
                    if error != 0 {
                        // SAFETY: stream is still valid and closes its duplicate descriptor.
                        unsafe { closedir(stream) };
                        return Err(Error::Io(io::Error::from_raw_os_error(error).to_string()));
                    }
                    break;
                }
                // SAFETY: d_name is NUL-terminated by readdir.
                let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
                let name = name.to_string_lossy();
                if name != "." && name != ".." {
                    names.push(name.into_owned());
                }
            }
            // SAFETY: stream is valid and closes its duplicate descriptor.
            if unsafe { closedir(stream) } != 0 {
                return Err(Error::Io(io::Error::last_os_error().to_string()));
            }
            names.sort();
            Ok(names)
        }

        fn open_file(&self, name: &str, access: c_int) -> Result<fs::File> {
            self.open_file_flags(name, access | O_CLOEXEC | O_NOFOLLOW, 0)
        }

        fn create_temp_file(&self, name: &str) -> Result<(String, fs::File)> {
            for _ in 0..16 {
                let mut nonce = [0u8; 16];
                getrandom::fill(&mut nonce).map_err(|error| Error::Io(error.to_string()))?;
                let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
                let temp = format!(".{name}.tmp-{}-{counter:016x}", hex(&nonce));
                match self.open_file_flags(
                    &temp,
                    O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                    0o600,
                ) {
                    Ok(file) => return Ok((temp, file)),
                    Err(Error::Io(message)) if is_already_exists_message(&message) => continue,
                    Err(error) => return Err(error),
                }
            }
            Err(Error::Io(
                "could not create unique anchored temporary file".into(),
            ))
        }

        fn open_file_flags(&self, name: &str, flags: c_int, mode: c_int) -> Result<fs::File> {
            self.verify()?;
            let name = component(name)?;
            // SAFETY: name is a NUL-terminated single component and descriptor is held.
            let descriptor =
                unsafe { openat(self.directory.as_raw_fd(), name.as_ptr(), flags, mode) };
            if descriptor < 0 {
                return Err(Error::Io(io::Error::last_os_error().to_string()));
            }
            // SAFETY: openat returned a newly-owned descriptor.
            Ok(unsafe { fs::File::from_raw_fd(descriptor as RawFd) })
        }

        fn verify(&self) -> Result<()> {
            let metadata = self
                .directory
                .metadata()
                .map_err(|error| Error::Io(error.to_string()))?;
            if !metadata.is_dir() || metadata.dev() != self.device || metadata.ino() != self.inode {
                return Err(Error::Decode(
                    "recorder root directory anchor changed".into(),
                ));
            }
            Ok(())
        }
    }

    fn component(name: &str) -> Result<CString> {
        if name.is_empty() || name == "." || name == ".." || name.as_bytes().contains(&b'/') {
            return Err(Error::Decode("invalid anchored path component".into()));
        }
        CString::new(name.as_bytes())
            .map_err(|_| Error::Decode("NUL in anchored path component".into()))
    }

    fn is_not_found_message(message: &str) -> bool {
        message.contains("No such file or directory") || message.contains("os error 2")
    }

    fn is_already_exists_message(message: &str) -> bool {
        message.contains("File exists") || message.contains("os error 17")
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    unsafe fn errno_pointer() -> *mut c_int {
        // SAFETY: delegated to the platform C runtime.
        unsafe { __errno_location() }
    }

    #[cfg(target_os = "macos")]
    unsafe fn errno_pointer() -> *mut c_int {
        // SAFETY: delegated to the platform C runtime.
        unsafe { __error() }
    }
}

#[cfg(any(
    target_os = "macos",
    all(
        any(target_os = "linux", target_os = "android"),
        target_pointer_width = "64"
    )
))]
pub(crate) use anchored::AnchoredDir;

#[cfg(not(any(
    target_os = "macos",
    all(
        any(target_os = "linux", target_os = "android"),
        target_pointer_width = "64"
    )
)))]
#[derive(Debug)]
pub(crate) struct AnchoredDir;

#[cfg(not(any(
    target_os = "macos",
    all(
        any(target_os = "linux", target_os = "android"),
        target_pointer_width = "64"
    )
)))]
impl AnchoredDir {
    pub(crate) fn open(_path: &std::path::Path) -> Result<Self> {
        unsupported()
    }

    pub(crate) fn verify_path(&self, _path: &std::path::Path) -> Result<()> {
        unsupported()
    }

    pub(crate) fn open_lock_or_create(&self) -> Result<std::fs::File> {
        unsupported()
    }

    pub(crate) fn open_lock(&self) -> Result<std::fs::File> {
        unsupported()
    }

    pub(crate) fn read(&self, _name: &str, _maximum: usize, _label: &str) -> Result<Vec<u8>> {
        unsupported()
    }

    pub(crate) fn read_optional(
        &self,
        _name: &str,
        _maximum: usize,
        _label: &str,
    ) -> Result<Option<Vec<u8>>> {
        unsupported()
    }

    pub(crate) fn atomic_write(&self, _name: &str, _bytes: &[u8]) -> Result<()> {
        unsupported()
    }

    pub(crate) fn create_empty_if_missing(&self, _name: &str) -> Result<bool> {
        unsupported()
    }

    pub(crate) fn exists(&self, _name: &str) -> Result<bool> {
        unsupported()
    }

    pub(crate) fn open_append(&self, _name: &str) -> Result<std::fs::File> {
        unsupported()
    }

    pub(crate) fn truncate(&self, _name: &str, _len: u64) -> Result<()> {
        unsupported()
    }

    pub(crate) fn rename(&self, _from: &str, _to: &str) -> Result<()> {
        unsupported()
    }

    pub(crate) fn remove(&self, _name: &str) -> Result<()> {
        unsupported()
    }

    pub(crate) fn list(&self) -> Result<Vec<String>> {
        unsupported()
    }

    pub(crate) fn sync(&self) -> Result<()> {
        unsupported()
    }
}

#[cfg(not(any(
    target_os = "macos",
    all(
        any(target_os = "linux", target_os = "android"),
        target_pointer_width = "64"
    )
)))]
fn unsupported<T>() -> Result<T> {
    Err(Error::Decode(
        "anchored directory operations are unsupported on this platform".into(),
    ))
}
