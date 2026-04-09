//! Port of `env/io_posix.{h,cc}` + `env/fs_posix.cc`.
//!
//! POSIX-backed concrete implementation of the Layer 0 [`FileSystem`] /
//! [`FsSequentialFile`] / [`FsRandomAccessFile`] / [`FsWritableFile`] /
//! [`FsDirectory`] traits. This is the filesystem a developer gets when
//! running on plain Linux/macOS — the Flink bridge replaces it with a
//! JNI-backed implementation (Layer 7).
//!
//! Only Unix targets are supported at Layer 2. Windows support would
//! need its own module using `std::os::windows::fs::FileExt` and is
//! deliberately deferred.
//!
//! ## Implementation notes
//!
//! - `PosixRandomAccessFile::read_at` uses
//!   [`std::os::unix::fs::FileExt::read_at`] (pread), which takes
//!   `&File`, letting the trait be `Sync` without locking.
//! - Writable files are opened in buffered mode; the engine adds its
//!   own buffering via `file::writable_file_writer::WritableFileWriter`,
//!   so the OS buffer is mostly redundant but harmless.
//! - `lock_file` uses `OpenOptions::create_new(true)` for an atomic
//!   "only one process holds it" primitive. Weak — if the process
//!   crashes the file persists until cleaned up by hand. Real RocksDB
//!   uses `fcntl` advisory locks; a follow-up layer can swap in a
//!   `nix` or `rustix`-based implementation.

use crate::core::status::{Result, Status};
use crate::core::types::Temperature;
use crate::env::file_system::{
    FileAttributes, FileLock, FileSystem, FsDirectory, FsRandomAccessFile, FsSequentialFile,
    FsWritableFile, IoOptions,
};
use crate::api::options::FileOptions;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Error mapping helpers
// ---------------------------------------------------------------------------

/// Turn an `std::io::Error` into a [`Status`] tagged with the file path
/// so errors tell you *which* file failed. This is the pattern upstream
/// RocksDB uses for every IO error surface.
fn io_status(path: &Path, err: std::io::Error) -> Status {
    use std::io::ErrorKind;
    let code_hint = match err.kind() {
        ErrorKind::NotFound => crate::core::status::SubCode::PathNotFound,
        ErrorKind::PermissionDenied | ErrorKind::Other => crate::core::status::SubCode::None,
        _ => crate::core::status::SubCode::None,
    };
    let mut status = Status::io_error(format!("{}: {}", path.display(), err));
    status.subcode = code_hint;
    status
}

// ---------------------------------------------------------------------------
// PosixSequentialFile
// ---------------------------------------------------------------------------

/// Sequential reader backed by a [`File`]. Not `Sync`, matches the
/// `FsSequentialFile` trait.
pub struct PosixSequentialFile {
    file: File,
    path: PathBuf,
}

impl FsSequentialFile for PosixSequentialFile {
    fn read(&mut self, buf: &mut [u8], _opts: &IoOptions) -> Result<usize> {
        self.file.read(buf).map_err(|e| io_status(&self.path, e))
    }

    fn skip(&mut self, n: u64, _opts: &IoOptions) -> Result<()> {
        self.file
            .seek(SeekFrom::Current(n as i64))
            .map(|_| ())
            .map_err(|e| io_status(&self.path, e))
    }
}

// ---------------------------------------------------------------------------
// PosixRandomAccessFile
// ---------------------------------------------------------------------------

/// Random-access reader backed by [`FileExt::read_at`]. `Sync` because
/// `read_at` takes `&File` — multiple threads can pread concurrently
/// without any locking on our side.
pub struct PosixRandomAccessFile {
    file: File,
    path: PathBuf,
    file_size: u64,
}

impl FsRandomAccessFile for PosixRandomAccessFile {
    fn read_at(&self, offset: u64, buf: &mut [u8], _opts: &IoOptions) -> Result<usize> {
        self.file
            .read_at(buf, offset)
            .map_err(|e| io_status(&self.path, e))
    }

    fn size(&self) -> Result<u64> {
        Ok(self.file_size)
    }
}

// ---------------------------------------------------------------------------
// PosixWritableFile
// ---------------------------------------------------------------------------

/// Writable file backed by [`File`]. Not `Sync` — the engine owns a
/// single writer per file at a time.
pub struct PosixWritableFile {
    file: Option<File>,
    path: PathBuf,
    file_size: u64,
}

impl FsWritableFile for PosixWritableFile {
    fn append(&mut self, data: &[u8], _opts: &IoOptions) -> Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| Status::io_error("append on closed file"))?;
        file.write_all(data).map_err(|e| io_status(&self.path, e))?;
        self.file_size += data.len() as u64;
        Ok(())
    }

    fn truncate(&mut self, size: u64, _opts: &IoOptions) -> Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| Status::io_error("truncate on closed file"))?;
        file.set_len(size).map_err(|e| io_status(&self.path, e))?;
        self.file_size = size;
        Ok(())
    }

    fn flush(&mut self, _opts: &IoOptions) -> Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| Status::io_error("flush on closed file"))?;
        file.flush().map_err(|e| io_status(&self.path, e))
    }

    fn sync(&mut self, _opts: &IoOptions) -> Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| Status::io_error("sync on closed file"))?;
        // fdatasync on Unix via sync_data
        file.sync_data().map_err(|e| io_status(&self.path, e))
    }

    fn fsync(&mut self, _opts: &IoOptions) -> Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| Status::io_error("fsync on closed file"))?;
        file.sync_all().map_err(|e| io_status(&self.path, e))
    }

    fn close(&mut self, _opts: &IoOptions) -> Result<()> {
        // Drop the File; any errors during close are surfaced by a
        // prior `sync` in the upstream contract.
        self.file = None;
        Ok(())
    }

    fn file_size(&self) -> u64 {
        self.file_size
    }
}

// ---------------------------------------------------------------------------
// PosixDirectory
// ---------------------------------------------------------------------------

/// Directory handle. On Unix, `fsync` on a directory ensures rename/
/// delete operations on files within it are durable.
pub struct PosixDirectory {
    file: Option<File>,
    path: PathBuf,
}

impl FsDirectory for PosixDirectory {
    fn fsync(&mut self, _opts: &IoOptions) -> Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| Status::io_error("fsync on closed directory"))?;
        file.sync_all().map_err(|e| io_status(&self.path, e))
    }

    fn close(&mut self, _opts: &IoOptions) -> Result<()> {
        self.file = None;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PosixFileLock
// ---------------------------------------------------------------------------

/// A "lock" implemented by atomically creating an empty LOCK file.
///
/// # Limitations
///
/// This is *not* a robust cross-process lock. If the owning process
/// crashes, the file is left behind and future opens see
/// `AlreadyExists`. Real RocksDB uses `fcntl` advisory locks. A
/// follow-up layer can swap in a `rustix::fs::flock` implementation.
pub struct PosixFileLock {
    path: PathBuf,
    /// Remember whether we created the file so `Drop` only deletes if
    /// we own it.
    owned: bool,
}

impl FileLock for PosixFileLock {}

impl Drop for PosixFileLock {
    fn drop(&mut self) {
        if self.owned {
            // Best-effort cleanup; don't panic on failure.
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

// ---------------------------------------------------------------------------
// PosixFileSystem
// ---------------------------------------------------------------------------

/// POSIX-backed [`FileSystem`] implementation. Cheap to construct — all
/// state lives inside each opened file. The engine instantiates one of
/// these per DB open.
#[derive(Debug, Default)]
pub struct PosixFileSystem {
    _private: (),
}

impl PosixFileSystem {
    /// Create a new POSIX filesystem.
    pub fn new() -> Self {
        Self::default()
    }
}

impl FileSystem for PosixFileSystem {
    fn name(&self) -> &'static str {
        "PosixFileSystem"
    }

    // -- File open --

    fn new_sequential_file(
        &self,
        path: &Path,
        _opts: &FileOptions,
    ) -> Result<Box<dyn FsSequentialFile>> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| io_status(path, e))?;
        Ok(Box::new(PosixSequentialFile {
            file,
            path: path.to_path_buf(),
        }))
    }

    fn new_random_access_file(
        &self,
        path: &Path,
        _opts: &FileOptions,
    ) -> Result<Box<dyn FsRandomAccessFile>> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| io_status(path, e))?;
        let file_size = file
            .metadata()
            .map_err(|e| io_status(path, e))?
            .len();
        Ok(Box::new(PosixRandomAccessFile {
            file,
            path: path.to_path_buf(),
            file_size,
        }))
    }

    fn new_writable_file(
        &self,
        path: &Path,
        _opts: &FileOptions,
    ) -> Result<Box<dyn FsWritableFile>> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .map_err(|e| io_status(path, e))?;
        Ok(Box::new(PosixWritableFile {
            file: Some(file),
            path: path.to_path_buf(),
            file_size: 0,
        }))
    }

    fn reopen_writable_file(
        &self,
        path: &Path,
        _opts: &FileOptions,
    ) -> Result<Box<dyn FsWritableFile>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| io_status(path, e))?;
        let file_size = file.metadata().map_err(|e| io_status(path, e))?.len();
        Ok(Box::new(PosixWritableFile {
            file: Some(file),
            path: path.to_path_buf(),
            file_size,
        }))
    }

    fn new_directory(&self, path: &Path) -> Result<Box<dyn FsDirectory>> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| io_status(path, e))?;
        Ok(Box::new(PosixDirectory {
            file: Some(file),
            path: path.to_path_buf(),
        }))
    }

    // -- Metadata --

    fn file_exists(&self, path: &Path) -> Result<bool> {
        match std::fs::metadata(path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(io_status(path, e)),
        }
    }

    fn get_children(&self, dir: &Path) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).map_err(|e| io_status(dir, e))? {
            let entry = entry.map_err(|e| io_status(dir, e))?;
            if let Some(name) = entry.file_name().to_str() {
                out.push(name.to_string());
            }
        }
        Ok(out)
    }

    fn get_children_with_attributes(&self, dir: &Path) -> Result<Vec<FileAttributes>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).map_err(|e| io_status(dir, e))? {
            let entry = entry.map_err(|e| io_status(dir, e))?;
            let name = match entry.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };
            let metadata = entry.metadata().map_err(|e| io_status(dir, e))?;
            out.push(FileAttributes {
                name,
                size_bytes: metadata.len(),
            });
        }
        Ok(out)
    }

    fn get_file_size(&self, path: &Path) -> Result<u64> {
        Ok(std::fs::metadata(path)
            .map_err(|e| io_status(path, e))?
            .len())
    }

    fn get_file_modification_time(&self, path: &Path) -> Result<u64> {
        let metadata = std::fs::metadata(path).map_err(|e| io_status(path, e))?;
        let modified = metadata.modified().map_err(|e| io_status(path, e))?;
        Ok(modified
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0))
    }

    fn is_directory(&self, path: &Path) -> Result<bool> {
        Ok(std::fs::metadata(path)
            .map_err(|e| io_status(path, e))?
            .is_dir())
    }

    // -- Mutation --

    fn create_dir(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir(dir).map_err(|e| io_status(dir, e))
    }

    fn create_dir_if_missing(&self, dir: &Path) -> Result<()> {
        match std::fs::create_dir_all(dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(e) => Err(io_status(dir, e)),
        }
    }

    fn delete_dir(&self, dir: &Path) -> Result<()> {
        std::fs::remove_dir(dir).map_err(|e| io_status(dir, e))
    }

    fn delete_file(&self, path: &Path) -> Result<()> {
        std::fs::remove_file(path).map_err(|e| io_status(path, e))
    }

    fn rename_file(&self, src: &Path, dst: &Path) -> Result<()> {
        std::fs::rename(src, dst).map_err(|e| io_status(src, e))
    }

    fn link_file(&self, src: &Path, dst: &Path) -> Result<()> {
        std::fs::hard_link(src, dst).map_err(|e| io_status(src, e))
    }

    // -- Locking --

    fn lock_file(&self, path: &Path) -> Result<Box<dyn FileLock>> {
        // Atomic create — fails with AlreadyExists if the file is
        // present, which catches both intra- and inter-process
        // double-locks. The PosixFileLock Drop impl removes the
        // file on release.
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| io_status(path, e))?;
        Ok(Box::new(PosixFileLock {
            path: path.to_path_buf(),
            owned: true,
        }))
    }

    fn unlock_file(&self, lock: Box<dyn FileLock>) -> Result<()> {
        // Drop runs PosixFileLock::drop which removes the lock file.
        drop(lock);
        Ok(())
    }

    // -- Misc --

    fn get_absolute_path(&self, db_path: &Path) -> Result<PathBuf> {
        db_path
            .canonicalize()
            .or_else(|_| std::env::current_dir().map(|cwd| cwd.join(db_path)))
            .map_err(|e| io_status(db_path, e))
    }

    fn get_temperature(&self, _path: &Path) -> Result<Temperature> {
        Ok(Temperature::Unknown)
    }
}

/// Helper to get the current wall-clock time in seconds since epoch,
/// used by tests.
#[allow(dead_code)]
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Create a unique temp directory under the process's temp root.
    fn temp_dir(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("st-rs-posix-{tag}-{pid}-{n}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cleanup(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn write_and_read_back_round_trip() {
        let fs = PosixFileSystem::new();
        let dir = temp_dir("roundtrip");
        let path = dir.join("file.txt");
        let opts = FileOptions::default();
        let io = IoOptions::default();

        {
            let mut w = fs.new_writable_file(&path, &opts).unwrap();
            w.append(b"hello world", &io).unwrap();
            w.flush(&io).unwrap();
            w.close(&io).unwrap();
        }

        assert_eq!(fs.get_file_size(&path).unwrap(), 11);
        assert!(fs.file_exists(&path).unwrap());

        let mut buf = [0u8; 11];
        let r = fs.new_random_access_file(&path, &opts).unwrap();
        let n = r.read_at(0, &mut buf, &io).unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf, b"hello world");

        cleanup(&dir);
    }

    #[test]
    fn sequential_read_advances_position() {
        let fs = PosixFileSystem::new();
        let dir = temp_dir("seq");
        let path = dir.join("seq.txt");
        let opts = FileOptions::default();
        let io = IoOptions::default();

        {
            let mut w = fs.new_writable_file(&path, &opts).unwrap();
            w.append(b"abcdef", &io).unwrap();
            w.close(&io).unwrap();
        }

        let mut seq = fs.new_sequential_file(&path, &opts).unwrap();
        let mut buf = [0u8; 3];
        assert_eq!(seq.read(&mut buf, &io).unwrap(), 3);
        assert_eq!(&buf, b"abc");
        assert_eq!(seq.read(&mut buf, &io).unwrap(), 3);
        assert_eq!(&buf, b"def");
        assert_eq!(seq.read(&mut buf, &io).unwrap(), 0); // EOF

        cleanup(&dir);
    }

    #[test]
    fn get_children_lists_files() {
        let fs = PosixFileSystem::new();
        let dir = temp_dir("children");
        let opts = FileOptions::default();
        let io = IoOptions::default();

        for name in ["a.txt", "b.txt", "c.txt"] {
            let mut w = fs.new_writable_file(&dir.join(name), &opts).unwrap();
            w.append(b"x", &io).unwrap();
            w.close(&io).unwrap();
        }

        let mut children = fs.get_children(&dir).unwrap();
        children.sort();
        assert_eq!(children, vec!["a.txt", "b.txt", "c.txt"]);

        cleanup(&dir);
    }

    #[test]
    fn rename_and_delete() {
        let fs = PosixFileSystem::new();
        let dir = temp_dir("rename");
        let src = dir.join("old.txt");
        let dst = dir.join("new.txt");
        let opts = FileOptions::default();
        let io = IoOptions::default();

        let mut w = fs.new_writable_file(&src, &opts).unwrap();
        w.append(b"payload", &io).unwrap();
        w.close(&io).unwrap();

        fs.rename_file(&src, &dst).unwrap();
        assert!(!fs.file_exists(&src).unwrap());
        assert!(fs.file_exists(&dst).unwrap());

        fs.delete_file(&dst).unwrap();
        assert!(!fs.file_exists(&dst).unwrap());

        cleanup(&dir);
    }

    #[test]
    fn lock_file_is_exclusive_within_process() {
        let fs = PosixFileSystem::new();
        let dir = temp_dir("lock");
        let path = dir.join("LOCK");

        let lock = fs.lock_file(&path).unwrap();
        assert!(
            fs.lock_file(&path).is_err(),
            "second lock_file should fail while the first is held"
        );
        drop(lock);
        // After drop, the file is gone and we can re-lock.
        let lock2 = fs.lock_file(&path).unwrap();
        drop(lock2);

        cleanup(&dir);
    }

    #[test]
    fn create_dir_if_missing_is_idempotent() {
        let fs = PosixFileSystem::new();
        let dir = temp_dir("mkdir");
        let sub = dir.join("a/b/c");
        fs.create_dir_if_missing(&sub).unwrap();
        fs.create_dir_if_missing(&sub).unwrap(); // second time OK
        assert!(fs.is_directory(&sub).unwrap());
        cleanup(&dir);
    }

    #[test]
    fn absolute_path_resolves() {
        let fs = PosixFileSystem::new();
        let dir = temp_dir("abs");
        let abs = fs.get_absolute_path(&dir).unwrap();
        assert!(abs.is_absolute());
        cleanup(&dir);
    }
}
