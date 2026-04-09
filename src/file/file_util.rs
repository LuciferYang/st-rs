//! Port of `file/file_util.{h,cc}`.
//!
//! High-level helpers layered on top of [`FileSystem`]. These are
//! convenience wrappers used throughout the engine — copy a file,
//! synchronously delete it, fsync a parent directory after a rename,
//! etc.
//!
//! At Layer 2 we port only the helpers that don't pull in higher-layer
//! dependencies. Things like `DeleteDBFile` (which talks to
//! `SstFileManager` for rate-limited deletes) and `GenerateOneFileChecksum`
//! (which needs a live checksum implementation) belong in later layers.

use crate::core::status::Result;
use crate::env::file_system::{FileSystem, IoOptions};
use crate::file::sequence_file_reader::SequentialFileReader;
use crate::file::writable_file_writer::WritableFileWriter;
use std::path::Path;

/// Copy `src` to `dst`, creating the destination. Used during
/// checkpoint and backup operations in the real engine.
pub fn copy_file(fs: &dyn FileSystem, src: &Path, dst: &Path) -> Result<()> {
    let src_file = fs.new_sequential_file(src, &Default::default())?;
    let mut reader = SequentialFileReader::new(src_file, src.display().to_string());

    let dst_file = fs.new_writable_file(dst, &Default::default())?;
    let mut writer = WritableFileWriter::new(dst_file);

    let io = IoOptions::default();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf, &io)?;
        if n == 0 {
            break;
        }
        writer.append(&buf[..n], &io)?;
    }
    writer.close(&io)?;
    Ok(())
}

/// `fsync` the directory containing `path`. Needed on some filesystems
/// (e.g. ext4 without `data=journal`) to guarantee that a just-created
/// file survives power loss. Upstream calls this `SyncDir`.
pub fn sync_directory(fs: &dyn FileSystem, dir: &Path) -> Result<()> {
    let mut d = fs.new_directory(dir)?;
    d.fsync(&IoOptions::default())
}

/// Best-effort delete. Swallows "not found" errors, propagates everything
/// else. Upstream's `DeleteDBFile` equivalent calls through
/// `SstFileManager` for rate limiting — we don't have one yet.
pub fn delete_if_exists(fs: &dyn FileSystem, path: &Path) -> Result<()> {
    if fs.file_exists(path)? {
        fs.delete_file(path)?;
    }
    Ok(())
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use crate::env::posix::PosixFileSystem;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("st-rs-fileutil-{tag}-{pid}-{n}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn copy_file_round_trip() {
        let fs = PosixFileSystem::new();
        let dir = temp_dir("copy");
        let src = dir.join("src.txt");
        let dst = dir.join("dst.txt");
        let io = IoOptions::default();

        {
            let mut w = fs
                .new_writable_file(&src, &Default::default())
                .unwrap();
            w.append(b"copy me please", &io).unwrap();
            w.close(&io).unwrap();
        }

        copy_file(&fs, &src, &dst).unwrap();

        // Read dst back and compare.
        let r = fs
            .new_random_access_file(&dst, &Default::default())
            .unwrap();
        let mut buf = vec![0u8; 14];
        let n = r.read_at(0, &mut buf, &io).unwrap();
        assert_eq!(n, 14);
        assert_eq!(&buf, b"copy me please");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_if_exists_is_idempotent() {
        let fs = PosixFileSystem::new();
        let dir = temp_dir("delete");
        let path = dir.join("f.txt");
        // Doesn't exist → no-op.
        delete_if_exists(&fs, &path).unwrap();
        // Create then delete.
        let mut w = fs
            .new_writable_file(&path, &Default::default())
            .unwrap();
        w.append(b"x", &IoOptions::default()).unwrap();
        w.close(&IoOptions::default()).unwrap();
        assert!(fs.file_exists(&path).unwrap());
        delete_if_exists(&fs, &path).unwrap();
        assert!(!fs.file_exists(&path).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
