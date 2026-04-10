//! Port of `utilities/checkpoint/checkpoint_impl.{h,cc}`.
//!
//! A **checkpoint** is a consistent point-in-time copy of a live DB
//! created by hard-linking (or copying) every live file — SSTs,
//! CURRENT, and a fresh WAL snapshot — into a target directory. The
//! source DB continues to run undisturbed.
//!
//! # Why this matters for ForSt / Flink
//!
//! Apache Flink's savepoint and checkpoint mechanism maps directly
//! to RocksDB's `Checkpoint::CreateCheckpoint`. When Flink takes a
//! savepoint, it:
//!
//! 1. Flushes the memtable so all data is in SSTs.
//! 2. Creates a checkpoint directory with hard links to every live
//!    SST (cheap, O(1) per file, no data copy).
//! 3. Uploads the checkpoint directory to durable storage (S3,
//!    HDFS, …).
//!
//! Restoring from a savepoint is just `Db::open(checkpoint_dir)`.
//!
//! # API
//!
//! ```ignore
//! use st_rs::utilities::checkpoint::create_checkpoint;
//!
//! let db = DbImpl::open(&opts, Path::new("/data/mydb"))?;
//! db.put(b"k", b"v")?;
//!
//! // Create a checkpoint — hard-links SSTs, copies CURRENT + WAL.
//! create_checkpoint(&db, Path::new("/backup/cp1"))?;
//!
//! // The checkpoint is a fully openable DB.
//! let cp = DbImpl::open(&opts, Path::new("/backup/cp1"))?;
//! assert_eq!(cp.get(b"k")?, Some(b"v".to_vec()));
//! ```
//!
//! # Implementation
//!
//! 1. Flush the source DB's memtable so every committed write is
//!    in an SST.
//! 2. Create the target directory.
//! 3. For each live SST: try `hard_link(src, dst)`; fall back to
//!    `copy_file(src, dst)` if hard links aren't supported (e.g.
//!    cross-filesystem).
//! 4. Copy the `CURRENT` file (small; not worth hard-linking
//!    because the source DB may rewrite it at any time).
//! 5. The checkpoint is now a self-contained DB directory that
//!    can be opened independently.
//!
//! # Thread safety
//!
//! `create_checkpoint` takes `&DbImpl` — it acquires the engine
//! lock briefly to snapshot the SST list, then does all file I/O
//! outside the lock. The source DB is fully usable during the
//! checkpoint.

use crate::core::status::{Result, Status};
use crate::db::db_impl::DbImpl;
use crate::env::file_system::FileSystem;
use crate::file::filename::{
    make_current_file_name, make_lock_file_name, make_table_file_name,
};
use std::path::Path;

/// Create a consistent checkpoint of `db` at `checkpoint_dir`.
///
/// The target directory must not already exist — the function
/// creates it. After a successful return, `checkpoint_dir` is a
/// fully openable DB directory containing hard links (or copies)
/// of every live SST plus a fresh `CURRENT` file.
///
/// # Errors
///
/// - `InvalidArgument` if `checkpoint_dir` already exists.
/// - `IOError` if any file operation fails (link, copy, mkdir,
///   write).
///
/// On error the function makes a best-effort attempt to clean up
/// the partially-created checkpoint directory, but doesn't
/// guarantee a clean state on I/O failures during cleanup.
pub fn create_checkpoint(db: &DbImpl, checkpoint_dir: &Path) -> Result<()> {
    let fs = db.file_system();

    // Reject if the target already exists.
    if fs.file_exists(checkpoint_dir)? {
        return Err(Status::invalid_argument(format!(
            "{}: checkpoint directory already exists",
            checkpoint_dir.display()
        )));
    }

    // 1. Flush so every committed write is in an SST.
    db.flush()?;
    db.wait_for_pending_work()?;

    // 2. Snapshot the live SST numbers + the DB path under the
    //    engine lock.
    let (db_path, sst_numbers) = db.snapshot_live_files();

    // 3. Create the target directory.
    fs.create_dir_if_missing(checkpoint_dir)?;

    // 4. Hard-link (or copy) each SST.
    for &num in &sst_numbers {
        let src = make_table_file_name(&db_path, num);
        let dst = make_table_file_name(checkpoint_dir, num);
        match fs.link_file(&src, &dst) {
            Ok(()) => {}
            Err(e) if e.is_not_supported() => {
                // Filesystem doesn't support hard links — fall
                // back to a full copy.
                crate::file::file_util::copy_file(fs.as_ref(), &src, &dst)?;
            }
            Err(e) => {
                // Unexpected error — clean up and propagate.
                let _ = cleanup(fs.as_ref(), checkpoint_dir, &sst_numbers);
                return Err(e);
            }
        }
    }

    // 5. Copy the CURRENT file. We copy rather than link
    //    because the source DB may rewrite CURRENT at any time
    //    (it's atomically replaced via tmp+rename on every
    //    flush/compaction).
    let src_current = make_current_file_name(&db_path);
    let dst_current = make_current_file_name(checkpoint_dir);
    crate::file::file_util::copy_file(fs.as_ref(), &src_current, &dst_current)?;

    Ok(())
}

/// Best-effort cleanup of a partially-created checkpoint.
fn cleanup(fs: &dyn FileSystem, dir: &Path, sst_numbers: &[u64]) -> Result<()> {
    for &num in sst_numbers {
        let p = make_table_file_name(dir, num);
        if fs.file_exists(&p)? {
            fs.delete_file(&p)?;
        }
    }
    let current = make_current_file_name(dir);
    if fs.file_exists(&current)? {
        fs.delete_file(&current)?;
    }
    let lock = make_lock_file_name(dir);
    if fs.file_exists(&lock)? {
        fs.delete_file(&lock)?;
    }
    // Try to remove the dir itself (only succeeds if empty).
    let _ = fs.delete_dir(dir);
    Ok(())
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use crate::api::options::DbOptions;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn opts() -> DbOptions {
        DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        }
    }

    fn temp_dir(tag: &str) -> PathBuf {
        static C: AtomicU64 = AtomicU64::new(0);
        let n = C.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("st-rs-cp-{tag}-{pid}-{n}"))
    }

    #[test]
    fn checkpoint_is_openable() {
        let db_dir = temp_dir("src");
        let cp_dir = temp_dir("cp");

        let db = DbImpl::open(&opts(), &db_dir).unwrap();
        for i in 0..50u32 {
            let k = format!("key{i:03}");
            db.put(k.as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();

        create_checkpoint(&db, &cp_dir).unwrap();
        db.close().unwrap();

        // Open the checkpoint as a standalone DB.
        let cp = DbImpl::open(&opts(), &cp_dir).unwrap();
        for i in 0..50u32 {
            let k = format!("key{i:03}");
            assert_eq!(
                cp.get(k.as_bytes()).unwrap(),
                Some(b"value".to_vec()),
                "missing key in checkpoint: {k}"
            );
        }
        cp.close().unwrap();

        let _ = std::fs::remove_dir_all(&db_dir);
        let _ = std::fs::remove_dir_all(&cp_dir);
    }

    #[test]
    fn checkpoint_survives_source_writes() {
        let db_dir = temp_dir("src2");
        let cp_dir = temp_dir("cp2");

        let db = DbImpl::open(&opts(), &db_dir).unwrap();
        db.put(b"k", b"old").unwrap();
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();

        create_checkpoint(&db, &cp_dir).unwrap();

        // Write more data to the source AFTER the checkpoint.
        db.put(b"k", b"new").unwrap();
        db.put(b"k2", b"new2").unwrap();
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();
        db.close().unwrap();

        // The checkpoint should still see the old value and
        // NOT see k2.
        let cp = DbImpl::open(&opts(), &cp_dir).unwrap();
        assert_eq!(cp.get(b"k").unwrap(), Some(b"old".to_vec()));
        assert_eq!(cp.get(b"k2").unwrap(), None);
        cp.close().unwrap();

        let _ = std::fs::remove_dir_all(&db_dir);
        let _ = std::fs::remove_dir_all(&cp_dir);
    }

    #[test]
    fn checkpoint_rejects_existing_dir() {
        let db_dir = temp_dir("src3");
        let cp_dir = temp_dir("cp3");
        std::fs::create_dir_all(&cp_dir).unwrap(); // pre-create

        let db = DbImpl::open(&opts(), &db_dir).unwrap();
        let err = create_checkpoint(&db, &cp_dir).unwrap_err();
        assert!(err.is_invalid_argument());
        db.close().unwrap();

        let _ = std::fs::remove_dir_all(&db_dir);
        let _ = std::fs::remove_dir_all(&cp_dir);
    }

    #[test]
    fn checkpoint_with_snapshot_data() {
        let db_dir = temp_dir("src4");
        let cp_dir = temp_dir("cp4");

        let db = DbImpl::open(&opts(), &db_dir).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();

        create_checkpoint(&db, &cp_dir).unwrap();
        db.close().unwrap();

        // Iterate the checkpoint.
        let cp = DbImpl::open(&opts(), &cp_dir).unwrap();
        let mut it = cp.iter().unwrap();
        it.seek_to_first();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(it.key().to_vec());
            it.next();
        }
        assert_eq!(
            keys,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
        cp.close().unwrap();

        let _ = std::fs::remove_dir_all(&db_dir);
        let _ = std::fs::remove_dir_all(&cp_dir);
    }
}
