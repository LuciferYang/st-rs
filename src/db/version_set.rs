// Copyright 2025 The st-rs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Port of `db/version_set.{h,cc}` — manifest management and per-CF
//! version state.
//!
//! `VersionSet` owns the MANIFEST log file, replays it on recovery,
//! and applies incremental `VersionEdit`s during the lifetime of the
//! DB. Each column family tracks its own L0 and L1 file lists (a
//! simplified two-level model matching the Layer 4a scope).

#![warn(missing_docs)]

use crate::core::status::{Result, Status};
use crate::core::types::ColumnFamilyId;
use crate::db::log_reader::LogReader;
use crate::db::log_writer::LogWriter;
use crate::db::version_edit::{FileMetaData, VersionEdit};
use crate::env::file_system::{FileSystem, IoOptions};
use crate::file::filename::{make_current_file_name, make_descriptor_file_name};
use crate::file::sequence_file_reader::SequentialFileReader;
use crate::file::writable_file_writer::WritableFileWriter;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The default column family ID.
const DEFAULT_CF_ID: ColumnFamilyId = 0;

// ---------------------------------------------------------------------------
// Per-CF file state
// ---------------------------------------------------------------------------

/// Per-column-family file state, tracking L0 and L1 files.
#[derive(Debug, Clone, Default)]
pub struct CfFileState {
    /// Level-0 files (unsorted; may overlap).
    pub l0: Vec<FileMetaData>,
    /// Level-1 files (sorted, non-overlapping in a real engine).
    pub l1: Vec<FileMetaData>,
}

// ---------------------------------------------------------------------------
// VersionSet
// ---------------------------------------------------------------------------

/// Manages the MANIFEST log and per-column-family version state.
///
/// On `recover` it replays all `VersionEdit` records from the
/// MANIFEST to rebuild in-memory state. During the DB's lifetime,
/// `log_and_apply` persists new edits to the MANIFEST and applies
/// them to the in-memory state atomically.
pub struct VersionSet {
    /// Root directory of the database.
    db_path: PathBuf,
    /// Filesystem abstraction.
    fs: Arc<dyn FileSystem>,
    /// Active MANIFEST writer; `None` until the first `log_and_apply`.
    manifest_writer: Option<LogWriter>,
    /// File number of the current MANIFEST.
    manifest_file_number: u64,
    /// Per-CF file state keyed by CF ID.
    cf_files: HashMap<ColumnFamilyId, CfFileState>,
    /// Human-readable CF names keyed by CF ID.
    cf_names: HashMap<ColumnFamilyId, String>,
    /// Next file number to allocate.
    next_file_number: u64,
    /// Last sequence number written.
    last_sequence: u64,
    /// Current WAL log number.
    log_number: u64,
    /// Next column family ID to allocate.
    next_cf_id: u32,
}

impl VersionSet {
    /// Recover a `VersionSet` from the given `db_path`.
    ///
    /// If the CURRENT file is missing, returns a fresh (empty) state.
    /// If CURRENT points at a MANIFEST file, the MANIFEST is opened
    /// and all `VersionEdit` records are replayed to rebuild state.
    pub fn recover(db_path: &Path, fs: Arc<dyn FileSystem>) -> Result<Self> {
        let mut vs = Self {
            db_path: db_path.to_path_buf(),
            fs: Arc::clone(&fs),
            manifest_writer: None,
            manifest_file_number: 0,
            cf_files: HashMap::new(),
            cf_names: HashMap::new(),
            next_file_number: 2, // 1 is reserved for the first WAL
            last_sequence: 0,
            log_number: 0,
            next_cf_id: 1, // 0 is default CF
        };

        // Ensure the default CF always exists.
        vs.cf_files.entry(DEFAULT_CF_ID).or_default();
        vs.cf_names
            .entry(DEFAULT_CF_ID)
            .or_insert_with(|| "default".to_string());

        let current_path = make_current_file_name(db_path);
        if !fs.file_exists(&current_path)? {
            return Ok(vs);
        }

        // Read CURRENT contents.
        let current_contents = Self::read_file_to_string(&fs, &current_path)?;
        let trimmed = current_contents.trim();

        if trimmed.is_empty() {
            return Ok(vs);
        }

        // CURRENT should contain "MANIFEST-NNNNNN".
        if !trimmed.starts_with("MANIFEST-") {
            // Legacy / old-format CURRENT — return fresh state.
            return Ok(vs);
        }

        // Parse the manifest file number from the name.
        let manifest_name = trimmed;
        let num_str = manifest_name
            .strip_prefix("MANIFEST-")
            .ok_or_else(|| Status::corruption("bad CURRENT content"))?;
        let manifest_number: u64 = num_str
            .parse()
            .map_err(|_| Status::corruption(format!("bad manifest number in CURRENT: {num_str}")))?;

        vs.manifest_file_number = manifest_number;
        if manifest_number >= vs.next_file_number {
            vs.next_file_number = manifest_number + 1;
        }

        // Open the manifest and replay edits.
        let manifest_path = make_descriptor_file_name(db_path, manifest_number);
        let seq_file = fs.new_sequential_file(
            &manifest_path,
            &crate::api::options::FileOptions::default(),
        )?;
        let reader = SequentialFileReader::new(seq_file, manifest_path.to_string_lossy());
        let mut log_reader = LogReader::new(reader);

        let mut record = Vec::new();
        while log_reader.read_record(&mut record)? {
            let edit = VersionEdit::decode_from(&record)?;
            vs.apply_edit(&edit);
        }

        Ok(vs)
    }

    /// Persist `edit` to the MANIFEST and apply it to in-memory state.
    ///
    /// If no MANIFEST writer exists yet, a new MANIFEST file is
    /// created and a snapshot of the current state is written first.
    pub fn log_and_apply(&mut self, edit: &VersionEdit) -> Result<()> {
        // Lazily create a new MANIFEST if we don't have one.
        if self.manifest_writer.is_none() {
            self.create_new_manifest()?;
        }

        // Encode the edit and write it to the MANIFEST.
        let mut buf = Vec::new();
        edit.encode_to(&mut buf);

        let writer = self
            .manifest_writer
            .as_mut()
            .expect("manifest_writer should exist after create_new_manifest");
        writer.add_record(&buf)?;
        writer.sync()?;

        // Apply to in-memory state.
        self.apply_edit(edit);
        Ok(())
    }

    /// Write the CURRENT file to point at the current MANIFEST.
    ///
    /// Uses write-to-tmp + rename for atomicity.
    pub fn write_current_pointer(&self) -> Result<()> {
        let content = format!("MANIFEST-{:06}\n", self.manifest_file_number);
        let current_path = make_current_file_name(&self.db_path);
        let tmp_path = current_path.with_extension("tmp");

        // Write to a temp file.
        let wf = self.fs.new_writable_file(
            &tmp_path,
            &crate::api::options::FileOptions::default(),
        )?;
        let mut writer = WritableFileWriter::new(wf);
        let io = IoOptions::default();
        writer.append(content.as_bytes(), &io)?;
        writer.flush(&io)?;
        writer.sync(&io)?;
        writer.close(&io)?;

        // Atomically rename tmp → CURRENT.
        self.fs.rename_file(&tmp_path, &current_path)?;
        Ok(())
    }

    /// Apply a single `VersionEdit` to the in-memory CF state.
    pub fn apply_edit(&mut self, edit: &VersionEdit) {
        // Handle CF add.
        if let Some(ref cf_name) = edit.is_column_family_add {
            let cf_id = self.next_cf_id;
            self.next_cf_id += 1;
            self.cf_files.entry(cf_id).or_default();
            self.cf_names.insert(cf_id, cf_name.clone());
        }

        // Handle CF drop.
        if edit.is_column_family_drop {
            if let Some(cf_id) = edit.column_family_id {
                self.cf_files.remove(&cf_id);
                self.cf_names.remove(&cf_id);
            }
        }

        // Determine the target CF.
        let cf_id = edit.column_family_id.unwrap_or(DEFAULT_CF_ID);
        let cf_state = self.cf_files.entry(cf_id).or_default();

        // Remove deleted files.
        for &(level, file_num) in &edit.deleted_files {
            match level {
                0 => cf_state.l0.retain(|f| f.number != file_num),
                1 => cf_state.l1.retain(|f| f.number != file_num),
                _ => { /* higher levels not tracked in this simplified model */ }
            }
        }

        // Add new files.
        for (level, meta) in &edit.new_files {
            match level {
                0 => cf_state.l0.push(meta.clone()),
                1 => cf_state.l1.push(meta.clone()),
                _ => { /* higher levels not tracked */ }
            }
        }

        // Update counters.
        if let Some(v) = edit.next_file_number {
            if v > self.next_file_number {
                self.next_file_number = v;
            }
        }
        if let Some(v) = edit.last_sequence {
            if v > self.last_sequence {
                self.last_sequence = v;
            }
        }
        if let Some(v) = edit.log_number {
            if v > self.log_number {
                self.log_number = v;
            }
        }
    }

    // -- Accessors --

    /// Next file number to allocate.
    pub fn next_file_number(&self) -> u64 {
        self.next_file_number
    }

    /// Last sequence number written.
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// Current WAL log number.
    pub fn log_number(&self) -> u64 {
        self.log_number
    }

    /// Per-CF file state.
    pub fn cf_files(&self) -> &HashMap<ColumnFamilyId, CfFileState> {
        &self.cf_files
    }

    /// CF name map.
    pub fn cf_names(&self) -> &HashMap<ColumnFamilyId, String> {
        &self.cf_names
    }

    /// Next column family ID.
    pub fn next_cf_id(&self) -> u32 {
        self.next_cf_id
    }

    /// Manifest file number.
    pub fn manifest_file_number(&self) -> u64 {
        self.manifest_file_number
    }

    // -- Internals --

    /// Create a brand-new MANIFEST file, write a snapshot of current
    /// state as the first record, and update CURRENT.
    fn create_new_manifest(&mut self) -> Result<()> {
        let file_number = self.next_file_number;
        self.next_file_number += 1;
        self.manifest_file_number = file_number;

        let manifest_path = make_descriptor_file_name(&self.db_path, file_number);
        let wf = self.fs.new_writable_file(
            &manifest_path,
            &crate::api::options::FileOptions::default(),
        )?;
        let writer_file = WritableFileWriter::new(wf);
        let mut writer = LogWriter::new(writer_file);

        // Write snapshot edits containing all current state (one for
        // default CF + counters, then one per non-default CF).
        let snapshots = self.build_snapshot_edits();
        for snapshot in &snapshots {
            let mut buf = Vec::new();
            snapshot.encode_to(&mut buf);
            writer.add_record(&buf)?;
        }
        writer.sync()?;

        self.manifest_writer = Some(writer);
        self.write_current_pointer()?;
        Ok(())
    }

    /// Build a list of `VersionEdit`s that capture the entire current
    /// state. The first edit contains counters and default CF files.
    /// Subsequent edits each create a non-default CF and add its files.
    fn build_snapshot_edits(&self) -> Vec<VersionEdit> {
        let mut edits = Vec::new();

        // First edit: counters + default CF files.
        let mut base_edit = VersionEdit::new();
        base_edit.next_file_number = Some(self.next_file_number);
        base_edit.last_sequence = Some(self.last_sequence);
        base_edit.log_number = Some(self.log_number);

        if let Some(cf_state) = self.cf_files.get(&DEFAULT_CF_ID) {
            for meta in &cf_state.l0 {
                base_edit.new_files.push((0, meta.clone()));
            }
            for meta in &cf_state.l1 {
                base_edit.new_files.push((1, meta.clone()));
            }
        }
        edits.push(base_edit);

        // One edit per non-default CF: create the CF + add its files.
        let mut cf_ids: Vec<ColumnFamilyId> = self
            .cf_files
            .keys()
            .copied()
            .filter(|id| *id != DEFAULT_CF_ID)
            .collect();
        cf_ids.sort();
        for cf_id in cf_ids {
            let mut cf_edit = VersionEdit::new();
            if let Some(cf_name) = self.cf_names.get(&cf_id) {
                cf_edit.is_column_family_add = Some(cf_name.clone());
            }
            cf_edit.column_family_id = Some(cf_id);
            if let Some(cf_state) = self.cf_files.get(&cf_id) {
                for meta in &cf_state.l0 {
                    cf_edit.new_files.push((0, meta.clone()));
                }
                for meta in &cf_state.l1 {
                    cf_edit.new_files.push((1, meta.clone()));
                }
            }
            edits.push(cf_edit);
        }

        edits
    }

    /// Read an entire small file into a `String`.
    fn read_file_to_string(fs: &Arc<dyn FileSystem>, path: &Path) -> Result<String> {
        let seq_file =
            fs.new_sequential_file(path, &crate::api::options::FileOptions::default())?;
        let mut reader = SequentialFileReader::new(seq_file, path.to_string_lossy());
        let mut buf = vec![0u8; 4096];
        let mut contents = Vec::new();
        let io = IoOptions::default();
        loop {
            let n = reader.read(&mut buf, &io)?;
            if n == 0 {
                break;
            }
            contents.extend_from_slice(&buf[..n]);
        }
        String::from_utf8(contents)
            .map_err(|_| Status::corruption("CURRENT file contains invalid UTF-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::posix::PosixFileSystem;

    fn temp_dir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static C: AtomicU64 = AtomicU64::new(0);
        let n = C.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("st-rs-vs-{tag}-{pid}-{n}"))
    }

    fn make_fs() -> Arc<dyn FileSystem> {
        Arc::new(PosixFileSystem::new())
    }

    #[test]
    fn recover_fresh_db() {
        let dir = temp_dir("fresh");
        std::fs::create_dir_all(&dir).unwrap();
        let fs = make_fs();
        let vs = VersionSet::recover(&dir, fs).unwrap();

        // Fresh state: no manifest writer, default CF present, counters at defaults.
        assert_eq!(vs.next_file_number(), 2);
        assert_eq!(vs.last_sequence(), 0);
        assert_eq!(vs.log_number(), 0);
        assert!(vs.cf_files().contains_key(&DEFAULT_CF_ID));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn log_and_apply_creates_manifest() {
        let dir = temp_dir("manifest");
        std::fs::create_dir_all(&dir).unwrap();
        let fs = make_fs();
        let mut vs = VersionSet::recover(&dir, Arc::clone(&fs)).unwrap();

        let mut edit = VersionEdit::new();
        edit.log_number = Some(1);
        edit.last_sequence = Some(10);
        edit.new_files.push((
            0,
            FileMetaData {
                number: 5,
                file_size: 1024,
                smallest_key: b"a".to_vec(),
                largest_key: b"z".to_vec(),
            },
        ));
        vs.log_and_apply(&edit).unwrap();

        // MANIFEST file should exist.
        let manifest_path = make_descriptor_file_name(&dir, vs.manifest_file_number());
        assert!(fs.file_exists(&manifest_path).unwrap());

        // CURRENT should exist and point at the manifest.
        let current_path = make_current_file_name(&dir);
        assert!(fs.file_exists(&current_path).unwrap());

        // In-memory state should reflect the edit.
        assert_eq!(vs.last_sequence(), 10);
        assert_eq!(vs.log_number(), 1);
        let cf = vs.cf_files().get(&DEFAULT_CF_ID).unwrap();
        assert_eq!(cf.l0.len(), 1);
        assert_eq!(cf.l0[0].number, 5);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recover_replays_edits() {
        let dir = temp_dir("replay");
        std::fs::create_dir_all(&dir).unwrap();
        let fs = make_fs();

        // Write some edits.
        {
            let mut vs = VersionSet::recover(&dir, Arc::clone(&fs)).unwrap();
            let mut edit1 = VersionEdit::new();
            edit1.log_number = Some(1);
            edit1.last_sequence = Some(100);
            edit1.new_files.push((
                0,
                FileMetaData {
                    number: 10,
                    file_size: 2048,
                    smallest_key: b"aaa".to_vec(),
                    largest_key: b"mmm".to_vec(),
                },
            ));
            vs.log_and_apply(&edit1).unwrap();

            let mut edit2 = VersionEdit::new();
            edit2.last_sequence = Some(200);
            edit2.new_files.push((
                1,
                FileMetaData {
                    number: 11,
                    file_size: 4096,
                    smallest_key: b"nnn".to_vec(),
                    largest_key: b"zzz".to_vec(),
                },
            ));
            vs.log_and_apply(&edit2).unwrap();
        }

        // Recover and verify state.
        let vs2 = VersionSet::recover(&dir, Arc::clone(&fs)).unwrap();
        assert_eq!(vs2.last_sequence(), 200);
        assert_eq!(vs2.log_number(), 1);

        let cf = vs2.cf_files().get(&DEFAULT_CF_ID).unwrap();
        assert_eq!(cf.l0.len(), 1);
        assert_eq!(cf.l0[0].number, 10);
        assert_eq!(cf.l1.len(), 1);
        assert_eq!(cf.l1[0].number, 11);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn apply_edit_adds_and_removes_files() {
        let dir = temp_dir("apply");
        std::fs::create_dir_all(&dir).unwrap();
        let fs = make_fs();
        let mut vs = VersionSet::recover(&dir, Arc::clone(&fs)).unwrap();

        // Add two L0 files.
        let mut add_edit = VersionEdit::new();
        add_edit.new_files.push((
            0,
            FileMetaData {
                number: 1,
                file_size: 100,
                smallest_key: b"a".to_vec(),
                largest_key: b"b".to_vec(),
            },
        ));
        add_edit.new_files.push((
            0,
            FileMetaData {
                number: 2,
                file_size: 200,
                smallest_key: b"c".to_vec(),
                largest_key: b"d".to_vec(),
            },
        ));
        vs.apply_edit(&add_edit);

        let cf = vs.cf_files().get(&DEFAULT_CF_ID).unwrap();
        assert_eq!(cf.l0.len(), 2);

        // Delete one of them.
        let mut del_edit = VersionEdit::new();
        del_edit.deleted_files.push((0, 1));
        vs.apply_edit(&del_edit);

        let cf = vs.cf_files().get(&DEFAULT_CF_ID).unwrap();
        assert_eq!(cf.l0.len(), 1);
        assert_eq!(cf.l0[0].number, 2);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
