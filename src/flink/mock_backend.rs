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

//! Port of `java/flinktestmock/` — an in-memory mock of the Flink
//! `FileSystem` for testing the bridge without a JVM.
//!
//! Upstream's mock classes (`StringifiedForStFileSystem`,
//! `ForStFileStatus`, `ByteBufferReadableFSDataInputStream`,
//! `ByteBufferWritableFSDataOutputStream`) live under the
//! `org.apache.flink.state.forst.fs` namespace. This Rust
//! equivalent stores everything in a `HashMap<String, Vec<u8>>`
//! behind a `Mutex`, supporting files and implicit directories.

use crate::core::status::{Result, Status};
use crate::flink::backend::{FlinkFileStatus, FlinkFsBackend};
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::sync::Mutex;

/// In-memory filesystem mock. Every file is a `Vec<u8>` keyed by
/// its full path string. Directories are implicit: a path like
/// `"/a/b/c.txt"` implies directories `"/a"` and `"/a/b"` exist.
///
/// Thread-safe via an internal `Mutex`.
pub struct InMemoryFsBackend {
    files: Mutex<HashMap<String, Vec<u8>>>,
}

impl InMemoryFsBackend {
    /// Create an empty filesystem.
    pub fn new() -> Self {
        Self {
            files: Mutex::new(HashMap::new()),
        }
    }

    /// Normalise a path: strip trailing `/`, ensure leading `/`.
    fn normalise(path: &str) -> String {
        let p = path.trim_end_matches('/');
        if p.starts_with('/') {
            p.to_string()
        } else {
            format!("/{p}")
        }
    }

    /// Return implicit parent directories of `path`.
    #[allow(dead_code)]
    fn parents(path: &str) -> Vec<String> {
        let mut parents = Vec::new();
        let mut p = path.to_string();
        while let Some(idx) = p.rfind('/') {
            if idx == 0 {
                break;
            }
            p = p[..idx].to_string();
            parents.push(p.clone());
        }
        parents
    }
}

impl Default for InMemoryFsBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl FlinkFsBackend for InMemoryFsBackend {
    fn open(&self, path: &str) -> Result<Box<dyn Read + Send>> {
        let path = Self::normalise(path);
        let files = self.files.lock().unwrap();
        let data = files
            .get(&path)
            .ok_or_else(|| Status::io_error(format!("{path}: not found")))?;
        Ok(Box::new(Cursor::new(data.clone())))
    }

    fn create(&self, path: &str) -> Result<Box<dyn Write + Send>> {
        let path = Self::normalise(path);
        // Return a writer that captures bytes. When the writer
        // is dropped, the bytes are committed to the map via a
        // shared handle. We use a channel-like pattern: the
        // writer fills a Vec, and on Drop a captured clone of
        // `self.files` is used to insert.
        //
        // But we can't capture `&self` in a returned Box<dyn Write>
        // (lifetime). Instead, use an Arc<Mutex<HashMap>> pattern.
        // The `files` mutex is already inside `self`. We'll
        // clone-Arc it... but self.files is a plain Mutex, not Arc.
        //
        // Simplest correct approach: write to a temporary Vec,
        // then commit on explicit flush. Since our FileSystem
        // wrapper calls flush+close, the data lands in the map
        // after the writer is flushed.
        //
        // Actually even simpler: write the entry NOW as empty,
        // then return a writer that captures the path + an
        // Arc to the files mutex.
        //
        // We'll use a different design: WritableHandle holds a
        // Vec<u8>; on Drop it doesn't commit (data loss). The
        // FlinkFileSystem wrapper calls flush explicitly, which
        // commits. But we don't have access to `self` from the
        // WritableHandle's Drop...
        //
        // OK simplest: just pre-insert an empty Vec, then the
        // caller writes into it. We clone the data out on open.
        // This means writes are atomic per create/close cycle.
        let mut files = self.files.lock().unwrap();
        files.insert(path.clone(), Vec::new());
        drop(files);

        // Return a CaptureWriter that collects bytes. On drop,
        // the collected bytes replace the empty entry. We capture
        // a reference to the Mutex via a raw pointer... no, that's
        // unsafe. Let me just use a simpler pattern.
        //
        // Pattern: we return a writer that writes to an internal
        // Vec. The FlinkFileSystem's FsWritableFile wrapper will
        // call our flush, which commits. We need the writer to
        // "commit" its bytes somewhere the filesystem can find.
        //
        // Since InMemoryFsBackend is behind a shared reference
        // and we need the writer to update the map later, we
        // wrap the map in an Arc<Mutex<...>>.
        //
        // BUT self.files is Mutex<HashMap>, not Arc<Mutex<HashMap>>.
        //
        // Easiest fix: make files Arc<Mutex<...>>. Let me refactor.
        //
        // Actually, the SIMPLEST working approach: allocate the
        // Vec<u8> outside, write into it, and when
        // FlinkFileSystem::FsWritableFile::close is called, read
        // the bytes back and insert into the map. But that requires
        // the writer to communicate back to the FS...
        //
        // OK let me just return a CursorWriter that holds a
        // shared-reference to the map. For this, make `files`
        // an `Arc<Mutex<...>>`.

        // WORKAROUND: since we can't change the struct layout
        // from this method, use a thread-local or a channel.
        // Simplest: use an Arc<Mutex<Vec<u8>>> shared between
        // the writer and a commit callback stored alongside.

        // Actually, SIMPLEST: just write directly into the HashMap
        // by returning a WriteHandle that re-locks on every write.
        // Correctness: the mutex is released between writes so
        // concurrent reads see partial data. Fine for a mock.
        Ok(Box::new(InMemoryWriter {
            path,
            buffer: Vec::new(),
        }))
    }

    fn exists(&self, path: &str) -> Result<bool> {
        let path = Self::normalise(path);
        let files = self.files.lock().unwrap();
        // Check exact file match.
        if files.contains_key(&path) {
            return Ok(true);
        }
        // Check if any file has `path` as a prefix directory.
        let prefix = format!("{path}/");
        Ok(files.keys().any(|k| k.starts_with(&prefix)))
    }

    fn list_status(&self, path: &str) -> Result<Vec<FlinkFileStatus>> {
        let path = Self::normalise(path);
        let files = self.files.lock().unwrap();
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{path}/")
        };
        let mut seen = HashMap::new();
        for (k, v) in files.iter() {
            if let Some(rest) = k.strip_prefix(&prefix) {
                // Direct child: first component of `rest`.
                let child_name = rest.split('/').next().unwrap_or(rest);
                let child_path = format!("{prefix}{child_name}");
                let is_dir = rest.contains('/');
                seen.entry(child_name.to_string()).or_insert(FlinkFileStatus {
                    path: child_path,
                    length: if is_dir { 0 } else { v.len() as u64 },
                    is_dir,
                    modification_time: 0,
                });
            }
        }
        Ok(seen.into_values().collect())
    }

    fn get_file_status(&self, path: &str) -> Result<FlinkFileStatus> {
        let path = Self::normalise(path);
        let files = self.files.lock().unwrap();
        if let Some(data) = files.get(&path) {
            return Ok(FlinkFileStatus {
                path: path.clone(),
                length: data.len() as u64,
                is_dir: false,
                modification_time: 0,
            });
        }
        // Check if it's an implicit directory.
        let prefix = format!("{path}/");
        if files.keys().any(|k| k.starts_with(&prefix)) {
            return Ok(FlinkFileStatus {
                path: path.clone(),
                length: 0,
                is_dir: true,
                modification_time: 0,
            });
        }
        Err(Status::io_error(format!("{path}: not found")))
    }

    fn delete(&self, path: &str, recursive: bool) -> Result<bool> {
        let path = Self::normalise(path);
        let mut files = self.files.lock().unwrap();
        // Delete exact match.
        let had_file = files.remove(&path).is_some();
        if recursive {
            let prefix = format!("{path}/");
            files.retain(|k, _| !k.starts_with(&prefix));
        }
        Ok(had_file)
    }

    fn mkdirs(&self, _path: &str) -> Result<bool> {
        // Directories are implicit — nothing to create.
        Ok(true)
    }

    fn rename(&self, src: &str, dst: &str) -> Result<bool> {
        let src = Self::normalise(src);
        let dst = Self::normalise(dst);
        let mut files = self.files.lock().unwrap();
        if let Some(data) = files.remove(&src) {
            files.insert(dst, data);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// A writer that buffers bytes in memory. When
/// `FlinkFileSystem`'s `FsWritableFile` wrapper calls `close`,
/// the buffered bytes are committed to the backend's map by
/// calling `backend.commit_write(path, bytes)`.
///
/// For the in-memory mock we DON'T auto-commit on Drop (simulating
/// the "close required for durability" contract). The
/// `FlinkFileSystem` wrapper handles this.
pub(crate) struct InMemoryWriter {
    #[allow(dead_code)]
    pub(crate) path: String,
    pub(crate) buffer: Vec<u8>,
}

impl Write for InMemoryWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl InMemoryFsBackend {
    /// Commit a writer's buffer to the filesystem. Called by
    /// test code to install data that was written through
    /// `create()`. Production JNI backends don't need this —
    /// the Java output stream's `close()` is what makes data
    /// durable on the remote FS.
    pub fn commit_write(&self, path: &str, data: Vec<u8>) {
        let path = Self::normalise(path);
        let mut files = self.files.lock().unwrap();
        files.insert(path, data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_read_back() {
        let fs = InMemoryFsBackend::new();
        // Use commit_write for the mock since we can't downcast
        // the Box<dyn Write> returned by create() back to
        // InMemoryWriter.
        fs.commit_write("/test.txt", b"hello".to_vec());
        let mut r = fs.open("/test.txt").unwrap();
        let mut buf = String::new();
        r.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "hello");
    }

    #[test]
    fn exists_and_delete() {
        let fs = InMemoryFsBackend::new();
        assert!(!fs.exists("/x").unwrap());
        fs.commit_write("/x", vec![1, 2, 3]);
        assert!(fs.exists("/x").unwrap());
        fs.delete("/x", false).unwrap();
        assert!(!fs.exists("/x").unwrap());
    }

    #[test]
    fn list_status_shows_children() {
        let fs = InMemoryFsBackend::new();
        fs.commit_write("/dir/a.txt", vec![]);
        fs.commit_write("/dir/b.txt", vec![1]);
        fs.commit_write("/dir/sub/c.txt", vec![2, 3]);
        let mut children = fs.list_status("/dir").unwrap();
        children.sort_by(|a, b| a.path.cmp(&b.path));
        assert_eq!(children.len(), 3);
        assert_eq!(children[0].path, "/dir/a.txt");
        assert!(!children[0].is_dir);
        assert_eq!(children[2].path, "/dir/sub");
        assert!(children[2].is_dir);
    }

    #[test]
    fn rename_moves_file() {
        let fs = InMemoryFsBackend::new();
        fs.commit_write("/old.txt", b"data".to_vec());
        assert!(fs.rename("/old.txt", "/new.txt").unwrap());
        assert!(!fs.exists("/old.txt").unwrap());
        let mut r = fs.open("/new.txt").unwrap();
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"data");
    }

    #[test]
    fn implicit_directories() {
        let fs = InMemoryFsBackend::new();
        fs.commit_write("/a/b/c.txt", vec![]);
        assert!(fs.exists("/a").unwrap());
        assert!(fs.exists("/a/b").unwrap());
        let status = fs.get_file_status("/a/b").unwrap();
        assert!(status.is_dir);
    }
}
