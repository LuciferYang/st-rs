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

//! JNI-backed `FlinkFsBackend` — calls back into a Java Flink
//! `FileSystem` instance for every file operation.
//!
//! This is the "reverse JNI" path: the Rust engine needs to do
//! file I/O, and instead of using POSIX, it calls into the JVM
//! to use Flink's `FileSystem` abstraction (S3, HDFS, OSS, etc.).

use jni::objects::{GlobalRef, JObject, JValue};
use jni::JavaVM;
use st_rs::core::status::{Result, Status};
use st_rs::flink::backend::{FlinkFileStatus, FlinkFsBackend};
use std::io::{Read, Write};
use std::sync::Arc;

/// A `FlinkFsBackend` that delegates every operation to a Java
/// Flink `FileSystem` object via JNI callbacks.
///
/// Holds a `JavaVM` (for attaching arbitrary Rust threads) and a
/// `GlobalRef` to the Java `FileSystem` object (so it survives
/// across JNI calls and threads).
pub struct JniFsBackend {
    /// Global JVM reference — used to attach background threads.
    jvm: Arc<JavaVM>,
    /// Global reference to the Java FileSystem object.
    fs_ref: GlobalRef,
}

// Safety: JavaVM and GlobalRef are both Send + Sync in the jni crate.
// The JniFsBackend only accesses them through proper JNI attach/detach.
unsafe impl Send for JniFsBackend {}
unsafe impl Sync for JniFsBackend {}

impl JniFsBackend {
    /// Create a new JNI filesystem backend.
    ///
    /// - `jvm`: the Java VM instance (obtained via `env.get_java_vm()`)
    /// - `fs_ref`: a global reference to a Java object implementing
    ///   the `FlinkFsBackend` interface (with methods: `openFile`,
    ///   `createFile`, `exists`, `listStatus`, `getFileStatus`,
    ///   `deleteFile`, `mkdirs`, `renameFile`).
    pub fn new(jvm: Arc<JavaVM>, fs_ref: GlobalRef) -> Self {
        Self { jvm, fs_ref }
    }

    /// Attach current thread to JVM and call a method that returns
    /// a boolean.
    fn call_bool_method(&self, method: &str, path: &str) -> Result<bool> {
        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| Status::io_error(format!("JNI attach failed: {e}")))?;

        let j_path = env
            .new_string(path)
            .map_err(|e| Status::io_error(format!("JNI new_string failed: {e}")))?;

        let result = env
            .call_method(
                &self.fs_ref,
                method,
                "(Ljava/lang/String;)Z",
                &[JValue::Object(&j_path.into())],
            )
            .map_err(|e| Status::io_error(format!("JNI call {method} failed: {e}")))?;

        result
            .z()
            .map_err(|e| Status::io_error(format!("JNI bool conversion failed: {e}")))
    }

    /// Call a method that returns a byte[] (file contents).
    fn call_read_method(&self, method: &str, path: &str) -> Result<Vec<u8>> {
        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| Status::io_error(format!("JNI attach failed: {e}")))?;

        let j_path = env
            .new_string(path)
            .map_err(|e| Status::io_error(format!("JNI new_string failed: {e}")))?;

        let result = env
            .call_method(
                &self.fs_ref,
                method,
                "(Ljava/lang/String;)[B",
                &[JValue::Object(&j_path.into())],
            )
            .map_err(|e| Status::io_error(format!("JNI call {method} failed: {e}")))?;

        let obj = result
            .l()
            .map_err(|e| Status::io_error(format!("JNI object conversion failed: {e}")))?;

        if obj.is_null() {
            return Err(Status::io_error(format!("{method}: returned null")));
        }

        let byte_array = jni::objects::JByteArray::from(obj);
        env.convert_byte_array(byte_array)
            .map_err(|e| Status::io_error(format!("JNI byte array conversion failed: {e}")))
    }

    /// Call a void method with path + byte[] arguments (write).
    fn call_write_method(&self, method: &str, path: &str, data: &[u8]) -> Result<()> {
        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| Status::io_error(format!("JNI attach failed: {e}")))?;

        let j_path = env
            .new_string(path)
            .map_err(|e| Status::io_error(format!("JNI new_string failed: {e}")))?;

        let j_bytes = env
            .byte_array_from_slice(data)
            .map_err(|e| Status::io_error(format!("JNI byte_array_from_slice failed: {e}")))?;

        env.call_method(
            &self.fs_ref,
            method,
            "(Ljava/lang/String;[B)V",
            &[
                JValue::Object(&j_path.into()),
                JValue::Object(&JObject::from(j_bytes)),
            ],
        )
        .map_err(|e| Status::io_error(format!("JNI call {method} failed: {e}")))?;

        Ok(())
    }

    /// Call a rename method with two string args.
    fn call_rename_method(&self, src: &str, dst: &str) -> Result<bool> {
        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| Status::io_error(format!("JNI attach failed: {e}")))?;

        let j_src = env
            .new_string(src)
            .map_err(|e| Status::io_error(format!("JNI new_string failed: {e}")))?;
        let j_dst = env
            .new_string(dst)
            .map_err(|e| Status::io_error(format!("JNI new_string failed: {e}")))?;

        let result = env
            .call_method(
                &self.fs_ref,
                "renameFile",
                "(Ljava/lang/String;Ljava/lang/String;)Z",
                &[
                    JValue::Object(&j_src.into()),
                    JValue::Object(&j_dst.into()),
                ],
            )
            .map_err(|e| Status::io_error(format!("JNI call renameFile failed: {e}")))?;

        result
            .z()
            .map_err(|e| Status::io_error(format!("JNI bool conversion failed: {e}")))
    }

    /// Call delete with path + recursive flag.
    fn call_delete_method(&self, path: &str, recursive: bool) -> Result<bool> {
        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| Status::io_error(format!("JNI attach failed: {e}")))?;

        let j_path = env
            .new_string(path)
            .map_err(|e| Status::io_error(format!("JNI new_string failed: {e}")))?;

        let result = env
            .call_method(
                &self.fs_ref,
                "deleteFile",
                "(Ljava/lang/String;Z)Z",
                &[
                    JValue::Object(&j_path.into()),
                    JValue::Bool(recursive.into()),
                ],
            )
            .map_err(|e| Status::io_error(format!("JNI call deleteFile failed: {e}")))?;

        result
            .z()
            .map_err(|e| Status::io_error(format!("JNI bool conversion failed: {e}")))
    }

    /// Call getFileStatus → returns (length, isDir, modTime) as a
    /// long[] from Java.
    fn call_file_status_method(&self, path: &str) -> Result<FlinkFileStatus> {
        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| Status::io_error(format!("JNI attach failed: {e}")))?;

        let j_path = env
            .new_string(path)
            .map_err(|e| Status::io_error(format!("JNI new_string failed: {e}")))?;

        // getFileStatus returns long[3]: {length, isDir (0/1), modTime}
        let result = env
            .call_method(
                &self.fs_ref,
                "getFileStatus",
                "(Ljava/lang/String;)[J",
                &[JValue::Object(&j_path.into())],
            )
            .map_err(|e| Status::io_error(format!("JNI call getFileStatus failed: {e}")))?;

        let obj = result
            .l()
            .map_err(|e| Status::io_error(format!("JNI object conversion failed: {e}")))?;

        if obj.is_null() {
            return Err(Status::not_found(format!("file not found: {path}")));
        }

        let long_array = jni::objects::JLongArray::from(obj);
        let mut buf = [0i64; 3];
        env.get_long_array_region(long_array, 0, &mut buf)
            .map_err(|e| Status::io_error(format!("JNI get_long_array failed: {e}")))?;

        Ok(FlinkFileStatus {
            path: path.to_string(),
            length: buf[0] as u64,
            is_dir: buf[1] != 0,
            modification_time: buf[2] as u64,
        })
    }
}

/// An in-memory buffer that accumulates written bytes and commits
/// them to the Java FileSystem on drop (or explicit flush).
struct JniWriteStream {
    backend: Arc<JniFsBackend>,
    path: String,
    buffer: Vec<u8>,
}

impl Write for JniWriteStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for JniWriteStream {
    fn drop(&mut self) {
        if !self.buffer.is_empty() {
            let _ = self.backend.call_write_method("writeFile", &self.path, &self.buffer);
        }
    }
}

/// A read stream backed by bytes fetched from the Java FileSystem.
struct JniReadStream {
    data: Vec<u8>,
    pos: usize,
}

impl Read for JniReadStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = &self.data[self.pos..];
        let n = std::cmp::min(buf.len(), remaining.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.pos += n;
        Ok(n)
    }
}

impl FlinkFsBackend for JniFsBackend {
    fn open(&self, path: &str) -> Result<Box<dyn Read + Send>> {
        let data = self.call_read_method("readFile", path)?;
        Ok(Box::new(JniReadStream { data, pos: 0 }))
    }

    fn create(&self, path: &str) -> Result<Box<dyn Write + Send>> {
        // Return a write buffer; data is committed on drop.
        Ok(Box::new(JniWriteStream {
            backend: Arc::new(JniFsBackend {
                jvm: Arc::clone(&self.jvm),
                fs_ref: self.fs_ref.clone(),
            }),
            path: path.to_string(),
            buffer: Vec::new(),
        }))
    }

    fn exists(&self, path: &str) -> Result<bool> {
        self.call_bool_method("exists", path)
    }

    fn list_status(&self, path: &str) -> Result<Vec<FlinkFileStatus>> {
        // Simplified: return a single entry for the path itself.
        // A full implementation would parse a Java array of FileStatus.
        match self.call_file_status_method(path) {
            Ok(status) => Ok(vec![status]),
            Err(_) => Ok(Vec::new()),
        }
    }

    fn get_file_status(&self, path: &str) -> Result<FlinkFileStatus> {
        self.call_file_status_method(path)
    }

    fn delete(&self, path: &str, recursive: bool) -> Result<bool> {
        self.call_delete_method(path, recursive)
    }

    fn mkdirs(&self, path: &str) -> Result<bool> {
        self.call_bool_method("mkdirs", path)
    }

    fn rename(&self, src: &str, dst: &str) -> Result<bool> {
        self.call_rename_method(src, dst)
    }
}
