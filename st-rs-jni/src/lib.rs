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

//! JNI bindings for st-rs.
//!
//! Produces `libst_rs_jni.so` / `libst_rs_jni.dylib` which is loaded
//! by the Java `org.forstdb.RocksDB` class via `System.loadLibrary`.
//!
//! # Safety
//!
//! This crate necessarily uses `unsafe` for JNI interop — raw pointers
//! cross the Java/Rust boundary as `jlong` handles. The core `st-rs`
//! crate remains `#![deny(unsafe_code)]`.
//!
//! # Handle convention
//!
//! Every Rust object exposed to Java is `Box::into_raw` → `jlong`.
//! Java holds the pointer as a `long nativeHandle_`. On close/dispose,
//! Java calls the corresponding `disposeInternal` which does
//! `Box::from_raw` to reclaim and drop the object.

use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jboolean, jbyteArray, jint, jlong, jstring, JNI_TRUE};
use jni::JNIEnv;
use std::path::Path;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Handle helpers
// ---------------------------------------------------------------------------

/// Convert a Rust object into a jlong handle for Java to hold.
fn to_handle<T>(obj: T) -> jlong {
    Box::into_raw(Box::new(obj)) as jlong
}

/// Recover a reference from a jlong handle. Caller must ensure the
/// handle is valid and points to the correct type.
unsafe fn from_handle<T>(handle: jlong) -> &'static T {
    &*(handle as *const T)
}

/// Recover and drop a Rust object from a jlong handle.
unsafe fn drop_handle<T>(handle: jlong) {
    if handle != 0 {
        let _ = Box::from_raw(handle as *mut T);
    }
}

/// Helper: throw a Java RocksDBException with the given message.
fn throw_rocks_exception(env: &mut JNIEnv, msg: &str) {
    let _ = env.throw_new("org/forstdb/RocksDBException", msg);
}

// ---------------------------------------------------------------------------
// RocksDB (org.forstdb.RocksDB)
// ---------------------------------------------------------------------------

/// `RocksDB.open(DBOptions, String path)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_open(
    mut env: JNIEnv,
    _class: JClass,
    _db_options_handle: jlong,
    path: JString,
) -> jlong {
    let path_str: String = match env.get_string(&path) {
        Ok(s) => s.into(),
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read path string");
            return 0;
        }
    };
    let opts = st_rs::DbOptions {
        create_if_missing: true,
        db_write_buffer_size: 64 * 1024 * 1024,
        ..st_rs::DbOptions::default()
    };
    match st_rs::DbImpl::open(&opts, Path::new(&path_str)) {
        Ok(db) => to_handle(db),
        Err(e) => {
            throw_rocks_exception(&mut env, &e.to_string());
            0
        }
    }
}

/// `RocksDB.closeDatabase(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_closeDatabase(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    if let Err(e) = db.close() {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.disposeInternal(long handle)` — release the Rust object.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_disposeInternal(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    unsafe { drop_handle::<Arc<st_rs::DbImpl>>(handle) };
}

/// `RocksDB.get(long handle, long cfHandle, byte[] key) -> byte[]`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_get__JJ_3B(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray,
) -> jbyteArray {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    let key_bytes = match env.convert_byte_array(&key) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read key bytes");
            return std::ptr::null_mut();
        }
    };

    let result = if cf_handle == 0 {
        db.get(&key_bytes)
    } else {
        let cf = unsafe { from_handle::<st_rs::ColumnFamilyHandleImpl>(cf_handle) };
        db.get_cf(cf, &key_bytes)
    };

    match result {
        Ok(Some(value)) => match env.byte_array_from_slice(&value) {
            Ok(arr) => arr.into_raw(),
            Err(_) => {
                throw_rocks_exception(&mut env, "failed to create byte array");
                std::ptr::null_mut()
            }
        },
        Ok(None) => std::ptr::null_mut(),
        Err(e) => {
            throw_rocks_exception(&mut env, &e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// `RocksDB.put(long handle, long cfHandle, long writeOptsHandle, byte[] key, byte[] value)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_put__JJJ_3B_3B(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    _write_opts_handle: jlong,
    key: JByteArray,
    value: JByteArray,
) {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    let key_bytes = match env.convert_byte_array(&key) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read key");
            return;
        }
    };
    let val_bytes = match env.convert_byte_array(&value) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read value");
            return;
        }
    };

    let result = if cf_handle == 0 {
        db.put(&key_bytes, &val_bytes)
    } else {
        let cf = unsafe { from_handle::<st_rs::ColumnFamilyHandleImpl>(cf_handle) };
        db.put_cf(cf, &key_bytes, &val_bytes)
    };

    if let Err(e) = result {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.delete(long handle, long cfHandle, long writeOptsHandle, byte[] key)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_delete__JJJ_3B(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    _write_opts_handle: jlong,
    key: JByteArray,
) {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    let key_bytes = match env.convert_byte_array(&key) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read key");
            return;
        }
    };

    let result = if cf_handle == 0 {
        db.delete(&key_bytes)
    } else {
        let cf = unsafe { from_handle::<st_rs::ColumnFamilyHandleImpl>(cf_handle) };
        db.delete_cf(cf, &key_bytes)
    };

    if let Err(e) = result {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.merge(long handle, long cfHandle, long writeOptsHandle, byte[] key, byte[] value)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_merge__JJJ_3B_3B(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    _write_opts_handle: jlong,
    key: JByteArray,
    value: JByteArray,
) {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    let key_bytes = match env.convert_byte_array(&key) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read key");
            return;
        }
    };
    let val_bytes = match env.convert_byte_array(&value) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read value");
            return;
        }
    };

    let mut batch = st_rs::WriteBatch::new();
    if cf_handle == 0 {
        batch.merge(key_bytes, val_bytes);
    } else {
        let cf = unsafe { from_handle::<st_rs::ColumnFamilyHandleImpl>(cf_handle) };
        use st_rs::api::db::ColumnFamilyHandle;
        batch.merge_cf(cf.id(), key_bytes, val_bytes);
    }

    if let Err(e) = db.write(&batch) {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.flush(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_flush__J(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    if let Err(e) = db.flush() {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.getProperty(long handle, String name) -> String`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getProperty__JLjava_lang_String_2(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    name: JString,
) -> jstring {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    let prop_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => return std::ptr::null_mut(),
    };

    match db.get_property(&prop_name) {
        Some(val) => match env.new_string(&val) {
            Ok(s) => s.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        None => std::ptr::null_mut(),
    }
}

/// `RocksDB.disableFileDeletions(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_disableFileDeletions(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    if let Err(e) = db.disable_file_deletions() {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.enableFileDeletions(long handle, boolean force)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_enableFileDeletions(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    _force: jboolean,
) {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    if let Err(e) = db.enable_file_deletions() {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.getNativeHandle(long handle) -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getNativeHandle(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jlong {
    handle
}

// ---------------------------------------------------------------------------
// ColumnFamilyHandle (org.forstdb.ColumnFamilyHandle)
// ---------------------------------------------------------------------------

/// `ColumnFamilyHandle.getNativeHandle() -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyHandle_getNativeHandle(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jlong {
    handle
}

/// `ColumnFamilyHandle.disposeInternal(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyHandle_disposeInternal(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    unsafe { drop_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(handle) };
}

// ---------------------------------------------------------------------------
// RocksDB.createColumnFamily
// ---------------------------------------------------------------------------

/// `RocksDB.createColumnFamily(long handle, String name) -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_createColumnFamily(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    name: JString,
) -> jlong {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    let cf_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read CF name");
            return 0;
        }
    };

    // Default: StringAppendOperator enabled (Flink pattern).
    let cf_opts = st_rs::api::options::ColumnFamilyOptions {
        merge_operator_name: st_rs::StringAppendOperator::NAME.to_string(),
        ..st_rs::api::options::ColumnFamilyOptions::default()
    };

    match db.create_column_family(&cf_name, &cf_opts) {
        Ok(cf) => to_handle(cf),
        Err(e) => {
            throw_rocks_exception(&mut env, &e.to_string());
            0
        }
    }
}

// ---------------------------------------------------------------------------
// WriteBatch (org.forstdb.WriteBatch)
// ---------------------------------------------------------------------------

/// `WriteBatch.newWriteBatch() -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_newWriteBatch(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    to_handle(st_rs::WriteBatch::new())
}

/// `WriteBatch.put(long handle, long cfHandle, byte[] key, byte[] value)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_put__JJ_3B_3B(
    env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray,
    value: JByteArray,
) {
    let batch = unsafe { &mut *(handle as *mut st_rs::WriteBatch) };
    let k = env.convert_byte_array(&key).unwrap_or_default();
    let v = env.convert_byte_array(&value).unwrap_or_default();
    if cf_handle == 0 {
        batch.put(k, v);
    } else {
        let cf = unsafe { from_handle::<st_rs::ColumnFamilyHandleImpl>(cf_handle) };
        use st_rs::api::db::ColumnFamilyHandle;
        batch.put_cf(cf.id(), k, v);
    }
}

/// `WriteBatch.delete(long handle, long cfHandle, byte[] key)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_delete__JJ_3B(
    env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray,
) {
    let batch = unsafe { &mut *(handle as *mut st_rs::WriteBatch) };
    let k = env.convert_byte_array(&key).unwrap_or_default();
    if cf_handle == 0 {
        batch.delete(k);
    } else {
        let cf = unsafe { from_handle::<st_rs::ColumnFamilyHandleImpl>(cf_handle) };
        use st_rs::api::db::ColumnFamilyHandle;
        batch.delete_cf(cf.id(), k);
    }
}

/// `WriteBatch.merge(long handle, long cfHandle, byte[] key, byte[] value)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_merge__JJ_3B_3B(
    env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray,
    value: JByteArray,
) {
    let batch = unsafe { &mut *(handle as *mut st_rs::WriteBatch) };
    let k = env.convert_byte_array(&key).unwrap_or_default();
    let v = env.convert_byte_array(&value).unwrap_or_default();
    if cf_handle == 0 {
        batch.merge(k, v);
    } else {
        let cf = unsafe { from_handle::<st_rs::ColumnFamilyHandleImpl>(cf_handle) };
        use st_rs::api::db::ColumnFamilyHandle;
        batch.merge_cf(cf.id(), k, v);
    }
}

/// `WriteBatch.count(long handle) -> int`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_count(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jint {
    let batch = unsafe { from_handle::<st_rs::WriteBatch>(handle) };
    batch.count() as jint
}

/// `WriteBatch.clear(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_clear(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    let batch = unsafe { &mut *(handle as *mut st_rs::WriteBatch) };
    batch.clear();
}

/// `WriteBatch.disposeInternal(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_disposeInternal(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    unsafe { drop_handle::<st_rs::WriteBatch>(handle) };
}

/// `RocksDB.write(long handle, long writeOptsHandle, long batchHandle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_write__JJJ(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    _write_opts_handle: jlong,
    batch_handle: jlong,
) {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    let batch = unsafe { from_handle::<st_rs::WriteBatch>(batch_handle) };
    if let Err(e) = db.write(batch) {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

// ---------------------------------------------------------------------------
// WriteOptions (org.forstdb.WriteOptions)
// ---------------------------------------------------------------------------

/// WriteOptions is currently a no-op struct. Flink sets
/// `setDisableWAL(true)` but st-rs doesn't honor it yet.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteOptions_newWriteOptions(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    // Return a non-zero handle so Java doesn't NPE.
    to_handle(())
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteOptions_setDisableWAL(
    _env: JNIEnv,
    _this: JObject,
    _handle: jlong,
    _disable: jboolean,
) -> jlong {
    // No-op — WAL behavior controlled by the engine.
    _handle
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteOptions_disposeInternal(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    unsafe { drop_handle::<()>(handle) };
}

// ---------------------------------------------------------------------------
// ReadOptions (org.forstdb.ReadOptions)
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_org_forstdb_ReadOptions_newReadOptions(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    to_handle(())
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_ReadOptions_disposeInternal(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    unsafe { drop_handle::<()>(handle) };
}

// ---------------------------------------------------------------------------
// DBOptions (org.forstdb.DBOptions)
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_newDBOptions(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    to_handle(st_rs::DbOptions {
        create_if_missing: true,
        ..st_rs::DbOptions::default()
    })
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setCreateIfMissing(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
    flag: jboolean,
) -> jlong {
    let opts = unsafe { &mut *(handle as *mut st_rs::DbOptions) };
    opts.create_if_missing = flag == JNI_TRUE;
    handle
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_disposeInternal(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    unsafe { drop_handle::<st_rs::DbOptions>(handle) };
}

// ---------------------------------------------------------------------------
// Snapshot (org.forstdb.Snapshot)
// ---------------------------------------------------------------------------

/// `RocksDB.getSnapshot(long dbHandle) -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getSnapshot(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jlong {
    let db = unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) };
    let snap = db.snapshot();
    to_handle(snap)
}

/// `RocksDB.releaseSnapshot(long dbHandle, long snapHandle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_releaseSnapshot(
    _env: JNIEnv,
    _this: JObject,
    _db_handle: jlong,
    snap_handle: jlong,
) {
    unsafe { drop_handle::<Arc<st_rs::DbSnapshot>>(snap_handle) };
}
