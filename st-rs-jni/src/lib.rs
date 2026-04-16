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

mod jni_fs_backend;

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

/// Recover a reference from a jlong handle. Returns `None` if the
/// handle is null (zero). Caller must ensure a non-zero handle
/// points to the correct type.
unsafe fn from_handle<T>(handle: jlong) -> Option<&'static T> {
    if handle == 0 {
        return None;
    }
    Some(&*(handle as *const T))
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
    db_options_handle: jlong,
    path: JString,
) -> jlong {
    let path_str: String = match env.get_string(&path) {
        Ok(s) => s.into(),
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read path string");
            return 0;
        }
    };
    let opts = if db_options_handle != 0 {
        match unsafe { from_handle::<st_rs::DbOptions>(db_options_handle) } {
            Some(o) => o.clone(),
            None => st_rs::DbOptions {
                create_if_missing: true,
                ..st_rs::DbOptions::default()
            },
        }
    } else {
        st_rs::DbOptions {
            create_if_missing: true,
            ..st_rs::DbOptions::default()
        }
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
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
    if let Err(e) = db.close() {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.disposeInternalNative(long handle)` — release the Rust object.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_disposeInternalNative(
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
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return std::ptr::null_mut();
        }
    };
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
        let cf_arc = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
            Some(cf) => cf,
            None => {
                throw_rocks_exception(&mut env, "null column family handle");
                return std::ptr::null_mut();
            }
        };
        db.get_cf(&**cf_arc, &key_bytes)
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
    write_opts_handle: jlong,
    key: JByteArray,
    value: JByteArray,
) {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
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
    let disable_wal = unsafe { from_handle::<RustWriteOptions>(write_opts_handle) }
        .map_or(false, |wo| wo.disable_wal);

    let mut batch = st_rs::WriteBatch::new();
    if cf_handle == 0 {
        batch.put(key_bytes, val_bytes);
    } else {
        let cf_arc = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
            Some(cf) => cf,
            None => {
                throw_rocks_exception(&mut env, "null column family handle");
                return;
            }
        };
        use st_rs::api::db::ColumnFamilyHandle;
        batch.put_cf(cf_arc.id(), key_bytes, val_bytes);
    }
    if let Err(e) = db.write_opt(&batch, disable_wal) {
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
    write_opts_handle: jlong,
    key: JByteArray,
) {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
    let key_bytes = match env.convert_byte_array(&key) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read key");
            return;
        }
    };
    let disable_wal = unsafe { from_handle::<RustWriteOptions>(write_opts_handle) }
        .map_or(false, |wo| wo.disable_wal);

    let mut batch = st_rs::WriteBatch::new();
    if cf_handle == 0 {
        batch.delete(key_bytes);
    } else {
        let cf_arc = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
            Some(cf) => cf,
            None => {
                throw_rocks_exception(&mut env, "null column family handle");
                return;
            }
        };
        use st_rs::api::db::ColumnFamilyHandle;
        batch.delete_cf(cf_arc.id(), key_bytes);
    }
    let result = db.write_opt(&batch, disable_wal);

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
    write_opts_handle: jlong,
    key: JByteArray,
    value: JByteArray,
) {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
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
    let disable_wal = unsafe { from_handle::<RustWriteOptions>(write_opts_handle) }
        .map_or(false, |wo| wo.disable_wal);

    let mut batch = st_rs::WriteBatch::new();
    if cf_handle == 0 {
        batch.merge(key_bytes, val_bytes);
    } else {
        let cf_arc = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
            Some(cf) => cf,
            None => {
                throw_rocks_exception(&mut env, "null column family handle");
                return;
            }
        };
        use st_rs::api::db::ColumnFamilyHandle;
        batch.merge_cf(cf_arc.id(), key_bytes, val_bytes);
    }

    if let Err(e) = db.write_opt(&batch, disable_wal) {
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
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
    if let Err(e) = db.flush() {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.flushCf(long dbHandle, long cfHandle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_flushCf(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
) {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
    if cf_handle == 0 {
        if let Err(e) = db.flush() {
            throw_rocks_exception(&mut env, &e.to_string());
        }
    } else {
        let cf = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
            Some(cf) => cf,
            None => {
                throw_rocks_exception(&mut env, "null CF handle");
                return;
            }
        };
        if let Err(e) = db.flush_cf(&**cf) {
            throw_rocks_exception(&mut env, &e.to_string());
        }
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
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return std::ptr::null_mut();
        }
    };
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
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
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
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
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

/// `ColumnFamilyHandle.disposeColumnFamilyHandle(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyHandle_disposeColumnFamilyHandle(
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
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return 0;
        }
    };
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

/// `RocksDB.getColumnFamilyByName(long handle, String name) -> long`
///
/// Returns a handle to an existing CF, or 0 if not found.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getColumnFamilyByName(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    name: JString,
) -> jlong {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return 0;
        }
    };
    let cf_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => return 0,
    };
    match db.get_column_family_by_name(&cf_name) {
        Some(cf) => {
            // Set the StringAppendOperator on recovered CFs so that
            // merge operands from SSTs are correctly combined on read.
            let operator: Arc<dyn st_rs::MergeOperator> =
                Arc::new(st_rs::StringAppendOperator::default());
            let _ = db.set_merge_operator(&*cf, operator);
            to_handle(cf)
        }
        None => 0,
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
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray,
    value: JByteArray,
) {
    if handle == 0 {
        throw_rocks_exception(&mut env, "null handle");
        return;
    }
    let batch = unsafe { &mut *(handle as *mut st_rs::WriteBatch) };
    let k = match env.convert_byte_array(&key) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read key");
            return;
        }
    };
    let v = match env.convert_byte_array(&value) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read value");
            return;
        }
    };
    if cf_handle == 0 {
        batch.put(k, v);
    } else {
        let cf_arc = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
            Some(cf) => cf,
            None => {
                throw_rocks_exception(&mut env, "null column family handle");
                return;
            }
        };
        use st_rs::api::db::ColumnFamilyHandle;
        batch.put_cf(cf_arc.id(), k, v);
    }
}

/// `WriteBatch.delete(long handle, long cfHandle, byte[] key)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_delete__JJ_3B(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray,
) {
    if handle == 0 {
        throw_rocks_exception(&mut env, "null handle");
        return;
    }
    let batch = unsafe { &mut *(handle as *mut st_rs::WriteBatch) };
    let k = match env.convert_byte_array(&key) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read key");
            return;
        }
    };
    if cf_handle == 0 {
        batch.delete(k);
    } else {
        let cf_arc = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
            Some(cf) => cf,
            None => {
                throw_rocks_exception(&mut env, "null column family handle");
                return;
            }
        };
        use st_rs::api::db::ColumnFamilyHandle;
        batch.delete_cf(cf_arc.id(), k);
    }
}

/// `WriteBatch.merge(long handle, long cfHandle, byte[] key, byte[] value)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_merge__JJ_3B_3B(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray,
    value: JByteArray,
) {
    if handle == 0 {
        throw_rocks_exception(&mut env, "null handle");
        return;
    }
    let batch = unsafe { &mut *(handle as *mut st_rs::WriteBatch) };
    let k = match env.convert_byte_array(&key) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read key");
            return;
        }
    };
    let v = match env.convert_byte_array(&value) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read value");
            return;
        }
    };
    if cf_handle == 0 {
        batch.merge(k, v);
    } else {
        let cf_arc = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
            Some(cf) => cf,
            None => {
                throw_rocks_exception(&mut env, "null column family handle");
                return;
            }
        };
        use st_rs::api::db::ColumnFamilyHandle;
        batch.merge_cf(cf_arc.id(), k, v);
    }
}

/// `WriteBatch.deleteRange(long handle, long cfHandle, byte[] beginKey, byte[] endKey)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_deleteRange(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    begin_key: JByteArray,
    end_key: JByteArray,
) {
    let batch = unsafe { &mut *(handle as *mut st_rs::WriteBatch) };
    let begin = match env.convert_byte_array(&begin_key) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read begin key");
            return;
        }
    };
    let end = match env.convert_byte_array(&end_key) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read end key");
            return;
        }
    };
    if cf_handle == 0 {
        batch.delete_range(begin, end);
    } else {
        let cf = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
            Some(cf) => cf,
            None => {
                throw_rocks_exception(&mut env, "null CF handle");
                return;
            }
        };
        use st_rs::api::db::ColumnFamilyHandle;
        batch.delete_range_cf(cf.id(), begin, end);
    }
}

/// `WriteBatch.count(long handle) -> int`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_count(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jint {
    let batch = match unsafe { from_handle::<st_rs::WriteBatch>(handle) } {
        Some(batch) => batch,
        None => return 0,
    };
    batch.count() as jint
}

/// `WriteBatch.clear(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_clear(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    if handle == 0 {
        return;
    }
    let batch = unsafe { &mut *(handle as *mut st_rs::WriteBatch) };
    batch.clear();
}

/// `WriteBatch.disposeWriteBatch(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_disposeWriteBatch(
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
    write_opts_handle: jlong,
    batch_handle: jlong,
) {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
    let batch = match unsafe { from_handle::<st_rs::WriteBatch>(batch_handle) } {
        Some(batch) => batch,
        None => {
            throw_rocks_exception(&mut env, "null batch handle");
            return;
        }
    };
    let disable_wal = unsafe { from_handle::<RustWriteOptions>(write_opts_handle) }
        .map_or(false, |wo| wo.disable_wal);
    if let Err(e) = db.write_opt(batch, disable_wal) {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

// ---------------------------------------------------------------------------
// WriteOptions (org.forstdb.WriteOptions)
// ---------------------------------------------------------------------------

/// Rust-side WriteOptions — carries the `disable_wal` flag.
struct RustWriteOptions {
    disable_wal: bool,
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteOptions_newWriteOptions(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    to_handle(RustWriteOptions { disable_wal: false })
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteOptions_setDisableWAL(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
    disable: jboolean,
) {
    if let Some(_opts) = unsafe { from_handle::<RustWriteOptions>(handle) } {
        // Safety: single-threaded access from Java.
        let opts = unsafe { &mut *(handle as *mut RustWriteOptions) };
        opts.disable_wal = disable == JNI_TRUE;
    }
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteOptions_disposeWriteOptions(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    unsafe { drop_handle::<RustWriteOptions>(handle) };
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
pub extern "system" fn Java_org_forstdb_ReadOptions_disposeReadOptions(
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
pub extern "system" fn Java_org_forstdb_DBOptions_setMaxOpenFiles(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
    max_open_files: jint,
) {
    let opts = unsafe { &mut *(handle as *mut st_rs::DbOptions) };
    opts.max_open_files = max_open_files;
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setUseFsync(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
    flag: jboolean,
) {
    let opts = unsafe { &mut *(handle as *mut st_rs::DbOptions) };
    opts.use_fsync = flag == JNI_TRUE;
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setDbWriteBufferSize(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
    size: jlong,
) {
    let opts = unsafe { &mut *(handle as *mut st_rs::DbOptions) };
    opts.db_write_buffer_size = size as usize;
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setBlockCacheSize(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
    size: jlong,
) {
    let opts = unsafe { &mut *(handle as *mut st_rs::DbOptions) };
    opts.block_cache_size = size as usize;
}

// No-op setters for fields that have no Rust-side equivalent yet.
// The Java side stores the value but it is not passed to the engine.

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setAvoidFlushDuringShutdown(
    _env: JNIEnv,
    _this: JObject,
    _handle: jlong,
    _flag: jboolean,
) {
    // No-op: no Rust-side field.
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setMaxBackgroundJobs(
    _env: JNIEnv,
    _this: JObject,
    _handle: jlong,
    _max_background_jobs: jint,
) {
    // No-op: Rust engine manages its own thread pool.
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setInfoLogLevel(
    _env: JNIEnv,
    _this: JObject,
    _handle: jlong,
    _level: jint,
) {
    // No-op: log level is not wired to the Rust engine yet.
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setStatsDumpPeriodSec(
    _env: JNIEnv,
    _this: JObject,
    _handle: jlong,
    _period: jint,
) {
    // No-op: stats dump is not supported yet.
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setDbLogDir(
    _env: JNIEnv,
    _this: JObject,
    _handle: jlong,
    _dir: JString,
) {
    // No-op: log directory is not wired to the Rust engine yet.
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setMaxLogFileSize(
    _env: JNIEnv,
    _this: JObject,
    _handle: jlong,
    _size: jlong,
) {
    // No-op: log file size is not wired to the Rust engine yet.
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setKeepLogFileNum(
    _env: JNIEnv,
    _this: JObject,
    _handle: jlong,
    _num: jint,
) {
    // No-op: log file count is not wired to the Rust engine yet.
}

#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_disposeDBOptions(
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
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => return 0,
    };
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

// ---------------------------------------------------------------------------
// RocksDB.newIterator / newIteratorCf
// ---------------------------------------------------------------------------

/// `RocksDB.newIterator(long dbHandle, long cfHandle) -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_newIterator(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
) -> jlong {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return 0;
        }
    };

    let result = if cf_handle == 0 {
        db.iter()
    } else {
        let cf_arc = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
            Some(cf) => cf,
            None => {
                throw_rocks_exception(&mut env, "null column family handle");
                return 0;
            }
        };
        db.iter_cf(&**cf_arc)
    };

    match result {
        Ok(iter) => to_handle(iter),
        Err(e) => {
            throw_rocks_exception(&mut env, &e.to_string());
            0
        }
    }
}

/// `RocksDB.newIteratorCf(long dbHandle, long cfHandle, long readOptsHandle) -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_newIteratorCf(
    env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    _read_opts_handle: jlong,
) -> jlong {
    // ReadOptions are currently a no-op; delegate to newIterator.
    Java_org_forstdb_RocksDB_newIterator(env, _this, handle, cf_handle)
}

// ---------------------------------------------------------------------------
// RocksIterator (org.forstdb.RocksIterator)
// ---------------------------------------------------------------------------

/// `RocksIterator.isValid0(long handle) -> boolean`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_isValid0(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jboolean {
    let iter = match unsafe { from_handle::<st_rs::DbIterator>(handle) } {
        Some(it) => it,
        None => return 0,
    };
    if iter.valid() { 1 } else { 0 }
}

/// `RocksIterator.seekToFirst0(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_seekToFirst0(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    if handle == 0 {
        return;
    }
    let iter = unsafe { &mut *(handle as *mut st_rs::DbIterator) };
    iter.seek_to_first();
}

/// `RocksIterator.seekToLast0(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_seekToLast0(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    if handle == 0 {
        return;
    }
    let iter = unsafe { &mut *(handle as *mut st_rs::DbIterator) };
    iter.seek_to_last();
}

/// `RocksIterator.seek0(long handle, byte[] target)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_seek0(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    target: JByteArray,
) {
    if handle == 0 {
        return;
    }
    let target_bytes = match env.convert_byte_array(&target) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read target bytes");
            return;
        }
    };
    let iter = unsafe { &mut *(handle as *mut st_rs::DbIterator) };
    iter.seek(&target_bytes);
}

/// `RocksIterator.next0(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_next0(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    if handle == 0 {
        return;
    }
    let iter = unsafe { &mut *(handle as *mut st_rs::DbIterator) };
    iter.next();
}

/// `RocksIterator.prev0(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_prev0(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    if handle == 0 {
        return;
    }
    let iter = unsafe { &mut *(handle as *mut st_rs::DbIterator) };
    iter.prev();
}

/// `RocksIterator.key0(long handle) -> byte[]`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_key0(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jbyteArray {
    let iter = match unsafe { from_handle::<st_rs::DbIterator>(handle) } {
        Some(it) => it,
        None => return std::ptr::null_mut(),
    };
    if !iter.valid() {
        return std::ptr::null_mut();
    }
    match env.byte_array_from_slice(iter.key()) {
        Ok(arr) => arr.into_raw(),
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to create key byte array");
            std::ptr::null_mut()
        }
    }
}

/// `RocksIterator.value0(long handle) -> byte[]`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_value0(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jbyteArray {
    let iter = match unsafe { from_handle::<st_rs::DbIterator>(handle) } {
        Some(it) => it,
        None => return std::ptr::null_mut(),
    };
    if !iter.valid() {
        return std::ptr::null_mut();
    }
    match env.byte_array_from_slice(iter.value()) {
        Ok(arr) => arr.into_raw(),
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to create value byte array");
            std::ptr::null_mut()
        }
    }
}

/// `RocksIterator.disposeIterator(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_disposeIterator(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    unsafe { drop_handle::<st_rs::DbIterator>(handle) };
}

// ---------------------------------------------------------------------------
// RocksDB — deleteRange, dropColumnFamily, compactRange,
//            deleteFilesInRanges, getLiveFilesMetaData
// ---------------------------------------------------------------------------

/// `RocksDB.deleteRange0(long dbHandle, long cfHandle, byte[] begin, byte[] end)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_deleteRange0(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    begin: JByteArray,
    end: JByteArray,
) {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
    let begin_bytes = match env.convert_byte_array(&begin) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read begin bytes");
            return;
        }
    };
    let end_bytes = match env.convert_byte_array(&end) {
        Ok(b) => b,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read end bytes");
            return;
        }
    };

    let result = if cf_handle == 0 {
        db.delete_range(&begin_bytes, &end_bytes)
    } else {
        let cf_arc = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
            Some(cf) => cf,
            None => {
                throw_rocks_exception(&mut env, "null column family handle");
                return;
            }
        };
        db.delete_range_cf(&**cf_arc, &begin_bytes, &end_bytes)
    };

    if let Err(e) = result {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.dropColumnFamily0(long dbHandle, long cfHandle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_dropColumnFamily0(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
) {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
    let cf_arc = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
        Some(cf) => cf,
        None => {
            throw_rocks_exception(&mut env, "null column family handle");
            return;
        }
    };
    if let Err(e) = db.drop_column_family(&**cf_arc) {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.compactRange0(long dbHandle, long cfHandle)` — no-op stub.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_compactRange0(
    _env: JNIEnv,
    _this: JObject,
    _handle: jlong,
    _cf_handle: jlong,
) {
    // Manual compaction not yet implemented. No-op.
}

/// `RocksDB.deleteFilesInRanges0(long dbHandle, long cfHandle, byte[][] ranges)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_deleteFilesInRanges0(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    cf_handle: jlong,
    ranges: JByteArray, // Actually a byte[][] but received as jobjectArray
) {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
    let cf_arc = match unsafe { from_handle::<Arc<st_rs::ColumnFamilyHandleImpl>>(cf_handle) } {
        Some(cf) => cf,
        None => {
            throw_rocks_exception(&mut env, "null column family handle");
            return;
        }
    };

    // The ranges parameter is a byte[][] with alternating begin/end pairs.
    // Parse it from the jobjectArray.
    let ranges_obj = unsafe { jni::objects::JObjectArray::from_raw(ranges.into_raw()) };
    let len = match env.get_array_length(&ranges_obj) {
        Ok(l) => l as usize,
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read ranges array length");
            return;
        }
    };
    if len % 2 != 0 {
        throw_rocks_exception(&mut env, "ranges array must have even length");
        return;
    }

    let mut range_vecs: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(len / 2);
    let mut i = 0;
    while i < len {
        let begin_obj = match env.get_object_array_element(&ranges_obj, i as i32) {
            Ok(o) => o,
            Err(_) => {
                throw_rocks_exception(&mut env, "failed to read range begin");
                return;
            }
        };
        let end_obj = match env.get_object_array_element(&ranges_obj, (i + 1) as i32) {
            Ok(o) => o,
            Err(_) => {
                throw_rocks_exception(&mut env, "failed to read range end");
                return;
            }
        };
        let begin_arr: JByteArray = begin_obj.into();
        let end_arr: JByteArray = end_obj.into();
        let begin_bytes = match env.convert_byte_array(&begin_arr) {
            Ok(b) => b,
            Err(_) => {
                throw_rocks_exception(&mut env, "failed to convert range begin bytes");
                return;
            }
        };
        let end_bytes = match env.convert_byte_array(&end_arr) {
            Ok(b) => b,
            Err(_) => {
                throw_rocks_exception(&mut env, "failed to convert range end bytes");
                return;
            }
        };
        range_vecs.push((begin_bytes, end_bytes));
        i += 2;
    }

    let range_refs: Vec<(&[u8], &[u8])> = range_vecs
        .iter()
        .map(|(b, e)| (b.as_slice(), e.as_slice()))
        .collect();
    if let Err(e) = db.delete_files_in_ranges(&**cf_arc, &range_refs) {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

/// `RocksDB.getLiveFilesMetaData0(long dbHandle) -> long[]`
///
/// Returns a flat array of handles to `LiveFileMetaData` objects.
/// Each handle is a boxed `LiveFileMetaData` that Java unpacks
/// via the `LiveFileMetaData_*` accessors below.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getLiveFilesMetaData0(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jbyteArray {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return std::ptr::null_mut();
        }
    };
    let metas = db.get_live_files_metadata();

    // Serialise all metadata into a single byte[]:
    //   For each entry:
    //     4 bytes: cfName length (N)
    //     N bytes: cfName (UTF-8)
    //     8 bytes: fileNumber
    //     4 bytes: level
    //     8 bytes: fileSize
    //     4 bytes: smallestKey length (S)
    //     S bytes: smallestKey
    //     4 bytes: largestKey length (L)
    //     L bytes: largestKey
    let mut buf: Vec<u8> = Vec::new();
    // 4 bytes: entry count
    buf.extend_from_slice(&(metas.len() as u32).to_be_bytes());
    for m in &metas {
        let cf_bytes = m.column_family_name.as_bytes();
        buf.extend_from_slice(&(cf_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(cf_bytes);
        buf.extend_from_slice(&m.file_number.to_be_bytes());
        buf.extend_from_slice(&m.level.to_be_bytes());
        buf.extend_from_slice(&m.file_size.to_be_bytes());
        buf.extend_from_slice(&(m.smallest_key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&m.smallest_key);
        buf.extend_from_slice(&(m.largest_key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&m.largest_key);
    }

    match env.byte_array_from_slice(&buf) {
        Ok(arr) => arr.into_raw(),
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to create metadata byte array");
            std::ptr::null_mut()
        }
    }
}

// ---------------------------------------------------------------------------
// Checkpoint (org.forstdb.Checkpoint)
// ---------------------------------------------------------------------------

/// `Checkpoint.createCheckpoint0(long dbHandle, String path)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Checkpoint_createCheckpoint0(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    path: JString,
) {
    let db = match unsafe { from_handle::<Arc<st_rs::DbImpl>>(handle) } {
        Some(db) => db,
        None => {
            throw_rocks_exception(&mut env, "null handle");
            return;
        }
    };
    let path_str: String = match env.get_string(&path) {
        Ok(s) => s.into(),
        Err(_) => {
            throw_rocks_exception(&mut env, "failed to read path string");
            return;
        }
    };
    if let Err(e) = st_rs::create_checkpoint(db, Path::new(&path_str)) {
        throw_rocks_exception(&mut env, &e.to_string());
    }
}

// ---------------------------------------------------------------------------
// FlinkEnv (org.forstdb.FlinkEnv)
// ---------------------------------------------------------------------------

/// `FlinkEnv.createWithBackend(Object javaFsBackend) -> long`
///
/// Creates a `JniFsBackend` wrapping the given Java object. The Java
/// object must implement these methods:
/// - `boolean exists(String path)`
/// - `boolean mkdirs(String path)`
/// - `boolean renameFile(String src, String dst)`
/// - `boolean deleteFile(String path, boolean recursive)`
/// - `byte[] readFile(String path)`
/// - `void writeFile(String path, byte[] data)`
/// - `long[] getFileStatus(String path)` → {length, isDir, modTime}
///
/// Returns a handle to an `Arc<dyn FileSystem>` (a `FlinkFileSystem`
/// backed by the `JniFsBackend`).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlinkEnv_createWithBackend(
    mut env: JNIEnv,
    _class: JClass,
    java_fs_backend: JObject,
) -> jlong {
    let jvm = match env.get_java_vm() {
        Ok(vm) => Arc::new(vm),
        Err(e) => {
            throw_rocks_exception(&mut env, &format!("failed to get JavaVM: {e}"));
            return 0;
        }
    };

    let global_ref = match env.new_global_ref(java_fs_backend) {
        Ok(r) => r,
        Err(e) => {
            throw_rocks_exception(&mut env, &format!("failed to create global ref: {e}"));
            return 0;
        }
    };

    let backend: Arc<dyn st_rs::FlinkFsBackend> =
        Arc::new(jni_fs_backend::JniFsBackend::new(jvm, global_ref));
    let flink_fs = st_rs::FlinkFileSystem::new(backend, "/".to_string());
    let fs: Arc<dyn st_rs::FileSystem> = Arc::new(flink_fs);
    to_handle(fs)
}

/// `FlinkEnv.disposeFlinkEnv(long handle)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlinkEnv_disposeFlinkEnv(
    _env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    unsafe { drop_handle::<Arc<dyn st_rs::FileSystem>>(handle) };
}
