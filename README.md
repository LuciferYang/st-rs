# st-rs

A Rust LSM-tree key-value engine compatible with [Apache Flink](https://flink.apache.org/)'s
state backend. Drop-in replacement for [ForSt](https://github.com/ververica/ForSt)
(Flink's RocksDB fork) via JNI bindings in the `org.forstdb` package.

## Project Structure

```
st-rs/
├── src/                        Rust engine library
├── st-rs-jni/                  JNI native library (→ libst_rs_jni.so)
├── java/                       Java API (org.forstdb.*)
│   └── src/main/java/org/forstdb/
└── examples/                   simple, db_bench, sst_dump
```

## Features

**Engine:**
column families, WAL, memtable (skip list), background flush, L0/L1 compaction,
block-based SSTs, bloom filters, CRC32C, MANIFEST-based crash recovery,
MVCC snapshots with snapshot-aware compaction retention.

**Flink integration:**
merge operator (`StringAppendOperator` for `ListState`),
compaction filter (TTL state expiration),
incremental checkpoints (`disableFileDeletions` / `getLiveFiles` / `enableFileDeletions`),
DeleteRange + `deleteFilesInRanges` (rescaling),
SST ingestion + `createColumnFamilyWithImport` (restore),
write stall (back-pressure),
engine properties (`rocksdb.*` metrics),
`FlinkStateBackend` facade, `FlinkFileSystem` for remote storage.

**Performance:**
block cache (shared LRU), Snappy + LZ4 compression (feature-gated, pure Rust),
batch APIs (`multi_get` / `multi_put` / `multi_delete` / `prefix_scan`).

**JNI:**
`org.forstdb.RocksDB`, `WriteBatch`, `ColumnFamilyHandle`, `WriteOptions`,
`ReadOptions`, `DBOptions`, `Snapshot` — matching ForSt's Java API surface.

## Building

```bash
# Rust engine + tests
cargo build --release
cargo test

# JNI native library
cargo build --release -p st-rs-jni
# → target/release/libst_rs_jni.so (Linux)
# → target/release/libst_rs_jni.dylib (macOS)

# Java JAR
cd java && mvn package -DskipTests
# → java/target/st-rs-jni-0.0.1-SNAPSHOT.jar
```

## Using with Flink

Replace `forstjni.jar` + `libforstjni.so` with `st-rs-jni.jar` + `libst_rs_jni.so`:

```bash
cp java/target/st-rs-jni-0.0.1-SNAPSHOT.jar $FLINK_HOME/lib/
export LD_LIBRARY_PATH=/path/to/st-rs/target/release:$LD_LIBRARY_PATH
```

Flink configuration is unchanged — the `org.forstdb.RocksDB` class in
`st-rs-jni.jar` provides the same API, backed by the Rust engine.

## Architecture

```
Flink Operator (Java)
  → ForStKeyedStateBackend
  → org.forstdb.RocksDB        ← st-rs-jni.jar
  → JNI
  → libst_rs_jni.so            ← Rust cdylib
  → st_rs::DbImpl              ← Rust engine
```

## Rust API

```rust
use st_rs::{DbImpl, DbOptions};

let opts = DbOptions { create_if_missing: true, ..Default::default() };
let db = DbImpl::open(&opts, path)?;
db.put(b"key", b"value")?;
assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
db.close()?;
```

## Java API

```java
import org.forstdb.*;

try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
     RocksDB db = RocksDB.open(opts, "/tmp/test-db")) {
    db.put("key".getBytes(), "value".getBytes());
    byte[] value = db.get("key".getBytes());
}
```

## Design Constraints

- `#![deny(unsafe_code)]` on the core engine — `unsafe` confined to `st-rs-jni`
- No C/C++ dependencies — compression uses pure-Rust crates (`snap`, `lz4_flex`)
- 373 tests, clippy clean

## License

Apache-2.0
