# st-rs

A Rust port of [ForSt](https://github.com/ververica/ForSt) — a RocksDB
fork used as the state backend for [Apache Flink](https://flink.apache.org/)
streaming jobs.

st-rs replaces the C++ ForSt engine with a safe-Rust implementation while
providing JNI bindings so Flink's Java state backend can use it as a drop-in
replacement.

## Project Structure

```
st-rs/
├── src/                        Rust engine library (st-rs crate)
├── st-rs-jni/                  JNI bindings (cdylib → libst_rs_jni.so)
├── java/                       Java wrapper (org.forstdb.* classes)
│   └── src/main/java/org/forstdb/
├── examples/                   Demo binaries (simple, db_bench, sst_dump)
├── FLINK-INTEGRATION-PLAN.md   14-phase implementation plan
├── VECTORIZED-STATE-DESIGN.md  Gluten + Velox vectorized state design
└── FLINK-FULL-INTEGRATION-ANALYSIS.md  Full Flink integration analysis
```

## Features

- **Full LSM engine** — WAL, memtable (skip list), flush, L0/L1 compaction,
  block-based SSTs with bloom filters, CRC32C verification
- **Column families** — per-CF memtable, SST hierarchy, flush, compaction
- **Merge operator** — built-in `StringAppendOperator` for Flink `ListState`
- **Compaction filter** — TTL-based state expiration for Flink
- **Block cache** — shared LRU cache across all SST readers
- **Compression** — Snappy and LZ4 (feature-gated, pure Rust)
- **MANIFEST** — crash-safe version management with incremental edits
- **Incremental checkpoints** — `disableFileDeletions` / `getLiveFiles` /
  `enableFileDeletions` for Flink's checkpoint workflow
- **DeleteRange** — range tombstones + `deleteFilesInRanges` for rescaling
- **SST ingestion** — `ingestExternalFile` + `createColumnFamilyWithImport`
- **Write stall** — back-pressure when L0 count exceeds threshold
- **Properties & metrics** — all Flink-monitored properties (`rocksdb.*`)
- **Snapshots** — MVCC point-in-time reads with snapshot-aware compaction
- **Batch APIs** — `multi_get` / `multi_put` / `multi_delete` / `prefix_scan`
- **`Db` trait** — full trait implementation for dependency injection
- **Flink bridge** — `FlinkStateBackend` facade + `FlinkFileSystem` for
  remote storage (S3/HDFS via Flink's FileSystem abstraction)
- **JNI bindings** — `org.forstdb.*` Java classes matching ForSt's API

## Building

### Prerequisites

- Rust 1.75+ (`rustup` recommended)
- Java 11+ and Maven 3.6+ (for Java bindings)

### Rust Engine

```bash
cargo build --release
cargo test                          # 373 tests
cargo clippy --all-targets -- -D warnings
```

### JNI Native Library

```bash
cargo build --release -p st-rs-jni
# Output: target/release/libst_rs_jni.dylib (macOS)
#         target/release/libst_rs_jni.so    (Linux)
```

### Java Wrapper JAR

```bash
cd java
mvn package -DskipTests
# Output: java/target/st-rs-jni-0.0.1-SNAPSHOT.jar
```

### Using with Flink

1. Build the native library for your platform:
   ```bash
   cargo build --release -p st-rs-jni
   ```

2. Build the Java JAR:
   ```bash
   cd java && mvn package -DskipTests
   ```

3. Add both to Flink's classpath:
   ```bash
   # Copy JAR to Flink's lib directory
   cp java/target/st-rs-jni-0.0.1-SNAPSHOT.jar $FLINK_HOME/lib/

   # Set native library path
   export LD_LIBRARY_PATH=/path/to/st-rs/target/release:$LD_LIBRARY_PATH
   ```

4. Configure Flink to use the st-rs backend:
   ```yaml
   # flink-conf.yaml
   state.backend.type: rocksdb
   ```

   The `org.forstdb.RocksDB` class in `st-rs-jni.jar` replaces
   `forstjni.jar` — Flink's state backend code calls the same Java
   API but the native calls go to the Rust engine.

**Note:** For production, the native library should be bundled inside
the JAR (fat JAR) so it's extracted automatically at runtime. This
requires a Maven plugin configuration to embed the `.so` file —
see the build customization section below.

### Build Customization

**Compression:** Snappy and LZ4 are enabled by default. To build without:
```bash
cargo build --release -p st-rs --no-default-features
```

**Features:**
- `snappy` — Snappy compression (via `snap` crate, pure Rust)
- `lz4` — LZ4 compression (via `lz4_flex` crate, pure Rust)

### Cross-Compilation for Linux (from macOS)

```bash
rustup target add x86_64-unknown-linux-gnu
cargo build --release -p st-rs-jni --target x86_64-unknown-linux-gnu
# Requires a Linux cross-compilation toolchain (e.g., via cross or zigbuild)
```

## Architecture

```
Flink Operator (Java)
  → ValueState.get() / ListState.add() / MapState.put()
  → ForStKeyedStateBackend (Flink's code, unchanged)
  → org.forstdb.RocksDB (Java class in st-rs-jni.jar)
  → JNI native call
  → Java_org_forstdb_RocksDB_get() (in libst_rs_jni.so)
  → st_rs::DbImpl::get_cf() (Rust engine)
```

The Java classes in `org.forstdb` match the API surface of ForSt's
Java bindings. Flink's `ForStKeyedStateBackend` calls the same methods
— the difference is that the native calls go to Rust instead of C++.

## API Quick Start (Rust)

```rust
use st_rs::{DbImpl, DbOptions, FlinkStateBackend};

// Option 1: Direct engine API
let opts = DbOptions { create_if_missing: true, ..Default::default() };
let db = DbImpl::open(&opts, path)?;
db.put(b"key", b"value")?;
assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
db.close()?;

// Option 2: Flink facade (auto-configures merge operator, etc.)
let sb = FlinkStateBackend::open(path)?;
let cf = sb.create_column_family("state1")?;  // StringAppendOperator enabled
sb.put_cf(&*cf, b"list", b"a")?;
sb.merge_cf(&*cf, b"list", b"b")?;
assert_eq!(sb.get_cf(&*cf, b"list")?, Some(b"a,b".to_vec()));
sb.close()?;
```

## API Quick Start (Java)

```java
import org.forstdb.*;

try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
     RocksDB db = RocksDB.open(opts, "/tmp/test-db")) {
    db.put("key".getBytes(), "value".getBytes());
    byte[] value = db.get("key".getBytes());
    // value == "value"
}
```

## Test Suite

```bash
# Rust tests (373 tests)
cargo test

# Without compression features (336 tests — 3 compression tests gated)
cargo test --no-default-features

# Java compilation check
cd java && mvn compile
```

## Design Constraints

- **`#![deny(unsafe_code)]`** on the core `st-rs` crate — safe Rust only
- **`unsafe` confined to `st-rs-jni`** — JNI handle management requires
  raw pointer operations, isolated in the separate cdylib crate
- **Zero C/C++ dependencies** — Snappy and LZ4 use pure-Rust crates
  (`snap`, `lz4_flex`), not C library bindings
- **Apache 2.0 license** — matching ForSt and Apache Flink

## Related Documents

- [FLINK-INTEGRATION-PLAN.md](FLINK-INTEGRATION-PLAN.md) — 14-phase
  implementation plan with milestones
- [VECTORIZED-STATE-DESIGN.md](VECTORIZED-STATE-DESIGN.md) — design for
  Gluten + Velox vectorized state access
- [FLINK-FULL-INTEGRATION-ANALYSIS.md](FLINK-FULL-INTEGRATION-ANALYSIS.md) —
  full Flink integration analysis (JNI, C ABI, backend module)

## License

Apache-2.0 (matching ForSt and Apache Flink).
