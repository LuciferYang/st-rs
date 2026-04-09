# Layer 4a Rust Port — Implementation Record

Layer 4a is the **minimum viable engine**. It composes everything from
Layers 0–3 into a working key-value store with crash-safe writes:

- Open a DB at a path
- `Put` / `Delete` / `Write(WriteBatch)` with WAL durability
- `Get` that walks memtable → SST list (newest first)
- Manual `Flush` that turns the active memtable into an SST
- Crash recovery: reopen replays the WAL into a fresh memtable

Companion docs:
- `LAYER0-PORT.md` … `LAYER3B-PORT.md` — earlier layers
- `ForSt/FORST-READING-ORDER.md` — the layered reading plan

This commit was preceded by an honest audit of Layers 0–3 against the
upstream tree (74 public headers + every relevant `db/`, `util/`,
`memory/`, `cache/`, `file/`, `env/`, `table/`, `memtable/` source
file). Two genuinely missed items surfaced — `util/heap.h` and
`include/rocksdb/rate_limiter.h` — but neither blocks Layer 4 and
both can be added inline when first needed. Everything else missing
was already documented as a deferral in the relevant `LAYER*-PORT.md`.

## What landed

**4 new source files, 1,410 LOC**. Still safe Rust, still zero
external dependencies.

```
src/db/
├── log_format.rs        ← NEW  WAL record framing constants
├── log_writer.rs        ← NEW  LogWriter — append + WAL framing
├── log_reader.rs        ← NEW  LogReader — replay + reassembly
└── db_impl.rs           ← NEW  DbImpl: open / put / get / write /
                                flush / close, with WAL replay
```

Plus:
- `src/db/mod.rs` — adds the new submodules
- `src/lib.rs` — Layer 4a re-exports

## What constitutes "minimum viable"

A user can now do this end-to-end:

```rust
use st_rs::{DbImpl, DbOptions};
use std::path::Path;

let opts = DbOptions {
    create_if_missing: true,
    ..DbOptions::default()
};
let db = DbImpl::open(&opts, Path::new("/tmp/mydb"))?;

db.put(b"hello", b"world")?;
db.put(b"foo", b"bar")?;
assert_eq!(db.get(b"hello")?, Some(b"world".to_vec()));

db.flush()?;  // memtable → SST
db.put(b"after_flush", b"yes")?;
db.close()?;

// --- crash / restart ---
let db = DbImpl::open(&opts, Path::new("/tmp/mydb"))?;
// WAL replay restores `after_flush`; SST has the rest.
assert_eq!(db.get(b"hello")?, Some(b"world".to_vec()));
assert_eq!(db.get(b"after_flush")?, Some(b"yes".to_vec()));
```

This works through real POSIX I/O: real files on disk, real fsync,
real CRC32C verification on every block read. The seven new tests
in `db::db_impl::tests` cover put+get, delete-then-get, atomic
write-batch, flush-then-get, reopen-replays-WAL, reopen-after-flush,
no-create rejection, and "newer writes shadow older across flush".

## Module summaries

### `db/log_format.rs` — WAL framing constants

Wire-exact with upstream's `db/log_format.h`:

| Constant | Value | Purpose |
|---|---|---|
| `BLOCK_SIZE` | 32 KiB | WAL is split into fixed blocks; each record (or fragment) lives entirely within one block. |
| `HEADER_SIZE` | 7 bytes | `[masked_crc:4][len:2][type:1]` |
| `RecordType` | enum | `Zero` (padding), `Full`, `First`, `Middle`, `Last` |

Records longer than the remaining block space are split into a
`First`/`Middle`*/`Last` chain so that crash recovery can detect a
torn block and skip to the next.

### `db/log_writer.rs` — `LogWriter`

Append-only WAL writer over a Layer 2 [`WritableFileWriter`].

```rust
let writer = WritableFileWriter::new(fs.new_writable_file(&wal_path, ..)?);
let mut log = LogWriter::new(writer);
log.add_record(&serialised_write_batch)?;
log.sync()?;  // fsync
```

The `add_record` path:

1. Compute CRC32C over `[type byte][payload]`, then mask.
2. Emit a 7-byte header followed by the payload.
3. If the current block can't fit a full payload, split into
   `First` + `Middle…` + `Last` and pad short blocks at the end.
4. Track `block_offset` so the next record knows where it stands.

Three tests cover the small/full/large/spanning cases plus the
"awkward 5-byte tail forces a fresh block" path.

### `db/log_reader.rs` — `LogReader`

Replays records written by `LogWriter`. Holds a 32 KiB scratch
buffer that mirrors the on-disk block layout. The `read_record`
loop:

1. Refill the block buffer if there's no header left.
2. Parse the header, verify the masked CRC.
3. Reassemble `First`/`Middle`/`Last` chains into a single payload.
4. Skip torn blocks (CRC mismatch, bad type byte, truncated header)
   and continue with the next valid record. Matches upstream's
   `kSkipAnyCorruptedRecords` mode.

Three round-trip tests cover small records, a 100 KiB record that
spans three blocks, and the empty-log path.

### `db/db_impl.rs` — the engine

The headline file. ~700 LOC of code + tests. Here's the data flow:

#### Internal state

```rust
struct DbState {
    memtable: MemTable,
    ssts: Vec<SstEntry>,             // newest first
    wal: LogWriter,
    wal_number: u64,
    next_file_number: u64,
    last_sequence: SequenceNumber,
}

struct SstEntry {
    number: u64,
    reader: Arc<BlockBasedTableReader>,
}

pub struct DbImpl {
    path: PathBuf,
    fs: Arc<dyn FileSystem>,
    state: Mutex<DbState>,
    lock: Mutex<Option<Box<dyn FileLock>>>,
    user_comparator: Arc<dyn Comparator>,
    write_buffer_size: usize,
}
```

Single big mutex for now — Layer 4b adds leader-follower group commit.

#### Open path

1. Create the directory if `create_if_missing`.
2. Acquire the LOCK file via `PosixFileSystem::lock_file`.
3. List the directory; classify children via `parse_file_name`:
   - `*.sst` → live SSTs
   - `*.log` → WAL files to replay
4. Open every SST through Layer 3b's `BlockBasedTableReader`.
5. For each WAL file (in order), create a `LogReader`, read every
   record, decode it via `replay_record`, and apply each entry to
   a fresh `MemTable`. Delete the WAL after replay.
6. Open a fresh WAL file for new writes.

#### Write path

```rust
fn write(&self, batch: &WriteBatch) -> Result<()> {
    let mut state = self.state.lock().unwrap();
    let first_seq = state.last_sequence + 1;
    let mut record = Vec::new();
    encode_batch_record(batch, first_seq, &mut record);
    state.wal.add_record(&record)?;          // 1. WAL
    state.wal.sync()?;                       // 2. fsync
    let mut handler = MemTableInsertHandler { ... };
    batch.iterate(&mut handler)?;            // 3. memtable
    state.last_sequence = handler.seq - 1;
    let need_flush = state.memtable.approximate_memory_usage() >= self.write_buffer_size;
    drop(state);
    if need_flush { self.flush()?; }
    Ok(())
}
```

The WAL record format is intentionally simpler than upstream — at
Layer 4a there's no need for upstream-wire-compatibility because
this DB only reads its own files. The format is:

```text
sequence:varint64  count:varint32
  ( type:u8  key_len:varint32  key:bytes  value_len:varint32  value:bytes ) × count
```

Where `type` is `0x0` Delete / `0x1` Put / `0x2` Merge / `0x7`
SingleDelete. Layer 4b can swap in upstream's exact WAL record
format if/when wire compatibility matters.

#### Read path

```rust
fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    // 1. Snapshot under the lock.
    let (lookup, ssts) = {
        let state = self.state.lock().unwrap();
        let lookup = LookupKey::new(key, state.last_sequence);
        match state.memtable.get(&lookup) {
            MemTableGetResult::Found(v) => return Ok(Some(v)),
            MemTableGetResult::Deleted => return Ok(None),
            MemTableGetResult::NotFound => {}
        }
        let ssts = state.ssts.iter().map(|e| Arc::clone(&e.reader)).collect();
        (lookup, ssts)
    };
    // 2. Walk SSTs newest-first; first hit wins.
    for table in ssts {
        if let Some(v) = table.get(key)? {
            if v.is_empty() { return Ok(None); }  // tombstone
            return Ok(Some(v));
        }
    }
    Ok(None)
}
```

Note the lock is dropped before any SST I/O — concurrent gets don't
serialise on each other.

#### Flush path

`flush` is synchronous in Layer 4a — the caller blocks until the
SST is on disk. The sequence:

1. Pick a fresh file number, open `<num>.sst`.
2. Walk the memtable in internal-key order; dedup by user key
   (the InternalKeyComparator already places the newest version
   first). For each unique user key, write the value (or an empty
   payload for tombstones) into a `BlockBasedTableBuilder`.
3. Call `builder.finish()` — the SST is now durable.
4. Open the new SST through `BlockBasedTableReader` and prepend it
   to the live-SST list.
5. Swap in a fresh empty memtable; drop the old one.
6. Roll the WAL: open a new log file, close + delete the old one.
7. Persist the live-SST list to `CURRENT` (a single line of
   comma-separated file numbers — Layer 4b will replace this with
   a real manifest).

## Layer 0 trait shim

The Layer 0 [`crate::api::db::Db`] trait expects column families,
snapshots, iterators, multi-get, properties, and ~20 other methods.
Layer 4a's `DbImpl` is a concrete struct with the *minimum* surface
needed for the engine to be useful — `open`, `put`, `delete`, `write`,
`get`, `flush`, `close`. It does **not** implement the full `Db`
trait yet.

Why: implementing the full trait at Layer 4a would force me to stub
out 20 methods that have no meaningful behaviour without column
families and iterators. Layer 4b will:

- Add column-family handles (the `default` CF will be the only one
  for a while)
- Add a real `DbIterator` that merges across memtable + SSTs
- Implement the full `Db` trait on top of these
- Move `DbImpl` to a private module and expose only the trait

For Layer 4a, callers get a concrete `DbImpl` API that mirrors what
the trait *will* look like.

## Verification

| Check | Command | Result |
|---|---|---|
| Clean build | `cargo build` | ✅ 0 warnings, 0 errors |
| Clippy (all targets, `-D warnings`) | `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| Unit tests | `cargo test` | ✅ **214 passed**, 3 ignored doc-tests |
| No unsafe | `#![deny(unsafe_code)]` crate-wide | ✅ enforced |
| Every public item documented | `#![warn(missing_docs)]` | ✅ enforced |
| Zero external dependencies | empty `[dependencies]` | ✅ still std-only |

### Test count by layer

| Layer | Tests |
|---|--:|
| Layer 0 | 28 |
| Layer 1 | 52 |
| Layer 2 | 42 |
| Layer 3a | 47 |
| Layer 3b | 31 |
| **Layer 4a (new)** | **14** |
| **Total** | **214** |

### Layer 4a test breakdown

| Module | Tests | What they verify |
|---|--:|---|
| `db::log_writer` | 3 | Small record fits in one block; large record uses First/Middle/Last; awkward 5-byte tail forces fresh block |
| `db::log_reader` | 3 | Round-trip small records; large record spans blocks; empty log reads nothing |
| `db::db_impl` | 8 | Open + put + get; delete + get returns None; write batch is atomic; flush then get reads from SST; reopen replays WAL; reopen after flush reads SST; open rejects when create_if_missing=false; newer writes shadow older across flush |

The headline test is **`reopen_replays_wal`**: open a fresh DB, put
two keys, close, reopen — and the values must come back. This
exercises the full `WriteBatch → encode → LogWriter → fsync →
[crash] → directory listing → LogReader → replay_record → MemTable
→ Get` path end-to-end through real POSIX I/O.

## Known simplifications (intentional, deferred to Layer 4b)

1. **No background flush thread** — `flush()` is synchronous and
   only runs when the user calls it (or when the memtable's
   approximate memory usage crosses `db_write_buffer_size`).
2. **No compaction** — SSTs accumulate forever. A long-running DB
   will eventually have hundreds of L0 SSTs and `get()` will get
   slow. Layer 4b's compaction job will merge them.
3. **No real `Db` trait impl** — `DbImpl` is its own concrete
   struct with the minimum API. The Layer 0 `Db` trait will be
   implemented in Layer 4b once column families and iterators land.
4. **Single column family** — only `default` exists; the engine
   doesn't track CFs at all internally.
5. **No iterators** — point lookup only. `iter()` exposes the
   memtable iterator for tests but no merging iterator across
   memtable + SSTs yet.
6. **No snapshots beyond "now"** — the read path uses
   `state.last_sequence` as the implicit snapshot.
7. **No group commit** — every write holds a single big mutex.
   Layer 4b's write thread will add leader-follower batching.
8. **No real manifest** — `CURRENT` is just a comma-separated list
   of SST file numbers. Layer 4b will add `VersionEdit` +
   `MANIFEST-NNNNNN` files.
9. **WAL record format is custom**, not upstream-wire-compatible.
   Within `st-rs` the writer/reader use the same encoding so round
   trips work.
10. **No range deletions in the SST** — `WriteBatch::delete_range`
    is accepted but becomes a no-op on the WAL replay path.
11. **SST stores user keys**, not internal keys. The Layer 3b
    `BlockBasedTableBuilder` doesn't yet know about MVCC, so the
    flush path collapses each user key to its newest version
    before writing. This will need to change in Layer 4b alongside
    proper iterator/snapshot semantics.
12. **`db_write_buffer_size`** is interpreted as the per-CF
    write_buffer_size since there's no global write buffer manager
    yet. The `write_buffer_manager` trait + impl from Layer 0/3
    will land alongside the `WriteBufferManager` Layer 0 trait.

## Audit findings (text)

The full audit is in the chat history; the short version:

**Truly missed (will add inline when needed):**
- `util/heap.h` — generic min-heap with comparator. Layer 4b's
  merging iterator will need it; today's loop-over-all-sources
  approach is fine for `k < 16`.
- `include/rocksdb/rate_limiter.h` — `RateLimiter` trait. Mentioned
  in `WritableFileWriter` doc-comments but never defined. Layer 4b
  compaction code will define it.

**Minor cosmetic gaps:**
- `include/rocksdb/iostats_context.h` — companion to `perf_context`,
  belongs in Layer 1 monitoring.
- `include/rocksdb/version.h` — version constants. One-liner.

**Everything else** is a documented deferral consistent with the
layered build strategy.

## Change log

- **v0.0.6** — Layer 4a landed. 4 new source files (1410 LOC),
  214 passing tests, clippy clean, still zero dependencies. The
  crate now exposes a working KV store with WAL durability and
  crash recovery via `DbImpl::open` / `put` / `get` / `flush` /
  `close`.
