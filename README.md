# st-rs — ForSt Layer 0 in Rust

A faithful Rust port of **Layer 0** of the [ForSt](https://github.com/ververica/ForSt) project — that is, the public API surface (`include/rocksdb/*.h`) and the platform abstraction shim (`port/`).

Layer 0 is the foundation every other layer depends on: types (`Status`, `Slice`, `SequenceNumber`), the `FileSystem` abstraction that ForSt's Flink bridge plugs into, the `Comparator`, `Iterator`, `DB`, `WriteBatch`, and `Options` contracts, and the extension-point traits (`Cache`, `TableFactory`, `FilterPolicy`, `MergeOperator`, `CompactionFilter`).

See the reading order that inspired this port: `ForSt/FORST-READING-ORDER.md` (layers 0 → 7).

## Scope

This crate **only** contains the public API surface. It does **not** contain:

- An actual LSM engine (Layers 3–4)
- A memtable or SST format (Layer 3)
- Block cache / I/O reader implementations (Layer 2)
- Any `utilities/` features (Layer 5)
- Tools / benchmarks (Layer 6)
- The Flink JNI bridge (Layer 7)

Those belong in follow-up crates (`st-engine`, `st-table`, `st-env`, `st-flink`, …) that depend on this one.

## Design notes

- **`Slice`** is a type alias for `&[u8]`. Rust already has a zero-cost borrowed-bytes primitive; there's no need for a newtype. A `SliceExt` trait adds the non-std helpers (`difference_offset`).
- **`Status`** is a struct (not a `Result`) so the code/subcode/severity/message layout matches upstream. A `pub type Result<T> = core::result::Result<T, Status>` alias gives you idiomatic `?`-style error propagation.
- **`IoStatus`** is a type alias for `Status`. Upstream splits them for type safety via subclassing; Rust lacks inheritance so the split would need a newtype which is not worth the boilerplate at this layer.
- **`DbIterator`** trait is named to avoid collision with `core::iter::Iterator`. The RocksDB iterator is bidirectional and seekable, so it does not fit the `core::iter::Iterator` contract anyway.
- **`FileSystem`** uses `&Path` and `&[u8]` slices. File handles are returned as `Box<dyn FSSequentialFile>` etc. to match the upstream `std::unique_ptr` ownership model.
- **`Comparator`**, **`Cache`**, **`TableFactory`**, **`FilterPolicy`**, **`MergeOperator`**, **`CompactionFilter`**: trait objects. Every implementation must be `Send + Sync`, matching RocksDB's thread-safety contract.
- **`port`** module holds the small platform-specific constants (cache-line size, page size, endianness). Everything else (`Mutex`, `CondVar`, atomics, threads) comes from `std::sync` / `std::thread` / `std::sync::atomic`.

## Building

```bash
cargo build
cargo test
cargo doc --no-deps --open
```

No dependencies. Minimum Rust 1.75.

## License

Apache-2.0 (matching ForSt).
