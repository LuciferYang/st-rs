//! SST file inspector — the Rust equivalent of upstream's
//! `tools/sst_dump_tool.cc`.
//!
//! Opens one or more SST files and prints their contents: the
//! footer, index block entries, filter status, and every
//! data-block key-value record.
//!
//! Usage:
//!   cargo run --example sst_dump -- path/to/file.sst [...]
//!
//! If no path is given, the tool creates a small SST in a temp
//! directory and dumps it.

use st_rs::db::dbformat::ParsedInternalKey;
use st_rs::sst::block_based::table_reader::BlockBasedTableReader;
use st_rs::sst::format::Footer;
use st_rs::{FileSystem, RandomAccessFileReader, Status};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn main() -> st_rs::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let paths: Vec<PathBuf> = if args.is_empty() {
        // No arguments — create a demo SST and dump it.
        let path = create_demo_sst()?;
        println!("(no arguments — created demo SST at {})\n", path.display());
        vec![path]
    } else {
        args.iter().map(PathBuf::from).collect()
    };

    for path in &paths {
        dump_sst(path)?;
    }
    Ok(())
}

fn dump_sst(path: &Path) -> st_rs::Result<()> {
    println!("=== {} ===\n", path.display());

    let fs = st_rs::env::posix::PosixFileSystem::new();
    let raw = fs.new_random_access_file(path, &Default::default())?;
    let file_size = raw.size()?;
    let reader = RandomAccessFileReader::new(raw, path.display().to_string())?;

    // 1. Footer.
    if file_size < Footer::ENCODED_LENGTH as u64 {
        return Err(Status::corruption("file too small for a footer"));
    }
    let mut footer_buf = vec![0u8; Footer::ENCODED_LENGTH];
    reader.read(
        file_size - Footer::ENCODED_LENGTH as u64,
        &mut footer_buf,
        &Default::default(),
    )?;
    let footer = Footer::decode_from(&footer_buf)?;

    println!("File size     : {} bytes", file_size);
    println!(
        "Metaindex     : offset={}, size={}",
        footer.metaindex_handle.offset, footer.metaindex_handle.size
    );
    println!(
        "Index         : offset={}, size={}",
        footer.index_handle.offset, footer.index_handle.size
    );
    println!();

    // 2. Open the table reader and walk every entry.
    let table = BlockBasedTableReader::open(Arc::new(reader))?;
    println!("Has filter    : {}", table.has_filter());
    println!("Data blocks   : {}", table.num_data_blocks());
    println!();

    let mut it = table.iter();
    it.seek_to_first();
    let mut count = 0u64;
    println!("--- Entries (internal key → value) ---");
    while it.valid() {
        let key_hex = it
            .key()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" ");
        let val_preview: String = if it.value().len() <= 40 {
            String::from_utf8_lossy(it.value()).to_string()
        } else {
            format!(
                "{}... ({} bytes)",
                String::from_utf8_lossy(&it.value()[..40]),
                it.value().len()
            )
        };

        // Try to parse as an internal key.
        let annotation = match ParsedInternalKey::parse(it.key()) {
            Ok(p) => format!(
                "  [user_key={:?} seq={} type={:?}]",
                String::from_utf8_lossy(p.user_key),
                p.sequence,
                p.value_type,
            ),
            Err(_) => String::new(),
        };

        println!("  {key_hex}{annotation}");
        println!("    -> {val_preview}");
        count += 1;
        it.next();
    }
    println!("\nTotal entries : {count}");
    println!();

    Ok(())
}

/// Create a small demo SST for the no-arguments case.
fn create_demo_sst() -> st_rs::Result<PathBuf> {
    use st_rs::db::dbformat::InternalKey;
    use st_rs::sst::block_based::table_builder::{BlockBasedTableBuilder, BlockBasedTableOptions};
    use st_rs::WritableFileWriter;
    use st_rs::core::types::ValueType;

    let fs = st_rs::env::posix::PosixFileSystem::new();
    let dir = std::env::temp_dir().join("st-rs-sst-dump-demo");
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("demo.sst");

    let writable = fs.new_writable_file(&path, &Default::default())?;
    let mut tb = BlockBasedTableBuilder::new(
        WritableFileWriter::new(writable),
        BlockBasedTableOptions::default(),
    );

    // Write some internal-key entries.
    let entries: Vec<(Vec<u8>, &[u8])> = [
        ("alice", 3, ValueType::Value, b"wonderland" as &[u8]),
        ("alice", 1, ValueType::Value, b"in chains"),
        ("bob", 2, ValueType::Value, b"the builder"),
        ("charlie", 4, ValueType::Deletion, b""),
        ("dave", 5, ValueType::Value, b"grohl"),
    ]
    .iter()
    .map(|(k, seq, t, v)| {
        let ik = InternalKey::new(k.as_bytes(), *seq, *t).into_bytes();
        (ik, *v)
    })
    .collect();

    // Sort by internal key (bytewise = MVCC order).
    let mut sorted = entries;
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    for (ik, v) in &sorted {
        tb.add(ik, v)?;
    }
    tb.finish()?;

    Ok(path)
}
