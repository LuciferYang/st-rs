//! Minimal benchmark tool — the Rust equivalent of upstream's
//! `tools/db_bench_tool.cc`.
//!
//! Measures sequential fill, random read, and forward scan
//! throughput on a temporary DB. All numbers go to stdout.
//!
//! Run with: `cargo run --release --example db_bench`
//!
//! Tuning knobs are constants at the top of main(). For a quick
//! smoke test: `cargo run --example db_bench` (debug mode is
//! ~10× slower but exercises the same code paths).

use st_rs::{DbImpl, DbOptions};
use std::time::Instant;

fn main() -> st_rs::Result<()> {
    // ---- Configuration ----
    const NUM_ENTRIES: u64 = 100_000;
    const KEY_SIZE: usize = 16;
    const VALUE_SIZE: usize = 100;
    const WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

    let dir = std::env::temp_dir().join("st-rs-bench");
    let _ = std::fs::remove_dir_all(&dir);
    let opts = DbOptions {
        create_if_missing: true,
        db_write_buffer_size: WRITE_BUFFER_SIZE,
        ..DbOptions::default()
    };

    println!("st-rs db_bench");
    println!("  entries     : {NUM_ENTRIES}");
    println!("  key size    : {KEY_SIZE} bytes");
    println!("  value size  : {VALUE_SIZE} bytes");
    println!("  write buffer: {} KiB", WRITE_BUFFER_SIZE / 1024);
    println!();

    // ---- Sequential fill ----
    let db = DbImpl::open(&opts, &dir)?;
    let value = vec![0x42u8; VALUE_SIZE];
    let start = Instant::now();
    for i in 0..NUM_ENTRIES {
        let key = format!("{i:0>width$}", width = KEY_SIZE);
        db.put(key.as_bytes(), &value)?;
    }
    db.flush()?;
    db.wait_for_pending_work()?;
    let fill_dur = start.elapsed();
    let fill_ops = NUM_ENTRIES as f64 / fill_dur.as_secs_f64();
    let fill_mb = (NUM_ENTRIES as f64 * (KEY_SIZE + VALUE_SIZE) as f64)
        / (1024.0 * 1024.0)
        / fill_dur.as_secs_f64();
    println!(
        "fillseq      : {:>10.0} ops/s   {:>6.1} MB/s   ({:.3}s)",
        fill_ops,
        fill_mb,
        fill_dur.as_secs_f64()
    );

    // ---- Random read (sequential keys, but read in a scrambled order) ----
    let mut rng = st_rs::util::random::Random::new(0x1234);
    let start = Instant::now();
    let read_count = NUM_ENTRIES.min(50_000); // cap for debug builds
    let mut found = 0u64;
    for _ in 0..read_count {
        let i = rng.uniform(NUM_ENTRIES as u32) as u64;
        let key = format!("{i:0>width$}", width = KEY_SIZE);
        if db.get(key.as_bytes())?.is_some() {
            found += 1;
        }
    }
    let read_dur = start.elapsed();
    let read_ops = read_count as f64 / read_dur.as_secs_f64();
    println!(
        "readrandom   : {:>10.0} ops/s   ({:.3}s, {} found / {} queries)",
        read_ops,
        read_dur.as_secs_f64(),
        found,
        read_count
    );

    // ---- Forward scan ----
    let start = Instant::now();
    let mut it = db.iter()?;
    it.seek_to_first();
    let mut scanned = 0u64;
    while it.valid() {
        scanned += 1;
        it.next();
    }
    let scan_dur = start.elapsed();
    let scan_ops = scanned as f64 / scan_dur.as_secs_f64();
    println!(
        "scanforward  : {:>10.0} ops/s   ({:.3}s, {} entries)",
        scan_ops,
        scan_dur.as_secs_f64(),
        scanned
    );

    db.close()?;
    let _ = std::fs::remove_dir_all(&dir);
    println!("\nDone.");
    Ok(())
}
