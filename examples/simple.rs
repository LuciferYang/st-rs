//! Minimal "hello world" demonstrating the st-rs API.
//!
//! Run with: `cargo run --example simple`

use st_rs::{create_checkpoint, DbImpl, DbOptions};


fn main() -> st_rs::Result<()> {
    let dir = std::env::temp_dir().join("st-rs-simple-example");
    let _ = std::fs::remove_dir_all(&dir);

    // 1. Open (or create) a DB.
    let opts = DbOptions {
        create_if_missing: true,
        db_write_buffer_size: 64 * 1024,
        ..DbOptions::default()
    };
    let db = DbImpl::open(&opts, &dir)?;

    // 2. Put and get.
    db.put(b"hello", b"world")?;
    db.put(b"foo", b"bar")?;
    println!(
        "get(hello) = {:?}",
        db.get(b"hello")?.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // 3. Snapshot — sees data as of *now*.
    let snap = db.snapshot();
    db.put(b"hello", b"updated")?; // after the snapshot

    println!(
        "get(hello) current  = {:?}",
        db.get(b"hello")?.map(|v| String::from_utf8_lossy(&v).to_string())
    );
    println!(
        "get(hello) snapshot = {:?}",
        db.get_at(b"hello", &*snap)?.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // 4. Iterator.
    db.put(b"alpha", b"1")?;
    db.put(b"beta", b"2")?;
    db.put(b"gamma", b"3")?;

    let mut it = db.iter()?;
    it.seek_to_first();
    println!("\n-- full scan --");
    while it.valid() {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(it.key()),
            String::from_utf8_lossy(it.value())
        );
        it.next();
    }

    // 5. Checkpoint — a hard-linked copy of the DB.
    let cp_dir = std::env::temp_dir().join("st-rs-simple-checkpoint");
    let _ = std::fs::remove_dir_all(&cp_dir);
    create_checkpoint(&db, &cp_dir)?;

    let cp = DbImpl::open(&opts, &cp_dir)?;
    println!(
        "\ncheckpoint get(hello) = {:?}",
        cp.get(b"hello")?.map(|v| String::from_utf8_lossy(&v).to_string())
    );
    cp.close()?;

    // 6. Close.
    drop(snap);
    db.close()?;

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&cp_dir);
    println!("\nDone.");
    Ok(())
}
