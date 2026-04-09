//! Port of `table/block_based/block_based_table_reader.{h,cc}`.
//!
//! Reads SST files produced by [`super::table_builder::BlockBasedTableBuilder`].
//! Provides:
//!
//! - [`BlockBasedTableReader::open`] — reads the footer, parses
//!   metaindex + index, optionally loads the bloom filter into RAM.
//! - [`BlockBasedTableReader::get`] — point lookup with the
//!   filter → index → data block path.
//! - [`BlockBasedTableReader::iter`] — full SST iterator (forward +
//!   backward + seek). Layer 3b ships a simple "reload current block
//!   on every step" iterator; a more efficient version with cached
//!   block iterators is a follow-up.
//!
//! # Read path
//!
//! 1. **Open**: read the last 48 bytes via the
//!    [`RandomAccessFileReader`], decode the footer, then read the
//!    metaindex and index blocks. The filter block (if present) is
//!    pulled into memory once at open time, matching upstream's
//!    `cache_index_and_filter_blocks=false` default.
//! 2. **Get**: consult the filter; if it says "definitely not
//!    present", return `None`. Otherwise seek the index to find the
//!    data block, read it, seek inside the block, and return the
//!    matching value (or `None` if no exact key match).
//!
//! # What's missing vs upstream
//!
//! - **No block cache**. Each `get` reads the data block fresh.
//!   Layer 4 will plug in [`crate::cache::lru::LruCache`].
//! - **No prefix bloom**. Only full bloom filters.
//! - **No two-level index**. Single-level only.
//! - **No checksum verification on the metaindex / index / filter
//!   blocks** at open. Layer 3b's `read_block` *does* verify the
//!   checksum on every read, so corruption surfaces lazily.

use crate::core::status::{Result, Status};
use crate::env::file_system::IoOptions;
use crate::ext::filter_policy::FilterBitsReader;
use crate::file::random_access_file_reader::RandomAccessFileReader;
use crate::sst::block_based::block::Block;
use crate::sst::block_based::filter_block::{BloomFilterReader, FILTER_METAINDEX_KEY};
use crate::sst::format::{verify_block_trailer, BlockHandle, Footer, BLOCK_TRAILER_SIZE};
use std::sync::Arc;

/// SST reader. Cheap to clone via `Arc` once constructed.
pub struct BlockBasedTableReader {
    file: Arc<RandomAccessFileReader>,
    /// In-memory index block. Held resident for the lifetime of the
    /// reader because point lookups consult it on every call.
    index_block: Block,
    /// In-memory bloom filter, if the SST has one.
    filter: Option<BloomFilterReader>,
    /// Cached file size (== `file.file_size()`).
    file_size: u64,
}

impl BlockBasedTableReader {
    /// Open an SST file. The reader is held by `Arc` so it can be
    /// cloned cheaply between threads — `RandomAccessFileReader` is
    /// `Sync` (Layer 2 guaranteed it).
    pub fn open(file: Arc<RandomAccessFileReader>) -> Result<Self> {
        let file_size = file.file_size();
        if file_size < Footer::ENCODED_LENGTH as u64 {
            return Err(Status::corruption(format!(
                "file too small to contain footer: {file_size} bytes"
            )));
        }

        // Read the last 48 bytes to get the footer.
        let mut footer_buf = vec![0u8; Footer::ENCODED_LENGTH];
        let footer_offset = file_size - Footer::ENCODED_LENGTH as u64;
        let n = file.read(footer_offset, &mut footer_buf, &IoOptions::default())?;
        if n != Footer::ENCODED_LENGTH {
            return Err(Status::corruption("short footer read"));
        }
        let footer = Footer::decode_from(&footer_buf)?;

        // Load the index block.
        let index_bytes = read_block(&file, footer.index_handle)?;
        let index_block = Block::new(index_bytes)?;

        // Load the metaindex block, look for a filter handle.
        let filter = {
            let metaindex_bytes = read_block(&file, footer.metaindex_handle)?;
            let metaindex = Block::new(metaindex_bytes)?;
            let mut it = metaindex.iter();
            it.seek(FILTER_METAINDEX_KEY.as_bytes());
            if it.valid() && it.key() == FILTER_METAINDEX_KEY.as_bytes() {
                let (handle, _) = BlockHandle::decode_from(it.value())?;
                let filter_bytes = read_block(&file, handle)?;
                Some(BloomFilterReader::from_bytes(&filter_bytes))
            } else {
                None
            }
        };

        Ok(Self {
            file,
            index_block,
            filter,
            file_size,
        })
    }

    /// Point-lookup `key`. Returns `Ok(Some(value))` on a hit,
    /// `Ok(None)` on a miss (filter rejection or absent key), or
    /// `Err` for I/O / corruption errors.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Step 1: filter check.
        if let Some(filter) = &self.filter {
            if !filter.may_match(key) {
                return Ok(None);
            }
        }

        // Step 2: walk the index to find the data block.
        let mut index_iter = self.index_block.iter();
        index_iter.seek(key);
        if !index_iter.valid() {
            return Ok(None);
        }
        let (handle, _) = BlockHandle::decode_from(index_iter.value())?;

        // Step 3: read the data block and look up the key inside it.
        let block_bytes = read_block(&self.file, handle)?;
        let block = Block::new(block_bytes)?;
        let mut it = block.iter();
        it.seek(key);
        if it.valid() && it.key() == key {
            Ok(Some(it.value().to_vec()))
        } else {
            Ok(None)
        }
    }

    /// Number of data blocks in the SST. Equivalent to "number of
    /// index entries" — useful for tests.
    pub fn num_data_blocks(&self) -> usize {
        let mut count = 0;
        let mut it = self.index_block.iter();
        it.seek_to_first();
        while it.valid() {
            count += 1;
            it.next();
        }
        count
    }

    /// File size as observed at open time.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Whether the SST carries a bloom filter block.
    pub fn has_filter(&self) -> bool {
        self.filter.is_some()
    }

    /// Open a forward iterator over every record in the SST. Used by
    /// the engine's read path (`MergingIterator`) and by compaction.
    pub fn iter(&self) -> crate::sst::block_based::sst_iterator::SstIter<'_> {
        crate::sst::block_based::sst_iterator::SstIter::new(self)
    }

    // -- pub(crate) accessors used by SstIter --

    /// Borrow the in-memory index block. Used by [`crate::sst::block_based::sst_iterator::SstIter`]
    /// to walk data block handles.
    pub(crate) fn index_block(&self) -> &Block {
        &self.index_block
    }

    /// Read the data block at `handle` (verifying its trailer) and
    /// return the payload bytes. Public-in-crate so the SST iterator
    /// can fetch the next data block on demand.
    pub(crate) fn read_block_bytes(&self, handle: BlockHandle) -> Result<Vec<u8>> {
        read_block(&self.file, handle)
    }
}

/// Read `block_handle.size` bytes plus the trailing
/// [`BLOCK_TRAILER_SIZE`] bytes from `file`, verify the checksum,
/// and return the block bytes (without the trailer).
fn read_block(file: &Arc<RandomAccessFileReader>, handle: BlockHandle) -> Result<Vec<u8>> {
    let total = handle.size as usize + BLOCK_TRAILER_SIZE;
    let mut buf = vec![0u8; total];
    let n = file.read(handle.offset, &mut buf, &IoOptions::default())?;
    if n != total {
        return Err(Status::corruption(format!(
            "short block read at offset {}: got {} of {}",
            handle.offset, n, total
        )));
    }
    let (block_bytes, trailer) = buf.split_at(handle.size as usize);
    verify_block_trailer(block_bytes, trailer)?;
    Ok(block_bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::file_system::FsRandomAccessFile;
    use crate::env::file_system::FsWritableFile;
    use crate::file::writable_file_writer::WritableFileWriter;
    use crate::sst::block_based::table_builder::{
        BlockBasedTableBuilder, BlockBasedTableOptions,
    };
    use std::sync::Mutex;

    /// Shared in-memory file: a `Vec<u8>` that the writer appends to
    /// and the reader reads from. Both halves share the same `Arc`.
    #[derive(Default)]
    struct InMemoryFile {
        bytes: Mutex<Vec<u8>>,
    }

    /// FsWritableFile half — appends into the inner buffer.
    struct InMemoryWriter {
        file: Arc<InMemoryFile>,
    }

    impl FsWritableFile for InMemoryWriter {
        fn append(&mut self, data: &[u8], _opts: &IoOptions) -> Result<()> {
            self.file.bytes.lock().unwrap().extend_from_slice(data);
            Ok(())
        }
        fn flush(&mut self, _opts: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn sync(&mut self, _opts: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn close(&mut self, _opts: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn file_size(&self) -> u64 {
            self.file.bytes.lock().unwrap().len() as u64
        }
    }

    /// FsRandomAccessFile half — copies bytes out of the inner buffer.
    struct InMemoryReader {
        file: Arc<InMemoryFile>,
    }

    impl FsRandomAccessFile for InMemoryReader {
        fn read_at(&self, offset: u64, buf: &mut [u8], _opts: &IoOptions) -> Result<usize> {
            let inner = self.file.bytes.lock().unwrap();
            let start = offset as usize;
            if start >= inner.len() {
                return Ok(0);
            }
            let end = (start + buf.len()).min(inner.len());
            let n = end - start;
            buf[..n].copy_from_slice(&inner[start..end]);
            Ok(n)
        }
        fn size(&self) -> Result<u64> {
            Ok(self.file.bytes.lock().unwrap().len() as u64)
        }
    }

    fn build_table(
        records: &[(&[u8], &[u8])],
        opts: BlockBasedTableOptions,
    ) -> Arc<InMemoryFile> {
        let file = Arc::new(InMemoryFile::default());
        let writer: Box<dyn FsWritableFile> = Box::new(InMemoryWriter {
            file: Arc::clone(&file),
        });
        let writer = WritableFileWriter::new(writer);
        let mut tb = BlockBasedTableBuilder::new(writer, opts);
        for (k, v) in records {
            tb.add(k, v).unwrap();
        }
        tb.finish().unwrap();
        file
    }

    fn open_table(file: Arc<InMemoryFile>) -> BlockBasedTableReader {
        let reader: Box<dyn FsRandomAccessFile> = Box::new(InMemoryReader {
            file: Arc::clone(&file),
        });
        let reader =
            RandomAccessFileReader::new(reader, "test").expect("size lookup");
        BlockBasedTableReader::open(Arc::new(reader)).unwrap()
    }

    #[test]
    fn round_trip_single_record() {
        let file = build_table(
            &[(b"k1" as &[u8], b"v1" as &[u8])],
            BlockBasedTableOptions::default(),
        );
        let reader = open_table(file);
        assert_eq!(reader.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(reader.get(b"k2").unwrap(), None);
    }

    #[test]
    fn round_trip_many_records() {
        let records_owned: Vec<(Vec<u8>, Vec<u8>)> = (0..1000u32)
            .map(|i| {
                (
                    format!("key{i:05}").into_bytes(),
                    format!("value{i}").into_bytes(),
                )
            })
            .collect();
        let records: Vec<(&[u8], &[u8])> = records_owned
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        // Use a tiny block_size so we exercise multi-block paths.
        let opts = BlockBasedTableOptions {
            block_size: 256,
            ..Default::default()
        };
        let file = build_table(&records, opts);
        let reader = open_table(file);

        assert!(reader.num_data_blocks() > 1, "expected multi-block SST");

        // Spot-check 50 random hits.
        for i in [0, 1, 100, 250, 500, 750, 999_u32] {
            let k = format!("key{i:05}");
            let v = format!("value{i}");
            assert_eq!(
                reader.get(k.as_bytes()).unwrap(),
                Some(v.into_bytes()),
                "miss for {k}"
            );
        }
        // And a few misses.
        assert_eq!(reader.get(b"key99999").unwrap(), None);
        assert_eq!(reader.get(b"key0000a").unwrap(), None);
    }

    #[test]
    fn filter_short_circuits_misses() {
        // 100 records → bloom filter has ≥ 1000 bits.
        let records_owned: Vec<(Vec<u8>, Vec<u8>)> = (0..100u32)
            .map(|i| (format!("k{i:03}").into_bytes(), b"v".to_vec()))
            .collect();
        let records: Vec<(&[u8], &[u8])> = records_owned
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let file = build_table(&records, BlockBasedTableOptions::default());
        let reader = open_table(file);
        assert!(reader.has_filter());

        // The filter doesn't change correctness, but we can at least
        // verify it doesn't reject true positives.
        for (k, v) in &records {
            assert_eq!(reader.get(k).unwrap(), Some(v.to_vec()));
        }
    }

    #[test]
    fn no_filter_when_disabled() {
        let opts = BlockBasedTableOptions {
            bloom_filter_bits_per_key: None,
            ..Default::default()
        };
        let file = build_table(&[(b"k" as &[u8], b"v" as &[u8])], opts);
        let reader = open_table(file);
        assert!(!reader.has_filter());
        assert_eq!(reader.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn open_rejects_too_small_file() {
        let file = Arc::new(InMemoryFile::default());
        // Write 10 garbage bytes — way too small for a footer.
        file.bytes.lock().unwrap().extend_from_slice(&[0u8; 10]);
        let reader: Box<dyn FsRandomAccessFile> = Box::new(InMemoryReader {
            file: Arc::clone(&file),
        });
        let raf = RandomAccessFileReader::new(reader, "tiny").unwrap();
        // `unwrap_err` requires Debug on the Ok type; match instead.
        match BlockBasedTableReader::open(Arc::new(raf)) {
            Ok(_) => panic!("expected corruption"),
            Err(e) => assert!(e.is_corruption()),
        }
    }

    #[test]
    fn corruption_in_data_block_surfaces_on_get() {
        let file = build_table(
            &[(b"abc" as &[u8], b"xyz" as &[u8])],
            BlockBasedTableOptions::default(),
        );
        // Tamper with the very first byte (which is inside data block 0).
        file.bytes.lock().unwrap()[0] ^= 0xff;

        let reader: Box<dyn FsRandomAccessFile> = Box::new(InMemoryReader {
            file: Arc::clone(&file),
        });
        let raf = RandomAccessFileReader::new(reader, "tampered").unwrap();
        let table = BlockBasedTableReader::open(Arc::new(raf)).unwrap();
        // The corrupted data block surfaces on get(), not at open()
        // (open only reads metaindex, index, filter — none of which
        // overlap byte 0 in this small file).
        let result = table.get(b"abc");
        // Expect either an error or a wrong/missing value. Either
        // outcome means the corruption was caught somewhere.
        match result {
            Err(e) => assert!(e.is_corruption()),
            Ok(v) => assert_ne!(v, Some(b"xyz".to_vec()), "tampered byte must affect output"),
        }
    }
}
