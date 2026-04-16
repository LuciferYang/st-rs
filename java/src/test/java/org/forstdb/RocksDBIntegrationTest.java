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

package org.forstdb;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that exercise the full Java → JNI → Rust → back path.
 * These load libst_rs_jni and operate on real on-disk databases.
 */
class RocksDBIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void openPutGetClose() throws RocksDBException {
        String dbPath = tempDir.resolve("test-open").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            db.put("hello".getBytes(), "world".getBytes());
            byte[] value = db.get("hello".getBytes());
            assertNotNull(value);
            assertArrayEquals("world".getBytes(), value);

            assertNull(db.get("missing".getBytes()));
        }
    }

    @Test
    void putGetDeleteCycle() throws RocksDBException {
        String dbPath = tempDir.resolve("test-delete").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            db.put("k".getBytes(), "v".getBytes());
            assertNotNull(db.get("k".getBytes()));

            db.delete(new ColumnFamilyHandle(0), new WriteOptions(), "k".getBytes());
            assertNull(db.get("k".getBytes()));
        }
    }

    @Test
    void writeBatchAtomicity() throws RocksDBException {
        String dbPath = tempDir.resolve("test-batch").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath);
             WriteBatch batch = new WriteBatch();
             WriteOptions wo = new WriteOptions()) {
            batch.put(new ColumnFamilyHandle(0), "k1".getBytes(), "v1".getBytes());
            batch.put(new ColumnFamilyHandle(0), "k2".getBytes(), "v2".getBytes());
            batch.put(new ColumnFamilyHandle(0), "k3".getBytes(), "v3".getBytes());
            assertEquals(3, batch.count());

            db.write(wo, batch);

            assertArrayEquals("v1".getBytes(), db.get("k1".getBytes()));
            assertArrayEquals("v2".getBytes(), db.get("k2".getBytes()));
            assertArrayEquals("v3".getBytes(), db.get("k3".getBytes()));
        }
    }

    @Test
    void columnFamilyIsolation() throws RocksDBException {
        String dbPath = tempDir.resolve("test-cf").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            ColumnFamilyHandle cf = db.createColumnFamily("mystate");
            try (WriteOptions wo = new WriteOptions()) {
                db.put("dk".getBytes(), "default-val".getBytes());
                db.put(cf, wo, "ck".getBytes(), "cf-val".getBytes());

                // Each CF sees only its own data.
                assertArrayEquals("default-val".getBytes(), db.get("dk".getBytes()));
                assertArrayEquals("cf-val".getBytes(), db.get(cf, "ck".getBytes()));
                assertNull(db.get("ck".getBytes()));       // not in default
                assertNull(db.get(cf, "dk".getBytes()));    // not in CF
            }
        }
    }

    @Test
    void mergeOperator() throws RocksDBException {
        // Column families created via st-rs get StringAppendOperator by default.
        String dbPath = tempDir.resolve("test-merge").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            ColumnFamilyHandle cf = db.createColumnFamily("list");
            try (WriteOptions wo = new WriteOptions()) {
                db.put(cf, wo, "k".getBytes(), "a".getBytes());
                db.merge(cf, wo, "k".getBytes(), "b".getBytes());
                db.merge(cf, wo, "k".getBytes(), "c".getBytes());

                byte[] result = db.get(cf, "k".getBytes());
                assertNotNull(result);
                assertEquals("a,b,c", new String(result));
            }
        }
    }

    @Test
    void iteratorForwardScan() throws RocksDBException {
        String dbPath = tempDir.resolve("test-iter").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            db.put("a".getBytes(), "1".getBytes());
            db.put("b".getBytes(), "2".getBytes());
            db.put("c".getBytes(), "3".getBytes());

            try (RocksIterator it = db.newIterator(new ColumnFamilyHandle(0))) {
                it.seekToFirst();
                assertTrue(it.isValid());
                assertArrayEquals("a".getBytes(), it.key());
                assertArrayEquals("1".getBytes(), it.value());

                it.next();
                assertTrue(it.isValid());
                assertArrayEquals("b".getBytes(), it.key());

                it.next();
                assertTrue(it.isValid());
                assertArrayEquals("c".getBytes(), it.key());

                it.next();
                assertFalse(it.isValid());
            }
        }
    }

    @Test
    void iteratorSeek() throws RocksDBException {
        String dbPath = tempDir.resolve("test-seek").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            db.put("aa".getBytes(), "1".getBytes());
            db.put("bb".getBytes(), "2".getBytes());
            db.put("cc".getBytes(), "3".getBytes());

            try (RocksIterator it = db.newIterator(new ColumnFamilyHandle(0))) {
                it.seek("bb".getBytes());
                assertTrue(it.isValid());
                assertArrayEquals("bb".getBytes(), it.key());
                assertArrayEquals("2".getBytes(), it.value());
            }
        }
    }

    @Test
    void flushAndGetProperty() throws RocksDBException {
        String dbPath = tempDir.resolve("test-flush").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            db.put("k".getBytes(), "v".getBytes());
            db.flush();

            String numFiles = db.getProperty("rocksdb.num-files-at-level0");
            assertNotNull(numFiles);
        }
    }

    @Test
    void snapshotRead() throws RocksDBException {
        String dbPath = tempDir.resolve("test-snap").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            db.put("k".getBytes(), "old".getBytes());

            Snapshot snap = db.getSnapshot();
            db.put("k".getBytes(), "new".getBytes());

            // Current read sees new value.
            assertArrayEquals("new".getBytes(), db.get("k".getBytes()));

            // Snapshot is pinned (released later).
            db.releaseSnapshot(snap);
        }
    }

    @Test
    void disableEnableFileDeletions() throws RocksDBException {
        String dbPath = tempDir.resolve("test-filedel").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            db.put("k".getBytes(), "v".getBytes());
            db.flush();

            db.disableFileDeletions();
            // Files are stable for upload.
            db.enableFileDeletions(false);
        }
    }

    @Test
    void multiCfOpen() throws RocksDBException {
        String dbPath = tempDir.resolve("test-multicf").toString();

        // Open with CF list (all new CFs).
        List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
        cfDescs.add(new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()));
        cfDescs.add(new ColumnFamilyDescriptor("cf1", new ColumnFamilyOptions()));

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath, cfDescs, cfHandles)) {
            assertEquals(2, cfHandles.size());
            // Write to cf1 and verify isolation.
            try (WriteOptions wo = new WriteOptions()) {
                db.put(cfHandles.get(1), wo, "k".getBytes(), "from-cf1".getBytes());
            }
            byte[] value = db.get(cfHandles.get(1), "k".getBytes());
            assertNotNull(value);
            assertArrayEquals("from-cf1".getBytes(), value);
            // Default CF doesn't see cf1's data.
            assertNull(db.get("k".getBytes()));
        }
    }

    @Test
    void getLiveFilesMetaData() throws RocksDBException {
        String dbPath = tempDir.resolve("test-livemeta").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            db.put("k".getBytes(), "v".getBytes());
            db.flush();

            List<LiveFileMetaData> meta = db.getLiveFilesMetaData();
            assertNotNull(meta);
            assertFalse(meta.isEmpty());

            LiveFileMetaData first = meta.get(0);
            assertNotNull(first.columnFamilyName());
            assertTrue(first.size() > 0);
        }
    }

    @Test
    void checkpointCreateAndReopen() throws RocksDBException {
        String dbPath = tempDir.resolve("test-cp-db").toString();
        String cpPath = tempDir.resolve("test-cp-out").toString();
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            db.put("k1".getBytes(), "v1".getBytes());
            db.put("k2".getBytes(), "v2".getBytes());
            db.flush();

            Checkpoint cp = Checkpoint.create(db);
            cp.createCheckpoint(cpPath);
        }

        // Reopen the checkpoint as an independent DB.
        try (DBOptions opts = new DBOptions().setCreateIfMissing(false);
             RocksDB cp = RocksDB.open(opts, cpPath)) {
            assertArrayEquals("v1".getBytes(), cp.get("k1".getBytes()));
            assertArrayEquals("v2".getBytes(), cp.get("k2".getBytes()));
            assertNull(cp.get("missing".getBytes()));
        }
    }
}
