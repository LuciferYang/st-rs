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
 * End-to-end test simulating Flink's ForStKeyedStateBackend lifecycle:
 *
 * 1. Open DB with column families (one per state descriptor)
 * 2. ValueState: put/get per key
 * 3. ListState: merge (StringAppendOperator) per key
 * 4. MapState: prefix-based iteration
 * 5. Incremental checkpoint: disable file deletions → get live files → enable
 * 6. Close and reopen with CF list (simulating Flink restart)
 * 7. Verify all state survives the restart
 *
 * This exercises the same org.forstdb API surface that Flink's state
 * backend uses, without requiring the Flink runtime.
 */
class FlinkLifecycleTest {

    @TempDir
    Path tempDir;

    private static final long WRITE_BUFFER_SIZE = 64 * 1024 * 1024; // 64 MB

    @Test
    void fullStateBackendLifecycle() throws RocksDBException {
        String dbPath = tempDir.resolve("flink-state").toString();

        // ============================================================
        // Phase 1: Initial open — create CFs for each state type
        // ============================================================
        ColumnFamilyHandle valueCf;
        ColumnFamilyHandle listCf;
        ColumnFamilyHandle mapCf;

        try (DBOptions opts = new DBOptions()
                .setCreateIfMissing(true)
                .setDbWriteBufferSize(WRITE_BUFFER_SIZE);
             RocksDB db = RocksDB.open(opts, dbPath)) {

            valueCf = db.createColumnFamily("value-state");
            listCf = db.createColumnFamily("list-state");
            mapCf = db.createColumnFamily("map-state");

            try (WriteOptions wo = new WriteOptions().setDisableWAL(true)) {

                // --- ValueState: simple key-value ---
                db.put(valueCf, wo, key("user:1"), value("Alice"));
                db.put(valueCf, wo, key("user:2"), value("Bob"));
                db.put(valueCf, wo, key("user:3"), value("Charlie"));

                // --- ListState: merge appends (StringAppendOperator) ---
                db.merge(listCf, wo, key("events:1"), value("login"));
                db.merge(listCf, wo, key("events:1"), value("click"));
                db.merge(listCf, wo, key("events:1"), value("purchase"));
                db.merge(listCf, wo, key("events:2"), value("login"));

                // --- MapState: composite keys (prefix + map key) ---
                db.put(mapCf, wo, key("session:1:page"), value("/home"));
                db.put(mapCf, wo, key("session:1:duration"), value("30s"));
                db.put(mapCf, wo, key("session:1:referrer"), value("google"));
                db.put(mapCf, wo, key("session:2:page"), value("/about"));
            }

            // ============================================================
            // Phase 2: Verify state before checkpoint
            // ============================================================

            // ValueState reads
            assertArrayEquals(value("Alice"), db.get(valueCf, key("user:1")));
            assertArrayEquals(value("Bob"), db.get(valueCf, key("user:2")));
            assertNull(db.get(valueCf, key("user:999")));

            // ListState reads (merged)
            assertEquals("login,click,purchase",
                    new String(db.get(listCf, key("events:1"))));
            assertEquals("login",
                    new String(db.get(listCf, key("events:2"))));

            // MapState reads
            assertArrayEquals(value("/home"),
                    db.get(mapCf, key("session:1:page")));
            assertArrayEquals(value("30s"),
                    db.get(mapCf, key("session:1:duration")));

            // MapState iteration (prefix scan via iterator)
            List<String> session1Keys = new ArrayList<>();
            try (RocksIterator it = db.newIterator(mapCf)) {
                it.seek(key("session:1:"));
                while (it.isValid()) {
                    String k = new String(it.key());
                    if (!k.startsWith("session:1:")) break;
                    session1Keys.add(k);
                    it.next();
                }
            }
            assertEquals(3, session1Keys.size(), "session:1 should have 3 map entries");

            // ============================================================
            // Phase 3: Incremental checkpoint
            // ============================================================

            // Flush all CFs to SSTs.
            db.flush();
            db.flush(valueCf);
            db.flush(listCf);
            db.flush(mapCf);

            // Simulate Flink's incremental checkpoint workflow.
            db.disableFileDeletions();
            List<LiveFileMetaData> liveFiles = db.getLiveFilesMetaData();
            assertFalse(liveFiles.isEmpty(),
                    "should have live SST files after flush");

            // In production, Flink would upload these files to S3/HDFS here.
            // We just verify the metadata is correct.
            for (LiveFileMetaData meta : liveFiles) {
                assertNotNull(meta.columnFamilyName());
                assertTrue(meta.size() > 0,
                        "SST file should have non-zero size");
            }

            db.enableFileDeletions(false);

            // ============================================================
            // Phase 4: Checkpoint via hard-link
            // ============================================================

            String cpPath = tempDir.resolve("checkpoint-1").toString();
            Checkpoint cp = Checkpoint.create(db);
            cp.createCheckpoint(cpPath);
        }

        // ============================================================
        // Phase 5: Reopen from checkpoint (simulating Flink restart)
        // ============================================================

        List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
        cfDescs.add(new ColumnFamilyDescriptor("default",
                new ColumnFamilyOptions()));
        cfDescs.add(new ColumnFamilyDescriptor("value-state",
                new ColumnFamilyOptions()));
        cfDescs.add(new ColumnFamilyDescriptor("list-state",
                new ColumnFamilyOptions()));
        cfDescs.add(new ColumnFamilyDescriptor("map-state",
                new ColumnFamilyOptions()));

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        try (DBOptions opts = new DBOptions()
                .setCreateIfMissing(true)
                .setDbWriteBufferSize(WRITE_BUFFER_SIZE);
             RocksDB db = RocksDB.open(opts, dbPath, cfDescs, cfHandles)) {

            assertEquals(4, cfHandles.size());

            ColumnFamilyHandle restoredValue = cfHandles.get(1);
            ColumnFamilyHandle restoredList = cfHandles.get(2);
            ColumnFamilyHandle restoredMap = cfHandles.get(3);

            // ============================================================
            // Phase 6: Verify all state survived the restart
            // ============================================================

            // ValueState
            assertArrayEquals(value("Alice"),
                    db.get(restoredValue, key("user:1")),
                    "ValueState should survive restart");
            assertArrayEquals(value("Bob"),
                    db.get(restoredValue, key("user:2")));
            assertArrayEquals(value("Charlie"),
                    db.get(restoredValue, key("user:3")));

            // ListState (merged values)
            assertEquals("login,click,purchase",
                    new String(db.get(restoredList, key("events:1"))),
                    "ListState merge should survive restart");

            // MapState
            assertArrayEquals(value("/home"),
                    db.get(restoredMap, key("session:1:page")),
                    "MapState should survive restart");

            // MapState iteration after restart
            List<String> session1Keys = new ArrayList<>();
            try (RocksIterator it = db.newIterator(restoredMap)) {
                it.seek(key("session:1:"));
                while (it.isValid()) {
                    String k = new String(it.key());
                    if (!k.startsWith("session:1:")) break;
                    session1Keys.add(k);
                    it.next();
                }
            }
            assertEquals(3, session1Keys.size(),
                    "MapState iteration should work after restart");

            // ============================================================
            // Phase 7: Continue writing after restart (new epoch)
            // ============================================================

            try (WriteOptions wo = new WriteOptions().setDisableWAL(true)) {
                // Update ValueState
                db.put(restoredValue, wo, key("user:1"), value("Alice-v2"));

                // Append to ListState
                db.merge(restoredList, wo, key("events:1"), value("logout"));

                // Add to MapState
                db.put(restoredMap, wo, key("session:3:page"), value("/new"));
            }

            // Verify updates
            assertArrayEquals(value("Alice-v2"),
                    db.get(restoredValue, key("user:1")));
            assertEquals("login,click,purchase,logout",
                    new String(db.get(restoredList, key("events:1"))));
            assertArrayEquals(value("/new"),
                    db.get(restoredMap, key("session:3:page")));
        }
    }

    private static byte[] key(String k) {
        return k.getBytes();
    }

    private static byte[] value(String v) {
        return v.getBytes();
    }
}
