/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.forstdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the vectorized {@link RocksIterator#nextBatch} surface
 * end-to-end: write data, pull batches across the JNI boundary,
 * confirm key/value bytes round-trip cleanly.
 *
 * <p>Designed so a Velox / Gluten consumer can replace per-key
 * {@code next()/key()/value()} loops with a single batched call.
 */
class BatchIteratorTest {

    @Test
    void nextBatch_returnsRequestedCountAndAdvances(@TempDir Path dbDir)
            throws Exception {
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true)) {
            final RocksDB db = RocksDB.open(opts, dbDir.toString());
            for (int i = 0; i < 50; i++) {
                final String k = String.format("k%04d", i);
                db.put(k.getBytes(), k.getBytes());
            }

            try (RocksIterator it = db.newIterator(new ColumnFamilyHandle(0))) {
                it.seekToFirst();

                final byte[][] batch = it.nextBatch(10);
                // 10 entries × 2 (key + value).
                assertEquals(20, batch.length);
                assertArrayEquals("k0000".getBytes(), batch[0]);
                assertArrayEquals("k0000".getBytes(), batch[1]);
                assertArrayEquals("k0009".getBytes(), batch[18]);
                assertArrayEquals("k0009".getBytes(), batch[19]);

                final byte[][] batch2 = it.nextBatch(10);
                assertEquals(20, batch2.length);
                assertArrayEquals("k0010".getBytes(), batch2[0]);
            }

            db.close();
        }
    }

    @Test
    void nextBatch_underfillsAndThenReturnsEmpty(@TempDir Path dbDir)
            throws Exception {
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true)) {
            final RocksDB db = RocksDB.open(opts, dbDir.toString());
            for (int i = 0; i < 7; i++) {
                final String k = String.format("k%04d", i);
                db.put(k.getBytes(), k.getBytes());
            }

            try (RocksIterator it = db.newIterator(new ColumnFamilyHandle(0))) {
                it.seekToFirst();
                final byte[][] batch = it.nextBatch(1000);
                // Only 7 entries available.
                assertEquals(14, batch.length);
                // Next call hits exhaustion.
                final byte[][] empty = it.nextBatch(10);
                assertEquals(0, empty.length);
            }

            db.close();
        }
    }

    @Test
    void nextBatch_drainsLargeDataset(@TempDir Path dbDir) throws Exception {
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true)) {
            final RocksDB db = RocksDB.open(opts, dbDir.toString());
            // 5000 entries — exercises multiple internal chunk refills
            // (default chunk_size=1024) within a single nextBatch call.
            final int n = 5000;
            for (int i = 0; i < n; i++) {
                final String k = String.format("k%06d", i);
                db.put(k.getBytes(), k.getBytes());
            }

            try (RocksIterator it = db.newIterator(new ColumnFamilyHandle(0))) {
                it.seekToFirst();
                final List<byte[]> keys = new ArrayList<>(n);
                while (true) {
                    final byte[][] batch = it.nextBatch(512);
                    if (batch.length == 0) {
                        break;
                    }
                    for (int i = 0; i < batch.length; i += 2) {
                        keys.add(batch[i]);
                    }
                }
                assertEquals(n, keys.size());
                assertArrayEquals("k000000".getBytes(), keys.get(0));
                assertArrayEquals(
                        String.format("k%06d", n - 1).getBytes(),
                        keys.get(n - 1));
            }

            db.close();
        }
    }

    @Test
    void nextBatch_zeroReturnsEmptyWithoutAdvancing(@TempDir Path dbDir)
            throws Exception {
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true)) {
            final RocksDB db = RocksDB.open(opts, dbDir.toString());
            db.put("k".getBytes(), "v".getBytes());

            try (RocksIterator it = db.newIterator(new ColumnFamilyHandle(0))) {
                it.seekToFirst();
                assertTrue(it.isValid());
                final byte[][] batch = it.nextBatch(0);
                assertEquals(0, batch.length);
                // Iterator unchanged — key()/value() still work.
                assertArrayEquals("k".getBytes(), it.key());
                assertArrayEquals("v".getBytes(), it.value());
            }

            db.close();
        }
    }

    @Test
    void nextBatchPacked_roundTripsKeyValuePairs(@TempDir Path dbDir)
            throws Exception {
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true)) {
            final RocksDB db = RocksDB.open(opts, dbDir.toString());
            for (int i = 0; i < 25; i++) {
                final String k = String.format("k%04d", i);
                final String v = "value-of-" + k;
                db.put(k.getBytes(), v.getBytes());
            }

            try (RocksIterator it = db.newIterator(new ColumnFamilyHandle(0))) {
                it.seekToFirst();

                final byte[] packed = it.nextBatchPacked(10);
                final List<byte[]> keys = new ArrayList<>();
                final List<byte[]> values = new ArrayList<>();
                decodePacked(packed, keys, values);

                assertEquals(10, keys.size());
                assertEquals(10, values.size());
                for (int i = 0; i < 10; i++) {
                    final String expectedK = String.format("k%04d", i);
                    assertArrayEquals(expectedK.getBytes(), keys.get(i));
                    assertArrayEquals(("value-of-" + expectedK).getBytes(), values.get(i));
                }

                // Pull the rest and verify exhaustion.
                final byte[] rest = it.nextBatchPacked(100);
                final List<byte[]> restKeys = new ArrayList<>();
                final List<byte[]> restValues = new ArrayList<>();
                decodePacked(rest, restKeys, restValues);
                assertEquals(15, restKeys.size());

                final byte[] empty = it.nextBatchPacked(10);
                final List<byte[]> emptyKeys = new ArrayList<>();
                final List<byte[]> emptyValues = new ArrayList<>();
                decodePacked(empty, emptyKeys, emptyValues);
                assertEquals(0, emptyKeys.size());
            }

            db.close();
        }
    }

    /**
     * Decode the big-endian packed buffer produced by
     * {@link RocksIterator#nextBatchPacked(int)} into parallel
     * key / value lists. Test helper, not for production paths
     * where consumers can avoid the per-pair byte[] copy.
     */
    private static void decodePacked(
            final byte[] packed,
            final List<byte[]> keys,
            final List<byte[]> values) {
        final ByteBuffer bb = ByteBuffer.wrap(packed);
        final int count = bb.getInt();
        for (int i = 0; i < count; i++) {
            final int keyLen = bb.getInt();
            final byte[] key = new byte[keyLen];
            bb.get(key);
            keys.add(key);
            final int valLen = bb.getInt();
            final byte[] val = new byte[valLen];
            bb.get(val);
            values.add(val);
        }
    }
}
