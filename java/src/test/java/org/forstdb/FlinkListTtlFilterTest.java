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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Proves that the reverse-JNI list-element TTL compaction filter actually
 * runs end-to-end: configure a {@link FlinkCompactionFilter.Config} with a
 * Java {@link FlinkCompactionFilter.ListElementFilterFactory}, write a
 * mixture of expired and fresh list-state values, trigger compaction, and
 * verify the engine called back into Java to truncate / drop them.
 */
class FlinkListTtlFilterTest {

    /**
     * Simple framed list serialization: 4-byte big-endian element length
     * followed by 8-byte big-endian timestamp + payload bytes. Matches the
     * shape Flink uses for variable-length element lists where each entry
     * carries its own TTL stamp.
     */
    private static byte[] frameElement(final long timestampMillis, final byte[] payload) {
        final int len = 8 + payload.length;
        final byte[] out = new byte[4 + len];
        ByteBuffer.wrap(out).putInt(len).putLong(timestampMillis);
        System.arraycopy(payload, 0, out, 12, payload.length);
        return out;
    }

    private static byte[] concat(final byte[]... parts) {
        int total = 0;
        for (final byte[] p : parts) {
            total += p.length;
        }
        final byte[] out = new byte[total];
        int pos = 0;
        for (final byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }

    /**
     * Walks the framed list and returns the offset of the first element
     * whose timestamp is within the TTL window. Mirrors what a real Flink
     * {@link FlinkCompactionFilter.ListElementFilter} would compute.
     */
    static final class FramedListFilter
            implements FlinkCompactionFilter.ListElementFilter {
        @Override
        public int nextUnexpiredOffset(
                final byte[] list, final long ttl, final long currentTimestamp) {
            int pos = 0;
            while (pos < list.length) {
                if (pos + 4 > list.length) {
                    return list.length;
                }
                final int len = ByteBuffer.wrap(list, pos, 4).getInt();
                if (pos + 4 + len > list.length || len < 8) {
                    return list.length;
                }
                final long ts = ByteBuffer.wrap(list, pos + 4, 8).getLong();
                if (currentTimestamp - ts <= ttl) {
                    return pos;
                }
                pos += 4 + len;
            }
            return list.length;
        }
    }

    static final class FramedListFilterFactory
            implements FlinkCompactionFilter.ListElementFilterFactory {
        @Override
        public FlinkCompactionFilter.ListElementFilter createListElementFilter() {
            return new FramedListFilter();
        }
    }

    @Test
    void listEntriesTruncatedOnCompact(@TempDir Path dbDir) throws Exception {
        final DBOptions opts = new DBOptions()
                .setCreateIfMissing(true)
                .setDbWriteBufferSize(64L * 1024L);
        final RocksDB db = RocksDB.open(opts, dbDir.toString());

        final FlinkCompactionFilter.FlinkCompactionFilterFactory factory =
                new FlinkCompactionFilter.FlinkCompactionFilterFactory(null, null);
        // TTL 1 second; element-level filter via reverse-JNI.
        factory.configure(FlinkCompactionFilter.Config.createForList(
                1000L, 0L, new FramedListFilterFactory()));

        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        cfOpts.setCompactionFilterFactory(factory);
        final ColumnFamilyDescriptor desc = new ColumnFamilyDescriptor(
                "list-ttl-cf", cfOpts);
        final ColumnFamilyHandle cf = db.createColumnFamily(desc);

        final long now = System.currentTimeMillis();
        final byte[] mixedKey = "mixed".getBytes();
        final byte[] allFreshKey = "fresh".getBytes();
        final byte[] allStaleKey = "stale".getBytes();

        // mixed: two expired prefix elements, then a fresh tail element.
        final byte[] mixed = concat(
                frameElement(now - 60_000L, "old1".getBytes()),
                frameElement(now - 30_000L, "old2".getBytes()),
                frameElement(now, "alive".getBytes()));
        // all-fresh: every element within the TTL window.
        final byte[] allFresh = concat(
                frameElement(now, "a".getBytes()),
                frameElement(now, "b".getBytes()));
        // all-stale: every element expired.
        final byte[] allStale = concat(
                frameElement(now - 60_000L, "x".getBytes()),
                frameElement(now - 60_000L, "y".getBytes()));

        try (WriteOptions wo = new WriteOptions()) {
            db.put(cf, wo, mixedKey, mixed);
            db.put(cf, wo, allFreshKey, allFresh);
            db.put(cf, wo, allStaleKey, allStale);
            // Force enough L0 files to cross the auto-compaction trigger.
            for (int i = 0; i < 5; i++) {
                db.put(cf, wo, ("filler" + i).getBytes(),
                        frameElement(now, ("f" + i).getBytes()));
                db.flush(cf);
            }
        }
        db.waitForPendingWork();

        // all-fresh value should be untouched (filter returns offset 0).
        final byte[] freshOut = db.get(cf, allFreshKey);
        assertNotNull(freshOut, "all-fresh list must survive");
        assertArrayEquals(allFresh, freshOut,
                "all-fresh list value must be byte-identical");

        // all-stale value should be dropped entirely.
        final byte[] staleOut = db.get(cf, allStaleKey);
        assertNull(staleOut, "all-stale list must be removed");

        // mixed list should keep only the fresh tail element.
        final byte[] expectedTail = frameElement(now, "alive".getBytes());
        final byte[] mixedOut = db.get(cf, mixedKey);
        assertNotNull(mixedOut, "mixed list must survive (truncated)");
        assertArrayEquals(expectedTail, mixedOut,
                "mixed list must be truncated to its fresh tail element");

        factory.close();
        cf.close();
        db.close();
    }
}
