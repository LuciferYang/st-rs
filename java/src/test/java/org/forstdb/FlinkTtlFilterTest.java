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
 * Proves that the Flink-style TTL compaction filter installed via
 * {@link FlinkCompactionFilter.FlinkCompactionFilterFactory#configure} +
 * {@link ColumnFamilyOptions#setCompactionFilterFactory} actually drops
 * expired entries during compaction on the st-rs engine side.
 */
class FlinkTtlFilterTest {

    /**
     * Build a Flink-style value: 8 bytes big-endian timestamp followed by
     * the user payload. Matches the Value-state layout (timestampOffset=0)
     * that Flink's {@code FlinkCompactionFilter.Config.createForValue} uses.
     */
    private static byte[] valueWithTimestamp(final long timestampMillis,
            final byte[] payload) {
        final byte[] out = new byte[8 + payload.length];
        ByteBuffer.wrap(out).putLong(timestampMillis);
        System.arraycopy(payload, 0, out, 8, payload.length);
        return out;
    }

    @Test
    void expiredEntriesRemovedOnCompact(@TempDir Path dbDir) throws Exception {
        // Tiny write buffer matches the existing Rust filter test setup;
        // with the default 64MB buffer, our flushes don't reliably create
        // L0 SSTs that hit the compaction trigger.
        final DBOptions opts = new DBOptions()
                .setCreateIfMissing(true)
                .setDbWriteBufferSize(64L * 1024L);
        final RocksDB db = RocksDB.open(opts, dbDir.toString());

        final FlinkCompactionFilter.FlinkCompactionFilterFactory factory =
                new FlinkCompactionFilter.FlinkCompactionFilterFactory(null, null);
        // TTL 1 second; value-state means timestamp at offset 0.
        factory.configure(FlinkCompactionFilter.Config.createForValue(1000L, 0L));

        // Use a custom CF — same shape Flink uses for keyed state. M2.5
        // taught the picker to scan all CFs, so the filter actually runs.
        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        cfOpts.setCompactionFilterFactory(factory);
        final ColumnFamilyDescriptor desc = new ColumnFamilyDescriptor(
                "ttl-cf", cfOpts);
        final ColumnFamilyHandle cf = db.createColumnFamily(desc);

        final long now = System.currentTimeMillis();
        final byte[] freshKey = "fresh".getBytes();
        final byte[] staleKey = "stale".getBytes();
        final byte[] freshPayload = "alive".getBytes();
        final byte[] stalePayload = "dead".getBytes();

        try (WriteOptions wo = new WriteOptions()) {
            db.put(cf, wo, freshKey, valueWithTimestamp(now, freshPayload));
            db.put(cf, wo, staleKey,
                    valueWithTimestamp(now - 60_000L, stalePayload));
            // st-rs's compactRange is a no-op today; trigger automatic
            // compaction by producing enough L0 SSTs to cross the L0 file
            // threshold (level0_file_num_compaction_trigger=4 by default).
            for (int i = 0; i < 5; i++) {
                db.put(cf, wo, ("filler" + i).getBytes(),
                        valueWithTimestamp(now, ("f" + i).getBytes()));
                db.flush(cf);
            }
        }
        // Block until background compaction completes (no sleep race).
        db.waitForPendingWork();

        final byte[] freshOut = db.get(cf, freshKey);
        assertNotNull(freshOut, "fresh entry should survive compaction");
        final byte[] freshPayloadOut = new byte[freshPayload.length];
        System.arraycopy(freshOut, 8, freshPayloadOut, 0, freshPayload.length);
        assertArrayEquals(freshPayload, freshPayloadOut);

        final byte[] staleOut = db.get(cf, staleKey);
        assertNull(staleOut, "stale entry should have been filtered out");

        factory.close();
        cf.close();
        db.close();
    }
}
