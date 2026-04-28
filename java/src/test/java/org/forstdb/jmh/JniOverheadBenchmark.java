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

package org.forstdb.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.DBOptions;
import org.forstdb.RocksDB;
import org.forstdb.RocksDBException;
import org.forstdb.RocksIterator;
import org.forstdb.WriteBatch;
import org.forstdb.WriteOptions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JNI-side counterpart to {@code benches/engine.rs}. Times the same
 * read/write paths from Java so the difference between Rust-direct and
 * Java-via-JNI numbers reveals per-call boundary overhead (byte-array
 * marshalling, GlobalRef churn, exception checks).
 *
 * <p>Run via: {@code cd java && mvn -Pbench test-compile exec:exec
 * -Dexec.executable=java}, or simpler — use the {@link BenchmarkRunner}
 * entry point: {@code mvn -Pbench test-compile exec:java}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class JniOverheadBenchmark {

    /** Population size for read benches; matches criterion's N_ENTRIES. */
    private static final int N_ENTRIES = 50_000;
    private static final int KEY_SIZE = 16;
    private static final int VALUE_SIZE = 100;

    /** Owned by the bench, recreated per @Setup. */
    private Path dbDir;
    private DBOptions opts;
    private RocksDB db;
    private ColumnFamilyHandle defaultCf;
    private WriteOptions writeOpts;
    private byte[] valueTemplate;

    /** Fast non-allocating PRNG state for read keys. */
    private long rngState;
    /** Sequential counter for write benches (avoids dup-key short-circuit). */
    private long writeCursor;

    private static byte[] keyOf(final long i) {
        final String s = String.format("%0" + KEY_SIZE + "d", i);
        return s.getBytes();
    }

    @Setup(Level.Trial)
    public void setupTrial() throws IOException, RocksDBException {
        dbDir = Files.createTempDirectory("st-rs-jmh-");
        opts = new DBOptions()
                .setCreateIfMissing(true)
                .setDbWriteBufferSize(4L * 1024L * 1024L);
        db = RocksDB.open(opts, dbDir.toString());
        defaultCf = db.getDefaultColumnFamily();
        writeOpts = new WriteOptions();
        valueTemplate = new byte[VALUE_SIZE];
        java.util.Arrays.fill(valueTemplate, (byte) 0x42);

        // Pre-populate so read benches don't pay fill cost.
        for (long i = 0; i < N_ENTRIES; i++) {
            db.put(keyOf(i), valueTemplate);
        }
        // Leave the DB flushed so reads exercise SST + bloom path
        // (matches the Rust open_populated() helper).
        db.flush();
        db.waitForPendingWork();

        rngState = 0x9E3779B97F4A7C15L;
        // Start writes after the populated range so we don't overwrite
        // and skew the LSM into a tiny working set.
        writeCursor = N_ENTRIES;
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws Exception {
        try {
            if (writeOpts != null) writeOpts.close();
            if (db != null) db.close();
            if (opts != null) opts.close();
        } finally {
            // Best-effort cleanup. JMH runs in an isolated JVM so the
            // file lock is released even if close() throws.
            deleteRecursively(dbDir);
        }
    }

    private static void deleteRecursively(final Path p) {
        if (p == null) return;
        try (java.util.stream.Stream<Path> walk = Files.walk(p)) {
            walk.sorted(java.util.Comparator.reverseOrder())
                    .forEach(f -> {
                        try { Files.deleteIfExists(f); } catch (IOException ignore) {}
                    });
        } catch (IOException ignore) {
        }
    }

    private long nextRandomKey() {
        // xorshift64; identical algorithm to the Rust criterion bench so
        // the access patterns line up.
        rngState ^= rngState << 13;
        rngState ^= rngState >>> 7;
        rngState ^= rngState << 17;
        long n = rngState % N_ENTRIES;
        return n < 0 ? n + N_ENTRIES : n;
    }

    // ------------------------------------------------------------------
    // Write paths
    // ------------------------------------------------------------------

    @Benchmark
    @Measurement(iterations = 5, time = 2)
    public void put_one() throws Exception {
        db.put(keyOf(writeCursor++), valueTemplate);
    }

    @State(Scope.Benchmark)
    public static class BatchSizeParam {
        @Param({"8", "64", "512"})
        public int n;
    }

    @Benchmark
    @Measurement(iterations = 5, time = 2)
    public void put_batch(final BatchSizeParam p) throws Exception {
        try (WriteBatch wb = new WriteBatch()) {
            for (int k = 0; k < p.n; k++) {
                wb.put(keyOf(writeCursor + k), valueTemplate);
            }
            db.write(writeOpts, wb);
            writeCursor += p.n;
        }
    }

    // ------------------------------------------------------------------
    // Read paths
    // ------------------------------------------------------------------

    @Benchmark
    @Measurement(iterations = 5, time = 2)
    public void get_hit(final Blackhole bh) throws Exception {
        bh.consume(db.get(keyOf(nextRandomKey())));
    }

    @Benchmark
    @Measurement(iterations = 5, time = 2)
    public void get_miss(final Blackhole bh) throws Exception {
        bh.consume(db.get(keyOf(N_ENTRIES + nextRandomKey())));
    }

    @Benchmark
    @Measurement(iterations = 3, time = 3)
    public void scan_forward(final Blackhole bh) throws Exception {
        try (RocksIterator it = db.newIterator(defaultCf)) {
            it.seekToFirst();
            int n = 0;
            while (it.isValid()) {
                n++;
                it.next();
            }
            bh.consume(n);
        }
    }

    @State(Scope.Benchmark)
    public static class ChunkSizeParam {
        @Param({"64", "256", "1024"})
        public int chunk;
    }

    @Benchmark
    @Measurement(iterations = 3, time = 3)
    public void next_batch(final ChunkSizeParam p, final Blackhole bh) throws Exception {
        try (RocksIterator it = db.newIterator(defaultCf)) {
            it.seekToFirst();
            int total = 0;
            while (true) {
                byte[][] chunk = it.nextBatch(p.chunk);
                if (chunk == null || chunk.length == 0) break;
                // nextBatch returns alternating key/value entries —
                // halve to count logical rows.
                total += chunk.length / 2;
            }
            bh.consume(total);
        }
    }
}
