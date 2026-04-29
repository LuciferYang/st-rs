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
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Reference benchmark against upstream RocksDB JNI on the same workloads
 * as {@link JniOverheadBenchmark}. JMH reports both classes' methods
 * side-by-side, so {@code JniOverheadBenchmark.get_hit} vs
 * {@code RocksdbBenchmark.get_hit} is a direct apples-to-apples ratio.
 *
 * <p>Notes on fairness:
 * <ul>
 *   <li>Same key/value sizes (16 / 100 bytes), same N_ENTRIES (50k),
 *       same 4 MiB write-buffer cap, same xorshift PRNG for read
 *       access patterns.</li>
 *   <li>RocksDB's default block cache is left at its 32 MiB default,
 *       which is comparable to st-rs's untuned default. We deliberately
 *       do NOT disable bloom filters or compression on either side —
 *       both engines run with their out-of-box configuration.</li>
 *   <li>RocksDB ships its native lib inside the JAR and extracts it at
 *       runtime via {@code NativeLibraryLoader}, so no extra setup is
 *       needed beyond declaring the dep in the {@code bench} profile.</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class RocksdbBenchmark {

    private static final int N_ENTRIES = 50_000;
    private static final int KEY_SIZE = 16;
    private static final int VALUE_SIZE = 100;

    static {
        RocksDB.loadLibrary();
    }

    private Path dbDir;
    private Options opts;
    private RocksDB db;
    private WriteOptions writeOpts;
    private byte[] valueTemplate;

    private long rngState;
    private long writeCursor;

    private static byte[] keyOf(final long i) {
        return String.format("%0" + KEY_SIZE + "d", i).getBytes();
    }

    @Setup(Level.Trial)
    public void setupTrial() throws IOException, RocksDBException {
        dbDir = Files.createTempDirectory("rocksdb-jmh-");
        opts = new Options()
                .setCreateIfMissing(true)
                .setDbWriteBufferSize(4L * 1024L * 1024L);
        db = RocksDB.open(opts, dbDir.toString());
        writeOpts = new WriteOptions();
        valueTemplate = new byte[VALUE_SIZE];
        java.util.Arrays.fill(valueTemplate, (byte) 0x42);

        for (long i = 0; i < N_ENTRIES; i++) {
            db.put(keyOf(i), valueTemplate);
        }
        db.flush(new org.rocksdb.FlushOptions().setWaitForFlush(true));

        rngState = 0x9E3779B97F4A7C15L;
        writeCursor = N_ENTRIES;
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        try {
            if (writeOpts != null) writeOpts.close();
            if (db != null) db.close();
            if (opts != null) opts.close();
        } finally {
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
        try (RocksIterator it = db.newIterator()) {
            it.seekToFirst();
            int n = 0;
            while (it.isValid()) {
                n++;
                it.next();
            }
            bh.consume(n);
        }
    }
}
