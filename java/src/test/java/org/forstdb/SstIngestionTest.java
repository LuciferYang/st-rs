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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies {@link RocksDB#ingestExternalFile} and
 * {@link RocksDB#createColumnFamilyWithImport} — the SST-ingestion path
 * Flink uses to bootstrap keyed-state CFs from a checkpoint on
 * recovery / rescaling.
 *
 * <p>Source SST files are produced by writing into one DB, flushing,
 * then reading {@link RocksDB#getLiveFilesMetaData()} for the on-disk
 * paths. The same files are then ingested into a fresh second DB.
 */
class SstIngestionTest {

    private static List<String> liveSstPaths(final Path dbDir,
            final RocksDB db) throws RocksDBException {
        final List<String> out = new ArrayList<>();
        for (final LiveFileMetaData meta : db.getLiveFilesMetaData()) {
            // fileName() is just the bare SST name (e.g. "000004.sst");
            // resolve against the DB directory to get a real path.
            out.add(dbDir.resolve(meta.fileName()).toString());
        }
        return out;
    }

    @Test
    void ingestExternalFile_intoExistingCf_makesKeysVisible(
            @TempDir Path srcDir, @TempDir Path dstDir) throws Exception {
        // 1. Build SSTs by writing data into a source DB and flushing.
        final List<String> sstPaths;
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
                WriteOptions wo = new WriteOptions()) {
            final RocksDB src = RocksDB.open(opts, srcDir.toString());
            src.put("ik1".getBytes(), "iv1".getBytes());
            src.put("ik2".getBytes(), "iv2".getBytes());
            src.flush();
            sstPaths = liveSstPaths(srcDir, src);
            src.close();
        }
        assertFalse(sstPaths.isEmpty(), "source DB produced no SSTs");

        // 2. Ingest into a fresh destination DB on a brand-new CF.
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true)) {
            final RocksDB dst = RocksDB.open(opts, dstDir.toString());
            final ColumnFamilyHandle cf = dst.createColumnFamily(
                    new ColumnFamilyDescriptor("ingest-cf",
                            new ColumnFamilyOptions()));

            dst.ingestExternalFile(cf, sstPaths,
                    new IngestExternalFileOptions().setMoveFiles(false));

            // 3. Verify ingested keys are queryable on the destination.
            assertArrayEquals("iv1".getBytes(), dst.get(cf, "ik1".getBytes()));
            assertArrayEquals("iv2".getBytes(), dst.get(cf, "ik2".getBytes()));

            // 4. Sanity: a key that wasn't in the source SST is still missing.
            assertNull(dst.get(cf, "missing".getBytes()));

            cf.close();
            dst.close();
        }
    }

    @Test
    void createColumnFamilyWithImport_atomicallyBootstrapsCf(
            @TempDir Path srcDir, @TempDir Path dstDir) throws Exception {
        // 1. Build SSTs identically to the previous test.
        final List<String> sstPaths;
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true)) {
            final RocksDB src = RocksDB.open(opts, srcDir.toString());
            src.put("alpha".getBytes(), "1".getBytes());
            src.put("beta".getBytes(), "2".getBytes());
            src.put("gamma".getBytes(), "3".getBytes());
            src.flush();
            sstPaths = liveSstPaths(srcDir, src);
            src.close();
        }

        // 2. Use createColumnFamilyWithImport — the actual path Flink
        //    takes during keyed-state restore. Wrap the SST paths in an
        //    ExportImportFilesMetaData (Flink would normally get this
        //    from a checkpoint export).
        try (DBOptions opts = new DBOptions().setCreateIfMissing(true);
                ImportColumnFamilyOptions importOpts =
                        new ImportColumnFamilyOptions().setMoveFiles(false)) {
            final RocksDB dst = RocksDB.open(opts, dstDir.toString());
            final ColumnFamilyDescriptor desc = new ColumnFamilyDescriptor(
                    "restored-cf", new ColumnFamilyOptions());

            final ExportImportFilesMetaData metadata =
                    new ExportImportFilesMetaData(sstPaths);

            final ColumnFamilyHandle cf = dst.createColumnFamilyWithImport(
                    desc, importOpts, List.of(metadata));

            assertArrayEquals("1".getBytes(), dst.get(cf, "alpha".getBytes()));
            assertArrayEquals("2".getBytes(), dst.get(cf, "beta".getBytes()));
            assertArrayEquals("3".getBytes(), dst.get(cf, "gamma".getBytes()));

            cf.close();
            dst.close();
        }
    }
}
