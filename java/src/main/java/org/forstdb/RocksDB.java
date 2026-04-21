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

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A RocksDB database instance backed by the st-rs Rust engine via JNI.
 *
 * <p>Use {@link #open(DBOptions, String)} to create a database, then call
 * {@link #close()} when finished to release native resources.</p>
 */
public class RocksDB extends RocksObject {

    public static final byte[] DEFAULT_COLUMN_FAMILY =
            "default".getBytes(StandardCharsets.UTF_8);

    static {
        NativeLibraryLoader.load();
    }

    /**
     * Compatibility shim for Flink's {@code ForStStateBackend}, which
     * calls {@code RocksDB.loadLibrary()} during backend init. The actual
     * load already happened in the static initializer above; this method
     * just ensures the symbol exists for callers that look it up by name.
     */
    public static void loadLibrary() {
        NativeLibraryLoader.load();
    }

    private WriteOptions defaultWriteOptions = null;
    private ReadOptions defaultReadOptions = null;

    private RocksDB(final long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Opens a database with the given options at the specified path.
     *
     * @param options the database options
     * @param path    the filesystem path for the database
     * @return a new RocksDB instance
     * @throws RocksDBException if the database cannot be opened
     */
    public static RocksDB open(final DBOptions options, final String path)
            throws RocksDBException {
        final long handle = open(options.getNativeHandle(), path);
        return new RocksDB(handle);
    }

    // --- Get ---

    public byte[] get(final ColumnFamilyHandle cf, final byte[] key)
            throws RocksDBException {
        return get(nativeHandle_, cf.getNativeHandle(), key);
    }

    public byte[] get(final byte[] key) throws RocksDBException {
        return get(nativeHandle_, 0, key);
    }

    // --- Put ---

    public void put(final ColumnFamilyHandle cf, final WriteOptions opts,
            final byte[] key, final byte[] value) throws RocksDBException {
        put(nativeHandle_, cf.getNativeHandle(), opts.getNativeHandle(),
                key, value);
    }

    public void put(final byte[] key, final byte[] value)
            throws RocksDBException {
        put(nativeHandle_, 0, 0, key, value);
    }

    // --- Delete ---

    public void delete(final ColumnFamilyHandle cf, final WriteOptions opts,
            final byte[] key) throws RocksDBException {
        delete(nativeHandle_, cf.getNativeHandle(), opts.getNativeHandle(),
                key);
    }

    // --- Merge ---

    public void merge(final ColumnFamilyHandle cf, final WriteOptions opts,
            final byte[] key, final byte[] value) throws RocksDBException {
        merge(nativeHandle_, cf.getNativeHandle(), opts.getNativeHandle(),
                key, value);
    }

    // --- WriteBatch ---

    public void write(final WriteOptions opts, final WriteBatch batch)
            throws RocksDBException {
        write(nativeHandle_, opts.getNativeHandle(), batch.getNativeHandle());
    }

    // --- Flush ---

    public void flush() throws RocksDBException {
        flush(nativeHandle_);
    }

    public void flush(final ColumnFamilyHandle cf) throws RocksDBException {
        flushCf(nativeHandle_, cf.getNativeHandle());
    }

    // --- Property ---

    public String getProperty(final String name) throws RocksDBException {
        return getProperty(nativeHandle_, name);
    }

    // --- File Deletion Control ---

    public void disableFileDeletions() throws RocksDBException {
        disableFileDeletions(nativeHandle_);
    }

    public void enableFileDeletions(final boolean force)
            throws RocksDBException {
        enableFileDeletions(nativeHandle_, force);
    }

    // --- Snapshots ---

    public Snapshot getSnapshot() {
        final long snapHandle = getSnapshot(nativeHandle_);
        return new Snapshot(snapHandle);
    }

    public void releaseSnapshot(final Snapshot snap) {
        releaseSnapshot(nativeHandle_, snap.getNativeHandle());
    }

    // --- Multi-CF Open ---

    /**
     * Opens a database with multiple column families.
     *
     * @param options   the database options
     * @param path      the filesystem path for the database
     * @param cfDescs   column family descriptors
     * @param cfHandles output list populated with column family handles
     * @return a new RocksDB instance
     * @throws RocksDBException if the database cannot be opened
     */
    public static RocksDB open(final DBOptions options, final String path,
            final List<ColumnFamilyDescriptor> cfDescs,
            final List<ColumnFamilyHandle> cfHandles)
            throws RocksDBException {
        // Open the DB (recovers existing CFs from MANIFEST).
        final RocksDB db = open(options, path);
        // For each requested CF, look up existing or create new.
        for (final ColumnFamilyDescriptor desc : cfDescs) {
            final String name = desc.getNameAsString();
            // Try to find an existing CF recovered from MANIFEST.
            final long existingHandle = getColumnFamilyByName(
                    db.nativeHandle_, name);
            if (existingHandle != 0) {
                cfHandles.add(new ColumnFamilyHandle(existingHandle));
            } else if (!"default".equals(name)) {
                // CF does not exist yet — create it.
                final ColumnFamilyHandle cfh = db.createColumnFamily(name);
                cfHandles.add(cfh);
            } else {
                // Default CF always exists with handle convention 0.
                cfHandles.add(new ColumnFamilyHandle(0));
            }
        }
        return db;
    }

    // --- Column Families ---

    public ColumnFamilyHandle createColumnFamily(final String name)
            throws RocksDBException {
        final long cfHandle = createColumnFamily(nativeHandle_, name);
        return new ColumnFamilyHandle(cfHandle);
    }

    /**
     * Create a column family from a descriptor. The descriptor's options
     * are stored on the returned handle (via {@link ColumnFamilyHandle#getDescriptor})
     * but are not yet handed to the engine — that's a follow-up. The
     * native engine still uses defaults; this overload exists so Flink's
     * {@code ForStOperationUtils.createColumnFamily} can register CFs.
     */
    public ColumnFamilyHandle createColumnFamily(final ColumnFamilyDescriptor desc)
            throws RocksDBException {
        final long cfHandle = createColumnFamily(
                nativeHandle_, desc.getNameAsString());
        return new ColumnFamilyHandle(cfHandle, desc);
    }

    public void dropColumnFamily(final ColumnFamilyHandle cf)
            throws RocksDBException {
        dropColumnFamily0(nativeHandle_, cf.getNativeHandle());
    }

    // --- Iterators ---

    public RocksIterator newIterator(final ColumnFamilyHandle cf) {
        final long iterHandle = newIterator(nativeHandle_,
                cf.getNativeHandle());
        return new RocksIterator(iterHandle);
    }

    public RocksIterator newIterator(final ColumnFamilyHandle cf,
            final ReadOptions opts) {
        final long iterHandle = newIteratorCf(nativeHandle_,
                cf.getNativeHandle(), opts.getNativeHandle());
        return new RocksIterator(iterHandle);
    }

    // --- Metadata ---

    public List<LiveFileMetaData> getLiveFilesMetaData()
            throws RocksDBException {
        final byte[] raw = getLiveFilesMetaData0(nativeHandle_);
        if (raw == null || raw.length == 0) {
            return java.util.Collections.emptyList();
        }
        return parseLiveFilesMetaData(raw);
    }

    private static List<LiveFileMetaData> parseLiveFilesMetaData(
            final byte[] raw) {
        final java.nio.ByteBuffer buf =
                java.nio.ByteBuffer.wrap(raw);
        final int count = buf.getInt();
        final List<LiveFileMetaData> result =
                new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final int cfNameLen = buf.getInt();
            final byte[] cfNameBytes = new byte[cfNameLen];
            buf.get(cfNameBytes);
            final String cfName = new String(cfNameBytes,
                    StandardCharsets.UTF_8);
            final long fileNumber = buf.getLong();
            final int level = buf.getInt();
            final long fileSize = buf.getLong();
            final int smallestLen = buf.getInt();
            final byte[] smallest = new byte[smallestLen];
            buf.get(smallest);
            final int largestLen = buf.getInt();
            final byte[] largest = new byte[largestLen];
            buf.get(largest);
            final String fileName = String.valueOf(fileNumber) + ".sst";
            result.add(new LiveFileMetaData(cfName, fileName, level,
                    fileSize, smallest, largest));
        }
        return result;
    }

    // --- Range Operations ---

    public void deleteRange(final ColumnFamilyHandle cf,
            final byte[] begin, final byte[] end)
            throws RocksDBException {
        deleteRange0(nativeHandle_, cf.getNativeHandle(), begin, end);
    }

    public void deleteFilesInRanges(final ColumnFamilyHandle cf,
            final List<byte[]> ranges, final boolean force)
            throws RocksDBException {
        final byte[][] rangeArray = ranges.toArray(new byte[0][]);
        deleteFilesInRanges0(nativeHandle_, cf.getNativeHandle(),
                rangeArray);
    }

    public void compactRange(final ColumnFamilyHandle cf)
            throws RocksDBException {
        compactRange0(nativeHandle_, cf.getNativeHandle());
    }

    // --- Default Options Accessors ---

    public WriteOptions getWriteOptions() {
        if (defaultWriteOptions == null) {
            defaultWriteOptions = new WriteOptions();
        }
        return defaultWriteOptions;
    }

    public ReadOptions getReadOptions() {
        if (defaultReadOptions == null) {
            defaultReadOptions = new ReadOptions();
        }
        return defaultReadOptions;
    }

    // --- Close ---

    @Override
    public synchronized void close() {
        if (nativeHandle_ != 0) {
            closeDatabase(nativeHandle_);
        }
        super.close();
    }

    @Override
    protected void disposeInternal(final long handle) {
        disposeInternalNative(handle);
    }

    // ---- Native methods ----

    private static native long open(long optionsHandle, String path)
            throws RocksDBException;

    private static native byte[] get(long dbHandle, long cfHandle,
            byte[] key) throws RocksDBException;

    private static native void put(long dbHandle, long cfHandle,
            long writeOptsHandle, byte[] key,
            byte[] value) throws RocksDBException;

    private static native void delete(long dbHandle, long cfHandle,
            long writeOptsHandle, byte[] key)
            throws RocksDBException;

    private static native void merge(long dbHandle, long cfHandle,
            long writeOptsHandle, byte[] key,
            byte[] value) throws RocksDBException;

    private static native void write(long dbHandle, long writeOptsHandle,
            long batchHandle) throws RocksDBException;

    private static native void flush(long dbHandle) throws RocksDBException;
    private static native void flushCf(long dbHandle, long cfHandle) throws RocksDBException;

    private static native String getProperty(long dbHandle, String name)
            throws RocksDBException;

    private static native void disableFileDeletions(long dbHandle)
            throws RocksDBException;

    private static native void enableFileDeletions(long dbHandle,
            boolean force) throws RocksDBException;

    private static native long getSnapshot(long dbHandle);

    private static native void releaseSnapshot(long dbHandle,
            long snapshotHandle);

    private static native long createColumnFamily(long dbHandle, String name)
            throws RocksDBException;

    private static native long getColumnFamilyByName(long dbHandle, String name);

    private static native long newIterator(long dbHandle, long cfHandle);

    private static native long newIteratorCf(long dbHandle, long cfHandle,
            long readOptsHandle);

    private static native void deleteRange0(long dbHandle, long cfHandle,
            byte[] begin, byte[] end) throws RocksDBException;

    private static native void dropColumnFamily0(long dbHandle,
            long cfHandle) throws RocksDBException;

    private static native void compactRange0(long dbHandle, long cfHandle)
            throws RocksDBException;

    private static native void deleteFilesInRanges0(long dbHandle,
            long cfHandle, byte[][] ranges) throws RocksDBException;

    private static native byte[] getLiveFilesMetaData0(long dbHandle)
            throws RocksDBException;

    private static native void closeDatabase(long dbHandle);

    private static native void disposeInternalNative(long dbHandle);
}
