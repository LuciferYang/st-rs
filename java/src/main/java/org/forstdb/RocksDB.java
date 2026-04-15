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

/**
 * A RocksDB database instance backed by the st-rs Rust engine via JNI.
 *
 * <p>Use {@link #open(DBOptions, String)} to create a database, then call
 * {@link #close()} when finished to release native resources.</p>
 */
public class RocksDB extends RocksObject {

    static {
        NativeLibraryLoader.load();
    }

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

    // --- Column Families ---

    public ColumnFamilyHandle createColumnFamily(final String name)
            throws RocksDBException {
        final long cfHandle = createColumnFamily(nativeHandle_, name);
        return new ColumnFamilyHandle(cfHandle);
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

    private static native void closeDatabase(long dbHandle);

    private static native void disposeInternalNative(long dbHandle);
}
