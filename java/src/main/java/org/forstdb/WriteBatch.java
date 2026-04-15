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
 * A batch of writes to be applied atomically.
 */
public class WriteBatch extends RocksObject {

    static {
        NativeLibraryLoader.load();
    }

    public WriteBatch() {
        super(newWriteBatch());
    }

    public void put(final ColumnFamilyHandle cf, final byte[] key,
            final byte[] value) throws RocksDBException {
        put(nativeHandle_, cf.getNativeHandle(), key, value);
    }

    public void put(final byte[] key, final byte[] value)
            throws RocksDBException {
        put(nativeHandle_, 0, key, value);
    }

    public void delete(final ColumnFamilyHandle cf, final byte[] key)
            throws RocksDBException {
        delete(nativeHandle_, cf.getNativeHandle(), key);
    }

    public void merge(final ColumnFamilyHandle cf, final byte[] key,
            final byte[] value) throws RocksDBException {
        merge(nativeHandle_, cf.getNativeHandle(), key, value);
    }

    public int count() {
        return count(nativeHandle_);
    }

    public void clear() {
        clear(nativeHandle_);
    }

    @Override
    protected void disposeInternal(final long handle) {
        disposeWriteBatch(handle);
    }

    // ---- Native methods ----

    private static native long newWriteBatch();

    private static native void put(long batchHandle, long cfHandle,
            byte[] key, byte[] value)
            throws RocksDBException;

    private static native void delete(long batchHandle, long cfHandle,
            byte[] key) throws RocksDBException;

    private static native void merge(long batchHandle, long cfHandle,
            byte[] key, byte[] value)
            throws RocksDBException;

    private static native int count(long batchHandle);

    private static native void clear(long batchHandle);

    private static native void disposeWriteBatch(long batchHandle);
}
