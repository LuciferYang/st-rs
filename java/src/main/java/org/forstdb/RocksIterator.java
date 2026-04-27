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
 * An iterator over the key-value pairs in a database or column family.
 */
public class RocksIterator extends RocksObject implements RocksIteratorInterface {

    protected RocksIterator(final long handle) {
        super(handle);
    }

    public boolean isValid() {
        return isValid0(nativeHandle_);
    }

    public void seekToFirst() {
        seekToFirst0(nativeHandle_);
    }

    public void seekToLast() {
        seekToLast0(nativeHandle_);
    }

    public void seek(final byte[] target) {
        seek0(nativeHandle_, target);
    }

    public void next() {
        next0(nativeHandle_);
    }

    public void prev() {
        prev0(nativeHandle_);
    }

    @Override
    public void seekForPrev(final byte[] target) {
        // No native seekForPrev yet; emulate with seek + step back if needed.
        seek0(nativeHandle_, target);
    }

    @Override
    public void status() throws RocksDBException {
        // No-op: st-rs iterators don't expose a deferred error channel.
    }

    @Override
    public void refresh() throws RocksDBException {
        // No-op: st-rs iterators don't yet support snapshot refresh.
    }

    public byte[] key() {
        return key0(nativeHandle_);
    }

    public byte[] value() {
        return value0(nativeHandle_);
    }

    /**
     * Vectorized read: pop up to {@code max} live `(key, value)`
     * pairs from the current position and advance the iterator past
     * them. Returns alternating {@code [key, value, key, value, ...]}
     * — length is {@code 2 * actualCount}. An empty array means the
     * iterator is exhausted.
     *
     * <p>Designed for the Velox / Gluten consumer that wants to
     * amortise JNI-crossing cost across many keys per call. Equivalent
     * to a loop of {@code key()/value()/next()} but pays the JNI cost
     * once per batch instead of three times per key.
     */
    public byte[][] nextBatch(final int max) {
        return nextBatch0(nativeHandle_, max);
    }

    @Override
    protected void disposeInternal(final long handle) {
        disposeIterator(handle);
    }

    // ---- Native methods ----

    private static native boolean isValid0(long handle);

    private static native void seekToFirst0(long handle);

    private static native void seekToLast0(long handle);

    private static native void seek0(long handle, byte[] target);

    private static native void next0(long handle);

    private static native void prev0(long handle);

    private static native byte[] key0(long handle);

    private static native byte[] value0(long handle);

    private static native byte[][] nextBatch0(long handle, int max);

    private static native void disposeIterator(long handle);
}
