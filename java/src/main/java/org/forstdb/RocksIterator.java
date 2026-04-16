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
public class RocksIterator extends RocksObject {

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

    public byte[] key() {
        return key0(nativeHandle_);
    }

    public byte[] value() {
        return value0(nativeHandle_);
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

    private static native void disposeIterator(long handle);
}
