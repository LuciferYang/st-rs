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
 * Base class for all objects backed by a native handle.
 *
 * <p>Subclasses must implement {@link #disposeInternal(long)} to release
 * the native resource when {@link #close()} is called.</p>
 */
public abstract class RocksObject implements AutoCloseable {

    protected long nativeHandle_;

    protected RocksObject(final long nativeHandle) {
        this.nativeHandle_ = nativeHandle;
    }

    public long getNativeHandle() {
        return nativeHandle_;
    }

    @Override
    public synchronized void close() {
        if (nativeHandle_ != 0) {
            disposeInternal(nativeHandle_);
            nativeHandle_ = 0;
        }
    }

    protected abstract void disposeInternal(long handle);
}
