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
 * Handle to a column family in the database.
 */
public class ColumnFamilyHandle extends RocksObject {

    private final ColumnFamilyDescriptor descriptor;

    ColumnFamilyHandle(final long nativeHandle) {
        this(nativeHandle, null);
    }

    ColumnFamilyHandle(final long nativeHandle,
            final ColumnFamilyDescriptor descriptor) {
        super(nativeHandle);
        this.descriptor = descriptor;
    }

    @Override
    public long getNativeHandle() {
        return nativeHandle_;
    }

    /**
     * Returns the descriptor used to create this CF, or {@code null} if
     * this handle was recovered from MANIFEST without one. Mirrors
     * upstream's accessor; Flink's {@code ForStOperationUtils} reads
     * options off the descriptor and is null-tolerant.
     */
    public ColumnFamilyDescriptor getDescriptor() throws RocksDBException {
        return descriptor;
    }

    @Override
    protected void disposeInternal(final long handle) {
        disposeColumnFamilyHandle(handle);
    }

    private static native void disposeColumnFamilyHandle(long handle);
}
