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
 * A point-in-time view of the database.
 *
 * <p>Snapshots are created via {@link RocksDB#getSnapshot()} and released
 * via {@link RocksDB#releaseSnapshot(Snapshot)}.</p>
 */
public class Snapshot {

    private final long nativeHandle_;

    Snapshot(final long nativeHandle) {
        this.nativeHandle_ = nativeHandle;
    }

    public long getNativeHandle() {
        return nativeHandle_;
    }
}
