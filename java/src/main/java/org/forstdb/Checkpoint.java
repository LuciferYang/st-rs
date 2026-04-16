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
 * Creates a point-in-time snapshot of the database on disk.
 */
public class Checkpoint {

    private final RocksDB db;

    private Checkpoint(final RocksDB db) {
        this.db = db;
    }

    public static Checkpoint create(final RocksDB db) {
        return new Checkpoint(db);
    }

    public void createCheckpoint(final String path)
            throws RocksDBException {
        createCheckpoint0(db.getNativeHandle(), path);
    }

    private static native void createCheckpoint0(long dbHandle, String path)
            throws RocksDBException;
}
