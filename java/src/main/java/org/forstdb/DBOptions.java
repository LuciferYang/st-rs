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
 * Options controlling database behavior.
 */
public class DBOptions extends RocksObject {

    static {
        NativeLibraryLoader.load();
    }

    public DBOptions() {
        super(newDBOptions());
    }

    /**
     * Sets whether the database should be created if it does not exist.
     *
     * @param flag true to create the database if missing
     * @return this instance for method chaining
     */
    public DBOptions setCreateIfMissing(final boolean flag) {
        setCreateIfMissing(nativeHandle_, flag);
        return this;
    }

    @Override
    protected void disposeInternal(final long handle) {
        disposeDBOptions(handle);
    }

    // ---- Native methods ----

    private static native long newDBOptions();

    private static native void setCreateIfMissing(long handle, boolean flag);

    private static native void disposeDBOptions(long handle);
}
