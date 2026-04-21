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
 * Metadata returned from an SST file export operation.
 *
 * <p>Upstream RocksDB stores everything on the native side and exposes
 * an opaque handle. st-rs has no engine-side export yet, so we keep
 * the SST file paths on the Java side and let
 * {@link RocksDB#createColumnFamilyWithImport} flatten them into a path
 * array before crossing JNI. Once a real export path lands the storage
 * can be moved native-side without changing this class's signature.
 */
public class ExportImportFilesMetaData extends RocksObject {

    private final java.util.List<String> sstFiles;

    protected ExportImportFilesMetaData(final long handle) {
        super(handle);
        this.sstFiles = java.util.Collections.emptyList();
    }

    /**
     * Construct from an explicit list of SST file paths. Convenient
     * for tests and for callers that already know the file layout
     * (e.g. ones that read it from a checkpoint manifest).
     */
    public ExportImportFilesMetaData(final java.util.List<String> sstFiles) {
        super(0L);
        this.sstFiles = java.util.List.copyOf(sstFiles);
    }

    public java.util.List<String> getSstFiles() {
        return sstFiles;
    }

    @Override
    protected void disposeInternal(final long handle) {
        // no native resource yet
    }
}
