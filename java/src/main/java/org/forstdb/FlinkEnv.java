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
 * An environment that delegates file I/O to a Flink FileSystem via
 * JNI callbacks.
 *
 * <p>The Java-side backend object must implement these methods:
 * <ul>
 *   <li>{@code boolean exists(String path)}</li>
 *   <li>{@code boolean mkdirs(String path)}</li>
 *   <li>{@code boolean renameFile(String src, String dst)}</li>
 *   <li>{@code boolean deleteFile(String path, boolean recursive)}</li>
 *   <li>{@code byte[] readFile(String path)}</li>
 *   <li>{@code void writeFile(String path, byte[] data)}</li>
 *   <li>{@code long[] getFileStatus(String path)} — returns {length, isDir(0/1), modTime}</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * FlinkEnv env = FlinkEnv.create(myFsBackend);
 * DBOptions opts = new DBOptions().setEnv(env);
 * RocksDB db = RocksDB.open(opts, path);
 * }</pre>
 */
public class FlinkEnv extends Env {

    static {
        NativeLibraryLoader.load();
    }

    private FlinkEnv(final long handle) {
        super(handle);
    }

    /**
     * Create a FlinkEnv backed by the given Java filesystem object.
     *
     * @param fsBackend an object implementing the required filesystem
     *                  methods (exists, mkdirs, readFile, writeFile, etc.)
     * @return a new FlinkEnv
     * @throws RocksDBException if the native environment cannot be created
     */
    public static FlinkEnv create(final Object fsBackend)
            throws RocksDBException {
        final long handle = createWithBackend(fsBackend);
        if (handle == 0) {
            throw new RocksDBException("failed to create FlinkEnv");
        }
        return new FlinkEnv(handle);
    }

    @Override
    protected void disposeInternal(final long handle) {
        disposeFlinkEnv(handle);
    }

    /**
     * Returns the native handle for use with DBOptions.setEnv().
     */
    public long getNativeHandle() {
        return nativeHandle_;
    }

    private static native long createWithBackend(Object fsBackend)
            throws RocksDBException;

    private static native void disposeFlinkEnv(long handle);
}
