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

    private boolean avoidFlushDuringShutdown = false;
    private int maxBackgroundJobs = 2;
    private int maxOpenFiles = -1;
    private InfoLogLevel infoLogLevel = InfoLogLevel.INFO_LEVEL;
    private boolean useFsync = false;
    private int statsDumpPeriodSec = 600;
    private WriteBufferManager writeBufferManager = null;
    private Statistics statistics = null;
    private FlinkEnv env = null;
    private String dbLogDir = "";
    private long maxLogFileSize = 0;
    private int keepLogFileNum = 1000;

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

    public DBOptions setAvoidFlushDuringShutdown(final boolean flag) {
        this.avoidFlushDuringShutdown = flag;
        return this;
    }

    public DBOptions setMaxBackgroundJobs(final int maxBackgroundJobs) {
        this.maxBackgroundJobs = maxBackgroundJobs;
        return this;
    }

    public DBOptions setMaxOpenFiles(final int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    public DBOptions setInfoLogLevel(final InfoLogLevel level) {
        this.infoLogLevel = level;
        return this;
    }

    public DBOptions setUseFsync(final boolean flag) {
        this.useFsync = flag;
        return this;
    }

    public DBOptions setStatsDumpPeriodSec(final int period) {
        this.statsDumpPeriodSec = period;
        return this;
    }

    public DBOptions setWriteBufferManager(
            final WriteBufferManager writeBufferManager) {
        this.writeBufferManager = writeBufferManager;
        return this;
    }

    public DBOptions setStatistics(final Statistics statistics) {
        this.statistics = statistics;
        return this;
    }

    public DBOptions setEnv(final FlinkEnv env) {
        this.env = env;
        return this;
    }

    public DBOptions setDbLogDir(final String dbLogDir) {
        this.dbLogDir = dbLogDir;
        return this;
    }

    public DBOptions setMaxLogFileSize(final long maxLogFileSize) {
        this.maxLogFileSize = maxLogFileSize;
        return this;
    }

    public DBOptions setKeepLogFileNum(final int keepLogFileNum) {
        this.keepLogFileNum = keepLogFileNum;
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
