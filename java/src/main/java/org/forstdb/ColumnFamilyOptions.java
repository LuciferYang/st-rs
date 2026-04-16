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

import java.util.List;

/**
 * Options for a column family.
 *
 * <p>Pure Java for now -- fields will be expanded as Flink usage requires.</p>
 */
public class ColumnFamilyOptions {

    private int writeBufferSize = 64 * 1024 * 1024; // 64 MB
    private int maxWriteBufferNumber = 2;
    private int minWriteBufferNumberToMerge = 1;
    private CompactionStyle compactionStyle = CompactionStyle.LEVEL;
    private List<CompressionType> compressionPerLevel = null;
    private boolean levelCompactionDynamicLevelBytes = false;
    private long targetFileSizeBase = 64 * 1024 * 1024;
    private long maxBytesForLevelBase = 256 * 1024 * 1024;
    private long periodicCompactionSeconds = 0;
    private String mergeOperatorName = null;
    private TableFormatConfig tableFormatConfig = null;
    private FlinkCompactionFilter.FlinkCompactionFilterFactory
            compactionFilterFactory = null;

    public ColumnFamilyOptions() {
    }

    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    public ColumnFamilyOptions setWriteBufferSize(final int writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
        return this;
    }

    public int getMaxWriteBufferNumber() {
        return maxWriteBufferNumber;
    }

    public ColumnFamilyOptions setMaxWriteBufferNumber(
            final int maxWriteBufferNumber) {
        this.maxWriteBufferNumber = maxWriteBufferNumber;
        return this;
    }

    public int getMinWriteBufferNumberToMerge() {
        return minWriteBufferNumberToMerge;
    }

    public ColumnFamilyOptions setMinWriteBufferNumberToMerge(
            final int minWriteBufferNumberToMerge) {
        this.minWriteBufferNumberToMerge = minWriteBufferNumberToMerge;
        return this;
    }

    public ColumnFamilyOptions setCompactionStyle(
            final CompactionStyle compactionStyle) {
        this.compactionStyle = compactionStyle;
        return this;
    }

    public ColumnFamilyOptions setCompressionPerLevel(
            final List<CompressionType> compressionPerLevel) {
        this.compressionPerLevel = compressionPerLevel;
        return this;
    }

    public ColumnFamilyOptions setLevelCompactionDynamicLevelBytes(
            final boolean flag) {
        this.levelCompactionDynamicLevelBytes = flag;
        return this;
    }

    public ColumnFamilyOptions setTargetFileSizeBase(
            final long targetFileSizeBase) {
        this.targetFileSizeBase = targetFileSizeBase;
        return this;
    }

    public ColumnFamilyOptions setMaxBytesForLevelBase(
            final long maxBytesForLevelBase) {
        this.maxBytesForLevelBase = maxBytesForLevelBase;
        return this;
    }

    public ColumnFamilyOptions setPeriodicCompactionSeconds(
            final long periodicCompactionSeconds) {
        this.periodicCompactionSeconds = periodicCompactionSeconds;
        return this;
    }

    public ColumnFamilyOptions setMergeOperatorName(
            final String mergeOperatorName) {
        this.mergeOperatorName = mergeOperatorName;
        return this;
    }

    public ColumnFamilyOptions setTableFormatConfig(
            final TableFormatConfig tableFormatConfig) {
        this.tableFormatConfig = tableFormatConfig;
        return this;
    }

    public ColumnFamilyOptions setCompactionFilterFactory(
            final FlinkCompactionFilter.FlinkCompactionFilterFactory
                    compactionFilterFactory) {
        this.compactionFilterFactory = compactionFilterFactory;
        return this;
    }
}
