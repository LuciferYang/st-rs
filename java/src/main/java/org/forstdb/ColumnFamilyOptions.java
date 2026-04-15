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
 * Options for a column family.
 *
 * <p>Pure Java for now -- fields will be expanded as Flink usage requires.</p>
 */
public class ColumnFamilyOptions {

    private int writeBufferSize = 64 * 1024 * 1024; // 64 MB
    private int maxWriteBufferNumber = 2;
    private int minWriteBufferNumberToMerge = 1;

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
}
