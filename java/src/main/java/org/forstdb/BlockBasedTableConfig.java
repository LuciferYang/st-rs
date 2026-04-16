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
 * Configuration for the block-based table format.
 *
 * <p>All fields are stored as pure Java state (no native handle).</p>
 */
public class BlockBasedTableConfig extends TableFormatConfig {

    private long blockSize = 4 * 1024;
    private long metadataBlockSize = 4 * 1024;
    private long blockCacheSize = 8 * 1024 * 1024;
    private boolean cacheIndexAndFilterBlocks = false;
    private boolean cacheIndexAndFilterBlocksWithHighPriority = false;
    private boolean pinL0FilterAndIndexBlocksInCache = false;
    private boolean pinTopLevelIndexAndFilter = false;
    private IndexType indexType = IndexType.kBinarySearch;
    private boolean partitionFilters = false;
    private Filter filterPolicy = null;
    private Cache blockCache = null;

    public BlockBasedTableConfig() {
    }

    public BlockBasedTableConfig setBlockSize(final long blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public BlockBasedTableConfig setMetadataBlockSize(
            final long metadataBlockSize) {
        this.metadataBlockSize = metadataBlockSize;
        return this;
    }

    public BlockBasedTableConfig setBlockCacheSize(final long blockCacheSize) {
        this.blockCacheSize = blockCacheSize;
        return this;
    }

    public BlockBasedTableConfig setCacheIndexAndFilterBlocks(
            final boolean flag) {
        this.cacheIndexAndFilterBlocks = flag;
        return this;
    }

    public BlockBasedTableConfig setCacheIndexAndFilterBlocksWithHighPriority(
            final boolean flag) {
        this.cacheIndexAndFilterBlocksWithHighPriority = flag;
        return this;
    }

    public BlockBasedTableConfig setPinL0FilterAndIndexBlocksInCache(
            final boolean flag) {
        this.pinL0FilterAndIndexBlocksInCache = flag;
        return this;
    }

    public BlockBasedTableConfig setPinTopLevelIndexAndFilter(
            final boolean flag) {
        this.pinTopLevelIndexAndFilter = flag;
        return this;
    }

    public BlockBasedTableConfig setIndexType(final IndexType indexType) {
        this.indexType = indexType;
        return this;
    }

    public BlockBasedTableConfig setPartitionFilters(final boolean flag) {
        this.partitionFilters = flag;
        return this;
    }

    public BlockBasedTableConfig setFilterPolicy(final Filter filterPolicy) {
        this.filterPolicy = filterPolicy;
        return this;
    }

    public Filter filterPolicy() {
        return filterPolicy;
    }

    public BlockBasedTableConfig setBlockCache(final Cache blockCache) {
        this.blockCache = blockCache;
        return this;
    }
}
