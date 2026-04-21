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
 * An LRU block cache.
 */
public class LRUCache extends Cache {

    public LRUCache(final long capacity) {
        super(0);
    }

    /**
     * Compatibility constructor for Flink's {@code ForStMemoryControllerUtils},
     * which calls {@code new LRUCache(capacity, numShardBits,
     * strictCapacityLimit, highPriPoolRatio)} when allocating shared block
     * cache memory. The extra parameters are accepted for API compatibility
     * but currently ignored — st-rs does not yet expose shard-count or
     * high-priority-pool tuning to the engine.
     */
    public LRUCache(
            final long capacity,
            final int numShardBits,
            final boolean strictCapacityLimit,
            final double highPriPoolRatio) {
        super(0);
    }

    @Override
    protected void disposeInternal(final long handle) {
        // no native resource yet
    }
}
