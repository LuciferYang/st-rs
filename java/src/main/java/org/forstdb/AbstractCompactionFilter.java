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
 * Compatibility stub for upstream's {@code AbstractCompactionFilter<T>},
 * extended by {@link FlinkCompactionFilter}. Carries the per-compaction
 * {@link Context} type that the factory passes when constructing a new
 * filter instance.
 */
public abstract class AbstractCompactionFilter<T extends AbstractSlice<?>>
        extends RocksObject {

    protected AbstractCompactionFilter(final long nativeHandle) {
        super(nativeHandle);
    }

    /** Per-compaction context handed to the filter at construction. */
    public static class Context {
        private final boolean fullCompaction;
        private final boolean manualCompaction;

        public Context(final boolean fullCompaction, final boolean manualCompaction) {
            this.fullCompaction = fullCompaction;
            this.manualCompaction = manualCompaction;
        }

        public boolean isFullCompaction() {
            return fullCompaction;
        }

        public boolean isManualCompaction() {
            return manualCompaction;
        }
    }
}
