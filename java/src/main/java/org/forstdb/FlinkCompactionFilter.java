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
 * Compatibility surface for Flink's TTL compaction filter API. The native
 * filter logic is not yet wired into the st-rs engine — this class exists
 * primarily so Flink's {@code ForStDBTtlCompactFiltersManager} can build
 * its {@link Config} objects without {@code NoClassDefFoundError}s during
 * keyed-state-backend init.
 *
 * <p>Once the JNI bridge for compaction filters is implemented (gap M2 in
 * FLINK-INTEGRATION-STATUS.md), the filter will actually expire TTL'd
 * state on compaction. Today, configs are accepted but ignored.
 */
public class FlinkCompactionFilter extends AbstractCompactionFilter<Slice> {

    protected FlinkCompactionFilter() {
        super(0L);
    }

    @Override
    protected void disposeInternal(final long handle) {
        // no native resource yet
    }


    /**
     * Order matches upstream's JNI translation — do not reorder.
     */
    public enum StateType {
        Disabled,
        Value,
        List
    }

    /** Provides current timestamp for TTL checks. Must be thread-safe. */
    public interface TimeProvider {
        long currentTimestamp();
    }

    /**
     * Returns the offset of the first unexpired list element. Used for
     * variable-length list serializations where Flink's serializer must
     * compute element boundaries.
     */
    public interface ListElementFilter {
        int nextUnexpiredOffset(byte[] list, long ttl, long currentTimestamp);
    }

    /** Factory for {@link ListElementFilter} instances. */
    public interface ListElementFilterFactory {
        ListElementFilter createListElementFilter();
    }

    /**
     * TTL compaction filter configuration. Mirrors upstream's static
     * factories so Flink's call sites compile and run; values are stored
     * but not yet handed to the engine.
     */
    public static class Config {
        public final StateType stateType;
        public final int timestampOffset;
        public final long ttl;
        public final long queryTimeAfterNumEntries;
        public final int fixedElementLength;
        public final ListElementFilterFactory listElementFilterFactory;

        private Config(
                final StateType stateType,
                final int timestampOffset,
                final long ttl,
                final long queryTimeAfterNumEntries,
                final int fixedElementLength,
                final ListElementFilterFactory listElementFilterFactory) {
            this.stateType = stateType;
            this.timestampOffset = timestampOffset;
            this.ttl = ttl;
            this.queryTimeAfterNumEntries = queryTimeAfterNumEntries;
            this.fixedElementLength = fixedElementLength;
            this.listElementFilterFactory = listElementFilterFactory;
        }

        public static Config createNotList(
                final StateType stateType,
                final int timestampOffset,
                final long ttl,
                final long queryTimeAfterNumEntries) {
            return new Config(
                    stateType, timestampOffset, ttl, queryTimeAfterNumEntries, -1, null);
        }

        public static Config createForValue(
                final long ttl, final long queryTimeAfterNumEntries) {
            return createNotList(StateType.Value, 0, ttl, queryTimeAfterNumEntries);
        }

        public static Config createForMap(
                final long ttl, final long queryTimeAfterNumEntries) {
            return createNotList(StateType.Value, 1, ttl, queryTimeAfterNumEntries);
        }

        public static Config createForFixedElementList(
                final long ttl,
                final long queryTimeAfterNumEntries,
                final int fixedElementLength) {
            return new Config(
                    StateType.List, 0, ttl, queryTimeAfterNumEntries,
                    fixedElementLength, null);
        }

        public static Config createForList(
                final long ttl,
                final long queryTimeAfterNumEntries,
                final ListElementFilterFactory listElementFilterFactory) {
            return new Config(
                    StateType.List, 0, ttl, queryTimeAfterNumEntries, -1,
                    listElementFilterFactory);
        }
    }

    /**
     * Factory used by Flink's {@code ForStDBTtlCompactFiltersManager} to
     * register a per-CF compaction filter. The Java surface is in place;
     * the native binding is the next milestone (M2 in
     * FLINK-INTEGRATION-STATUS.md).
     */
    public static class FlinkCompactionFilterFactory
            extends AbstractCompactionFilterFactory<FlinkCompactionFilter>
            implements AutoCloseable {

        static {
            NativeLibraryLoader.load();
        }

        private final TimeProvider timeProvider;
        private final Logger logger;
        private long nativeFactoryHandle;

        public FlinkCompactionFilterFactory(
                final TimeProvider timeProvider, final Logger logger) {
            this.timeProvider = timeProvider;
            this.logger = logger;
            this.nativeFactoryHandle = 0L;
        }

        /**
         * Build the native TTL filter factory from the Flink-supplied
         * {@link Config}. Called once per CF before
         * {@code setCompactionFilterFactory}. List state with element-level
         * filtering is not yet supported (silently skipped).
         */
        public synchronized void configure(final Config config) {
            if (nativeFactoryHandle != 0L) {
                return;
            }
            if (config == null || config.stateType == StateType.Disabled
                    || config.stateType == StateType.List) {
                return;
            }
            nativeFactoryHandle = createFlinkTtlFactory(
                    config.ttl, config.timestampOffset);
        }

        long getNativeFactoryHandle() {
            return nativeFactoryHandle;
        }

        @Override
        public FlinkCompactionFilter createCompactionFilter(
                final AbstractCompactionFilter.Context context) {
            return new FlinkCompactionFilter();
        }

        @Override
        public String name() {
            return "FlinkCompactionFilterFactory";
        }

        @Override
        public synchronized void close() {
            if (nativeFactoryHandle != 0L) {
                disposeFlinkTtlFactory(nativeFactoryHandle);
                nativeFactoryHandle = 0L;
            }
        }

        private static native long createFlinkTtlFactory(
                long ttlMillis, int timestampOffset);

        private static native void disposeFlinkTtlFactory(long handle);
    }
}
