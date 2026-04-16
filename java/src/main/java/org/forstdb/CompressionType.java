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
 * Compression algorithm used for SST blocks.
 */
public enum CompressionType {
    NO_COMPRESSION(0),
    SNAPPY_COMPRESSION(1),
    ZLIB_COMPRESSION(2),
    BZLIB2_COMPRESSION(3),
    LZ4_COMPRESSION(4),
    LZ4HC_COMPRESSION(5),
    XPRESS_COMPRESSION(6),
    ZSTD_COMPRESSION(7);

    private final byte value;

    CompressionType(final int value) {
        this.value = (byte) value;
    }

    public byte getValue() {
        return value;
    }
}
