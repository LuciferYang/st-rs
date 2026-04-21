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

import java.nio.charset.StandardCharsets;

/**
 * Describes a column family by name and options.
 *
 * <p>Pure-Java descriptor with no native handle. Internally stores the
 * name as {@code byte[]} to match upstream's RocksDB / ForSt API
 * (Flink's ForSt backend constructs descriptors with {@code byte[]} keys).
 */
public class ColumnFamilyDescriptor {

    private final byte[] name;
    private final ColumnFamilyOptions options;

    public ColumnFamilyDescriptor(final byte[] name,
            final ColumnFamilyOptions options) {
        this.name = name;
        this.options = options;
    }

    public ColumnFamilyDescriptor(final byte[] name) {
        this(name, new ColumnFamilyOptions());
    }

    public ColumnFamilyDescriptor(final String name,
            final ColumnFamilyOptions options) {
        this(name.getBytes(StandardCharsets.UTF_8), options);
    }

    public ColumnFamilyDescriptor(final String name) {
        this(name, new ColumnFamilyOptions());
    }

    /**
     * Returns the raw column-family name. Mirrors upstream's
     * {@code byte[]}-typed accessor.
     */
    public byte[] getName() {
        return name;
    }

    /**
     * Convenience accessor that decodes the CF name as UTF-8. Used by
     * call sites that historically passed a String.
     */
    public String getNameAsString() {
        return new String(name, StandardCharsets.UTF_8);
    }

    public ColumnFamilyOptions getOptions() {
        return options;
    }
}
