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
 * Describes a column family by name and options.
 *
 * <p>This is a pure-Java descriptor with no native handle.</p>
 */
public class ColumnFamilyDescriptor {

    private final String name;
    private final ColumnFamilyOptions options;

    public ColumnFamilyDescriptor(final String name,
            final ColumnFamilyOptions options) {
        this.name = name;
        this.options = options;
    }

    public ColumnFamilyDescriptor(final String name) {
        this(name, new ColumnFamilyOptions());
    }

    public String getName() {
        return name;
    }

    public ColumnFamilyOptions getOptions() {
        return options;
    }
}
