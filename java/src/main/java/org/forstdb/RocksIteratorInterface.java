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
 * Iterator contract shared between {@link RocksIterator} and other
 * iterator types. Required as a class-load dependency by Flink's
 * {@code ForStDBCachingPriorityQueueSet}.
 *
 * <p>Method set mirrors the upstream interface; the byte[]/ByteBuffer
 * dual-shape seek methods are intentionally narrowed to byte[] only
 * (st-rs has no ByteBuffer iterator path yet).
 */
public interface RocksIteratorInterface {

    boolean isValid();

    void seekToFirst();

    void seekToLast();

    void seek(byte[] target);

    void seekForPrev(byte[] target);

    void next();

    void prev();

    void status() throws RocksDBException;

    void refresh() throws RocksDBException;
}
