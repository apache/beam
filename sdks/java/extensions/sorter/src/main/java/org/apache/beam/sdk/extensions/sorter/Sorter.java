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
package org.apache.beam.sdk.extensions.sorter;

import java.io.IOException;
import org.apache.beam.sdk.values.KV;

/**
 * Interface for classes which can sort {@code <key, value>} pairs by the key.
 *
 * <p>Records must first be added by calling {@link #add(KV)}. Then {@link #sort()} can be called at
 * most once.
 *
 * <p>TODO: Support custom comparison functions.
 */
interface Sorter {

  /**
   * Adds a given record to the sorter.
   *
   * <p>Records can only be added before calling {@link #sort()}.
   */
  void add(KV<byte[], byte[]> record) throws IOException;

  /**
   * Sorts the added elements and returns an {@link Iterable} over the sorted elements.
   *
   * <p>Can be called at most once.
   */
  Iterable<KV<byte[], byte[]>> sort() throws IOException;
}
