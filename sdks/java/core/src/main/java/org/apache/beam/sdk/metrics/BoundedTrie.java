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
package org.apache.beam.sdk.metrics;

import java.util.Arrays;
import org.apache.beam.sdk.annotations.Internal;

/**
 * <b>Internal:</b> For internal use only and not for public consumption. This API is subject to
 * incompatible changes, or even removal, in a future release.
 *
 * <p>A metric that represents a bounded trie data structure. This interface extends the {@link
 * Metric} interface and provides methods for adding string sequences (paths) to the trie.
 *
 * <p>The trie is bounded in size (max=100), meaning it has a maximum capacity for storing paths.
 * When the trie reaches its capacity, it truncates paths. This is useful for tracking and
 * aggregating a large number of distinct paths while limiting memory usage. It is not necessary but
 * recommended that parts of paths provided as strings are hierarchical in nature so the truncation
 * reduces granularity rather than complete data loss.
 */
@Internal
public interface BoundedTrie extends Metric {

  /**
   * Adds a path to the trie. The path is represented as an iterable of string segments.
   *
   * @param values The segments of the path to add.
   */
  void add(Iterable<String> values);

  /**
   * Adds a path to the trie. The path is represented as a variable number of string arguments.
   *
   * @param values The segments of the path to add.
   */
  default void add(String... values) {
    add(Arrays.asList(values));
  }
}
