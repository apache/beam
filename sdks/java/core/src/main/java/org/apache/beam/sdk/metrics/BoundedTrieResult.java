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

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * An immutable class representing the result of {@link BoundedTrie} metric. The result is a set of
 * lists, where each list represents a path in the bounded trie. The last element in the in each
 * path is a boolean in string representation denoting whether this path was truncated. i.e. <["a",
 * "b", "false"], ["c", "true"]>
 */
@AutoValue
public abstract class BoundedTrieResult {
  /**
   * Returns an immutable set of lists, where each list represents a path in the bounded trie. The
   * last element in the in each path is a boolean in string representation denoting whether this
   * path was truncated. i.e. <["a", "b", "false"], ["c", "true"]>
   *
   * @return The set of paths.
   */
  public abstract Set<List<String>> getResults();

  /**
   * Creates a {@link BoundedTrieResult} from the given set of paths.
   *
   * @param paths The set of paths to include in the result.
   * @return A new {@link BoundedTrieResult} instance.
   */
  public static BoundedTrieResult create(Set<List<String>> paths) {
    return new AutoValue_BoundedTrieResult(ImmutableSet.copyOf(paths));
  }

  /**
   * Returns an empty {@link BoundedTrieResult} instance.
   *
   * @return An empty {@link BoundedTrieResult}.
   */
  public static BoundedTrieResult empty() {
    return EmptyBoundedTrieResult.INSTANCE;
  }

  /**
   * An immutable class representing an empty {@link BoundedTrieResult}. This class provides a
   * singleton instance for efficiency.
   */
  public static class EmptyBoundedTrieResult extends BoundedTrieResult {

    private static final EmptyBoundedTrieResult INSTANCE = new EmptyBoundedTrieResult();

    private EmptyBoundedTrieResult() {}

    /** Returns an empty immutable set of paths. */
    @Override
    public Set<List<String>> getResults() {
      return ImmutableSet.of();
    }
  }
}
