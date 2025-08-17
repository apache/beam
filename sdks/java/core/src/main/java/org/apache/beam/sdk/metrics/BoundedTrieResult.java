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
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * <b>Internal:</b> For internal use only and not for public consumption. This API is subject to
 * incompatible changes, or even removal, in a future release.
 *
 * <p>The result of a {@link BoundedTrie} metric. The {@link BoundedTrieResult} hold an immutable
 * copy of the set from which it was initially created representing that a result cannot be modified
 * once created.
 */
@Internal
@AutoValue
public abstract class BoundedTrieResult {

  public abstract Set<List<String>> getResult();

  /**
   * Creates a {@link BoundedTrieResult} from the given {@link Set} by making an immutable copy.
   *
   * @param s the set from which the {@link BoundedTrieResult} should be created.
   * @return {@link BoundedTrieResult} containing an immutable copy of the given set.
   */
  public static BoundedTrieResult create(Set<List<String>> s) {
    return new AutoValue_BoundedTrieResult(
        ImmutableSet.copyOf(s.stream().map(ImmutableList::copyOf).collect(Collectors.toSet())));
  }

  /** @return an empty {@link BoundedTrieResult} */
  public static BoundedTrieResult empty() {
    return create(ImmutableSet.of());
  }
}
