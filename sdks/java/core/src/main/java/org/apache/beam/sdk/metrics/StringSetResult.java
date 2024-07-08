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
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * The result of a {@link StringSet} metric. The {@link StringSetResult} hold an immutable copy of
 * the set from which it was initially created representing that a result cannot be modified once
 * created.
 */
@AutoValue
public abstract class StringSetResult {
  public abstract Set<String> getStringSet();

  /**
   * Creates a {@link StringSetResult} from the given {@link Set} by making an immutable copy.
   *
   * @param s the set from which the {@link StringSetResult} should be created.
   * @return {@link StringSetResult} containing an immutable copy of the given set.
   */
  public static StringSetResult create(Set<String> s) {
    return new AutoValue_StringSetResult(ImmutableSet.copyOf(s));
  }

  /** @return a {@link EmptyStringSetResult} */
  public static StringSetResult empty() {
    return EmptyStringSetResult.INSTANCE;
  }

  /** Empty {@link StringSetResult}, representing no values reported and is immutable. */
  public static class EmptyStringSetResult extends StringSetResult {

    private static final EmptyStringSetResult INSTANCE = new EmptyStringSetResult();

    private EmptyStringSetResult() {}

    /** Returns an empty immutable set. */
    @Override
    public Set<String> getStringSet() {
      return ImmutableSet.of();
    }
  }
}
