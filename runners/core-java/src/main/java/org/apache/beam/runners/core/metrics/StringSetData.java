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
package org.apache.beam.runners.core.metrics;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.metrics.StringSetResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * Data describing the StringSet. The {@link StringSetData} hold an immutable copy of the set from
 * which it was initially created. This should retain enough detail that it can be combined with
 * other {@link StringSetData}.
 */
@AutoValue
public abstract class StringSetData implements Serializable {

  public abstract Set<String> stringSet();

  /** Returns a {@link StringSetData} which is made from an immutable copy of the given set. */
  public static StringSetData create(Set<String> set) {
    return new AutoValue_StringSetData(ImmutableSet.copyOf(set));
  }

  /** Return a {@link EmptyStringSetData#INSTANCE} representing an empty {@link StringSetData}. */
  public static StringSetData empty() {
    return EmptyStringSetData.INSTANCE;
  }

  /**
   * Combines this {@link StringSetData} with other, both original StringSetData are left intact.
   */
  public StringSetData combine(StringSetData other) {
    if (this.stringSet().isEmpty()) {
      return other;
    } else if (other.stringSet().isEmpty()) {
      return this;
    } else {
      ImmutableSet.Builder<String> combined = ImmutableSet.builder();
      combined.addAll(this.stringSet());
      combined.addAll(other.stringSet());
      return StringSetData.create(combined.build());
    }
  }

  /**
   * Combines this {@link StringSetData} with others, all original StringSetData are left intact.
   */
  public StringSetData combine(Iterable<StringSetData> others) {
    Set<String> combined =
        StreamSupport.stream(others.spliterator(), true)
            .flatMap(other -> other.stringSet().stream())
            .collect(Collectors.toSet());
    combined.addAll(this.stringSet());
    return StringSetData.create(combined);
  }

  /** Returns a {@link StringSetResult} representing this {@link StringSetData}. */
  public StringSetResult extractResult() {
    return StringSetResult.create(stringSet());
  }

  /** Empty {@link StringSetData}, representing no values reported and is immutable. */
  public static class EmptyStringSetData extends StringSetData {

    private static final EmptyStringSetData INSTANCE = new EmptyStringSetData();

    private EmptyStringSetData() {}

    /** Returns an immutable empty set. */
    @Override
    public Set<String> stringSet() {
      return ImmutableSet.of();
    }

    /** Return a {@link StringSetResult#empty()} which is immutable empty set. */
    @Override
    public StringSetResult extractResult() {
      return StringSetResult.empty();
    }
  }
}
