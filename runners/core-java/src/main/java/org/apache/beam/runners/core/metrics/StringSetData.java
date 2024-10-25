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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.metrics.StringSetResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data describing the StringSet. The {@link StringSetData} hold a copy of the set from which it was
 * initially created. This should retain enough detail that it can be combined with other {@link
 * StringSetData}.
 *
 * <p>The underlying set is mutable for {@link #addAll} operation, otherwise a copy set will be
 * generated.
 *
 * <p>The summation of all string length for a {@code StringSetData} cannot exceed 1 MB. Further
 * addition of elements are dropped.
 */
@AutoValue
public abstract class StringSetData implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(StringSetData.class);
  // 1 MB
  @VisibleForTesting static final long STRING_SET_SIZE_LIMIT = 1_000_000L;

  public abstract Set<String> stringSet();

  public abstract long stringSize();

  /** Returns a {@link StringSetData} which is made from an immutable copy of the given set. */
  public static StringSetData create(Set<String> set) {
    if (set.isEmpty()) {
      return empty();
    }
    HashSet<String> combined = new HashSet<>();
    long stringSize = addUntilCapacity(combined, 0L, set);
    return new AutoValue_StringSetData(combined, stringSize);
  }

  /** Returns a {@link StringSetData} which is made from the given set in place. */
  private static StringSetData createInPlace(HashSet<String> set, long stringSize) {
    return new AutoValue_StringSetData(set, stringSize);
  }

  /** Return a {@link EmptyStringSetData#INSTANCE} representing an empty {@link StringSetData}. */
  public static StringSetData empty() {
    return EmptyStringSetData.INSTANCE;
  }

  /**
   * Add strings into this {@code StringSetData} and return the result {@code StringSetData}. Reuse
   * the original StringSetData's set. As a result, current StringSetData will become invalid.
   *
   * <p>>Should only be used by {@link StringSetCell#add}.
   */
  public StringSetData addAll(String... strings) {
    HashSet<String> combined;
    if (this.stringSet() instanceof HashSet) {
      combined = (HashSet<String>) this.stringSet();
    } else {
      combined = new HashSet<>(this.stringSet());
    }
    long stringSize = addUntilCapacity(combined, this.stringSize(), Arrays.asList(strings));
    return StringSetData.createInPlace(combined, stringSize);
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
      HashSet<String> combined = new HashSet<>(this.stringSet());
      long stringSize = addUntilCapacity(combined, this.stringSize(), other.stringSet());
      return StringSetData.createInPlace(combined, stringSize);
    }
  }

  /**
   * Combines this {@link StringSetData} with others, all original StringSetData are left intact.
   */
  public StringSetData combine(Iterable<StringSetData> others) {
    HashSet<String> combined = new HashSet<>(this.stringSet());
    long stringSize = this.stringSize();
    for (StringSetData other : others) {
      stringSize = addUntilCapacity(combined, stringSize, other.stringSet());
    }
    return StringSetData.createInPlace(combined, stringSize);
  }

  /** Returns a {@link StringSetResult} representing this {@link StringSetData}. */
  public StringSetResult extractResult() {
    return StringSetResult.create(stringSet());
  }

  /** Add strings into set until reach capacity. Return the all string size of added set. */
  private static long addUntilCapacity(
      HashSet<String> combined, long currentSize, Iterable<String> others) {
    if (currentSize > STRING_SET_SIZE_LIMIT) {
      // already at capacity
      return currentSize;
    }
    for (String string : others) {
      if (combined.add(string)) {
        currentSize += string.length();

        // check capacity both before insert and after insert one, so the warning only emit once.
        if (currentSize > STRING_SET_SIZE_LIMIT) {
          LOG.warn(
              "StringSet metrics reaches capacity. Further incoming elements won't be recorded."
                  + " Current size: {}, last element size: {}.",
              currentSize,
              string.length());
          break;
        }
      }
    }
    return currentSize;
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

    @Override
    public long stringSize() {
      return 0L;
    }

    /** Return a {@link StringSetResult#empty()} which is immutable empty set. */
    @Override
    public StringSetResult extractResult() {
      return StringSetResult.empty();
    }
  }
}
