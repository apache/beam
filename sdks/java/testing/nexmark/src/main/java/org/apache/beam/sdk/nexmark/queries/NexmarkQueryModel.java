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
package org.apache.beam.sdk.nexmark.queries;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.core.IsEqual;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;

/**
 * Base class for models of the eight NEXMark queries. Provides an assertion function which can be
 * applied against the actual query results to check their consistency with the model.
 */
public abstract class NexmarkQueryModel<T extends KnownSize> implements Serializable {
  public final NexmarkConfiguration configuration;

  NexmarkQueryModel(NexmarkConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Return the start of the most recent window of {@code size} and {@code period} which ends
   * strictly before {@code timestamp}.
   */
  static Instant windowStart(Duration size, Duration period, Instant timestamp) {
    long ts = timestamp.getMillis();
    long p = period.getMillis();
    long lim = ts - ts % p;
    long s = size.getMillis();
    return new Instant(lim - s);
  }

  /** Convert {@code itr} to strings capturing values and timestamps. */
  static <T> Set<String> toValueTimestamp(Iterator<TimestampedValue<T>> itr) {
    Set<String> strings = new HashSet<>();
    while (itr.hasNext()) {
      strings.add(itr.next().toString());
    }
    return strings;
  }

  /** Convert {@code itr} to strings capturing values only. */
  static <T> Set<String> toValue(Iterator<TimestampedValue<T>> itr) {
    Set<String> strings = new HashSet<>();
    while (itr.hasNext()) {
      strings.add(itr.next().getValue().toString());
    }
    return strings;
  }

  /** Return simulator for query. */
  public abstract AbstractSimulator<?, T> simulator();

  /** Return sub-sequence of results which are significant for model. */
  Iterable<TimestampedValue<T>> relevantResults(Iterable<TimestampedValue<T>> results) {
    return results;
  }

  /**
   * Convert iterator of elements to collection of strings to use when testing coherence of model
   * against actual query results.
   */
  protected abstract Collection<String> toCollection(Iterator<TimestampedValue<T>> itr);

  /** Return assertion to use on results of pipeline for this query. */
  public SerializableFunction<Iterable<TimestampedValue<T>>, Void> assertionFor() {
    final Collection<String> expectedStrings = toCollection(simulator().results());
    Assert.assertFalse(expectedStrings.isEmpty());

    return new SerializableFunction<Iterable<TimestampedValue<T>>, Void>() {
      @Override
      public @Nullable Void apply(Iterable<TimestampedValue<T>> actual) {
        Collection<String> actualStrings = toCollection(relevantResults(actual).iterator());
        Assert.assertThat("wrong pipeline output", actualStrings, IsEqual.equalTo(expectedStrings));
        return null;
      }
    };
  }
}
