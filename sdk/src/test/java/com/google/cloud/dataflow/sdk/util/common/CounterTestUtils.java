/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MEAN;

import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.util.CloudCounterUtils;

import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Utilities for testing {@link Counter}s.
 */
public class CounterTestUtils {

  /**
   * Extracts a MetricUpdate update from the given counter. This is used mainly
   * for testing.
   *
   * @param extractDelta specifies whether or not to extract the cumulative
   *        aggregate value or the delta since the last extraction.
   */
  public static MetricUpdate extractCounterUpdate(Counter<?> counter,
      boolean extractDelta) {
    // This may be invoked asynchronously with regular counter updates but
    // access to counter data is synchronized, so this is safe.
    return CloudCounterUtils.extractCounter(counter, extractDelta);
  }

  /**
   * Extracts MetricUpdate updates from the given counters. This is used mainly
   * for testing.
   *
   * @param extractDelta specifies whether or not to extract the cumulative
   *        aggregate values or the deltas since the last extraction.
   */
  public static List<MetricUpdate> extractCounterUpdates(
      Collection<Counter<?>> counters, boolean extractDelta) {
    // This may be invoked asynchronously with regular counter updates but
    // access to counter data is synchronized, so this is safe. Note however
    // that the result is NOT an atomic snapshot across all given counters.
    List<MetricUpdate> cloudCounters = new ArrayList<>(counters.size());
    for (Counter<?> counter : counters) {
      MetricUpdate cloudCounter = extractCounterUpdate(counter, extractDelta);
      if (null != cloudCounter) {
        cloudCounters.add(cloudCounter);
      }
    }
    return cloudCounters;
  }


  // These methods expose a counter's values for testing.

  public static <T> T getTotalAggregate(Counter<T> counter) {
    return counter.getTotalAggregate();
  }

  public static <T> T getDeltaAggregate(Counter<T> counter) {
    return counter.getDeltaAggregate();
  }

  public static <T> long getTotalCount(Counter<T> counter) {
    return counter.getTotalCount();
  }

  public static <T> long getDeltaCount(Counter<T> counter) {
    return counter.getDeltaCount();
  }

  public static <T> Set<T> getTotalSet(Counter<T> counter) {
    return counter.getTotalSet();
  }

  public static <T> Set<T> getDeltaSet(Counter<T> counter) {
    return counter.getDeltaSet();
  }

  /**
   * A utility method that passes the given (unencoded) elements through
   * coder's registerByteSizeObserver() and encode() methods, and confirms
   * they are mutually consistent. This is useful for testing coder
   * implementations.
   */
  public static <T> void testByteCount(Coder<T> coder, Coder.Context context, T[] elements)
      throws Exception {
    Counter<Long> meanByteCount = Counter.longs("meanByteCount", MEAN);
    ElementByteSizeObserver observer = new ElementByteSizeObserver(meanByteCount);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    for (T elem : elements) {
      coder.registerByteSizeObserver(elem, observer, context);
      coder.encode(elem, os, context);
      observer.advance();
    }
    long expectedLength = os.toByteArray().length;

    Assert.assertEquals(expectedLength, (long) getTotalAggregate(meanByteCount));
    Assert.assertEquals(elements.length, getTotalCount(meanByteCount));
  }
}
