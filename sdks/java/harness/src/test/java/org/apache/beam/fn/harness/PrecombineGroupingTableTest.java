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
package org.apache.beam.fn.harness;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.beam.fn.harness.GroupingTable.Receiver;
import org.apache.beam.fn.harness.PrecombineGroupingTable.Combiner;
import org.apache.beam.fn.harness.PrecombineGroupingTable.GroupingKeyCreator;
import org.apache.beam.fn.harness.PrecombineGroupingTable.SamplingSizeEstimator;
import org.apache.beam.fn.harness.PrecombineGroupingTable.SizeEstimator;
import org.apache.beam.sdk.values.KV;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PrecombineGroupingTable}. */
@RunWith(JUnit4.class)
public class PrecombineGroupingTableTest {

  private static class TestOutputReceiver implements Receiver {
    final List<Object> outputElems = new ArrayList<>();

    @Override
    public void process(Object elem) {
      outputElems.add(elem);
    }
  }

  @Test
  public void testCombiningGroupingTable() throws Exception {
    Combiner<Object, Integer, Long, Long> summingCombineFn =
        new Combiner<Object, Integer, Long, Long>() {

          @Override
          public Long createAccumulator(Object key) {
            return 0L;
          }

          @Override
          public Long add(Object key, Long accumulator, Integer value) {
            return accumulator + value;
          }

          @Override
          public Long merge(Object key, Iterable<Long> accumulators) {
            long sum = 0;
            for (Long part : accumulators) {
              sum += part;
            }
            return sum;
          }

          @Override
          public Long compact(Object key, Long accumulator) {
            return accumulator;
          }

          @Override
          public Long extract(Object key, Long accumulator) {
            return accumulator;
          }
        };

    PrecombineGroupingTable<String, Integer, Long> table =
        new PrecombineGroupingTable<>(
            100_000_000L,
            new IdentityGroupingKeyCreator(),
            new KvPairInfo(),
            summingCombineFn,
            new StringPowerSizeEstimator(),
            new IdentitySizeEstimator());
    table.setMaxSize(1000);

    TestOutputReceiver receiver = new TestOutputReceiver();

    table.put("A", 1, receiver);
    table.put("B", 2, receiver);
    table.put("B", 3, receiver);
    table.put("C", 4, receiver);
    assertThat(receiver.outputElems, empty());

    table.put("C", 5000, receiver);
    assertThat(receiver.outputElems, hasItem((Object) KV.of("C", 5004L)));

    table.put("DDDD", 6, receiver);
    assertThat(receiver.outputElems, hasItem((Object) KV.of("DDDD", 6L)));

    table.flush(receiver);
    assertThat(
        receiver.outputElems,
        IsIterableContainingInAnyOrder.containsInAnyOrder(
            KV.of("A", 1L), KV.of("B", 2L + 3), KV.of("C", 5000L + 4), KV.of("DDDD", 6L)));
  }

  ////////////////////////////////////////////////////////////////////////////
  // Tests for the sampling size estimator.

  @Test
  public void testSampleFlatSizes() throws Exception {
    IdentitySizeEstimator underlying = new IdentitySizeEstimator();
    SizeEstimator<Long> estimator =
        new SamplingSizeEstimator<>(underlying, 0.05, 1.0, 10, new Random(1));
    // First 10 elements are always sampled.
    for (int k = 0; k < 10; k++) {
      assertEquals(100, estimator.estimateSize(100L));
      assertEquals(k + 1, underlying.calls);
    }
    // Next 10 are sometimes sampled.
    for (int k = 10; k < 20; k++) {
      assertEquals(100, estimator.estimateSize(100L));
    }
    assertThat(underlying.calls, between(11, 19));
    int initialCalls = underlying.calls;
    // Next 1000 are sampled at about 5%.
    for (int k = 20; k < 1020; k++) {
      assertEquals(100, estimator.estimateSize(100L));
    }
    assertThat(underlying.calls - initialCalls, between(40, 60));
  }

  @Test
  public void testSampleBoringSizes() throws Exception {
    IdentitySizeEstimator underlying = new IdentitySizeEstimator();
    SizeEstimator<Long> estimator =
        new SamplingSizeEstimator<>(underlying, 0.05, 1.0, 10, new Random(1));
    // First 10 elements are always sampled.
    for (int k = 0; k < 10; k += 2) {
      assertEquals(100, estimator.estimateSize(100L));
      assertEquals(102, estimator.estimateSize(102L));
      assertEquals(k + 2, underlying.calls);
    }
    // Next 10 are sometimes sampled.
    for (int k = 10; k < 20; k += 2) {
      assertThat(estimator.estimateSize(100L), between(100L, 102L));
      assertThat(estimator.estimateSize(102L), between(100L, 102L));
    }
    assertThat(underlying.calls, between(11, 19));
    int initialCalls = underlying.calls;
    // Next 1000 are sampled at about 5%.
    for (int k = 20; k < 1020; k += 2) {
      assertThat(estimator.estimateSize(100L), between(100L, 102L));
      assertThat(estimator.estimateSize(102L), between(100L, 102L));
    }
    assertThat(underlying.calls - initialCalls, between(40, 60));
  }

  @Test
  public void testSampleHighVarianceSizes() throws Exception {
    // The largest element is much larger than the average.
    List<Long> sizes = Arrays.asList(1L, 10L, 100L, 1000L);
    IdentitySizeEstimator underlying = new IdentitySizeEstimator();
    SizeEstimator<Long> estimator =
        new SamplingSizeEstimator<>(underlying, 0.1, 0.2, 10, new Random(1));
    // First 10 elements are always sampled.
    for (int k = 0; k < 10; k++) {
      long size = sizes.get(k % sizes.size());
      assertEquals(size, estimator.estimateSize(size));
      assertEquals(k + 1, underlying.calls);
    }
    // We're still not out of the woods; sample every element.
    for (int k = 10; k < 20; k++) {
      long size = sizes.get(k % sizes.size());
      assertEquals(size, estimator.estimateSize(size));
      assertEquals(k + 1, underlying.calls);
    }
    // Sample some more to let things settle down.
    for (int k = 20; k < 500; k++) {
      estimator.estimateSize(sizes.get(k % sizes.size()));
    }
    // Next 1000 are sampled at about 20% (maxSampleRate).
    int initialCalls = underlying.calls;
    for (int k = 500; k < 1500; k++) {
      long size = sizes.get(k % sizes.size());
      assertThat(estimator.estimateSize(size), anyOf(is(in(sizes)), between(250L, 350L)));
    }
    assertThat(underlying.calls - initialCalls, between(180, 220));
    // Sample some more to let things settle down.
    for (int k = 1500; k < 3000; k++) {
      estimator.estimateSize(sizes.get(k % sizes.size()));
    }
    // Next 1000 are sampled at about 10% (minSampleRate).
    initialCalls = underlying.calls;
    for (int k = 3000; k < 4000; k++) {
      long size = sizes.get(k % sizes.size());
      assertThat(estimator.estimateSize(size), anyOf(is(in(sizes)), between(250L, 350L)));
    }
    assertThat(underlying.calls - initialCalls, between(90, 110));
  }

  @Test
  public void testSampleChangingSizes() throws Exception {
    IdentitySizeEstimator underlying = new IdentitySizeEstimator();
    SizeEstimator<Long> estimator =
        new SamplingSizeEstimator<>(underlying, 0.05, 1.0, 10, new Random(1));
    // First 10 elements are always sampled.
    for (int k = 0; k < 10; k++) {
      assertEquals(100, estimator.estimateSize(100L));
      assertEquals(k + 1, underlying.calls);
    }
    // Next 10 are sometimes sampled.
    for (int k = 10; k < 20; k++) {
      assertEquals(100, estimator.estimateSize(100L));
    }
    assertThat(underlying.calls, between(11, 19));
    int initialCalls = underlying.calls;
    // Next 1000 are sampled at about 5%.
    for (int k = 20; k < 1020; k++) {
      assertEquals(100, estimator.estimateSize(100L));
    }
    assertThat(underlying.calls - initialCalls, between(40, 60));
    // Inject a big element until it is sampled.
    while (estimator.estimateSize(1000000L) == 100) {}
    // Check that we have started sampling more regularly again.
    assertEquals(99, estimator.estimateSize(99L));
  }

  private static <T extends Comparable<T>> TypeSafeDiagnosingMatcher<T> between(
      final T min, final T max) {
    return new TypeSafeDiagnosingMatcher<T>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("is between " + min + " and " + max);
      }

      @Override
      protected boolean matchesSafely(T item, Description mismatchDescription) {
        return min.compareTo(item) <= 0 && item.compareTo(max) <= 0;
      }
    };
  }

  /** Return the key as its grouping key. */
  private static class IdentityGroupingKeyCreator implements GroupingKeyCreator<Object> {
    @Override
    public Object createGroupingKey(Object key) {
      return key;
    }
  }

  /** "Estimate" the size of longs by looking at their value. */
  private static class IdentitySizeEstimator implements SizeEstimator<Long> {
    int calls = 0;

    @Override
    public long estimateSize(Long element) {
      calls++;
      return element;
    }
  }

  /** "Estimate" the size of strings by taking the tenth power of their length. */
  private static class StringPowerSizeEstimator implements SizeEstimator<String> {
    @Override
    public long estimateSize(String element) {
      return (long) Math.pow(10, element.length());
    }
  }

  private static class KvPairInfo implements PrecombineGroupingTable.PairInfo {
    @SuppressWarnings("unchecked")
    @Override
    public Object getKeyFromInputPair(Object pair) {
      return ((KV<Object, ?>) pair).getKey();
    }

    @Override
    public Object getValueFromInputPair(Object pair) {
      return ((KV<?, ?>) pair).getValue();
    }

    @Override
    public Object makeOutputPair(Object key, Object value) {
      return KV.of(key, value);
    }
  }
}
