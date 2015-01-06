/*
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
 */

package com.google.cloud.dataflow.sdk.util.common.worker;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MEAN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.worker.MapTaskExecutorFactory.CoderSizeEstimator;
import com.google.cloud.dataflow.sdk.runners.worker.MapTaskExecutorFactory.ElementByteSizeObservableCoder;
import com.google.cloud.dataflow.sdk.runners.worker.MapTaskExecutorFactory.PairInfo;
import com.google.cloud.dataflow.sdk.runners.worker.MapTaskExecutorFactory.WindowingCoderGroupingKeyCreator;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils.TestReceiver;
import com.google.cloud.dataflow.sdk.util.common.worker.PartialGroupByKeyOperation.BufferingGroupingTable;
import com.google.cloud.dataflow.sdk.util.common.worker.PartialGroupByKeyOperation.Combiner;
import com.google.cloud.dataflow.sdk.util.common.worker.PartialGroupByKeyOperation.CombiningGroupingTable;
import com.google.cloud.dataflow.sdk.util.common.worker.PartialGroupByKeyOperation.GroupingKeyCreator;
import com.google.cloud.dataflow.sdk.util.common.worker.PartialGroupByKeyOperation.SamplingSizeEstimator;
import com.google.cloud.dataflow.sdk.util.common.worker.PartialGroupByKeyOperation.SizeEstimator;
import com.google.cloud.dataflow.sdk.values.KV;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Tests for PartialGroupByKeyOperation.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class PartialGroupByKeyOperationTest {
  @Test
  public void testRunPartialGroupByKeyOperation() throws Exception {
    Coder keyCoder = StringUtf8Coder.of();
    Coder valueCoder = BigEndianIntegerCoder.of();

    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(
        counterPrefix, counterSet.getAddCounterMutator());
    TestReceiver receiver =
        new TestReceiver(
            new ElementByteSizeObservableCoder(
                WindowedValue.getValueOnlyCoder(
                    KvCoder.of(keyCoder, IterableCoder.of(valueCoder)))),
            counterSet, counterPrefix);

    PartialGroupByKeyOperation pgbkOperation =
        new PartialGroupByKeyOperation(
            new WindowingCoderGroupingKeyCreator(keyCoder),
            new CoderSizeEstimator(WindowedValue.getValueOnlyCoder(keyCoder)),
            new CoderSizeEstimator(valueCoder),
            PairInfo.create(),
            receiver,
            counterPrefix,
            counterSet.getAddCounterMutator(),
            stateSampler);

    pgbkOperation.start();

    pgbkOperation.process(WindowedValue.valueInEmptyWindows(KV.of("hi", 4)));
    pgbkOperation.process(WindowedValue.valueInEmptyWindows(KV.of("there", 5)));
    pgbkOperation.process(WindowedValue.valueInEmptyWindows(KV.of("hi", 6)));
    pgbkOperation.process(WindowedValue.valueInEmptyWindows(KV.of("joe", 7)));
    pgbkOperation.process(WindowedValue.valueInEmptyWindows(KV.of("there", 8)));
    pgbkOperation.process(WindowedValue.valueInEmptyWindows(KV.of("hi", 9)));

    pgbkOperation.finish();

    assertThat(receiver.outputElems,
               IsIterableContainingInAnyOrder.<Object>containsInAnyOrder(
                   WindowedValue.valueInEmptyWindows(KV.of("hi", Arrays.asList(4, 6, 9))),
                   WindowedValue.valueInEmptyWindows(KV.of("there", Arrays.asList(5, 8))),
                   WindowedValue.valueInEmptyWindows(KV.of("joe", Arrays.asList(7)))));

    // Exact counter values depend on size of encoded data.  If encoding
    // changes, then these expected counters should change to match.
    assertEquals(
        new CounterSet(
            Counter.longs("test-PartialGroupByKeyOperation-start-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                    "test-PartialGroupByKeyOperation-start-msecs")).getAggregate(false)),
            Counter.longs("test-PartialGroupByKeyOperation-process-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                    "test-PartialGroupByKeyOperation-process-msecs")).getAggregate(false)),
            Counter.longs("test-PartialGroupByKeyOperation-finish-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                    "test-PartialGroupByKeyOperation-finish-msecs")).getAggregate(false)),
            Counter.longs("test_receiver_out-ElementCount", SUM)
                .resetToValue(3L),
            Counter.longs("test_receiver_out-MeanByteCount", MEAN)
                .resetToValue(3, 49L)),
        counterSet);
  }

  // TODO: Add tests about early flushing when the table fills.

  ////////////////////////////////////////////////////////////////////////////
  // Tests for PartialGroupByKey internals.

  /**
   * Return the key as its grouping key.
   */
  public static class IdentityGroupingKeyCreator implements GroupingKeyCreator<Object> {
    @Override
    public Object createGroupingKey(Object key) {
      return key;
    }
  }

  /**
   * "Estimate" the size of longs by looking at their value.
   */
  private static class IdentitySizeEstimator implements SizeEstimator<Long> {
    public int calls = 0;
    @Override
    public long estimateSize(Long element) {
      calls++;
      return element;
    }
  }

  /**
   * "Estimate" the size of strings by taking the tenth power of their length.
   */
  private static class StringPowerSizeEstimator implements SizeEstimator<String> {
    @Override
    public long estimateSize(String element) {
      return (long) Math.pow(10, element.length());
    }
  }

  private static class KvPairInfo implements PartialGroupByKeyOperation.PairInfo {
    @Override
    public Object getKeyFromInputPair(Object pair) {
      return ((KV<?, ?>) pair).getKey();
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

  @Test
  public void testBufferingGroupingTable() throws Exception {
    BufferingGroupingTable<String, String> table =
        new BufferingGroupingTable<>(
            1000, new IdentityGroupingKeyCreator(), new KvPairInfo(),
            new StringPowerSizeEstimator(), new StringPowerSizeEstimator());
    TestReceiver receiver = new TestReceiver(
        KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())));

    table.put("A", "a", receiver);
    table.put("B", "b1", receiver);
    table.put("B", "b2", receiver);
    table.put("C", "c", receiver);
    assertThat(receiver.outputElems, empty());

    table.put("C", "cccc", receiver);
    assertThat(receiver.outputElems,
               hasItem((Object) KV.of("C", Arrays.asList("c", "cccc"))));

    table.put("DDDD", "d", receiver);
    assertThat(receiver.outputElems,
               hasItem((Object) KV.of("DDDD", Arrays.asList("d"))));

    table.flush(receiver);
    assertThat(receiver.outputElems,
               IsIterableContainingInAnyOrder.<Object>containsInAnyOrder(
                   KV.of("A", Arrays.asList("a")),
                   KV.of("B", Arrays.asList("b1", "b2")),
                   KV.of("C", Arrays.asList("c", "cccc")),
                   KV.of("DDDD", Arrays.asList("d"))));
  }

  @Test
  public void testCombiningGroupingTable() throws Exception {
    Combiner<Object, Integer, Long, Long> summingCombineFn =
        new Combiner<Object, Integer, Long, Long>() {
          public Long createAccumulator(Object key) {
            return 0L;
          }
          public Long add(Object key, Long accumulator, Integer value) {
            return accumulator + value;
          }
          public Long merge(Object key, Iterable<Long> accumulators) {
            long sum = 0;
            for (Long part : accumulators) { sum += part; }
            return sum;
          }
          public Long extract(Object key, Long accumulator) {
            return accumulator;
          }
        };

    CombiningGroupingTable<String, Integer, Long> table =
        new CombiningGroupingTable<String, Integer, Long>(
            1000, new IdentityGroupingKeyCreator(), new KvPairInfo(),
            summingCombineFn,
            new StringPowerSizeEstimator(), new IdentitySizeEstimator());

    TestReceiver receiver = new TestReceiver(
        KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()));

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
    assertThat(receiver.outputElems,
               IsIterableContainingInAnyOrder.<Object>containsInAnyOrder(
                   KV.of("A", 1L),
                   KV.of("B", 2L + 3),
                   KV.of("C", 5000L + 4),
                   KV.of("DDDD", 6L)));
  }


  ////////////////////////////////////////////////////////////////////////////
  // Tests for the sampling size estimator.

  @Test
  public void testSampleFlatSizes() throws Exception {
    IdentitySizeEstimator underlying = new IdentitySizeEstimator();
    SizeEstimator<Long> estimator =
        new SamplingSizeEstimator<Long>(underlying, 0.05, 1.0, 10, new Random(1));
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
        new SamplingSizeEstimator<Long>(underlying, 0.05, 1.0, 10, new Random(1));
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
        new SamplingSizeEstimator<Long>(underlying, 0.1, 0.2, 10, new Random(1));
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
      assertThat(estimator.estimateSize(size),
                 anyOf(isIn(sizes), between(250L, 350L)));
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
      assertThat(estimator.estimateSize(size),
                 anyOf(isIn(sizes), between(250L, 350L)));
    }
    assertThat(underlying.calls - initialCalls, between(90, 110));
  }

  @Test
  public void testSampleChangingSizes() throws Exception {
    IdentitySizeEstimator underlying = new IdentitySizeEstimator();
    SizeEstimator<Long> estimator =
        new SamplingSizeEstimator<Long>(underlying, 0.05, 1.0, 10, new Random(1));
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
    while (estimator.estimateSize(1000000L) == 100) { }
    // Check that we have started sampling more regularly again.
    assertEquals(99, estimator.estimateSize(99L));
  }

  private static <T extends Comparable<T>> TypeSafeDiagnosingMatcher<T>
      between(final T min, final T max) {
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
}
