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

import static org.apache.beam.sdk.util.WindowedValue.timestampedValueInGlobalWindow;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.fn.harness.PrecombineGroupingTable.SamplingSizeEstimator;
import org.apache.beam.fn.harness.PrecombineGroupingTable.SizeEstimator;
import org.apache.beam.runners.core.GlobalCombineFnRunner;
import org.apache.beam.runners.core.GlobalCombineFnRunners;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PrecombineGroupingTable}. */
@RunWith(JUnit4.class)
public class PrecombineGroupingTableTest {

  @Rule
  public TestExecutorService executorService = TestExecutors.from(Executors.newCachedThreadPool());

  private static class TestOutputReceiver<T> implements FnDataReceiver<T> {
    final List<T> outputElems = new ArrayList<>();

    @Override
    public void accept(T elem) {
      outputElems.add(elem);
    }
  }

  private static final CombineFn<Integer, Long, Long> COMBINE_FN =
      new CombineFn<Integer, Long, Long>() {

        @Override
        public Long createAccumulator() {
          return 0L;
        }

        @Override
        public Long addInput(Long accumulator, Integer value) {
          return accumulator + value;
        }

        @Override
        public Long mergeAccumulators(Iterable<Long> accumulators) {
          long sum = 0;
          for (Long part : accumulators) {
            sum += part;
          }
          return sum;
        }

        @Override
        public Long compact(Long accumulator) {
          if (accumulator % 2 == 0) {
            return accumulator / 4;
          }
          return accumulator;
        }

        @Override
        public Long extractOutput(Long accumulator) {
          return accumulator;
        }
      };

  @Test
  public void testCombiningInheritsOneOfTheValuesTimestamps() throws Exception {
    PrecombineGroupingTable<String, Integer, Long> table =
        new PrecombineGroupingTable<>(
            PipelineOptionsFactory.create(),
            Caches.forMaximumBytes(2500L),
            StringUtf8Coder.of(),
            GlobalCombineFnRunners.create(COMBINE_FN),
            new TestSizeEstimator(),
            false);

    TestOutputReceiver<WindowedValue<KV<String, Long>>> receiver = new TestOutputReceiver<>();

    table.put(timestampedValueInGlobalWindow(KV.of("A", 1), new Instant(1)), receiver);
    table.put(timestampedValueInGlobalWindow(KV.of("B", 9), new Instant(21)), receiver);
    table.put(timestampedValueInGlobalWindow(KV.of("A", 2), new Instant(1)), receiver);
    table.put(timestampedValueInGlobalWindow(KV.of("B", 2), new Instant(20)), receiver);
    table.put(timestampedValueInGlobalWindow(KV.of("A", 4), new Instant(1)), receiver);
    table.flush(receiver);
    assertThat(
        receiver.outputElems,
        containsInAnyOrder(
            timestampedValueInGlobalWindow(KV.of("A", 1L + 2 + 4), new Instant(1)),
            timestampedValueInGlobalWindow(KV.of("B", 9L + 2), new Instant(21))));
  }

  @Test
  public void testCombiningGroupingTableHonorsKeyWeights() throws Exception {
    PrecombineGroupingTable<String, Integer, Long> table =
        new PrecombineGroupingTable<>(
            PipelineOptionsFactory.create(),
            Caches.forMaximumBytes(2500L),
            StringUtf8Coder.of(),
            GlobalCombineFnRunners.create(COMBINE_FN),
            new TestSizeEstimator(),
            false);

    TestOutputReceiver<WindowedValue<KV<String, Long>>> receiver = new TestOutputReceiver<>();

    // Putting the same 1000 weight key in should not cause any eviction.
    table.put(valueInGlobalWindow(KV.of("AAA", 1)), receiver);
    table.put(valueInGlobalWindow(KV.of("AAA", 2)), receiver);
    table.put(valueInGlobalWindow(KV.of("AAA", 4)), receiver);
    assertThat(receiver.outputElems, empty());

    // Putting in other large keys should cause eviction.
    table.put(valueInGlobalWindow(KV.of("BB", 509)), receiver);
    table.put(valueInGlobalWindow(KV.of("CCC", 11)), receiver);
    assertThat(
        receiver.outputElems,
        containsInAnyOrder(
            valueInGlobalWindow(KV.of("AAA", 1L + 2 + 4)), valueInGlobalWindow(KV.of("BB", 509L))));

    table.flush(receiver);
    assertThat(
        receiver.outputElems,
        containsInAnyOrder(
            valueInGlobalWindow(KV.of("AAA", 1L + 2 + 4)),
            valueInGlobalWindow(KV.of("BB", 509L)),
            valueInGlobalWindow(KV.of("CCC", 11L))));
  }

  @Test
  public void testCombiningGroupingTableEvictsAllOnLargeEntry() throws Exception {
    PrecombineGroupingTable<String, Integer, Long> table =
        new PrecombineGroupingTable<>(
            PipelineOptionsFactory.create(),
            Caches.forMaximumBytes(2500L),
            StringUtf8Coder.of(),
            GlobalCombineFnRunners.create(COMBINE_FN),
            new TestSizeEstimator(),
            false);

    TestOutputReceiver<WindowedValue<KV<String, Long>>> receiver = new TestOutputReceiver<>();

    table.put(valueInGlobalWindow(KV.of("A", 1)), receiver);
    table.put(valueInGlobalWindow(KV.of("B", 3)), receiver);
    table.put(valueInGlobalWindow(KV.of("B", 6)), receiver);
    table.put(valueInGlobalWindow(KV.of("C", 7)), receiver);
    assertThat(receiver.outputElems, empty());

    // Add beyond the size which causes compaction which still leads to evicting all since the
    // largest is most recent.
    table.put(valueInGlobalWindow(KV.of("C", 9999)), receiver);
    assertThat(
        receiver.outputElems,
        containsInAnyOrder(
            valueInGlobalWindow(KV.of("A", 1L)),
            valueInGlobalWindow(KV.of("B", 9L)),
            valueInGlobalWindow(KV.of("C", (9999L + 7) / 4))));

    table.flush(receiver);
    assertThat(
        receiver.outputElems,
        containsInAnyOrder(
            valueInGlobalWindow(KV.of("A", 1L)),
            valueInGlobalWindow(KV.of("B", 3L + 6)),
            valueInGlobalWindow(KV.of("C", (9999L + 7) / 4))));
  }

  @Test
  public void testCombiningGroupingTableCompactionSaves() throws Exception {
    PrecombineGroupingTable<String, Integer, Long> table =
        new PrecombineGroupingTable<>(
            PipelineOptionsFactory.create(),
            Caches.forMaximumBytes(2500L),
            StringUtf8Coder.of(),
            GlobalCombineFnRunners.create(COMBINE_FN),
            new TestSizeEstimator(),
            false);

    TestOutputReceiver<WindowedValue<KV<String, Long>>> receiver = new TestOutputReceiver<>();

    // Insert three compactable values which shouldn't lead to eviction even though we are over
    // the maximum size.
    table.put(valueInGlobalWindow(KV.of("A", 804)), receiver);
    table.put(valueInGlobalWindow(KV.of("B", 904)), receiver);
    table.put(valueInGlobalWindow(KV.of("C", 1004)), receiver);
    assertThat(receiver.outputElems, empty());

    // Ensure that compaction occurred during the insertion of the above elements before flushing.
    assertThat(table.getWeight(), lessThan(804L + 904L + 1004L));

    table.flush(receiver);
    assertThat(
        receiver.outputElems,
        containsInAnyOrder(
            valueInGlobalWindow(KV.of("A", 804L / 4)),
            valueInGlobalWindow(KV.of("B", 904L / 4)),
            valueInGlobalWindow(KV.of("C", 1004L / 4))));
  }

  @Test
  public void testCombiningGroupingTablePartialEviction() throws Exception {
    PrecombineGroupingTable<String, Integer, Long> table =
        new PrecombineGroupingTable<>(
            PipelineOptionsFactory.create(),
            Caches.forMaximumBytes(2500L),
            StringUtf8Coder.of(),
            GlobalCombineFnRunners.create(COMBINE_FN),
            new TestSizeEstimator(),
            false);

    TestOutputReceiver<WindowedValue<KV<String, Long>>> receiver = new TestOutputReceiver<>();

    // Insert three values which even with compaction isn't enough so we evict A & B to get
    // under the max weight.
    table.put(valueInGlobalWindow(KV.of("A", 801)), receiver);
    table.put(valueInGlobalWindow(KV.of("B", 901)), receiver);
    table.put(valueInGlobalWindow(KV.of("C", 1001)), receiver);
    assertThat(
        receiver.outputElems,
        containsInAnyOrder(
            valueInGlobalWindow(KV.of("A", 801L)), valueInGlobalWindow(KV.of("B", 901L))));

    table.flush(receiver);
    assertThat(
        receiver.outputElems,
        containsInAnyOrder(
            valueInGlobalWindow(KV.of("A", 801L)),
            valueInGlobalWindow(KV.of("B", 901L)),
            valueInGlobalWindow(KV.of("C", 1001L))));
  }

  @Test
  public void testCombiningGroupingTableEmitsCorrectValuesUnderHighCacheContention()
      throws Exception {
    Long[] expectedKeys = new Long[1000];
    for (int j = 1; j <= 1000; ++j) {
      expectedKeys[j - 1] = (long) j;
    }

    int numThreads = 1000;
    List<Future<?>> futures = new ArrayList<>(numThreads);
    PipelineOptions options = PipelineOptionsFactory.create();
    GlobalCombineFnRunner<Integer, Long, Long> combineFnRunner =
        GlobalCombineFnRunners.create(COMBINE_FN);
    Cache<Object, Object> cache = Caches.forMaximumBytes(numThreads * 50000);
    for (int i = 0; i < numThreads; ++i) {
      final int currentI = i;
      futures.add(
          executorService.submit(
              () -> {
                ArrayListMultimap<Long, Long> values = ArrayListMultimap.create();
                PrecombineGroupingTable<Long, Integer, Long> table =
                    new PrecombineGroupingTable<>(
                        options,
                        Caches.subCache(cache, currentI),
                        VarLongCoder.of(),
                        combineFnRunner,
                        new TestSizeEstimator(),
                        false);
                for (int j = 1; j <= 1000; ++j) {
                  table.put(
                      valueInGlobalWindow(KV.of((long) j, j)),
                      (input) ->
                          values.put(input.getValue().getKey(), input.getValue().getValue()));
                }
                for (int j = 1; j <= 1000; ++j) {
                  table.flush(
                      (input) ->
                          values.put(input.getValue().getKey(), input.getValue().getValue()));
                }

                assertThat(values.keySet(), containsInAnyOrder(expectedKeys));
                for (Map.Entry<Long, Long> value : values.entries()) {
                  if (value.getKey() % 2 == 0) {
                    assertThat(value.getValue(), equalTo(value.getKey() / 4));
                  } else {
                    assertThat(value.getValue(), equalTo(value.getKey()));
                  }
                }
                return null;
              }));
    }
    for (Future<?> future : futures) {
      future.get();
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  // Tests for the sampling size estimator.

  @Test
  public void testSampleFlatSizes() throws Exception {
    TestSizeEstimator underlying = new TestSizeEstimator();
    SizeEstimator estimator = new SamplingSizeEstimator(underlying, 0.05, 1.0, 10, new Random(1));
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
    TestSizeEstimator underlying = new TestSizeEstimator();
    SizeEstimator estimator = new SamplingSizeEstimator(underlying, 0.05, 1.0, 10, new Random(1));
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
    TestSizeEstimator underlying = new TestSizeEstimator();
    SizeEstimator estimator = new SamplingSizeEstimator(underlying, 0.1, 0.2, 10, new Random(1));
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
    TestSizeEstimator underlying = new TestSizeEstimator();
    SizeEstimator estimator = new SamplingSizeEstimator(underlying, 0.05, 1.0, 10, new Random(1));
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

  /**
   * Used to simulate very specific compaction/eviction tests under certain scenarios instead of
   * relying on JAMM for size estimation. Strings are 10^length and longs are their value.
   */
  private static class TestSizeEstimator implements SizeEstimator {
    int calls = 0;

    @Override
    public long estimateSize(Object element) {
      calls++;
      if (element instanceof PrecombineGroupingTable.GloballyWindowedTableGroupingKey) {
        element =
            ((PrecombineGroupingTable.GloballyWindowedTableGroupingKey) element).getStructuralKey();
      } else if (element instanceof PrecombineGroupingTable.WindowedGroupingTableKey) {
        element = ((PrecombineGroupingTable.WindowedGroupingTableKey) element).getStructuralKey();
      } else if (element instanceof PrecombineGroupingTable.GroupingTableEntry) {
        element = ((PrecombineGroupingTable.GroupingTableEntry) element).getAccumulator();
      }
      if (element instanceof String) {
        return (long) Math.pow(10, ((String) element).length());
      } else if (element instanceof Long) {
        return (Long) element;
      }
      throw new IllegalArgumentException(
          "Unknown type " + (element == null ? "null" : element.getClass().toString()));
    }
  }
}
