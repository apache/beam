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
package org.apache.beam.sdk.extensions.euphoria.core.testkit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.MergingWindowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.TimeInterval;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.WindowedElement;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceStateByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceWindow;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.NoopTrigger;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.Trigger;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.TriggerContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Fold;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SnapshotProvider;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.AbstractOperatorTest;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.Processing;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.KryoCoder;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;

/** Test operator {@code ReduceByKey}. */
@Processing(Processing.Type.ALL)
public class ReduceByKeyTest extends AbstractOperatorTest {

  /** Validates the output type upon a `.reduceBy` operation on global window. */
  @Test
  public void testReductionType0() {
    execute(
        new AbstractTestCase<Integer, KV<Integer, Set<Integer>>>() {
          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9);
          }

          @Override
          protected Dataset<KV<Integer, Set<Integer>>> getOutput(Dataset<Integer> input) {
            return ReduceByKey.of(input)
                .keyBy(e -> e % 2)
                .valueBy(e -> e)
                .reduceBy(s -> s.collect(Collectors.toSet()))
                .windowBy(new GlobalWindows())
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @Override
          public List<KV<Integer, Set<Integer>>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(0, Sets.newHashSet(2, 4, 6)), KV.of(1, Sets.newHashSet(1, 3, 5, 7, 9)));
          }
        });
  }

  /** Validates the output type upon a `.reduceBy` operation on global window. */
  @Test
  public void testReductionType0_outputValues() {
    execute(
        new AbstractTestCase<Integer, Set<Integer>>() {
          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9);
          }

          @Override
          protected Dataset<Set<Integer>> getOutput(Dataset<Integer> input) {
            return ReduceByKey.of(input)
                .keyBy(e -> e % 2)
                .valueBy(e -> e)
                .reduceBy(s -> s.collect(Collectors.toSet()))
                .windowBy(new GlobalWindows())
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .outputValues();
          }

          @Override
          public List<Set<Integer>> getUnorderedOutput() {
            return Arrays.asList(Sets.newHashSet(2, 4, 6), Sets.newHashSet(1, 3, 5, 7, 9));
          }
        });
  }

  /** Validates the output type upon a `.reduceBy` operation on global window. */
  @Ignore("Sorting of values is not supported yet.")
  @Test
  public void testReductionType0WithSortedValues() {
    execute(
        new AbstractTestCase<Integer, List<KV<Integer, List<Integer>>>>(
            /* don't parallelize this test, because it doesn't work
             * well with count windows */
            1) {
          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(9, 8, 7, 6, 5, 4, 3, 2, 1);
          }

          @Override
          protected Dataset<List<KV<Integer, List<Integer>>>> getOutput(Dataset<Integer> input) {
            Dataset<KV<Integer, List<Integer>>> reducedByWindow =
                ReduceByKey.of(input)
                    .keyBy(e -> e % 2)
                    .valueBy(e -> e)
                    .reduceBy(s -> s.collect(Collectors.toList()))
                    .withSortedValues(Integer::compare)
                    //.windowBy(Count.of(3)) //TODO rewrite to Beam windowing
                    .output();

            return ReduceWindow.of(reducedByWindow)
                .reduceBy(s -> s.collect(Collectors.toList()))
                .withSortedValues(
                    (l, r) -> {
                      int cmp = l.getKey().compareTo(r.getKey());
                      if (cmp == 0) {
                        int firstLeft = l.getValue().get(0);
                        int firstRight = r.getValue().get(0);
                        cmp = Integer.compare(firstLeft, firstRight);
                      }
                      return cmp;
                    })
                .windowBy(new GlobalWindows())
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @Override
          public void validate(List<List<KV<Integer, List<Integer>>>> outputs)
              throws AssertionError {

            assertEquals(1, outputs.size());
            assertEquals(
                Lists.newArrayList(
                    KV.of(0, Lists.newArrayList(2)),
                    KV.of(0, Lists.newArrayList(4, 6, 8)),
                    KV.of(1, Lists.newArrayList(1, 3)),
                    KV.of(1, Lists.newArrayList(5, 7, 9))),
                outputs.get(0));
          }
        });
  }

  /** Validates the output type upon a `.reduceBy` operation on windows of size one. */
  @Test
  public void testReductionType0MultiValues() {
    execute(
        new AbstractTestCase<Integer, KV<Integer, Integer>>(
            /* don't parallelize this test, because it doesn't work
             * well with count windows */
            1) {

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9);
          }

          @Override
          protected Dataset<KV<Integer, Integer>> getOutput(Dataset<Integer> input) {
            return ReduceByKey.of(input)
                .keyBy(e -> e % 2)
                .reduceBy(Fold.whileEmittingEach(0, (a, b) -> a + b))
                .windowBy(new GlobalWindows())
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @Override
          public void validate(List<KV<Integer, Integer>> output) {
            Map<Integer, List<Integer>> byKey =
                output
                    .stream()
                    .collect(
                        Collectors.groupingBy(
                            KV::getKey, Collectors.mapping(KV::getValue, Collectors.toList())));

            assertEquals(2, byKey.size());

            assertNotNull(byKey.get(0));
            assertEquals(3, byKey.get(0).size());
            assertEquals(Arrays.asList(2, 6, 12), byKey.get(0));

            assertNotNull(byKey.get(1));
            assertEquals(Sets.newHashSet(1, 4, 9, 16, 25), new HashSet<>(byKey.get(1)));
          }

          @Override
          public List<KV<Integer, Integer>> getUnorderedOutput() {
            //            return Arrays.asList(KV.of(0, 12), KV.of(1, 9), KV.of(1, 16));
            return Arrays.asList(KV.of(0, 12), KV.of(1, 25));
          }
        });
  }

  @Test
  public void testEventTime() {
    execute(
        new AbstractTestCase<KV<Integer, Long>, KV<Integer, Long>>() {

          @Override
          protected Dataset<KV<Integer, Long>> getOutput(Dataset<KV<Integer, Long>> input) {
            input = AssignEventTime.of(input).using(KV::getValue).output();
            return ReduceByKey.of(input)
                .keyBy(KV::getKey)
                .valueBy(e -> 1L)
                .combineBy(Sums.ofLongs())
                .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<KV<Integer, Long>> getInput() {
            return Arrays.asList(
                KV.of(1, 300L),
                KV.of(2, 600L),
                KV.of(3, 900L),
                KV.of(2, 1300L),
                KV.of(3, 1600L),
                KV.of(1, 1900L),
                KV.of(3, 2300L),
                KV.of(2, 2600L),
                KV.of(1, 2900L),
                KV.of(2, 3300L),
                KV.of(2, 300L),
                KV.of(4, 600L),
                KV.of(3, 900L),
                KV.of(4, 1300L),
                KV.of(2, 1600L),
                KV.of(3, 1900L),
                KV.of(4, 2300L),
                KV.of(1, 2600L),
                KV.of(3, 2900L),
                KV.of(4, 3300L),
                KV.of(3, 3600L));
          }

          @Override
          public List<KV<Integer, Long>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(2, 2L),
                KV.of(4, 1L), // first window
                KV.of(2, 2L),
                KV.of(4, 1L), // second window
                KV.of(2, 1L),
                KV.of(4, 1L), // third window
                KV.of(2, 1L),
                KV.of(4, 1L), // fourth window
                KV.of(1, 1L),
                KV.of(3, 2L), // first window
                KV.of(1, 1L),
                KV.of(3, 2L), // second window
                KV.of(1, 2L),
                KV.of(3, 2L), // third window
                KV.of(3, 1L)); // fourth window
          }
        });
  }

  @Test
  public void testReduceWithWindowing() {
    execute(
        new AbstractTestCase<Integer, KV<Integer, Long>>() {
          @Override
          protected Dataset<KV<Integer, Long>> getOutput(Dataset<Integer> input) {
            WindowFn<Object, CountWindow> windowing = (WindowFn) new TestWindowFn();

            return ReduceByKey.of(input)
                .keyBy(e -> e % 3)
                .valueBy(e -> 1L)
                .combineBy(Sums.ofLongs())
                .windowBy(windowing)
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(
                1,
                2,
                3 /* first window, keys 1, 2, 0 */,
                4,
                5,
                6,
                7 /* second window, keys 1, 2, 0, 1 */,
                8,
                9,
                10 /* third window, keys 2, 0, 1 */,
                5,
                6,
                7 /* second window, keys 2, 0, 1 */,
                8,
                9,
                10,
                11 /* third window, keys 2, 0, 1, 2 */,
                12,
                13,
                14,
                15 /* fourth window, keys 0, 1, 2, 0 */);
          }

          @Override
          public List<KV<Integer, Long>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(0, 1L),
                KV.of(2, 1L) /* first window */,
                KV.of(0, 2L),
                KV.of(2, 2L) /* second window */,
                KV.of(0, 2L),
                KV.of(2, 3L) /* third window */,
                KV.of(0, 2L),
                KV.of(2, 1L) /* fourth window */,
                KV.of(1, 1L) /* first window*/,
                KV.of(1, 3L) /* second window */,
                KV.of(1, 2L) /* third window */,
                KV.of(1, 1L) /* fourth window */);
          }
        });
  }

  // ~ Makes no sense to test UNBOUNDED input without windowing defined.
  // It would run infinitely without providing any result.
  @Processing(Processing.Type.BOUNDED)
  @Test
  public void testReduceWithoutWindowing() {
    execute(
        new AbstractTestCase<String, KV<String, Long>>() {
          @Override
          protected List<String> getInput() {
            String[] words =
                "one two three four one two three four one two three one two one".split(" ");
            return Arrays.asList(words);
          }

          @Override
          public List<KV<String, Long>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of("one", 5L), KV.of("two", 4L), KV.of("three", 3L), KV.of("four", 2L));
          }

          @Override
          protected Dataset<KV<String, Long>> getOutput(Dataset<String> input) {
            return ReduceByKey.of(input)
                .keyBy(e -> e, TypeDescriptor.of(String.class))
                .valueBy(e -> 1L, TypeDescriptor.of(Long.class))
                .combineBy(Sums.ofLongs(), TypeDescriptor.of(Long.class))
                .output();
          }
        });
  }

  @Ignore("Sorting of values is not supported yet.")
  @Processing(Processing.Type.BOUNDED)
  @Test
  public void testReduceSorted() {
    execute(
        new AbstractTestCase<KV<String, Long>, KV<String, List<Long>>>() {

          @Override
          protected List<KV<String, Long>> getInput() {
            return Arrays.asList(
                KV.of("one", 3L),
                KV.of("one", 2L),
                KV.of("one", 1L),
                KV.of("two", 3L),
                KV.of("two", 2L),
                KV.of("two", 1L),
                KV.of("three", 3L),
                KV.of("three", 2L),
                KV.of("three", 1L));
          }

          @Override
          public List<KV<String, List<Long>>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of("one", Arrays.asList(1L, 2L, 3L)),
                KV.of("two", Arrays.asList(1L, 2L, 3L)),
                KV.of("three", Arrays.asList(1L, 2L, 3L)));
          }

          @Override
          protected Dataset<KV<String, List<Long>>> getOutput(Dataset<KV<String, Long>> input) {
            return ReduceByKey.of(input)
                .keyBy(KV::getKey)
                .valueBy(KV::getValue)
                .reduceBy(
                    (Stream<Long> values, Collector<List<Long>> coll) ->
                        coll.collect(values.collect(Collectors.toList())))
                .withSortedValues(Long::compareTo)
                .output();
          }
        });
  }

  @Ignore("Test adaption to Beam windowing failed so far.")
  @Test
  public void testMergingAndTriggering() {
    execute(
        new AbstractTestCase<KV<String, Long>, KV<String, Long>>(1) {

          @Override
          protected List<KV<String, Long>> getInput() {
            return Arrays.asList(
                KV.of("a", 20L),
                KV.of("c", 3_000L),
                KV.of("b", 10L),
                KV.of("b", 100L),
                KV.of("a", 4_000L),
                KV.of("c", 300L),
                KV.of("b", 1_000L),
                KV.of("b", 50_000L),
                KV.of("a", 100_000L),
                KV.of("a", 800L),
                KV.of("a", 80L));
          }

          @Override
          protected Dataset<KV<String, Long>> getOutput(Dataset<KV<String, Long>> input) {
            return ReduceByKey.of(input)
                .keyBy(KV::getKey)
                .valueBy(KV::getValue)
                .combineBy(Sums.ofLongs())
                .windowBy(new MergingByBucketSizeWindowFn<>(3))
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @SuppressWarnings("unchecked")
          @Override
          public List<KV<String, Long>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of("a", 880L),
                KV.of("a", 104_020L),
                KV.of("b", 1_110L),
                KV.of("b", 50_000L),
                KV.of("c", 3_300L));
          }
        });
  }

  // ----------------------------------------------------------------------------
  @Ignore("Test depends on yet unsupported functionality (access to window from Collector). ")
  @Test
  public void testSessionWindowing() {
    execute(
        new AbstractTestCase<KV<String, Integer>, Triple<TimeInterval, Integer, Set<String>>>() {

          @Override
          protected List<KV<String, Integer>> getInput() {
            return Arrays.asList(
                KV.of("1-one", 1),
                KV.of("2-one", 2),
                KV.of("1-two", 4),
                KV.of("1-three", 8),
                KV.of("1-four", 10),
                KV.of("2-two", 10),
                KV.of("1-five", 18),
                KV.of("2-three", 20),
                KV.of("1-six", 22));
          }

          @Override
          protected Dataset<Triple<TimeInterval, Integer, Set<String>>> getOutput(
              Dataset<KV<String, Integer>> input) {
            input = AssignEventTime.of(input).using(KV::getValue).output();
            Dataset<KV<Integer, Set<String>>> reduced =
                ReduceByKey.of(input)
                    .keyBy(e -> e.getKey().charAt(0) - '0')
                    .valueBy(KV::getKey)
                    .reduceBy(s -> s.collect(Collectors.toSet()))
                    .windowBy(FixedWindows.of(org.joda.time.Duration.millis(5)))
                    .triggeredBy(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .output();

            return FlatMap.of(reduced)
                .using(
                    (UnaryFunctor<
                            KV<Integer, Set<String>>, Triple<TimeInterval, Integer, Set<String>>>)
                        (elem, context) ->
                            context.collect(
                                Triple.of(
                                    (TimeInterval) context.getWindow(),
                                    elem.getKey(),
                                    elem.getValue())))
                .output();
          }

          @Override
          public List<Triple<TimeInterval, Integer, Set<String>>> getUnorderedOutput() {
            return Arrays.asList(
                Triple.of(
                    new TimeInterval(1, 15),
                    1,
                    Sets.newHashSet("1-four", "1-one", "1-three", "1-two")),
                Triple.of(new TimeInterval(10, 15), 2, Sets.newHashSet("2-two")),
                Triple.of(new TimeInterval(18, 27), 1, Sets.newHashSet("1-five", "1-six")),
                Triple.of(new TimeInterval(2, 7), 2, Sets.newHashSet("2-one")),
                Triple.of(new TimeInterval(20, 25), 2, Sets.newHashSet("2-three")));
          }
        });
  }

  @Ignore("Test depends on unsupported ReduceStateByKey operator.")
  @Test
  public void testElementTimestamp() {

    class AssertingWindowFn<T> extends WindowFn<T, BoundedWindow> {

      @Override
      public Collection<BoundedWindow> assignWindows(AssignContext c) throws Exception {
        long timestamp = c.timestamp().getMillis();

        // ~ we expect the 'element time' to be the end of the window which produced the
        // element in the preceding upstream (stateful and windowed) operator
        assertTrue(
            "Invalid timestamp " + timestamp, timestamp == 15_000L - 1 || timestamp == 25_000L - 1);

        return Collections.singleton(GlobalWindow.INSTANCE);
      }

      @Override
      public void mergeWindows(MergeContext c) throws Exception {}

      @Override
      public boolean isCompatible(WindowFn<?, ?> other) {
        return other instanceof GlobalWindows;
      }

      @Override
      public Coder<BoundedWindow> windowCoder() {
        return KryoCoder.withoutClassRegistration();
      }

      @Override
      @Nullable
      public WindowMappingFn<BoundedWindow> getDefaultWindowMappingFn() {
        return null;
      }
    }

    execute(
        new AbstractTestCase<KV<Integer, Long>, Integer>() {
          @Override
          protected List<KV<Integer, Long>> getInput() {
            return Arrays.asList(
                // ~ KV.of(value, time)
                KV.of(1, 10_123L),
                KV.of(2, 11_234L),
                KV.of(3, 12_345L),
                // ~ note: exactly one element for the window on purpose (to test out
                // all is well even in case our `.combineBy` user function is not called.)
                KV.of(4, 21_456L));
          }

          @Override
          protected Dataset<Integer> getOutput(Dataset<KV<Integer, Long>> input) {
            // ~ this operator is supposed to emit elements internally with a timestamp
            // which equals the emission (== end in this case) of the time window
            input = AssignEventTime.of(input).using(KV::getValue).output();
            Dataset<KV<String, Integer>> reduced =
                ReduceByKey.of(input)
                    .keyBy(e -> "", TypeDescriptors.strings())
                    .valueBy(KV::getKey, TypeDescriptors.integers())
                    .combineBy(Sums.ofInts(), TypeDescriptors.integers())
                    //                    .windowBy(Time.of(Duration.ofSeconds(5)))
                    .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(5)))
                    .triggeredBy(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .output();
            // ~ now use a custom windowing with a trigger which does
            // the assertions subject to this test (use RSBK which has to
            // use triggering, unlike an optimized RBK)
            Dataset<KV<String, Integer>> output =
                ReduceStateByKey.of(reduced)
                    .keyBy(KV::getKey)
                    .valueBy(KV::getValue)
                    .stateFactory(SumState::new)
                    .mergeStatesBy(SumState::combine)
                    .windowBy(new AssertingWindowFn<>())
                    .triggeredBy(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .output();
            return FlatMap.of(output)
                .using(
                    (UnaryFunctor<KV<String, Integer>, Integer>)
                        (elem, context) -> context.collect(elem.getValue()))
                .output();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(4, 6);
          }
        });
  }

  @Test
  public void testReduceByKeyWithWrongHashCodeImpl() {
    execute(
        new AbstractTestCase<KV<Word, Long>, KV<Word, Long>>() {

          @Override
          protected Dataset<KV<Word, Long>> getOutput(Dataset<KV<Word, Long>> input) {
            input = AssignEventTime.of(input).using(KV::getValue).output();
            return ReduceByKey.of(input)
                .keyBy(KV::getKey)
                .valueBy(e -> 1L)
                .combineBy(Sums.ofLongs())
                .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<KV<Word, Long>> getInput() {
            return Arrays.asList(
                KV.of(new Word("euphoria"), 300L),
                KV.of(new Word("euphoria"), 600L),
                KV.of(new Word("spark"), 900L),
                KV.of(new Word("euphoria"), 1300L),
                KV.of(new Word("flink"), 1600L),
                KV.of(new Word("spark"), 1900L));
          }

          @Override
          public List<KV<Word, Long>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(new Word("euphoria"), 2L),
                KV.of(new Word("spark"), 1L), // first window
                KV.of(new Word("euphoria"), 1L),
                KV.of(new Word("spark"), 1L), // second window
                KV.of(new Word("flink"), 1L));
          }
        });
  }

  @Test
  public void testAccumulators() {
    execute(
        new AbstractTestCase<Integer, KV<Integer, Integer>>() {
          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5);
          }

          @Override
          protected Dataset<KV<Integer, Integer>> getOutput(Dataset<Integer> input) {
            return ReduceByKey.of(input)
                .keyBy(e -> e % 2)
                .valueBy(e -> e)
                .reduceBy(
                    Fold.of(
                        0,
                        (Integer a, Integer b, Collector<Integer> ctx) -> {
                          if (b % 2 == 0) {
                            ctx.getCounter("evens").increment();
                          } else {
                            ctx.getCounter("odds").increment();
                          }
                          ctx.collect(a + b);
                        }))
                .windowBy(new GlobalWindows())
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @SuppressWarnings("unchecked")
          @Override
          public List<KV<Integer, Integer>> getUnorderedOutput() {
            return Arrays.asList(KV.of(1, 9), KV.of(0, 6));
          }

          @Override
          public void validateAccumulators(SnapshotProvider snapshots) {
            Map<String, Long> counters = snapshots.getCounterSnapshots();
            assertEquals(Long.valueOf(2), counters.get("evens"));
            assertEquals(Long.valueOf(3), counters.get("odds"));
          }
        });
  }

  static class TestWindowing implements Windowing<Integer, IntWindow> {

    @Override
    public Iterable<IntWindow> assignWindowsToElement(WindowedElement<?, Integer> input) {
      return Collections.singleton(new IntWindow(input.getElement() / 4));
    }

    @Override
    public Trigger<IntWindow> getTrigger() {
      return NoopTrigger.get();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TestWindowing;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  private static class TestWindowFn extends WindowFn<Number, CountWindow> {

    @Override
    public Collection<CountWindow> assignWindows(AssignContext c) throws Exception {
      Number element = c.element();
      return Collections.singleton(new CountWindow(element.longValue() / 4));
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {}

    @Override
    public boolean isNonMerging() {
      return true;
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return false;
    }

    @Override
    public Coder<CountWindow> windowCoder() {
      return KryoCoder.withoutClassRegistration();
    }

    @Override
    @Nullable
    public WindowMappingFn<CountWindow> getDefaultWindowMappingFn() {
      return null;
    }
  }

  // ~ ------------------------------------------------------------------------------

  private static class CountWindow extends BoundedWindow {

    private long value;

    public CountWindow(long value) {
      this.value = value;
    }

    @Override
    public Instant maxTimestamp() {
      return GlobalWindow.INSTANCE.maxTimestamp();
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof CountWindow) {
        return value == (((CountWindow) other).value);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }
  }

  private static class UniqueWindow extends BoundedWindow {

    private static final AtomicInteger idCounter = new AtomicInteger();
    private final int id;

    public UniqueWindow() {
      this.id = idCounter.getAndIncrement();
    }

    @Override
    public Instant maxTimestamp() {
      return GlobalWindow.INSTANCE.maxTimestamp();
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof UniqueWindow && this.id == ((UniqueWindow) obj).id;
    }

    @Override
    public String toString() {
      return "UniqueWindow{id=" + id + "}";
    }
  }

  /** Every instance is unique: this allows us to exercise merging. */
  public static final class CWindow extends Window<CWindow> {

    private static final Object idCounterMutex = new Object();
    static int idCounter = 0;
    private final int id;
    private final int bucket;

    public CWindow(int bucket) {
      this.id = new_id();
      this.bucket = bucket;
    }

    static int new_id() {
      synchronized (idCounterMutex) {
        return ++idCounter;
      }
    }

    @Override
    public int hashCode() {
      return this.id;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof CWindow && this.id == ((CWindow) obj).id;
    }

    @Override
    public int compareTo(CWindow that) {
      return Integer.compare(this.id, that.id);
    }

    @Override
    public String toString() {
      return "CWindow{" + "bucket=" + bucket + ", identity=" + id + '}';
    }
  }

  // count windowing; firing based on window.bucket (size of the window)
  static final class CWindowTrigger implements Trigger<CWindow> {

    private final ValueStorageDescriptor<Long> countDesc =
        ValueStorageDescriptor.of("count", Long.class, 0L, (x, y) -> x + y);

    @Override
    public TriggerResult onElement(long time, CWindow w, TriggerContext ctx) {
      ValueStorage<Long> cnt = ctx.getValueStorage(countDesc);
      cnt.set(cnt.get() + 1);
      if (cnt.get() >= w.bucket) {
        return TriggerResult.FLUSH_AND_PURGE;
      }
      return TriggerResult.NOOP;
    }

    @Override
    public TriggerResult onTimer(long time, CWindow w, TriggerContext ctx) {
      return TriggerResult.NOOP;
    }

    @Override
    public void onClear(CWindow window, TriggerContext ctx) {
      ctx.getValueStorage(countDesc).clear();
    }

    @Override
    public void onMerge(CWindow w, TriggerContext.TriggerMergeContext ctx) {
      ctx.mergeStoredState(countDesc);
    }
  }

  private static class MergingByBucketSizeWindowFn<T> extends WindowFn<T, UniqueWindow> {

    private final int bucketSize;

    private MergingByBucketSizeWindowFn(int bucketSize) {
      this.bucketSize = bucketSize;
    }

    @Override
    public Collection<UniqueWindow> assignWindows(AssignContext c) throws Exception {
      return Collections.singleton(new UniqueWindow());
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {

      //       merge windows up to bucket size
      Collection<UniqueWindow> windows = c.windows();
      List<UniqueWindow> merges = new ArrayList<>();
      for (UniqueWindow w : windows) {

        merges.add(w);

        if (merges.size() == bucketSize) { // time to merge
          c.merge(merges, w);
          merges.clear();
        }
      }

      if (merges.size() > 1) {
        c.merge(merges, merges.get(merges.size() - 1));
      }
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return other instanceof MergingByBucketSizeWindowFn
          && this.bucketSize == ((MergingByBucketSizeWindowFn) other).bucketSize;
    }

    @Override
    public Coder<UniqueWindow> windowCoder() {
      return KryoCoder.withoutClassRegistration();
    }

    @Override
    @Nullable
    public WindowMappingFn<UniqueWindow> getDefaultWindowMappingFn() {
      return null;
    }
  }

  static final class CWindowing<T> implements MergingWindowing<T, CWindow> {

    private final int size;

    CWindowing(int size) {
      this.size = size;
    }

    @Override
    public Iterable<CWindow> assignWindowsToElement(WindowedElement<?, T> input) {
      return Sets.newHashSet(new CWindow(size));
    }

    @Override
    public Collection<KV<Collection<CWindow>, CWindow>> mergeWindows(Collection<CWindow> actives) {
      Map<Integer, List<CWindow>> byMergeType = new HashMap<>();
      for (CWindow cw : actives) {
        byMergeType.computeIfAbsent(cw.bucket, k -> new ArrayList<>()).add(cw);
      }
      List<KV<Collection<CWindow>, CWindow>> merges = new ArrayList<>();
      for (List<CWindow> siblings : byMergeType.values()) {
        if (siblings.size() >= 2) {
          merges.add(KV.of(siblings, siblings.get(0)));
        }
      }
      return merges;
    }

    @Override
    public Trigger<CWindow> getTrigger() {
      return new CWindowTrigger();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof CWindowing) {
        CWindowing other = (CWindowing) obj;
        return other.size == size;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return size;
    }
  }

  static class SumState implements State<Integer, Integer> {

    private final ValueStorage<Integer> sum;

    SumState(StateContext context, Collector<Integer> collector) {
      sum =
          context
              .getStorageProvider()
              .getValueStorage(ValueStorageDescriptor.of("sum-state", Integer.class, 0));
    }

    static void combine(SumState target, Iterable<SumState> others) {
      for (SumState other : others) {
        target.add(other.sum.get());
      }
    }

    @Override
    public void add(Integer element) {
      sum.set(sum.get() + element);
    }

    @Override
    public void flush(Collector<Integer> context) {
      context.collect(sum.get());
    }

    @Override
    public void close() {
      sum.clear();
    }
  }

  /** String with invalid hash code implementation returning constant. */
  public static class Word implements Serializable {

    private final String str;

    Word(String str) {
      this.str = str;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof Word)) {
        return false;
      }

      Word word = (Word) o;

      return !(str != null ? !str.equals(word.str) : word.str != null);
    }

    @Override
    public int hashCode() {
      return 42;
    }

    @Override
    public String toString() {
      return str;
    }
  }
}
