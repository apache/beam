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

import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Fold;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.extensions.euphoria.core.coder.KryoCoder;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SnapshotProvider;
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
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;

/** Test operator {@code ReduceByKey}. */
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
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
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
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected Dataset<Set<Integer>> getOutput(Dataset<Integer> input) {
            return ReduceByKey.of(input)
                .keyBy(e -> e % 2)
                .valueBy(e -> e)
                .reduceBy(s -> s.collect(Collectors.toSet()))
                .outputValues();
          }

          @Override
          public List<Set<Integer>> getUnorderedOutput() {
            return Arrays.asList(Sets.newHashSet(2, 4, 6), Sets.newHashSet(1, 3, 5, 7, 9));
          }
        });
  }

  //  /** Validates the output type upon a `.reduceBy` operation on global window. */
  //  @Ignore("Sorting of values is not supported yet.")
  //  @Test
  //  public void testReductionType0WithSortedValues() {
  //    execute(
  //        new AbstractTestCase<Integer, List<KV<Integer, List<Integer>>>>() {
  //
  //          @Override
  //          protected List<Integer> getInput() {
  //            return Arrays.asList(9, 8, 7, 6, 5, 4, 3, 2, 1);
  //          }
  //
  //          @Override
  //          protected TypeDescriptor<Integer> getInputType() {
  //            return TypeDescriptors.integers();
  //          }
  //
  //          @Override
  //          protected Dataset<List<KV<Integer, List<Integer>>>> getOutput(Dataset<Integer>
  // input) {
  //            Dataset<KV<Integer, List<Integer>>> reducedByWindow =
  //                ReduceByKey.of(input)
  //                    .keyBy(e -> e % 2)
  //                    .valueBy(e -> e)
  //                    .reduceBy(s -> s.collect(Collectors.toList()))
  //                    .withSortedValues(Integer::compare)
  //                    //.windowBy(Count.of(3)) //TODO rewrite to Beam windowing
  //                    .output();
  //
  //            return ReduceWindow.of(reducedByWindow)
  //                .reduceBy(s -> s.collect(Collectors.toList()))
  //                .withSortedValues(
  //                    (l, r) -> {
  //                      int cmp = l.getKey().compareTo(r.getKey());
  //                      if (cmp == 0) {
  //                        int firstLeft = l.getValue().get(0);
  //                        int firstRight = r.getValue().get(0);
  //                        cmp = Integer.compare(firstLeft, firstRight);
  //                      }
  //                      return cmp;
  //                    })
  //                .windowBy(new GlobalWindows())
  //                .triggeredBy(AfterWatermark.pastEndOfWindow())
  //                .discardingFiredPanes()
  //                .output();
  //          }
  //
  //          @Override
  //          public void validate(List<List<KV<Integer, List<Integer>>>> outputs)
  //              throws AssertionError {
  //
  //            assertEquals(1, outputs.size());
  //            assertEquals(
  //                Lists.newArrayList(
  //                    KV.of(0, Lists.newArrayList(2)),
  //                    KV.of(0, Lists.newArrayList(4, 6, 8)),
  //                    KV.of(1, Lists.newArrayList(1, 3)),
  //                    KV.of(1, Lists.newArrayList(5, 7, 9))),
  //                outputs.get(0));
  //          }
  //        });
  //  }

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
                .withAllowedLateness(Duration.ZERO)
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
          protected TypeDescriptor<KV<Integer, Long>> getInputType() {
            return TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.longs());
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
            @SuppressWarnings("unchecked")
            final WindowFn<Object, CountWindow> windowFn = (WindowFn) new TestWindowFn();
            return ReduceByKey.of(input)
                .keyBy(e -> e % 3)
                .valueBy(e -> 1L)
                .combineBy(Sums.ofLongs())
                .windowBy(windowFn)
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO)
                .output();
          }

          @Override
          protected List<Integer> getInput() {
            final List<Integer> input = new ArrayList<>();
            // first window, keys 1, 2, 0
            input.addAll(Arrays.asList(1, 2, 3));
            // second window, keys 1, 2, 0, 1
            input.addAll(Arrays.asList(4, 5, 6, 7));
            // third window, kes 2, 0, 1
            input.addAll(Arrays.asList(8, 9, 10));
            // second window, keys 2, 0, 1
            input.addAll(Arrays.asList(5, 6, 7));
            // third window, keys 2, 0, 1, 2
            input.addAll(Arrays.asList(8, 9, 10, 11));
            // fourth window, keys 0, 1, 2, 0
            input.addAll(Arrays.asList(12, 13, 14, 15));
            return input;
          }

          @Override
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          public List<KV<Integer, Long>> getUnorderedOutput() {
            final List<KV<Integer, Long>> output = new ArrayList<>();
            // first window
            output.addAll(Arrays.asList(KV.of(0, 1L), KV.of(2, 1L)));
            // second window
            output.addAll(Arrays.asList(KV.of(0, 2L), KV.of(2, 2L)));
            // third window
            output.addAll(Arrays.asList(KV.of(0, 2L), KV.of(2, 3L)));
            // fourth window
            output.addAll(Arrays.asList(KV.of(0, 2L), KV.of(2, 1L)));
            // first window
            output.add(KV.of(1, 1L));
            // second window
            output.add(KV.of(1, 3L));
            // third window
            output.add(KV.of(1, 2L));
            // fourth window
            output.add(KV.of(1, 1L));
            return output;
          }
        });
  }

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
          protected TypeDescriptor<String> getInputType() {
            return TypeDescriptors.strings();
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
          protected TypeDescriptor<KV<String, Long>> getInputType() {
            return TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs());
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
        new AbstractTestCase<KV<String, Long>, KV<String, Long>>() {

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
          protected TypeDescriptor<KV<String, Long>> getInputType() {
            return TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs());
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
                .withAllowedLateness(Duration.ZERO)
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

  //  // ----------------------------------------------------------------------------
  //  @Ignore("Test depends on yet unsupported functionality (access to window from Collector). ")
  //  @Test
  //  public void testSessionWindowing() {
  //    execute(
  //        new AbstractTestCase<KV<String, Integer>, Triple<TimeInterval, Integer, Set<String>>>
  // () {
  //
  //          @Override
  //          protected List<KV<String, Integer>> getInput() {
  //            return Arrays.asList(
  //                KV.of("1-one", 1),
  //                KV.of("2-one", 2),
  //                KV.of("1-two", 4),
  //                KV.of("1-three", 8),
  //                KV.of("1-four", 10),
  //                KV.of("2-two", 10),
  //                KV.of("1-five", 18),
  //                KV.of("2-three", 20),
  //                KV.of("1-six", 22));
  //          }
  //
  //          @Override
  //          protected Dataset<Triple<TimeInterval, Integer, Set<String>>> getOutput(
  //              Dataset<KV<String, Integer>> input) {
  //            input = AssignEventTime.of(input).using(KV::getValue).output();
  //            Dataset<KV<Integer, Set<String>>> reduced =
  //                ReduceByKey.of(input)
  //                    .keyBy(e -> e.getKey().charAt(0) - '0')
  //                    .valueBy(KV::getKey)
  //                    .reduceBy(s -> s.collect(Collectors.toSet()))
  //                    .windowBy(FixedWindows.of(org.joda.time.Duration.millis(5)))
  //                    .triggeredBy(AfterWatermark.pastEndOfWindow())
  //                    .discardingFiredPanes()
  //                    .output();
  //
  //            return FlatMap.of(reduced)
  //                .using(
  //                    (UnaryFunctor<
  //                            KV<Integer, Set<String>>, Triple<TimeInterval, Integer,
  // Set<String>>>)
  //                        (elem, context) ->
  //                            context.collect(
  //                                Triple.of(
  //                                    (TimeInterval) context.getWindow(),
  //                                    elem.getKey(),
  //                                    elem.getValue())))
  //                .output();
  //          }
  //
  //          @Override
  //          public List<Triple<TimeInterval, Integer, Set<String>>> getUnorderedOutput() {
  //            return Arrays.asList(
  //                Triple.of(
  //                    new TimeInterval(1, 15),
  //                    1,
  //                    Sets.newHashSet("1-four", "1-one", "1-three", "1-two")),
  //                Triple.of(new TimeInterval(10, 15), 2, Sets.newHashSet("2-two")),
  //                Triple.of(new TimeInterval(18, 27), 1, Sets.newHashSet("1-five", "1-six")),
  //                Triple.of(new TimeInterval(2, 7), 2, Sets.newHashSet("2-one")),
  //                Triple.of(new TimeInterval(20, 25), 2, Sets.newHashSet("2-three")));
  //          }
  //        });
  //  }

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
                .withAllowedLateness(Duration.ZERO)
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
          protected TypeDescriptor<KV<Word, Long>> getInputType() {
            return TypeDescriptors.kvs(new TypeDescriptor<Word>() {}, TypeDescriptors.longs());
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
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected Dataset<KV<Integer, Integer>> getOutput(Dataset<Integer> input) {
            return ReduceByKey.named("test")
                .of(input)
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

    @Deprecated
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

    private CountWindow(long value) {
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

    private UniqueWindow() {
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

    @Deprecated
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
