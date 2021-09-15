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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Fold;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SnapshotProvider;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test operator {@code ReduceByKey}. */
@RunWith(JUnit4.class)
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
          protected PCollection<KV<Integer, Set<Integer>>> getOutput(PCollection<Integer> input) {
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
          protected PCollection<Set<Integer>> getOutput(PCollection<Integer> input) {
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

  @Test
  public void testEventTime() {
    execute(
        new AbstractTestCase<KV<Integer, Long>, KV<Integer, Long>>() {

          @Override
          protected PCollection<KV<Integer, Long>> getOutput(PCollection<KV<Integer, Long>> input) {
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
          protected PCollection<KV<Integer, Long>> getOutput(PCollection<Integer> input) {
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
          protected PCollection<KV<String, Long>> getOutput(PCollection<String> input) {
            return ReduceByKey.of(input)
                .keyBy(e -> e, TypeDescriptor.of(String.class))
                .valueBy(e -> 1L, TypeDescriptor.of(Long.class))
                .combineBy(Sums.ofLongs())
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
          protected PCollection<KV<String, List<Long>>> getOutput(
              PCollection<KV<String, Long>> input) {
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
          protected PCollection<KV<String, Long>> getOutput(PCollection<KV<String, Long>> input) {
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

  @Test
  public void testReduceByKeyWithWrongHashCodeImpl() {
    execute(
        new AbstractTestCase<KV<Word, Long>, KV<Word, Long>>() {

          @Override
          protected PCollection<KV<Word, Long>> getOutput(PCollection<KV<Word, Long>> input) {
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
          protected PCollection<KV<Integer, Integer>> getOutput(PCollection<Integer> input) {
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

  @Test
  public void testCombineFull() {
    execute(
        new AbstractTestCase<Integer, KV<Integer, Integer>>() {

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9);
          }

          @Override
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected PCollection<KV<Integer, Integer>> getOutput(PCollection<Integer> input) {
            return ReduceByKey.of(input)
                .keyBy(e -> e % 2)
                .valueBy(e -> e)
                .combineBy(
                    () -> new ArrayList<>(),
                    (acc, e) -> {
                      acc.add(e);
                      return acc;
                    },
                    (l, r) -> Lists.newArrayList(Iterables.concat(l, r)),
                    List::size,
                    TypeDescriptors.lists(TypeDescriptors.integers()),
                    TypeDescriptors.integers())
                .windowBy(new GlobalWindows())
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @Override
          public List<KV<Integer, Integer>> getUnorderedOutput() {
            return Arrays.asList(KV.of(0, 3), KV.of(1, 5));
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

    /**
     * @param other
     * @return
     * @deprecated deprecated in super class
     */
    @Deprecated
    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return false;
    }

    @Override
    public Coder<CountWindow> windowCoder() {
      return KryoCoder.of(PipelineOptionsFactory.create());
    }

    @Override
    public @Nullable WindowMappingFn<CountWindow> getDefaultWindowMappingFn() {
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
    public boolean equals(@Nullable Object other) {
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
    public boolean equals(@Nullable Object obj) {
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

    /**
     * @param other
     * @return
     * @deprecated deprecated in super class
     */
    @Deprecated
    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return other instanceof MergingByBucketSizeWindowFn
          && this.bucketSize == ((MergingByBucketSizeWindowFn) other).bucketSize;
    }

    @Override
    public Coder<UniqueWindow> windowCoder() {
      return KryoCoder.of(PipelineOptionsFactory.create());
    }

    @Override
    public @Nullable WindowMappingFn<UniqueWindow> getDefaultWindowMappingFn() {
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
    public boolean equals(@Nullable Object o) {
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
