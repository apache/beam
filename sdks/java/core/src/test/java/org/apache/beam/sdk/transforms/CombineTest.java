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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.sdk.testing.CombineFnTester.testCombineFn;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasNamespace;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFor;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSideInputs;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineTest.SharedTestBase.TestCombineFn.Accumulator;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Combine} transforms. */
public class CombineTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  /** Base class to share setup/teardown and helpers. */
  public abstract static class SharedTestBase {
    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    protected void runTestSimpleCombine(
        List<KV<String, Integer>> table, int globalSum, List<KV<String, String>> perKeyCombines) {
      PCollection<KV<String, Integer>> input = createInput(pipeline, table);

      PCollection<Integer> sum =
          input.apply(Values.create()).apply(Combine.globally(new SumInts()));

      PCollection<KV<String, String>> sumPerKey = input.apply(Combine.perKey(new TestCombineFn()));

      PAssert.that(sum).containsInAnyOrder(globalSum);
      PAssert.that(sumPerKey).containsInAnyOrder(perKeyCombines);

      pipeline.run();
    }

    @SuppressWarnings("unchecked")
    protected void runTestBasicCombine(
        List<KV<String, Integer>> table,
        Set<Integer> globalUnique,
        List<KV<String, Set<Integer>>> perKeyUnique) {
      PCollection<KV<String, Integer>> input = createInput(pipeline, table);

      PCollection<Set<Integer>> unique =
          input.apply(Values.create()).apply(Combine.globally(new UniqueInts()));

      PCollection<KV<String, Set<Integer>>> uniquePerKey =
          input.apply(Combine.perKey(new UniqueInts()));

      PAssert.that(unique).containsInAnyOrder(globalUnique);
      PAssert.that(uniquePerKey).containsInAnyOrder(perKeyUnique);

      pipeline.run();
    }

    protected void runTestSimpleCombineWithContext(
        List<KV<String, Integer>> table,
        int globalSum,
        List<KV<String, String>> perKeyCombines,
        String[] globallyCombines) {
      PCollection<KV<String, Integer>> perKeyInput = createInput(pipeline, table);
      PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

      PCollection<Integer> sum = globallyInput.apply("Sum", Combine.globally(new SumInts()));

      PCollectionView<Integer> globallySumView = sum.apply(View.asSingleton());

      PCollection<KV<String, String>> combinePerKey =
          perKeyInput.apply(
              Combine.<String, Integer, String>perKey(new TestCombineFnWithContext(globallySumView))
                  .withSideInputs(globallySumView));

      PCollection<String> combineGlobally =
          globallyInput.apply(
              Combine.globally(new TestCombineFnWithContext(globallySumView))
                  .withoutDefaults()
                  .withSideInputs(globallySumView));

      PAssert.that(sum).containsInAnyOrder(globalSum);
      PAssert.that(combinePerKey).containsInAnyOrder(perKeyCombines);
      PAssert.that(combineGlobally).containsInAnyOrder(globallyCombines);

      pipeline.run();
    }

    protected void runTestAccumulatingCombine(
        List<KV<String, Integer>> table, Double globalMean, List<KV<String, Double>> perKeyMeans) {
      PCollection<KV<String, Integer>> input = createInput(pipeline, table);

      PCollection<Double> mean =
          input.apply(Values.create()).apply(Combine.globally(new MeanInts()));

      PCollection<KV<String, Double>> meanPerKey = input.apply(Combine.perKey(new MeanInts()));

      PAssert.that(mean).containsInAnyOrder(globalMean);
      PAssert.that(meanPerKey).containsInAnyOrder(perKeyMeans);

      pipeline.run();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Test classes, for different kinds of combining fns.

    /** Another example AccumulatingCombineFn. */
    public static class TestCounter
        extends Combine.AccumulatingCombineFn<Integer, TestCounter.Counter, Iterable<Long>> {

      /** An accumulator that observes its merges and outputs. */
      public static class Counter
          implements Combine.AccumulatingCombineFn.Accumulator<Integer, Counter, Iterable<Long>>,
              Serializable {

        public long sum = 0;
        public long inputs = 0;
        public long merges = 0;
        public long outputs = 0;

        public Counter(long sum, long inputs, long merges, long outputs) {
          this.sum = sum;
          this.inputs = inputs;
          this.merges = merges;
          this.outputs = outputs;
        }

        @Override
        public void addInput(Integer element) {
          checkState(merges == 0);
          checkState(outputs == 0);

          inputs++;
          sum += element;
        }

        @Override
        public void mergeAccumulator(Counter accumulator) {
          checkState(outputs == 0);
          assertEquals(0, accumulator.outputs);

          merges += accumulator.merges + 1;
          inputs += accumulator.inputs;
          sum += accumulator.sum;
        }

        @Override
        public Iterable<Long> extractOutput() {
          checkState(outputs == 0);

          return Arrays.asList(sum, inputs, merges, outputs);
        }

        @Override
        public int hashCode() {
          return (int) (sum * 17 + inputs * 31 + merges * 43 + outputs * 181);
        }

        @Override
        public boolean equals(@Nullable Object otherObj) {
          if (otherObj instanceof Counter) {
            Counter other = (Counter) otherObj;
            return (sum == other.sum
                && inputs == other.inputs
                && merges == other.merges
                && outputs == other.outputs);
          }
          return false;
        }

        @Override
        public String toString() {
          return sum + ":" + inputs + ":" + merges + ":" + outputs;
        }
      }

      @Override
      public Counter createAccumulator() {
        return new Counter(0, 0, 0, 0);
      }

      @Override
      public Coder<Counter> getAccumulatorCoder(CoderRegistry registry, Coder<Integer> inputCoder) {
        // This is a *very* inefficient encoding to send over the wire, but suffices
        // for tests.
        return SerializableCoder.of(Counter.class);
      }
    }

    /**
     * A {@link CombineFn} that results in a sorted list of all characters occurring in the key and
     * the decimal representations of each value.
     */
    public static class TestCombineFn
        extends CombineFn<Integer, TestCombineFn.Accumulator, String> {

      // Not serializable.
      static class Accumulator {
        final String seed;
        String value;

        public Accumulator(String seed, String value) {
          this.seed = seed;
          this.value = value;
        }

        public static Coder<Accumulator> getCoder() {
          return new AtomicCoder<Accumulator>() {
            @Override
            public void encode(Accumulator accumulator, OutputStream outStream) throws IOException {
              StringUtf8Coder.of().encode(accumulator.seed, outStream);
              StringUtf8Coder.of().encode(accumulator.value, outStream);
            }

            @Override
            public Accumulator decode(InputStream inStream) throws IOException {
              String seed = StringUtf8Coder.of().decode(inStream);
              String value = StringUtf8Coder.of().decode(inStream);
              return new Accumulator(seed, value);
            }
          };
        }
      }

      @Override
      public Coder<Accumulator> getAccumulatorCoder(
          CoderRegistry registry, Coder<Integer> inputCoder) {
        return Accumulator.getCoder();
      }

      @Override
      public Accumulator createAccumulator() {
        return new Accumulator("", "");
      }

      @Override
      public Accumulator addInput(Accumulator accumulator, Integer value) {
        try {
          return new Accumulator(accumulator.seed, accumulator.value + String.valueOf(value));
        } finally {
          accumulator.value = "cleared in addInput";
        }
      }

      @Override
      public Accumulator mergeAccumulators(Iterable<Accumulator> accumulators) {
        Accumulator seedAccumulator = null;
        StringBuilder all = new StringBuilder();
        for (Accumulator accumulator : accumulators) {
          if (seedAccumulator == null) {
            seedAccumulator = accumulator;
          } else {
            assertEquals(
                String.format(
                    "Different seed values in accumulator: %s vs. %s",
                    seedAccumulator, accumulator),
                seedAccumulator.seed,
                accumulator.seed);
          }
          all.append(accumulator.value);
          accumulator.value = "cleared in mergeAccumulators";
        }
        return new Accumulator(checkNotNull(seedAccumulator).seed, all.toString());
      }

      @Override
      public String extractOutput(Accumulator accumulator) {
        char[] chars = accumulator.value.toCharArray();
        Arrays.sort(chars);
        return new String(chars);
      }
    }

    /**
     * A {@link CombineFnWithContext} that produces a sorted list of all characters occurring in the
     * key and the decimal representations of main and side inputs values.
     */
    public static class TestCombineFnWithContext
        extends CombineFnWithContext<Integer, Accumulator, String> {
      private final PCollectionView<Integer> view;

      public TestCombineFnWithContext(PCollectionView<Integer> view) {
        this.view = view;
      }

      @Override
      public Coder<TestCombineFn.Accumulator> getAccumulatorCoder(
          CoderRegistry registry, Coder<Integer> inputCoder) {
        return TestCombineFn.Accumulator.getCoder();
      }

      @Override
      public TestCombineFn.Accumulator createAccumulator(Context c) {
        Integer sideInputValue = c.sideInput(view);
        return new TestCombineFn.Accumulator(sideInputValue.toString(), "");
      }

      @Override
      public TestCombineFn.Accumulator addInput(
          TestCombineFn.Accumulator accumulator, Integer value, Context c) {
        try {
          assertThat(
              "Not expecting view contents to change",
              accumulator.seed,
              Matchers.equalTo(Integer.toString(c.sideInput(view))));
          return new TestCombineFn.Accumulator(
              accumulator.seed, accumulator.value + String.valueOf(value));
        } finally {
          accumulator.value = "cleared in addInput";
        }
      }

      @Override
      public TestCombineFn.Accumulator mergeAccumulators(
          Iterable<TestCombineFn.Accumulator> accumulators, Context c) {
        String sideInputValue = c.sideInput(view).toString();
        StringBuilder all = new StringBuilder();
        for (TestCombineFn.Accumulator accumulator : accumulators) {
          assertThat(
              "Accumulators should all have the same Side Input Value",
              accumulator.seed,
              Matchers.equalTo(sideInputValue));
          all.append(accumulator.value);
          accumulator.value = "cleared in mergeAccumulators";
        }
        return new TestCombineFn.Accumulator(sideInputValue, all.toString());
      }

      @Override
      public String extractOutput(TestCombineFn.Accumulator accumulator, Context c) {
        assertThat(accumulator.seed, Matchers.startsWith(c.sideInput(view).toString()));
        char[] chars = accumulator.value.toCharArray();
        Arrays.sort(chars);
        return accumulator.seed + ":" + new String(chars);
      }
    }

    /** Sample DoFn for testing combine. */
    protected static class FormatPaneInfo extends DoFn<Integer, String> {
      @ProcessElement
      public void processElement(ProcessContext c) {
        c.output(c.element() + ": " + c.pane().isLast());
      }
    }

    protected static final SerializableFunction<String, Integer> HOT_KEY_FANOUT =
        input -> "a".equals(input) ? 3 : 0;

    protected static final SerializableFunction<String, Integer> SPLIT_HOT_KEY_FANOUT =
        input -> Math.random() < 0.5 ? 3 : 0;

    /** Sample DoFn for testing hot keys. */
    protected static class GetLast extends DoFn<Integer, Integer> {
      @ProcessElement
      public void processElement(ProcessContext c) {
        if (c.pane().isLast()) {
          c.output(c.element());
        }
      }
    }

    /** Sample BinaryCombineFn for testing int inputs. */
    protected static final class TestProdInt extends Combine.BinaryCombineIntegerFn {
      @Override
      public int apply(int left, int right) {
        return left * right;
      }

      @Override
      public int identity() {
        return 1;
      }
    }

    /** Sample BinaryCombineFn for testing Integer inputs. */
    protected static final class TestProdObj extends Combine.BinaryCombineFn<Integer> {
      @Override
      public Integer apply(Integer left, Integer right) {
        return left * right;
      }
    }

    /** Computes the product, considering null values to be 2. */
    protected static final class NullCombiner extends Combine.BinaryCombineFn<Integer> {
      @Override
      public Integer apply(Integer left, Integer right) {
        return (left == null ? 2 : left) * (right == null ? 2 : right);
      }
    }

    /** Example SerializableFunction combiner. */
    public static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
      @Override
      public Integer apply(Iterable<Integer> input) {
        int sum = 0;
        for (int item : input) {
          sum += item;
        }
        return sum;
      }
    }

    /** Example CombineFn. */
    public static class UniqueInts extends Combine.CombineFn<Integer, Set<Integer>, Set<Integer>> {

      @Override
      public Set<Integer> createAccumulator() {
        return new HashSet<>();
      }

      @Override
      public Set<Integer> addInput(Set<Integer> accumulator, Integer input) {
        accumulator.add(input);
        return accumulator;
      }

      @Override
      public Set<Integer> mergeAccumulators(Iterable<Set<Integer>> accumulators) {
        Set<Integer> all = new HashSet<>();
        for (Set<Integer> part : accumulators) {
          all.addAll(part);
        }
        return all;
      }

      @Override
      public Set<Integer> extractOutput(Set<Integer> accumulator) {
        return accumulator;
      }
    }

    /** Example AccumulatingCombineFn. */
    protected static class MeanInts
        extends Combine.AccumulatingCombineFn<Integer, MeanInts.CountSum, Double> {
      private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
      private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();

      static class CountSum
          implements Combine.AccumulatingCombineFn.Accumulator<Integer, CountSum, Double> {
        long count = 0;
        double sum = 0.0;

        CountSum(long count, double sum) {
          this.count = count;
          this.sum = sum;
        }

        @Override
        public void addInput(Integer element) {
          count++;
          sum += element.doubleValue();
        }

        @Override
        public void mergeAccumulator(CountSum accumulator) {
          count += accumulator.count;
          sum += accumulator.sum;
        }

        @Override
        public Double extractOutput() {
          return count == 0 ? 0.0 : sum / count;
        }

        @Override
        public int hashCode() {
          return Objects.hash(count, sum);
        }

        @Override
        public boolean equals(@Nullable Object obj) {
          if (obj == this) {
            return true;
          }
          if (!(obj instanceof CountSum)) {
            return false;
          }
          CountSum other = (CountSum) obj;
          return this.count == other.count && (Math.abs(this.sum - other.sum) < 0.1);
        }

        @Override
        public String toString() {
          return MoreObjects.toStringHelper(this).add("count", count).add("sum", sum).toString();
        }
      }

      @Override
      public CountSum createAccumulator() {
        return new CountSum(0, 0.0);
      }

      @Override
      public Coder<CountSum> getAccumulatorCoder(
          CoderRegistry registry, Coder<Integer> inputCoder) {
        return new CountSumCoder();
      }

      /** A {@link Coder} for {@link CountSum}. */
      private static class CountSumCoder extends AtomicCoder<CountSum> {
        @Override
        public void encode(CountSum value, OutputStream outStream) throws IOException {
          LONG_CODER.encode(value.count, outStream);
          DOUBLE_CODER.encode(value.sum, outStream);
        }

        @Override
        public CountSum decode(InputStream inStream) throws IOException {
          long count = LONG_CODER.decode(inStream);
          double sum = DOUBLE_CODER.decode(inStream);
          return new CountSum(count, sum);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}

        @Override
        public boolean isRegisterByteSizeObserverCheap(CountSum value) {
          return true;
        }

        @Override
        public void registerByteSizeObserver(CountSum value, ElementByteSizeObserver observer)
            throws Exception {
          LONG_CODER.registerByteSizeObserver(value.count, observer);
          DOUBLE_CODER.registerByteSizeObserver(value.sum, observer);
        }
      }
    }

    protected static <T> PCollection<T> copy(PCollection<T> pc, final int n) {
      return pc.apply(
          ParDo.of(
              new DoFn<T, T>() {
                @ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                  for (int i = 0; i < n; i++) {
                    c.output(c.element());
                  }
                }
              }));
    }

    /** Class for use in testing use of Java 8 method references. */
    protected static class Summer implements Serializable {
      public int sum(Iterable<Integer> integers) {
        int sum = 0;
        for (int i : integers) {
          sum += i;
        }
        return sum;
      }

      public int add(int a, int b) {
        return a + b;
      }
    }
  }

  static final List<KV<String, Integer>> EMPTY_TABLE = Collections.emptyList();

  private static PCollection<KV<String, Integer>> createInput(
      Pipeline p, List<KV<String, Integer>> table) {
    return p.apply(
        Create.of(table).withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
  }

  /** Tests validating basic Combine transform scenarios. */
  @RunWith(JUnit4.class)
  public static class BasicTests extends SharedTestBase {
    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testSimpleCombine() {
      runTestSimpleCombine(
          Arrays.asList(KV.of("a", 1), KV.of("a", 1), KV.of("a", 4), KV.of("b", 1), KV.of("b", 13)),
          20,
          Arrays.asList(KV.of("a", "114"), KV.of("b", "113")));
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testSimpleCombineEmpty() {
      runTestSimpleCombine(EMPTY_TABLE, 0, Collections.emptyList());
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testBasicCombine() {
      runTestBasicCombine(
          Arrays.asList(KV.of("a", 1), KV.of("a", 1), KV.of("a", 4), KV.of("b", 1), KV.of("b", 13)),
          ImmutableSet.of(1, 13, 4),
          Arrays.asList(
              KV.of("a", (Set<Integer>) ImmutableSet.of(1, 4)),
              KV.of("b", (Set<Integer>) ImmutableSet.of(1, 13))));
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testBasicCombineEmpty() {
      runTestBasicCombine(EMPTY_TABLE, ImmutableSet.of(), Collections.emptyList());
    }

    // Checks that Min, Max, Mean, Sum (operations that pass-through to Combine) have good names.
    @Test
    public void testCombinerNames() {
      Combine.PerKey<String, Integer, Integer> min = Min.integersPerKey();
      Combine.PerKey<String, Integer, Integer> max = Max.integersPerKey();
      Combine.PerKey<String, Integer, Double> mean = Mean.perKey();
      Combine.PerKey<String, Integer, Integer> sum = Sum.integersPerKey();

      assertThat(min.getName(), equalTo("Combine.perKey(MinInteger)"));
      assertThat(max.getName(), equalTo("Combine.perKey(MaxInteger)"));
      assertThat(mean.getName(), equalTo("Combine.perKey(Mean)"));
      assertThat(sum.getName(), equalTo("Combine.perKey(SumInteger)"));
    }

    @Test
    @Category({ValidatesRunner.class})
    public void testHotKeyCombining() {
      PCollection<KV<String, Integer>> input =
          copy(
              createInput(
                  pipeline,
                  Arrays.asList(
                      KV.of("a", 1), KV.of("a", 1), KV.of("a", 4), KV.of("b", 1), KV.of("b", 13))),
              10);

      CombineFn<Integer, ?, Double> mean = new MeanInts();
      PCollection<KV<String, Double>> coldMean =
          input.apply(
              "ColdMean", Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(0));
      PCollection<KV<String, Double>> warmMean =
          input.apply(
              "WarmMean",
              Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(HOT_KEY_FANOUT));
      PCollection<KV<String, Double>> hotMean =
          input.apply("HotMean", Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(5));
      PCollection<KV<String, Double>> splitMean =
          input.apply(
              "SplitMean",
              Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(SPLIT_HOT_KEY_FANOUT));

      List<KV<String, Double>> expected = Arrays.asList(KV.of("a", 2.0), KV.of("b", 7.0));
      PAssert.that(coldMean).containsInAnyOrder(expected);
      PAssert.that(warmMean).containsInAnyOrder(expected);
      PAssert.that(hotMean).containsInAnyOrder(expected);
      PAssert.that(splitMean).containsInAnyOrder(expected);

      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testHotKeyCombiningWithAccumulationMode() {
      PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5));

      PCollection<Integer> output =
          input
              .apply(
                  Window.<Integer>into(new GlobalWindows())
                      .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                      .accumulatingFiredPanes()
                      .withAllowedLateness(new Duration(0), ClosingBehavior.FIRE_ALWAYS))
              .apply(Sum.integersGlobally().withoutDefaults().withFanout(2))
              .apply(ParDo.of(new GetLast()));

      PAssert.that(output)
          .satisfies(
              input1 -> {
                assertThat(input1, hasItem(15));
                return null;
              });

      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testBinaryCombineFn() {
      PCollection<KV<String, Integer>> input =
          copy(
              createInput(
                  pipeline,
                  Arrays.asList(
                      KV.of("a", 1), KV.of("a", 1), KV.of("a", 4), KV.of("b", 1), KV.of("b", 13))),
              2);
      PCollection<KV<String, Integer>> intProduct =
          input.apply("IntProduct", Combine.perKey(new TestProdInt()));
      PCollection<KV<String, Integer>> objProduct =
          input.apply("ObjProduct", Combine.perKey(new TestProdObj()));

      List<KV<String, Integer>> expected = Arrays.asList(KV.of("a", 16), KV.of("b", 169));
      PAssert.that(intProduct).containsInAnyOrder(expected);
      PAssert.that(objProduct).containsInAnyOrder(expected);

      pipeline.run();
    }

    @Test
    public void testBinaryCombineFnWithNulls() {
      testCombineFn(new NullCombiner(), Arrays.asList(3, 3, 5), 45);
      testCombineFn(new NullCombiner(), Arrays.asList(null, 3, 5), 30);
      testCombineFn(new NullCombiner(), Arrays.asList(3, 3, null), 18);
      testCombineFn(new NullCombiner(), Arrays.asList(null, 3, null), 12);
      testCombineFn(new NullCombiner(), Arrays.asList(null, null, null), 8);
    }

    @Test
    public void testCombineGetName() {
      assertEquals("Combine.globally(SumInts)", Combine.globally(new SumInts()).getName());
      assertEquals(
          "Combine.GloballyAsSingletonView",
          Combine.globally(new SumInts()).asSingletonView().getName());
      assertEquals("Combine.perKey(Test)", Combine.perKey(new TestCombineFn()).getName());
      assertEquals(
          "Combine.perKeyWithFanout(Test)",
          Combine.perKey(new TestCombineFn()).withHotKeyFanout(10).getName());
    }

    @Test
    public void testDisplayData() {
      UniqueInts combineFn =
          new UniqueInts() {
            @Override
            public void populateDisplayData(DisplayData.Builder builder) {
              builder.add(DisplayData.item("fnMetadata", "foobar"));
            }
          };
      Combine.Globally<?, ?> combine = Combine.globally(combineFn).withFanout(1234);
      DisplayData displayData = DisplayData.from(combine);

      assertThat(displayData, hasDisplayItem("combineFn", combineFn.getClass()));
      assertThat(displayData, hasDisplayItem("emitDefaultOnEmptyInput", true));
      assertThat(displayData, hasDisplayItem("fanout", 1234));
      assertThat(displayData, includesDisplayDataFor("combineFn", combineFn));
    }

    @Test
    public void testDisplayDataForWrappedFn() {
      UniqueInts combineFn =
          new UniqueInts() {
            @Override
            public void populateDisplayData(DisplayData.Builder builder) {
              builder.add(DisplayData.item("foo", "bar"));
            }
          };
      Combine.PerKey<?, ?, ?> combine = Combine.perKey(combineFn);
      DisplayData displayData = DisplayData.from(combine);

      assertThat(displayData, hasDisplayItem("combineFn", combineFn.getClass()));
      assertThat(displayData, hasDisplayItem(hasNamespace(combineFn.getClass())));
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testCombinePerKeyPrimitiveDisplayData() {
      DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

      UniqueInts combineFn = new UniqueInts();
      PTransform<PCollection<KV<Integer, Integer>>, ? extends POutput> combine =
          Combine.perKey(combineFn);

      Set<DisplayData> displayData =
          evaluator.displayDataForPrimitiveTransforms(
              combine, KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));

      assertThat(
          "Combine.perKey should include the combineFn in its primitive transform",
          displayData,
          hasItem(hasDisplayItem("combineFn", combineFn.getClass())));
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testCombinePerKeyWithHotKeyFanoutPrimitiveDisplayData() {
      int hotKeyFanout = 2;
      DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

      UniqueInts combineFn = new UniqueInts();
      PTransform<PCollection<KV<Integer, Integer>>, PCollection<KV<Integer, Set<Integer>>>>
          combine =
              Combine.<Integer, Integer, Set<Integer>>perKey(combineFn)
                  .withHotKeyFanout(hotKeyFanout);

      Set<DisplayData> displayData =
          evaluator.displayDataForPrimitiveTransforms(
              combine, KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));

      assertThat(
          "Combine.perKey.withHotKeyFanout should include the combineFn in its primitive "
              + "transform",
          displayData,
          hasItem(hasDisplayItem("combineFn", combineFn.getClass())));
      assertThat(
          "Combine.perKey.withHotKeyFanout(int) should include the fanout in its primitive "
              + "transform",
          displayData,
          hasItem(hasDisplayItem("fanout", hotKeyFanout)));
    }

    /** Tests creation of a per-key {@link Combine} via a Java 8 lambda. */
    @Test
    @Category(ValidatesRunner.class)
    public void testCombinePerKeyLambda() {

      PCollection<KV<String, Integer>> output =
          pipeline
              .apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3), KV.of("c", 4)))
              .apply(
                  Combine.perKey(
                      integers -> {
                        int sum = 0;
                        for (int i : integers) {
                          sum += i;
                        }
                        return sum;
                      }));

      PAssert.that(output).containsInAnyOrder(KV.of("a", 4), KV.of("b", 2), KV.of("c", 4));
      pipeline.run();
    }

    /** Tests creation of a per-key binary {@link Combine} via a Java 8 lambda. */
    @Test
    @Category(ValidatesRunner.class)
    public void testBinaryCombinePerKeyLambda() {

      PCollection<KV<String, Integer>> output =
          pipeline
              .apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3), KV.of("c", 4)))
              .apply(Combine.perKey((a, b) -> a + b));

      PAssert.that(output).containsInAnyOrder(KV.of("a", 4), KV.of("b", 2), KV.of("c", 4));
      pipeline.run();
    }

    /** Tests creation of a per-key {@link Combine} via a Java 8 method reference. */
    @Test
    @Category(ValidatesRunner.class)
    public void testCombinePerKeyInstanceMethodReference() {

      PCollection<KV<String, Integer>> output =
          pipeline
              .apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3), KV.of("c", 4)))
              .apply(Combine.perKey(new Summer()::sum));

      PAssert.that(output).containsInAnyOrder(KV.of("a", 4), KV.of("b", 2), KV.of("c", 4));
      pipeline.run();
    }

    /** Tests creation of a per-key binary {@link Combine} via a Java 8 method reference. */
    @Test
    @Category(ValidatesRunner.class)
    public void testBinaryCombinePerKeyInstanceMethodReference() {

      PCollection<KV<String, Integer>> output =
          pipeline
              .apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3), KV.of("c", 4)))
              .apply(Combine.perKey(new Summer()::add));

      PAssert.that(output).containsInAnyOrder(KV.of("a", 4), KV.of("b", 2), KV.of("c", 4));
      pipeline.run();
    }

    /**
     * Tests that we can serialize {@link Combine.CombineFn CombineFns} constructed from a lambda.
     * Lambdas can be problematic because the {@link Class} object is synthetic and cannot be
     * deserialized.
     */
    @Test
    public void testLambdaSerialization() {
      SerializableFunction<Iterable<Object>, Object> combiner = xs -> Iterables.getFirst(xs, 0);

      boolean lambdaClassSerializationThrows;
      try {
        SerializableUtils.clone(combiner.getClass());
        lambdaClassSerializationThrows = false;
      } catch (IllegalArgumentException e) {
        // Expected
        lambdaClassSerializationThrows = true;
      }
      Assume.assumeTrue(
          "Expected lambda class serialization to fail. "
              + "If it's fixed, we can remove special behavior in Combine.",
          lambdaClassSerializationThrows);

      Combine.Globally<?, ?> combine = Combine.globally(combiner);
      SerializableUtils.clone(combine); // should not throw.
    }

    @Test
    public void testLambdaDisplayData() {
      Combine.Globally<?, ?> combine = Combine.globally(xs -> Iterables.getFirst(xs, 0));
      DisplayData displayData = DisplayData.from(combine);
      MatcherAssert.assertThat(displayData.items(), not(empty()));
    }
  }

  /** Tests validating CombineWithContext behaviors. */
  @RunWith(JUnit4.class)
  public static class CombineWithContextTests extends SharedTestBase {
    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testSimpleCombineWithContext() {
      runTestSimpleCombineWithContext(
          Arrays.asList(KV.of("a", 1), KV.of("a", 1), KV.of("a", 4), KV.of("b", 1), KV.of("b", 13)),
          20,
          Arrays.asList(KV.of("a", "20:114"), KV.of("b", "20:113")),
          new String[] {"20:111134"});
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testSimpleCombineWithContextEmpty() {
      runTestSimpleCombineWithContext(EMPTY_TABLE, 0, Collections.emptyList(), new String[] {});
    }

    @Test
    public void testWithDefaultsPreservesSideInputs() {
      final PCollectionView<Integer> view =
          pipeline.apply(Create.of(1)).apply(Sum.integersGlobally().asSingletonView());

      Combine.Globally<Integer, String> combine =
          Combine.globally(new TestCombineFnWithContext(view))
              .withSideInputs(view)
              .withoutDefaults();

      assertEquals(Collections.singletonList(view), combine.getSideInputs());
    }

    @Test
    public void testWithFanoutPreservesSideInputs() {
      final PCollectionView<Integer> view =
          pipeline.apply(Create.of(1)).apply(Sum.integersGlobally().asSingletonView());

      Combine.Globally<Integer, String> combine =
          Combine.globally(new TestCombineFnWithContext(view)).withSideInputs(view).withFanout(1);

      assertEquals(Collections.singletonList(view), combine.getSideInputs());
    }
  }

  /** Tests validating windowing behaviors. */
  @RunWith(JUnit4.class)
  public static class WindowingTests extends SharedTestBase implements Serializable {
    @Test
    @Category({ValidatesRunner.class})
    public void testFixedWindowsCombine() {
      PCollection<KV<String, Integer>> input =
          pipeline
              .apply(
                  Create.timestamped(
                          TimestampedValue.of(KV.of("a", 1), new Instant(0L)),
                          TimestampedValue.of(KV.of("a", 1), new Instant(1L)),
                          TimestampedValue.of(KV.of("a", 4), new Instant(6L)),
                          TimestampedValue.of(KV.of("b", 1), new Instant(7L)),
                          TimestampedValue.of(KV.of("b", 13), new Instant(8L)))
                      .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
              .apply(Window.into(FixedWindows.of(Duration.millis(2))));

      PCollection<Integer> sum =
          input.apply(Values.create()).apply(Combine.globally(new SumInts()).withoutDefaults());

      PCollection<KV<String, String>> sumPerKey = input.apply(Combine.perKey(new TestCombineFn()));

      PAssert.that(sum).containsInAnyOrder(2, 5, 13);
      PAssert.that(sumPerKey)
          .containsInAnyOrder(
              Arrays.asList(KV.of("a", "11"), KV.of("a", "4"), KV.of("b", "1"), KV.of("b", "13")));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testFixedWindowsCombineWithContext() {
      PCollection<KV<String, Integer>> perKeyInput =
          pipeline
              .apply(
                  Create.timestamped(
                          TimestampedValue.of(KV.of("a", 1), new Instant(0L)),
                          TimestampedValue.of(KV.of("a", 1), new Instant(1L)),
                          TimestampedValue.of(KV.of("a", 4), new Instant(6L)),
                          TimestampedValue.of(KV.of("b", 1), new Instant(7L)),
                          TimestampedValue.of(KV.of("b", 13), new Instant(8L)))
                      .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
              .apply(Window.into(FixedWindows.of(Duration.millis(2))));

      PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

      PCollection<Integer> sum =
          globallyInput.apply("Sum", Combine.globally(new SumInts()).withoutDefaults());

      PCollectionView<Integer> globallySumView = sum.apply(View.asSingleton());

      PCollection<KV<String, String>> combinePerKeyWithContext =
          perKeyInput.apply(
              Combine.<String, Integer, String>perKey(new TestCombineFnWithContext(globallySumView))
                  .withSideInputs(globallySumView));

      PCollection<String> combineGloballyWithContext =
          globallyInput.apply(
              Combine.globally(new TestCombineFnWithContext(globallySumView))
                  .withoutDefaults()
                  .withSideInputs(globallySumView));

      PAssert.that(sum).containsInAnyOrder(2, 5, 13);
      PAssert.that(combinePerKeyWithContext)
          .containsInAnyOrder(
              Arrays.asList(
                  KV.of("a", "2:11"), KV.of("a", "5:4"), KV.of("b", "5:1"), KV.of("b", "13:13")));
      PAssert.that(combineGloballyWithContext).containsInAnyOrder("2:11", "5:14", "13:13");
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testSlidingWindowsCombine() {
      PCollection<String> input =
          pipeline
              .apply(
                  Create.timestamped(
                      TimestampedValue.of("a", new Instant(1L)),
                      TimestampedValue.of("b", new Instant(2L)),
                      TimestampedValue.of("c", new Instant(3L))))
              .apply(Window.into(SlidingWindows.of(Duration.millis(3)).every(Duration.millis(1L))));
      PCollection<List<String>> combined =
          input.apply(
              Combine.globally(
                      new CombineFn<String, List<String>, List<String>>() {
                        @Override
                        public List<String> createAccumulator() {
                          return new ArrayList<>();
                        }

                        @Override
                        public List<String> addInput(List<String> accumulator, String input) {
                          accumulator.add(input);
                          return accumulator;
                        }

                        @Override
                        public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
                          // Mutate all of the accumulators. Instances should be used in only one
                          // place, and not
                          // reused after merging.
                          List<String> cur = createAccumulator();
                          for (List<String> accumulator : accumulators) {
                            accumulator.addAll(cur);
                            cur = accumulator;
                          }
                          return cur;
                        }

                        @Override
                        public List<String> extractOutput(List<String> accumulator) {
                          List<String> result = new ArrayList<>(accumulator);
                          Collections.sort(result);
                          return result;
                        }
                      })
                  .withoutDefaults());

      PAssert.that(combined)
          .containsInAnyOrder(
              ImmutableList.of("a"),
              ImmutableList.of("a", "b"),
              ImmutableList.of("a", "b", "c"),
              ImmutableList.of("b", "c"),
              ImmutableList.of("c"));

      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testSlidingWindowsCombineWithContext() {
      // [a: 1, 1], [a: 4; b: 1], [b: 13]
      PCollection<KV<String, Integer>> perKeyInput =
          pipeline
              .apply(
                  Create.timestamped(
                          TimestampedValue.of(KV.of("a", 1), new Instant(2L)),
                          TimestampedValue.of(KV.of("a", 1), new Instant(3L)),
                          TimestampedValue.of(KV.of("a", 4), new Instant(8L)),
                          TimestampedValue.of(KV.of("b", 1), new Instant(9L)),
                          TimestampedValue.of(KV.of("b", 13), new Instant(10L)))
                      .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
              .apply(Window.into(SlidingWindows.of(Duration.millis(2))));

      PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

      PCollection<Integer> sum =
          globallyInput.apply("Sum", Sum.integersGlobally().withoutDefaults());

      PCollectionView<Integer> globallySumView = sum.apply(View.asSingleton());

      PCollection<KV<String, String>> combinePerKeyWithContext =
          perKeyInput.apply(
              Combine.<String, Integer, String>perKey(new TestCombineFnWithContext(globallySumView))
                  .withSideInputs(globallySumView));

      PCollection<String> combineGloballyWithContext =
          globallyInput.apply(
              Combine.globally(new TestCombineFnWithContext(globallySumView))
                  .withoutDefaults()
                  .withSideInputs(globallySumView));

      PAssert.that(sum).containsInAnyOrder(1, 2, 1, 4, 5, 14, 13);
      PAssert.that(combinePerKeyWithContext)
          .containsInAnyOrder(
              Arrays.asList(
                  KV.of("a", "1:1"),
                  KV.of("a", "2:11"),
                  KV.of("a", "1:1"),
                  KV.of("a", "4:4"),
                  KV.of("a", "5:4"),
                  KV.of("b", "5:1"),
                  KV.of("b", "14:113"),
                  KV.of("b", "13:13")));
      PAssert.that(combineGloballyWithContext)
          .containsInAnyOrder("1:1", "2:11", "1:1", "4:4", "5:14", "14:113", "13:13");
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testGlobalCombineWithDefaultsAndTriggers() {
      PCollection<Integer> input = pipeline.apply(Create.of(1, 1));

      PCollection<String> output =
          input
              .apply(
                  Window.<Integer>into(new GlobalWindows())
                      .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                      .accumulatingFiredPanes()
                      .withAllowedLateness(new Duration(0), ClosingBehavior.FIRE_ALWAYS))
              .apply(Sum.integersGlobally())
              .apply(ParDo.of(new FormatPaneInfo()));

      // The actual elements produced are nondeterministic. Could be one, could be two.
      // But it should certainly have a final element with the correct final sum.
      PAssert.that(output)
          .satisfies(
              input1 -> {
                assertThat(input1, hasItem("2: true"));
                return null;
              });

      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testSessionsCombine() {
      PCollection<KV<String, Integer>> input =
          pipeline
              .apply(
                  Create.timestamped(
                          TimestampedValue.of(KV.of("a", 1), new Instant(0L)),
                          TimestampedValue.of(KV.of("a", 1), new Instant(4L)),
                          TimestampedValue.of(KV.of("a", 4), new Instant(7L)),
                          TimestampedValue.of(KV.of("b", 1), new Instant(10L)),
                          TimestampedValue.of(KV.of("b", 13), new Instant(16L)))
                      .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
              .apply(Window.into(Sessions.withGapDuration(Duration.millis(5))));

      PCollection<Integer> sum =
          input.apply(Values.create()).apply(Combine.globally(new SumInts()).withoutDefaults());

      PCollection<KV<String, String>> sumPerKey = input.apply(Combine.perKey(new TestCombineFn()));

      PAssert.that(sum).containsInAnyOrder(7, 13);
      PAssert.that(sumPerKey)
          .containsInAnyOrder(Arrays.asList(KV.of("a", "114"), KV.of("b", "1"), KV.of("b", "13")));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testSessionsCombineWithContext() {
      PCollection<KV<String, Integer>> perKeyInput =
          pipeline.apply(
              Create.timestamped(
                      TimestampedValue.of(KV.of("a", 1), new Instant(0L)),
                      TimestampedValue.of(KV.of("a", 1), new Instant(4L)),
                      TimestampedValue.of(KV.of("a", 4), new Instant(7L)),
                      TimestampedValue.of(KV.of("b", 1), new Instant(10L)),
                      TimestampedValue.of(KV.of("b", 13), new Instant(16L)))
                  .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

      PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

      PCollection<Integer> fixedWindowsSum =
          globallyInput
              .apply("FixedWindows", Window.into(FixedWindows.of(Duration.millis(5))))
              .apply("Sum", Combine.globally(new SumInts()).withoutDefaults());

      PCollectionView<Integer> globallyFixedWindowsView =
          fixedWindowsSum.apply(View.<Integer>asSingleton().withDefaultValue(0));

      PCollection<KV<String, String>> sessionsCombinePerKey =
          perKeyInput
              .apply(
                  "PerKey Input Sessions",
                  Window.into(Sessions.withGapDuration(Duration.millis(5))))
              .apply(
                  Combine.<String, Integer, String>perKey(
                          new TestCombineFnWithContext(globallyFixedWindowsView))
                      .withSideInputs(globallyFixedWindowsView));

      PCollection<String> sessionsCombineGlobally =
          globallyInput
              .apply(
                  "Globally Input Sessions",
                  Window.into(Sessions.withGapDuration(Duration.millis(5))))
              .apply(
                  Combine.globally(new TestCombineFnWithContext(globallyFixedWindowsView))
                      .withoutDefaults()
                      .withSideInputs(globallyFixedWindowsView));

      PAssert.that(fixedWindowsSum).containsInAnyOrder(2, 4, 1, 13);
      PAssert.that(sessionsCombinePerKey)
          .containsInAnyOrder(
              Arrays.asList(KV.of("a", "1:114"), KV.of("b", "1:1"), KV.of("b", "0:13")));
      PAssert.that(sessionsCombineGlobally).containsInAnyOrder("1:1114", "0:13");
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class})
    public void testWindowedCombineEmpty() {
      PCollection<Double> mean =
          pipeline
              .apply(Create.empty(BigEndianIntegerCoder.of()))
              .apply(Window.into(FixedWindows.of(Duration.millis(1))))
              .apply(Combine.globally(new MeanInts()).withoutDefaults());

      PAssert.that(mean).empty();

      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testCombineGloballyAsSingletonView() {
      final PCollectionView<Integer> view =
          pipeline
              .apply("CreateEmptySideInput", Create.empty(BigEndianIntegerCoder.of()))
              .apply(Sum.integersGlobally().asSingletonView());

      PCollection<Integer> output =
          pipeline
              .apply("CreateVoidMainInput", Create.of((Void) null))
              .apply(
                  "OutputSideInput",
                  ParDo.of(
                          new DoFn<Void, Integer>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                              c.output(c.sideInput(view));
                            }
                          })
                      .withSideInputs(view));

      PAssert.thatSingleton(output).isEqualTo(0);
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testWindowedCombineGloballyAsSingletonView() {
      FixedWindows windowFn = FixedWindows.of(Duration.standardMinutes(1));
      final PCollectionView<Integer> view =
          pipeline
              .apply(
                  "CreateSideInput",
                  Create.timestamped(
                      TimestampedValue.of(1, new Instant(100)),
                      TimestampedValue.of(3, new Instant(100))))
              .apply("WindowSideInput", Window.into(windowFn))
              .apply("CombineSideInput", Sum.integersGlobally().asSingletonView());

      TimestampedValue<Void> nonEmptyElement = TimestampedValue.of(null, new Instant(100));
      TimestampedValue<Void> emptyElement = TimestampedValue.atMinimumTimestamp(null);
      PCollection<Integer> output =
          pipeline
              .apply(
                  "CreateMainInput",
                  Create.timestamped(nonEmptyElement, emptyElement).withCoder(VoidCoder.of()))
              .apply("WindowMainInput", Window.into(windowFn))
              .apply(
                  "OutputSideInput",
                  ParDo.of(
                          new DoFn<Void, Integer>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                              c.output(c.sideInput(view));
                            }
                          })
                      .withSideInputs(view));

      PAssert.that(output).containsInAnyOrder(4, 0);
      PAssert.that(output)
          .inWindow(windowFn.assignWindow(nonEmptyElement.getTimestamp()))
          .containsInAnyOrder(4);
      PAssert.that(output)
          .inWindow(windowFn.assignWindow(emptyElement.getTimestamp()))
          .containsInAnyOrder(0);
      pipeline.run();
    }

    /** Tests creation of a global {@link Combine} via Java 8 lambda. */
    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testCombineGloballyLambda() {

      PCollection<Integer> output =
          pipeline
              .apply(Create.of(1, 2, 3, 4))
              .apply(
                  Combine.globally(
                      integers -> {
                        int sum = 0;
                        for (int i : integers) {
                          sum += i;
                        }
                        return sum;
                      }));

      PAssert.that(output).containsInAnyOrder(10);
      pipeline.run();
    }

    /** Tests creation of a global {@link Combine} via a Java 8 method reference. */
    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testCombineGloballyInstanceMethodReference() {

      PCollection<Integer> output =
          pipeline.apply(Create.of(1, 2, 3, 4)).apply(Combine.globally(new Summer()::sum));

      PAssert.that(output).containsInAnyOrder(10);
      pipeline.run();
    }
  }

  /** Tests validating accumulation scenarios. */
  @RunWith(JUnit4.class)
  public static class AccumulationTests extends SharedTestBase {
    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testAccumulatingCombine() {
      runTestAccumulatingCombine(
          Arrays.asList(KV.of("a", 1), KV.of("a", 1), KV.of("a", 4), KV.of("b", 1), KV.of("b", 13)),
          4.0,
          Arrays.asList(KV.of("a", 2.0), KV.of("b", 7.0)));
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testAccumulatingCombineEmpty() {
      runTestAccumulatingCombine(EMPTY_TABLE, 0.0, Collections.emptyList());
    }
  }
}
