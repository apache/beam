/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.transforms;

import static com.google.cloud.dataflow.sdk.TestUtils.checkCombineFn;
import static org.junit.Assert.assertThat;

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.CustomCoder;
import com.google.cloud.dataflow.sdk.coders.DoubleCoder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.RecordingPipelineVisitor;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Tests for Combine transforms.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class CombineTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  @SuppressWarnings({"rawtypes", "unchecked"})
  static final KV<String, Integer>[] TABLE = new KV[] {
    KV.of("a", 1),
    KV.of("a", 1),
    KV.of("a", 4),
    KV.of("b", 1),
    KV.of("b", 13),
  };

  @SuppressWarnings({"rawtypes", "unchecked"})
  static final KV<String, Integer>[] EMPTY_TABLE = new KV[] {
  };

  static final Integer[] NUMBERS = new Integer[] {
    1, 1, 2, 3, 5, 8, 13, 21, 34, 55
  };

  PCollection<KV<String, Integer>> createInput(Pipeline p,
                                               KV<String, Integer>[] table) {
    return p.apply(Create.of(Arrays.asList(table)).withCoder(
        KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
  }

  private void runTestSimpleCombine(KV<String, Integer>[] table,
                                    int globalSum,
                                    KV<String, String>[] perKeyCombines) {
    Pipeline p = TestPipeline.create();
    PCollection<KV<String, Integer>> input = createInput(p, table);

    PCollection<Integer> sum = input
        .apply(Values.<Integer>create())
        .apply(Combine.globally(new SumInts()));

    // Java 8 will infer.
    PCollection<KV<String, String>> sumPerKey = input
        .apply(Combine.perKey(new TestKeyedCombineFn()));

    DataflowAssert.that(sum).containsInAnyOrder(globalSum);
    DataflowAssert.that(sumPerKey).containsInAnyOrder(perKeyCombines);

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testSimpleCombine() {
    runTestSimpleCombine(TABLE, 20, new KV[] {
        KV.of("a", "114a"), KV.of("b", "113b") });
  }

  @Test
  @Category(RunnableOnService.class)
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testSimpleCombineEmpty() {
    runTestSimpleCombine(EMPTY_TABLE, 0, new KV[] { });
  }

  @SuppressWarnings("unchecked")
  private void runTestBasicCombine(KV<String, Integer>[] table,
                                   Set<Integer> globalUnique,
                                   KV<String, Set<Integer>>[] perKeyUnique) {
    Pipeline p = TestPipeline.create();
    p.getCoderRegistry().registerCoder(Set.class, SetCoder.class);
    PCollection<KV<String, Integer>> input = createInput(p, table);

    PCollection<Set<Integer>> unique = input
        .apply(Values.<Integer>create())
        .apply(Combine.globally(new UniqueInts()));

    // Java 8 will infer.
    PCollection<KV<String, Set<Integer>>> uniquePerKey = input
        .apply(Combine.<String, Integer, Set<Integer>>perKey(new UniqueInts()));

    DataflowAssert.that(unique).containsInAnyOrder(globalUnique);
    DataflowAssert.that(uniquePerKey).containsInAnyOrder(perKeyUnique);

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testBasicCombine() {
    runTestBasicCombine(TABLE, ImmutableSet.of(1, 13, 4), new KV[] {
        KV.of("a", (Set<Integer>) ImmutableSet.of(1, 4)),
        KV.of("b", (Set<Integer>) ImmutableSet.of(1, 13)) });
  }

  @Test
  @Category(RunnableOnService.class)
  @SuppressWarnings("rawtypes")
  public void testBasicCombineEmpty() {
    runTestBasicCombine(EMPTY_TABLE, ImmutableSet.<Integer>of(), new KV[] { });
  }

  private void runTestAccumulatingCombine(KV<String, Integer>[] table,
                                          Double globalMean,
                                          KV<String, Double>[] perKeyMeans) {
    Pipeline p = TestPipeline.create();
    PCollection<KV<String, Integer>> input = createInput(p, table);

    PCollection<Double> mean = input
        .apply(Values.<Integer>create())
        .apply(Combine.globally(new MeanInts()));

    // Java 8 will infer.
    PCollection<KV<String, Double>> meanPerKey = input.apply(
        Combine.<String, Integer, Double>perKey(new MeanInts()));

    DataflowAssert.that(mean).containsInAnyOrder(globalMean);
    DataflowAssert.that(meanPerKey).containsInAnyOrder(perKeyMeans);

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testFixedWindowsCombine() {
    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.timestamped(Arrays.asList(TABLE),
                                   Arrays.asList(0L, 1L, 6L, 7L, 8L))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
         .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(2))));

    PCollection<Integer> sum = input
        .apply(Values.<Integer>create())
        .apply(Combine.globally(new SumInts()).withoutDefaults());

    PCollection<KV<String, String>> sumPerKey = input
        .apply(Combine.perKey(new TestKeyedCombineFn()));

    DataflowAssert.that(sum).containsInAnyOrder(2, 5, 13);
    DataflowAssert.that(sumPerKey).containsInAnyOrder(
        KV.of("a", "11a"),
        KV.of("a", "4a"),
        KV.of("b", "1b"),
        KV.of("b", "13b"));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSessionsCombine() {
    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.timestamped(Arrays.asList(TABLE),
                                   Arrays.asList(0L, 4L, 7L, 10L, 16L))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
         .apply(Window.<KV<String, Integer>>into(Sessions.withGapDuration(Duration.millis(5))));

    PCollection<Integer> sum = input
        .apply(Values.<Integer>create())
        .apply(Combine.globally(new SumInts()).withoutDefaults());

    PCollection<KV<String, String>> sumPerKey = input
        .apply(Combine.perKey(new TestKeyedCombineFn()));

    DataflowAssert.that(sum).containsInAnyOrder(7, 13);
    DataflowAssert.that(sumPerKey).containsInAnyOrder(
        KV.of("a", "114a"),
        KV.of("b", "1b"),
        KV.of("b", "13b"));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedCombineEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<Double> mean = p
        .apply(Create.<Integer>of().withCoder(BigEndianIntegerCoder.of()))
        .apply(Window.<Integer>into(FixedWindows.of(Duration.millis(1))))
        .apply(Combine.globally(new MeanInts()).withoutDefaults());

    DataflowAssert.that(mean).containsInAnyOrder();

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testAccumulatingCombine() {
    runTestAccumulatingCombine(TABLE, 4.0, new KV[] {
        KV.of("a", 2.0), KV.of("b", 7.0) });
  }

  @Test
  @Category(RunnableOnService.class)
  public void testAccumulatingCombineEmpty() {
    runTestAccumulatingCombine(EMPTY_TABLE, 0.0, new KV[] { });
  }

  // Checks that Min, Max, Mean, Sum (operations that pass-through to Combine),
  // provide their own top-level name.
  @Test
  public void testCombinerNames() {
    Pipeline p = TestPipeline.create();
    PCollection<KV<String, Integer>> input = createInput(p, TABLE);

    Combine.PerKey<String, Integer, Integer> min = Min.integersPerKey();
    Combine.PerKey<String, Integer, Integer> max = Max.integersPerKey();
    Combine.PerKey<String, Integer, Double> mean = Mean.perKey();
    Combine.PerKey<String, Integer, Integer> sum = Sum.integersPerKey();

    input.apply(min);
    input.apply(max);
    input.apply(mean);
    input.apply(sum);

    p.traverseTopologically(new RecordingPipelineVisitor());

    assertThat(p.getFullNameForTesting(min), Matchers.startsWith("Min"));
    assertThat(p.getFullNameForTesting(max), Matchers.startsWith("Max"));
    assertThat(p.getFullNameForTesting(mean), Matchers.startsWith("Mean"));
    assertThat(p.getFullNameForTesting(sum), Matchers.startsWith("Sum"));
  }

  @Test
  public void testAddInputsRandomly() {
    TestCounter counter = new TestCounter();
    Combine.KeyedCombineFn<
        String, Integer, TestCounter.Counter, Iterable<Long>> fn =
        counter.asKeyedFn();

    List<TestCounter.Counter> accums = DirectPipelineRunner.TestCombineDoFn.addInputsRandomly(
        fn, "bob", Arrays.asList(NUMBERS), new Random(42));

    assertThat(accums, Matchers.contains(
        counter.new Counter(3, 2, 0, 0),
        counter.new Counter(131, 5, 0, 0),
        counter.new Counter(8, 2, 0, 0),
        counter.new Counter(1, 1, 0, 0)));
  }

  private static final SerializableFunction<String, Integer> hotKeyFanout =
      new SerializableFunction<String, Integer>() {
        @Override
        public Integer apply(String input) {
          return input.equals("a") ? 3 : 0;
        }
      };

  @Test
  @Category(RunnableOnService.class)
  public void testHotKeyCombining() {
    Pipeline p = TestPipeline.create();
    PCollection<KV<String, Integer>> input = copy(createInput(p, TABLE), 10);

    KeyedCombineFn<String, Integer, ?, Double> mean =
        new MeanInts().<String>asKeyedFn();
    PCollection<KV<String, Double>> coldMean = input.apply("ColdMean",
        Combine.perKey(mean).withHotKeyFanout(0));
    PCollection<KV<String, Double>> warmMean = input.apply("WarmMean",
        Combine.perKey(mean).withHotKeyFanout(hotKeyFanout));
    PCollection<KV<String, Double>> hotMean = input.apply("HotMean",
        Combine.perKey(mean).withHotKeyFanout(5));

    List<KV<String, Double>> expected = Arrays.asList(KV.of("a", 2.0), KV.of("b", 7.0));
    DataflowAssert.that(coldMean).containsInAnyOrder(expected);
    DataflowAssert.that(warmMean).containsInAnyOrder(expected);
    DataflowAssert.that(hotMean).containsInAnyOrder(expected);

    p.run();
  }

  @Test
  public void testBinaryCombineFn() {
    Pipeline p = TestPipeline.create();
    PCollection<KV<String, Integer>> input = copy(createInput(p, TABLE), 2);
    PCollection<KV<String, Integer>> intProduct = input
        .apply("IntProduct", Combine.<String, Integer, Integer>perKey(new TestProdInt()));
    PCollection<KV<String, Integer>> objProduct = input
        .apply("ObjProduct", Combine.<String, Integer, Integer>perKey(new TestProdObj()));

    List<KV<String, Integer>> expected = Arrays.asList(KV.of("a", 16), KV.of("b", 169));
    DataflowAssert.that(intProduct).containsInAnyOrder(expected);
    DataflowAssert.that(objProduct).containsInAnyOrder(expected);

    p.run();
  }

  @Test
  public void testBinaryCombineFnWithNulls() {
    checkCombineFn(new NullCombiner(), Arrays.asList(3, 3, 5), 45);
    checkCombineFn(new NullCombiner(), Arrays.asList(null, 3, 5), 30);
    checkCombineFn(new NullCombiner(), Arrays.asList(3, 3, null), 18);
    checkCombineFn(new NullCombiner(), Arrays.asList(null, 3, null), 12);
    checkCombineFn(new NullCombiner(), Arrays.<Integer>asList(null, null, null), 8);
  }

  private static final class TestProdInt extends Combine.BinaryCombineIntegerFn {
    public int apply(int left, int right) {
      return left * right;
    }
    public int identity() {
      return 1;
    }
    @Override
    public Counter<Integer> getCounter(String name) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class TestProdObj extends Combine.BinaryCombineFn<Integer> {
    public Integer apply(Integer left, Integer right) {
      return left * right;
    }
  }

  /**
   * Computes the product, considering null values to be 2.
   */
  private static final class NullCombiner extends Combine.BinaryCombineFn<Integer> {
    public Integer apply(Integer left, Integer right) {
      return (left == null ? 2 : left) * (right == null ? 2 : right);
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCombineGloballyAsSingletonView() {
    Pipeline p = TestPipeline.create();
    final PCollectionView<Integer> view = p
        .apply("CreateEmptySideInput", Create.<Integer>of().withCoder(BigEndianIntegerCoder.of()))
        .apply(Sum.integersGlobally().asSingletonView());

    PCollection<Integer> output = p
        .apply("CreateVoidMainInput", Create.of((Void) null))
        .apply("OutputSideInput", ParDo.of(new DoFn<Void, Integer>() {
                  @Override
                  public void processElement(ProcessContext c) {
                    c.output(c.sideInput(view));
                  }
                }).withSideInputs(view));

    DataflowAssert.thatSingleton(output).isEqualTo(0);
    p.run();
  }

  ////////////////////////////////////////////////////////////////////////////
  // Test classes, for different kinds of combining fns.

  /** Example SerializableFunction combiner. */
  public static class SumInts
      implements SerializableFunction<Iterable<Integer>, Integer> {
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
  public static class UniqueInts extends
      Combine.CombineFn<Integer, Set<Integer>, Set<Integer>> {

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

  // Note: not a deterministic encoding
  private static class SetCoder<T> extends StandardCoder<Set<T>> {

    public static <T> SetCoder<T> of(Coder<T> elementCoder) {
      return new SetCoder<>(elementCoder);
    }

    @JsonCreator
    public static SetCoder<?> of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
        List<Coder<?>> components) {
      Preconditions.checkArgument(components.size() == 1,
          "Expecting 1 component, got " + components.size());
      return of((Coder<?>) components.get(0));
    }

    public static <T> List<Object> getInstanceComponents(Set<T> exampleValue) {
      return IterableCoder.getInstanceComponents(exampleValue);
    }

    private final Coder<Iterable<T>> iterableCoder;

    private SetCoder(Coder<T> elementCoder) {
      iterableCoder = IterableCoder.of(elementCoder);
    }

    @Override
    public void encode(Set<T> value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      iterableCoder.encode(value, outStream, context);
    }

    @Override
    public Set<T> decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      // TODO: Eliminate extra copy if used in production.
      return Sets.newHashSet(iterableCoder.decode(inStream, context));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return iterableCoder.getCoderArguments();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      throw new NonDeterministicException(this,
          "CombineTest.SetCoder does not encode in a deterministic order.");
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(Set<T> value, Context context) {
      return iterableCoder.isRegisterByteSizeObserverCheap(value, context);
    }

    @Override
    public void registerByteSizeObserver(
        Set<T> value, ElementByteSizeObserver observer, Context context)
        throws Exception {
      iterableCoder.registerByteSizeObserver(value, observer, context);
    }
  }

  /** Example AccumulatingCombineFn. */
  public static class MeanInts extends
      Combine.AccumulatingCombineFn<Integer, MeanInts.CountSum, Double> {
    private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
    private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();

    class CountSum implements
        Combine.AccumulatingCombineFn.Accumulator<Integer, CountSum, Double> {
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

    /**
     * A {@link Coder} for {@link CountSum}.
     */
    public class CountSumCoder extends CustomCoder<CountSum> {
      @Override
      public void encode(CountSum value, OutputStream outStream,
          Context context) throws CoderException, IOException {
        LONG_CODER.encode(value.count, outStream, context);
        DOUBLE_CODER.encode(value.sum, outStream, context);
      }

      @Override
      public CountSum decode(InputStream inStream, Coder.Context context)
          throws CoderException, IOException {
        long count = LONG_CODER.decode(inStream, context);
        double sum = DOUBLE_CODER.decode(inStream, context);
        return new CountSum(count, sum);
      }

      @Override
      public void verifyDeterministic() throws NonDeterministicException { }

      @Override
      public boolean isRegisterByteSizeObserverCheap(
          CountSum value, Context context) {
        return true;
      }

      @Override
      public void registerByteSizeObserver(
          CountSum value, ElementByteSizeObserver observer, Context context)
          throws Exception {
        LONG_CODER.registerByteSizeObserver(value.count, observer, context);
        DOUBLE_CODER.registerByteSizeObserver(value.sum, observer, context);
      }
    }
  }

  /**
   * A KeyedCombineFn that exercises the full generality of [Keyed]CombineFn.
   *
   * <p> The net result of applying this CombineFn is a sorted list of all
   * characters occurring in the key and the decimal representations of
   * each value.
   */
  public class TestKeyedCombineFn extends KeyedCombineFn
      <String, Integer, TestKeyedCombineFn.Accumulator, String> {

    // Not serializable.
    private class Accumulator {
      String value;
      public Accumulator(String value) {
        this.value = value;
      }
    }

    @Override
    public Coder<Accumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<String> keyCoder, Coder<Integer> inputCoder) {
      return new CustomCoder<Accumulator>() {
        @Override
        public void encode(Accumulator accumulator, OutputStream outStream, Coder.Context context)
            throws CoderException, IOException {
          StringUtf8Coder.of().encode(accumulator.value, outStream, context);
        }

        @Override
        public Accumulator decode(InputStream inStream, Coder.Context context)
            throws CoderException, IOException {
          return new Accumulator(StringUtf8Coder.of().decode(inStream, context));
        }
      };
    }

    @Override
    public Accumulator createAccumulator(String key) {
      return new Accumulator(key);
    }

    @Override
    public Accumulator addInput(String key, Accumulator accumulator, Integer value) {
      try {
        assertThat(accumulator.value, Matchers.startsWith(key));
        return new Accumulator(accumulator.value + String.valueOf(value));
      } finally {
        accumulator.value = "cleared in addInput";
      }
    }

    @Override
    public Accumulator mergeAccumulators(String key, Iterable<Accumulator> accumulators) {
      String all = key;
      for (Accumulator accumulator : accumulators) {
        assertThat(accumulator.value, Matchers.startsWith(key));
        all += accumulator.value.substring(key.length());
        accumulator.value = "cleared in mergeAccumulators";
      }
      return new Accumulator(all);
    }

    @Override
    public String extractOutput(String key, Accumulator accumulator) {
      assertThat(accumulator.value, Matchers.startsWith(key));
      char[] chars = accumulator.value.toCharArray();
      Arrays.sort(chars);
      return new String(chars);
    }
  }

  /** Another example AccumulatingCombineFn. */
  public static class TestCounter extends
      Combine.AccumulatingCombineFn<
          Integer, TestCounter.Counter, Iterable<Long>> {

    /** An accumulator that observes its merges and outputs. */
    public class Counter implements
        Combine.AccumulatingCombineFn.Accumulator<Integer, Counter, Iterable<Long>>,
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
        Preconditions.checkState(merges == 0);
        Preconditions.checkState(outputs == 0);

        inputs++;
        sum += element;
      }

      @Override
      public void mergeAccumulator(Counter accumulator) {
        Preconditions.checkState(outputs == 0);
        Preconditions.checkArgument(accumulator.outputs == 0);

        merges += accumulator.merges + 1;
        inputs += accumulator.inputs;
        sum += accumulator.sum;
      }

      @Override
      public Iterable<Long> extractOutput() {
        Preconditions.checkState(outputs == 0);

        return Arrays.asList(sum, inputs, merges, outputs);
      }

      @Override
      public int hashCode() {
        return (int) (sum * 17 + inputs * 31 + merges * 43 + outputs * 181);
      }

      @Override
      public boolean equals(Object otherObj) {
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
    public Coder<Counter> getAccumulatorCoder(
        CoderRegistry registry, Coder<Integer> inputCoder) {
      // This is a *very* inefficient encoding to send over the wire, but suffices
      // for tests.
      return SerializableCoder.of(Counter.class);
    }
  }

  private static <T> PCollection<T> copy(PCollection<T> pc, final int n) {
    return pc.apply(ParDo.of(new DoFn<T, T>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {
        for (int i = 0; i < n; i++) {
          c.output(c.element());
        }
      }
    }));
  }
}
