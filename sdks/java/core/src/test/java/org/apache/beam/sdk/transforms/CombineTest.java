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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.TestUtils.checkCombineFn;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasNamespace;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFor;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
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
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TimestampedValue;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

/**
 * Tests for Combine transforms.
 */
@RunWith(JUnit4.class)
public class CombineTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  static final List<KV<String, Integer>> TABLE = Arrays.asList(
    KV.of("a", 1),
    KV.of("a", 1),
    KV.of("a", 4),
    KV.of("b", 1),
    KV.of("b", 13)
  );

  static final List<KV<String, Integer>> EMPTY_TABLE = Collections.emptyList();

  @Mock private DoFn<?, ?>.ProcessContext processContext;

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  PCollection<KV<String, Integer>> createInput(Pipeline p,
                                               List<KV<String, Integer>> table) {
    return p.apply(Create.of(table).withCoder(
        KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
  }

  private void runTestSimpleCombine(List<KV<String, Integer>> table,
                                    int globalSum,
                                    List<KV<String, String>> perKeyCombines) {
    PCollection<KV<String, Integer>> input = createInput(pipeline, table);

    PCollection<Integer> sum = input
        .apply(Values.<Integer>create())
        .apply(Combine.globally(new SumInts()));

    // Java 8 will infer.
    PCollection<KV<String, String>> sumPerKey = input
        .apply(Combine.perKey(new TestKeyedCombineFn()));

    PAssert.that(sum).containsInAnyOrder(globalSum);
    PAssert.that(sumPerKey).containsInAnyOrder(perKeyCombines);

    pipeline.run();
  }

  private void runTestSimpleCombineWithContext(List<KV<String, Integer>> table,
                                               int globalSum,
                                               List<KV<String, String>> perKeyCombines,
                                               String[] globallyCombines) {
    PCollection<KV<String, Integer>> perKeyInput = createInput(pipeline, table);
    PCollection<Integer> globallyInput = perKeyInput.apply(Values.<Integer>create());

    PCollection<Integer> sum = globallyInput.apply("Sum", Combine.globally(new SumInts()));

    PCollectionView<Integer> globallySumView = sum.apply(View.<Integer>asSingleton());

    // Java 8 will infer.
    PCollection<KV<String, String>> combinePerKey = perKeyInput
        .apply(Combine.perKey(new TestKeyedCombineFnWithContext(globallySumView))
            .withSideInputs(Arrays.asList(globallySumView)));

    PCollection<String> combineGlobally = globallyInput
        .apply(Combine.globally(new TestKeyedCombineFnWithContext(globallySumView)
            .forKey("G", StringUtf8Coder.of()))
            .withoutDefaults()
            .withSideInputs(Arrays.asList(globallySumView)));

    PAssert.that(sum).containsInAnyOrder(globalSum);
    PAssert.that(combinePerKey).containsInAnyOrder(perKeyCombines);
    PAssert.that(combineGlobally).containsInAnyOrder(globallyCombines);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testSimpleCombine() {
    runTestSimpleCombine(TABLE, 20, Arrays.asList(KV.of("a", "114a"), KV.of("b", "113b")));
  }

  @Test
  @Category(ValidatesRunner.class)
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testSimpleCombineWithContext() {
    runTestSimpleCombineWithContext(TABLE, 20,
        Arrays.asList(KV.of("a", "01124a"), KV.of("b", "01123b")),
        new String[] {"01111234G"});
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSimpleCombineWithContextEmpty() {
    runTestSimpleCombineWithContext(
        EMPTY_TABLE, 0, Collections.<KV<String, String>>emptyList(), new String[] {});
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSimpleCombineEmpty() {
    runTestSimpleCombine(EMPTY_TABLE, 0, Collections.<KV<String, String>>emptyList());
  }

  @SuppressWarnings("unchecked")
  private void runTestBasicCombine(List<KV<String, Integer>> table,
                                   Set<Integer> globalUnique,
                                   List<KV<String, Set<Integer>>> perKeyUnique) {
    PCollection<KV<String, Integer>> input = createInput(pipeline, table);

    PCollection<Set<Integer>> unique = input
        .apply(Values.<Integer>create())
        .apply(Combine.globally(new UniqueInts()));

    // Java 8 will infer.
    PCollection<KV<String, Set<Integer>>> uniquePerKey = input
        .apply(Combine.<String, Integer, Set<Integer>>perKey(new UniqueInts()));

    PAssert.that(unique).containsInAnyOrder(globalUnique);
    PAssert.that(uniquePerKey).containsInAnyOrder(perKeyUnique);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testBasicCombine() {
    runTestBasicCombine(TABLE, ImmutableSet.of(1, 13, 4), Arrays.asList(
        KV.of("a", (Set<Integer>) ImmutableSet.of(1, 4)),
        KV.of("b", (Set<Integer>) ImmutableSet.of(1, 13))));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testBasicCombineEmpty() {
    runTestBasicCombine(
        EMPTY_TABLE, ImmutableSet.<Integer>of(), Collections.<KV<String, Set<Integer>>>emptyList());
  }

  private void runTestAccumulatingCombine(List<KV<String, Integer>> table,
                                          Double globalMean,
                                          List<KV<String, Double>> perKeyMeans) {
    PCollection<KV<String, Integer>> input = createInput(pipeline, table);

    PCollection<Double> mean = input
        .apply(Values.<Integer>create())
        .apply(Combine.globally(new MeanInts()));

    // Java 8 will infer.
    PCollection<KV<String, Double>> meanPerKey = input.apply(
        Combine.<String, Integer, Double>perKey(new MeanInts()));

    PAssert.that(mean).containsInAnyOrder(globalMean);
    PAssert.that(meanPerKey).containsInAnyOrder(perKeyMeans);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFixedWindowsCombine() {
    PCollection<KV<String, Integer>> input =
        pipeline.apply(Create.timestamped(TABLE, Arrays.asList(0L, 1L, 6L, 7L, 8L))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
         .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(2))));

    PCollection<Integer> sum = input
        .apply(Values.<Integer>create())
        .apply(Combine.globally(new SumInts()).withoutDefaults());

    PCollection<KV<String, String>> sumPerKey = input
        .apply(Combine.perKey(new TestKeyedCombineFn()));

    PAssert.that(sum).containsInAnyOrder(2, 5, 13);
    PAssert.that(sumPerKey).containsInAnyOrder(
        KV.of("a", "11a"),
        KV.of("a", "4a"),
        KV.of("b", "1b"),
        KV.of("b", "13b"));
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFixedWindowsCombineWithContext() {
    PCollection<KV<String, Integer>> perKeyInput =
        pipeline.apply(Create.timestamped(TABLE, Arrays.asList(0L, 1L, 6L, 7L, 8L))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
         .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(2))));

    PCollection<Integer> globallyInput = perKeyInput.apply(Values.<Integer>create());

    PCollection<Integer> sum = globallyInput
        .apply("Sum", Combine.globally(new SumInts()).withoutDefaults());

    PCollectionView<Integer> globallySumView = sum.apply(View.<Integer>asSingleton());

    PCollection<KV<String, String>> combinePerKeyWithContext = perKeyInput
        .apply(Combine.perKey(new TestKeyedCombineFnWithContext(globallySumView))
            .withSideInputs(Arrays.asList(globallySumView)));

    PCollection<String> combineGloballyWithContext = globallyInput
        .apply(Combine.globally(new TestKeyedCombineFnWithContext(globallySumView)
            .forKey("G", StringUtf8Coder.of()))
            .withoutDefaults()
            .withSideInputs(Arrays.asList(globallySumView)));

    PAssert.that(sum).containsInAnyOrder(2, 5, 13);
    PAssert.that(combinePerKeyWithContext).containsInAnyOrder(
        KV.of("a", "112a"),
        KV.of("a", "45a"),
        KV.of("b", "15b"),
        KV.of("b", "1133b"));
    PAssert.that(combineGloballyWithContext).containsInAnyOrder("112G", "145G", "1133G");
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSlidingWindowsCombineWithContext() {
    PCollection<KV<String, Integer>> perKeyInput =
        pipeline.apply(Create.timestamped(TABLE, Arrays.asList(2L, 3L, 8L, 9L, 10L))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
         .apply(Window.<KV<String, Integer>>into(SlidingWindows.of(Duration.millis(2))));

    PCollection<Integer> globallyInput = perKeyInput.apply(Values.<Integer>create());

    PCollection<Integer> sum = globallyInput
        .apply("Sum", Combine.globally(new SumInts()).withoutDefaults());

    PCollectionView<Integer> globallySumView = sum.apply(View.<Integer>asSingleton());

    PCollection<KV<String, String>> combinePerKeyWithContext = perKeyInput
        .apply(Combine.perKey(new TestKeyedCombineFnWithContext(globallySumView))
            .withSideInputs(Arrays.asList(globallySumView)));

    PCollection<String> combineGloballyWithContext = globallyInput
        .apply(Combine.globally(new TestKeyedCombineFnWithContext(globallySumView)
            .forKey("G", StringUtf8Coder.of()))
            .withoutDefaults()
            .withSideInputs(Arrays.asList(globallySumView)));

    PAssert.that(sum).containsInAnyOrder(1, 2, 1, 4, 5, 14, 13);
    PAssert.that(combinePerKeyWithContext).containsInAnyOrder(
        KV.of("a", "11a"),
        KV.of("a", "112a"),
        KV.of("a", "11a"),
        KV.of("a", "44a"),
        KV.of("a", "45a"),
        KV.of("b", "15b"),
        KV.of("b", "11134b"),
        KV.of("b", "1133b"));
    PAssert.that(combineGloballyWithContext).containsInAnyOrder(
      "11G", "112G", "11G", "44G", "145G", "11134G", "1133G");
    pipeline.run();
  }

  private static class FormatPaneInfo extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element() + ": " + c.pane().isLast());
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testGlobalCombineWithDefaultsAndTriggers() {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 1));

    PCollection<String> output = input
        .apply(Window.<Integer>into(new GlobalWindows())
            .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
            .accumulatingFiredPanes()
            .withAllowedLateness(new Duration(0)))
        .apply(Sum.integersGlobally())
        .apply(ParDo.of(new FormatPaneInfo()));

    // The actual elements produced are nondeterministic. Could be one, could be two.
    // But it should certainly have a final element with the correct final sum.
    PAssert.that(output).satisfies(new SerializableFunction<Iterable<String>, Void>() {
      @Override
      public Void apply(Iterable<String> input) {
        assertThat(input, hasItem("2: true"));
        return null;
      }
    });

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSessionsCombine() {
    PCollection<KV<String, Integer>> input =
        pipeline.apply(Create.timestamped(TABLE, Arrays.asList(0L, 4L, 7L, 10L, 16L))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
         .apply(Window.<KV<String, Integer>>into(Sessions.withGapDuration(Duration.millis(5))));

    PCollection<Integer> sum = input
        .apply(Values.<Integer>create())
        .apply(Combine.globally(new SumInts()).withoutDefaults());

    PCollection<KV<String, String>> sumPerKey = input
        .apply(Combine.perKey(new TestKeyedCombineFn()));

    PAssert.that(sum).containsInAnyOrder(7, 13);
    PAssert.that(sumPerKey).containsInAnyOrder(
        KV.of("a", "114a"),
        KV.of("b", "1b"),
        KV.of("b", "13b"));
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSessionsCombineWithContext() {
    PCollection<KV<String, Integer>> perKeyInput =
        pipeline.apply(Create.timestamped(TABLE, Arrays.asList(0L, 4L, 7L, 10L, 16L))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<Integer> globallyInput = perKeyInput.apply(Values.<Integer>create());

    PCollection<Integer> fixedWindowsSum = globallyInput
        .apply("FixedWindows",
            Window.<Integer>into(FixedWindows.of(Duration.millis(5))))
        .apply("Sum", Combine.globally(new SumInts()).withoutDefaults());

    PCollectionView<Integer> globallyFixedWindowsView =
        fixedWindowsSum.apply(View.<Integer>asSingleton().withDefaultValue(0));

    PCollection<KV<String, String>> sessionsCombinePerKey = perKeyInput
        .apply("PerKey Input Sessions",
            Window.<KV<String, Integer>>into(Sessions.withGapDuration(Duration.millis(5))))
        .apply(Combine.perKey(new TestKeyedCombineFnWithContext(globallyFixedWindowsView))
            .withSideInputs(Arrays.asList(globallyFixedWindowsView)));

    PCollection<String> sessionsCombineGlobally = globallyInput
        .apply("Globally Input Sessions",
            Window.<Integer>into(Sessions.withGapDuration(Duration.millis(5))))
        .apply(Combine.globally(new TestKeyedCombineFnWithContext(globallyFixedWindowsView)
            .forKey("G", StringUtf8Coder.of()))
            .withoutDefaults()
            .withSideInputs(Arrays.asList(globallyFixedWindowsView)));

    PAssert.that(fixedWindowsSum).containsInAnyOrder(2, 4, 1, 13);
    PAssert.that(sessionsCombinePerKey).containsInAnyOrder(
        KV.of("a", "1114a"),
        KV.of("b", "11b"),
        KV.of("b", "013b"));
    PAssert.that(sessionsCombineGlobally).containsInAnyOrder("11114G", "013G");
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedCombineEmpty() {
    PCollection<Double> mean = pipeline
        .apply(Create.empty(BigEndianIntegerCoder.of()))
        .apply(Window.<Integer>into(FixedWindows.of(Duration.millis(1))))
        .apply(Combine.globally(new MeanInts()).withoutDefaults());

    PAssert.that(mean).empty();

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testAccumulatingCombine() {
    runTestAccumulatingCombine(TABLE, 4.0, Arrays.asList(KV.of("a", 2.0), KV.of("b", 7.0)));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testAccumulatingCombineEmpty() {
    runTestAccumulatingCombine(EMPTY_TABLE, 0.0, Collections.<KV<String, Double>>emptyList());
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

  private static final SerializableFunction<String, Integer> hotKeyFanout =
      new SerializableFunction<String, Integer>() {
        @Override
        public Integer apply(String input) {
          return input.equals("a") ? 3 : 0;
        }
      };

  private static final SerializableFunction<String, Integer> splitHotKeyFanout =
      new SerializableFunction<String, Integer>() {
        @Override
        public Integer apply(String input) {
          return Math.random() < 0.5 ? 3 : 0;
        }
      };

  @Test
  @Category(ValidatesRunner.class)
  public void testHotKeyCombining() {
    PCollection<KV<String, Integer>> input = copy(createInput(pipeline, TABLE), 10);

    KeyedCombineFn<String, Integer, ?, Double> mean =
        new MeanInts().<String>asKeyedFn();
    PCollection<KV<String, Double>> coldMean = input.apply("ColdMean",
        Combine.perKey(mean).withHotKeyFanout(0));
    PCollection<KV<String, Double>> warmMean = input.apply("WarmMean",
        Combine.perKey(mean).withHotKeyFanout(hotKeyFanout));
    PCollection<KV<String, Double>> hotMean = input.apply("HotMean",
        Combine.perKey(mean).withHotKeyFanout(5));
    PCollection<KV<String, Double>> splitMean = input.apply("SplitMean",
        Combine.perKey(mean).withHotKeyFanout(splitHotKeyFanout));

    List<KV<String, Double>> expected = Arrays.asList(KV.of("a", 2.0), KV.of("b", 7.0));
    PAssert.that(coldMean).containsInAnyOrder(expected);
    PAssert.that(warmMean).containsInAnyOrder(expected);
    PAssert.that(hotMean).containsInAnyOrder(expected);
    PAssert.that(splitMean).containsInAnyOrder(expected);

    pipeline.run();
  }

  private static class GetLast extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.pane().isLast()) {
        c.output(c.element());
      }
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testHotKeyCombiningWithAccumulationMode() {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5));

    PCollection<Integer> output = input
        .apply(Window.<Integer>into(new GlobalWindows())
            .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
            .accumulatingFiredPanes()
            .withAllowedLateness(new Duration(0), ClosingBehavior.FIRE_ALWAYS))
        .apply(Sum.integersGlobally().withoutDefaults().withFanout(2))
        .apply(ParDo.of(new GetLast()));

    PAssert.that(output).satisfies(new SerializableFunction<Iterable<Integer>, Void>() {
      @Override
      public Void apply(Iterable<Integer> input) {
        assertThat(input, hasItem(15));
        return null;
      }
    });

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBinaryCombineFn() {
    PCollection<KV<String, Integer>> input = copy(createInput(pipeline, TABLE), 2);
    PCollection<KV<String, Integer>> intProduct = input
        .apply("IntProduct", Combine.<String, Integer, Integer>perKey(new TestProdInt()));
    PCollection<KV<String, Integer>> objProduct = input
        .apply("ObjProduct", Combine.<String, Integer, Integer>perKey(new TestProdObj()));

    List<KV<String, Integer>> expected = Arrays.asList(KV.of("a", 16), KV.of("b", 169));
    PAssert.that(intProduct).containsInAnyOrder(expected);
    PAssert.that(objProduct).containsInAnyOrder(expected);

    pipeline.run();
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
    @Override
    public int apply(int left, int right) {
      return left * right;
    }

    @Override
    public int identity() {
      return 1;
    }
  }

  private static final class TestProdObj extends Combine.BinaryCombineFn<Integer> {
    @Override
    public Integer apply(Integer left, Integer right) {
      return left * right;
    }
  }

  /**
   * Computes the product, considering null values to be 2.
   */
  private static final class NullCombiner extends Combine.BinaryCombineFn<Integer> {
    @Override
    public Integer apply(Integer left, Integer right) {
      return (left == null ? 2 : left) * (right == null ? 2 : right);
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCombineGloballyAsSingletonView() {
    final PCollectionView<Integer> view = pipeline
        .apply("CreateEmptySideInput", Create.empty(BigEndianIntegerCoder.of()))
        .apply(Sum.integersGlobally().asSingletonView());

    PCollection<Integer> output = pipeline
        .apply("CreateVoidMainInput", Create.of((Void) null))
        .apply("OutputSideInput", ParDo.of(new DoFn<Void, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.sideInput(view));
                  }
                }).withSideInputs(view));

    PAssert.thatSingleton(output).isEqualTo(0);
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedCombineGloballyAsSingletonView() {
    FixedWindows windowFn = FixedWindows.of(Duration.standardMinutes(1));
    final PCollectionView<Integer> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(1, new Instant(100)),
                    TimestampedValue.of(3, new Instant(100))))
            .apply("WindowSideInput", Window.<Integer>into(windowFn))
            .apply("CombineSideInput", Sum.integersGlobally().asSingletonView());

    TimestampedValue<Void> nonEmptyElement = TimestampedValue.of(null, new Instant(100));
    TimestampedValue<Void> emptyElement = TimestampedValue.atMinimumTimestamp(null);
    PCollection<Integer> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.<Void>timestamped(nonEmptyElement, emptyElement).withCoder(VoidCoder.of()))
            .apply("WindowMainInput", Window.<Void>into(windowFn))
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

  @Test
  public void testCombineGetName() {
    assertEquals("Combine.globally(SumInts)", Combine.globally(new SumInts()).getName());
    assertEquals(
        "Combine.GloballyAsSingletonView",
        Combine.globally(new SumInts()).asSingletonView().getName());
    assertEquals("Combine.perKey(TestKeyed)", Combine.perKey(new TestKeyedCombineFn()).getName());
    assertEquals(
        "Combine.perKeyWithFanout(TestKeyed)",
        Combine.perKey(new TestKeyedCombineFn()).withHotKeyFanout(10).getName());
  }

  @Test
  public void testDisplayData() {
    UniqueInts combineFn = new UniqueInts() {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("fnMetadata", "foobar"));
      }
    };
    Combine.Globally<?, ?> combine = Combine.globally(combineFn)
        .withFanout(1234);
    DisplayData displayData = DisplayData.from(combine);

    assertThat(displayData, hasDisplayItem("combineFn", combineFn.getClass()));
    assertThat(displayData, hasDisplayItem("emitDefaultOnEmptyInput", true));
    assertThat(displayData, hasDisplayItem("fanout", 1234));
    assertThat(displayData, includesDisplayDataFor("combineFn", combineFn));
  }

  @Test
  public void testDisplayDataForWrappedFn() {
    UniqueInts combineFn = new UniqueInts() {
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

    CombineTest.UniqueInts combineFn = new CombineTest.UniqueInts();
    PTransform<PCollection<KV<Integer, Integer>>, ? extends POutput> combine =
        Combine.perKey(combineFn);

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(combine,
        KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));

    assertThat("Combine.perKey should include the combineFn in its primitive transform",
        displayData, hasItem(hasDisplayItem("combineFn", combineFn.getClass())));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCombinePerKeyWithHotKeyFanoutPrimitiveDisplayData() {
    int hotKeyFanout = 2;
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

    CombineTest.UniqueInts combineFn = new CombineTest.UniqueInts();
    PTransform<PCollection<KV<Integer, Integer>>, PCollection<KV<Integer, Set<Integer>>>> combine =
        Combine.<Integer, Integer, Set<Integer>>perKey(combineFn).withHotKeyFanout(hotKeyFanout);

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(combine,
        KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));

    assertThat("Combine.perKey.withHotKeyFanout should include the combineFn in its primitive "
        + "transform", displayData, hasItem(hasDisplayItem("combineFn", combineFn.getClass())));
    assertThat("Combine.perKey.withHotKeyFanout(int) should include the fanout in its primitive "
        + "transform", displayData, hasItem(hasDisplayItem("fanout", hotKeyFanout)));
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

  /** Example AccumulatingCombineFn. */
  private static class MeanInts extends
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

      @Override
      public int hashCode() {
        return Objects.hash(count, sum);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        }
        if (!(obj instanceof CountSum)) {
          return false;
        }
        CountSum other = (CountSum) obj;
        return this.count == other.count
            && (Math.abs(this.sum - other.sum) < 0.1);
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("count", count)
            .add("sum", sum)
            .toString();
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
    private class CountSumCoder extends CustomCoder<CountSum> {
      @Override
      public void encode(CountSum value, OutputStream outStream,
          Context context) throws CoderException, IOException {
        LONG_CODER.encode(value.count, outStream, context.nested());
        DOUBLE_CODER.encode(value.sum, outStream, context);
      }

      @Override
      public CountSum decode(InputStream inStream, Coder.Context context)
          throws CoderException, IOException {
        long count = LONG_CODER.decode(inStream, context.nested());
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
        LONG_CODER.registerByteSizeObserver(value.count, observer, context.nested());
        DOUBLE_CODER.registerByteSizeObserver(value.sum, observer, context);
      }
    }
  }

  /**
   * A KeyedCombineFn that exercises the full generality of [Keyed]CombineFn.
   *
   * <p>The net result of applying this CombineFn is a sorted list of all
   * characters occurring in the key and the decimal representations of
   * each value.
   */
  public static class TestKeyedCombineFn
      extends KeyedCombineFn<String, Integer, TestKeyedCombineFn.Accumulator, String> {

    // Not serializable.
    static class Accumulator {
      String value;
      public Accumulator(String value) {
        this.value = value;
      }

      public static Coder<Accumulator> getCoder() {
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

          @Override
          public String getEncodingId() {
            return "CombineTest.TestKeyedCombineFn.getAccumulatorCoder()";
          }
        };
      }
    }

    @Override
    public Coder<Accumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<String> keyCoder, Coder<Integer> inputCoder) {
      return Accumulator.getCoder();
    }

    @Override
    public Accumulator createAccumulator(String key) {
      return new Accumulator(key);
    }

    @Override
    public Accumulator addInput(String key, Accumulator accumulator, Integer value) {
      checkNotNull(key);
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

  /**
   * A {@link KeyedCombineFnWithContext} that exercises the full generality
   * of [Keyed]CombineFnWithContext.
   *
   * <p>The net result of applying this CombineFn is a sorted list of all
   * characters occurring in the key and the decimal representations of
   * main and side inputs values.
   */
  public class TestKeyedCombineFnWithContext
      extends KeyedCombineFnWithContext<String, Integer, TestKeyedCombineFn.Accumulator, String> {
    private final PCollectionView<Integer> view;

    public TestKeyedCombineFnWithContext(PCollectionView<Integer> view) {
      this.view = view;
    }

    @Override
    public Coder<TestKeyedCombineFn.Accumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<String> keyCoder, Coder<Integer> inputCoder) {
      return TestKeyedCombineFn.Accumulator.getCoder();
    }

    @Override
    public TestKeyedCombineFn.Accumulator createAccumulator(String key, Context c) {
      return new TestKeyedCombineFn.Accumulator(key + c.sideInput(view).toString());
    }

    @Override
    public TestKeyedCombineFn.Accumulator addInput(
        String key, TestKeyedCombineFn.Accumulator accumulator, Integer value, Context c) {
      try {
        assertThat(accumulator.value, Matchers.startsWith(key + c.sideInput(view).toString()));
        return new TestKeyedCombineFn.Accumulator(accumulator.value + String.valueOf(value));
      } finally {
        accumulator.value = "cleared in addInput";
      }

    }

    @Override
    public TestKeyedCombineFn.Accumulator mergeAccumulators(
        String key, Iterable<TestKeyedCombineFn.Accumulator> accumulators, Context c) {
      String keyPrefix = key + c.sideInput(view).toString();
      String all = keyPrefix;
      for (TestKeyedCombineFn.Accumulator accumulator : accumulators) {
        assertThat(accumulator.value, Matchers.startsWith(keyPrefix));
        all += accumulator.value.substring(keyPrefix.length());
        accumulator.value = "cleared in mergeAccumulators";
      }
      return new TestKeyedCombineFn.Accumulator(all);
    }

    @Override
    public String extractOutput(String key, TestKeyedCombineFn.Accumulator accumulator, Context c) {
      assertThat(accumulator.value, Matchers.startsWith(key + c.sideInput(view).toString()));
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
        checkState(merges == 0);
        checkState(outputs == 0);

        inputs++;
        sum += element;
      }

      @Override
      public void mergeAccumulator(Counter accumulator) {
        checkState(outputs == 0);
        checkArgument(accumulator.outputs == 0);

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
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        for (int i = 0; i < n; i++) {
          c.output(c.element());
        }
      }
    }));
  }
}
