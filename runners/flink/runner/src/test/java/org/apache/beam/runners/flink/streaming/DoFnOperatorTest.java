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
package org.apache.beam.runners.flink.streaming;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PCollectionViewTesting;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.Timer;
import org.apache.beam.sdk.util.TimerSpec;
import org.apache.beam.sdk.util.TimerSpecs;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateSpecs;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DoFnOperator}.
 */
@RunWith(JUnit4.class)
public class DoFnOperatorTest {

  // views and windows for testing side inputs
  private static final long WINDOW_MSECS_1 = 100;
  private static final long WINDOW_MSECS_2 = 500;

  private WindowingStrategy<Object, IntervalWindow> windowingStrategy1 =
      WindowingStrategy.of(FixedWindows.of(new Duration(WINDOW_MSECS_1)));

  private PCollectionView<Iterable<String>> view1 =
      PCollectionViewTesting.testingView(
          new TupleTag<Iterable<WindowedValue<String>>>() {},
          new PCollectionViewTesting.IdentityViewFn<String>(),
          StringUtf8Coder.of(),
          windowingStrategy1);

  private WindowingStrategy<Object, IntervalWindow> windowingStrategy2 =
      WindowingStrategy.of(FixedWindows.of(new Duration(WINDOW_MSECS_2)));

  private PCollectionView<Iterable<String>> view2 =
      PCollectionViewTesting.testingView(
          new TupleTag<Iterable<WindowedValue<String>>>() {},
          new PCollectionViewTesting.IdentityViewFn<String>(),
          StringUtf8Coder.of(),
          windowingStrategy2);

  @Test
  @SuppressWarnings("unchecked")
  public void testSingleOutput() throws Exception {

    WindowedValue.ValueOnlyWindowedValueCoder<String> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    DoFnOperator<String, String, String> doFnOperator = new DoFnOperator<>(
        new IdentityDoFn<String>(),
        windowedValueCoder,
        outputTag,
        Collections.<TupleTag<?>>emptyList(),
        new DoFnOperator.DefaultOutputManagerFactory(),
        WindowingStrategy.globalDefault(),
        new HashMap<Integer, PCollectionView<?>>(), /* side-input mapping */
        Collections.<PCollectionView<?>>emptyList(), /* side inputs */
        PipelineOptionsFactory.as(FlinkPipelineOptions.class),
        null);

    OneInputStreamOperatorTestHarness<WindowedValue<String>, String> testHarness =
        new OneInputStreamOperatorTestHarness<>(doFnOperator);

    testHarness.open();

    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("Hello")));

    assertThat(
        this.<String>stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(WindowedValue.valueInGlobalWindow("Hello")));

    testHarness.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultiOutputOutput() throws Exception {

    WindowedValue.ValueOnlyWindowedValueCoder<String> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());

    TupleTag<String> mainOutput = new TupleTag<>("main-output");
    TupleTag<String> additionalOutput1 = new TupleTag<>("output-1");
    TupleTag<String> additionalOutput2 = new TupleTag<>("output-2");
    ImmutableMap<TupleTag<?>, Integer> outputMapping = ImmutableMap.<TupleTag<?>, Integer>builder()
        .put(mainOutput, 1)
        .put(additionalOutput1, 2)
        .put(additionalOutput2, 3)
        .build();

    DoFnOperator<String, String, RawUnionValue> doFnOperator = new DoFnOperator<>(
        new MultiOutputDoFn(additionalOutput1, additionalOutput2),
        windowedValueCoder,
        mainOutput,
        ImmutableList.<TupleTag<?>>of(additionalOutput1, additionalOutput2),
        new DoFnOperator.MultiOutputOutputManagerFactory(outputMapping),
        WindowingStrategy.globalDefault(),
        new HashMap<Integer, PCollectionView<?>>(), /* side-input mapping */
        Collections.<PCollectionView<?>>emptyList(), /* side inputs */
        PipelineOptionsFactory.as(FlinkPipelineOptions.class),
        null);

    OneInputStreamOperatorTestHarness<WindowedValue<String>, RawUnionValue> testHarness =
        new OneInputStreamOperatorTestHarness<>(doFnOperator);

    testHarness.open();

    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("one")));
    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("two")));
    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("hello")));

    assertThat(
        this.stripStreamRecordFromRawUnion(testHarness.getOutput()),
        contains(
            new RawUnionValue(2, WindowedValue.valueInGlobalWindow("extra: one")),
            new RawUnionValue(3, WindowedValue.valueInGlobalWindow("extra: two")),
            new RawUnionValue(1, WindowedValue.valueInGlobalWindow("got: hello")),
            new RawUnionValue(2, WindowedValue.valueInGlobalWindow("got: hello")),
            new RawUnionValue(3, WindowedValue.valueInGlobalWindow("got: hello"))));

    testHarness.close();
  }

  @Test
  public void testLateDroppingForStatefulFn() throws Exception {

    WindowingStrategy<Object, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(new Duration(10)));

    DoFn<Integer, String> fn = new DoFn<Integer, String>() {

      @StateId("state")
      private final StateSpec<Object, ValueState<String>> stateSpec =
          StateSpecs.value(StringUtf8Coder.of());

      @ProcessElement
      public void processElement(ProcessContext context) {
        context.output(context.element().toString());
      }
    };

    WindowedValue.FullWindowedValueCoder<Integer> windowedValueCoder =
        WindowedValue.getFullCoder(
            VarIntCoder.of(),
            windowingStrategy.getWindowFn().windowCoder());

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    DoFnOperator<Integer, String, WindowedValue<String>> doFnOperator = new DoFnOperator<>(
        fn,
        windowedValueCoder,
        outputTag,
        Collections.<TupleTag<?>>emptyList(),
        new DoFnOperator.DefaultOutputManagerFactory<WindowedValue<String>>(),
        windowingStrategy,
        new HashMap<Integer, PCollectionView<?>>(), /* side-input mapping */
        Collections.<PCollectionView<?>>emptyList(), /* side inputs */
        PipelineOptionsFactory.as(FlinkPipelineOptions.class),
        VarIntCoder.of() /* key coder */);

    OneInputStreamOperatorTestHarness<WindowedValue<Integer>, WindowedValue<String>> testHarness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            doFnOperator,
            new KeySelector<WindowedValue<Integer>, Integer>() {
              @Override
              public Integer getKey(WindowedValue<Integer> integerWindowedValue) throws Exception {
                return integerWindowedValue.getValue();
              }
            },
            new CoderTypeInformation<>(VarIntCoder.of()));

    testHarness.open();

    testHarness.processWatermark(0);

    IntervalWindow window1 = new IntervalWindow(new Instant(0), Duration.millis(10));

    // this should not be late
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.of(13, new Instant(0), window1, PaneInfo.NO_FIRING)));

    assertThat(
        this.<String>stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(WindowedValue.of("13", new Instant(0), window1, PaneInfo.NO_FIRING)));

    testHarness.getOutput().clear();

    testHarness.processWatermark(9);

    // this should still not be considered late
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.of(17, new Instant(0), window1, PaneInfo.NO_FIRING)));

    assertThat(
        this.<String>stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(WindowedValue.of("17", new Instant(0), window1, PaneInfo.NO_FIRING)));

    testHarness.getOutput().clear();

    testHarness.processWatermark(10);

    // this should now be considered late
    testHarness.processElement(
        new StreamRecord<>(WindowedValue.of(17, new Instant(0), window1, PaneInfo.NO_FIRING)));

    assertThat(
        this.<String>stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        emptyIterable());

    testHarness.close();
  }

  @Test
  public void testStateGCForStatefulFn() throws Exception {

    WindowingStrategy<Object, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(new Duration(10))).withAllowedLateness(Duration.ZERO);

    final String timerId = "boo";
    final String stateId = "dazzle";

    final int offset = 5000;
    final int timerOutput = 4093;

    DoFn<KV<String, Integer>, KV<String, Integer>> fn =
        new DoFn<KV<String, Integer>, KV<String, Integer>>() {

          @TimerId(timerId)
          private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @StateId(stateId)
          private final StateSpec<Object, ValueState<String>> stateSpec =
              StateSpecs.value(StringUtf8Coder.of());

          @ProcessElement
          public void processElement(
              ProcessContext context,
              @TimerId(timerId) Timer timer,
              @StateId(stateId) ValueState<String> state,
              BoundedWindow window) {
            timer.set(window.maxTimestamp());
            state.write(context.element().getKey());
            context.output(
                KV.of(context.element().getKey(), context.element().getValue() + offset));
          }

          @OnTimer(timerId)
          public void onTimer(OnTimerContext context, @StateId(stateId) ValueState<String> state) {
            context.output(KV.of(state.read(), timerOutput));
          }
        };

    WindowedValue.FullWindowedValueCoder<KV<String, Integer>> windowedValueCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
            windowingStrategy.getWindowFn().windowCoder());

    TupleTag<KV<String, Integer>> outputTag = new TupleTag<>("main-output");

    DoFnOperator<
        KV<String, Integer>, KV<String, Integer>, WindowedValue<KV<String, Integer>>> doFnOperator =
        new DoFnOperator<>(
            fn,
            windowedValueCoder,
            outputTag,
            Collections.<TupleTag<?>>emptyList(),
            new DoFnOperator.DefaultOutputManagerFactory<WindowedValue<KV<String, Integer>>>(),
            windowingStrategy,
            new HashMap<Integer, PCollectionView<?>>(), /* side-input mapping */
            Collections.<PCollectionView<?>>emptyList(), /* side inputs */
            PipelineOptionsFactory.as(FlinkPipelineOptions.class),
            StringUtf8Coder.of() /* key coder */);

    KeyedOneInputStreamOperatorTestHarness<
        String,
        WindowedValue<KV<String, Integer>>,
        WindowedValue<KV<String, Integer>>> testHarness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            doFnOperator,
            new KeySelector<WindowedValue<KV<String, Integer>>, String>() {
              @Override
              public String getKey(
                  WindowedValue<KV<String, Integer>> kvWindowedValue) throws Exception {
                return kvWindowedValue.getValue().getKey();
              }
            },
            new CoderTypeInformation<>(StringUtf8Coder.of()));

    testHarness.open();

    testHarness.processWatermark(0);

    assertEquals(0, testHarness.numKeyedStateEntries());

    IntervalWindow window1 = new IntervalWindow(new Instant(0), Duration.millis(10));

    testHarness.processElement(
        new StreamRecord<>(
            WindowedValue.of(KV.of("key1", 5), new Instant(1), window1, PaneInfo.NO_FIRING)));

    testHarness.processElement(
        new StreamRecord<>(
            WindowedValue.of(KV.of("key2", 7), new Instant(3), window1, PaneInfo.NO_FIRING)));

    assertThat(
        this.<KV<String, Integer>>stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.of(
                KV.of("key1", 5 + offset), new Instant(1), window1, PaneInfo.NO_FIRING),
            WindowedValue.of(
                KV.of("key2", 7 + offset), new Instant(3), window1, PaneInfo.NO_FIRING)));

    assertEquals(2, testHarness.numKeyedStateEntries());

    testHarness.getOutput().clear();

    // this should trigger both the window.maxTimestamp() timer and the GC timer
    // this tests that the GC timer fires after the user timer
    testHarness.processWatermark(
        window1.maxTimestamp()
            .plus(windowingStrategy.getAllowedLateness())
            .plus(StatefulDoFnRunner.TimeInternalsCleanupTimer.GC_DELAY_MS)
            .getMillis());

    assertThat(
        this.<KV<String, Integer>>stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(
            WindowedValue.of(
                KV.of("key1", timerOutput), new Instant(9), window1, PaneInfo.NO_FIRING),
            WindowedValue.of(
                KV.of("key2", timerOutput), new Instant(9), window1, PaneInfo.NO_FIRING)));

    // ensure the state was garbage collected
    assertEquals(0, testHarness.numKeyedStateEntries());

    testHarness.close();
  }

  public void testSideInputs(boolean keyed) throws Exception {

    WindowedValue.ValueOnlyWindowedValueCoder<String> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    ImmutableMap<Integer, PCollectionView<?>> sideInputMapping =
        ImmutableMap.<Integer, PCollectionView<?>>builder()
            .put(1, view1)
            .put(2, view2)
            .build();

    Coder<String> keyCoder = null;
    if (keyed) {
      keyCoder = StringUtf8Coder.of();
    }

    DoFnOperator<String, String, String> doFnOperator = new DoFnOperator<>(
        new IdentityDoFn<String>(),
        windowedValueCoder,
        outputTag,
        Collections.<TupleTag<?>>emptyList(),
        new DoFnOperator.DefaultOutputManagerFactory<String>(),
        WindowingStrategy.globalDefault(),
        sideInputMapping, /* side-input mapping */
        ImmutableList.<PCollectionView<?>>of(view1, view2), /* side inputs */
        PipelineOptionsFactory.as(FlinkPipelineOptions.class),
        keyCoder);

    TwoInputStreamOperatorTestHarness<WindowedValue<String>, RawUnionValue, String> testHarness =
        new TwoInputStreamOperatorTestHarness<>(doFnOperator);

    if (keyed) {
      // we use a dummy key for the second input since it is considered to be broadcast
      testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
          doFnOperator,
          new StringKeySelector(),
          new DummyKeySelector(),
          BasicTypeInfo.STRING_TYPE_INFO);
    }

    testHarness.open();

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(100));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(0), new Instant(500));

    // test the keep of sideInputs events
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                1,
                valuesInWindow(ImmutableList.of("hello", "ciao"), new Instant(0), firstWindow))));
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                2,
                valuesInWindow(ImmutableList.of("foo", "bar"), new Instant(0), secondWindow))));

    // push in a regular elements
    WindowedValue<String> helloElement = valueInWindow("Hello", new Instant(0), firstWindow);
    WindowedValue<String> worldElement = valueInWindow("World", new Instant(1000), firstWindow);
    testHarness.processElement1(new StreamRecord<>(helloElement));
    testHarness.processElement1(new StreamRecord<>(worldElement));

    // test the keep of pushed-back events
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                1,
                valuesInWindow(ImmutableList.of("hello", "ciao"),
                    new Instant(1000), firstWindow))));
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                2,
                valuesInWindow(ImmutableList.of("foo", "bar"), new Instant(1000), secondWindow))));

    assertThat(
        this.<String>stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(helloElement, worldElement));

    testHarness.close();

  }

  /**
   * {@link TwoInputStreamOperatorTestHarness} support OperatorStateBackend,
   * but don't support KeyedStateBackend. So we just test sideInput of normal ParDo.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testNormalParDoSideInputs() throws Exception {
    testSideInputs(false);
  }

  @Test
  public void testKeyedSideInputs() throws Exception {
    testSideInputs(true);
  }

  private <T> Iterable<WindowedValue<T>> stripStreamRecordFromWindowedValue(
      Iterable<Object> input) {

    return FluentIterable.from(input).filter(new Predicate<Object>() {
      @Override
      public boolean apply(@Nullable Object o) {
        return o instanceof StreamRecord && ((StreamRecord) o).getValue() instanceof WindowedValue;
      }
    }).transform(new Function<Object, WindowedValue<T>>() {
      @Nullable
      @Override
      @SuppressWarnings({"unchecked", "rawtypes"})
      public WindowedValue<T> apply(@Nullable Object o) {
        if (o instanceof StreamRecord && ((StreamRecord) o).getValue() instanceof WindowedValue) {
          return (WindowedValue) ((StreamRecord) o).getValue();
        }
        throw new RuntimeException("unreachable");
      }
    });
  }

  private Iterable<RawUnionValue> stripStreamRecordFromRawUnion(Iterable<Object> input) {
    return FluentIterable.from(input).filter(new Predicate<Object>() {
      @Override
      public boolean apply(@Nullable Object o) {
        return o instanceof StreamRecord && ((StreamRecord) o).getValue() instanceof RawUnionValue;
      }
    }).transform(new Function<Object, RawUnionValue>() {
      @Nullable
      @Override
      @SuppressWarnings({"unchecked", "rawtypes"})
      public RawUnionValue apply(@Nullable Object o) {
        if (o instanceof StreamRecord && ((StreamRecord) o).getValue() instanceof RawUnionValue) {
          return (RawUnionValue) ((StreamRecord) o).getValue();
        }
        throw new RuntimeException("unreachable");
      }
    });
  }

  private static class MultiOutputDoFn extends DoFn<String, String> {
    private TupleTag<String> additionalOutput1;
    private TupleTag<String> additionalOutput2;

    public MultiOutputDoFn(TupleTag<String> additionalOutput1, TupleTag<String> additionalOutput2) {
      this.additionalOutput1 = additionalOutput1;
      this.additionalOutput2 = additionalOutput2;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      if (c.element().equals("one")) {
        c.output(additionalOutput1, "extra: one");
      } else if (c.element().equals("two")) {
        c.output(additionalOutput2, "extra: two");
      } else {
        c.output("got: " + c.element());
        c.output(additionalOutput1, "got: " + c.element());
        c.output(additionalOutput2, "got: " + c.element());
      }
    }
  }

  private static class IdentityDoFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element());
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private WindowedValue<Iterable<?>> valuesInWindow(
      Iterable<?> values, Instant timestamp, BoundedWindow window) {
    return (WindowedValue) WindowedValue.of(values, timestamp, window, PaneInfo.NO_FIRING);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private <T> WindowedValue<T> valueInWindow(
      T value, Instant timestamp, BoundedWindow window) {
    return WindowedValue.of(value, timestamp, window, PaneInfo.NO_FIRING);
  }


  private static class DummyKeySelector implements KeySelector<RawUnionValue, String> {
    @Override
    public String getKey(RawUnionValue stringWindowedValue) throws Exception {
      return "dummy_key";
    }
  }

  private static class StringKeySelector implements KeySelector<WindowedValue<String>, String> {
    @Override
    public String getKey(WindowedValue<String> stringWindowedValue) throws Exception {
      return stringWindowedValue.getValue();
    }
  }
}
