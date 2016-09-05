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

import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.HashMap;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PCollectionViewTesting;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
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

  private PCollectionView<Iterable<String>> view1 = PCollectionViewTesting.testingView(
      new TupleTag<Iterable<WindowedValue<String>>>() {},
      new PCollectionViewTesting.IdentityViewFn<String>(),
      StringUtf8Coder.of(),
      windowingStrategy1);

  private WindowingStrategy<Object, IntervalWindow> windowingStrategy2 =
      WindowingStrategy.of(FixedWindows.of(new Duration(WINDOW_MSECS_2)));

  private PCollectionView<Iterable<String>> view2 = PCollectionViewTesting.testingView(
      new TupleTag<Iterable<WindowedValue<String>>>() {},
      new PCollectionViewTesting.IdentityViewFn<String>(),
      StringUtf8Coder.of(),
      windowingStrategy2);

  @Test
  @SuppressWarnings("unchecked")
  public void testSingleOutput() throws Exception {

    WindowedValue.ValueOnlyWindowedValueCoder<String> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());

    CoderTypeInformation<WindowedValue<String>> coderTypeInfo =
        new CoderTypeInformation<>(windowedValueCoder);

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    DoFnOperator<String, String, String> doFnOperator = new DoFnOperator<>(
        new IdentityDoFn<String>(),
        coderTypeInfo,
        outputTag,
        Collections.<TupleTag<?>>emptyList(),
        new DoFnOperator.DefaultOutputManagerFactory(),
        WindowingStrategy.globalDefault(),
        new HashMap<Integer, PCollectionView<?>>(), /* side-input mapping */
        Collections.<PCollectionView<?>>emptyList(), /* side inputs */
        PipelineOptionsFactory.as(FlinkPipelineOptions.class));

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

    CoderTypeInformation<WindowedValue<String>> coderTypeInfo =
        new CoderTypeInformation<>(windowedValueCoder);

    TupleTag<String> mainOutput = new TupleTag<>("main-output");
    TupleTag<String> sideOutput1 = new TupleTag<>("side-output-1");
    TupleTag<String> sideOutput2 = new TupleTag<>("side-output-2");
    ImmutableMap<TupleTag<?>, Integer> outputMapping = ImmutableMap.<TupleTag<?>, Integer>builder()
        .put(mainOutput, 1)
        .put(sideOutput1, 2)
        .put(sideOutput2, 3)
        .build();

    DoFnOperator<String, String, RawUnionValue> doFnOperator = new DoFnOperator<>(
        new MultiOutputDoFn(sideOutput1, sideOutput2),
        coderTypeInfo,
        mainOutput,
        ImmutableList.<TupleTag<?>>of(sideOutput1, sideOutput2),
        new DoFnOperator.MultiOutputOutputManagerFactory(outputMapping),
        WindowingStrategy.globalDefault(),
        new HashMap<Integer, PCollectionView<?>>(), /* side-input mapping */
        Collections.<PCollectionView<?>>emptyList(), /* side inputs */
        PipelineOptionsFactory.as(FlinkPipelineOptions.class));

    OneInputStreamOperatorTestHarness<WindowedValue<String>, RawUnionValue> testHarness =
        new OneInputStreamOperatorTestHarness<>(doFnOperator);

    testHarness.open();

    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("one")));
    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("two")));
    testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("hello")));

    assertThat(
        this.stripStreamRecordFromRawUnion(testHarness.getOutput()),
        contains(
            new RawUnionValue(2, WindowedValue.valueInGlobalWindow("side: one")),
            new RawUnionValue(3, WindowedValue.valueInGlobalWindow("side: two")),
            new RawUnionValue(1, WindowedValue.valueInGlobalWindow("got: hello")),
            new RawUnionValue(2, WindowedValue.valueInGlobalWindow("got: hello")),
            new RawUnionValue(3, WindowedValue.valueInGlobalWindow("got: hello"))));

    testHarness.close();
  }

  /**
   * For now, this test doesn't work because {@link TwoInputStreamOperatorTestHarness} is not
   * sufficiently well equipped to handle more complex operators that require a state backend.
   * We have to revisit this once we update to a newer version of Flink and also add some more
   * tests that verify pushback behaviour and watermark hold behaviour.
   *
   * <p>The behaviour that we would test here is also exercised by the
   * {@link org.apache.beam.sdk.testing.RunnableOnService} tests, so the code is not untested.
   */
  @Test
  @Ignore
  @SuppressWarnings("unchecked")
  public void testSideInputs() throws Exception {

    WindowedValue.ValueOnlyWindowedValueCoder<String> windowedValueCoder =
        WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());

    CoderTypeInformation<WindowedValue<String>> coderTypeInfo =
        new CoderTypeInformation<>(windowedValueCoder);

    TupleTag<String> outputTag = new TupleTag<>("main-output");

    ImmutableMap<Integer, PCollectionView<?>> sideInputMapping =
        ImmutableMap.<Integer, PCollectionView<?>>builder()
            .put(1, view1)
            .put(2, view2)
            .build();

    DoFnOperator<String, String, String> doFnOperator = new DoFnOperator<>(
        new IdentityDoFn<String>(),
        coderTypeInfo,
        outputTag,
        Collections.<TupleTag<?>>emptyList(),
        new DoFnOperator.DefaultOutputManagerFactory(),
        WindowingStrategy.globalDefault(),
        sideInputMapping, /* side-input mapping */
        ImmutableList.<PCollectionView<?>>of(view1, view2), /* side inputs */
        PipelineOptionsFactory.as(FlinkPipelineOptions.class));

    TwoInputStreamOperatorTestHarness<WindowedValue<String>, RawUnionValue, String> testHarness =
        new TwoInputStreamOperatorTestHarness<>(doFnOperator);

    testHarness.open();

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(100));

    // push in some side-input elements
    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                1,
                valuesInWindow(ImmutableList.of("hello", "ciao"), new Instant(0), firstWindow))));

    testHarness.processElement2(
        new StreamRecord<>(
            new RawUnionValue(
                2,
                valuesInWindow(ImmutableList.of("foo", "bar"), new Instant(0), firstWindow))));

    // push in a regular elements
    testHarness.processElement1(new StreamRecord<>(WindowedValue.valueInGlobalWindow("Hello")));

    assertThat(
        this.<String>stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        contains(WindowedValue.valueInGlobalWindow("Hello")));

    testHarness.close();
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

  private static class MultiOutputDoFn extends OldDoFn<String, String> {
    private TupleTag<String> sideOutput1;
    private TupleTag<String> sideOutput2;

    public MultiOutputDoFn(TupleTag<String> sideOutput1, TupleTag<String> sideOutput2) {
      this.sideOutput1 = sideOutput1;
      this.sideOutput2 = sideOutput2;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      if (c.element().equals("one")) {
        c.sideOutput(sideOutput1, "side: one");
      } else if (c.element().equals("two")) {
        c.sideOutput(sideOutput2, "side: two");
      } else {
        c.output("got: " + c.element());
        c.sideOutput(sideOutput1, "got: " + c.element());
        c.sideOutput(sideOutput2, "got: " + c.element());
      }
    }
  }

  private static class IdentityDoFn<T> extends OldDoFn<T, T> {
    @Override
    public void processElement(OldDoFn<T, T>.ProcessContext c) throws Exception {
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


}
