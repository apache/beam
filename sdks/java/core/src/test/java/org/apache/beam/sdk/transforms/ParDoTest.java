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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasKey;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasLabel;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasType;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasValue;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFor;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;
import static org.apache.beam.sdk.util.StringUtils.jsonStringToByteArray;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.DataflowPortabilityApiUnsupported;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesKeyInParDo;
import org.apache.beam.sdk.testing.UsesMapState;
import org.apache.beam.sdk.testing.UsesOnWindowExpiration;
import org.apache.beam.sdk.testing.UsesOrderedListState;
import org.apache.beam.sdk.testing.UsesRequiresTimeSortedInput;
import org.apache.beam.sdk.testing.UsesSetState;
import org.apache.beam.sdk.testing.UsesSideInputs;
import org.apache.beam.sdk.testing.UsesSideInputsWithDifferentCoders;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.UsesStrictTimerOrdering;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.UsesTestStreamWithOutputTimestamp;
import org.apache.beam.sdk.testing.UsesTestStreamWithProcessingTime;
import org.apache.beam.sdk.testing.UsesTimerMap;
import org.apache.beam.sdk.testing.UsesTimersInParDo;
import org.apache.beam.sdk.testing.UsesUnboundedPCollections;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.DoFn.OnTimer;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.Mean.CountSum;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.display.DisplayDataMatchers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ParDo. */
public class ParDoTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  /** Shared base test base with setup/teardown helpers. */
  public abstract static class SharedTestBase {
    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    @Rule public transient ExpectedException thrown = ExpectedException.none();
  }

  private static class PrintingDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(
        @Element String element,
        @Timestamp Instant timestamp,
        BoundedWindow window,
        OutputReceiver<String> receiver) {
      receiver.output(
          element + ":" + timestamp.getMillis() + ":" + window.maxTimestamp().getMillis());
    }
  }

  static class TestNoOutputDoFn extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(DoFn<Integer, String>.ProcessContext c) throws Exception {}
  }

  static class TestDoFn extends DoFn<Integer, String> {
    enum State {
      NOT_SET_UP,
      UNSTARTED,
      STARTED,
      PROCESSING,
      FINISHED
    }

    State state = State.NOT_SET_UP;

    final List<PCollectionView<Integer>> sideInputViews = new ArrayList<>();
    final List<TupleTag<String>> additionalOutputTupleTags = new ArrayList<>();

    public TestDoFn() {}

    public TestDoFn(
        List<PCollectionView<Integer>> sideInputViews,
        List<TupleTag<String>> additionalOutputTupleTags) {
      this.sideInputViews.addAll(sideInputViews);
      this.additionalOutputTupleTags.addAll(additionalOutputTupleTags);
    }

    @Setup
    public void prepare() {
      assertEquals(State.NOT_SET_UP, state);
      state = State.UNSTARTED;
    }

    @StartBundle
    public void startBundle() {
      assertThat(state, anyOf(equalTo(State.UNSTARTED), equalTo(State.FINISHED)));

      state = State.STARTED;
    }

    @ProcessElement
    public void processElement(ProcessContext c, @Element Integer element) {
      assertThat(state, anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
      state = State.PROCESSING;
      outputToAllWithSideInputs(c, "processing: " + element);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      assertThat(state, anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
      state = State.FINISHED;
      c.output("finished", BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
      for (TupleTag<String> additionalOutputTupleTag : additionalOutputTupleTags) {
        c.output(
            additionalOutputTupleTag,
            additionalOutputTupleTag.getId() + ": " + "finished",
            BoundedWindow.TIMESTAMP_MIN_VALUE,
            GlobalWindow.INSTANCE);
      }
    }

    private void outputToAllWithSideInputs(ProcessContext c, String value) {
      if (!sideInputViews.isEmpty()) {
        List<Integer> sideInputValues = new ArrayList<>();
        for (PCollectionView<Integer> sideInputView : sideInputViews) {
          sideInputValues.add(c.sideInput(sideInputView));
        }
        value += ": " + sideInputValues;
      }
      c.output(value);
      for (TupleTag<String> additionalOutputTupleTag : additionalOutputTupleTags) {
        c.output(additionalOutputTupleTag, additionalOutputTupleTag.getId() + ": " + value);
      }
    }
  }

  static class TestStartBatchErrorDoFn extends DoFn<Integer, String> {
    @StartBundle
    public void startBundle() {
      throw new RuntimeException("test error in initialize");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      // This has to be here.
    }
  }

  static class TestProcessElementErrorDoFn extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      throw new RuntimeException("test error in process");
    }
  }

  static class TestFinishBatchErrorDoFn extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      // This has to be here.
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      throw new RuntimeException("test error in finalize");
    }
  }

  private static class StrangelyNamedDoer extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {}
  }

  static class TestOutputTimestampDoFn<T extends Number> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(@Element T value, OutputReceiver<T> r) {
      r.outputWithTimestamp(value, new Instant(value.longValue()));
    }
  }

  static class TestShiftTimestampDoFn<T extends Number> extends DoFn<T, T> {
    private Duration allowedTimestampSkew;
    private Duration durationToShift;

    public TestShiftTimestampDoFn(Duration allowedTimestampSkew, Duration durationToShift) {
      this.allowedTimestampSkew = allowedTimestampSkew;
      this.durationToShift = durationToShift;
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return allowedTimestampSkew;
    }

    @ProcessElement
    public void processElement(
        @Element T value, @Timestamp Instant timestamp, OutputReceiver<T> r) {
      checkNotNull(timestamp);
      r.outputWithTimestamp(value, timestamp.plus(durationToShift));
    }
  }

  static class TestFormatTimestampDoFn<T extends Number> extends DoFn<T, String> {
    @ProcessElement
    public void processElement(
        @Element T element, @Timestamp Instant timestamp, OutputReceiver<String> r) {
      checkNotNull(timestamp);
      r.output("processing: " + element + ", timestamp: " + timestamp.getMillis());
    }
  }

  static class MultiFilter extends PTransform<PCollection<Integer>, PCollectionTuple> {

    private static final TupleTag<Integer> BY2 = new TupleTag<Integer>("by2") {};
    private static final TupleTag<Integer> BY3 = new TupleTag<Integer>("by3") {};

    @Override
    public PCollectionTuple expand(PCollection<Integer> input) {
      PCollection<Integer> by2 = input.apply("Filter2s", ParDo.of(new FilterFn(2)));
      PCollection<Integer> by3 = input.apply("Filter3s", ParDo.of(new FilterFn(3)));
      return PCollectionTuple.of(BY2, by2).and(BY3, by3);
    }

    static class FilterFn extends DoFn<Integer, Integer> {
      private final int divisor;

      FilterFn(int divisor) {
        this.divisor = divisor;
      }

      @ProcessElement
      public void processElement(@Element Integer element, OutputReceiver<Integer> r)
          throws Exception {
        if (element % divisor == 0) {
          r.output(element);
        }
      }
    }
  }

  /** Tests for basic {@link ParDo} scenarios. */
  @RunWith(JUnit4.class)
  public static class BasicTests extends SharedTestBase implements Serializable {
    @Test
    @Category(ValidatesRunner.class)
    public void testParDo() {
      List<Integer> inputs = Arrays.asList(3, -42, 666);

      PCollection<String> output =
          pipeline.apply(Create.of(inputs)).apply(ParDo.of(new TestDoFn()));

      PAssert.that(output).satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));
      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testParDoEmpty() {

      List<Integer> inputs = Arrays.asList();

      PCollection<String> output =
          pipeline
              .apply(Create.of(inputs).withCoder(VarIntCoder.of()))
              .apply("TestDoFn", ParDo.of(new TestDoFn()));

      PAssert.that(output).satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testParDoEmptyOutputs() {

      List<Integer> inputs = Arrays.asList();

      PCollection<String> output =
          pipeline
              .apply(Create.of(inputs).withCoder(VarIntCoder.of()))
              .apply("TestDoFn", ParDo.of(new TestNoOutputDoFn()));

      PAssert.that(output).empty();

      pipeline.run();
    }

    @Test
    public void testParDoTransformNameBasedDoFnWithTrimmedSuffix() {
      assertThat(ParDo.of(new PrintingDoFn()).getName(), containsString("ParDo(Printing)"));
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testParDoInCustomTransform() {

      List<Integer> inputs = Arrays.asList(3, -42, 666);

      PCollection<String> output =
          pipeline
              .apply(Create.of(inputs))
              .apply(
                  "CustomTransform",
                  new PTransform<PCollection<Integer>, PCollection<String>>() {
                    @Override
                    public PCollection<String> expand(PCollection<Integer> input) {
                      return input.apply(ParDo.of(new TestDoFn()));
                    }
                  });

      // Test that Coder inference of the result works through
      // user-defined PTransforms.
      PAssert.that(output).satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

      pipeline.run();
    }

    @Test
    public void testJsonEscaping() {
      // Declare an arbitrary function and make sure we can serialize it
      DoFn<Integer, Integer> doFn =
          new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElement(@Element Integer element, OutputReceiver<Integer> r) {
              r.output(element + 1);
            }
          };

      byte[] serializedBytes = serializeToByteArray(doFn);
      String serializedJson = byteArrayToJsonString(serializedBytes);
      assertArrayEquals(serializedBytes, jsonStringToByteArray(serializedJson));
    }

    @Test
    public void testDoFnDisplayData() {
      DoFn<String, String> fn =
          new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {}

            @Override
            public void populateDisplayData(Builder builder) {
              builder.add(DisplayData.item("doFnMetadata", "bar"));
            }
          };

      SingleOutput<String, String> parDo = ParDo.of(fn);

      DisplayData displayData = DisplayData.from(parDo);
      assertThat(
          displayData,
          hasDisplayItem(
              allOf(
                  hasKey("fn"),
                  hasType(DisplayData.Type.JAVA_CLASS),
                  DisplayDataMatchers.hasValue(fn.getClass().getName()))));

      assertThat(displayData, includesDisplayDataFor("fn", fn));
    }

    @Test
    public void testDoFnWithContextDisplayData() {
      DoFn<String, String> fn =
          new DoFn<String, String>() {
            @ProcessElement
            public void proccessElement(ProcessContext c) {}

            @Override
            public void populateDisplayData(Builder builder) {
              builder.add(DisplayData.item("fnMetadata", "foobar"));
            }
          };

      SingleOutput<String, String> parDo = ParDo.of(fn);

      DisplayData displayData = DisplayData.from(parDo);
      assertThat(displayData, includesDisplayDataFor("fn", fn));
      assertThat(displayData, hasDisplayItem("fn", fn.getClass()));
    }

    private static class SerializableClass implements Serializable {}

    @Test
    public void testStatefulDoFnDisplayData() {
      DoFn<KV<String, String>, String> fn =
          new DoFn<KV<String, String>, String>() {
            @StateId("int")
            final StateSpec<ValueState<Integer>> intState = StateSpecs.value();

            @StateId("map")
            final StateSpec<MapState<String, SerializableClass>> mapState = StateSpecs.map();

            @ProcessElement
            public void proccessElement(ProcessContext c) {}
          };

      SingleOutput<KV<String, String>, String> parDo = ParDo.of(fn);

      // Use the parDo in a pipeline to cause state coders to be inferred.
      pipeline.apply(Create.of(KV.of("input", "value"))).apply(parDo);

      DisplayData displayData = DisplayData.from(parDo);
      assertThat(
          displayData,
          hasDisplayItem(
              allOf(
                  hasKey("state_int"),
                  hasType(DisplayData.Type.STRING),
                  hasValue("ValueState<VarIntCoder>"),
                  hasLabel("State \"int\""))));
      assertThat(
          displayData,
          hasDisplayItem(
              allOf(
                  hasKey("state_map"),
                  hasType(DisplayData.Type.STRING),
                  hasValue(
                      "MapState<StringUtf8Coder, "
                          + "SerializableCoder(org.apache.beam.sdk.transforms.ParDoTest"
                          + "$BasicTests$SerializableClass)>"),
                  hasLabel("State \"map\""))));
    }

    @Test
    public void testRejectsWrongWindowType() {

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage(GlobalWindow.class.getSimpleName());
      thrown.expectMessage(IntervalWindow.class.getSimpleName());
      thrown.expectMessage("window type");
      thrown.expectMessage("not a supertype");

      pipeline
          .apply(Create.of(1, 2, 3))
          .apply(
              ParDo.of(
                  new DoFn<Integer, Integer>() {
                    @ProcessElement
                    public void process(ProcessContext c, IntervalWindow w) {}
                  }));
    }

    /**
     * Tests that it is OK to use different window types in the parameter lists to different {@link
     * DoFn} functions, as long as they are all subtypes of the actual window type of the input.
     *
     * <p>Today, the only method other than {@link ProcessElement @ProcessElement} that can accept
     * extended parameters is {@link OnTimer @OnTimer}, which is rejected before it reaches window
     * type validation. Rather than delay validation, this test is temporarily disabled.
     */
    @Ignore("ParDo rejects this on account of it using timers")
    @Test
    public void testMultipleWindowSubtypesOK() {
      final String timerId = "gobbledegook";

      pipeline
          .apply(Create.of(1, 2, 3))
          .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
          .apply(
              ParDo.of(
                  new DoFn<Integer, Integer>() {
                    @TimerId(timerId)
                    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                    @ProcessElement
                    public void process(ProcessContext c, IntervalWindow w) {}

                    @OnTimer(timerId)
                    public void onTimer(BoundedWindow w) {}
                  }));

      // If it doesn't crash, we made it!
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testPipelineOptionsParameter() {
      PCollection<String> results =
          pipeline
              .apply(Create.of(1))
              .apply(
                  ParDo.of(
                      new DoFn<Integer, String>() {
                        @ProcessElement
                        public void process(OutputReceiver<String> r, PipelineOptions options) {
                          r.output(options.as(MyOptions.class).getFakeOption());
                        }
                      }));

      String testOptionValue = "not fake anymore";
      pipeline.getOptions().as(MyOptions.class).setFakeOption(testOptionValue);
      PAssert.that(results).containsInAnyOrder("not fake anymore");

      pipeline.run();
    }
  }

  /** Tests to validate behaviors around multiple inputs or outputs. */
  @RunWith(JUnit4.class)
  public static class MultipleInputsAndOutputTests extends SharedTestBase implements Serializable {
    @Test
    @Category(ValidatesRunner.class)
    public void testParDoWithTaggedOutput() {

      List<Integer> inputs = Arrays.asList(3, -42, 666);

      TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
      TupleTag<String> additionalOutputTag1 = new TupleTag<String>("additional1") {};
      TupleTag<String> additionalOutputTag2 = new TupleTag<String>("additional2") {};
      TupleTag<String> additionalOutputTag3 = new TupleTag<String>("additional3") {};
      TupleTag<String> additionalOutputTagUnwritten = new TupleTag<String>("unwrittenOutput") {};

      PCollectionTuple outputs =
          pipeline
              .apply(Create.of(inputs))
              .apply(
                  ParDo.of(
                          new TestDoFn(
                              Arrays.asList(),
                              Arrays.asList(
                                  additionalOutputTag1,
                                  additionalOutputTag2,
                                  additionalOutputTag3)))
                      .withOutputTags(
                          mainOutputTag,
                          TupleTagList.of(additionalOutputTag3)
                              .and(additionalOutputTag1)
                              .and(additionalOutputTagUnwritten)
                              .and(additionalOutputTag2)));

      PAssert.that(outputs.get(mainOutputTag))
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

      PAssert.that(outputs.get(additionalOutputTag1))
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag1));
      PAssert.that(outputs.get(additionalOutputTag2))
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag2));
      PAssert.that(outputs.get(additionalOutputTag3))
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag3));
      PAssert.that(outputs.get(additionalOutputTagUnwritten)).empty();

      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testParDoEmptyWithTaggedOutput() {
      TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
      TupleTag<String> additionalOutputTag1 = new TupleTag<String>("additional1") {};
      TupleTag<String> additionalOutputTag2 = new TupleTag<String>("additional2") {};
      TupleTag<String> additionalOutputTag3 = new TupleTag<String>("additional3") {};
      TupleTag<String> additionalOutputTagUnwritten = new TupleTag<String>("unwrittenOutput") {};

      PCollectionTuple outputs =
          pipeline
              .apply(Create.empty(VarIntCoder.of()))
              .apply(
                  ParDo.of(
                          new TestDoFn(
                              Arrays.asList(),
                              Arrays.asList(
                                  additionalOutputTag1,
                                  additionalOutputTag2,
                                  additionalOutputTag3)))
                      .withOutputTags(
                          mainOutputTag,
                          TupleTagList.of(additionalOutputTag3)
                              .and(additionalOutputTag1)
                              .and(additionalOutputTagUnwritten)
                              .and(additionalOutputTag2)));

      List<Integer> inputs = Collections.emptyList();
      PAssert.that(outputs.get(mainOutputTag))
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

      PAssert.that(outputs.get(additionalOutputTag1))
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag1));
      PAssert.that(outputs.get(additionalOutputTag2))
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag2));
      PAssert.that(outputs.get(additionalOutputTag3))
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs).fromOutput(additionalOutputTag3));
      PAssert.that(outputs.get(additionalOutputTagUnwritten)).empty();

      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testParDoWithEmptyTaggedOutput() {
      TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
      TupleTag<String> additionalOutputTag1 = new TupleTag<String>("additional1") {};
      TupleTag<String> additionalOutputTag2 = new TupleTag<String>("additional2") {};

      PCollectionTuple outputs =
          pipeline
              .apply(Create.empty(VarIntCoder.of()))
              .apply(
                  ParDo.of(new TestNoOutputDoFn())
                      .withOutputTags(
                          mainOutputTag,
                          TupleTagList.of(additionalOutputTag1).and(additionalOutputTag2)));

      PAssert.that(outputs.get(mainOutputTag)).empty();

      PAssert.that(outputs.get(additionalOutputTag1)).empty();
      PAssert.that(outputs.get(additionalOutputTag2)).empty();

      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testParDoWithOnlyTaggedOutput() {

      List<Integer> inputs = Arrays.asList(3, -42, 666);

      final TupleTag<Void> mainOutputTag = new TupleTag<Void>("main") {};
      final TupleTag<Integer> additionalOutputTag = new TupleTag<Integer>("additional") {};

      PCollectionTuple outputs =
          pipeline
              .apply(Create.of(inputs))
              .apply(
                  ParDo.of(
                          new DoFn<Integer, Void>() {
                            @ProcessElement
                            public void processElement(
                                @Element Integer element, MultiOutputReceiver r) {
                              r.get(additionalOutputTag).output(element);
                            }
                          })
                      .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

      PAssert.that(outputs.get(mainOutputTag)).empty();
      PAssert.that(outputs.get(additionalOutputTag)).containsInAnyOrder(inputs);

      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testParDoWritingToUndeclaredTag() {
      List<Integer> inputs = Arrays.asList(3, -42, 666);

      TupleTag<String> notOutputTag = new TupleTag<String>("additional") {};

      pipeline
          .apply(Create.of(inputs))
          .apply(
              ParDo.of(new TestDoFn(Arrays.asList(), Arrays.asList(notOutputTag)))
              // No call to .withOutputTags - should cause error
              );

      thrown.expectMessage("additional");
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testParDoWithSideInputs() {

      List<Integer> inputs = Arrays.asList(3, -42, 666);

      PCollectionView<Integer> sideInput1 =
          pipeline
              .apply("CreateSideInput1", Create.of(11))
              .apply("ViewSideInput1", View.asSingleton());
      PCollectionView<Integer> sideInputUnread =
          pipeline
              .apply("CreateSideInputUnread", Create.of(-3333))
              .apply("ViewSideInputUnread", View.asSingleton());
      PCollectionView<Integer> sideInput2 =
          pipeline
              .apply("CreateSideInput2", Create.of(222))
              .apply("ViewSideInput2", View.asSingleton());

      PCollection<String> output =
          pipeline
              .apply(Create.of(inputs))
              .apply(
                  ParDo.of(new TestDoFn(Arrays.asList(sideInput1, sideInput2), Arrays.asList()))
                      .withSideInputs(sideInput1, sideInputUnread, sideInput2));

      PAssert.that(output)
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs).andSideInputs(11, 222));

      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesSideInputs.class})
    public void testSideInputAnnotationFailedValidationMissing() {
      // SideInput tag id
      final String sideInputTag1 = "tag1";

      DoFn<Integer, List<Integer>> fn =
          new DoFn<Integer, List<Integer>>() {
            @ProcessElement
            public void processElement(@SideInput(sideInputTag1) String tag1) {}
          };

      thrown.expect(IllegalArgumentException.class);
      PCollection<List<Integer>> output =
          pipeline.apply("Create main input", Create.of(2)).apply(ParDo.of(fn));
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesSideInputs.class})
    public void testSideInputAnnotationFailedValidationSingletonType() {

      final PCollectionView<Integer> sideInput1 =
          pipeline
              .apply("CreateSideInput1", Create.of(2))
              .apply("ViewSideInput1", View.asSingleton());

      // SideInput tag id
      final String sideInputTag1 = "tag1";

      DoFn<Integer, List<Integer>> fn =
          new DoFn<Integer, List<Integer>>() {
            @ProcessElement
            public void processElement(@SideInput(sideInputTag1) String tag1) {}
          };

      thrown.expect(IllegalArgumentException.class);
      PCollection<List<Integer>> output =
          pipeline
              .apply("Create main input", Create.of(2))
              .apply(ParDo.of(fn).withSideInput(sideInputTag1, sideInput1));
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesSideInputs.class})
    public void testSideInputAnnotationFailedValidationListType() {

      final PCollectionView<List<Integer>> sideInput1 =
          pipeline
              .apply("CreateSideInput1", Create.of(2, 1, 0))
              .apply("ViewSideInput1", View.asList());

      // SideInput tag id
      final String sideInputTag1 = "tag1";

      DoFn<Integer, List<Integer>> fn =
          new DoFn<Integer, List<Integer>>() {
            @ProcessElement
            public void processElement(@SideInput(sideInputTag1) List<String> tag1) {}
          };

      thrown.expect(IllegalArgumentException.class);
      PCollection<List<Integer>> output =
          pipeline
              .apply("Create main input", Create.of(2))
              .apply(ParDo.of(fn).withSideInput(sideInputTag1, sideInput1));
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesSideInputs.class})
    public void testSideInputAnnotationFailedValidationIterableType() {

      final PCollectionView<Iterable<Integer>> sideInput1 =
          pipeline
              .apply("CreateSideInput1", Create.of(2, 1, 0))
              .apply("ViewSideInput1", View.asIterable());

      // SideInput tag id
      final String sideInputTag1 = "tag1";

      DoFn<Integer, List<Integer>> fn =
          new DoFn<Integer, List<Integer>>() {
            @ProcessElement
            public void processElement(@SideInput(sideInputTag1) List<String> tag1) {}
          };

      thrown.expect(IllegalArgumentException.class);
      PCollection<List<Integer>> output =
          pipeline
              .apply("Create main input", Create.of(2))
              .apply(ParDo.of(fn).withSideInput(sideInputTag1, sideInput1));
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesSideInputs.class})
    public void testSideInputAnnotationFailedValidationMapType() {

      final PCollectionView<Map<Integer, Integer>> sideInput1 =
          pipeline
              .apply("CreateSideInput1", Create.of(KV.of(1, 2), KV.of(2, 3), KV.of(3, 4)))
              .apply("ViewSideInput1", View.asMap());

      // SideInput tag id
      final String sideInputTag1 = "tag1";

      DoFn<Integer, List<Integer>> fn =
          new DoFn<Integer, List<Integer>>() {
            @ProcessElement
            public void processElement(@SideInput(sideInputTag1) Map<String, String> tag1) {}
          };

      thrown.expect(IllegalArgumentException.class);
      PCollection<List<Integer>> output =
          pipeline
              .apply("Create main input", Create.of(2))
              .apply(ParDo.of(fn).withSideInput(sideInputTag1, sideInput1));
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesSideInputs.class})
    public void testSideInputAnnotationFailedValidationMultiMapType() {

      final PCollectionView<Map<Integer, Iterable<Integer>>> sideInput1 =
          pipeline
              .apply("CreateSideInput1", Create.of(KV.of(1, 2), KV.of(1, 3), KV.of(3, 4)))
              .apply("ViewSideInput1", View.asMultimap());

      // SideInput tag id
      final String sideInputTag1 = "tag1";

      DoFn<Integer, List<Integer>> fn =
          new DoFn<Integer, List<Integer>>() {
            @ProcessElement
            public void processElement(@SideInput(sideInputTag1) Map<Integer, Integer> tag1) {}
          };

      thrown.expect(IllegalArgumentException.class);
      PCollection<List<Integer>> output =
          pipeline
              .apply("Create main input", Create.of(2))
              .apply(ParDo.of(fn).withSideInput(sideInputTag1, sideInput1));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testSideInputAnnotation() {

      final PCollectionView<List<Integer>> sideInput1 =
          pipeline
              .apply("CreateSideInput1", Create.of(2, 1, 0))
              .apply("ViewSideInput1", View.asList());

      // SideInput tag id
      final String sideInputTag1 = "tag1";

      DoFn<Integer, List<Integer>> fn =
          new DoFn<Integer, List<Integer>>() {
            @ProcessElement
            public void processElement(
                OutputReceiver<List<Integer>> r, @SideInput(sideInputTag1) List<Integer> tag1) {

              List<Integer> sideSorted = Lists.newArrayList(tag1);
              Collections.sort(sideSorted);
              r.output(sideSorted);
            }
          };

      PCollection<List<Integer>> output =
          pipeline
              .apply("Create main input", Create.of(2))
              .apply(ParDo.of(fn).withSideInput(sideInputTag1, sideInput1));

      PAssert.that(output).containsInAnyOrder(Lists.newArrayList(0, 1, 2));
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesSideInputs.class,
      UsesSideInputsWithDifferentCoders.class
    })
    public void testSideInputAnnotationWithMultipleSideInputs() {

      final List<Integer> side1Data = ImmutableList.of(2, 0);
      final PCollectionView<List<Integer>> sideInput1 =
          pipeline
              .apply("CreateSideInput1", Create.of(side1Data))
              .apply("ViewSideInput1", View.asList());

      final Integer side2Data = 5;
      final PCollectionView<Integer> sideInput2 =
          pipeline
              .apply("CreateSideInput2", Create.of(side2Data))
              .apply("ViewSideInput2", View.asSingleton());

      final List<Integer> side3Data = ImmutableList.of(1, 3);
      final PCollectionView<Iterable<Integer>> sideInput3 =
          pipeline
              .apply("CreateSideInput3", Create.of(side3Data))
              .apply("ViewSideInput3", View.asIterable());

      final List<KV<Integer, Integer>> side4Data =
          ImmutableList.of(KV.of(1, 2), KV.of(2, 3), KV.of(3, 4));
      final PCollectionView<Map<Integer, Integer>> sideInput4 =
          pipeline
              .apply("CreateSideInput4", Create.of(side4Data))
              .apply("ViewSideInput4", View.asMap());

      final List<KV<Integer, Integer>> side5Data =
          ImmutableList.of(KV.of(1, 2), KV.of(1, 3), KV.of(3, 4));
      final PCollectionView<Map<Integer, Iterable<Integer>>> sideInput5 =
          pipeline
              .apply("CreateSideInput5", Create.of(side5Data))
              .apply("ViewSideInput5", View.asMultimap());

      // SideInput tag id
      final String sideInputTag1 = "tag1";
      final String sideInputTag2 = "tag2";
      final String sideInputTag3 = "tag3";
      final String sideInputTag4 = "tag4";
      final String sideInputTag5 = "tag5";

      final TupleTag<Integer> outputTag1 = new TupleTag<>();
      final TupleTag<Integer> outputTag2 = new TupleTag<>();
      final TupleTag<Integer> outputTag3 = new TupleTag<>();
      final TupleTag<KV<Integer, Integer>> outputTag4 = new TupleTag<>();
      final TupleTag<KV<Integer, Integer>> outputTag5 = new TupleTag<>();

      DoFn<Integer, Integer> fn =
          new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElement(
                MultiOutputReceiver r,
                @SideInput(sideInputTag1) List<Integer> side1,
                @SideInput(sideInputTag2) Integer side2,
                @SideInput(sideInputTag3) Iterable<Integer> side3,
                @SideInput(sideInputTag4) Map<Integer, Integer> side4,
                @SideInput(sideInputTag5) Map<Integer, Iterable<Integer>> side5) {
              side1.forEach(i -> r.get(outputTag1).output(i));
              r.get(outputTag2).output(side2);
              side3.forEach(i -> r.get(outputTag3).output(i));
              side4.forEach((k, v) -> r.get(outputTag4).output(KV.of(k, v)));
              side5.forEach((k, v) -> v.forEach(v2 -> r.get(outputTag5).output(KV.of(k, v2))));
            }
          };

      PCollectionTuple output =
          pipeline
              .apply("Create main input", Create.of(2))
              .apply(
                  ParDo.of(fn)
                      .withSideInput(sideInputTag1, sideInput1)
                      .withSideInput(sideInputTag2, sideInput2)
                      .withSideInput(sideInputTag3, sideInput3)
                      .withSideInput(sideInputTag4, sideInput4)
                      .withSideInput(sideInputTag5, sideInput5)
                      .withOutputTags(
                          outputTag1,
                          TupleTagList.of(outputTag2)
                              .and(outputTag3)
                              .and(outputTag4)
                              .and(outputTag5)));

      output.get(outputTag1).setCoder(VarIntCoder.of());
      output.get(outputTag2).setCoder(VarIntCoder.of());
      output.get(outputTag3).setCoder(VarIntCoder.of());
      output.get(outputTag4).setCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));
      output.get(outputTag5).setCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));

      PAssert.that(output.get(outputTag1)).containsInAnyOrder(side1Data);
      PAssert.that(output.get(outputTag2)).containsInAnyOrder(side2Data);
      PAssert.that(output.get(outputTag3)).containsInAnyOrder(side3Data);
      PAssert.that(output.get(outputTag4)).containsInAnyOrder(side4Data);
      PAssert.that(output.get(outputTag5)).containsInAnyOrder(side5Data);

      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testParDoWithSideInputsIsCumulative() {

      List<Integer> inputs = Arrays.asList(3, -42, 666);

      PCollectionView<Integer> sideInput1 =
          pipeline
              .apply("CreateSideInput1", Create.of(11))
              .apply("ViewSideInput1", View.asSingleton());
      PCollectionView<Integer> sideInputUnread =
          pipeline
              .apply("CreateSideInputUnread", Create.of(-3333))
              .apply("ViewSideInputUnread", View.asSingleton());
      PCollectionView<Integer> sideInput2 =
          pipeline
              .apply("CreateSideInput2", Create.of(222))
              .apply("ViewSideInput2", View.asSingleton());

      PCollection<String> output =
          pipeline
              .apply(Create.of(inputs))
              .apply(
                  ParDo.of(new TestDoFn(Arrays.asList(sideInput1, sideInput2), Arrays.asList()))
                      .withSideInputs(sideInput1)
                      .withSideInputs(sideInputUnread)
                      .withSideInputs(sideInput2));

      PAssert.that(output)
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs).andSideInputs(11, 222));

      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testMultiOutputParDoWithSideInputs() {

      List<Integer> inputs = Arrays.asList(3, -42, 666);

      final TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
      final TupleTag<Void> additionalOutputTag = new TupleTag<Void>("output") {};

      PCollectionView<Integer> sideInput1 =
          pipeline
              .apply("CreateSideInput1", Create.of(11))
              .apply("ViewSideInput1", View.asSingleton());
      PCollectionView<Integer> sideInputUnread =
          pipeline
              .apply("CreateSideInputUnread", Create.of(-3333))
              .apply("ViewSideInputUnread", View.asSingleton());
      PCollectionView<Integer> sideInput2 =
          pipeline
              .apply("CreateSideInput2", Create.of(222))
              .apply("ViewSideInput2", View.asSingleton());

      PCollectionTuple outputs =
          pipeline
              .apply(Create.of(inputs))
              .apply(
                  ParDo.of(new TestDoFn(Arrays.asList(sideInput1, sideInput2), Arrays.asList()))
                      .withSideInputs(sideInput1)
                      .withSideInputs(sideInputUnread)
                      .withSideInputs(sideInput2)
                      .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

      PAssert.that(outputs.get(mainOutputTag))
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs).andSideInputs(11, 222));

      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testMultiOutputParDoWithSideInputsIsCumulative() {

      List<Integer> inputs = Arrays.asList(3, -42, 666);

      final TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
      final TupleTag<Void> additionalOutputTag = new TupleTag<Void>("output") {};

      PCollectionView<Integer> sideInput1 =
          pipeline
              .apply("CreateSideInput1", Create.of(11))
              .apply("ViewSideInput1", View.asSingleton());
      PCollectionView<Integer> sideInputUnread =
          pipeline
              .apply("CreateSideInputUnread", Create.of(-3333))
              .apply("ViewSideInputUnread", View.asSingleton());
      PCollectionView<Integer> sideInput2 =
          pipeline
              .apply("CreateSideInput2", Create.of(222))
              .apply("ViewSideInput2", View.asSingleton());

      PCollectionTuple outputs =
          pipeline
              .apply(Create.of(inputs))
              .apply(
                  ParDo.of(new TestDoFn(Arrays.asList(sideInput1, sideInput2), Arrays.asList()))
                      .withSideInputs(sideInput1)
                      .withSideInputs(sideInputUnread)
                      .withSideInputs(sideInput2)
                      .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

      PAssert.that(outputs.get(mainOutputTag))
          .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs).andSideInputs(11, 222));

      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testParDoReadingFromUnknownSideInput() {

      List<Integer> inputs = Arrays.asList(3, -42, 666);

      PCollectionView<Integer> sideView =
          pipeline.apply("Create3", Create.of(3)).apply(View.asSingleton());

      pipeline
          .apply("CreateMain", Create.of(inputs))
          .apply(ParDo.of(new TestDoFn(Arrays.asList(sideView), Arrays.asList())));

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("calling sideInput() with unknown view");
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesSideInputs.class})
    public void testSideInputsWithMultipleWindows() {
      // Tests that the runner can safely run a DoFn that uses side inputs
      // on an input where the element is in multiple windows. The complication is
      // that side inputs are per-window, so the runner has to make sure
      // to process each window individually.

      MutableDateTime mutableNow = Instant.now().toMutableDateTime();
      mutableNow.setMillisOfSecond(0);
      Instant now = mutableNow.toInstant();

      SlidingWindows windowFn =
          SlidingWindows.of(Duration.standardSeconds(5)).every(Duration.standardSeconds(1));
      PCollectionView<Integer> view = pipeline.apply(Create.of(1)).apply(View.asSingleton());
      PCollection<String> res =
          pipeline
              .apply(Create.timestamped(TimestampedValue.of("a", now)))
              .apply(Window.into(windowFn))
              .apply(ParDo.of(new FnWithSideInputs(view)).withSideInputs(view));

      for (int i = 0; i < 4; ++i) {
        Instant base = now.minus(Duration.standardSeconds(i));
        IntervalWindow window = new IntervalWindow(base, base.plus(Duration.standardSeconds(5)));
        PAssert.that(res).inWindow(window).containsInAnyOrder("a:1");
      }

      pipeline.run();
    }

    @Test
    public void testParDoOutputNameBasedOnDoFnWithTrimmedSuffix() {
      pipeline.enableAbandonedNodeEnforcement(false);

      PCollection<String> output = pipeline.apply(Create.of(1)).apply(ParDo.of(new TestDoFn()));
      assertThat(output.getName(), containsString("ParDo(Test)"));
    }

    @Test
    public void testParDoOutputNameBasedOnLabel() {
      pipeline.enableAbandonedNodeEnforcement(false);

      PCollection<String> output =
          pipeline.apply(Create.of(1)).apply("MyParDo", ParDo.of(new TestDoFn()));
      assertThat(output.getName(), containsString("MyParDo"));
    }

    @Test
    public void testParDoOutputNameBasedDoFnWithoutMatchingSuffix() {
      pipeline.enableAbandonedNodeEnforcement(false);

      PCollection<String> output =
          pipeline.apply(Create.of(1)).apply(ParDo.of(new StrangelyNamedDoer()));
      assertThat(output.getName(), containsString("ParDo(StrangelyNamedDoer)"));
    }

    @Test
    public void testParDoMultiNameBasedDoFnWithTrimmerSuffix() {
      assertThat(
          ParDo.of(new TaggedOutputDummyFn(null, null)).withOutputTags(null, null).getName(),
          containsString("ParMultiDo(TaggedOutputDummy)"));
    }

    @Test
    public void testParDoWithTaggedOutputName() {
      pipeline.enableAbandonedNodeEnforcement(false);

      TupleTag<String> mainOutputTag = new TupleTag<String>("main") {};
      TupleTag<String> additionalOutputTag1 = new TupleTag<String>("output1") {};
      TupleTag<String> additionalOutputTag2 = new TupleTag<String>("output2") {};
      TupleTag<String> additionalOutputTag3 = new TupleTag<String>("output3") {};
      TupleTag<String> additionalOutputTagUnwritten = new TupleTag<String>("unwrittenOutput") {};

      PCollectionTuple outputs =
          pipeline
              .apply(Create.of(Arrays.asList(3, -42, 666)))
              .setName("MyInput")
              .apply(
                  "MyParDo",
                  ParDo.of(
                          new TestDoFn(
                              Arrays.asList(),
                              Arrays.asList(
                                  additionalOutputTag1,
                                  additionalOutputTag2,
                                  additionalOutputTag3)))
                      .withOutputTags(
                          mainOutputTag,
                          TupleTagList.of(additionalOutputTag3)
                              .and(additionalOutputTag1)
                              .and(additionalOutputTagUnwritten)
                              .and(additionalOutputTag2)));

      assertEquals("MyParDo.main", outputs.get(mainOutputTag).getName());
      assertEquals("MyParDo.output1", outputs.get(additionalOutputTag1).getName());
      assertEquals("MyParDo.output2", outputs.get(additionalOutputTag2).getName());
      assertEquals("MyParDo.output3", outputs.get(additionalOutputTag3).getName());
      assertEquals("MyParDo.unwrittenOutput", outputs.get(additionalOutputTagUnwritten).getName());
    }

    @Test
    public void testMultiOutputAppliedMultipleTimesDifferentOutputs() {
      pipeline.enableAbandonedNodeEnforcement(false);
      PCollection<Long> longs = pipeline.apply(GenerateSequence.from(0));

      TupleTag<Long> mainOut = new TupleTag<>();
      final TupleTag<String> valueAsString = new TupleTag<>();
      final TupleTag<Integer> valueAsInt = new TupleTag<>();
      DoFn<Long, Long> fn =
          new DoFn<Long, Long>() {
            @ProcessElement
            public void processElement(ProcessContext cxt, @Element Long element) {
              cxt.output(cxt.element());
              cxt.output(valueAsString, Long.toString(cxt.element()));
              cxt.output(valueAsInt, element.intValue());
            }
          };

      ParDo.MultiOutput<Long, Long> parDo =
          ParDo.of(fn).withOutputTags(mainOut, TupleTagList.of(valueAsString).and(valueAsInt));
      PCollectionTuple firstApplication = longs.apply("first", parDo);
      PCollectionTuple secondApplication = longs.apply("second", parDo);
      assertThat(firstApplication, not(equalTo(secondApplication)));
      assertThat(
          firstApplication.getAll().keySet(),
          Matchers.containsInAnyOrder(mainOut, valueAsString, valueAsInt));
      assertThat(
          secondApplication.getAll().keySet(),
          Matchers.containsInAnyOrder(mainOut, valueAsString, valueAsInt));
    }

    @Test
    @Category(NeedsRunner.class)
    public void testMultiOutputChaining() {

      PCollectionTuple filters =
          pipeline.apply(Create.of(Arrays.asList(3, 4, 5, 6))).apply(new MultiFilter());
      PCollection<Integer> by2 = filters.get(MultiFilter.BY2);
      PCollection<Integer> by3 = filters.get(MultiFilter.BY3);

      // Apply additional filters to each operation.
      PCollection<Integer> by2then3 =
          by2.apply("Filter3sAgain", ParDo.of(new MultiFilter.FilterFn(3)));
      PCollection<Integer> by3then2 =
          by3.apply("Filter2sAgain", ParDo.of(new MultiFilter.FilterFn(2)));

      PAssert.that(by2then3).containsInAnyOrder(6);
      PAssert.that(by3then2).containsInAnyOrder(6);
      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testTaggedOutputUnknownCoder() throws Exception {

      PCollection<Integer> input = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));

      final TupleTag<Integer> mainOutputTag = new TupleTag<>("main");
      final TupleTag<TestDummy> additionalOutputTag = new TupleTag<>("unknownSide");
      input.apply(
          ParDo.of(new TaggedOutputDummyFn(mainOutputTag, additionalOutputTag))
              .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("Unable to return a default Coder");
      pipeline.run();
    }

    @Test
    public void testTaggedOutputUnregisteredExplicitCoder() throws Exception {
      pipeline.enableAbandonedNodeEnforcement(false);

      PCollection<Integer> input = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));

      final TupleTag<Integer> mainOutputTag = new TupleTag<>("main");
      final TupleTag<TestDummy> additionalOutputTag = new TupleTag<>("unregisteredSide");
      ParDo.MultiOutput<Integer, Integer> pardo =
          ParDo.of(new TaggedOutputDummyFn(mainOutputTag, additionalOutputTag))
              .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag));
      PCollectionTuple outputTuple = input.apply(pardo);

      outputTuple.get(additionalOutputTag).setCoder(new TestDummyCoder());

      outputTuple.get(additionalOutputTag).apply(View.asSingleton());

      assertEquals(new TestDummyCoder(), outputTuple.get(additionalOutputTag).getCoder());
      outputTuple
          .get(additionalOutputTag)
          .finishSpecifyingOutput("ParDo", input, pardo); // Check for crashes
      assertEquals(
          new TestDummyCoder(),
          outputTuple.get(additionalOutputTag).getCoder()); // Check for corruption
    }

    @Test
    @Category(NeedsRunner.class)
    public void testMainOutputUnregisteredExplicitCoder() {

      PCollection<Integer> input = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));

      final TupleTag<TestDummy> mainOutputTag = new TupleTag<>("unregisteredMain");
      final TupleTag<Integer> additionalOutputTag = new TupleTag<Integer>("additionalOutput") {};
      PCollectionTuple outputTuple =
          input.apply(
              ParDo.of(new MainOutputDummyFn(mainOutputTag, additionalOutputTag))
                  .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

      outputTuple.get(mainOutputTag).setCoder(new TestDummyCoder());

      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testMainOutputApplyTaggedOutputNoCoder() {
      // Regression test: applying a transform to the main output
      // should not cause a crash based on lack of a coder for the
      // additional output.

      final TupleTag<TestDummy> mainOutputTag = new TupleTag<>("main");
      final TupleTag<TestDummy> additionalOutputTag = new TupleTag<>("additionalOutput");
      PCollectionTuple tuple =
          pipeline
              .apply(Create.of(new TestDummy()).withCoder(TestDummyCoder.of()))
              .apply(
                  ParDo.of(
                          new DoFn<TestDummy, TestDummy>() {
                            @ProcessElement
                            public void processElement(
                                ProcessContext context, @Element TestDummy element) {
                              context.output(element);
                              context.output(additionalOutputTag, element);
                            }
                          })
                      .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

      // Before fix, tuple.get(mainOutputTag).apply(...) would indirectly trigger
      // tuple.get(additionalOutputTag).finishSpecifyingOutput(), which would crash
      // on a missing coder.
      tuple
          .get(mainOutputTag)
          .setCoder(TestDummyCoder.of())
          .apply(
              "Output1",
              ParDo.of(
                  new DoFn<TestDummy, Integer>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      context.output(1);
                    }
                  }));

      tuple.get(additionalOutputTag).setCoder(TestDummyCoder.of());

      pipeline.run();
    }

    @Test
    public void testWithOutputTagsDisplayData() {
      DoFn<String, String> fn =
          new DoFn<String, String>() {
            @ProcessElement
            public void proccessElement(ProcessContext c) {}

            @Override
            public void populateDisplayData(Builder builder) {
              builder.add(DisplayData.item("fnMetadata", "foobar"));
            }
          };

      ParDo.MultiOutput<String, String> parDo =
          ParDo.of(fn).withOutputTags(new TupleTag<>(), TupleTagList.empty());

      DisplayData displayData = DisplayData.from(parDo);
      assertThat(displayData, includesDisplayDataFor("fn", fn));
      assertThat(displayData, hasDisplayItem("fn", fn.getClass()));
    }
  }

  /** Tests for ParDo lifecycle methods. */
  @RunWith(JUnit4.class)
  public static class LifecycleTests extends SharedTestBase implements Serializable {
    @Test
    @Category(NeedsRunner.class)
    public void testParDoWithErrorInStartBatch() {
      List<Integer> inputs = Arrays.asList(3, -42, 666);

      pipeline.apply(Create.of(inputs)).apply(ParDo.of(new TestStartBatchErrorDoFn()));

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("test error in initialize");
      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testParDoWithErrorInProcessElement() {

      List<Integer> inputs = Arrays.asList(3, -42, 666);

      pipeline.apply(Create.of(inputs)).apply(ParDo.of(new TestProcessElementErrorDoFn()));

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("test error in process");
      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testParDoWithErrorInFinishBatch() {

      List<Integer> inputs = Arrays.asList(3, -42, 666);

      pipeline.apply(Create.of(inputs)).apply(ParDo.of(new TestFinishBatchErrorDoFn()));

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("test error in finalize");
      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testWindowingInStartAndFinishBundle() {

      final FixedWindows windowFn = FixedWindows.of(Duration.millis(1));
      PCollection<String> output =
          pipeline
              .apply(Create.timestamped(TimestampedValue.of("elem", new Instant(1))))
              .apply(Window.into(windowFn))
              .apply(
                  ParDo.of(
                      new DoFn<String, String>() {
                        @ProcessElement
                        public void processElement(
                            @Element String element,
                            @Timestamp Instant timestamp,
                            OutputReceiver<String> r) {
                          r.output(element);
                          System.out.println("Process: " + element + ":" + timestamp.getMillis());
                        }

                        @FinishBundle
                        public void finishBundle(FinishBundleContext c) {
                          Instant ts = new Instant(3);
                          c.output("finish", ts, windowFn.assignWindow(ts));
                          System.out.println("Finish: 3");
                        }
                      }))
              .apply(ParDo.of(new PrintingDoFn()));

      PAssert.that(output).satisfies(new Checker());

      pipeline.run();
    }
  }

  /** Tests to validate output timestamps. */
  @RunWith(JUnit4.class)
  public static class TimestampTests extends SharedTestBase implements Serializable {
    @Test
    @Category(NeedsRunner.class)
    public void testParDoOutputWithTimestamp() {

      PCollection<Integer> input = pipeline.apply(Create.of(Arrays.asList(3, 42, 6)));

      PCollection<String> output =
          input
              .apply(ParDo.of(new TestOutputTimestampDoFn<>()))
              .apply(ParDo.of(new TestShiftTimestampDoFn<>(Duration.ZERO, Duration.ZERO)))
              .apply(ParDo.of(new TestFormatTimestampDoFn<>()));

      PAssert.that(output)
          .containsInAnyOrder(
              "processing: 3, timestamp: 3",
              "processing: 42, timestamp: 42",
              "processing: 6, timestamp: 6");

      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testParDoTaggedOutputWithTimestamp() {

      PCollection<Integer> input = pipeline.apply(Create.of(Arrays.asList(3, 42, 6)));

      final TupleTag<Integer> mainOutputTag = new TupleTag<Integer>("main") {};
      final TupleTag<Integer> additionalOutputTag = new TupleTag<Integer>("additional") {};

      PCollection<String> output =
          input
              .apply(
                  ParDo.of(
                          new DoFn<Integer, Integer>() {
                            @ProcessElement
                            public void processElement(
                                @Element Integer element, MultiOutputReceiver r) {
                              r.get(additionalOutputTag)
                                  .outputWithTimestamp(element, new Instant(element.longValue()));
                            }
                          })
                      .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)))
              .get(additionalOutputTag)
              .apply(ParDo.of(new TestShiftTimestampDoFn<>(Duration.ZERO, Duration.ZERO)))
              .apply(ParDo.of(new TestFormatTimestampDoFn<>()));

      PAssert.that(output)
          .containsInAnyOrder(
              "processing: 3, timestamp: 3",
              "processing: 42, timestamp: 42",
              "processing: 6, timestamp: 6");

      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testParDoShiftTimestamp() {

      PCollection<Integer> input = pipeline.apply(Create.of(Arrays.asList(3, 42, 6)));

      PCollection<String> output =
          input
              .apply(ParDo.of(new TestOutputTimestampDoFn<>()))
              .apply(
                  ParDo.of(
                      new TestShiftTimestampDoFn<>(Duration.millis(1000), Duration.millis(-1000))))
              .apply(ParDo.of(new TestFormatTimestampDoFn<>()));

      PAssert.that(output)
          .containsInAnyOrder(
              "processing: 3, timestamp: -997",
              "processing: 42, timestamp: -958",
              "processing: 6, timestamp: -994");

      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testParDoShiftTimestampInvalid() {

      pipeline
          .apply(Create.of(Arrays.asList(3, 42, 6)))
          .apply(ParDo.of(new TestOutputTimestampDoFn<>()))
          .apply(
              ParDo.of(
                  new TestShiftTimestampDoFn<>(
                      Duration.millis(1000), // allowed skew = 1 second
                      Duration.millis(-1001))))
          .apply(ParDo.of(new TestFormatTimestampDoFn<>()));

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("Cannot output with timestamp");
      thrown.expectMessage(
          "Output timestamps must be no earlier than the timestamp of the current input");
      thrown.expectMessage("minus the allowed skew (1 second).");
      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testParDoShiftTimestampInvalidZeroAllowed() {
      pipeline
          .apply(Create.of(Arrays.asList(3, 42, 6)))
          .apply(ParDo.of(new TestOutputTimestampDoFn<>()))
          .apply(ParDo.of(new TestShiftTimestampDoFn<>(Duration.ZERO, Duration.millis(-1001))))
          .apply(ParDo.of(new TestFormatTimestampDoFn<>()));

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("Cannot output with timestamp");
      thrown.expectMessage(
          "Output timestamps must be no earlier than the timestamp of the current input");
      thrown.expectMessage("minus the allowed skew (0 milliseconds).");
      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testParDoShiftTimestampUnlimited() {
      PCollection<Long> outputs =
          pipeline
              .apply(
                  Create.of(
                      Arrays.asList(
                          0L,
                          BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis(),
                          GlobalWindow.INSTANCE.maxTimestamp().getMillis())))
              .apply("AssignTimestampToValue", ParDo.of(new TestOutputTimestampDoFn<>()))
              .apply(
                  "ReassignToMinimumTimestamp",
                  ParDo.of(
                      new DoFn<Long, Long>() {
                        @ProcessElement
                        public void reassignTimestamps(
                            ProcessContext context, @Element Long element) {
                          // Shift the latest element as far backwards in time as the model permits
                          context.outputWithTimestamp(element, BoundedWindow.TIMESTAMP_MIN_VALUE);
                        }

                        @Override
                        public Duration getAllowedTimestampSkew() {
                          return Duration.millis(Long.MAX_VALUE);
                        }
                      }));

      PAssert.that(outputs)
          .satisfies(
              input -> {
                // This element is not shifted backwards in time. It must be present in the output.
                assertThat(input, hasItem(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()));
                for (Long elem : input) {
                  // Sanity check the outputs. 0L and the end of the global window are shifted
                  // backwards in time and theoretically could be dropped.
                  assertThat(
                      elem,
                      anyOf(
                          equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()),
                          equalTo(GlobalWindow.INSTANCE.maxTimestamp().getMillis()),
                          equalTo(0L)));
                }
                return null;
              });

      pipeline.run();
    }
  }

  /** Tests to validate ParDo state. */
  @RunWith(JUnit4.class)
  public static class StateTests extends SharedTestBase implements Serializable {
    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class})
    public void testValueStateSimple() {
      final String stateId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @StateId(stateId)
            private final StateSpec<ValueState<Integer>> intState =
                StateSpecs.value(VarIntCoder.of());

            @ProcessElement
            public void processElement(
                @AlwaysFetched @StateId(stateId) ValueState<Integer> state,
                OutputReceiver<Integer> r) {
              Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
              r.output(currentValue);
              state.write(currentValue + 1);
            }
          };

      PCollection<Integer> output =
          pipeline
              .apply(Create.of(KV.of("hello", 42), KV.of("hello", 97), KV.of("hello", 84)))
              .apply(ParDo.of(fn));

      PAssert.that(output).containsInAnyOrder(0, 1, 2);
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class})
    public void testValueStateDedup() {
      final String stateId = "foo";

      DoFn<KV<Integer, Integer>, Integer> onePerKey =
          new DoFn<KV<Integer, Integer>, Integer>() {

            @StateId(stateId)
            private final StateSpec<ValueState<Integer>> seenSpec =
                StateSpecs.value(VarIntCoder.of());

            @ProcessElement
            public void processElement(
                @Element KV<Integer, Integer> element,
                @StateId(stateId) ValueState<Integer> seenState,
                OutputReceiver<Integer> r) {
              Integer seen = MoreObjects.firstNonNull(seenState.read(), 0);

              if (seen == 0) {
                seenState.write(seen + 1);
                r.output(element.getValue());
              }
            }
          };

      int numKeys = 50;
      // A big enough list that we can see some deduping
      List<KV<Integer, Integer>> input = new ArrayList<>();

      // The output should have no dupes
      Set<Integer> expectedOutput = new HashSet<>();

      for (int key = 0; key < numKeys; ++key) {
        int output = 1000 + key;
        expectedOutput.add(output);

        for (int i = 0; i < 15; ++i) {
          input.add(KV.of(key, output));
        }
      }

      Collections.shuffle(input);

      PCollection<Integer> output = pipeline.apply(Create.of(input)).apply(ParDo.of(onePerKey));

      PAssert.that(output).containsInAnyOrder(expectedOutput);
      pipeline.run();
    }

    @Test
    public void testStateNotKeyed() {
      final String stateId = "foo";

      DoFn<String, Integer> fn =
          new DoFn<String, Integer>() {

            @StateId(stateId)
            private final StateSpec<ValueState<Integer>> intState = StateSpecs.value();

            @ProcessElement
            public void processElement(
                ProcessContext c, @StateId(stateId) ValueState<Integer> state) {}
          };

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("state");
      thrown.expectMessage("KvCoder");

      pipeline.apply(Create.of("hello", "goodbye", "hello again")).apply(ParDo.of(fn));
    }

    @Test
    public void testStateNotDeterministic() {
      final String stateId = "foo";

      // DoubleCoder is not deterministic, so this should crash
      DoFn<KV<Double, String>, Integer> fn =
          new DoFn<KV<Double, String>, Integer>() {

            @StateId(stateId)
            private final StateSpec<ValueState<Integer>> intState = StateSpecs.value();

            @ProcessElement
            public void processElement(
                ProcessContext c, @StateId(stateId) ValueState<Integer> state) {}
          };

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("state");
      thrown.expectMessage("deterministic");

      pipeline
          .apply(Create.of(KV.of(1.0, "hello"), KV.of(5.4, "goodbye"), KV.of(7.2, "hello again")))
          .apply(ParDo.of(fn));
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testCoderInferenceOfList() {
      final String stateId = "foo";
      MyIntegerCoder myIntegerCoder = MyIntegerCoder.of();
      pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, myIntegerCoder);

      DoFn<KV<String, Integer>, List<MyInteger>> fn =
          new DoFn<KV<String, Integer>, List<MyInteger>>() {

            @StateId(stateId)
            private final StateSpec<ValueState<List<MyInteger>>> intState = StateSpecs.value();

            @ProcessElement
            public void processElement(
                @Element KV<String, Integer> element,
                @StateId(stateId) ValueState<List<MyInteger>> state,
                OutputReceiver<List<MyInteger>> r) {
              MyInteger myInteger = new MyInteger(element.getValue());
              List<MyInteger> currentValue = state.read();
              List<MyInteger> newValue =
                  currentValue != null
                      ? ImmutableList.<MyInteger>builder()
                          .addAll(currentValue)
                          .add(myInteger)
                          .build()
                      : Collections.singletonList(myInteger);
              r.output(newValue);
              state.write(newValue);
            }
          };

      pipeline
          .apply(Create.of(KV.of("hello", 42), KV.of("hello", 97), KV.of("hello", 84)))
          .apply(ParDo.of(fn))
          .setCoder(ListCoder.of(myIntegerCoder));

      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testValueStateFixedWindows() {
      final String stateId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @StateId(stateId)
            private final StateSpec<ValueState<Integer>> intState =
                StateSpecs.value(VarIntCoder.of());

            @ProcessElement
            public void processElement(
                @StateId(stateId) ValueState<Integer> state, OutputReceiver<Integer> r) {
              Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
              r.output(currentValue);
              state.write(currentValue + 1);
            }
          };

      IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
      IntervalWindow secondWindow = new IntervalWindow(new Instant(10), new Instant(20));

      PCollection<Integer> output =
          pipeline
              .apply(
                  Create.timestamped(
                      // first window
                      TimestampedValue.of(KV.of("hello", 7), new Instant(1)),
                      TimestampedValue.of(KV.of("hello", 14), new Instant(2)),
                      TimestampedValue.of(KV.of("hello", 21), new Instant(3)),

                      // second window
                      TimestampedValue.of(KV.of("hello", 28), new Instant(11)),
                      TimestampedValue.of(KV.of("hello", 35), new Instant(13))))
              .apply(Window.into(FixedWindows.of(Duration.millis(10))))
              .apply("Stateful ParDo", ParDo.of(fn));

      PAssert.that(output).inWindow(firstWindow).containsInAnyOrder(0, 1, 2);
      PAssert.that(output).inWindow(secondWindow).containsInAnyOrder(0, 1);
      pipeline.run();
    }

    /**
     * Tests that there is no state bleeding between adjacent stateful {@link ParDo} transforms,
     * which may (or may not) be executed in similar contexts after runner optimizations.
     */
    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class})
    public void testValueStateSameId() {
      final String stateId = "foo";

      DoFn<KV<String, Integer>, KV<String, Integer>> fn =
          new DoFn<KV<String, Integer>, KV<String, Integer>>() {

            @StateId(stateId)
            private final StateSpec<ValueState<Integer>> intState =
                StateSpecs.value(VarIntCoder.of());

            @ProcessElement
            public void processElement(
                @StateId(stateId) ValueState<Integer> state,
                OutputReceiver<KV<String, Integer>> r) {
              Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
              r.output(KV.of("sizzle", currentValue));
              state.write(currentValue + 1);
            }
          };

      DoFn<KV<String, Integer>, Integer> fn2 =
          new DoFn<KV<String, Integer>, Integer>() {

            @StateId(stateId)
            private final StateSpec<ValueState<Integer>> intState =
                StateSpecs.value(VarIntCoder.of());

            @ProcessElement
            public void processElement(
                @StateId(stateId) ValueState<Integer> state, OutputReceiver<Integer> r) {
              Integer currentValue = MoreObjects.firstNonNull(state.read(), 13);
              r.output(currentValue);
              state.write(currentValue + 13);
            }
          };

      PCollection<KV<String, Integer>> intermediate =
          pipeline
              .apply(Create.of(KV.of("hello", 42), KV.of("hello", 97), KV.of("hello", 84)))
              .apply("First stateful ParDo", ParDo.of(fn));

      PCollection<Integer> output = intermediate.apply("Second stateful ParDo", ParDo.of(fn2));

      PAssert.that(intermediate)
          .containsInAnyOrder(KV.of("sizzle", 0), KV.of("sizzle", 1), KV.of("sizzle", 2));
      PAssert.that(output).containsInAnyOrder(13, 26, 39);
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testValueStateTaggedOutput() {
      final String stateId = "foo";

      final TupleTag<Integer> evenTag = new TupleTag<Integer>() {};
      final TupleTag<Integer> oddTag = new TupleTag<Integer>() {};

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @StateId(stateId)
            private final StateSpec<ValueState<Integer>> intState =
                StateSpecs.value(VarIntCoder.of());

            @ProcessElement
            public void processElement(
                @StateId(stateId) ValueState<Integer> state, MultiOutputReceiver r) {
              Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
              if (currentValue % 2 == 0) {
                r.get(evenTag).output(currentValue);
              } else {
                r.get(oddTag).output(currentValue);
              }
              state.write(currentValue + 1);
            }
          };

      PCollectionTuple output =
          pipeline
              .apply(
                  Create.of(
                      KV.of("hello", 42),
                      KV.of("hello", 97),
                      KV.of("hello", 84),
                      KV.of("goodbye", 33),
                      KV.of("hello", 859),
                      KV.of("goodbye", 83945)))
              .apply(ParDo.of(fn).withOutputTags(evenTag, TupleTagList.of(oddTag)));

      PCollection<Integer> evens = output.get(evenTag);
      PCollection<Integer> odds = output.get(oddTag);

      // There are 0 and 2 from "hello" and just 0 from "goodbye"
      PAssert.that(evens).containsInAnyOrder(0, 2, 0);

      // There are 1 and 3 from "hello" and just "1" from "goodbye"
      PAssert.that(odds).containsInAnyOrder(1, 3, 1);
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testBagState() {
      final String stateId = "foo";

      DoFn<KV<String, Integer>, List<Integer>> fn =
          new DoFn<KV<String, Integer>, List<Integer>>() {

            @StateId(stateId)
            private final StateSpec<BagState<Integer>> bufferState =
                StateSpecs.bag(VarIntCoder.of());

            @ProcessElement
            public void processElement(
                @Element KV<String, Integer> element,
                @StateId(stateId) BagState<Integer> state,
                OutputReceiver<List<Integer>> r) {
              ReadableState<Boolean> isEmpty = state.isEmpty();
              state.add(element.getValue());
              assertFalse(isEmpty.read());
              Iterable<Integer> currentValue = state.read();
              if (Iterables.size(currentValue) >= 4) {
                // Make sure that the cached Iterable doesn't change when new elements are added.
                state.add(-1);
                assertEquals(4, Iterables.size(currentValue));
                assertEquals(5, Iterables.size(state.read()));

                List<Integer> sorted = Lists.newArrayList(currentValue);
                Collections.sort(sorted);
                r.output(sorted);
              }
            }
          };

      PCollection<List<Integer>> output =
          pipeline
              .apply(
                  Create.of(
                      KV.of("hello", 97),
                      KV.of("hello", 42),
                      KV.of("hello", 84),
                      KV.of("hello", 12)))
              .apply(ParDo.of(fn));

      PAssert.that(output).containsInAnyOrder(Lists.newArrayList(12, 42, 84, 97));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesSetState.class})
    public void testSetState() {
      final String stateId = "foo";
      final String countStateId = "count";

      DoFn<KV<String, Integer>, Set<Integer>> fn =
          new DoFn<KV<String, Integer>, Set<Integer>>() {

            @StateId(stateId)
            private final StateSpec<SetState<Integer>> setState = StateSpecs.set(VarIntCoder.of());

            @StateId(countStateId)
            private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

            @ProcessElement
            public void processElement(
                @Element KV<String, Integer> element,
                @StateId(stateId) SetState<Integer> state,
                @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                OutputReceiver<Set<Integer>> r) {
              ReadableState<Boolean> isEmpty = state.isEmpty();
              state.add(element.getValue());
              assertFalse(isEmpty.read());
              count.add(1);
              if (count.read() >= 4) {
                // Make sure that the cached Iterable doesn't change when new elements are added.
                Iterable<Integer> ints = state.read();
                state.add(-1);
                assertEquals(3, Iterables.size(ints));
                assertEquals(4, Iterables.size(state.read()));

                Set<Integer> set = Sets.newHashSet(ints);
                r.output(set);
              }
            }
          };

      PCollection<Set<Integer>> output =
          pipeline
              .apply(
                  Create.of(
                      KV.of("hello", 97),
                      KV.of("hello", 42),
                      KV.of("hello", 42),
                      KV.of("hello", 12)))
              .apply(ParDo.of(fn));

      PAssert.that(output).containsInAnyOrder(Sets.newHashSet(97, 42, 12));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesMapState.class})
    public void testMapState() {
      final String stateId = "foo";
      final String countStateId = "count";

      DoFn<KV<String, KV<String, Integer>>, KV<String, Integer>> fn =
          new DoFn<KV<String, KV<String, Integer>>, KV<String, Integer>>() {

            @StateId(stateId)
            private final StateSpec<MapState<String, Integer>> mapState =
                StateSpecs.map(StringUtf8Coder.of(), VarIntCoder.of());

            @StateId(countStateId)
            private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

            @ProcessElement
            public void processElement(
                ProcessContext c,
                @Element KV<String, KV<String, Integer>> element,
                @StateId(stateId) MapState<String, Integer> state,
                @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                OutputReceiver<KV<String, Integer>> r) {
              KV<String, Integer> value = element.getValue();
              ReadableState<Iterable<Entry<String, Integer>>> entriesView = state.entries();
              state.put(value.getKey(), value.getValue());
              count.add(1);
              if (count.read() >= 4) {
                Iterable<Map.Entry<String, Integer>> iterate = state.entries().read();
                // Make sure that the cached Iterable doesn't change when new elements are added,
                // but that cached ReadableState views of the state do change.
                state.put("BadKey", -1);
                assertEquals(3, Iterables.size(iterate));
                assertEquals(4, Iterables.size(entriesView.read()));
                assertEquals(4, Iterables.size(state.entries().read()));

                for (Map.Entry<String, Integer> entry : iterate) {
                  r.output(KV.of(entry.getKey(), entry.getValue()));
                }
              }
            }
          };

      PCollection<KV<String, Integer>> output =
          pipeline
              .apply(
                  Create.of(
                      KV.of("hello", KV.of("a", 97)), KV.of("hello", KV.of("b", 42)),
                      KV.of("hello", KV.of("b", 42)), KV.of("hello", KV.of("c", 12))))
              .apply(ParDo.of(fn));

      PAssert.that(output).containsInAnyOrder(KV.of("a", 97), KV.of("b", 42), KV.of("c", 12));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesOrderedListState.class})
    public void testOrderedListState() {
      final String stateId = "foo";

      DoFn<KV<String, TimestampedValue<String>>, Iterable<TimestampedValue<String>>> fn =
          new DoFn<KV<String, TimestampedValue<String>>, Iterable<TimestampedValue<String>>>() {

            @StateId(stateId)
            private final StateSpec<OrderedListState<String>> orderedListState =
                StateSpecs.orderedList(StringUtf8Coder.of());

            @ProcessElement
            public void processElement(
                @Element KV<String, TimestampedValue<String>> element,
                @StateId(stateId) OrderedListState<String> state) {
              state.add(element.getValue());
            }

            @OnWindowExpiration
            public void onWindowExpiration(
                @StateId(stateId) OrderedListState<String> state,
                OutputReceiver<Iterable<TimestampedValue<String>>> o) {
              Iterable<TimestampedValue<String>> strings = state.read();
              o.output(strings);
            }
          };

      PCollection<Iterable<TimestampedValue<String>>> output =
          pipeline
              .apply(
                  Create.of(
                      KV.of("hello", TimestampedValue.of("a", Instant.ofEpochMilli(97))),
                      KV.of("hello", TimestampedValue.of("b", Instant.ofEpochMilli(42))),
                      KV.of("hello", TimestampedValue.of("b", Instant.ofEpochMilli(52))),
                      KV.of("hello", TimestampedValue.of("c", Instant.ofEpochMilli(12)))))
              .apply(ParDo.of(fn));

      List<TimestampedValue<String>> expected =
          Lists.newArrayList(
              TimestampedValue.of("c", Instant.ofEpochMilli(12)),
              TimestampedValue.of("b", Instant.ofEpochMilli(42)),
              TimestampedValue.of("b", Instant.ofEpochMilli(52)),
              TimestampedValue.of("a", Instant.ofEpochMilli(97)));

      PAssert.that(output).containsInAnyOrder(expected);
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesOrderedListState.class})
    public void testOrderedListStateRangeFetch() {
      final String stateId = "foo";

      DoFn<KV<String, TimestampedValue<String>>, Iterable<TimestampedValue<String>>> fn =
          new DoFn<KV<String, TimestampedValue<String>>, Iterable<TimestampedValue<String>>>() {

            @StateId(stateId)
            private final StateSpec<OrderedListState<String>> orderedListState =
                StateSpecs.orderedList(StringUtf8Coder.of());

            @ProcessElement
            public void processElement(
                @Element KV<String, TimestampedValue<String>> element,
                @StateId(stateId) OrderedListState<String> state) {
              state.add(element.getValue());
            }

            @OnWindowExpiration
            public void onWindowExpiration(
                @StateId(stateId) OrderedListState<String> state,
                OutputReceiver<Iterable<TimestampedValue<String>>> o) {
              o.output(state.readRange(Instant.ofEpochMilli(0), Instant.ofEpochMilli(12)));
              o.output(state.readRange(Instant.ofEpochMilli(0), Instant.ofEpochMilli(13)));
              o.output(state.readRange(Instant.ofEpochMilli(13), Instant.ofEpochMilli(53)));
              o.output(state.readRange(Instant.ofEpochMilli(52), Instant.ofEpochMilli(98)));
            }
          };

      PCollection<Iterable<TimestampedValue<String>>> output =
          pipeline
              .apply(
                  Create.of(
                      KV.of("hello", TimestampedValue.of("a", Instant.ofEpochMilli(97))),
                      KV.of("hello", TimestampedValue.of("b", Instant.ofEpochMilli(42))),
                      KV.of("hello", TimestampedValue.of("b", Instant.ofEpochMilli(52))),
                      KV.of("hello", TimestampedValue.of("c", Instant.ofEpochMilli(12)))))
              .apply(ParDo.of(fn));

      List<TimestampedValue<String>> expected1 = Lists.newArrayList();

      List<TimestampedValue<String>> expected2 =
          Lists.newArrayList(TimestampedValue.of("c", Instant.ofEpochMilli(12)));

      List<TimestampedValue<String>> expected3 =
          Lists.newArrayList(
              TimestampedValue.of("b", Instant.ofEpochMilli(42)),
              TimestampedValue.of("b", Instant.ofEpochMilli(52)));

      List<TimestampedValue<String>> expected4 =
          Lists.newArrayList(
              TimestampedValue.of("b", Instant.ofEpochMilli(52)),
              TimestampedValue.of("a", Instant.ofEpochMilli(97)));

      PAssert.that(output).containsInAnyOrder(expected1, expected2, expected3, expected4);
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesOrderedListState.class})
    public void testOrderedListStateRangeDelete() {
      final String stateId = "foo";
      DoFn<KV<String, TimestampedValue<String>>, Iterable<TimestampedValue<String>>> fn =
          new DoFn<KV<String, TimestampedValue<String>>, Iterable<TimestampedValue<String>>>() {

            @StateId(stateId)
            private final StateSpec<OrderedListState<String>> orderedListState =
                StateSpecs.orderedList(StringUtf8Coder.of());

            @TimerId("timer")
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @Element KV<String, TimestampedValue<String>> element,
                @StateId(stateId) OrderedListState<String> state,
                @TimerId("timer") Timer timer) {
              state.add(element.getValue());
              timer.set(Instant.ofEpochMilli(42));
            }

            @OnTimer("timer")
            public void processTimer(
                @StateId(stateId) OrderedListState<String> state, @Timestamp Instant ts) {
              state.clearRange(Instant.EPOCH, ts.plus(1));
            }

            @OnWindowExpiration
            public void onWindowExpiration(
                @StateId(stateId) OrderedListState<String> state,
                OutputReceiver<Iterable<TimestampedValue<String>>> o) {
              o.output(state.read());
            }
          };

      PCollection<Iterable<TimestampedValue<String>>> output =
          pipeline
              .apply(
                  Create.of(
                      KV.of("hello", TimestampedValue.of("a", Instant.ofEpochMilli(97))),
                      KV.of("hello", TimestampedValue.of("b", Instant.ofEpochMilli(42))),
                      KV.of("hello", TimestampedValue.of("b", Instant.ofEpochMilli(52))),
                      KV.of("hello", TimestampedValue.of("c", Instant.ofEpochMilli(12)))))
              .apply(ParDo.of(fn));

      List<TimestampedValue<String>> expected =
          Lists.newArrayList(
              TimestampedValue.of("b", Instant.ofEpochMilli(52)),
              TimestampedValue.of("a", Instant.ofEpochMilli(97)));

      PAssert.that(output).containsInAnyOrder(expected);
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class})
    public void testCombiningState() {
      final String stateId = "foo";

      DoFn<KV<String, Double>, String> fn =
          new DoFn<KV<String, Double>, String>() {

            private static final double EPSILON = 0.0001;

            @StateId(stateId)
            private final StateSpec<CombiningState<Double, CountSum<Double>, Double>>
                combiningState = StateSpecs.combining(new Mean.CountSumCoder<Double>(), Mean.of());

            @ProcessElement
            public void processElement(
                ProcessContext c,
                @Element KV<String, Double> element,
                @StateId(stateId) CombiningState<Double, CountSum<Double>, Double> state,
                OutputReceiver<String> r) {
              state.add(element.getValue());
              Double currentValue = state.read();
              if (Math.abs(currentValue - 0.5) < EPSILON) {
                r.output("right on");
              }
            }
          };

      PCollection<String> output =
          pipeline
              .apply(Create.of(KV.of("hello", 0.3), KV.of("hello", 0.6), KV.of("hello", 0.6)))
              .apply(ParDo.of(fn));

      // There should only be one moment at which the average is exactly 0.5
      PAssert.that(output).containsInAnyOrder("right on");
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class})
    public void testCombiningStateParameterSuperclass() {
      final String stateId = "foo";

      DoFn<KV<Integer, Integer>, String> fn =
          new DoFn<KV<Integer, Integer>, String>() {
            private static final int EXPECTED_SUM = 8;

            @StateId(stateId)
            private final StateSpec<CombiningState<Integer, int[], Integer>> state =
                StateSpecs.combining(Sum.ofIntegers());

            @ProcessElement
            public void processElement(
                @Element KV<Integer, Integer> element,
                @StateId(stateId) GroupingState<Integer, Integer> state,
                OutputReceiver<String> r) {
              state.add(element.getValue());
              Integer currentValue = state.read();
              if (currentValue == EXPECTED_SUM) {
                r.output("right on");
              }
            }
          };

      PCollection<String> output =
          pipeline
              .apply(Create.of(KV.of(123, 4), KV.of(123, 7), KV.of(123, -3)))
              .apply(ParDo.of(fn));

      // There should only be one moment at which the sum is exactly 8
      PAssert.that(output).containsInAnyOrder("right on");
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesSideInputs.class})
    public void testBagStateSideInput() {

      final PCollectionView<List<Integer>> listView =
          pipeline.apply("Create list for side input", Create.of(2, 1, 0)).apply(View.asList());
      final String stateId = "foo";
      DoFn<KV<String, Integer>, List<Integer>> fn =
          new DoFn<KV<String, Integer>, List<Integer>>() {

            @StateId(stateId)
            private final StateSpec<BagState<Integer>> bufferState =
                StateSpecs.bag(VarIntCoder.of());

            @ProcessElement
            public void processElement(
                ProcessContext c,
                @Element KV<String, Integer> element,
                @StateId(stateId) BagState<Integer> state,
                OutputReceiver<List<Integer>> r) {
              state.add(element.getValue());
              Iterable<Integer> currentValue = state.read();
              if (Iterables.size(currentValue) >= 4) {
                List<Integer> sorted = Lists.newArrayList(currentValue);
                Collections.sort(sorted);
                r.output(sorted);

                List<Integer> sideSorted = Lists.newArrayList(c.sideInput(listView));
                Collections.sort(sideSorted);
                r.output(sideSorted);
              }
            }
          };

      PCollection<List<Integer>> output =
          pipeline
              .apply(
                  "Create main input",
                  Create.of(
                      KV.of("hello", 97),
                      KV.of("hello", 42),
                      KV.of("hello", 84),
                      KV.of("hello", 12)))
              .apply(ParDo.of(fn).withSideInputs(listView));

      PAssert.that(output)
          .containsInAnyOrder(Lists.newArrayList(12, 42, 84, 97), Lists.newArrayList(0, 1, 2));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesSideInputs.class})
    public void testStateSideInput() {

      // SideInput tag id
      final String sideInputTag1 = "tag1";

      Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();
      pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, myIntegerCoder);
      final PCollectionView<Integer> sideInput =
          pipeline
              .apply("CreateSideInput1", Create.of(2))
              .apply("ViewSideInput1", View.asSingleton());

      TestSimpleStatefulDoFn fn = new TestSimpleStatefulDoFn(sideInput);
      pipeline
          .apply(Create.of(KV.of(1, 2)))
          .apply(ParDo.of(fn).withSideInput(sideInputTag1, sideInput));

      pipeline.run();
    }

    private static class TestSimpleStatefulDoFn extends DoFn<KV<Integer, Integer>, Integer> {

      // SideInput tag id
      final String sideInputTag1 = "tag1";
      private final PCollectionView<Integer> view;

      final String stateId = "foo";
      Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();

      @StateId(stateId)
      private final StateSpec<BagState<MyInteger>> bufferState = StateSpecs.bag();

      private TestSimpleStatefulDoFn(PCollectionView<Integer> view) {
        this.view = view;
      }

      @ProcessElement
      public void processElem(
          ProcessContext c,
          @SideInput(sideInputTag1) Integer sideInputTag,
          @StateId(stateId) BagState<MyInteger> state) {
        state.add(new MyInteger(sideInputTag));
        c.output(sideInputTag);
      }

      @Override
      public boolean equals(@Nullable Object other) {
        return other instanceof TestSimpleStatefulDoFn;
      }

      @Override
      public int hashCode() {
        return getClass().hashCode();
      }
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      UsesRequiresTimeSortedInput.class,
      UsesStrictTimerOrdering.class
    })
    public void testRequiresTimeSortedInput() {
      // generate list long enough to rule out random shuffle in sorted order
      int numElements = 1000;
      List<Long> eventStamps =
          LongStream.range(0, numElements)
              .mapToObj(i -> numElements - i)
              .collect(Collectors.toList());
      testTimeSortedInput(numElements, pipeline.apply(Create.of(eventStamps)));
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      UsesRequiresTimeSortedInput.class,
      UsesStrictTimerOrdering.class,
      UsesTestStream.class
    })
    public void testRequiresTimeSortedInputWithTestStream() {
      // generate list long enough to rule out random shuffle in sorted order
      int numElements = 1000;
      List<Long> eventStamps =
          LongStream.range(0, numElements)
              .mapToObj(i -> numElements - i)
              .collect(Collectors.toList());
      TestStream.Builder<Long> stream = TestStream.create(VarLongCoder.of());
      for (Long stamp : eventStamps) {
        stream = stream.addElements(stamp);
      }
      testTimeSortedInput(numElements, pipeline.apply(stream.advanceWatermarkToInfinity()));
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      UsesRequiresTimeSortedInput.class,
      UsesStrictTimerOrdering.class,
      UsesTestStream.class
    })
    public void testRequiresTimeSortedInputWithLateDataAndAllowedLateness() {
      // generate list long enough to rule out random shuffle in sorted order
      int numElements = 1000;
      List<Long> eventStamps =
          LongStream.range(0, numElements)
              .mapToObj(i -> numElements - i)
              .collect(Collectors.toList());
      TestStream.Builder<Long> input = TestStream.create(VarLongCoder.of());
      for (Long stamp : eventStamps) {
        input = input.addElements(TimestampedValue.of(stamp, Instant.ofEpochMilli(stamp)));
        if (stamp == 100) {
          // advance watermark when we have 100 remaining elements
          // all the rest are going to be late elements
          input = input.advanceWatermarkTo(Instant.ofEpochMilli(stamp));
        }
      }
      testTimeSortedInput(
          numElements,
          pipeline
              .apply(input.advanceWatermarkToInfinity())
              .apply(
                  Window.<Long>into(new GlobalWindows())
                      .withAllowedLateness(Duration.millis(5000))));
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      UsesRequiresTimeSortedInput.class,
      UsesStrictTimerOrdering.class,
      UsesTestStream.class
    })
    public void testRequiresTimeSortedInputWithLateData() {
      // generate list long enough to rule out random shuffle in sorted order
      int numElements = 1000;
      List<Long> eventStamps =
          LongStream.range(0, numElements)
              .mapToObj(i -> numElements - i)
              .collect(Collectors.toList());
      TestStream.Builder<Long> input = TestStream.create(VarLongCoder.of());
      for (Long stamp : eventStamps) {
        input = input.addElements(TimestampedValue.of(stamp, Instant.ofEpochMilli(stamp)));
        if (stamp == 100) {
          // advance watermark when we have 100 remaining elements
          // all the rest are going to be late elements
          input = input.advanceWatermarkTo(Instant.ofEpochMilli(stamp));
        }
      }
      testTimeSortedInput(
          numElements - 100,
          numElements - 1,
          pipeline.apply(input.advanceWatermarkToInfinity()),
          // cannot validate exactly which data gets dropped, because that is runner dependent
          false);
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      UsesRequiresTimeSortedInput.class,
      UsesStrictTimerOrdering.class,
      UsesTestStream.class
    })
    public void testTwoRequiresTimeSortedInputWithLateData() {
      // generate list long enough to rule out random shuffle in sorted order
      int numElements = 1000;
      List<Long> eventStamps =
          LongStream.range(0, numElements)
              .mapToObj(i -> numElements - i)
              .collect(Collectors.toList());
      TestStream.Builder<Long> input = TestStream.create(VarLongCoder.of());
      for (Long stamp : eventStamps) {
        input = input.addElements(TimestampedValue.of(stamp, Instant.ofEpochMilli(stamp)));
        if (stamp == 100) {
          // advance watermark when we have 100 remaining elements
          // all the rest are going to be late elements
          input = input.advanceWatermarkTo(Instant.ofEpochMilli(stamp));
        }
      }
      // apply the sorted function for the first time
      PCollection<Long> first =
          pipeline
              .apply(input.advanceWatermarkToInfinity())
              .apply(WithTimestamps.of(e -> Instant.ofEpochMilli(e)))
              .apply(
                  "first.MapElements",
                  MapElements.into(
                          TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                      .via(e -> KV.of("", e)))
              .apply("first.ParDo", ParDo.of(timeSortedDoFn()))
              .apply(MapElements.into(TypeDescriptors.longs()).via(e -> (long) e));
      // apply the test to the already sorted outcome so that we test that we don't loose any
      // more data
      testTimeSortedInputAlreadyHavingStamps(
          numElements - 100,
          numElements - 1,
          first,
          // cannot validate exactly which data gets dropped, because that is runner dependent
          false);
    }

    private static void testTimeSortedInput(int exactNumExpectedElements, PCollection<Long> input) {
      testTimeSortedInput(exactNumExpectedElements, exactNumExpectedElements, input, true);
    }

    private static void testTimeSortedInput(
        int minNumExpectedElements,
        int maxNumExpectedElements,
        PCollection<Long> input,
        boolean validateContents) {
      testTimeSortedInputAlreadyHavingStamps(
          minNumExpectedElements,
          maxNumExpectedElements,
          input.apply(WithTimestamps.of(e -> Instant.ofEpochMilli(e))),
          validateContents);
    }

    private static void testTimeSortedInputAlreadyHavingStamps(
        int minNumExpectedElements,
        int maxNumExpectedElements,
        PCollection<Long> input,
        boolean validateContents) {

      PCollection<Integer> output =
          input
              .apply(
                  "sorted.MapElements",
                  MapElements.into(
                          TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                      .via(e -> KV.of("", e)))
              .apply("sorted.ParDo", ParDo.of(timeSortedDoFn()));
      PAssert.that(output)
          .satisfies(
              values -> {
                // validate that sum equals count, so that the whole list is made of ones
                long numElements = StreamSupport.stream(values.spliterator(), false).count();
                assertTrue(
                    "Expected at least " + minNumExpectedElements + ", got " + numElements,
                    minNumExpectedElements <= numElements);
                assertTrue(
                    "Expected at most " + maxNumExpectedElements + ", got " + numElements,
                    maxNumExpectedElements >= numElements);
                if (validateContents) {
                  assertFalse(
                      "Expected all ones in " + values,
                      StreamSupport.stream(values.spliterator(), false).anyMatch(e -> e != 1));
                }
                return null;
              });
      input.getPipeline().run();
    }

    private static DoFn<KV<String, Long>, Integer> timeSortedDoFn() {
      return new DoFn<KV<String, Long>, Integer>() {

        @StateId("last")
        private final StateSpec<ValueState<Long>> lastSpec = StateSpecs.value();

        @RequiresTimeSortedInput
        @ProcessElement
        public void process(
            @Element KV<String, Long> element,
            @StateId("last") ValueState<Long> last,
            OutputReceiver<Integer> output) {
          long lastVal = MoreObjects.firstNonNull(last.read(), element.getValue() - 1);
          last.write(element.getValue());
          output.output((int) (element.getValue() - lastVal));
        }
      };
    }
  }

  /** Tests for state coder inference behaviors. */
  @RunWith(JUnit4.class)
  public static class StateCoderInferenceTests extends SharedTestBase implements Serializable {
    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testBagStateCoderInference() {
      final String stateId = "foo";
      Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();
      pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, myIntegerCoder);

      DoFn<KV<String, Integer>, List<MyInteger>> fn =
          new DoFn<KV<String, Integer>, List<MyInteger>>() {

            @StateId(stateId)
            private final StateSpec<BagState<MyInteger>> bufferState = StateSpecs.bag();

            @ProcessElement
            public void processElement(
                @Element KV<String, Integer> element,
                @StateId(stateId) BagState<MyInteger> state,
                OutputReceiver<List<MyInteger>> r) {
              state.add(new MyInteger(element.getValue()));
              Iterable<MyInteger> currentValue = state.read();
              if (Iterables.size(currentValue) >= 4) {
                List<MyInteger> sorted = Lists.newArrayList(currentValue);
                Collections.sort(sorted);
                r.output(sorted);
              }
            }
          };

      PCollection<List<MyInteger>> output =
          pipeline
              .apply(
                  Create.of(
                      KV.of("hello", 97),
                      KV.of("hello", 42),
                      KV.of("hello", 84),
                      KV.of("hello", 12)))
              .apply(ParDo.of(fn))
              .setCoder(ListCoder.of(myIntegerCoder));

      PAssert.that(output)
          .containsInAnyOrder(
              Lists.newArrayList(
                  new MyInteger(12), new MyInteger(42),
                  new MyInteger(84), new MyInteger(97)));

      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class})
    public void testBagStateCoderInferenceFailure() throws Exception {
      final String stateId = "foo";
      Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();

      DoFn<KV<String, Integer>, List<MyInteger>> fn =
          new DoFn<KV<String, Integer>, List<MyInteger>>() {

            @StateId(stateId)
            private final StateSpec<BagState<MyInteger>> bufferState = StateSpecs.bag();

            @ProcessElement
            public void processElement(
                @Element KV<String, Integer> element,
                @StateId(stateId) BagState<MyInteger> state,
                OutputReceiver<List<MyInteger>> r) {
              state.add(new MyInteger(element.getValue()));
              Iterable<MyInteger> currentValue = state.read();
              if (Iterables.size(currentValue) >= 4) {
                List<MyInteger> sorted = Lists.newArrayList(currentValue);
                Collections.sort(sorted);
                r.output(sorted);
              }
            }
          };

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("Unable to infer a coder for BagState and no Coder was specified.");

      pipeline
          .apply(
              Create.of(
                  KV.of("hello", 97), KV.of("hello", 42), KV.of("hello", 84), KV.of("hello", 12)))
          .apply(ParDo.of(fn))
          .setCoder(ListCoder.of(myIntegerCoder));

      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesSetState.class})
    public void testSetStateCoderInference() {
      final String stateId = "foo";
      final String countStateId = "count";
      Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();
      pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, myIntegerCoder);

      DoFn<KV<String, Integer>, Set<MyInteger>> fn =
          new DoFn<KV<String, Integer>, Set<MyInteger>>() {

            @StateId(stateId)
            private final StateSpec<SetState<MyInteger>> setState = StateSpecs.set();

            @StateId(countStateId)
            private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

            @ProcessElement
            public void processElement(
                @Element KV<String, Integer> element,
                @StateId(stateId) SetState<MyInteger> state,
                @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                OutputReceiver<Set<MyInteger>> r) {
              state.add(new MyInteger(element.getValue()));
              count.add(1);
              if (count.read() >= 4) {
                Set<MyInteger> set = Sets.newHashSet(state.read());
                r.output(set);
              }
            }
          };

      PCollection<Set<MyInteger>> output =
          pipeline
              .apply(
                  Create.of(
                      KV.of("hello", 97),
                      KV.of("hello", 42),
                      KV.of("hello", 42),
                      KV.of("hello", 12)))
              .apply(ParDo.of(fn))
              .setCoder(SetCoder.of(myIntegerCoder));

      PAssert.that(output)
          .containsInAnyOrder(
              Sets.newHashSet(new MyInteger(97), new MyInteger(42), new MyInteger(12)));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesSetState.class})
    public void testSetStateCoderInferenceFailure() throws Exception {
      final String stateId = "foo";
      final String countStateId = "count";
      Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();

      DoFn<KV<String, Integer>, Set<MyInteger>> fn =
          new DoFn<KV<String, Integer>, Set<MyInteger>>() {

            @StateId(stateId)
            private final StateSpec<SetState<MyInteger>> setState = StateSpecs.set();

            @StateId(countStateId)
            private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

            @ProcessElement
            public void processElement(
                @Element KV<String, Integer> element,
                @StateId(stateId) SetState<MyInteger> state,
                @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                OutputReceiver<Set<MyInteger>> r) {
              state.add(new MyInteger(element.getValue()));
              count.add(1);
              if (count.read() >= 4) {
                Set<MyInteger> set = Sets.newHashSet(state.read());
                r.output(set);
              }
            }
          };

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("Unable to infer a coder for SetState and no Coder was specified.");

      pipeline
          .apply(
              Create.of(
                  KV.of("hello", 97), KV.of("hello", 42), KV.of("hello", 42), KV.of("hello", 12)))
          .apply(ParDo.of(fn))
          .setCoder(SetCoder.of(myIntegerCoder));

      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesMapState.class})
    public void testMapStateCoderInference() {
      final String stateId = "foo";
      final String countStateId = "count";
      Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();
      pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, myIntegerCoder);

      DoFn<KV<String, KV<String, Integer>>, KV<String, MyInteger>> fn =
          new DoFn<KV<String, KV<String, Integer>>, KV<String, MyInteger>>() {

            @StateId(stateId)
            private final StateSpec<MapState<String, MyInteger>> mapState = StateSpecs.map();

            @StateId(countStateId)
            private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

            @ProcessElement
            public void processElement(
                @Element KV<String, KV<String, Integer>> element,
                @StateId(stateId) MapState<String, MyInteger> state,
                @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                OutputReceiver<KV<String, MyInteger>> r) {
              KV<String, Integer> value = element.getValue();
              state.put(value.getKey(), new MyInteger(value.getValue()));
              count.add(1);
              if (count.read() >= 4) {
                Iterable<Map.Entry<String, MyInteger>> iterate = state.entries().read();
                for (Map.Entry<String, MyInteger> entry : iterate) {
                  r.output(KV.of(entry.getKey(), entry.getValue()));
                }
              }
            }
          };

      PCollection<KV<String, MyInteger>> output =
          pipeline
              .apply(
                  Create.of(
                      KV.of("hello", KV.of("a", 97)), KV.of("hello", KV.of("b", 42)),
                      KV.of("hello", KV.of("b", 42)), KV.of("hello", KV.of("c", 12))))
              .apply(ParDo.of(fn))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), myIntegerCoder));

      PAssert.that(output)
          .containsInAnyOrder(
              KV.of("a", new MyInteger(97)),
              KV.of("b", new MyInteger(42)),
              KV.of("c", new MyInteger(12)));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesMapState.class})
    public void testMapStateCoderInferenceFailure() throws Exception {
      final String stateId = "foo";
      final String countStateId = "count";
      Coder<MyInteger> myIntegerCoder = MyIntegerCoder.of();

      DoFn<KV<String, KV<String, Integer>>, KV<String, MyInteger>> fn =
          new DoFn<KV<String, KV<String, Integer>>, KV<String, MyInteger>>() {

            @StateId(stateId)
            private final StateSpec<MapState<String, MyInteger>> mapState = StateSpecs.map();

            @StateId(countStateId)
            private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
                StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

            @ProcessElement
            public void processElement(
                ProcessContext c,
                @Element KV<String, KV<String, Integer>> element,
                @StateId(stateId) MapState<String, MyInteger> state,
                @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
                OutputReceiver<KV<String, MyInteger>> r) {
              KV<String, Integer> value = element.getValue();
              state.put(value.getKey(), new MyInteger(value.getValue()));
              count.add(1);
              if (count.read() >= 4) {
                Iterable<Map.Entry<String, MyInteger>> iterate = state.entries().read();
                for (Map.Entry<String, MyInteger> entry : iterate) {
                  r.output(KV.of(entry.getKey(), entry.getValue()));
                }
              }
            }
          };

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("Unable to infer a coder for MapState and no Coder was specified.");

      pipeline
          .apply(
              Create.of(
                  KV.of("hello", KV.of("a", 97)), KV.of("hello", KV.of("b", 42)),
                  KV.of("hello", KV.of("b", 42)), KV.of("hello", KV.of("c", 12))))
          .apply(ParDo.of(fn))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), myIntegerCoder));

      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class})
    public void testCombiningStateCoderInference() {
      pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, MyIntegerCoder.of());

      final String stateId = "foo";

      DoFn<KV<String, Integer>, String> fn =
          new DoFn<KV<String, Integer>, String>() {
            private static final int EXPECTED_SUM = 16;

            @StateId(stateId)
            private final StateSpec<CombiningState<Integer, MyInteger, Integer>> combiningState =
                StateSpecs.combining(
                    new Combine.CombineFn<Integer, MyInteger, Integer>() {
                      @Override
                      public MyInteger createAccumulator() {
                        return new MyInteger(0);
                      }

                      @Override
                      public MyInteger addInput(MyInteger accumulator, Integer input) {
                        return new MyInteger(accumulator.getValue() + input);
                      }

                      @Override
                      public MyInteger mergeAccumulators(Iterable<MyInteger> accumulators) {
                        int newValue = 0;
                        for (MyInteger myInteger : accumulators) {
                          newValue += myInteger.getValue();
                        }
                        return new MyInteger(newValue);
                      }

                      @Override
                      public Integer extractOutput(MyInteger accumulator) {
                        return accumulator.getValue();
                      }
                    });

            @ProcessElement
            public void processElement(
                @Element KV<String, Integer> element,
                @StateId(stateId) CombiningState<Integer, MyInteger, Integer> state,
                OutputReceiver<String> r) {
              state.add(element.getValue());
              Integer currentValue = state.read();
              if (currentValue == EXPECTED_SUM) {
                r.output("right on");
              }
            }
          };

      PCollection<String> output =
          pipeline
              .apply(Create.of(KV.of("hello", 3), KV.of("hello", 6), KV.of("hello", 7)))
              .apply(ParDo.of(fn));

      // There should only be one moment at which the average is exactly 16
      PAssert.that(output).containsInAnyOrder("right on");
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class})
    public void testCombiningStateCoderInferenceFailure() throws Exception {
      final String stateId = "foo";

      DoFn<KV<String, Integer>, String> fn =
          new DoFn<KV<String, Integer>, String>() {
            private static final int EXPECTED_SUM = 16;

            @StateId(stateId)
            private final StateSpec<CombiningState<Integer, MyInteger, Integer>> combiningState =
                StateSpecs.combining(
                    new Combine.CombineFn<Integer, MyInteger, Integer>() {
                      @Override
                      public MyInteger createAccumulator() {
                        return new MyInteger(0);
                      }

                      @Override
                      public MyInteger addInput(MyInteger accumulator, Integer input) {
                        return new MyInteger(accumulator.getValue() + input);
                      }

                      @Override
                      public MyInteger mergeAccumulators(Iterable<MyInteger> accumulators) {
                        int newValue = 0;
                        for (MyInteger myInteger : accumulators) {
                          newValue += myInteger.getValue();
                        }
                        return new MyInteger(newValue);
                      }

                      @Override
                      public Integer extractOutput(MyInteger accumulator) {
                        return accumulator.getValue();
                      }
                    });

            @ProcessElement
            public void processElement(
                @Element KV<String, Integer> element,
                @StateId(stateId) CombiningState<Integer, MyInteger, Integer> state,
                OutputReceiver<String> r) {
              state.add(element.getValue());
              Integer currentValue = state.read();
              if (currentValue == EXPECTED_SUM) {
                r.output("right on");
              }
            }
          };

      thrown.expect(RuntimeException.class);
      thrown.expectMessage(
          "Unable to infer a coder for CombiningState and no Coder was specified.");

      pipeline
          .apply(Create.of(KV.of("hello", 3), KV.of("hello", 6), KV.of("hello", 7)))
          .apply(ParDo.of(fn));

      pipeline.run();
    }
  }

  /** Tests to validate ParDo timers. */
  @RunWith(JUnit4.class)
  public static class TimerTests extends SharedTestBase implements Serializable {
    @Test
    public void testTimerNotKeyed() {
      final String timerId = "foo";

      DoFn<String, Integer> fn =
          new DoFn<String, Integer>() {

            @TimerId(timerId)
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(ProcessContext c, @TimerId(timerId) Timer timer) {}

            @OnTimer(timerId)
            public void onTimer() {}
          };

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("timer");
      thrown.expectMessage("KvCoder");

      pipeline.apply(Create.of("hello", "goodbye", "hello again")).apply(ParDo.of(fn));
    }

    @Test
    public void testTimerNotDeterministic() {
      final String timerId = "foo";

      // DoubleCoder is not deterministic, so this should crash
      DoFn<KV<Double, String>, Integer> fn =
          new DoFn<KV<Double, String>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(ProcessContext c, @TimerId(timerId) Timer timer) {}

            @OnTimer(timerId)
            public void onTimer() {}
          };

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("timer");
      thrown.expectMessage("deterministic");

      pipeline
          .apply(Create.of(KV.of(1.0, "hello"), KV.of(5.4, "goodbye"), KV.of(7.2, "hello again")))
          .apply(ParDo.of(fn));
    }

    /**
     * Tests that an event time timer fires and results in supplementary output.
     *
     * <p>This test relies on two properties:
     *
     * <ol>
     *   <li>A timer that is set on time should always get a chance to fire. For this to be true,
     *       timers per-key-and-window must be delivered in order so the timer is not wiped out
     *       until the window is expired by the runner.
     *   <li>A {@link Create} transform sends its elements on time, and later advances the watermark
     *       to infinity
     * </ol>
     *
     * <p>Note that {@link TestStream} is not applicable because it requires very special runner
     * hooks and is only supported by the direct runner.
     */
    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testEventTimeTimerBounded() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Long>, Long> fn =
          new DoFn<KV<String, Long>, Long>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer, OutputReceiver<Long> r) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              r.output(3L);
            }

            @OnTimer(timerId)
            public void onTimer(TimeDomain timeDomain, OutputReceiver<Long> r) {
              if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
                r.output(42L);
              }
            }
          };

      PCollection<Long> output = pipeline.apply(Create.of(KV.of("hello", 37L))).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(3L, 42L);
      pipeline.run();
    }

    /**
     * Tests a GBK followed immediately by a {@link ParDo} that users timers. This checks a common
     * case where both GBK and the user code share a timer delivery bundle.
     */
    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testGbkFollowedByUserTimers() throws Exception {

      DoFn<KV<String, Iterable<Integer>>, Integer> fn =
          new DoFn<KV<String, Iterable<Integer>>, Integer>() {

            public static final String TIMER_ID = "foo";

            @TimerId(TIMER_ID)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(TIMER_ID) Timer timer, OutputReceiver<Integer> r) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              r.output(3);
            }

            @OnTimer(TIMER_ID)
            public void onTimer(TimeDomain timeDomain, OutputReceiver<Integer> r) {
              if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
                r.output(42);
              }
            }
          };

      PCollection<Integer> output =
          pipeline
              .apply(Create.of(KV.of("hello", 37)))
              .apply(GroupByKey.create())
              .apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(3, 42);
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testEventTimeTimerAlignBounded() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Long>, KV<Long, Instant>> fn =
          new DoFn<KV<String, Long>, KV<Long, Instant>>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @TimerId(timerId) Timer timer,
                @Timestamp Instant timestamp,
                OutputReceiver<KV<Long, Instant>> r) {
              timer.align(Duration.standardSeconds(1)).offset(Duration.millis(1)).setRelative();
              r.output(KV.of(3L, timestamp));
            }

            @OnTimer(timerId)
            public void onTimer(@Timestamp Instant timestamp, OutputReceiver<KV<Long, Instant>> r) {
              r.output(KV.of(42L, timestamp));
            }
          };

      PCollection<KV<Long, Instant>> output =
          pipeline.apply(Create.of(KV.of("hello", 37L))).apply(ParDo.of(fn));
      PAssert.that(output)
          .containsInAnyOrder(
              KV.of(3L, BoundedWindow.TIMESTAMP_MIN_VALUE),
              KV.of(42L, BoundedWindow.TIMESTAMP_MIN_VALUE.plus(1774)));
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testTimerReceivedInOriginalWindow() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, BoundedWindow> fn =
          new DoFn<KV<String, Integer>, BoundedWindow>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
            }

            @OnTimer(timerId)
            public void onTimer(BoundedWindow window, OutputReceiver<BoundedWindow> r) {
              r.output(window);
            }

            @Override
            public TypeDescriptor<BoundedWindow> getOutputTypeDescriptor() {
              return (TypeDescriptor) TypeDescriptor.of(IntervalWindow.class);
            }
          };

      SlidingWindows windowing =
          SlidingWindows.of(Duration.standardMinutes(3)).every(Duration.standardMinutes(1));
      PCollection<BoundedWindow> output =
          pipeline
              .apply(Create.timestamped(TimestampedValue.of(KV.of("hello", 24), new Instant(0L))))
              .apply(Window.into(windowing))
              .apply(ParDo.of(fn));

      PAssert.that(output)
          .containsInAnyOrder(
              new IntervalWindow(new Instant(0), Duration.standardMinutes(3)),
              new IntervalWindow(
                  new Instant(0).minus(Duration.standardMinutes(1)), Duration.standardMinutes(3)),
              new IntervalWindow(
                  new Instant(0).minus(Duration.standardMinutes(2)), Duration.standardMinutes(3)));
      pipeline.run();
    }

    /**
     * Tests that an event time timer set absolutely for the last possible moment fires and results
     * in supplementary output. The test is otherwise identical to {@link
     * #testEventTimeTimerBounded()}.
     */
    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testEventTimeTimerAbsolute() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Long>, Long> fn =
          new DoFn<KV<String, Long>, Long>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @TimerId(timerId) Timer timer, BoundedWindow window, OutputReceiver<Long> r) {
              timer.set(window.maxTimestamp());
              r.output(3L);
            }

            @OnTimer(timerId)
            public void onTimer(OutputReceiver<Long> r) {
              r.output(42L);
            }
          };

      PCollection<Long> output = pipeline.apply(Create.of(KV.of("hello", 37L))).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(3L, 42L);
      pipeline.run();
    }

    @Ignore(
        "https://issues.apache.org/jira/browse/BEAM-2791, "
            + "https://issues.apache.org/jira/browse/BEAM-2535")
    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesTimersInParDo.class})
    public void testEventTimeTimerLoop() {
      final String stateId = "count";
      final String timerId = "timer";
      final int loopCount = 5;

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec loopSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @StateId(stateId)
            private final StateSpec<ValueState<Integer>> countSpec = StateSpecs.value();

            @ProcessElement
            public void processElement(
                @StateId(stateId) ValueState<Integer> countState,
                @TimerId(timerId) Timer loopTimer) {
              loopTimer.offset(Duration.millis(1)).setRelative();
            }

            @OnTimer(timerId)
            public void onLoopTimer(
                @StateId(stateId) ValueState<Integer> countState,
                @TimerId(timerId) Timer loopTimer,
                OutputReceiver<Integer> r) {
              int count = MoreObjects.firstNonNull(countState.read(), 0);
              if (count < loopCount) {
                r.output(count);
                countState.write(count + 1);
                loopTimer.offset(Duration.millis(1)).setRelative();
              }
            }
          };

      PCollection<Integer> output =
          pipeline.apply(Create.of(KV.of("hello", 42))).apply(ParDo.of(fn));

      PAssert.that(output).containsInAnyOrder(0, 1, 2, 3, 4);
      pipeline.run();
    }

    /**
     * Tests that event time timers for multiple keys both fire. This particularly exercises
     * implementations that may GC in ways not simply governed by the watermark.
     */
    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testEventTimeTimerMultipleKeys() throws Exception {
      final String timerId = "foo";
      final String stateId = "sizzle";

      final int offset = 5000;
      final long timerOutput = 4093;

      DoFn<KV<String, Long>, KV<String, Long>> fn =
          new DoFn<KV<String, Long>, KV<String, Long>>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @StateId(stateId)
            private final StateSpec<ValueState<String>> stateSpec =
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
            public void onTimer(
                @StateId(stateId) ValueState<String> state, OutputReceiver<KV<String, Long>> r) {
              r.output(KV.of(state.read(), timerOutput));
            }
          };

      // Enough keys that we exercise interesting code paths
      int numKeys = 50;
      List<KV<String, Long>> input = new ArrayList<>();
      List<KV<String, Long>> expectedOutput = new ArrayList<>();

      for (Long key = 0L; key < numKeys; ++key) {
        // Each key should have just one final output at GC time
        expectedOutput.add(KV.of(key.toString(), timerOutput));

        for (long i = 0; i < 15; ++i) {
          // Each input should be output with the offset added
          input.add(KV.of(key.toString(), i));
          expectedOutput.add(KV.of(key.toString(), i + offset));
        }
      }

      Collections.shuffle(input);

      PCollection<KV<String, Long>> output = pipeline.apply(Create.of(input)).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(expectedOutput);
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testAbsoluteProcessingTimeTimerRejected() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer) {
              try {
                timer.set(new Instant(0));
                fail("Should have failed due to processing time with absolute timer.");
              } catch (RuntimeException e) {
                String message = e.getMessage();
                List<String> expectedSubstrings =
                    Arrays.asList("relative timers", "processing time");
                expectedSubstrings.forEach(
                    str ->
                        Preconditions.checkState(
                            message.contains(str),
                            "Pipeline didn't fail with the expected strings: %s",
                            expectedSubstrings));
              }
            }

            @OnTimer(timerId)
            public void onTimer() {}
          };

      pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testOutOfBoundsEventTimeTimer() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                ProcessContext context, BoundedWindow window, @TimerId(timerId) Timer timer) {
              try {
                timer.set(window.maxTimestamp().plus(1L));
                fail("Should have failed due to out-of-bounds timer.");
              } catch (RuntimeException e) {
                String message = e.getMessage();

                // Make sure these words are contained in error messages for
                // both SimpleDoFnRunner and FnApiDoFnRunner (portability)
                List<String> expectedSubstrings = Arrays.asList("timer", "expiration");
                expectedSubstrings.forEach(
                    str ->
                        Preconditions.checkState(
                            message.contains(str),
                            "Pipeline didn't fail with the expected strings: %s",
                            expectedSubstrings));
              }
            }

            @OnTimer(timerId)
            public void onTimer() {}
          };

      pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testOutputTimestampDefaultBounded() throws Exception {
      runTestOutputTimestampDefault(false);
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesUnboundedPCollections.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testOutputTimestampDefaultUnbounded() throws Exception {
      runTestOutputTimestampDefault(true);
    }

    public void runTestOutputTimestampDefault(boolean useStreaming) throws Exception {
      final String timerId = "foo";
      DoFn<KV<String, Long>, Long> fn1 =
          new DoFn<KV<String, Long>, Long>() {

            @TimerId(timerId)
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @TimerId(timerId) Timer timer, @Timestamp Instant timestamp) {
              timer
                  .withOutputTimestamp(timestamp.plus(Duration.millis(5)))
                  .set(timestamp.plus(Duration.millis(10)));
            }

            @OnTimer(timerId)
            public void onTimer(@Timestamp Instant timestamp, OutputReceiver<Long> o) {
              o.output(timestamp.getMillis());
            }
          };

      PCollection<Long> output =
          pipeline
              .apply(Create.timestamped(TimestampedValue.of(KV.of("hello", 1L), new Instant(3))))
              .setIsBoundedInternal(useStreaming ? IsBounded.UNBOUNDED : IsBounded.BOUNDED)
              .apply("first", ParDo.of(fn1));

      PAssert.that(output).containsInAnyOrder(new Instant(8).getMillis()); // result output
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testOutOfBoundsEventTimeTimerHold() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                ProcessContext context, BoundedWindow window, @TimerId(timerId) Timer timer) {
              try {
                timer
                    .withOutputTimestamp(window.maxTimestamp().plus(1L))
                    .set(window.maxTimestamp());
                fail("Should have failed due to out-of-bounds timer.");
              } catch (RuntimeException e) {
                String message = e.getMessage();
                List<String> expectedSubstrings =
                    // Make sure these words are contained in error messages for
                    // both SimpleDoFnRunner and FnApiDoFnRunner (portability)
                    Arrays.asList("timer", "output timestamp");
                expectedSubstrings.forEach(
                    str ->
                        Preconditions.checkState(
                            message.contains(str),
                            "Pipeline didn't fail with the expected strings: %s",
                            expectedSubstrings));
              }
            }

            @OnTimer(timerId)
            public void onTimer() {}
          };

      pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testOutOfBoundsProcessingTimeTimerHold() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

            @ProcessElement
            public void processElement(
                ProcessContext context, BoundedWindow window, @TimerId(timerId) Timer timer) {
              try {
                timer
                    .withOutputTimestamp(window.maxTimestamp().plus(1L))
                    .offset(Duration.standardSeconds(1))
                    .setRelative();
                fail("Should have failed due to processing time with absolute timer.");
              } catch (RuntimeException e) {
                String message = e.getMessage();
                List<String> expectedSubstrings =
                    // Make sure these words are contained in error messages for
                    // both SimpleDoFnRunner and FnApiDoFnRunner (portability)
                    Arrays.asList("timer", "output timestamp");
                expectedSubstrings.forEach(
                    str ->
                        Preconditions.checkState(
                            message.contains(str),
                            "Pipeline didn't fail with the expected strings: %s",
                            expectedSubstrings));
              }
            }

            @OnTimer(timerId)
            public void onTimer() {}
          };

      pipeline.apply(Create.of(KV.of("hello", 37))).apply(ParDo.of(fn));
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesTimersInParDo.class, UsesTestStreamWithProcessingTime.class})
    public void testSimpleProcessingTimerTimer() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer, OutputReceiver<Integer> r) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              r.output(3);
            }

            @OnTimer(timerId)
            public void onTimer(TimeDomain timeDomain, OutputReceiver<Integer> r) {
              if (timeDomain.equals(TimeDomain.PROCESSING_TIME)) {
                r.output(42);
              }
            }
          };

      TestStream<KV<String, Integer>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
              .addElements(KV.of("hello", 37))
              .advanceProcessingTime(
                  Duration.millis(DateTimeUtils.currentTimeMillis())
                      .plus(Duration.standardSeconds(60)))
              .advanceWatermarkToInfinity();

      PCollection<Integer> output = pipeline.apply(stream).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(3, 42);
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesTimersInParDo.class, UsesTestStream.class})
    public void testEventTimeTimerUnbounded() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer, OutputReceiver<Integer> r) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              r.output(3);
            }

            @OnTimer(timerId)
            public void onTimer(OutputReceiver<Integer> r) {
              r.output(42);
            }
          };

      TestStream<KV<String, Integer>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
              .advanceWatermarkTo(new Instant(0))
              .addElements(KV.of("hello", 37))
              .advanceWatermarkTo(new Instant(0).plus(Duration.standardSeconds(1)))
              .advanceWatermarkToInfinity();

      PCollection<Integer> output = pipeline.apply(stream).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(3, 42);
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesTimersInParDo.class, UsesTestStream.class})
    public void testEventTimeTimerAlignUnbounded() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, KV<Integer, Instant>> fn =
          new DoFn<KV<String, Integer>, KV<Integer, Instant>>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @TimerId(timerId) Timer timer,
                @Timestamp Instant timestamp,
                OutputReceiver<KV<Integer, Instant>> r) {
              timer
                  .align(Duration.standardMinutes(1))
                  .offset(Duration.standardSeconds(1))
                  .setRelative();
              r.output(KV.of(3, timestamp));
            }

            @OnTimer(timerId)
            public void onTimer(
                @Timestamp Instant timestamp, OutputReceiver<KV<Integer, Instant>> r) {
              r.output(KV.of(42, timestamp));
            }
          };

      TestStream<KV<String, Integer>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
              .advanceWatermarkTo(new Instant(0).plus(Duration.standardSeconds(5)))
              .addElements(KV.of("hello", 37))
              .advanceWatermarkTo(new Instant(0).plus(Duration.standardMinutes(1)))
              .advanceWatermarkToInfinity();

      PCollection<KV<Integer, Instant>> output = pipeline.apply(stream).apply(ParDo.of(fn));
      PAssert.that(output)
          .containsInAnyOrder(
              KV.of(3, new Instant(0).plus(Duration.standardSeconds(5))),
              KV.of(
                  42,
                  new Instant(
                      Duration.standardMinutes(1).minus(Duration.standardSeconds(1)).getMillis())));
      pipeline.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesTimersInParDo.class, UsesTestStream.class})
    public void testEventTimeTimerAlignAfterGcTimeUnbounded() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, KV<Integer, Instant>> fn =
          new DoFn<KV<String, Integer>, KV<Integer, Instant>>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(ProcessContext context, @TimerId(timerId) Timer timer) {
              // This aligned time will exceed the END_OF_GLOBAL_WINDOW
              timer.align(Duration.standardDays(1)).setRelative();
              context.output(KV.of(3, context.timestamp()));
            }

            @OnTimer(timerId)
            public void onTimer(
                @Timestamp Instant timestamp, OutputReceiver<KV<Integer, Instant>> r) {
              r.output(KV.of(42, timestamp));
            }
          };

      TestStream<KV<String, Integer>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
              // See GlobalWindow,
              // END_OF_GLOBAL_WINDOW is TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(1))
              .advanceWatermarkTo(GlobalWindow.INSTANCE.maxTimestamp())
              .addElements(KV.of("hello", 37))
              .advanceWatermarkToInfinity();

      PCollection<KV<Integer, Instant>> output = pipeline.apply(stream).apply(ParDo.of(fn));
      PAssert.that(output)
          .containsInAnyOrder(
              KV.of(3, GlobalWindow.INSTANCE.maxTimestamp()),
              KV.of(42, GlobalWindow.INSTANCE.maxTimestamp()));
      pipeline.run();
    }

    /**
     * A test makes sure that a processing time timer should reset rather than creating duplicate
     * timers when a "set" method is called on it before it goes off.
     */
    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      UsesTestStreamWithProcessingTime.class
    })
    public void testProcessingTimeTimerCanBeReset() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, String>, String> fn =
          new DoFn<KV<String, String>, String>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

            @ProcessElement
            public void processElement(ProcessContext context, @TimerId(timerId) Timer timer) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              context.output(context.element().getValue());
            }

            @OnTimer(timerId)
            public void onTimer(OutputReceiver<String> r) {
              r.output("timer_output");
            }
          };

      TestStream<KV<String, String>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .addElements(KV.of("key", "input1"))
              .addElements(KV.of("key", "input2"))
              .advanceProcessingTime(
                  Duration.millis(DateTimeUtils.currentTimeMillis())
                      .plus(Duration.standardSeconds(60)))
              .advanceWatermarkToInfinity();

      PCollection<String> output = pipeline.apply(stream).apply(ParDo.of(fn));
      // Timer "foo" is set twice because input1 and input 2 are outputted. However, only one
      // "timer_output" is outputted. Therefore, the timer is overwritten.
      PAssert.that(output).containsInAnyOrder("input1", "input2", "timer_output");
      pipeline.run();
    }

    /**
     * A test makes sure that an event time timer should reset rather than creating duplicate timers
     * when a "set" method is called on it before it goes off.
     */
    @Test
    @Category({ValidatesRunner.class, UsesTimersInParDo.class, UsesTestStream.class})
    public void testEventTimeTimerCanBeReset() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, String>, String> fn =
          new DoFn<KV<String, String>, String>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(ProcessContext context, @TimerId(timerId) Timer timer) {
              timer.offset(Duration.standardSeconds(1)).setRelative();
              context.output(context.element().getValue());
            }

            @OnTimer(timerId)
            public void onTimer(OutputReceiver<String> r) {
              r.output("timer_output");
            }
          };

      TestStream<KV<String, String>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .advanceWatermarkTo(new Instant(0))
              .addElements(KV.of("hello", "input1"))
              .addElements(KV.of("hello", "input2"))
              .advanceWatermarkToInfinity();

      PCollection<String> output = pipeline.apply(stream).apply(ParDo.of(fn));
      // Timer "foo" is set twice because input1 and input 2 are outputted. However, only one
      // "timer_output" is outputted. Therefore, the timer is overwritten.
      PAssert.that(output).containsInAnyOrder("input1", "input2", "timer_output");
      pipeline.run();
    }

    /** A test makes sure that an event time timers are correctly ordered. */
    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      UsesTestStream.class,
      UsesStatefulParDo.class,
      UsesStrictTimerOrdering.class
    })
    public void testEventTimeTimerOrdering() throws Exception {
      final int numTestElements = 100;
      final Instant now = new Instant(1500000000000L);
      TestStream.Builder<KV<String, String>> builder =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .advanceWatermarkTo(new Instant(0));

      for (int i = 0; i < numTestElements; i++) {
        builder =
            builder.addElements(TimestampedValue.of(KV.of("dummy", "" + i), now.plus(i * 1000)));
        if ((i + 1) % 10 == 0) {
          builder = builder.advanceWatermarkTo(now.plus((i + 1) * 1000));
        }
      }

      testEventTimeTimerOrderingWithInputPTransform(
          now, numTestElements, builder.advanceWatermarkToInfinity(), IsBounded.BOUNDED);
    }

    /** A test makes sure that an event time timers are correctly ordered using Create transform. */
    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      UsesStatefulParDo.class,
      UsesStrictTimerOrdering.class
    })
    public void testEventTimeTimerOrderingWithCreateBounded() throws Exception {
      final int numTestElements = 100;
      final Instant now = new Instant(1500000000000L);

      List<TimestampedValue<KV<String, String>>> elements = new ArrayList<>();
      for (int i = 0; i < numTestElements; i++) {
        elements.add(TimestampedValue.of(KV.of("dummy", "" + i), now.plus(i * 1000)));
      }

      testEventTimeTimerOrderingWithInputPTransform(
          now, numTestElements, Create.timestamped(elements), IsBounded.BOUNDED);
    }

    /**
     * A test makes sure that an event time timers are correctly ordered using Create transform
     * unbounded.
     */
    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      UsesStatefulParDo.class,
      UsesUnboundedPCollections.class,
      UsesStrictTimerOrdering.class
    })
    public void testEventTimeTimerOrderingWithCreateUnbounded() throws Exception {
      final int numTestElements = 100;
      final Instant now = new Instant(1500000000000L);

      List<TimestampedValue<KV<String, String>>> elements = new ArrayList<>();
      for (int i = 0; i < numTestElements; i++) {
        elements.add(TimestampedValue.of(KV.of("dummy", "" + i), now.plus(i * 1000)));
      }

      testEventTimeTimerOrderingWithInputPTransform(
          now, numTestElements, Create.timestamped(elements), IsBounded.UNBOUNDED);
    }

    private void testEventTimeTimerOrderingWithInputPTransform(
        Instant now,
        int numTestElements,
        PTransform<PBegin, PCollection<KV<String, String>>> transform,
        IsBounded isBounded)
        throws Exception {

      final String timerIdBagAppend = "append";
      final String timerIdGc = "gc";
      final String bag = "bag";
      final String minTimestamp = "minTs";
      final Instant gcTimerStamp = now.plus((numTestElements + 1) * 1000);

      DoFn<KV<String, String>, String> fn =
          new DoFn<KV<String, String>, String>() {

            @TimerId(timerIdBagAppend)
            private final TimerSpec appendSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @TimerId(timerIdGc)
            private final TimerSpec gcSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @StateId(bag)
            private final StateSpec<BagState<TimestampedValue<String>>> bagStateSpec =
                StateSpecs.bag();

            @StateId(minTimestamp)
            private final StateSpec<ValueState<Instant>> minTimestampSpec = StateSpecs.value();

            @ProcessElement
            public void processElement(
                ProcessContext context,
                @TimerId(timerIdBagAppend) Timer bagTimer,
                @TimerId(timerIdGc) Timer gcTimer,
                @StateId(bag) BagState<TimestampedValue<String>> bagState,
                @StateId(minTimestamp) ValueState<Instant> minStampState) {

              Instant currentMinStamp =
                  MoreObjects.firstNonNull(minStampState.read(), BoundedWindow.TIMESTAMP_MAX_VALUE);
              if (currentMinStamp.equals(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
                gcTimer.set(gcTimerStamp);
              }
              if (currentMinStamp.isAfter(context.timestamp())) {
                minStampState.write(context.timestamp());
                bagTimer.set(context.timestamp());
              }
              bagState.add(TimestampedValue.of(context.element().getValue(), context.timestamp()));
            }

            @OnTimer(timerIdBagAppend)
            public void onTimer(
                OnTimerContext context,
                @TimerId(timerIdBagAppend) Timer timer,
                @StateId(bag) BagState<TimestampedValue<String>> bagState) {

              List<TimestampedValue<String>> flush = new ArrayList<>();
              Instant flushTime = context.timestamp();
              for (TimestampedValue<String> val : bagState.read()) {
                if (!val.getTimestamp().isAfter(flushTime)) {
                  flush.add(val);
                }
              }
              flush.sort(Comparator.comparing(TimestampedValue::getTimestamp));
              context.output(
                  Joiner.on(":").join(flush.stream().map(TimestampedValue::getValue).iterator()));
              Instant newMinStamp = flushTime.plus(1000);
              if (flush.size() < numTestElements) {
                timer.set(newMinStamp);
              }
            }

            @OnTimer(timerIdGc)
            public void onTimer(
                OnTimerContext context, @StateId(bag) BagState<TimestampedValue<String>> bagState) {

              String output =
                  Joiner.on(":")
                          .join(
                              StreamSupport.stream(bagState.read().spliterator(), false)
                                  .sorted(Comparator.comparing(TimestampedValue::getTimestamp))
                                  .map(TimestampedValue::getValue)
                                  .iterator())
                      + ":cleanup";
              context.output(output);
              bagState.clear();
            }
          };

      PCollection<String> output =
          pipeline.apply(transform).setIsBoundedInternal(isBounded).apply(ParDo.of(fn));
      List<String> expected =
          IntStream.rangeClosed(0, numTestElements)
              .mapToObj(expandFn(numTestElements))
              .collect(Collectors.toList());
      PAssert.that(output).containsInAnyOrder(expected);
      pipeline.run();
    }

    private IntFunction<String> expandFn(int numTestElements) {
      return i ->
          Joiner.on(":")
                  .join(
                      IntStream.rangeClosed(0, Math.min(numTestElements - 1, i))
                          .mapToObj(String::valueOf)
                          .iterator())
              + (i == numTestElements ? ":cleanup" : "");
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testPipelineOptionsParameterOnTimer() {
      final String timerId = "thisTimer";

      PCollection<String> results =
          pipeline
              .apply(Create.of(KV.of(0L, 0L)))
              .apply(
                  ParDo.of(
                      new DoFn<KV<Long, Long>, String>() {
                        @TimerId(timerId)
                        private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                        @ProcessElement
                        public void process(
                            ProcessContext c, BoundedWindow w, @TimerId(timerId) Timer timer) {
                          timer.set(w.maxTimestamp());
                        }

                        @OnTimer(timerId)
                        public void onTimer(OutputReceiver<String> r, PipelineOptions options) {
                          r.output(options.as(MyOptions.class).getFakeOption());
                        }
                      }));

      String testOptionValue = "not fake anymore";
      pipeline.getOptions().as(MyOptions.class).setFakeOption(testOptionValue);
      PAssert.that(results).containsInAnyOrder("not fake anymore");

      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesTimersInParDo.class, UsesTestStream.class})
    public void duplicateTimerSetting() {
      TestStream<KV<String, String>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .addElements(KV.of("key1", "v1"))
              .advanceWatermarkToInfinity();

      PCollection<String> result = pipeline.apply(stream).apply(ParDo.of(new TwoTimerDoFn()));
      PAssert.that(result).containsInAnyOrder("It works");

      pipeline.run().waitUntilFinish();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      UsesTestStream.class,
      UsesStrictTimerOrdering.class
    })
    public void testTwoTimersSettingEachOther() {
      Instant now = new Instant(1500000000000L);
      Instant end = now.plus(100);
      TestStream<KV<Void, Void>> input =
          TestStream.create(KvCoder.of(VoidCoder.of(), VoidCoder.of()))
              .addElements(KV.of(null, null))
              .advanceWatermarkToInfinity();
      pipeline.apply(TwoTimerTest.of(now, end, input, IsBounded.BOUNDED));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesTimersInParDo.class, UsesStrictTimerOrdering.class})
    public void testTwoTimersSettingEachOtherWithCreateAsInputBounded() {
      Instant now = new Instant(1500000000000L);
      Instant end = now.plus(100);
      pipeline.apply(TwoTimerTest.of(now, end, Create.of(KV.of(null, null)), IsBounded.BOUNDED));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesTimersInParDo.class, UsesStrictTimerOrdering.class})
    public void testTwoTimersSettingEachOtherWithCreateAsInputUnbounded() {
      Instant now = new Instant(1500000000000L);
      Instant end = now.plus(100);
      pipeline.apply(TwoTimerTest.of(now, end, Create.of(KV.of(null, null)), IsBounded.UNBOUNDED));
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      UsesTimersInParDo.class,
      UsesTestStream.class,
    })
    public void testOutputTimestamp() {
      final String timerId = "bar";
      DoFn<KV<String, Long>, KV<String, Long>> fn1 =
          new DoFn<KV<String, Long>, KV<String, Long>>() {

            @TimerId(timerId)
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @TimerId(timerId) Timer timer,
                @Timestamp Instant timestamp,
                OutputReceiver<KV<String, Long>> o) {
              timer
                  .withOutputTimestamp(timestamp.plus(Duration.millis(5)))
                  .set(timestamp.plus(Duration.millis(10)));
              // Output a message. This will cause the next DoFn to set a timer as well.
              o.output(KV.of("foo", 100L));
            }

            @OnTimer(timerId)
            public void onTimer(OnTimerContext c, BoundedWindow w) {}
          };

      DoFn<KV<String, Long>, Long> fn2 =
          new DoFn<KV<String, Long>, Long>() {

            @TimerId(timerId)
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @StateId("timerFired")
            final StateSpec<ValueState<Boolean>> timerFiredState = StateSpecs.value();

            @ProcessElement
            public void processElement(
                @TimerId(timerId) Timer timer,
                @Timestamp Instant timestamp,
                @StateId("timerFired") ValueState<Boolean> timerFiredState) {
              Boolean timerFired = timerFiredState.read();
              assertTrue(timerFired == null || !timerFired);
              // Set a timer to 8. This is earlier than the previous DoFn's timer, but after the
              // previous
              // DoFn timer's watermark hold. This timer should not fire until the previous timer
              // fires and removes
              // the watermark hold.
              timer.set(timestamp.plus(Duration.millis(8)));
            }

            @OnTimer(timerId)
            public void onTimer(
                @StateId("timerFired") ValueState<Boolean> timerFiredState,
                OutputReceiver<Long> o) {
              timerFiredState.write(true);
              o.output(100L);
            }
          };

      TestStream<KV<String, Long>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
              .advanceWatermarkTo(new Instant(0))
              // Cause fn2 to set a timer.
              .addElements(KV.of("key", 1L))
              // Normally this would case fn2's timer to expire, but it shouldn't here because of
              // the output timestamp.
              .advanceWatermarkTo(new Instant(9))
              // If the timer fired, then this would case fn2 to fail with an assertion error.
              .addElements(KV.of("key", 1L))
              .advanceWatermarkToInfinity();
      PCollection<Long> output =
          pipeline.apply(stream).apply("first", ParDo.of(fn1)).apply("second", ParDo.of(fn2));

      PAssert.that(output).containsInAnyOrder(100L); // result output
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      UsesTimersInParDo.class,
      UsesTestStreamWithProcessingTime.class,
      UsesTestStreamWithOutputTimestamp.class
    })
    public void testOutputTimestampWithProcessingTime() {
      final String timerId = "foo";
      DoFn<KV<String, Integer>, KV<String, Integer>> fn1 =
          new DoFn<KV<String, Integer>, KV<String, Integer>>() {

            @TimerId(timerId)
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

            @ProcessElement
            public void processElement(
                @TimerId(timerId) Timer timer,
                @Timestamp Instant timestamp,
                OutputReceiver<KV<String, Integer>> o) {
              timer
                  .withOutputTimestamp(timestamp.plus(Duration.standardSeconds(5)))
                  .offset(Duration.standardSeconds(10))
                  .setRelative();
              // Output a message. This will cause the next DoFn to set a timer as well.
              o.output(KV.of("foo", 100));
            }

            @OnTimer(timerId)
            public void onTimer(OnTimerContext c, BoundedWindow w) {}
          };

      DoFn<KV<String, Integer>, Integer> fn2 =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @StateId("timerFired")
            final StateSpec<ValueState<Boolean>> timerFiredState = StateSpecs.value();

            @ProcessElement
            public void processElement(
                @TimerId(timerId) Timer timer,
                @StateId("timerFired") ValueState<Boolean> timerFiredState) {
              Boolean timerFired = timerFiredState.read();
              assertTrue(timerFired == null || !timerFired);
              // Set a timer to 8. This is earlier than the previous DoFn's timer, but after the
              // previous
              // DoFn timer's watermark hold. This timer should not fire until the previous timer
              // fires and removes
              // the watermark hold.
              timer.set(new Instant(8));
            }

            @OnTimer(timerId)
            public void onTimer(
                @StateId("timerFired") ValueState<Boolean> timerFiredState,
                OutputReceiver<Integer> o) {
              timerFiredState.write(true);
              o.output(100);
            }
          };

      TestStream<KV<String, Integer>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
              .advanceProcessingTime(Duration.standardSeconds(1))
              // Cause fn2 to set a timer.
              .addElements(KV.of("key", 1))
              // Normally this would case fn2's timer to expire, but it shouldn't here because of
              // the output timestamp.
              .advanceProcessingTime(Duration.standardSeconds(9))
              .advanceWatermarkTo(new Instant(11))
              // If the timer fired, then this would case fn2 to fail with an assertion error.
              .addElements(KV.of("key", 1))
              .advanceProcessingTime(Duration.standardSeconds(100))
              .advanceWatermarkToInfinity();
      PCollection<Integer> output =
          pipeline.apply(stream).apply("first", ParDo.of(fn1)).apply("second", ParDo.of(fn2));

      PAssert.that(output).containsInAnyOrder(100); // result output
      pipeline.run();
    }

    private static class TwoTimerTest extends PTransform<PBegin, PDone> {

      private static PTransform<PBegin, PDone> of(
          Instant start,
          Instant end,
          PTransform<PBegin, PCollection<KV<Void, Void>>> input,
          IsBounded isBounded) {
        return new TwoTimerTest(start, end, input, isBounded);
      }

      private final Instant start;
      private final Instant end;
      private final IsBounded isBounded;
      private final transient PTransform<PBegin, PCollection<KV<Void, Void>>> inputPTransform;

      public TwoTimerTest(
          Instant start,
          Instant end,
          PTransform<PBegin, PCollection<KV<Void, Void>>> input,
          IsBounded isBounded) {
        this.start = start;
        this.end = end;
        this.isBounded = isBounded;
        this.inputPTransform = input;
      }

      @Override
      public PDone expand(PBegin input) {

        final String timerName1 = "t1";
        final String timerName2 = "t2";
        final String countStateName = "count";
        PCollection<String> result =
            input
                .apply(inputPTransform)
                .setIsBoundedInternal(isBounded)
                .apply(
                    ParDo.of(
                        new DoFn<KV<Void, Void>, String>() {

                          @TimerId(timerName1)
                          final TimerSpec timerSpec1 = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                          @TimerId(timerName2)
                          final TimerSpec timerSpec2 = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                          @StateId(countStateName)
                          final StateSpec<ValueState<Integer>> countStateSpec = StateSpecs.value();

                          @ProcessElement
                          public void processElement(
                              ProcessContext context,
                              @TimerId(timerName1) Timer t1,
                              @TimerId(timerName2) Timer t2,
                              @StateId(countStateName) ValueState<Integer> state) {

                            state.write(0);
                            t1.set(start);
                            // set the t2 timer after end, so that we test that
                            // timers are correctly ordered in this case
                            t2.set(end.plus(1));
                          }

                          @OnTimer(timerName1)
                          public void onTimer1(
                              OnTimerContext context,
                              @TimerId(timerName2) Timer t2,
                              @StateId(countStateName) ValueState<Integer> state) {

                            Integer current = state.read();
                            t2.set(context.timestamp());

                            context.output(
                                "t1:"
                                    + current
                                    + ":"
                                    + context.timestamp().minus(start.getMillis()).getMillis());
                          }

                          @OnTimer(timerName2)
                          public void onTimer2(
                              OnTimerContext context,
                              @TimerId(timerName1) Timer t1,
                              @StateId(countStateName) ValueState<Integer> state) {
                            Integer current = state.read();
                            if (context.timestamp().isBefore(end)) {
                              state.write(current + 1);
                              t1.set(context.timestamp().plus(1));
                            } else {
                              state.write(-1);
                            }
                            context.output(
                                "t2:"
                                    + current
                                    + ":"
                                    + context.timestamp().minus(start.getMillis()).getMillis());
                          }
                        }));

        List<String> expected =
            LongStream.rangeClosed(0, end.minus(start.getMillis()).getMillis())
                .mapToObj(e -> (Long) e)
                .flatMap(e -> Arrays.asList("t1:" + e + ":" + e, "t2:" + e + ":" + e).stream())
                .collect(Collectors.toList());
        PAssert.that(result).containsInAnyOrder(expected);

        return PDone.in(input.getPipeline());
      }
    }
  }

  /** Tests validating Timer coder inference behaviors. */
  @RunWith(JUnit4.class)
  public static class TimerCoderInferenceTests extends SharedTestBase implements Serializable {
    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testValueStateCoderInference() {
      final String stateId = "foo";
      MyIntegerCoder myIntegerCoder = MyIntegerCoder.of();
      pipeline.getCoderRegistry().registerCoderForClass(MyInteger.class, myIntegerCoder);

      DoFn<KV<String, Integer>, MyInteger> fn =
          new DoFn<KV<String, Integer>, MyInteger>() {

            @StateId(stateId)
            private final StateSpec<ValueState<MyInteger>> intState = StateSpecs.value();

            @ProcessElement
            public void processElement(
                ProcessContext c,
                @StateId(stateId) ValueState<MyInteger> state,
                OutputReceiver<MyInteger> r) {
              MyInteger currentValue = MoreObjects.firstNonNull(state.read(), new MyInteger(0));
              r.output(currentValue);
              state.write(new MyInteger(currentValue.getValue() + 1));
            }
          };

      PCollection<MyInteger> output =
          pipeline
              .apply(Create.of(KV.of("hello", 42), KV.of("hello", 97), KV.of("hello", 84)))
              .apply(ParDo.of(fn))
              .setCoder(myIntegerCoder);

      PAssert.that(output).containsInAnyOrder(new MyInteger(0), new MyInteger(1), new MyInteger(2));
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesStatefulParDo.class})
    public void testValueStateCoderInferenceFailure() throws Exception {
      final String stateId = "foo";
      MyIntegerCoder myIntegerCoder = MyIntegerCoder.of();

      DoFn<KV<String, Integer>, MyInteger> fn =
          new DoFn<KV<String, Integer>, MyInteger>() {

            @StateId(stateId)
            private final StateSpec<ValueState<MyInteger>> intState = StateSpecs.value();

            @ProcessElement
            public void processElement(
                @StateId(stateId) ValueState<MyInteger> state, OutputReceiver<MyInteger> r) {
              MyInteger currentValue = MoreObjects.firstNonNull(state.read(), new MyInteger(0));
              r.output(currentValue);
              state.write(new MyInteger(currentValue.getValue() + 1));
            }
          };

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("Unable to infer a coder for ValueState and no Coder was specified.");

      pipeline
          .apply(Create.of(KV.of("hello", 42), KV.of("hello", 97), KV.of("hello", 84)))
          .apply(ParDo.of(fn))
          .setCoder(myIntegerCoder);

      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      DataflowPortabilityApiUnsupported.class
    })
    public void testValueStateCoderInferenceFromInputCoder() {
      final String stateId = "foo";
      MyIntegerCoder myIntegerCoder = MyIntegerCoder.of();

      DoFn<KV<String, MyInteger>, MyInteger> fn =
          new DoFn<KV<String, MyInteger>, MyInteger>() {

            @StateId(stateId)
            private final StateSpec<ValueState<MyInteger>> intState = StateSpecs.value();

            @ProcessElement
            public void processElement(
                @StateId(stateId) ValueState<MyInteger> state, OutputReceiver<MyInteger> r) {
              MyInteger currentValue = MoreObjects.firstNonNull(state.read(), new MyInteger(0));
              r.output(currentValue);
              state.write(new MyInteger(currentValue.getValue() + 1));
            }
          };

      pipeline
          .apply(
              Create.of(
                      KV.of("hello", new MyInteger(42)),
                      KV.of("hello", new MyInteger(97)),
                      KV.of("hello", new MyInteger(84)))
                  .withCoder(KvCoder.of(StringUtf8Coder.of(), myIntegerCoder)))
          .apply(ParDo.of(fn))
          .setCoder(myIntegerCoder);

      pipeline.run();
    }
  }

  private static class FnWithSideInputs extends DoFn<String, String> {
    private final PCollectionView<Integer> view;

    private FnWithSideInputs(PCollectionView<Integer> view) {
      this.view = view;
    }

    @ProcessElement
    public void processElement(ProcessContext c, @Element String element) {
      c.output(element + ":" + c.sideInput(view));
    }
  }

  private static class TestDummy {}

  private static class TestDummyCoder extends AtomicCoder<TestDummy> {
    private TestDummyCoder() {}

    private static final TestDummyCoder INSTANCE = new TestDummyCoder();

    @JsonCreator
    public static TestDummyCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(TestDummy value, OutputStream outStream) throws IOException {}

    @Override
    public TestDummy decode(InputStream inStream) throws IOException {
      return new TestDummy();
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(TestDummy value) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(TestDummy value, ElementByteSizeObserver observer)
        throws Exception {
      observer.update(0L);
    }

    @Override
    public void verifyDeterministic() {}
  }

  private static class TaggedOutputDummyFn extends DoFn<Integer, Integer> {
    private TupleTag<Integer> mainOutputTag;
    private TupleTag<TestDummy> dummyOutputTag;

    public TaggedOutputDummyFn(
        TupleTag<Integer> mainOutputTag, TupleTag<TestDummy> dummyOutputTag) {
      this.mainOutputTag = mainOutputTag;
      this.dummyOutputTag = dummyOutputTag;
    }

    @ProcessElement
    public void processElement(MultiOutputReceiver r) {
      r.get(mainOutputTag).output(1);
      r.get(dummyOutputTag).output(new TestDummy());
    }
  }

  private static class MainOutputDummyFn extends DoFn<Integer, TestDummy> {
    private TupleTag<TestDummy> mainOutputTag;
    private TupleTag<Integer> intOutputTag;

    public MainOutputDummyFn(TupleTag<TestDummy> mainOutputTag, TupleTag<Integer> intOutputTag) {
      this.mainOutputTag = mainOutputTag;
      this.intOutputTag = intOutputTag;
    }

    @ProcessElement
    public void processElement(MultiOutputReceiver r) {
      r.get(mainOutputTag).output(new TestDummy());
      r.get(intOutputTag).output(1);
    }
  }

  private static class MyInteger implements Comparable<MyInteger> {
    private final int value;

    MyInteger(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof MyInteger)) {
        return false;
      }

      MyInteger myInteger = (MyInteger) o;

      return value == myInteger.value;
    }

    @Override
    public int hashCode() {
      return value;
    }

    @Override
    public int compareTo(MyInteger o) {
      return Integer.compare(this.getValue(), o.getValue());
    }

    @Override
    public String toString() {
      return "MyInteger{" + "value=" + value + '}';
    }
  }

  private static class MyIntegerCoder extends AtomicCoder<MyInteger> {
    private static final MyIntegerCoder INSTANCE = new MyIntegerCoder();

    private final VarIntCoder delegate = VarIntCoder.of();

    public static MyIntegerCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(MyInteger value, OutputStream outStream) throws IOException {
      delegate.encode(value.getValue(), outStream);
    }

    @Override
    public MyInteger decode(InputStream inStream) throws IOException {
      return new MyInteger(delegate.decode(inStream));
    }
  }

  /** PAssert "matcher" for expected output. */
  static class HasExpectedOutput
      implements SerializableFunction<Iterable<String>, Void>, Serializable {
    private final List<Integer> inputs;
    private final List<Integer> sideInputs;
    private final String additionalOutput;

    public static HasExpectedOutput forInput(List<Integer> inputs) {
      return new HasExpectedOutput(new ArrayList<>(inputs), new ArrayList<>(), "");
    }

    private HasExpectedOutput(
        List<Integer> inputs, List<Integer> sideInputs, String additionalOutput) {
      this.inputs = inputs;
      this.sideInputs = sideInputs;
      this.additionalOutput = additionalOutput;
    }

    public HasExpectedOutput andSideInputs(Integer... sideInputValues) {
      return new HasExpectedOutput(inputs, Arrays.asList(sideInputValues), additionalOutput);
    }

    public HasExpectedOutput fromOutput(TupleTag<String> outputTag) {
      return fromOutput(outputTag.getId());
    }

    public HasExpectedOutput fromOutput(String outputId) {
      return new HasExpectedOutput(inputs, sideInputs, outputId);
    }

    @Override
    public Void apply(Iterable<String> outputs) {
      List<String> processeds = new ArrayList<>();
      List<String> finisheds = new ArrayList<>();
      for (String output : outputs) {
        if (output.contains("finished")) {
          finisheds.add(output);
        } else {
          processeds.add(output);
        }
      }

      String sideInputsSuffix;
      if (sideInputs.isEmpty()) {
        sideInputsSuffix = "";
      } else {
        sideInputsSuffix = ": " + sideInputs;
      }

      String additionalOutputPrefix;
      if (additionalOutput.isEmpty()) {
        additionalOutputPrefix = "";
      } else {
        additionalOutputPrefix = additionalOutput + ": ";
      }

      List<String> expectedProcesseds = new ArrayList<>();
      for (Integer input : inputs) {
        expectedProcesseds.add(additionalOutputPrefix + "processing: " + input + sideInputsSuffix);
      }
      String[] expectedProcessedsArray =
          expectedProcesseds.toArray(new String[expectedProcesseds.size()]);
      assertThat(processeds, containsInAnyOrder(expectedProcessedsArray));

      for (String finished : finisheds) {
        assertEquals(additionalOutputPrefix + "finished", finished);
      }

      return null;
    }
  }

  private static class Checker implements SerializableFunction<Iterable<String>, Void> {
    @Override
    public Void apply(Iterable<String> input) {
      boolean foundElement = false;
      boolean foundFinish = false;
      for (String str : input) {
        if ("elem:1:1".equals(str)) {
          if (foundElement) {
            throw new AssertionError("Received duplicate element");
          }
          foundElement = true;
        } else if ("finish:3:3".equals(str)) {
          foundFinish = true;
        } else {
          throw new AssertionError("Got unexpected value: " + str);
        }
      }
      if (!foundElement) {
        throw new AssertionError("Missing \"elem:1:1\"");
      }
      if (!foundFinish) {
        throw new AssertionError("Missing \"finish:3:3\"");
      }

      return null;
    }
  }

  /** A {@link PipelineOptions} subclass for testing passing to a {@link DoFn}. */
  public interface MyOptions extends PipelineOptions {
    @Default.String("fake option")
    String getFakeOption();

    void setFakeOption(String value);
  }

  private static class TwoTimerDoFn extends DoFn<KV<String, String>, String> {
    @TimerId("timer")
    private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(ProcessContext c, @TimerId("timer") Timer timer) {
      timer.offset(Duration.standardMinutes(10)).setRelative();
      timer.offset(Duration.standardMinutes(30)).setRelative();
    }

    @OnTimer("timer")
    public void onTimer(OutputReceiver<String> r, @TimerId("timer") Timer timer) {
      r.output("It works");
    }
  }

  /** Tests to validate ParDo timerFamily. */
  @RunWith(JUnit4.class)
  public static class TimerFamilyTests extends SharedTestBase implements Serializable {

    @Test
    @Category({ValidatesRunner.class, UsesTimersInParDo.class, UsesTimerMap.class})
    public void testTimerFamilyEventTimeBounded() throws Exception {
      runTestTimerFamilyEventTime(false);
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesUnboundedPCollections.class,
      UsesTimersInParDo.class,
      UsesTimerMap.class
    })
    public void testTimerFamilyEventTimeUnbounded() throws Exception {
      runTestTimerFamilyEventTime(true);
    }

    public void runTestTimerFamilyEventTime(boolean useStreaming) {
      final String timerFamilyId = "foo";

      DoFn<KV<String, Integer>, String> fn =
          new DoFn<KV<String, Integer>, String>() {

            @TimerFamily(timerFamilyId)
            private final TimerSpec spec = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @TimerFamily(timerFamilyId) TimerMap timers, OutputReceiver<String> r) {
              timers.set("timer1", new Instant(1));
              timers.set("timer2", new Instant(2));
              r.output("process");
            }

            @OnTimerFamily(timerFamilyId)
            public void onTimer(
                @TimerId String timerId,
                @Timestamp Instant ts,
                @TimerFamily(timerFamilyId) TimerMap timerMap,
                OutputReceiver<String> r) {
              System.out.println("timer Id : " + timerId);
              System.out.println("timerMap : " + timerMap.toString());
              r.output(timerId);
            }
          };

      PCollection<String> output =
          pipeline
              .apply(Create.of(KV.of("hello", 37)))
              .setIsBoundedInternal(useStreaming ? IsBounded.UNBOUNDED : IsBounded.BOUNDED)
              .apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder("process", "timer1", "timer2");
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesTimersInParDo.class, UsesTimerMap.class})
    public void testTimerWithMultipleTimerFamilyBounded() throws Exception {
      runTestTimerWithMultipleTimerFamily(false);
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesUnboundedPCollections.class,
      UsesTimersInParDo.class,
      UsesTimerMap.class
    })
    public void testTimerWithMultipleTimerFamilyUnbounded() throws Exception {
      runTestTimerWithMultipleTimerFamily(true);
    }

    public void runTestTimerWithMultipleTimerFamily(boolean useStreaming) throws Exception {
      final String timerFamilyId1 = "foo";
      final String timerFamilyId2 = "bar";

      DoFn<KV<String, Integer>, String> fn =
          new DoFn<KV<String, Integer>, String>() {

            @TimerFamily(timerFamilyId1)
            private final TimerSpec spec1 = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

            @TimerFamily(timerFamilyId2)
            private final TimerSpec spec2 = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @TimerFamily(timerFamilyId1) TimerMap timerMap1,
                @TimerFamily(timerFamilyId2) TimerMap timerMap2,
                OutputReceiver<String> r) {
              timerMap1.set("timer", new Instant(1));
              timerMap2.set("timer", new Instant(2));
              r.output("process");
            }

            @OnTimerFamily(timerFamilyId1)
            public void onTimer1(
                @TimerId String timerId, @Timestamp Instant ts, OutputReceiver<String> r) {
              r.output(timerId);
            }

            @OnTimerFamily(timerFamilyId2)
            public void onTimer2(
                @TimerId String timerId, @Timestamp Instant ts, OutputReceiver<String> r) {
              r.output(timerId);
            }
          };

      PCollection<String> output =
          pipeline
              .apply(Create.of(KV.of("hello", 37)))
              .setIsBoundedInternal(useStreaming ? IsBounded.UNBOUNDED : IsBounded.BOUNDED)
              .apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder("process", "timer", "timer");
      pipeline.run();
    }

    @Test
    @Category({ValidatesRunner.class, UsesTimersInParDo.class, UsesTimerMap.class})
    public void testTimerFamilyAndTimerBounded() throws Exception {
      runTestTimerFamilyAndTimer(false);
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesUnboundedPCollections.class,
      UsesTimersInParDo.class,
      UsesTimerMap.class
    })
    public void testTimerFamilyAndTimerUnbounded() throws Exception {
      runTestTimerFamilyAndTimer(true);
    }

    public void runTestTimerFamilyAndTimer(boolean useStreaming) throws Exception {
      final String timerFamilyId = "foo";
      final String timerId = "timer";

      DoFn<KV<String, Integer>, String> fn =
          new DoFn<KV<String, Integer>, String>() {

            @TimerFamily(timerFamilyId)
            private final TimerSpec spec1 = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

            @TimerId(timerId)
            private final TimerSpec spec2 = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(
                @TimerFamily(timerFamilyId) TimerMap timerMap,
                @TimerId(timerId) Timer timer,
                OutputReceiver<String> r) {
              timerMap.set("timer", new Instant(1));
              timer.set(new Instant(2));
              r.output("process");
            }

            @OnTimerFamily(timerFamilyId)
            public void onTimer1(
                @TimerId String timerId, @Timestamp Instant ts, OutputReceiver<String> r) {
              r.output("family:" + timerFamilyId + ":" + timerId);
            }

            @OnTimer(timerId)
            public void onTimer2(@Timestamp Instant ts, OutputReceiver<String> r) {
              r.output(timerId);
            }
          };

      PCollection<String> output =
          pipeline
              .apply(Create.of(KV.of("hello", 37)))
              .setIsBoundedInternal(useStreaming ? IsBounded.UNBOUNDED : IsBounded.BOUNDED)
              .apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder("process", "family:foo:timer", "timer");
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      UsesTestStreamWithProcessingTime.class,
      UsesTimerMap.class
    })
    public void testTimerFamilyProcessingTime() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerFamily(timerId)
            private final TimerSpec spec = TimerSpecs.timerMap(TimeDomain.PROCESSING_TIME);

            @ProcessElement
            public void processElement(
                @TimerFamily(timerId) TimerMap timerMap, OutputReceiver<Integer> r) {
              Timer timer = timerMap.get("timerId1");
              timer.offset(Duration.standardSeconds(1)).setRelative();
              r.output(3);
            }

            @OnTimerFamily(timerId)
            public void onTimer(TimeDomain timeDomain, OutputReceiver<Integer> r) {
              if (timeDomain.equals(TimeDomain.PROCESSING_TIME)) {
                r.output(42);
              }
            }
          };

      TestStream<KV<String, Integer>> stream =
          TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
              .addElements(KV.of("hello", 37))
              .advanceProcessingTime(Duration.standardSeconds(2))
              .advanceWatermarkToInfinity();

      PCollection<Integer> output = pipeline.apply(stream).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(3, 42);
      pipeline.run();
    }
  }

  /** Tests to validate Key in OnTimer. */
  @RunWith(JUnit4.class)
  public static class KeyTests extends SharedTestBase implements Serializable {

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      UsesKeyInParDo.class,
    })
    public void testKeyInOnTimer() throws Exception {
      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer, OutputReceiver<Integer> r) {
              timer.set(new Instant(1));
            }

            @OnTimer(timerId)
            public void onTimer(@Key String key, OutputReceiver<Integer> r) {
              r.output(Integer.parseInt(key));
            }
          };

      PCollection<Integer> output =
          pipeline.apply(Create.of(KV.of("1", 37), KV.of("2", 3))).apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(1, 2);
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      UsesKeyInParDo.class,
    })
    public void testKeyInOnTimerWithGenericKey() throws Exception {
      final String timerId = "foo";

      DoFn<KV<KV<String, String>, Integer>, Integer> fn =
          new DoFn<KV<KV<String, String>, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer, OutputReceiver<Integer> r) {
              timer.set(new Instant(1));
            }

            @OnTimer(timerId)
            public void onTimer(@Key KV<String, String> key, OutputReceiver<Integer> r) {
              r.output(Integer.parseInt(key.getKey()));
            }
          };

      PCollection<Integer> output =
          pipeline
              .apply(Create.of(KV.of(KV.of("1", "1"), 37), KV.of(KV.of("1", "1"), 3)))
              .apply(ParDo.of(fn));
      PAssert.that(output).containsInAnyOrder(1);
      pipeline.run();
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      UsesKeyInParDo.class,
    })
    public void testKeyInOnTimerWithWrongKeyType() throws Exception {

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("@Key argument is expected to be type of");
      thrown.expectMessage(", but found ");

      final String timerId = "foo";

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer, OutputReceiver<Integer> r) {
              timer.set(new Instant(1));
            }

            @OnTimer(timerId)
            public void onTimer(@Key Integer key, OutputReceiver<Integer> r) {}
          };

      pipeline.apply(Create.of(KV.of("1", 37), KV.of("1", 4), KV.of("2", 3))).apply(ParDo.of(fn));
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      UsesKeyInParDo.class,
    })
    public void testKeyInOnTimerWithoutKV() throws Exception {

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("@Key argument is expected to be use with input element of type KV.");

      final String timerId = "foo";

      DoFn<String, Integer> fn =
          new DoFn<String, Integer>() {

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer, OutputReceiver<Integer> r) {
              timer.set(new Instant(1));
            }

            @OnTimer(timerId)
            public void onTimer(@Key Integer key, OutputReceiver<Integer> r) {}
          };

      pipeline.apply(Create.of("1")).apply(ParDo.of(fn));
    }
  }

  /** Tests to validate OnWindowExpiration. */
  @RunWith(JUnit4.class)
  public static class OnWindowExpirationTests extends SharedTestBase implements Serializable {

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      UsesTimersInParDo.class,
      UsesOnWindowExpiration.class,
    })
    public void testOnWindowExpirationSimpleBounded() {
      runOnWindowExpirationSimple(false);
    }

    @Test
    @Category({
      ValidatesRunner.class,
      UsesStatefulParDo.class,
      UsesTimersInParDo.class,
      UsesOnWindowExpiration.class,
      UsesUnboundedPCollections.class,
    })
    public void testOnWindowExpirationSimpleUnbounded() {
      runOnWindowExpirationSimple(true);
    }

    public void runOnWindowExpirationSimple(boolean useStreaming) {
      final String stateId = "foo";
      final String timerId = "bar";
      IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
      IntervalWindow secondWindow = new IntervalWindow(new Instant(10), new Instant(20));

      DoFn<KV<String, Integer>, Integer> fn =
          new DoFn<KV<String, Integer>, Integer>() {

            @StateId(stateId)
            private final StateSpec<ValueState<Integer>> intState =
                StateSpecs.value(VarIntCoder.of());

            @TimerId(timerId)
            private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(@TimerId(timerId) Timer timer, BoundedWindow window) {
              timer.set(new Instant(1));
            }

            @OnTimer(timerId)
            public void onTimer(
                @Timestamp Instant timestamp,
                @AlwaysFetched @StateId(stateId) ValueState<Integer> state) {
              // To check if state is persisted until OnWindowExpiration
              Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
              state.write(currentValue + 1);
            }

            @OnWindowExpiration
            public void onWindowExpiration(
                @AlwaysFetched @StateId(stateId) ValueState<Integer> state,
                @Key String key,
                OutputReceiver<Integer> r) {
              Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
              // verify state
              assertEquals(1, (int) currentValue);
              // To check output is received from OnWindowExpiration
              r.output(currentValue);
            }
          };

      PCollection<Integer> output =
          pipeline
              .apply(
                  Create.timestamped(
                      // first window
                      TimestampedValue.of(KV.of("hello", 7), new Instant(3)),

                      // second window
                      TimestampedValue.of(KV.of("hi", 35), new Instant(13))))
              .apply(Window.into(FixedWindows.of(Duration.millis(10))))
              .setIsBoundedInternal(useStreaming ? IsBounded.UNBOUNDED : IsBounded.BOUNDED)
              .apply(ParDo.of(fn));

      PAssert.that(output)
          .inWindow(firstWindow)
          // verify output
          .containsInAnyOrder(1)
          .inWindow(secondWindow)
          // verify output
          .containsInAnyOrder(1);
      pipeline.run();
    }
  }
}
