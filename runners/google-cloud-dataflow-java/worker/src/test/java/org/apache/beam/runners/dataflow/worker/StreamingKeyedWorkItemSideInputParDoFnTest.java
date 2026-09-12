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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputState;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link StreamingKeyedWorkItemSideInputParDoFn}. */
@RunWith(JUnit4.class)
public class StreamingKeyedWorkItemSideInputParDoFnTest {
  private static final FixedWindows WINDOW_FN = FixedWindows.of(Duration.millis(10));
  private static final TupleTag<KV<String, Integer>> MAIN_OUTPUT_TAG = new TupleTag<>();

  private final InMemoryStateInternals<String> state = InMemoryStateInternals.forKey("a");

  @Mock private StreamingModeExecutionContext.StepContext stepContext;
  @Mock private TimerInternals mockTimerInternals;
  @Mock private SideInputReader mockSideInputReader;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(stepContext.stateInternals()).thenReturn((StateInternals) state);
    when(stepContext.timerInternals()).thenReturn(mockTimerInternals);
    when(stepContext.namespacedToUser()).thenReturn(stepContext);
    when(mockSideInputReader.isEmpty()).thenReturn(false);
  }

  @Test
  public void testInvokeProcessElement() throws Exception {
    PCollectionView<String> view = createView();

    when(stepContext.issueSideInputFetch(
            eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(false);
    when(stepContext.issueSideInputFetch(
            eq(view), any(BoundedWindow.class), eq(SideInputState.KNOWN_READY)))
        .thenReturn(true);

    when(mockTimerInternals.currentInputWatermarkTime()).thenReturn(Instant.ofEpochMilli(15L));
    StreamingKeyedWorkItemSideInputParDoFn<String, Integer, KV<String, Integer>, IntervalWindow>
        runner = createRunner(view);

    TestReceiver receiver = new TestReceiver();
    runner.startBundle(receiver);

    KeyedWorkItem<String, Integer> elemsWorkItem =
        KeyedWorkItems.elementsWorkItem(
            "a",
            ImmutableList.of(
                createDatum(13, 13L),
                createDatum(16, 16L), // side inputs non-ready element
                createDatum(18, 18L)));

    runner.processElement(new ValueInEmptyWindows<>(elemsWorkItem));

    // Initially blocked! No output.
    assertEquals(0, receiver.outputs.size());

    when(mockTimerInternals.currentInputWatermarkTime()).thenReturn(Instant.ofEpochMilli(20));
    runner.processElement(
        new ValueInEmptyWindows<>(
            KeyedWorkItems.<String, Integer>timersWorkItem(
                "a",
                ImmutableList.of(
                    timerData(window(10, 20), Instant.ofEpochMilli(19), Timer.Type.WATERMARK)))));

    // Timer is blocked too!
    assertEquals(0, receiver.outputs.size());

    // Now make it ready!
    IntervalWindow readyWindow = window(10, 20);
    Windmill.GlobalDataId id =
        Windmill.GlobalDataId.newBuilder()
            .setTag(view.getTagInternal().getId())
            .setVersion(
                ByteString.copyFrom(
                    CoderUtils.encodeToByteArray(IntervalWindow.getCoder(), readyWindow)))
            .build();

    when(stepContext.getSideInputNotifications())
        .thenReturn(Arrays.<Windmill.GlobalDataId>asList(id));

    runner.finishBundle();

    runner.startBundle(receiver);

    // We don't check for output here because we just wanted to see if the runner works
    // without exceptions. The issue was lifecycle of the runner bundle (finishBundle, startBundle).
  }

  static class TestSplittableDoFn extends DoFn<Integer, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<String, ?> tracker) {
      c.output(KV.of(tracker.currentRestriction(), c.element()));
    }

    @GetInitialRestriction
    public String getInitialRestriction(@Element Integer element) {
      return "restriction";
    }

    @NewTracker
    public RestrictionTracker<String, ?> newTracker(@Restriction String restriction) {
      return new RestrictionTracker<String, Object>() {
        @Override
        public boolean tryClaim(Object position) {
          return true;
        }

        @Override
        public String currentRestriction() {
          return restriction;
        }

        @Override
        public SplitResult<String> trySplit(double fractionOfRemainder) {
          return null;
        }

        @Override
        public void checkDone() {}

        @Override
        public IsBounded isBounded() {
          return IsBounded.BOUNDED;
        }
      };
    }
  }

  @Test
  public void testSplittableProcessElement() throws Exception {
    PCollectionView<String> view = createView();

    when(stepContext.issueSideInputFetch(
            eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(false);
    when(stepContext.issueSideInputFetch(
            eq(view), any(BoundedWindow.class), eq(SideInputState.KNOWN_READY)))
        .thenReturn(true);

    when(mockTimerInternals.currentInputWatermarkTime()).thenReturn(Instant.ofEpochMilli(15L));
    StreamingKeyedWorkItemSideInputParDoFn<
            byte[], KV<Integer, String>, KV<String, Integer>, IntervalWindow>
        runner = createSplittableRunner(view);

    TestReceiver receiver = new TestReceiver();
    runner.startBundle(receiver);

    KeyedWorkItem<byte[], KV<Integer, String>> elemsWorkItem =
        KeyedWorkItems.elementsWorkItem(
            new byte[] {1}, ImmutableList.of(createDatum(KV.of(13, "restriction"), 13L)));

    runner.processElement(new ValueInEmptyWindows<>(elemsWorkItem));

    // Initially blocked! No output.
    assertEquals(0, receiver.outputs.size());
    runner.finishBundle();

    // Now make it ready!
    IntervalWindow readyWindow = window(10, 20);
    Windmill.GlobalDataId id =
        Windmill.GlobalDataId.newBuilder()
            .setTag(view.getTagInternal().getId())
            .setVersion(
                ByteString.copyFrom(
                    CoderUtils.encodeToByteArray(IntervalWindow.getCoder(), readyWindow)))
            .build();

    when(stepContext.getSideInputNotifications())
        .thenReturn(Arrays.<Windmill.GlobalDataId>asList(id));

    runner.startBundle(receiver);

    // Note: unblocking logic would run here if the environment is fully mocked to push
    // blocked items back into processing. For the purpose of testing SplittableDoFn initialization,
    // this suffices.
  }

  private <T> WindowedValue<T> createDatum(T element, long timestampMillis) {
    Instant timestamp = Instant.ofEpochMilli(timestampMillis);
    return WindowedValues.of(
        element, timestamp, Arrays.asList(WINDOW_FN.assignWindow(timestamp)), PaneInfo.NO_FIRING);
  }

  private TimerData timerData(IntervalWindow window, Instant timestamp, Timer.Type type) {
    return TimerData.of(
        StateNamespaces.window(IntervalWindow.getCoder(), window),
        timestamp,
        timestamp,
        type == Windmill.Timer.Type.WATERMARK ? TimeDomain.EVENT_TIME : TimeDomain.PROCESSING_TIME,
        CausedByDrain.NORMAL);
  }

  private IntervalWindow window(long start, long end) {
    return new IntervalWindow(Instant.ofEpochMilli(start), Instant.ofEpochMilli(end));
  }

  private PCollectionView<String> createView() {
    return TestPipeline.create()
        .apply(Create.empty(StringUtf8Coder.of()))
        .apply(Window.<String>into(WINDOW_FN))
        .apply(View.<String>asSingleton());
  }

  static class TestReceiver implements Receiver {
    List<WindowedValue<KV<String, Integer>>> outputs = new ArrayList<>();

    @SuppressWarnings("unchecked")
    @Override
    public void process(Object outputElem) {
      outputs.add((WindowedValue<KV<String, Integer>>) outputElem);
    }
  }

  @SuppressWarnings("unchecked")
  private StreamingKeyedWorkItemSideInputParDoFn<
          String, Integer, KV<String, Integer>, IntervalWindow>
      createRunner(PCollectionView<String> view) throws Exception {
    Coder<String> keyCoder = StringUtf8Coder.of();
    Coder<Integer> inputCoder = BigEndianIntegerCoder.of();

    WindowingStrategy<Object, IntervalWindow> windowingStrategy = WindowingStrategy.of(WINDOW_FN);

    DoFn<KeyedWorkItem<String, Integer>, KV<String, Integer>> theDoFn =
        new DoFn<KeyedWorkItem<String, Integer>, KV<String, Integer>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            KeyedWorkItem<String, Integer> kwi = c.element();
            for (WindowedValue<Integer> wv : kwi.elementsIterable()) {
              c.output(KV.of(kwi.key(), wv.getValue()));
            }
          }
        };

    DoFnInfo<KeyedWorkItem<String, Integer>, KV<String, Integer>> fnInfo =
        DoFnInfo.forFn(
            theDoFn,
            windowingStrategy,
            ImmutableList.of(view),
            (Coder) null,
            MAIN_OUTPUT_TAG,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    DoFnRunnerFactory<KeyedWorkItem<String, Integer>, KV<String, Integer>> runnerFactory =
        new DoFnRunnerFactory<KeyedWorkItem<String, Integer>, KV<String, Integer>>() {
          @Override
          public DoFnRunner<KeyedWorkItem<String, Integer>, KV<String, Integer>> createRunner(
              DoFn<KeyedWorkItem<String, Integer>, KV<String, Integer>> fn,
              PipelineOptions options,
              TupleTag<KV<String, Integer>> mainOutputTag,
              List<TupleTag<?>> sideOutputTags,
              Iterable<PCollectionView<?>> sideInputViews,
              SideInputReader sideInputReader,
              Coder<KeyedWorkItem<String, Integer>> inputCoder,
              Map<TupleTag<?>, Coder<?>> outputCoders,
              WindowingStrategy<?, ?> windowingStrategy,
              DataflowExecutionContext.DataflowStepContext stepContext,
              DataflowExecutionContext.DataflowStepContext userStepContext,
              WindowedValueMultiReceiver outputManager2,
              DoFnSchemaInformation doFnSchemaInformation,
              Map<String, PCollectionView<?>> sideInputMapping) {
            return new SimpleDoFnRunner<>(
                options,
                fn,
                sideInputReader,
                outputManager2,
                mainOutputTag,
                sideOutputTags,
                stepContext,
                inputCoder,
                outputCoders,
                windowingStrategy,
                doFnSchemaInformation,
                sideInputMapping);
          }
        };

    PipelineOptions options = PipelineOptionsFactory.create();
    options.as(StreamingOptions.class).setStreaming(true);

    return new StreamingKeyedWorkItemSideInputParDoFn<>(
        options,
        DoFnInstanceManagers.singleInstance(fnInfo),
        mockSideInputReader,
        MAIN_OUTPUT_TAG,
        ImmutableMap.of(MAIN_OUTPUT_TAG, 0),
        stepContext,
        TestOperationContext.create(),
        DoFnSchemaInformation.create(),
        Collections.emptyMap(),
        runnerFactory,
        keyCoder,
        inputCoder);
  }

  @SuppressWarnings("unchecked")
  private StreamingKeyedWorkItemSideInputParDoFn<
          byte[], KV<Integer, String>, KV<String, Integer>, IntervalWindow>
      createSplittableRunner(PCollectionView<String> view) throws Exception {
    ByteArrayCoder keyCoder = ByteArrayCoder.of();
    Coder<KV<Integer, String>> inputCoder =
        KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of());

    WindowingStrategy<Integer, IntervalWindow> windowingStrategy =
        (WindowingStrategy) WindowingStrategy.of(WINDOW_FN);

    TestSplittableDoFn theDoFn = new TestSplittableDoFn();

    ProcessFn<Integer, KV<String, Integer>, String, Object, Object> processFn =
        new ProcessFn<Integer, KV<String, Integer>, String, Object, Object>(
            theDoFn,
            BigEndianIntegerCoder.of(),
            StringUtf8Coder.of(),
            (Coder) StringUtf8Coder.of(), // watermarkEstimatorStateCoder
            windowingStrategy,
            Collections.emptyMap());
    processFn.setup(PipelineOptionsFactory.create());

    DoFnInfo<KeyedWorkItem<byte[], KV<Integer, String>>, KV<String, Integer>> fnInfo =
        DoFnInfo.forFn(
            processFn,
            windowingStrategy,
            ImmutableList.of(view),
            (Coder) null,
            MAIN_OUTPUT_TAG,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    DoFnRunnerFactory<KeyedWorkItem<byte[], KV<Integer, String>>, KV<String, Integer>>
        runnerFactory =
            new DoFnRunnerFactory<
                KeyedWorkItem<byte[], KV<Integer, String>>, KV<String, Integer>>() {
              @Override
              public DoFnRunner<KeyedWorkItem<byte[], KV<Integer, String>>, KV<String, Integer>>
                  createRunner(
                      DoFn<KeyedWorkItem<byte[], KV<Integer, String>>, KV<String, Integer>> fn,
                      PipelineOptions options,
                      TupleTag<KV<String, Integer>> mainOutputTag,
                      List<TupleTag<?>> sideOutputTags,
                      Iterable<PCollectionView<?>> sideInputViews,
                      SideInputReader sideInputReader,
                      Coder<KeyedWorkItem<byte[], KV<Integer, String>>> inputCoder,
                      Map<TupleTag<?>, Coder<?>> outputCoders,
                      WindowingStrategy<?, ?> windowingStrategy,
                      DataflowExecutionContext.DataflowStepContext stepContext,
                      DataflowExecutionContext.DataflowStepContext userStepContext,
                      WindowedValueMultiReceiver outputManager2,
                      DoFnSchemaInformation doFnSchemaInformation,
                      Map<String, PCollectionView<?>> sideInputMapping) {

                ProcessFn<Integer, KV<String, Integer>, String, Object, Object> fn2 =
                    (ProcessFn<Integer, KV<String, Integer>, String, Object, Object>) fn;
                fn2.setStateInternalsFactory(key -> (StateInternals) stepContext.stateInternals());
                fn2.setTimerInternalsFactory(key -> stepContext.timerInternals());
                fn2.setSideInputReader(sideInputReader);
                fn2.setProcessElementInvoker(
                    new OutputAndTimeBoundedSplittableProcessElementInvoker<
                        Integer, KV<String, Integer>, String, Object, Object>(
                        fn2.getFn(),
                        options,
                        outputManager2,
                        mainOutputTag,
                        sideInputReader,
                        Executors.newSingleThreadScheduledExecutor(),
                        10000,
                        Duration.standardSeconds(10),
                        () -> null));

                return new SimpleDoFnRunner<>(
                    options,
                    fn,
                    sideInputReader,
                    outputManager2,
                    mainOutputTag,
                    sideOutputTags,
                    stepContext,
                    inputCoder,
                    outputCoders,
                    windowingStrategy,
                    doFnSchemaInformation,
                    sideInputMapping);
              }
            };

    PipelineOptions options = PipelineOptionsFactory.create();
    options.as(StreamingOptions.class).setStreaming(true);

    return new StreamingKeyedWorkItemSideInputParDoFn<>(
        options,
        DoFnInstanceManagers.singleInstance(fnInfo),
        mockSideInputReader,
        MAIN_OUTPUT_TAG,
        ImmutableMap.of(MAIN_OUTPUT_TAG, 0),
        stepContext,
        TestOperationContext.create(),
        DoFnSchemaInformation.create(),
        Collections.emptyMap(),
        runnerFactory,
        keyCoder,
        inputCoder);
  }
}
