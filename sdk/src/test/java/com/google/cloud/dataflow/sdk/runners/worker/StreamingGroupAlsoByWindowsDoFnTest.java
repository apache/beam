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

package com.google.cloud.dataflow.sdk.runners.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.CollectionCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.InputMessageBundle;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.Timer;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.WorkItem;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.util.AppliedCombineFn;
import com.google.cloud.dataflow.sdk.util.DirectModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.DoFnRunner;
import com.google.cloud.dataflow.sdk.util.DoFnRunnerBase;
import com.google.cloud.dataflow.sdk.util.DoFnRunners;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.NullSideInputReader;
import com.google.cloud.dataflow.sdk.util.ReshuffleTriggerTest;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.protobuf.ByteString;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/** Unit tests for {@link StreamingGroupAlsoByWindowsDoFn}. */
@RunWith(JUnit4.class)
public class StreamingGroupAlsoByWindowsDoFnTest {
  private static final String KEY = "k";
  private static final String STATE_FAMILY = "stateFamily";
  private static final long WORK_TOKEN = 1000L;
  private static final String SOURCE_COMPUTATION_ID = "sourceComputationId";
  private ExecutionContext execContext;
  private CounterSet counters;

  @Mock
  private TimerInternals mockTimerInternals;

  private Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();
  private Coder<Collection<IntervalWindow>> windowsCoder = CollectionCoder.of(windowCoder);

  @Before public void setUp() {
    MockitoAnnotations.initMocks(this);
    execContext = new DirectModeExecutionContext() {
      // Normally timerInternals doesn't come from the execution context, but
      // StreamingGroupAlsoByWindows expects it to. So, hook that up.

      @Override
      public StepContext createStepContext(
          String stepName, String transformName, StateSampler stateSampler) {
        StepContext context =
            Mockito.spy(super.createStepContext(stepName, transformName, stateSampler));
        Mockito.doReturn(mockTimerInternals).when(context).timerInternals();
        return context;
      }
    };
    counters = new CounterSet();
  }

  @Test public void testReshufle() throws Exception {
    DoFn<?, ?> fn = StreamingGroupAlsoByWindowsDoFn.createForIterable(
        WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(30)))
        .withTrigger(ReshuffleTriggerTest.forTest()),
        VarIntCoder.of());
    assertTrue(fn instanceof StreamingGroupAlsoByWindowsReshuffleDoFn);
  }

  @Test public void testEmpty() throws Exception {
    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunnerBase.ListOutputManager outputManager = new DoFnRunnerBase.ListOutputManager();
    DoFnRunner<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> runner =
        makeRunner(
            outputTag, outputManager, WindowingStrategy.of(FixedWindows.of(Duration.millis(10))));

    runner.startBundle();

    runner.finishBundle();

    List<?> result = outputManager.getOutput(outputTag);

    assertEquals(0, result.size());
  }

  private void addTimer(WorkItem.Builder workItem,
      IntervalWindow window, Instant timestamp, Windmill.Timer.Type type) {
    StateNamespace namespace = StateNamespaces.window(windowCoder, window);
    workItem.getTimersBuilder().addTimersBuilder()
        .setTag(StreamingModeExecutionContext.timerTag(TimerData.of(
            namespace, timestamp,
            type == Windmill.Timer.Type.WATERMARK
                ? TimeDomain.EVENT_TIME : TimeDomain.PROCESSING_TIME)))
        .setTimestamp(WindmillTimeUtils.harnessToWindmillTimestamp(timestamp))
        .setType(type)
        .setStateFamily(STATE_FAMILY);
  }

  private <V> void addElement(
      InputMessageBundle.Builder messageBundle, Collection<IntervalWindow> windows,
      Instant timestamp, Coder<V> valueCoder, V value) throws IOException {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Coder<Collection<? extends BoundedWindow>> windowsCoder =
        (Coder) CollectionCoder.of(windowCoder);

    ByteString.Output dataOutput = ByteString.newOutput();
    valueCoder.encode(value, dataOutput, Context.OUTER);
    messageBundle.addMessagesBuilder()
        .setMetadata(WindmillSink.encodeMetadata(windowsCoder, windows, PaneInfo.NO_FIRING))
        .setData(dataOutput.toByteString())
        .setTimestamp(WindmillTimeUtils.harnessToWindmillTimestamp(timestamp));
  }

  private <T> WindowedValue<KeyedWorkItem<String, T>> createValue(
      WorkItem.Builder workItem, Coder<T> valueCoder) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Coder<Collection<? extends BoundedWindow>> wildcardWindowsCoder = (Coder) windowsCoder;
    return WindowedValue.valueInEmptyWindows(KeyedWorkItems.windmillWorkItem(
        KEY, workItem.build(), windowCoder, wildcardWindowsCoder, valueCoder));
  }

  @Test public void testFixedWindows() throws Exception {
    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunnerBase.ListOutputManager outputManager = new DoFnRunnerBase.ListOutputManager();
    DoFnRunner<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> runner =
        makeRunner(
            outputTag, outputManager, WindowingStrategy.of(FixedWindows.of(Duration.millis(10))));

    runner.startBundle();

    WorkItem.Builder workItem1 = WorkItem.newBuilder();
    workItem1.setKey(ByteString.copyFromUtf8(KEY));
    workItem1.setWorkToken(WORK_TOKEN);
    InputMessageBundle.Builder messageBundle = workItem1.addMessageBundlesBuilder();
    messageBundle.setSourceComputationId(SOURCE_COMPUTATION_ID);

    Coder<String> valueCoder = StringUtf8Coder.of();
    addElement(messageBundle, Arrays.asList(window(0, 10)), new Instant(1), valueCoder, "v1");
    addElement(messageBundle, Arrays.asList(window(0, 10)), new Instant(2), valueCoder, "v2");
    addElement(messageBundle, Arrays.asList(window(0, 10)), new Instant(0), valueCoder, "v0");
    addElement(messageBundle, Arrays.asList(window(10, 20)), new Instant(13), valueCoder, "v3");

    runner.processElement(createValue(workItem1, valueCoder));

    runner.finishBundle();
    runner.startBundle();

    WorkItem.Builder workItem2 = WorkItem.newBuilder();
    workItem2.setKey(ByteString.copyFromUtf8(KEY));
    workItem2.setWorkToken(WORK_TOKEN);
    addTimer(workItem2, window(0, 10), new Instant(9), Timer.Type.WATERMARK);
    addTimer(workItem2, window(10, 20), new Instant(19), Timer.Type.WATERMARK);

    runner.processElement(createValue(workItem2, valueCoder));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals(KEY, item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.containsInAnyOrder("v0", "v1", "v2"));
    assertEquals(new Instant(0), item0.getTimestamp());
    assertThat(item0.getWindows(), Matchers.<BoundedWindow>contains(window(0, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals(KEY, item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.containsInAnyOrder("v3"));
    assertEquals(new Instant(13), item1.getTimestamp());
    assertThat(item1.getWindows(), Matchers.<BoundedWindow>contains(window(10, 20)));
  }

  @Test public void testSlidingWindows() throws Exception {
    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunnerBase.ListOutputManager outputManager = new DoFnRunnerBase.ListOutputManager();
    DoFnRunner<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> runner =
        makeRunner(
            outputTag,
            outputManager,
            WindowingStrategy.of(
            SlidingWindows.of(Duration.millis(20)).every(Duration.millis(10))));

    when(mockTimerInternals.currentInputWatermarkTime()).thenReturn(new Instant(5));

    runner.startBundle();

    WorkItem.Builder workItem1 = WorkItem.newBuilder();
    workItem1.setKey(ByteString.copyFromUtf8(KEY));
    workItem1.setWorkToken(WORK_TOKEN);
    InputMessageBundle.Builder messageBundle = workItem1.addMessageBundlesBuilder();
    messageBundle.setSourceComputationId(SOURCE_COMPUTATION_ID);

    Coder<String> valueCoder = StringUtf8Coder.of();
    addElement(messageBundle,
        Arrays.asList(window(-10, 10), window(0, 20)), new Instant(5), valueCoder, "v1");
    addElement(messageBundle,
        Arrays.asList(window(-10, 10), window(0, 20)), new Instant(2), valueCoder, "v0");
    addElement(messageBundle,
        Arrays.asList(window(0, 20), window(10, 30)), new Instant(15), valueCoder, "v2");

    runner.processElement(createValue(workItem1, valueCoder));

    runner.finishBundle();
    runner.startBundle();

    WorkItem.Builder workItem2 = WorkItem.newBuilder();
    workItem2.setKey(ByteString.copyFromUtf8(KEY));
    workItem2.setWorkToken(WORK_TOKEN);
    addTimer(workItem2, window(-10, 10), new Instant(9), Timer.Type.WATERMARK);
    addTimer(workItem2, window(0, 20), new Instant(19), Timer.Type.WATERMARK);
    addTimer(workItem2, window(10, 30), new Instant(29), Timer.Type.WATERMARK);

    runner.processElement(createValue(workItem2, valueCoder));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(3, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals(KEY, item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.containsInAnyOrder("v0", "v1"));
    assertEquals(new Instant(2), item0.getTimestamp());
    assertThat(item0.getWindows(), Matchers.<BoundedWindow>contains(window(-10, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals(KEY, item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.containsInAnyOrder("v0", "v1", "v2"));
    // For this sliding window, the minimum output timestmap was 10, since we didn't want to overlap
    // with the previous window that was [-10, 10).
    assertEquals(new Instant(10), item1.getTimestamp());
    assertThat(item1.getWindows(), Matchers.<BoundedWindow>contains(window(0, 20)));

    WindowedValue<KV<String, Iterable<String>>> item2 = result.get(2);
    assertEquals(KEY, item2.getValue().getKey());
    assertThat(item2.getValue().getValue(), Matchers.containsInAnyOrder("v2"));
    assertEquals(new Instant(20), item2.getTimestamp());
    assertThat(item2.getWindows(), Matchers.<BoundedWindow>contains(window(10, 30)));
  }

  @Test public void testSlidingWindowsAndLateData() throws Exception {
    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunnerBase.ListOutputManager outputManager = new DoFnRunnerBase.ListOutputManager();
    WindowingStrategy<? super String, IntervalWindow> windowingStrategy = WindowingStrategy.of(
        SlidingWindows.of(Duration.millis(20)).every(Duration.millis(10)));
    DoFn<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> fn =
        StreamingGroupAlsoByWindowsDoFn.createForIterable(windowingStrategy, StringUtf8Coder.of());
    DoFnRunner<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> runner =
        makeRunner(
            outputTag,
            outputManager,
            windowingStrategy,
            fn);

    when(mockTimerInternals.currentInputWatermarkTime()).thenReturn(new Instant(15));

    runner.startBundle();

    WorkItem.Builder workItem1 = WorkItem.newBuilder();
    workItem1.setKey(ByteString.copyFromUtf8(KEY));
    workItem1.setWorkToken(WORK_TOKEN);
    InputMessageBundle.Builder messageBundle = workItem1.addMessageBundlesBuilder();
    messageBundle.setSourceComputationId(SOURCE_COMPUTATION_ID);

    Coder<String> valueCoder = StringUtf8Coder.of();
    addElement(messageBundle,
        Arrays.asList(window(-10, 10), window(0, 20)), new Instant(5), valueCoder, "v1");
    addElement(messageBundle,
        Arrays.asList(window(-10, 10), window(0, 20)), new Instant(2), valueCoder, "v0");
    addElement(messageBundle,
        Arrays.asList(window(0, 20), window(10, 30)), new Instant(15), valueCoder, "v2");

    runner.processElement(createValue(workItem1, valueCoder));

    runner.finishBundle();
    runner.startBundle();

    WorkItem.Builder workItem2 = WorkItem.newBuilder();
    workItem2.setKey(ByteString.copyFromUtf8(KEY));
    workItem2.setWorkToken(WORK_TOKEN);
    addTimer(workItem2, window(-10, 10), new Instant(9), Timer.Type.WATERMARK);
    addTimer(workItem2, window(0, 20), new Instant(19), Timer.Type.WATERMARK);
    addTimer(workItem2, window(10, 30), new Instant(29), Timer.Type.WATERMARK);

    runner.processElement(createValue(workItem2, valueCoder));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(3, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals(KEY, item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.containsInAnyOrder());
    assertEquals(new Instant(9), item0.getTimestamp());
    assertThat(item0.getWindows(), Matchers.<BoundedWindow>contains(window(-10, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals(KEY, item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.containsInAnyOrder("v0", "v1", "v2"));
    // For this sliding window, the minimum output timestmap was 10, since we didn't want to overlap
    // with the previous window that was [-10, 10).
    assertEquals(new Instant(10), item1.getTimestamp());
    assertThat(item1.getWindows(), Matchers.<BoundedWindow>contains(window(0, 20)));

    WindowedValue<KV<String, Iterable<String>>> item2 = result.get(2);
    assertEquals(KEY, item2.getValue().getKey());
    assertThat(item2.getValue().getValue(), Matchers.containsInAnyOrder("v2"));
    assertEquals(new Instant(20), item2.getTimestamp());
    assertThat(item2.getWindows(), Matchers.<BoundedWindow>contains(window(10, 30)));

    assertEquals(
        counters.getExistingCounter("user-merge-DroppedDueToLateness").getAggregate(),
        new Long(2));
  }

  @Test public void testSessions() throws Exception {
    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    DoFnRunnerBase.ListOutputManager outputManager = new DoFnRunnerBase.ListOutputManager();
    DoFnRunner<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> runner = makeRunner(
        outputTag,
        outputManager,
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10))));

    runner.startBundle();

    WorkItem.Builder workItem1 = WorkItem.newBuilder();
    workItem1.setKey(ByteString.copyFromUtf8(KEY));
    workItem1.setWorkToken(WORK_TOKEN);
    InputMessageBundle.Builder messageBundle = workItem1.addMessageBundlesBuilder();
    messageBundle.setSourceComputationId(SOURCE_COMPUTATION_ID);

    Coder<String> valueCoder = StringUtf8Coder.of();
    addElement(messageBundle, Arrays.asList(window(0, 10)), new Instant(0), valueCoder, "v1");
    addElement(messageBundle, Arrays.asList(window(5, 15)), new Instant(5), valueCoder, "v2");
    addElement(messageBundle, Arrays.asList(window(15, 25)), new Instant(15), valueCoder, "v3");
    addElement(messageBundle, Arrays.asList(window(3, 13)), new Instant(3), valueCoder, "v0");

    runner.processElement(createValue(workItem1, valueCoder));

    runner.finishBundle();
    runner.startBundle();

    WorkItem.Builder workItem2 = WorkItem.newBuilder();
    workItem2.setKey(ByteString.copyFromUtf8(KEY));
    workItem2.setWorkToken(WORK_TOKEN);
    // Note that the WATERMARK timer for Instant(9) will have been deleted by
    // ReduceFnRunner when window(0, 10) was merged away.
    addTimer(workItem2, window(0, 15), new Instant(14), Timer.Type.WATERMARK);
    addTimer(workItem2, window(15, 25), new Instant(24), Timer.Type.WATERMARK);

    runner.processElement(createValue(workItem2, valueCoder));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals(KEY, item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.containsInAnyOrder("v0", "v1", "v2"));
    assertEquals(new Instant(0), item0.getTimestamp());
    assertThat(item0.getWindows(), Matchers.<BoundedWindow>contains(window(0, 15)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals(KEY, item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.containsInAnyOrder("v3"));
    assertEquals(new Instant(15), item1.getTimestamp());
    assertThat(item1.getWindows(), Matchers.<BoundedWindow>contains(window(15, 25)));
  }

  /**
   * A custom combine fn that doesn't take any performace shortcuts
   * to ensure that we are using the CombineFn API properly.
   */
  private static class SumLongs extends CombineFn<Long, Long, Long> {
    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long addInput(Long accumulator, Long input) {
      return accumulator + input;
    }

    @Override
    public Long mergeAccumulators(Iterable<Long> accumulators) {
      Long sum = 0L;
      for (Long value : accumulators) {
        sum += value;
      }
      return sum;
    }

    @Override
    public Long extractOutput(Long accumulator) {
      return new Long(accumulator);
    }
  }

  @Test public void testSessionsCombine() throws Exception {
    TupleTag<KV<String, Long>> outputTag = new TupleTag<>();
    CombineFn<Long, ?, Long> combineFn = new SumLongs();
    CoderRegistry registry = new CoderRegistry();
    registry.registerStandardCoders();

    AppliedCombineFn<String, Long, ?, Long> appliedCombineFn = AppliedCombineFn.withInputCoder(
        combineFn.asKeyedFn(), registry, KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()));

    DoFnRunnerBase.ListOutputManager outputManager = new DoFnRunnerBase.ListOutputManager();
    DoFnRunner<KeyedWorkItem<String, Long>, KV<String, Long>> runner = makeRunner(
        outputTag,
        outputManager,
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10))),
        appliedCombineFn);

    runner.startBundle();

    WorkItem.Builder workItem1 = WorkItem.newBuilder();
    workItem1.setKey(ByteString.copyFromUtf8(KEY));
    workItem1.setWorkToken(WORK_TOKEN);
    InputMessageBundle.Builder messageBundle = workItem1.addMessageBundlesBuilder();
    messageBundle.setSourceComputationId(SOURCE_COMPUTATION_ID);

    Coder<Long> valueCoder = BigEndianLongCoder.of();
    addElement(messageBundle, Arrays.asList(window(0, 10)), new Instant(0), valueCoder, 1L);
    addElement(messageBundle, Arrays.asList(window(5, 15)), new Instant(5), valueCoder, 2L);
    addElement(messageBundle, Arrays.asList(window(15, 25)), new Instant(15), valueCoder, 3L);
    addElement(messageBundle, Arrays.asList(window(3, 13)), new Instant(3), valueCoder, 4L);

    runner.processElement(createValue(workItem1, valueCoder));

    runner.finishBundle();
    runner.startBundle();

    WorkItem.Builder workItem2 = WorkItem.newBuilder();
    workItem2.setKey(ByteString.copyFromUtf8(KEY));
    workItem2.setWorkToken(WORK_TOKEN);
    // Note that the WATERMARK timer for Instant(9) will have been deleted by
    // ReduceFnRunner when window(0, 10) was merged away.
    addTimer(workItem2, window(0, 15), new Instant(14), Timer.Type.WATERMARK);
    addTimer(workItem2, window(15, 25), new Instant(24), Timer.Type.WATERMARK);

    runner.processElement(createValue(workItem2, valueCoder));

    runner.finishBundle();

    List<WindowedValue<KV<String, Long>>> result = outputManager.getOutput(outputTag);

    assertEquals(2, result.size());

    WindowedValue<KV<String, Long>> item0 = result.get(0);
    assertEquals(KEY, item0.getValue().getKey());
    assertEquals((Long) 7L, item0.getValue().getValue());
    assertEquals(new Instant(0), item0.getTimestamp());
    assertThat(item0.getWindows(), Matchers.<BoundedWindow>contains(window(0, 15)));

    WindowedValue<KV<String, Long>> item1 = result.get(1);
    assertEquals(KEY, item1.getValue().getKey());
    assertEquals((Long) 3L, item1.getValue().getValue());
    assertEquals(new Instant(15), item1.getTimestamp());
    assertThat(item1.getWindows(), Matchers.<BoundedWindow>contains(window(15, 25)));
  }

  private DoFnRunner<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> makeRunner(
          TupleTag<KV<String, Iterable<String>>> outputTag,
          DoFnRunners.OutputManager outputManager,
          WindowingStrategy<? super String, IntervalWindow> windowingStrategy) throws Exception {

    DoFn<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> fn =
        StreamingGroupAlsoByWindowsDoFn.createForIterable(windowingStrategy, StringUtf8Coder.of());

    return makeRunner(outputTag, outputManager, windowingStrategy, fn);
  }

  private DoFnRunner<KeyedWorkItem<String, Long>, KV<String, Long>> makeRunner(
          TupleTag<KV<String, Long>> outputTag,
          DoFnRunners.OutputManager outputManager,
          WindowingStrategy<? super String, IntervalWindow> windowingStrategy,
          AppliedCombineFn<String, Long, ?, Long> combineFn) throws Exception {

    DoFn<KeyedWorkItem<String, Long>, KV<String, Long>> fn =
        StreamingGroupAlsoByWindowsDoFn.create(windowingStrategy, combineFn, StringUtf8Coder.of());

    return makeRunner(outputTag, outputManager, windowingStrategy, fn);
  }

  private <InputT, OutputT>
      DoFnRunner<KeyedWorkItem<String, InputT>, KV<String, OutputT>> makeRunner(
          TupleTag<KV<String, OutputT>> outputTag,
          DoFnRunners.OutputManager outputManager,
          WindowingStrategy<? super String, IntervalWindow> windowingStrategy,
          DoFn<KeyedWorkItem<String, InputT>, KV<String, OutputT>> fn) throws Exception {
    DoFnInfo<KeyedWorkItem<String, InputT>, KV<String, OutputT>> doFnInfo =
        new DoFnInfo<>(fn, windowingStrategy);

    return DoFnRunners.lateDataDroppingRunner(
        PipelineOptionsFactory.create(),
        doFnInfo,
        NullSideInputReader.empty(),
        outputManager,
        outputTag,
        new ArrayList<TupleTag<?>>(),
        execContext.getOrCreateStepContext("merge", "merge", null),
        counters.getAddCounterMutator());
  }

  private IntervalWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }
}
