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
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.worker.util.ListOutputManager;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link StreamingKeyedWorkItemSideInputDoFnRunner}. */
@RunWith(JUnit4.class)
public class StreamingKeyedWorkItemSideInputDoFnRunnerTest {
  private static final FixedWindows WINDOW_FN = FixedWindows.of(Duration.millis(10));
  private static TupleTag<KV<String, Integer>> mainOutputTag = new TupleTag<>();

  private final InMemoryStateInternals<String> state = InMemoryStateInternals.forKey("dummyKey");

  @Mock private StreamingModeExecutionContext.StepContext stepContext;
  @Mock private StreamingSideInputFetcher<Integer, IntervalWindow> sideInputFetcher;
  @Mock private SideInputReader mockSideInputReader;
  @Mock private TimerInternals mockTimerInternals;
  @Mock private BagState<WindowedValue<Integer>> elemsBag;
  @Mock private BagState<TimerData> timersBag;

  // Suppressing the rawtype cast to StateInternals. Because Mockito causes a covariant ?
  // to become a contravariant ?, it is not possible to cast state to an appropriate type
  // without rawtypes.
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(stepContext.stateInternals()).thenReturn((StateInternals) state);
    when(stepContext.timerInternals()).thenReturn(mockTimerInternals);
  }

  @Test
  public void testInvokeProcessElement() throws Exception {
    when(sideInputFetcher.storeIfBlocked(Matchers.<WindowedValue<Integer>>any()))
        .thenReturn(false, true, false)
        .thenThrow(new RuntimeException("Does not expect more calls"));
    when(mockTimerInternals.currentInputWatermarkTime()).thenReturn(new Instant(15L));
    ListOutputManager outputManager = new ListOutputManager();
    StreamingKeyedWorkItemSideInputDoFnRunner<String, Integer, KV<String, Integer>, IntervalWindow>
        runner = createRunner(outputManager);

    KeyedWorkItem<String, Integer> elemsWorkItem =
        KeyedWorkItems.elementsWorkItem(
            "a",
            ImmutableList.of(
                createDatum(13, 13L),
                createDatum(16, 16L), // side inputs non-ready element
                createDatum(18, 18L)));

    runner.processElement(new ValueInEmptyWindows<>(elemsWorkItem));

    when(mockTimerInternals.currentInputWatermarkTime()).thenReturn(new Instant(20));
    runner.processElement(
        new ValueInEmptyWindows<>(
            KeyedWorkItems.<String, Integer>timersWorkItem(
                "a",
                ImmutableList.of(
                    timerData(window(10, 20), new Instant(19), Timer.Type.WATERMARK)))));

    List<WindowedValue<KV<String, Integer>>> result = outputManager.getOutput(mainOutputTag);
    assertEquals(1, result.size());
    WindowedValue<KV<String, Integer>> item0 = result.get(0);
    assertEquals("a", item0.getValue().getKey());
    assertEquals(31, item0.getValue().getValue().intValue());
    assertEquals("a", runner.keyValue().read());
  }

  @Test
  public void testStartBundle() throws Exception {
    ListOutputManager outputManager = new ListOutputManager();
    StreamingKeyedWorkItemSideInputDoFnRunner<String, Integer, KV<String, Integer>, IntervalWindow>
        runner = createRunner(outputManager);

    runner.keyValue().write("a");
    Set<IntervalWindow> readyWindows = ImmutableSet.of(window(10, 20));
    when(sideInputFetcher.getReadyWindows()).thenReturn(readyWindows);
    when(sideInputFetcher.prefetchElements(readyWindows)).thenReturn(ImmutableList.of(elemsBag));
    when(sideInputFetcher.prefetchTimers(readyWindows)).thenReturn(ImmutableList.of(timersBag));
    when(elemsBag.read()).thenReturn(ImmutableList.of(createDatum(13, 13L), createDatum(18, 18L)));
    when(timersBag.read())
        .thenReturn(
            ImmutableList.of(timerData(window(10, 20), new Instant(19), Timer.Type.WATERMARK)));
    when(mockTimerInternals.currentInputWatermarkTime()).thenReturn(new Instant(20));

    runner.startBundle();

    List<WindowedValue<KV<String, Integer>>> result = outputManager.getOutput(mainOutputTag);
    assertEquals(1, result.size());
    WindowedValue<KV<String, Integer>> item0 = result.get(0);
    assertEquals("a", item0.getValue().getKey());
    assertEquals(31, item0.getValue().getValue().intValue());
  }

  private <T> WindowedValue<T> createDatum(T element, long timestampMillis) {
    Instant timestamp = new Instant(timestampMillis);
    return WindowedValue.of(
        element, timestamp, Arrays.asList(WINDOW_FN.assignWindow(timestamp)), PaneInfo.NO_FIRING);
  }

  private TimerData timerData(IntervalWindow window, Instant timestamp, Timer.Type type) {
    return TimerData.of(
        StateNamespaces.window(IntervalWindow.getCoder(), window),
        timestamp,
        timestamp,
        type == Windmill.Timer.Type.WATERMARK ? TimeDomain.EVENT_TIME : TimeDomain.PROCESSING_TIME);
  }

  private IntervalWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }

  @SuppressWarnings("unchecked")
  private StreamingKeyedWorkItemSideInputDoFnRunner<
          String, Integer, KV<String, Integer>, IntervalWindow>
      createRunner(DoFnRunners.OutputManager outputManager) throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    Coder<String> keyCoder = StringUtf8Coder.of();
    Coder<Integer> inputCoder = BigEndianIntegerCoder.of();

    AppliedCombineFn<String, Integer, ?, Integer> combineFn =
        AppliedCombineFn.withInputCoder(
            Sum.ofIntegers(), registry, KvCoder.of(keyCoder, inputCoder));

    WindowingStrategy<Object, IntervalWindow> windowingStrategy = WindowingStrategy.of(WINDOW_FN);
    @SuppressWarnings("rawtypes")
    StreamingGroupAlsoByWindowViaWindowSetFn doFn =
        (StreamingGroupAlsoByWindowViaWindowSetFn)
            StreamingGroupAlsoByWindowsDoFns.create(
                windowingStrategy, key -> state, combineFn, keyCoder);

    DoFnRunner<KeyedWorkItem<String, Integer>, KV<String, Integer>> simpleDoFnRunner =
        new GroupAlsoByWindowFnRunner<>(
            PipelineOptionsFactory.create(),
            doFn.asDoFn(),
            mockSideInputReader,
            outputManager,
            mainOutputTag,
            stepContext);
    return new StreamingKeyedWorkItemSideInputDoFnRunner<
        String, Integer, KV<String, Integer>, IntervalWindow>(
        simpleDoFnRunner, keyCoder, sideInputFetcher, stepContext);
  }
}
