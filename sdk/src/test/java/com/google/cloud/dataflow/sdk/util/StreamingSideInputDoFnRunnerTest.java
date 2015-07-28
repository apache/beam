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

package com.google.cloud.dataflow.sdk.util;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.GlobalDataRequest;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.StateFetcher.SideInputState;
import com.google.cloud.dataflow.sdk.util.state.InMemoryStateInternals;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.cloud.dataflow.sdk.util.state.ValueState;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Unit tests for {@link StreamingSideInputDoFnRunner}. */
@RunWith(JUnit4.class)
public class StreamingSideInputDoFnRunnerTest {

  private static final FixedWindows WINDOW_FN = FixedWindows.of(Duration.millis(10));

  static TupleTag<String> mainOutputTag = new TupleTag<String>();
  @Mock StreamingModeExecutionContext execContext;
  @Mock StreamingModeExecutionContext.StepContext stepContext;
  @Mock SideInputReader mockSideInputReader;

  private final InMemoryStateInternals state = new InMemoryStateInternals();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(stepContext.getExecutionContext()).thenReturn(execContext);
    when(stepContext.stateInternals()).thenReturn(state);
  }

  @Test
  public void testSideInputReady() throws Exception {
    PCollectionView<String> view = createView();

    when(execContext.getSideInputNotifications())
        .thenReturn(Arrays.<Windmill.GlobalDataId>asList());
    when(stepContext.issueSideInputFetch(
             eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(true);
    when(execContext.getSideInputReaderForViews(
        Mockito.<Iterable<? extends PCollectionView<?>>>any())).thenReturn(mockSideInputReader);
    when(mockSideInputReader.contains(eq(view))).thenReturn(true);
    when(mockSideInputReader.get(eq(view), any(BoundedWindow.class))).thenReturn("data");

    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    StreamingSideInputDoFnRunner<String, String, IntervalWindow> runner =
        createRunner(outputManager, Arrays.asList(view));

    runner.startBundle();
    runner.processElement(createDatum("e", 0));
    runner.finishBundle();

    assertThat(outputManager.getOutput(mainOutputTag), contains(createDatum("e:data", 0)));
  }

  @Test
  public void testSideInputNotReady() throws Exception {
    PCollectionView<String> view = createView();

    when(execContext.getSideInputNotifications())
        .thenReturn(Arrays.<Windmill.GlobalDataId>asList());
    when(stepContext.issueSideInputFetch(
             eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(false);

    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    StreamingSideInputDoFnRunner<String, String, IntervalWindow> runner =
        createRunner(outputManager, Arrays.asList(view));

    runner.startBundle();
    runner.processElement(createDatum("e", 0));
    runner.finishBundle();

    assertTrue(outputManager.getOutput(mainOutputTag).isEmpty());

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    // Verify that we added the element to an appropriate tag list, and that we buffered the element
    ValueState<Map<IntervalWindow, Set<GlobalDataRequest>>> blockedMapState =
        state.state(StateNamespaces.global(),
            StreamingSideInputDoFnRunner.blockedMapAddr(WINDOW_FN));
    assertEquals(
        blockedMapState.get().read(),
        Collections.singletonMap(
            window,
            Collections.singleton(Windmill.GlobalDataRequest.newBuilder()
                .setDataId(Windmill.GlobalDataId.newBuilder()
                    .setTag(view.getTagInternal().getId())
                    .setVersion(ByteString.copyFrom(CoderUtils.encodeToByteArray(
                        IntervalWindow.getCoder(), window)))
                        .build())
                        .setExistenceWatermarkDeadline(9000)
                        .build())));
    assertThat(runner.elementBag(createWindow(0)).get().read(),
        Matchers.contains(createDatum("e", 0)));
    assertEquals(runner.watermarkHold(createWindow(0)).get().read(), new Instant(0));
  }

  @Test
  public void testSideInputNotification() throws Exception {
    PCollectionView<String> view = createView();

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    Windmill.GlobalDataId id = Windmill.GlobalDataId.newBuilder()
        .setTag(view.getTagInternal().getId())
        .setVersion(ByteString.copyFrom(CoderUtils.encodeToByteArray(
            IntervalWindow.getCoder(), window)))
        .build();

    Set<Windmill.GlobalDataRequest> requestSet = new HashSet<>();
    requestSet.add(Windmill.GlobalDataRequest.newBuilder().setDataId(id).build());
    Map<IntervalWindow, Set<Windmill.GlobalDataRequest>> blockedMap = new HashMap<>();
    blockedMap.put(window, requestSet);

    ValueState<Map<IntervalWindow, Set<GlobalDataRequest>>> blockedMapState =
        state.state(StateNamespaces.global(),
            StreamingSideInputDoFnRunner.blockedMapAddr(WINDOW_FN));
    blockedMapState.set(blockedMap);

    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    StreamingSideInputDoFnRunner<String, String, IntervalWindow> runner =
        createRunner(outputManager, Arrays.asList(view));
    runner.watermarkHold(createWindow(0)).add(new Instant(0));
    runner.elementBag(createWindow(0)).add(createDatum("e", 0));

    when(execContext.getSideInputNotifications()).thenReturn(Arrays.asList(id));
    when(stepContext.issueSideInputFetch(
             eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(false);
    when(stepContext.issueSideInputFetch(
             eq(view), any(BoundedWindow.class), eq(SideInputState.KNOWN_READY)))
        .thenReturn(true);
    when(execContext.getSideInputReaderForViews(
        Mockito.<Iterable<? extends PCollectionView<?>>>any())).thenReturn(mockSideInputReader);
    when(mockSideInputReader.contains(eq(view))).thenReturn(true);
    when(mockSideInputReader.get(eq(view), any(BoundedWindow.class))).thenReturn("data");

    runner.startBundle();
    runner.finishBundle();

    assertThat(outputManager.getOutput(mainOutputTag), contains(createDatum("e:data", 0)));

    assertThat(blockedMapState.get().read().keySet(), Matchers.empty());
    assertThat(runner.watermarkHold(createWindow(0)).get().read(), Matchers.nullValue());
    assertThat(runner.elementBag(createWindow(0)).get().read(), Matchers.emptyIterable());
  }

  @Test
  public void testMultipleSideInputs() throws Exception {
    PCollectionView<String> view1 = createView();
    PCollectionView<String> view2 = createView();

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    Windmill.GlobalDataId id = Windmill.GlobalDataId.newBuilder()
        .setTag(view1.getTagInternal().getId())
        .setVersion(ByteString.copyFrom(CoderUtils.encodeToByteArray(
            IntervalWindow.getCoder(), window)))
        .build();

    Set<Windmill.GlobalDataRequest> requestSet = new HashSet<>();
    requestSet.add(Windmill.GlobalDataRequest.newBuilder().setDataId(id).build());
    Map<IntervalWindow, Set<Windmill.GlobalDataRequest>> blockedMap = new HashMap<>();
    blockedMap.put(window, requestSet);

    ValueState<Map<IntervalWindow, Set<GlobalDataRequest>>> blockedMapState =
        state.state(StateNamespaces.global(),
            StreamingSideInputDoFnRunner.blockedMapAddr(WINDOW_FN));
    blockedMapState.set(blockedMap);

    when(execContext.getSideInputNotifications()).thenReturn(Arrays.asList(id));
    when(stepContext.issueSideInputFetch(
             any(PCollectionView.class), any(BoundedWindow.class), any(SideInputState.class)))
        .thenReturn(true);
    when(execContext.getSideInputReaderForViews(
        Mockito.<Iterable<? extends PCollectionView<?>>>any())).thenReturn(mockSideInputReader);
    when(mockSideInputReader.contains(eq(view1))).thenReturn(true);
    when(mockSideInputReader.contains(eq(view2))).thenReturn(true);
    when(mockSideInputReader.get(eq(view1), any(BoundedWindow.class))).thenReturn("data1");
    when(mockSideInputReader.get(eq(view2), any(BoundedWindow.class))).thenReturn("data2");

    DoFnRunner.ListOutputManager outputManager = new DoFnRunner.ListOutputManager();
    StreamingSideInputDoFnRunner<String, String, IntervalWindow> runner =
        createRunner(outputManager, Arrays.asList(view1, view2));
    runner.watermarkHold(createWindow(0)).add(new Instant(0));
    runner.elementBag(createWindow(0)).add(createDatum("e1", 0));

    runner.startBundle();
    runner.processElement(createDatum("e2", 2));
    runner.finishBundle();

    assertThat(outputManager.getOutput(mainOutputTag),
        contains(createDatum("e1:data1:data2", 0), createDatum("e2:data1:data2", 2)));

    assertThat(blockedMapState.get().read().keySet(), Matchers.empty());
    assertThat(runner.watermarkHold(createWindow(0)).get().read(), Matchers.nullValue());
    assertThat(runner.elementBag(createWindow(0)).get().read(), Matchers.emptyIterable());
  }

  private <ReceiverT> StreamingSideInputDoFnRunner<String, String, IntervalWindow>
      createRunner(DoFnRunner.OutputManager outputManager, List<PCollectionView<String>> views)
          throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Iterable<PCollectionView<?>> typedViews = (Iterable) views;

    DoFnInfo<String, String> doFnInfo = new DoFnInfo<String, String>(
        new SideInputFn(views), WindowingStrategy.of(WINDOW_FN),
        typedViews, StringUtf8Coder.of());

    return new StreamingSideInputDoFnRunner<String, String, IntervalWindow>(
        PipelineOptionsFactory.create(),
        doFnInfo,
        mockSideInputReader,
        outputManager,
        mainOutputTag,
        Arrays.<TupleTag<?>>asList(),
        stepContext,
        null);
  }

  private static class SideInputFn extends DoFn<String, String> {
    private static final long serialVersionUID = 0;

    private List<PCollectionView<String>> views;

    public SideInputFn(List<PCollectionView<String>> views) {
      this.views = views;
    }

    @Override
    public void processElement(ProcessContext c) {
      String output = c.element();
      for (PCollectionView<String> view : views) {
        output += ":" + c.sideInput(view);
      }
      c.output(output);
    }
  }

  private PCollectionView<String> createView() {
    return TestPipeline.create()
        .apply(Create.<String>of().withCoder(StringUtf8Coder.of()))
        .apply(Window.<String>into(WINDOW_FN))
        .apply(View.<String>asSingleton());
  }

  private WindowedValue<String> createDatum(String element, long timestamp) {
    return WindowedValue.of(
        element,
        new Instant(timestamp),
        Arrays.asList(createWindow(timestamp)),
        PaneInfo.NO_FIRING);
  }

  private IntervalWindow createWindow(long timestamp) {
    return new IntervalWindow(
        new Instant(timestamp - timestamp % 10),
        new Instant(timestamp - timestamp % 10 + 10));
  }
}
