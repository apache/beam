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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputState;
import org.apache.beam.runners.dataflow.worker.util.ListOutputManager;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
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

/** Unit tests for {@link StreamingSideInputDoFnRunner}. */
@RunWith(JUnit4.class)
public class StreamingSideInputDoFnRunnerTest {

  private static final FixedWindows WINDOW_FN = FixedWindows.of(Duration.millis(10));

  static TupleTag<String> mainOutputTag = new TupleTag<>();
  @Mock StreamingModeExecutionContext execContext;
  @Mock StreamingModeExecutionContext.StepContext stepContext;
  @Mock SideInputReader mockSideInputReader;

  private final InMemoryStateInternals<String> state = InMemoryStateInternals.forKey("dummyKey");

  // Mockito causes the covariant ? in StepContext#stateInternals to become contravariant
  // because it is accepted as a parameter to #thenReturn. It is completely safe to
  // treat InMemoryStateInternals<String> as StateInternals<?>.
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(stepContext.stateInternals()).thenReturn((StateInternals) state);
  }

  @Test
  public void testSideInputReady() throws Exception {
    PCollectionView<String> view = createView();

    when(stepContext.getSideInputNotifications())
        .thenReturn(Arrays.<Windmill.GlobalDataId>asList());
    when(stepContext.issueSideInputFetch(
            eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(true);
    when(execContext.getSideInputReaderForViews(
            Mockito.<Iterable<? extends PCollectionView<?>>>any()))
        .thenReturn(mockSideInputReader);
    when(mockSideInputReader.contains(eq(view))).thenReturn(true);
    when(mockSideInputReader.get(eq(view), any(BoundedWindow.class))).thenReturn("data");

    ListOutputManager outputManager = new ListOutputManager();
    List<PCollectionView<String>> views = Arrays.asList(view);
    StreamingSideInputFetcher<String, IntervalWindow> sideInputFetcher = createFetcher(views);
    StreamingSideInputDoFnRunner<String, String, IntervalWindow> runner =
        createRunner(outputManager, views, sideInputFetcher);

    runner.startBundle();
    runner.processElement(createDatum("e", 0));
    runner.finishBundle();

    assertThat(outputManager.getOutput(mainOutputTag), contains(createDatum("e:data", 0)));
  }

  @Test
  public void testSideInputNotReady() throws Exception {
    PCollectionView<String> view = createView();

    when(stepContext.getSideInputNotifications())
        .thenReturn(Arrays.<Windmill.GlobalDataId>asList());
    when(stepContext.issueSideInputFetch(
            eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(false);

    ListOutputManager outputManager = new ListOutputManager();

    List<PCollectionView<String>> views = Arrays.asList(view);
    StreamingSideInputFetcher<String, IntervalWindow> sideInputFetcher = createFetcher(views);
    StreamingSideInputDoFnRunner<String, String, IntervalWindow> runner =
        createRunner(outputManager, views, sideInputFetcher);

    runner.startBundle();
    runner.processElement(createDatum("e", 0));
    runner.finishBundle();

    assertTrue(outputManager.getOutput(mainOutputTag).isEmpty());

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    // Verify that we added the element to an appropriate tag list, and that we buffered the element
    ValueState<Map<IntervalWindow, Set<GlobalDataRequest>>> blockedMapState =
        state.state(
            StateNamespaces.global(),
            StreamingSideInputFetcher.blockedMapAddr(WINDOW_FN.windowCoder()));
    assertEquals(
        blockedMapState.read(),
        Collections.singletonMap(
            window,
            Collections.singleton(
                Windmill.GlobalDataRequest.newBuilder()
                    .setDataId(
                        Windmill.GlobalDataId.newBuilder()
                            .setTag(view.getTagInternal().getId())
                            .setVersion(
                                ByteString.copyFrom(
                                    CoderUtils.encodeToByteArray(
                                        IntervalWindow.getCoder(), window)))
                            .build())
                    .setExistenceWatermarkDeadline(9000)
                    .build())));
    assertThat(
        sideInputFetcher.elementBag(createWindow(0)).read(),
        Matchers.contains(createDatum("e", 0)));
    assertEquals(sideInputFetcher.watermarkHold(createWindow(0)).read(), new Instant(0));
  }

  @Test
  public void testMultipleWindowsNotReady() throws Exception {
    PCollectionView<String> view = createView();

    when(stepContext.getSideInputNotifications())
        .thenReturn(Arrays.<Windmill.GlobalDataId>asList());
    when(stepContext.issueSideInputFetch(
            eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(false);

    ListOutputManager outputManager = new ListOutputManager();

    List<PCollectionView<String>> views = Arrays.asList(view);
    StreamingSideInputFetcher<String, IntervalWindow> sideInputFetcher = createFetcher(views);
    StreamingSideInputDoFnRunner<String, String, IntervalWindow> runner =
        createRunner(
            SlidingWindows.of(Duration.millis(10)).every(Duration.millis(10)),
            outputManager,
            views,
            sideInputFetcher);

    IntervalWindow window1 = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow window2 = new IntervalWindow(new Instant(-5), new Instant(5));
    long timestamp = 1L;

    WindowedValue<String> elem =
        WindowedValue.of(
            "e", new Instant(timestamp), Arrays.asList(window1, window2), PaneInfo.NO_FIRING);

    runner.startBundle();
    runner.processElement(elem);
    runner.finishBundle();

    assertTrue(outputManager.getOutput(mainOutputTag).isEmpty());

    // Verify that we added the element to an appropriate tag list, and that we buffered the element
    // in both windows separately
    ValueState<Map<IntervalWindow, Set<GlobalDataRequest>>> blockedMapState =
        state.state(
            StateNamespaces.global(),
            StreamingSideInputFetcher.blockedMapAddr(WINDOW_FN.windowCoder()));

    Map<IntervalWindow, Set<GlobalDataRequest>> blockedMap = blockedMapState.read();

    assertThat(
        blockedMap.get(window1),
        equalTo(
            Collections.singleton(
                Windmill.GlobalDataRequest.newBuilder()
                    .setDataId(
                        Windmill.GlobalDataId.newBuilder()
                            .setTag(view.getTagInternal().getId())
                            .setVersion(
                                ByteString.copyFrom(
                                    CoderUtils.encodeToByteArray(
                                        IntervalWindow.getCoder(), window1)))
                            .build())
                    .setExistenceWatermarkDeadline(9000)
                    .build())));

    assertThat(
        blockedMap.get(window2),
        equalTo(
            Collections.singleton(
                Windmill.GlobalDataRequest.newBuilder()
                    .setDataId(
                        Windmill.GlobalDataId.newBuilder()
                            .setTag(view.getTagInternal().getId())
                            .setVersion(
                                ByteString.copyFrom(
                                    CoderUtils.encodeToByteArray(
                                        IntervalWindow.getCoder(), window1)))
                            .build())
                    .setExistenceWatermarkDeadline(9000)
                    .build())));

    assertThat(
        sideInputFetcher.elementBag(window1).read(),
        contains(Iterables.get(elem.explodeWindows(), 0)));

    assertThat(
        sideInputFetcher.elementBag(window2).read(),
        contains(Iterables.get(elem.explodeWindows(), 1)));

    assertEquals(sideInputFetcher.watermarkHold(window1).read(), new Instant(timestamp));
    assertEquals(sideInputFetcher.watermarkHold(window2).read(), new Instant(timestamp));
  }

  @Test
  public void testSideInputNotification() throws Exception {
    PCollectionView<String> view = createView();

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    Windmill.GlobalDataId id =
        Windmill.GlobalDataId.newBuilder()
            .setTag(view.getTagInternal().getId())
            .setVersion(
                ByteString.copyFrom(
                    CoderUtils.encodeToByteArray(IntervalWindow.getCoder(), window)))
            .build();

    Set<Windmill.GlobalDataRequest> requestSet = new HashSet<>();
    requestSet.add(Windmill.GlobalDataRequest.newBuilder().setDataId(id).build());
    Map<IntervalWindow, Set<Windmill.GlobalDataRequest>> blockedMap = new HashMap<>();
    blockedMap.put(window, requestSet);

    ValueState<Map<IntervalWindow, Set<GlobalDataRequest>>> blockedMapState =
        state.state(
            StateNamespaces.global(),
            StreamingSideInputFetcher.blockedMapAddr(WINDOW_FN.windowCoder()));
    blockedMapState.write(blockedMap);

    ListOutputManager outputManager = new ListOutputManager();
    List<PCollectionView<String>> views = Arrays.asList(view);
    StreamingSideInputFetcher<String, IntervalWindow> sideInputFetcher = createFetcher(views);
    StreamingSideInputDoFnRunner<String, String, IntervalWindow> runner =
        createRunner(outputManager, views, sideInputFetcher);
    sideInputFetcher.watermarkHold(createWindow(0)).add(new Instant(0));
    sideInputFetcher.elementBag(createWindow(0)).add(createDatum("e", 0));

    when(stepContext.getSideInputNotifications()).thenReturn(Arrays.asList(id));
    when(stepContext.issueSideInputFetch(
            eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(false);
    when(stepContext.issueSideInputFetch(
            eq(view), any(BoundedWindow.class), eq(SideInputState.KNOWN_READY)))
        .thenReturn(true);
    when(execContext.getSideInputReaderForViews(
            Mockito.<Iterable<? extends PCollectionView<?>>>any()))
        .thenReturn(mockSideInputReader);
    when(mockSideInputReader.contains(eq(view))).thenReturn(true);
    when(mockSideInputReader.get(eq(view), any(BoundedWindow.class))).thenReturn("data");

    runner.startBundle();
    runner.finishBundle();

    assertThat(outputManager.getOutput(mainOutputTag), contains(createDatum("e:data", 0)));

    assertThat(blockedMapState.read(), Matchers.nullValue());
    assertThat(sideInputFetcher.watermarkHold(createWindow(0)).read(), Matchers.nullValue());
    assertThat(sideInputFetcher.elementBag(createWindow(0)).read(), Matchers.emptyIterable());
  }

  @Test
  public void testMultipleSideInputs() throws Exception {
    PCollectionView<String> view1 = createView();
    PCollectionView<String> view2 = createView();

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    Windmill.GlobalDataId id =
        Windmill.GlobalDataId.newBuilder()
            .setTag(view1.getTagInternal().getId())
            .setVersion(
                ByteString.copyFrom(
                    CoderUtils.encodeToByteArray(IntervalWindow.getCoder(), window)))
            .build();

    Set<Windmill.GlobalDataRequest> requestSet = new HashSet<>();
    requestSet.add(Windmill.GlobalDataRequest.newBuilder().setDataId(id).build());
    Map<IntervalWindow, Set<Windmill.GlobalDataRequest>> blockedMap = new HashMap<>();
    blockedMap.put(window, requestSet);

    ValueState<Map<IntervalWindow, Set<GlobalDataRequest>>> blockedMapState =
        state.state(
            StateNamespaces.global(),
            StreamingSideInputFetcher.blockedMapAddr(WINDOW_FN.windowCoder()));
    blockedMapState.write(blockedMap);

    when(stepContext.getSideInputNotifications()).thenReturn(Arrays.asList(id));
    when(stepContext.issueSideInputFetch(
            any(PCollectionView.class), any(BoundedWindow.class), any(SideInputState.class)))
        .thenReturn(true);
    when(execContext.getSideInputReaderForViews(
            Mockito.<Iterable<? extends PCollectionView<?>>>any()))
        .thenReturn(mockSideInputReader);
    when(mockSideInputReader.contains(eq(view1))).thenReturn(true);
    when(mockSideInputReader.contains(eq(view2))).thenReturn(true);
    when(mockSideInputReader.get(eq(view1), any(BoundedWindow.class))).thenReturn("data1");
    when(mockSideInputReader.get(eq(view2), any(BoundedWindow.class))).thenReturn("data2");

    ListOutputManager outputManager = new ListOutputManager();
    List<PCollectionView<String>> views = Arrays.asList(view1, view2);
    StreamingSideInputFetcher<String, IntervalWindow> sideInputFetcher = createFetcher(views);
    StreamingSideInputDoFnRunner<String, String, IntervalWindow> runner =
        createRunner(outputManager, views, sideInputFetcher);
    sideInputFetcher.watermarkHold(createWindow(0)).add(new Instant(0));
    sideInputFetcher.elementBag(createWindow(0)).add(createDatum("e1", 0));

    runner.startBundle();
    runner.processElement(createDatum("e2", 2));
    runner.finishBundle();

    assertThat(
        outputManager.getOutput(mainOutputTag),
        contains(createDatum("e1:data1:data2", 0), createDatum("e2:data1:data2", 2)));

    assertThat(blockedMapState.read(), Matchers.nullValue());
    assertThat(sideInputFetcher.watermarkHold(createWindow(0)).read(), Matchers.nullValue());
    assertThat(sideInputFetcher.elementBag(createWindow(0)).read(), Matchers.emptyIterable());
  }

  @SuppressWarnings("unchecked")
  private <ReceiverT> StreamingSideInputDoFnRunner<String, String, IntervalWindow> createRunner(
      DoFnRunners.OutputManager outputManager,
      List<PCollectionView<String>> views,
      StreamingSideInputFetcher<String, IntervalWindow> sideInputFetcher)
      throws Exception {
    return createRunner(WINDOW_FN, outputManager, views, sideInputFetcher);
  }

  @SuppressWarnings("unchecked")
  private <ReceiverT> StreamingSideInputDoFnRunner<String, String, IntervalWindow> createRunner(
      WindowFn<?, ?> windowFn,
      DoFnRunners.OutputManager outputManager,
      List<PCollectionView<String>> views,
      StreamingSideInputFetcher<String, IntervalWindow> sideInputFetcher)
      throws Exception {
    DoFnRunner<String, String> simpleDoFnRunner =
        DoFnRunners.simpleRunner(
            PipelineOptionsFactory.create(),
            new SideInputFn(views),
            mockSideInputReader,
            outputManager,
            mainOutputTag,
            Arrays.<TupleTag<?>>asList(),
            stepContext,
            null,
            Collections.emptyMap(),
            WindowingStrategy.of(windowFn),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    return new StreamingSideInputDoFnRunner<>(simpleDoFnRunner, sideInputFetcher);
  }

  private <ReceiverT> StreamingSideInputFetcher<String, IntervalWindow> createFetcher(
      List<PCollectionView<String>> views) throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Iterable<PCollectionView<?>> typedViews = (Iterable) views;

    return new StreamingSideInputFetcher<>(
        typedViews, StringUtf8Coder.of(), WindowingStrategy.of(WINDOW_FN), stepContext);
  }

  private static class SideInputFn extends DoFn<String, String> {
    private List<PCollectionView<String>> views;

    public SideInputFn(List<PCollectionView<String>> views) {
      this.views = views;
    }

    @ProcessElement
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
        .apply(Create.empty(StringUtf8Coder.of()))
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
        new Instant(timestamp - timestamp % 10), new Instant(timestamp - timestamp % 10 + 10));
  }
}
