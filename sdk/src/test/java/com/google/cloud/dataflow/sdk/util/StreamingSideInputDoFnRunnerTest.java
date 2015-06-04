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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.StateFetcher.SideInputState;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.protobuf.ByteString;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Unit tests for {@link StreamingSideInputDoFnRunner}. */
@RunWith(JUnit4.class)
public class StreamingSideInputDoFnRunnerTest {

  static TupleTag<String> mainOutputTag = new TupleTag<String>();
  @Mock StreamingModeExecutionContext execContext;
  @Mock ExecutionContext.StepContext stepContext;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(stepContext.getExecutionContext()).thenReturn(execContext);
  }

  @Test
  public void testSideInputReady() throws Exception {
    PCollectionView<String> view = createView();

    when(stepContext.lookup(any(CodedTupleTag.class))).thenReturn(new HashMap());
    when(execContext.getSideInputNotifications())
        .thenReturn(Arrays.<Windmill.GlobalDataId>asList());
    when(execContext.issueSideInputFetch(
             eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(true);
    when(execContext.getSideInput(eq(view), any(BoundedWindow.class), any(PTuple.class)))
        .thenReturn("data");

    StreamingSideInputDoFnRunner<String, String, List, IntervalWindow> runner =
        createRunner(Arrays.asList(view));

    runner.startBundle();
    runner.processElement(createDatum("e", 0));
    runner.finishBundle();

    assertThat((List<WindowedValue<String>>) runner.getReceiver(mainOutputTag),
        contains(createDatum("e:data", 0)));
  }

  @Test
  public void testSideInputNotReady() throws Exception {
    PCollectionView<String> view = createView();

    when(stepContext.lookup(any(CodedTupleTag.class))).thenReturn(new HashMap());
    when(execContext.getSideInputNotifications())
        .thenReturn(Arrays.<Windmill.GlobalDataId>asList());
    when(execContext.issueSideInputFetch(
             eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(false);

    StreamingSideInputDoFnRunner<String, String, List, IntervalWindow> runner =
        createRunner(Arrays.asList(view));

    runner.startBundle();
    runner.processElement(createDatum("e", 0));
    runner.finishBundle();

    assertTrue(runner.getReceiver(mainOutputTag).isEmpty());

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));

    verify(stepContext).writeToTagList(
        any(CodedTupleTag.class), eq(createDatum("e", 0)), eq(new Instant(0)));
    verify(stepContext).store(any(CodedTupleTag.class), eq(
        Collections.singletonMap(
            window,
            Collections.singleton(Windmill.GlobalDataRequest.newBuilder()
                .setDataId(Windmill.GlobalDataId.newBuilder()
                    .setTag(view.getTagInternal().getId())
                    .setVersion(ByteString.copyFrom(CoderUtils.encodeToByteArray(
                        IntervalWindow.getFixedSizeCoder(Duration.millis(10)), window)))
                    .build())
                .setExistenceWatermarkDeadline(9000)
                .build()))));
  }

  @Test
  public void testSideInputNotification() throws Exception {
    PCollectionView<String> view = createView();

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    Windmill.GlobalDataId id = Windmill.GlobalDataId.newBuilder()
        .setTag(view.getTagInternal().getId())
        .setVersion(ByteString.copyFrom(CoderUtils.encodeToByteArray(
            IntervalWindow.getFixedSizeCoder(Duration.millis(10)), window)))
        .build();

    Set<Windmill.GlobalDataRequest> requestSet = new HashSet<>();
    requestSet.add(Windmill.GlobalDataRequest.newBuilder().setDataId(id).build());
    Map<IntervalWindow, Set<Windmill.GlobalDataRequest>> blockedMap = new HashMap<>();
    blockedMap.put(window, requestSet);

    when(stepContext
        .lookup(Mockito.<CodedTupleTag<Map<IntervalWindow, Set<Windmill.GlobalDataRequest>>>>any()))
        .thenReturn(blockedMap);
    when(execContext.getSideInputNotifications()).thenReturn(Arrays.asList(id));
    when(execContext.issueSideInputFetch(
             eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(false);
    when(execContext.issueSideInputFetch(
             eq(view), any(BoundedWindow.class), eq(SideInputState.KNOWN_READY)))
        .thenReturn(true);
    when(execContext.getSideInput(eq(view), eq(window), any(PTuple.class)))
        .thenReturn("data");
    when(stepContext.readTagLists(
        Mockito.<Iterable<CodedTupleTag<WindowedValue<String>>>>any()))
        .thenAnswer(readTagListAnswer(Arrays.asList(createDatum("e", 0))));

    StreamingSideInputDoFnRunner<String, String, List, IntervalWindow> runner =
        createRunner(Arrays.asList(view));

    runner.startBundle();
    runner.finishBundle();

    assertThat((List<WindowedValue<String>>) runner.getReceiver(mainOutputTag),
        contains(createDatum("e:data", 0)));

    verify(stepContext).store(any(CodedTupleTag.class), eq(new HashMap()));
  }

  private <T> Answer<Map<CodedTupleTag<T>, Iterable<T>>> readTagListAnswer(
      final Iterable<T> answer) {
    return new Answer<Map<CodedTupleTag<T>, Iterable<T>>>() {
      @Override
      public Map<CodedTupleTag<T>, Iterable<T>> answer(InvocationOnMock invocation)
          throws Throwable {
        Map<CodedTupleTag<T>, Iterable<T>> result = new LinkedHashMap<>();
        @SuppressWarnings("unchecked")
        Iterable<CodedTupleTag<T>> tags =
            (Iterable<CodedTupleTag<T>>) invocation.getArguments()[0];
        for (CodedTupleTag<T> tag : tags) {
          result.put(tag, answer);
        }
        return result;
      }
    };
  }

  @Test
  public void testMultipleSideInputs() throws Exception {
    PCollectionView<String> view1 = createView();
    PCollectionView<String> view2 = createView();

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    Windmill.GlobalDataId id = Windmill.GlobalDataId.newBuilder()
        .setTag(view1.getTagInternal().getId())
        .setVersion(ByteString.copyFrom(CoderUtils.encodeToByteArray(
            IntervalWindow.getFixedSizeCoder(Duration.millis(10)), window)))
        .build();

    Set<Windmill.GlobalDataRequest> requestSet = new HashSet<>();
    requestSet.add(Windmill.GlobalDataRequest.newBuilder().setDataId(id).build());
    Map<IntervalWindow, Set<Windmill.GlobalDataRequest>> blockedMap = new HashMap<>();
    blockedMap.put(window, requestSet);

    when(stepContext.lookup(
        Mockito.<CodedTupleTag<Map<IntervalWindow, Set<Windmill.GlobalDataRequest>>>>any()))
        .thenReturn(blockedMap);
    when(execContext.getSideInputNotifications()).thenReturn(Arrays.asList(id));
    when(execContext.issueSideInputFetch(
             any(PCollectionView.class), any(BoundedWindow.class), any(SideInputState.class)))
        .thenReturn(true);
    when(execContext.getSideInput(eq(view1), eq(window), any(PTuple.class)))
        .thenReturn("data1");
    when(execContext.getSideInput(eq(view2), eq(window), any(PTuple.class)))
        .thenReturn("data2");
    when(stepContext.readTagLists(
        Mockito.<Iterable<CodedTupleTag<WindowedValue<String>>>>any()))
        .thenAnswer(readTagListAnswer(Arrays.asList(createDatum("e1", 0))));

    StreamingSideInputDoFnRunner<String, String, List, IntervalWindow> runner =
        createRunner(Arrays.asList(view1, view2));

    runner.startBundle();
    runner.processElement(createDatum("e2", 2));
    runner.finishBundle();

    System.out.println(runner.getReceiver(mainOutputTag));

    assertThat((List<WindowedValue<String>>) runner.getReceiver(mainOutputTag),
        contains(createDatum("e1:data1:data2", 0), createDatum("e2:data1:data2", 2)));

    verify(stepContext).store(any(CodedTupleTag.class), eq(new HashMap()));
  }

  private StreamingSideInputDoFnRunner<String, String, List, IntervalWindow> createRunner(
      List<PCollectionView<String>> views) throws Exception {
    DoFnInfo doFnInfo = new DoFnInfo<String, String>(
        new SideInputFn(views),
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10))),
        (Iterable) views, StringUtf8Coder.of());

    PTuple sideInputs = PTuple.empty();
    for (PCollectionView<String> view : views) {
      sideInputs = sideInputs.and(view.getTagInternal(), null);
    }

    return new StreamingSideInputDoFnRunner<String, String, List, IntervalWindow>(
        PipelineOptionsFactory.create(),
        doFnInfo,
        sideInputs,
        new DoFnRunner.OutputManager<List>() {
          @Override
          public List initialize(TupleTag<?> tag) {
            return new ArrayList<>();
          }
          @Override
          public void output(List list, WindowedValue<?> output) {
            list.add(output);
          }
        },
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
        .apply(Window.<String>into(FixedWindows.of(Duration.millis(10))))
        .apply(View.<String>asSingleton());
  }

  private WindowedValue<String> createDatum(String element, long timestamp) {
    return WindowedValue.of(
        element,
        new Instant(timestamp),
        Arrays.asList(new IntervalWindow(
            new Instant(timestamp - timestamp % 10),
            new Instant(timestamp - timestamp % 10 + 10))));
  }
}
