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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.coders.CollectionCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.InputMessageBundle;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.WorkItem;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.DirectModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.DoFnRunner;
import com.google.cloud.dataflow.sdk.util.DoFnRunnerBase;
import com.google.cloud.dataflow.sdk.util.DoFnRunners;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.NullSideInputReader;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
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
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/** Unit tests for {@link StreamingGroupAlsoByWindowsReshuffleDoFn}. */
@RunWith(JUnit4.class)
public class StreamingGroupAlsoByWindowsReshuffleDoFnTest {
  private static final String KEY = "k";
  private static final long WORK_TOKEN = 1000L;
  private static final String SOURCE_COMPUTATION_ID = "sourceComputationId";
  private ExecutionContext execContext;
  private CounterSet counters;

  private Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();
  private Coder<Collection<IntervalWindow>> windowsCoder = CollectionCoder.of(windowCoder);

  @Before public void setUp() {
    execContext = new DirectModeExecutionContext() {
      // Normally timerInternals doesn't come from the execution context, but
      // StreamingGroupAlsoByWindows expects it to. So, hook that up.

      @Override
      public StepContext createStepContext(
          String stepName, String transformName, StateSampler stateSampler) {
        StepContext context =
            Mockito.spy(super.createStepContext(stepName, transformName, stateSampler));
        Mockito.doReturn(null).when(context).timerInternals();
        return context;
      }
    };
    counters = new CounterSet();
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

    WorkItem.Builder workItem = WorkItem.newBuilder();
    workItem.setKey(ByteString.copyFromUtf8(KEY));
    workItem.setWorkToken(WORK_TOKEN);
    InputMessageBundle.Builder messageBundle = workItem.addMessageBundlesBuilder();
    messageBundle.setSourceComputationId(SOURCE_COMPUTATION_ID);

    Coder<String> valueCoder = StringUtf8Coder.of();
    addElement(messageBundle, Arrays.asList(window(0, 10)), new Instant(1), valueCoder, "v1");
    addElement(messageBundle, Arrays.asList(window(0, 10)), new Instant(2), valueCoder, "v2");
    addElement(messageBundle, Arrays.asList(window(0, 10)), new Instant(0), valueCoder, "v0");
    addElement(messageBundle, Arrays.asList(window(10, 20)), new Instant(13), valueCoder, "v3");

    runner.processElement(createValue(workItem, valueCoder));

    runner.finishBundle();

    List<WindowedValue<KV<String, Iterable<String>>>> result = outputManager.getOutput(outputTag);

    assertEquals(4, result.size());

    WindowedValue<KV<String, Iterable<String>>> item0 = result.get(0);
    assertEquals(KEY, item0.getValue().getKey());
    assertThat(item0.getValue().getValue(), Matchers.containsInAnyOrder("v1"));
    assertEquals(new Instant(1), item0.getTimestamp());
    assertThat(item0.getWindows(), Matchers.<BoundedWindow>contains(window(0, 10)));

    WindowedValue<KV<String, Iterable<String>>> item1 = result.get(1);
    assertEquals(KEY, item1.getValue().getKey());
    assertThat(item1.getValue().getValue(), Matchers.containsInAnyOrder("v2"));
    assertEquals(new Instant(2), item1.getTimestamp());
    assertThat(item1.getWindows(), Matchers.<BoundedWindow>contains(window(0, 10)));

    WindowedValue<KV<String, Iterable<String>>> item2 = result.get(2);
    assertEquals(KEY, item2.getValue().getKey());
    assertThat(item2.getValue().getValue(), Matchers.containsInAnyOrder("v0"));
    assertEquals(new Instant(0), item2.getTimestamp());
    assertThat(item2.getWindows(), Matchers.<BoundedWindow>contains(window(0, 10)));

    WindowedValue<KV<String, Iterable<String>>> item3 = result.get(3);
    assertEquals(KEY, item3.getValue().getKey());
    assertThat(item3.getValue().getValue(), Matchers.containsInAnyOrder("v3"));
    assertEquals(new Instant(13), item3.getTimestamp());
    assertThat(item3.getWindows(), Matchers.<BoundedWindow>contains(window(10, 20)));
  }

  private DoFnRunner<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> makeRunner(
          TupleTag<KV<String, Iterable<String>>> outputTag,
          DoFnRunners.OutputManager outputManager,
          WindowingStrategy<? super String, IntervalWindow> windowingStrategy) {

    DoFn<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> fn =
        new StreamingGroupAlsoByWindowsReshuffleDoFn<>();

    return makeRunner(outputTag, outputManager, windowingStrategy, fn);
  }

  private <InputT, OutputT>
      DoFnRunner<KeyedWorkItem<String, InputT>, KV<String, OutputT>> makeRunner(
          TupleTag<KV<String, OutputT>> outputTag,
          DoFnRunners.OutputManager outputManager,
          WindowingStrategy<? super String, IntervalWindow> windowingStrategy,
          DoFn<KeyedWorkItem<String, InputT>, KV<String, OutputT>> fn) {
    return
        DoFnRunners.simpleRunner(
            PipelineOptionsFactory.create(),
            fn,
            NullSideInputReader.empty(),
            outputManager,
            outputTag,
            new ArrayList<TupleTag<?>>(),
            execContext.getOrCreateStepContext("merge", "merge", null),
            counters.getAddCounterMutator(),
            windowingStrategy);
  }

  private IntervalWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }
}
