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
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.dataflow.worker.util.ListOutputManager;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.InputMessageBundle;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link StreamingGroupAlsoByWindowReshuffleFn}. */
@RunWith(JUnit4.class)
public class StreamingGroupAlsoByWindowsReshuffleDoFnTest {
  private static final String KEY = "k";
  private static final long WORK_TOKEN = 1000L;
  private static final String SOURCE_COMPUTATION_ID = "sourceComputationId";

  private Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();
  private Coder<Collection<IntervalWindow>> windowsCoder = CollectionCoder.of(windowCoder);
  private StepContext stepContext;

  @Before
  public void setUp() {
    stepContext =
        new StepContext() {

          @Override
          public TimerInternals timerInternals() {
            return null;
          }

          @Override
          public StateInternals stateInternals() {
            return null;
          }
        };
  }

  @Test
  public void testEmpty() throws Exception {
    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    ListOutputManager outputManager = new ListOutputManager();
    DoFnRunner<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> runner =
        makeRunner(outputTag, outputManager);

    runner.startBundle();

    runner.finishBundle();

    List<?> result = outputManager.getOutput(outputTag);

    assertEquals(0, result.size());
  }

  private <V> void addElement(
      InputMessageBundle.Builder messageBundle,
      Collection<IntervalWindow> windows,
      Instant timestamp,
      Coder<V> valueCoder,
      V value)
      throws IOException {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Coder<Collection<? extends BoundedWindow>> windowsCoder =
        (Coder) CollectionCoder.of(windowCoder);

    ByteStringOutputStream dataOutput = new ByteStringOutputStream();
    valueCoder.encode(value, dataOutput, Context.OUTER);
    messageBundle
        .addMessagesBuilder()
        .setMetadata(WindmillSink.encodeMetadata(windowsCoder, windows, PaneInfo.NO_FIRING))
        .setData(dataOutput.toByteString())
        .setTimestamp(WindmillTimeUtils.harnessToWindmillTimestamp(timestamp));
  }

  private <T> WindowedValue<KeyedWorkItem<String, T>> createValue(
      WorkItem.Builder workItem, Coder<T> valueCoder) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Coder<Collection<? extends BoundedWindow>> wildcardWindowsCoder = (Coder) windowsCoder;
    return new ValueInEmptyWindows<>(
        (KeyedWorkItem<String, T>)
            new WindmillKeyedWorkItem<>(
                KEY, workItem.build(), windowCoder, wildcardWindowsCoder, valueCoder));
  }

  @Test
  public void testFixedWindows() throws Exception {
    TupleTag<KV<String, Iterable<String>>> outputTag = new TupleTag<>();
    ListOutputManager outputManager = new ListOutputManager();
    DoFnRunner<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> runner =
        makeRunner(outputTag, outputManager);

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
      TupleTag<KV<String, Iterable<String>>> outputTag, DoFnRunners.OutputManager outputManager) {

    GroupAlsoByWindowFn<KeyedWorkItem<String, String>, KV<String, Iterable<String>>> fn =
        new StreamingGroupAlsoByWindowReshuffleFn<>();

    return makeRunner(outputTag, outputManager, fn);
  }

  private <InputT, OutputT>
      DoFnRunner<KeyedWorkItem<String, InputT>, KV<String, OutputT>> makeRunner(
          TupleTag<KV<String, OutputT>> outputTag,
          DoFnRunners.OutputManager outputManager,
          GroupAlsoByWindowFn<KeyedWorkItem<String, InputT>, KV<String, OutputT>> fn) {
    return new GroupAlsoByWindowFnRunner<>(
        PipelineOptionsFactory.create(),
        fn,
        NullSideInputReader.empty(),
        outputManager,
        outputTag,
        stepContext);
  }

  private IntervalWindow window(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }
}
