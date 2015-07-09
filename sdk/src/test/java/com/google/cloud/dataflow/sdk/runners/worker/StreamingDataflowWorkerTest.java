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

import static com.google.cloud.dataflow.sdk.util.Structs.addObject;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Sink;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.WriteInstruction;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CollectionCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerHarnessOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.dataflow.BasicSerializableSourceFormat;
import com.google.cloud.dataflow.sdk.runners.dataflow.CountingSource;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.WorkItemCommitRequest;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.util.AssignWindowsDoFn;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.StringUtils;
import com.google.cloud.dataflow.sdk.util.TimerOrElement.TimerOrElementCoder;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.FullWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Unit tests for {@link StreamingDataflowWorker}. */
@RunWith(JUnit4.class)
public class StreamingDataflowWorkerTest {
  private static final IntervalWindow DEFAULT_WINDOW =
      new IntervalWindow(new Instant(1234), new Duration(1000));

  private static final IntervalWindow WINDOW_AT_ZERO =
      new IntervalWindow(new Instant(0), new Instant(1000));

  private static final IntervalWindow WINDOW_AT_ONE_SECOND =
      new IntervalWindow(new Instant(1000), new Instant(2000));

  private static final Coder<IntervalWindow> DEFAULT_WINDOW_CODER = IntervalWindow.getCoder();
  private static final Coder<Collection<IntervalWindow>> DEFAULT_WINDOW_COLLECTION_CODER =
      CollectionCoder.of(DEFAULT_WINDOW_CODER);

  private static final byte[] defaultWindowsBytes() throws Exception {
    return CoderUtils.encodeToByteArray(
        DEFAULT_WINDOW_COLLECTION_CODER, Arrays.asList(DEFAULT_WINDOW));
  }

  private static final byte[] windowAtZeroBytes() throws Exception {
    return CoderUtils.encodeToByteArray(
        DEFAULT_WINDOW_COLLECTION_CODER, Arrays.asList(WINDOW_AT_ZERO));
  }

  private static final byte[] windowAtOneSecondBytes() throws Exception {
    return CoderUtils.encodeToByteArray(
        DEFAULT_WINDOW_COLLECTION_CODER, Arrays.asList(WINDOW_AT_ONE_SECOND));
  }

  private static final byte[] emptyWindowsBytes() throws Exception {
    return CoderUtils.encodeToByteArray(
        DEFAULT_WINDOW_COLLECTION_CODER, Collections.<IntervalWindow>emptyList());
  }

  // Default values that are unimportant for correctness, but must be consistent
  // between pieces of this test suite
  private static final String DEFAULT_COMPUTATION_ID = "computation";
  private static final String DEFAULT_MAP_STAGE_NAME = "computation";
  private static final String DEFAULT_MAP_SYSTEM_NAME = "computation";
  private static final String DEFAULT_PARDO_SYSTEM_NAME = "parDo";
  private static final String DEFAULT_SOURCE_SYSTEM_NAME = "source";
  private static final String DEFAULT_SINK_SYSTEM_NAME = "sink";
  private static final String DEFAULT_SOURCE_COMPUTATION_ID = "upstream";
  private static final String DEFAULT_KEY_STRING = "key";
  private static final String DEFAULT_DATA_STRING = "data";
  private static final String DEFAULT_DESTINATION_STREAM_ID = "out";

  private String keyStringForIndex(int index) {
    return DEFAULT_KEY_STRING + index;
  }

  private String dataStringForIndex(long index) {
    return DEFAULT_DATA_STRING + index;
  }

  private ParallelInstruction makeWindowingSourceInstruction(Coder<?> coder) {
    CloudObject encodedCoder = FullWindowedValueCoder.of(
        TimerOrElementCoder.of(coder), IntervalWindow.getCoder()).asCloudObject();
    return new ParallelInstruction()
        .setSystemName(DEFAULT_SOURCE_SYSTEM_NAME)
        .setRead(new ReadInstruction().setSource(
            new Source()
            .setSpec(CloudObject.forClass(WindowingWindmillReader.class))
            .setCodec(encodedCoder)))
        .setOutputs(Arrays.asList(
            new InstructionOutput()
            .setName("read_output")
            .setCodec(encodedCoder)));
  }

  private ParallelInstruction makeSourceInstruction(Coder<?> coder) {
    return new ParallelInstruction()
        .setSystemName(DEFAULT_SOURCE_SYSTEM_NAME)
        .setRead(new ReadInstruction().setSource(
            new Source()
            .setSpec(CloudObject.forClass(UngroupedWindmillReader.class))
            .setCodec(WindowedValue.getFullCoder(coder, IntervalWindow.getCoder())
                                   .asCloudObject())))
        .setOutputs(Arrays.asList(
            new InstructionOutput()
            .setName("read_output")
            .setCodec(WindowedValue.getFullCoder(coder, IntervalWindow.getCoder())
                                   .asCloudObject())));
  }

  private ParallelInstruction makeDoFnInstruction(
      DoFn<?, ?> doFn,
      int producerIndex,
      Coder<?> outputCoder,
      Coder<? extends BoundedWindow> windowCoder) {
    CloudObject spec = CloudObject.forClassName("DoFn");
    addString(spec, PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(new DoFnInfo<>(doFn, null))));
    return new ParallelInstruction()
        .setSystemName(DEFAULT_PARDO_SYSTEM_NAME)
        .setParDo(new ParDoInstruction()
            .setInput(
                new InstructionInput().setProducerInstructionIndex(producerIndex).setOutputNum(0))
            .setNumOutputs(1)
            .setUserFn(spec))
        .setOutputs(Arrays.asList(
            new InstructionOutput()
            .setName("par_do_output")
            .setCodec(WindowedValue.getFullCoder(outputCoder, windowCoder)
                                   .asCloudObject())));
  }

  private ParallelInstruction makeDoFnInstruction(
      DoFn<?, ?> doFn, int producerIndex, Coder<?> outputCoder) {
    return makeDoFnInstruction(doFn, producerIndex, outputCoder, IntervalWindow.getCoder());
  }

  private ParallelInstruction makeSinkInstruction(
      Coder<?> coder, int producerIndex, Coder<? extends BoundedWindow> windowCoder) {
    CloudObject spec = CloudObject.forClass(WindmillSink.class);
    addString(spec, "stream_id", DEFAULT_DESTINATION_STREAM_ID);
    return new ParallelInstruction()
        .setSystemName(DEFAULT_SINK_SYSTEM_NAME)
        .setWrite(new WriteInstruction()
            .setInput(
                new InstructionInput().setProducerInstructionIndex(producerIndex).setOutputNum(0))
            .setSink(new Sink()
                .setSpec(spec)
                .setCodec(WindowedValue.getFullCoder(coder, windowCoder)
                                       .asCloudObject())));
  }

  private ParallelInstruction makeSinkInstruction(Coder<?> coder, int producerIndex) {
    return makeSinkInstruction(coder, producerIndex, IntervalWindow.getCoder());
  }

  /**
   * Returns a {@link MapTask} with the provided {@code instructions} and default values
   * everywhere else.
   */
  private MapTask defaultMapTask(List<ParallelInstruction> instructions) {
    return new MapTask()
        .setStageName(DEFAULT_MAP_STAGE_NAME)
        .setSystemName(DEFAULT_MAP_SYSTEM_NAME)
        .setInstructions(instructions);
  }

  private Windmill.GetWorkResponse buildTimerInput(String input) throws Exception {
    Windmill.GetWorkResponse.Builder builder = Windmill.GetWorkResponse.newBuilder();
    TextFormat.merge(input, builder);
    return builder.build();
  }

  private Windmill.GetWorkResponse buildInput(String input, byte[] metadata) throws Exception {
    Windmill.GetWorkResponse.Builder builder = Windmill.GetWorkResponse.newBuilder();
    TextFormat.merge(input, builder);
    if (metadata != null) {
      Windmill.InputMessageBundle.Builder messageBundleBuilder =
          builder.getWorkBuilder(0).getWorkBuilder(0).getMessageBundlesBuilder(0);
      for (Windmill.Message.Builder messageBuilder :
          messageBundleBuilder.getMessagesBuilderList()) {
        messageBuilder.setMetadata(addNullPaneTag(ByteString.copyFrom(metadata)));
      }
    }
    return builder.build();
  }

  private Windmill.GetDataResponse buildData(String input) throws Exception {
    Windmill.GetDataResponse.Builder builder = Windmill.GetDataResponse.newBuilder();
    TextFormat.merge(input, builder);
    return builder.build();
  }

  private Windmill.GetWorkResponse makeInput(int index, long timestamp) throws Exception {
    return buildInput(
        "work {" +
        "  computation_id: \"" + DEFAULT_COMPUTATION_ID + "\"" +
        "  work {" +
        "    key: \"" + DEFAULT_KEY_STRING + "\"" +
        "    work_token: " + index +
        "    message_bundles {" +
        "      source_computation_id: \"" + DEFAULT_SOURCE_COMPUTATION_ID + "\"" +
        "      messages {" +
        "        timestamp: " + timestamp +
        "        data: \"data" + index + "\"" +
        "      }" +
        "    }" +
        "  }" +
        "}",
        CoderUtils.encodeToByteArray(
            CollectionCoder.of(IntervalWindow.getCoder()), Arrays.asList(DEFAULT_WINDOW)));
  }

  /**
   * Returns a {@link WorkItemCommitRequest.Builder} parsed from the provided text format proto.
   */
  private WorkItemCommitRequest.Builder parseCommitRequest(String output) throws Exception {
    WorkItemCommitRequest.Builder builder = Windmill.WorkItemCommitRequest.newBuilder();
    TextFormat.merge(output, builder);
    return builder;
  }

  /**
   * Sets the metadata of the first contained message in this WorkItemCommitRequest
   * (it should only have one message).
   */
  private WorkItemCommitRequest.Builder setMessagesMetadata(
      byte[] metadata, WorkItemCommitRequest.Builder builder) throws Exception {
    if (metadata != null) {
      builder.getOutputMessagesBuilder(0)
          .getBundlesBuilder(0)
          .getMessagesBuilder(0)
          .setMetadata(addNullPaneTag(ByteString.copyFrom(metadata)));
    }
    return builder;
  }

  private Windmill.WorkItemCommitRequest makeExpectedOutput(int index, long timestamp, String key)
      throws Exception {
    return setMessagesMetadata(defaultWindowsBytes(),
        parseCommitRequest(
            "key: \"" + DEFAULT_KEY_STRING + "\" " +
            "work_token: " + index + " " +
            "output_messages {" +
            "  destination_stream_id: \"" + DEFAULT_DESTINATION_STREAM_ID + "\"" +
            "  bundles {" +
            "    key: \"" + key + "\"" +
            "    messages {" +
            "      timestamp: " + timestamp +
            "      data: \"" + dataStringForIndex(index) + "\"" +
            "      metadata: \"\"" +
            "    }" +
            "  }" +
            "}"))
        .build();
  }

  private ByteString addNullPaneTag(ByteString windows) {
    return ByteString.copyFrom(new byte[1]).concat(windows);
  }

  private DataflowWorkerHarnessOptions createTestingPipelineOptions() {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    options.setAppName("StreamingWorkerHarnessTest");
    options.setStreaming(true);
    return options;
  }

  private Windmill.WorkItemCommitRequest stripCounters(Windmill.WorkItemCommitRequest request) {
    return Windmill.WorkItemCommitRequest.newBuilder(request).clearCounterUpdates().build();
  }

  @Test
  public void testBasicHarness() throws Exception {
    List<ParallelInstruction> instructions = Arrays.asList(
        makeSourceInstruction(StringUtf8Coder.of()),
        makeSinkInstruction(StringUtf8Coder.of(), 0));

    FakeWindmillServer server = new FakeWindmillServer();
    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(defaultMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    final int numIters = 2000;
    for (int i = 0; i < numIters; ++i) {
      server.addWorkToOffer(makeInput(i, TimeUnit.MILLISECONDS.toMicros(i)));
    }

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(numIters);
    worker.stop();

    for (int i = 0; i < numIters; ++i) {
      assertTrue(result.containsKey((long) i));
      assertEquals(makeExpectedOutput(i, TimeUnit.MILLISECONDS.toMicros(i), "key"),
                          stripCounters(result.get((long) i)));
    }
  }

  static class ChangeKeysFn extends DoFn<KV<String, String>, KV<String, String>> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) {
      KV<String, String> elem = c.element();
      c.output(KV.of(elem.getKey() + "_" + elem.getValue(), elem.getValue()));
    }
  }

  @Test
  public void testKeyChange() throws Exception {
    KvCoder<String, String> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    List<ParallelInstruction> instructions = Arrays.asList(
        makeSourceInstruction(kvCoder),
        makeDoFnInstruction(new ChangeKeysFn(), 0, kvCoder),
        makeSinkInstruction(kvCoder, 1));

    FakeWindmillServer server = new FakeWindmillServer();
    server.addWorkToOffer(makeInput(0, 0));
    server.addWorkToOffer(makeInput(1, TimeUnit.MILLISECONDS.toMicros(1)));

    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(defaultMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(2);

    assertThat(stripCounters(result.get(0L)),
        equalTo(makeExpectedOutput(0, 0, "key_data0")));

    assertThat(stripCounters(result.get(1L)),
        equalTo(makeExpectedOutput(1, TimeUnit.MILLISECONDS.toMicros(1), "key_data1")));
  }

  static class TestExceptionFn extends DoFn<String, String> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) throws Exception {
      try {
        throw new Exception("Exception!");
      } catch (Exception e) {
        throw new Exception("Another exception!", e);
      }
    }
  }

  @Test
  public void testExceptions() throws Exception {
    List<ParallelInstruction> instructions = Arrays.asList(
        makeSourceInstruction(StringUtf8Coder.of()),
        makeDoFnInstruction(new TestExceptionFn(), 0, StringUtf8Coder.of()),
        makeSinkInstruction(StringUtf8Coder.of(), 1));

    FakeWindmillServer server = new FakeWindmillServer();
    server.setExpectedExceptionCount(1);
    server.addWorkToOffer(buildInput(
        "work {" +
        "  computation_id: \"" + DEFAULT_COMPUTATION_ID + "\"" +
        "  work {" +
        "    key: \"" + keyStringForIndex(0) + "\"" +
        "    work_token: 0" +
        "    message_bundles {" +
        "      source_computation_id: \"" + DEFAULT_SOURCE_COMPUTATION_ID + "\"" +
        "      messages {" +
        "        timestamp: 0" +
        "        data: \"0\"" +
        "      }" +
        "    }" +
        "  }" +
        "}",
        CoderUtils.encodeToByteArray(CollectionCoder.of(IntervalWindow.getCoder()),
                                     Arrays.asList(DEFAULT_WINDOW))));

    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(defaultMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    Windmill.Exception exception = server.getException();

    assertThat(exception.getStackFrames(0),
        containsString("Another exception!"));
    assertThat(exception.getStackFrames(1),
        containsString("processElement"));
    assertTrue(exception.hasCause());

    assertThat(exception.getCause().getStackFrames(0),
        containsString("Exception!"));
    assertThat(exception.getCause().getStackFrames(1),
        containsString("processElement"));
    assertFalse(exception.getCause().hasCause());

    // The server should retry the work since reporting the exception succeeded.
    // Make next retry should fail because we only expected 1 exception.
    exception = server.getException();
  }

  /**
   * An {@link AssignWindowsDoFn} that does not actually assign windows to values,
   * but extracts their timestamps, which are output in {@code KV} pairs on a fixed key.
   *
   * <p>The key and value of the input are both discarded.
   */
  private static class TestTimerFn
      extends AssignWindowsDoFn<KV<String, String>, BoundedWindow> {
    private static final long serialVersionUID = 0;

    private final String key;

    public TestTimerFn(String key) {
      super(null);
      this.key = key;
    }

    @Override
    public void processElement(ProcessContext c) {
      c.output(KV.of(key, Long.toString(c.timestamp().getMillis())));
    }
  }

  @Test
  public void testTimers() throws Exception {
    KvCoder<String, String> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    long timestamp = 3000L;
    String key = keyStringForIndex(0);

    List<ParallelInstruction> instructions = Arrays.asList(
        makeWindowingSourceInstruction(kvCoder),
        makeDoFnInstruction(new TestTimerFn(key), 0, kvCoder),
        makeSinkInstruction(kvCoder, 1));

    FakeWindmillServer server = new FakeWindmillServer();

    server.addWorkToOffer(buildTimerInput(
        "work {" +
        "  computation_id: \"" + DEFAULT_COMPUTATION_ID + "\"" +
        "  work {" +
        "    key: \"" + key + "\"" +
        "    work_token: 0" +
        "    timers {" +
        "      timers {" +
        "        tag: \"tag\"" +
        "        timestamp: " + timestamp +
        "      }" +
        "    }" +
        "  }" +
        "}"));

    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(defaultMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);

    assertThat(
        stripCounters(result.get(0L)),
        equalTo(setMessagesMetadata(emptyWindowsBytes(),
            parseCommitRequest(
                "key: \"" + keyStringForIndex(0) + "\" " +
                "work_token: 0 " +
                "output_messages {" +
                "  destination_stream_id: \"" + DEFAULT_DESTINATION_STREAM_ID + "\"" +
                "  bundles {" +
                "    key: \"" + keyStringForIndex(0) + "\"" +
                "    messages {" +
                "      timestamp: " + timestamp +
                "      data: \"" + TimeUnit.MILLISECONDS.toSeconds(timestamp) + "\"" +
                "    }" +
                "  }" +
                "} "))
            .build()));
  }

  @Test
  public void testAssignWindows() throws Exception {
    Duration gapDuration = Duration.standardSeconds(1);
    CloudObject spec = CloudObject.forClassName("AssignWindowsDoFn");
    addString(spec, PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(
                WindowingStrategy.of(FixedWindows.of(gapDuration)))));

    ParallelInstruction addWindowsInstruction =
        new ParallelInstruction()
        .setSystemName("AssignWindows")
        .setParDo(new ParDoInstruction()
            .setInput(new InstructionInput().setProducerInstructionIndex(0).setOutputNum(0))
            .setNumOutputs(1)
            .setUserFn(spec))
        .setOutputs(Arrays.asList(new InstructionOutput()
                .setName("output")
                .setCodec(WindowedValue.getFullCoder(StringUtf8Coder.of(),
                                                     IntervalWindow.getCoder()).asCloudObject())));

    List<ParallelInstruction> instructions = Arrays.asList(
        makeSourceInstruction(StringUtf8Coder.of()),
        addWindowsInstruction,
        makeSinkInstruction(StringUtf8Coder.of(), 1));

    FakeWindmillServer server = new FakeWindmillServer();

    int timestamp1 = 0;
    int timestamp2 = 1000000;

    server.addWorkToOffer(makeInput(timestamp1, timestamp1));
    server.addWorkToOffer(makeInput(timestamp2, timestamp2));

    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(defaultMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(2);

    assertThat(
        stripCounters(result.get(0L)),
        equalTo(setMessagesMetadata(windowAtZeroBytes(),
            parseCommitRequest(
                "key: \"" + DEFAULT_KEY_STRING + "\" " +
                "work_token: 0 " +
                "output_messages {" +
                "  destination_stream_id: \"" + DEFAULT_DESTINATION_STREAM_ID + "\"" +
                "  bundles {" +
                "    key: \"" + DEFAULT_KEY_STRING + "\"" +
                "    messages {" +
                "      timestamp: 0" +
                "      data: \"" + dataStringForIndex(timestamp1) + "\"" +
                "    }" +
                "  }" +
                "} "))
            .build()));

    assertThat(stripCounters(result.get(1000000L)),
        equalTo(setMessagesMetadata(windowAtOneSecondBytes(),
            parseCommitRequest(
                "key: \"" + DEFAULT_KEY_STRING + "\" " +
                "work_token: 1000000 " +
                "output_messages {" +
                "  destination_stream_id: \"" + DEFAULT_DESTINATION_STREAM_ID + "\"" +
                "  bundles {" +
                "    key: \"" + DEFAULT_KEY_STRING + "\"" +
                "    messages {" +
                "      timestamp: " + timestamp2 +
                "      data: \"" + dataStringForIndex(timestamp2) + "\"" +
                "    }" +
                "  }" +
                "} "))
            .build()));
  }

  @Test
  public void testMergeWindows() throws Exception {
    Coder<KV<String, String>> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    Coder<WindowedValue<KV<String, String>>> windowedKvCoder =
        FullWindowedValueCoder.of(kvCoder, IntervalWindow.getCoder());
    KvCoder<String, List<String>> groupedCoder =
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()));
    Coder<WindowedValue<KV<String, List<String>>>> windowedGroupedCoder =
        FullWindowedValueCoder.of(groupedCoder, IntervalWindow.getCoder());

    CloudObject spec = CloudObject.forClassName("MergeWindowsDoFn");
    addString(spec, PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(
                WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(1))))));
    addObject(spec, PropertyNames.INPUT_CODER, windowedKvCoder.asCloudObject());

    ParallelInstruction mergeWindowsInstruction =
        new ParallelInstruction()
        .setSystemName("MergeWindows")
        .setParDo(new ParDoInstruction()
            .setInput(new InstructionInput().setProducerInstructionIndex(0).setOutputNum(0))
            .setNumOutputs(1)
            .setUserFn(spec))
        .setOutputs(Arrays.asList(new InstructionOutput()
                .setName("output")
                .setCodec(windowedGroupedCoder.asCloudObject())));

    List<ParallelInstruction> instructions = Arrays.asList(
        makeWindowingSourceInstruction(kvCoder),
        mergeWindowsInstruction,
        makeSinkInstruction(groupedCoder, 1));

    FakeWindmillServer server = new FakeWindmillServer();

    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(defaultMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    server.addWorkToOffer(buildInput(
        "work {" +
            "  computation_id: \"" + DEFAULT_COMPUTATION_ID + "\"" +
            "  input_data_watermark: 0" +
            "  work {" +
            "    key: \"" + DEFAULT_KEY_STRING + "\"" +
            "    work_token: 0" +
            "    message_bundles {" +
            "      source_computation_id: \"" + DEFAULT_SOURCE_COMPUTATION_ID + "\"" +
            "      messages {" +
            "        timestamp: 0" +
            "        data: \"" + dataStringForIndex(0) + "\"" +
            "      }" +
            "    }" +
            "  }" +
            "}",
            windowAtZeroBytes()));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);

    // These tags and data are opaque strings and this is a change detector test.
    String timer1Tag = "gAAAAAAAAACAAAAAAAAD6P____8P";
    String timer2Tag = "gAAAAAAAAACAAAAAAAAD6AA";
    String timer3Tag = "gAAAAAAAAACAAAAAAAAD6AAAAAA";
    String bufferTag = "MergeWindows:gAAAAAAAAACAAAAAAAAD6A/__buffer";
    String watermarkHoldTag = "MergeWindows:gAAAAAAAAACAAAAAAAAD6A/__watermark_hold";
    String watermarkHoldData = "\000\\200\\000\\000\\000\\000\\000\\000\\000";
    String bufferData = "\000data0";
    String outputData = "\\377\\377\\377\\377\\001\\005data0\\000";

    // These values are not essential to the change detector test
    long timer1Timestamp = 1000000L;
    long timer2Timestamp = 999000L;

    WorkItemCommitRequest actualOutput = stripCounters(result.get(0L));

    WorkItemCommitRequest expectedOutput = parseCommitRequest(
        "key: \"" + DEFAULT_KEY_STRING + "\" " +
            "work_token: 0 " +
            "output_timers {" +
            "  tag: \"" + timer1Tag + "\"" +
            "  timestamp: " + timer1Timestamp +
            "  type: WATERMARK" +
            "}" +
            "output_timers {" +
            "  tag: \"" + timer2Tag + "\"" +
            "  timestamp: " + timer2Timestamp +
            "  type: WATERMARK" +
            "} " +
            "list_updates {" +
            "  tag: \"" + bufferTag + "\"" +
            "  values {" +
            "    timestamp: 9223372036854775000" +
            "    data: \"" + bufferData + "\"" +
            "  }" +
            "}" +
            "list_updates {" +
            "  tag: \"" + watermarkHoldTag + "\"" +
            "  values {" +
            "    timestamp: 0" +
            "    data: \"" + watermarkHoldData + "\"" +
            "  }" +
        "}").build();

    assertThat(actualOutput.getOutputTimersCount(), equalTo(expectedOutput.getOutputTimersCount()));
    for (int i = 0; i < actualOutput.getOutputTimersCount(); i++) {
      assertThat(actualOutput.getOutputTimers(i), equalTo(expectedOutput.getOutputTimers(i)));
    }

    assertThat(actualOutput.getListUpdatesCount(), equalTo(expectedOutput.getListUpdatesCount()));
    for (int i = 0; i < actualOutput.getListUpdatesCount(); i++) {
      assertThat(actualOutput.getListUpdates(i), equalTo(expectedOutput.getListUpdates(i)));
    }

    server.addWorkToOffer(buildTimerInput(
        "work {" +
            "  computation_id: \"" + DEFAULT_COMPUTATION_ID + "\"" +
            "  input_data_watermark: 0" +
            "  work {" +
            "    key: \"" + DEFAULT_KEY_STRING + "\"" +
            "    work_token: 1" +
            "    timers {" +
            "      timers {" +
            "        tag: \"" + timer3Tag + "\"" +
            "        timestamp: " + timer2Timestamp +
            "      }" +
            "    }" +
            "  }" +
        "}"));

    server.addDataToOffer(buildData(
        "data {" +
            "  computation_id: \"" + DEFAULT_COMPUTATION_ID + "\"" +
            "  data {" +
            "    key: \"" + DEFAULT_KEY_STRING + "\"" +
            "    lists {" +
            "      tag: \"" + watermarkHoldTag + "\"" +
            "      values {" +
            "        timestamp: 0" +
            "        data: \"" + watermarkHoldData + "\"" +
            "      }" +
            "    }" +
            "  }" +
        "}"));

    server.addDataToOffer(buildData(
        "data {" +
            "  computation_id: \"" + DEFAULT_COMPUTATION_ID + "\"" +
            "  data {" +
            "    key: \"" + DEFAULT_KEY_STRING + "\"" +
            "    lists {" +
            "      tag: \"" + bufferTag + "\"" +
            "      values {" +
            "        timestamp: 0" +
            "        data: \"" + bufferData + "\"" +
            "      }" +
            "    }" +
            "  }" +
        "}"));

    result = server.waitForAndGetCommits(1);

    assertThat(stripCounters(result.get(1L)),
        equalTo(setMessagesMetadata(windowAtZeroBytes(),
            parseCommitRequest(
                "key: \"" + DEFAULT_KEY_STRING + "\" " +
                    "work_token: 1 " +
                    "output_messages {" +
                    "  destination_stream_id: \"" + DEFAULT_DESTINATION_STREAM_ID + "\"" +
                    "  bundles {" +
                    "    key: \"" + DEFAULT_KEY_STRING + "\"" +
                    "    messages {" +
                    "      timestamp: 0" +
                    "      data: \"" + outputData + "\"" +
                    "    }" +
                    "  }" +
                    "} " +
                    "list_updates {" +
                    "  tag: \"" + watermarkHoldTag + "\"" +
                    "  end_timestamp: 9223372036854775807" +
                    "}" +
                    "list_updates {" +
                    "  tag: \"" + bufferTag + "\"" +
                    "  end_timestamp: 9223372036854775807" +
                "}")).build()));
  }

  static class PrintFn extends DoFn<KV<Integer, Integer>, String> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) {
      KV<Integer, Integer> elem = c.element();
      c.output(elem.getKey() + ":" + elem.getValue());
    }
  }

  @Test
  public void testUnboundedSources() throws Exception {
    List<Integer> finalizeTracker = Lists.newArrayList();

    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setNumWorkers(1);

    List<ParallelInstruction> instructions =
        Arrays.asList(new ParallelInstruction()
            .setSystemName("Read")
            .setRead(new ReadInstruction()
                .setSource(
                    BasicSerializableSourceFormat.serializeToCloudSource(
                        new CountingSource(1), options)))
            .setOutputs(
                Arrays.asList(new InstructionOutput()
                    .setName("read_output")
                    .setCodec(
                        WindowedValue.getFullCoder(
                            KvCoder.of(VarIntCoder.of(), VarIntCoder.of()),
                            GlobalWindow.Coder.INSTANCE)
                        .asCloudObject()))),
            makeDoFnInstruction(
                new PrintFn(), 0, StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE),
            makeSinkInstruction(StringUtf8Coder.of(), 1, GlobalWindow.Coder.INSTANCE));

    CountingSource.setFinalizeTracker(finalizeTracker);

    FakeWindmillServer server = new FakeWindmillServer();
    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(defaultMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    // Test new key.
    server.addWorkToOffer(buildInput(
        "work {" +
        "  computation_id: \"computation\"" +
        "  input_data_watermark: 0" +
        "  work {" +
        "    key: \"0000000000000001\"" +
        "    work_token: 1" +
        "  }" +
        "}", null));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);

    Windmill.WorkItemCommitRequest commit = stripCounters(result.get(1L));
    UnsignedLong finalizeId =
        UnsignedLong.fromLongBits(commit.getSourceStateUpdates().getFinalizeIds(0));

    assertThat(commit,
        equalTo(setMessagesMetadata(
            CoderUtils.encodeToByteArray(
                CollectionCoder.of(GlobalWindow.Coder.INSTANCE),
                Arrays.asList(GlobalWindow.INSTANCE)),
            parseCommitRequest(
                "key: \"0000000000000001\" " +
                "work_token: 1 " +
                "output_messages {" +
                "  destination_stream_id: \"out\"" +
                "  bundles {" +
                "    key: \"0000000000000001\"" +
                "    messages {" +
                "      timestamp: 0" +
                "      data: \"0:0\"" +
                "    }" +
                "  }" +
                "} " +
                "source_state_updates {" +
                "  state: \"\000\"" +
                "  finalize_ids: " + finalizeId +
                "} " +
                "source_watermark: 9223372036854775000")).build()));

    // Test same key continuing.
    server.addWorkToOffer(buildInput(
        "work {" +
        "  computation_id: \"computation\"" +
        "  input_data_watermark: 0" +
        "  work {" +
        "    key: \"0000000000000001\"" +
        "    work_token: 2" +
        "    source_state {" +
        "      state: \"\000\"" +
        "      finalize_ids: " + finalizeId +
        "    } " +
        "  }" +
        "}", null));

    result = server.waitForAndGetCommits(1);

    commit = stripCounters(result.get(2L));
    finalizeId = UnsignedLong.fromLongBits(commit.getSourceStateUpdates().getFinalizeIds(0));

    assertThat(commit,
        equalTo(parseCommitRequest(
            "key: \"0000000000000001\" " +
            "work_token: 2 " +
            "source_state_updates {" +
            "  state: \"\000\"" +
            "  finalize_ids: " + finalizeId +
            "} " +
            "source_watermark: 9223372036854775000").build()));

    assertThat(finalizeTracker, contains(0));

    // Test recovery.
    server.addWorkToOffer(buildInput(
        "work {" +
        "  computation_id: \"computation\"" +
        "  input_data_watermark: 0" +
        "  work {" +
        "    key: \"0000000000000002\"" +
        "    work_token: 3" +
        "    source_state {" +
        "      state: \"\005\"" +
        "    } " +
        "  }" +
        "}", null));

    result = server.waitForAndGetCommits(1);

    commit = stripCounters(result.get(3L));
    finalizeId = UnsignedLong.fromLongBits(commit.getSourceStateUpdates().getFinalizeIds(0));

    assertThat(commit,
        equalTo(setMessagesMetadata(
            CoderUtils.encodeToByteArray(
                CollectionCoder.of(GlobalWindow.Coder.INSTANCE),
                Arrays.asList(GlobalWindow.INSTANCE)),
            parseCommitRequest(
                "key: \"0000000000000002\" " +
                "work_token: 3 " +
                "output_messages {" +
                "  destination_stream_id: \"out\"" +
                "  bundles {" +
                "    key: \"0000000000000002\"" +
                "    messages {" +
                "      timestamp: 5000" +
                "      data: \"1:5\"" +
                "    }" +
                "  }" +
                "} " +
                "source_state_updates {" +
                "  state: \"\005\"" +
                "  finalize_ids: " + finalizeId +
                "} " +
                "source_watermark: 9223372036854775000")).build()));
  }
}
