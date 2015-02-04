/*
 * Copyright (C) 2014 Google Inc.
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
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.WindmillServerStub;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
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
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.CodedTupleTagMap;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Unit tests for {@link StreamingDataflowWorker}. */
@RunWith(JUnit4.class)
public class StreamingDataflowWorkerTest {
  private static final IntervalWindow DEFAULT_WINDOW =
      new IntervalWindow(new Instant(1234), new Duration(1000));

  private static class FakeWindmillServer extends WindmillServerStub {
    private Queue<Windmill.GetWorkResponse> workToOffer;
    private Queue<Windmill.GetDataResponse> dataToOffer;
    private Map<Long, Windmill.WorkItemCommitRequest> commitsReceived;
    private LinkedBlockingQueue<Windmill.Exception> exceptions;
    private int commitsRequested = 0;

    public FakeWindmillServer() {
      workToOffer = new ConcurrentLinkedQueue<Windmill.GetWorkResponse>();
      dataToOffer = new ConcurrentLinkedQueue<Windmill.GetDataResponse>();
      commitsReceived = new ConcurrentHashMap<Long, Windmill.WorkItemCommitRequest>();
      exceptions = new LinkedBlockingQueue<>();
    }

    public void addWorkToOffer(Windmill.GetWorkResponse work) {
      workToOffer.add(work);
    }

    public void addDataToOffer(Windmill.GetDataResponse data) {
      dataToOffer.add(data);
    }

    @Override
    public Windmill.GetWorkResponse getWork(Windmill.GetWorkRequest request) {
      Windmill.GetWorkResponse response = workToOffer.poll();
      if (response == null) {
        return Windmill.GetWorkResponse.newBuilder().build();
      }
      return response;
    }

    @Override
    public Windmill.GetDataResponse getData(Windmill.GetDataRequest request) {
      Windmill.GetDataResponse response = dataToOffer.poll();
      if (response == null) {
        return Windmill.GetDataResponse.newBuilder().build();
      }
      return response;
    }

    @Override
    public Windmill.CommitWorkResponse commitWork(Windmill.CommitWorkRequest request) {
      for (Windmill.ComputationCommitWorkRequest computationRequest : request.getRequestsList()) {
        for (Windmill.WorkItemCommitRequest commit : computationRequest.getRequestsList()) {
          commitsReceived.put(commit.getWorkToken(), commit);
        }
      }
      return Windmill.CommitWorkResponse.newBuilder().build();
    }

    @Override
    public Windmill.GetConfigResponse getConfig(Windmill.GetConfigRequest request) {
      return Windmill.GetConfigResponse.newBuilder().build();
    }

    @Override
    public Windmill.ReportStatsResponse reportStats(Windmill.ReportStatsRequest request) {
      for (Windmill.Exception exception : request.getExceptionsList()) {
        try {
          exceptions.put(exception);
        } catch (InterruptedException e) {}
      }
      return Windmill.ReportStatsResponse.newBuilder().build();
    }

    public Map<Long, Windmill.WorkItemCommitRequest> waitForAndGetCommits(int numCommits) {
      while (commitsReceived.size() < commitsRequested + numCommits) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {}
      }

      commitsRequested += numCommits;

      return commitsReceived;
    }

    public Windmill.Exception getException() throws InterruptedException {
      return exceptions.take();
    }
  }

  private ParallelInstruction makeWindowingSourceInstruction(Coder coder) {
    CloudObject encodedCoder = FullWindowedValueCoder.of(
        TimerOrElementCoder.of(coder), IntervalWindow.getCoder()).asCloudObject();
    return new ParallelInstruction()
        .setSystemName("source")
        .setRead(new ReadInstruction().setSource(
            new Source()
            .setSpec(CloudObject.forClass(WindowingWindmillReader.class))
            .setCodec(encodedCoder)))
        .setOutputs(Arrays.asList(
            new InstructionOutput()
            .setName("read_output")
            .setCodec(encodedCoder)));
  }

  private ParallelInstruction makeSourceInstruction(Coder coder) {
    return new ParallelInstruction()
        .setSystemName("source")
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
      DoFn<?, ?> doFn, int producerIndex, Coder outputCoder) {
    CloudObject spec = CloudObject.forClassName("DoFn");
    addString(spec, PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(new DoFnInfo(doFn, null))));
    return new ParallelInstruction()
        .setSystemName("parDo")
        .setParDo(new ParDoInstruction()
            .setInput(
                new InstructionInput().setProducerInstructionIndex(producerIndex).setOutputNum(0))
            .setNumOutputs(1)
            .setUserFn(spec))
        .setOutputs(Arrays.asList(
            new InstructionOutput()
            .setName("par_do_output")
            .setCodec(WindowedValue.getFullCoder(outputCoder, IntervalWindow.getCoder())
                                   .asCloudObject())));
  }

  private ParallelInstruction makeSinkInstruction(Coder coder, int producerIndex) {
    CloudObject spec = CloudObject.forClass(WindmillSink.class);
    addString(spec, "stream_id", "out");
    return new ParallelInstruction()
        .setSystemName("sink")
        .setWrite(new WriteInstruction()
            .setInput(
                new InstructionInput().setProducerInstructionIndex(producerIndex).setOutputNum(0))
            .setSink(new Sink()
                .setSpec(spec)
                .setCodec(WindowedValue.getFullCoder(coder, IntervalWindow.getCoder())
                                       .asCloudObject())));
  }

  private MapTask makeMapTask(List<ParallelInstruction> instructions) {
    return new MapTask()
        .setStageName("computation")
        .setSystemName("computation")
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
    Windmill.InputMessageBundle.Builder messageBundleBuilder =
        builder.getWorkBuilder(0)
        .getWorkBuilder(0)
        .getMessageBundlesBuilder(0);
    for (Windmill.Message.Builder messageBuilder : messageBundleBuilder.getMessagesBuilderList()) {
      messageBuilder.setMetadata(ByteString.copyFrom(metadata));
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
        "  computation_id: \"computation\"" +
        "  work {" +
        "    key: \"key\"" +
        "    work_token: " + index +
        "    message_bundles {" +
        "      source_computation_id: \"upstream\"" +
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

  private Windmill.WorkItemCommitRequest buildExpectedOutput(String output) throws Exception {
    Windmill.WorkItemCommitRequest.Builder builder = Windmill.WorkItemCommitRequest.newBuilder();
    TextFormat.merge(output, builder);
    return builder.build();
  }

  private Windmill.WorkItemCommitRequest buildExpectedOutput(String output, byte[] metadata)
      throws Exception {
    Windmill.WorkItemCommitRequest.Builder builder = Windmill.WorkItemCommitRequest.newBuilder();
    TextFormat.merge(output, builder);
    builder.getOutputMessagesBuilder(0)
           .getBundlesBuilder(0)
           .getMessagesBuilder(0)
           .setMetadata(ByteString.copyFrom(metadata));
    return builder.build();
  }

  private Windmill.WorkItemCommitRequest makeExpectedOutput(int index, long timestamp, String key)
      throws Exception {
    return buildExpectedOutput(
        "key: \"key\" " +
        "work_token: " + index + " " +
        "output_messages {" +
        "  destination_stream_id: \"out\"" +
        "  bundles {" +
        "    key: \"" + key + "\"" +
        "    messages {" +
        "      timestamp: " + timestamp +
        "      data: \"data" + index + "\"" +
        "      metadata: \"\"" +
        "    }" +
        "  }" +
        "}",
        CoderUtils.encodeToByteArray(CollectionCoder.of(IntervalWindow.getCoder()),
                                     Arrays.asList(DEFAULT_WINDOW)));
  }

  private DataflowPipelineOptions createTestingPipelineOptions() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("StreamingWorkerHarnessTest");
    options.setStreaming(true);
    return options;
  }

  private Windmill.WorkItemCommitRequest stripCounters(Windmill.WorkItemCommitRequest request) {
    return Windmill.WorkItemCommitRequest.newBuilder(request).clearCounterUpdates().build();
  }

  private Windmill.WorkItemCommitRequest stripProcessingTimeCounters(
      Windmill.WorkItemCommitRequest request) {
    Windmill.WorkItemCommitRequest.Builder builder =
        Windmill.WorkItemCommitRequest.newBuilder(request).clearCounterUpdates();
    TreeMap<String, Windmill.Counter> sortedCounters = new TreeMap<>();
    for (Windmill.Counter counter : request.getCounterUpdatesList()) {
      String name = counter.getName();
      if (!(name.startsWith("computation-") && name.endsWith("-msecs"))) {
        sortedCounters.put(name, counter);
      }
    }
    for (Windmill.Counter counter : sortedCounters.values()) {
      builder.addCounterUpdates(counter);
    }
    return builder.build();
  }


  @Test public void testBasicHarness() throws Exception {
    List<ParallelInstruction> instructions = Arrays.asList(
        makeSourceInstruction(StringUtf8Coder.of()),
        makeSinkInstruction(StringUtf8Coder.of(), 0));

    FakeWindmillServer server = new FakeWindmillServer();
    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(makeMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    final int numIters = 2000;
    for (int i = 0; i < numIters; ++i) {
      server.addWorkToOffer(makeInput(i, TimeUnit.MILLISECONDS.toMicros(i)));
    }

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(numIters);
    worker.stop();

    for (int i = 0; i < numIters; ++i) {
      Assert.assertTrue(result.containsKey((long) i));
      Assert.assertEquals(makeExpectedOutput(i, TimeUnit.MILLISECONDS.toMicros(i), "key"),
                          stripCounters(result.get((long) i)));
    }
  }

  static class ChangeKeysFn extends DoFn<KV<String, String>, KV<String, String>> {
    @Override
    public void processElement(ProcessContext c) {
      KV<String, String> elem = c.element();
      c.output(KV.of(elem.getKey() + "_" + elem.getValue(), elem.getValue()));
    }
  }

  @Test public void testKeyChange() throws Exception {
    KvCoder<String, String> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    List<ParallelInstruction> instructions = Arrays.asList(
        makeSourceInstruction(kvCoder),
        makeDoFnInstruction(new ChangeKeysFn(), 0, kvCoder),
        makeSinkInstruction(kvCoder, 1));

    FakeWindmillServer server = new FakeWindmillServer();
    server.addWorkToOffer(makeInput(0, 0));
    server.addWorkToOffer(makeInput(1, TimeUnit.MILLISECONDS.toMicros(1)));
    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(makeMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(2);

    Assert.assertEquals(makeExpectedOutput(0, 0, "key_data0"), stripCounters(result.get(0L)));
    Assert.assertEquals(makeExpectedOutput(1, TimeUnit.MILLISECONDS.toMicros(1), "key_data1"),
                        stripCounters(result.get(1L)));
  }

  static class TestStateFn extends DoFn<KV<String, String>, KV<String, String>>
      implements DoFn.RequiresKeyedState {
    @Override
    public void processElement(ProcessContext c) {
      try {
        CodedTupleTag<String> stateTag = CodedTupleTag.of("state", StringUtf8Coder.of());
        CodedTupleTag<String> emptyStateTag =
            CodedTupleTag.of("other_state", StringUtf8Coder.of());
        CodedTupleTagMap result =
            c.keyedState().lookup(Arrays.asList(stateTag, emptyStateTag));
        Assert.assertNull(result.get(emptyStateTag));
        String state = result.get(stateTag);
        state += "-" + c.element().getValue();
        c.keyedState().store(CodedTupleTag.of("state", StringUtf8Coder.of()), state);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test public void testState() throws Exception {
    KvCoder<String, String> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    List<ParallelInstruction> instructions = Arrays.asList(
        makeSourceInstruction(kvCoder),
        makeDoFnInstruction(new TestStateFn(), 0, kvCoder),
        makeSinkInstruction(kvCoder, 1));

    FakeWindmillServer server = new FakeWindmillServer();
    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(makeMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    server.addDataToOffer(buildData(
        "data {" +
        "  computation_id: \"computation\"" +
        "  data {" +
        "    key: \"key0\"" +
        "    values {" +
        "      tag: \"5:parDostate\"" +
        "      value {" +
        "        timestamp: 0" +
        "        data: \"key0\"" +
        "      }" +
        "    }" +
        "  }" +
        "}"));
    server.addDataToOffer(buildData(
        "data {" +
        "  computation_id: \"computation\"" +
        "  data {" +
        "    key: \"key0\"" +
        "    values {" +
        "      tag: \"5:Stagestate\"" +
        "      value {" +
        "        timestamp: 1" +
        "        data: \"key0\"" +
        "      }" +
        "    }" +
        "  }" +
        "}"));

    server.addWorkToOffer(buildInput(
        "work {" +
        "  computation_id: \"computation\"" +
        "  work {" +
        "    key: \"key0\"" +
        "    work_token: 0" +
        "    message_bundles {" +
        "      source_computation_id: \"upstream\"" +
        "      messages {" +
        "        timestamp: 0" +
        "        data: \"0\"" +
        "      }" +
        "      messages {" +
        "        timestamp: 1" +
        "        data: \"1\"" +
        "      }" +
        "    }" +
        "  }" +
        "}",
        CoderUtils.encodeToByteArray(CollectionCoder.of(IntervalWindow.getCoder()),
                                     Arrays.asList(DEFAULT_WINDOW))));

    server.waitForAndGetCommits(1);

    server.addDataToOffer(buildData(
        "data {" +
        "  computation_id: \"computation\"" +
        "  data {" +
        "    key: \"key1\"" +
        "    values {" +
        "      tag: \"5:parDostate\"" +
        "      value {" +
        "        timestamp: 0" +
        "        data: \"key1\"" +
        "      }" +
        "    }" +
        "  }" +
        "}"));

    server.addWorkToOffer(buildInput(
        "work {" +
        "  computation_id: \"computation\"" +
        "  work {" +
        "    key: \"key1\"" +
        "    work_token: 1" +
        "    message_bundles {" +
        "      source_computation_id: \"upstream\"" +
        "      messages {" +
        "        timestamp: 2" +
        "        data: \"2\"" +
        "      }" +
        "    }" +
        "  }" +
        "}",
        CoderUtils.encodeToByteArray(CollectionCoder.of(IntervalWindow.getCoder()),
            Arrays.asList(DEFAULT_WINDOW))));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);

    Assert.assertEquals(buildExpectedOutput(
        "key: \"key0\" " +
        "work_token: 0 " +
        "value_updates {" +
        "  tag: \"5:parDostate\"" +
        "  value {" +
        "    timestamp: 9223372036854775807" +
        "    data: \"key0-0-1\"" +
        "  }" +
        "} " +
        "counter_updates {" +
        "  name: \"par_do_output-ElementCount\"" +
        "  kind: SUM" +
        "} " +
        "counter_updates {" +
        "  name: \"read_output-ElementCount\"" +
        "  kind: SUM" +
        "  int_scalar: 2" +
        "} " +
        "counter_updates {" +
        "  name: \"read_output-MeanByteCount\"" +
        "  kind: MEAN" +
        "  int_scalar: 70" +
        "  mean_count: 2" +
        "} " +
        "counter_updates {" +
        "  name: \"sink-ByteCount\"" +
        "  kind: SUM" +
        "}" +
        "counter_updates {" +
        "  name: \"source-ByteCount\"" +
        "  kind: SUM" +
        "  int_scalar: 10" +
        "} "),
        stripProcessingTimeCounters(result.get(0L)));

    Assert.assertEquals(buildExpectedOutput(
        "key: \"key1\" " +
        "work_token: 1 " +
        "value_updates {" +
        "  tag: \"5:parDostate\"" +
        "  value {" +
        "    timestamp: 9223372036854775807" +
        "    data: \"key1-2\"" +
        "  }" +
        "}" +
        "counter_updates {" +
        "  name: \"par_do_output-ElementCount\"" +
        "  kind: SUM" +
        "} " +
        "counter_updates {" +
        "  name: \"read_output-ElementCount\"" +
        "  kind: SUM" +
        "  int_scalar: 1" +
        "} " +
        "counter_updates {" +
        "  name: \"read_output-MeanByteCount\"" +
        "  kind: MEAN" +
        "  int_scalar: 35" +
        "  mean_count: 1" +
        "} " +
        "counter_updates {" +
        "  name: \"sink-ByteCount\"" +
        "  kind: SUM" +
        "} " +
        "counter_updates {" +
        "  name: \"source-ByteCount\"" +
        "  kind: SUM" +
        "  int_scalar: 5" +
        "} "),
        stripProcessingTimeCounters(result.get(1L)));
  }

  static class TestExceptionFn extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) throws Exception {
      try {
        throw new Exception("Exception!");
      } catch (Exception e) {
        throw new Exception("Another exception!", e);
      }
    }
  }

  @Test public void testExceptions() throws Exception {
    List<ParallelInstruction> instructions = Arrays.asList(
        makeSourceInstruction(StringUtf8Coder.of()),
        makeDoFnInstruction(new TestExceptionFn(), 0, StringUtf8Coder.of()),
        makeSinkInstruction(StringUtf8Coder.of(), 1));

    FakeWindmillServer server = new FakeWindmillServer();
    server.addWorkToOffer(buildInput(
        "work {" +
        "  computation_id: \"computation\"" +
        "  work {" +
        "    key: \"key0\"" +
        "    work_token: 0" +
        "    message_bundles {" +
        "      source_computation_id: \"upstream\"" +
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
        Arrays.asList(makeMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    Windmill.Exception exception = server.getException();

    Assert.assertThat(exception.getStackFrames(0),
        JUnitMatchers.containsString("Another exception!"));
    Assert.assertThat(exception.getStackFrames(1),
        JUnitMatchers.containsString("processElement"));
    Assert.assertTrue(exception.hasCause());

    Assert.assertThat(exception.getCause().getStackFrames(0),
        JUnitMatchers.containsString("Exception!"));
    Assert.assertThat(exception.getCause().getStackFrames(1),
        JUnitMatchers.containsString("processElement"));
    Assert.assertFalse(exception.getCause().hasCause());
  }

  private static class TestTimerFn
      extends AssignWindowsDoFn<KV<String, String>, BoundedWindow> {
    public TestTimerFn() {
      super(null);
    }
    @Override
    public void processElement(ProcessContext c) {
      c.output(KV.of("key0", Long.toString(c.timestamp().getMillis())));
    }
  }

  @Test public void testTimers() throws Exception {
    KvCoder<String, String> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    List<ParallelInstruction> instructions = Arrays.asList(
        makeWindowingSourceInstruction(kvCoder),
        makeDoFnInstruction(new TestTimerFn(), 0, kvCoder),
        makeSinkInstruction(kvCoder, 1));

    FakeWindmillServer server = new FakeWindmillServer();

    server.addWorkToOffer(buildTimerInput(
        "work {" +
        "  computation_id: \"computation\"" +
        "  work {" +
        "    key: \"key0\"" +
        "    work_token: 0" +
        "    timers {" +
        "      timers {" +
        "        tag: \"tag\"" +
        "        timestamp: 3000" +
        "      }" +
        "    }" +
        "  }" +
        "}"));

    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(makeMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);

    Assert.assertEquals(buildExpectedOutput(
        "key: \"key0\" " +
        "work_token: 0 " +
        "output_messages {" +
        "  destination_stream_id: \"out\"" +
        "  bundles {" +
        "    key: \"key0\"" +
        "    messages {" +
        "      timestamp: 3000" +
        "      data: \"3\"" +
        "    }" +
        "  }" +
        "} ",
        CoderUtils.encodeToByteArray(CollectionCoder.of(IntervalWindow.getCoder()),
                                     new ArrayList())),
        stripCounters(result.get(0L)));
  }

  @Test public void testAssignWindows() throws Exception {
    Duration gapDuration = Duration.standardSeconds(1);
    CloudObject spec = CloudObject.forClassName("AssignWindowsDoFn");
    addString(spec, PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(FixedWindows.of(gapDuration))));

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

    server.addWorkToOffer(makeInput(0, 0));
    server.addWorkToOffer(makeInput(1000000, 1000000));

    StreamingDataflowWorker worker = new StreamingDataflowWorker(
        Arrays.asList(makeMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(2);

    Assert.assertEquals(buildExpectedOutput(
        "key: \"key\" " +
        "work_token: 0 " +
        "output_messages {" +
        "  destination_stream_id: \"out\"" +
        "  bundles {" +
        "    key: \"key\"" +
        "    messages {" +
        "      timestamp: 0" +
        "      data: \"data0\"" +
        "    }" +
        "  }" +
        "} ",
        CoderUtils.encodeToByteArray(
            CollectionCoder.of(IntervalWindow.getCoder()),
            Arrays.asList(new IntervalWindow(new Instant(0), new Instant(1000))))),
        stripCounters(result.get(0L)));

    Windmill.WorkItemCommitRequest.Builder expected = buildExpectedOutput(
        "key: \"key\" " +
        "work_token: 1000000 " +
        "output_messages {" +
        "  destination_stream_id: \"out\"" +
        "  bundles {" +
        "    key: \"key\"" +
        "    messages {" +
        "      timestamp: 1000000" +
        "      data: \"data1000000\"" +
        "    }" +
        "  }" +
        "} ",
        CoderUtils.encodeToByteArray(
            CollectionCoder.of(IntervalWindow.getCoder()),
            Arrays.asList(new IntervalWindow(new Instant(1000), new Instant(2000))))).toBuilder();

    Assert.assertEquals(expected.build(), stripCounters(result.get(1000000L)));
  }

  @Test public void testMergeWindows() throws Exception {
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
            SerializableUtils.serializeToByteArray(FixedWindows.of(Duration.standardSeconds(1)))));
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
        Arrays.asList(makeMapTask(instructions)), server, createTestingPipelineOptions());
    worker.start();

    server.addWorkToOffer(buildInput(
        "work {" +
        "  computation_id: \"computation\"" +
        "  work {" +
        "    key: \"key\"" +
        "    work_token: 0" +
        "    message_bundles {" +
        "      source_computation_id: \"upstream\"" +
        "      messages {" +
        "        timestamp: 0" +
        "        data: \"data0\"" +
        "      }" +
        "    }" +
        "  }" +
        "}",
        CoderUtils.encodeToByteArray(
            CollectionCoder.of(IntervalWindow.getCoder()),
            Arrays.asList(new IntervalWindow(new Instant(0), new Instant(1000))))));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);

    Assert.assertEquals(buildExpectedOutput(
        "key: \"key\" " +
        "work_token: 0 " +
        "output_timers {" +
        "  tag: \"gAAAAAAAAAA=\"" +
        "  timestamp: 999000" +
        "} " +
        "list_updates {" +
        "  tag: \"12:MergeWindowsbuffer:gAAAAAAAAAA=\"" +
        "    values {" +
        "    timestamp: 0" +
        "    data: \"data0\"" +
        "  }" +
        "}"),
        stripCounters(result.get(0L)));

    server.addWorkToOffer(buildTimerInput(
        "work {" +
        "  computation_id: \"computation\"" +
        "  work {" +
        "    key: \"key\"" +
        "    work_token: 1" +
        "    timers {" +
        "      timers {" +
        "        tag: \"gAAAAAAAAAA=\"" +
        "        timestamp: 999000" +
        "      }" +
        "    }" +
        "  }" +
        "}"));
    server.addDataToOffer(buildData(
        "data {" +
        "  computation_id: \"computation\"" +
        "  data {" +
        "    key: \"key\"" +
        "    lists {" +
        "      tag: \"12:MergeWindowsbuffer:gAAAAAAAAAA=\"" +
        "      values {" +
        "        timestamp: 0" +
        "        data: \"data0\"" +
        "      }" +
        "    }" +
        "  }" +
        "}"));

    result = server.waitForAndGetCommits(1);

    Assert.assertEquals(buildExpectedOutput(
        "key: \"key\" " +
        "work_token: 1 " +
        "output_messages {" +
        "  destination_stream_id: \"out\"" +
        "  bundles {" +
        "    key: \"key\"" +
        "    messages {" +
        "      timestamp: 999000" +
        "      data: \"\000\000\000\001\005data0\"" +
        "    }" +
        "  }" +
        "} " +
        "list_updates {" +
        "  tag: \"12:MergeWindowsbuffer:gAAAAAAAAAA=\"" +
        "  end_timestamp: 9223372036854775807" +
        "}",
        CoderUtils.encodeToByteArray(
            CollectionCoder.of(IntervalWindow.getCoder()),
            Arrays.asList(new IntervalWindow(new Instant(0), new Instant(1000))))),
        stripCounters(result.get(1L)));
  }
}
