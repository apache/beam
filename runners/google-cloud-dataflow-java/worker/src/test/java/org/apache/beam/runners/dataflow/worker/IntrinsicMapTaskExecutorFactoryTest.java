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

import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.apache.beam.runners.dataflow.worker.DataflowOutputCounter.getElementCounterName;
import static org.apache.beam.runners.dataflow.worker.DataflowOutputCounter.getMeanByteCounterName;
import static org.apache.beam.runners.dataflow.worker.DataflowOutputCounter.getObjectCounterName;
import static org.apache.beam.runners.dataflow.worker.counters.CounterName.named;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.FlattenInstruction;
import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.PartialGroupByKeyInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.WriteInstruction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.apiary.FixMultiOutputInfosOnParDoInstructions;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.Counter.CounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterMean;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.MultiOutputInfoEdge;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutionLocation;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.OperationNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.util.common.worker.FlattenOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.runners.dataflow.worker.util.common.worker.WriteOperation;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.Network;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link IntrinsicMapTaskExecutorFactory}. */
@RunWith(JUnit4.class)
public class IntrinsicMapTaskExecutorFactoryTest {
  private static final String STAGE = "test";

  private static final IdGenerator idGenerator = IdGenerators.decrementingLongs();

  private static final Function<MapTask, MutableNetwork<Node, Edge>> mapTaskToNetwork =
      new FixMultiOutputInfosOnParDoInstructions(idGenerator)
          .andThen(new MapTaskToNetworkFunction(idGenerator));

  private static final CloudObject windowedStringCoder =
      CloudObjects.asCloudObject(
          WindowedValue.getValueOnlyCoder(StringUtf8Coder.of()), /*sdkComponents=*/ null);

  private IntrinsicMapTaskExecutorFactory mapTaskExecutorFactory;
  private PipelineOptions options;
  private ReaderRegistry readerRegistry;
  private SinkRegistry sinkRegistry;
  private static final String PCOLLECTION_ID = "fakeId";

  @Mock private Network<Node, Edge> network;
  @Mock private CounterUpdateExtractor<?> updateExtractor;

  private final CounterSet counterSet = new CounterSet();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    mapTaskExecutorFactory = IntrinsicMapTaskExecutorFactory.defaultFactory();
    options = PipelineOptionsFactory.create();
    readerRegistry =
        ReaderRegistry.defaultRegistry()
            .register(
                ReaderFactoryTest.TestReaderFactory.class.getName(),
                new ReaderFactoryTest.TestReaderFactory())
            .register(
                ReaderFactoryTest.SingletonTestReaderFactory.class.getName(),
                new ReaderFactoryTest.SingletonTestReaderFactory());

    sinkRegistry =
        SinkRegistry.defaultRegistry()
            .register(TestSinkFactory.class.getName(), new TestSinkFactory());
  }

  @Test
  public void testCreateMapTaskExecutor() throws Exception {
    List<ParallelInstruction> instructions =
        Arrays.asList(
            createReadInstruction("Read"),
            createParDoInstruction(0, 0, "DoFn1"),
            createParDoInstruction(0, 0, "DoFnWithContext"),
            createFlattenInstruction(1, 0, 2, 0, "Flatten"),
            createWriteInstruction(3, 0, "Write"));

    MapTask mapTask = new MapTask();
    mapTask.setStageName(STAGE);
    mapTask.setSystemName("systemName");
    mapTask.setInstructions(instructions);
    mapTask.setFactory(Transport.getJsonFactory());

    try (DataflowMapTaskExecutor executor =
        mapTaskExecutorFactory.create(
            null /* beamFnControlClientHandler */,
            null /* GrpcFnServer<GrpcDataService> */,
            null /* ApiServiceDescriptor */,
            null, /* GrpcFnServer<GrpcStateService> */
            mapTaskToNetwork.apply(mapTask),
            options,
            STAGE,
            readerRegistry,
            sinkRegistry,
            BatchModeExecutionContext.forTesting(options, counterSet, "testStage"),
            counterSet,
            idGenerator)) {
      // Safe covariant cast not expressible without rawtypes.
      @SuppressWarnings({"rawtypes", "unchecked"})
      List<Object> operations = (List) executor.operations;
      assertThat(
          operations,
          hasItems(
              instanceOf(ReadOperation.class),
              instanceOf(ParDoOperation.class),
              instanceOf(ParDoOperation.class),
              instanceOf(FlattenOperation.class),
              instanceOf(WriteOperation.class)));

      // Verify that the inputs are attached.
      ReadOperation readOperation =
          Iterables.getOnlyElement(Iterables.filter(operations, ReadOperation.class));
      assertEquals(2, readOperation.receivers[0].getReceiverCount());

      FlattenOperation flattenOperation =
          Iterables.getOnlyElement(Iterables.filter(operations, FlattenOperation.class));
      for (ParDoOperation operation : Iterables.filter(operations, ParDoOperation.class)) {
        assertSame(flattenOperation, operation.receivers[0].getOnlyReceiver());
      }
      WriteOperation writeOperation =
          Iterables.getOnlyElement(Iterables.filter(operations, WriteOperation.class));
      assertSame(writeOperation, flattenOperation.receivers[0].getOnlyReceiver());
    }

    @SuppressWarnings("unchecked")
    Counter<Long, ?> otherMsecCounter =
        (Counter<Long, ?>) counterSet.getExistingCounter("test-other-msecs");

    // "other" state only got created upon MapTaskExecutor.execute().
    assertNull(otherMsecCounter);

    counterSet.extractUpdates(false, updateExtractor);
    verifyOutputCounters(
        updateExtractor,
        "read_output_name",
        "DoFn1_output",
        "DoFnWithContext_output",
        "flatten_output_name");
    verify(updateExtractor).longSum(eq(named("Read-ByteCount")), anyBoolean(), anyLong());
    verify(updateExtractor).longSum(eq(named("Write-ByteCount")), anyBoolean(), anyLong());
    verifyNoMoreInteractions(updateExtractor);
  }

  private static void verifyOutputCounters(
      CounterUpdateExtractor<?> updateExtractor, String... outputNames) {
    for (String outputName : outputNames) {
      verify(updateExtractor)
          .longSum(eq(named(getElementCounterName(outputName))), anyBoolean(), anyLong());
      verify(updateExtractor)
          .longSum(eq(named(getObjectCounterName(outputName))), anyBoolean(), anyLong());
      verify(updateExtractor)
          .longMean(
              eq(named(getMeanByteCounterName(outputName))),
              anyBoolean(),
              Mockito.<CounterMean<Long>>any());
    }
  }

  @Test
  public void testExecutionContextPlumbing() throws Exception {
    List<ParallelInstruction> instructions =
        Arrays.asList(
            createReadInstruction("Read", ReaderFactoryTest.SingletonTestReaderFactory.class),
            createParDoInstruction(0, 0, "DoFn1", "DoFnUserName"),
            createParDoInstruction(1, 0, "DoFnWithContext", "DoFnWithContextUserName"));

    MapTask mapTask = new MapTask();
    mapTask.setStageName(STAGE);
    mapTask.setInstructions(instructions);
    mapTask.setFactory(Transport.getJsonFactory());

    BatchModeExecutionContext context =
        BatchModeExecutionContext.forTesting(options, counterSet, "testStage");

    try (DataflowMapTaskExecutor executor =
        mapTaskExecutorFactory.create(
            null /* beamFnControlClientHandler */,
            null /* beamFnDataService */,
            null /* beamFnStateService */,
            null,
            mapTaskToNetwork.apply(mapTask),
            options,
            STAGE,
            readerRegistry,
            sinkRegistry,
            context,
            counterSet,
            idGenerator)) {
      executor.execute();
    }

    List<String> stepNames = new ArrayList<>();
    for (BatchModeExecutionContext.StepContext stepContext : context.getAllStepContexts()) {
      stepNames.add(stepContext.getNameContext().systemName());
    }
    assertThat(stepNames, hasItems("DoFn1", "DoFnWithContext"));
  }

  static ParallelInstruction createReadInstruction(String name) {
    return createReadInstruction(name, ReaderFactoryTest.TestReaderFactory.class);
  }

  static ParallelInstruction createReadInstruction(
      String name, Class<? extends ReaderFactory> readerFactoryClass) {
    CloudObject spec = CloudObject.forClass(readerFactoryClass);

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(windowedStringCoder);

    ReadInstruction readInstruction = new ReadInstruction();
    readInstruction.setSource(cloudSource);

    InstructionOutput output = new InstructionOutput();
    output.setName("read_output_name");
    output.setCodec(windowedStringCoder);
    output.setOriginalName("originalName");
    output.setSystemName("systemName");

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setSystemName(name);
    instruction.setOriginalName(name + "OriginalName");
    instruction.setRead(readInstruction);
    instruction.setOutputs(Arrays.asList(output));

    return instruction;
  }

  @Test
  public void testCreateReadOperation() throws Exception {
    ParallelInstructionNode instructionNode =
        ParallelInstructionNode.create(createReadInstruction("Read"), ExecutionLocation.UNKNOWN);

    when(network.successors(instructionNode))
        .thenReturn(
            ImmutableSet.<Node>of(
                IntrinsicMapTaskExecutorFactory.createOutputReceiversTransform(STAGE, counterSet)
                    .apply(
                        InstructionOutputNode.create(
                            instructionNode.getParallelInstruction().getOutputs().get(0),
                            PCOLLECTION_ID))));
    when(network.outDegree(instructionNode)).thenReturn(1);

    Node operationNode =
        mapTaskExecutorFactory
            .createOperationTransformForParallelInstructionNodes(
                STAGE,
                network,
                PipelineOptionsFactory.create(),
                readerRegistry,
                sinkRegistry,
                BatchModeExecutionContext.forTesting(options, counterSet, "testStage"))
            .apply(instructionNode);
    assertThat(operationNode, instanceOf(OperationNode.class));
    assertThat(((OperationNode) operationNode).getOperation(), instanceOf(ReadOperation.class));
    ReadOperation readOperation = (ReadOperation) ((OperationNode) operationNode).getOperation();

    assertEquals(1, readOperation.receivers.length);
    assertEquals(0, readOperation.receivers[0].getReceiverCount());
    assertEquals(Operation.InitializationState.UNSTARTED, readOperation.initializationState);
    assertThat(readOperation.reader, instanceOf(ReaderFactoryTest.TestReader.class));

    counterSet.extractUpdates(false, updateExtractor);
    verifyOutputCounters(updateExtractor, "read_output_name");
    verify(updateExtractor).longSum(eq(named("Read-ByteCount")), anyBoolean(), anyLong());
    verifyNoMoreInteractions(updateExtractor);
  }

  static ParallelInstruction createWriteInstruction(
      int producerIndex, int producerOutputNum, String systemName) {
    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(producerIndex);
    cloudInput.setOutputNum(producerOutputNum);

    CloudObject spec =
        CloudObject.forClass(IntrinsicMapTaskExecutorFactoryTest.TestSinkFactory.class);

    com.google.api.services.dataflow.model.Sink cloudSink =
        new com.google.api.services.dataflow.model.Sink();
    cloudSink.setSpec(spec);
    cloudSink.setCodec(windowedStringCoder);

    WriteInstruction writeInstruction = new WriteInstruction();
    writeInstruction.setInput(cloudInput);
    writeInstruction.setSink(cloudSink);

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setWrite(writeInstruction);
    instruction.setSystemName(systemName);
    instruction.setOriginalName(systemName + "OriginalName");

    return instruction;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCreateWriteOperation() throws Exception {
    int producerIndex = 1;
    int producerOutputNum = 2;

    ParallelInstructionNode instructionNode =
        ParallelInstructionNode.create(
            createWriteInstruction(producerIndex, producerOutputNum, "WriteOperation"),
            ExecutionLocation.UNKNOWN);
    Node operationNode =
        mapTaskExecutorFactory
            .createOperationTransformForParallelInstructionNodes(
                STAGE,
                network,
                options,
                readerRegistry,
                sinkRegistry,
                BatchModeExecutionContext.forTesting(options, counterSet, "testStage"))
            .apply(instructionNode);
    assertThat(operationNode, instanceOf(OperationNode.class));
    assertThat(((OperationNode) operationNode).getOperation(), instanceOf(WriteOperation.class));
    WriteOperation writeOperation = (WriteOperation) ((OperationNode) operationNode).getOperation();

    assertEquals(0, writeOperation.receivers.length);
    assertEquals(Operation.InitializationState.UNSTARTED, writeOperation.initializationState);
    assertThat(writeOperation.sink, instanceOf(SizeReportingSinkWrapper.class));
    assertThat(
        ((SizeReportingSinkWrapper<?>) writeOperation.sink).getUnderlyingSink(),
        instanceOf(TestSink.class));

    counterSet.extractUpdates(false, updateExtractor);
    verify(updateExtractor).longSum(eq(named("WriteOperation-ByteCount")), anyBoolean(), anyLong());
    verifyNoMoreInteractions(updateExtractor);
  }

  static class TestDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  static class TestSink extends Sink<Integer> {
    @Override
    public SinkWriter<Integer> writer() {
      return new TestSinkWriter();
    }

    /** A sink writer that drops its input values, for testing. */
    static class TestSinkWriter implements SinkWriter<Integer> {
      @Override
      public long add(Integer outputElem) {
        return 4;
      }

      @Override
      public void close() {}

      @Override
      public void abort() throws IOException {
        close();
      }
    }
  }

  static class TestSinkFactory implements SinkFactory {
    @Override
    public TestSink create(
        CloudObject o,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext) {
      return new TestSink();
    }
  }

  static ParallelInstruction createParDoInstruction(
      int producerIndex, int producerOutputNum, String systemName) {
    return createParDoInstruction(producerIndex, producerOutputNum, systemName, "");
  }

  static ParallelInstruction createParDoInstruction(
      int producerIndex, int producerOutputNum, String systemName, String userName) {
    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(producerIndex);
    cloudInput.setOutputNum(producerOutputNum);

    TestDoFn fn = new TestDoFn();

    String serializedFn =
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(
                DoFnInfo.forFn(
                    fn,
                    WindowingStrategy.globalDefault(),
                    null /* side input views */,
                    null /* input coder */,
                    new TupleTag<>(PropertyNames.OUTPUT) /* main output id */,
                    DoFnSchemaInformation.create(),
                    Collections.emptyMap())));

    CloudObject cloudUserFn = CloudObject.forClassName("DoFn");
    addString(cloudUserFn, PropertyNames.SERIALIZED_FN, serializedFn);

    MultiOutputInfo mainOutputTag = new MultiOutputInfo();
    mainOutputTag.setTag("1");

    ParDoInstruction parDoInstruction = new ParDoInstruction();
    parDoInstruction.setInput(cloudInput);
    parDoInstruction.setNumOutputs(1);
    parDoInstruction.setMultiOutputInfos(ImmutableList.of(mainOutputTag));
    parDoInstruction.setUserFn(cloudUserFn);

    InstructionOutput output = new InstructionOutput();
    output.setName(systemName + "_output");
    output.setCodec(windowedStringCoder);
    output.setOriginalName("originalName");
    output.setSystemName("systemName");

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setParDo(parDoInstruction);
    instruction.setOutputs(Arrays.asList(output));
    instruction.setSystemName(systemName);
    instruction.setOriginalName(systemName + "OriginalName");
    instruction.setName(userName);
    return instruction;
  }

  @Test
  public void testCreateParDoOperation() throws Exception {
    int producerIndex = 1;
    int producerOutputNum = 2;

    BatchModeExecutionContext context =
        BatchModeExecutionContext.forTesting(options, counterSet, "testStage");
    ParallelInstructionNode instructionNode =
        ParallelInstructionNode.create(
            createParDoInstruction(producerIndex, producerOutputNum, "DoFn"),
            ExecutionLocation.UNKNOWN);

    Node outputReceiverNode =
        IntrinsicMapTaskExecutorFactory.createOutputReceiversTransform(STAGE, counterSet)
            .apply(
                InstructionOutputNode.create(
                    instructionNode.getParallelInstruction().getOutputs().get(0), PCOLLECTION_ID));

    when(network.successors(instructionNode)).thenReturn(ImmutableSet.of(outputReceiverNode));
    when(network.outDegree(instructionNode)).thenReturn(1);
    when(network.edgesConnecting(instructionNode, outputReceiverNode))
        .thenReturn(
            ImmutableSet.<Edge>of(
                MultiOutputInfoEdge.create(
                    instructionNode
                        .getParallelInstruction()
                        .getParDo()
                        .getMultiOutputInfos()
                        .get(0))));

    Node operationNode =
        mapTaskExecutorFactory
            .createOperationTransformForParallelInstructionNodes(
                STAGE, network, options, readerRegistry, sinkRegistry, context)
            .apply(instructionNode);
    assertThat(operationNode, instanceOf(OperationNode.class));
    assertThat(((OperationNode) operationNode).getOperation(), instanceOf(ParDoOperation.class));
    ParDoOperation parDoOperation = (ParDoOperation) ((OperationNode) operationNode).getOperation();

    assertEquals(1, parDoOperation.receivers.length);
    assertEquals(0, parDoOperation.receivers[0].getReceiverCount());
    assertEquals(Operation.InitializationState.UNSTARTED, parDoOperation.initializationState);
  }

  static ParallelInstruction createPartialGroupByKeyInstruction(
      int producerIndex, int producerOutputNum) {
    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(producerIndex);
    cloudInput.setOutputNum(producerOutputNum);

    PartialGroupByKeyInstruction pgbkInstruction = new PartialGroupByKeyInstruction();
    pgbkInstruction.setInput(cloudInput);
    pgbkInstruction.setInputElementCodec(
        CloudObjects.asCloudObject(
            FullWindowedValueCoder.of(
                KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()),
                IntervalWindowCoder.of()),
            /*sdkComponents=*/ null));

    InstructionOutput output = new InstructionOutput();
    output.setName("pgbk_output_name");
    output.setCodec(
        CloudObjects.asCloudObject(
            KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(BigEndianIntegerCoder.of())),
            /*sdkComponents=*/ null));
    output.setOriginalName("originalName");
    output.setSystemName("systemName");

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setOriginalName("pgbk_original_name");
    instruction.setSystemName("pgbk_system_name");
    instruction.setPartialGroupByKey(pgbkInstruction);
    instruction.setOutputs(Arrays.asList(output));

    return instruction;
  }

  @Test
  public void testCreatePartialGroupByKeyOperation() throws Exception {
    int producerIndex = 1;
    int producerOutputNum = 2;

    ParallelInstructionNode instructionNode =
        ParallelInstructionNode.create(
            createPartialGroupByKeyInstruction(producerIndex, producerOutputNum),
            ExecutionLocation.UNKNOWN);

    when(network.successors(instructionNode))
        .thenReturn(
            ImmutableSet.<Node>of(
                IntrinsicMapTaskExecutorFactory.createOutputReceiversTransform(STAGE, counterSet)
                    .apply(
                        InstructionOutputNode.create(
                            instructionNode.getParallelInstruction().getOutputs().get(0),
                            PCOLLECTION_ID))));
    when(network.outDegree(instructionNode)).thenReturn(1);

    Node operationNode =
        mapTaskExecutorFactory
            .createOperationTransformForParallelInstructionNodes(
                STAGE,
                network,
                PipelineOptionsFactory.create(),
                readerRegistry,
                sinkRegistry,
                BatchModeExecutionContext.forTesting(options, counterSet, "testStage"))
            .apply(instructionNode);
    assertThat(operationNode, instanceOf(OperationNode.class));
    assertThat(((OperationNode) operationNode).getOperation(), instanceOf(ParDoOperation.class));
    ParDoOperation pgbkOperation = (ParDoOperation) ((OperationNode) operationNode).getOperation();

    assertEquals(1, pgbkOperation.receivers.length);
    assertEquals(0, pgbkOperation.receivers[0].getReceiverCount());
    assertEquals(Operation.InitializationState.UNSTARTED, pgbkOperation.initializationState);
  }

  @Test
  public void testCreatePartialGroupByKeyOperationWithCombine() throws Exception {
    int producerIndex = 1;
    int producerOutputNum = 2;

    ParallelInstruction instruction =
        createPartialGroupByKeyInstruction(producerIndex, producerOutputNum);

    AppliedCombineFn<?, ?, ?, ?> combineFn =
        AppliedCombineFn.withInputCoder(
            Sum.ofIntegers(),
            CoderRegistry.createDefault(),
            KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));
    CloudObject cloudCombineFn = CloudObject.forClassName("CombineFn");
    addString(
        cloudCombineFn,
        PropertyNames.SERIALIZED_FN,
        byteArrayToJsonString(serializeToByteArray(combineFn)));
    instruction.getPartialGroupByKey().setValueCombiningFn(cloudCombineFn);

    ParallelInstructionNode instructionNode =
        ParallelInstructionNode.create(instruction, ExecutionLocation.UNKNOWN);

    when(network.successors(instructionNode))
        .thenReturn(
            ImmutableSet.<Node>of(
                IntrinsicMapTaskExecutorFactory.createOutputReceiversTransform(STAGE, counterSet)
                    .apply(
                        InstructionOutputNode.create(
                            instructionNode.getParallelInstruction().getOutputs().get(0),
                            PCOLLECTION_ID))));
    when(network.outDegree(instructionNode)).thenReturn(1);

    Node operationNode =
        mapTaskExecutorFactory
            .createOperationTransformForParallelInstructionNodes(
                STAGE,
                network,
                options,
                readerRegistry,
                sinkRegistry,
                BatchModeExecutionContext.forTesting(options, counterSet, "testStage"))
            .apply(instructionNode);
    assertThat(operationNode, instanceOf(OperationNode.class));
    assertThat(((OperationNode) operationNode).getOperation(), instanceOf(ParDoOperation.class));
    ParDoOperation pgbkOperation = (ParDoOperation) ((OperationNode) operationNode).getOperation();

    assertEquals(1, pgbkOperation.receivers.length);
    assertEquals(0, pgbkOperation.receivers[0].getReceiverCount());
    assertEquals(Operation.InitializationState.UNSTARTED, pgbkOperation.initializationState);
  }

  static ParallelInstruction createFlattenInstruction(
      int producerIndex1,
      int producerOutputNum1,
      int producerIndex2,
      int producerOutputNum2,
      String systemName) {
    List<InstructionInput> cloudInputs = new ArrayList<>();

    InstructionInput cloudInput1 = new InstructionInput();
    cloudInput1.setProducerInstructionIndex(producerIndex1);
    cloudInput1.setOutputNum(producerOutputNum1);
    cloudInputs.add(cloudInput1);

    InstructionInput cloudInput2 = new InstructionInput();
    cloudInput2.setProducerInstructionIndex(producerIndex2);
    cloudInput2.setOutputNum(producerOutputNum2);
    cloudInputs.add(cloudInput2);

    FlattenInstruction flattenInstruction = new FlattenInstruction();
    flattenInstruction.setInputs(cloudInputs);

    InstructionOutput output = new InstructionOutput();
    output.setName("flatten_output_name");
    output.setCodec(CloudObjects.asCloudObject(StringUtf8Coder.of(), /*sdkComponents=*/ null));
    output.setOriginalName("originalName");
    output.setSystemName("systemName");

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setFlatten(flattenInstruction);
    instruction.setOutputs(Arrays.asList(output));
    instruction.setSystemName(systemName);
    instruction.setOriginalName(systemName + "OriginalName");

    return instruction;
  }

  @Test
  public void testCreateFlattenOperation() throws Exception {
    int producerIndex1 = 1;
    int producerOutputNum1 = 2;
    int producerIndex2 = 0;
    int producerOutputNum2 = 1;

    ParallelInstructionNode instructionNode =
        ParallelInstructionNode.create(
            createFlattenInstruction(
                producerIndex1, producerOutputNum1, producerIndex2, producerOutputNum2, "Flatten"),
            ExecutionLocation.UNKNOWN);

    when(network.successors(instructionNode))
        .thenReturn(
            ImmutableSet.<Node>of(
                IntrinsicMapTaskExecutorFactory.createOutputReceiversTransform(STAGE, counterSet)
                    .apply(
                        InstructionOutputNode.create(
                            instructionNode.getParallelInstruction().getOutputs().get(0),
                            PCOLLECTION_ID))));
    when(network.outDegree(instructionNode)).thenReturn(1);

    Node operationNode =
        mapTaskExecutorFactory
            .createOperationTransformForParallelInstructionNodes(
                STAGE,
                network,
                options,
                readerRegistry,
                sinkRegistry,
                BatchModeExecutionContext.forTesting(options, counterSet, "testStage"))
            .apply(instructionNode);
    assertThat(operationNode, instanceOf(OperationNode.class));
    assertThat(((OperationNode) operationNode).getOperation(), instanceOf(FlattenOperation.class));
    FlattenOperation flattenOperation =
        (FlattenOperation) ((OperationNode) operationNode).getOperation();

    assertEquals(1, flattenOperation.receivers.length);
    assertEquals(0, flattenOperation.receivers[0].getReceiverCount());
    assertEquals(Operation.InitializationState.UNSTARTED, flattenOperation.initializationState);
  }
}
