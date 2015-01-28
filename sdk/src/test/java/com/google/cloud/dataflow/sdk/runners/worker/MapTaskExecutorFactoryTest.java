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

import static com.google.cloud.dataflow.sdk.util.CoderUtils.makeCloudEncoding;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MEAN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import com.google.api.services.dataflow.model.FlattenInstruction;
import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.PartialGroupByKeyInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.WriteInstruction;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.worker.ReaderFactoryTest.TestReader;
import com.google.cloud.dataflow.sdk.runners.worker.ReaderFactoryTest.TestReaderFactory;
import com.google.cloud.dataflow.sdk.runners.worker.SinkFactoryTest.TestSink;
import com.google.cloud.dataflow.sdk.runners.worker.SinkFactoryTest.TestSinkFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.StringUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue.FullWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils.TestOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.FlattenOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.MapTaskExecutor;
import com.google.cloud.dataflow.sdk.util.common.worker.Operation;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.PartialGroupByKeyOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.ReadOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.util.common.worker.WriteOperation;

import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for MapTaskExecutorFactory.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class MapTaskExecutorFactoryTest {
  @Test
  public void testCreateMapTaskExecutor() throws Exception {
    List<ParallelInstruction> instructions = Arrays.asList(createReadInstruction("Read"),
        createParDoInstruction(0, 0, "DoFn1"), createParDoInstruction(0, 0, "DoFn2"),
        createFlattenInstruction(1, 0, 2, 0, "Flatten"), createWriteInstruction(3, 0, "Write"));

    MapTask mapTask = new MapTask();
    mapTask.setStageName("test");
    mapTask.setInstructions(instructions);

    CounterSet counterSet = null;
    try (
        MapTaskExecutor executor = MapTaskExecutorFactory.create(
            PipelineOptionsFactory.create(), mapTask, new BatchModeExecutionContext())) {
      @SuppressWarnings("unchecked")
      List<Object> operations = (List) executor.operations;
      assertThat(
          operations,
          CoreMatchers.hasItems(new IsInstanceOf(ReadOperation.class),
              new IsInstanceOf(ParDoOperation.class), new IsInstanceOf(ParDoOperation.class),
              new IsInstanceOf(FlattenOperation.class), new IsInstanceOf(WriteOperation.class)));
      counterSet = executor.getOutputCounters();
    }

    assertEquals(
        new CounterSet(Counter.longs("read_output_name-ElementCount", SUM).resetToValue(0L),
            Counter.longs("read_output_name-MeanByteCount", MEAN).resetToValue(0, 0L),
            Counter.longs("Read-ByteCount", SUM).resetToValue(0L),
            Counter.longs("test-Read-start-msecs", SUM).resetToValue(0L),
            Counter.longs("test-Read-read-msecs", SUM).resetToValue(0L),
            Counter.longs("test-Read-process-msecs", SUM).resetToValue(0L),
            Counter.longs("test-Read-finish-msecs", SUM).resetToValue(0L),
            Counter.longs("DoFn1_output-ElementCount", SUM).resetToValue(0L),
            Counter.longs("DoFn1_output-MeanByteCount", MEAN).resetToValue(0, 0L),
            Counter.longs("test-DoFn1-start-msecs", SUM).resetToValue(0L),
            Counter.longs("test-DoFn1-process-msecs", SUM).resetToValue(0L),
            Counter.longs("test-DoFn1-finish-msecs", SUM).resetToValue(0L),
            Counter.longs("DoFn2_output-ElementCount", SUM).resetToValue(0L),
            Counter.longs("DoFn2_output-MeanByteCount", MEAN).resetToValue(0, 0L),
            Counter.longs("test-DoFn2-start-msecs", SUM).resetToValue(0L),
            Counter.longs("test-DoFn2-process-msecs", SUM).resetToValue(0L),
            Counter.longs("test-DoFn2-finish-msecs", SUM).resetToValue(0L),
            Counter.longs("flatten_output_name-ElementCount", SUM).resetToValue(0L),
            Counter.longs("flatten_output_name-MeanByteCount", MEAN).resetToValue(0, 0L),
            Counter.longs("test-Flatten-start-msecs", SUM).resetToValue(0L),
            Counter.longs("test-Flatten-process-msecs", SUM).resetToValue(0L),
            Counter.longs("test-Flatten-finish-msecs", SUM).resetToValue(0L),
            Counter.longs("Write-ByteCount", SUM).resetToValue(0L),
            Counter.longs("test-Write-start-msecs", SUM).resetToValue(0L),
            Counter.longs("test-Write-process-msecs", SUM).resetToValue(0L),
            Counter.longs("test-Write-finish-msecs", SUM).resetToValue(0L),
            Counter.longs("test-other-msecs", SUM)
                .resetToValue(
                    ((Counter<Long>)
                        counterSet.getExistingCounter("test-other-msecs")).getAggregate(false))),
        counterSet);
  }

  @Test
  public void testExecutionContextPlumbing() throws Exception {
    List<ParallelInstruction> instructions =
        Arrays.asList(createReadInstruction("Read"), createParDoInstruction(0, 0, "DoFn1"),
            createParDoInstruction(1, 0, "DoFn2"), createWriteInstruction(2, 0, "Write"));

    MapTask mapTask = new MapTask();
    mapTask.setInstructions(instructions);

    BatchModeExecutionContext context = new BatchModeExecutionContext();

    try (MapTaskExecutor executor =
        MapTaskExecutorFactory.create(PipelineOptionsFactory.create(), mapTask, context)) {
      executor.execute();
    }

    List<String> stepNames = new ArrayList<>();
    for (ExecutionContext.StepContext stepContext : context.getAllStepContexts()) {
      stepNames.add(stepContext.getStepName());
    }
    assertThat(stepNames, CoreMatchers.hasItems("DoFn1", "DoFn2"));
  }

  static ParallelInstruction createReadInstruction(String name) {
    CloudObject spec = CloudObject.forClass(TestReaderFactory.class);

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(CloudObject.forClass(StringUtf8Coder.class));

    ReadInstruction readInstruction = new ReadInstruction();
    readInstruction.setSource(cloudSource);

    InstructionOutput output = new InstructionOutput();
    output.setName("read_output_name");
    output.setCodec(CloudObject.forClass(StringUtf8Coder.class));

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setSystemName(name);
    instruction.setRead(readInstruction);
    instruction.setOutputs(Arrays.asList(output));

    return instruction;
  }

  @Test
  public void testCreateReadOperation() throws Exception {
    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counterSet.getAddCounterMutator());
    Operation operation = MapTaskExecutorFactory.createOperation(PipelineOptionsFactory.create(),
        createReadInstruction("Read"), new BatchModeExecutionContext(),
        Collections.<Operation>emptyList(), counterPrefix, counterSet.getAddCounterMutator(),
        stateSampler);
    assertThat(operation, new IsInstanceOf(ReadOperation.class));
    ReadOperation readOperation = (ReadOperation) operation;

    assertEquals(readOperation.receivers.length, 1);
    assertEquals(readOperation.receivers[0].getReceiverCount(), 0);
    assertEquals(readOperation.initializationState, Operation.InitializationState.UNSTARTED);
    assertThat(readOperation.reader, new IsInstanceOf(TestReader.class));

    assertEquals(
        new CounterSet(
            Counter.longs("test-Read-start-msecs", SUM).resetToValue(0L),
            Counter.longs("read_output_name-MeanByteCount", MEAN).resetToValue(0, 0L),
            Counter.longs("Read-ByteCount", SUM).resetToValue(0L),
            Counter.longs("test-Read-finish-msecs", SUM).resetToValue(0L),
            Counter.longs("test-Read-read-msecs", SUM),
            Counter.longs("test-Read-process-msecs", SUM),
            Counter.longs("read_output_name-ElementCount", SUM).resetToValue(0L)),
        counterSet);
  }

  static ParallelInstruction createWriteInstruction(
      int producerIndex, int producerOutputNum, String systemName) {
    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(producerIndex);
    cloudInput.setOutputNum(producerOutputNum);

    CloudObject spec = CloudObject.forClass(TestSinkFactory.class);

    com.google.api.services.dataflow.model.Sink cloudSink =
        new com.google.api.services.dataflow.model.Sink();
    cloudSink.setSpec(spec);
    cloudSink.setCodec(CloudObject.forClass(StringUtf8Coder.class));

    WriteInstruction writeInstruction = new WriteInstruction();
    writeInstruction.setInput(cloudInput);
    writeInstruction.setSink(cloudSink);

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setWrite(writeInstruction);
    instruction.setSystemName(systemName);

    return instruction;
  }

  @Test
  public void testCreateWriteOperation() throws Exception {
    List<Operation> priorOperations = Arrays.asList(
        new Operation[] {new TestOperation(3), new TestOperation(5), new TestOperation(1)});

    int producerIndex = 1;
    int producerOutputNum = 2;

    ParallelInstruction instruction =
        createWriteInstruction(producerIndex, producerOutputNum, "WriteOperation");

    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counterSet.getAddCounterMutator());
    Operation operation = MapTaskExecutorFactory.createOperation(PipelineOptionsFactory.create(),
        instruction, new BatchModeExecutionContext(), priorOperations, counterPrefix,
        counterSet.getAddCounterMutator(), stateSampler);
    assertThat(operation, new IsInstanceOf(WriteOperation.class));
    WriteOperation writeOperation = (WriteOperation) operation;

    assertEquals(writeOperation.receivers.length, 0);
    assertEquals(writeOperation.initializationState, Operation.InitializationState.UNSTARTED);
    assertThat(writeOperation.sink, new IsInstanceOf(TestSink.class));

    assertSame(
        writeOperation,
        priorOperations.get(producerIndex).receivers[producerOutputNum].getOnlyReceiver());

    assertEquals(
        new CounterSet(Counter.longs("WriteOperation-ByteCount", SUM).resetToValue(0L),
            Counter.longs("test-WriteOperation-start-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                                   "test-WriteOperation-start-msecs")).getAggregate(false)),
            Counter.longs("test-WriteOperation-process-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                                   "test-WriteOperation-process-msecs")).getAggregate(false)),
            Counter.longs("test-WriteOperation-finish-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                                   "test-WriteOperation-finish-msecs")).getAggregate(false))),
        counterSet);
  }

  static class TestDoFn extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) {}
  }

  static ParallelInstruction createParDoInstruction(
      int producerIndex, int producerOutputNum, String systemName) {
    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(producerIndex);
    cloudInput.setOutputNum(producerOutputNum);

    TestDoFn fn = new TestDoFn();

    String serializedFn =
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(new DoFnInfo(fn, new GlobalWindows())));

    CloudObject cloudUserFn = CloudObject.forClassName("DoFn");
    addString(cloudUserFn, PropertyNames.SERIALIZED_FN, serializedFn);

    ParDoInstruction parDoInstruction = new ParDoInstruction();
    parDoInstruction.setInput(cloudInput);
    parDoInstruction.setNumOutputs(1);
    parDoInstruction.setUserFn(cloudUserFn);

    InstructionOutput output = new InstructionOutput();
    output.setName(systemName + "_output");
    output.setCodec(CloudObject.forClass(StringUtf8Coder.class));

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setParDo(parDoInstruction);
    instruction.setOutputs(Arrays.asList(output));
    instruction.setSystemName(systemName);
    return instruction;
  }

  @Test
  public void testCreateParDoOperation() throws Exception {
    List<Operation> priorOperations = Arrays.asList(
        new Operation[] {new TestOperation(3), new TestOperation(5), new TestOperation(1)});

    int producerIndex = 1;
    int producerOutputNum = 2;

    ParallelInstruction instruction =
        createParDoInstruction(producerIndex, producerOutputNum, "DoFn");

    BatchModeExecutionContext context = new BatchModeExecutionContext();
    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counterSet.getAddCounterMutator());
    Operation operation = MapTaskExecutorFactory.createOperation(PipelineOptionsFactory.create(),
        instruction, context, priorOperations, counterPrefix, counterSet.getAddCounterMutator(),
        stateSampler);
    assertThat(operation, new IsInstanceOf(ParDoOperation.class));
    ParDoOperation parDoOperation = (ParDoOperation) operation;

    assertEquals(parDoOperation.receivers.length, 1);
    assertEquals(parDoOperation.receivers[0].getReceiverCount(), 0);
    assertEquals(parDoOperation.initializationState, Operation.InitializationState.UNSTARTED);
    assertThat(parDoOperation.fn, new IsInstanceOf(NormalParDoFn.class));
    NormalParDoFn normalParDoFn = (NormalParDoFn) parDoOperation.fn;

    assertThat(normalParDoFn.fnFactory.createDoFnInfo().getDoFn(),
        new IsInstanceOf(TestDoFn.class));

    assertSame(
        parDoOperation,
        priorOperations.get(producerIndex).receivers[producerOutputNum].getOnlyReceiver());

    assertEquals(context, normalParDoFn.executionContext);
  }

  static ParallelInstruction createPartialGroupByKeyInstruction(
      int producerIndex, int producerOutputNum) {
    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(producerIndex);
    cloudInput.setOutputNum(producerOutputNum);

    PartialGroupByKeyInstruction pgbkInstruction = new PartialGroupByKeyInstruction();
    pgbkInstruction.setInput(cloudInput);
    pgbkInstruction.setInputElementCodec(makeCloudEncoding(
        FullWindowedValueCoder.class.getName(),
        makeCloudEncoding("KvCoder", makeCloudEncoding("StringUtf8Coder"),
            makeCloudEncoding("BigEndianIntegerCoder")),
        IntervalWindow.getCoder().asCloudObject()));

    InstructionOutput output = new InstructionOutput();
    output.setName("pgbk_output_name");
    output.setCodec(makeCloudEncoding("KvCoder", makeCloudEncoding("StringUtf8Coder"),
        makeCloudEncoding("IterableCoder", makeCloudEncoding("BigEndianIntegerCoder"))));

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setPartialGroupByKey(pgbkInstruction);
    instruction.setOutputs(Arrays.asList(output));

    return instruction;
  }

  @Test
  public void testCreatePartialGroupByKeyOperation() throws Exception {
    List<Operation> priorOperations = Arrays.asList(
        new Operation[] {new TestOperation(3), new TestOperation(5), new TestOperation(1)});

    int producerIndex = 1;
    int producerOutputNum = 2;

    ParallelInstruction instruction =
        createPartialGroupByKeyInstruction(producerIndex, producerOutputNum);

    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counterSet.getAddCounterMutator());
    Operation operation = MapTaskExecutorFactory.createOperation(PipelineOptionsFactory.create(),
        instruction, new BatchModeExecutionContext(), priorOperations, counterPrefix,
        counterSet.getAddCounterMutator(), stateSampler);
    assertThat(operation, instanceOf(PartialGroupByKeyOperation.class));
    PartialGroupByKeyOperation pgbkOperation = (PartialGroupByKeyOperation) operation;

    assertEquals(pgbkOperation.receivers.length, 1);
    assertEquals(pgbkOperation.receivers[0].getReceiverCount(), 0);
    assertEquals(pgbkOperation.initializationState, Operation.InitializationState.UNSTARTED);

    assertSame(
        pgbkOperation,
        priorOperations.get(producerIndex).receivers[producerOutputNum].getOnlyReceiver());
  }

  static ParallelInstruction createFlattenInstruction(int producerIndex1, int producerOutputNum1,
      int producerIndex2, int producerOutputNum2, String systemName) {
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
    output.setCodec(makeCloudEncoding(StringUtf8Coder.class.getName()));

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setFlatten(flattenInstruction);
    instruction.setOutputs(Arrays.asList(output));
    instruction.setSystemName(systemName);

    return instruction;
  }

  @Test
  public void testCreateFlattenOperation() throws Exception {
    List<Operation> priorOperations = Arrays.asList(
        new Operation[] {new TestOperation(3), new TestOperation(5), new TestOperation(1)});

    int producerIndex1 = 1;
    int producerOutputNum1 = 2;
    int producerIndex2 = 0;
    int producerOutputNum2 = 1;

    ParallelInstruction instruction = createFlattenInstruction(
        producerIndex1, producerOutputNum1, producerIndex2, producerOutputNum2, "Flatten");

    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counterSet.getAddCounterMutator());
    Operation operation = MapTaskExecutorFactory.createOperation(PipelineOptionsFactory.create(),
        instruction, new BatchModeExecutionContext(), priorOperations, counterPrefix,
        counterSet.getAddCounterMutator(), stateSampler);
    assertThat(operation, new IsInstanceOf(FlattenOperation.class));
    FlattenOperation flattenOperation = (FlattenOperation) operation;

    assertEquals(flattenOperation.receivers.length, 1);
    assertEquals(flattenOperation.receivers[0].getReceiverCount(), 0);
    assertEquals(flattenOperation.initializationState, Operation.InitializationState.UNSTARTED);

    assertSame(
        flattenOperation,
        priorOperations.get(producerIndex1).receivers[producerOutputNum1].getOnlyReceiver());
    assertSame(
        flattenOperation,
        priorOperations.get(producerIndex2).receivers[producerOutputNum2].getOnlyReceiver());
  }
}
