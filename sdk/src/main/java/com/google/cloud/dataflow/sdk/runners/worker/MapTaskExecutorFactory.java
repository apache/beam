/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.api.services.dataflow.model.FlattenInstruction;
import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.PartialGroupByKeyInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.WriteInstruction;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.Serializer;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObservable;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.cloud.dataflow.sdk.util.common.worker.FlattenOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.MapTaskExecutor;
import com.google.cloud.dataflow.sdk.util.common.worker.Operation;
import com.google.cloud.dataflow.sdk.util.common.worker.OutputReceiver;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.PartialGroupByKeyOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.PartialGroupByKeyOperation.GroupingKeyCreator;
import com.google.cloud.dataflow.sdk.util.common.worker.ReadOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.ReceivingOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.util.common.worker.WriteOperation;
import com.google.cloud.dataflow.sdk.values.KV;

import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Creates a MapTaskExecutor from a MapTask definition.
 */
public class MapTaskExecutorFactory {
  /**
   * Creates a new MapTaskExecutor from the given MapTask definition.
   */
  public static MapTaskExecutor create(
      PipelineOptions options, MapTask mapTask, ExecutionContext context) throws Exception {
    List<Operation> operations = new ArrayList<>();
    CounterSet counters = new CounterSet();
    String counterPrefix = mapTask.getStageName() + "-";
    StateSampler stateSampler = new StateSampler(counterPrefix, counters.getAddCounterMutator());
    // Open-ended state.
    stateSampler.setState("other");

    // Instantiate operations for each instruction in the graph.
    for (ParallelInstruction instruction : mapTask.getInstructions()) {
      operations.add(createOperation(options, instruction, context, operations, counterPrefix,
          counters.getAddCounterMutator(), stateSampler));
    }

    return new MapTaskExecutor(operations, counters, stateSampler);
  }

  /**
   * Creates an Operation from the given ParallelInstruction definition.
   */
  static Operation createOperation(PipelineOptions options, ParallelInstruction instruction,
      ExecutionContext executionContext, List<Operation> priorOperations, String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) throws Exception {
    if (instruction.getRead() != null) {
      return createReadOperation(options, instruction, executionContext, priorOperations,
          counterPrefix, addCounterMutator, stateSampler);
    } else if (instruction.getWrite() != null) {
      return createWriteOperation(options, instruction, executionContext, priorOperations,
          counterPrefix, addCounterMutator, stateSampler);
    } else if (instruction.getParDo() != null) {
      return createParDoOperation(options, instruction, executionContext, priorOperations,
          counterPrefix, addCounterMutator, stateSampler);
    } else if (instruction.getPartialGroupByKey() != null) {
      return createPartialGroupByKeyOperation(options, instruction, executionContext,
          priorOperations, counterPrefix, addCounterMutator, stateSampler);
    } else if (instruction.getFlatten() != null) {
      return createFlattenOperation(options, instruction, executionContext, priorOperations,
          counterPrefix, addCounterMutator, stateSampler);
    } else {
      throw new Exception("Unexpected instruction: " + instruction);
    }
  }

  static ReadOperation createReadOperation(PipelineOptions options, ParallelInstruction instruction,
      ExecutionContext executionContext, List<Operation> priorOperations, String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) throws Exception {
    ReadInstruction read = instruction.getRead();

    Reader<?> reader = ReaderFactory.create(options, read.getSource(), executionContext);

    OutputReceiver[] receivers =
        createOutputReceivers(instruction, counterPrefix, addCounterMutator, stateSampler, 1);

    return new ReadOperation(instruction.getSystemName(), reader, receivers, counterPrefix,
        addCounterMutator, stateSampler);
  }

  static WriteOperation createWriteOperation(PipelineOptions options,
      ParallelInstruction instruction, ExecutionContext executionContext,
      List<Operation> priorOperations, String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) throws Exception {
    WriteInstruction write = instruction.getWrite();

    Sink sink = SinkFactory.create(options, write.getSink(), executionContext);

    OutputReceiver[] receivers =
        createOutputReceivers(instruction, counterPrefix, addCounterMutator, stateSampler, 0);

    WriteOperation operation = new WriteOperation(instruction.getSystemName(), sink, receivers,
        counterPrefix, addCounterMutator, stateSampler);

    attachInput(operation, write.getInput(), priorOperations);

    return operation;
  }

  static ParDoOperation createParDoOperation(PipelineOptions options,
      ParallelInstruction instruction, ExecutionContext executionContext,
      List<Operation> priorOperations, String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) throws Exception {
    ParDoInstruction parDo = instruction.getParDo();

    ParDoFn fn = ParDoFnFactory.create(options, CloudObject.fromSpec(parDo.getUserFn()),
        instruction.getSystemName(), parDo.getSideInputs(), parDo.getMultiOutputInfos(),
        parDo.getNumOutputs(), executionContext, addCounterMutator, stateSampler);

    OutputReceiver[] receivers = createOutputReceivers(
        instruction, counterPrefix, addCounterMutator, stateSampler, parDo.getNumOutputs());

    ParDoOperation operation = new ParDoOperation(
        instruction.getSystemName(), fn, receivers, counterPrefix, addCounterMutator, stateSampler);

    attachInput(operation, parDo.getInput(), priorOperations);

    return operation;
  }

  static PartialGroupByKeyOperation createPartialGroupByKeyOperation(PipelineOptions options,
      ParallelInstruction instruction, ExecutionContext executionContext,
      List<Operation> priorOperations, String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) throws Exception {
    PartialGroupByKeyInstruction pgbk = instruction.getPartialGroupByKey();

    Coder<?> windowedCoder = Serializer.deserialize(pgbk.getInputElementCodec(), Coder.class);
    if (!(windowedCoder instanceof WindowedValueCoder)) {
      throw new Exception(
          "unexpected kind of input coder for PartialGroupByKeyOperation: " + windowedCoder);
    }
    Coder<?> elemCoder = ((WindowedValueCoder<?>) windowedCoder).getValueCoder();
    if (!(elemCoder instanceof KvCoder)) {
      throw new Exception(
          "unexpected kind of input element coder for PartialGroupByKeyOperation: " + elemCoder);
    }
    KvCoder<?, ?> kvCoder = (KvCoder<?, ?>) elemCoder;
    Coder<?> keyCoder = kvCoder.getKeyCoder();
    Coder<?> valueCoder = kvCoder.getValueCoder();

    OutputReceiver[] receivers =
        createOutputReceivers(instruction, counterPrefix, addCounterMutator, stateSampler, 1);

    PartialGroupByKeyOperation operation =
        new PartialGroupByKeyOperation(instruction.getSystemName(),
            new WindowingCoderGroupingKeyCreator(keyCoder),
            new CoderSizeEstimator(WindowedValue.getValueOnlyCoder(keyCoder)),
            new CoderSizeEstimator(valueCoder), 0.001/*sizeEstimatorSampleRate*/, PairInfo.create(),
            receivers, counterPrefix, addCounterMutator, stateSampler);

    attachInput(operation, pgbk.getInput(), priorOperations);

    return operation;
  }

  /**
   * Implements PGBKOp.PairInfo via KVs.
   */
  public static class PairInfo implements PartialGroupByKeyOperation.PairInfo {
    private static PairInfo theInstance = new PairInfo();
    public static PairInfo create() {
      return theInstance;
    }
    private PairInfo() {}
    @Override
    public Object getKeyFromInputPair(Object pair) {
      @SuppressWarnings("unchecked")
      WindowedValue<KV<?, ?>> windowedKv = (WindowedValue<KV<?, ?>>) pair;
      return WindowedValue.of(
          windowedKv.getValue().getKey(), windowedKv.getTimestamp(), windowedKv.getWindows());
    }
    @Override
    public Object getValueFromInputPair(Object pair) {
      @SuppressWarnings("unchecked")
      WindowedValue<KV<?, ?>> windowedKv = (WindowedValue<KV<?, ?>>) pair;
      return windowedKv.getValue().getValue();
    }
    @Override
    public Object makeOutputPair(Object key, Object values) {
      WindowedValue<?> windowedKey = (WindowedValue<?>) key;
      return WindowedValue.of(
          KV.of(windowedKey.getValue(), values),
          windowedKey.getTimestamp(),
          windowedKey.getWindows());
    }
  }

  /**
   * Implements PGBKOp.GroupingKeyCreator via Coder.
   */
  // TODO: Actually support window merging in the combiner table.
  public static class WindowingCoderGroupingKeyCreator
      implements GroupingKeyCreator {

    private static final Instant ignored = new Instant(0);

    private final Coder coder;

    public WindowingCoderGroupingKeyCreator(Coder coder) {
      this.coder = coder;
    }

    @Override
    public Object createGroupingKey(Object key) throws Exception {
      WindowedValue<?> windowedKey = (WindowedValue<?>) key;
      // Ignore timestamp for grouping purposes.
      // The PGBK output will inherit the timestamp of one of its inputs.
      return WindowedValue.of(
          new PartialGroupByKeyOperation.StructuralByteArray(
              CoderUtils.encodeToByteArray(coder, windowedKey.getValue())),
          ignored,
          windowedKey.getWindows());
    }
  }

  /**
   * Implements PGBKOp.SizeEstimator via Coder.
   */
  public static class CoderSizeEstimator implements PartialGroupByKeyOperation.SizeEstimator {
    final Coder coder;

    public CoderSizeEstimator(Coder coder) {
      this.coder = coder;
    }

    @Override
    public long estimateSize(Object value) throws Exception {
      return CoderUtils.encodeToByteArray(coder, value).length;
    }
  }

  static FlattenOperation createFlattenOperation(PipelineOptions options,
      ParallelInstruction instruction, ExecutionContext executionContext,
      List<Operation> priorOperations, String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) throws Exception {
    FlattenInstruction flatten = instruction.getFlatten();

    OutputReceiver[] receivers =
        createOutputReceivers(instruction, counterPrefix, addCounterMutator, stateSampler, 1);

    FlattenOperation operation = new FlattenOperation(
        instruction.getSystemName(), receivers, counterPrefix, addCounterMutator, stateSampler);

    for (InstructionInput input : flatten.getInputs()) {
      attachInput(operation, input, priorOperations);
    }

    return operation;
  }

  /**
   * Returns an array of OutputReceivers for the given
   * ParallelInstruction definition.
   */
  static OutputReceiver[] createOutputReceivers(ParallelInstruction instruction,
      String counterPrefix, CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler, int expectedNumOutputs) throws Exception {
    int numOutputs = 0;
    if (instruction.getOutputs() != null) {
      numOutputs = instruction.getOutputs().size();
    }
    if (numOutputs != expectedNumOutputs) {
      throw new AssertionError("ParallelInstruction.Outputs has an unexpected length");
    }
    OutputReceiver[] receivers = new OutputReceiver[numOutputs];
    for (int i = 0; i < numOutputs; i++) {
      InstructionOutput cloudOutput = instruction.getOutputs().get(i);
      receivers[i] = new OutputReceiver(cloudOutput.getName(),
          new ElementByteSizeObservableCoder(Serializer.deserialize(
              cloudOutput.getCodec(), Coder.class)),
          counterPrefix, addCounterMutator);
    }
    return receivers;
  }

  /**
   * Adapts a Coder to the ElementByteSizeObservable interface.
   */
  public static class ElementByteSizeObservableCoder<T> implements ElementByteSizeObservable<T> {
    final Coder<T> coder;

    public ElementByteSizeObservableCoder(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(T value) {
      return coder.isRegisterByteSizeObserverCheap(value, Coder.Context.OUTER);
    }

    @Override
    public void registerByteSizeObserver(T value, ElementByteSizeObserver observer)
        throws Exception {
      coder.registerByteSizeObserver(value, observer, Coder.Context.OUTER);
    }
  }

  /**
   * Adds an input to the given Operation, coming from the given
   * producer instruction output.
   */
  static void attachInput(ReceivingOperation operation, @Nullable InstructionInput input,
      List<Operation> priorOperations) {
    Integer producerInstructionIndex = 0;
    Integer outputNum = 0;
    if (input != null) {
      if (input.getProducerInstructionIndex() != null) {
        producerInstructionIndex = input.getProducerInstructionIndex();
      }
      if (input.getOutputNum() != null) {
        outputNum = input.getOutputNum();
      }
    }
    // Input id must refer to an operation that has already been seen.
    Operation source = priorOperations.get(producerInstructionIndex);
    operation.attachInput(source, outputNum);
  }
}
