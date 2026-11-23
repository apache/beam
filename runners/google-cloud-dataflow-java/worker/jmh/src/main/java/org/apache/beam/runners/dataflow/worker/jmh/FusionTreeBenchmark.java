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
package org.apache.beam.runners.dataflow.worker.jmh;

import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;

import com.google.api.services.dataflow.model.FlattenInstruction;
import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.WriteInstruction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.BatchModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.ReaderFactory;
import org.apache.beam.runners.dataflow.worker.SinkFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * End-to-end JMH microbenchmarks for Dataflow Runner v1 fused operation trees built via {@link
 * IntrinsicMapTaskExecutorFactory}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@SuppressWarnings({"rawtypes", "unchecked", "nullness"})
public class FusionTreeBenchmark {

  private static final int BUNDLE_SIZE = 1024;
  private static final TupleTag<String> MAIN_TAG = new TupleTag<>("out");
  private static final CloudObject WINDOWED_STRING_CODER =
      CloudObjects.asCloudObject(
          WindowedValues.getFullCoder(
              StringUtf8Coder.of(), WindowingStrategy.globalDefault().getWindowFn().windowCoder()),
          null);

  // 8 distinct DoFn classes using OutputReceiver to trigger realistic TypeProfileWidth=2
  // polymorphism across fused steps.
  public static final class StepReceiverDoFn0 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (!e.isEmpty()) {
        out.output(e);
      }
    }
  }

  public static final class StepReceiverDoFn1 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.length() > 0) {
        out.output(e);
      }
    }
  }

  public static final class StepReceiverDoFn2 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.charAt(0) != 0) {
        out.output(e);
      }
    }
  }

  public static final class StepReceiverDoFn3 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e != null) {
        out.output(e);
      }
    }
  }

  public static final class StepReceiverDoFn4 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.length() >= 1) {
        out.output(e);
      }
    }
  }

  public static final class StepReceiverDoFn5 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.hashCode() != 123456789) {
        out.output(e);
      }
    }
  }

  public static final class StepReceiverDoFn6 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.charAt(0) >= ' ') {
        out.output(e);
      }
    }
  }

  public static final class StepReceiverDoFn7 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.length() < 1_000_000) {
        out.output(e);
      }
    }
  }

  // 8 distinct DoFn classes using ProcessContext.output
  public static final class StepContextDoFn0 extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      c.output(c.element());
    }
  }

  public static final class StepContextDoFn1 extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      if (!c.element().isEmpty()) {
        c.output(c.element());
      }
    }
  }

  public static final class StepContextDoFn2 extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      if (c.element().length() > 0) {
        c.output(c.element());
      }
    }
  }

  public static final class StepContextDoFn3 extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      if (c.element().charAt(0) != 0) {
        c.output(c.element());
      }
    }
  }

  public static final class StepContextDoFn4 extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      c.output(c.element());
    }
  }

  public static final class StepContextDoFn5 extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      if (c.element().length() >= 1) {
        c.output(c.element());
      }
    }
  }

  public static final class StepContextDoFn6 extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      if (c.element().charAt(0) >= ' ') {
        c.output(c.element());
      }
    }
  }

  public static final class StepContextDoFn7 extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      if (c.element().length() < 1_000_000) {
        c.output(c.element());
      }
    }
  }

  // Shared wrapper DoFns mimicking MapElements and Filter (same DoFn class reused with distinct
  // ProcessFunction lambdas)
  public static final class SharedMapDoFn extends DoFn<String, String> {
    private final ProcessFunction<String, String> fn;

    public SharedMapDoFn(ProcessFunction<String, String> fn) {
      this.fn = fn;
    }

    @ProcessElement
    public void process(@Element String elem, OutputReceiver<String> out) throws Exception {
      out.output(fn.apply(elem));
    }
  }

  public static final class SharedFilterDoFn extends DoFn<String, String> {
    private final ProcessFunction<String, Boolean> pred;

    public SharedFilterDoFn(ProcessFunction<String, Boolean> pred) {
      this.pred = pred;
    }

    @ProcessElement
    public void process(@Element String elem, OutputReceiver<String> out) throws Exception {
      if (pred.apply(elem)) {
        out.output(elem);
      }
    }
  }

  public static final class DiamondLeftDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String elem, OutputReceiver<String> out) {
      if (elem.charAt(0) == 'A') {
        out.output(elem);
      }
    }
  }

  public static final class DiamondRightDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String elem, OutputReceiver<String> out) {
      if (elem.charAt(0) != 'A') {
        out.output(elem);
      }
    }
  }

  private static DoFn<String, String> receiverDoFnFor(int idx) {
    switch (idx & 7) {
      case 0:
        return new StepReceiverDoFn0();
      case 1:
        return new StepReceiverDoFn1();
      case 2:
        return new StepReceiverDoFn2();
      case 3:
        return new StepReceiverDoFn3();
      case 4:
        return new StepReceiverDoFn4();
      case 5:
        return new StepReceiverDoFn5();
      case 6:
        return new StepReceiverDoFn6();
      default:
        return new StepReceiverDoFn7();
    }
  }

  private static DoFn<String, String> contextDoFnFor(int idx) {
    switch (idx & 7) {
      case 0:
        return new StepContextDoFn0();
      case 1:
        return new StepContextDoFn1();
      case 2:
        return new StepContextDoFn2();
      case 3:
        return new StepContextDoFn3();
      case 4:
        return new StepContextDoFn4();
      case 5:
        return new StepContextDoFn5();
      case 6:
        return new StepContextDoFn6();
      default:
        return new StepContextDoFn7();
    }
  }

  private static DoFn<String, String> mapOrFilterDoFnFor(int idx) {
    switch (idx & 7) {
      case 0:
        return new SharedMapDoFn(s -> s);
      case 1:
        return new SharedFilterDoFn(s -> !s.isEmpty());
      case 2:
        return new SharedMapDoFn(s -> s.length() > 0 ? s : "");
      case 3:
        return new SharedFilterDoFn(s -> s.charAt(0) != 0);
      case 4:
        return new SharedMapDoFn(s -> s != null ? s : "");
      case 5:
        return new SharedFilterDoFn(s -> s.length() >= 1);
      case 6:
        return new SharedMapDoFn(s -> s.charAt(0) >= ' ' ? s : "");
      default:
        return new SharedFilterDoFn(s -> s.length() < 1_000_000);
    }
  }

  private static final class InMemoryReader extends NativeReader<WindowedValue<String>> {
    private final WindowedValue<String> elemEven;
    private final WindowedValue<String> elemOdd;
    private final int count;

    InMemoryReader(WindowedValue<String> elemEven, WindowedValue<String> elemOdd, int count) {
      this.elemEven = elemEven;
      this.elemOdd = elemOdd;
      this.count = count;
    }

    @Override
    public NativeReaderIterator<WindowedValue<String>> iterator() {
      return new NativeReaderIterator<WindowedValue<String>>() {
        private int idx = -1;

        @Override
        public boolean start() {
          idx = 0;
          return idx < count;
        }

        @Override
        public boolean advance() {
          idx++;
          return idx < count;
        }

        @Override
        public WindowedValue<String> getCurrent() {
          if (idx < 0 || idx >= count) {
            throw new NoSuchElementException();
          }
          return (idx & 1) == 0 ? elemEven : elemOdd;
        }
      };
    }

    @Override
    public boolean supportsRestart() {
      return true;
    }
  }

  private static final class CountingSink extends Sink<WindowedValue<String>> {
    long written = 0;

    @Override
    public SinkWriter<WindowedValue<String>> writer() {
      return new SinkWriter<WindowedValue<String>>() {
        @Override
        public long add(WindowedValue<String> value) {
          written++;
          return 16L;
        }

        @Override
        public void close() {}

        @Override
        public void abort() {}
      };
    }

    @Override
    public boolean supportsRestart() {
      return true;
    }
  }

  private static ParallelInstruction createReadInstruction() {
    Source cloudSource = new Source();
    cloudSource.setSpec(CloudObject.forClassName("InMemorySource"));
    cloudSource.setCodec(WINDOWED_STRING_CODER);

    ReadInstruction readInstruction = new ReadInstruction();
    readInstruction.setSource(cloudSource);

    InstructionOutput output = new InstructionOutput();
    output.setName("read_out");
    output.setCodec(WINDOWED_STRING_CODER);
    output.setOriginalName("Read.out");
    output.setSystemName("s0_out");

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setRead(readInstruction);
    instruction.setOutputs(Collections.singletonList(output));
    instruction.setSystemName("s0");
    instruction.setOriginalName("Read");
    instruction.setName("Read");
    return instruction;
  }

  private static ParallelInstruction createParDoInstruction(
      int producerIdx, int outputNum, String sysName, DoFn<String, String> fn) {
    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(producerIdx);
    cloudInput.setOutputNum(outputNum);

    Map<TupleTag<?>, Coder<?>> outputCoders =
        Collections.singletonMap(MAIN_TAG, StringUtf8Coder.of());
    DoFnInfo<String, String> fnInfo =
        DoFnInfo.forFn(
            fn,
            WindowingStrategy.globalDefault(),
            null,
            StringUtf8Coder.of(),
            outputCoders,
            MAIN_TAG,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    CloudObject cloudUserFn = CloudObject.forClassName("DoFn");
    addString(
        cloudUserFn,
        PropertyNames.SERIALIZED_FN,
        byteArrayToJsonString(serializeToByteArray(fnInfo)));

    MultiOutputInfo mainOut = new MultiOutputInfo();
    mainOut.setTag(MAIN_TAG.getId());

    ParDoInstruction parDo = new ParDoInstruction();
    parDo.setInput(cloudInput);
    parDo.setNumOutputs(1);
    parDo.setMultiOutputInfos(ImmutableList.of(mainOut));
    parDo.setUserFn(cloudUserFn);

    InstructionOutput output = new InstructionOutput();
    output.setName(sysName + "_out");
    output.setCodec(WINDOWED_STRING_CODER);
    output.setOriginalName(sysName + ".out");
    output.setSystemName(sysName + "_out");

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setParDo(parDo);
    instruction.setOutputs(Collections.singletonList(output));
    instruction.setSystemName(sysName);
    instruction.setOriginalName(sysName);
    instruction.setName(sysName);
    return instruction;
  }

  private static ParallelInstruction createFlattenInstruction(
      int prod1, int out1, int prod2, int out2, String sysName) {
    InstructionInput in1 = new InstructionInput();
    in1.setProducerInstructionIndex(prod1);
    in1.setOutputNum(out1);
    InstructionInput in2 = new InstructionInput();
    in2.setProducerInstructionIndex(prod2);
    in2.setOutputNum(out2);

    FlattenInstruction flatten = new FlattenInstruction();
    flatten.setInputs(ImmutableList.of(in1, in2));

    InstructionOutput output = new InstructionOutput();
    output.setName(sysName + "_out");
    output.setCodec(WINDOWED_STRING_CODER);
    output.setOriginalName(sysName + ".out");
    output.setSystemName(sysName + "_out");

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setFlatten(flatten);
    instruction.setOutputs(Collections.singletonList(output));
    instruction.setSystemName(sysName);
    instruction.setOriginalName(sysName);
    instruction.setName(sysName);
    return instruction;
  }

  private static ParallelInstruction createWriteInstruction(int producerIdx, int outputNum) {
    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(producerIdx);
    cloudInput.setOutputNum(outputNum);

    com.google.api.services.dataflow.model.Sink cloudSink =
        new com.google.api.services.dataflow.model.Sink();
    cloudSink.setSpec(CloudObject.forClassName("CountingSink"));
    cloudSink.setCodec(WINDOWED_STRING_CODER);

    WriteInstruction write = new WriteInstruction();
    write.setInput(cloudInput);
    write.setSink(cloudSink);

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setWrite(write);
    instruction.setSystemName("write_sink");
    instruction.setOriginalName("Write");
    instruction.setName("Write");
    return instruction;
  }

  private static final class ExecutorBundle {
    final PipelineOptions options;
    final MapTask mapTask;
    final ReaderFactory readerFactory;
    final SinkFactory sinkFactory;
    final CountingSink countingSink;
    final CounterSet counterSet;
    final BatchModeExecutionContext execContext;
    final IdGenerator idGenerator = IdGenerators.decrementingLongs();
    final MapTaskToNetworkFunction mapTaskToNetwork = new MapTaskToNetworkFunction(idGenerator);

    ExecutorBundle(List<ParallelInstruction> instructions) {
      this(instructions, false);
    }

    ExecutorBundle(List<ParallelInstruction> instructions, boolean disableTrampoline) {
      this.options = PipelineOptionsFactory.create();
      if (disableTrampoline) {
        this.options
            .as(DataflowPipelineDebugOptions.class)
            .setExperiments(ImmutableList.of("disable_fusion_trampoline"));
      }
      this.mapTask = new MapTask();
      this.mapTask.setStageName("BenchStage");
      this.mapTask.setSystemName("BenchStage");
      this.mapTask.setInstructions(instructions);
      this.mapTask.setFactory(Transport.getJsonFactory());

      WindowedValue<String> sampleEven =
          WindowedValues.valueInGlobalWindow("A-fusion-benchmark-payload-element");
      WindowedValue<String> sampleOdd =
          WindowedValues.valueInGlobalWindow("B-fusion-benchmark-payload-element");
      InMemoryReader inMemoryReader = new InMemoryReader(sampleEven, sampleOdd, BUNDLE_SIZE);
      this.countingSink = new CountingSink();
      this.readerFactory =
          new ReaderFactory() {
            @Override
            public NativeReader<?> create(
                CloudObject cloudSourceSpec,
                @Nullable Coder<?> coder,
                @Nullable PipelineOptions options,
                @Nullable DataflowExecutionContext executionContext,
                DataflowOperationContext operationContext) {
              return inMemoryReader;
            }
          };
      this.sinkFactory =
          new SinkFactory() {
            @Override
            public Sink<?> create(
                CloudObject sinkSpec,
                @Nullable Coder<?> coder,
                @Nullable PipelineOptions options,
                @Nullable DataflowExecutionContext executionContext,
                DataflowOperationContext operationContext) {
              return countingSink;
            }
          };
      this.counterSet = new CounterSet();
      this.execContext = BatchModeExecutionContext.forTesting(options, counterSet, "BenchStage");
    }

    DataflowMapTaskExecutor createExecutor() {
      MutableNetwork<Node, Edge> network = mapTaskToNetwork.apply(mapTask);
      DataflowMapTaskExecutor executor =
          IntrinsicMapTaskExecutorFactory.defaultFactory()
              .create(
                  network,
                  options,
                  "BenchStage",
                  readerFactory,
                  sinkFactory,
                  execContext,
                  counterSet,
                  idGenerator);
      executor.getReadOperation().setProgressUpdatePeriodMs(-1L);
      return executor;
    }
  }

  @State(Scope.Thread)
  public static class ChainState {
    @Param({"2", "4", "8", "16", "32"})
    public int depth;

    @Param({"false"})
    public boolean disableTrampoline;

    ExecutorBundle receiverChainBundle;
    DataflowMapTaskExecutor receiverExecutor;

    ExecutorBundle contextChainBundle;
    DataflowMapTaskExecutor contextExecutor;

    ExecutorBundle mapFilterChainBundle;
    DataflowMapTaskExecutor mapFilterExecutor;

    ExecutorBundle diamondFlattenBundle;
    DataflowMapTaskExecutor diamondExecutor;

    @Setup
    public void setUp() throws Exception {
      // 1. Heterogeneous OutputReceiver DoFn chain
      List<ParallelInstruction> receiverInstructions = new ArrayList<>();
      receiverInstructions.add(createReadInstruction());
      for (int i = 0; i < depth; i++) {
        receiverInstructions.add(
            createParDoInstruction(i, 0, "recv_step_" + i, receiverDoFnFor(i)));
      }
      receiverInstructions.add(createWriteInstruction(depth, 0));
      receiverChainBundle = new ExecutorBundle(receiverInstructions, disableTrampoline);
      receiverExecutor = receiverChainBundle.createExecutor();

      // 2. Heterogeneous ProcessContext DoFn chain
      List<ParallelInstruction> contextInstructions = new ArrayList<>();
      contextInstructions.add(createReadInstruction());
      for (int i = 0; i < depth; i++) {
        contextInstructions.add(createParDoInstruction(i, 0, "ctx_step_" + i, contextDoFnFor(i)));
      }
      contextInstructions.add(createWriteInstruction(depth, 0));
      contextChainBundle = new ExecutorBundle(contextInstructions, disableTrampoline);
      contextExecutor = contextChainBundle.createExecutor();

      // 3. Shared MapElements / Filter DoFn chain
      List<ParallelInstruction> mapFilterInstructions = new ArrayList<>();
      mapFilterInstructions.add(createReadInstruction());
      for (int i = 0; i < depth; i++) {
        mapFilterInstructions.add(
            createParDoInstruction(i, 0, "mf_step_" + i, mapOrFilterDoFnFor(i)));
      }
      mapFilterInstructions.add(createWriteInstruction(depth, 0));
      mapFilterChainBundle = new ExecutorBundle(mapFilterInstructions, disableTrampoline);
      mapFilterExecutor = mapFilterChainBundle.createExecutor();

      // 4. Diamond Fan-out + Flatten chain (each diamond has 2 parallel ParDos + 1 Flatten;
      // capped at 16 diamonds because Networks.topologicalOrder is O(2^diamonds) at setup time)
      if (depth <= 32) {
        List<ParallelInstruction> diamondInstructions = new ArrayList<>();
        diamondInstructions.add(createReadInstruction());
        int prevIdx = 0;
        int diamonds = Math.max(1, depth / 2);
        for (int d = 0; d < diamonds; d++) {
          int leftIdx = diamondInstructions.size();
          diamondInstructions.add(
              createParDoInstruction(prevIdx, 0, "diam_L_" + d, new DiamondLeftDoFn()));
          int rightIdx = diamondInstructions.size();
          diamondInstructions.add(
              createParDoInstruction(prevIdx, 0, "diam_R_" + d, new DiamondRightDoFn()));
          int flatIdx = diamondInstructions.size();
          diamondInstructions.add(
              createFlattenInstruction(leftIdx, 0, rightIdx, 0, "diam_flat_" + d));
          prevIdx = flatIdx;
        }
        diamondInstructions.add(createWriteInstruction(prevIdx, 0));
        diamondFlattenBundle = new ExecutorBundle(diamondInstructions, disableTrampoline);
        diamondExecutor = diamondFlattenBundle.createExecutor();
      }
    }
  }

  @Benchmark
  public long heterogeneousOutputReceiverChain(ChainState state) throws Exception {
    state.receiverChainBundle.countingSink.written = 0;
    state.receiverExecutor.execute();
    return state.receiverChainBundle.countingSink.written;
  }

  @Benchmark
  public long heterogeneousProcessContextChain(ChainState state) throws Exception {
    state.contextChainBundle.countingSink.written = 0;
    state.contextExecutor.execute();
    return state.contextChainBundle.countingSink.written;
  }

  @Benchmark
  public long mapAndFilterChain(ChainState state) throws Exception {
    state.mapFilterChainBundle.countingSink.written = 0;
    state.mapFilterExecutor.execute();
    return state.mapFilterChainBundle.countingSink.written;
  }

  @Benchmark
  public long diamondFlattenChain(ChainState state) throws Exception {
    state.diamondFlattenBundle.countingSink.written = 0;
    state.diamondExecutor.execute();
    return state.diamondFlattenBundle.countingSink.written;
  }
}
