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

import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.WriteInstruction;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.BatchModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.ReaderFactory;
import org.apache.beam.runners.dataflow.worker.ReaderRegistry;
import org.apache.beam.runners.dataflow.worker.SinkFactory;
import org.apache.beam.runners.dataflow.worker.SinkRegistry;
import org.apache.beam.runners.dataflow.worker.apiary.FixMultiOutputInfosOnParDoInstructions;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Microbenchmarks for {@link org.apache.beam.runners.core.SimpleDoFnRunner} and fused Dataflow Java
 * Runner {@link ParDoOperation} chains built via {@link IntrinsicMapTaskExecutorFactory}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ParDoDispatchBenchmark {

  private static final TupleTag<String> MAIN_TAG = new TupleTag<>("main");
  private static final TupleTag<String> SIDE_TAG = new TupleTag<>("side");

  private static final class PassThroughDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String elem, DoFn.OutputReceiver<String> out) {
      out.output(elem);
    }
  }

  private static final class MultiOutDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String elem, MultiOutputReceiver out) {
      out.get(MAIN_TAG).output(elem);
      out.get(SIDE_TAG).output(elem);
    }
  }

  private static final class WindowObservingDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(
        @Element String elem, BoundedWindow window, DoFn.OutputReceiver<String> out) {
      if (window != null) {
        out.output(elem);
      }
    }
  }

  private static final class SkewTimestampDoFn extends DoFn<String, String> {
    private static final Duration SKEW = Duration.standardHours(1);

    @Override
    public Duration getAllowedTimestampSkew() {
      return SKEW;
    }

    @ProcessElement
    public void processElement(
        @Element String elem, @Timestamp Instant ts, DoFn.OutputReceiver<String> out) {
      out.outputWithTimestamp(elem, ts);
    }
  }

  /** Minimal {@link ReaderFactory} used to root a fused {@link MapTask} instruction graph. */
  private static final class EmptyReaderFactory implements ReaderFactory {
    @Override
    @SuppressWarnings("rawtypes")
    public NativeReader<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext) {
      return new NativeReader<WindowedValue<String>>() {
        @Override
        public boolean supportsRestart() {
          return true;
        }

        @Override
        public NativeReaderIterator<WindowedValue<String>> iterator() {
          return new NativeReaderIterator<WindowedValue<String>>() {
            @Override
            public boolean start() {
              return false;
            }

            @Override
            public boolean advance() {
              return false;
            }

            @Override
            public WindowedValue<String> getCurrent() {
              throw new NoSuchElementException();
            }

            @Override
            public void close() {}
          };
        }
      };
    }
  }

  /** Minimal {@link SinkFactory} that counts emitted elements at the tail of a fused chain. */
  private static final class CountingSinkFactory implements SinkFactory {
    private final long[] outputCounter;

    CountingSinkFactory(long[] outputCounter) {
      this.outputCounter = outputCounter;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Sink<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext) {
      return new Sink<WindowedValue<String>>() {
        @Override
        public boolean supportsRestart() {
          return true;
        }

        @Override
        public SinkWriter<WindowedValue<String>> writer() {
          return new SinkWriter<WindowedValue<String>>() {
            @Override
            public long add(WindowedValue<String> value) {
              outputCounter[0]++;
              return 0L;
            }

            @Override
            public void close() {}

            @Override
            public void abort() throws IOException {}
          };
        }
      };
    }
  }

  private static CloudObject windowedStringCloudCoder(WindowingStrategy<?, ?> strategy) {
    Coder<WindowedValue<String>> fullCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), strategy.getWindowFn().windowCoder());
    return CloudObjects.asCloudObject(fullCoder, /* sdkComponents= */ null);
  }

  private static ParallelInstruction createReadInstruction(String name, CloudObject cloudCoder) {
    Source cloudSource = new Source();
    cloudSource.setSpec(CloudObject.forClass(EmptyReaderFactory.class));
    cloudSource.setCodec(cloudCoder);

    ReadInstruction readInstruction = new ReadInstruction();
    readInstruction.setSource(cloudSource);

    InstructionOutput output = new InstructionOutput();
    output.setName(name + ".out");
    output.setOriginalName(name + ".out");
    output.setSystemName(name + ".out");
    output.setCodec(cloudCoder);

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setRead(readInstruction);
    instruction.setOutputs(Collections.singletonList(output));
    instruction.setName(name);
    instruction.setSystemName(name);
    instruction.setOriginalName(name);
    return instruction;
  }

  private static ParallelInstruction createParDoInstruction(
      int producerIndex,
      String systemName,
      WindowingStrategy<?, ?> strategy,
      CloudObject cloudCoder) {
    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(producerIndex);
    cloudInput.setOutputNum(0);

    String serializedFn =
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(
                DoFnInfo.forFn(
                    new PassThroughDoFn(),
                    strategy,
                    /* sideInputViews= */ Collections.emptyList(),
                    StringUtf8Coder.of(),
                    Collections.singletonMap(MAIN_TAG, StringUtf8Coder.of()),
                    MAIN_TAG,
                    DoFnSchemaInformation.create(),
                    Collections.emptyMap())));

    CloudObject cloudUserFn = CloudObject.forClassName("DoFn");
    addString(cloudUserFn, PropertyNames.SERIALIZED_FN, serializedFn);

    MultiOutputInfo mainOutputInfo = new MultiOutputInfo();
    mainOutputInfo.setTag(MAIN_TAG.getId());

    ParDoInstruction parDoInstruction = new ParDoInstruction();
    parDoInstruction.setInput(cloudInput);
    parDoInstruction.setNumOutputs(1);
    parDoInstruction.setMultiOutputInfos(Collections.singletonList(mainOutputInfo));
    parDoInstruction.setUserFn(cloudUserFn);

    InstructionOutput output = new InstructionOutput();
    output.setName(systemName + ".out");
    output.setOriginalName(systemName + ".out");
    output.setSystemName(systemName + ".out");
    output.setCodec(cloudCoder);

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setParDo(parDoInstruction);
    instruction.setOutputs(Collections.singletonList(output));
    instruction.setSystemName(systemName);
    instruction.setOriginalName(systemName);
    instruction.setName(systemName);
    return instruction;
  }

  private static ParallelInstruction createWriteInstruction(
      int producerIndex, String systemName, CloudObject cloudCoder) {
    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(producerIndex);
    cloudInput.setOutputNum(0);

    com.google.api.services.dataflow.model.Sink cloudSink =
        new com.google.api.services.dataflow.model.Sink();
    cloudSink.setSpec(CloudObject.forClass(CountingSinkFactory.class));
    cloudSink.setCodec(cloudCoder);

    WriteInstruction writeInstruction = new WriteInstruction();
    writeInstruction.setInput(cloudInput);
    writeInstruction.setSink(cloudSink);

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setWrite(writeInstruction);
    instruction.setSystemName(systemName);
    instruction.setOriginalName(systemName);
    instruction.setName(systemName);
    return instruction;
  }

  private static DataflowMapTaskExecutor createFusedChainExecutor(
      PipelineOptions options,
      String stageName,
      BatchModeExecutionContext execCtx,
      CounterSet counterSet,
      WindowingStrategy<?, ?> strategy,
      int depth,
      long[] outputCounter) {
    CloudObject cloudCoder = windowedStringCloudCoder(strategy);
    List<ParallelInstruction> instructions = new ArrayList<>(depth + 2);
    instructions.add(createReadInstruction(stageName + "_read", cloudCoder));
    for (int i = 0; i < depth; i++) {
      instructions.add(createParDoInstruction(i, stageName + "_s" + i, strategy, cloudCoder));
    }
    instructions.add(createWriteInstruction(depth, stageName + "_write", cloudCoder));

    MapTask mapTask = new MapTask();
    mapTask.setStageName(stageName);
    mapTask.setSystemName(stageName);
    mapTask.setInstructions(instructions);
    mapTask.setFactory(Transport.getJsonFactory());

    IdGenerator idGenerator = IdGenerators.decrementingLongs();
    Function<MapTask, MutableNetwork<Node, Edge>> mapTaskToNetwork =
        new FixMultiOutputInfosOnParDoInstructions(idGenerator)
            .andThen(new MapTaskToNetworkFunction(idGenerator));

    ReaderRegistry readerRegistry =
        ReaderRegistry.defaultRegistry()
            .register(EmptyReaderFactory.class.getName(), new EmptyReaderFactory());
    SinkRegistry sinkRegistry =
        SinkRegistry.defaultRegistry()
            .register(CountingSinkFactory.class.getName(), new CountingSinkFactory(outputCounter));

    DataflowMapTaskExecutor executor =
        IntrinsicMapTaskExecutorFactory.defaultFactory()
            .create(
                mapTaskToNetwork.apply(mapTask),
                options,
                stageName,
                readerRegistry,
                sinkRegistry,
                execCtx,
                counterSet,
                idGenerator);
    executor.getReadOperation().setProgressUpdatePeriodMs(ReadOperation.DONT_UPDATE_PERIODICALLY);
    return executor;
  }

  private static ParDoOperation startAndWarmFusedChain(
      DataflowMapTaskExecutor executor, WindowedValue<String> warmElem, long[] outputCounter)
      throws Exception {
    List<Operation> ops = executor.operations;
    // Start Write and ParDo operations in reverse topological order (skipping ReadOperation at 0).
    for (int i = ops.size() - 1; i >= 1; i--) {
      ops.get(i).start();
    }
    ParDoOperation head = (ParDoOperation) ops.get(1);
    outputCounter[0] = 0L;
    // Warm past the adaptive byte-size sampling ramp so DataflowOutputCounters operate at
    // steady-state minimum sampling probability (0.001%).
    for (int i = 0; i < OutputObjectAndByteCounter.SAMPLING_TOKEN_UPPER_BOUND; i++) {
      head.process(warmElem);
    }
    if (outputCounter[0] != OutputObjectAndByteCounter.SAMPLING_TOKEN_UPPER_BOUND) {
      throw new IllegalStateException(
          "Expected "
              + OutputObjectAndByteCounter.SAMPLING_TOKEN_UPPER_BOUND
              + " warmed outputs, got "
              + outputCounter[0]);
    }
    return head;
  }

  private static void finishChainOperations(DataflowMapTaskExecutor executor) throws Exception {
    List<Operation> ops = executor.operations;
    for (int i = 1; i < ops.size(); i++) {
      ops.get(i).finish();
    }
  }

  @State(Scope.Thread)
  public static class SimpleRunnerState {
    DoFnRunner<String, String> passThroughRunner;
    DoFnRunner<String, String> multiOutRunner;
    DoFnRunner<String, String> windowObservingRunner;
    DoFnRunner<String, String> skewTimestampRunner;

    WindowedValue<String> globalElem;
    WindowedValue<String> singleIntervalElem;
    long outputCount;

    @Setup
    public void setUp() {
      PipelineOptions options = PipelineOptionsFactory.create();
      Instant ts = Instant.now();
      IntervalWindow window = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
      globalElem = WindowedValues.valueInGlobalWindow("bench-element-payload");
      singleIntervalElem =
          WindowedValues.of("bench-element-payload", ts, window, PaneInfo.NO_FIRING);

      WindowingStrategy<?, ?> globalStrategy = WindowingStrategy.globalDefault();
      WindowingStrategy<?, ?> fixedStrategy =
          WindowingStrategy.of(FixedWindows.of(Duration.standardMinutes(1)));

      BatchModeExecutionContext runnerCtx = BatchModeExecutionContext.forTesting(options, "S0");
      DataflowOperationContext opCtx =
          runnerCtx.createOperationContext(NameContext.create("S0", "s0", "s0", "Op0"));
      WindowedValueMultiReceiver sink =
          new WindowedValueMultiReceiver() {
            @Override
            public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
              outputCount++;
            }
          };

      Map<TupleTag<?>, Coder<?>> mainCoders =
          Collections.singletonMap(MAIN_TAG, StringUtf8Coder.of());
      Map<TupleTag<?>, Coder<?>> multiCoders = new HashMap<>();
      multiCoders.put(MAIN_TAG, StringUtf8Coder.of());
      multiCoders.put(SIDE_TAG, StringUtf8Coder.of());

      passThroughRunner =
          DoFnRunners.simpleRunner(
              options,
              new PassThroughDoFn(),
              NullSideInputReader.empty(),
              sink,
              MAIN_TAG,
              Collections.emptyList(),
              runnerCtx.getStepContext(opCtx),
              StringUtf8Coder.of(),
              mainCoders,
              globalStrategy,
              DoFnSchemaInformation.create(),
              Collections.emptyMap());
      passThroughRunner.startBundle();

      multiOutRunner =
          DoFnRunners.simpleRunner(
              options,
              new MultiOutDoFn(),
              NullSideInputReader.empty(),
              sink,
              MAIN_TAG,
              Collections.singletonList(SIDE_TAG),
              runnerCtx.getStepContext(opCtx),
              StringUtf8Coder.of(),
              multiCoders,
              globalStrategy,
              DoFnSchemaInformation.create(),
              Collections.emptyMap());
      multiOutRunner.startBundle();

      windowObservingRunner =
          DoFnRunners.simpleRunner(
              options,
              new WindowObservingDoFn(),
              NullSideInputReader.empty(),
              sink,
              MAIN_TAG,
              Collections.emptyList(),
              runnerCtx.getStepContext(opCtx),
              StringUtf8Coder.of(),
              mainCoders,
              fixedStrategy,
              DoFnSchemaInformation.create(),
              Collections.emptyMap());
      windowObservingRunner.startBundle();

      skewTimestampRunner =
          DoFnRunners.simpleRunner(
              options,
              new SkewTimestampDoFn(),
              NullSideInputReader.empty(),
              sink,
              MAIN_TAG,
              Collections.emptyList(),
              runnerCtx.getStepContext(opCtx),
              StringUtf8Coder.of(),
              mainCoders,
              fixedStrategy,
              DoFnSchemaInformation.create(),
              Collections.emptyMap());
      skewTimestampRunner.startBundle();
    }

    @TearDown
    public void tearDown() {
      passThroughRunner.finishBundle();
      multiOutRunner.finishBundle();
      windowObservingRunner.finishBundle();
      skewTimestampRunner.finishBundle();
    }
  }

  @State(Scope.Thread)
  public static class GlobalChainState {
    DataflowMapTaskExecutor executor;
    ParDoOperation head;
    Closeable activation;
    WindowedValue<String> elem;
    final long[] outputCounter = new long[1];

    @Setup
    public void setUp() throws Exception {
      PipelineOptions options = PipelineOptionsFactory.create();
      elem = WindowedValues.valueInGlobalWindow("bench-element-payload");
      CounterSet counterSet = new CounterSet();
      BatchModeExecutionContext execCtx =
          BatchModeExecutionContext.forTesting(options, counterSet, "S_GLOBAL");
      activation = execCtx.getExecutionStateTracker().activate();
      executor =
          createFusedChainExecutor(
              options,
              "S_GLOBAL",
              execCtx,
              counterSet,
              WindowingStrategy.globalDefault(),
              4,
              outputCounter);
      head = startAndWarmFusedChain(executor, elem, outputCounter);
    }

    @TearDown
    public void tearDown() throws Exception {
      if (executor != null) {
        finishChainOperations(executor);
        executor.close();
      }
      if (activation != null) {
        activation.close();
      }
    }
  }

  @State(Scope.Thread)
  public static class FixedWindowChainState {
    DataflowMapTaskExecutor executor;
    ParDoOperation head;
    Closeable activation;
    WindowedValue<String> elem;
    final long[] outputCounter = new long[1];

    @Setup
    public void setUp() throws Exception {
      PipelineOptions options = PipelineOptionsFactory.create();
      Instant ts = Instant.now();
      IntervalWindow window = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
      elem = WindowedValues.of("bench-element-payload", ts, window, PaneInfo.NO_FIRING);
      WindowingStrategy<?, ?> fixedStrategy =
          WindowingStrategy.of(FixedWindows.of(Duration.standardMinutes(1)));
      CounterSet counterSet = new CounterSet();
      BatchModeExecutionContext execCtx =
          BatchModeExecutionContext.forTesting(options, counterSet, "S_FIXED");
      activation = execCtx.getExecutionStateTracker().activate();
      executor =
          createFusedChainExecutor(
              options, "S_FIXED", execCtx, counterSet, fixedStrategy, 4, outputCounter);
      head = startAndWarmFusedChain(executor, elem, outputCounter);
    }

    @TearDown
    public void tearDown() throws Exception {
      if (executor != null) {
        finishChainOperations(executor);
        executor.close();
      }
      if (activation != null) {
        activation.close();
      }
    }
  }

  @Benchmark
  public void simpleDoFnRunner_processElement_outputReceiver(SimpleRunnerState state) {
    state.passThroughRunner.processElement(state.globalElem);
  }

  @Benchmark
  public void simpleDoFnRunner_processElement_singleWindow(SimpleRunnerState state) {
    state.passThroughRunner.processElement(state.singleIntervalElem);
  }

  @Benchmark
  public void simpleDoFnRunner_processElement_multiOutput(SimpleRunnerState state) {
    state.multiOutRunner.processElement(state.globalElem);
  }

  @Benchmark
  public void simpleDoFnRunner_processElement_observesWindow(SimpleRunnerState state) {
    state.windowObservingRunner.processElement(state.singleIntervalElem);
  }

  @Benchmark
  public void simpleDoFnRunner_processElement_outputWithTimestamp(SimpleRunnerState state) {
    state.skewTimestampRunner.processElement(state.singleIntervalElem);
  }

  @Benchmark
  public void fusedChain_4ParDos_globalWindow(GlobalChainState state) throws Exception {
    state.head.process(state.elem);
  }

  @Benchmark
  public void fusedChain_4ParDos_singleIntervalWindow(FixedWindowChainState state)
      throws Exception {
    state.head.process(state.elem);
  }
}
