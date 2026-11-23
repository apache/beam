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
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.dataflow.model.FlattenInstruction;
import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Sink;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.WriteInstruction;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.apiary.FixMultiOutputInfosOnParDoInstructions;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterMean;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.util.common.worker.FlattenOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.MapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.StageTrampoline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link StageTrampoline} and {@link IntrinsicMapTaskExecutorFactory} cuts. */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked", "SameNameButDifferent"})
public class StageTrampolineTest {

  private static final TupleTag<String> MAIN_TAG = new TupleTag<>("main");
  private static final TupleTag<String> SIDE_TAG = new TupleTag<>("side");
  private static final ContextKey<String> OTEL_KEY = ContextKey.named("trampoline-otel-key");
  private static final List<WindowedValue<String>> SINK_ELEMENTS = new ArrayList<>();
  private static final AtomicInteger MAX_OBSERVED_FRAMES = new AtomicInteger();
  private static List<String> currentSourceElements = Collections.emptyList();

  @Before
  public void setUp() {
    SINK_ELEMENTS.clear();
    MAX_OBSERVED_FRAMES.set(0);
    currentSourceElements = Collections.emptyList();
  }

  public static final class ListReaderFactory implements ReaderFactory {
    @Override
    public NativeReader<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext) {
      final List<String> items = currentSourceElements;
      return new NativeReader<WindowedValue<String>>() {
        @Override
        public NativeReaderIterator<WindowedValue<String>> iterator() {
          return new NativeReaderIterator<WindowedValue<String>>() {
            private int idx = -1;

            @Override
            public boolean start() {
              idx = 0;
              return idx < items.size();
            }

            @Override
            public boolean advance() {
              idx++;
              return idx < items.size();
            }

            @Override
            public WindowedValue<String> getCurrent() {
              if (idx < 0 || idx >= items.size()) {
                throw new NoSuchElementException();
              }
              return WindowedValues.valueInGlobalWindow(items.get(idx));
            }
          };
        }
      };
    }
  }

  public static final class CapturingSinkFactory implements SinkFactory {
    @Override
    public org.apache.beam.runners.dataflow.worker.util.common.worker.Sink<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext context) {
      return new org.apache.beam.runners.dataflow.worker.util.common.worker.Sink<
          WindowedValue<String>>() {
        @Override
        public SinkWriter<WindowedValue<String>> writer() {
          return new SinkWriter<WindowedValue<String>>() {
            @Override
            public long add(WindowedValue<String> value) {
              SINK_ELEMENTS.add(value);
              return 4L;
            }

            @Override
            public void close() {}

            @Override
            public void abort() {}
          };
        }
      };
    }
  }

  private static final class AppendStepDoFn extends DoFn<String, String> {
    private final int stepIdx;
    private final boolean recordStack;

    AppendStepDoFn(int stepIdx, boolean recordStack) {
      this.stepIdx = stepIdx;
      this.recordStack = recordStack;
    }

    @ProcessElement
    public void process(@Element String elem, DoFn.OutputReceiver<String> out) {
      if (recordStack) {
        int frames =
            StackWalker.getInstance()
                .walk(
                    s ->
                        (int)
                            s.takeWhile(
                                    f -> !f.getClassName().equals(MapTaskExecutor.class.getName()))
                                .count());
        MAX_OBSERVED_FRAMES.accumulateAndGet(frames, Math::max);
      }
      out.output(elem + ":" + stepIdx);
    }
  }

  private static final class FanOutDoFn extends DoFn<String, String> {
    private final int count;

    FanOutDoFn(int count) {
      this.count = count;
    }

    @ProcessElement
    public void process(@Element String elem, DoFn.OutputReceiver<String> out) {
      for (int i = 0; i < count; i++) {
        out.output(elem + "#" + i);
      }
    }
  }

  private static final class MultiOutputSplitDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String elem, MultiOutputReceiver out) {
      if ((elem.hashCode() & 1) == 0) {
        out.get(MAIN_TAG).output(elem + "L");
      } else {
        out.get(SIDE_TAG).output(elem + "R");
      }
    }
  }

  private static CloudObject windowedStringCoder() {
    return CloudObjects.asCloudObject(
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE), null);
  }

  private static ParallelInstruction createReadInstruction(String name) {
    Source cloudSource = new Source();
    cloudSource.setSpec(CloudObject.forClass(ListReaderFactory.class));
    cloudSource.setCodec(windowedStringCoder());
    ReadInstruction read = new ReadInstruction();
    read.setSource(cloudSource);
    InstructionOutput output = new InstructionOutput();
    output.setName(name + "_out");
    output.setCodec(windowedStringCoder());
    output.setOriginalName(name + "_out");
    output.setSystemName(name + "_out");
    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setName(name);
    instruction.setOriginalName(name);
    instruction.setSystemName(name);
    instruction.setRead(read);
    instruction.setOutputs(Collections.singletonList(output));
    return instruction;
  }

  private static ParallelInstruction createWriteInstruction(
      int producerIdx, int producerOutNum, String name) {
    InstructionInput input = new InstructionInput();
    input.setProducerInstructionIndex(producerIdx);
    input.setOutputNum(producerOutNum);
    Sink cloudSink = new Sink();
    cloudSink.setSpec(CloudObject.forClass(CapturingSinkFactory.class));
    cloudSink.setCodec(windowedStringCoder());
    WriteInstruction write = new WriteInstruction();
    write.setInput(input);
    write.setSink(cloudSink);
    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setName(name);
    instruction.setOriginalName(name);
    instruction.setSystemName(name);
    instruction.setWrite(write);
    return instruction;
  }

  private static ParallelInstruction createParDoInstruction(
      int producerIdx,
      int producerOutNum,
      String name,
      DoFn<String, String> fn,
      List<TupleTag<String>> outputTags) {
    InstructionInput input = new InstructionInput();
    input.setProducerInstructionIndex(producerIdx);
    input.setOutputNum(producerOutNum);

    Map<TupleTag<?>, Coder<?>> outputCoders = new HashMap<>();
    for (TupleTag<String> tag : outputTags) {
      outputCoders.put(tag, StringUtf8Coder.of());
    }
    DoFnInfo<String, String> doFnInfo =
        DoFnInfo.forFn(
            fn,
            WindowingStrategy.globalDefault(),
            null,
            StringUtf8Coder.of(),
            outputCoders,
            MAIN_TAG,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    CloudObject userFnSpec = CloudObject.forClassName("DoFn");
    addString(
        userFnSpec,
        PropertyNames.SERIALIZED_FN,
        byteArrayToJsonString(serializeToByteArray(doFnInfo)));

    List<MultiOutputInfo> multiOutputInfos = new ArrayList<>();
    List<InstructionOutput> outputs = new ArrayList<>();
    for (int i = 0; i < outputTags.size(); i++) {
      TupleTag<String> tag = outputTags.get(i);
      MultiOutputInfo moi = new MultiOutputInfo();
      moi.setTag(tag.getId());
      multiOutputInfos.add(moi);

      InstructionOutput out = new InstructionOutput();
      out.setName(name + "_out_" + i);
      out.setOriginalName(name + "_out_" + i);
      out.setSystemName(name + "_out_" + i);
      out.setCodec(windowedStringCoder());
      outputs.add(out);
    }

    ParDoInstruction parDo = new ParDoInstruction();
    parDo.setInput(input);
    parDo.setNumOutputs(outputTags.size());
    parDo.setUserFn(userFnSpec);
    parDo.setMultiOutputInfos(multiOutputInfos);

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setName(name);
    instruction.setOriginalName(name);
    instruction.setSystemName(name);
    instruction.setParDo(parDo);
    instruction.setOutputs(outputs);
    return instruction;
  }

  private static ParallelInstruction createFlattenInstruction(
      int prod1, int out1, int prod2, int out2, String name) {
    InstructionInput in1 = new InstructionInput();
    in1.setProducerInstructionIndex(prod1);
    in1.setOutputNum(out1);
    InstructionInput in2 = new InstructionInput();
    in2.setProducerInstructionIndex(prod2);
    in2.setOutputNum(out2);

    FlattenInstruction flatten = new FlattenInstruction();
    flatten.setInputs(Arrays.asList(in1, in2));

    InstructionOutput out = new InstructionOutput();
    out.setName(name + "_out");
    out.setOriginalName(name + "_out");
    out.setSystemName(name + "_out");
    out.setCodec(windowedStringCoder());

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setName(name);
    instruction.setOriginalName(name);
    instruction.setSystemName(name);
    instruction.setFlatten(flatten);
    instruction.setOutputs(Collections.singletonList(out));
    return instruction;
  }

  private static DataflowMapTaskExecutor buildExecutor(
      List<ParallelInstruction> instructions, PipelineOptions options, CounterSet counterSet) {
    IdGenerator idGenerator = IdGenerators.decrementingLongs();
    Function<MapTask, MutableNetwork<Node, Edge>> mapTaskToNetwork =
        new FixMultiOutputInfosOnParDoInstructions(idGenerator)
            .andThen(new MapTaskToNetworkFunction(idGenerator));
    MapTask mapTask = new MapTask();
    mapTask.setStageName("stage");
    mapTask.setSystemName("stage");
    mapTask.setInstructions(instructions);
    mapTask.setFactory(Transport.getJsonFactory());

    ReaderRegistry readerRegistry =
        ReaderRegistry.defaultRegistry()
            .register(ListReaderFactory.class.getName(), new ListReaderFactory());
    SinkRegistry sinkRegistry =
        SinkRegistry.defaultRegistry()
            .register(CapturingSinkFactory.class.getName(), new CapturingSinkFactory());

    return IntrinsicMapTaskExecutorFactory.defaultFactory()
        .create(
            mapTaskToNetwork.apply(mapTask),
            options,
            "stage",
            readerRegistry,
            sinkRegistry,
            BatchModeExecutionContext.forTesting(options, counterSet, "stage"),
            counterSet,
            idGenerator);
  }

  @Test
  public void testDeepChain50StepsBoundsStackDepth() throws Exception {
    currentSourceElements = Arrays.asList("a", "b", "c", "d", "e");
    List<ParallelInstruction> instructions = new ArrayList<>();
    instructions.add(createReadInstruction("Read"));
    for (int i = 1; i <= 50; i++) {
      instructions.add(
          createParDoInstruction(
              i - 1,
              0,
              "Step" + i,
              new AppendStepDoFn(i, i == 50),
              Collections.singletonList(MAIN_TAG)));
    }
    instructions.add(createWriteInstruction(50, 0, "Write"));

    // 1. Run with default StageTrampoline enabled (threshold = 90, deep-stack guardrail)
    PipelineOptions defaultOpts = PipelineOptionsFactory.create();
    CounterSet counters1 = new CounterSet();
    int framesWithDefaultTrampoline;
    List<String> outWithTrampoline = new ArrayList<>();
    try (DataflowMapTaskExecutor executor = buildExecutor(instructions, defaultOpts, counters1)) {
      assertNotNull(executor.getStageTrampoline());
      assertThat(executor.getStageTrampoline().cutCount(), greaterThanOrEqualTo(1));
      executor.execute();
      framesWithDefaultTrampoline = MAX_OBSERVED_FRAMES.get();
      for (WindowedValue<String> wv : SINK_ELEMENTS) {
        outWithTrampoline.add(wv.getValue());
      }
    }

    // 2. Run with tighter threshold = 12 to verify strict <= 20 frame cap
    SINK_ELEMENTS.clear();
    MAX_OBSERVED_FRAMES.set(0);
    PipelineOptions tightOpts = PipelineOptionsFactory.create();
    tightOpts
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(Collections.singletonList("trampoline_fusion_threshold=12"));
    int framesWithTightTrampoline;
    try (DataflowMapTaskExecutor executor =
        buildExecutor(instructions, tightOpts, new CounterSet())) {
      assertNotNull(executor.getStageTrampoline());
      assertThat(executor.getStageTrampoline().cutCount(), greaterThan(10));
      executor.execute();
      framesWithTightTrampoline = MAX_OBSERVED_FRAMES.get();
    }

    // 3. Run with StageTrampoline disabled for direct comparison
    SINK_ELEMENTS.clear();
    MAX_OBSERVED_FRAMES.set(0);
    PipelineOptions disabledOpts = PipelineOptionsFactory.create();
    disabledOpts
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(Collections.singletonList("disable_fusion_trampoline"));
    CounterSet counters2 = new CounterSet();
    int framesWithoutTrampoline;
    List<String> outWithoutTrampoline = new ArrayList<>();
    try (DataflowMapTaskExecutor executor = buildExecutor(instructions, disabledOpts, counters2)) {
      assertNull(executor.getStageTrampoline());
      executor.execute();
      framesWithoutTrampoline = MAX_OBSERVED_FRAMES.get();
      for (WindowedValue<String> wv : SINK_ELEMENTS) {
        outWithoutTrampoline.add(wv.getValue());
      }
    }

    assertEquals(outWithoutTrampoline, outWithTrampoline);
    assertEquals(5, outWithTrampoline.size());
    assertThat(framesWithDefaultTrampoline, lessThanOrEqualTo(140));
    assertThat(framesWithTightTrampoline, lessThanOrEqualTo(20));
    assertThat(framesWithoutTrampoline, greaterThan(145));
  }

  @Test
  public void testDiamondFlattenMultiOutputAndOver64CutsExtraMasks() throws Exception {
    currentSourceElements = Arrays.asList("e0", "e1", "e2", "e3", "e4", "e5");
    List<ParallelInstruction> instructions = new ArrayList<>();
    instructions.add(createReadInstruction("Read")); // idx 0

    // Multi-output split -> two branches -> Flatten diamond
    instructions.add(
        createParDoInstruction(
            0, 0, "Split", new MultiOutputSplitDoFn(), Arrays.asList(MAIN_TAG, SIDE_TAG))); // idx 1
    instructions.add(
        createParDoInstruction(
            1,
            0,
            "Left",
            new AppendStepDoFn(101, false),
            Collections.singletonList(MAIN_TAG))); // idx 2
    instructions.add(
        createParDoInstruction(
            1,
            1,
            "Right",
            new AppendStepDoFn(102, false),
            Collections.singletonList(MAIN_TAG))); // idx 3
    instructions.add(createFlattenInstruction(2, 0, 3, 0, "Merge")); // idx 4

    // Append 70 more ParDo steps with threshold=1 to force > 64 cuts (extraMasks path)
    int prevIdx = 4;
    for (int i = 1; i <= 70; i++) {
      instructions.add(
          createParDoInstruction(
              prevIdx,
              0,
              "Chain" + i,
              new AppendStepDoFn(i, i == 70),
              Collections.singletonList(MAIN_TAG)));
      prevIdx = instructions.size() - 1;
    }
    instructions.add(createWriteInstruction(prevIdx, 0, "Write"));

    PipelineOptions stressOpts = PipelineOptionsFactory.create();
    stressOpts
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(Collections.singletonList("trampoline_fusion_threshold=1"));
    CounterSet counters = new CounterSet();
    try (DataflowMapTaskExecutor executor = buildExecutor(instructions, stressOpts, counters)) {
      StageTrampoline trampoline = executor.getStageTrampoline();
      assertNotNull(trampoline);
      assertThat(trampoline.cutCount(), greaterThan(64));

      // Verify FlattenOperation's immediate OutputReceiver never carries a trampoline cut
      for (Operation op : executor.operations) {
        if (op instanceof FlattenOperation) {
          for (OutputReceiver r : op.receivers) {
            assertFalse(
                "FlattenOperation output must not have a trampoline cut", r.hasTrampolineCut());
          }
        }
      }

      executor.execute();
    }

    assertEquals(6, SINK_ELEMENTS.size());
    assertThat(MAX_OBSERVED_FRAMES.get(), lessThanOrEqualTo(16));
  }

  @Test
  public void testHotKey100kFanOutOverflowAndNestedCuts() throws Exception {
    currentSourceElements = Collections.singletonList("hotkey");
    List<ParallelInstruction> instructions = new ArrayList<>();
    instructions.add(createReadInstruction("Read")); // idx 0
    // Emit 50,000 elements in a single root @ProcessElement call (195x ring buffer capacity)
    instructions.add(
        createParDoInstruction(
            0, 0, "Explode50k", new FanOutDoFn(50_000), Collections.singletonList(MAIN_TAG)));
    // Mid-chain 1->2 amplifier + 18 steps across multiple trampoline cuts -> 100,000 elements
    for (int i = 1; i <= 9; i++) {
      instructions.add(
          createParDoInstruction(
              instructions.size() - 1,
              0,
              "Pre" + i,
              new AppendStepDoFn(i, false),
              Collections.singletonList(MAIN_TAG)));
    }
    instructions.add(
        createParDoInstruction(
            instructions.size() - 1,
            0,
            "Double",
            new FanOutDoFn(2),
            Collections.singletonList(MAIN_TAG)));
    for (int i = 10; i <= 18; i++) {
      instructions.add(
          createParDoInstruction(
              instructions.size() - 1,
              0,
              "Post" + i,
              new AppendStepDoFn(i, i == 18),
              Collections.singletonList(MAIN_TAG)));
    }
    instructions.add(createWriteInstruction(instructions.size() - 1, 0, "Write"));

    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.as(DataflowPipelineDebugOptions.class)
        .setExperiments(Collections.singletonList("trampoline_fusion_threshold=12"));
    CounterSet counters = new CounterSet();
    try (DataflowMapTaskExecutor executor = buildExecutor(instructions, opts, counters)) {
      assertNotNull(executor.getStageTrampoline());
      assertThat(executor.getStageTrampoline().cutCount(), greaterThan(2));
      executor.execute();
    }

    assertEquals(100_000, SINK_ELEMENTS.size());
    assertTrue(SINK_ELEMENTS.get(0).getValue().startsWith("hotkey#0:"));
    assertTrue(SINK_ELEMENTS.get(99_999).getValue().startsWith("hotkey#49999:"));
    assertThat(MAX_OBSERVED_FRAMES.get(), lessThanOrEqualTo(36));
  }

  /** Lazy iterable that only reports element byte sizes when actually iterated by downstream. */
  private static final class LazyTestIterable
      extends ElementByteSizeObservableIterable<String, ElementByteSizeObservableIterator<String>> {
    private final List<String> values;

    LazyTestIterable(List<String> values) {
      this.values = values;
    }

    @Override
    protected ElementByteSizeObservableIterator<String> createIterator() {
      final Iterator<String> delegate = values.iterator();
      return new ElementByteSizeObservableIterator<String>() {
        @Override
        public boolean hasNext() {
          return delegate.hasNext();
        }

        @Override
        public String next() {
          String v = delegate.next();
          notifyValueReturned(v.length() + 1L);
          return v;
        }
      };
    }
  }

  @Test
  public void testPreservesAll8WindowedValueMetadataFieldsAndLazyByteObserverAcrossCut()
      throws Exception {
    CounterSet counters = new CounterSet();
    Coder<WindowedValue<Iterable<String>>> fullCoder =
        WindowedValues.getFullCoder(
            IterableCoder.of(StringUtf8Coder.of()), GlobalWindow.Coder.INSTANCE);

    List<WindowedValue<Iterable<String>>> consumed = new ArrayList<>();
    List<String> iteratedItems = new ArrayList<>();
    Receiver consumer =
        elem -> {
          WindowedValue<Iterable<String>> wv = (WindowedValue<Iterable<String>>) elem;
          consumed.add(wv);
          for (String s : wv.getValue()) {
            iteratedItems.add(s);
          }
        };

    // Flatten downstream of cutReceiver -> consumer
    OutputReceiver postFlattenReceiver = new OutputReceiver();
    postFlattenReceiver.addOutput(consumer);
    FlattenOperation flattenOp =
        new FlattenOperation(
            postFlattenReceiver,
            TestOperationContext.create(
                counters, NameContext.create("stage", "flat", "flat", "flat")));

    // OutputReceiver with lazy byte counter AND trampoline cut upstream of it
    OutputReceiver cutOutputReceiver = new OutputReceiver();
    OutputObjectAndByteCounter byteCounter =
        new OutputObjectAndByteCounter(
                new IntrinsicMapTaskExecutorFactory.ElementByteSizeObservableCoder<>(fullCoder),
                counters,
                NameContext.create("stage", "prod", "prod", "prod"))
            .countObject("cut_edge_out-ObjectCount")
            .countMeanByte("cut_edge_out-MeanByteCount");
    cutOutputReceiver.addOutputCounter(byteCounter);
    cutOutputReceiver.addOutput(flattenOp);

    StageTrampoline trampoline = new StageTrampoline(Collections.singletonList(cutOutputReceiver));
    flattenOp.start();

    Context childOtel = Context.root().with(OTEL_KEY, "span-abc");
    Instant ts = new Instant(777_888L);
    PaneInfo pane = PaneInfo.createPane(false, true, PaneInfo.Timing.LATE, 5, 3);
    LazyTestIterable lazyItems = new LazyTestIterable(Arrays.asList("alpha", "beta", "gamma"));

    WindowedValue<Iterable<String>> input =
        WindowedValues.of(
            lazyItems,
            ts,
            GlobalWindow.INSTANCE,
            pane,
            "record-id-99",
            4242L,
            CausedByDrain.CAUSED_BY_DRAIN,
            childOtel,
            ValueKind.DELETE);

    try (Scope ignored = childOtel.makeCurrent()) {
      cutOutputReceiver.process(input);
      // Buffered in trampoline cut before drain!
      assertTrue(trampoline.hasPending());
      assertEquals(0, consumed.size());
    }

    // Drain after upstream OTel Scope has already closed!
    trampoline.drain();
    flattenOp.finish();

    assertFalse(trampoline.hasPending());
    assertEquals(Arrays.asList("alpha", "beta", "gamma"), iteratedItems);
    assertEquals(1, consumed.size());

    WindowedValue<Iterable<String>> out = consumed.get(0);
    assertEquals(ts, out.getTimestamp());
    assertEquals(
        Collections.singletonList(GlobalWindow.INSTANCE), new ArrayList<>(out.getWindows()));
    assertEquals(pane, out.getPaneInfo());
    assertEquals("record-id-99", out.getRecordId());
    assertEquals(Long.valueOf(4242L), out.getRecordOffset());
    assertEquals(CausedByDrain.CAUSED_BY_DRAIN, out.causedByDrain());
    assertEquals(ValueKind.DELETE, out.getValueKind());
    assertEquals("span-abc", out.getOpenTelemetryContext().get(OTEL_KEY));

    // Verify lazy byte observer recorded all 3 iterated elements (6 + 5 + 6 = 17 bytes + windowing)
    CounterMean<Long> meanBytes = byteCounter.getMeanByteCount().getAggregate();
    assertEquals(1L, meanBytes.getCount());
    assertThat(meanBytes.getAggregate(), greaterThan(17L));
  }

  @Test
  public void testBatchModeUngroupingParDoFnElementBeforeTimerOrderingAcrossCut() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CounterSet counters = new CounterSet();
    DataflowOperationContext opCtx =
        TestOperationContext.create(
            counters, NameContext.create("stage", "ungroup", "ungroup", "ungroup"));
    BatchModeExecutionContext.StepContext stepCtx =
        BatchModeExecutionContext.forTesting(options, counters, "stage").getStepContext(opCtx);

    List<String> downstreamEvents = new ArrayList<>();
    OutputReceiver downstreamReceiver = new OutputReceiver();
    downstreamReceiver.addOutput(elem -> downstreamEvents.add((String) elem));
    StageTrampoline trampoline = new StageTrampoline(Collections.singletonList(downstreamReceiver));

    ParDoFn underlying =
        new ParDoFn() {
          private Receiver out;

          @Override
          public void startBundle(Receiver... receivers) {
            this.out = receivers[0];
          }

          @Override
          public void processElement(Object untypedElem) throws Exception {
            WindowedValue<String> wv = (WindowedValue<String>) untypedElem;
            ValueState<String> state =
                stepCtx
                    .stateInternals()
                    .state(StateNamespaces.global(), StateTags.value("last", StringUtf8Coder.of()));
            state.write(wv.getValue());
            out.process("elem:" + wv.getValue());
          }

          @Override
          public void processTimers() throws Exception {
            ValueState<String> state =
                stepCtx
                    .stateInternals()
                    .state(StateNamespaces.global(), StateTags.value("last", StringUtf8Coder.of()));
            out.process("timerAfter:" + state.read());
          }

          @Override
          public void finishKey(Object key) {}

          @Override
          public void finishBundle() {}

          @Override
          public void abort() {}
        };

    BatchModeUngroupingParDoFn<String, String> ungrouping =
        new BatchModeUngroupingParDoFn<>(stepCtx, underlying);
    ungrouping.setStageTrampoline(trampoline);
    ungrouping.startBundle(downstreamReceiver);

    WindowedValue<KV<String, Iterable<KV<Instant, WindowedValue<String>>>>> keyA =
        WindowedValues.valueInGlobalWindow(
            KV.of(
                "kA",
                Arrays.asList(
                    KV.of(Instant.EPOCH, WindowedValues.valueInGlobalWindow("v1")),
                    KV.of(Instant.EPOCH, WindowedValues.valueInGlobalWindow("v2")))));
    WindowedValue<KV<String, Iterable<KV<Instant, WindowedValue<String>>>>> keyB =
        WindowedValues.valueInGlobalWindow(
            KV.of(
                "kB",
                Collections.singletonList(
                    KV.of(Instant.EPOCH, WindowedValues.valueInGlobalWindow("v3")))));

    ungrouping.processElement(keyA);
    ungrouping.processElement(keyB);
    ungrouping.finishBundle();

    assertEquals(
        Arrays.asList("elem:v1", "elem:v2", "timerAfter:v2", "elem:v3", "timerAfter:v3"),
        downstreamEvents);
  }

  @Test
  public void testDownstreamCutDrainsDuringOneToManyAndPreservesUngroupingOrderBehindUpstreamCut()
      throws Exception {
    // 1. Cut_0 -> 1-to-2000 FlatMap -> Cut_1 -> sink: Cut_1 must drain incrementally (bounded
    // memory)
    AtomicInteger produced = new AtomicInteger();
    AtomicInteger consumed = new AtomicInteger();
    AtomicInteger maxInFlight = new AtomicInteger();

    OutputReceiver cut0Recv = new OutputReceiver();
    OutputReceiver cut1Recv = new OutputReceiver();
    cut1Recv.addOutput(
        elem -> {
          int curInFlight = produced.get() - consumed.incrementAndGet();
          maxInFlight.accumulateAndGet(curInFlight, Math::max);
        });
    cut0Recv.addOutput(
        elem -> {
          for (int i = 0; i < 2000; i++) {
            produced.incrementAndGet();
            cut1Recv.process(i);
          }
        });

    StageTrampoline flatMapTrampoline = new StageTrampoline(Arrays.asList(cut0Recv, cut1Recv));
    cut0Recv.process("trigger");
    flatMapTrampoline.drain();
    assertEquals(2000, consumed.get());
    assertThat(
        "Downstream cut must drain at INITIAL_BUFFER_CAPACITY even while upstream cut is active",
        maxInFlight.get(),
        lessThanOrEqualTo(256));

    // 2. Upstream Cut_0 -> BatchModeUngroupingParDoFn -> {element Cut_1, timer Cut_2 (2 > 1)}
    PipelineOptions options = PipelineOptionsFactory.create();
    CounterSet counters = new CounterSet();
    DataflowOperationContext opCtx =
        TestOperationContext.create(
            counters, NameContext.create("stage", "ungroup2", "ungroup2", "ungroup2"));
    BatchModeExecutionContext.StepContext stepCtx =
        BatchModeExecutionContext.forTesting(options, counters, "stage").getStepContext(opCtx);

    List<String> events = new ArrayList<>();
    OutputReceiver upstreamCut0 = new OutputReceiver();
    OutputReceiver elemCut1 = new OutputReceiver();
    OutputReceiver timerCut2 = new OutputReceiver();
    elemCut1.addOutput(e -> events.add((String) e));
    timerCut2.addOutput(e -> events.add((String) e));

    ParDoFn underlying =
        new ParDoFn() {
          private Receiver elemOut;
          private Receiver timerOut;

          @Override
          public void startBundle(Receiver... receivers) {
            this.elemOut = receivers[0];
            this.timerOut = receivers[1];
          }

          @Override
          public void processElement(Object untypedElem) throws Exception {
            WindowedValue<String> wv = (WindowedValue<String>) untypedElem;
            elemOut.process("elem:" + wv.getValue());
          }

          @Override
          public void processTimers() throws Exception {
            timerOut.process("timer");
          }

          @Override
          public void finishKey(Object key) {}

          @Override
          public void finishBundle() {}

          @Override
          public void abort() {}
        };

    BatchModeUngroupingParDoFn<String, String> ungrouping =
        new BatchModeUngroupingParDoFn<>(stepCtx, underlying);
    StageTrampoline multiCutTrampoline =
        new StageTrampoline(Arrays.asList(upstreamCut0, elemCut1, timerCut2));
    ungrouping.setStageTrampoline(multiCutTrampoline);
    upstreamCut0.addOutput(ungrouping::processElement);
    ungrouping.startBundle(elemCut1, timerCut2);

    WindowedValue<KV<String, Iterable<KV<Instant, WindowedValue<String>>>>> keyElem =
        WindowedValues.valueInGlobalWindow(
            KV.of(
                "k1",
                Arrays.asList(
                    KV.of(Instant.EPOCH, WindowedValues.valueInGlobalWindow("a")),
                    KV.of(Instant.EPOCH, WindowedValues.valueInGlobalWindow("b")))));
    upstreamCut0.process(keyElem);
    multiCutTrampoline.drain();
    ungrouping.finishBundle();

    assertEquals(Arrays.asList("elem:a", "elem:b", "timer"), events);
  }
}
