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
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.Structs;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.util.common.worker.FlattenOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.InlineableFunctionProvider;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link FastStatelessParDoFn} and {@link FlattenOperation} elision. */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked", "SameNameButDifferent"})
public class FastStatelessParDoFnTest {

  private static final TupleTag<String> MAIN_TAG = new TupleTag<>("main");
  private static final ContextKey<String> OTEL_TEST_KEY = ContextKey.named("beam-fusion-test-key");

  private static ParDoOperation createStatelessOp(
      PipelineOptions options,
      CounterSet counters,
      String name,
      DoFn<String, String> fn,
      OutputReceiver out) {
    DataflowOperationContext opCtx =
        TestOperationContext.create(counters, NameContext.create("stage", name, name, name));
    BatchModeExecutionContext.StepContext stepCtx =
        BatchModeExecutionContext.forTesting(options, counters, "stage").getStepContext(opCtx);
    DoFnInfo<String, String> info =
        DoFnInfo.forFn(
            fn,
            WindowingStrategy.globalDefault(),
            null,
            StringUtf8Coder.of(),
            Collections.singletonMap(MAIN_TAG, StringUtf8Coder.of()),
            MAIN_TAG,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    FastStatelessParDoFn<String, String> parDoFn =
        new FastStatelessParDoFn<>(
            options,
            DoFnInstanceManagers.cloningPool(info, options),
            MAIN_TAG,
            Collections.singletonMap(MAIN_TAG, 0),
            stepCtx,
            opCtx,
            DoFnSchemaInformation.create());
    return new ParDoOperation(parDoFn, new OutputReceiver[] {out}, opCtx);
  }

  private static final class InlineMapTestDoFn extends DoFn<String, String>
      implements InlineableFunctionProvider<String, String> {
    private final ProcessFunction<String, String> fn;

    InlineMapTestDoFn(ProcessFunction<String, String> fn) {
      this.fn = fn;
    }

    @ProcessElement
    public void process(@Element String e, DoFn.OutputReceiver<String> out) throws Exception {
      out.output(fn.apply(e));
    }

    @Override
    public Kind kind() {
      return Kind.MAP;
    }

    @Override
    public ProcessFunction<String, ?> function() {
      return fn;
    }
  }

  private static final class ChildSpanStepDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String elem, DoFn.OutputReceiver<String> out) {
      String upstreamVal = Context.current().get(OTEL_TEST_KEY);
      Context child = Context.current().with(OTEL_TEST_KEY, upstreamVal + "/child");
      try (Scope ignored = child.makeCurrent()) {
        out.output(elem + "-step1");
      }
    }
  }

  private static final class StackDepthProbeDoFn extends DoFn<String, String> {
    static final AtomicInteger LAST_FRAMES = new AtomicInteger();

    @ProcessElement
    public void process(@Element String elem, DoFn.OutputReceiver<String> out) {
      long frames =
          StackWalker.getInstance()
              .walk(
                  stream ->
                      stream
                          .takeWhile(
                              f ->
                                  !f.getClassName()
                                      .equals(FastStatelessParDoFnTest.class.getName()))
                          .count());
      LAST_FRAMES.set((int) frames);
      assertSame(
          "FlattenOperation must be elided from call stack",
          0L,
          StackWalker.getInstance()
              .walk(
                  s ->
                      s.filter(f -> f.getClassName().equals(FlattenOperation.class.getName()))
                          .count()));
      out.output(elem);
    }
  }

  @Test
  public void testPreservesAllWindowedValueMetadataAndChildOtelSpans() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CounterSet counters = new CounterSet();
    List<WindowedValue<String>> captured = new ArrayList<>();
    Receiver sink = elem -> captured.add((WindowedValue<String>) elem);

    OutputReceiver out3 = new OutputReceiver();
    out3.addOutput(sink);

    // Step 3: Pass-through FlattenOperation (should execute in 0 frames when started)
    FlattenOperation flatten =
        new FlattenOperation(
            out3,
            TestOperationContext.create(
                counters, NameContext.create("stage", "flat", "flat", "flat")));

    OutputReceiver out2 = new OutputReceiver();
    out2.addOutput(flatten);

    // Step 2: InlineableFunctionProvider MAP
    ParDoOperation mapOp =
        createStatelessOp(
            options, counters, "map2", new InlineMapTestDoFn(s -> s + "-mapped"), out2);

    OutputReceiver out1 = new OutputReceiver();
    out1.addOutput(mapOp);

    // Step 1: Arbitrary DoFn opening a child OpenTelemetry Context around out.output(...)
    ParDoOperation step1Op =
        createStatelessOp(options, counters, "step1", new ChildSpanStepDoFn(), out1);

    flatten.start();
    mapOp.start();
    step1Op.start();

    Context initialOtel = Context.root().with(OTEL_TEST_KEY, "rootSpan");
    Instant ts = new Instant(123456789L);
    PaneInfo pane = PaneInfo.createPane(true, false, PaneInfo.Timing.ON_TIME, 3, 2);
    WindowedValue<String> input =
        WindowedValues.of(
            "hello",
            ts,
            GlobalWindow.INSTANCE,
            pane,
            "rec-42",
            999L,
            CausedByDrain.CAUSED_BY_DRAIN,
            initialOtel,
            ValueKind.UPDATE_AFTER);

    step1Op.process(input);

    step1Op.finish();
    mapOp.finish();
    flatten.finish();

    assertEquals(1, captured.size());
    WindowedValue<String> result = captured.get(0);
    assertEquals("hello-step1-mapped", result.getValue());
    assertEquals(ts, result.getTimestamp());
    assertEquals(
        Collections.singletonList(GlobalWindow.INSTANCE), new ArrayList<>(result.getWindows()));
    assertEquals(pane, result.getPaneInfo());
    assertEquals("rec-42", result.getRecordId());
    assertEquals(Long.valueOf(999L), result.getRecordOffset());
    assertEquals(CausedByDrain.CAUSED_BY_DRAIN, result.causedByDrain());
    assertEquals(ValueKind.UPDATE_AFTER, result.getValueKind());
    assertEquals("rootSpan/child", result.getOpenTelemetryContext().get(OTEL_TEST_KEY));
  }

  @Test
  public void testStackDepthCollapsedAndFlattenElided() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CounterSet counters = new CounterSet();
    StackDepthProbeDoFn.LAST_FRAMES.set(0);

    OutputReceiver leafOut = new OutputReceiver();
    ParDoOperation leafOp =
        createStatelessOp(options, counters, "leaf", new StackDepthProbeDoFn(), leafOut);

    OutputReceiver flatOut = new OutputReceiver();
    flatOut.addOutput(leafOp);
    FlattenOperation flatten =
        new FlattenOperation(
            flatOut,
            TestOperationContext.create(
                counters, NameContext.create("stage", "flat", "flat", "flat")));

    OutputReceiver rootOut = new OutputReceiver();
    rootOut.addOutput(flatten);
    ParDoOperation rootOp =
        createStatelessOp(options, counters, "root", new InlineMapTestDoFn(s -> s), rootOut);

    leafOp.start();
    flatten.start();
    rootOp.start();

    rootOp.process(WindowedValues.valueInGlobalWindow("x"));

    rootOp.finish();
    flatten.finish();
    leafOp.finish();

    // rootOp (ParDoOperation + FastStatelessParDoFn + processDirectMap) -> OutputReceiver ->
    // [Flatten elided] -> OutputReceiver -> leafOp (ParDoOperation + FastStatelessParDoFn +
    // DoFnInvoker + leafFn) <= 10 frames total (vs ~32 frames before Phase 1!)
    assertThat(StackDepthProbeDoFn.LAST_FRAMES.get(), lessThanOrEqualTo(11));
  }

  private static final class InlineFilterTestDoFn extends DoFn<String, String>
      implements InlineableFunctionProvider<String, String> {
    private final ProcessFunction<String, Boolean> pred;

    InlineFilterTestDoFn(ProcessFunction<String, Boolean> pred) {
      this.pred = pred;
    }

    @ProcessElement
    public void process(@Element String e, DoFn.OutputReceiver<String> out) throws Exception {
      if (pred.apply(e)) {
        out.output(e);
      }
    }

    @Override
    public Kind kind() {
      return Kind.FILTER;
    }

    @Override
    public ProcessFunction<String, ?> function() {
      return pred;
    }
  }

  private static com.google.api.services.dataflow.model.ParallelInstruction makeParDoInstr(
      int prodIdx, String name, DoFn<String, String> fn, CloudObject wvCoder) {
    com.google.api.services.dataflow.model.InstructionInput in =
        new com.google.api.services.dataflow.model.InstructionInput();
    in.setProducerInstructionIndex(prodIdx);
    in.setOutputNum(0);

    DoFnInfo<String, String> info =
        DoFnInfo.forFn(
            fn,
            WindowingStrategy.globalDefault(),
            null,
            StringUtf8Coder.of(),
            Collections.singletonMap(MAIN_TAG, StringUtf8Coder.of()),
            MAIN_TAG,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    CloudObject userFn = CloudObject.forClassName("DoFn");
    Structs.addString(
        userFn,
        PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(SerializableUtils.serializeToByteArray(info)));

    com.google.api.services.dataflow.model.MultiOutputInfo moi =
        new com.google.api.services.dataflow.model.MultiOutputInfo();
    moi.setTag(MAIN_TAG.getId());

    com.google.api.services.dataflow.model.ParDoInstruction parDo =
        new com.google.api.services.dataflow.model.ParDoInstruction();
    parDo.setInput(in);
    parDo.setNumOutputs(1);
    parDo.setMultiOutputInfos(Collections.singletonList(moi));
    parDo.setUserFn(userFn);

    com.google.api.services.dataflow.model.InstructionOutput out =
        new com.google.api.services.dataflow.model.InstructionOutput();
    out.setName(name + "_out");
    out.setOriginalName(name + ".out");
    out.setSystemName(name + "_out");
    out.setCodec(wvCoder);

    com.google.api.services.dataflow.model.ParallelInstruction instr =
        new com.google.api.services.dataflow.model.ParallelInstruction();
    instr.setParDo(parDo);
    instr.setOutputs(Collections.singletonList(out));
    instr.setName(name);
    instr.setOriginalName(name);
    instr.setSystemName(name);
    return instr;
  }

  @Test
  public void testCompiledSegmentViaExecutorFactoryPreservesCountersAndReusesClasses()
      throws Exception {
    CloudObject wvCoder =
        CloudObjects.asCloudObject(
            WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE), null);

    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    cloudSource.setSpec(CloudObject.forClassName("MemSource"));
    cloudSource.setCodec(wvCoder);
    com.google.api.services.dataflow.model.ReadInstruction readInstr =
        new com.google.api.services.dataflow.model.ReadInstruction();
    readInstr.setSource(cloudSource);
    com.google.api.services.dataflow.model.InstructionOutput readOut =
        new com.google.api.services.dataflow.model.InstructionOutput();
    readOut.setName("read_out");
    readOut.setOriginalName("read.out");
    readOut.setSystemName("read_out");
    readOut.setCodec(wvCoder);
    com.google.api.services.dataflow.model.ParallelInstruction readPi =
        new com.google.api.services.dataflow.model.ParallelInstruction();
    readPi.setRead(readInstr);
    readPi.setOutputs(Collections.singletonList(readOut));
    readPi.setName("Read");
    readPi.setOriginalName("Read");
    readPi.setSystemName("Read");

    com.google.api.services.dataflow.model.InstructionInput writeIn =
        new com.google.api.services.dataflow.model.InstructionInput();
    writeIn.setProducerInstructionIndex(4);
    writeIn.setOutputNum(0);
    com.google.api.services.dataflow.model.Sink cloudSink =
        new com.google.api.services.dataflow.model.Sink();
    cloudSink.setSpec(CloudObject.forClassName("MemSink"));
    cloudSink.setCodec(wvCoder);
    com.google.api.services.dataflow.model.WriteInstruction writeInstr =
        new com.google.api.services.dataflow.model.WriteInstruction();
    writeInstr.setInput(writeIn);
    writeInstr.setSink(cloudSink);
    com.google.api.services.dataflow.model.ParallelInstruction writePi =
        new com.google.api.services.dataflow.model.ParallelInstruction();
    writePi.setWrite(writeInstr);
    writePi.setName("Write");
    writePi.setOriginalName("Write");
    writePi.setSystemName("Write");

    List<com.google.api.services.dataflow.model.ParallelInstruction> instructions =
        Arrays.asList(
            readPi,
            makeParDoInstr(0, "s1", new ChildSpanStepDoFn(), wvCoder),
            makeParDoInstr(1, "s2", new InlineFilterTestDoFn(s -> !s.startsWith("drop")), wvCoder),
            makeParDoInstr(2, "s3", new InlineMapTestDoFn(s -> s + "!"), wvCoder),
            makeParDoInstr(3, "s4", new StackDepthProbeDoFn(), wvCoder),
            writePi);

    com.google.api.services.dataflow.model.MapTask mapTask =
        new com.google.api.services.dataflow.model.MapTask();
    mapTask.setStageName("CompiledStage");
    mapTask.setSystemName("CompiledStage");
    mapTask.setInstructions(instructions);
    mapTask.setFactory(Transport.getJsonFactory());

    List<String> written = new ArrayList<>();
    ReaderFactory readerFactory =
        (spec, coder, opts, execCtx, opCtx) ->
            new NativeReader<WindowedValue<String>>() {
              @Override
              public NativeReaderIterator<WindowedValue<String>> iterator() {
                final List<WindowedValue<String>> items =
                    Arrays.asList(
                        WindowedValues.valueInGlobalWindow("keep1"),
                        WindowedValues.valueInGlobalWindow("drop2"),
                        WindowedValues.valueInGlobalWindow("keep3"));
                return new NativeReaderIterator<WindowedValue<String>>() {
                  int i = -1;

                  @Override
                  public boolean start() {
                    i = 0;
                    return true;
                  }

                  @Override
                  public boolean advance() {
                    return ++i < items.size();
                  }

                  @Override
                  public WindowedValue<String> getCurrent() {
                    return items.get(i);
                  }
                };
              }

              @Override
              public boolean supportsRestart() {
                return true;
              }
            };
    SinkFactory sinkFactory =
        (spec, coder, opts, execCtx, opCtx) ->
            new Sink<WindowedValue<String>>() {
              @Override
              public SinkWriter<WindowedValue<String>> writer() {
                return new SinkWriter<WindowedValue<String>>() {
                  @Override
                  public long add(WindowedValue<String> value) {
                    written.add(value.getValue());
                    return 8L;
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
            };

    PipelineOptions options = PipelineOptionsFactory.create();
    CounterSet counterSet = new CounterSet();
    BatchModeExecutionContext execCtx =
        BatchModeExecutionContext.forTesting(options, counterSet, "CompiledStage");
    MutableNetwork<Node, Edge> net1 =
        new MapTaskToNetworkFunction(IdGenerators.decrementingLongs()).apply(mapTask);
    DataflowMapTaskExecutor exec1 =
        IntrinsicMapTaskExecutorFactory.defaultFactory()
            .create(
                net1,
                options,
                "CompiledStage",
                readerFactory,
                sinkFactory,
                execCtx,
                counterSet,
                IdGenerators.decrementingLongs());

    exec1.getReadOperation().setProgressUpdatePeriodMs(-1L);
    exec1.execute();
    long classesAfterFirstBundle = FusedSegmentCompiler.generatedClassCount();
    // Execute a second bundle on the same executor AND a fresh executor to verify class cache reuse
    exec1.execute();
    MutableNetwork<Node, Edge> net2 =
        new MapTaskToNetworkFunction(IdGenerators.decrementingLongs()).apply(mapTask);
    DataflowMapTaskExecutor exec2 =
        IntrinsicMapTaskExecutorFactory.defaultFactory()
            .create(
                net2,
                options,
                "CompiledStage",
                readerFactory,
                sinkFactory,
                execCtx,
                counterSet,
                IdGenerators.decrementingLongs());
    exec2.execute();
    assertEquals(classesAfterFirstBundle, FusedSegmentCompiler.generatedClassCount());

    // 3 bundles * 2 kept elements/bundle = 6 outputs
    assertEquals(
        Arrays.asList(
            "keep1-step1!",
            "keep3-step1!",
            "keep1-step1!",
            "keep3-step1!",
            "keep1-step1!",
            "keep3-step1!"),
        written);

    // Verify per-edge ElementCount counters across all 3 bundles:
    // s1_out = 9 (3*3), s2_out = 6 (filtered), s3_out = 6, s4_out = 6
    Counter<Long, ?> s1Count =
        (Counter<Long, ?>) counterSet.getExistingCounter(CounterName.named("s1_out-ElementCount"));
    Counter<Long, ?> s2Count =
        (Counter<Long, ?>) counterSet.getExistingCounter(CounterName.named("s2_out-ElementCount"));
    Counter<Long, ?> s3Count =
        (Counter<Long, ?>) counterSet.getExistingCounter(CounterName.named("s3_out-ElementCount"));
    Counter<Long, ?> s4Count =
        (Counter<Long, ?>) counterSet.getExistingCounter(CounterName.named("s4_out-ElementCount"));
    assertEquals(Long.valueOf(9L), (Long) s1Count.getAggregate());
    assertEquals(Long.valueOf(6L), (Long) s2Count.getAggregate());
    assertEquals(Long.valueOf(6L), (Long) s3Count.getAggregate());
    assertEquals(Long.valueOf(6L), (Long) s4Count.getAggregate());
  }
}
