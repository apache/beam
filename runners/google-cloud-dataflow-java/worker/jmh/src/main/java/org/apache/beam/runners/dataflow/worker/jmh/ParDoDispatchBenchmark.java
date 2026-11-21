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

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.ElementByteSizeObservable;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.dataflow.worker.BatchModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.DataflowOutputCounter;
import org.apache.beam.runners.dataflow.worker.WorkerJmhAccess;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
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
 * Microbenchmarks for {@link org.apache.beam.runners.core.SimpleDoFnRunner} and fused Dataflow
 * Runner v1 {@link ParDoOperation} chains.
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

  private static final class CoderByteSizeObservable<T> implements ElementByteSizeObservable<T> {
    private final Coder<T> coder;

    CoderByteSizeObservable(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(T value) {
      return coder.isRegisterByteSizeObserverCheap(value);
    }

    @Override
    public void registerByteSizeObserver(T value, ElementByteSizeObserver observer)
        throws Exception {
      coder.registerByteSizeObserver(value, observer);
    }
  }

  private static ParDoOperation[] buildFusedChain(
      PipelineOptions options,
      BatchModeExecutionContext execCtx,
      WindowingStrategy<?, ?> strategy,
      int depth,
      WindowedValue<String> warmElem,
      long[] outputCounter)
      throws Exception {
    ParDoOperation[] ops = new ParDoOperation[depth];
    OutputReceiver[] receivers = new OutputReceiver[depth];
    Map<TupleTag<?>, Coder<?>> coders = Collections.singletonMap(MAIN_TAG, StringUtf8Coder.of());
    Map<TupleTag<?>, Integer> tagToIdx = Collections.singletonMap(MAIN_TAG, 0);
    Coder<WindowedValue<String>> fullCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), strategy.getWindowFn().windowCoder());
    ElementByteSizeObservable<WindowedValue<String>> observable =
        new CoderByteSizeObservable<>(fullCoder);

    for (int i = 0; i < depth; i++) {
      NameContext nc = NameContext.create("S1", "s" + i, "s" + i, "ParDo" + i);
      DataflowOperationContext opCtx = execCtx.createOperationContext(nc);
      DataflowOutputCounter counter =
          DataflowOutputCounter.create(
              "s" + i + ".out", observable, opCtx.counterFactory(), nc, false);
      for (int w = 0; w < 1_000_010; w++) {
        counter.update(warmElem);
      }
      OutputReceiver receiver = new OutputReceiver();
      receiver.addOutputCounter("out", counter);
      receivers[i] = receiver;
      ParDoFn fn =
          WorkerJmhAccess.createSimpleParDoFn(
              options,
              new PassThroughDoFn(),
              StringUtf8Coder.of(),
              coders,
              strategy,
              MAIN_TAG,
              tagToIdx,
              execCtx,
              opCtx);
      ops[i] = new ParDoOperation(fn, new OutputReceiver[] {receiver}, opCtx);
    }

    for (int i = 0; i < depth - 1; i++) {
      receivers[i].addOutput(ops[i + 1]);
    }
    receivers[depth - 1].addOutput(elem -> outputCounter[0]++);

    for (int i = depth - 1; i >= 0; i--) {
      ops[i].start();
    }
    return ops;
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
      Instant ts = new Instant(1_700_000_000_000L);
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
    ParDoOperation head;
    ParDoOperation[] ops;
    Closeable activation;
    WindowedValue<String> elem;
    final long[] outputCounter = new long[1];

    @Setup
    public void setUp() throws Exception {
      PipelineOptions options = PipelineOptionsFactory.create();
      elem = WindowedValues.valueInGlobalWindow("bench-element-payload");
      BatchModeExecutionContext execCtx =
          BatchModeExecutionContext.forTesting(options, new CounterFactory(), "S_GLOBAL");
      activation = execCtx.getExecutionStateTracker().activate();
      ops =
          buildFusedChain(
              options, execCtx, WindowingStrategy.globalDefault(), 4, elem, outputCounter);
      head = ops[0];
    }

    @TearDown
    public void tearDown() throws Exception {
      for (ParDoOperation op : ops) {
        op.finish();
      }
      if (activation != null) {
        activation.close();
      }
    }
  }

  @State(Scope.Thread)
  public static class FixedWindowChainState {
    ParDoOperation head;
    ParDoOperation[] ops;
    Closeable activation;
    WindowedValue<String> elem;
    final long[] outputCounter = new long[1];

    @Setup
    public void setUp() throws Exception {
      PipelineOptions options = PipelineOptionsFactory.create();
      Instant ts = new Instant(1_700_000_000_000L);
      IntervalWindow window = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
      elem = WindowedValues.of("bench-element-payload", ts, window, PaneInfo.NO_FIRING);
      WindowingStrategy<?, ?> fixedStrategy =
          WindowingStrategy.of(FixedWindows.of(Duration.standardMinutes(1)));
      BatchModeExecutionContext execCtx =
          BatchModeExecutionContext.forTesting(options, new CounterFactory(), "S_FIXED");
      activation = execCtx.getExecutionStateTracker().activate();
      ops = buildFusedChain(options, execCtx, fixedStrategy, 4, elem, outputCounter);
      head = ops[0];
    }

    @TearDown
    public void tearDown() throws Exception {
      for (ParDoOperation op : ops) {
        op.finish();
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
