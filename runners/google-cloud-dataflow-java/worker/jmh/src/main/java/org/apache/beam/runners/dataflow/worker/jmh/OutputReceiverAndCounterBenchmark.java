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

import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.ElementByteSizeObservable;
import org.apache.beam.runners.dataflow.worker.DataflowOutputCounter;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory.ElementByteSizeObservableCoder;
import org.apache.beam.runners.dataflow.worker.WorkerJmhAccess;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Microbenchmarks for {@link OutputReceiver}, {@link DataflowOutputCounter}, and {@link Counter}
 * per-element dispatch and counting overhead on Dataflow Java Runner fused edges.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class OutputReceiverAndCounterBenchmark {

  @State(Scope.Thread)
  public static class ReceiverState {
    OutputReceiver globalReceiver;
    OutputReceiver intervalReceiver;
    OutputReceiver twoReceivers;
    DataflowOutputCounter standaloneCounter;
    Counter<Long, Long> longSumCounter;

    WindowedValue<String> globalElem;
    WindowedValue<String> singleIntervalElem;
    long sinkCount;

    @Setup
    public void setUp() throws Exception {
      CounterFactory counterFactory = new CounterFactory();
      NameContext nameContext = NameContext.create("S01", "s1", "step1", "ParDo1");
      Coder<WindowedValue<String>> globalCoder =
          WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
      Coder<WindowedValue<String>> intervalCoder =
          WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());
      ElementByteSizeObservable<WindowedValue<String>> globalObservable =
          new ElementByteSizeObservableCoder<>(globalCoder);
      ElementByteSizeObservable<WindowedValue<String>> intervalObservable =
          new ElementByteSizeObservableCoder<>(intervalCoder);

      Instant ts = Instant.now();
      IntervalWindow window = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
      globalElem = WindowedValues.valueInGlobalWindow("payload-element-0001");
      singleIntervalElem =
          WindowedValues.of("payload-element-0001", ts, window, PaneInfo.NO_FIRING);

      DataflowOutputCounter counterGlobal =
          WorkerJmhAccess.createWarmedOutputCounter(
              "out1", globalObservable, counterFactory, nameContext, false, globalElem);
      globalReceiver = new OutputReceiver();
      globalReceiver.addOutputCounter("out1", counterGlobal);
      globalReceiver.addOutput(elem -> sinkCount++);

      DataflowOutputCounter counterInterval =
          WorkerJmhAccess.createWarmedOutputCounter(
              "out1i", intervalObservable, counterFactory, nameContext, false, singleIntervalElem);
      intervalReceiver = new OutputReceiver();
      intervalReceiver.addOutputCounter("out1i", counterInterval);
      intervalReceiver.addOutput(elem -> sinkCount++);

      DataflowOutputCounter counter2 =
          WorkerJmhAccess.createWarmedOutputCounter(
              "out2", globalObservable, counterFactory, nameContext, false, globalElem);
      twoReceivers = new OutputReceiver();
      twoReceivers.addOutputCounter("out2", counter2);
      twoReceivers.addOutput(elem -> sinkCount++);
      twoReceivers.addOutput(elem -> sinkCount++);

      standaloneCounter =
          WorkerJmhAccess.createWarmedOutputCounter(
              "out3", intervalObservable, counterFactory, nameContext, false, singleIntervalElem);

      longSumCounter = counterFactory.longSum(CounterName.named("bench-long-sum"));
      longSumCounter.addValue(1L);
    }
  }

  @Benchmark
  public void outputReceiver_globalWindow(ReceiverState state) throws Exception {
    state.globalReceiver.process(state.globalElem);
  }

  @Benchmark
  public void outputReceiver_singleIntervalWindow(ReceiverState state) throws Exception {
    state.intervalReceiver.process(state.singleIntervalElem);
  }

  @Benchmark
  public void outputReceiver_twoOutputs(ReceiverState state) throws Exception {
    state.twoReceivers.process(state.globalElem);
  }

  @Benchmark
  public void dataflowOutputCounter_updateSingleWindow(ReceiverState state) throws Exception {
    state.standaloneCounter.update(state.singleIntervalElem);
  }

  /**
   * Isolates the {@link Counter#addValue} primitive called on every emitted element inside {@link
   * DataflowOutputCounter#update(Object)}.
   */
  @Benchmark
  public void counter_longSumAddValue(ReceiverState state) {
    state.longSumCounter.addValue(1L);
  }
}
