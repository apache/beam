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

import java.util.Collections;
import org.apache.beam.runners.core.ElementByteSizeObservable;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext.StreamingModeExecutionStateRegistry;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.NoopProfileScope;
import org.apache.beam.runners.dataflow.worker.streaming.config.FixedGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcherFactory;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.StreamingEngineFailureTracker;
import org.joda.time.Duration;

/**
 * Package-private bridge exposing worker internals to JMH benchmarks in {@code
 * org.apache.beam.runners.dataflow.worker.jmh} without altering production visibility.
 */
public final class WorkerJmhAccess {
  private WorkerJmhAccess() {}

  /**
   * Creates a {@link StreamingModeExecutionContext} backed by a {@link
   * StreamingModeExecutionStateRegistry} and streaming-enabled {@link
   * DataflowExecutionStateTracker} for JMH benchmarking.
   */
  public static StreamingModeExecutionContext createStreamingExecutionContext(
      DataflowWorkerHarnessOptions options, String stageName) {
    options.setStreaming(true);
    CounterSet counterSet = new CounterSet();
    StreamingModeExecutionStateRegistry stateRegistry = new StreamingModeExecutionStateRegistry();
    DataflowExecutionStateTracker tracker =
        new DataflowExecutionStateTracker(
            ExecutionStateSampler.newForTest(),
            stateRegistry.getState(
                NameContext.forStage(stageName), "other", null, NoopProfileScope.NOOP),
            counterSet,
            options,
            "jmh-work-item");
    return new StreamingModeExecutionContext(
        counterSet,
        stageName,
        new ReaderCache(Duration.standardMinutes(1), Runnable::run),
        Collections.emptyMap(),
        WindmillStateCache.builder().setSizeMb(1).build().forComputation(stageName),
        StreamingStepMetricsContainer.createRegistry(),
        tracker,
        stateRegistry,
        new FixedGlobalConfigHandle(StreamingGlobalConfig.builder().build()),
        Long.MAX_VALUE,
        /* throwExceptionOnLargeOutput= */ false,
        new HotKeyLogger(),
        /* hotKeyLoggingEnabled= */ false,
        /* stepName= */ stageName,
        /* systemName= */ stageName,
        StreamingCounters.create(),
        StreamingEngineFailureTracker.create(10, 10),
        MultiKeyBundleOptions.fromOptions(options),
        SideInputStateFetcherFactory.fromOptions(options));
  }

  /**
   * Creates a {@link DataflowOutputCounter} warmed through {@link
   * OutputObjectAndByteCounter#SAMPLING_TOKEN_UPPER_BOUND} updates so its adaptive byte-size
   * sampling is at the steady-state minimum probability (0.001%).
   */
  public static DataflowOutputCounter createWarmedOutputCounter(
      String outputName,
      ElementByteSizeObservable<?> observable,
      CounterFactory counterFactory,
      NameContext nameContext,
      boolean isStreaming,
      Object warmElem)
      throws Exception {
    DataflowOutputCounter counter =
        DataflowOutputCounter.create(
            outputName, observable, counterFactory, nameContext, isStreaming);
    for (int i = 0; i < OutputObjectAndByteCounter.SAMPLING_TOKEN_UPPER_BOUND; i++) {
      counter.update(warmElem);
    }
    return counter;
  }
}
