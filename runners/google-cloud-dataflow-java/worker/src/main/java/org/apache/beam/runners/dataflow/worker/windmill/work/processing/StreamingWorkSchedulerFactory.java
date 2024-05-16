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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.ReaderCache;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.WorkFailureProcessor;
import org.apache.beam.sdk.fn.IdGenerator;
import org.joda.time.Instant;

public final class StreamingWorkSchedulerFactory {
  private StreamingWorkSchedulerFactory() {}

  public static StreamingWorkScheduler createStreamingWorkScheduler(
      DataflowWorkerHarnessOptions options,
      Supplier<Instant> clock,
      ReaderCache readerCache,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      BoundedQueueExecutor workExecutor,
      Function<String, WindmillStateCache.ForComputation> stateCacheFactory,
      Function<Windmill.GlobalDataRequest, Windmill.GlobalData> fetchGlobalDataFn,
      FailureTracker failureTracker,
      WorkFailureProcessor workFailureProcessor,
      StreamingCounters streamingCounters,
      HotKeyLogger hotKeyLogger,
      DataflowExecutionStateSampler sampler,
      AtomicInteger maxWorkItemCommitBytes,
      IdGenerator idGenerator,
      ConcurrentMap<String, StageInfo> stageInfoMap) {
    StreamingCommitFinalizer streamingCommitFinalizer =
        StreamingCommitFinalizer.create(workExecutor);
    ExecutionStateFactory executionStateFactory =
        new ExecutionStateFactory(
            options,
            mapTaskExecutorFactory,
            readerCache,
            stateCacheFactory,
            sampler,
            streamingCounters.pendingDeltaCounters(),
            idGenerator);
    SideInputStateFetcher sideInputStateFetcher =
        new SideInputStateFetcher(fetchGlobalDataFn, options);

    return new StreamingWorkScheduler(
        options,
        clock,
        executionStateFactory,
        sideInputStateFetcher,
        failureTracker,
        workFailureProcessor,
        streamingCommitFinalizer,
        streamingCounters,
        hotKeyLogger,
        stageInfoMap,
        sampler,
        maxWorkItemCommitBytes);
  }
}
