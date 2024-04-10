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
package org.apache.beam.runners.dataflow.worker.streaming.processing;

import java.util.Collection;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkProcessingContext;
import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Instant;

@Internal
@ThreadSafe
public final class StreamingWorkItemScheduler {

  private final Supplier<Instant> clock;
  private final StreamingWorkExecutor streamingWorkExecutor;

  public StreamingWorkItemScheduler(
      Supplier<Instant> clock, StreamingWorkExecutor streamingWorkExecutor) {
    this.clock = clock;
    this.streamingWorkExecutor = streamingWorkExecutor;
  }

  /**
   * * Schedules the {@link Work} for immediate (or future) execution by queueing it up it's owning
   * {@link ComputationState}.
   */
  public void scheduleWork(
      ComputationState computationState,
      Instant inputDataWatermark,
      @Nullable Instant synchronizedProcessingTime,
      Windmill.WorkItem workItem,
      Consumer<Commit> workCommitter,
      BiFunction<String, Windmill.KeyedGetDataRequest, Windmill.KeyedGetDataResponse> getDataFn,
      Collection<Windmill.LatencyAttribution> getWorkStreamLatencies) {
    Work scheduledWork =
        createWork(
            workItem,
            computationState,
            synchronizedProcessingTime,
            inputDataWatermark,
            workCommitter,
            getDataFn,
            getWorkStreamLatencies);
    computationState.activateWork(scheduledWork);
  }

  public void scheduleWork(
      ComputationState computationState,
      WorkProcessingContext workProcessingContext,
      Collection<Windmill.LatencyAttribution> getWorkStreamLatencies) {
    Work scheduledWork =
        Work.create(
            workProcessingContext,
            clock,
            getWorkStreamLatencies,
            work -> streamingWorkExecutor.execute(computationState, work));
    computationState.activateWork(scheduledWork);
  }

  private Work createWork(
      Windmill.WorkItem workItem,
      ComputationState computationState,
      @Nullable Instant synchronizedProcessingTime,
      Instant inputDataWatermark,
      Consumer<Commit> workCommitter,
      BiFunction<String, Windmill.KeyedGetDataRequest, Windmill.KeyedGetDataResponse> getDataFn,
      Collection<Windmill.LatencyAttribution> getWorkStreamLatencies) {
    return Work.create(
        WorkProcessingContext.builder(
                computationState.getComputationId(),
                (computationId, request) ->
                    Optional.ofNullable(getDataFn.apply(computationId, request)))
            .setWorkCommitter(workCommitter)
            .setWorkItem(workItem)
            .setSynchronizedProcessingTime(synchronizedProcessingTime)
            .setInputDataWatermark(inputDataWatermark)
            .setOutputDataWatermark(
                WindmillTimeUtils.windmillToHarnessWatermark(workItem.getOutputDataWatermark()))
            .setComputationId(computationState.getComputationId())
            .build(),
        clock,
        getWorkStreamLatencies,
        work -> streamingWorkExecutor.execute(computationState, work));
  }
}
