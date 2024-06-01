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
package org.apache.beam.runners.dataflow.worker.windmill.work.refresh;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * {@link ActiveWorkRefresher} implementation that fans out refreshes active work to multiple {@link
 * GetDataStream}.
 */
@Internal
@ThreadSafe
public class FanOutActiveWorkRefresher extends ActiveWorkRefresher {

  private static final String REFRESH_ACTIVE_WORK_EXECUTOR = "RefreshWorkWithFanOut";
  private final Consumer<Map<GetDataStream, Map<String, List<HeartbeatRequest>>>>
      refreshActiveWorkFn;

  private FanOutActiveWorkRefresher(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      DataflowExecutionStateSampler sampler,
      Consumer<Map<GetDataStream, Map<String, List<HeartbeatRequest>>>> refreshActiveWorkFn,
      ScheduledExecutorService activeWorkRefreshExecutor) {
    super(
        clock,
        activeWorkRefreshPeriodMillis,
        stuckCommitDurationMillis,
        computations,
        sampler,
        activeWorkRefreshExecutor);
    this.refreshActiveWorkFn = refreshActiveWorkFn;
  }

  public static ActiveWorkRefresher create(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      DataflowExecutionStateSampler sampler,
      Consumer<Map<GetDataStream, Map<String, List<HeartbeatRequest>>>> refreshActiveWorkFn) {
    return new FanOutActiveWorkRefresher(
        clock,
        activeWorkRefreshPeriodMillis,
        stuckCommitDurationMillis,
        computations,
        sampler,
        refreshActiveWorkFn,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat(REFRESH_ACTIVE_WORK_EXECUTOR).build()));
  }

  @VisibleForTesting
  static FanOutActiveWorkRefresher forTesting(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      DataflowExecutionStateSampler sampler,
      Consumer<Map<GetDataStream, Map<String, List<HeartbeatRequest>>>> refreshActiveWorkFn,
      ScheduledExecutorService activeWorkRefreshExecutor) {
    return new FanOutActiveWorkRefresher(
        clock,
        activeWorkRefreshPeriodMillis,
        stuckCommitDurationMillis,
        computations,
        sampler,
        refreshActiveWorkFn,
        activeWorkRefreshExecutor);
  }

  @Override
  protected void refreshActiveWork() {
    Instant refreshDeadline = clock.get().minus(Duration.millis(activeWorkRefreshPeriodMillis));

    Map<GetDataStream, Map<String, List<HeartbeatRequest>>> fannedOutHeartbeatRequests =
        new HashMap<>();
    for (ComputationState computationState : computations.get()) {
      String computationId = computationState.getComputationId();

      // Get heartbeat requests for computation's current active work, aggregated by GetDataStream
      // to correctly fan-out the heartbeat requests.
      ImmutableListMultimap<GetDataStream, HeartbeatRequest> heartbeats =
          HeartbeatRequests.getRefreshableDirectKeyHeartbeats(
              computationState.currentActiveWorkReadOnly(), refreshDeadline, sampler);
      // Aggregate the heartbeats across computations by GetDataStream for correct fan out.
      for (Map.Entry<GetDataStream, Collection<HeartbeatRequest>> heartbeatsPerStream :
          heartbeats.asMap().entrySet()) {
        Map<String, List<HeartbeatRequest>> existingHeartbeats =
            fannedOutHeartbeatRequests.computeIfAbsent(
                heartbeatsPerStream.getKey(), ignored -> new HashMap<>());
        List<HeartbeatRequest> existingHeartbeatsForComputation =
            existingHeartbeats.computeIfAbsent(computationId, ignored -> new ArrayList<>());
        existingHeartbeatsForComputation.addAll(heartbeatsPerStream.getValue());
      }
    }

    refreshActiveWorkFn.accept(fannedOutHeartbeatRequests);
  }
}
