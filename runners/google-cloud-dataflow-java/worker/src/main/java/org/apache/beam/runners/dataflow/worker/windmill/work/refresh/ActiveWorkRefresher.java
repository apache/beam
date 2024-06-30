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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Asynchronously GetData requests to Streaming Engine for all sufficiently old active work and
 * invalidates stuck commits.
 *
 * <p>This informs Windmill that processing is ongoing and the work should not be retried. The age
 * threshold is determined by {@link #activeWorkRefreshPeriodMillis}
 */
@ThreadSafe
@Internal
public final class ActiveWorkRefresher {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveWorkRefresher.class);

  private final Supplier<Instant> clock;
  private final int activeWorkRefreshPeriodMillis;
  private final Supplier<Collection<ComputationState>> computations;
  private final DataflowExecutionStateSampler sampler;
  private final int stuckCommitDurationMillis;
  private final ScheduledExecutorService activeWorkRefreshExecutor;
  private final Consumer<Map<HeartbeatSender, Map<String, List<Windmill.HeartbeatRequest>>>>
      heartbeatSender;

  public ActiveWorkRefresher(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      DataflowExecutionStateSampler sampler,
      ScheduledExecutorService activeWorkRefreshExecutor,
      Consumer<Map<HeartbeatSender, Map<String, List<Windmill.HeartbeatRequest>>>>
          heartbeatSender) {
    this.clock = clock;
    this.activeWorkRefreshPeriodMillis = activeWorkRefreshPeriodMillis;
    this.stuckCommitDurationMillis = stuckCommitDurationMillis;
    this.computations = computations;
    this.sampler = sampler;
    this.activeWorkRefreshExecutor = activeWorkRefreshExecutor;
    this.heartbeatSender = heartbeatSender;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    if (activeWorkRefreshPeriodMillis > 0) {
      activeWorkRefreshExecutor.scheduleWithFixedDelay(
          () -> {
            try {
              refreshActiveWork();
            } catch (RuntimeException e) {
              LOG.warn("Failed to refresh active work: ", e);
            }
          },
          activeWorkRefreshPeriodMillis,
          activeWorkRefreshPeriodMillis,
          TimeUnit.MILLISECONDS);
    }

    if (stuckCommitDurationMillis > 0) {
      int periodMillis = Math.max(stuckCommitDurationMillis / 10, 100);
      activeWorkRefreshExecutor.scheduleWithFixedDelay(
          this::invalidateStuckCommits, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }
  }

  public void stop() {
    if (activeWorkRefreshPeriodMillis > 0 || stuckCommitDurationMillis > 0) {
      activeWorkRefreshExecutor.shutdown();
      try {
        activeWorkRefreshExecutor.awaitTermination(300, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        activeWorkRefreshExecutor.shutdownNow();
      }
    }
  }

  private void invalidateStuckCommits() {
    Instant stuckCommitDeadline = clock.get().minus(Duration.millis(stuckCommitDurationMillis));
    for (ComputationState computationState : computations.get()) {
      computationState.invalidateStuckCommits(stuckCommitDeadline);
    }
  }

  private void refreshActiveWork() {
    Instant refreshDeadline = clock.get().minus(Duration.millis(activeWorkRefreshPeriodMillis));

    Map<HeartbeatSender, Map<String, List<Windmill.HeartbeatRequest>>> fannedOutHeartbeatRequests =
        new HashMap<>();
    for (ComputationState computationState : computations.get()) {
      String computationId = computationState.getComputationId();

      // Get heartbeat requests for computation's current active work, aggregated by GetDataStream
      // to correctly fan-out the heartbeat requests.
      ImmutableListMultimap<HeartbeatSender, Windmill.HeartbeatRequest> heartbeats =
          HeartbeatRequests.getRefreshableKeyHeartbeats(
              computationState.currentActiveWorkReadOnly(), refreshDeadline, sampler);
      // Aggregate the heartbeats across computations by GetDataStream for correct fan out.
      for (Map.Entry<HeartbeatSender, Collection<Windmill.HeartbeatRequest>> heartbeatsPerStream :
          heartbeats.asMap().entrySet()) {
        Map<String, List<Windmill.HeartbeatRequest>> existingHeartbeats =
            fannedOutHeartbeatRequests.computeIfAbsent(
                heartbeatsPerStream.getKey(), ignored -> new HashMap<>());
        List<Windmill.HeartbeatRequest> existingHeartbeatsForComputation =
            existingHeartbeats.computeIfAbsent(computationId, ignored -> new ArrayList<>());
        existingHeartbeatsForComputation.addAll(heartbeatsPerStream.getValue());
      }
    }

    heartbeatSender.accept(fannedOutHeartbeatRequests);
  }
}
