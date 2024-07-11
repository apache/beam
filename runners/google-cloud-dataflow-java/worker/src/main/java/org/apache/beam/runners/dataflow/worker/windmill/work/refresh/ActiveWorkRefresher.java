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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.toImmutableMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.RefreshableWork;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.annotations.Internal;
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
  private final Consumer<Map<HeartbeatSender, Heartbeats>> heartbeatSender;

  public ActiveWorkRefresher(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      DataflowExecutionStateSampler sampler,
      ScheduledExecutorService activeWorkRefreshExecutor,
      Consumer<Map<HeartbeatSender, Heartbeats>> heartbeatSender) {
    this.clock = clock;
    this.activeWorkRefreshPeriodMillis = activeWorkRefreshPeriodMillis;
    this.stuckCommitDurationMillis = stuckCommitDurationMillis;
    this.computations = computations;
    this.sampler = sampler;
    this.activeWorkRefreshExecutor = activeWorkRefreshExecutor;
    this.heartbeatSender = heartbeatSender;
  }

  private static Windmill.HeartbeatRequest createHeartbeatRequest(
      RefreshableWork work, DataflowExecutionStateSampler sampler) {
    return Windmill.HeartbeatRequest.newBuilder()
        .setShardingKey(work.getShardedKey().shardingKey())
        .setWorkToken(work.id().workToken())
        .setCacheToken(work.id().cacheToken())
        .addAllLatencyAttribution(work.getHeartbeatLatencyAttributions(sampler))
        .build();
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

    Map<HeartbeatSender, Heartbeats.Builder> heartbeatsBySender = new HashMap<>();

    // Aggregate the heartbeats across computations by HeartbeatSender for correct fan out.
    for (ComputationState computationState : computations.get()) {
      for (RefreshableWork work : computationState.getRefreshableWork(refreshDeadline)) {
        heartbeatsBySender
            .computeIfAbsent(work.heartbeatSender(), ignored -> Heartbeats.builder())
            .addWork(work)
            .addHeartbeatRequest(
                computationState.getComputationId(), createHeartbeatRequest(work, sampler));
      }
    }

    heartbeatSender.accept(
        heartbeatsBySender.entrySet().stream()
            .collect(toImmutableMap(Map.Entry::getKey, e -> e.getValue().build())));
  }
}
