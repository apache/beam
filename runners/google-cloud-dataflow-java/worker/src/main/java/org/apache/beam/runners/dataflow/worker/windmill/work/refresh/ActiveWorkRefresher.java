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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.RefreshableWork;
import org.apache.beam.runners.dataflow.worker.util.TerminatingExecutors;
import org.apache.beam.runners.dataflow.worker.util.TerminatingExecutors.TerminatingExecutorService;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
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
  private static final String FAN_OUT_REFRESH_WORK_EXECUTOR_NAME =
      "FanOutActiveWorkRefreshExecutor-%d";

  private final Supplier<Instant> clock;
  private final int activeWorkRefreshPeriodMillis;
  private final Supplier<Collection<ComputationState>> computations;
  private final DataflowExecutionStateSampler sampler;
  private final int stuckCommitDurationMillis;
  private final HeartbeatTracker heartbeatTracker;
  private final ScheduledExecutorService activeWorkRefreshExecutor;
  private final TerminatingExecutorService fanOutActiveWorkRefreshExecutor;

  public ActiveWorkRefresher(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      DataflowExecutionStateSampler sampler,
      Function<Logger, ScheduledExecutorService> activeWorkRefreshExecutor,
      HeartbeatTracker heartbeatTracker) {
    this.clock = clock;
    this.activeWorkRefreshPeriodMillis = activeWorkRefreshPeriodMillis;
    this.stuckCommitDurationMillis = stuckCommitDurationMillis;
    this.computations = computations;
    this.sampler = sampler;
    this.activeWorkRefreshExecutor = activeWorkRefreshExecutor.apply(LOG);
    this.heartbeatTracker = heartbeatTracker;
    this.fanOutActiveWorkRefreshExecutor =
        TerminatingExecutors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat(FAN_OUT_REFRESH_WORK_EXECUTOR_NAME), LOG);
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
    Map<HeartbeatSender, Heartbeats> heartbeatsBySender =
        aggregateHeartbeatsBySender(refreshDeadline);
    if (heartbeatsBySender.isEmpty()) {
      return;
    }

    List<CompletableFuture<Void>> fanOutRefreshActiveWork = new ArrayList<>();

    // Send the first heartbeat on the calling thread, and fan out the rest via the
    // fanOutActiveWorkRefreshExecutor.
    @Nullable Map.Entry<HeartbeatSender, Heartbeats> firstHeartbeat = null;
    for (Map.Entry<HeartbeatSender, Heartbeats> heartbeat : heartbeatsBySender.entrySet()) {
      if (firstHeartbeat == null) {
        firstHeartbeat = heartbeat;
      } else {
        fanOutRefreshActiveWork.add(
            CompletableFuture.runAsync(
                () -> sendHeartbeatSafely(heartbeat), fanOutActiveWorkRefreshExecutor));
      }
    }

    sendHeartbeatSafely(firstHeartbeat);
    fanOutRefreshActiveWork.forEach(CompletableFuture::join);
  }

  /** Aggregate the heartbeats across computations by HeartbeatSender for correct fan out. */
  private Map<HeartbeatSender, Heartbeats> aggregateHeartbeatsBySender(Instant refreshDeadline) {
    Map<HeartbeatSender, Heartbeats.Builder> heartbeatsBySender = new HashMap<>();

    // Aggregate the heartbeats across computations by HeartbeatSender for correct fan out.
    for (ComputationState computationState : computations.get()) {
      for (RefreshableWork work : computationState.getRefreshableWork(refreshDeadline)) {
        heartbeatsBySender
            .computeIfAbsent(work.heartbeatSender(), ignored -> Heartbeats.builder())
            .add(computationState.getComputationId(), work, sampler);
      }
    }

    return heartbeatsBySender.entrySet().stream()
        .collect(toImmutableMap(Map.Entry::getKey, e -> e.getValue().build()));
  }

  /**
   * Send the {@link Heartbeats} using the {@link HeartbeatSender}. Safe since exceptions are caught
   * and logged.
   */
  private void sendHeartbeatSafely(Map.Entry<HeartbeatSender, Heartbeats> heartbeat) {
    try (AutoCloseable ignored = heartbeatTracker.trackHeartbeats(heartbeat.getValue().size())) {
      HeartbeatSender sender = heartbeat.getKey();
      Heartbeats heartbeats = heartbeat.getValue();
      sender.sendHeartbeats(heartbeats);
    } catch (Exception e) {
      LOG.error(
          "Unable to send {} heartbeats to {}.",
          heartbeat.getValue().size(),
          heartbeat.getKey(),
          e);
    }
  }

  @FunctionalInterface
  public interface HeartbeatTracker {
    AutoCloseable trackHeartbeats(int numHeartbeats);
  }
}
