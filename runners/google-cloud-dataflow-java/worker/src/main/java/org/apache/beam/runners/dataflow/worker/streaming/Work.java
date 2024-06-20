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
package org.apache.beam.runners.dataflow.worker.streaming;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.value.AutoValue;
import com.google.common.base.Objects;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.worker.ActiveMessageMetadata;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution.ActiveLatencyBreakdown;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution.ActiveLatencyBreakdown.ActiveElementMetadata;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution.ActiveLatencyBreakdown.Distribution;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.DirectHeartbeatSender;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the state of an attempt to process a {@link WorkItem} by executing user code.
 *
 * @implNote Not thread safe, should not be executed or accessed by more than 1 thread at a time.
 */
@NotThreadSafe
@Internal
public final class Work implements RefreshableWork {
  private static final Logger LOG = LoggerFactory.getLogger(Work.class);

  private final ShardedKey shardedKey;
  private final WorkItem workItem;
  private final ProcessingContext processingContext;
  private final Watermarks watermarks;
  private final Supplier<Instant> clock;
  private final Instant startTime;
  private final Map<LatencyAttribution.State, Duration> totalDurationPerState;
  private final WorkId id;
  private final String latencyTrackingId;
  private final Runnable onFailed;
  private TimedState currentState;
  private volatile boolean isFailed;

  private Work(
      ShardedKey shardedKey,
      WorkItem workItem,
      ProcessingContext processingContext,
      Watermarks watermarks,
      Supplier<Instant> clock,
      Instant startTime,
      Map<LatencyAttribution.State, Duration> totalDurationPerState,
      WorkId id,
      String latencyTrackingId,
      Runnable onFailed,
      TimedState currentState,
      boolean isFailed) {
    this.shardedKey = shardedKey;
    this.workItem = workItem;
    this.watermarks = watermarks;
    this.clock = clock;
    this.startTime = startTime;
    this.totalDurationPerState = totalDurationPerState;
    this.id = id;
    this.latencyTrackingId = latencyTrackingId;
    this.onFailed = onFailed;
    this.currentState = currentState;
    this.isFailed = isFailed;
    this.processingContext =
        processingContext.heartbeatSender() instanceof DirectHeartbeatSender
                && !((DirectHeartbeatSender) processingContext.heartbeatSender())
                    .hasStreamClosedHandler()
            ? processingContext
                .toBuilder()
                .setHeartbeatSender(
                    ((DirectHeartbeatSender) processingContext.heartbeatSender())
                        .withStreamClosedHandler(() -> this.isFailed = true))
                .build()
            : processingContext;
  }

  public static Work create(
      WorkItem workItem,
      Watermarks watermarks,
      ProcessingContext processingContext,
      Supplier<Instant> clock,
      Collection<LatencyAttribution> getWorkStreamLatencies) {
    Instant startTime = clock.get();
    Work work =
        new Work(
            ShardedKey.create(workItem.getKey(), workItem.getShardingKey()),
            workItem,
            processingContext,
            watermarks,
            clock,
            startTime,
            new EnumMap<>(LatencyAttribution.State.class),
            WorkId.of(workItem),
            buildLatencyTrackingId(workItem),
            () -> {},
            TimedState.initialState(startTime),
            false);
    work.recordGetWorkStreamLatencies(getWorkStreamLatencies);
    return work;
  }

  public static ProcessingContext createProcessingContext(
      String computationId,
      BiFunction<String, KeyedGetDataRequest, KeyedGetDataResponse> getKeyedDataFn,
      Consumer<Commit> workCommitter,
      HeartbeatSender heartbeatSender) {
    return ProcessingContext.create(computationId, getKeyedDataFn, workCommitter, heartbeatSender);
  }

  private static LatencyAttribution.Builder createLatencyAttributionWithActiveLatencyBreakdown(
      boolean isHeartbeat, String workId, DataflowExecutionStateSampler sampler) {
    LatencyAttribution.Builder latencyAttribution = LatencyAttribution.newBuilder();
    if (isHeartbeat) {
      ActiveLatencyBreakdown.Builder stepBuilder = ActiveLatencyBreakdown.newBuilder();
      Optional<ActiveMessageMetadata> activeMessage =
          sampler.getActiveMessageMetadataForWorkId(workId);
      if (!activeMessage.isPresent()) {
        return latencyAttribution;
      }
      stepBuilder.setUserStepName(activeMessage.get().userStepName());
      ActiveElementMetadata.Builder activeElementBuilder = ActiveElementMetadata.newBuilder();
      activeElementBuilder.setProcessingTimeMillis(
          activeMessage.get().stopwatch().elapsed().toMillis());
      stepBuilder.setActiveMessageMetadata(activeElementBuilder);
      latencyAttribution.addActiveLatencyBreakdown(stepBuilder.build());
      return latencyAttribution;
    }

    Map<String, IntSummaryStatistics> processingDistributions =
        sampler.getProcessingDistributionsForWorkId(workId);
    for (Entry<String, IntSummaryStatistics> entry : processingDistributions.entrySet()) {
      ActiveLatencyBreakdown.Builder stepBuilder = ActiveLatencyBreakdown.newBuilder();
      stepBuilder.setUserStepName(entry.getKey());
      Distribution.Builder distributionBuilder =
          Distribution.newBuilder()
              .setCount(entry.getValue().getCount())
              .setMin(entry.getValue().getMin())
              .setMax(entry.getValue().getMax())
              .setMean((long) entry.getValue().getAverage())
              .setSum(entry.getValue().getSum());
      stepBuilder.setProcessingTimesDistribution(distributionBuilder.build());
      latencyAttribution.addActiveLatencyBreakdown(stepBuilder.build());
    }
    return latencyAttribution;
  }

  private static String buildLatencyTrackingId(WorkItem workItem) {
    return Long.toHexString(workItem.getShardingKey())
        + '-'
        + Long.toHexString(workItem.getWorkToken());
  }

  /** Returns a new {@link Work} instance with the same state and a different failure handler. */
  public Work withFailureHandler(Runnable onFailed) {
    return new Work(
        shardedKey,
        workItem,
        processingContext,
        watermarks,
        clock,
        startTime,
        totalDurationPerState,
        id,
        latencyTrackingId,
        onFailed,
        currentState,
        isFailed);
  }

  public WorkItem getWorkItem() {
    return workItem;
  }

  public ShardedKey getShardedKey() {
    return shardedKey;
  }

  public Optional<KeyedGetDataResponse> fetchKeyedState(KeyedGetDataRequest keyedGetDataRequest) {
    return processingContext.keyedDataFetcher().apply(keyedGetDataRequest);
  }

  public Watermarks watermarks() {
    return watermarks;
  }

  public Instant getStartTime() {
    return startTime;
  }

  public State getState() {
    return currentState.state();
  }

  public void setState(State state) {
    Instant now = clock.get();
    totalDurationPerState.compute(
        this.currentState.state().toLatencyAttributionState(),
        (s, d) ->
            new Duration(this.currentState.startTime(), now).plus(d == null ? Duration.ZERO : d));
    this.currentState = TimedState.create(state, now);
  }

  @Override
  public boolean isRefreshable(Instant refreshDeadline) {
    return getStartTime().isBefore(refreshDeadline) && !isFailed;
  }

  @Override
  public HeartbeatSender heartbeatSender() {
    return processingContext.heartbeatSender();
  }

  public void fail() {
    LOG.debug(
        "Failing work: [computationId= "
            + processingContext.computationId()
            + ", key="
            + shardedKey
            + ", workId="
            + id
            + "]. The work will be retried and is not lost.");
    this.isFailed = true;
    onFailed.run();
  }

  public boolean isCommitPending() {
    return currentState.isCommitPending();
  }

  public Instant getStateStartTime() {
    return currentState.startTime();
  }

  public String getLatencyTrackingId() {
    return latencyTrackingId;
  }

  public void queueCommit(WorkItemCommitRequest commitRequest, ComputationState computationState) {
    setState(State.COMMIT_QUEUED);
    processingContext.workCommitter().accept(Commit.create(commitRequest, computationState, this));
  }

  public WindmillStateReader createWindmillStateReader() {
    return WindmillStateReader.forWork(this);
  }

  @Override
  public WorkId id() {
    return id;
  }

  private void recordGetWorkStreamLatencies(Collection<LatencyAttribution> getWorkStreamLatencies) {
    for (LatencyAttribution latency : getWorkStreamLatencies) {
      totalDurationPerState.put(
          latency.getState(), Duration.millis(latency.getTotalDurationMillis()));
    }
  }

  @Override
  public ImmutableList<LatencyAttribution> getLatencyAttributions(
      boolean isHeartbeat, DataflowExecutionStateSampler sampler) {
    return Arrays.stream(LatencyAttribution.State.values())
        .map(state -> Pair.of(state, getTotalDurationAtLatencyAttributionState(state)))
        .filter(
            stateAndLatencyAttribution ->
                !stateAndLatencyAttribution.getValue().isEqual(Duration.ZERO))
        .map(
            stateAndLatencyAttribution ->
                createLatencyAttribution(
                    stateAndLatencyAttribution.getKey(),
                    isHeartbeat,
                    sampler,
                    stateAndLatencyAttribution.getValue()))
        .collect(toImmutableList());
  }

  private Duration getTotalDurationAtLatencyAttributionState(LatencyAttribution.State state) {
    Duration duration = totalDurationPerState.getOrDefault(state, Duration.ZERO);
    return state == this.currentState.state().toLatencyAttributionState()
        ? duration.plus(new Duration(this.currentState.startTime(), clock.get()))
        : duration;
  }

  private LatencyAttribution createLatencyAttribution(
      LatencyAttribution.State state,
      boolean isHeartbeat,
      DataflowExecutionStateSampler sampler,
      Duration latencyAttributionDuration) {
    LatencyAttribution.Builder latencyAttribution =
        state == LatencyAttribution.State.ACTIVE
            ? createLatencyAttributionWithActiveLatencyBreakdown(
                isHeartbeat, latencyTrackingId, sampler)
            : LatencyAttribution.newBuilder();
    return latencyAttribution
        .setState(state)
        .setTotalDurationMillis(latencyAttributionDuration.getMillis())
        .build();
  }

  public boolean isFailed() {
    return isFailed;
  }

  boolean isStuckCommittingAt(Instant stuckCommitDeadline) {
    return currentState.state() == Work.State.COMMITTING
        && currentState.startTime().isBefore(stuckCommitDeadline);
  }

  /** Returns a view of this {@link Work} instance for work refreshing. */
  public RefreshableWork refreshableView() {
    return this;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == null) return false;
    if (this == o) return true;
    if (!(o instanceof Work)) return false;
    Work work = (Work) o;
    return isFailed == work.isFailed
        && Objects.equal(shardedKey, work.shardedKey)
        && Objects.equal(workItem, work.workItem)
        && Objects.equal(processingContext, work.processingContext)
        && Objects.equal(watermarks, work.watermarks)
        && Objects.equal(clock, work.clock)
        && Objects.equal(startTime, work.startTime)
        && Objects.equal(totalDurationPerState, work.totalDurationPerState)
        && Objects.equal(id, work.id)
        && Objects.equal(latencyTrackingId, work.latencyTrackingId)
        && Objects.equal(currentState, work.currentState);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        shardedKey,
        workItem,
        processingContext,
        watermarks,
        clock,
        startTime,
        totalDurationPerState,
        id,
        latencyTrackingId,
        currentState,
        isFailed);
  }

  public enum State {
    QUEUED(LatencyAttribution.State.QUEUED),
    PROCESSING(LatencyAttribution.State.ACTIVE),
    READING(LatencyAttribution.State.READING),
    COMMIT_QUEUED(LatencyAttribution.State.COMMITTING),
    COMMITTING(LatencyAttribution.State.COMMITTING),
    GET_WORK_IN_WINDMILL_WORKER(LatencyAttribution.State.GET_WORK_IN_WINDMILL_WORKER),
    GET_WORK_IN_TRANSIT_TO_DISPATCHER(LatencyAttribution.State.GET_WORK_IN_TRANSIT_TO_DISPATCHER),
    GET_WORK_IN_TRANSIT_TO_USER_WORKER(LatencyAttribution.State.GET_WORK_IN_TRANSIT_TO_USER_WORKER);

    private final LatencyAttribution.State latencyAttributionState;

    State(LatencyAttribution.State latencyAttributionState) {
      this.latencyAttributionState = latencyAttributionState;
    }

    LatencyAttribution.State toLatencyAttributionState() {
      return latencyAttributionState;
    }
  }

  /**
   * Represents the current state of an instance of {@link Work}. Contains the {@link State} and
   * {@link Instant} when it started.
   */
  @AutoValue
  abstract static class TimedState {
    private static TimedState create(State state, Instant startTime) {
      return new AutoValue_Work_TimedState(state, startTime);
    }

    private static TimedState initialState(Instant startTime) {
      return create(State.QUEUED, startTime);
    }

    private boolean isCommitPending() {
      return state() == Work.State.COMMITTING || state() == Work.State.COMMIT_QUEUED;
    }

    abstract State state();

    abstract Instant startTime();
  }

  @AutoValue
  public abstract static class ProcessingContext {

    private static ProcessingContext create(
        String computationId,
        BiFunction<String, KeyedGetDataRequest, KeyedGetDataResponse> getKeyedDataFn,
        Consumer<Commit> workCommitter,
        HeartbeatSender heartbeatSender) {
      return new AutoValue_Work_ProcessingContext.Builder()
          .setComputationId(computationId)
          .setHeartbeatSender(heartbeatSender)
          .setWorkCommitter(workCommitter)
          .setKeyedDataFetcher(
              request -> Optional.ofNullable(getKeyedDataFn.apply(computationId, request)))
          .build();
    }

    /** Computation that the {@link Work} belongs to. */
    public abstract String computationId();

    /** Handles GetData requests to streaming backend. */
    public abstract Function<KeyedGetDataRequest, Optional<KeyedGetDataResponse>>
        keyedDataFetcher();

    /**
     * {@link WorkCommitter} that commits completed work to the backend Windmill worker handling the
     * {@link WorkItem}.
     */
    public abstract Consumer<Commit> workCommitter();

    public abstract HeartbeatSender heartbeatSender();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setComputationId(String value);

      abstract Builder setKeyedDataFetcher(
          Function<KeyedGetDataRequest, Optional<KeyedGetDataResponse>> value);

      abstract Builder setWorkCommitter(Consumer<Commit> value);

      abstract Builder setHeartbeatSender(HeartbeatSender value);

      abstract ProcessingContext build();
    }
  }
}
