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
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.worker.ActiveMessageMetadata;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
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
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Represents the state of an attempt to process a {@link WorkItem} by executing user code.
 *
 * @implNote Not thread safe, should not be modified by more than 1 thread at a time.
 */
@NotThreadSafe
@Internal
public final class Work implements RefreshableWork {
  private final ShardedKey shardedKey;
  private final WorkItem workItem;
  private final ProcessingContext processingContext;
  private final Watermarks watermarks;
  private final Supplier<Instant> clock;
  private final Instant startTime;
  private final Map<LatencyAttribution.State, Duration> totalDurationPerState;
  private final WorkId id;
  private final String latencyTrackingId;
  private volatile TimedState currentState;
  private volatile boolean isFailed;
  private volatile String processingThreadName = "";

  private Work(
      WorkItem workItem,
      Watermarks watermarks,
      ProcessingContext processingContext,
      Supplier<Instant> clock) {
    this.shardedKey = ShardedKey.create(workItem.getKey(), workItem.getShardingKey());
    this.workItem = workItem;
    this.processingContext = processingContext;
    this.watermarks = watermarks;
    this.clock = clock;
    this.startTime = clock.get();
    this.totalDurationPerState = new EnumMap<>(LatencyAttribution.State.class);
    this.id = WorkId.of(workItem);
    this.latencyTrackingId =
        Long.toHexString(workItem.getShardingKey())
            + '-'
            + Long.toHexString(workItem.getWorkToken());
    this.currentState = TimedState.initialState(startTime);
    this.isFailed = false;
  }

  public static Work create(
      WorkItem workItem,
      Watermarks watermarks,
      ProcessingContext processingContext,
      Supplier<Instant> clock,
      Collection<LatencyAttribution> getWorkStreamLatencies) {
    Work work = new Work(workItem, watermarks, processingContext, clock);
    work.recordGetWorkStreamLatencies(getWorkStreamLatencies);
    return work;
  }

  public static ProcessingContext createProcessingContext(
      String computationId,
      GetDataClient getDataClient,
      Consumer<Commit> workCommitter,
      HeartbeatSender heartbeatSender) {
    return ProcessingContext.create(
        computationId, getDataClient, workCommitter, heartbeatSender, /* backendWorkerToken= */ "");
  }

  public static ProcessingContext createProcessingContext(
      String computationId,
      GetDataClient getDataClient,
      Consumer<Commit> workCommitter,
      HeartbeatSender heartbeatSender,
      String backendWorkerToken) {
    return ProcessingContext.create(
        computationId, getDataClient, workCommitter, heartbeatSender, backendWorkerToken);
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

  public WorkItem getWorkItem() {
    return workItem;
  }

  @Override
  public ShardedKey getShardedKey() {
    return shardedKey;
  }

  public Optional<KeyedGetDataResponse> fetchKeyedState(KeyedGetDataRequest keyedGetDataRequest) {
    return processingContext.fetchKeyedState(keyedGetDataRequest);
  }

  public GlobalData fetchSideInput(GlobalDataRequest request) {
    return processingContext.getDataClient().getSideInputData(request);
  }

  public String backendWorkerToken() {
    return processingContext.backendWorkerToken();
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

  public String getProcessingThreadName() {
    return processingThreadName;
  }

  public void setProcessingThreadName(String processingThreadName) {
    this.processingThreadName = processingThreadName;
  }

  @Override
  public void setFailed() {
    this.isFailed = true;
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

  @Override
  public HeartbeatSender heartbeatSender() {
    return processingContext.heartbeatSender();
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
  public ImmutableList<LatencyAttribution> getHeartbeatLatencyAttributions(
      DataflowExecutionStateSampler sampler) {
    return getLatencyAttributions(/* isHeartbeat= */ true, sampler);
  }

  public ImmutableList<LatencyAttribution> getLatencyAttributions(
      DataflowExecutionStateSampler sampler) {
    return getLatencyAttributions(/* isHeartbeat= */ false, sampler);
  }

  private Duration getTotalDurationAtLatencyAttributionState(LatencyAttribution.State state) {
    Duration duration = totalDurationPerState.getOrDefault(state, Duration.ZERO);
    return state == this.currentState.state().toLatencyAttributionState()
        ? duration.plus(new Duration(this.currentState.startTime(), clock.get()))
        : duration;
  }

  private ImmutableList<LatencyAttribution> getLatencyAttributions(
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
        GetDataClient getDataClient,
        Consumer<Commit> workCommitter,
        HeartbeatSender heartbeatSender,
        String backendWorkerToken) {
      return new AutoValue_Work_ProcessingContext(
          computationId, getDataClient, heartbeatSender, workCommitter, backendWorkerToken);
    }

    /** Computation that the {@link Work} belongs to. */
    public abstract String computationId();

    /** Handles GetData requests to streaming backend. */
    public abstract GetDataClient getDataClient();

    public abstract HeartbeatSender heartbeatSender();

    /**
     * {@link WorkCommitter} that commits completed work to the backend Windmill worker handling the
     * {@link WorkItem}.
     */
    public abstract Consumer<Commit> workCommitter();

    public abstract String backendWorkerToken();

    private Optional<KeyedGetDataResponse> fetchKeyedState(KeyedGetDataRequest request) {
      return Optional.ofNullable(getDataClient().getStateData(computationId(), request));
    }
  }
}
