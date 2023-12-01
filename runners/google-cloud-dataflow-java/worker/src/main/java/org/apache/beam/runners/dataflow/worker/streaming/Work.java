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

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.dataflow.worker.ActiveMessageMetadata;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution.ActiveLatencyBreakdown;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution.ActiveLatencyBreakdown.ActiveElementMetadata;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution.ActiveLatencyBreakdown.Distribution;
import org.joda.time.Duration;
import org.joda.time.Instant;

@NotThreadSafe
public class Work implements Runnable {

  private final Windmill.WorkItem workItem;
  private final Supplier<Instant> clock;
  private final Instant startTime;
  private final Map<Windmill.LatencyAttribution.State, Duration> totalDurationPerState;
  private final Consumer<Work> processWorkFn;
  private TimedState currentState;

  private Work(Windmill.WorkItem workItem, Supplier<Instant> clock, Consumer<Work> processWorkFn) {
    this.workItem = workItem;
    this.clock = clock;
    this.processWorkFn = processWorkFn;
    this.startTime = clock.get();
    this.totalDurationPerState = new EnumMap<>(Windmill.LatencyAttribution.State.class);
    this.currentState = TimedState.initialState(startTime);
  }

  public static Work create(
      Windmill.WorkItem workItem,
      Supplier<Instant> clock,
      Collection<Windmill.LatencyAttribution> getWorkStreamLatencies,
      Consumer<Work> processWorkFn) {
    Work work = new Work(workItem, clock, processWorkFn);
    work.recordGetWorkStreamLatencies(getWorkStreamLatencies);
    return work;
  }

  @Override
  public void run() {
    processWorkFn.accept(this);
  }

  public Windmill.WorkItem getWorkItem() {
    return workItem;
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

  public boolean isCommitPending() {
    return currentState.isCommitPending();
  }

  public Instant getStateStartTime() {
    return currentState.startTime();
  }

  private void recordGetWorkStreamLatencies(
      Collection<Windmill.LatencyAttribution> getWorkStreamLatencies) {
    for (Windmill.LatencyAttribution latency : getWorkStreamLatencies) {
      totalDurationPerState.put(
          latency.getState(), Duration.millis(latency.getTotalDurationMillis()));
    }
  }

  public Collection<Windmill.LatencyAttribution> getLatencyAttributions(Boolean isHeartbeat,
      String workId, DataflowExecutionStateSampler sampler) {
    List<Windmill.LatencyAttribution> list = new ArrayList<>();
    for (Windmill.LatencyAttribution.State state : Windmill.LatencyAttribution.State.values()) {
      Duration duration = totalDurationPerState.getOrDefault(state, Duration.ZERO);
      if (state == this.currentState.state().toLatencyAttributionState()) {
        duration = duration.plus(new Duration(this.currentState.startTime(), clock.get()));
      }
      if (duration.equals(Duration.ZERO)) {
        continue;
      }
      LatencyAttribution.Builder laBuilder = Windmill.LatencyAttribution.newBuilder();
      if (state == LatencyAttribution.State.ACTIVE) {
        laBuilder = addActiveLatencyBreakdownToBuilder(isHeartbeat, laBuilder,
            workId, sampler);
      }
      Windmill.LatencyAttribution la = laBuilder
          .setState(state)
          .setTotalDurationMillis(duration.getMillis())
          .build();
      list.add(la);
    }
    return list;
  }

  private static LatencyAttribution.Builder addActiveLatencyBreakdownToBuilder(Boolean isHeartbeat,
      LatencyAttribution.Builder builder, String workId, DataflowExecutionStateSampler sampler) {
    if (isHeartbeat) {
      ActiveLatencyBreakdown.Builder stepBuilder = ActiveLatencyBreakdown.newBuilder();
      ActiveMessageMetadata activeMessage = sampler.getActiveMessageMetadataForWorkId(
          workId);
      if (activeMessage == null) {
        return builder;
      }
      stepBuilder.setUserStepName(activeMessage.userStepName);
      ActiveElementMetadata.Builder activeElementBuilder = ActiveElementMetadata.newBuilder();
      activeElementBuilder.setProcessingTimeMillis(
          System.currentTimeMillis() - activeMessage.startTime);
      stepBuilder.setActiveMessageMetadata(activeElementBuilder);
      builder.addActiveLatencyBreakdown(stepBuilder.build());
      return builder;
    }
    
    Map<String, IntSummaryStatistics> processingDistributions = sampler.getProcessingDistributionsForWorkId(
        workId);
    if (processingDistributions == null) {
      return builder;
    }
    for (Entry<String, IntSummaryStatistics> entry : processingDistributions.entrySet()) {
      ActiveLatencyBreakdown.Builder stepBuilder = ActiveLatencyBreakdown.newBuilder();
      stepBuilder.setUserStepName(entry.getKey());
      Distribution.Builder distributionBuilder = Distribution.newBuilder()
          .setCount(entry.getValue().getCount())
          .setMin(entry.getValue().getMin()).setMax(entry.getValue()
              .getMax()).setMean((long) entry.getValue().getAverage())
          .setSum(entry.getValue().getSum());
      stepBuilder.setProcessingTimesDistribution(distributionBuilder.build());
      builder.addActiveLatencyBreakdown(stepBuilder.build());
    }
    return builder;
  }

  boolean isStuckCommittingAt(Instant stuckCommitDeadline) {
    return currentState.state() == Work.State.COMMITTING
        && currentState.startTime().isBefore(stuckCommitDeadline);
  }

  public enum State {
    QUEUED(Windmill.LatencyAttribution.State.QUEUED),
    PROCESSING(Windmill.LatencyAttribution.State.ACTIVE),
    READING(Windmill.LatencyAttribution.State.READING),
    COMMIT_QUEUED(Windmill.LatencyAttribution.State.COMMITTING),
    COMMITTING(Windmill.LatencyAttribution.State.COMMITTING),
    GET_WORK_IN_WINDMILL_WORKER(Windmill.LatencyAttribution.State.GET_WORK_IN_WINDMILL_WORKER),
    GET_WORK_IN_TRANSIT_TO_DISPATCHER(
        Windmill.LatencyAttribution.State.GET_WORK_IN_TRANSIT_TO_DISPATCHER),
    GET_WORK_IN_TRANSIT_TO_USER_WORKER(
        Windmill.LatencyAttribution.State.GET_WORK_IN_TRANSIT_TO_USER_WORKER);

    private final Windmill.LatencyAttribution.State latencyAttributionState;

    State(Windmill.LatencyAttribution.State latencyAttributionState) {
      this.latencyAttributionState = latencyAttributionState;
    }

    Windmill.LatencyAttribution.State toLatencyAttributionState() {
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
}
