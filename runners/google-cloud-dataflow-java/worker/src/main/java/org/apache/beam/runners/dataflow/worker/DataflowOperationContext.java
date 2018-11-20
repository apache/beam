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

import static org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor.longToSplitInt;

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.Closeable;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.ProfileScope;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OperationContext} that manages the current {@link ExecutionState} to ensure the
 * start/process/finish/abort states are properly tracked.
 */
public class DataflowOperationContext implements OperationContext {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowOperationContext.class);

  private final CounterFactory counterFactory;

  private final NameContext nameContext;
  private final ProfileScope profileScope;
  private final ExecutionState startState;
  private final ExecutionState processElementState;
  private final ExecutionState processTimersState;
  private final ExecutionState finishState;
  private final ExecutionState abortState;
  private final MetricsContainer metricsContainer;
  private final ExecutionStateTracker executionStateTracker;
  private final ExecutionStateRegistry executionStateRegistry;

  DataflowOperationContext(
      CounterFactory counterFactory,
      NameContext nameContext,
      MetricsContainer metricsContainer,
      ExecutionStateTracker executionStateTracker,
      ExecutionStateRegistry executionStateRegistry) {
    this(
        counterFactory,
        nameContext,
        metricsContainer,
        executionStateTracker,
        executionStateRegistry,
        ScopedProfiler.INSTANCE);
  }

  @VisibleForTesting
  DataflowOperationContext(
      CounterFactory counterFactory,
      NameContext nameContext,
      MetricsContainer metricsContainer,
      ExecutionStateTracker executionStateTracker,
      ExecutionStateRegistry executionStateRegistry,
      ScopedProfiler scopedProfiler) {
    this.counterFactory = counterFactory;
    this.nameContext = nameContext;
    this.metricsContainer = metricsContainer;
    this.executionStateTracker = executionStateTracker;
    this.executionStateRegistry = executionStateRegistry;

    profileScope = scopedProfiler.registerScope(nameContext.originalName());
    startState = newExecutionState(ExecutionStateTracker.START_STATE_NAME);
    processElementState = newExecutionState(ExecutionStateTracker.PROCESS_STATE_NAME);
    processTimersState = newExecutionState(ExecutionStateTracker.PROCESS_TIMERS_STATE_NAME);
    finishState = newExecutionState(ExecutionStateTracker.FINISH_STATE_NAME);
    abortState = newExecutionState(ExecutionStateTracker.ABORT_STATE_NAME);
  }

  public ExecutionState newExecutionState(String stateName) {
    return executionStateRegistry.getState(nameContext, stateName, metricsContainer, profileScope);
  }

  @Override
  public Closeable enterStart() {
    return enter(startState);
  }

  @Override
  public Closeable enterProcess() {
    return enter(processElementState);
  }

  @Override
  public Closeable enterProcessTimers() {
    // TODO: It could be useful to capture enough context to report per-timer
    // msec counters.
    return enter(processTimersState);
  }

  @Override
  public Closeable enterFinish() {
    return enter(finishState);
  }

  @Override
  public Closeable enterAbort() {
    return enter(abortState);
  }

  @Override
  public CounterFactory counterFactory() {
    return counterFactory;
  }

  @Override
  public NameContext nameContext() {
    return nameContext;
  }

  public MetricsContainer metricsContainer() {
    return metricsContainer;
  }

  private Closeable enter(ExecutionState state) {
    return executionStateTracker.enterState(state);
  }

  /**
   * An {@link DataflowExecutionState} represents the current state of an execution thread. It also
   * tracks the {@link MetricsContainer} and {@link ProfileScope} for this state, along with
   * Dataflow-specific data and functionality.
   */
  public abstract static class DataflowExecutionState extends ExecutionState {

    /**
     * For states that represent consumption / output of IO, this represents the step running when
     * the IO is triggered.
     */
    @Nullable private final String requestingStepName;

    /**
     * For states that represent consumption of IO, this represents the index of the PCollection
     * that is associated to the IO performed (e.g. for side input reading, this is the index of the
     * side input).
     */
    @Nullable private final Integer inputIndex;

    private final ProfileScope profileScope;
    @Nullable private final MetricsContainer metricsContainer;

    public DataflowExecutionState(
        NameContext nameContext,
        String stateName,
        @Nullable String requestingStepName,
        @Nullable Integer inputIndex,
        @Nullable MetricsContainer metricsContainer,
        ProfileScope profileScope) {
      super(nameContext, stateName);
      this.requestingStepName = requestingStepName;
      this.inputIndex = inputIndex;
      this.profileScope = Preconditions.checkNotNull(profileScope);
      this.metricsContainer = metricsContainer;
    }

    @Override
    public void onActivate(boolean unusedPushing) {
      profileScope.activate();
    }

    public ProfileScope getProfileScope() {
      return profileScope;
    }

    private static final ImmutableSet<String> FRAMEWORK_CLASSES =
        ImmutableSet.of(SimpleDoFnRunner.class.getName(), DoFnInstanceManagers.class.getName());

    private String getLullMessage(Thread trackedThread, Duration millis) {
      StringBuilder message = new StringBuilder();
      message.append("Processing stuck");
      if (getStepName() != null) {
        message.append(" in step ").append(getStepName().userName());
      }
      message
          .append(" for at least ")
          .append(formatDuration(millis))
          .append(" without outputting or completing in state ")
          .append(getStateName());
      message.append("\n");

      StackTraceElement[] fullTrace = trackedThread.getStackTrace();
      for (StackTraceElement e : fullTrace) {
        if (FRAMEWORK_CLASSES.contains(e.getClassName())) {
          break;
        }
        message.append("  at ").append(e).append("\n");
      }
      return message.toString();
    }

    @Override
    public void reportLull(Thread trackedThread, long millis) {
      // If we're not logging warnings, nothing to report.
      if (!LOG.isWarnEnabled()) {
        return;
      }

      // Since the lull reporting executes in the sampler thread, it won't automatically inherit the
      // context of the current step. To ensure things are logged correctly, we get the currently
      // registered DataflowWorkerLoggingHandler and log directly in the desired context.
      LogRecord logRecord =
          new LogRecord(Level.WARNING, getLullMessage(trackedThread, Duration.millis(millis)));
      logRecord.setLoggerName(DataflowOperationContext.LOG.getName());

      // Publish directly in the context of this specific ExecutionState.
      DataflowWorkerLoggingInitializer.getLoggingHandler().publish(this, logRecord);
    }

    @Nullable
    public MetricsContainer getMetricsContainer() {
      return metricsContainer;
    }

    @Nullable
    public abstract CounterUpdate extractUpdate(boolean isFinalUpdate);

    protected CounterUpdate createUpdate(boolean isCumulative, long value) {
      CounterStructuredName name =
          new CounterStructuredName()
              .setName(getStateName() + "-msecs")
              .setOrigin("SYSTEM")
              .setExecutionStepName(getStepName().stageName());

      if (getStepName().originalName() != null) {
        name.setOriginalStepName(getStepName().originalName());
      }
      if (requestingStepName != null) {
        name.setOriginalRequestingStepName(requestingStepName);
      }
      if (inputIndex != null && inputIndex > 0) {
        name.setInputIndex(inputIndex);
      }

      return new CounterUpdate()
          .setStructuredNameAndMetadata(
              new CounterStructuredNameAndMetadata()
                  .setName(name)
                  .setMetadata(new CounterMetadata().setKind("SUM")))
          .setCumulative(isCumulative)
          .setInteger(longToSplitInt(value));
    }
  }

  @VisibleForTesting
  static String formatDuration(Duration duration) {
    return DURATION_FORMATTER.print(duration.toPeriod());
  }

  private static final PeriodFormatter DURATION_FORMATTER =
      new PeriodFormatterBuilder()
          .appendDays()
          .appendSuffix("d")
          .minimumPrintedDigits(2)
          .appendHours()
          .appendSuffix("h")
          .printZeroAlways()
          .appendMinutes()
          .appendSuffix("m")
          .appendSeconds()
          .appendSuffix("s")
          .toFormatter();
}
