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

import com.google.api.client.util.Clock;
import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import java.io.Closeable;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingHandler;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.ProfileScope;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OperationContext} that manages the current {@link ExecutionState} to ensure the
 * start/process/finish/abort states are properly tracked.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
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
  private final DataflowExecutionStateRegistry executionStateRegistry;

  DataflowOperationContext(
      CounterFactory counterFactory,
      NameContext nameContext,
      MetricsContainer metricsContainer,
      ExecutionStateTracker executionStateTracker,
      DataflowExecutionStateRegistry executionStateRegistry) {
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
      DataflowExecutionStateRegistry executionStateRegistry,
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
    private final @Nullable String requestingStepName;

    /**
     * For states that represent consumption of IO, this represents the index of the PCollection
     * that is associated to the IO performed (e.g. for side input reading, this is the index of the
     * side input).
     */
    private final @Nullable Integer inputIndex;

    private final NameContext stepName;

    private final ProfileScope profileScope;
    private final @Nullable MetricsContainer metricsContainer;

    /** Clock used to either provide real system time or mocked to virtualize time for testing. */
    private final Clock clock;

    public DataflowExecutionState(
        NameContext nameContext,
        String stateName,
        @Nullable String requestingStepName,
        @Nullable Integer inputIndex,
        @Nullable MetricsContainer metricsContainer,
        ProfileScope profileScope) {
      this(
          nameContext,
          stateName,
          requestingStepName,
          inputIndex,
          metricsContainer,
          profileScope,
          Clock.SYSTEM);
    }

    public DataflowExecutionState(
        NameContext nameContext,
        String stateName,
        @Nullable String requestingStepName,
        @Nullable Integer inputIndex,
        @Nullable MetricsContainer metricsContainer,
        ProfileScope profileScope,
        Clock clock) {
      super(stateName);
      this.stepName = nameContext;
      this.requestingStepName = requestingStepName;
      this.inputIndex = inputIndex;
      this.profileScope = Preconditions.checkNotNull(profileScope);
      this.metricsContainer = metricsContainer;
      this.clock = clock;
    }

    /**
     * Returns the {@link NameContext} identifying the executing step associated with this state.
     */
    public NameContext getStepName() {
      return stepName;
    }

    @Override
    public void onActivate(boolean unusedPushing) {
      profileScope.activate();
    }

    public ProfileScope getProfileScope() {
      return profileScope;
    }

    @Override
    public String getDescription() {
      StringBuilder description = new StringBuilder();
      description.append(getStepName().stageName());
      description.append("-");
      if (getStepName().originalName() != null) {
        description.append(getStepName().originalName());
        description.append("-");
      }
      description.append(getStateName());
      return description.toString();
    }

    private static final ImmutableSet<String> FRAMEWORK_CLASSES =
        ImmutableSet.of(SimpleDoFnRunner.class.getName(), DoFnInstanceManagers.class.getName());

    protected String getLullMessage(Thread trackedThread, Duration lullDuration) {
      StringBuilder message = new StringBuilder();
      message.append("Operation ongoing");
      if (getStepName() != null) {
        message.append(" in step ").append(getStepName().userName());
      }
      message
          .append(" for at least ")
          .append(formatDuration(lullDuration))
          .append(" without outputting or completing in state ")
          .append(getStateName())
          .append(" in thread ")
          .append(trackedThread.getName())
          .append(" with id ")
          .append(trackedThread.getId());

      message.append("\n");

      message.append(getStackTraceForLullMessage(trackedThread.getStackTrace()));
      return message.toString();
    }

    @Override
    public void reportLull(Thread trackedThread, long millis) {
      // If we're not logging warnings, nothing to report.
      if (!LOG.isWarnEnabled()) {
        return;
      }

      Duration lullDuration = Duration.millis(millis);

      // Since the lull reporting executes in the sampler thread, it won't automatically inherit the
      // context of the current step. To ensure things are logged correctly, we get the currently
      // registered DataflowWorkerLoggingHandler and log directly in the desired context.
      LogRecord logRecord =
          new LogRecord(Level.WARNING, getLullMessage(trackedThread, lullDuration));
      logRecord.setLoggerName(DataflowOperationContext.LOG.getName());

      // Publish directly in the context of this specific ExecutionState.
      DataflowWorkerLoggingHandler dataflowLoggingHandler =
          DataflowWorkerLoggingInitializer.getLoggingHandler();
      dataflowLoggingHandler.publish(this, logRecord);

      if (shouldLogFullThreadDump(lullDuration)) {
        Map<Thread, StackTraceElement[]> threadSet = Thread.getAllStackTraces();
        for (Map.Entry<Thread, StackTraceElement[]> entry : threadSet.entrySet()) {
          Thread thread = entry.getKey();
          StackTraceElement[] stackTrace = entry.getValue();
          StringBuilder message = new StringBuilder();
          message.append(thread.toString()).append(":\n");
          message.append(getStackTraceForLullMessage(stackTrace));
          logRecord = new LogRecord(Level.INFO, message.toString());
          logRecord.setLoggerName(DataflowOperationContext.LOG.getName());
          dataflowLoggingHandler.publish(this, logRecord);
        }
      }
    }

    /**
     * The time interval between two full thread dump. (A full thread dump is performed at most once
     * every 20 minutes.)
     */
    private static final long LOG_LULL_FULL_THREAD_DUMP_INTERVAL_MS = 20 * 60 * 1000;

    /** The minimum lull duration to perform a full thread dump. */
    private static final long LOG_LULL_FULL_THREAD_DUMP_LULL_MS = 20 * 60 * 1000;

    /** Last time when a full thread dump was performed. */
    private long lastFullThreadDumpMillis = 0;

    private boolean shouldLogFullThreadDump(Duration lullDuration) {
      if (lullDuration.getMillis() < LOG_LULL_FULL_THREAD_DUMP_LULL_MS) {
        return false;
      }
      long now = clock.currentTimeMillis();
      if (lastFullThreadDumpMillis + LOG_LULL_FULL_THREAD_DUMP_INTERVAL_MS < now) {
        lastFullThreadDumpMillis = now;
        return true;
      }
      return false;
    }

    private String getStackTraceForLullMessage(StackTraceElement[] stackTrace) {
      StringBuilder message = new StringBuilder();
      for (StackTraceElement e : stackTrace) {
        if (FRAMEWORK_CLASSES.contains(e.getClassName())) {
          break;
        }
        message.append("  at ").append(e).append("\n");
      }
      return message.toString();
    }

    public @Nullable MetricsContainer getMetricsContainer() {
      return metricsContainer;
    }

    public abstract @Nullable CounterUpdate extractUpdate(boolean isFinalUpdate);

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
                  .setMetadata(new CounterMetadata().setKind(Kind.SUM.toString())))
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
