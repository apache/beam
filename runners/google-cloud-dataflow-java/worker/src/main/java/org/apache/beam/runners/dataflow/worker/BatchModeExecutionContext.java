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

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.ProfileScope;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WeightedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** {@link DataflowExecutionContext} for use in batch mode. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BatchModeExecutionContext
    extends DataflowExecutionContext<BatchModeExecutionContext.StepContext> {

  protected final Cache<?, WeightedValue<?>> dataCache;
  protected final Cache<?, ?> logicalReferenceCache;
  protected final PipelineOptions options;
  protected final ReaderFactory readerFactory;
  private Object key;

  private final MetricsContainerRegistry<MetricsContainerImpl> containerRegistry;
  protected static final String DATASTORE_THROTTLE_TIME_NAMESPACE =
      "org.apache.beam.sdk.io.gcp.datastore.DatastoreV1$DatastoreWriterFn";
  protected static final String HTTP_CLIENT_API_THROTTLE_TIME_NAMESPACE =
      "org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer$LoggingHttpBackOffHandler";
  protected static final String BIGQUERY_STREAMING_INSERT_THROTTLE_TIME_NAMESPACE =
      "org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl$DatasetServiceImpl";
  protected static final String BIGQUERY_READ_THROTTLE_TIME_NAMESPACE =
      "org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl$StorageClientImpl";

  private BatchModeExecutionContext(
      CounterFactory counterFactory,
      Cache<?, WeightedValue<?>> dataCache,
      Cache<?, ?> logicalReferenceCache,
      ReaderFactory readerFactory,
      PipelineOptions options,
      DataflowExecutionStateTracker executionStateTracker,
      DataflowExecutionStateRegistry executionStateRegistry) {
    super(
        counterFactory,
        createMetricsContainerRegistry(),
        executionStateTracker,
        executionStateRegistry,
        Long.MAX_VALUE);
    this.logicalReferenceCache = logicalReferenceCache;
    this.readerFactory = readerFactory;
    this.options = options;
    this.dataCache = dataCache;
    this.containerRegistry =
        (MetricsContainerRegistry<MetricsContainerImpl>) getMetricsContainerRegistry();
  }

  private static MetricsContainerRegistry<MetricsContainerImpl> createMetricsContainerRegistry() {
    return new MetricsContainerRegistry<MetricsContainerImpl>() {
      @Override
      protected MetricsContainerImpl createContainer(String stepName) {
        return new MetricsContainerImpl(stepName);
      }
    };
  }

  public static BatchModeExecutionContext forTesting(
      PipelineOptions options, CounterFactory counterFactory, String stageName) {
    BatchModeExecutionStateRegistry stateRegistry = new BatchModeExecutionStateRegistry();
    return new BatchModeExecutionContext(
        counterFactory,
        CacheBuilder.newBuilder()
            .maximumWeight(1_000_000) // weights are in bytes
            .weigher(Weighers.fixedWeightKeys(8))
            .softValues()
            .recordStats()
            .<Object, WeightedValue<?>>build(),
        CacheBuilder.newBuilder().weakValues().build(),
        ReaderRegistry.defaultRegistry(),
        options,
        new DataflowExecutionStateTracker(
            ExecutionStateSampler.newForTest(),
            stateRegistry.getState(
                NameContext.forStage(stageName),
                "other",
                null,
                ScopedProfiler.INSTANCE.emptyScope()),
            counterFactory,
            options,
            "test-work-item-id"),
        stateRegistry);
  }

  public static BatchModeExecutionContext forTesting(PipelineOptions options, String stageName) {
    CounterFactory counterFactory = new CounterFactory();
    return forTesting(options, counterFactory, stageName);
  }

  /**
   * A version of {@link DataflowOperationContext.DataflowExecutionState} supporting per-bundle MSEC
   * counters.
   */
  @VisibleForTesting
  static class BatchModeExecutionState extends DataflowOperationContext.DataflowExecutionState {

    // Must be volatile so that totalMillisInState modifications are atomic. This field is only
    // written by the sampler thread in takeSample, and read in a separate thread within
    // extractUpdate.
    private volatile long totalMillisInState = 0;

    // This thread is only read and written within extractUpdate, which will be called from either
    // the progress reporting thread or the execution thread when producing the final update.
    // The value is not used as part of producing the final update, so the fact this is used in
    // multiple threads does not lead to incorrect results.
    private long lastReportedMillis = 0;

    public BatchModeExecutionState(
        NameContext nameContext,
        String stateName,
        @Nullable String requestingStepName,
        @Nullable Integer inputIndex,
        @Nullable MetricsContainer metricsContainer,
        ProfileScope profileScope) {
      super(nameContext, stateName, requestingStepName, inputIndex, metricsContainer, profileScope);
    }

    /**
     * Take sample is only called by the ExecutionStateSampler thread and it is the only place that
     * writes to totalMillisInState. Thus, we don't need to synchronize these writes, but we just
     * need to make sure that the value seen by readers (which are in the reporting thread) is
     * consistent. This is ensured since totalMillisInState is volatile, and thus is atomic.
     */
    @SuppressWarnings("NonAtomicVolatileUpdate") // Single writer.
    @Override
    public void takeSample(long millisSinceLastSample) {
      totalMillisInState += millisSinceLastSample;
    }

    /**
     * Extract updates in the from of a {@link CounterUpdate}.
     *
     * <p>Non-final updates are extracted by the progress reporting thread (single reader, different
     * from the sampler thread calling takeSample).
     *
     * <p>Final updates are extracted by the execution thread, and will be reported after all
     * processing has completed and the writer thread has been shutdown.
     */
    @Override
    public @Nullable CounterUpdate extractUpdate(boolean isFinalUpdate) {
      long millisToReport = totalMillisInState;
      if (millisToReport == lastReportedMillis && !isFinalUpdate) {
        return null;
      }

      lastReportedMillis = millisToReport;
      return createUpdate(true, totalMillisInState);
    }
  }

  /**
   * {@link DataflowExecutionStateRegistry} that creates {@link BatchModeExecutionState} instances.
   */
  @VisibleForTesting
  public static class BatchModeExecutionStateRegistry extends DataflowExecutionStateRegistry {

    @Override
    protected DataflowOperationContext.DataflowExecutionState createState(
        NameContext nameContext,
        String stateName,
        @Nullable String requestingStepName,
        @Nullable Integer inputIndex,
        MetricsContainer container,
        ProfileScope profileScope) {
      return new BatchModeExecutionState(
          nameContext, stateName, requestingStepName, inputIndex, container, profileScope);
    }
  }

  public static BatchModeExecutionContext create(
      CounterFactory counterFactory,
      Cache<?, WeightedValue<?>> dataCache,
      Cache<?, ?> logicalReferenceCache,
      ReaderFactory readerFactory,
      DataflowPipelineOptions options,
      String stageName,
      String workItemId) {
    BatchModeExecutionStateRegistry executionStateRegistry = new BatchModeExecutionStateRegistry();
    return new BatchModeExecutionContext(
        counterFactory,
        dataCache,
        logicalReferenceCache,
        readerFactory,
        options,
        new DataflowExecutionStateTracker(
            ExecutionStateSampler.instance(),
            executionStateRegistry.getState(
                NameContext.forStage(stageName),
                "other",
                null,
                ScopedProfiler.INSTANCE.emptyScope()),
            counterFactory,
            options,
            workItemId),
        executionStateRegistry);
  }

  /** Create a new {@link StepContext}. */
  @Override
  protected StepContext createStepContext(DataflowOperationContext operationContext) {
    return new StepContext(operationContext);
  }

  /** Sets the key of the work currently being processed. */
  public void setKey(Object key) {
    if (!Objects.equals(key, this.key)) {
      switchStateKey(key);
    }

    this.key = key;
  }

  /** @param newKey the key being switched to */
  protected void switchStateKey(Object newKey) {
    for (StepContext stepContext : getAllStepContexts()) {
      stepContext.setKey(newKey);
    }
  }

  /**
   * Returns the key of the work currently being processed.
   *
   * <p>If there is not a currently defined key, returns null.
   */
  public @Nullable Object getKey() {
    return key;
  }

  @Override
  protected SideInputReader getSideInputReader(
      Iterable<? extends SideInputInfo> sideInputInfos, DataflowOperationContext operationContext)
      throws Exception {
    return new LazilyInitializedSideInputReader(
        sideInputInfos,
        () -> {
          try {
            return IsmSideInputReader.of(
                sideInputInfos,
                options,
                BatchModeExecutionContext.this,
                readerFactory,
                operationContext);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  protected SideInputReader getSideInputReaderForViews(
      Iterable<? extends PCollectionView<?>> sideInputViews) {
    throw new UnsupportedOperationException(
        "Cannot call getSideInputReaderForViews for batch DataflowWorker: "
            + "the MapTask specification should have had SideInputInfo descriptors "
            + "for each side input, and a SideInputReader provided via getSideInputReader");
  }

  // TODO: Expose a keyed sub-cache which allows one to store all cached values in their
  // own namespace.
  public <K, V> Cache<K, V> getDataCache() {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Cache<K, V> rval = (Cache) dataCache;
    return rval;
  }

  public <K, V> Cache<K, V> getLogicalReferenceCache() {
    @SuppressWarnings({
      "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
      "unchecked"
    })
    Cache<K, V> rval = (Cache) logicalReferenceCache;
    return rval;
  }

  /** {@link DataflowStepContext} used in batch mode. */
  public class StepContext extends DataflowExecutionContext.DataflowStepContext {

    // State internals only for use by the system, lazily instantiated
    private @Nullable InMemoryStateInternals<Object> systemStateInternals;

    // State internals scoped to the user, lazily instantiated
    private @Nullable InMemoryStateInternals<Object> userStateInternals;

    // Timer internals scoped to the user, lazily instantiated
    private @Nullable InMemoryTimerInternals userTimerInternals;

    // Timer internals added for OnWindowExpiration functionality
    private @Nullable InMemoryTimerInternals systemTimerInternals;

    private InMemoryStateInternals<Object> getUserStateInternals() {
      if (userStateInternals == null) {
        userStateInternals = InMemoryStateInternals.forKey(getKey());
      }
      return userStateInternals;
    }

    private InMemoryTimerInternals getUserTimerInternals() {
      if (userTimerInternals == null) {
        userTimerInternals = new InMemoryTimerInternals();
      }
      return userTimerInternals;
    }

    private StepContext(DataflowOperationContext operationContext) {
      super(operationContext.nameContext());
      systemStateInternals = null;
      userStateInternals = null;
      userTimerInternals = null;
      systemTimerInternals = null;
    }

    public void setKey(Object newKey) {
      // When the key changes, wipe out existing state and timers for later
      // lazy instantiation.

      // In batch mode, a specific key is always processed contiguously
      // because the state is either used after a GroupByKeyOnly where
      // each key only occurs once, or after some ParDo's that preserved
      // the key.
      systemStateInternals = null;

      userStateInternals = null;
      userTimerInternals = null;
      systemTimerInternals = null;
    }

    @Override
    public StateInternals stateInternals() {
      if (systemStateInternals == null) {
        systemStateInternals = InMemoryStateInternals.forKey(getKey());
      }
      return systemStateInternals;
    }

    @Override
    public InMemoryTimerInternals timerInternals() {
      if (systemTimerInternals == null) {
        systemTimerInternals = new InMemoryTimerInternals();
      }
      return systemTimerInternals;
    }

    @Override
    public @Nullable <W extends BoundedWindow> TimerData getNextFiredTimer(Coder<W> windowCoder) {
      try {
        timerInternals().advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);
      } catch (Exception e) {
        throw new IllegalStateException("Exception thrown advancing watermark", e);
      }

      return timerInternals().removeNextEventTimer();
    }

    @Override
    public <W extends BoundedWindow> void setStateCleanupTimer(
        String timerId,
        W window,
        Coder<W> windowCoder,
        Instant cleanupTime,
        Instant cleanupOutputTimestamp) {
      timerInternals()
          .setTimer(
              StateNamespaces.window(windowCoder, window),
              timerId,
              "",
              cleanupTime,
              cleanupOutputTimestamp,
              TimeDomain.EVENT_TIME);
    }

    @Override
    public DataflowStepContext namespacedToUser() {
      return new UserStepContext(this);
    }
  }

  /**
   * A specialized {@link StepContext} that uses provided {@link StateInternals} and {@link
   * TimerInternals} for user state and timers.
   */
  private static class UserStepContext extends DataflowStepContext {

    private final BatchModeExecutionContext.StepContext wrapped;

    public UserStepContext(BatchModeExecutionContext.StepContext wrapped) {
      super(wrapped.getNameContext());
      this.wrapped = wrapped;
    }

    @Override
    public StateInternals stateInternals() {
      return wrapped.getUserStateInternals();
    }

    @Override
    public TimerInternals timerInternals() {
      return wrapped.getUserTimerInternals();
    }

    @Override
    public <W extends BoundedWindow> @Nullable TimerData getNextFiredTimer(Coder<W> windowCoder) {
      // Only event time timers fire, as processing time timers are reserved until after the
      // bundle is complete, so they are all delivered droppably late
      //
      // Note also that timers hold the _output_ watermark, but the input watermark is
      // advanced to infinity when timers begin to process
      try {
        wrapped.getUserTimerInternals().advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);
      } catch (Exception e) {
        throw new IllegalStateException("Exception thrown advancing watermark", e);
      }

      return wrapped.getUserTimerInternals().removeNextEventTimer();
    }

    @Override
    public <W extends BoundedWindow> void setStateCleanupTimer(
        String timerId,
        W window,
        Coder<W> windowCoder,
        Instant cleanupTime,
        Instant cleanupOutputTimestamp) {
      throw new UnsupportedOperationException(
          String.format(
              "setStateCleanupTimer should not be called on %s, only on a system %s",
              getClass().getSimpleName(),
              BatchModeExecutionContext.StepContext.class.getSimpleName()));
    }

    @Override
    public DataflowStepContext namespacedToUser() {
      return this;
    }
  }

  /**
   * Returns {@link CounterUpdate} protos representing the latest cumulative values of all
   * user-defined metrics reported within this execution context.
   */
  public Iterable<CounterUpdate> extractMetricUpdates(boolean isFinalUpdate) {
    return containerRegistry
        .getContainers()
        .transformAndConcat(
            container -> {
              MetricUpdates updates;
              if (isFinalUpdate) {
                // getCumulative returns cumulative values for all metrics.
                updates = container.getCumulative();
              } else {
                // getUpdates returns cumulative values only for metrics which have changes
                // since the last time updates were committed.
                updates = container.getUpdates();
              }
              return Iterables.concat(
                  FluentIterable.from(updates.counterUpdates())
                      .transform(
                          update ->
                              MetricsToCounterUpdateConverter.fromCounter(
                                  update.getKey(), true, update.getUpdate())),
                  FluentIterable.from(updates.distributionUpdates())
                      .transform(
                          update ->
                              MetricsToCounterUpdateConverter.fromDistribution(
                                  update.getKey(), true, update.getUpdate())),
                  FluentIterable.from(updates.stringSetUpdates())
                      .transform(
                          update ->
                              MetricsToCounterUpdateConverter.fromStringSet(
                                  update.getKey(), update.getUpdate())));
            });
  }

  public void commitMetricUpdates() {
    for (MetricsContainerImpl container : containerRegistry.getContainers()) {
      container.commitUpdates();
    }
  }

  public Iterable<CounterUpdate> extractMsecCounters(boolean isFinalUpdate) {
    return executionStateRegistry.extractUpdates(isFinalUpdate);
  }

  public Long extractThrottleTime() {
    long totalThrottleMsecs = 0L;
    for (MetricsContainerImpl container : containerRegistry.getContainers()) {
      CounterCell userThrottlingTime =
          container.tryGetCounter(
              MetricName.named(
                  Metrics.THROTTLE_TIME_NAMESPACE, Metrics.THROTTLE_TIME_COUNTER_NAME));
      if (userThrottlingTime != null) {
        totalThrottleMsecs += userThrottlingTime.getCumulative();
      }

      CounterCell dataStoreThrottlingTime =
          container.tryGetCounter(
              MetricName.named(
                  DATASTORE_THROTTLE_TIME_NAMESPACE, Metrics.THROTTLE_TIME_COUNTER_NAME));
      if (dataStoreThrottlingTime != null) {
        totalThrottleMsecs += dataStoreThrottlingTime.getCumulative();
      }

      CounterCell httpClientApiThrottlingTime =
          container.tryGetCounter(
              MetricName.named(
                  HTTP_CLIENT_API_THROTTLE_TIME_NAMESPACE, Metrics.THROTTLE_TIME_COUNTER_NAME));
      if (httpClientApiThrottlingTime != null) {
        totalThrottleMsecs += httpClientApiThrottlingTime.getCumulative();
      }

      CounterCell bigqueryStreamingInsertThrottleTime =
          container.tryGetCounter(
              MetricName.named(
                  BIGQUERY_STREAMING_INSERT_THROTTLE_TIME_NAMESPACE,
                  Metrics.THROTTLE_TIME_COUNTER_NAME));
      if (bigqueryStreamingInsertThrottleTime != null) {
        totalThrottleMsecs += bigqueryStreamingInsertThrottleTime.getCumulative();
      }

      CounterCell bigqueryReadThrottleTime =
          container.tryGetCounter(
              MetricName.named(
                  BIGQUERY_READ_THROTTLE_TIME_NAMESPACE, Metrics.THROTTLE_TIME_COUNTER_NAME));
      if (bigqueryReadThrottleTime != null) {
        totalThrottleMsecs += bigqueryReadThrottleTime.getCumulative();
      }

      CounterCell throttlingMsecs =
          container.tryGetCounter(DataflowSystemMetrics.THROTTLING_MSECS_METRIC_NAME);
      if (throttlingMsecs != null) {
        totalThrottleMsecs += throttlingMsecs.getCumulative();
      }
    }
    return TimeUnit.MILLISECONDS.toSeconds(totalThrottleMsecs);
  }
}
