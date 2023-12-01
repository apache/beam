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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataflow.model.SideInputInfo;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementExecutionTracker;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Closer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.joda.time.Instant;

/** Execution context for the Dataflow worker. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class DataflowExecutionContext<T extends DataflowStepContext> {

  private final CounterFactory counterFactory;
  private final MetricsContainerRegistry<?> metricsContainerRegistry;
  private final ExecutionStateTracker executionStateTracker;
  protected final DataflowExecutionStateRegistry executionStateRegistry;
  // Desired limit on amount of data sinked. Cumulative
  // across all the sinks, when there are more than one sinks.
  private final long sinkByteLimit;
  private long bytesSinked = 0;

  public DataflowExecutionContext(
      CounterFactory counterFactory,
      MetricsContainerRegistry<?> metricsRegistry,
      DataflowExecutionStateTracker executionStateTracker,
      DataflowExecutionStateRegistry executionStateRegistry,
      long sinkByteLimit) {
    this.counterFactory = counterFactory;
    this.metricsContainerRegistry = metricsRegistry;
    this.executionStateTracker = executionStateTracker;
    this.executionStateRegistry = executionStateRegistry;
    this.sinkByteLimit = sinkByteLimit;
  }

  // Created step contexts, keyed by step name
  private Map<String, T> cachedStepContexts = new LinkedHashMap<>();

  /**
   * Returns a {@link SideInputReader} based on {@link SideInputInfo} descriptors and {@link
   * PCollectionView PCollectionViews}.
   *
   * <p>If side input source metadata is provided by the service in {@link SideInputInfo
   * sideInputInfos}, we request a {@link SideInputReader} from the {@code executionContext} using
   * that info. If no side input source metadata is provided but the DoFn expects side inputs, as a
   * fallback, we request a {@link SideInputReader} based only on the expected views.
   *
   * <p>These cases are not disjoint: Whenever a {@link GroupAlsoByWindowFn} takes side inputs,
   * {@code doFnInfo.getSideInputViews()} should be non-empty.
   *
   * <p>A note on the behavior of the Dataflow service: Today, the first case corresponds to batch
   * mode, while the fallback corresponds to streaming mode.
   */
  public SideInputReader getSideInputReader(
      @Nullable Iterable<? extends SideInputInfo> sideInputInfos,
      @Nullable Iterable<? extends PCollectionView<?>> views,
      DataflowOperationContext operationContext)
      throws Exception {
    if (sideInputInfos != null && !Iterables.isEmpty(sideInputInfos)) {
      return getSideInputReader(sideInputInfos, operationContext);
    } else if (views != null && !Iterables.isEmpty(views)) {
      return getSideInputReaderForViews(views);
    } else {
      return NullSideInputReader.empty();
    }
  }

  public CounterFactory getCounterFactory() {
    return counterFactory;
  }

  /** Returns a collection view of all of the {@link StepContext}s. */
  public Collection<? extends T> getAllStepContexts() {
    return Collections.unmodifiableCollection(cachedStepContexts.values());
  }

  /**
   * A hint for currently executing step if the context prefers stopping processing more input since
   * the sinks are 'full'. This is polled by readers to stop consuming more records, when they can.
   * Currently the hint is set only by the sinks in streaming.
   */
  boolean isSinkFullHintSet() {
    return bytesSinked >= sinkByteLimit;
    // In addition to hint from the sinks, we could consider other factors likes global memory
    // pressure.
    // TODO: We should have state bytes also to contribute to this hint, otherwise,
    //               the state size might grow unbounded.
  }

  /**
   * Sets a flag to indicate that a sink has enough data written to it. This hint is read by
   * upstream producers to stop producing if they can. Mainly used in streaming.
   */
  void reportBytesSinked(long bytes) {
    bytesSinked += bytes;
  }

  protected void clearSinkFullHint() {
    // Cleared in Streaming when the context is reused for new work item.
    bytesSinked = 0;
  }

  /**
   * Returns a {@link SideInputReader} for all the side inputs described in the given {@link
   * SideInputInfo} descriptors.
   */
  protected abstract SideInputReader getSideInputReader(
      Iterable<? extends SideInputInfo> sideInputInfos, DataflowOperationContext operationContext)
      throws Exception;

  protected abstract T createStepContext(DataflowOperationContext operationContext);

  // TODO: Move StepContext creation to the OperationContext.
  public T getStepContext(DataflowOperationContext operationContext) {
    NameContext nameContext = operationContext.nameContext();
    T context = cachedStepContexts.get(nameContext.systemName());
    if (context == null) {
      context = createStepContext(operationContext);
      cachedStepContexts.put(nameContext.systemName(), context);
    }
    return context;
  }

  /**
   * Returns a {@link SideInputReader} for all the provided views, where the execution context
   * itself knows how to read data for the view.
   */
  protected abstract SideInputReader getSideInputReaderForViews(
      Iterable<? extends PCollectionView<?>> views) throws Exception;

  /** Dataflow specific {@link StepContext}. */
  public abstract static class DataflowStepContext implements StepContext {
    private final NameContext nameContext;

    public DataflowStepContext(NameContext nameContext) {
      this.nameContext = nameContext;
    }

    public NameContext getNameContext() {
      return nameContext;
    }

    /**
     * Returns the next fired timer for this step.
     *
     * <p>The {@code windowCoder} is passed here as it is more convenient than doing so when the
     * {@link DataflowStepContext} is created.
     */
    public abstract @Nullable <W extends BoundedWindow> TimerData getNextFiredTimer(
        Coder<W> windowCoder);

    public abstract <W extends BoundedWindow> void setStateCleanupTimer(
        String timerId,
        W window,
        Coder<W> windowCoder,
        Instant cleanupTime,
        Instant cleanupOutputTimestamp);

    public abstract DataflowStepContext namespacedToUser();
  }

  /**
   * Creates the context for an operation with the stage this execution context belongs to.
   *
   * @param nameContext the set of names identifying the operation
   */
  public DataflowOperationContext createOperationContext(NameContext nameContext) {
    MetricsContainer metricsContainer =
        metricsContainerRegistry.getContainer(
            checkNotNull(
                nameContext.originalName(),
                "All operations must have an original name, but %s doesn't.",
                nameContext));
    return new DataflowOperationContext(
        counterFactory,
        nameContext,
        metricsContainer,
        executionStateTracker,
        executionStateRegistry);
  }

  protected MetricsContainerRegistry<?> getMetricsContainerRegistry() {
    return metricsContainerRegistry;
  }

  protected DataflowExecutionStateRegistry getExecutionStateRegistry() {
    return executionStateRegistry;
  }

  public ExecutionStateTracker getExecutionStateTracker() {
    return executionStateTracker;
  }

  /**
   * An extension of {@link ExecutionStateTracker} that also installs a {@link MetricsContainer}
   * using the current state to locate the {@link MetricsEnvironment}.
   */
  public static class DataflowExecutionStateTracker extends ExecutionStateTracker {

    private final ElementExecutionTracker elementExecutionTracker;
    private final DataflowOperationContext.DataflowExecutionState otherState;
    private final ContextActivationObserverRegistry contextActivationObserverRegistry;
    private final String workItemId;

    /**
     * Metadata on the message whose processing is currently being managed by this tracker. If no
     * message is actively being processed, activeMessageMetadata will be null.
     */
    private ActiveMessageMetadata activeMessageMetadata = null;

    private MillisProvider clock = System::currentTimeMillis;

    private Map<String, IntSummaryStatistics> processingTimesByStep = new HashMap<>();

    public DataflowExecutionStateTracker(
        ExecutionStateSampler sampler,
        DataflowOperationContext.DataflowExecutionState otherState,
        CounterFactory counterFactory,
        PipelineOptions options,
        String workItemId) {
      super(sampler);
      this.elementExecutionTracker =
          DataflowElementExecutionTracker.create(counterFactory, options);
      this.otherState = otherState;
      this.workItemId = workItemId;
      this.contextActivationObserverRegistry = ContextActivationObserverRegistry.createDefault();
    }

    @Override
    public Closeable activate() {
      Closer closer = Closer.create();
      try {
        closer.register(super.activate());
        for (ContextActivationObserver p :
            contextActivationObserverRegistry.getContextActivationObservers()) {
          closer.register(p.activate(this));
        }
        closer.register(enterState(otherState));
        return closer;
      } catch (Exception e) {
        try {
          closer.close();
        } catch (IOException suppressed) {
          // Shouldn't happen -- none of the things being closed actually throw.
          e.addSuppressed(suppressed);
        }
        throw e;
      }
    }

    @Override
    protected void takeSampleOnce(long millisSinceLastSample) {
      elementExecutionTracker.takeSample(millisSinceLastSample);
      super.takeSampleOnce(millisSinceLastSample);
    }

    /**
     * Enter a new state on the tracker. If the new state is a Dataflow processing state, tracks the
     * activeMessageMetadata with the start time of the new state.
     */
    @Override
    public Closeable enterState(ExecutionState newState) {
      Closeable baseCloseable = super.enterState(newState);
      final boolean isDataflowProcessElementState =
          newState.isProcessElementState && newState instanceof DataflowExecutionState;
      if (isDataflowProcessElementState) {
        DataflowExecutionState newDFState = (DataflowExecutionState) newState;
        if (newDFState.getStepName() != null && newDFState.getStepName().userName() != null) {
          if (this.activeMessageMetadata != null) {
            recordActiveMessageInProcessingTimesMap();
          }
          this.activeMessageMetadata =
              new ActiveMessageMetadata(newDFState.getStepName().userName(), clock.getMillis());
        }
        elementExecutionTracker.enter(newDFState.getStepName());
      }

      return () -> {
        if (isDataflowProcessElementState) {
          if (this.activeMessageMetadata != null) {
            recordActiveMessageInProcessingTimesMap();
          }
          elementExecutionTracker.exit();
        }
        baseCloseable.close();
      };
    }

    public String getWorkItemId() {
      return this.workItemId;
    }

    public ActiveMessageMetadata getActiveMessageMetadata() {
      return activeMessageMetadata;
    }

    public Map<String, IntSummaryStatistics> getProcessingTimesByStep() {
      return processingTimesByStep;
    }

    /**
     * Transitions the metadata for the currently active message to an entry in the completed
     * processing times map. Sets the activeMessageMetadata to null after the entry has been
     * recorded.
     */
    private void recordActiveMessageInProcessingTimesMap() {
      if (this.activeMessageMetadata == null) {
        return;
      }
      this.processingTimesByStep.compute(
          this.activeMessageMetadata.userStepName,
          (k, v) -> {
            if (v == null) {
              v = new IntSummaryStatistics();
            }
            v.accept((int) (System.currentTimeMillis() - this.activeMessageMetadata.startTime));
            return v;
          });
      this.activeMessageMetadata = null;
    }
  }
}
