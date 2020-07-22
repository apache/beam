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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.SideInputInfo;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext.StepContext;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.ProfileScope;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataId;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link DataflowExecutionContext} for use in streaming mode. */
public class StreamingModeExecutionContext extends DataflowExecutionContext<StepContext> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingModeExecutionContext.class);

  /**
   * The current user-facing key for this execution context.
   *
   * <p>This field is set to a new key upon each call to {@link #start}, which corresponds to one
   * input bundle. For non-keyed bundles, the key will be {@code null}.
   *
   * <p>This key should not be mistaken for the sharding key of the computation, which is always
   * present.
   */
  private @Nullable Object key = null;

  private final String computationId;
  private final Map<TupleTag<?>, Map<BoundedWindow, Object>> sideInputCache;

  // Per-key cache of active Reader objects in use by this process.
  private final ImmutableMap<String, String> stateNameMap;
  private final WindmillStateCache.ForComputation stateCache;

  private Windmill.WorkItem work;
  private StateFetcher stateFetcher;
  private Windmill.WorkItemCommitRequest.Builder outputBuilder;
  private UnboundedSource.UnboundedReader<?> activeReader;
  private volatile long backlogBytes;
  private final ReaderCache readerCache;

  public StreamingModeExecutionContext(
      CounterFactory counterFactory,
      String computationId,
      ReaderCache readerCache,
      Map<String, String> stateNameMap,
      WindmillStateCache.ForComputation stateCache,
      MetricsContainerRegistry<StreamingStepMetricsContainer> metricsContainerRegistry,
      DataflowExecutionStateTracker executionStateTracker,
      StreamingModeExecutionStateRegistry executionStateRegistry,
      long sinkByteLimit) {
    super(
        counterFactory,
        metricsContainerRegistry,
        executionStateTracker,
        executionStateRegistry,
        sinkByteLimit);
    this.computationId = computationId;
    this.readerCache = readerCache;
    this.sideInputCache = new HashMap<>();
    this.stateNameMap = ImmutableMap.copyOf(stateNameMap);
    this.stateCache = stateCache;
    this.backlogBytes = UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
  }

  /**
   * Execution states in Streaming are shared between multiple map-task executors. Thus this class
   * needs to be thread safe for multiple writers. A single stage could have have multiple executors
   * running concurrently.
   */
  public static class StreamingModeExecutionState
      extends DataflowOperationContext.DataflowExecutionState {

    // AtomicLong is used because this value is written in two places:
    // 1. The sampling thread calls takeSample to increment the time spent in this state
    // 2. The reporting thread calls extractUpdate which reads the current sum *AND* sets it to 0.
    private final AtomicLong totalMillisInState = new AtomicLong();

    // The worker that created this state.  Used to report lulls back to the worker.
    private final StreamingDataflowWorker worker;

    public StreamingModeExecutionState(
        NameContext nameContext,
        String stateName,
        MetricsContainer metricsContainer,
        ProfileScope profileScope,
        StreamingDataflowWorker worker) {
      // TODO: Take in the requesting step name and side input index for streaming.
      super(nameContext, stateName, null, null, metricsContainer, profileScope);
      this.worker = worker;
    }

    /**
     * Take sample is only called by the ExecutionStateSampler thread. It is the only place that
     * increments totalMillisInState, however the reporting thread periodically calls extractUpdate
     * which will read the sum and reset it to 0, so totalMillisInState does have multiple writers.
     */
    @Override
    public void takeSample(long millisSinceLastSample) {
      totalMillisInState.addAndGet(millisSinceLastSample);
    }

    /**
     * Extract updates in the form of a {@link CounterUpdate}.
     *
     * <p>Non-final updates are extracted periodically and report the physical value as a delta.
     * This requires setting the totalMillisInState back to 0.
     *
     * <p>Final updates should never be requested from a Streaming job since the work unit never
     * completes.
     */
    @Nullable
    @Override
    public CounterUpdate extractUpdate(boolean isFinalUpdate) {
      // Streaming reports deltas, so isFinalUpdate doesn't matter, and should never be true.
      long sum = totalMillisInState.getAndSet(0);
      return sum == 0 ? null : createUpdate(false, sum);
    }
  }

  /**
   * Implementation of DataflowExecutionStateRegistry that creates Streaming versions of
   * ExecutionState.
   */
  public static class StreamingModeExecutionStateRegistry extends DataflowExecutionStateRegistry {
    private final StreamingDataflowWorker worker;

    public StreamingModeExecutionStateRegistry(StreamingDataflowWorker worker) {
      this.worker = worker;
    }

    @Override
    protected DataflowOperationContext.DataflowExecutionState createState(
        NameContext nameContext,
        String stateName,
        String requestingStepName,
        Integer inputIndex,
        MetricsContainer container,
        ProfileScope profileScope) {
      return new StreamingModeExecutionState(
          nameContext, stateName, container, profileScope, worker);
    }
  }

  @VisibleForTesting
  public long getBacklogBytes() {
    return backlogBytes;
  }

  public void start(
      @Nullable Object key,
      Windmill.WorkItem work,
      Instant inputDataWatermark,
      @Nullable Instant outputDataWatermark,
      @Nullable Instant synchronizedProcessingTime,
      WindmillStateReader stateReader,
      StateFetcher stateFetcher,
      Windmill.WorkItemCommitRequest.Builder outputBuilder) {
    this.key = key;
    this.work = work;
    this.stateFetcher = stateFetcher;
    this.outputBuilder = outputBuilder;
    this.sideInputCache.clear();
    clearSinkFullHint();

    Instant processingTime = Instant.now();
    // Ensure that the processing time is greater than any fired processing time
    // timers.  Otherwise a trigger could ignore the timer and orphan the window.
    for (Windmill.Timer timer : work.getTimers().getTimersList()) {
      if (timer.getType() == Windmill.Timer.Type.REALTIME) {
        Instant inferredFiringTime =
            WindmillTimeUtils.windmillToHarnessTimestamp(timer.getTimestamp())
                .plus(Duration.millis(1));
        if (inferredFiringTime.isAfter(processingTime)) {
          processingTime = inferredFiringTime;
        }
      }
    }

    for (StepContext stepContext : getAllStepContexts()) {
      stepContext.start(
          stateReader,
          inputDataWatermark,
          processingTime,
          outputDataWatermark,
          synchronizedProcessingTime);
    }
  }

  @Override
  public StepContext createStepContext(DataflowOperationContext operationContext) {
    return new StepContext(operationContext);
  }

  @Override
  protected SideInputReader getSideInputReader(
      Iterable<? extends SideInputInfo> sideInputInfos, DataflowOperationContext operationContext) {
    throw new UnsupportedOperationException(
        "Cannot call getSideInputReader for StreamingDataflowWorker: "
            + "the MapTask specification should not have had any SideInputInfo descriptors "
            + "since the streaming runner does not yet support them.");
  }

  @Override
  protected SideInputReader getSideInputReaderForViews(
      Iterable<? extends PCollectionView<?>> views) {
    return StreamingModeSideInputReader.of(views, this);
  }

  /**
   * Fetches the requested sideInput, and maintains a view of the cache that doesn't remove items
   * until the active work item is finished.
   *
   * <p>If the side input was not ready, throws {@code IllegalStateException} if the state is
   * {@literal CACHED_IN_WORKITEM} or returns null otherwise.
   *
   * <p>If the side input was ready and null, returns {@literal Optional.absent()}. If the side
   * input was ready and non-null returns {@literal Optional.present(...)}.
   */
  private @Nullable <T> Optional<T> fetchSideInput(
      PCollectionView<T> view,
      BoundedWindow sideInputWindow,
      String stateFamily,
      StateFetcher.SideInputState state,
      Supplier<Closeable> scopedReadStateSupplier) {
    Map<BoundedWindow, Object> tagCache = sideInputCache.get(view.getTagInternal());
    if (tagCache == null) {
      tagCache = new HashMap<>();
      sideInputCache.put(view.getTagInternal(), tagCache);
    }

    if (tagCache.containsKey(sideInputWindow)) {
      @SuppressWarnings("unchecked")
      T typed = (T) tagCache.get(sideInputWindow);
      return Optional.fromNullable(typed);
    } else {
      if (state == StateFetcher.SideInputState.CACHED_IN_WORKITEM) {
        throw new IllegalStateException(
            "Expected side input to be cached. Tag: " + view.getTagInternal().getId());
      }
      Optional<T> fetched =
          stateFetcher.fetchSideInput(
              view, sideInputWindow, stateFamily, state, scopedReadStateSupplier);
      if (fetched != null) {
        tagCache.put(sideInputWindow, fetched.orNull());
      }
      return fetched;
    }
  }

  public Iterable<Windmill.GlobalDataId> getSideInputNotifications() {
    return work.getGlobalDataIdNotificationsList();
  }

  private List<Timer> getFiredTimers() {
    return work.getTimers().getTimersList();
  }

  public @Nullable ByteString getSerializedKey() {
    return work == null ? null : work.getKey();
  }

  public long getWorkToken() {
    return work.getWorkToken();
  }

  public Windmill.WorkItem getWork() {
    return work;
  }

  public Windmill.WorkItemCommitRequest.Builder getOutputBuilder() {
    return outputBuilder;
  }

  /**
   * Returns cached reader for this key if one exists. The reader is removed from the cache. NOTE:
   * The caller is responsible for the reader and should appropriately close it as required.
   */
  public UnboundedSource.UnboundedReader<?> getCachedReader() {
    return readerCache.acquireReader(computationId, getSerializedKey(), getWork().getCacheToken());
  }

  public void setActiveReader(UnboundedSource.UnboundedReader<?> reader) {
    checkState(activeReader == null, "not expected to be overwritten");
    activeReader = reader;
  }

  /** Invalidate the state and reader caches for this computation and key. */
  public void invalidateCache() {
    ByteString key = getSerializedKey();
    if (key != null) {
      readerCache.invalidateReader(computationId, key);
      if (activeReader != null) {
        try {
          activeReader.close();
        } catch (IOException e) {
          LOG.warn("Failed to close reader for {}-{}", computationId, key.toStringUtf8(), e);
        }
      }
      activeReader = null;
      stateCache.invalidate(key);
    }
  }

  public UnboundedSource.CheckpointMark getReaderCheckpoint(
      Coder<? extends UnboundedSource.CheckpointMark> coder) {
    try {
      ByteString state = work.getSourceState().getState();
      if (state.isEmpty()) {
        return null;
      }
      return coder.decode(state.newInput(), Coder.Context.OUTER);
    } catch (IOException e) {
      throw new RuntimeException("Exception while decoding checkpoint", e);
    }
  }

  public Map<Long, Runnable> flushState() {
    Map<Long, Runnable> callbacks = new HashMap<>();

    for (StepContext stepContext : getAllStepContexts()) {
      stepContext.flushState();
    }

    if (activeReader != null) {
      Windmill.SourceState.Builder sourceStateBuilder =
          outputBuilder.getSourceStateUpdatesBuilder();
      final UnboundedSource.CheckpointMark checkpointMark = activeReader.getCheckpointMark();
      final Instant watermark = activeReader.getWatermark();
      long id = ThreadLocalRandom.current().nextLong();
      sourceStateBuilder.addFinalizeIds(id);
      callbacks.put(
          id,
          () -> {
            try {
              checkpointMark.finalizeCheckpoint();
            } catch (IOException e) {
              throw new RuntimeException("Exception while finalizing checkpoint", e);
            }
          });

      @SuppressWarnings("unchecked")
      Coder<UnboundedSource.CheckpointMark> checkpointCoder =
          ((UnboundedSource<?, UnboundedSource.CheckpointMark>) activeReader.getCurrentSource())
              .getCheckpointMarkCoder();
      if (checkpointCoder != null) {
        ByteString.Output stream = ByteString.newOutput();
        try {
          checkpointCoder.encode(checkpointMark, stream, Coder.Context.OUTER);
        } catch (IOException e) {
          throw new RuntimeException("Exception while encoding checkpoint", e);
        }
        sourceStateBuilder.setState(stream.toByteString());
      }
      outputBuilder.setSourceWatermark(WindmillTimeUtils.harnessToWindmillTimestamp(watermark));

      backlogBytes = activeReader.getSplitBacklogBytes();
      if (backlogBytes == UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN
          && WorkerCustomSources.isFirstUnboundedSourceSplit(getSerializedKey())) {
        // Only call getTotalBacklogBytes() on the first split.
        backlogBytes = activeReader.getTotalBacklogBytes();
      }
      outputBuilder.setSourceBacklogBytes(backlogBytes);

      readerCache.cacheReader(
          computationId, getSerializedKey(), getWork().getCacheToken(), activeReader);
      activeReader = null;
    }
    return callbacks;
  }

  interface StreamingModeStepContext {
    boolean issueSideInputFetch(
        PCollectionView<?> view, BoundedWindow w, StateFetcher.SideInputState s);

    void addBlockingSideInput(Windmill.GlobalDataRequest blocked);

    void addBlockingSideInputs(Iterable<Windmill.GlobalDataRequest> blocked);

    StateInternals stateInternals();

    Iterable<Windmill.GlobalDataId> getSideInputNotifications();

    /** Writes the given {@code PCollectionView} data to a globally accessible location. */
    <T, W extends BoundedWindow> void writePCollectionViewData(
        TupleTag<?> tag,
        Iterable<T> data,
        Coder<Iterable<T>> dataCoder,
        W window,
        Coder<W> windowCoder)
        throws IOException;
  }

  String getStateFamily(NameContext nameContext) {
    return nameContext.userName() == null ? null : stateNameMap.get(nameContext.userName());
  }

  class StepContext extends DataflowExecutionContext.DataflowStepContext
      implements StreamingModeStepContext {

    private WindmillStateInternals<Object> stateInternals;

    private WindmillTimerInternals systemTimerInternals;
    private WindmillTimerInternals userTimerInternals;
    private final String stateFamily;
    private final Supplier<Closeable> scopedReadStateSupplier;

    public StepContext(DataflowOperationContext operationContext) {
      super(operationContext.nameContext());
      this.stateFamily =
          StreamingModeExecutionContext.this.getStateFamily(operationContext.nameContext());

      this.scopedReadStateSupplier =
          new Supplier<Closeable>() {
            private ExecutionState readState = operationContext.newExecutionState("windmill-read");

            @Override
            public Closeable get() {
              ExecutionStateTracker tracker =
                  StreamingModeExecutionContext.this.getExecutionStateTracker();
              if (tracker == null) {
                return null;
              }
              return tracker.enterState(readState);
            }
          };
    }

    /** Update the {@code stateReader} used by this {@code StepContext}. */
    public void start(
        WindmillStateReader stateReader,
        Instant inputDataWatermark,
        Instant processingTime,
        @Nullable Instant outputDataWatermark,
        @Nullable Instant synchronizedProcessingTime) {
      this.stateInternals =
          new WindmillStateInternals<>(
              key,
              stateFamily,
              stateReader,
              work.getIsNewKey(),
              stateCache.forKey(
                  getSerializedKey(), stateFamily, getWork().getCacheToken(), getWorkToken()),
              scopedReadStateSupplier);

      this.systemTimerInternals =
          new WindmillTimerInternals(
              stateFamily,
              WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
              inputDataWatermark,
              processingTime,
              outputDataWatermark,
              synchronizedProcessingTime);

      this.userTimerInternals =
          new WindmillTimerInternals(
              stateFamily,
              WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
              inputDataWatermark,
              processingTime,
              outputDataWatermark,
              synchronizedProcessingTime);

      this.cachedFiredTimers = null;
      this.cachedFiredUserTimers = null;
    }

    public void flushState() {
      stateInternals.persist(outputBuilder);
      systemTimerInternals.persistTo(outputBuilder);
      userTimerInternals.persistTo(outputBuilder);
    }

    // Lazily initialized
    private Iterator<TimerData> cachedFiredTimers = null;

    @Override
    public <W extends BoundedWindow> TimerData getNextFiredTimer(Coder<W> windowCoder) {
      if (cachedFiredTimers == null) {
        cachedFiredTimers =
            FluentIterable.<Timer>from(StreamingModeExecutionContext.this.getFiredTimers())
                .filter(
                    timer ->
                        WindmillTimerInternals.isSystemTimer(timer)
                            && timer.getStateFamily().equals(stateFamily))
                .transform(
                    timer ->
                        WindmillTimerInternals.windmillTimerToTimerData(
                            WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX, timer, windowCoder))
                .iterator();
      }

      if (!cachedFiredTimers.hasNext()) {
        return null;
      }
      TimerData nextTimer = cachedFiredTimers.next();
      // system timers ( GC timer) must be explicitly deleted if only there is a hold.
      // if timestamp is not equals to outputTimestamp then there should be a hold
      if (!nextTimer.getTimestamp().equals(nextTimer.getOutputTimestamp())) {
        systemTimerInternals.deleteTimer(nextTimer);
      }
      return nextTimer;
    }

    // Lazily initialized
    private Iterator<TimerData> cachedFiredUserTimers = null;

    public <W extends BoundedWindow> TimerData getNextFiredUserTimer(Coder<W> windowCoder) {
      if (cachedFiredUserTimers == null) {
        cachedFiredUserTimers =
            FluentIterable.<Timer>from(StreamingModeExecutionContext.this.getFiredTimers())
                .filter(
                    timer ->
                        WindmillTimerInternals.isUserTimer(timer)
                            && timer.getStateFamily().equals(stateFamily))
                .transform(
                    timer ->
                        WindmillTimerInternals.windmillTimerToTimerData(
                            WindmillNamespacePrefix.USER_NAMESPACE_PREFIX, timer, windowCoder))
                .iterator();
      }

      if (!cachedFiredUserTimers.hasNext()) {
        return null;
      }
      TimerData nextTimer = cachedFiredUserTimers.next();
      // User timers must be explicitly deleted when delivered, to release the implied hold
      userTimerInternals.deleteTimer(nextTimer);
      return nextTimer;
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

    @Override
    public Iterable<Windmill.GlobalDataId> getSideInputNotifications() {
      return StreamingModeExecutionContext.this.getSideInputNotifications();
    }

    @Override
    public <T, W extends BoundedWindow> void writePCollectionViewData(
        TupleTag<?> tag,
        Iterable<T> data,
        Coder<Iterable<T>> dataCoder,
        W window,
        Coder<W> windowCoder)
        throws IOException {
      if (getSerializedKey().size() != 0) {
        throw new IllegalStateException("writePCollectionViewData must follow a Combine.globally");
      }

      ByteString.Output dataStream = ByteString.newOutput();
      dataCoder.encode(data, dataStream, Coder.Context.OUTER);

      ByteString.Output windowStream = ByteString.newOutput();
      windowCoder.encode(window, windowStream, Coder.Context.OUTER);

      if (stateFamily == null) {
        throw new IllegalStateException(
            "Tried to write view data for stateless step: " + getNameContext());
      }

      Windmill.GlobalData.Builder builder =
          Windmill.GlobalData.newBuilder()
              .setDataId(
                  Windmill.GlobalDataId.newBuilder()
                      .setTag(tag.getId())
                      .setVersion(windowStream.toByteString())
                      .build())
              .setData(dataStream.toByteString())
              .setStateFamily(stateFamily);

      outputBuilder.addGlobalDataUpdates(builder.build());
    }

    /** Fetch the given side input asynchronously and return true if it is present. */
    @Override
    public boolean issueSideInputFetch(
        PCollectionView<?> view, BoundedWindow mainInputWindow, StateFetcher.SideInputState state) {
      BoundedWindow sideInputWindow = view.getWindowMappingFn().getSideInputWindow(mainInputWindow);
      return fetchSideInput(view, sideInputWindow, stateFamily, state, scopedReadStateSupplier)
          != null;
    }

    /** Note that there is data on the current key that is blocked on the given side input. */
    @Override
    public void addBlockingSideInput(Windmill.GlobalDataRequest sideInput) {
      checkState(
          stateFamily != null,
          "Tried to set global data request for stateless step: " + getNameContext());
      sideInput =
          Windmill.GlobalDataRequest.newBuilder(sideInput).setStateFamily(stateFamily).build();
      outputBuilder.addGlobalDataRequests(sideInput);
      outputBuilder.addGlobalDataIdRequests(sideInput.getDataId());
    }

    /** Note that there is data on the current key that is blocked on the given side inputs. */
    @Override
    public void addBlockingSideInputs(Iterable<Windmill.GlobalDataRequest> sideInputs) {
      for (Windmill.GlobalDataRequest sideInput : sideInputs) {
        addBlockingSideInput(sideInput);
      }
    }

    @Override
    public StateInternals stateInternals() {
      checkState(
          stateFamily != null, "Tried to access state for stateless step: " + getNameContext());
      return checkNotNull(stateInternals);
    }

    @Override
    public TimerInternals timerInternals() {
      checkState(
          stateFamily != null, "Tried to access timers for stateless step: " + getNameContext());
      return checkNotNull(systemTimerInternals);
    }

    public TimerInternals userTimerInternals() {
      checkState(
          stateFamily != null,
          "Tried to access user timers for stateless step: " + getNameContext());
      return checkNotNull(userTimerInternals);
    }
  }

  /**
   * A specialized {@link StepContext} that uses provided {@link StateInternals} and {@link
   * TimerInternals} for user state and timers.
   */
  private static class UserStepContext extends DataflowStepContext
      implements StreamingModeStepContext {

    private final StreamingModeExecutionContext.StepContext wrapped;

    public UserStepContext(StreamingModeExecutionContext.StepContext wrapped) {
      super(wrapped.getNameContext());
      this.wrapped = wrapped;
    }

    @Override
    public boolean issueSideInputFetch(
        PCollectionView<?> view, BoundedWindow w, StateFetcher.SideInputState s) {
      return wrapped.issueSideInputFetch(view, w, s);
    }

    @Override
    public void addBlockingSideInput(GlobalDataRequest blocked) {
      wrapped.addBlockingSideInput(blocked);
    }

    @Override
    public void addBlockingSideInputs(Iterable<GlobalDataRequest> blocked) {
      wrapped.addBlockingSideInputs(blocked);
    }

    @Override
    public StateInternals stateInternals() {
      return wrapped.stateInternals();
    }

    @Override
    public Iterable<GlobalDataId> getSideInputNotifications() {
      return wrapped.getSideInputNotifications();
    }

    @Override
    public <T, W extends BoundedWindow> void writePCollectionViewData(
        TupleTag<?> tag,
        Iterable<T> data,
        Coder<Iterable<T>> dataCoder,
        W window,
        Coder<W> windowCoder)
        throws IOException {
      throw new IllegalStateException("User DoFns cannot write PCollectionView data");
    }

    @Override
    public TimerInternals timerInternals() {
      return wrapped.userTimerInternals();
    }

    @Override
    public <W extends BoundedWindow> TimerData getNextFiredTimer(Coder<W> windowCoder) {
      return wrapped.getNextFiredUserTimer(windowCoder);
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
              StreamingModeExecutionContext.StepContext.class.getSimpleName()));
    }

    @Override
    public DataflowStepContext namespacedToUser() {
      return this;
    }
  }

  /** A {@link SideInputReader} that fetches side inputs from the streaming worker's cache. */
  public static class StreamingModeSideInputReader implements SideInputReader {
    private StreamingModeExecutionContext context;
    private Set<PCollectionView<?>> viewSet;

    private StreamingModeSideInputReader(
        Iterable<? extends PCollectionView<?>> views, StreamingModeExecutionContext context) {
      this.context = context;
      this.viewSet = ImmutableSet.copyOf(views);
    }

    public static StreamingModeSideInputReader of(
        Iterable<? extends PCollectionView<?>> views, StreamingModeExecutionContext context) {
      return new StreamingModeSideInputReader(views, context);
    }

    @Override
    public <T> T get(PCollectionView<T> view, BoundedWindow window) {
      if (!contains(view)) {
        throw new RuntimeException("get() called with unknown view");
      }

      // We are only fetching the cached value here, so we don't need stateFamily or
      // readStateSupplier.
      return context
          .fetchSideInput(
              view,
              window,
              null /* unused stateFamily */,
              StateFetcher.SideInputState.CACHED_IN_WORKITEM,
              null /* unused readStateSupplier */)
          .orNull();
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return viewSet.contains(view);
    }

    @Override
    public boolean isEmpty() {
      return viewSet.isEmpty();
    }
  }
}
