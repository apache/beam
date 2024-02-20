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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing.context;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataflow.model.SideInputInfo;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.MetricsContainerRegistry;
import org.apache.beam.runners.dataflow.worker.ReaderCache;
import org.apache.beam.runners.dataflow.worker.StreamingStepMetricsContainer;
import org.apache.beam.runners.dataflow.worker.WindmillComputationKey;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.WorkerCustomSources;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInput;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputState;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link DataflowExecutionContext} for use in streaming mode. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@NotThreadSafe
public class StreamingModeExecutionContext
    extends DataflowExecutionContext<DataflowStreamingModeStepContext> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingModeExecutionContext.class);
  private final String computationId;
  private final Map<TupleTag<?>, Map<BoundedWindow, SideInput<?>>> sideInputCache;
  // Per-key cache of active Reader objects in use by this process.
  private final ImmutableMap<String, String> stateNameMap;
  private final WindmillStateCache.ForComputation stateCache;
  private final ReaderCache readerCache;

  private Windmill.@Nullable WorkItem work;
  private @Nullable WindmillComputationKey computationKey;
  private @Nullable SideInputStateFetcher sideInputStateFetcher;
  private WorkItemCommitRequest.@Nullable Builder outputBuilder;
  private @Nullable UnboundedReader<?> activeReader;
  private volatile long backlogBytes;
  private Supplier<Boolean> workIsFailed;

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
    this.backlogBytes = UnboundedReader.BACKLOG_UNKNOWN;
    this.workIsFailed = () -> Boolean.FALSE;
  }

  private static Instant computeProcessingTime(List<Timer> timers) {
    Instant processingTime = Instant.now();
    // Ensure that the processing time is greater than any fired processing time
    // timers.  Otherwise, a trigger could ignore the timer and orphan the window.
    for (Timer timer : timers) {
      if (timer.getType() == Timer.Type.REALTIME) {
        Instant inferredFiringTime =
            WindmillTimeUtils.windmillToHarnessTimestamp(timer.getTimestamp())
                .plus(Duration.millis(1));
        if (inferredFiringTime.isAfter(processingTime)) {
          processingTime = inferredFiringTime;
        }
      }
    }

    return processingTime;
  }

  @VisibleForTesting
  public long getBacklogBytes() {
    return backlogBytes;
  }

  public boolean workIsFailed() {
    return workIsFailed.get();
  }

  public void start(
      @Nullable Object key,
      Windmill.WorkItem work,
      Instant inputDataWatermark,
      @Nullable Instant outputDataWatermark,
      @Nullable Instant synchronizedProcessingTime,
      WindmillStateReader stateReader,
      SideInputStateFetcher sideInputStateFetcher,
      WorkItemCommitRequest.Builder outputBuilder,
      Supplier<Boolean> workFailed) {
    this.work = work;
    this.workIsFailed = workFailed;
    this.computationKey =
        WindmillComputationKey.create(computationId, work.getKey(), work.getShardingKey());
    this.sideInputStateFetcher = sideInputStateFetcher;
    this.outputBuilder = outputBuilder;
    this.sideInputCache.clear();
    clearSinkFullHint();

    Collection<? extends DataflowStreamingModeStepContext> stepContexts = getAllStepContexts();
    if (!stepContexts.isEmpty()) {
      // This must be only created once for the workItem as token validation will fail if the same
      // work token is reused.
      WindmillStateCache.ForKey cacheForKey =
          stateCache.forKey(getComputationKey(), getWork().getCacheToken(), getWorkToken());
      for (DataflowStreamingModeStepContext stepContext : stepContexts) {
        stepContext.start(
            key,
            work.getIsNewKey(),
            stateReader,
            inputDataWatermark,
            computeProcessingTime(work.getTimers().getTimersList()),
            cacheForKey,
            outputDataWatermark,
            synchronizedProcessingTime);
      }
    }
  }

  @Override
  public DataflowStreamingModeStepContext createStepContext(
      DataflowOperationContext operationContext) {
    return new DataflowStreamingModeStepContext(
        operationContext,
        getStateFamily(operationContext.nameContext()),
        new ScopedReadStateSupplier(operationContext, getExecutionStateTracker()),
        () -> work.getTimers().getTimersList(),
        () -> outputBuilder,
        () -> work.getGlobalDataIdNotificationsList(),
        this::fetchSideInput,
        this::canWritePCollectionViewData);
  }

  private boolean canWritePCollectionViewData() {
    return Optional.ofNullable(work)
        .map(Windmill.WorkItem::getKey)
        .map(ByteString::size)
        .map(size -> size != 0)
        .orElse(false);
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
  public SideInputReader getSideInputReaderForViews(Iterable<? extends PCollectionView<?>> views) {
    return StreamingModeSideInputReader.of(views, this);
  }

  @SuppressWarnings("deprecation")
  private <T> TupleTag<?> getInternalTag(PCollectionView<T> view) {
    return view.getTagInternal();
  }

  /**
   * Fetches the requested sideInput, and maintains a view of the cache that doesn't remove items
   * until the active work item is finished.
   *
   * <p>If the side input was not cached, throws {@code IllegalStateException} if the state is
   * {@literal CACHED_IN_WORK_ITEM} or returns {@link SideInput<T>} which contains {@link
   * Optional<T>}.
   */
  <T> SideInput<T> fetchSideInput(
      PCollectionView<T> view,
      BoundedWindow sideInputWindow,
      @Nullable String stateFamily,
      SideInputState state,
      @Nullable Supplier<Closeable> scopedReadStateSupplier) {
    TupleTag<?> viewInternalTag = getInternalTag(view);
    Map<BoundedWindow, SideInput<?>> tagCache =
        sideInputCache.computeIfAbsent(viewInternalTag, k -> new HashMap<>());

    @SuppressWarnings("unchecked")
    Optional<SideInput<T>> cachedSideInput =
        Optional.ofNullable((SideInput<T>) tagCache.get(sideInputWindow));

    if (cachedSideInput.isPresent()) {
      return cachedSideInput.get();
    }

    if (state == SideInputState.CACHED_IN_WORK_ITEM) {
      throw new IllegalStateException(
          "Expected side input to be cached. Tag: " + viewInternalTag.getId());
    }

    return fetchSideInputFromWindmill(
        view,
        sideInputWindow,
        Preconditions.checkNotNull(stateFamily),
        state,
        Preconditions.checkNotNull(scopedReadStateSupplier),
        tagCache);
  }

  private <T> SideInput<T> fetchSideInputFromWindmill(
      PCollectionView<T> view,
      BoundedWindow sideInputWindow,
      String stateFamily,
      SideInputState state,
      Supplier<Closeable> scopedReadStateSupplier,
      Map<BoundedWindow, SideInput<?>> tagCache) {
    SideInput<T> fetched =
        sideInputStateFetcher.fetchSideInput(
            view, sideInputWindow, stateFamily, state, scopedReadStateSupplier);

    if (fetched.isReady()) {
      tagCache.put(sideInputWindow, fetched);
    }

    return fetched;
  }

  public @Nullable ByteString getSerializedKey() {
    return work == null ? null : work.getKey();
  }

  public WindmillComputationKey getComputationKey() {
    return computationKey;
  }

  public long getWorkToken() {
    return work.getWorkToken();
  }

  public Windmill.WorkItem getWork() {
    return work;
  }

  public WorkItemCommitRequest.Builder getOutputBuilder() {
    return outputBuilder;
  }

  /**
   * Returns cached reader for this key if one exists. The reader is removed from the cache. NOTE:
   * The caller is responsible for the reader and should appropriately close it as required.
   */
  public UnboundedReader<?> getCachedReader() {
    return readerCache.acquireReader(
        getComputationKey(), getWork().getCacheToken(), getWork().getWorkToken());
  }

  public void setActiveReader(UnboundedReader<?> reader) {
    checkState(activeReader == null, "not expected to be overwritten");
    activeReader = reader;
  }

  /** Invalidate the state and reader caches for this computation and key. */
  public void invalidateCache() {
    ByteString key = getSerializedKey();
    if (key != null) {
      readerCache.invalidateReader(getComputationKey());
      if (activeReader != null) {
        try {
          activeReader.close();
        } catch (IOException e) {
          LOG.warn("Failed to close reader for {}-{}", computationId, key.toStringUtf8(), e);
        }
      }
      activeReader = null;
      stateCache.invalidate(key, getWork().getShardingKey());
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

    for (DataflowStreamingModeStepContext stepContext : getAllStepContexts()) {
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
        ByteStringOutputStream stream = new ByteStringOutputStream();
        try {
          checkpointCoder.encode(checkpointMark, stream, Coder.Context.OUTER);
        } catch (IOException e) {
          throw new RuntimeException("Exception while encoding checkpoint", e);
        }
        sourceStateBuilder.setState(stream.toByteString());
      }
      outputBuilder.setSourceWatermark(WindmillTimeUtils.harnessToWindmillTimestamp(watermark));

      backlogBytes = activeReader.getSplitBacklogBytes();
      if (backlogBytes == UnboundedReader.BACKLOG_UNKNOWN
          && WorkerCustomSources.isFirstUnboundedSourceSplit(getSerializedKey())) {
        // Only call getTotalBacklogBytes() on the first split.
        backlogBytes = activeReader.getTotalBacklogBytes();
      }
      outputBuilder.setSourceBacklogBytes(backlogBytes);

      readerCache.cacheReader(
          getComputationKey(), getWork().getCacheToken(), getWork().getWorkToken(), activeReader);
      activeReader = null;
    }
    return callbacks;
  }

  String getStateFamily(NameContext nameContext) {
    return nameContext.userName() == null ? null : stateNameMap.get(nameContext.userName());
  }
}
