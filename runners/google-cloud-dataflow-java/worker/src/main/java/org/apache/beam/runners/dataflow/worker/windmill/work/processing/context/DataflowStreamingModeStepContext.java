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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.WindmillNamespacePrefix;
import org.apache.beam.runners.dataflow.worker.WindmillTimerInternals;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputState;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateInternals;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.PeekingIterator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** {@link DataflowStepContext} from Streaming pipelines. */
public class DataflowStreamingModeStepContext extends DataflowStepContext
    implements StreamingModeStepContext {

  private final @Nullable String stateFamily;
  private final Supplier<Closeable> scopedReadStateSupplier;
  private final Supplier<List<Timer>> timersSupplier;
  private final Supplier<Windmill.WorkItemCommitRequest.Builder> outputBuilderSupplier;
  private final Supplier<Iterable<Windmill.GlobalDataId>> sideInputNotificationSupplier;
  private final LoadSideInput loadSideInput;
  private final Supplier<Boolean> canWritePCollectionViewData;
  private final SortedModifiedTimers sortedModifiedTimers;

  // The following are lazily initialized when start() is called. Previous values/references
  // assigned to them will be reset/cleared and lost.
  private WindmillStateInternals<Object> stateInternals;
  private WindmillTimerInternals systemTimerInternals;
  private WindmillTimerInternals userTimerInternals;
  private Iterator<TimerInternals.TimerData> cachedFiredSystemTimers = null;
  private PeekingIterator<TimerInternals.TimerData> cachedFiredUserTimers = null;

  DataflowStreamingModeStepContext(
      DataflowOperationContext operationContext,
      String stateFamily,
      ScopedReadStateSupplier scopedReadStateSupplier,
      Supplier<List<Timer>> timersSupplier,
      Supplier<Windmill.WorkItemCommitRequest.Builder> outputBuilderSupplier,
      Supplier<Iterable<Windmill.GlobalDataId>> sideInputNotificationSupplier,
      LoadSideInput loadSideInput,
      Supplier<Boolean> canWritePCollectionViewData) {
    super(operationContext.nameContext());
    this.stateFamily = stateFamily;
    this.scopedReadStateSupplier = scopedReadStateSupplier;
    this.timersSupplier = timersSupplier;
    this.sortedModifiedTimers = SortedModifiedTimers.create();
    this.outputBuilderSupplier = outputBuilderSupplier;
    this.sideInputNotificationSupplier = sideInputNotificationSupplier;
    this.loadSideInput = loadSideInput;
    this.canWritePCollectionViewData = canWritePCollectionViewData;
  }

  @SuppressWarnings("deprecation")
  private static <T> ByteString encodeToByteString(Coder<T> coder, T rawValue) throws IOException {
    ByteStringOutputStream stream = new ByteStringOutputStream();
    coder.encode(rawValue, stream, Coder.Context.OUTER);
    return stream.toByteString();
  }

  /**
   * Update the {@code stateReader} used by this {@code StepContext}.
   *
   * @param key /** * The current user-facing key for this execution context. * *
   *     <p>This field is set to a new key upon each call to start(), which corresponds to one *
   *     input bundle. For non-keyed bundles, the key will be {@code null}. * *
   *     <p>This key should not be mistaken for the sharding key of the computation, which is always
   *     * present.
   */
  public void start(
      @Nullable Object key,
      boolean isNewKey,
      WindmillStateReader stateReader,
      Instant inputDataWatermark,
      Instant processingTime,
      WindmillStateCache.ForKey cacheForKey,
      @Nullable Instant outputDataWatermark,
      @Nullable Instant synchronizedProcessingTime) {
    this.stateInternals =
        new WindmillStateInternals<>(
            key,
            stateFamily,
            stateReader,
            isNewKey,
            cacheForKey.forFamily(stateFamily),
            scopedReadStateSupplier);

    this.systemTimerInternals =
        new WindmillTimerInternals(
            stateFamily,
            WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
            inputDataWatermark,
            processingTime,
            outputDataWatermark,
            synchronizedProcessingTime,
            td -> {});

    this.userTimerInternals =
        new WindmillTimerInternals(
            stateFamily,
            WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
            inputDataWatermark,
            processingTime,
            outputDataWatermark,
            synchronizedProcessingTime,
            this::onUserTimerModified);

    resetTimerState();
  }

  private void resetTimerState() {
    this.cachedFiredSystemTimers = null;
    this.cachedFiredUserTimers = null;
    this.sortedModifiedTimers.reset();
  }

  public void flushState() {
    Windmill.WorkItemCommitRequest.Builder outputBuilder = outputBuilderSupplier.get();
    stateInternals.persist(outputBuilder);
    systemTimerInternals.persistTo(outputBuilder);
    userTimerInternals.persistTo(outputBuilder);
  }

  @Override
  public <W extends BoundedWindow> TimerInternals.TimerData getNextFiredTimer(
      Coder<W> windowCoder) {
    if (cachedFiredSystemTimers == null) {
      cachedFiredSystemTimers =
          FluentIterable.from(timersSupplier.get())
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

    if (!cachedFiredSystemTimers.hasNext()) {
      return null;
    }
    TimerInternals.TimerData nextTimer = cachedFiredSystemTimers.next();
    // system timers ( GC timer) must be explicitly deleted if only there is a hold.
    // if timestamp is not equals to outputTimestamp then there should be a hold
    if (!nextTimer.getTimestamp().equals(nextTimer.getOutputTimestamp())) {
      systemTimerInternals.deleteTimer(nextTimer);
    }
    return nextTimer;
  }

  private NavigableSet<TimerInternals.TimerData> getModifiedUserTimersOrdered(
      TimeDomain timeDomain) {
    switch (timeDomain) {
      case EVENT_TIME:
        return sortedModifiedTimers.userEventTimers();
      case PROCESSING_TIME:
        return sortedModifiedTimers.userProcessingTimers();
      case SYNCHRONIZED_PROCESSING_TIME:
        return sortedModifiedTimers.userSynchronizedProcessingTimers();
      default:
        throw new RuntimeException("Unexpected time domain " + timeDomain);
    }
  }

  private void onUserTimerModified(TimerInternals.TimerData timerData) {
    if (!timerData.getDeleted()) {
      sortedModifiedTimers.getModifiedUserTimersOrdered(timerData.getDomain()).add(timerData);
    }
    sortedModifiedTimers
        .userTimerKeys()
        .put(
            WindmillTimerInternals.getTimerDataKey(timerData), timerData.getNamespace(), timerData);
  }

  private boolean isUnmodifiedTimer(TimerInternals.TimerData timerData) {
    String timerKey = WindmillTimerInternals.getTimerDataKey(timerData);
    TimerInternals.TimerData updatedTimer =
        sortedModifiedTimers.userTimerKeys().get(timerKey, timerData.getNamespace());
    return updatedTimer == null || updatedTimer.equals(timerData);
  }

  public <W extends BoundedWindow> TimerInternals.TimerData getNextFiredUserTimer(
      Coder<W> windowCoder) {
    if (cachedFiredUserTimers == null) {
      // This is the first call to getNextFiredUserTimer in this bundle. Extract any user timers
      // from the bundle
      // and cache the list for the rest of this bundle processing.
      cachedFiredUserTimers =
          Iterators.peekingIterator(
              FluentIterable.from(timersSupplier.get())
                  .filter(
                      timer ->
                          WindmillTimerInternals.isUserTimer(timer)
                              && timer.getStateFamily().equals(stateFamily))
                  .transform(
                      timer ->
                          WindmillTimerInternals.windmillTimerToTimerData(
                              WindmillNamespacePrefix.USER_NAMESPACE_PREFIX, timer, windowCoder))
                  .iterator());
    }

    while (cachedFiredUserTimers.hasNext()) {
      TimerInternals.TimerData nextInBundle = cachedFiredUserTimers.peek();
      NavigableSet<TimerInternals.TimerData> modifiedUserTimersOrdered =
          getModifiedUserTimersOrdered(nextInBundle.getDomain());
      // If there is a modified timer that is earlier than the next timer in the bundle, try and
      // fire that first.
      while (!modifiedUserTimersOrdered.isEmpty()
          && modifiedUserTimersOrdered.first().compareTo(nextInBundle) <= 0) {
        TimerInternals.TimerData earlierTimer = modifiedUserTimersOrdered.pollFirst();
        if (isUnmodifiedTimer(earlierTimer)) {
          // We must delete the timer. This prevents it from being committed to the backing store.
          // It also handles the
          // case where the timer had been set to the far future and then modified in bundle;
          // without deleting the
          // timer, the runner will still have that future timer stored, and would fire it
          // spuriously.
          userTimerInternals.deleteTimer(earlierTimer);
          return earlierTimer;
        }
      }
      // There is no earlier timer to fire, so return the next timer in the bundle.
      nextInBundle = cachedFiredUserTimers.next();
      if (isUnmodifiedTimer(nextInBundle)) {
        // User timers must be explicitly deleted when delivered, to release the implied hold.
        userTimerInternals.deleteTimer(nextInBundle);
        return nextInBundle;
      }
    }
    return null;
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
    return sideInputNotificationSupplier.get();
  }

  private void ensureStateful(String errorPrefix) {
    if (stateFamily == null) {
      throw new IllegalStateException(errorPrefix + " for stateless step: " + getNameContext());
    }
  }

  @Override
  public <T, W extends BoundedWindow> void writePCollectionViewData(
      TupleTag<?> tag,
      Iterable<T> data,
      Coder<Iterable<T>> dataCoder,
      W window,
      Coder<W> windowCoder)
      throws IOException {
    if (!canWritePCollectionViewData.get()) {
      throw new IllegalStateException("writePCollectionViewData must follow a Combine.globally");
    }

    ensureStateful("Tried to write view data");

    Windmill.GlobalData.Builder builder =
        Windmill.GlobalData.newBuilder()
            .setDataId(
                Windmill.GlobalDataId.newBuilder()
                    .setTag(tag.getId())
                    .setVersion(encodeToByteString(windowCoder, window))
                    .build())
            .setData(encodeToByteString(dataCoder, data))
            .setStateFamily(stateFamily);

    outputBuilderSupplier.get().addGlobalDataUpdates(builder.build());
  }

  /** Fetch the given side input asynchronously and return true if it is present. */
  @Override
  public boolean issueSideInputFetch(
      PCollectionView<?> view, BoundedWindow mainInputWindow, SideInputState state) {
    BoundedWindow sideInputWindow = view.getWindowMappingFn().getSideInputWindow(mainInputWindow);
    return loadSideInput
        .load(view, sideInputWindow, stateFamily, state, scopedReadStateSupplier)
        .isReady();
  }

  /** Note that there is data on the current key that is blocked on the given side input. */
  @Override
  public void addBlockingSideInput(GlobalDataRequest sideInput) {
    ensureStateful("Tried to set global data request");
    GlobalDataRequest sideInputWithStateFamily = withStateFamily(sideInput);
    outputBuilderSupplier
        .get()
        .addGlobalDataRequests(sideInputWithStateFamily)
        .addGlobalDataIdRequests(sideInputWithStateFamily.getDataId());
  }

  private GlobalDataRequest withStateFamily(GlobalDataRequest sideInputRequest) {
    return GlobalDataRequest.newBuilder(sideInputRequest).setStateFamily(stateFamily).build();
  }

  /** Note that there is data on the current key that is blocked on the given side inputs. */
  @Override
  public void addBlockingSideInputs(Iterable<GlobalDataRequest> sideInputs) {
    for (GlobalDataRequest sideInput : sideInputs) {
      addBlockingSideInput(sideInput);
    }
  }

  @Override
  public StateInternals stateInternals() {
    ensureStateful("Tried to access state");
    return checkNotNull(stateInternals);
  }

  @Override
  public TimerInternals timerInternals() {
    ensureStateful("Tried to access timers");
    return checkNotNull(systemTimerInternals);
  }

  public TimerInternals userTimerInternals() {
    ensureStateful("Tried to access user timers");
    return checkNotNull(userTimerInternals);
  }

  /**
   * Sorted timers that were set or modified by user processing earlier in this bundle.
   *
   * @implNote We use a NavigableSet instead of a priority queue to prevent duplicate elements from
   *     ending up in the queue.
   */
  @AutoValue
  abstract static class SortedModifiedTimers {

    private static SortedModifiedTimers create() {
      return new AutoValue_DataflowStreamingModeStepContext_SortedModifiedTimers(
          Sets.newTreeSet(), Sets.newTreeSet(), Sets.newTreeSet(), HashBasedTable.create());
    }

    private void reset() {
      userEventTimers().clear();
      userProcessingTimers().clear();
      userSynchronizedProcessingTimers().clear();
      userTimerKeys().clear();
    }

    private NavigableSet<TimerInternals.TimerData> getModifiedUserTimersOrdered(
        TimeDomain timeDomain) {
      switch (timeDomain) {
        case EVENT_TIME:
          return userEventTimers();
        case PROCESSING_TIME:
          return userProcessingTimers();
        case SYNCHRONIZED_PROCESSING_TIME:
          return userSynchronizedProcessingTimers();
        default:
          throw new RuntimeException("Unexpected time domain " + timeDomain);
      }
    }

    abstract NavigableSet<TimerInternals.TimerData> userEventTimers();

    abstract NavigableSet<TimerInternals.TimerData> userProcessingTimers();

    abstract NavigableSet<TimerInternals.TimerData> userSynchronizedProcessingTimers();

    /**
     * Timer keys that were modified by user processing earlier in this bundle. This serves a
     * tombstone, so that we know not to fire any bundle timers that were modified.
     */
    abstract Table<String, StateNamespace, TimerInternals.TimerData> userTimerKeys();
  }
}
