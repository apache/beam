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
package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.Table;
import com.google.common.collect.TreeMultiset;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.joda.time.Instant;

/**
 * Manages watermarks of {@link PCollection PCollections} and input and output watermarks of
 * {@link AppliedPTransform AppliedPTransforms} to provide event-time and completion tracking for
 * in-memory execution. {@link WatermarkManager} is designed to update and return a
 * consistent view of watermarks in the presence of concurrent updates.
 *
 * <p>An {@link WatermarkManager} is provided with the collection of root
 * {@link AppliedPTransform AppliedPTransforms} and a map of {@link PCollection PCollections} to
 * all the {@link AppliedPTransform AppliedPTransforms} that consume them at construction time.
 *
 * <p>Whenever a root {@link AppliedPTransform transform} produces elements, the
 * {@link WatermarkManager} is provided with the produced elements and the output watermark
 * of the producing {@link AppliedPTransform transform}. The
 * {@link WatermarkManager watermark manager} is responsible for computing the watermarks
 * of all {@link AppliedPTransform transforms} that consume one or more
 * {@link PCollection PCollections}.
 *
 * <p>Whenever a non-root {@link AppliedPTransform} finishes processing one or more in-flight
 * elements (referred to as the input {@link CommittedBundle bundle}), the following occurs
 * atomically:
 * <ul>
 *  <li>All of the in-flight elements are removed from the collection of pending elements for the
 *      {@link AppliedPTransform}.</li>
 *  <li>All of the elements produced by the {@link AppliedPTransform} are added to the collection
 *      of pending elements for each {@link AppliedPTransform} that consumes them.</li>
 *  <li>The input watermark for the {@link AppliedPTransform} becomes the maximum value of
 *    <ul>
 *      <li>the previous input watermark</li>
 *      <li>the minimum of
 *        <ul>
 *          <li>the timestamps of all currently pending elements</li>
 *          <li>all input {@link PCollection} watermarks</li>
 *        </ul>
 *      </li>
 *    </ul>
 *  </li>
 *  <li>The output watermark for the {@link AppliedPTransform} becomes the maximum of
 *    <ul>
 *      <li>the previous output watermark</li>
 *      <li>the minimum of
 *        <ul>
 *          <li>the current input watermark</li>
 *          <li>the current watermark holds</li>
 *        </ul>
 *      </li>
 *    </ul>
 *  </li>
 *  <li>The watermark of the output {@link PCollection} can be advanced to the output watermark of
 *      the {@link AppliedPTransform}</li>
 *  <li>The watermark of all downstream {@link AppliedPTransform AppliedPTransforms} can be
 *      advanced.</li>
 * </ul>
 *
 * <p>The watermark of a {@link PCollection} is equal to the output watermark of the
 * {@link AppliedPTransform} that produces it.
 *
 * <p>The watermarks for a {@link PTransform} are updated as follows when output is committed:<pre>
 * Watermark_In'  = MAX(Watermark_In, MIN(U(TS_Pending), U(Watermark_InputPCollection)))
 * Watermark_Out' = MAX(Watermark_Out, MIN(Watermark_In', U(StateHold)))
 * Watermark_PCollection = Watermark_Out_ProducingPTransform
 * </pre>
 */
class WatermarkManager {
  // The number of updates to apply in #tryApplyPendingUpdates
  private static final int MAX_INCREMENTAL_UPDATES = 10;


  /**
   * The watermark of some {@link Pipeline} element, usually a {@link PTransform} or a
   * {@link PCollection}.
   *
   * <p>A watermark is a monotonically increasing value, which represents the point up to which the
   * system believes it has received all of the data. Data that arrives with a timestamp that is
   * before the watermark is considered late. {@link BoundedWindow#TIMESTAMP_MAX_VALUE} is a special
   * timestamp which indicates we have received all of the data and there will be no more on-time or
   * late data. This value is represented by {@link WatermarkManager#THE_END_OF_TIME}.
   */
  @VisibleForTesting interface Watermark {
    /**
     * Returns the current value of this watermark.
     */
    Instant get();

    /**
     * Refreshes the value of this watermark from its input watermarks and watermark holds.
     *
     * @return true if the value of the watermark has changed (and thus dependent watermark must
     *         also be updated
     */
    WatermarkUpdate refresh();
  }

  /**
   * The result of computing a {@link Watermark}.
   */
  private enum WatermarkUpdate {
    /** The watermark is later than the value at the previous time it was computed. */
    ADVANCED(true),
    /** The watermark is equal to the value at the previous time it was computed. */
    NO_CHANGE(false);

    private final boolean advanced;

    private WatermarkUpdate(boolean advanced) {
      this.advanced = advanced;
    }

    public boolean isAdvanced() {
      return advanced;
    }

    /**
     * Returns the {@link WatermarkUpdate} that is a result of combining the two watermark updates.
     *
     * <p>If either of the input {@link WatermarkUpdate WatermarkUpdates} were advanced, the result
     * {@link WatermarkUpdate} has been advanced.
     */
    public WatermarkUpdate union(WatermarkUpdate that) {
      if (this.advanced) {
        return this;
      }
      return that;
    }

    /**
     * Returns the {@link WatermarkUpdate} based on the former and current
     * {@link Instant timestamps}.
     */
    public static WatermarkUpdate fromTimestamps(Instant oldTime, Instant currentTime) {
      if (currentTime.isAfter(oldTime)) {
        return ADVANCED;
      }
      return NO_CHANGE;
    }
  }

  /**
   * The input {@link Watermark} of an {@link AppliedPTransform}.
   *
   * <p>At any point, the value of an {@link AppliedPTransformInputWatermark} is equal to the
   * minimum watermark across all of its input {@link Watermark Watermarks}, and the minimum
   * timestamp of all of the pending elements, restricted to be monotonically increasing.
   *
   * <p>See {@link #refresh()} for more information.
   */
  @VisibleForTesting static class AppliedPTransformInputWatermark implements Watermark {
    private final Collection<? extends Watermark> inputWatermarks;
    private final SortedMultiset<CommittedBundle<?>> pendingElements;

    // This tracks only the quantity of timers at each timestamp, for quickly getting the cross-key
    // minimum
    private final SortedMultiset<TimerData> pendingTimers;

    // Entries in this table represent the authoritative timestamp for which
    // a per-key-and-StateNamespace timer is set.
    private final Map<StructuralKey<?>, Table<StateNamespace, String, TimerData>> existingTimers;

    // This per-key sorted set allows quick retrieval of timers that should fire for a key
    private final Map<StructuralKey<?>, NavigableSet<TimerData>> objectTimers;

    private AtomicReference<Instant> currentWatermark;

    public AppliedPTransformInputWatermark(Collection<? extends Watermark> inputWatermarks) {
      this.inputWatermarks = inputWatermarks;
      // The ordering must order elements by timestamp, and must not compare two distinct elements
      // as equal. This is built on the assumption that any element added as a pending element will
      // be consumed without modifications.
      //
      // The same logic is applied for pending timers
      Ordering<CommittedBundle<?>> pendingBundleComparator =
          new BundleByElementTimestampComparator().compound(Ordering.arbitrary());
      this.pendingElements =
          TreeMultiset.create(pendingBundleComparator);
      this.pendingTimers = TreeMultiset.create();
      this.objectTimers = new HashMap<>();
      this.existingTimers = new HashMap<>();
      currentWatermark = new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @Override
    public Instant get() {
      return currentWatermark.get();
    }

    /**
     * {@inheritDoc}.
     *
     * <p>When refresh is called, the value of the {@link AppliedPTransformInputWatermark} becomes
     * equal to the maximum value of
     * <ul>
     *   <li>the previous input watermark</li>
     *   <li>the minimum of
     *     <ul>
     *       <li>the timestamps of all currently pending elements</li>
     *       <li>all input {@link PCollection} watermarks</li>
     *     </ul>
     *   </li>
     * </ul>
     */
    @Override
    public synchronized WatermarkUpdate refresh() {
      Instant oldWatermark = currentWatermark.get();
      Instant minInputWatermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (Watermark inputWatermark : inputWatermarks) {
        minInputWatermark = INSTANT_ORDERING.min(minInputWatermark, inputWatermark.get());
      }
      if (!pendingElements.isEmpty()) {
        minInputWatermark =
            INSTANT_ORDERING.min(
                minInputWatermark, pendingElements.firstEntry().getElement().getMinimumTimestamp());
      }
      Instant newWatermark = INSTANT_ORDERING.max(oldWatermark, minInputWatermark);
      currentWatermark.set(newWatermark);
      return WatermarkUpdate.fromTimestamps(oldWatermark, newWatermark);
    }

    private synchronized void addPending(CommittedBundle<?> newPending) {
      pendingElements.add(newPending);
    }

    private synchronized void removePending(CommittedBundle<?> completed) {
      pendingElements.remove(completed);
    }

    @VisibleForTesting synchronized Instant getEarliestTimerTimestamp() {
      if (pendingTimers.isEmpty()) {
        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      } else {
        return pendingTimers.firstEntry().getElement().getTimestamp();
      }
    }

    @VisibleForTesting synchronized void updateTimers(TimerUpdate update) {
      NavigableSet<TimerData> keyTimers =
          objectTimers.computeIfAbsent(update.key, k -> new TreeSet<>());
      Table<StateNamespace, String, TimerData> existingTimersForKey =
              existingTimers.computeIfAbsent(update.key, k -> HashBasedTable.create());

      for (TimerData timer : update.getSetTimers()) {
        if (TimeDomain.EVENT_TIME.equals(timer.getDomain())) {
          @Nullable
          TimerData existingTimer =
              existingTimersForKey.get(timer.getNamespace(), timer.getTimerId());

          if (existingTimer == null) {
            pendingTimers.add(timer);
            keyTimers.add(timer);
          } else if (!existingTimer.equals(timer)) {
            pendingTimers.remove(existingTimer);
            keyTimers.remove(existingTimer);
            pendingTimers.add(timer);
            keyTimers.add(timer);
          } // else the timer is already set identically, so noop

          existingTimersForKey.put(timer.getNamespace(), timer.getTimerId(), timer);
        }
      }

      for (TimerData timer : update.getDeletedTimers()) {
        if (TimeDomain.EVENT_TIME.equals(timer.getDomain())) {
          @Nullable
          TimerData existingTimer =
              existingTimersForKey.get(timer.getNamespace(), timer.getTimerId());

          if (existingTimer != null) {
            pendingTimers.remove(existingTimer);
            keyTimers.remove(existingTimer);
            existingTimersForKey.remove(existingTimer.getNamespace(), existingTimer.getTimerId());
          }
        }
      }

      for (TimerData timer : update.getCompletedTimers()) {
        if (TimeDomain.EVENT_TIME.equals(timer.getDomain())) {
          keyTimers.remove(timer);
          pendingTimers.remove(timer);
        }
      }
    }

    @VisibleForTesting
    synchronized Map<StructuralKey<?>, List<TimerData>> extractFiredEventTimeTimers() {
      return extractFiredTimers(currentWatermark.get(), objectTimers);
    }

    @Override
    public synchronized String toString() {
      return MoreObjects.toStringHelper(AppliedPTransformInputWatermark.class)
          .add("pendingElements", pendingElements)
          .add("currentWatermark", currentWatermark)
          .toString();
    }
  }

  /**
   * The output {@link Watermark} of an {@link AppliedPTransform}.
   *
   * <p>The value of an {@link AppliedPTransformOutputWatermark} is equal to the minimum of the
   * current watermark hold and the {@link AppliedPTransformInputWatermark} for the same
   * {@link AppliedPTransform}, restricted to be monotonically increasing. See
   * {@link #refresh()} for more information.
   */
  private static class AppliedPTransformOutputWatermark implements Watermark {
    private final AppliedPTransformInputWatermark inputWatermark;
    private final PerKeyHolds holds;
    private AtomicReference<Instant> currentWatermark;

    public AppliedPTransformOutputWatermark(
        AppliedPTransformInputWatermark inputWatermark) {
      this.inputWatermark = inputWatermark;
      holds = new PerKeyHolds();
      currentWatermark = new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    public synchronized void updateHold(Object key, Instant newHold) {
      if (newHold == null) {
        holds.removeHold(key);
      } else {
        holds.updateHold(key, newHold);
      }
    }

    @Override
    public Instant get() {
      return currentWatermark.get();
    }

    /**
     * {@inheritDoc}.
     *
     * <p>When refresh is called, the value of the {@link AppliedPTransformOutputWatermark} becomes
     * equal to the maximum value of:
     * <ul>
     *   <li>the previous output watermark</li>
     *   <li>the minimum of
     *     <ul>
     *       <li>the current input watermark</li>
     *       <li>the current watermark holds</li>
     *     </ul>
     *   </li>
     * </ul>
     */
    @Override
    public synchronized WatermarkUpdate refresh() {
      Instant oldWatermark = currentWatermark.get();
      Instant newWatermark = INSTANT_ORDERING.min(
          inputWatermark.get(),
          inputWatermark.getEarliestTimerTimestamp(),
          holds.getMinHold());
      newWatermark = INSTANT_ORDERING.max(oldWatermark, newWatermark);
      currentWatermark.set(newWatermark);
      return WatermarkUpdate.fromTimestamps(oldWatermark, newWatermark);
    }

    @Override
    public synchronized String toString() {
      return MoreObjects.toStringHelper(AppliedPTransformOutputWatermark.class)
          .add("holds", holds)
          .add("currentWatermark", currentWatermark)
          .toString();
    }
  }

  /**
   * The input {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} hold for an
   * {@link AppliedPTransform}.
   *
   * <p>At any point, the hold value of an {@link SynchronizedProcessingTimeInputWatermark} is equal
   * to the minimum across all pending bundles at the {@link AppliedPTransform} and all upstream
   * {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} watermarks. The value of the input
   * synchronized processing time at any step is equal to the maximum of:
   * <ul>
   *   <li>The most recently returned synchronized processing input time
   *   <li>The minimum of
   *     <ul>
   *       <li>The current processing time
   *       <li>The current synchronized processing time input hold
   *     </ul>
   * </ul>
   */
  private static class SynchronizedProcessingTimeInputWatermark implements Watermark {
    private final Collection<? extends Watermark> inputWms;
    private final Collection<CommittedBundle<?>> pendingBundles;
    private final Map<StructuralKey<?>, NavigableSet<TimerData>> processingTimers;
    private final Map<StructuralKey<?>, NavigableSet<TimerData>> synchronizedProcessingTimers;
    private final Map<StructuralKey<?>, Table<StateNamespace, String, TimerData>> existingTimers;

    private final NavigableSet<TimerData> pendingTimers;

    private AtomicReference<Instant> earliestHold;

    public SynchronizedProcessingTimeInputWatermark(Collection<? extends Watermark> inputWms) {
      this.inputWms = inputWms;
      this.pendingBundles = new HashSet<>();
      this.processingTimers = new HashMap<>();
      this.synchronizedProcessingTimers = new HashMap<>();
      this.existingTimers = new HashMap<>();
      this.pendingTimers = new TreeSet<>();
      Instant initialHold = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (Watermark wm : inputWms) {
        initialHold = INSTANT_ORDERING.min(initialHold, wm.get());
      }
      earliestHold = new AtomicReference<>(initialHold);
    }

    @Override
    public Instant get() {
      return earliestHold.get();
    }

    /**
     * {@inheritDoc}.
     *
     * <p>When refresh is called, the value of the {@link SynchronizedProcessingTimeInputWatermark}
     * becomes equal to the minimum value of
     * <ul>
     *   <li>the timestamps of all currently pending bundles</li>
     *   <li>all input {@link PCollection} synchronized processing time watermarks</li>
     * </ul>
     *
     * <p>Note that this value is not monotonic, but the returned value for the synchronized
     * processing time must be.
     */
    @Override
    public synchronized WatermarkUpdate refresh() {
      Instant oldHold = earliestHold.get();
      Instant minTime = THE_END_OF_TIME.get();
      for (Watermark input : inputWms) {
        minTime = INSTANT_ORDERING.min(minTime, input.get());
      }
      for (CommittedBundle<?> bundle : pendingBundles) {
        // TODO: Track elements in the bundle by the processing time they were output instead of
        // entire bundles. Requried to support arbitrarily splitting and merging bundles between
        // steps
        minTime = INSTANT_ORDERING.min(minTime, bundle.getSynchronizedProcessingOutputWatermark());
      }
      earliestHold.set(minTime);
      return WatermarkUpdate.fromTimestamps(oldHold, minTime);
    }

    public synchronized void addPending(CommittedBundle<?> bundle) {
      pendingBundles.add(bundle);
    }

    public synchronized void removePending(CommittedBundle<?> bundle) {
      pendingBundles.remove(bundle);
    }

    /**
     * Return the earliest timestamp of the earliest timer that has not been completed. This is
     * either the earliest timestamp across timers that have not been completed, or the earliest
     * timestamp across timers that have been delivered but have not been completed.
     */
    public synchronized Instant getEarliestTimerTimestamp() {
      Instant earliest = THE_END_OF_TIME.get();
      for (NavigableSet<TimerData> timers : processingTimers.values()) {
        if (!timers.isEmpty()) {
          earliest = INSTANT_ORDERING.min(timers.first().getTimestamp(), earliest);
        }
      }
      for (NavigableSet<TimerData> timers : synchronizedProcessingTimers.values()) {
        if (!timers.isEmpty()) {
          earliest = INSTANT_ORDERING.min(timers.first().getTimestamp(), earliest);
        }
      }
      if (!pendingTimers.isEmpty()) {
        earliest = INSTANT_ORDERING.min(pendingTimers.first().getTimestamp(), earliest);
      }
      return earliest;
    }

    private synchronized void updateTimers(TimerUpdate update) {
      Map<TimeDomain, NavigableSet<TimerData>> timerMap = timerMap(update.key);
      Table<StateNamespace, String, TimerData> existingTimersForKey =
          existingTimers.computeIfAbsent(update.key, k -> HashBasedTable.create());

      for (TimerData addedTimer : update.setTimers) {
        NavigableSet<TimerData> timerQueue = timerMap.get(addedTimer.getDomain());
        if (timerQueue == null) {
          continue;
        }

        @Nullable
        TimerData existingTimer =
            existingTimersForKey.get(addedTimer.getNamespace(), addedTimer.getTimerId());
        if (existingTimer == null) {
          timerQueue.add(addedTimer);
        } else if (!existingTimer.equals(addedTimer)) {
          timerQueue.remove(existingTimer);
          timerQueue.add(addedTimer);
        } // else the timer is already set identically, so noop.

        existingTimersForKey.put(addedTimer.getNamespace(), addedTimer.getTimerId(), addedTimer);
      }

      for (TimerData deletedTimer : update.deletedTimers) {
        NavigableSet<TimerData> timerQueue = timerMap.get(deletedTimer.getDomain());
        if (timerQueue == null) {
          continue;
        }

        @Nullable
        TimerData existingTimer =
            existingTimersForKey.get(deletedTimer.getNamespace(), deletedTimer.getTimerId());

        if (existingTimer != null) {
          pendingTimers.remove(deletedTimer);
          timerQueue.remove(deletedTimer);
          existingTimersForKey.remove(existingTimer.getNamespace(), existingTimer.getTimerId());
        }
      }

      for (TimerData completedTimer : update.completedTimers) {
        pendingTimers.remove(completedTimer);
      }
    }

    private synchronized Map<StructuralKey<?>, List<TimerData>> extractFiredDomainTimers(
        TimeDomain domain, Instant firingTime) {
      Map<StructuralKey<?>, List<TimerData>> firedTimers;
      switch (domain) {
        case PROCESSING_TIME:
          firedTimers = extractFiredTimers(firingTime, processingTimers);
          break;
        case SYNCHRONIZED_PROCESSING_TIME:
          firedTimers =
              extractFiredTimers(
                  INSTANT_ORDERING.min(firingTime, earliestHold.get()),
                  synchronizedProcessingTimers);
          break;
        default:
          throw new IllegalArgumentException(
              "Called getFiredTimers on a Synchronized Processing Time watermark"
                  + " and gave a non-processing time domain "
                  + domain);
      }
      for (Map.Entry<StructuralKey<?>, ? extends Collection<TimerData>> firedTimer :
          firedTimers.entrySet()) {
        pendingTimers.addAll(firedTimer.getValue());
      }
      return firedTimers;
    }

    private Map<TimeDomain, NavigableSet<TimerData>> timerMap(StructuralKey<?> key) {
      NavigableSet<TimerData> processingQueue =
          processingTimers.computeIfAbsent(key, k -> new TreeSet<>());
      NavigableSet<TimerData> synchronizedProcessingQueue =
              synchronizedProcessingTimers.computeIfAbsent(key, k -> new TreeSet<>());
      EnumMap<TimeDomain, NavigableSet<TimerData>> result = new EnumMap<>(TimeDomain.class);
      result.put(TimeDomain.PROCESSING_TIME, processingQueue);
      result.put(TimeDomain.SYNCHRONIZED_PROCESSING_TIME, synchronizedProcessingQueue);
      return result;
    }

    @Override
    public synchronized String toString() {
      return MoreObjects.toStringHelper(SynchronizedProcessingTimeInputWatermark.class)
          .add("earliestHold", earliestHold)
          .toString();
    }
  }

  /**
   * The output {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} hold for an
   * {@link AppliedPTransform}.
   *
   * <p>At any point, the hold value of an {@link SynchronizedProcessingTimeOutputWatermark} is
   * equal to the minimum across all incomplete timers at the {@link AppliedPTransform} and all
   * upstream {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} watermarks. The value of the output
   * synchronized processing time at any step is equal to the maximum of:
   * <ul>
   *   <li>The most recently returned synchronized processing output time
   *   <li>The minimum of
   *     <ul>
   *       <li>The current processing time
   *       <li>The current synchronized processing time output hold
   *     </ul>
   * </ul>
   */
  private static class SynchronizedProcessingTimeOutputWatermark implements Watermark {
    private final SynchronizedProcessingTimeInputWatermark inputWm;
    private AtomicReference<Instant> latestRefresh;

    public SynchronizedProcessingTimeOutputWatermark(
        SynchronizedProcessingTimeInputWatermark inputWm) {
      this.inputWm = inputWm;
      this.latestRefresh = new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @Override
    public Instant get() {
      return latestRefresh.get();
    }

    /**
     * {@inheritDoc}.
     *
     * <p>When refresh is called, the value of the {@link SynchronizedProcessingTimeOutputWatermark}
     * becomes equal to the minimum value of:
     * <ul>
     *   <li>the current input watermark.
     *   <li>all {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} timers that are based on the input
     *       watermark.
     *   <li>all {@link TimeDomain#PROCESSING_TIME} timers that are based on the input watermark.
     * </ul>
     *
     * <p>Note that this value is not monotonic, but the returned value for the synchronized
     * processing time must be.
     */
    @Override
    public synchronized WatermarkUpdate refresh() {
      // Hold the output synchronized processing time to the input watermark, which takes into
      // account buffered bundles, and the earliest pending timer, which determines what to hold
      // downstream timers to.
      Instant oldRefresh = latestRefresh.get();
      Instant newTimestamp =
          INSTANT_ORDERING.min(inputWm.get(), inputWm.getEarliestTimerTimestamp());
      latestRefresh.set(newTimestamp);
      return WatermarkUpdate.fromTimestamps(oldRefresh, newTimestamp);
    }

    @Override
    public synchronized String toString() {
      return MoreObjects.toStringHelper(SynchronizedProcessingTimeOutputWatermark.class)
          .add("latestRefresh", latestRefresh)
          .toString();
    }
  }

  /**
   * The {@code Watermark} that is after the latest time it is possible to represent in the global
   * window. This is a distinguished value representing a complete {@link PTransform}.
   */
  private static final Watermark THE_END_OF_TIME = new Watermark() {
        @Override
        public WatermarkUpdate refresh() {
          // THE_END_OF_TIME is a distinguished value that cannot be advanced.
          return WatermarkUpdate.NO_CHANGE;
        }

        @Override
        public Instant get() {
          return BoundedWindow.TIMESTAMP_MAX_VALUE;
        }
      };

  private static final Ordering<Instant> INSTANT_ORDERING = Ordering.natural();

  /**
   * For each (Object, NavigableSet) pair in the provided map, remove each Timer that is before the
   * latestTime argument and put in in the result with the same key, then remove all of the keys
   * which have no more pending timers.
   *
   * <p>The result collection retains ordering of timers (from earliest to latest).
   */
  private static Map<StructuralKey<?>, List<TimerData>> extractFiredTimers(
      Instant latestTime, Map<StructuralKey<?>, NavigableSet<TimerData>> objectTimers) {
    Map<StructuralKey<?>, List<TimerData>> result = new HashMap<>();
    Set<StructuralKey<?>> emptyKeys = new HashSet<>();
    for (Map.Entry<StructuralKey<?>, NavigableSet<TimerData>> pendingTimers :
        objectTimers.entrySet()) {
      NavigableSet<TimerData> timers = pendingTimers.getValue();
      if (!timers.isEmpty() && timers.first().getTimestamp().isBefore(latestTime)) {
        ArrayList<TimerData> keyFiredTimers = new ArrayList<>();
        result.put(pendingTimers.getKey(), keyFiredTimers);
        while (!timers.isEmpty() && timers.first().getTimestamp().isBefore(latestTime)) {
          keyFiredTimers.add(timers.first());
          timers.remove(timers.first());
        }
      }
      if (timers.isEmpty()) {
        emptyKeys.add(pendingTimers.getKey());
      }
    }
    objectTimers.keySet().removeAll(emptyKeys);
    return result;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * The {@link Clock} providing the current time in the {@link TimeDomain#PROCESSING_TIME} domain.
   */
  private final Clock clock;

  /**
   * The {@link DirectGraph} representing the {@link Pipeline} this {@link WatermarkManager} tracks
   * watermarks for.
   */
  private final DirectGraph graph;

  /**
   * The input and output watermark of each {@link AppliedPTransform}.
   */
  private final Map<AppliedPTransform<?, ?, ?>, TransformWatermarks> transformToWatermarks;

  /**
   * A queue of pending updates to the state of this {@link WatermarkManager}.
   */
  private final ConcurrentLinkedQueue<PendingWatermarkUpdate> pendingUpdates;

  /**
   * A lock used to control concurrency for updating pending values.
   */
  private final Lock refreshLock;

  /**
   * A queue of pending {@link AppliedPTransform AppliedPTransforms} that have potentially
   * stale data.
   */
  @GuardedBy("refreshLock")
  private final Set<AppliedPTransform<?, ?, ?>> pendingRefreshes;

  /**
   * Creates a new {@link WatermarkManager}. All watermarks within the newly created {@link
   * WatermarkManager} start at {@link BoundedWindow#TIMESTAMP_MIN_VALUE}, the minimum watermark,
   * with no watermark holds or pending elements.
   *
   * @param clock the clock to use to determine processing time
   * @param graph the graph representing this pipeline
   */
  public static WatermarkManager create(Clock clock, DirectGraph graph) {
    return new WatermarkManager(clock, graph);
  }

  private WatermarkManager(Clock clock, DirectGraph graph) {
    this.clock = clock;
    this.graph = graph;

    this.pendingUpdates = new ConcurrentLinkedQueue<>();

    this.refreshLock = new ReentrantLock();
    this.pendingRefreshes = new HashSet<>();

    transformToWatermarks = new HashMap<>();

    for (AppliedPTransform<?, ?, ?> rootTransform : graph.getRootTransforms()) {
      getTransformWatermark(rootTransform);
    }
    for (AppliedPTransform<?, ?, ?> primitiveTransform : graph.getPrimitiveTransforms()) {
      getTransformWatermark(primitiveTransform);
    }
  }

  private TransformWatermarks getValueWatermark(PValue pvalue) {
    if (pvalue instanceof PCollection) {
      return getTransformWatermark(graph.getProducer((PCollection<?>) pvalue));
    } else if (pvalue instanceof PCollectionView<?>) {
      return getTransformWatermark(graph.getWriter((PCollectionView<?>) pvalue));
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unknown type of %s %s", PValue.class.getSimpleName(), pvalue.getClass()));
    }
  }

  private TransformWatermarks getTransformWatermark(AppliedPTransform<?, ?, ?> transform) {
    TransformWatermarks wms = transformToWatermarks.get(transform);
    if (wms == null) {
      List<Watermark> inputCollectionWatermarks = getInputWatermarks(transform);
      AppliedPTransformInputWatermark inputWatermark =
          new AppliedPTransformInputWatermark(inputCollectionWatermarks);
      AppliedPTransformOutputWatermark outputWatermark =
          new AppliedPTransformOutputWatermark(inputWatermark);

      SynchronizedProcessingTimeInputWatermark inputProcessingWatermark =
          new SynchronizedProcessingTimeInputWatermark(getInputProcessingWatermarks(transform));
      SynchronizedProcessingTimeOutputWatermark outputProcessingWatermark =
          new SynchronizedProcessingTimeOutputWatermark(inputProcessingWatermark);

      wms =
          new TransformWatermarks(
              transform,
              inputWatermark,
              outputWatermark,
              inputProcessingWatermark,
              outputProcessingWatermark);
      transformToWatermarks.put(transform, wms);
    }
    return wms;
  }

  private Collection<Watermark> getInputProcessingWatermarks(AppliedPTransform<?, ?, ?> transform) {
    ImmutableList.Builder<Watermark> inputWmsBuilder = ImmutableList.builder();
    Collection<PValue> inputs = TransformInputs.nonAdditionalInputs(transform);
    if (inputs.isEmpty()) {
      inputWmsBuilder.add(THE_END_OF_TIME);
    }
    for (PValue pvalue : inputs) {
      Watermark producerOutputWatermark =
          getValueWatermark(pvalue).synchronizedProcessingOutputWatermark;
      inputWmsBuilder.add(producerOutputWatermark);
    }
    return inputWmsBuilder.build();
  }

  private List<Watermark> getInputWatermarks(AppliedPTransform<?, ?, ?> transform) {
    ImmutableList.Builder<Watermark> inputWatermarksBuilder = ImmutableList.builder();
    Collection< PValue> inputs = TransformInputs.nonAdditionalInputs(transform);
    if (inputs.isEmpty()) {
      inputWatermarksBuilder.add(THE_END_OF_TIME);
    }
    for (PValue pvalue : inputs) {
      Watermark producerOutputWatermark = getValueWatermark(pvalue).outputWatermark;
      inputWatermarksBuilder.add(producerOutputWatermark);
    }
    List<Watermark> inputCollectionWatermarks = inputWatermarksBuilder.build();
    return inputCollectionWatermarks;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Gets the input and output watermarks for an {@link AppliedPTransform}. If the
   * {@link AppliedPTransform PTransform} has not processed any elements, return a watermark of
   * {@link BoundedWindow#TIMESTAMP_MIN_VALUE}.
   *
   * @return a snapshot of the input watermark and output watermark for the provided transform
   */
  public TransformWatermarks getWatermarks(AppliedPTransform<?, ?, ?> transform) {
    return transformToWatermarks.get(transform);
  }

  public void initialize(
      Map<AppliedPTransform<?, ?, ?>, ? extends Iterable<CommittedBundle<?>>> initialBundles) {
    refreshLock.lock();
    try {
      for (Map.Entry<AppliedPTransform<?, ?, ?>, ? extends Iterable<CommittedBundle<?>>> rootEntry :
          initialBundles.entrySet()) {
        TransformWatermarks rootWms = transformToWatermarks.get(rootEntry.getKey());
        for (CommittedBundle<?> initialBundle : rootEntry.getValue()) {
          rootWms.addPending(initialBundle);
        }
        pendingRefreshes.add(rootEntry.getKey());
      }
    } finally {
      refreshLock.unlock();
    }
  }

  /**
   * Updates the watermarks of a transform with one or more inputs.
   *
   * <p>Each transform has two monotonically increasing watermarks: the input watermark, which can,
   * at any time, be updated to equal:
   * <pre>
   * MAX(CurrentInputWatermark, MIN(PendingElements, InputPCollectionWatermarks))
   * </pre>
   * and the output watermark, which can, at any time, be updated to equal:
   * <pre>
   * MAX(CurrentOutputWatermark, MIN(InputWatermark, WatermarkHolds))
   * </pre>.
   *
   * @param completed the input that has completed
   * @param timerUpdate the timers that were added, removed, and completed as part of producing
   *                    this update
   * @param result the result that was produced by processing the input
   * @param earliestHold the earliest watermark hold in the transform's state. {@code null} if there
   *                     is no hold
   */
  public void updateWatermarks(
      @Nullable CommittedBundle<?> completed,
      TimerUpdate timerUpdate,
      CommittedResult result,
      Instant earliestHold) {
    pendingUpdates.offer(PendingWatermarkUpdate.create(completed,
        timerUpdate,
        result,
        earliestHold));
    tryApplyPendingUpdates();
  }

  private void tryApplyPendingUpdates() {
    if (refreshLock.tryLock()) {
      try {
        applyNUpdates(MAX_INCREMENTAL_UPDATES);
      } finally {
        refreshLock.unlock();
      }
    }
  }

  /**
   * Applies all pending updates to this {@link WatermarkManager}, causing the pending state
   * of all {@link TransformWatermarks} to be advanced as far as possible.
   */
  private void applyAllPendingUpdates() {
    refreshLock.lock();
    try {
      applyNUpdates(-1);
    } finally {
      refreshLock.unlock();
    }
  }

  @GuardedBy("refreshLock")
  /**
   * Applies up to {@code numUpdates}, or all available updates if numUpdates is non-positive.
   */
  private void applyNUpdates(int numUpdates) {
    for (int i = 0; !pendingUpdates.isEmpty() && (i < numUpdates || numUpdates <= 0); i++) {
      PendingWatermarkUpdate pending = pendingUpdates.poll();
      applyPendingUpdate(pending);
      pendingRefreshes.add(pending.getTransform());
    }
  }

  private void applyPendingUpdate(PendingWatermarkUpdate pending) {
    CommittedResult result = pending.getResult();
    AppliedPTransform<?, ?, ?> transform = result.getTransform();
    CommittedBundle<?> inputBundle = pending.getInputBundle();

    updatePending(inputBundle, pending.getTimerUpdate(), result);

    TransformWatermarks transformWms = transformToWatermarks.get(transform);
    transformWms.setEventTimeHold(inputBundle == null ? null : inputBundle.getKey(),
        pending.getEarliestHold());
  }

  /**
   * First adds all produced elements to the queue of pending elements for each consumer, then adds
   * all pending timers to the collection of pending timers, then removes all completed and deleted
   * timers from the collection of pending timers, then removes all completed elements from the
   * pending queue of the transform.
   *
   * <p>It is required that all newly pending elements are added to the queue of pending elements
   * for each consumer prior to the completed elements being removed, as doing otherwise could cause
   * a Watermark to appear in a state in which the upstream (completed) element does not hold the
   * watermark but the element it produced is not yet pending. This can cause the watermark to
   * erroneously advance.
   */
  private void updatePending(
      CommittedBundle<?> input,
      TimerUpdate timerUpdate,
      CommittedResult result) {
    // Newly pending elements must be added before completed elements are removed, as the two
    // do not share a Mutex within this call and thus can be interleaved with external calls to
    // refresh.
    for (CommittedBundle<?> bundle : result.getOutputs()) {
      for (AppliedPTransform<?, ?, ?> consumer :
          graph.getPerElementConsumers(bundle.getPCollection())) {
        TransformWatermarks watermarks = transformToWatermarks.get(consumer);
        watermarks.addPending(bundle);
      }
    }

    TransformWatermarks completedTransform = transformToWatermarks.get(result.getTransform());
    if (result.getUnprocessedInputs().isPresent()) {
      // Add the unprocessed inputs
      completedTransform.addPending(result.getUnprocessedInputs().get());
    }
    completedTransform.updateTimers(timerUpdate);
    if (input != null) {
      completedTransform.removePending(input);
    }
  }

  /**
   * Refresh the watermarks contained within this {@link WatermarkManager}, causing all
   * watermarks to be advanced as far as possible.
   */
  synchronized void refreshAll() {
    refreshLock.lock();
    try {
      applyAllPendingUpdates();
      Set<AppliedPTransform<?, ?, ?>> toRefresh = pendingRefreshes;
      while (!toRefresh.isEmpty()) {
        toRefresh = refreshAllOf(toRefresh);
      }
    } finally {
      refreshLock.unlock();
    }
  }

  private Set<AppliedPTransform<?, ?, ?>> refreshAllOf(Set<AppliedPTransform<?, ?, ?>> toRefresh) {
    Set<AppliedPTransform<?, ?, ?>> newRefreshes = new HashSet<>();
    for (AppliedPTransform<?, ?, ?> transform : toRefresh) {
      newRefreshes.addAll(refreshWatermarks(transform));
    }
    return newRefreshes;
  }

  private Set<AppliedPTransform<?, ?, ?>> refreshWatermarks(AppliedPTransform<?, ?, ?> toRefresh) {
    TransformWatermarks myWatermarks = transformToWatermarks.get(toRefresh);
    WatermarkUpdate updateResult = myWatermarks.refresh();
    if (updateResult.isAdvanced()) {
      Set<AppliedPTransform<?, ?, ?>> additionalRefreshes = new HashSet<>();
      for (PValue outputPValue : toRefresh.getOutputs().values()) {
        additionalRefreshes.addAll(graph.getPerElementConsumers(outputPValue));
      }
      return additionalRefreshes;
    }
    return Collections.emptySet();
  }

  /**
   * Returns a map of each {@link PTransform} that has pending timers to those timers. All of the
   * pending timers will be removed from this {@link WatermarkManager}.
   */
  public Collection<FiredTimers> extractFiredTimers() {
    Collection<FiredTimers> allTimers = new ArrayList<>();
    refreshLock.lock();
    try {
      for (Map.Entry<AppliedPTransform<?, ?, ?>, TransformWatermarks> watermarksEntry
          : transformToWatermarks.entrySet()) {
        Collection<FiredTimers> firedTimers = watermarksEntry.getValue().extractFiredTimers();
        allTimers.addAll(firedTimers);
      }
      return allTimers;
    } finally {
      refreshLock.unlock();
    }
  }

  /**
   * A (key, Instant) pair that holds the watermark. Holds are per-key, but the watermark is global,
   * and as such the watermark manager must track holds and the release of holds on a per-key basis.
   *
   * <p>The {@link #compareTo(KeyedHold)} method of {@link KeyedHold} is not consistent with equals,
   * as the key is arbitrarily ordered via identity, rather than object equality.
   */
  private static final class KeyedHold implements Comparable<KeyedHold> {
    private static final Ordering<Object> KEY_ORDERING = Ordering.arbitrary().nullsLast();

    private final Object key;
    private final Instant timestamp;

    /**
     * Create a new KeyedHold with the specified key and timestamp.
     */
    public static KeyedHold of(Object key, Instant timestamp) {
      return new KeyedHold(key, MoreObjects.firstNonNull(timestamp, THE_END_OF_TIME.get()));
    }

    private KeyedHold(Object key, Instant timestamp) {
      this.key = key;
      this.timestamp = timestamp;
    }

    @Override
    public int compareTo(KeyedHold that) {
      return ComparisonChain.start()
          .compare(this.timestamp, that.timestamp)
          .compare(this.key, that.key, KEY_ORDERING)
          .result();
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp, key);
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof KeyedHold)) {
        return false;
      }
      KeyedHold that = (KeyedHold) other;
      return Objects.equals(this.timestamp, that.timestamp) && Objects.equals(this.key, that.key);
    }

    /**
     * Get the value of this {@link KeyedHold}.
     */
    public Instant getTimestamp() {
      return timestamp;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(KeyedHold.class)
          .add("key", key)
          .add("hold", timestamp)
          .toString();
    }
  }

  private static class PerKeyHolds {
    private final Map<Object, KeyedHold> keyedHolds;
    private final NavigableSet<KeyedHold> allHolds;

    private PerKeyHolds() {
      this.keyedHolds = new HashMap<>();
      this.allHolds = new TreeSet<>();
    }

    /**
     * Gets the minimum hold across all keys in this {@link PerKeyHolds}, or THE_END_OF_TIME if
     * there are no holds within this {@link PerKeyHolds}.
     */
    public Instant getMinHold() {
      return allHolds.isEmpty() ? THE_END_OF_TIME.get() : allHolds.first().getTimestamp();
    }

    /**
     * Updates the hold of the provided key to the provided value, removing any other holds for
     * the same key.
     */
    public void updateHold(@Nullable Object key, Instant newHold) {
      removeHold(key);
      KeyedHold newKeyedHold = KeyedHold.of(key, newHold);
      keyedHolds.put(key, newKeyedHold);
      allHolds.add(newKeyedHold);
    }

    /**
     * Removes the hold of the provided key.
     */
    public void removeHold(Object key) {
      KeyedHold oldHold = keyedHolds.remove(key);
      if (oldHold != null) {
        allHolds.remove(oldHold);
      }
    }
  }

  /**
   * A reference to the input and output watermarks of an {@link AppliedPTransform}.
   */
  public class TransformWatermarks {
    private final AppliedPTransform<?, ?, ?> transform;

    private final AppliedPTransformInputWatermark inputWatermark;
    private final AppliedPTransformOutputWatermark outputWatermark;

    private final SynchronizedProcessingTimeInputWatermark synchronizedProcessingInputWatermark;
    private final SynchronizedProcessingTimeOutputWatermark synchronizedProcessingOutputWatermark;

    private Instant latestSynchronizedInputWm;
    private Instant latestSynchronizedOutputWm;

    private TransformWatermarks(
        AppliedPTransform<?, ?, ?> transform,
        AppliedPTransformInputWatermark inputWatermark,
        AppliedPTransformOutputWatermark outputWatermark,
        SynchronizedProcessingTimeInputWatermark inputSynchProcessingWatermark,
        SynchronizedProcessingTimeOutputWatermark outputSynchProcessingWatermark) {
      this.transform = transform;
      this.inputWatermark = inputWatermark;
      this.outputWatermark = outputWatermark;

      this.synchronizedProcessingInputWatermark = inputSynchProcessingWatermark;
      this.synchronizedProcessingOutputWatermark = outputSynchProcessingWatermark;
      this.latestSynchronizedInputWm = BoundedWindow.TIMESTAMP_MIN_VALUE;
      this.latestSynchronizedOutputWm = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    /**
     * Returns the input watermark of the {@link AppliedPTransform}.
     */
    public Instant getInputWatermark() {
      return checkNotNull(inputWatermark.get());
    }

    /**
     * Returns the output watermark of the {@link AppliedPTransform}.
     */
    public Instant getOutputWatermark() {
      return outputWatermark.get();
    }

    /**
     * Returns the synchronized processing input time of the {@link AppliedPTransform}.
     *
     * <p>The returned value is guaranteed to be monotonically increasing, and outside of the
     * presence of holds, will increase as the system time progresses.
     */
    public synchronized Instant getSynchronizedProcessingInputTime() {
      latestSynchronizedInputWm = INSTANT_ORDERING.max(
          latestSynchronizedInputWm,
          INSTANT_ORDERING.min(clock.now(), synchronizedProcessingInputWatermark.get()));
      return latestSynchronizedInputWm;
    }

    /**
     * Returns the synchronized processing output time of the {@link AppliedPTransform}.
     *
     * <p>The returned value is guaranteed to be monotonically increasing, and outside of the
     * presence of holds, will increase as the system time progresses.
     */
    public synchronized Instant getSynchronizedProcessingOutputTime() {
      latestSynchronizedOutputWm = INSTANT_ORDERING.max(
          latestSynchronizedOutputWm,
          INSTANT_ORDERING.min(clock.now(), synchronizedProcessingOutputWatermark.get()));
      return latestSynchronizedOutputWm;
    }

    private WatermarkUpdate refresh() {
      inputWatermark.refresh();
      synchronizedProcessingInputWatermark.refresh();
      WatermarkUpdate eventOutputUpdate = outputWatermark.refresh();
      WatermarkUpdate syncOutputUpdate = synchronizedProcessingOutputWatermark.refresh();
      return eventOutputUpdate.union(syncOutputUpdate);
    }

    private void setEventTimeHold(Object key, Instant newHold) {
      outputWatermark.updateHold(key, newHold);
    }

    private void removePending(CommittedBundle<?> bundle) {
      inputWatermark.removePending(bundle);
      synchronizedProcessingInputWatermark.removePending(bundle);
    }

    private void addPending(CommittedBundle<?> bundle) {
      inputWatermark.addPending(bundle);
      synchronizedProcessingInputWatermark.addPending(bundle);
    }

    private Collection<FiredTimers> extractFiredTimers() {
      Map<StructuralKey<?>, List<TimerData>> eventTimeTimers =
          inputWatermark.extractFiredEventTimeTimers();
      Map<StructuralKey<?>, List<TimerData>> processingTimers;
      Map<StructuralKey<?>, List<TimerData>> synchronizedTimers;
      processingTimers = synchronizedProcessingInputWatermark.extractFiredDomainTimers(
          TimeDomain.PROCESSING_TIME, clock.now());
      synchronizedTimers = synchronizedProcessingInputWatermark.extractFiredDomainTimers(
          TimeDomain.SYNCHRONIZED_PROCESSING_TIME, getSynchronizedProcessingInputTime());

      Map<StructuralKey<?>, List<TimerData>> timersPerKey =
          groupFiredTimers(eventTimeTimers, processingTimers, synchronizedTimers);
      Collection<FiredTimers> keyFiredTimers = new ArrayList<>(timersPerKey.size());
      for (Map.Entry<StructuralKey<?>, List<TimerData>> firedTimers :
          timersPerKey.entrySet()) {
        keyFiredTimers.add(
            new FiredTimers(transform, firedTimers.getKey(), firedTimers.getValue()));
      }
      return keyFiredTimers;
    }

    @SafeVarargs
    private final Map<StructuralKey<?>, List<TimerData>> groupFiredTimers(
        Map<StructuralKey<?>, List<TimerData>>... timersToGroup) {
      Map<StructuralKey<?>, List<TimerData>> groupedTimers = new HashMap<>();
      for (Map<StructuralKey<?>, List<TimerData>> subGroup : timersToGroup) {
        for (Map.Entry<StructuralKey<?>, List<TimerData>> newTimers : subGroup.entrySet()) {
          List<TimerData> grouped =
              groupedTimers.computeIfAbsent(newTimers.getKey(), k -> new ArrayList<>());
          grouped.addAll(newTimers.getValue());
        }
      }
      return groupedTimers;
    }

    private void updateTimers(TimerUpdate update) {
      inputWatermark.updateTimers(update);
      synchronizedProcessingInputWatermark.updateTimers(update);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(TransformWatermarks.class)
          .add("inputWatermark", inputWatermark)
          .add("outputWatermark", outputWatermark)
          .add("inputProcessingTime", synchronizedProcessingInputWatermark)
          .add("outputProcessingTime", synchronizedProcessingOutputWatermark)
          .toString();
    }
  }

  /**
   * A collection of newly set, deleted, and completed timers.
   *
   * <p>setTimers and deletedTimers are collections of {@link TimerData} that have been added to the
   * {@link TimerInternals} of an executed step. completedTimers are timers that were delivered as
   * the input to the executed step.
   */
  public static class TimerUpdate {
    private final StructuralKey<?> key;
    private final Iterable<? extends TimerData> completedTimers;

    private final Iterable<? extends TimerData> setTimers;
    private final Iterable<? extends TimerData> deletedTimers;

    /**
     * Returns a TimerUpdate for a null key with no timers.
     */
    public static TimerUpdate empty() {
      return new TimerUpdate(
          null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Creates a new {@link TimerUpdate} builder with the provided completed timers that needs the
     * set and deleted timers to be added to it.
     */
    public static TimerUpdateBuilder builder(StructuralKey<?> key) {
      return new TimerUpdateBuilder(key);
    }

    /**
     * A {@link TimerUpdate} builder that needs to be provided with set timers and deleted timers.
     */
    public static final class TimerUpdateBuilder {
      private final StructuralKey<?> key;
      private final Collection<TimerData> completedTimers;
      private final Collection<TimerData> setTimers;
      private final Collection<TimerData> deletedTimers;

      private TimerUpdateBuilder(StructuralKey<?> key) {
        this.key = key;
        this.completedTimers = new LinkedHashSet<>();
        this.setTimers = new LinkedHashSet<>();
        this.deletedTimers = new LinkedHashSet<>();
      }

      /**
       * Adds all of the provided timers to the collection of completed timers, and returns this
       * {@link TimerUpdateBuilder}.
       */
      public TimerUpdateBuilder withCompletedTimers(Iterable<TimerData> completedTimers) {
        Iterables.addAll(this.completedTimers, completedTimers);
        return this;
      }

      /**
       * Adds the provided timer to the collection of set timers, removing it from deleted timers if
       * it has previously been deleted. Returns this {@link TimerUpdateBuilder}.
       */
      public TimerUpdateBuilder setTimer(TimerData setTimer) {
        checkArgument(
            setTimer.getTimestamp().isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE),
            "Got a timer for after the end of time (%s), got %s",
            BoundedWindow.TIMESTAMP_MAX_VALUE,
            setTimer.getTimestamp());
        deletedTimers.remove(setTimer);
        setTimers.add(setTimer);
        return this;
      }

      /**
       * Adds the provided timer to the collection of deleted timers, removing it from set timers if
       * it has previously been set. Returns this {@link TimerUpdateBuilder}.
       */
      public TimerUpdateBuilder deletedTimer(TimerData deletedTimer) {
        deletedTimers.add(deletedTimer);
        setTimers.remove(deletedTimer);
        return this;
      }

      /**
       * Returns a new {@link TimerUpdate} with the most recently set completedTimers, setTimers,
       * and deletedTimers.
       */
      public TimerUpdate build() {
        return new TimerUpdate(
            key,
            ImmutableList.copyOf(completedTimers),
            ImmutableList.copyOf(setTimers),
            ImmutableList.copyOf(deletedTimers));
      }
    }

    private TimerUpdate(
        StructuralKey<?> key,
        Iterable<? extends TimerData> completedTimers,
        Iterable<? extends TimerData> setTimers,
        Iterable<? extends TimerData> deletedTimers) {
      this.key = key;
      this.completedTimers = completedTimers;
      this.setTimers = setTimers;
      this.deletedTimers = deletedTimers;
    }

    @VisibleForTesting
    StructuralKey<?> getKey() {
      return key;
    }

    @VisibleForTesting
    Iterable<? extends TimerData> getCompletedTimers() {
      return completedTimers;
    }

    @VisibleForTesting
    Iterable<? extends TimerData> getSetTimers() {
      return setTimers;
    }

    @VisibleForTesting
    Iterable<? extends TimerData> getDeletedTimers() {
      return deletedTimers;
    }

    /**
     * Returns a {@link TimerUpdate} that is like this one, but with the specified completed timers.
     */
    public TimerUpdate withCompletedTimers(Iterable<TimerData> completedTimers) {
      return new TimerUpdate(this.key, completedTimers, setTimers, deletedTimers);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, completedTimers, setTimers, deletedTimers);
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof TimerUpdate)) {
        return false;
      }
      TimerUpdate that = (TimerUpdate) other;
      return Objects.equals(this.key, that.key)
          && Objects.equals(this.completedTimers, that.completedTimers)
          && Objects.equals(this.setTimers, that.setTimers)
          && Objects.equals(this.deletedTimers, that.deletedTimers);
    }
  }

  /**
   * A pair of {@link TimerData} and key which can be delivered to the appropriate
   * {@link AppliedPTransform}. A timer fires at the transform that set it with a specific key when
   * the time domain in which it lives progresses past a specified time, as determined by the
   * {@link WatermarkManager}.
   */
  public static class FiredTimers {
    /** The transform the timers were set at and will be delivered to. */
    private final AppliedPTransform<?, ?, ?> transform;
    /** The key the timers were set for and will be delivered to. */
    private final StructuralKey<?> key;
    private final Collection<TimerData> timers;

    private FiredTimers(
        AppliedPTransform<?, ?, ?> transform, StructuralKey<?> key, Collection<TimerData> timers) {
      this.transform = transform;
      this.key = key;
      this.timers = timers;
    }

    public AppliedPTransform<?, ?, ?> getTransform() {
      return transform;
    }

    public StructuralKey<?> getKey() {
      return key;
    }

    /**
     * Gets all of the timers that have fired within the provided {@link TimeDomain}. If no timers
     * fired within the provided domain, return an empty collection.
     *
     * <p>Timers within a {@link TimeDomain} are guaranteed to be in order of increasing timestamp.
     */
    public Collection<TimerData> getTimers() {
      return timers;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(FiredTimers.class).add("timers", timers).toString();
    }
  }

  private static class BundleByElementTimestampComparator extends Ordering<CommittedBundle<?>>
      implements Serializable {
    @Override
    public int compare(CommittedBundle<?> o1, CommittedBundle<?> o2) {
      return ComparisonChain.start()
          .compare(o1.getMinimumTimestamp(), o2.getMinimumTimestamp())
          .result();
    }
  }

  public Set<AppliedPTransform<?, ?, ?>> getCompletedTransforms() {
    Set<AppliedPTransform<?, ?, ?>> result = new HashSet<>();
    for (Map.Entry<AppliedPTransform<?, ?, ?>, TransformWatermarks> wms :
        transformToWatermarks.entrySet()) {
      if (wms.getValue().getOutputWatermark().equals(THE_END_OF_TIME.get())) {
        result.add(wms.getKey());
      }
    }
    return result;
  }

  @AutoValue
  abstract static class PendingWatermarkUpdate {
    @Nullable
    public abstract CommittedBundle<?> getInputBundle();
    public abstract TimerUpdate getTimerUpdate();
    public abstract CommittedResult getResult();
    public abstract Instant getEarliestHold();

    /**
     * Gets the {@link AppliedPTransform} that generated this result.
     */
    public AppliedPTransform<?, ?, ?> getTransform() {
      return getResult().getTransform();
    }

    public static PendingWatermarkUpdate create(
        CommittedBundle<?> inputBundle,
        TimerUpdate timerUpdate,
        CommittedResult result, Instant earliestHold) {
      return new AutoValue_WatermarkManager_PendingWatermarkUpdate(inputBundle,
          timerUpdate,
          result,
          earliestHold);
    }
  }
}
