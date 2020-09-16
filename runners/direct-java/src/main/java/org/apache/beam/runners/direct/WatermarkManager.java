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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.local.Bundle;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ComparisonChain;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Ordering;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.SortedMultiset;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Table;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.TreeMultiset;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Manages watermarks of {@link PCollection PCollections} and input and output watermarks of {@link
 * AppliedPTransform AppliedPTransforms} to provide event-time and completion tracking for in-memory
 * execution. {@link WatermarkManager} is designed to update and return a consistent view of
 * watermarks in the presence of concurrent updates.
 *
 * <p>An {@link WatermarkManager} is provided with the collection of root {@link AppliedPTransform
 * AppliedPTransforms} and a map of {@link PCollection PCollections} to all the {@link
 * AppliedPTransform AppliedPTransforms} that consume them at construction time.
 *
 * <p>Whenever a root {@link AppliedPTransform executable} produces elements, the {@link
 * WatermarkManager} is provided with the produced elements and the output watermark of the
 * producing {@link AppliedPTransform executable}. The {@link WatermarkManager watermark manager} is
 * responsible for computing the watermarks of all {@link AppliedPTransform transforms} that consume
 * one or more {@link PCollection PCollections}.
 *
 * <p>Whenever a non-root {@link AppliedPTransform} finishes processing one or more in-flight
 * elements (referred to as the input {@link CommittedBundle bundle}), the following occurs
 * atomically:
 *
 * <ul>
 *   <li>All of the in-flight elements are removed from the collection of pending elements for the
 *       {@link AppliedPTransform}.
 *   <li>All of the elements produced by the {@link AppliedPTransform} are added to the collection
 *       of pending elements for each {@link AppliedPTransform} that consumes them.
 *   <li>The input watermark for the {@link AppliedPTransform} becomes the maximum value of
 *       <ul>
 *         <li>the previous input watermark
 *         <li>the minimum of
 *             <ul>
 *               <li>the timestamps of all currently pending elements
 *               <li>all input {@link PCollection} watermarks
 *             </ul>
 *       </ul>
 *   <li>The output watermark for the {@link AppliedPTransform} becomes the maximum of
 *       <ul>
 *         <li>the previous output watermark
 *         <li>the minimum of
 *             <ul>
 *               <li>the current input watermark
 *               <li>the current watermark holds
 *             </ul>
 *       </ul>
 *   <li>The watermark of the output {@link PCollection} can be advanced to the output watermark of
 *       the {@link AppliedPTransform}
 *   <li>The watermark of all downstream {@link AppliedPTransform AppliedPTransforms} can be
 *       advanced.
 * </ul>
 *
 * <p>The watermark of a {@link PCollection} is equal to the output watermark of the {@link
 * AppliedPTransform} that produces it.
 *
 * <p>The watermarks for a {@link PTransform} are updated as follows when output is committed:
 *
 * <pre>
 * Watermark_In'  = MAX(Watermark_In, MIN(U(TS_Pending), U(Watermark_InputPCollection)))
 * Watermark_Out' = MAX(Watermark_Out, MIN(Watermark_In', U(StateHold)))
 * Watermark_PCollection = Watermark_Out_ProducingPTransform
 * </pre>
 */
public class WatermarkManager<ExecutableT, CollectionT> {
  // The number of updates to apply in #tryApplyPendingUpdates
  private static final int MAX_INCREMENTAL_UPDATES = 10;

  /**
   * The watermark of some {@link Pipeline} element, usually a {@link PTransform} or a {@link
   * PCollection}.
   *
   * <p>A watermark is a monotonically increasing value, which represents the point up to which the
   * system believes it has received all of the data. Data that arrives with a timestamp that is
   * before the watermark is considered late. {@link BoundedWindow#TIMESTAMP_MAX_VALUE} is a special
   * timestamp which indicates we have received all of the data and there will be no more on-time or
   * late data. This value is represented by {@link WatermarkManager#THE_END_OF_TIME}.
   */
  @VisibleForTesting
  interface Watermark {
    String getName();

    /** Returns the current value of this watermark. */
    Instant get();

    /**
     * Refreshes the value of this watermark from its input watermarks and watermark holds.
     *
     * @return true if the value of the watermark has changed (and thus dependent watermark must
     *     also be updated
     */
    WatermarkUpdate refresh();
  }

  /** The result of computing a {@link Watermark}. */
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
     * Returns the {@link WatermarkUpdate} based on the former and current {@link Instant
     * timestamps}.
     */
    public static WatermarkUpdate fromTimestamps(Instant oldTime, Instant currentTime) {
      if (currentTime.isAfter(oldTime)) {
        return ADVANCED;
      }
      return NO_CHANGE;
    }
  }

  private static WatermarkUpdate updateAndTrace(String name, Instant oldTime, Instant currentTime) {
    WatermarkUpdate res = WatermarkUpdate.fromTimestamps(oldTime, currentTime);
    if (res.isAdvanced()) {
      WindowTracing.debug("Watermark {} advanced from {} to {}", name, oldTime, currentTime);
    }
    return res;
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
  @VisibleForTesting
  static class AppliedPTransformInputWatermark implements Watermark {
    private final String name;

    private final Collection<? extends Watermark> inputWatermarks;
    private final SortedMultiset<Bundle<?, ?>> pendingElements;

    // This tracks only the quantity of timers at each timestamp, for quickly getting the cross-key
    // minimum
    private final SortedMultiset<TimerData> pendingTimers;

    // Entries in this table represent the authoritative timestamp for which
    // a per-key-and-StateNamespace timer is set.
    private final Map<StructuralKey<?>, Table<StateNamespace, String, TimerData>> existingTimers;

    // This per-key sorted set allows quick retrieval of timers that should fire for a key
    private final Map<StructuralKey<?>, NavigableSet<TimerData>> objectTimers;

    private final AtomicReference<Instant> currentWatermark;

    private final Consumer<TimerData> timerUpdateNotification;

    public AppliedPTransformInputWatermark(
        String name,
        Collection<? extends Watermark> inputWatermarks,
        Consumer<TimerData> timerUpdateNotification) {

      this.name = name;
      this.inputWatermarks = inputWatermarks;

      // The ordering must order elements by timestamp, and must not compare two distinct elements
      // as equal. This is built on the assumption that any element added as a pending element will
      // be consumed without modifications.
      //
      // The same logic is applied for pending timers
      Ordering<Bundle<?, ?>> pendingBundleComparator =
          new BundleByElementTimestampComparator().compound(Ordering.arbitrary());
      this.pendingElements = TreeMultiset.create(pendingBundleComparator);
      this.pendingTimers = TreeMultiset.create();
      this.objectTimers = new HashMap<>();
      this.existingTimers = new HashMap<>();
      this.currentWatermark = new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);
      this.timerUpdateNotification = timerUpdateNotification;
    }

    @Override
    public String getName() {
      return name;
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
     *
     * <ul>
     *   <li>the previous input watermark
     *   <li>the minimum of
     *       <ul>
     *         <li>the timestamps of all currently pending elements
     *         <li>all input {@link PCollection} watermarks
     *       </ul>
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
      return updateAndTrace(getName(), oldWatermark, newWatermark);
    }

    private synchronized void addPending(Bundle<?, ?> newPending) {
      pendingElements.add(newPending);
    }

    private synchronized void removePending(Bundle<?, ?> completed) {
      pendingElements.remove(completed);
    }

    @VisibleForTesting
    synchronized Instant getEarliestTimerTimestamp() {
      if (pendingTimers.isEmpty()) {
        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      } else {
        return getMinimumOutputTimestamp(pendingTimers);
      }
    }

    private Instant getMinimumOutputTimestamp(SortedMultiset<TimerData> timers) {
      Instant minimumOutputTimestamp = timers.firstEntry().getElement().getOutputTimestamp();
      for (TimerData timerData : timers) {
        minimumOutputTimestamp =
            INSTANT_ORDERING.min(timerData.getOutputTimestamp(), minimumOutputTimestamp);
      }
      return minimumOutputTimestamp;
    }

    @VisibleForTesting
    synchronized void updateTimers(TimerUpdate update) {
      NavigableSet<TimerData> keyTimers =
          objectTimers.computeIfAbsent(update.key, k -> new TreeSet<>());
      Table<StateNamespace, String, TimerData> existingTimersForKey =
          existingTimers.computeIfAbsent(update.key, k -> HashBasedTable.create());

      for (TimerData timer : update.getSetTimers()) {
        if (TimeDomain.EVENT_TIME.equals(timer.getDomain())) {
          @Nullable
          TimerData existingTimer =
              existingTimersForKey.get(
                  timer.getNamespace(), timer.getTimerId() + '+' + timer.getTimerFamilyId());

          if (existingTimer == null) {
            pendingTimers.add(timer);
            keyTimers.add(timer);
          } else {
            // reinitialize the timer even if identical,
            // because it might be removed from objectTimers
            // by timer push back
            pendingTimers.remove(existingTimer);
            keyTimers.remove(existingTimer);
            pendingTimers.add(timer);
            keyTimers.add(timer);
          }

          existingTimersForKey.put(
              timer.getNamespace(), timer.getTimerId() + '+' + timer.getTimerFamilyId(), timer);
        }
      }

      for (TimerData timer : update.getDeletedTimers()) {
        if (TimeDomain.EVENT_TIME.equals(timer.getDomain())) {
          @Nullable
          TimerData existingTimer =
              existingTimersForKey.get(
                  timer.getNamespace(), timer.getTimerId() + '+' + timer.getTimerFamilyId());

          if (existingTimer != null) {
            pendingTimers.remove(existingTimer);
            keyTimers.remove(existingTimer);
            existingTimersForKey.remove(
                existingTimer.getNamespace(),
                existingTimer.getTimerId() + '+' + existingTimer.getTimerFamilyId());
          }
        }
      }

      for (TimerData timer : update.getCompletedTimers()) {
        if (TimeDomain.EVENT_TIME.equals(timer.getDomain())) {
          keyTimers.remove(timer);
          pendingTimers.remove(timer);
        }
      }

      if (!update.isEmpty()) {
        // notify of TimerData update
        Iterables.concat(
                update.getCompletedTimers(), update.getDeletedTimers(), update.getSetTimers())
            .forEach(timerUpdateNotification);
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
   * current watermark hold and the {@link AppliedPTransformInputWatermark} for the same {@link
   * AppliedPTransform}, restricted to be monotonically increasing. See {@link #refresh()} for more
   * information.
   */
  private static class AppliedPTransformOutputWatermark implements Watermark {
    private final String name;

    private final AppliedPTransformInputWatermark inputWatermark;
    private final PerKeyHolds holds;
    private AtomicReference<Instant> currentWatermark;

    public AppliedPTransformOutputWatermark(
        String name, AppliedPTransformInputWatermark inputWatermark) {
      this.name = name;
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
    public String getName() {
      return name;
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
     *
     * <ul>
     *   <li>the previous output watermark
     *   <li>the minimum of
     *       <ul>
     *         <li>the current input watermark
     *         <li>the current watermark holds
     *       </ul>
     * </ul>
     */
    @Override
    public synchronized WatermarkUpdate refresh() {
      Instant oldWatermark = currentWatermark.get();
      Instant newWatermark =
          INSTANT_ORDERING.min(
              inputWatermark.get(), holds.getMinHold(), inputWatermark.getEarliestTimerTimestamp());

      newWatermark = INSTANT_ORDERING.max(oldWatermark, newWatermark);
      currentWatermark.set(newWatermark);
      return updateAndTrace(getName(), oldWatermark, newWatermark);
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
   * The input {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} hold for an {@link
   * AppliedPTransform}.
   *
   * <p>At any point, the hold value of an {@link SynchronizedProcessingTimeInputWatermark} is equal
   * to the minimum across all pending bundles at the {@link AppliedPTransform} and all upstream
   * {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} watermarks. The value of the input synchronized
   * processing time at any step is equal to the maximum of:
   *
   * <ul>
   *   <li>The most recently returned synchronized processing input time
   *   <li>The minimum of
   *       <ul>
   *         <li>The current processing time
   *         <li>The current synchronized processing time input hold
   *       </ul>
   * </ul>
   */
  private static class SynchronizedProcessingTimeInputWatermark implements Watermark {
    private final String name;

    private final Collection<? extends Watermark> inputWms;
    private final Collection<Bundle<?, ?>> pendingBundles;
    private final Map<StructuralKey<?>, NavigableSet<TimerData>> processingTimers;
    private final Map<StructuralKey<?>, NavigableSet<TimerData>> synchronizedProcessingTimers;
    private final Map<StructuralKey<?>, Table<StateNamespace, String, TimerData>> existingTimers;

    private final NavigableSet<TimerData> pendingTimers;

    private AtomicReference<Instant> earliestHold;

    private final Consumer<TimerData> timerUpdateNotification;

    public SynchronizedProcessingTimeInputWatermark(
        String name,
        Collection<? extends Watermark> inputWms,
        Consumer<TimerData> timerUpdateNotification) {

      this.name = name;
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
      this.earliestHold = new AtomicReference<>(initialHold);
      this.timerUpdateNotification = timerUpdateNotification;
    }

    @Override
    public String getName() {
      return name;
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
     *
     * <ul>
     *   <li>the timestamps of all currently pending bundles
     *   <li>all input {@link PCollection} synchronized processing time watermarks
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
      for (Bundle<?, ?> bundle : pendingBundles) {
        // TODO: Track elements in the bundle by the processing time they were output instead of
        // entire bundles. Requried to support arbitrarily splitting and merging bundles between
        // steps
        minTime = INSTANT_ORDERING.min(minTime, bundle.getSynchronizedProcessingOutputWatermark());
      }
      earliestHold.set(minTime);
      return updateAndTrace(getName(), oldHold, minTime);
    }

    public synchronized void addPending(Bundle<?, ?> bundle) {
      pendingBundles.add(bundle);
    }

    public synchronized void removePending(Bundle<?, ?> bundle) {
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
          earliest = INSTANT_ORDERING.min(getMinimumOutputTimestamp(timers), earliest);
        }
      }
      for (NavigableSet<TimerData> timers : synchronizedProcessingTimers.values()) {
        if (!timers.isEmpty()) {
          earliest = INSTANT_ORDERING.min(getMinimumOutputTimestamp(timers), earliest);
        }
      }
      if (!pendingTimers.isEmpty()) {
        earliest = INSTANT_ORDERING.min(getMinimumOutputTimestamp(pendingTimers), earliest);
      }
      return earliest;
    }

    private Instant getMinimumOutputTimestamp(NavigableSet<TimerData> timers) {
      Instant minimumOutputTimestamp = timers.first().getOutputTimestamp();
      for (TimerData timerData : timers) {
        minimumOutputTimestamp =
            INSTANT_ORDERING.min(timerData.getOutputTimestamp(), minimumOutputTimestamp);
      }
      return minimumOutputTimestamp;
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
            existingTimersForKey.get(
                addedTimer.getNamespace(),
                addedTimer.getTimerId() + '+' + addedTimer.getTimerFamilyId());
        if (existingTimer == null) {
          timerQueue.add(addedTimer);
        } else if (!existingTimer.equals(addedTimer)) {
          timerQueue.remove(existingTimer);
          timerQueue.add(addedTimer);
        } // else the timer is already set identically, so noop.

        existingTimersForKey.put(
            addedTimer.getNamespace(),
            addedTimer.getTimerId() + '+' + addedTimer.getTimerFamilyId(),
            addedTimer);
      }

      for (TimerData deletedTimer : update.deletedTimers) {
        NavigableSet<TimerData> timerQueue = timerMap.get(deletedTimer.getDomain());
        if (timerQueue == null) {
          continue;
        }

        @Nullable
        TimerData existingTimer =
            existingTimersForKey.get(
                deletedTimer.getNamespace(),
                deletedTimer.getTimerId() + '+' + deletedTimer.getTimerFamilyId());

        if (existingTimer != null) {
          pendingTimers.remove(deletedTimer);
          timerQueue.remove(deletedTimer);
          existingTimersForKey.remove(
              existingTimer.getNamespace(),
              existingTimer.getTimerId() + '+' + existingTimer.getTimerFamilyId());
        }
      }

      for (TimerData completedTimer : update.completedTimers) {
        pendingTimers.remove(completedTimer);
      }

      // notify of TimerData update
      Iterables.concat(
              update.getCompletedTimers(), update.getDeletedTimers(), update.getSetTimers())
          .forEach(timerUpdateNotification);
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
   * The output {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} hold for an {@link
   * AppliedPTransform}.
   *
   * <p>At any point, the hold value of an {@link SynchronizedProcessingTimeOutputWatermark} is
   * equal to the minimum across all incomplete timers at the {@link AppliedPTransform} and all
   * upstream {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} watermarks. The value of the output
   * synchronized processing time at any step is equal to the maximum of:
   *
   * <ul>
   *   <li>The most recently returned synchronized processing output time
   *   <li>The minimum of
   *       <ul>
   *         <li>The current processing time
   *         <li>The current synchronized processing time output hold
   *       </ul>
   * </ul>
   */
  private static class SynchronizedProcessingTimeOutputWatermark implements Watermark {
    private final String name;

    private final SynchronizedProcessingTimeInputWatermark inputWm;
    private final PerKeyHolds holds;
    private AtomicReference<Instant> latestRefresh;

    public SynchronizedProcessingTimeOutputWatermark(
        String name, SynchronizedProcessingTimeInputWatermark inputWm) {
      this.name = name;
      this.inputWm = inputWm;
      holds = new PerKeyHolds();
      this.latestRefresh = new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    public synchronized void updateHold(Object key, Instant newHold) {
      if (newHold == null) {
        holds.removeHold(key);
      } else {
        holds.updateHold(key, newHold);
      }
    }

    @Override
    public String getName() {
      return name;
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
     *
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
          INSTANT_ORDERING.min(
              inputWm.get(), holds.getMinHold(), inputWm.getEarliestTimerTimestamp());
      latestRefresh.set(newTimestamp);
      return updateAndTrace(getName(), oldRefresh, newTimestamp);
    }

    @Override
    public synchronized String toString() {
      return MoreObjects.toStringHelper(SynchronizedProcessingTimeOutputWatermark.class)
          .add("holds", holds)
          .add("latestRefresh", latestRefresh)
          .toString();
    }
  }

  /**
   * The {@code Watermark} that is after the latest time it is possible to represent in the global
   * window. This is a distinguished value representing a complete {@link PTransform}.
   */
  private static final Watermark THE_END_OF_TIME =
      new Watermark() {
        @Override
        public String getName() {
          return "THE_END_OF_TIME";
        }

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
  private final ExecutableGraph<ExecutableT, CollectionT> graph;

  private final Function<ExecutableT, String> getName;

  /** The input and output watermark of each {@link AppliedPTransform}. */
  private final Map<ExecutableT, TransformWatermarks> transformToWatermarks;

  /** A queue of pending updates to the state of this {@link WatermarkManager}. */
  private final ConcurrentLinkedQueue<PendingWatermarkUpdate<ExecutableT, CollectionT>>
      pendingUpdates;

  /** A lock used to control concurrency for updating pending values. */
  private final Lock refreshLock;

  /**
   * A queue of pending {@link AppliedPTransform AppliedPTransforms} that have potentially stale
   * data.
   */
  @GuardedBy("refreshLock")
  private final Set<ExecutableT> pendingRefreshes;

  /**
   * A set of executables with currently extracted timers, that are to be processed. Note that, due
   * to consistency, we can have only single extracted set of timers that are being processed by
   * bundle processor at a time.
   */
  private final Map<ExecutableT, Set<String>> transformsWithAlreadyExtractedTimers =
      new ConcurrentHashMap<>();

  /**
   * Creates a new {@link WatermarkManager}. All watermarks within the newly created {@link
   * WatermarkManager} start at {@link BoundedWindow#TIMESTAMP_MIN_VALUE}, the minimum watermark,
   * with no watermark holds or pending elements.
   *
   * @param clock the clock to use to determine processing time
   * @param graph the graph representing this pipeline
   * @param getName a function for producing a short identifier for the executable in watermark
   *     tracing log messages.
   */
  public static <ExecutableT, CollectionT>
      WatermarkManager<ExecutableT, ? super CollectionT> create(
          Clock clock,
          ExecutableGraph<ExecutableT, ? super CollectionT> graph,
          Function<ExecutableT, String> getName) {
    return new WatermarkManager<>(clock, graph, getName);
  }

  private WatermarkManager(
      Clock clock,
      ExecutableGraph<ExecutableT, CollectionT> graph,
      Function<ExecutableT, String> getName) {
    this.clock = clock;
    this.graph = graph;
    this.getName = getName;

    this.pendingUpdates = new ConcurrentLinkedQueue<>();

    this.refreshLock = new ReentrantLock();
    this.pendingRefreshes = new HashSet<>();

    transformToWatermarks = new HashMap<>();

    for (ExecutableT rootTransform : graph.getRootTransforms()) {
      getTransformWatermark(rootTransform);
    }
    for (ExecutableT primitiveTransform : graph.getExecutables()) {
      getTransformWatermark(primitiveTransform);
    }
  }

  private TransformWatermarks getValueWatermark(CollectionT value) {
    return getTransformWatermark(graph.getProducer(value));
  }

  private TransformWatermarks getTransformWatermark(ExecutableT executable) {
    String name = getName.apply(executable);

    TransformWatermarks wms = transformToWatermarks.get(executable);
    if (wms == null) {
      List<Watermark> inputCollectionWatermarks = getInputWatermarks(executable);
      AppliedPTransformInputWatermark inputWatermark =
          new AppliedPTransformInputWatermark(
              name + ".in",
              inputCollectionWatermarks,
              timerUpdateConsumer(transformsWithAlreadyExtractedTimers, executable));
      AppliedPTransformOutputWatermark outputWatermark =
          new AppliedPTransformOutputWatermark(name + ".out", inputWatermark);

      SynchronizedProcessingTimeInputWatermark inputProcessingWatermark =
          new SynchronizedProcessingTimeInputWatermark(
              name + ".inProcessing",
              getInputProcessingWatermarks(executable),
              timerUpdateConsumer(transformsWithAlreadyExtractedTimers, executable));
      SynchronizedProcessingTimeOutputWatermark outputProcessingWatermark =
          new SynchronizedProcessingTimeOutputWatermark(
              name + ".outProcessing", inputProcessingWatermark);

      wms =
          new TransformWatermarks(
              executable,
              inputWatermark,
              outputWatermark,
              inputProcessingWatermark,
              outputProcessingWatermark);
      transformToWatermarks.put(executable, wms);
    }
    return wms;
  }

  private static <ExecutableT> Consumer<TimerData> timerUpdateConsumer(
      Map<ExecutableT, Set<String>> transformsWithAlreadyExtractedTimers, ExecutableT executable) {

    return update -> {
      String timerIdWithNs = TimerUpdate.getTimerIdAndTimerFamilyIdWithNamespace(update);
      transformsWithAlreadyExtractedTimers.compute(
          executable,
          (k, v) -> {
            if (v != null) {
              v.remove(timerIdWithNs);
              if (v.isEmpty()) {
                v = null;
              }
            }
            return v;
          });
    };
  }

  private Collection<Watermark> getInputProcessingWatermarks(ExecutableT executable) {
    ImmutableList.Builder<Watermark> inputWmsBuilder = ImmutableList.builder();
    Collection<CollectionT> inputs = graph.getPerElementInputs(executable);
    if (inputs.isEmpty()) {
      inputWmsBuilder.add(THE_END_OF_TIME);
    }
    for (CollectionT input : inputs) {
      Watermark producerOutputWatermark =
          getValueWatermark(input).synchronizedProcessingOutputWatermark;
      inputWmsBuilder.add(producerOutputWatermark);
    }
    return inputWmsBuilder.build();
  }

  private List<Watermark> getInputWatermarks(ExecutableT executable) {
    ImmutableList.Builder<Watermark> inputWatermarksBuilder = ImmutableList.builder();
    Collection<CollectionT> inputs = graph.getPerElementInputs(executable);
    if (inputs.isEmpty()) {
      inputWatermarksBuilder.add(THE_END_OF_TIME);
    }
    for (CollectionT input : inputs) {
      Watermark producerOutputWatermark = getValueWatermark(input).outputWatermark;
      inputWatermarksBuilder.add(producerOutputWatermark);
    }
    return inputWatermarksBuilder.build();
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Gets the input and output watermarks for an {@link AppliedPTransform}. If the {@link
   * AppliedPTransform PTransform} has not processed any elements, return a watermark of {@link
   * BoundedWindow#TIMESTAMP_MIN_VALUE}.
   *
   * @return a snapshot of the input watermark and output watermark for the provided executable
   */
  public TransformWatermarks getWatermarks(ExecutableT executable) {
    return transformToWatermarks.get(executable);
  }

  public void initialize(
      Map<ExecutableT, ? extends Iterable<Bundle<?, CollectionT>>> initialBundles) {
    refreshLock.lock();
    try {
      for (Map.Entry<ExecutableT, ? extends Iterable<Bundle<?, CollectionT>>> rootEntry :
          initialBundles.entrySet()) {
        TransformWatermarks rootWms = transformToWatermarks.get(rootEntry.getKey());
        for (Bundle<?, ? extends CollectionT> initialBundle : rootEntry.getValue()) {
          rootWms.addPending(initialBundle);
        }
        pendingRefreshes.add(rootEntry.getKey());
      }
    } finally {
      refreshLock.unlock();
    }
  }

  /**
   * Updates the watermarks of a executable with one or more inputs.
   *
   * <p>Each executable has two monotonically increasing watermarks: the input watermark, which can,
   * at any time, be updated to equal:
   *
   * <pre>
   * MAX(CurrentInputWatermark, MIN(PendingElements, InputPCollectionWatermarks))
   * </pre>
   *
   * and the output watermark, which can, at any time, be updated to equal:
   *
   * <pre>
   * MAX(CurrentOutputWatermark, MIN(InputWatermark, WatermarkHolds))
   * </pre>
   *
   * .
   *
   * <p>Updates to watermarks may not be immediately visible.
   *
   * @param completed the input that has completed
   * @param timerUpdate the timers that were added, removed, and completed as part of producing this
   *     update
   * @param executable the executable applied to {@code completed} to produce the outputs
   * @param unprocessedInputs inputs that could not be processed
   * @param outputs outputs that were produced by the application of the {@code executable} to the
   *     input
   * @param earliestHold the earliest watermark hold in the executable's state.
   */
  public void updateWatermarks(
      @Nullable Bundle<?, ? extends CollectionT> completed,
      TimerUpdate timerUpdate,
      ExecutableT executable,
      @Nullable Bundle<?, ? extends CollectionT> unprocessedInputs,
      Iterable<? extends Bundle<?, ? extends CollectionT>> outputs,
      Instant earliestHold) {
    pendingUpdates.offer(
        PendingWatermarkUpdate.create(
            executable, completed, timerUpdate, unprocessedInputs, outputs, earliestHold));
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
   * Applies all pending updates to this {@link WatermarkManager}, causing the pending state of all
   * {@link TransformWatermarks} to be advanced as far as possible.
   */
  private void applyAllPendingUpdates() {
    refreshLock.lock();
    try {
      applyNUpdates(-1);
    } finally {
      refreshLock.unlock();
    }
  }

  /** Applies up to {@code numUpdates}, or all available updates if numUpdates is non-positive. */
  @GuardedBy("refreshLock")
  private void applyNUpdates(int numUpdates) {
    for (int i = 0; !pendingUpdates.isEmpty() && (i < numUpdates || numUpdates <= 0); i++) {
      PendingWatermarkUpdate<ExecutableT, CollectionT> pending = pendingUpdates.poll();
      applyPendingUpdate(pending);
      pendingRefreshes.add(pending.getExecutable());
    }
  }

  /** Apply a {@link PendingWatermarkUpdate} to the {@link WatermarkManager}. */
  private void applyPendingUpdate(PendingWatermarkUpdate<ExecutableT, CollectionT> pending) {
    ExecutableT executable = pending.getExecutable();
    Bundle<?, ? extends CollectionT> inputBundle = pending.getInputBundle();

    updatePending(
        inputBundle,
        pending.getTimerUpdate(),
        executable,
        pending.getUnprocessedInputs(),
        pending.getOutputs());

    TransformWatermarks transformWms = transformToWatermarks.get(executable);
    transformWms.setEventTimeHold(
        inputBundle == null ? null : inputBundle.getKey(), pending.getEarliestHold());

    transformWms.setSynchronizedProcessingTimeHold(
        inputBundle == null ? null : inputBundle.getKey(), pending.getEarliestHold());
  }

  /**
   * First adds all produced elements to the queue of pending elements for each consumer, then adds
   * all pending timers to the collection of pending timers, then removes all completed and deleted
   * timers from the collection of pending timers, then removes all completed elements from the
   * pending queue of the executable.
   *
   * <p>It is required that all newly pending elements are added to the queue of pending elements
   * for each consumer prior to the completed elements being removed, as doing otherwise could cause
   * a Watermark to appear in a state in which the upstream (completed) element does not hold the
   * watermark but the element it produced is not yet pending. This can cause the watermark to
   * erroneously advance.
   *
   * <p>See {@link #updateWatermarks(Bundle, TimerUpdate, Object, Bundle, Iterable, Instant)} for
   * information about the parameters of this method.
   */
  private void updatePending(
      Bundle<?, ? extends CollectionT> input,
      TimerUpdate timerUpdate,
      ExecutableT executable,
      @Nullable Bundle<?, ? extends CollectionT> unprocessedInputs,
      Iterable<? extends Bundle<?, ? extends CollectionT>> outputs) {
    // Newly pending elements must be added before completed elements are removed, as the two
    // do not share a Mutex within this call and thus can be interleaved with external calls to
    // refresh.
    for (Bundle<?, ? extends CollectionT> bundle : outputs) {
      for (ExecutableT consumer : graph.getPerElementConsumers(bundle.getPCollection())) {
        TransformWatermarks watermarks = transformToWatermarks.get(consumer);
        watermarks.addPending(bundle);
      }
    }

    TransformWatermarks completedTransform = transformToWatermarks.get(executable);
    if (unprocessedInputs != null) {
      // Add the unprocessed inputs
      completedTransform.addPending(unprocessedInputs);
    }
    completedTransform.updateTimers(timerUpdate);
    if (input != null) {
      completedTransform.removePending(input);
    }
  }

  /**
   * Refresh the watermarks contained within this {@link WatermarkManager}, causing all watermarks
   * to be advanced as far as possible.
   */
  public synchronized void refreshAll() {
    refreshLock.lock();
    try {
      applyAllPendingUpdates();
      Set<ExecutableT> toRefresh = pendingRefreshes;
      while (!toRefresh.isEmpty()) {
        toRefresh = refreshAllOf(toRefresh);
      }
      pendingRefreshes.clear();
    } finally {
      refreshLock.unlock();
    }
  }

  private Set<ExecutableT> refreshAllOf(Set<ExecutableT> toRefresh) {
    Set<ExecutableT> newRefreshes = new HashSet<>();
    for (ExecutableT executable : toRefresh) {
      newRefreshes.addAll(refreshWatermarks(executable));
    }
    return newRefreshes;
  }

  private Set<ExecutableT> refreshWatermarks(final ExecutableT toRefresh) {
    TransformWatermarks myWatermarks = transformToWatermarks.get(toRefresh);
    WatermarkUpdate updateResult = myWatermarks.refresh();
    if (updateResult.isAdvanced()) {
      Set<ExecutableT> additionalRefreshes = new HashSet<>();
      for (CollectionT outputPValue : graph.getProduced(toRefresh)) {
        additionalRefreshes.addAll(graph.getPerElementConsumers(outputPValue));
      }
      return additionalRefreshes;
    }
    return Collections.emptySet();
  }

  @VisibleForTesting
  Collection<FiredTimers<ExecutableT>> extractFiredTimers() {
    return extractFiredTimers(Collections.emptyList());
  }

  /**
   * Returns a map of each {@link PTransform} that has pending timers to those timers. All of the
   * pending timers will be removed from this {@link WatermarkManager}.
   */
  public Collection<FiredTimers<ExecutableT>> extractFiredTimers(
      Collection<ExecutableT> ignoredExecutables) {

    Collection<FiredTimers<ExecutableT>> allTimers = new ArrayList<>();
    refreshLock.lock();
    try {
      for (Map.Entry<ExecutableT, TransformWatermarks> watermarksEntry :
          transformToWatermarks.entrySet()) {
        ExecutableT transform = watermarksEntry.getKey();
        if (ignoredExecutables.contains(transform)) {
          continue;
        }
        if (!transformsWithAlreadyExtractedTimers.containsKey(transform)) {
          TransformWatermarks watermarks = watermarksEntry.getValue();
          Collection<FiredTimers<ExecutableT>> firedTimers = watermarks.extractFiredTimers();
          if (!firedTimers.isEmpty()) {
            List<TimerData> newTimers =
                firedTimers.stream()
                    .flatMap(f -> f.getTimers().stream())
                    .collect(Collectors.toList());
            transformsWithAlreadyExtractedTimers.compute(
                transform,
                (k, v) -> {
                  if (v == null) {
                    v = new HashSet<>();
                  }
                  final Set<String> toUpdate = v;
                  newTimers.forEach(
                      td -> toUpdate.add(TimerUpdate.getTimerIdAndTimerFamilyIdWithNamespace(td)));
                  return v;
                });
            allTimers.addAll(firedTimers);
          }
        }
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

    /** Create a new KeyedHold with the specified key and timestamp. */
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
    public boolean equals(@Nullable Object other) {
      if (other == null || !(other instanceof KeyedHold)) {
        return false;
      }
      KeyedHold that = (KeyedHold) other;
      return Objects.equals(this.timestamp, that.timestamp) && Objects.equals(this.key, that.key);
    }

    /** Get the value of this {@link KeyedHold}. */
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
     * Updates the hold of the provided key to the provided value, removing any other holds for the
     * same key.
     */
    public void updateHold(@Nullable Object key, Instant newHold) {
      removeHold(key);
      KeyedHold newKeyedHold = KeyedHold.of(key, newHold);
      keyedHolds.put(key, newKeyedHold);
      allHolds.add(newKeyedHold);
    }

    /** Removes the hold of the provided key. */
    public void removeHold(Object key) {
      KeyedHold oldHold = keyedHolds.remove(key);
      if (oldHold != null) {
        allHolds.remove(oldHold);
      }
    }
  }

  /** A reference to the input and output watermarks of an {@link AppliedPTransform}. */
  public class TransformWatermarks {
    private final ExecutableT executable;

    private final AppliedPTransformInputWatermark inputWatermark;
    private final AppliedPTransformOutputWatermark outputWatermark;

    private final SynchronizedProcessingTimeInputWatermark synchronizedProcessingInputWatermark;
    private final SynchronizedProcessingTimeOutputWatermark synchronizedProcessingOutputWatermark;

    private Instant latestSynchronizedInputWm;
    private Instant latestSynchronizedOutputWm;

    private final ReadWriteLock transformWatermarkLock = new ReentrantReadWriteLock();

    private TransformWatermarks(
        ExecutableT executable,
        AppliedPTransformInputWatermark inputWatermark,
        AppliedPTransformOutputWatermark outputWatermark,
        SynchronizedProcessingTimeInputWatermark inputSynchProcessingWatermark,
        SynchronizedProcessingTimeOutputWatermark outputSynchProcessingWatermark) {
      this.executable = executable;
      this.inputWatermark = inputWatermark;
      this.outputWatermark = outputWatermark;

      this.synchronizedProcessingInputWatermark = inputSynchProcessingWatermark;
      this.synchronizedProcessingOutputWatermark = outputSynchProcessingWatermark;
      this.latestSynchronizedInputWm = BoundedWindow.TIMESTAMP_MIN_VALUE;
      this.latestSynchronizedOutputWm = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    /** Returns the input watermark of the {@link AppliedPTransform}. */
    public Instant getInputWatermark() {
      return checkNotNull(inputWatermark.get());
    }

    /** Returns the output watermark of the {@link AppliedPTransform}. */
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
      latestSynchronizedInputWm =
          INSTANT_ORDERING.max(
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
      latestSynchronizedOutputWm =
          INSTANT_ORDERING.max(
              latestSynchronizedOutputWm,
              INSTANT_ORDERING.min(clock.now(), synchronizedProcessingOutputWatermark.get()));
      return latestSynchronizedOutputWm;
    }

    private ReadWriteLock getWatermarkLock() {
      return transformWatermarkLock;
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

    private void setSynchronizedProcessingTimeHold(Object key, Instant newHold) {
      synchronizedProcessingOutputWatermark.updateHold(key, newHold);
    }

    private void removePending(Bundle<?, ?> bundle) {
      inputWatermark.removePending(bundle);
      synchronizedProcessingInputWatermark.removePending(bundle);
    }

    private void addPending(Bundle<?, ?> bundle) {
      inputWatermark.addPending(bundle);
      synchronizedProcessingInputWatermark.addPending(bundle);
    }

    private Collection<FiredTimers<ExecutableT>> extractFiredTimers() {
      Map<StructuralKey<?>, List<TimerData>> eventTimeTimers =
          inputWatermark.extractFiredEventTimeTimers();
      Map<StructuralKey<?>, List<TimerData>> processingTimers;
      Map<StructuralKey<?>, List<TimerData>> synchronizedTimers;
      processingTimers =
          synchronizedProcessingInputWatermark.extractFiredDomainTimers(
              TimeDomain.PROCESSING_TIME, clock.now());
      synchronizedTimers =
          synchronizedProcessingInputWatermark.extractFiredDomainTimers(
              TimeDomain.SYNCHRONIZED_PROCESSING_TIME, getSynchronizedProcessingInputTime());

      Map<StructuralKey<?>, List<TimerData>> timersPerKey =
          groupFiredTimers(eventTimeTimers, processingTimers, synchronizedTimers);
      Collection<FiredTimers<ExecutableT>> keyFiredTimers = new ArrayList<>(timersPerKey.size());
      for (Map.Entry<StructuralKey<?>, List<TimerData>> firedTimers : timersPerKey.entrySet()) {
        keyFiredTimers.add(
            new FiredTimers<>(executable, firedTimers.getKey(), firedTimers.getValue()));
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
   * the input to the executed step. pushedBackTimers are timers that were in completedTimers at the
   * input, but were pushed back due to processing constraints.
   */
  public static class TimerUpdate {
    private final StructuralKey<?> key;
    private final Iterable<? extends TimerData> completedTimers;
    private final Iterable<? extends TimerData> setTimers;
    private final Iterable<? extends TimerData> deletedTimers;
    private final Iterable<? extends TimerData> pushedBackTimers;

    /** Returns a TimerUpdate for a null key with no timers. */
    public static TimerUpdate empty() {
      return new TimerUpdate(
          null,
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList());
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
        deletedTimers.remove(
            TimerData.of(
                setTimer.getTimerId(),
                setTimer.getNamespace(),
                Instant.EPOCH,
                Instant.EPOCH,
                setTimer.getDomain()));
        setTimers.add(setTimer);
        return this;
      }

      /**
       * Adds the provided timer to the collection of deleted timers, removing it from set timers if
       * it has previously been set. Returns this {@link TimerUpdateBuilder}.
       */
      public TimerUpdateBuilder deletedTimer(TimerData deletedTimer) {
        deletedTimers.add(deletedTimer);
        TimerData timerToDelete = null;
        for (TimerData timer : setTimers) {
          if (timer.getDomain().equals(deletedTimer.getDomain())
              && timer.getNamespace().equals(deletedTimer.getNamespace())
              && timer.getTimerId().equals(deletedTimer.getTimerId())) {
            timerToDelete = timer;
          }
        }
        setTimers.remove(timerToDelete);
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
            ImmutableList.copyOf(deletedTimers),
            Collections.emptyList());
      }
    }

    private static Map<String, TimerData> indexTimerData(Iterable<? extends TimerData> timerData) {
      return StreamSupport.stream(timerData.spliterator(), false)
          .collect(
              Collectors.toMap(
                  TimerUpdate::getTimerIdAndTimerFamilyIdWithNamespace, e -> e, (a, b) -> b));
    }

    private static String getTimerIdAndTimerFamilyIdWithNamespace(TimerData td) {
      return td.getNamespace() + td.getTimerId() + td.getTimerFamilyId();
    }

    private TimerUpdate(
        StructuralKey<?> key,
        Iterable<? extends TimerData> completedTimers,
        Iterable<? extends TimerData> setTimers,
        Iterable<? extends TimerData> deletedTimers,
        Iterable<? extends TimerData> pushedBackTimers) {
      this.key = key;
      this.completedTimers = completedTimers;
      this.setTimers = setTimers;
      this.deletedTimers = deletedTimers;
      this.pushedBackTimers = pushedBackTimers;
    }

    @VisibleForTesting
    StructuralKey<?> getKey() {
      return key;
    }

    @VisibleForTesting
    public Iterable<? extends TimerData> getCompletedTimers() {
      return completedTimers;
    }

    @VisibleForTesting
    public Iterable<? extends TimerData> getSetTimers() {
      return setTimers;
    }

    @VisibleForTesting
    public Iterable<? extends TimerData> getDeletedTimers() {
      return deletedTimers;
    }

    Iterable<? extends TimerData> getPushedBackTimers() {
      return pushedBackTimers;
    }

    boolean isEmpty() {
      return Iterables.isEmpty(completedTimers)
          && Iterables.isEmpty(setTimers)
          && Iterables.isEmpty(deletedTimers)
          && Iterables.isEmpty(pushedBackTimers);
    }

    /**
     * Returns a {@link TimerUpdate} that is like this one, but with the specified completed timers.
     * Note that if any of the completed timers is in pushedBackTimers, then it is set instead. The
     * pushedBackTimers are cleared afterwards.
     */
    public TimerUpdate withCompletedTimers(Iterable<TimerData> completedTimers) {
      List<TimerData> timersToComplete = new ArrayList<>();
      Set<TimerData> pushedBack = Sets.newHashSet(pushedBackTimers);
      Map<String, TimerData> newSetTimers = indexTimerData(setTimers);
      for (TimerData td : completedTimers) {
        String timerIdWithNs = getTimerIdAndTimerFamilyIdWithNamespace(td);
        if (!pushedBack.contains(td)) {
          timersToComplete.add(td);
        } else if (!newSetTimers.containsKey(timerIdWithNs)) {
          newSetTimers.put(timerIdWithNs, td);
        }
      }
      return new TimerUpdate(
          key, timersToComplete, newSetTimers.values(), deletedTimers, Collections.emptyList());
    }

    /**
     * Returns a {@link TimerUpdate} that is like this one, but with the pushedBackTimersare removed
     * set by provided pushedBackTimers.
     */
    public TimerUpdate withPushedBackTimers(Iterable<TimerData> pushedBackTimers) {
      return new TimerUpdate(
          key, completedTimers, setTimers, deletedTimers, Lists.newArrayList(pushedBackTimers));
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, completedTimers, setTimers, deletedTimers);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == null || !(other instanceof TimerUpdate)) {
        return false;
      }
      TimerUpdate that = (TimerUpdate) other;
      return Objects.equals(this.key, that.key)
          && Objects.equals(this.completedTimers, that.completedTimers)
          && Objects.equals(this.setTimers, that.setTimers)
          && Objects.equals(this.deletedTimers, that.deletedTimers);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("key", key)
          .add("setTimers", setTimers)
          .add("completedTimers", completedTimers)
          .add("deletedTimers", deletedTimers)
          .add("pushedBackTimers", pushedBackTimers)
          .toString();
    }
  }

  /**
   * A pair of {@link TimerData} and key which can be delivered to the appropriate {@link
   * AppliedPTransform}. A timer fires at the executable that set it with a specific key when the
   * time domain in which it lives progresses past a specified time, as determined by the {@link
   * WatermarkManager}.
   */
  public static class FiredTimers<ExecutableT> {
    /** The executable the timers were set at and will be delivered to. */
    private final ExecutableT executable;
    /** The key the timers were set for and will be delivered to. */
    private final StructuralKey<?> key;

    private final Collection<TimerData> timers;

    private FiredTimers(
        ExecutableT executable, StructuralKey<?> key, Collection<TimerData> timers) {
      this.executable = executable;
      this.key = key;
      this.timers = timers;
    }

    public ExecutableT getExecutable() {
      return executable;
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
      return MoreObjects.toStringHelper(FiredTimers.class)
          .add("key", key)
          .add("timers", timers)
          .toString();
    }
  }

  private static class BundleByElementTimestampComparator extends Ordering<Bundle<?, ?>>
      implements Serializable {

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public int compare(@Nonnull Bundle<?, ?> o1, @Nonnull Bundle<?, ?> o2) {
      return ComparisonChain.start()
          .compare(o1.getMinimumTimestamp(), o2.getMinimumTimestamp())
          .result();
    }
  }

  @AutoValue
  abstract static class PendingWatermarkUpdate<ExecutableT, CollectionT> {
    abstract ExecutableT getExecutable();

    abstract @Nullable Bundle<?, ? extends CollectionT> getInputBundle();

    abstract TimerUpdate getTimerUpdate();

    abstract @Nullable Bundle<?, ? extends CollectionT> getUnprocessedInputs();

    abstract Iterable<? extends Bundle<?, ? extends CollectionT>> getOutputs();

    abstract Instant getEarliestHold();

    public static <ExecutableT, CollectionT>
        PendingWatermarkUpdate<ExecutableT, CollectionT> create(
            ExecutableT executable,
            @Nullable Bundle<?, ? extends CollectionT> inputBundle,
            TimerUpdate timerUpdate,
            @Nullable Bundle<?, ? extends CollectionT> unprocessedInputs,
            Iterable<? extends Bundle<?, ? extends CollectionT>> outputs,
            Instant earliestHold) {
      return new AutoValue_WatermarkManager_PendingWatermarkUpdate<>(
          executable, inputBundle, timerUpdate, unprocessedInputs, outputs, earliestHold);
    }
  }
}
