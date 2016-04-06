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
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Track which active windows have their state associated with merged-away windows.
 *
 * When windows are merged we must track which state previously associated with the merged windows
 * must now be associated with the result window. Some of that state may be combined eagerly when
 * the windows are merged. The rest is combined lazily when the final state is actually
 * required when emitting a pane. We keep track of this using an {@link ActiveWindowSet}.
 *
 * <p>An {@link ActiveWindowSet} considers a window to be in one of the following states:
 *
 * <ol>
 *   <li><b>NEW</b>: The initial state for a window on an incoming element; we do not yet know
 *       if it should be merged into an ACTIVE window, or whether it is already present as an
 *       ACTIVE window, since we have not yet called
 *       {@link WindowFn#mergeWindows}.</li>
 *   <li><b>ACTIVE</b>: A window that has state associated with it and has not itself been merged
 *       away. The window may have one or more <i>state address</i> windows under which its
 *       non-empty state is stored. A state value for an ACTIVE window must be derived by reading
 *       the state in all of its state address windows.</li>
 *   <li><b>EPHEMERAL</b>: A NEW window that has been merged into an ACTIVE window before any state
 *       has been associated with that window. Thus the window is neither ACTIVE nor MERGED. These
 *       windows are not persistently represented since if they reappear the merge function should
 *       again redirect them to an ACTIVE window. EPHEMERAL windows are an optimization for
 *       the common case of in-order events and {@link Sessions session window} by never associating
 *       state with windows that are created and immediately merged away.</li>
 *   <li><b>MERGED</b>: An ACTIVE window has been merged into another ACTIVE window after it had
 *       state associated with it. The window will thus appear as a state address window for exactly
 *       one ACTIVE window.</li>
 *   <li><b>EXPIRED</b>: The window has expired and may have been garbage collected. No new elements
 *       (even late elements) will ever be assigned to that window. These windows are not explicitly
 *       represented anywhere; it is expected that the user of {@link ActiveWindowSet} will store
 *       no state associated with the window.</li>
 * </ol>
 *
 * <p>
 *
 * <p>If no windows will ever be merged we can use the trivial implementation {@link
 * NonMergingActiveWindowSet}. Otherwise, the actual implementation of this data structure is in
 * {@link MergingActiveWindowSet}.
 *
 * @param <W> the type of window being managed
 */
public interface ActiveWindowSet<W extends BoundedWindow> {
  /**
   * Callback for {@link #merge}.
   */
  public interface MergeCallback<W extends BoundedWindow> {
    /**
     * Called when windows are about to be merged, but before any {@link #onMerge} callback
     * has been made.
     */
    void prefetchOnMerge(Collection<W> toBeMerged, Collection<W> activeToBeMerged, W mergeResult)
        throws Exception;

    /**
     * Called when windows are about to be merged, after all {@link #prefetchOnMerge} calls
     * have been made, but before the active window set has been updated to reflect the merge.
     *
     * @param toBeMerged the windows about to be merged.
     * @param activeToBeMerged the subset of {@code toBeMerged} corresponding to windows which
     * are currently ACTIVE (and about to be merged). The remaining windows have been deemed
     * EPHEMERAL, and thus have no state associated with them.
     * @param mergeResult the result window, either a member of {@code toBeMerged} or new.
     */
    void onMerge(Collection<W> toBeMerged, Collection<W> activeToBeMerged, W mergeResult)
        throws Exception;
  }

  /**
   * Remove EPHEMERAL windows and remaining NEW windows since we only need to know about them
   * while processing new elements.
   */
  void cleanupTemporaryWindows();

  /**
   * Save any state changes needed.
   */
  void persist();

  /**
   * Return the ACTIVE window into which {@code window} has been merged.
   * Return {@code window} itself if it is ACTIVE. Return null if {@code window} has not
   * yet been seen.
   */
  @Nullable
  W mergeResultWindow(W window);

  /**
   * Return (a view of) the set of currently ACTIVE windows.
   */
  Set<W> getActiveWindows();

  /**
   * Return {@code true} if {@code window} is ACTIVE.
   */
  boolean isActive(W window);

  /**
   * Called when an incoming element indicates it is a member of {@code window}, but before we
   * have started processing that element. If {@code window} is not already known to be ACTIVE,
   * MERGED or EPHEMERAL then add it as NEW.
   */
  void ensureWindowExists(W window);

  /**
   * Called when a NEW or ACTIVE window is now known to be ACTIVE.
   * Ensure that if it is NEW then it becomes ACTIVE (with itself as its only state address window).
   */
  void ensureWindowIsActive(W window);

  /**
   * If {@code window} is not already known to be ACTIVE, MERGED or EPHEMERAL then add it
   * as ACTIVE.
   */
  @VisibleForTesting
  void addActive(W window);

  /**
   * Remove {@code window} from the set.
   */
  void remove(W window);

  /**
   * Invoke {@link WindowFn#mergeWindows} on the {@code WindowFn} associated with this window set,
   * merging as many of the active windows as possible. {@code mergeCallback} will be invoked for
   * each group of windows that are merged. After this no NEW windows will remain, all merge
   * result windows will be ACTIVE, and all windows which have been merged away will not be ACTIVE.
   */
  void merge(MergeCallback<W> mergeCallback) throws Exception;

  /**
   * Signal that all state in {@link #readStateAddresses} for {@code window} has been merged into
   * the {@link #writeStateAddress} for {@code window}.
   */
  void merged(W window);

  /**
   * Return the state address windows for ACTIVE {@code window} from which all state associated
   * should be read and merged.
   */
  Set<W> readStateAddresses(W window);

  /**
   * Return the state address window of ACTIVE {@code window} into which all new state should be
   * written. Always one of the results of {@link #readStateAddresses}.
   */
  W writeStateAddress(W window);

  /**
   * Return the state address window into which all new state should be written after
   * ACTIVE windows {@code toBeMerged} have been merged into {@code mergeResult}.
   */
  W mergedWriteStateAddress(Collection<W> toBeMerged, W mergeResult);
}
