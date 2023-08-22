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
package org.apache.beam.runners.core;

import java.util.Collection;
import java.util.Set;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * Track which windows are <i>active</i>, and the <i>state address window(s)</i> under which their
 * state is stored. Also help with the multi-step process of merging windows and their associated
 * state.
 *
 * <p>When windows are merged we must also merge their state. For example, we may need to
 * concatenate buffered elements, sum a count of elements, or find a new minimum timestamp. If we
 * start with two windows {@code Wa} and {@code Wb} and later discover they should be merged into
 * window {@code Wab} then, naively, we must copy and merge the states of {@code Wa} and {@code Wab}
 * into {@code Wab}.
 *
 * <p>However, the common case for merging windows is for a new window to be merged into an existing
 * window. Thus, if {@code Wa} is the existing window and {@code Wb} the new window it is more
 * efficient to leave the state for {@code Wa} where it is, and simply redirect {@code Wab} to it.
 * In this case we say {@code Wab} has a state address window of {@code Wa}.
 *
 * <p>Even if windows {@code Wa} and {@code Wb} already have state, it can still be more efficient
 * to append the state of {@code Wb} onto {@code Wa} rather than copy the state from {@code Wa} and
 * {@code Wb} into {@code Wab}.
 *
 * <p>We use the following terminology for windows:
 *
 * <ol>
 *   <li><b>ACTIVE</b>: A window that has state associated with it and has not itself been merged
 *       away. The window may have one (or more) state address windows under which its non-empty
 *       state is stored. A state value for an ACTIVE window must be derived by reading the state in
 *       (all of) its state address windows. Note that only pre 1.4 pipelines use multiple state
 *       address windows per active window. From 1.4 onwards we eagerly merge window state into a
 *       single state address window.
 *   <li><b>NEW</b>: The initial state for a window of an incoming element which is not already
 *       ACTIVE. We have not yet called {@link WindowFn#mergeWindows}, and so don't yet know whether
 *       the window will be be merged into another NEW or ACTIVE window, or will become an ACTIVE
 *       window in its own right.
 * </ol>
 *
 * <p>If no windows will ever be merged we can use the trivial implementation {@link
 * NonMergingActiveWindowSet}. Otherwise, the actual implementation of this data structure is in
 * {@link MergingActiveWindowSet}.
 *
 * @param <W> the type of window being managed
 */
public interface ActiveWindowSet<W extends BoundedWindow> {
  /** Callback for {@link #merge}. */
  public interface MergeCallback<W extends BoundedWindow> {
    /**
     * Called when windows are about to be merged, but before any {@link #onMerge} callback has been
     * made.
     *
     * @param toBeMerged the windows about to be merged.
     * @param mergeResult the result window, either a member of {@code toBeMerged} or new.
     */
    void prefetchOnMerge(Collection<W> toBeMerged, W mergeResult) throws Exception;

    /**
     * Called when windows are about to be merged, after all {@link #prefetchOnMerge} calls have
     * been made, but before the active window set has been updated to reflect the merge.
     *
     * @param toBeMerged the windows about to be merged.
     * @param mergeResult the result window, either a member of {@code toBeMerged} or new.
     */
    void onMerge(Collection<W> toBeMerged, W mergeResult) throws Exception;
  }

  /**
   * Remove any remaining NEW windows since they were not promoted to being ACTIVE by {@link
   * #ensureWindowIsActive} and we don't need to record anything about them.
   */
  void cleanupTemporaryWindows();

  /** Save any state changes needed. */
  void persist();

  /** Return (a view of) the set of currently ACTIVE and NEW windows. */
  Set<W> getActiveAndNewWindows();

  /** Return {@code true} if {@code window} is ACTIVE. */
  boolean isActive(W window);

  /** Return {@code true} if {@code window} is ACTIVE or NEW. */
  boolean isActiveOrNew(W window);

  /**
   * Called when an incoming element indicates it is a member of {@code window}, but before we have
   * started processing that element. If {@code window} is not already known to be ACTIVE, then add
   * it as NEW.
   */
  void ensureWindowExists(W window);

  /**
   * Called when a NEW or ACTIVE window is now known to be ACTIVE. Ensure that if it is NEW then it
   * becomes ACTIVE (with itself as its only state address window).
   */
  void ensureWindowIsActive(W window);

  /**
   * If {@code window} is not already known to be ACTIVE then add it as ACTIVE. For testing only.
   */
  @VisibleForTesting
  void addActiveForTesting(W window);

  /** Remove {@code window} from the set. */
  void remove(W window);

  /**
   * Invoke {@link WindowFn#mergeWindows} on the {@code WindowFn} associated with this window set,
   * merging as many of the NEW and ACTIVE windows as possible. {@code mergeCallback} will be
   * invoked for each group of windows that are merged. After this all merge result windows will be
   * ACTIVE, and all windows which have been merged away will be neither ACTIVE nor NEW.
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
   * Return the state address window into which all new state should be written after ACTIVE windows
   * {@code toBeMerged} have been merged into {@code mergeResult}.
   */
  W mergedWriteStateAddress(Collection<W> toBeMerged, W mergeResult);
}
