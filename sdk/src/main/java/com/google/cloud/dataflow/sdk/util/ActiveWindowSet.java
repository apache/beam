/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;

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
 * <p>An element may belong to one or more windows. Each key may have zero or more windows
 * corresponding to elements with that key. A window can be in one of five states:
 * <ol>
 * <li>NEW: We have just encountered the window on an incoming element and do not yet know if
 * it should be merged into an ACTIVE window since we have not yet called
 * {@link WindowFn#mergeWindows}.
 * <li>EPHEMERAL: A NEW window has been merged into an ACTIVE window before any state has been
 * associated with that window. Thus the window is neither ACTIVE nor MERGED. These windows
 * are not persistently represented since if they reappear the merge function should again
 * redirect them to an ACTIVE window. (We could collapse EPHEMERAL into MERGED, but keeping them
 * separate cuts down on the number of windows we need to keep track of in the common case
 * of SessionWindows over in-order events.)
 * <li>ACTIVE: A NEW window has state associated with it and has not itself been merged away.
 * The window may have one or more 'state address' windows under which its non-empty state is
 * stored. The true state for an ACTIVE window must be derived by reading all of the state in its
 * state address windows.
 * <li>MERGED: An ACTIVE window has been merged into another ACTIVE window after it had state
 * associated with it. The window will thus appear as a state address window for exactly one
 * ACTIVE window.
 * <li>GARBAGE: The window has been garbage collected. No new elements (even late elements) will
 * ever be assigned to that window. These windows are not explicitly represented anywhere.
 * (Garbage collection is performed by {@link ReduceFnRunner#onTimer}).
 * </ol>
 *
 * <p>If no windows will ever be merged we can use the dummy implementation {@link
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
     * Called when windows are about to be merged.
     *
     * @param toBeMerged the windows about to be merged.
     * @param activeToBeMerged the subset of {@code toBeMerged} corresponding to windows which
     * are currently ACTIVE (and about to be merged). The remaining windows have been deemed
     * EPHEMERAL.
     * @param mergeResult the result window, either a member of {@code toBeMerged} or new.
     */
    void onMerge(Collection<W> toBeMerged, Collection<W> activeToBeMerged, W mergeResult)
        throws Exception;
  }

  /**
   * Remove EPHEMERAL windows since we only need to know about them while processing new elements.
   */
  void removeEphemeralWindows();

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
  W representative(W window);

  /**
   * Return (a view of) the set of currently ACTIVE windows.
   */
  Set<W> getActiveWindows();

  /**
   * Return {@code true} if {@code window} is ACTIVE.
   */
  boolean isActive(W window);

  /**
   * If {@code window} is not already known to be ACTIVE, MERGED or EPHEMERAL then add it
   * as NEW. All NEW windows will be accounted for as ACTIVE, MERGED or EPHEMERAL by a call
   * to {@link #merge}.
   */
  void addNew(W window);

  /**
   * If {@code window} is not already known to be ACTIVE, MERGED or EPHEMERAL then add it
   * as ACTIVE.
   */
  void addActive(W window);

  /**
   * Remove {@code window} from the set.
   */
  void remove(W window);

  /**
   * Invoke {@link WindowFn#mergeWindows} on the {@code WindowFn} associated with this window set,
   * merging as many of the active windows as possible. {@code mergeCallback} will be invoked for
   * each group of windows that are merged. After this no NEW windows will remain.
   */
  void merge(MergeCallback<W> mergeCallback) throws Exception;

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
}
