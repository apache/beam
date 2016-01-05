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

import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.google.cloud.dataflow.sdk.coders.SetCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.ValueState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Implementation of {@link ActiveWindowSet} for use with {@link WindowFn}s that support
 * merging. In effect maintains an equivalence class of windows (where two windows which have
 * been merged are in the same class), but also manages which windows contain state which
 * must be merged when a pane is fired.
 *
 * <p>Note that this object must be serialized and stored when work units are committed such
 * that subsequent work units can recover the equivalence classes etc.
 *
 * @param <W> the type of window being managed
 */
public class MergingActiveWindowSet<W extends BoundedWindow> implements ActiveWindowSet<W> {
  private final WindowFn<Object, W> windowFn;

  /**
   * A map from ACTIVE windows to their state address windows. Writes to the ACTIVE window
   * state can be redirected to any one of the state address windows. Reads need to merge
   * from all state address windows. If the set is empty then the window is NEW.
   *
   * <ul>
   * <li>The state address windows will be empty if the window is NEW, we don't yet know what other
   * windows it may be merged into, and the window does not yet have any state associated with it.
   * In this way we can distinguish between MERGED and EPHEMERAL windows when merging.
   * <li>The state address windows will contain just the window itself it it has never been merged
   * but has state.
   * <li>It is possible none of the state address windows correspond to the window itself. For
   * example, two windows W1 and W2 with state may be merged to form W12. From then on additional
   * state can be added to just W1 or W2. Thus the state address windows for W12 do not need to
   * include W12.
   * <li>If W1 is in the set for W2 then W1 is not a state address window of any other active
   * window. Furthermore W1 will map to W2 in {@link #windowToActiveWindow}.
   * </ul>
   */
  @Nullable
  private Map<W, Set<W>> activeWindowToStateAddressWindows;

  /**
   * As above, but only for EPHEMERAL windows. Does not need to be persisted.
   */
  private final Map<W, Set<W>> activeWindowToEphemeralWindows;

  /**
   * A map from window to the ACTIVE window it has been merged into.
   *
   * <p>Does not need to be persisted.
   *
   * <ul>
   * <li>Key window may be ACTIVE, MERGED or EPHEMERAL.
   * <li>ACTIVE windows map to themselves.
   * <li>If W1 maps to W2 then W2 is in {@link #activeWindowToStateAddressWindows}.
   * <li>If W1 = W2 then W1 is ACTIVE. If W1 is in the state address window set for W2 then W1 is
   * MERGED. Otherwise W1 is EPHEMERAL.
   * </ul>
   */
  @Nullable
  private Map<W, W> windowToActiveWindow;

  /**
   * Deep clone of {@link #activeWindowToStateAddressWindows} as of last commit.
   *
   * <p>Used to avoid writing to state if no changes have been made during the work unit.
   */
  @Nullable
  private Map<W, Set<W>> originalActiveWindowToStateAddressWindows;

  /**
   * Handle representing our state in the backend.
   */
  private final ValueState<Map<W, Set<W>>> valueState;

  public MergingActiveWindowSet(WindowFn<Object, W> windowFn, StateInternals state) {
    this.windowFn = windowFn;

    StateTag<ValueState<Map<W, Set<W>>>> mergeTreeAddr =
        StateTags.makeSystemTagInternal(StateTags.value(
            "tree", MapCoder.of(windowFn.windowCoder(), SetCoder.of(windowFn.windowCoder()))));
    valueState = state.state(StateNamespaces.global(), mergeTreeAddr);
    // Little use trying to prefetch this state since the ReduceFnRunner is stymied until it is
    // available.
    activeWindowToStateAddressWindows = emptyIfNull(valueState.get().read());
    activeWindowToEphemeralWindows = new HashMap<>();
    originalActiveWindowToStateAddressWindows = deepCopy(activeWindowToStateAddressWindows);
    windowToActiveWindow = invert(activeWindowToStateAddressWindows);
  }

  @Override
  public void removeEphemeralWindows() {
    for (Map.Entry<W, Set<W>> entry : activeWindowToEphemeralWindows.entrySet()) {
      for (W ephemeral : entry.getValue()) {
        windowToActiveWindow.remove(ephemeral);
      }
    }
    activeWindowToEphemeralWindows.clear();
  }

  @Override
  public void persist() {
    if (activeWindowToStateAddressWindows.equals(originalActiveWindowToStateAddressWindows)) {
      // No change.
      return;
    }
    // All NEW windows must have been accounted for.
    for (Map.Entry<W, Set<W>> entry : activeWindowToStateAddressWindows.entrySet()) {
      Preconditions.checkState(
          !entry.getValue().isEmpty(), "Cannot persist NEW window %s", entry.getKey());
    }
    // Should be no EPHEMERAL windows.
    Preconditions.checkState(
        activeWindowToEphemeralWindows.isEmpty(), "Unexpected EPHEMERAL windows before persist");

    valueState.set(activeWindowToStateAddressWindows);
    // No need to update originalActiveWindowToStateAddressWindows since this object is about to
    // become garbage.
  }

  @Override
  @Nullable
  public W representative(W window) {
    return windowToActiveWindow.get(window);
  }

  @Override
  public Set<W> getActiveWindows() {
    return activeWindowToStateAddressWindows.keySet();
  }

  @Override
  public boolean isActive(W window) {
    return activeWindowToStateAddressWindows.containsKey(window);
  }

  @Override
  public void addNew(W window) {
    if (!windowToActiveWindow.containsKey(window)) {
      activeWindowToStateAddressWindows.put(window, new HashSet<W>());
    }
  }

  @Override
  public void addActive(W window) {
    if (!windowToActiveWindow.containsKey(window)) {
      Set<W> stateAddressWindows = new HashSet<>();
      stateAddressWindows.add(window);
      activeWindowToStateAddressWindows.put(window, stateAddressWindows);
      windowToActiveWindow.put(window, window);
    }
  }

  @Override
  public void remove(W window) {
    for (W stateAddressWindow : activeWindowToStateAddressWindows.get(window)) {
      windowToActiveWindow.remove(stateAddressWindow);
    }
    activeWindowToStateAddressWindows.remove(window);
    Set<W> ephemeralWindows = activeWindowToEphemeralWindows.get(window);
    if (ephemeralWindows != null) {
      for (W ephemeralWindow : ephemeralWindows) {
        windowToActiveWindow.remove(ephemeralWindow);
      }
      activeWindowToEphemeralWindows.remove(window);
    }
    windowToActiveWindow.remove(window);
  }

  private class MergeContextImpl extends WindowFn<Object, W>.MergeContext {
    private MergeCallback<W> mergeCallback;

    public MergeContextImpl(MergeCallback<W> mergeCallback) {
      windowFn.super();
      this.mergeCallback = mergeCallback;
    }

    @Override
    public Collection<W> windows() {
      return activeWindowToStateAddressWindows.keySet();
    }

    @Override
    public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
      recordMerge(mergeCallback, toBeMerged, mergeResult);
    }
  }

  @Override
  public void merge(MergeCallback<W> mergeCallback) throws Exception {
    // See what the window function does with the NEW and already ACTIVE windows.
    windowFn.mergeWindows(new MergeContextImpl(mergeCallback));

    for (Map.Entry<W, Set<W>> entry : activeWindowToStateAddressWindows.entrySet()) {
      if (entry.getValue().isEmpty()) {
        // This window was NEW but since it survived merging must now become ACTIVE.
        W window = entry.getKey();
        entry.getValue().add(window);
        windowToActiveWindow.put(window, window);
      }
    }
  }

  /**
   * A {@code WindowFn.mergeWindows} call has requested {@code toBeMerged} (which must
   * all be ACTIVE} be considered equivalent to {@code activeWindow} (which is either a
   * member of {@code toBeMerged} or is a new window).
   */
  private void recordMerge(MergeCallback<W> mergeCallback, Collection<W> toBeMerged, W mergeResult)
      throws Exception {
    Set<W> newStateAddressWindows = new HashSet<>();
    Set<W> existingStateAddressWindows = activeWindowToStateAddressWindows.get(mergeResult);
    if (existingStateAddressWindows != null) {
      // Preserve all the existing state address windows for mergeResult.
      newStateAddressWindows.addAll(existingStateAddressWindows);
    }

    Set<W> newEphemeralWindows = new HashSet<>();
    Set<W> existingEphemeralWindows = activeWindowToEphemeralWindows.get(mergeResult);
    if (existingEphemeralWindows != null) {
      // Preserve all the existing EPHEMERAL windows for meregResult.
      newEphemeralWindows.addAll(existingEphemeralWindows);
    }

    Collection<W> activeToBeMerged = new ArrayList<>();

    for (W other : toBeMerged) {
      Set<W> otherStateAddressWindows = activeWindowToStateAddressWindows.get(other);
      Preconditions.checkState(otherStateAddressWindows != null, "Window %s is not ACTIVE", other);

      for (W otherStateAddressWindow : otherStateAddressWindows) {
        // Since otherTarget equiv other AND other equiv mergeResult
        // THEN otherTarget equiv mergeResult.
        newStateAddressWindows.add(otherStateAddressWindow);
        windowToActiveWindow.put(otherStateAddressWindow, mergeResult);
      }
      activeWindowToStateAddressWindows.remove(other);

      Set<W> otherEphemeralWindows = activeWindowToEphemeralWindows.get(other);
      if (otherEphemeralWindows != null) {
        for (W otherEphemeral : otherEphemeralWindows) {
          // Since otherEphemeral equiv other AND other equiv mergeResult
          // THEN otherEphemeral equiv mergeResult.
          newEphemeralWindows.add(otherEphemeral);
          windowToActiveWindow.put(otherEphemeral, mergeResult);
        }
      }
      activeWindowToEphemeralWindows.remove(other);

      // Now other equiv mergeResult.
      if (otherStateAddressWindows.contains(other)) {
        // Other was ACTIVE and is now known to be MERGED.
        newStateAddressWindows.add(other);
        activeToBeMerged.add(other);
      } else if (otherStateAddressWindows.isEmpty()) {
        // Other was NEW thus has no state. It is now EPHEMERAL.
        newEphemeralWindows.add(other);
      } else if (other.equals(mergeResult)) {
        // Other was ACTIVE, was never used to store elements, but is still ACTIVE.
        // Leave it as active.
        activeToBeMerged.add(other);
      } else {
        // Other was ACTIVE, was never used to store element, as is no longer considered ACTIVE.
        // It is now EPHEMERAL.
        newEphemeralWindows.add(other);
        // However, since it may have metadata state, include it in the ACTIVE to be merged set.
        activeToBeMerged.add(other);
      }
      windowToActiveWindow.put(other, mergeResult);
    }

    if (newStateAddressWindows.isEmpty()) {
      // If stateAddressWindows is empty then toBeMerged must have only contained EPHEMERAL windows.
      // Promote mergeResult to be active now.
      newStateAddressWindows.add(mergeResult);
    }
    windowToActiveWindow.put(mergeResult, mergeResult);

    activeWindowToStateAddressWindows.put(mergeResult, newStateAddressWindows);
    if (!newEphemeralWindows.isEmpty()) {
      activeWindowToEphemeralWindows.put(mergeResult, newEphemeralWindows);
    }

    mergeCallback.onMerge(toBeMerged, activeToBeMerged, mergeResult);
  }

  /**
   * Return the state address windows for ACTIVE {@code window} from which all state associated
   * should
   * be read and merged.
   */
  @Override
  public Set<W> readStateAddresses(W window) {
    Set<W> stateAddressWindows = activeWindowToStateAddressWindows.get(window);
    Preconditions.checkState(stateAddressWindows != null, "Window %s is not ACTIVE", window);
    return stateAddressWindows;
  }

  /**
   * Return the state address window of ACTIVE {@code window} into which all new state should be
   * written.
   */
  @Override
  public W writeStateAddress(W window) {
    Set<W> stateAddressWindows = activeWindowToStateAddressWindows.get(window);
    Preconditions.checkState(stateAddressWindows != null, "Window %s is not ACTIVE", window);
    W result = Iterables.getFirst(stateAddressWindows, null);
    Preconditions.checkState(result != null, "Window %s is still NEW", window);
    return result;
  }

  @VisibleForTesting
  public void checkInvariants() {
    Set<W> knownStateAddressWindows = new HashSet<>();
    for (Map.Entry<W, Set<W>> entry : activeWindowToStateAddressWindows.entrySet()) {
      W active = entry.getKey();
      Preconditions.checkState(!entry.getValue().isEmpty(),
          "Unexpected empty state address window set for ACTIVE window %s", active);
      for (W stateAddressWindow : entry.getValue()) {
        Preconditions.checkState(knownStateAddressWindows.add(stateAddressWindow),
            "%s is in more than one state address window set", stateAddressWindow);
        Preconditions.checkState(active.equals(windowToActiveWindow.get(stateAddressWindow)),
            "%s should have %s as its ACTIVE window", stateAddressWindow, active);
      }
    }
    for (Map.Entry<W, Set<W>> entry : activeWindowToEphemeralWindows.entrySet()) {
      W active = entry.getKey();
      Preconditions.checkState(activeWindowToStateAddressWindows.containsKey(active),
          "%s must be ACTIVE window", active);
      Preconditions.checkState(
          !entry.getValue().isEmpty(), "Unexpected empty EPHEMERAL set for %s", active);
      for (W ephemeralWindow : entry.getValue()) {
        Preconditions.checkState(knownStateAddressWindows.add(ephemeralWindow),
            "%s is EPHEMERAL/state address of more than one ACTIVE window", ephemeralWindow);
        Preconditions.checkState(active.equals(windowToActiveWindow.get(ephemeralWindow)),
            "%s should have %s as its ACTIVE window", ephemeralWindow, active);
      }
    }
    for (Map.Entry<W, W> entry : windowToActiveWindow.entrySet()) {
      Preconditions.checkState(activeWindowToStateAddressWindows.containsKey(entry.getValue()),
          "%s should be ACTIVE since representative for %s", entry.getValue(), entry.getKey());
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MergingActiveWindowSet {\n");
    for (Map.Entry<W, Set<W>> entry : activeWindowToStateAddressWindows.entrySet()) {
      W active = entry.getKey();
      Set<W> stateAddressWindows = entry.getValue();
      if (stateAddressWindows.isEmpty()) {
        sb.append("  NEW ");
        sb.append(active);
        sb.append('\n');
      } else {
        sb.append("  ACTIVE ");
        sb.append(active);
        sb.append(":\n");
        for (W stateAddressWindow : stateAddressWindows) {
          if (stateAddressWindow.equals(active)) {
            sb.append("    ACTIVE ");
          } else {
            sb.append("    MERGED ");
          }
          sb.append(stateAddressWindow);
          sb.append("\n");
          W active2 = windowToActiveWindow.get(stateAddressWindow);
          Preconditions.checkState(active2.equals(active));
        }
        Set<W> ephemeralWindows = activeWindowToEphemeralWindows.get(active);
        if (ephemeralWindows != null) {
          for (W ephemeralWindow : ephemeralWindows) {
            sb.append("    EPHEMERAL ");
            sb.append(ephemeralWindow);
            sb.append('\n');
          }
        }
      }
    }
    sb.append("}");
    return sb.toString();
  }

  // ======================================================================

  /**
   * Replace null {@code multimap} with empty map, and replace null entries in {@code multimap} with
   * empty sets.
   */
  private static <W> Map<W, Set<W>> emptyIfNull(Map<W, Set<W>> multimap) {
    if (multimap == null) {
      return new HashMap<>();
    } else {
      for (Map.Entry<W, Set<W>> entry : multimap.entrySet()) {
        if (entry.getValue() == null) {
          entry.setValue(new HashSet<W>());
        }
      }
      return multimap;
    }
  }

  /** Return a deep copy of {@code multimap}. */
  private static <W> Map<W, Set<W>> deepCopy(Map<W, Set<W>> multimap) {
    Map<W, Set<W>> newMultimap = new HashMap<>();
    for (Map.Entry<W, Set<W>> entry : multimap.entrySet()) {
      newMultimap.put(entry.getKey(), new HashSet<W>(entry.getValue()));
    }
    return newMultimap;
  }

  /** Return inversion of {@code multimap}, which must be invertible. */
  private static <W> Map<W, W> invert(Map<W, Set<W>> multimap) {
    Map<W, W> result = new HashMap<>();
    for (Map.Entry<W, Set<W>> entry : multimap.entrySet()) {
      W active = entry.getKey();
      for (W target : entry.getValue()) {
        W previous = result.put(target, active);
        Preconditions.checkState(previous == null,
            "Window %s has both %s and %s as representatives", target, previous, active);
      }
    }
    return result;
  }
}
