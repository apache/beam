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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * An {@link ActiveWindowSet} for merging {@link WindowFn} implementations.
 * <p>
 * <p>The underlying notion of {@link MergingActiveWindowSet} is that of representing equivalence
 * classes of merged windows as a mapping from the merged "super-window" to a set of
 * <i>state address</i> windows in which some state has been persisted. The mapping need not
 * contain EPHEMERAL windows, because they are created and merged without any persistent state.
 * Each window must be a state address window for at most one window, so the mapping is
 * invertible.
 * <p>
 * <p>The states of a non-expired window are treated as follows:
 * <p>
 * <ul>
 * <li><b>NEW</b>: a NEW has an empty set of associated state address windows.</li>
 * <li><b>ACTIVE</b>: an ACTIVE window will be associated with some nonempty set of state
 * address windows. If the window has not merged, this will necessarily be the singleton set
 * containing just itself, but it is not required that an ACTIVE window be amongst its
 * state address windows.</li>
 * <li><b>MERGED</b>: a MERGED window will be in the set of associated windows for some
 * other window - that window is retrieved via {@link #mergeResultWindow} (this reverse
 * association is implemented in O(1) time).</li>
 * <li><b>EPHEMERAL</b>: EPHEMERAL windows are not persisted but are tracked transiently;
 * an EPHEMERAL window must be registered with this {@link ActiveWindowSet} by a call
 * to {@link #recordMerge} prior to any request for a {@link #mergeResultWindow}.</li>
 * </ul>
 * <p>
 * <p>To illustrate why an ACTIVE window need not be amongst its own state address windows,
 * consider two active windows W1 and W2 that are merged to form W12. Further writes may be
 * applied to either of W1 or W2, since a read of W12 implies reading both of W12 and merging
 * their results. Hence W12 need not have state directly associated with it.
 */
public class MergingActiveWindowSet<W extends BoundedWindow> implements ActiveWindowSet<W> {
  private final WindowFn<Object, W> windowFn;

  /**
   * Map ACTIVE and NEW windows to their state address windows. Persisted.
   */
  private final Map<W, Set<W>> activeWindowToStateAddressWindows;

  /**
   * As above, but only for EPHEMERAL windows. Does not need to be persisted.
   */
  private final Map<W, Set<W>> activeWindowToEphemeralWindows;

  /**
   * A map from window to the ACTIVE window it has been merged into. Does not need to be persisted.
   * <p>
   * <ul>
   * <li>Key window may be ACTIVE, MERGED or EPHEMERAL.
   * <li>ACTIVE windows map to themselves.
   * <li>If W1 maps to W2 then W2 is in {@link #activeWindowToStateAddressWindows}.
   * <li>If W1 = W2 then W1 is ACTIVE. If W1 is in the state address window set for W2 then W1 is
   * MERGED. Otherwise W1 is EPHEMERAL.
   * </ul>
   */
  private final Map<W, W> windowToActiveWindow;

  /**
   * Deep clone of {@link #activeWindowToStateAddressWindows} as of last commit.
   * <p>
   * <p>Used to avoid writing to state if no changes have been made during the work unit.
   */
  private final Map<W, Set<W>> originalActiveWindowToStateAddressWindows;

  /**
   * Handle representing our state in the backend.
   */
  private final ValueState<Map<W, Set<W>>> valueState;

  public MergingActiveWindowSet(WindowFn<Object, W> windowFn, StateInternals<?> state) {
    this.windowFn = windowFn;

    StateTag<Object, ValueState<Map<W, Set<W>>>> mergeTreeAddr =
        StateTags.makeSystemTagInternal(StateTags.value(
            "tree", MapCoder.of(windowFn.windowCoder(), SetCoder.of(windowFn.windowCoder()))));
    valueState = state.state(StateNamespaces.global(), mergeTreeAddr);
    // Little use trying to prefetch this state since the ReduceFnRunner is stymied until it is
    // available.
    activeWindowToStateAddressWindows = emptyIfNull(valueState.read());
    activeWindowToEphemeralWindows = new HashMap<>();
    originalActiveWindowToStateAddressWindows = deepCopy(activeWindowToStateAddressWindows);
    windowToActiveWindow = invert(activeWindowToStateAddressWindows);
  }

  @Override
  public void cleanupTemporaryWindows() {
    // All NEW windows can be forgotten.
    Iterator<Map.Entry<W, Set<W>>> iter =
        activeWindowToStateAddressWindows.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<W, Set<W>> entry = iter.next();
      if (entry.getValue().isEmpty()) {
        windowToActiveWindow.remove(entry.getKey());
        iter.remove();
      }
    }

    // All EPHEMERAL windows can be forgotten.
    for (Map.Entry<W, Set<W>> entry : activeWindowToEphemeralWindows.entrySet()) {
      for (W ephemeral : entry.getValue()) {
        windowToActiveWindow.remove(ephemeral);
      }
    }
    activeWindowToEphemeralWindows.clear();
  }

  @Override
  public void persist() {
    if (activeWindowToStateAddressWindows.isEmpty()) {
      // Force all persistent state to disappear.
      valueState.clear();
      return;
    }
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

    valueState.write(activeWindowToStateAddressWindows);
    // No need to update originalActiveWindowToStateAddressWindows since this object is about to
    // become garbage.
  }

  @Override
  @Nullable
  public W mergeResultWindow(W window) {
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
  public void ensureWindowExists(W window) {
    if (!windowToActiveWindow.containsKey(window)) {
      Preconditions.checkState(!activeWindowToStateAddressWindows.containsKey(window));
      activeWindowToStateAddressWindows.put(window, new LinkedHashSet<W>());
      windowToActiveWindow.put(window, window);
    }
  }

  @Override
  public void ensureWindowIsActive(W window) {
    Set<W> stateAddressWindows = activeWindowToStateAddressWindows.get(window);
    Preconditions.checkState(stateAddressWindows != null,
        "Cannot ensure window %s is active since it is neither ACTIVE nor NEW",
        window);
    if (stateAddressWindows.isEmpty()) {
      // Window was NEW, make it ACTIVE.
      Preconditions.checkState(windowToActiveWindow.containsKey(window)
                               && windowToActiveWindow.get(window).equals(window));
      stateAddressWindows.add(window);
    }
  }

  @Override
  @VisibleForTesting
  public void addActive(W window) {
    if (!windowToActiveWindow.containsKey(window)) {
      Set<W> stateAddressWindows = new LinkedHashSet<>();
      stateAddressWindows.add(window);
      activeWindowToStateAddressWindows.put(window, stateAddressWindows);
      windowToActiveWindow.put(window, window);
    }
  }

  @Override
  public void remove(W window) {
    Set<W> stateAddressWindows = activeWindowToStateAddressWindows.remove(window);
    if (stateAddressWindows != null) {
      for (W stateAddressWindow : stateAddressWindows) {
        windowToActiveWindow.remove(stateAddressWindow);
      }
    }
    Set<W> ephemeralWindows = activeWindowToEphemeralWindows.remove(window);
    if (ephemeralWindows != null) {
      for (W ephemeralWindow : ephemeralWindows) {
        windowToActiveWindow.remove(ephemeralWindow);
      }
    }
    windowToActiveWindow.remove(window);
  }

  private class MergeContextImpl extends WindowFn<Object, W>.MergeContext {
    private MergeCallback<W> mergeCallback;
    private final List<Collection<W>> allToBeMerged;
    private final List<Collection<W>> allActiveToBeMerged;
    private final List<W> allMergeResults;
    private final Set<W> seen;

    public MergeContextImpl(MergeCallback<W> mergeCallback) {
      windowFn.super();
      this.mergeCallback = mergeCallback;
      allToBeMerged = new ArrayList<>();
      allActiveToBeMerged = new ArrayList<>();
      allMergeResults = new ArrayList<>();
      seen = new HashSet<>();
    }

    @Override
    public Collection<W> windows() {
      return activeWindowToStateAddressWindows.keySet();
    }

    @Override
    public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
      // The arguments have come from userland.
      Preconditions.checkNotNull(toBeMerged);
      Preconditions.checkNotNull(mergeResult);
      List<W> copyOfToBeMerged = new ArrayList<>(toBeMerged.size());
      List<W> activeToBeMerged = new ArrayList<>(toBeMerged.size());
      boolean includesMergeResult = false;
      for (W window : toBeMerged) {
        Preconditions.checkNotNull(window);
        Preconditions.checkState(
            isActive(window), "Expecting merge window %s to be active", window);
        if (window.equals(mergeResult)) {
          includesMergeResult = true;
        }
        boolean notDup = seen.add(window);
        Preconditions.checkState(
            notDup, "Expecting merge window %s to appear in at most one merge set", window);
        copyOfToBeMerged.add(window);
        if (!activeWindowToStateAddressWindows.get(window).isEmpty()) {
          activeToBeMerged.add(window);
        }
      }
      if (!includesMergeResult) {
        Preconditions.checkState(
            !isActive(mergeResult), "Expecting result window %s to be new", mergeResult);
      }
      allToBeMerged.add(copyOfToBeMerged);
      allActiveToBeMerged.add(activeToBeMerged);
      allMergeResults.add(mergeResult);
    }

    public void recordMerges() throws Exception {
      for (int i = 0; i < allToBeMerged.size(); i++) {
        mergeCallback.prefetchOnMerge(
            allToBeMerged.get(i), allActiveToBeMerged.get(i), allMergeResults.get(i));
      }
      for (int i = 0; i < allToBeMerged.size(); i++) {
        mergeCallback.onMerge(
            allToBeMerged.get(i), allActiveToBeMerged.get(i), allMergeResults.get(i));
        recordMerge(allToBeMerged.get(i), allMergeResults.get(i));
      }
      allToBeMerged.clear();
      allActiveToBeMerged.clear();
      allMergeResults.clear();
      seen.clear();
    }
  }

  @Override
  public void merge(MergeCallback<W> mergeCallback) throws Exception {
    MergeContextImpl context = new MergeContextImpl(mergeCallback);

    // See what the window function does with the NEW and already ACTIVE windows.
    // Entering userland.
    windowFn.mergeWindows(context);

    // Actually do the merging and invoke the callbacks.
    context.recordMerges();
  }

  /**
   * A {@link WindowFn#mergeWindows} call has determined that {@code toBeMerged} (which must
   * all be ACTIVE}) should be considered equivalent to {@code activeWindow} (which is either a
   * member of {@code toBeMerged} or is a new window). Make the corresponding change in
   * the active window set.
   */
  private void recordMerge(Collection<W> toBeMerged, W mergeResult) throws Exception {
    Set<W> newStateAddressWindows = new LinkedHashSet<>();
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
      } else if (otherStateAddressWindows.isEmpty()) {
        // Other was NEW thus has no state. It is now EPHEMERAL.
        newEphemeralWindows.add(other);
      } else if (other.equals(mergeResult)) {
        // Other was ACTIVE, was never used to store elements, but is still ACTIVE.
        // Leave it as active.
      } else {
        // Other was ACTIVE, was never used to store element, as is no longer considered ACTIVE.
        // It is now EPHEMERAL.
        newEphemeralWindows.add(other);
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

    merged(mergeResult);
  }

  @Override
  public void merged(W window) {
    Set<W> stateAddressWindows = activeWindowToStateAddressWindows.get(window);
    Preconditions.checkState(stateAddressWindows != null, "Window %s is not ACTIVE", window);
    W first = Iterables.getFirst(stateAddressWindows, null);
    stateAddressWindows.clear();
    stateAddressWindows.add(first);
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

  @Override
  public W mergedWriteStateAddress(Collection<W> toBeMerged, W mergeResult) {
    Set<W> stateAddressWindows = activeWindowToStateAddressWindows.get(mergeResult);
    if (stateAddressWindows != null && !stateAddressWindows.isEmpty()) {
      return Iterables.getFirst(stateAddressWindows, null);
    }
    for (W mergedWindow : toBeMerged) {
      stateAddressWindows = activeWindowToStateAddressWindows.get(mergedWindow);
      if (stateAddressWindows != null && !stateAddressWindows.isEmpty()) {
        return Iterables.getFirst(stateAddressWindows, null);
      }
    }
    return mergeResult;
  }

  @VisibleForTesting
  public void checkInvariants() {
    Set<W> knownStateAddressWindows = new HashSet<>();
    for (Map.Entry<W, Set<W>> entry : activeWindowToStateAddressWindows.entrySet()) {
      W active = entry.getKey();
      Preconditions.checkState(!entry.getValue().isEmpty(),
          "Unexpected empty state address window set for ACTIVE window %s",
          active);
      for (W stateAddressWindow : entry.getValue()) {
        Preconditions.checkState(knownStateAddressWindows.add(stateAddressWindow),
            "%s is in more than one state address window set",
            stateAddressWindow);
        Preconditions.checkState(active.equals(windowToActiveWindow.get(stateAddressWindow)),
            "%s should have %s as its ACTIVE window", stateAddressWindow,
            active);
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
            "%s is EPHEMERAL/state address of more than one ACTIVE window",
            ephemeralWindow);
        Preconditions.checkState(active.equals(windowToActiveWindow.get(ephemeralWindow)),
            "%s should have %s as its ACTIVE window", ephemeralWindow, active);
      }
    }
    for (Map.Entry<W, W> entry : windowToActiveWindow.entrySet()) {
      Preconditions.checkState(activeWindowToStateAddressWindows.containsKey(entry.getValue()),
          "%s should be ACTIVE since mergeResultWindow for %s",
          entry.getValue(), entry.getKey());
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
  private static <W> Map<W, Set<W>> emptyIfNull(@Nullable Map<W, Set<W>> multimap) {
    if (multimap == null) {
      return new HashMap<>();
    } else {
      for (Map.Entry<W, Set<W>> entry : multimap.entrySet()) {
        if (entry.getValue() == null) {
          entry.setValue(new LinkedHashSet<W>());
        }
      }
      return multimap;
    }
  }

  /**
   * Return a deep copy of {@code multimap}.
   */
  private static <W> Map<W, Set<W>> deepCopy(Map<W, Set<W>> multimap) {
    Map<W, Set<W>> newMultimap = new HashMap<>();
    for (Map.Entry<W, Set<W>> entry : multimap.entrySet()) {
      newMultimap.put(entry.getKey(), new LinkedHashSet<>(entry.getValue()));
    }
    return newMultimap;
  }

  /**
   * Return inversion of {@code multimap}, which must be invertible.
   */
  private static <W> Map<W, W> invert(Map<W, Set<W>> multimap) {
    Map<W, W> result = new HashMap<>();
    for (Map.Entry<W, Set<W>> entry : multimap.entrySet()) {
      W active = entry.getKey();
      for (W target : entry.getValue()) {
        W previous = result.put(target, active);
        Preconditions.checkState(
            previous == null,
            "Multimap is not invertible: Window %s has both %s and %s as representatives",
            target, previous, active);
      }
    }
    return result;
  }
}
