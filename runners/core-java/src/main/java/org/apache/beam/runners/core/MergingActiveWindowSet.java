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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An {@link ActiveWindowSet} for merging {@link WindowFn} implementations. */
public class MergingActiveWindowSet<W extends BoundedWindow> implements ActiveWindowSet<W> {
  private final WindowFn<Object, W> windowFn;

  /**
   * Map ACTIVE and NEW windows to their state address windows. Persisted.
   *
   * <ul>
   *   <li>A NEW window has the empty set as its value.
   *   <li>An ACTIVE window has its (typically singleton) set of state address windows as its value.
   * </ul>
   */
  private final Map<W, Set<W>> activeWindowToStateAddressWindows;

  /**
   * Deep clone of {@link #activeWindowToStateAddressWindows} as of last commit. Used to avoid
   * writing to state if no changes have been made during the work unit.
   */
  private final Map<W, Set<W>> originalActiveWindowToStateAddressWindows;

  /** Handle representing our state in the backend. */
  private final ValueState<Map<W, Set<W>>> valueState;

  public MergingActiveWindowSet(WindowFn<Object, W> windowFn, StateInternals state) {
    this.windowFn = windowFn;

    StateTag<ValueState<Map<W, Set<W>>>> tag =
        StateTags.makeSystemTagInternal(
            StateTags.value(
                "tree", MapCoder.of(windowFn.windowCoder(), SetCoder.of(windowFn.windowCoder()))));
    valueState = state.state(StateNamespaces.global(), tag);
    // Little use trying to prefetch this state since the ReduceFnRunner
    // is stymied until it is available.
    activeWindowToStateAddressWindows = emptyIfNull(valueState.read());
    originalActiveWindowToStateAddressWindows = deepCopy(activeWindowToStateAddressWindows);
  }

  @Override
  public void cleanupTemporaryWindows() {
    // All NEW windows can be forgotten since they must have ended up being merged into
    // some other ACTIVE window.
    activeWindowToStateAddressWindows.entrySet().removeIf(entry -> entry.getValue().isEmpty());
  }

  @Override
  public void persist() {
    checkInvariants();
    if (activeWindowToStateAddressWindows.isEmpty()) {
      // Force all persistent state to disappear.
      valueState.clear();
      return;
    }
    if (activeWindowToStateAddressWindows.equals(originalActiveWindowToStateAddressWindows)) {
      // No change.
      return;
    }
    valueState.write(activeWindowToStateAddressWindows);
    // No need to update originalActiveWindowToStateAddressWindows since this object is about to
    // become garbage.
  }

  @Override
  public Set<W> getActiveAndNewWindows() {
    return activeWindowToStateAddressWindows.keySet();
  }

  @Override
  public boolean isActive(W window) {
    Set<W> stateAddressWindows = activeWindowToStateAddressWindows.get(window);
    return stateAddressWindows != null && !stateAddressWindows.isEmpty();
  }

  @Override
  public boolean isActiveOrNew(W window) {
    return activeWindowToStateAddressWindows.containsKey(window);
  }

  @Override
  public void ensureWindowExists(W window) {
    if (!activeWindowToStateAddressWindows.containsKey(window)) {
      // Add window as NEW.
      activeWindowToStateAddressWindows.put(window, new LinkedHashSet<>());
    }
  }

  @Override
  public void ensureWindowIsActive(W window) {
    Set<W> stateAddressWindows = activeWindowToStateAddressWindows.get(window);
    checkState(
        stateAddressWindows != null,
        "Cannot ensure window %s is active since it is neither ACTIVE nor NEW",
        window);
    if (stateAddressWindows.isEmpty()) {
      // Window was NEW, make it ACTIVE with itself as its state address window.
      stateAddressWindows.add(window);
    }
  }

  @Override
  @VisibleForTesting
  public void addActiveForTesting(W window) {
    if (!activeWindowToStateAddressWindows.containsKey(window)) {
      // Make window ACTIVE with itself as its state address window.
      Set<W> stateAddressWindows = new LinkedHashSet<>();
      stateAddressWindows.add(window);
      activeWindowToStateAddressWindows.put(window, stateAddressWindows);
    }
  }

  @VisibleForTesting
  public void addActiveForTesting(W window, Iterable<W> stateAddressWindows) {
    if (!activeWindowToStateAddressWindows.containsKey(window)) {
      activeWindowToStateAddressWindows.put(window, Sets.newLinkedHashSet(stateAddressWindows));
    }
  }

  @Override
  public void remove(W window) {
    activeWindowToStateAddressWindows.remove(window);
  }

  private class MergeContextImpl extends WindowFn<Object, W>.MergeContext {
    private MergeCallback<W> mergeCallback;
    private final List<Collection<W>> allToBeMerged;
    private final List<W> allMergeResults;
    private final Set<W> seen;

    public MergeContextImpl(MergeCallback<W> mergeCallback) {
      windowFn.super();
      this.mergeCallback = mergeCallback;
      allToBeMerged = new ArrayList<>();
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
      checkNotNull(toBeMerged);
      checkNotNull(mergeResult);
      List<W> copyOfToBeMerged = new ArrayList<>(toBeMerged.size());
      boolean includesMergeResult = false;
      for (W window : toBeMerged) {
        checkNotNull(window);
        checkState(isActiveOrNew(window), "Expecting merge window %s to be ACTIVE or NEW", window);
        if (window.equals(mergeResult)) {
          includesMergeResult = true;
        }
        boolean notDup = seen.add(window);
        checkState(notDup, "Expecting merge window %s to appear in at most one merge set", window);
        copyOfToBeMerged.add(window);
      }
      if (!includesMergeResult) {
        checkState(!isActive(mergeResult), "Expecting result window %s to be NEW", mergeResult);
      }
      allToBeMerged.add(copyOfToBeMerged);
      allMergeResults.add(mergeResult);
    }

    public void recordMerges() throws Exception {
      for (int i = 0; i < allToBeMerged.size(); i++) {
        mergeCallback.prefetchOnMerge(allToBeMerged.get(i), allMergeResults.get(i));
      }
      for (int i = 0; i < allToBeMerged.size(); i++) {
        mergeCallback.onMerge(allToBeMerged.get(i), allMergeResults.get(i));
        recordMerge(allToBeMerged.get(i), allMergeResults.get(i));
      }
      allToBeMerged.clear();
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
   * A {@link WindowFn#mergeWindows} call has determined that {@code toBeMerged} (which must all be
   * ACTIVE}) should be considered equivalent to {@code activeWindow} (which is either a member of
   * {@code toBeMerged} or is a new window). Make the corresponding change in the active window set.
   */
  private void recordMerge(Collection<W> toBeMerged, W mergeResult) throws Exception {
    // Note that mergedWriteStateAddress must predict the result of writeStateAddress
    // after the corresponding merge has been applied.
    // Thus we must ensure the first state address window in the merged result here is
    // deterministic.
    // Thus we use a linked hash set.
    Set<W> newStateAddressWindows = new LinkedHashSet<>();
    Set<W> existingStateAddressWindows = activeWindowToStateAddressWindows.get(mergeResult);
    if (existingStateAddressWindows != null) {
      // Preserve all the existing state address windows for mergeResult.
      newStateAddressWindows.addAll(existingStateAddressWindows);
    }

    for (W other : toBeMerged) {
      Set<W> otherStateAddressWindows = activeWindowToStateAddressWindows.get(other);
      checkState(otherStateAddressWindows != null, "Window %s is not ACTIVE or NEW", other);

      for (W otherStateAddressWindow : otherStateAddressWindows) {
        // Since otherTarget equiv other AND other equiv mergeResult
        // THEN otherTarget equiv mergeResult.
        newStateAddressWindows.add(otherStateAddressWindow);
      }
      activeWindowToStateAddressWindows.remove(other);

      // Now other equiv mergeResult.
    }

    if (newStateAddressWindows.isEmpty()) {
      // If stateAddressWindows is empty then toBeMerged must have only contained EPHEMERAL windows.
      // Promote mergeResult to be ACTIVE now.
      newStateAddressWindows.add(mergeResult);
    }

    activeWindowToStateAddressWindows.put(mergeResult, newStateAddressWindows);

    merged(mergeResult);
  }

  @Override
  public void merged(W window) {
    // Take just the first state address window.
    Set<W> stateAddressWindows = activeWindowToStateAddressWindows.get(window);
    checkState(stateAddressWindows != null, "Window %s is not ACTIVE", window);
    W first = Iterables.getFirst(stateAddressWindows, null);
    stateAddressWindows.clear();
    stateAddressWindows.add(first);
  }

  /**
   * Return the state address windows for ACTIVE {@code window} from which all state associated
   * should be read and merged.
   */
  @Override
  public Set<W> readStateAddresses(W window) {
    Set<W> stateAddressWindows = activeWindowToStateAddressWindows.get(window);
    checkState(stateAddressWindows != null, "Window %s is not ACTIVE", window);
    return stateAddressWindows;
  }

  /**
   * Return the state address window of ACTIVE {@code window} into which all new state should be
   * written.
   */
  @Override
  public W writeStateAddress(W window) {
    Set<W> stateAddressWindows = activeWindowToStateAddressWindows.get(window);
    checkState(stateAddressWindows != null, "Window %s is not ACTIVE", window);
    W result = Iterables.getFirst(stateAddressWindows, null);
    checkState(result != null, "Window %s is still NEW", window);
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
      checkState(
          !entry.getValue().isEmpty(),
          "Unexpected empty state address window set for ACTIVE window %s",
          active);
      for (W stateAddressWindow : entry.getValue()) {
        checkState(
            knownStateAddressWindows.add(stateAddressWindow),
            "%s is in more than one state address window set",
            stateAddressWindow);
      }
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
          sb.append("    ");
          sb.append(stateAddressWindow);
          sb.append("\n");
        }
      }
    }
    sb.append("}");
    return sb.toString();
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (!(o instanceof MergingActiveWindowSet)) {
      return false;
    }

    @SuppressWarnings("unchecked")
    MergingActiveWindowSet<W> other = (MergingActiveWindowSet<W>) o;

    return activeWindowToStateAddressWindows.equals(other.activeWindowToStateAddressWindows);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(activeWindowToStateAddressWindows);
  }

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
          entry.setValue(new LinkedHashSet<>());
        }
      }
      return multimap;
    }
  }

  /** Return a deep copy of {@code multimap}. */
  private static <W> Map<W, Set<W>> deepCopy(Map<W, Set<W>> multimap) {
    Map<W, Set<W>> newMultimap = new HashMap<>();
    for (Map.Entry<W, Set<W>> entry : multimap.entrySet()) {
      newMultimap.put(entry.getKey(), new LinkedHashSet<>(entry.getValue()));
    }
    return newMultimap;
  }
}
