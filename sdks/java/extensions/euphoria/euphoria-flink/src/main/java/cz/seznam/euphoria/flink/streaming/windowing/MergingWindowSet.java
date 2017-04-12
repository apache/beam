/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.util.Pair;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to keep track of currently existing {@link Window} instances
 * when using {@link MergingWindowing}.
 *
 * @param <W> type of the window
 */
class MergingWindowSet<W extends Window> {

  private final MergingWindowing windowing;

  /**
   * Mapping from window to the window that keeps the window state. When
   * we are incrementally merging windows starting from some window we keep that starting
   * window as the state window to prevent costly state juggling.
   * Inspired by the idea from Flink,
   * see {@link org.apache.flink.api.common.state.MergingState}
   */
  private final Map<W, W> mapping;

  /**
   * Snapshot of initial state during WindowSet creation. Used to decide
   * if there are changes that needs to be persisted.
   */
  private final Map<W, W> initialMapping;

  private final ListState<Tuple2<W, W>> state;

  public MergingWindowSet(MergingWindowing windowing,
                          ListState<Tuple2<W, W>> state) throws Exception {
    this.windowing = windowing;
    this.state = state;

    mapping = new HashMap<>();

    // load windows from persistent state storage
    Iterable<Tuple2<W, W>> it = state.get();
    if (it != null) {
      for (Tuple2<W, W> window : it) {
        mapping.put(window.f0, window.f1);
      }
    }

    initialMapping = new HashMap<>();
    initialMapping.putAll(mapping);
  }

  /**
   * Persist current internal state of the {@link MergingWindowSet} to the
   * Flink state storage.
   */
  public void persist() throws Exception {
    if (!mapping.equals(initialMapping)) {
      state.clear();
      for (Map.Entry<W, W> e : mapping.entrySet()) {
        state.add(new Tuple2<>(e.getKey(), e.getValue()));
      }
    }
  }

  public void removeWindow(W window) throws Exception {
    if (mapping.remove(window) == null) {
      throw new IllegalStateException("Non-existing window " + window);
    }
  }

  public W getStateWindow(W window) {
    return mapping.get(window);
  }

  /**
   * Add a new {@link Window} to the window set. It may trigger merging.
   *
   * @param newWindow The window that added window ended up in. Can be result
   *                  of merge or the original itself.
   * @return Window that added window ended up in after merge.
   */
  public W addWindow(W newWindow, MergeCallback<W> callback) throws Exception {
    Set<W> windows = new HashSet<>(this.mapping.keySet());
    windows.add(newWindow);

    @SuppressWarnings("unchecked")
    Collection<Pair<Collection<W>, W>> mergeResults = windowing.mergeWindows(windows);

    W resultWindow = newWindow;
    boolean mergedNewWindow = false;

    for (Pair<Collection<W>, W> merges : mergeResults) {
      W mergeResult = merges.getSecond();
      Collection<W> mergedWindows = merges.getFirst();

      // If our new window is in the merged windows make the merge result the
      // result window
      if (mergedWindows.remove(newWindow)) {
        mergedNewWindow = true;
        resultWindow = mergeResult;
      }

      // Remove mergeResult result from mergedWindows if present.
      // This covers the case when windows (1, 2) are merged to (1)
      mergedWindows.remove(mergeResult);

      // If no window is actually merged we can skip merging phase.
      if (mergedWindows.isEmpty()) {
        continue;
      }

      // Pick any of the merged windows and choose that window's state window
      // as the state window for the merge result
      W mergedStateWindow = this.mapping.get(mergedWindows.iterator().next());

      // Compute the state windows that we are merging
      List<W> mergedStateWindows = new ArrayList<>();
      for (W mergedWindow : mergedWindows) {
        W res = this.mapping.remove(mergedWindow);
        if (res != null) {
          mergedStateWindows.add(res);
        }
      }

      this.mapping.put(mergeResult, mergedStateWindow);

      // Don't put the target state window into the merged windows
      mergedStateWindows.remove(mergedStateWindow);

      callback.merge(mergeResult,
              mergedWindows,
              mergedStateWindow,
              mergedStateWindows);
    }

    if (mergeResults.isEmpty() || (resultWindow.equals(newWindow) && !mergedNewWindow)) {
      mapping.put(resultWindow, resultWindow);
    }

    return resultWindow;
  }

  public interface MergeCallback<W> {

    /**
     * This gets called when a merge occurs.
     *
     * @param mergeResult Result window of merging.
     * @param mergedWindows ist of windows that have been removed by merge operation.
     * @param stateResultWindow Window actually holding the current result window state.
     * @param mergedStateWindows List of state windows that have been merged/removed.
     */
    void merge(W mergeResult,
               Iterable<W> mergedWindows,
               W stateResultWindow,
               Iterable<W> mergedStateWindows) throws Exception;
  }
}
