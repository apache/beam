package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.util.Pair;

import java.util.Collection;

public interface MergingWindowing<T, W extends Window & Comparable<W>>
    extends Windowing<T, W>
{
  /**
   * Given a set of active windows (for a single key) determine which of the windows
   * can be merged. The pairs in the return value define:
   * "(windows-to-be-merged, merge-window)". Returning the same window to be merged
   * multiple times into another is considered erroneous. Note that a window's
   * identity is solely determined by the {@link Window}.
   *
   * @param actives a set of active windows
   *
   * @return a set of merge commands
   */
  Collection<Pair<Collection<W>, W>> mergeWindows(Collection<W> actives);
}