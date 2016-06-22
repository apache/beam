package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.util.Pair;

import java.util.Collection;

public interface MergingWindowing<T, GROUP, LABEL, W extends Window<GROUP, LABEL>>
    extends Windowing<T, GROUP, LABEL, W>
{
  /**
   * Given a set of active windows (for a single group) determine which of the windows
   * can be merged. The pairs in the return value define:
   * "(windows-to-be-merged, merge-window)". Returning the same window to be merged
   * multiple times into another is considered erroneous. Note that a window's
   * identity (within the scope of a group) is solely determined by the window's
   * {@link Window#getLabel()}.
   *
   * @param actives a set of active windows
   *
   * @return a set of merge commands
   */
  Collection<Pair<Collection<W>, W>> mergeWindows(Collection<W> actives);

  /**
   * Determines whether the given window is considered complete and can be evicted.
   * Invoked by executors on merged windows to decide whether or not to evict them.
   * <p />
   *
   * An alternative way to signal completeness is to fire the trigger function
   * specified by the window method {@link Window#createTriggers()}}.
   *
   * @return {@code true} if this window is complete and can be evicted
   */
  default boolean isComplete(W window) {
    return false;
  }
}