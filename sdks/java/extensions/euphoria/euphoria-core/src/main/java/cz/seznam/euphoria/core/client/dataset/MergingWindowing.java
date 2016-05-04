package cz.seznam.euphoria.core.client.dataset;

import java.util.Set;

public interface MergingWindowing<KEY, W extends Window<KEY>> {

  interface Merging<KEY, W extends Window<KEY>> {
    void onMerge(Set<W> mergedWindows, W mergeResult);
  }

  void mergeWindows(Set<W> actives, Merging<KEY, W> merging);

}