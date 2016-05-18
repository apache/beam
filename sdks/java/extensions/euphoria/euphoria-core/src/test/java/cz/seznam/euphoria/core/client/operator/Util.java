package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSource;

import java.util.ArrayList;
import java.util.List;

class Util {

  static <T> Dataset<T> createMockDataset(Flow flow, int numPartitions) {
    @SuppressWarnings("unchecked")
    List<T>[] partitions = new List[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      partitions[i] = new ArrayList<>();
    }

    return flow.createInput(ListDataSource.bounded(partitions));
  }
}
