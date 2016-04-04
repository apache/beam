
package cz.seznam.euphoria.core.client.dataset;

import java.io.Serializable;

/**
 * Partitioning of a dataset.
 */
public interface Partitioning<T> extends Serializable {

  /** Retrieve partitioner for dataset. */
  Partitioner<T> getPartitioner();

  default int getNumPartitions() {
    return -1;
  }

}
