
package cz.seznam.euphoria.core.client.dataset;

import java.io.Serializable;

/**
 * Partitioning of a dataset.
 */
public interface Partitioning<T> extends Serializable {
  
  final static Partitioner DEFAULT_PARTITIONER = new HashPartitioner<>();

  /** Retrieve partitioner for dataset. */
  Partitioner<T> getPartitioner();

  default int getNumPartitions() {
    return -1;
  }
  
  /**
   * @return true if the default partitioner is used - e.g. no other has been explicitly set.
   *         Should not be called after distribution.
   */
  default boolean hasDefaultPartitioner() {
    return getPartitioner() == DEFAULT_PARTITIONER;
  }
}
