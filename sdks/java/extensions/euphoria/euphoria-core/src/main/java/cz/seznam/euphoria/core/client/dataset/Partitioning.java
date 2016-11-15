
package cz.seznam.euphoria.core.client.dataset;

import java.io.Serializable;

/**
 * Partitioning of a dataset.
 */
public interface Partitioning<T> extends Serializable {
  
  final static Partitioner DEFAULT_PARTITIONER = new DefaultPartitioner();

  /** Retrieve partitioner for dataset. */
  default Partitioner<T> getPartitioner() {
    return DEFAULT_PARTITIONER;
  }

  default int getNumPartitions() {
    return -1;
  }
  
  /**
   * @return true if the default partitioner is used - e.g. no other has been explicitly set.
   *         Should not be called after distribution.
   */
  default boolean hasDefaultPartitioner() {
    return getPartitioner() instanceof DefaultPartitioner; // DefaultPartitioner is final
  }
}
