
package cz.seznam.euphoria.core.client.dataset;

import java.io.Serializable;

/**
 * Partitioner assigns elements into partitions.
 */
public interface Partitioner<T> extends Serializable {

  /**
   * Retrieve ID of partition. The ID of partition is then taken modulo
   * the number of partitions.
   */
  int getPartition(T element);

}
