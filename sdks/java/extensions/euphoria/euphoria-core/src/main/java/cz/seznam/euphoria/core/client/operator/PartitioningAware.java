
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Partitioning;

/**
 * Operator changing partitioning of a dataset.
 */
public interface PartitioningAware<KEY> {

  /** Retrieve output partitioning. */
  Partitioning<KEY> getPartitioning();

}
