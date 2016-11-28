package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.Partitioning;
import org.apache.spark.Partitioner;

/**
 * Adapter between Euphoria {@link Partitioning} and Spark {@link Partitioner}
 */
class PartitioningWrapper extends Partitioner {

  private final Partitioning partitioning;

  public PartitioningWrapper(Partitioning partitioning) {
    this.partitioning = partitioning;
  }

  @Override
  public int numPartitions() {
    return partitioning.getNumPartitions();
  }

  @Override
  @SuppressWarnings("unchecked")
  public int getPartition(Object el) {
    KeyedWindow kw = (KeyedWindow) el;
    return partitioning.getPartitioner().getPartition(kw.key()) % numPartitions();
  }
}