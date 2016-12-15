package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.Partitioning;
import org.apache.spark.Partitioner;

/**
 * Adapter between Euphoria {@link Partitioning} and Spark {@link Partitioner}
 */
class PartitioningWrapper extends Partitioner {

  private final cz.seznam.euphoria.core.client.dataset.Partitioner partitioner;
  private final int numPartitions;

  public PartitioningWrapper(Partitioning partitioning) {
    this.partitioner = partitioning.getPartitioner();
    this.numPartitions = partitioning.getNumPartitions();
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }

  @Override
  @SuppressWarnings("unchecked")
  public int getPartition(Object el) {
    KeyedWindow kw = (KeyedWindow) el;
    int partitionId = partitioner.getPartition(kw.key());

    return (partitionId & Integer.MAX_VALUE) % numPartitions();
  }
}