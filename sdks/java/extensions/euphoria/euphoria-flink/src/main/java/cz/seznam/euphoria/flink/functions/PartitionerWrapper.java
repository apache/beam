package cz.seznam.euphoria.flink.functions;

import cz.seznam.euphoria.core.client.dataset.Partitioner;

public class PartitionerWrapper<T>
        implements org.apache.flink.api.common.functions.Partitioner<T>
{
  private final Partitioner<T> partitioner;

  public PartitionerWrapper(Partitioner<T> partitioner) {
    this.partitioner = partitioner;
  }

  @Override
  public int partition(T elem, int numPartitions) {
    return partitioner.getPartition(elem) % numPartitions;
  }
}
