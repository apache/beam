package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;

import java.util.Objects;

/**
 * Default implementation of {@link Partitioning} with one partition and
 * hash partitioner
 */
final class DefaultPartitioning<T> implements Partitioning<T> {

  private int numPartitions;
  private Partitioner<T> partitioner;

  public DefaultPartitioning() {
    this(1);
  }

  public DefaultPartitioning(int numPartitions) {
    this(numPartitions, new HashPartitioner<>());
  }

  public DefaultPartitioning(int numPartitions, Partitioner<T> partitioner) {
    setNumPartitions(numPartitions);
    setPartitioner(partitioner);
  }

  @Override
  public Partitioner<T> getPartitioner() {
    return partitioner;
  }

  public void setPartitioner(Partitioner<T> partitioner) {
    this.partitioner = Objects.requireNonNull(partitioner);
  }

  @Override
  public int getNumPartitions() {
    return numPartitions;
  }

  public void setNumPartitions(int numPartitions) {
    if (numPartitions <= 0) {
      throw new IllegalStateException("numPartitions must be greater than 0");
    }
    this.numPartitions = numPartitions;
  }
}
