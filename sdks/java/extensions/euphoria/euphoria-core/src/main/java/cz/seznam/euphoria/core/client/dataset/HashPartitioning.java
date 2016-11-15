
package cz.seznam.euphoria.core.client.dataset;

/**
 * Partitioning by hashcode of input.
 */
public class HashPartitioning<T> implements Partitioning<T> {

  private final int numPartitions;

  public HashPartitioning() {
    this(1);
  }

  public HashPartitioning(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  @Override
  public Partitioner<T> getPartitioner() {
    return new HashPartitioner<>();
  }

  @Override
  public int getNumPartitions() {
    return numPartitions;
  }
}
