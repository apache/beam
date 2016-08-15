package cz.seznam.euphoria.flink.batch.io;

import cz.seznam.euphoria.core.client.io.Partition;
import org.apache.flink.core.io.InputSplit;

class PartitionWrapper<T> implements InputSplit {

  private final int splitNumber;
  private final Partition<T> partition;

  public PartitionWrapper(int splitNumber, Partition<T> partition) {
    this.splitNumber = splitNumber;
    this.partition = partition;
  }

  @Override
  public int getSplitNumber() {
    return splitNumber;
  }

  public Partition<T> getPartition() {
    return partition;
  }
}
