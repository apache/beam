package cz.seznam.euphoria.flink.batch.io;

import cz.seznam.euphoria.core.client.io.Partition;
import org.apache.flink.core.io.LocatableInputSplit;

class PartitionWrapper<T> extends LocatableInputSplit {

  private final Partition<T> partition;

  public PartitionWrapper(int splitNumber, Partition<T> partition) {
    super(splitNumber, partition.getLocations().toArray(
            new String[partition.getLocations().size()]));

    this.partition = partition;
  }

  public Partition<T> getPartition() {
    return partition;
  }
}
