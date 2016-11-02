package cz.seznam.euphoria.spark.functions;

import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.spark.KeyedWindow;
import org.apache.spark.Partitioner;

/**
 * Wraps Euphoria {@link Partitioning} to be used in Spark API
 */
public class PartitioningWrapper extends Partitioner {

  private static final int bitShift = Integer.BYTES * 4;
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
    KeyedWindow window = (KeyedWindow) el;
    int partition = partitioning.getPartitioner().getPartition(window.key());
    int windowHash = window.window().hashCode();
    int swapped = (windowHash >> bitShift) | (windowHash << bitShift);
    return ((31 * partition ^ swapped) & Integer.MAX_VALUE) % numPartitions();
  }
}
