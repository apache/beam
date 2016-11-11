package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.operator.DefaultPartitioning;
import org.apache.spark.Partitioner;

/**
 * Wraps Euphoria {@link Partitioning} to be used in Spark API
 */
class PartitioningWrapper extends Partitioner {

  private static final int SHIFT_BITS = Integer.BYTES * 8 / 2;

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
    KeyedWindow w = (KeyedWindow) el;
    int partition = partitioning.getPartitioner().getPartition(w.key());
    final int windowHash;
    // FIXME: we can do this trick only if we have default partitioner
    // this test is insufficient
    if (partitioning.getPartitioner() instanceof HashPartitioner) {
      windowHash = w.window().hashCode();
    } else {
      windowHash = 0;
    }
    int shifted = (windowHash >> SHIFT_BITS) | (windowHash << SHIFT_BITS);
    return ((partition ^ shifted) & Integer.MAX_VALUE) % numPartitions();
  }
}
