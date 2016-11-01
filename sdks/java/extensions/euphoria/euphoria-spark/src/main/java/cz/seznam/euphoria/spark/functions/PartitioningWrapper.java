package cz.seznam.euphoria.spark.functions;

import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import org.apache.spark.Partitioner;

import java.util.Objects;

/**
 * Wraps Euphoria {@link Partitioning} to be used in Spark API
 */
public class PartitioningWrapper extends Partitioner {

  private final Partitioning partitioning;

  /** Function extracting key that will be used in the partitioner */
  private final UnaryFunction partitionBy;

  public PartitioningWrapper(Partitioning partitioning) {
    this.partitioning = partitioning;
    this.partitionBy = e -> e; // identity by default
  }

  public PartitioningWrapper(Partitioning partitioning, UnaryFunction partitionBy) {
    this.partitioning = Objects.requireNonNull(partitioning);
    this.partitionBy = Objects.requireNonNull(partitionBy);
  }

  @Override
  public int numPartitions() {
    return partitioning.getNumPartitions();
  }

  @Override
  @SuppressWarnings("unchecked")
  public int getPartition(Object el) {
    return partitioning.getPartitioner()
            .getPartition(partitionBy.apply(el)) % numPartitions();
  }
}
