
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.util.Objects;

/**
 * Operator changing partitioning of a dataset.
 */
public interface PartitioningAware<KEY> {

  /** Retrieve output partitioning. */
  Partitioning<KEY> getPartitioning();

  abstract class PartitioningBuilder<KEY, BUILDER> {
    private final DefaultPartitioning<KEY> defaultPartitioning;
    private Partitioning<KEY> partitioning;

    public PartitioningBuilder(DefaultPartitioning<KEY> defaultPartitioning) {
      this(defaultPartitioning, null);
    }

    public PartitioningBuilder(DefaultPartitioning<KEY> defaultPartitioning,
                               Partitioning<KEY> partitioning)
    {
      this.defaultPartitioning = Objects.requireNonNull(defaultPartitioning);
      this.partitioning = partitioning;
    }

    public PartitioningBuilder(PartitioningBuilder<KEY, ?> other) {
      this.defaultPartitioning = other.defaultPartitioning;
      this.partitioning = other.partitioning;
    }

    Partitioning<KEY> getPartitioning() {
      if (partitioning == null) {
        return defaultPartitioning;
      }

      return partitioning;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setPartitioning(Partitioning<KEY> p) {
      this.partitioning = Objects.requireNonNull(p);
      return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setPartitioner(Partitioner<KEY> p) {
      this.defaultPartitioning.setPartitioner(p);
      return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setNumPartitions(int numPartitions) {
      this.defaultPartitioning.setNumPartitions(numPartitions);
      return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER applyIf(boolean cond, UnaryFunction<BUILDER, BUILDER> apply) {
      Objects.requireNonNull(apply);
      return cond ? apply.apply((BUILDER) this) : (BUILDER) this;
    }
  }


}
