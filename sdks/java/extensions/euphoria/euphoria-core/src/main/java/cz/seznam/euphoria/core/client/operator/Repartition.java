
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Repartition input to some other number of partitions.
 */
public class Repartition<IN>
    extends ElementWiseOperator<IN, IN>
    implements PartitioningAware<IN> {

  public static class Builder1<IN> {
    final Dataset<IN> input;
    Builder1(Dataset<IN> input) {
      this.input = input;
    }
    public Builder2<IN> partitionBy(Partitioner<IN> partitioner) {
      return new Builder2<>(input, partitioner);
    }
    public Repartition<IN> setNumPartitions(int partitions) {
      Flow flow = input.getFlow();
      return flow.add(new Repartition<>(flow, input,
          new HashPartitioner<>(), partitions));
    }
  }
  public static class Builder2<IN> {
    final Dataset<IN> input;
    final Partitioner<IN> partitioner;
    Builder2(Dataset<IN> input, Partitioner<IN> partitioner) {
      this.input = input;
      this.partitioner = partitioner;
    }
    public Repartition<IN> setNumPartitions(int partitions) {
      Flow flow = input.getFlow();
      return flow.add(new Repartition<>(flow, input, partitioner, partitions));
    }
  }

  public static <IN> Builder1<IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }

  private final Partitioning<IN> partitioning;

  Repartition(Flow flow, Dataset<IN> input,
      final Partitioner<IN> partitioner, final int numPartitions) {
    super("Repartition", flow, input);
    this.partitioning = new Partitioning<IN>() {

      @Override
      public Partitioner<IN> getPartitioner() {
        return partitioner;
      }

      @Override
      public int getNumPartitions() {
        return numPartitions;
      }

    };
  }

  @Override
  public Partitioning<IN> getPartitioning() {
    return partitioning;
  }


}
