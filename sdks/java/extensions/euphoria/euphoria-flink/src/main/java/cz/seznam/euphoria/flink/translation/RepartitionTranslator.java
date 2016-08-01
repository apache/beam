package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.operator.Repartition;
import org.apache.flink.streaming.api.datastream.DataStream;

class RepartitionTranslator implements OperatorTranslator<Repartition> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(Repartition operator,
                                 ExecutorContext context)
  {
    DataStream input = context.getInputStream(operator);
    Partitioning partitioning = operator.getPartitioning();

    FlinkPartitioner flinkPartitioner =
            new FlinkPartitioner<>(partitioning.getPartitioner());

    // TODO not sure how to change number of output partitions
    // http://apache-flink-mailing-list-archive.1008284.n3.nabble.com/DataStream-partitionCustom-define-parallelism-td12597.html

    return input.partitionCustom(flinkPartitioner, elem -> elem);
  }

  private static class FlinkPartitioner<T>
          implements org.apache.flink.api.common.functions.Partitioner<T>
  {
    private final Partitioner<T> partitioner;

    public FlinkPartitioner(Partitioner<T> partitioner) {
      this.partitioner = partitioner;
    }

    @Override
    public int partition(T elem, int numPartitions) {
      return partitioner.getPartition(elem) % numPartitions;
    }
  }
}
