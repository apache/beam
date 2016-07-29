package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.operator.Repartition;
import org.apache.flink.streaming.api.datastream.DataStream;

class RepartitionTranslator implements OperatorTranslator<Repartition> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(Repartition operator,
                                 ExecutorContext context,
                                 int parallelism)
  {
    DataStream input = context.getInputStream(operator);
    Partitioning partitioning = operator.getPartitioning();

    FlinkPartitioner flinkPartitioner =
            new FlinkPartitioner<>(partitioning.getPartitioner());

    // ~ parallelism is not set directly to partitionCustom() transformation
    // but instead it's set on downstream operations
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
