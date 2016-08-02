package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.flink.translation.functions.PartitionerWrapper;
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

    PartitionerWrapper flinkPartitioner =
            new PartitionerWrapper<>(partitioning.getPartitioner());

    // ~ parallelism is not set directly to partitionCustom() transformation
    // but instead it's set on downstream operations
    // http://apache-flink-mailing-list-archive.1008284.n3.nabble.com/DataStream-partitionCustom-define-parallelism-td12597.html

    return input.partitionCustom(flinkPartitioner, elem -> elem);
  }
}
