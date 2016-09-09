package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;

class RepartitionTranslator implements StreamingOperatorTranslator<Repartition> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<Repartition> operator,
                                 StreamingExecutorContext context)
  {
    DataStream<StreamingWindowedElement> input =
        (DataStream<StreamingWindowedElement>) context.getSingleInputStream(operator);
    Partitioning partitioning = operator.getOriginalOperator().getPartitioning();

    PartitionerWrapper flinkPartitioner =
            new PartitionerWrapper<>(partitioning.getPartitioner());

    // ~ parallelism is not set directly to partitionCustom() transformation
    // but instead it's set on downstream operations
    // http://apache-flink-mailing-list-archive.1008284.n3.nabble.com/DataStream-partitionCustom-define-parallelism-td12597.html

    return input.partitionCustom(flinkPartitioner, elem -> elem.get());
  }
}
