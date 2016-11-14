package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import org.apache.flink.api.java.DataSet;

class RepartitionTranslator implements BatchOperatorTranslator<Repartition> {

  @Override
  public DataSet translate(FlinkOperator<Repartition> operator,
      BatchExecutorContext context) {
    
    DataSet<WindowedElement> input = 
        (DataSet<WindowedElement>)context.getSingleInputStream(operator);
    
    Partitioning partitioning = operator.getOriginalOperator().getPartitioning();
    PartitionerWrapper flinkPartitioner = 
        new PartitionerWrapper<>(partitioning.getPartitioner());
    
    return input.partitionCustom(
            flinkPartitioner,
            Utils.wrapQueryable((WindowedElement we) -> (Comparable) we.get(), Comparable.class))
        .setParallelism(operator.getParallelism());
  }
}
