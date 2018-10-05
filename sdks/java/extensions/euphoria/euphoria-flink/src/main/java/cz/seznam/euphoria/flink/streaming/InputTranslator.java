package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.streaming.io.DataSourceWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;

class InputTranslator implements StreamingOperatorTranslator<FlowUnfolder.InputOperator> {

  @Override
  public DataStream<?> translate(FlinkOperator<FlowUnfolder.InputOperator> operator,
                                 StreamingExecutorContext context)
  {
    // get original datasource from operator
    DataSource<?> ds = operator.output().getSource();

    return context.getExecutionEnvironment()
            .addSource(new DataSourceWrapper<>(ds))
            .setParallelism(operator.getParallelism())
            .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());
  }
}
