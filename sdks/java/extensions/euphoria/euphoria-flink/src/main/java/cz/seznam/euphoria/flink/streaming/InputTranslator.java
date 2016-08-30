package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.flink.streaming.io.DataSourceWrapper;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.flink.FlinkOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

class InputTranslator implements StreamingOperatorTranslator<FlowUnfolder.InputOperator> {

  @Override
  public DataStream<?> translate(FlinkOperator<FlowUnfolder.InputOperator> operator,
                                 StreamingExecutorContext context)
  {
    // get original datasource from operator
    DataSource<?> ds = operator.output().getSource();

    return context.getExecutionEnvironment()
            .addSource(new DataSourceWrapper<>(ds))
            .setParallelism(operator.getParallelism());
  }
}
