package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.flink.translation.io.DataSourceWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;

class InputTranslator implements OperatorTranslator<FlowUnfolder.InputOperator> {

  @Override
  public DataStream<?> translate(FlowUnfolder.InputOperator operator,
                                 ExecutorContext context,
                                 int parallelism)
  {
    // get original datasource from operator
    DataSource<?> ds = operator.output().getSource();

    return context.getExecutionEnvironment()
            .addSource(new DataSourceWrapper<>(ds))
            .setParallelism(parallelism);
  }
}
