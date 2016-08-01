package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Arrays;

class InputTranslator implements OperatorTranslator<FlowUnfolder.InputOperator> {

  @Override
  public DataStream<?> translate(FlowUnfolder.InputOperator operator,
                                 ExecutorContext context)
  {
    // get original datasource from operator
    DataSource<?> ds = operator.output().getSource();

    // TODO dummy input
    return context.getExecutionEnvironment().fromCollection(
            Arrays.asList("flink", "test"));
  }
}
