package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.batch.io.DataSourceWrapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

class InputTranslator implements BatchOperatorTranslator<FlowUnfolder.InputOperator> {

  @Override
  public DataSet translate(FlinkOperator<FlowUnfolder.InputOperator> operator,
                           BatchExecutorContext context)
  {
    // get original datasource from operator
    DataSource<?> ds = operator.output().getSource();

    org.apache.flink.api.java.operators.DataSource fds = context
        .getExecutionEnvironment()
        .createInput(new DataSourceWrapper<>(ds))
        .setParallelism(operator.getParallelism());
    return fds.map((MapFunction) value ->
        new WindowedElement<>(WindowID.aligned(Batch.Label.get()), value))
        .returns(WindowedElement.class)
        .setParallelism(operator.getParallelism());
  }
}
