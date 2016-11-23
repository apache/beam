package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.batch.io.DataSourceWrapper;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.util.function.BiFunction;

class InputTranslator implements BatchOperatorTranslator<FlowUnfolder.InputOperator> {
  
  private final BiFunction<InputSplit[], Integer, InputSplitAssigner> splitAssignerFactory;
  
  InputTranslator(BiFunction<InputSplit[], Integer, InputSplitAssigner> splitAssignerFactory) {
    this.splitAssignerFactory = splitAssignerFactory;
  }

  @Override
  public DataSet translate(FlinkOperator<FlowUnfolder.InputOperator> operator,
                           BatchExecutorContext context)
  {
    // get original datasource from operator
    DataSource<?> ds = operator.output().getSource();

    return context
        .getExecutionEnvironment()
        .createInput(new DataSourceWrapper<>(ds, splitAssignerFactory))
        .setParallelism(operator.getParallelism());
  }
}
