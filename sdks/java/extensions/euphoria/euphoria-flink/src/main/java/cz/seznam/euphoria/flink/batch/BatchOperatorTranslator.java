package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.flink.FlinkOperator;
import org.apache.flink.api.java.DataSet;

interface BatchOperatorTranslator<T extends Operator> {

  String CFG_MAX_MEMORY_ELEMENTS_KEY = "euphoria.flink.batch.state.max.memory.elements";
  int CFG_MAX_MEMORY_ELEMENTS_DEFAULT = 1000;

  /**
   * Translates Euphoria {@code FlinkOperator} to Flink transformation
   *
   * @param operator    Euphoria operator
   * @param context     Processing context aware of all inputs of given operator
   * @return Output of transformation in Flink API
   */
  DataSet translate(FlinkOperator<T> operator, BatchExecutorContext context);
}
