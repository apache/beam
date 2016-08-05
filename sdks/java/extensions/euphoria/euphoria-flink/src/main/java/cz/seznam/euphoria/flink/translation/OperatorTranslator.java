package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.operator.Operator;
import org.apache.flink.streaming.api.datastream.DataStream;

interface OperatorTranslator<T extends Operator> {

  /**
   * Translates Euphoria {@code FlinkOperator} to Flink transformation
   *
   * @param operator    Euphoria operator
   * @param context     Processing context aware of all inputs of given operator
   * @return Output of transformation in Flink API
   */
  DataStream translate(FlinkOperator<T> operator, ExecutorContext context);
}
