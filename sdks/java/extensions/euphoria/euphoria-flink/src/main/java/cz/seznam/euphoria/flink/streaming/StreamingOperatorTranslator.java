package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.flink.FlinkOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

interface StreamingOperatorTranslator<T extends Operator> {

  /**
   * Translates Euphoria {@code FlinkOperator} to Flink transformation
   *
   * @param operator    Euphoria operator
   * @param context     Processing context aware of all inputs of given operator
   * @return Output of transformation in Flink API
   */
  DataStream translate(FlinkOperator<T> operator, StreamingExecutorContext context);
}
