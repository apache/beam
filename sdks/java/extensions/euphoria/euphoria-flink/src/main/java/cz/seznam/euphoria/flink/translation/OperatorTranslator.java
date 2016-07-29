package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.operator.Operator;
import org.apache.flink.streaming.api.datastream.DataStream;

interface OperatorTranslator<T extends Operator> {

  /**
   * Translates Euphoria {@code Operator} to Flink transformation
   *
   * @param operator    Euphoria operator
   * @param context     Processing context aware of all inputs of given operator
   * @param parallelism Degree of parallelism of this operator
   * @return Output of transformation in Flink API
   */
  DataStream translate(T operator, ExecutorContext context, int parallelism);
}
