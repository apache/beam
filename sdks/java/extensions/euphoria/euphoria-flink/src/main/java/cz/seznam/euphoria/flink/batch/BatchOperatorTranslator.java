package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.streaming.StreamingExecutorContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;

interface BatchOperatorTranslator<T extends Operator> {

  /**
   * Translates Euphoria {@code FlinkOperator} to Flink transformation
   *
   * @param operator    Euphoria operator
   * @param context     Processing context aware of all inputs of given operator
   * @return Output of transformation in Flink API
   */
  DataSet translate(FlinkOperator<T> operator, BatchExecutorContext context);
}
