package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.operator.Operator;
import org.apache.spark.api.java.JavaRDD;

public interface SparkOperatorTranslator<OP extends Operator> {

  /**
   * Translates Euphoria {@code Operator} to Spark transformation
   *
   * @param operator    Euphoria operator
   * @param context     Processing context aware of all inputs of given operator
   * @return Output of transformation in Flink API
   */
  JavaRDD<?> translate(OP operator, SparkExecutorContext context);
}
