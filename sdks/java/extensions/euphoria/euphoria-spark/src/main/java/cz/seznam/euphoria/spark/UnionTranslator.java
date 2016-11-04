package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.operator.Union;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;


class UnionTranslator implements SparkOperatorTranslator<Union> {

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(Union operator,
                              SparkExecutorContext context) {

    List<JavaRDD<?>> inputs = context.getInputs(operator);
    if (inputs.size() != 2) {
      throw new IllegalStateException("Union operator needs 2 inputs");
    }
    JavaRDD<?> left = inputs.get(0);
    JavaRDD<?> right = inputs.get(1);

    return left.union((JavaRDD) right);
  }
}
