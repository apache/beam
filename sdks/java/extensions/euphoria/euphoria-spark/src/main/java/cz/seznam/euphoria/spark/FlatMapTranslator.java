package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import org.apache.spark.api.java.JavaRDD;


class FlatMapTranslator implements SparkOperatorTranslator<FlatMap> {

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(FlatMap operator,
                              SparkExecutorContext context) {

    final JavaRDD<?> input = context.getSingleInput(operator);
    final UnaryFunctor<?, ?> mapper = operator.getFunctor();

    return input.flatMap(new UnaryFunctorWrapper<>((UnaryFunctor) mapper));
  }
}
