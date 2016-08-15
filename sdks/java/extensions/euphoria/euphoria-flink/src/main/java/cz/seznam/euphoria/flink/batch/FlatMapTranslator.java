package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.functions.UnaryFunctorWrapper;
import org.apache.flink.api.java.DataSet;

class FlatMapTranslator implements BatchOperatorTranslator<FlatMap> {

  @Override
  @SuppressWarnings("unchecked")
  public DataSet<?> translate(FlinkOperator<FlatMap> operator,
                                 BatchExecutorContext context)
  {
    DataSet<?> input = context.getSingleInputStream(operator);
    UnaryFunctor mapper = operator.getOriginalOperator().getFunctor();
    return input
        .flatMap(new UnaryFunctorWrapper(mapper))
        .setParallelism(operator.getParallelism())
        .name(operator.getName());
  }
}
