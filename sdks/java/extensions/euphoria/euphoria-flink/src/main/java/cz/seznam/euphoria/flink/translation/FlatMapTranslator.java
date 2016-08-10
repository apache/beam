package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.flink.translation.functions.UnaryFunctorWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;

class FlatMapTranslator implements OperatorTranslator<FlatMap> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<FlatMap> operator,
                                 ExecutorContext context)
  {
    DataStream<?> input = context.getSingleInputStream(operator);
    UnaryFunctor mapper = operator.getOriginalOperator().getFunctor();
    return input
        .flatMap(new UnaryFunctorWrapper(mapper))
        .setParallelism(operator.getParallelism())
        .name(operator.getName());
  }
}
