package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.flink.translation.functions.UnaryFunctorWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;

class FlatMapTranslator implements OperatorTranslator<FlatMap> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<FlatMap> operator,
                                 ExecutorContext context)
  {
    FlatMap origOperator = operator.getOriginalOperator();
    DataStream<?> input = context.getInputStream(origOperator);

    return input.flatMap(new UnaryFunctorWrapper(origOperator.getFunctor()))
            .setParallelism(operator.getParallelism())
            .name(operator.getName());
  }
}
