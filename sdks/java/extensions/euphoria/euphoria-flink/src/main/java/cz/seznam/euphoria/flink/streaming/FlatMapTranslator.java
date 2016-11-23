package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.flink.FlinkOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

class FlatMapTranslator implements StreamingOperatorTranslator<FlatMap> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<FlatMap> operator,
                                 StreamingExecutorContext context)
  {
    DataStream<?> input = context.getSingleInputStream(operator);
    UnaryFunctor mapper = operator.getOriginalOperator().getFunctor();
    return input
        .flatMap(new StreamingUnaryFunctorWrapper<>(mapper))
        .returns((Class) StreamingWindowedElement.class)
        .name(operator.getName())
        .setParallelism(operator.getParallelism());
  }
}
