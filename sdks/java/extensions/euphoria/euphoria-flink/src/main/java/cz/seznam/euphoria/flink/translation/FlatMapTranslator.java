package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.flink.translation.functions.UnaryFunctorWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

class FlatMapTranslator implements OperatorTranslator<FlatMap> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream translate(FlatMap operator, TranslationContext context) {
    DataStream<?> input = context.getInputStream(operator);

    return input.flatMap(new UnaryFunctorWrapper(operator.getFunctor()))
            .name(operator.getName());
  }
}
