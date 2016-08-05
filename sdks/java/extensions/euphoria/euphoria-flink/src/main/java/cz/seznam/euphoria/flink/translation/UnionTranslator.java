package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.operator.Union;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

class UnionTranslator implements OperatorTranslator<Union> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<Union> operator,
                                 ExecutorContext context)
  {
    List<DataStream<?>> inputs = context.getInputStreams(operator);
    if (inputs.size() != 2) {
      throw new IllegalStateException("Union operator needs 2 inputs");
    }
    DataStream<?> left = inputs.get(0);
    DataStream<?> right = inputs.get(1);

    return left.union((DataStream) right);
  }
}
