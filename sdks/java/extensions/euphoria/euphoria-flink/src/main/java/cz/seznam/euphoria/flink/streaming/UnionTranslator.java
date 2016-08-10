package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.flink.FlinkOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

class UnionTranslator implements StreamingOperatorTranslator<Union> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<Union> operator,
                                 StreamingExecutorContext context)
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
