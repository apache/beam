
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.flink.FlinkOperator;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.java.DataSet;

/**
 * Translator of {@code Union} operator.
 */
class UnionTranslator implements BatchOperatorTranslator<Union> {

  @Override
  @SuppressWarnings("unchecked")
  public DataSet translate(
      FlinkOperator<Union> operator,
      BatchExecutorContext context) {

    List<DataSet> inputs = (List) context.getInputStreams(operator);
    if (inputs.size() != 2) {
      throw new IllegalStateException("Should have two datasets on input, got "
          + inputs.size());
    }
    Optional<DataSet> reduce = inputs.stream().reduce((l, r) -> l.union(r));
    return reduce.get();
  }


}
