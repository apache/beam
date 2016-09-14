
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.util.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Validate invariants. Throw exceptions if any invariant is violated.
 */
class FlowValidator {

  /**
   * Validate the {@code DAG}.
   * @returns the input dag if the validation succeeds
   */
  public static DAG<Operator<?, ?>> validate(DAG<Operator<?, ?>> dag) {
    checkSinks(dag);
    return dag;
  }

  /**
   * Validate that no two output datasets use the same sink.
   * This is not supported, because we cannot clone the sink
   * (it can be used in client code after the flow has completed).
   */
  @SuppressWarnings("unchecked")
  private static void checkSinks(DAG<Operator<?, ?>> dag) {
    List<Pair<Dataset, DataSink>> outputs = dag.nodes()
        .filter(n -> n.output().getOutputSink() != null)
        .map(o -> Pair.of((Dataset) o.output(), (DataSink) o.output().getOutputSink()))
        .collect(Collectors.toList());
    
    Map<DataSink, Dataset> sinkDatasets = new HashMap<>();

    outputs.stream()
        .forEach(p -> {
          Dataset current = sinkDatasets.get(p.getSecond());
          if (current != null) {
            throw new IllegalArgumentException(
                "Operator " + current.getProducer().getName() + " and "
                + " operator " + p.getFirst().getProducer().getName()
                + " use the same sink " + p.getSecond());
          }
          sinkDatasets.put(p.getSecond(), p.getFirst());
        });
  }

}
