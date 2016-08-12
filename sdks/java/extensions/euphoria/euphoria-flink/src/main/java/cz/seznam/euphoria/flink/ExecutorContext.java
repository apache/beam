package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.operator.SingleInputOperator;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps track of mapping between Euphoria {@link Dataset} and
 * Flink output {@link DataStream} or {@link org.apache.flink.api.java.DataSet}.
 *
 * @param <D> the type of the datasets handled in this context,
 *            either {@code Dataset} (for batch mode) or {@code DataStream} (for
 *            stream mode.)
 */
public abstract class ExecutorContext<E, D> {

  private final E env;
  private final DAG<FlinkOperator<?>> dag;
  private final Map<FlinkOperator<?>, D> outputs;

  public ExecutorContext(E env, DAG<FlinkOperator<?>> dag) {
    this.env = env;
    this.dag = dag;
    this.outputs = new IdentityHashMap<>();
  }

  public E getExecutionEnvironment() {
    return this.env;
  }

  /**
   * Retrieve list of Flink {@link DataStream} inputs of given operator
   */
  public List<D> getInputStreams(FlinkOperator<?> operator) {
    List<Node<FlinkOperator<?>>> parents = dag.getNode(operator).getParents();
    List<D> inputs = new ArrayList<>(parents.size());
    for (Node<FlinkOperator<?>> p : parents) {
      D pout = outputs.get(dag.getNode(p.get()).get());
      if (pout == null) {
        throw new IllegalArgumentException(
                "Output DataStream/DataSet missing for operator " + p.get().getName());
      }
      inputs.add(pout);
    }
    return inputs;
  }

  /** Assumes the specified operator is a single-input-operator. */
  public D getSingleInputStream(FlinkOperator<? extends SingleInputOperator> operator) {
    return Iterables.getOnlyElement(getInputStreams(operator));
  }

  public D getOutputStream(FlinkOperator<?> operator) {
    D out = outputs.get(operator);
    if (out == null) {
      throw new IllegalArgumentException("No output exists for operator " +
              operator.getName());
    }
    return out;
  }

  public void setOutput(FlinkOperator<?> operator, D output) {
    D prev = outputs.put(operator, output);
    if (prev != null) {
      throw new IllegalStateException(
              "Operator(" + operator.getName() + ") output already processed");
    }
  }
}
