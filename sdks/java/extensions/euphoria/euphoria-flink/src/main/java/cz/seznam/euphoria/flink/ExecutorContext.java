package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.SingleInputOperator;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps track of mapping between Euphoria {@link Dataset} and
 * Flink output {@link DataStream} or {@link org.apache.flink.api.java.DataSet}.
 */
public abstract class ExecutorContext<T> {

  private final DAG<FlinkOperator<?>> dag;
  private final Map<FlinkOperator<?>, T> outputs;

  public ExecutorContext(DAG<FlinkOperator<?>> dag) {
    this.dag = dag;
    this.outputs = new IdentityHashMap<>();
  }

  /**
   * Retrieve list of Flink {@link DataStream} inputs of given operator
   */
  public List<T> getInputStreams(FlinkOperator<?> operator) {
    List<Node<FlinkOperator<?>>> parents = dag.getNode(operator).getParents();
    List<T> inputs = new ArrayList<>(parents.size());
    for (Node<FlinkOperator<?>> p : parents) {
      T pout = outputs.get(dag.getNode(p.get()).get());
      if (pout == null) {
        throw new IllegalArgumentException(
                "Output DataStream/DataSet missing for operator " + p.get().getName());
      }
      inputs.add(pout);
    }
    return inputs;
  }

  /** Assumes the specified operator is a single-input-operator. */
  public T getSingleInputStream(FlinkOperator<? extends SingleInputOperator> operator) {
    return Iterables.getOnlyElement(getInputStreams(operator));
  }

  public T getOutputStream(FlinkOperator<?> operator) {
    T out = outputs.get(operator);
    if (out == null) {
      throw new IllegalArgumentException("No output exists for operator " +
              operator.getName());
    }
    return out;
  }

  public void setOutput(FlinkOperator<?> operator, T output) {
    T prev = outputs.put(operator, output);
    if (prev != null) {
      throw new IllegalStateException(
              "Operator(" + operator.getName() + ") output already processed");
    }
  }
}
