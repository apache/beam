package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps track of mapping between Euphoria {@link Dataset} and
 * Flink {@link DataStream}
 */
public class ExecutorContext {

  private final StreamExecutionEnvironment executionEnvironment;
  private final DAG<FlinkOperator<?>> dag;
  private final Map<FlinkOperator<?>, DataStream<?>> outputs;

  public ExecutorContext(
      StreamExecutionEnvironment executionEnvironment, DAG<FlinkOperator<?>> dag) {
    this.executionEnvironment = executionEnvironment;
    this.dag = dag;
    this.outputs = new IdentityHashMap<>();
  }

  /**
   * Retrieve list of Flink {@link DataStream} inputs of given operator
   */
  public List<DataStream<?>> getInputStreams(FlinkOperator<?> operator) {
    List<Node<FlinkOperator<?>>> parents = dag.getNode(operator).getParents();
    List<DataStream<?>> inputs = new ArrayList<>(parents.size());
    for (Node<FlinkOperator<?>> p : parents) {
      DataStream<?> pout = outputs.get(dag.getNode(p.get()).get());
      if (pout == null) {
        throw new IllegalArgumentException(
            "Matching DataStream missing for Dataset produced by " + p.get().getName());
      }
      inputs.add(pout);
    }
    return inputs;
  }

  /** Assumes the specified operator is a single-input-operator. */
  public DataStream<?> getSingleInputStream(FlinkOperator<?> operator) {
    return Iterables.getOnlyElement(getInputStreams(operator));
  }

  public DataStream<?> getOutputStream(FlinkOperator<?> operator) {
    DataStream<?> out = outputs.get(operator);
    if (out == null) {
      throw new IllegalArgumentException("Output stream doesn't exists for operator " +
              operator.getName());
    }
    return out;
  }

  public StreamExecutionEnvironment getExecutionEnvironment() {
    return executionEnvironment;
  }

  public void setOutputStream(FlinkOperator<?> operator, DataStream<?> output) {
    DataStream<?> prev = outputs.put(operator, output);
    if (prev != null) {
      throw new IllegalStateException(
              "Operator(" + operator.getName() + ") output already processed");
    }
  }
}
