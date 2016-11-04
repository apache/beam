package cz.seznam.euphoria.spark;

import com.google.common.collect.Iterables;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.operator.Operator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps track of mapping between Euphoria {@link Operator} and
 * Spark output {@link JavaRDD}.
 */
public class SparkExecutorContext {

  private final JavaSparkContext env;
  private final DAG<Operator<?, ?>> dag;
  private final Map<Operator<?, ?>, JavaRDD<?>> outputs;

  public SparkExecutorContext(JavaSparkContext env, DAG<Operator<?, ?>> dag) {
    this.env = env;
    this.dag = dag;
    this.outputs = new IdentityHashMap<>();
  }

  public JavaSparkContext getExecutionEnvironment() {
    return this.env;
  }

  /**
   * Retrieve list of Spark {@link JavaRDD} inputs of given operator
   */
  public List<JavaRDD<?>> getInputs(Operator<?, ?> operator) {
    List<Node<Operator<?, ?>>> parents = dag.getNode(operator).getParents();
    List<JavaRDD<?>> inputs = new ArrayList<>(parents.size());
    for (Node<Operator<?, ?>> p : parents) {
      JavaRDD pout = outputs.get(dag.getNode(p.get()).get());
      if (pout == null) {
        throw new IllegalArgumentException(
                "Output DataStream/DataSet missing for operator " + p.get().getName());
      }
      inputs.add(pout);
    }
    return inputs;
  }

  /**
   * Retrieves single Spark {@link JavaRDD} in case given operator has no more
   * than one input (single-input operator).
   */
  public JavaRDD<?> getSingleInput(Operator<?, ?> operator) {
    return Iterables.getOnlyElement(getInputs(operator));
  }

  /**
   * Retrieves Spark {@link JavaRDD} representing output of given operator
   */
  public JavaRDD<?> getOutput(Operator<?, ?> operator) {
    JavaRDD<?> out = outputs.get(operator);
    if (out == null) {
      throw new IllegalArgumentException("No output exists for operator " +
              operator.getName());
    }
    return out;
  }

  public void setOutput(Operator<?, ?> operator, JavaRDD<?> output) {
    JavaRDD<?> prev = outputs.put(operator, output);
    if (prev != null) {
      throw new IllegalStateException(
              "Operator(" + operator.getName() + ") output already processed");
    }
  }
}
