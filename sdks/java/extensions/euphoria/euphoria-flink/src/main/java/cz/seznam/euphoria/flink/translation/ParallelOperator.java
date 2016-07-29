package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.operator.Operator;

import java.util.Collection;

/**
 * Decorated representation of {@link Operator} by Flink specific information
 * that is needed for Euphoria to Flink translation.
 */
class ParallelOperator<IN, OUT> extends Operator<IN, OUT> {

  private final Operator<IN, OUT> wrapped;
  private int parallelism;

  public ParallelOperator(Operator<IN, OUT> wrapped) {
    super(wrapped.getName(), wrapped.getFlow());
    this.wrapped = wrapped;
  }

  @Override
  public Collection<Dataset<IN>> listInputs() {
    return wrapped.listInputs();
  }

  @Override
  public Dataset<OUT> output() {
    return wrapped.output();
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    return wrapped.getBasicOps();
  }

  public Operator<?, ?> getOriginalOperator() {
    return wrapped;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }
}
