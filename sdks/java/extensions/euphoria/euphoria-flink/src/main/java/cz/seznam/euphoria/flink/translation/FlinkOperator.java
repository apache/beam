package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.operator.Operator;

import java.util.Collection;

/**
 * Decorated representation of {@link Operator} by Flink specific information
 * that is needed for Euphoria to Flink translation.
 */
class FlinkOperator<OP extends Operator> extends Operator {

  private final Operator<?, ?> wrapped;
  private int parallelism;

  public FlinkOperator(OP wrapped) {
    super(wrapped.getName(), wrapped.getFlow());
    this.wrapped = wrapped;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Dataset<?>> listInputs() {
    return (Collection) wrapped.listInputs();
  }

  @Override
  public Dataset<?> output() {
    return wrapped.output();
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    return wrapped.getBasicOps();
  }

  @SuppressWarnings("unchecked")
  public OP getOriginalOperator() {
    return (OP) wrapped;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }
}
