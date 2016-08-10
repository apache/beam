package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.operator.Operator;

import java.util.Collection;

/**
 * Decorated representation of {@link Operator} by Flink specific information
 * that is needed for Euphoria to Flink translation.
 */
public class FlinkOperator<OP extends Operator> extends Operator {

  private final OP wrapped;
  private int parallelism;

  FlinkOperator(OP wrapped) {
    super(wrapped.getName(), wrapped.getFlow());
    this.wrapped = wrapped;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Dataset<?>> listInputs() {
    return  wrapped.listInputs();
  }

  @Override
  public Dataset<?> output() {
    return wrapped.output();
  }

  @Override
  @SuppressWarnings("unchecked")
  public DAG<Operator<?, ?>> getBasicOps() {
    return wrapped.getBasicOps();
  }

  public OP getOriginalOperator() {
    return wrapped;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }
}
