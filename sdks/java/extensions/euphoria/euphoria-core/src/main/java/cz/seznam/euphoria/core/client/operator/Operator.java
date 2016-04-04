
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.OutputPCollection;
import cz.seznam.euphoria.core.client.dataset.OutputPStream;
import cz.seznam.euphoria.core.client.dataset.PCollection;
import cz.seznam.euphoria.core.client.dataset.PStream;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;

import java.io.Serializable;
import java.util.Collection;

/**
 * An operator base class. All operators inherit his class.
 */
public abstract class Operator<IN, OUT, TYPE extends Dataset<OUT>>
    implements Output<OUT, TYPE>, Serializable {
  
  /** Name of the operator. */
  private final String name;
  /** Asssociated Flow. */
  private final Flow flow;

  protected Operator(String name, Flow flow) {
    this.name = name;
    this.flow = flow;
  }

  public final String getName() {
    return name;
  }

  public final Flow getFlow() {
    return flow;
  }

  /**
   * Retrieve basic operators that constitute this operator.
   * Override this method for all non basic operators.
   */
  @SuppressWarnings("unchecked")
  public DAG<Operator<?, ?, ?>> getBasicOps() {
    return DAG.of(this);
  }

  /** List all input datasets. */
  public abstract Collection<Dataset<IN>> listInputs();

  /**
   * Create a new dataset that will be output of this operator.
   * This is used when creating operator outputs.
   */
  @SuppressWarnings("unchecked")
  protected final TYPE createOutput(final Dataset<IN> input) {
    if (input.isBounded()) {
      return (TYPE) new OutputPCollection<OUT>(input.getFlow(), (Operator<?, OUT, PCollection<OUT>>) this) {

        @Override
        @SuppressWarnings("unchecked")
        public <X> Partitioning<X> getPartitioning()
        {
          if (Operator.this instanceof PartitioningAware) {
            // only state operators change the partitioning
            PartitioningAware<IN> state = (PartitioningAware<IN>) Operator.this;
            return (Partitioning<X>) state.getPartitioning();
          }
          return input.getPartitioning();
        }

      };
    }
    return (TYPE) new OutputPStream<OUT>(input.getFlow(), (Operator<?, OUT, PStream<OUT>>) this) {

      @Override
      @SuppressWarnings("unchecked")
      public <X> Partitioning<X> getPartitioning()
      {
        if (Operator.this instanceof PartitioningAware) {
          // only state operators change the partitioning
          PartitioningAware<IN> state = (PartitioningAware<IN>) Operator.this;
          return (Partitioning<X>) state.getPartitioning();
        }
        return input.getPartitioning();
      }

    };
  }

}
