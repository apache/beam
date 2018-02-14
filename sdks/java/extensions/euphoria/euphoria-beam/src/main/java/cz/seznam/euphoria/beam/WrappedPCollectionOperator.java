
package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Datasets;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link Operator} that serves as a wrapper between a {@link PCollection}
 * and {@link Dataset}.
 */
class WrappedPCollectionOperator<T> extends Operator<T, T> {

  final Dataset<T> output;
  final transient PCollection<T> input;

  WrappedPCollectionOperator(BeamFlow flow, PCollection<T> coll) {
    super("PCollectionWrapper", flow);
    this.input = coll;
    this.output = Datasets.createOutputFor(
        flow, coll.isBounded() == PCollection.IsBounded.BOUNDED, this);
  }

  @Override
  public Collection<Dataset<T>> listInputs() {
    return Collections.emptyList();
  }

  @Override
  public Dataset<T> output() {
    return output;
  }

  static PCollection<?> translate(
      Operator operator, BeamExecutorContext context) {
    return ((WrappedPCollectionOperator) operator).input;
  }

}
