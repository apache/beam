
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import java.util.List;

/**
 * Operator with multiple outputs.
 */
public interface MultipleOutputs<T> {

  /** Retrieve the outputs of this operator, with no explicit windowing. */
  List<Dataset<T>> listOutputs();

}
