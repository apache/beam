
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/**
 * Interface marking operator as state aware.
 * State aware operators are global operators that work on some type of key.
 */
public interface StateAware<IN, KEY> extends PartitioningAware<KEY> {

  /** Retrieve the extractor of key from given element. */
  UnaryFunction<IN, KEY> getKeyExtractor();


}
