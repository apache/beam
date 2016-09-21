
package cz.seznam.euphoria.core.client.operator.state;

import cz.seznam.euphoria.core.client.functional.BinaryFunction;

/**
 * Optional interface for descriptor to implement in order to enable state merging.
 */
public interface MergingStorageDescriptor<T> {

  /**
   * Retrieve the merging function for state storages.
   */
  BinaryFunction<? extends Storage<T>, ? extends Storage<T>, Void> getMerger();

}
