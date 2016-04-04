
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;

/**
 * Interface for operator. Private.
 */
public interface Output<T, TYPE extends Dataset<T>> {

  TYPE output();

}
