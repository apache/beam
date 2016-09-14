
package cz.seznam.euphoria.core.client.functional;

import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.state.StateStorageProvider;

/**
 * Factory for states.
 */
public interface StateFactory<T, STATE>
    extends BinaryFunction<Collector<T>, StateStorageProvider, STATE> {

}
