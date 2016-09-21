
package cz.seznam.euphoria.core.client.functional;

import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;

/**
 * Factory for states.
 */
public interface StateFactory<T, STATE> extends BinaryFunction<
    Collector<T>, StorageProvider, STATE> {

}
