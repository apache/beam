
package cz.seznam.euphoria.core.client.functional;

import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;

/**
 * Factory for states.
 */
public interface StateFactory<T, STATE>
    extends BinaryFunction<Context<T>, StorageProvider, STATE> {

}
