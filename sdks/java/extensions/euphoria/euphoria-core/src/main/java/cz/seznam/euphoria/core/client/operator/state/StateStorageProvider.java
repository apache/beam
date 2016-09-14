
package cz.seznam.euphoria.core.client.operator.state;

/**
 * Provider for state storage.
 * State can use this interface to create new storages. Single state can have
 * multiple associated storages.
 */
public interface StateStorageProvider {

  /** Retrieve new instance of state storage for values of given type. */
  <T> ValueStateStorage<T> getValueStorageFor(Class<T> what);

  /** Retrieve new instance of state storage for lists of values of given type. */
  <T> ListStateStorage<T> getListStorageFor(Class<T> what);

}
