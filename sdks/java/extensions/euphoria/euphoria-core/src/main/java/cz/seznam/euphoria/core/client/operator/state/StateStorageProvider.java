
package cz.seznam.euphoria.core.client.operator.state;

/**
 * Provider for state storage.
 * State can use this interface to create new storages. Single state can have
 * multiple associated storages.
 */
public interface StateStorageProvider extends StorageProvider {

  /**
   * Retrieve new instance of state storage for values of given type.
   * @param state the state that we create this storage for
   * @param name the name of the storage within scope of the given state (can be null)
   * @param what the class to be stored in the storage
   */
  default <T> ValueStateStorage<T> getValueStorage(State<?, ?> state, String name, Class<T> what) {
    return getValueStorage(name == null
        ? state.getAssociatedOperator().getName()
        : state.getAssociatedOperator().getName() + "::" + name,
        what);
  }

  /**
   * Retrieve new instance of state storage for values of given type.
   * Use this call if the state has only single associated storage.
   * @param state the state that we create this storage for
   * @param what the class to be stored in the storage
   */
  default <T> ValueStateStorage<T> getValueStorage(State<?, ?> state, Class<T> what) {
    return getValueStorage(state, null, what);
  }

  /**
   * Retrieve new instance of state storage for lists of values of given type.
   * @param state the state that we create this storage for
   * @param name the name of the storage within scope of the given state (can be null)
   * @param what the class to be stored in the storage
   */
  default <T> ListStateStorage<T> getListStorage(State<?, ?> state, String name, Class<T> what) {
    return getListStorage(name == null
        ? state.getAssociatedOperator().getName()
        : state.getAssociatedOperator().getName() + "::" + name,
        what);
  }

  /**
   * Retrieve new instance of state storage for lists of values of given type.
   * Use this call if the state has only single associated storage.
   * @param state the state that we create this storage for
   * @param name the name of the storage within scope of the given state (can be null)
   * @param what the class to be stored in the storage
   */
  default <T> ListStateStorage<T> getListStorage(State<?, ?> state, Class<T> what) {
    return getListStorage(state, null, what);
  }


}
