
package cz.seznam.euphoria.core.client.operator.state;

import java.io.Serializable;

/**
 * A provider of storage instances.
 */
public interface StorageProvider extends Serializable {

  /**
   * Retrieve new instance of state storage for values of given type.
   * @param name the name of the storage (unique)
   * @param what the class to be stored in the storage
   */
  <T> ValueStateStorage<T> getValueStorage(String name, Class<T> what);

  /**
   * Retrieve new instance of state storage for lists of values of given type.
   * @param name the name of the storage (unique)
   * @param what the class to be stored in the storage
   */
  <T> ListStateStorage<T> getListStorage(String name, Class<T> what);


}
