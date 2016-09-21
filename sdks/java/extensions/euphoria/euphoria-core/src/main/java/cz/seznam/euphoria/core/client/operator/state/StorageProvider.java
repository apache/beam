
package cz.seznam.euphoria.core.client.operator.state;

import java.io.Serializable;

/**
 * A provider of storage instances.
 */
public interface StorageProvider extends Serializable {

  /**
   * Retrieve new instance of state storage for values of given type.
   * @param descriptor descriptor of the storage within scope of given key and window/operator
   */
  <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor);

  /**
   * Retrieve new instance of state storage for lists of values of given type.
   * @param descriptor descriptor of the storage
   */
  <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor);


}
