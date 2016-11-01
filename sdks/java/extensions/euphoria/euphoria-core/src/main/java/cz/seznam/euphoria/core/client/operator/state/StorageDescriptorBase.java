
package cz.seznam.euphoria.core.client.operator.state;

import java.io.Serializable;

/**
 * Descriptor of storage.
 */
// FIXME rename to a mere 'StorageDescriptor'
public abstract class StorageDescriptorBase implements Serializable {

  final String name;

  protected StorageDescriptorBase(String name) {
    this.name = name;
  }

  /** Retrieve name of the storage with scope of operator and key. */
  public String getName() { return name; }

}
