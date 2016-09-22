
package cz.seznam.euphoria.core.client.operator.state;


/**
 * Descriptor of list storage.
 */
public class ListStorageDescriptor<T> extends StorageDescriptorBase {
  
  public static <T> ListStorageDescriptor<T> of(String name, Class<T> elementCls) {
    return new ListStorageDescriptor<>(name, elementCls);
  }

  private final Class<T> elementCls;

  private ListStorageDescriptor(String name, Class<T> elementCls) {
    super(name);
    this.elementCls = elementCls;
  }

  public Class<T> getElementClass() {
    return elementCls;
  }

}
