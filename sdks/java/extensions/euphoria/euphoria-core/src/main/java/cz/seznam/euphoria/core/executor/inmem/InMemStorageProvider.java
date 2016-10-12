
package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import java.util.ArrayList;
import java.util.List;

/**
 * Provider of state storage for inmem executor.
 */
public class InMemStorageProvider implements StorageProvider {

  private static class InMemValueStateStorage<T> implements ValueStorage<T> {

    private final T defVal;
    T value;

    InMemValueStateStorage(T defVal) {
      this.defVal = defVal;
      this.value = defVal;
    }

    @Override
    public void set(T value) {
      this.value = value;
    }

    @Override
    public T get() {
      return value;
    }

    @Override
    public void clear() {
      this.value = defVal;
    }
  }

  private static class InMemListStateStorage<T> implements ListStorage<T> {

    List<T> values = new ArrayList<>();

    @Override
    public void add(T element) {
      values.add(element);
    }

    @Override
    public Iterable<T> get() {
      return values;
    }

    @Override
    public void clear() {
      values.clear();
    }
    
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    return new InMemValueStateStorage(descriptor.getDefaultValue());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    return new InMemListStateStorage();
  }


}
