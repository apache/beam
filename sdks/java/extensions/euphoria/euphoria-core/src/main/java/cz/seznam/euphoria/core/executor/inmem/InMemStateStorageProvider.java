
package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.operator.state.ListStateStorage;
import cz.seznam.euphoria.core.client.operator.state.StateStorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStateStorage;
import java.util.ArrayList;
import java.util.List;

/**
 * Provider of state storage for inmem executor.
 */
public class InMemStateStorageProvider implements StateStorageProvider {

  private static class InMemValueStateStorage<T> implements ValueStateStorage<T> {

    T value;

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
      this.value = null;
    }
  }

  private static class InMemListStateStorage<T> implements ListStateStorage<T> {

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
  public <T> ValueStateStorage<T> getValueStorageFor(Class<T> what) {
    return new InMemValueStateStorage();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ListStateStorage<T> getListStorageFor(Class<T> what) {
    return new InMemListStateStorage();
  }


}
