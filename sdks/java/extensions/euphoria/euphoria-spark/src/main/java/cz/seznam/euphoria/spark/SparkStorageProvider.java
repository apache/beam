package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Storage provider for batch processing. Date are stored in memory.
 * We don't need to worry about checkpointing here, because the
 * storage is used in batch only and can therefore be reconstructed by
 * recalculation of the data.
 */
class SparkStorageProvider implements StorageProvider, Serializable {

  static class MemValueStorage<T> implements ValueStorage<T> {

    private T value;

    MemValueStorage(T defVal) {
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
      value = null;
    }
  }

  static class MemListStorage<T> implements ListStorage<T> {

    private final List<T> data = new ArrayList<>();

    @Override
    public void add(T element) {
      data.add(element);
    }

    @Override
    public Iterable<T> get() {
      return data;
    }

    @Override
    public final void clear() {
      data.clear();
    }
  }

  @Override
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    return new MemValueStorage<>(descriptor.getDefaultValue());
  }

  @Override
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
      return new MemListStorage<>();
  }
}
