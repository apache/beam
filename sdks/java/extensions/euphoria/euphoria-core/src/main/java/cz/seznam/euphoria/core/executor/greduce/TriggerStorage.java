package cz.seznam.euphoria.core.executor.greduce;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.Storage;
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptorBase;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;

import java.util.HashMap;
import java.util.Objects;

/**
 * Maintains triggers storages in memory for referencing
 * through value descriptors in the scope of a window.
 */
class TriggerStorage {

  static class Key {
    private final Window window;
    private final String storeId;

    Key(Window window, StorageDescriptorBase desc) {
      this.window = window;
      this.storeId = desc.getName();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Key) {
        Key that = (Key) o;
        return Objects.equals(window, that.window)
            && Objects.equals(storeId, that.storeId);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(window, storeId);
    }

    @Override
    public String toString() {
      return "Key{" +
          "window=" + window +
          ", storeId='" + storeId + '\'' +
          '}';
    }
  }

  class ClearingValueStorage<T> implements ValueStorage<T> {
    private final ValueStorage<T> wrap;
    private final Key key;

    ClearingValueStorage(ValueStorage<T> wrap, Key key) {
      this.wrap = wrap;
      this.key = key;
    }

    @Override
    public void clear() {
      wrap.clear();
      store.remove(key);
    }

    @Override
    public void set(T value) {
      wrap.set(value);
    }

    @Override
    public T get() {
      return wrap.get();
    }
  }

  class ClearingListStorage<T> implements ListStorage<T> {
    private final ListStorage<T> wrap;
    private final Key key;

    ClearingListStorage(ListStorage<T> wrap, Key key) {
      this.wrap = wrap;
      this.key = key;
    }

    @Override
    public void clear() {
      wrap.clear();
      store.remove(key);
    }

    @Override
    public void add(T element) {
      wrap.add(element);
    }

    @Override
    public Iterable<T> get() {
      return wrap.get();
    }
  }

  private final StorageProvider storageProvider;
  private final HashMap<Key, Storage> store = new HashMap<>();

  TriggerStorage(StorageProvider storageProvider) {
    this.storageProvider = Objects.requireNonNull(storageProvider);
  }

  Storage<?> getStorage(Window window, StorageDescriptorBase desc) {
    return store.get(new Key(window, desc));
  }

  <T> ValueStorage<T> getValueStorage(Window window, ValueStorageDescriptor<T> desc) {
    Key key = new Key(window, desc);
    Storage s = store.get(key);
    if (s == null) {
      store.put(key, s = storageProvider.getValueStorage(desc));
    }
    return new ClearingValueStorage<>((ValueStorage<T>) s, key);
  }

  <T> ListStorage<T> getListStorage(Window window, ListStorageDescriptor<T> desc) {
    Key key = new Key(window, desc);
    Storage s = store.get(key);
    if (s == null) {
      store.put(key, s = storageProvider.getListStorage(desc));
    }
    return new ClearingListStorage<>((ListStorage<T>) s, key);
  }

  int size() {
    return store.size();
  }
}
