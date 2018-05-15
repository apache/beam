/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.core.executor.greduce;

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.Storage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StorageProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorageDescriptor;

import java.util.HashMap;
import java.util.Objects;

/**
 * Maintains triggers storages in memory for referencing through value descriptors in the scope of a
 * window.
 */
class TriggerStorage {

  private final StorageProvider storageProvider;
  private final HashMap<Key, Storage> store = new HashMap<>();

  TriggerStorage(StorageProvider storageProvider) {
    this.storageProvider = Objects.requireNonNull(storageProvider);
  }

  Storage<?> getStorage(Window window, StorageDescriptor desc) {
    return store.get(new Key(window, desc));
  }

  <T> ValueStorage<T> getValueStorage(Window window, ValueStorageDescriptor<T> desc) {
    Key key = new Key(window, desc);
    @SuppressWarnings("unchecked")
    Storage<T> s = store.get(key);
    if (s == null) {
      store.put(key, s = storageProvider.getValueStorage(desc));
    }
    return new ClearingValueStorage<>((ValueStorage<T>) s, key);
  }

  <T> ListStorage<T> getListStorage(Window window, ListStorageDescriptor<T> desc) {
    Key key = new Key(window, desc);
    @SuppressWarnings("unchecked")
    Storage<T> s = store.get(key);
    if (s == null) {
      store.put(key, s = storageProvider.getListStorage(desc));
    }
    return new ClearingListStorage<>((ListStorage<T>) s, key);
  }

  int size() {
    return store.size();
  }

  static class Key {
    private final Window window;
    private final String storeId;

    Key(Window window, StorageDescriptor desc) {
      this.window = window;
      this.storeId = desc.getName();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Key) {
        Key that = (Key) o;
        return Objects.equals(window, that.window) && Objects.equals(storeId, that.storeId);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(window, storeId);
    }

    @Override
    public String toString() {
      return "Key{" + "window=" + window + ", storeId='" + storeId + '\'' + '}';
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
}
