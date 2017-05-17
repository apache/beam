/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.executor.storage.FsSpillingListStorage;
import org.apache.spark.serializer.Serializer;

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

  private final SparkSerializerFactory sf;
  private final int listStorageMaxElemsInMemory;

  SparkStorageProvider(Serializer serializer, int listStorageMaxElemsInMemory) {
    this.sf = new SparkSerializerFactory(serializer);
    this.listStorageMaxElemsInMemory = listStorageMaxElemsInMemory;
  }

  @Override
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    return new MemValueStorage<>(descriptor.getDefaultValue());
  }

  @Override
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
      return new FsSpillingListStorage<>(sf, listStorageMaxElemsInMemory);
  }
}
