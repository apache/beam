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
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.executor.storage.FsSpillingListStorage;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.Serializable;

/**
 * Storage provider for batch processing.
 * We will store the data in memory, for some amount of data and then
 * flush in on disk.
 * We don't need to worry about checkpointing here, because the
 * storage is used in batch only and can therefore be reconstructed by
 * recalculation of the data.
 */
class BatchStateStorageProvider implements StorageProvider, Serializable {

  static class MemValueStorage<T> implements ValueStorage<T> {

      T value;

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

  final int MAX_ELEMENTS_IN_MEMORY;
  final FlinkSerializerFactory serializerFactory;

  BatchStateStorageProvider(int maxInMemElements, ExecutionEnvironment env) {
    this.MAX_ELEMENTS_IN_MEMORY = maxInMemElements;
    serializerFactory = new FlinkSerializerFactory(env);
  }

  @Override
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    // this is purely in memory
    return new MemValueStorage<>(descriptor.getDefaultValue());
  }

  @Override
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    return new FsSpillingListStorage<>(serializerFactory, MAX_ELEMENTS_IN_MEMORY);
  }
}
