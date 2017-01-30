/**
 * Copyright 2016 Seznam.cz, a.s.
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
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.flink.storage.Descriptors;
import cz.seznam.euphoria.flink.storage.FlinkListStorage;
import cz.seznam.euphoria.flink.storage.FlinkValueStorage;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

/**
 * Storage provider using flink's state API.
 */
class FlinkStreamingStateStorageProvider implements StorageProvider, Serializable {

  private transient RuntimeContext context;

  void initialize(RuntimeContext context) {
    this.context = context;
  }

  @Override
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    return new FlinkValueStorage<>(context.getState(Descriptors.from(descriptor)));
  }

  @Override
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    return new FlinkListStorage<>(context.getListState(Descriptors.from(descriptor)));
  }
}
