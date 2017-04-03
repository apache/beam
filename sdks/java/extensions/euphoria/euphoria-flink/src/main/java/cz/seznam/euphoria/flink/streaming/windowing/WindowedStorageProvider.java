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
package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.flink.storage.Descriptors;
import cz.seznam.euphoria.flink.storage.FlinkListStorage;
import cz.seznam.euphoria.flink.storage.FlinkReducingValueStorage;
import cz.seznam.euphoria.flink.storage.FlinkValueStorage;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;

import java.util.IdentityHashMap;

/**
 * Storage provider using flink's state API. All states are namespaced by window.
 */
class WindowedStorageProvider<WID extends Window> implements StorageProvider {

  private final KeyedStateBackend keyedStateBackend;
  private final TypeSerializer<WID> windowSerializer;
  private Window window;

  private final IdentityHashMap<StorageDescriptor, StateDescriptor>
          storageToStateDescriptors = new IdentityHashMap<>();
  private final int descriptorsCacheMaxSize;

  /*
   * @descriptorsCacheMaxSize:
   *
   * The maximum size of the storage-to-state descriptor "cache". If we
   * exceed this value we are either running a program with this many or
   * more logical states or - rather more likely - the user's operators
   * are unnecessarily re-creating descriptor objects again and again,
   * which is something they want to avoid.
   *
   * TODO we should avoid the need for this cache in future by having user
   * code asking for the storage through an already "resolved descriptor"
   * such that the translation is not necessary at all when processing elements.
   */
  WindowedStorageProvider(KeyedStateBackend keyedStateBackend,
                          TypeSerializer<WID> windowSerializer,
                          int descriptorsCacheMaxSize) {
    this.keyedStateBackend = keyedStateBackend;
    this.windowSerializer = windowSerializer;
    this.descriptorsCacheMaxSize = descriptorsCacheMaxSize;
  }

  public void setWindow(Window window) {
    this.window = window;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    try {
      if (descriptor instanceof ValueStorageDescriptor.MergingValueStorageDescriptor) {
        ReducingStateDescriptor<T> flinkDescriptor =
                (ReducingStateDescriptor) storageToStateDescriptors.computeIfAbsent(
                        descriptor, k -> Descriptors.from((ValueStorageDescriptor.MergingValueStorageDescriptor<T>) k));
        validateStateDescriptorSize();

        return new FlinkReducingValueStorage<>((ReducingState)
                keyedStateBackend.getPartitionedState(window, windowSerializer, flinkDescriptor),
                descriptor.getDefaultValue(),
                window);
      } else {
        ValueStateDescriptor<T> flinkDescriptor =
                (ValueStateDescriptor<T>) storageToStateDescriptors.computeIfAbsent(
                        descriptor, k -> Descriptors.from((ValueStorageDescriptor<T>) k));
        validateStateDescriptorSize();

        return new FlinkValueStorage<>((ValueState)
                keyedStateBackend.getPartitionedState(window, windowSerializer, flinkDescriptor),
                window);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    try {
      ListStateDescriptor<T> flinkDescriptor =
              (ListStateDescriptor<T>) storageToStateDescriptors.computeIfAbsent(
                      descriptor, k -> Descriptors.from((ListStorageDescriptor<T>) k));
      validateStateDescriptorSize();

      return new FlinkListStorage<>((ListState)
              keyedStateBackend.getPartitionedState(window, windowSerializer, flinkDescriptor),
              window);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void validateStateDescriptorSize() {
    if (storageToStateDescriptors.size() > descriptorsCacheMaxSize) {
      throw new IllegalStateException(
          "Too many state descriptors! Likely some of the storage descriptors" +
          " are not declared as 'static final' and are generated for each element!");
    }
  }
}
