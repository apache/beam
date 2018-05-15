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
package org.apache.beam.sdk.extensions.euphoria.executor.local;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StorageProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorageDescriptor;

/** Provider of state storage for local executor. */
public class LocalStorageProvider implements StorageProvider {

  @Override
  @SuppressWarnings("unchecked")
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    return new LocalValueStateStorage(descriptor.getDefaultValue());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    return new LocalListStateStorage();
  }

  private static class LocalValueStateStorage<T> implements ValueStorage<T> {

    private final T defVal;
    T value;

    LocalValueStateStorage(T defVal) {
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

  private static class LocalListStateStorage<T> implements ListStorage<T> {

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
}
