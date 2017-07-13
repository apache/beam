/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.jstorm.translation.runtime.state;

import com.alibaba.jstorm.cache.ComposedKey;
import com.alibaba.jstorm.cache.IKvStore;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.sdk.state.ValueState;

/**
 * JStorm implementation of {@link ValueState}.
 */
public class JStormValueState<K, T> implements ValueState<T> {

  @Nullable
  private final K key;
  private final StateNamespace namespace;
  private final IKvStore<ComposedKey, T> kvState;

  JStormValueState(@Nullable K key, StateNamespace namespace, IKvStore<ComposedKey, T> kvState) {
    this.key = key;
    this.namespace = namespace;
    this.kvState = kvState;
  }

  @Override
  public void write(T t) {
    try {
      kvState.put(getComposedKey(), t);
    } catch (IOException e) {
      throw new RuntimeException(String.format(
          "Failed to write key: %s, namespace: %s, value: %s.", key, namespace, t));
    }
  }

  @Override
  public T read() {
    try {
      return kvState.get(getComposedKey());
    } catch (IOException e) {
      throw new RuntimeException(String.format(
          "Failed to read key: %s, namespace: %s.", key, namespace));
    }
  }

  @Override
  public ValueState<T> readLater() {
    // TODO: support prefetch.
    return this;
  }

  @Override
  public void clear() {
    try {
      kvState.remove(getComposedKey());
    } catch (IOException e) {
      throw new RuntimeException(String.format(
          "Failed to clear key: %s, namespace: %s.", key, namespace));
    }
  }

  private ComposedKey getComposedKey() {
    return ComposedKey.of(key, namespace);
  }
}
