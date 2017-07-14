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
package org.apache.beam.runners.jstorm.translation;

import com.alibaba.jstorm.cache.IKvStore;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link MapState} in JStorm runner.
 * @param <K>
 * @param <V>
 */
class JStormMapState<K, V> implements MapState<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(JStormMapState.class);

  private final K key;
  private final StateNamespace namespace;
  private IKvStore<K, V> kvStore;

  public JStormMapState(K key, StateNamespace namespace, IKvStore<K, V> kvStore) {
    this.key = key;
    this.namespace = namespace;
    this.kvStore = kvStore;
  }

  @Override
  public void put(K var1, V var2) {
    try {
      kvStore.put(var1, var2);
    } catch (IOException e) {
      reportError(String.format("Failed to put key=%s, value=%s", var1, var2), e);
    }
  }

  @Override
  public ReadableState<V> putIfAbsent(K var1, V var2) {
    ReadableState<V> ret = null;
    try {
      V value = kvStore.get(var1);
      if (value == null) {
        kvStore.put(var1, var2);
        ret = new MapReadableState<>(null);
      } else {
        ret = new MapReadableState<>(value);
      }
    } catch (IOException e) {
      reportError(String.format("Failed to putIfAbsent key=%s, value=%s", var1, var2), e);
    }
    return ret;
  }

  @Override
  public void remove(K var1) {
    try {
      kvStore.remove(var1);
    } catch (IOException e) {
      reportError(String.format("Failed to remove key=%s", var1), e);
    }
  }

  @Override
  public ReadableState<V> get(K var1) {
    ReadableState<V> ret = new MapReadableState<>(null);
    try {
      ret = new MapReadableState(kvStore.get(var1));
    } catch (IOException e) {
      reportError(String.format("Failed to get value for key=%s", var1), e);
    }
    return ret;
  }

  @Override
  public ReadableState<Iterable<K>> keys() {
    ReadableState<Iterable<K>> ret = new MapReadableState<>(null);
    try {
      ret = new MapReadableState<>(kvStore.keys());
    } catch (IOException e) {
      reportError(String.format("Failed to get keys"), e);
    }
    return ret;
  }

  @Override
  public ReadableState<Iterable<V>> values() {
    ReadableState<Iterable<V>> ret = new MapReadableState<>(null);
    try {
      ret = new MapReadableState<>(kvStore.values());
    } catch (IOException e) {
      reportError(String.format("Failed to get values"), e);
    }
    return ret;
  }

  @Override
  public ReadableState<Iterable<Map.Entry<K, V>>> entries() {
    ReadableState<Iterable<Map.Entry<K, V>>> ret = new MapReadableState<>(null);
    try {
      ret = new MapReadableState<>(kvStore.entries());
    } catch (IOException e) {
      reportError(String.format("Failed to get values"), e);
    }
    return ret;
  }

  @Override
  public void clear() {
    try {
      Iterable<K> keys = kvStore.keys();
      kvStore.removeBatch(keys);
    } catch (IOException e) {
      reportError(String.format("Failed to clear map state"), e);
    }
  }

  private void reportError(String errorInfo, IOException e) {
    LOG.error(errorInfo, e);
    throw new RuntimeException(errorInfo);
  }

  private class MapReadableState<T> implements ReadableState<T> {
    private T value;

    public MapReadableState(T value) {
      this.value = value;
    }

    @Override
    public T read() {
      return value;
    }

    @Override
    public ReadableState<T> readLater() {
      return this;
    }
  }
}
