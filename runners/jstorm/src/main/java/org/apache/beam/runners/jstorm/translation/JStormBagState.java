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

import static com.google.common.base.Preconditions.checkNotNull;

import com.alibaba.jstorm.cache.ComposedKey;
import com.alibaba.jstorm.cache.IKvStore;
import com.alibaba.jstorm.cache.KvStoreIterable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ReadableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link BagState} in JStorm runner.
 */
class JStormBagState<K, T> implements BagState<T> {
  private static final Logger LOG = LoggerFactory.getLogger(JStormBagState.class);

  @Nullable
  private final K key;
  private final StateNamespace namespace;
  private final IKvStore<ComposedKey, T> kvState;
  private final IKvStore<ComposedKey, Object> stateInfoKvState;
  private int elemIndex;

  public JStormBagState(@Nullable K key, StateNamespace namespace, IKvStore<ComposedKey, T> kvState,
                        IKvStore<ComposedKey, Object> stateInfoKvState) throws IOException {
    this.key = key;
    this.namespace = checkNotNull(namespace, "namespace");
    this.kvState = checkNotNull(kvState, "kvState");
    this.stateInfoKvState = checkNotNull(stateInfoKvState, "stateInfoKvState");

    Integer index = (Integer) stateInfoKvState.get(getComposedKey());
    this.elemIndex = index != null ? ++index : 0;
  }

  @Override
  public void add(T input) {
    try {
      kvState.put(getComposedKey(elemIndex), input);
      stateInfoKvState.put(getComposedKey(), elemIndex);
      elemIndex++;
    } catch (IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public ReadableState<Boolean> isEmpty() {
    return new ReadableState<Boolean>() {
      @Override
      public Boolean read() {
        return elemIndex <= 0;
      }

      @Override
      public ReadableState<Boolean> readLater() {
        // TODO: support prefetch.
        return this;
      }
    };
  }

  @Override
  public Iterable<T> read() {
    return new BagStateIterable(elemIndex);
  }

  @Override
  public BagState readLater() {
    // TODO: support prefetch.
    return this;
  }

  @Override
  public void clear() {
    try {
      for (int i = 0; i < elemIndex; i++) {
        kvState.remove(getComposedKey(i));
      }
      stateInfoKvState.remove(getComposedKey());
      elemIndex = 0;
    } catch (IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  private ComposedKey getComposedKey() {
    return ComposedKey.of(key, namespace);
  }

  private ComposedKey getComposedKey(int elemIndex) {
    return ComposedKey.of(key, namespace, elemIndex);
  }

  /**
   * Implementation of Bag state Iterable.
   */
  private class BagStateIterable implements KvStoreIterable<T> {

    private class BagStateIterator implements Iterator<T> {
      private final int size;
      private int cursor = 0;

      BagStateIterator() {
        Integer s = null;
        try {
          s = (Integer) stateInfoKvState.get(getComposedKey());
        } catch (IOException e) {
          LOG.error("Failed to get elemIndex for key={}", getComposedKey());
        }
        this.size = s != null ? ++s : 0;
      }

      @Override
      public boolean hasNext() {
        return cursor < size;
      }

      @Override
      public T next() {
        if (cursor >= size) {
          throw new NoSuchElementException();
        }

        T value = null;
        try {
          value = kvState.get(getComposedKey(cursor));
        } catch (IOException e) {
          LOG.error("Failed to read composed key-[{}]", getComposedKey(cursor));
        }
        cursor++;
        return value;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    private final int size;

    BagStateIterable(int size) {
      this.size = size;
    }

    @Override
    public Iterator<T> iterator() {
      return new BagStateIterator();
    }

    @Override
    public String toString() {
      return String.format("BagStateIterable: composedKey=%s", getComposedKey());
    }
  }
}
