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
import com.alibaba.jstorm.cache.IKvStoreManager;
import com.alibaba.jstorm.cache.KvStoreIterable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JStorm implementation of {@link StateInternals}.
 */
class JStormStateInternals<K> implements StateInternals {

  private static final Logger LOG = LoggerFactory.getLogger(JStormStateInternals.class);

  private static final String STATE_INFO = "state-info:";

  @Nullable
  private final K key;
  private final IKvStoreManager kvStoreManager;
  private final TimerService timerService;
  private final int executorId;

  public JStormStateInternals(K key, IKvStoreManager kvStoreManager,
                              TimerService timerService, int executorId) {
    this.key = key;
    this.kvStoreManager = checkNotNull(kvStoreManager, "kvStoreManager");
    this.timerService = checkNotNull(timerService, "timerService");
    this.executorId = executorId;
  }

  @Nullable
  @Override
  public K getKey() {
    return key;
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
    /**
     * TODO：
     * Same implementation as state() which is without StateContext. This might be updated after
     * we figure out if we really need StateContext for JStorm state internals.
     */
    return state(namespace, address);
  }

  @Override
  public <T extends State> T state(final StateNamespace namespace, StateTag<T> address) {
    return address.getSpec().bind(address.getId(), new StateBinder() {
      @Override
      public <T> ValueState<T> bindValue(String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
        try {
          return new JStormValueState<>(
              getKey(), namespace, kvStoreManager.<ComposedKey, T>getOrCreate(getStoreId(id)));
        } catch (IOException e) {
          throw new RuntimeException();
        }
      }

      @Override
      public <T> BagState<T> bindBag(String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
        try {
          return new JStormBagState(
              getKey(), namespace, kvStoreManager.<ComposedKey, T>getOrCreate(getStoreId(id)),
              kvStoreManager.<ComposedKey, Object>getOrCreate(STATE_INFO + getStoreId(id)));
        } catch (IOException e) {
          throw new RuntimeException();
        }
      }

      @Override
      public <T> SetState<T> bindSet(String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
          String id,
          StateSpec<MapState<KeyT, ValueT>> spec,
          Coder<KeyT> mapKeyCoder,
          Coder<ValueT> mapValueCoder) {
        try {
          return new JStormMapState<>(
              getKey(), namespace, kvStoreManager.<KeyT, ValueT>getOrCreate(getStoreId(id)));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public <InputT, AccumT, OutputT> CombiningState bindCombining(
          String id,
          StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
          Coder<AccumT> accumCoder,
          Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
        try {
          BagState<AccumT> accumBagState = new JStormBagState(
              getKey(), namespace,
              kvStoreManager.<ComposedKey, AccumT>getOrCreate(getStoreId(id)),
              kvStoreManager.<ComposedKey, Object>getOrCreate(STATE_INFO + getStoreId(id)));
          return new JStormCombiningState<>(accumBagState, combineFn);
        } catch (IOException e) {
          throw new RuntimeException();
        }
      }


      @Override
      public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT>
      bindCombiningWithContext(
          String id,
          StateSpec<CombiningState<InputT, AccumT, OutputT>> stateSpec, Coder<AccumT> coder,
          CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFnWithContext) {
        throw new UnsupportedOperationException();
      }

      @Override
      public WatermarkHoldState bindWatermark(
          String id,
          StateSpec<WatermarkHoldState> spec,
          final TimestampCombiner timestampCombiner) {
        try {
          BagState<Combine.Holder<Instant>> accumBagState = new JStormBagState(
              getKey(), namespace,
              kvStoreManager.<ComposedKey, Combine.Holder<Instant>>getOrCreate(getStoreId(id)),
              kvStoreManager.<ComposedKey, Object>getOrCreate(STATE_INFO + getStoreId(id)));

          Combine.CombineFn<Instant, Combine.Holder<Instant>, Instant> outputTimeCombineFn =
              new BinaryCombineFn<Instant>() {
                @Override
                public Instant apply(Instant left, Instant right) {
                  return timestampCombiner.combine(left, right);
                }
              };
          return new JStormWatermarkHoldState(
              namespace,
              new JStormCombiningState<>(
                  accumBagState,
                  outputTimeCombineFn),
              timestampCombiner,
              timerService);
        } catch (IOException e) {
          throw new RuntimeException();
        }
      }
    });
  }

  /**
   * JStorm implementation of {@link ValueState}.
   */
  private static class JStormValueState<K, T> implements ValueState<T> {

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

  /**
   * Implementation of {@link BagState} in JStorm runner.
   */
  private static class JStormBagState<K, T> implements BagState<T> {

    @Nullable
    private final K key;
    private final StateNamespace namespace;
    private final IKvStore<ComposedKey, T> kvState;
    private final IKvStore<ComposedKey, Object> stateInfoKvState;

    JStormBagState(
        @Nullable K key,
        StateNamespace namespace,
        IKvStore<ComposedKey, T> kvState,
        IKvStore<ComposedKey, Object> stateInfoKvState) throws IOException {
      this.key = key;
      this.namespace = checkNotNull(namespace, "namespace");
      this.kvState = checkNotNull(kvState, "kvState");
      this.stateInfoKvState = checkNotNull(stateInfoKvState, "stateInfoKvState");
    }

    private int getElementIndex() throws IOException {
      Integer elementIndex = (Integer) stateInfoKvState.get(getComposedKey());
      return elementIndex != null ? elementIndex : 0;
    }

    @Override
    public void add(T input) {
      try {
        int elemIndex = getElementIndex();
        kvState.put(getComposedKey(elemIndex), input);
        stateInfoKvState.put(getComposedKey(), ++elemIndex);
      } catch (IOException e) {
        throw new RuntimeException(e.getCause());
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            return getElementIndex() <= 0;
          } catch (IOException e) {
            LOG.error("Failed to read", e);
            return false;
          }
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
      return new BagStateIterable();
    }

    @Override
    public BagState readLater() {
      // TODO: support prefetch.
      return this;
    }

    @Override
    public void clear() {
      try {
        int elemIndex = getElementIndex();
        for (int i = 0; i < elemIndex; i++) {
          kvState.remove(getComposedKey(i));
        }
        stateInfoKvState.remove(getComposedKey());
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

    @Override
    public String toString() {
      int elemIndex = -1;
      try {
        elemIndex = getElementIndex();
      } catch (IOException e) {

      }
      return String.format("JStormBagState: key=%s, namespace=%s, elementIndex=%d",
              key, namespace, elemIndex);
    }

    /**
     * Implementation of Bag state Iterable.
     */
    private class BagStateIterable implements KvStoreIterable<T> {

      private class BagStateIterator implements Iterator<T> {
        private final int size;
        private int cursor = 0;

        BagStateIterator() {
          try {
            this.size = getElementIndex();
          } catch (IOException e) {
            throw new RuntimeException(e.getCause());
          }
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

      BagStateIterable() {

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

  /**
   * JStorm implementation of {@link CombiningState}.
   */
  private static class JStormCombiningState<InputT, AccumT, OutputT>
      implements CombiningState<InputT, AccumT, OutputT> {

    @Nullable
    private final BagState<AccumT> accumBagState;
    private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;

    JStormCombiningState(
        BagState<AccumT> accumBagState,
        Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
      this.accumBagState = checkNotNull(accumBagState, "accumBagState");
      this.combineFn = checkNotNull(combineFn, "combineFn");
    }

    @Override
    public AccumT getAccum() {
      // TODO: replacing the accumBagState with the merged accum.
      return combineFn.mergeAccumulators(accumBagState.read());
    }

    @Override
    public void addAccum(AccumT accumT) {
      accumBagState.add(accumT);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> iterable) {
      return combineFn.mergeAccumulators(iterable);
    }

    @Override
    public void add(InputT input) {
      accumBagState.add(
          combineFn.addInput(combineFn.createAccumulator(), input));
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return accumBagState.isEmpty();
    }

    @Override
    public OutputT read() {
      return combineFn.extractOutput(
          combineFn.mergeAccumulators(accumBagState.read()));
    }

    @Override
    public CombiningState<InputT, AccumT, OutputT> readLater() {
      // TODO: support prefetch.
      return this;
    }

    @Override
    public void clear() {
      accumBagState.clear();
    }
  }

  /**
   * Implementation of {@link MapState} in JStorm runner.
   * @param <K>
   * @param <V>
   */
  private static class JStormMapState<K, V> implements MapState<K, V> {

    private final K key;
    private final StateNamespace namespace;
    private IKvStore<K, V> kvStore;

    JStormMapState(K key, StateNamespace namespace, IKvStore<K, V> kvStore) {
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

  /**
   * JStorm implementation of {@link WatermarkHoldState}.
   */
  private static class JStormWatermarkHoldState implements WatermarkHoldState {

    private final StateNamespace namespace;
    private final GroupingState<Instant, Instant> watermarkHoldsState;
    private final TimestampCombiner timestampCombiner;
    private final TimerService timerService;

    JStormWatermarkHoldState(
        StateNamespace namespace,
        GroupingState<Instant, Instant> watermarkHoldsState,
        TimestampCombiner timestampCombiner,
        TimerService timerService) {
      this.namespace = checkNotNull(namespace, "namespace");
      this.watermarkHoldsState = checkNotNull(watermarkHoldsState, "watermarkHoldsState");
      this.timestampCombiner = checkNotNull(timestampCombiner, "timestampCombiner");
      this.timerService = checkNotNull(timerService, "timerService");
    }

    @Override
    public TimestampCombiner getTimestampCombiner() {
      return timestampCombiner;
    }

    @Override
    public void add(Instant instant) {
      timerService.addWatermarkHold(namespace.stringKey(), instant);
      watermarkHoldsState.add(instant);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return watermarkHoldsState.isEmpty();
    }

    @Override
    public Instant read() {
      return watermarkHoldsState.read();
    }

    @Override
    public WatermarkHoldState readLater() {
      // TODO: support prefetch.
      return this;
    }

    @Override
    public void clear() {
      timerService.clearWatermarkHold(namespace.stringKey());
      watermarkHoldsState.clear();
    }
  }

  private String getStoreId(String stateId) {
    return String.format("%s-%s", stateId, executorId);
  }
}
