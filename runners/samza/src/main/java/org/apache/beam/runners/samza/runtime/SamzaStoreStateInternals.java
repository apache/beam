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
package org.apache.beam.runners.samza.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.state.SamzaMapState;
import org.apache.beam.runners.samza.state.SamzaSetState;
import org.apache.beam.runners.samza.transforms.UpdatingCombineFn;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.ReadableStates;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateContexts;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
import org.apache.samza.config.Config;
import org.apache.samza.context.TaskContext;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** {@link StateInternals} that uses Samza local {@link KeyValueStore} to manage state. */
public class SamzaStoreStateInternals<K> implements StateInternals {
  static final String BEAM_STORE = "beamStore";

  private static ThreadLocal<SoftReference<ByteArrayOutputStream>> threadLocalBaos =
      new ThreadLocal<>();

  // the stores include both beamStore for system states as well as stores for user state
  private final Map<String, KeyValueStore<ByteArray, byte[]>> stores;
  private final K key;
  private final byte[] keyBytes;
  private final int batchGetSize;
  private final String stageId;

  private SamzaStoreStateInternals(
      Map<String, KeyValueStore<ByteArray, byte[]>> stores,
      @Nullable K key,
      byte @Nullable [] keyBytes,
      String stageId,
      int batchGetSize) {
    this.stores = stores;
    this.key = key;
    this.keyBytes = keyBytes;
    this.batchGetSize = batchGetSize;
    this.stageId = stageId;
  }

  @SuppressWarnings("unchecked")
  static KeyValueStore<ByteArray, byte[]> getBeamStore(TaskContext context) {
    return (KeyValueStore<ByteArray, byte[]>) context.getStore(SamzaStoreStateInternals.BEAM_STORE);
  }

  static Factory createStateInternalFactory(
      String id,
      Coder<?> keyCoder,
      TaskContext context,
      SamzaPipelineOptions pipelineOptions,
      DoFnSignature signature) {
    final int batchGetSize = pipelineOptions.getStoreBatchGetSize();
    final Map<String, KeyValueStore<ByteArray, byte[]>> stores = new HashMap<>();
    stores.put(BEAM_STORE, getBeamStore(context));

    final Coder stateKeyCoder;
    if (keyCoder != null) {
      signature
          .stateDeclarations()
          .keySet()
          .forEach(
              stateId ->
                  stores.put(
                      stateId, (KeyValueStore<ByteArray, byte[]>) context.getStore(stateId)));
      stateKeyCoder = keyCoder;
    } else {
      stateKeyCoder = VoidCoder.of();
    }
    return new Factory<>(Objects.toString(id), stores, stateKeyCoder, batchGetSize);
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public <T extends State> T state(StateNamespace stateNamespace, StateTag<T> stateTag) {
    return state(stateNamespace, stateTag, StateContexts.nullContext());
  }

  @Override
  public <V extends State> V state(
      StateNamespace namespace, StateTag<V> address, StateContext<?> stateContext) {
    return address.bind(
        new StateTag.StateBinder() {
          @Override
          public <T> ValueState<T> bindValue(StateTag<ValueState<T>> spec, Coder<T> coder) {
            return new SamzaValueState<>(namespace, address, coder);
          }

          @Override
          public <T> BagState<T> bindBag(StateTag<BagState<T>> spec, Coder<T> elemCoder) {
            return new SamzaBagState<>(namespace, address, elemCoder);
          }

          @Override
          public <T> SetState<T> bindSet(StateTag<SetState<T>> spec, Coder<T> elemCoder) {
            return new SamzaSetStateImpl<>(namespace, address, elemCoder);
          }

          @Override
          public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
              StateTag<MapState<KeyT, ValueT>> spec,
              Coder<KeyT> mapKeyCoder,
              Coder<ValueT> mapValueCoder) {
            return new SamzaMapStateImpl<>(namespace, address, mapKeyCoder, mapValueCoder);
          }

          @Override
          public <T> OrderedListState<T> bindOrderedList(
              StateTag<OrderedListState<T>> spec, Coder<T> elemCoder) {
            throw new UnsupportedOperationException(
                String.format("%s is not supported", OrderedListState.class.getSimpleName()));
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> spec,
                  Coder<AccumT> accumCoder,
                  Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
            return new SamzaAccumulatorCombiningState<>(namespace, address, accumCoder, combineFn);
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> spec,
                  Coder<AccumT> accumCoder,
                  CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
            throw new UnsupportedOperationException(
                String.format("%s is not supported", CombiningState.class.getSimpleName()));
          }

          @Override
          public WatermarkHoldState bindWatermark(
              StateTag<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
            return new SamzaWatermarkHoldState(namespace, address, timestampCombiner);
          }
        });
  }

  /** Reuse the ByteArrayOutputStream buffer. */
  private static ByteArrayOutputStream getThreadLocalBaos() {
    final SoftReference<ByteArrayOutputStream> refBaos = threadLocalBaos.get();
    ByteArrayOutputStream baos = refBaos == null ? null : refBaos.get();
    if (baos == null) {
      baos = new ByteArrayOutputStream();
      threadLocalBaos.set(new SoftReference<>(baos));
    }

    baos.reset();
    return baos;
  }

  /** Factory class to create {@link SamzaStoreStateInternals}. */
  public static class Factory<K> implements StateInternalsFactory<K> {
    private final String stageId;
    private final Map<String, KeyValueStore<ByteArray, byte[]>> stores;
    private final Coder<K> keyCoder;
    private final int batchGetSize;

    public Factory(
        String stageId,
        Map<String, KeyValueStore<ByteArray, byte[]>> stores,
        Coder<K> keyCoder,
        int batchGetSize) {
      this.stageId = stageId;
      this.stores = stores;
      this.keyCoder = keyCoder;
      this.batchGetSize = batchGetSize;
    }

    @Override
    public StateInternals stateInternalsForKey(@Nullable K key) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      try {
        if (key != null) {
          keyCoder.encode(key, baos);
        }
        final byte[] keyBytes = baos.toByteArray();
        baos.reset();

        dos.write(keyBytes.length);
        dos.write(keyBytes);
      } catch (IOException e) {
        throw new RuntimeException("Cannot encode key for state store", e);
      }

      return new SamzaStoreStateInternals<>(stores, key, baos.toByteArray(), stageId, batchGetSize);
    }
  }

  /** An internal State interface that holds underlying KeyValueIterators. */
  interface KeyValueIteratorState {
    void closeIterators();
  }

  private abstract class AbstractSamzaState<T> {
    private final Coder<T> coder;
    private final byte[] encodedStoreKey;
    private final String namespace;
    protected final KeyValueStore<ByteArray, byte[]> store;

    protected AbstractSamzaState(
        StateNamespace namespace, StateTag<? extends State> address, Coder<T> coder) {
      this.coder = coder;
      this.namespace = namespace.stringKey();

      final KeyValueStore<ByteArray, byte[]> userStore = stores.get(address.getId());
      this.store = userStore != null ? userStore : stores.get(BEAM_STORE);

      final ByteArrayOutputStream baos = getThreadLocalBaos();
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        dos.write(keyBytes);
        dos.writeUTF(namespace.stringKey());

        if (userStore == null) {
          // for system state, we need to differentiate based on the following:
          dos.writeUTF(stageId);
          dos.writeUTF(address.getId());
        }
      } catch (IOException e) {
        throw new RuntimeException(
            "Could not encode full address for state: " + address.getId(), e);
      }
      this.encodedStoreKey = baos.toByteArray();
    }

    protected void clearInternal() {
      store.delete(getEncodedStoreKey());
    }

    protected void writeInternal(T value) {
      store.put(getEncodedStoreKey(), encodeValue(value));
    }

    protected T readInternal() {
      final byte[] valueBytes = store.get(getEncodedStoreKey());
      return decodeValue(valueBytes);
    }

    protected ReadableState<Boolean> isEmptyInternal() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          return store.get(getEncodedStoreKey()) == null;
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    protected ByteArray getEncodedStoreKey() {
      return ByteArray.of(encodedStoreKey);
    }

    protected byte[] getEncodedStoreKeyBytes() {
      return encodedStoreKey;
    }

    protected byte[] encodeValue(T value) {
      final ByteArrayOutputStream baos = getThreadLocalBaos();
      try {
        coder.encode(value, baos);
      } catch (IOException e) {
        throw new RuntimeException("Could not encode state value: " + value, e);
      }
      return baos.toByteArray();
    }

    protected T decodeValue(byte[] valueBytes) {
      if (valueBytes != null) {
        try {
          return coder.decode(new ByteArrayInputStream(valueBytes));
        } catch (IOException e) {
          throw new RuntimeException("Could not decode state", e);
        }
      }
      return null;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      @SuppressWarnings("unchecked")
      final AbstractSamzaState<?> that = (AbstractSamzaState<?>) o;
      return Arrays.equals(encodedStoreKey, that.encodedStoreKey);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + Arrays.hashCode(encodedStoreKey);
      return result;
    }
  }

  private class SamzaValueState<T> extends AbstractSamzaState<T> implements ValueState<T> {
    private SamzaValueState(
        StateNamespace namespace, StateTag<? extends State> address, Coder<T> coder) {
      super(namespace, address, coder);
    }

    @Override
    public void write(T input) {
      writeInternal(input);
    }

    @Override
    public T read() {
      return readInternal();
    }

    @Override
    public ValueState<T> readLater() {
      return this;
    }

    @Override
    public void clear() {
      clearInternal();
    }
  }

  private class SamzaBagState<T> extends AbstractSamzaState<T> implements BagState<T> {

    private SamzaBagState(
        StateNamespace namespace, StateTag<? extends State> address, Coder<T> coder) {
      super(namespace, address, coder);
    }

    @Override
    public void add(T value) {
      synchronized (store) {
        final int size = getSize();
        final ByteArray encodedKey = encodeKey(size);
        store.put(encodedKey, encodeValue(value));
        store.put(getEncodedStoreKey(), Ints.toByteArray(size + 1));
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      synchronized (store) {
        return isEmptyInternal();
      }
    }

    @Override
    @Nonnull
    public List<T> read() {
      synchronized (store) {
        final int size = getSize();
        if (size == 0) {
          return Collections.emptyList();
        }

        final List<T> values = new ArrayList<>(size);
        final List<ByteArray> keys = new ArrayList<>(size);
        int start = 0;
        while (start < size) {
          final int end = Math.min(size, start + batchGetSize);
          for (int i = start; i < end; i++) {
            keys.add(encodeKey(i));
          }
          store.getAll(keys).values().forEach(value -> values.add(decodeValue(value)));

          start += batchGetSize;
          keys.clear();
        }
        return values;
      }
    }

    @Override
    public BagState<T> readLater() {
      return this;
    }

    @Override
    public void clear() {
      synchronized (store) {
        final int size = getSize();
        if (size != 0) {
          final List<ByteArray> keys = new ArrayList<>(size);
          for (int i = 0; i < size; i++) {
            keys.add(encodeKey(i));
          }
          store.deleteAll(keys);
          store.delete(getEncodedStoreKey());
        }
      }
    }

    private int getSize() {
      final byte[] sizeBytes = store.get(getEncodedStoreKey());
      return sizeBytes == null ? 0 : Ints.fromByteArray(sizeBytes);
    }

    private ByteArray encodeKey(int size) {
      final ByteArrayOutputStream baos = getThreadLocalBaos();
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        dos.write(getEncodedStoreKeyBytes());
        dos.writeInt(size);
        return ByteArray.of(baos.toByteArray());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class SamzaSetStateImpl<T> implements SamzaSetState<T>, KeyValueIteratorState {
    private final SamzaMapStateImpl<T, Boolean> mapState;

    private SamzaSetStateImpl(
        StateNamespace namespace, StateTag<? extends State> address, Coder<T> coder) {
      mapState = new SamzaMapStateImpl<>(namespace, address, coder, BooleanCoder.of());
    }

    @Override
    public ReadableState<Boolean> contains(T t) {
      return mapState.get(t);
    }

    @Override
    public @Nullable ReadableState<Boolean> addIfAbsent(T t) {
      return mapState.putIfAbsent(t, true);
    }

    @Override
    public void remove(T t) {
      mapState.remove(t);
    }

    @Override
    public void add(T value) {
      mapState.put(value, true);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {

        @Override
        public Boolean read() {
          return Iterables.isEmpty(mapState.entries().read());
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public Iterable<T> read() {
      return mapState.keys().read();
    }

    @Override
    public SetState<T> readLater() {
      return this;
    }

    @Override
    public void clear() {
      mapState.clear();
    }

    @Override
    public ReadableState<Iterator<T>> readIterator() {
      final Iterator<Map.Entry<T, Boolean>> iter = mapState.readIterator().read();
      return new ReadableState<Iterator<T>>() {
        @Nullable
        @Override
        public Iterator<T> read() {
          return new Iterator<T>() {
            @Override
            public boolean hasNext() {
              return iter.hasNext();
            }

            @Override
            public T next() {
              return iter.next().getKey();
            }
          };
        }

        @Override
        public ReadableState<Iterator<T>> readLater() {
          return this;
        }
      };
    }

    @Override
    public void closeIterators() {
      mapState.closeIterators();
    }
  }

  private class SamzaMapStateImpl<KeyT, ValueT> extends AbstractSamzaState<ValueT>
      implements SamzaMapState<KeyT, ValueT>, KeyValueIteratorState {

    private final Coder<KeyT> keyCoder;
    private final int storeKeySize;
    private final List<KeyValueIterator<ByteArray, byte[]>> openIterators =
        Collections.synchronizedList(new ArrayList<>());

    private int maxKeySize;

    protected SamzaMapStateImpl(
        StateNamespace namespace,
        StateTag<? extends State> address,
        Coder<KeyT> keyCoder,
        Coder<ValueT> valueCoder) {
      super(namespace, address, valueCoder);

      this.keyCoder = keyCoder;
      this.storeKeySize = getEncodedStoreKeyBytes().length;
      // initial max key size is around 100k, so we can restore timer keys
      this.maxKeySize = this.storeKeySize + 100_000;
    }

    @Override
    public void put(KeyT key, ValueT value) {
      final ByteArray encodedKey = encodeKey(key);
      maxKeySize = Math.max(maxKeySize, encodedKey.getValue().length);
      store.put(encodedKey, encodeValue(value));
    }

    @Override
    public @Nullable ReadableState<ValueT> putIfAbsent(KeyT key, ValueT value) {
      final ByteArray encodedKey = encodeKey(key);
      final ValueT current = decodeValue(store.get(encodedKey));
      if (current == null) {
        put(key, value);
      }

      return current == null ? null : ReadableStates.immediate(current);
    }

    @Override
    public void remove(KeyT key) {
      store.delete(encodeKey(key));
    }

    @Override
    public ReadableState<ValueT> get(KeyT key) {
      ValueT value = decodeValue(store.get(encodeKey(key)));
      return ReadableStates.immediate(value);
    }

    @Override
    public ReadableState<Iterable<KeyT>> keys() {
      return new ReadableState<Iterable<KeyT>>() {
        @Override
        public Iterable<KeyT> read() {
          return createIterable(entry -> decodeKey(entry.getKey()));
        }

        @Override
        public ReadableState<Iterable<KeyT>> readLater() {
          return this;
        }
      };
    }

    @Override
    public ReadableState<Iterable<ValueT>> values() {
      return new ReadableState<Iterable<ValueT>>() {
        @Override
        public Iterable<ValueT> read() {
          return createIterable(entry -> decodeValue(entry.getValue()));
        }

        @Override
        public ReadableState<Iterable<ValueT>> readLater() {
          return this;
        }
      };
    }

    @Override
    public ReadableState<Iterable<Map.Entry<KeyT, ValueT>>> entries() {
      return new ReadableState<Iterable<Map.Entry<KeyT, ValueT>>>() {
        @Override
        public Iterable<Map.Entry<KeyT, ValueT>> read() {
          return createIterable(
              entry ->
                  new AbstractMap.SimpleEntry<>(
                      decodeKey(entry.getKey()), decodeValue(entry.getValue())));
        }

        @Override
        public ReadableState<Iterable<Map.Entry<KeyT, ValueT>>> readLater() {
          return this;
        }
      };
    }

    @Override
    public ReadableState<Iterator<Map.Entry<KeyT, ValueT>>> readIterator() {
      final ByteArray maxKey = createMaxKey();
      final KeyValueIterator<ByteArray, byte[]> kvIter = store.range(getEncodedStoreKey(), maxKey);
      openIterators.add(kvIter);

      return new ReadableState<Iterator<Map.Entry<KeyT, ValueT>>>() {
        @Nullable
        @Override
        public Iterator<Map.Entry<KeyT, ValueT>> read() {
          return new Iterator<Map.Entry<KeyT, ValueT>>() {
            @Override
            public boolean hasNext() {
              boolean hasNext = kvIter.hasNext();
              if (!hasNext) {
                kvIter.close();
                openIterators.remove(kvIter);
              }
              return hasNext;
            }

            @Override
            public Map.Entry<KeyT, ValueT> next() {
              Entry<ByteArray, byte[]> entry = kvIter.next();
              return new AbstractMap.SimpleEntry<>(
                  decodeKey(entry.getKey()), decodeValue(entry.getValue()));
            }
          };
        }

        @Override
        public ReadableState<Iterator<Map.Entry<KeyT, ValueT>>> readLater() {
          return this;
        }
      };
    }

    /**
     * Since we are not able to track the instances of the iterators created here and close them
     * properly, we need to load the content into memory.
     */
    private <OutputT> Iterable<OutputT> createIterable(
        SerializableFunction<org.apache.samza.storage.kv.Entry<ByteArray, byte[]>, OutputT> fn) {
      final ByteArray maxKey = createMaxKey();
      final KeyValueIterator<ByteArray, byte[]> kvIter = store.range(getEncodedStoreKey(), maxKey);
      final List<Entry<ByteArray, byte[]>> iterable = ImmutableList.copyOf(kvIter);
      kvIter.close();

      return new Iterable<OutputT>() {
        @Override
        public Iterator<OutputT> iterator() {
          final Iterator<Entry<ByteArray, byte[]>> iter = iterable.iterator();

          return new Iterator<OutputT>() {
            @Override
            public boolean hasNext() {
              return iter.hasNext();
            }

            @Override
            public OutputT next() {
              return fn.apply(iter.next());
            }
          };
        }
      };
    }

    @Override
    public void clear() {
      final ByteArray maxKey = createMaxKey();
      final KeyValueIterator<ByteArray, byte[]> kvIter = store.range(getEncodedStoreKey(), maxKey);
      while (kvIter.hasNext()) {
        store.delete(kvIter.next().getKey());
      }
      kvIter.close();
    }

    private ByteArray encodeKey(KeyT key) {
      try {
        final ByteArrayOutputStream baos = getThreadLocalBaos();
        baos.write(getEncodedStoreKeyBytes());
        keyCoder.encode(key, baos);
        return ByteArray.of(baos.toByteArray());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private KeyT decodeKey(ByteArray keyBytes) {
      try {
        final byte[] realKey =
            Arrays.copyOfRange(keyBytes.value, storeKeySize, keyBytes.value.length);
        return keyCoder.decode(new ByteArrayInputStream(realKey));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private ByteArray createMaxKey() {
      byte[] maxKey = new byte[maxKeySize];
      Arrays.fill(maxKey, (byte) 0xff);

      final byte[] encodedKey = getEncodedStoreKeyBytes();
      System.arraycopy(encodedKey, 0, maxKey, 0, encodedKey.length);
      return ByteArray.of(maxKey);
    }

    @Override
    public void closeIterators() {
      openIterators.forEach(KeyValueIterator::close);
      openIterators.clear();
    }
  }

  private class SamzaAccumulatorCombiningState<InT, AccumT, OutT> extends AbstractSamzaState<AccumT>
      implements CombiningState<InT, AccumT, OutT> {

    private final Combine.CombineFn<InT, AccumT, OutT> combineFn;

    protected SamzaAccumulatorCombiningState(
        StateNamespace namespace,
        StateTag<? extends State> address,
        Coder<AccumT> coder,
        Combine.CombineFn<InT, AccumT, OutT> combineFn) {
      super(namespace, address, coder);

      this.combineFn = combineFn;
    }

    @Override
    public void clear() {
      clearInternal();
    }

    @Override
    public void add(InT value) {
      final AccumT accum = getAccum();
      final AccumT current = combineFn.addInput(accum, value);
      writeInternal(current);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return isEmptyInternal();
    }

    @Override
    public AccumT getAccum() {
      final AccumT accum = readInternal();
      return accum != null ? accum : combineFn.createAccumulator();
    }

    @Override
    public void addAccum(AccumT accum) {
      final AccumT currentAccum = getAccum();
      final AccumT mergedAccum = mergeAccumulators(Arrays.asList(currentAccum, accum));
      writeInternal(mergedAccum);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators);
    }

    @Override
    public CombiningState<InT, AccumT, OutT> readLater() {
      return this;
    }

    @Override
    @Nonnull
    public OutT read() {
      AccumT accum = getAccum();
      OutT output = combineFn.extractOutput(accum);
      if (combineFn instanceof UpdatingCombineFn) {
        AccumT updatedAccum =
            ((UpdatingCombineFn<InT, AccumT, OutT>) combineFn).updateAfterFiring(accum);
        writeInternal(updatedAccum);
      }
      return output;
    }
  }

  private class SamzaWatermarkHoldState extends AbstractSamzaState<Instant>
      implements WatermarkHoldState {

    private final TimestampCombiner timestampCombiner;

    public <V extends State> SamzaWatermarkHoldState(
        StateNamespace namespace, StateTag<V> address, TimestampCombiner timestampCombiner) {
      super(namespace, address, InstantCoder.of());
      this.timestampCombiner = timestampCombiner;
    }

    @Override
    public void add(Instant value) {
      final Instant currentValue = readInternal();
      final Instant combinedValue =
          currentValue == null ? value : timestampCombiner.combine(currentValue, value);

      if (!combinedValue.equals(currentValue)) {
        writeInternal(combinedValue);
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return isEmptyInternal();
    }

    @Override
    public Instant read() {
      return readInternal();
    }

    @Override
    public TimestampCombiner getTimestampCombiner() {
      return this.timestampCombiner;
    }

    @Override
    public WatermarkHoldState readLater() {
      return this;
    }

    @Override
    public void clear() {
      clearInternal();
    }
  }

  /** Wrapper of byte[] so it can used as key in the KeyValueStore for caching. */
  public static class ByteArray implements Serializable, Comparable<ByteArray> {

    private final byte[] value;

    public static ByteArray of(byte[] value) {
      return new ByteArray(value);
    }

    private ByteArray(byte[] value) {
      this.value = value;
    }

    public byte[] getValue() {
      return value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ByteArray byteArray = (ByteArray) o;
      return Arrays.equals(value, byteArray.value);
    }

    @Override
    public int hashCode() {
      return value != null ? Arrays.hashCode(value) : 0;
    }

    @Override
    public int compareTo(ByteArray other) {
      return UnsignedBytes.lexicographicalComparator().compare(value, other.value);
    }
  }

  /** Factory class to provide {@link ByteArraySerde}. */
  public static class ByteArraySerdeFactory implements SerdeFactory<ByteArray> {

    @Override
    public Serde<ByteArray> getSerde(String name, Config config) {
      return new ByteArraySerde();
    }

    /** Serde for {@link ByteArray}. */
    public static class ByteArraySerde implements Serde<ByteArray> {

      @Override
      public byte[] toBytes(ByteArray byteArray) {
        return byteArray.value;
      }

      @Override
      public ByteArray fromBytes(byte[] bytes) {
        return ByteArray.of(bytes);
      }
    }
  }
}
