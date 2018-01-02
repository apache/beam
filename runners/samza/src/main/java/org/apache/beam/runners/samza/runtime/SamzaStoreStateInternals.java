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
import com.google.common.primitives.Ints;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateContexts;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.joda.time.Instant;

/**
 * {@link StateInternals} that uses Samza local {@link KeyValueStore} to manage state.
 */
public class SamzaStoreStateInternals<K> implements StateInternals {
  private static ThreadLocal<SoftReference<ByteArrayOutputStream>> threadLocalBaos =
      new ThreadLocal<>();

  private final KeyValueStore<byte[], byte[]> store;
  private final K key;
  private final byte[] keyBytes;

  private SamzaStoreStateInternals(KeyValueStore<byte[], byte[]> store,
                                  K key,
                                  byte[] keyBytes) {
    this.store = store;
    this.key = key;
    this.keyBytes = keyBytes;
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
  public <V extends State> V state(StateNamespace namespace,
                                   StateTag<V> address,
                                   StateContext<?> stateContext) {
    return address.bind(new StateTag.StateBinder() {
      @Override
      public <T> ValueState<T> bindValue(StateTag<ValueState<T>> spec,
                                         Coder<T> coder) {
        return new SamzaValueState<>(namespace, address, coder);
      }

      @Override
      public <T> BagState<T> bindBag(StateTag<BagState<T>> spec,
                                     Coder<T> elemCoder) {
        return new SamzaBagState<>(namespace, address, elemCoder);
      }

      @Override
      public <T> SetState<T> bindSet(StateTag<SetState<T>> spec,
                                     Coder<T> elemCoder) {
        throw new UnsupportedOperationException(
            String.format("%s is not supported", SetState.class.getSimpleName()));
      }

      @Override
      public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
          StateTag<MapState<KeyT, ValueT>> spec,
          Coder<KeyT> mapKeyCoder,
          Coder<ValueT> mapValueCoder) {
        throw new UnsupportedOperationException(
            String.format("%s is not supported", MapState.class.getSimpleName()));
      }

      @Override
      public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT>
      bindCombiningValue(
          StateTag<CombiningState<InputT, AccumT, OutputT>> spec,
          Coder<AccumT> accumCoder,
          Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
        return new SamzaAccumulatorCombiningState<>(namespace, address, accumCoder, combineFn);
      }

      @Override
      public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT>
      bindCombiningValueWithContext(
          StateTag<CombiningState<InputT, AccumT, OutputT>> spec,
          Coder<AccumT> accumCoder,
          CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
        throw new UnsupportedOperationException(
            String.format("%s is not supported", CombiningState.class.getSimpleName()));
      }

      @Override
      public WatermarkHoldState bindWatermark(
          StateTag<WatermarkHoldState> spec,
          TimestampCombiner timestampCombiner) {
        return new SamzaWatermarkHoldState(namespace, address, timestampCombiner);
      }
    });
  }

  /**
   * Reuse the ByteArrayOutputStream buffer.
   */
  private ByteArrayOutputStream getBaos() {
    final SoftReference<ByteArrayOutputStream> refBaos = threadLocalBaos.get();
    final ByteArrayOutputStream baos;
    if (refBaos == null) {
      baos = new ByteArrayOutputStream();
      threadLocalBaos.set(new SoftReference<>(baos));
    } else {
      baos = refBaos.get();
      baos.reset();
    }
    return baos;
  }

  /**
   * Factory class to create {@link SamzaStoreStateInternals}.
   */
  public static class Factory<K> implements StateInternalsFactory<K> {
    private final String stageId;
    private final KeyValueStore<byte[], byte[]> store;
    private final Coder<K> keyCoder;

    public Factory(String stageId, KeyValueStore<byte[], byte[]> store, Coder<K> keyCoder) {
      this.stageId = stageId;
      this.store = store;
      this.keyCoder = keyCoder;
    }

    @Override
    public StateInternals stateInternalsForKey(K key) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      try {
        dos.writeUTF(stageId);
        keyCoder.encode(key, baos);
        final byte[] keyBytes = baos.toByteArray();
        baos.reset();

        dos.write(keyBytes.length);
        dos.write(keyBytes);
      } catch (IOException e) {
        throw new RuntimeException("Cannot encode key for state store", e);
      }

      return new SamzaStoreStateInternals<>(store, key, baos.toByteArray());
    }
  }

  private abstract class AbstractSamzaState<T> {
    private final Coder<T> coder;
    private final byte[] encodedStoreKey;
    private final String namespace;
    private final String addressId;

    protected AbstractSamzaState(StateNamespace namespace,
                                 StateTag<? extends State> address,
                                 Coder<T> coder) {
      this.coder = coder;

      final ByteArrayOutputStream baos = getBaos();
      final DataOutputStream dos = new DataOutputStream(baos);

      this.namespace = namespace.stringKey();
      this.addressId = address.getId();

      try {
        dos.write(keyBytes);
        dos.writeUTF(namespace.stringKey());
        dos.writeUTF(address.getId());
      } catch (IOException e) {
        throw new RuntimeException(
            "Could not encode full address for state: " + address.getId(),
            e);
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

    protected byte[] getEncodedStoreKey() {
      return encodedStoreKey;
    }

    protected byte[] encodeValue(T value) {
      final ByteArrayOutputStream baos = getBaos();
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
    public boolean equals(Object o) {
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
    private SamzaValueState(StateNamespace namespace,
                            StateTag<? extends State> address,
                            Coder<T> coder) {
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

    private SamzaBagState(StateNamespace namespace,
                          StateTag<? extends State> address,
                          Coder<T> coder) {
      super(namespace, address, coder);
    }

    @Override
    public void add(T value) {
      synchronized (store) {
        final int size = getSize();
        final byte[] encodedKey = encodeKey(size);
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
    public List<T> read() {
      synchronized (store) {
        final int size = getSize();
        if (size == 0) {
          return Collections.emptyList();
        }

        final byte[] from = encodeKey(0);
        final byte[] to = encodeKey(size);
        final List<T> list = new ArrayList<>(size);
        final KeyValueIterator<byte[], byte[]> iter = store.range(from, to);
        while (iter.hasNext()) {
          list.add(decodeValue(iter.next().getValue()));
        }
        return list;
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
          final List<byte[]> keys = new ArrayList<>(size);
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

    private byte[] encodeKey(int size) {
      try {
        final ByteArrayOutputStream baos = getBaos();
        final DataOutputStream dos = new DataOutputStream(baos);
        dos.write(getEncodedStoreKey());
        dos.writeInt(size);
        return baos.toByteArray();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class SamzaAccumulatorCombiningState<InT, AccumT, OutT>
      extends AbstractSamzaState<AccumT>
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
      combineFn.addInput(accum, value);
      writeInternal(accum);
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
    public OutT read() {
      return combineFn.extractOutput(getAccum());
    }
  }

  private class SamzaWatermarkHoldState
      extends AbstractSamzaState<Instant>
      implements WatermarkHoldState {

    private final TimestampCombiner timestampCombiner;

    public <V extends State> SamzaWatermarkHoldState(StateNamespace namespace,
                                                     StateTag<V> address,
                                                     TimestampCombiner timestampCombiner) {
      super(namespace, address, InstantCoder.of());
      this.timestampCombiner = timestampCombiner;
    }

    @Override
    public void add(Instant value) {
      final Instant currentValue = readInternal();
      final Instant combinedValue = currentValue == null
          ? value
          : timestampCombiner.combine(currentValue, value);
      writeInternal(combinedValue);
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
}
