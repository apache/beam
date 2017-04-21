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
package org.apache.beam.runners.apex.translation.utils;

import com.datatorrent.netlet.util.Slice;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTag.StateBinder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.CombiningState;
import org.apache.beam.sdk.util.state.MapState;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.beam.sdk.util.state.SetState;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.util.state.StateContext;
import org.apache.beam.sdk.util.state.StateContexts;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.util.state.WatermarkHoldState;
import org.joda.time.Instant;

/**
 * Implementation of {@link StateInternals} for transient use.
 *
 * <p>For fields that need to be serialized, use {@link ApexStateInternalsFactory}
 * or {@link StateInternalsProxy}
 */
public class ApexStateInternals<K> implements StateInternals<K> {
  private final K key;
  private final Table<String, String, byte[]> stateTable;

  protected ApexStateInternals(K key, Table<String, String, byte[]> stateTable) {
    this.key = key;
    this.stateTable = stateTable;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public <T extends State> T state(StateNamespace namespace, StateTag<? super K, T> address) {
    return state(namespace, address, StateContexts.nullContext());
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<? super K, T> address, final StateContext<?> c) {
    return address.bind(new ApexStateBinder(key, namespace, address, c));
  }

  /**
   * A {@link StateBinder} that returns {@link State} wrappers for serialized state.
   */
  private class ApexStateBinder implements StateBinder<K> {
    private final K key;
    private final StateNamespace namespace;
    private final StateContext<?> c;

    private ApexStateBinder(K key, StateNamespace namespace, StateTag<? super K, ?> address,
        StateContext<?> c) {
      this.key = key;
      this.namespace = namespace;
      this.c = c;
    }

    @Override
    public <T> ValueState<T> bindValue(
        StateTag<? super K, ValueState<T>> address, Coder<T> coder) {
      return new ApexValueState<>(namespace, address, coder);
    }

    @Override
    public <T> BagState<T> bindBag(
        final StateTag<? super K, BagState<T>> address, Coder<T> elemCoder) {
      return new ApexBagState<>(namespace, address, elemCoder);
    }

    @Override
    public <T> SetState<T> bindSet(
        StateTag<? super K, SetState<T>> address,
        Coder<T> elemCoder) {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", SetState.class.getSimpleName()));
    }

    @Override
    public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
        StateTag<? super K, MapState<KeyT, ValueT>> spec,
        Coder<KeyT> mapKeyCoder, Coder<ValueT> mapValueCoder) {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", MapState.class.getSimpleName()));
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT>
        bindCombiningValue(
            StateTag<? super K, CombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            final CombineFn<InputT, AccumT, OutputT> combineFn) {
      return new ApexCombiningState<>(
          namespace,
          address,
          accumCoder,
          key,
          combineFn.<K>asKeyedFn()
          );
    }

    @Override
    public <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
        StateTag<? super K, WatermarkHoldState<W>> address,
        OutputTimeFn<? super W> outputTimeFn) {
      return new ApexWatermarkHoldState<>(namespace, address, outputTimeFn);
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT>
        bindKeyedCombiningValue(
            StateTag<? super K, CombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
      return new ApexCombiningState<>(
          namespace,
          address,
          accumCoder,
          key, combineFn);
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT>
        bindKeyedCombiningValueWithContext(
            StateTag<? super K, CombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT> combineFn) {
      return bindKeyedCombiningValue(address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
    }
  }

  private class AbstractState<T> {
    protected final StateNamespace namespace;
    protected final StateTag<?, ? extends State> address;
    protected final Coder<T> coder;

    private AbstractState(
        StateNamespace namespace,
        StateTag<?, ? extends State> address,
        Coder<T> coder) {
      this.namespace = namespace;
      this.address = address;
      this.coder = coder;
    }

    protected T readValue() {
      T value = null;
      byte[] buf = stateTable.get(namespace.stringKey(), address.getId());
      if (buf != null) {
        // TODO: reuse input
        Input input = new Input(buf);
        try {
          return coder.decode(input, Context.OUTER);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return value;
    }

    public void writeValue(T input) {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      try {
        coder.encode(input, output, Context.OUTER);
        stateTable.put(namespace.stringKey(), address.getId(), output.toByteArray());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public void clear() {
      stateTable.remove(namespace.stringKey(), address.getId());
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
      AbstractState<?> that = (AbstractState<?>) o;
      return namespace.equals(that.namespace) && address.equals(that.address);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private class ApexValueState<T> extends AbstractState<T> implements ValueState<T> {

    private ApexValueState(
        StateNamespace namespace,
        StateTag<?, ValueState<T>> address,
        Coder<T> coder) {
      super(namespace, address, coder);
    }

    @Override
    public ApexValueState<T> readLater() {
      return this;
    }

    @Override
    public T read() {
      return readValue();
    }

    @Override
    public void write(T input) {
      writeValue(input);
    }
  }

  private final class ApexWatermarkHoldState<W extends BoundedWindow>
      extends AbstractState<Instant> implements WatermarkHoldState<W> {

    private final OutputTimeFn<? super W> outputTimeFn;

    public ApexWatermarkHoldState(
        StateNamespace namespace,
        StateTag<?, WatermarkHoldState<W>> address,
        OutputTimeFn<? super W> outputTimeFn) {
      super(namespace, address, InstantCoder.of());
      this.outputTimeFn = outputTimeFn;
    }

    @Override
    public ApexWatermarkHoldState<W> readLater() {
      return this;
    }

    @Override
    public Instant read() {
      return readValue();
    }

    @Override
    public void add(Instant outputTime) {
      Instant combined = read();
      combined = (combined == null) ? outputTime : outputTimeFn.combine(combined, outputTime);
      writeValue(combined);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
        @Override
        public Boolean read() {
          return stateTable.get(namespace.stringKey(), address.getId()) == null;
        }
      };
    }

    @Override
    public OutputTimeFn<? super W> getOutputTimeFn() {
      return outputTimeFn;
    }

  }

  private final class ApexCombiningState<K, InputT, AccumT, OutputT>
      extends AbstractState<AccumT>
      implements CombiningState<InputT, AccumT, OutputT> {
    private final K key;
    private final KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn;

    private ApexCombiningState(StateNamespace namespace,
        StateTag<? super K, CombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> coder,
        K key, KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
      super(namespace, address, coder);
      this.key = key;
      this.combineFn = combineFn;
    }

    @Override
    public ApexCombiningState<K, InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public OutputT read() {
      return combineFn.extractOutput(key, getAccum());
    }

    @Override
    public void add(InputT input) {
      AccumT accum = getAccum();
      combineFn.addInput(key, accum, input);
      writeValue(accum);
    }

    @Override
    public AccumT getAccum() {
      AccumT accum = readValue();
      if (accum == null) {
        accum = combineFn.createAccumulator(key);
      }
      return accum;
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
        @Override
        public Boolean read() {
          return stateTable.get(namespace.stringKey(), address.getId()) == null;
        }
      };
    }

    @Override
    public void addAccum(AccumT accum) {
      accum = combineFn.mergeAccumulators(key, Arrays.asList(getAccum(), accum));
      writeValue(accum);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(key, accumulators);
    }

  }

  private final class ApexBagState<T> extends AbstractState<List<T>> implements BagState<T> {
    private ApexBagState(
        StateNamespace namespace,
        StateTag<?, BagState<T>> address,
        Coder<T> coder) {
      super(namespace, address, ListCoder.of(coder));
    }

    @Override
    public ApexBagState<T> readLater() {
      return this;
    }

    @Override
    public List<T> read() {
      List<T> value = super.readValue();
      if (value == null) {
        value = new ArrayList<>();
      }
      return value;
    }

    @Override
    public void add(T input) {
      List<T> value = read();
      value.add(input);
      writeValue(value);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return stateTable.get(namespace.stringKey(), address.getId()) == null;
        }
      };
    }
  }

  /**
   * Implementation of {@link StateInternals} that can be serialized and
   * checkpointed with the operator. Suitable for small states, in the future this
   * should be based on the incremental state saving components in the Apex
   * library.
   *
   * @param <K> key type
   */
  @DefaultSerializer(JavaSerializer.class)
  public static class ApexStateInternalsFactory<K>
      implements StateInternalsFactory<K>, Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * Serializable state for internals (namespace to state tag to coded value).
     */
    private Map<Slice, Table<String, String, byte[]>> perKeyState = new HashMap<>();
    private final Coder<K> keyCoder;

    private ApexStateInternalsFactory(Coder<K> keyCoder) {
      this.keyCoder = keyCoder;
    }

    @Override
    public ApexStateInternals<K> stateInternalsForKey(K key) {
      final Slice keyBytes;
      try {
        keyBytes = (key != null) ? new Slice(CoderUtils.encodeToByteArray(keyCoder, key)) :
          new Slice(null);
      } catch (CoderException e) {
        throw new RuntimeException(e);
      }
      Table<String, String, byte[]> stateTable = perKeyState.get(keyBytes);
      if (stateTable == null) {
        stateTable = HashBasedTable.create();
        perKeyState.put(keyBytes, stateTable);
      }
      return new ApexStateInternals<>(key, stateTable);
    }

  }

  /**
   * Factory to create the state internals.
   */
  public static class ApexStateBackend implements Serializable {
    private static final long serialVersionUID = 1L;

    public <K> ApexStateInternalsFactory<K> newStateInternalsFactory(Coder<K> keyCoder) {
      return new ApexStateInternalsFactory<K>(keyCoder);
    }
  }

}
