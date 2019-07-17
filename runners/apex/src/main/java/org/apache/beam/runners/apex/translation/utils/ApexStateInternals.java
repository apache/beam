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
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Table;
import org.joda.time.Instant;

/**
 * Implementation of {@link StateInternals} for transient use.
 *
 * <p>For fields that need to be serialized, use {@link ApexStateInternalsFactory} or {@link
 * StateInternalsProxy}
 */
public class ApexStateInternals<K> implements StateInternals {
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
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, final StateContext<?> c) {
    return address.bind(new ApexStateBinder(namespace, address, c));
  }

  /** A {@link StateBinder} that returns {@link State} wrappers for serialized state. */
  private class ApexStateBinder implements StateBinder {
    private final StateNamespace namespace;
    private final StateContext<?> c;

    private ApexStateBinder(StateNamespace namespace, StateTag<?> address, StateContext<?> c) {
      this.namespace = namespace;
      this.c = c;
    }

    @Override
    public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
      return new ApexValueState<>(namespace, address, coder);
    }

    @Override
    public <T> BagState<T> bindBag(final StateTag<BagState<T>> address, Coder<T> elemCoder) {
      return new ApexBagState<>(namespace, address, elemCoder);
    }

    @Override
    public <T> SetState<T> bindSet(StateTag<SetState<T>> address, Coder<T> elemCoder) {
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
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
        StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> accumCoder,
        final CombineFn<InputT, AccumT, OutputT> combineFn) {
      return new ApexCombiningState<>(namespace, address, accumCoder, combineFn);
    }

    @Override
    public WatermarkHoldState bindWatermark(
        StateTag<WatermarkHoldState> address, TimestampCombiner timestampCombiner) {
      return new ApexWatermarkHoldState<>(namespace, address, timestampCombiner);
    }

    @Override
    public <InputT, AccumT, OutputT>
        CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
            StateTag<CombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      return bindCombiningValue(address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
    }
  }

  private class AbstractState<T> {
    protected final StateNamespace namespace;
    protected final StateTag<? extends State> address;
    protected final Coder<T> coder;

    private AbstractState(
        StateNamespace namespace, StateTag<? extends State> address, Coder<T> coder) {
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
          return coder.decode(input);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return value;
    }

    public void writeValue(T input) {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      try {
        coder.encode(input, output);
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
        StateNamespace namespace, StateTag<ValueState<T>> address, Coder<T> coder) {
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

  private final class ApexWatermarkHoldState<W extends BoundedWindow> extends AbstractState<Instant>
      implements WatermarkHoldState {

    private final TimestampCombiner timestampCombiner;

    public ApexWatermarkHoldState(
        StateNamespace namespace,
        StateTag<WatermarkHoldState> address,
        TimestampCombiner timestampCombiner) {
      super(namespace, address, InstantCoder.of());
      this.timestampCombiner = timestampCombiner;
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
      combined = (combined == null) ? outputTime : timestampCombiner.combine(combined, outputTime);
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
    public TimestampCombiner getTimestampCombiner() {
      return timestampCombiner;
    }
  }

  private final class ApexCombiningState<InputT, AccumT, OutputT> extends AbstractState<AccumT>
      implements CombiningState<InputT, AccumT, OutputT> {
    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    private ApexCombiningState(
        StateNamespace namespace,
        StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> coder,
        CombineFn<InputT, AccumT, OutputT> combineFn) {
      super(namespace, address, coder);
      this.combineFn = combineFn;
    }

    @Override
    public ApexCombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public OutputT read() {
      return combineFn.extractOutput(getAccum());
    }

    @Override
    public void add(InputT input) {
      AccumT accum = combineFn.addInput(getAccum(), input);
      writeValue(accum);
    }

    @Override
    public AccumT getAccum() {
      AccumT accum = readValue();
      if (accum == null) {
        accum = combineFn.createAccumulator();
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
      accum = combineFn.mergeAccumulators(Arrays.asList(getAccum(), accum));
      writeValue(accum);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators);
    }
  }

  private final class ApexBagState<T> extends AbstractState<List<T>> implements BagState<T> {
    private ApexBagState(StateNamespace namespace, StateTag<BagState<T>> address, Coder<T> coder) {
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
   * Implementation of {@link StateInternals} that can be serialized and checkpointed with the
   * operator. Suitable for small states, in the future this should be based on the incremental
   * state saving components in the Apex library.
   *
   * @param <K> key type
   */
  @DefaultSerializer(JavaSerializer.class)
  public static class ApexStateInternalsFactory<K>
      implements StateInternalsFactory<K>, Serializable {
    private static final long serialVersionUID = 1L;
    /** Serializable state for internals (namespace to state tag to coded value). */
    private Map<Slice, HashBasedTable<String, String, byte[]>> perKeyState = new HashMap<>();

    private final Coder<K> keyCoder;

    private ApexStateInternalsFactory(Coder<K> keyCoder) {
      this.keyCoder = keyCoder;
    }

    public Coder<K> getKeyCoder() {
      return this.keyCoder;
    }

    @Override
    public ApexStateInternals<K> stateInternalsForKey(K key) {
      final Slice keyBytes;
      try {
        keyBytes =
            (key != null)
                ? new Slice(CoderUtils.encodeToByteArray(keyCoder, key))
                : new Slice(null);
      } catch (CoderException e) {
        throw new RuntimeException(e);
      }
      HashBasedTable<String, String, byte[]> stateTable =
          perKeyState.computeIfAbsent(keyBytes, k -> HashBasedTable.create());
      return new ApexStateInternals<>(key, stateTable);
    }
  }

  /** Factory to create the state internals. */
  public static class ApexStateBackend implements Serializable {
    private static final long serialVersionUID = 1L;

    public <K> ApexStateInternalsFactory<K> newStateInternalsFactory(Coder<K> keyCoder) {
      return new ApexStateInternalsFactory<>(keyCoder);
    }
  }
}
