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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.MultimapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.ReadableStates;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Beam {@link StateInternals} on top of Spark 4 {@code transformWithState}, a port of the legacy
 * {@code org.apache.beam.runners.spark.stateful.SparkStateInternals} with its Guava {@code
 * Table<String, String, byte[]>} replaced by the {@link BytesKV} SPI.
 *
 * <p>Every Beam state cell is stored as one {@link BytesKV} entry under the composite key {@code
 * namespace.stringKey() + "+" + tag.getId()}. Window namespaces render as {@code /<encoded
 * window>/} and always end in a slash, so the composite key is unambiguous.
 *
 * <p>Writes go straight through to the underlying store, there is no write buffering and therefore
 * nothing to flush. Reads of aggregate cells (bag, set, map) decode the whole cell, mutate it in
 * memory and write it back, exactly like the legacy Spark implementation. That is quadratic for
 * very large bags and is an accepted POC limitation.
 *
 * <p>An instance is scoped to a single key and to a single {@code handleInputRows} or {@code
 * handleExpiredTimer} invocation, because the underlying {@code MapState} resolves against Spark's
 * implicit grouping key which is only set for the duration of that call.
 *
 * @param <K> the Beam key type
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TwsStateInternals<K> implements StateInternals {

  /** Separator between the state namespace and the state tag id in the composite store key. */
  private static final String SEPARATOR = "+";

  private final K key;
  private final BytesKV store;

  private TwsStateInternals(K key, BytesKV store) {
    this.key = key;
    this.store = store;
  }

  /** Creates state internals for {@code key} backed by {@code store}. */
  public static <K> TwsStateInternals<K> forKey(K key, BytesKV store) {
    return new TwsStateInternals<>(key, store);
  }

  /** Returns the composite store key used for a namespace and state tag id. */
  public static String storeKey(StateNamespace namespace, String tagId) {
    return namespace.stringKey() + SEPARATOR + tagId;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
    return address.getSpec().bind(address.getId(), new TwsStateBinder(namespace, c));
  }

  private class TwsStateBinder implements StateBinder {
    private final StateNamespace namespace;
    private final StateContext<?> stateContext;

    private TwsStateBinder(StateNamespace namespace, StateContext<?> stateContext) {
      this.namespace = namespace;
      this.stateContext = stateContext;
    }

    @Override
    public <T> ValueState<T> bindValue(String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
      return new TwsValueState<>(namespace, id, coder);
    }

    @Override
    public <T> BagState<T> bindBag(String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
      return new TwsBagState<>(namespace, id, elemCoder);
    }

    @Override
    public <T> SetState<T> bindSet(String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
      return new TwsSetState<>(namespace, id, elemCoder);
    }

    @Override
    public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
        String id,
        StateSpec<MapState<KeyT, ValueT>> spec,
        Coder<KeyT> mapKeyCoder,
        Coder<ValueT> mapValueCoder) {
      return new TwsMapState<>(namespace, id, MapCoder.of(mapKeyCoder, mapValueCoder));
    }

    @Override
    public <KeyT, ValueT> MultimapState<KeyT, ValueT> bindMultimap(
        String id,
        StateSpec<MultimapState<KeyT, ValueT>> spec,
        Coder<KeyT> keyCoder,
        Coder<ValueT> valueCoder) {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", MultimapState.class.getSimpleName()));
    }

    @Override
    public <T> OrderedListState<T> bindOrderedList(
        String id, StateSpec<OrderedListState<T>> spec, Coder<T> elemCoder) {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", OrderedListState.class.getSimpleName()));
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(
        String id,
        StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        CombineFn<InputT, AccumT, OutputT> combineFn) {
      return new TwsCombiningState<>(namespace, id, accumCoder, combineFn);
    }

    @Override
    public <InputT, AccumT, OutputT>
        CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
            String id,
            StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
            Coder<AccumT> accumCoder,
            CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      return new TwsCombiningState<>(
          namespace, id, accumCoder, CombineFnUtil.bindContext(combineFn, stateContext));
    }

    @Override
    public WatermarkHoldState bindWatermark(
        String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
      return new TwsWatermarkHoldState(namespace, id, timestampCombiner);
    }
  }

  private class AbstractState<T> {
    final StateNamespace namespace;
    final String id;
    final Coder<T> coder;

    private AbstractState(StateNamespace namespace, String id, Coder<T> coder) {
      this.namespace = namespace;
      this.id = id;
      this.coder = coder;
    }

    private String cellKey() {
      return storeKey(namespace, id);
    }

    boolean exists() {
      return store.get(cellKey()) != null;
    }

    @Nullable
    T readValue() {
      byte[] buf = store.get(cellKey());
      if (buf == null) {
        return null;
      }
      try {
        return CoderUtils.decodeFromByteArray(coder, buf);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to decode state cell " + cellKey(), e);
      }
    }

    void writeValue(T input) {
      try {
        store.put(cellKey(), CoderUtils.encodeToByteArray(coder, input));
      } catch (Exception e) {
        throw new IllegalStateException("Failed to encode state cell " + cellKey(), e);
      }
    }

    public void clear() {
      store.remove(cellKey());
    }

    ReadableState<Boolean> isEmptyState() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return !exists();
        }
      };
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AbstractState)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      AbstractState<?> that = (AbstractState<?>) o;
      return namespace.equals(that.namespace) && id.equals(that.id);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + id.hashCode();
      return result;
    }
  }

  private class TwsValueState<T> extends AbstractState<T> implements ValueState<T> {

    private TwsValueState(StateNamespace namespace, String id, Coder<T> coder) {
      super(namespace, id, coder);
    }

    @Override
    public TwsValueState<T> readLater() {
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

  private class TwsWatermarkHoldState extends AbstractState<Instant> implements WatermarkHoldState {

    private final TimestampCombiner timestampCombiner;

    TwsWatermarkHoldState(
        StateNamespace namespace, String id, TimestampCombiner timestampCombiner) {
      super(namespace, id, InstantCoder.of());
      this.timestampCombiner = timestampCombiner;
    }

    @Override
    public TwsWatermarkHoldState readLater() {
      return this;
    }

    @Override
    public Instant read() {
      return readValue();
    }

    @Override
    public void add(Instant outputTime) {
      Instant combined = read();
      combined =
          (combined == null) ? outputTime : getTimestampCombiner().combine(combined, outputTime);
      writeValue(combined);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return isEmptyState();
    }

    @Override
    public TimestampCombiner getTimestampCombiner() {
      return timestampCombiner;
    }
  }

  @SuppressWarnings("TypeParameterShadowing")
  private class TwsCombiningState<KeyT, InputT, AccumT, OutputT> extends AbstractState<AccumT>
      implements CombiningState<InputT, AccumT, OutputT> {

    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    private TwsCombiningState(
        StateNamespace namespace,
        String id,
        Coder<AccumT> coder,
        CombineFn<InputT, AccumT, OutputT> combineFn) {
      super(namespace, id, coder);
      this.combineFn = combineFn;
    }

    @Override
    public TwsCombiningState<KeyT, InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public OutputT read() {
      return combineFn.extractOutput(getAccum());
    }

    @Override
    public void add(InputT input) {
      writeValue(combineFn.addInput(getAccum(), input));
    }

    @Override
    public AccumT getAccum() {
      AccumT accum = readValue();
      return accum == null ? combineFn.createAccumulator() : accum;
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return isEmptyState();
    }

    @Override
    public void addAccum(AccumT accum) {
      writeValue(combineFn.mergeAccumulators(Arrays.asList(getAccum(), accum)));
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators);
    }
  }

  private final class TwsMapState<MapKeyT, MapValueT> extends AbstractState<Map<MapKeyT, MapValueT>>
      implements MapState<MapKeyT, MapValueT> {

    private TwsMapState(StateNamespace namespace, String id, Coder<Map<MapKeyT, MapValueT>> coder) {
      super(namespace, id, coder);
    }

    @Override
    public ReadableState<MapValueT> get(MapKeyT mapKey) {
      return getOrDefault(mapKey, null);
    }

    @Override
    public ReadableState<MapValueT> getOrDefault(MapKeyT mapKey, @Nullable MapValueT defaultValue) {
      return new ReadableState<MapValueT>() {
        @Override
        public MapValueT read() {
          return readAsMap().getOrDefault(mapKey, defaultValue);
        }

        @Override
        public ReadableState<MapValueT> readLater() {
          return this;
        }
      };
    }

    @Override
    public void put(MapKeyT mapKey, MapValueT value) {
      Map<MapKeyT, MapValueT> current = readAsMap();
      current.put(mapKey, value);
      writeValue(current);
    }

    @Override
    public ReadableState<MapValueT> computeIfAbsent(
        MapKeyT mapKey, Function<? super MapKeyT, ? extends MapValueT> mappingFunction) {
      Map<MapKeyT, MapValueT> current = readAsMap();
      MapValueT existing = current.get(mapKey);
      if (existing == null) {
        put(mapKey, mappingFunction.apply(mapKey));
      }
      return ReadableStates.immediate(existing);
    }

    private Map<MapKeyT, MapValueT> readAsMap() {
      Map<MapKeyT, MapValueT> current = readValue();
      return current == null ? new HashMap<>() : current;
    }

    @Override
    public void remove(MapKeyT mapKey) {
      Map<MapKeyT, MapValueT> current = readAsMap();
      current.remove(mapKey);
      writeValue(current);
    }

    @Override
    public ReadableState<Iterable<MapKeyT>> keys() {
      return new ReadableState<Iterable<MapKeyT>>() {
        @Override
        public Iterable<MapKeyT> read() {
          return ImmutableList.copyOf(readAsMap().keySet());
        }

        @Override
        public ReadableState<Iterable<MapKeyT>> readLater() {
          return this;
        }
      };
    }

    @Override
    public ReadableState<Iterable<MapValueT>> values() {
      return new ReadableState<Iterable<MapValueT>>() {
        @Override
        public Iterable<MapValueT> read() {
          return ImmutableList.copyOf(readAsMap().values());
        }

        @Override
        public ReadableState<Iterable<MapValueT>> readLater() {
          return this;
        }
      };
    }

    @Override
    public ReadableState<Iterable<Map.Entry<MapKeyT, MapValueT>>> entries() {
      return new ReadableState<Iterable<Map.Entry<MapKeyT, MapValueT>>>() {
        @Override
        public Iterable<Map.Entry<MapKeyT, MapValueT>> read() {
          return ImmutableList.copyOf(readAsMap().entrySet());
        }

        @Override
        public ReadableState<Iterable<Map.Entry<MapKeyT, MapValueT>>> readLater() {
          return this;
        }
      };
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return isEmptyState();
    }
  }

  private final class TwsSetState<InputT> extends AbstractState<Set<InputT>>
      implements SetState<InputT> {

    private TwsSetState(StateNamespace namespace, String id, Coder<InputT> coder) {
      super(namespace, id, SetCoder.of(coder));
    }

    @Override
    public ReadableState<Boolean> contains(InputT input) {
      return ReadableStates.immediate(readAsSet().contains(input));
    }

    @Override
    public ReadableState<Boolean> addIfAbsent(InputT input) {
      Set<InputT> current = readAsSet();
      boolean added = current.add(input);
      writeValue(current);
      return ReadableStates.immediate(added);
    }

    @Override
    public void remove(InputT input) {
      Set<InputT> current = readAsSet();
      current.remove(input);
      writeValue(current);
    }

    @Override
    public SetState<InputT> readLater() {
      return this;
    }

    @Override
    public void add(InputT value) {
      Set<InputT> current = readAsSet();
      current.add(value);
      writeValue(current);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return isEmptyState();
    }

    @Override
    public Iterable<InputT> read() {
      Set<InputT> value = readValue();
      return value == null ? Collections.emptySet() : value;
    }

    private Set<InputT> readAsSet() {
      Set<InputT> value = readValue();
      return value == null ? new HashSet<>() : value;
    }
  }

  private final class TwsBagState<T> extends AbstractState<List<T>> implements BagState<T> {
    private TwsBagState(StateNamespace namespace, String id, Coder<T> coder) {
      super(namespace, id, ListCoder.of(coder));
    }

    @Override
    public TwsBagState<T> readLater() {
      return this;
    }

    @Override
    public List<T> read() {
      List<T> value = readValue();
      return value == null ? new ArrayList<>() : value;
    }

    @Override
    public void add(T input) {
      List<T> value = read();
      value.add(input);
      writeValue(value);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return isEmptyState();
    }
  }
}
