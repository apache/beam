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
package org.apache.beam.runners.spark.stateful;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
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
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** An implementation of {@link StateInternals} for the SparkRunner. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SparkStateInternals<K> implements StateInternals {

  private final K key;
  // Serializable state for internals (namespace to state tag to coded value).
  private final Table<String, String, byte[]> stateTable;

  private SparkStateInternals(K key) {
    this.key = key;
    this.stateTable = HashBasedTable.create();
  }

  private SparkStateInternals(K key, Table<String, String, byte[]> stateTable) {
    this.key = key;
    this.stateTable = stateTable;
  }

  public static <K> SparkStateInternals<K> forKey(K key) {
    return new SparkStateInternals<>(key);
  }

  public static <K> SparkStateInternals<K> forKeyAndState(
      K key, Table<String, String, byte[]> stateTable) {
    return new SparkStateInternals<>(key, stateTable);
  }

  public Table<String, String, byte[]> getState() {
    return stateTable;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
    return address.getSpec().bind(address.getId(), new SparkStateBinder(namespace, c));
  }

  private class SparkStateBinder implements StateBinder {
    private final StateNamespace namespace;
    private final StateContext<?> stateContext;

    private SparkStateBinder(StateNamespace namespace, StateContext<?> stateContext) {
      this.namespace = namespace;
      this.stateContext = stateContext;
    }

    @Override
    public <T> ValueState<T> bindValue(String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
      return new SparkValueState<>(namespace, id, coder);
    }

    @Override
    public <T> BagState<T> bindBag(String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
      return new SparkBagState<>(namespace, id, elemCoder);
    }

    @Override
    public <T> SetState<T> bindSet(String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
      return new SparkSetState<>(namespace, id, elemCoder);
    }

    @Override
    public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
        String id,
        StateSpec<MapState<KeyT, ValueT>> spec,
        Coder<KeyT> mapKeyCoder,
        Coder<ValueT> mapValueCoder) {
      return new SparkMapState<>(namespace, id, MapCoder.of(mapKeyCoder, mapValueCoder));
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
      return new SparkOrderedListState<>(namespace, id, elemCoder);
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(
        String id,
        StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        CombineFn<InputT, AccumT, OutputT> combineFn) {
      return new SparkCombiningState<>(namespace, id, accumCoder, combineFn);
    }

    @Override
    public <InputT, AccumT, OutputT>
        CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
            String id,
            StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
            Coder<AccumT> accumCoder,
            CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      return new SparkCombiningState<>(
          namespace, id, accumCoder, CombineFnUtil.bindContext(combineFn, stateContext));
    }

    @Override
    public WatermarkHoldState bindWatermark(
        String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
      return new SparkWatermarkHoldState(namespace, id, timestampCombiner);
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

    T readValue() {
      byte[] buf = stateTable.get(namespace.stringKey(), id);
      if (buf != null) {
        return CoderHelpers.fromByteArray(buf, coder);
      }
      return null;
    }

    void writeValue(T input) {
      stateTable.put(namespace.stringKey(), id, CoderHelpers.toByteArray(input, coder));
    }

    public void clear() {
      stateTable.remove(namespace.stringKey(), id);
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

  private class SparkValueState<T> extends AbstractState<T> implements ValueState<T> {

    private SparkValueState(StateNamespace namespace, String id, Coder<T> coder) {
      super(namespace, id, coder);
    }

    @Override
    public SparkValueState<T> readLater() {
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

  private class SparkWatermarkHoldState extends AbstractState<Instant>
      implements WatermarkHoldState {

    private final TimestampCombiner timestampCombiner;

    SparkWatermarkHoldState(
        StateNamespace namespace, String id, TimestampCombiner timestampCombiner) {
      super(namespace, id, InstantCoder.of());
      this.timestampCombiner = timestampCombiner;
    }

    @Override
    public SparkWatermarkHoldState readLater() {
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
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return stateTable.get(namespace.stringKey(), id) == null;
        }
      };
    }

    @Override
    public TimestampCombiner getTimestampCombiner() {
      return timestampCombiner;
    }
  }

  @SuppressWarnings("TypeParameterShadowing")
  private class SparkCombiningState<KeyT, InputT, AccumT, OutputT> extends AbstractState<AccumT>
      implements CombiningState<InputT, AccumT, OutputT> {

    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    private SparkCombiningState(
        StateNamespace namespace,
        String id,
        Coder<AccumT> coder,
        CombineFn<InputT, AccumT, OutputT> combineFn) {
      super(namespace, id, coder);
      this.combineFn = combineFn;
    }

    @Override
    public SparkCombiningState<KeyT, InputT, AccumT, OutputT> readLater() {
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
          return stateTable.get(namespace.stringKey(), id) == null;
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

  private final class SparkMapState<MapKeyT, MapValueT>
      extends AbstractState<Map<MapKeyT, MapValueT>> implements MapState<MapKeyT, MapValueT> {

    private SparkMapState(
        StateNamespace namespace, String id, Coder<Map<MapKeyT, MapValueT>> coder) {
      super(namespace, id, coder);
    }

    @Override
    public ReadableState<MapValueT> get(MapKeyT key) {
      return getOrDefault(key, null);
    }

    @Override
    public ReadableState<MapValueT> getOrDefault(MapKeyT key, @Nullable MapValueT defaultValue) {
      return new ReadableState<MapValueT>() {
        @Override
        public MapValueT read() {
          Map<MapKeyT, MapValueT> sparkMapState = readValue();
          if (sparkMapState == null) {
            return defaultValue;
          }
          return sparkMapState.getOrDefault(key, defaultValue);
        }

        @Override
        public ReadableState<MapValueT> readLater() {
          return this;
        }
      };
    }

    @Override
    public void put(MapKeyT key, MapValueT value) {
      Map<MapKeyT, MapValueT> sparkMapState = readValue();
      if (sparkMapState == null) {
        sparkMapState = new HashMap<>();
      }
      sparkMapState.put(key, value);
      writeValue(sparkMapState);
    }

    @Override
    public ReadableState<MapValueT> computeIfAbsent(
        MapKeyT key, Function<? super MapKeyT, ? extends MapValueT> mappingFunction) {
      Map<MapKeyT, MapValueT> sparkMapState = readAsMap();
      MapValueT current = sparkMapState.get(key);
      if (current == null) {
        put(key, mappingFunction.apply(key));
      }
      return ReadableStates.immediate(current);
    }

    private Map<MapKeyT, MapValueT> readAsMap() {
      Map<MapKeyT, MapValueT> mapState = readValue();
      if (mapState == null) {
        mapState = new HashMap<>();
      }
      return mapState;
    }

    @Override
    public void remove(MapKeyT key) {
      Map<MapKeyT, MapValueT> sparkMapState = readAsMap();
      sparkMapState.remove(key);
      writeValue(sparkMapState);
    }

    @Override
    public ReadableState<Iterable<MapKeyT>> keys() {
      return new ReadableState<Iterable<MapKeyT>>() {
        @Override
        public Iterable<MapKeyT> read() {
          Map<MapKeyT, MapValueT> sparkMapState = readValue();
          if (sparkMapState == null) {
            return Collections.emptyList();
          }
          return sparkMapState.keySet();
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
          Map<MapKeyT, MapValueT> sparkMapState = readValue();
          if (sparkMapState == null) {
            return Collections.emptyList();
          }
          Iterable<MapValueT> result = readValue().values();
          return result != null ? ImmutableList.copyOf(result) : Collections.emptyList();
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
          Map<MapKeyT, MapValueT> sparkMapState = readValue();
          if (sparkMapState == null) {
            return Collections.emptyList();
          }
          return sparkMapState.entrySet();
        }

        @Override
        public ReadableState<Iterable<Map.Entry<MapKeyT, MapValueT>>> readLater() {
          return this;
        }
      };
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          return stateTable.get(namespace.stringKey(), id) == null;
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }
  }

  private final class SparkSetState<InputT> extends AbstractState<Set<InputT>>
      implements SetState<InputT> {

    private SparkSetState(StateNamespace namespace, String id, Coder<InputT> coder) {
      super(namespace, id, SetCoder.of(coder));
    }

    @Override
    public ReadableState<Boolean> contains(InputT input) {
      Set<InputT> sparkSetState = readAsSet();
      return ReadableStates.immediate(sparkSetState.contains(input));
    }

    @Override
    public ReadableState<Boolean> addIfAbsent(InputT input) {
      Set<InputT> sparkSetState = readAsSet();
      boolean alreadyContained = sparkSetState.contains(input);
      if (!alreadyContained) {
        sparkSetState.add(input);
      }
      writeValue(sparkSetState);
      return ReadableStates.immediate(!alreadyContained);
    }

    @Override
    public void remove(InputT input) {
      Set<InputT> sparkSetState = readAsSet();
      sparkSetState.remove(input);
      writeValue(sparkSetState);
    }

    @Override
    public SetState<InputT> readLater() {
      return this;
    }

    @Override
    public void add(InputT value) {
      final Set<InputT> sparkSetState = readAsSet();
      sparkSetState.add(value);
      writeValue(sparkSetState);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          return stateTable.get(namespace.stringKey(), id) == null;
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public Iterable<InputT> read() {
      Set<InputT> value = readValue();
      if (value == null) {
        value = new HashSet<>();
      }
      return value;
    }

    private Set<InputT> readAsSet() {
      return (Set<InputT>) read();
    }
  }

  private final class SparkBagState<T> extends AbstractState<List<T>> implements BagState<T> {
    private SparkBagState(StateNamespace namespace, String id, Coder<T> coder) {
      super(namespace, id, ListCoder.of(coder));
    }

    @Override
    public SparkBagState<T> readLater() {
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
          return stateTable.get(namespace.stringKey(), id) == null;
        }
      };
    }
  }

  private final class SparkOrderedListState<T> extends AbstractState<List<TimestampedValue<T>>>
      implements OrderedListState<T> {

    private SparkOrderedListState(StateNamespace namespace, String id, Coder<T> coder) {
      super(namespace, id, ListCoder.of(TimestampedValue.TimestampedValueCoder.of(coder)));
    }

    private SortedMap<Instant, TimestampedValue<T>> readAsMap() {
      final List<TimestampedValue<T>> listValues =
          MoreObjects.firstNonNull(this.readValue(), Lists.newArrayList());
      final SortedMap<Instant, TimestampedValue<T>> sortedMap = Maps.newTreeMap();
      for (TimestampedValue<T> value : listValues) {
        sortedMap.put(value.getTimestamp(), value);
      }
      return sortedMap;
    }

    @Override
    public Iterable<TimestampedValue<T>> readRange(Instant minTimestamp, Instant limitTimestamp) {
      return this.readAsMap().subMap(minTimestamp, limitTimestamp).values();
    }

    @Override
    public void clearRange(Instant minTimestamp, Instant limitTimestamp) {
      final SortedMap<Instant, TimestampedValue<T>> sortedMap = this.readAsMap();
      sortedMap.subMap(minTimestamp, limitTimestamp).clear();
      this.writeValue(Lists.newArrayList(sortedMap.values()));
    }

    @Override
    public OrderedListState<T> readRangeLater(Instant minTimestamp, Instant limitTimestamp) {
      return this;
    }

    @Override
    public void add(TimestampedValue<T> value) {
      final List<TimestampedValue<T>> listValue =
          MoreObjects.firstNonNull(this.readValue(), Lists.newArrayList());
      listValue.add(value);
      this.writeValue(listValue);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          final List<TimestampedValue<T>> listValue = readValue();
          return listValue == null || listValue.isEmpty();
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public Iterable<TimestampedValue<T>> read() {
      return this.readAsMap().values();
    }

    @Override
    public GroupingState<TimestampedValue<T>, Iterable<TimestampedValue<T>>> readLater() {
      return this;
    }

    @Override
    public void clear() {
      final List<TimestampedValue<T>> listValue = this.readValue();
      if (listValue != null) {
        listValue.clear();
        this.writeValue(listValue);
      }
    }
  }
}
