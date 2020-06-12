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
package org.apache.beam.runners.flink.translation.wrappers.streaming.state;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CombineContextFactory;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link StateInternals} that uses a Flink {@link OperatorStateBackend} to manage the broadcast
 * state. The state is the same on all parallel instances of the operator. So we just need store
 * state of operator-0 in OperatorStateBackend.
 *
 * <p>Note: Ignore index of key. Mainly for SideInputs.
 */
public class FlinkBroadcastStateInternals<K> implements StateInternals {

  private int indexInSubtaskGroup;
  private final OperatorStateBackend stateBackend;
  // stateName -> <namespace, state>
  private Map<String, Map<String, ?>> stateForNonZeroOperator;

  public FlinkBroadcastStateInternals(int indexInSubtaskGroup, OperatorStateBackend stateBackend) {
    this.stateBackend = stateBackend;
    this.indexInSubtaskGroup = indexInSubtaskGroup;
    if (indexInSubtaskGroup != 0) {
      stateForNonZeroOperator = new HashMap<>();
    }
  }

  @Override
  public @Nullable K getKey() {
    return null;
  }

  @Override
  public <T extends State> T state(
      final StateNamespace namespace, StateTag<T> address, final StateContext<?> context) {

    return address.bind(
        new StateTag.StateBinder() {

          @Override
          public <T2> ValueState<T2> bindValue(StateTag<ValueState<T2>> address, Coder<T2> coder) {

            return new FlinkBroadcastValueState<>(stateBackend, address, namespace, coder);
          }

          @Override
          public <T2> BagState<T2> bindBag(StateTag<BagState<T2>> address, Coder<T2> elemCoder) {

            return new FlinkBroadcastBagState<>(stateBackend, address, namespace, elemCoder);
          }

          @Override
          public <T2> SetState<T2> bindSet(StateTag<SetState<T2>> address, Coder<T2> elemCoder) {
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
          public <ElemT> OrderedListState<ElemT> bindOrderedList(
              StateTag<OrderedListState<ElemT>> spec, Coder<ElemT> elemCoder) {
            throw new UnsupportedOperationException(
                String.format("%s is not supported", OrderedListState.class.getSimpleName()));
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {

            return new FlinkCombiningState<>(
                stateBackend, address, combineFn, namespace, accumCoder);
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
            return new FlinkCombiningStateWithContext<>(
                stateBackend,
                address,
                combineFn,
                namespace,
                accumCoder,
                FlinkBroadcastStateInternals.this,
                CombineContextFactory.createFromStateContext(context));
          }

          @Override
          public WatermarkHoldState bindWatermark(
              StateTag<WatermarkHoldState> address, TimestampCombiner timestampCombiner) {
            throw new UnsupportedOperationException(
                String.format("%s is not supported", WatermarkHoldState.class.getSimpleName()));
          }
        });
  }

  /**
   * 1. The way we would use it is to only checkpoint anything from the operator with subtask index
   * 0 because we assume that the state is the same on all parallel instances of the operator.
   *
   * <p>2. Use map to support namespace.
   */
  private abstract class AbstractBroadcastState<T> {

    private String name;
    private final StateNamespace namespace;
    private final ListStateDescriptor<Map<String, T>> flinkStateDescriptor;
    private final OperatorStateStore flinkStateBackend;

    AbstractBroadcastState(
        OperatorStateBackend flinkStateBackend,
        String name,
        StateNamespace namespace,
        Coder<T> coder) {
      this.name = name;

      this.namespace = namespace;
      this.flinkStateBackend = flinkStateBackend;

      CoderTypeInformation<Map<String, T>> typeInfo =
          new CoderTypeInformation<>(MapCoder.of(StringUtf8Coder.of(), coder));

      flinkStateDescriptor =
          new ListStateDescriptor<>(name, typeInfo.createSerializer(new ExecutionConfig()));
    }

    /** Get map(namespce->T) from index 0. */
    Map<String, T> getMap() throws Exception {
      if (indexInSubtaskGroup == 0) {
        return getMapFromBroadcastState();
      } else {
        Map<String, T> result = (Map<String, T>) stateForNonZeroOperator.get(name);
        // maybe restore from BroadcastState of Operator-0
        if (result == null) {
          result = getMapFromBroadcastState();
          if (result != null) {
            stateForNonZeroOperator.put(name, result);
            // we don't need it anymore, must clear it.
            flinkStateBackend.getUnionListState(flinkStateDescriptor).clear();
          }
        }
        return result;
      }
    }

    Map<String, T> getMapFromBroadcastState() throws Exception {
      ListState<Map<String, T>> state = flinkStateBackend.getUnionListState(flinkStateDescriptor);
      Iterable<Map<String, T>> iterable = state.get();
      Map<String, T> ret = null;
      if (iterable != null) {
        // just use index 0
        Iterator<Map<String, T>> iterator = iterable.iterator();
        if (iterator.hasNext()) {
          ret = iterator.next();
        }
      }
      return ret;
    }

    /** Update map(namespce->T) from index 0. */
    void updateMap(Map<String, T> map) throws Exception {
      if (indexInSubtaskGroup == 0) {
        ListState<Map<String, T>> state = flinkStateBackend.getUnionListState(flinkStateDescriptor);
        state.clear();
        if (map.size() > 0) {
          state.add(map);
        }
      } else {
        if (map.isEmpty()) {
          stateForNonZeroOperator.remove(name);
          // updateMap is always behind getMap,
          // getMap will clear map in BroadcastOperatorState,
          // we don't need clear here.
        } else {
          stateForNonZeroOperator.put(name, map);
        }
      }
    }

    void writeInternal(T input) {
      try {
        Map<String, T> map = getMap();
        if (map == null) {
          map = new HashMap<>();
        }
        map.put(namespace.stringKey(), input);
        updateMap(map);
      } catch (Exception e) {
        throw new RuntimeException("Error updating state.", e);
      }
    }

    T readInternal() {
      try {
        Map<String, T> map = getMap();
        if (map == null) {
          return null;
        } else {
          return map.get(namespace.stringKey());
        }
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    void clearInternal() {
      try {
        Map<String, T> map = getMap();
        if (map != null) {
          map.remove(namespace.stringKey());
          updateMap(map);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }
  }

  private class FlinkBroadcastValueState<T> extends AbstractBroadcastState<T>
      implements ValueState<T> {

    private final StateNamespace namespace;
    private final StateTag<ValueState<T>> address;

    FlinkBroadcastValueState(
        OperatorStateBackend flinkStateBackend,
        StateTag<ValueState<T>> address,
        StateNamespace namespace,
        Coder<T> coder) {
      super(flinkStateBackend, address.getId(), namespace, coder);

      this.namespace = namespace;
      this.address = address;
    }

    @Override
    public void write(T input) {
      writeInternal(input);
    }

    @Override
    public ValueState<T> readLater() {
      return this;
    }

    @Override
    public T read() {
      return readInternal();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkBroadcastValueState<?> that = (FlinkBroadcastValueState<?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }

    @Override
    public void clear() {
      clearInternal();
    }
  }

  private class FlinkBroadcastBagState<T> extends AbstractBroadcastState<List<T>>
      implements BagState<T> {

    private final StateNamespace namespace;
    private final StateTag<BagState<T>> address;

    FlinkBroadcastBagState(
        OperatorStateBackend flinkStateBackend,
        StateTag<BagState<T>> address,
        StateNamespace namespace,
        Coder<T> coder) {
      super(flinkStateBackend, address.getId(), namespace, ListCoder.of(coder));

      this.namespace = namespace;
      this.address = address;
    }

    @Override
    public void add(T input) {
      List<T> list = readInternal();
      if (list == null) {
        list = new ArrayList<>();
      }
      list.add(input);
      writeInternal(list);
    }

    @Override
    public BagState<T> readLater() {
      return this;
    }

    @Override
    public Iterable<T> read() {
      List<T> result = readInternal();
      return result != null ? result : Collections.emptyList();
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            List<T> result = readInternal();
            return result == null;
          } catch (Exception e) {
            throw new RuntimeException("Error reading state.", e);
          }
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public void clear() {
      clearInternal();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkBroadcastBagState<?> that = (FlinkBroadcastBagState<?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private class FlinkCombiningState<InputT, AccumT, OutputT> extends AbstractBroadcastState<AccumT>
      implements CombiningState<InputT, AccumT, OutputT> {

    private final StateNamespace namespace;
    private final StateTag<CombiningState<InputT, AccumT, OutputT>> address;
    private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;

    FlinkCombiningState(
        OperatorStateBackend flinkStateBackend,
        StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
        StateNamespace namespace,
        Coder<AccumT> accumCoder) {
      super(flinkStateBackend, address.getId(), namespace, accumCoder);

      this.namespace = namespace;
      this.address = address;
      this.combineFn = combineFn;
    }

    @Override
    public CombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public void add(InputT value) {
      AccumT current = readInternal();
      if (current == null) {
        current = combineFn.createAccumulator();
      }
      current = combineFn.addInput(current, value);
      writeInternal(current);
    }

    @Override
    public void addAccum(AccumT accum) {
      AccumT current = readInternal();

      if (current == null) {
        writeInternal(accum);
      } else {
        current = combineFn.mergeAccumulators(Arrays.asList(current, accum));
        writeInternal(current);
      }
    }

    @Override
    public AccumT getAccum() {
      AccumT accum = readInternal();
      return accum != null ? accum : combineFn.createAccumulator();
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators);
    }

    @Override
    public OutputT read() {
      AccumT accum = readInternal();
      if (accum != null) {
        return combineFn.extractOutput(accum);
      } else {
        return combineFn.extractOutput(combineFn.createAccumulator());
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            return readInternal() == null;
          } catch (Exception e) {
            throw new RuntimeException("Error reading state.", e);
          }
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public void clear() {
      clearInternal();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkCombiningState<?, ?, ?> that = (FlinkCombiningState<?, ?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private class FlinkKeyedCombiningState<K2, InputT, AccumT, OutputT>
      extends AbstractBroadcastState<AccumT> implements CombiningState<InputT, AccumT, OutputT> {

    private final StateNamespace namespace;
    private final StateTag<CombiningState<InputT, AccumT, OutputT>> address;
    private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;
    private final FlinkBroadcastStateInternals<K2> flinkStateInternals;

    FlinkKeyedCombiningState(
        OperatorStateBackend flinkStateBackend,
        StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
        StateNamespace namespace,
        Coder<AccumT> accumCoder,
        FlinkBroadcastStateInternals<K2> flinkStateInternals) {
      super(flinkStateBackend, address.getId(), namespace, accumCoder);

      this.namespace = namespace;
      this.address = address;
      this.combineFn = combineFn;
      this.flinkStateInternals = flinkStateInternals;
    }

    @Override
    public CombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public void add(InputT value) {
      try {
        AccumT current = readInternal();
        if (current == null) {
          current = combineFn.createAccumulator();
        }
        current = combineFn.addInput(current, value);
        writeInternal(current);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state.", e);
      }
    }

    @Override
    public void addAccum(AccumT accum) {
      try {
        AccumT current = readInternal();
        if (current == null) {
          writeInternal(accum);
        } else {
          current = combineFn.mergeAccumulators(Arrays.asList(current, accum));
          writeInternal(current);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state.", e);
      }
    }

    @Override
    public AccumT getAccum() {
      try {
        AccumT accum = readInternal();
        return accum != null ? accum : combineFn.createAccumulator();
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators);
    }

    @Override
    public OutputT read() {
      try {
        AccumT accum = readInternal();
        if (accum != null) {
          return combineFn.extractOutput(accum);
        } else {
          return combineFn.extractOutput(combineFn.createAccumulator());
        }
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            return readInternal() == null;
          } catch (Exception e) {
            throw new RuntimeException("Error reading state.", e);
          }
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public void clear() {
      clearInternal();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkKeyedCombiningState<?, ?, ?, ?> that = (FlinkKeyedCombiningState<?, ?, ?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private class FlinkCombiningStateWithContext<K2, InputT, AccumT, OutputT>
      extends AbstractBroadcastState<AccumT> implements CombiningState<InputT, AccumT, OutputT> {

    private final StateNamespace namespace;
    private final StateTag<CombiningState<InputT, AccumT, OutputT>> address;
    private final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn;
    private final FlinkBroadcastStateInternals<K2> flinkStateInternals;
    private final CombineWithContext.Context context;

    FlinkCombiningStateWithContext(
        OperatorStateBackend flinkStateBackend,
        StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn,
        StateNamespace namespace,
        Coder<AccumT> accumCoder,
        FlinkBroadcastStateInternals<K2> flinkStateInternals,
        CombineWithContext.Context context) {
      super(flinkStateBackend, address.getId(), namespace, accumCoder);

      this.namespace = namespace;
      this.address = address;
      this.combineFn = combineFn;
      this.flinkStateInternals = flinkStateInternals;
      this.context = context;
    }

    @Override
    public CombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public void add(InputT value) {
      try {
        AccumT current = readInternal();
        if (current == null) {
          current = combineFn.createAccumulator(context);
        }
        current = combineFn.addInput(current, value, context);
        writeInternal(current);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state.", e);
      }
    }

    @Override
    public void addAccum(AccumT accum) {
      try {

        AccumT current = readInternal();
        if (current == null) {
          writeInternal(accum);
        } else {
          current = combineFn.mergeAccumulators(Arrays.asList(current, accum), context);
          writeInternal(current);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state.", e);
      }
    }

    @Override
    public AccumT getAccum() {
      try {
        AccumT accum = readInternal();
        return accum != null ? accum : combineFn.createAccumulator(context);
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators, context);
    }

    @Override
    public OutputT read() {
      try {
        AccumT accum = readInternal();
        if (accum == null) {
          accum = combineFn.createAccumulator(context);
        }
        return combineFn.extractOutput(accum, context);
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            return readInternal() == null;
          } catch (Exception e) {
            throw new RuntimeException("Error reading state.", e);
          }
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public void clear() {
      clearInternal();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkCombiningStateWithContext<?, ?, ?, ?> that =
          (FlinkCombiningStateWithContext<?, ?, ?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }
}
