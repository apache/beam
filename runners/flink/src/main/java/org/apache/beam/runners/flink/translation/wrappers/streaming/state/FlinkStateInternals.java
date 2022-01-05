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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.runners.flink.translation.wrappers.streaming.FlinkKeyUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.MapState;
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
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CombineContextFactory;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.TreeMultiset;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;

/**
 * {@link StateInternals} that uses a Flink {@link KeyedStateBackend} to manage state.
 *
 * <p>Note: In the Flink streaming runner the key is always encoded using an {@link Coder} and
 * stored in a {@link ByteBuffer}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class FlinkStateInternals<K> implements StateInternals {

  private static final StateNamespace globalWindowNamespace =
      StateNamespaces.window(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE);

  private final KeyedStateBackend<ByteBuffer> flinkStateBackend;
  private final Coder<K> keyCoder;

  private static class StateAndNamespaceDescriptor<T> {
    static <T> StateAndNamespaceDescriptor<T> of(
        StateDescriptor<?, ?> stateDescriptor, T namespace, TypeSerializer<T> namespaceSerializer) {
      return new StateAndNamespaceDescriptor<>(stateDescriptor, namespace, namespaceSerializer);
    }

    private final StateDescriptor<?, ?> stateDescriptor;
    private final T namespace;
    private final TypeSerializer<T> namespaceSerializer;

    private StateAndNamespaceDescriptor(
        StateDescriptor<?, ?> stateDescriptor, T namespace, TypeSerializer<T> namespaceSerializer) {
      this.stateDescriptor = stateDescriptor;
      this.namespace = namespace;
      this.namespaceSerializer = namespaceSerializer;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StateAndNamespaceDescriptor<?> other = (StateAndNamespaceDescriptor<?>) o;
      return Objects.equals(stateDescriptor, other.stateDescriptor);
    }

    @Override
    public int hashCode() {
      return Objects.hash(stateDescriptor);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("stateDescriptor", stateDescriptor)
          .add("namespace", namespace)
          .add("namespaceSerializer", namespaceSerializer)
          .toString();
    }
  }

  /**
   * A set which contains all state descriptors created in the global window. Used for cleanup on
   * final watermark.
   */
  private final Set<StateAndNamespaceDescriptor<?>> globalWindowStateDescriptors = new HashSet<>();

  // Watermark holds for all keys/windows of this partition, allows efficient lookup of the minimum
  private final TreeMultiset<Long> watermarkHolds = TreeMultiset.create();
  // State to persist combined watermark holds for all keys of this partition
  private final MapStateDescriptor<String, Instant> watermarkHoldStateDescriptor;

  private final SerializablePipelineOptions pipelineOptions;

  public FlinkStateInternals(
      KeyedStateBackend<ByteBuffer> flinkStateBackend,
      Coder<K> keyCoder,
      SerializablePipelineOptions pipelineOptions)
      throws Exception {
    this.flinkStateBackend = Objects.requireNonNull(flinkStateBackend);
    this.keyCoder = Objects.requireNonNull(keyCoder);
    watermarkHoldStateDescriptor =
        new MapStateDescriptor<>(
            "watermark-holds",
            StringSerializer.INSTANCE,
            new CoderTypeSerializer<>(InstantCoder.of(), pipelineOptions));
    this.pipelineOptions = pipelineOptions;

    restoreWatermarkHoldsView();
  }

  /** Returns the minimum over all watermark holds. */
  public Long minWatermarkHoldMs() {
    if (watermarkHolds.isEmpty()) {
      return Long.MAX_VALUE;
    } else {
      return watermarkHolds.firstEntry().getElement();
    }
  }

  @Override
  public K getKey() {
    ByteBuffer keyBytes = flinkStateBackend.getCurrentKey();
    return FlinkKeyUtils.decodeKey(keyBytes, keyCoder);
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, StateContext<?> context) {
    return address.getSpec().bind(address.getId(), new FlinkStateBinder(namespace, context));
  }

  /**
   * Allows to clear all state for the global watermark when the maximum watermark arrives. We do
   * not clean up the global window state via timers which would lead to an unbounded number of keys
   * and cleanup timers. Instead, the cleanup code below should be run when we finally receive the
   * max watermark.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void clearGlobalState() {
    try {
      for (StateAndNamespaceDescriptor stateAndNamespace : globalWindowStateDescriptors) {
        flinkStateBackend.applyToAllKeys(
            stateAndNamespace.namespace,
            stateAndNamespace.namespaceSerializer,
            stateAndNamespace.stateDescriptor,
            (key, state) -> state.clear());
      }
      // Clear set to avoid repeating the cleanup
      globalWindowStateDescriptors.clear();
    } catch (Exception e) {
      throw new RuntimeException("Failed to cleanup global state.", e);
    }
  }

  private class FlinkStateBinder implements StateBinder {

    private final StateNamespace namespace;
    private final StateContext<?> stateContext;

    private FlinkStateBinder(StateNamespace namespace, StateContext<?> stateContext) {
      this.namespace = namespace;
      this.stateContext = stateContext;
    }

    @Override
    public <T2> ValueState<T2> bindValue(
        String id, StateSpec<ValueState<T2>> spec, Coder<T2> coder) {
      FlinkValueState<T2> valueState =
          new FlinkValueState<>(flinkStateBackend, id, namespace, coder, pipelineOptions);
      collectGlobalWindowStateDescriptor(
          valueState.flinkStateDescriptor,
          valueState.namespace.stringKey(),
          StringSerializer.INSTANCE);
      return valueState;
    }

    @Override
    public <T2> BagState<T2> bindBag(String id, StateSpec<BagState<T2>> spec, Coder<T2> elemCoder) {
      FlinkBagState<T2> bagState =
          new FlinkBagState<>(flinkStateBackend, id, namespace, elemCoder, pipelineOptions);
      collectGlobalWindowStateDescriptor(
          bagState.flinkStateDescriptor, bagState.namespace.stringKey(), StringSerializer.INSTANCE);
      return bagState;
    }

    @Override
    public <T2> SetState<T2> bindSet(String id, StateSpec<SetState<T2>> spec, Coder<T2> elemCoder) {
      FlinkSetState<T2> setState =
          new FlinkSetState<>(flinkStateBackend, id, namespace, elemCoder, pipelineOptions);
      collectGlobalWindowStateDescriptor(
          setState.flinkStateDescriptor, setState.namespace.stringKey(), StringSerializer.INSTANCE);
      return setState;
    }

    @Override
    public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
        String id,
        StateSpec<MapState<KeyT, ValueT>> spec,
        Coder<KeyT> mapKeyCoder,
        Coder<ValueT> mapValueCoder) {
      FlinkMapState<KeyT, ValueT> mapState =
          new FlinkMapState<>(
              flinkStateBackend, id, namespace, mapKeyCoder, mapValueCoder, pipelineOptions);
      collectGlobalWindowStateDescriptor(
          mapState.flinkStateDescriptor, mapState.namespace.stringKey(), StringSerializer.INSTANCE);
      return mapState;
    }

    @Override
    public <T> OrderedListState<T> bindOrderedList(
        String id, StateSpec<OrderedListState<T>> spec, Coder<T> elemCoder) {
      FlinkOrderedListState<T> flinkOrderedListState =
          new FlinkOrderedListState<>(flinkStateBackend, id, namespace, elemCoder, pipelineOptions);
      collectGlobalWindowStateDescriptor(
          flinkOrderedListState.flinkStateDescriptor,
          flinkOrderedListState.namespace.stringKey(),
          StringSerializer.INSTANCE);
      return flinkOrderedListState;
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(
        String id,
        StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
      FlinkCombiningState<Object, InputT, AccumT, OutputT> combiningState =
          new FlinkCombiningState<>(
              flinkStateBackend, id, combineFn, namespace, accumCoder, pipelineOptions);
      collectGlobalWindowStateDescriptor(
          combiningState.flinkStateDescriptor,
          combiningState.namespace.stringKey(),
          StringSerializer.INSTANCE);
      return combiningState;
    }

    @Override
    public <InputT, AccumT, OutputT>
        CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
            String id,
            StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
            Coder<AccumT> accumCoder,
            CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      FlinkCombiningStateWithContext<Object, InputT, AccumT, OutputT> combiningStateWithContext =
          new FlinkCombiningStateWithContext<>(
              flinkStateBackend,
              id,
              combineFn,
              namespace,
              accumCoder,
              CombineContextFactory.createFromStateContext(stateContext),
              pipelineOptions);
      collectGlobalWindowStateDescriptor(
          combiningStateWithContext.flinkStateDescriptor,
          combiningStateWithContext.namespace.stringKey(),
          StringSerializer.INSTANCE);
      return combiningStateWithContext;
    }

    @Override
    public WatermarkHoldState bindWatermark(
        String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
      collectGlobalWindowStateDescriptor(
          watermarkHoldStateDescriptor, VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);
      return new FlinkWatermarkHoldState(
          flinkStateBackend, watermarkHoldStateDescriptor, id, namespace, timestampCombiner);
    }

    /** Take note of state bound to the global window for cleanup in clearGlobalState(). */
    private <T> void collectGlobalWindowStateDescriptor(
        StateDescriptor<?, ?> descriptor, T namespaceKey, TypeSerializer<T> keySerializer) {
      if (globalWindowNamespace.equals(namespace) || StateNamespaces.global().equals(namespace)) {
        globalWindowStateDescriptors.add(
            StateAndNamespaceDescriptor.of(descriptor, namespaceKey, keySerializer));
      }
    }
  }

  private static class FlinkValueState<T> implements ValueState<T> {

    private final StateNamespace namespace;
    private final String stateId;
    private final ValueStateDescriptor<T> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;

    FlinkValueState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        String stateId,
        StateNamespace namespace,
        Coder<T> coder,
        SerializablePipelineOptions pipelineOptions) {

      this.namespace = namespace;
      this.stateId = stateId;
      this.flinkStateBackend = flinkStateBackend;

      flinkStateDescriptor =
          new ValueStateDescriptor<>(stateId, new CoderTypeSerializer<>(coder, pipelineOptions));
    }

    @Override
    public void write(T input) {
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .update(input);
      } catch (Exception e) {
        throw new RuntimeException("Error updating state.", e);
      }
    }

    @Override
    public ValueState<T> readLater() {
      return this;
    }

    @Override
    public T read() {
      try {
        return flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .value();
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public void clear() {
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkValueState<?> that = (FlinkValueState<?>) o;

      return namespace.equals(that.namespace) && stateId.equals(that.stateId);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + stateId.hashCode();
      return result;
    }
  }

  private static class FlinkOrderedListState<T> implements OrderedListState<T> {
    private final StateNamespace namespace;
    private final ListStateDescriptor<TimestampedValue<T>> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;

    FlinkOrderedListState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        String stateId,
        StateNamespace namespace,
        Coder<T> coder,
        SerializablePipelineOptions pipelineOptions) {
      this.namespace = namespace;
      this.flinkStateBackend = flinkStateBackend;
      this.flinkStateDescriptor =
          new ListStateDescriptor<>(
              stateId, new CoderTypeSerializer<>(TimestampedValueCoder.of(coder), pipelineOptions));
    }

    @Override
    public Iterable<TimestampedValue<T>> readRange(Instant minTimestamp, Instant limitTimestamp) {
      return readAsMap().subMap(minTimestamp, limitTimestamp).values();
    }

    @Override
    public void clearRange(Instant minTimestamp, Instant limitTimestamp) {
      SortedMap<Instant, TimestampedValue<T>> sortedMap = readAsMap();
      sortedMap.subMap(minTimestamp, limitTimestamp).clear();
      try {
        ListState<TimestampedValue<T>> partitionedState =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);
        partitionedState.update(Lists.newArrayList(sortedMap.values()));
      } catch (Exception e) {
        throw new RuntimeException("Error adding to bag state.", e);
      }
    }

    @Override
    public OrderedListState<T> readRangeLater(Instant minTimestamp, Instant limitTimestamp) {
      return this;
    }

    @Override
    public void add(TimestampedValue<T> value) {
      try {
        ListState<TimestampedValue<T>> partitionedState =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);
        partitionedState.add(value);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to bag state.", e);
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            Iterable<TimestampedValue<T>> result =
                flinkStateBackend
                    .getPartitionedState(
                        namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                    .get();
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
    @Nullable
    public Iterable<TimestampedValue<T>> read() {
      return readAsMap().values();
    }

    private SortedMap<Instant, TimestampedValue<T>> readAsMap() {
      Iterable<TimestampedValue<T>> listValues;
      try {
        ListState<TimestampedValue<T>> partitionedState =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);
        listValues = MoreObjects.firstNonNull(partitionedState.get(), Collections.emptyList());
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }

      SortedMap<Instant, TimestampedValue<T>> sortedMap = Maps.newTreeMap();
      for (TimestampedValue<T> value : listValues) {
        sortedMap.put(value.getTimestamp(), value);
      }
      return sortedMap;
    }

    @Override
    public GroupingState<TimestampedValue<T>, Iterable<TimestampedValue<T>>> readLater() {
      return this;
    }

    @Override
    public void clear() {
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }
  }

  private static class FlinkBagState<T> implements BagState<T> {

    private final StateNamespace namespace;
    private final String stateId;
    private final ListStateDescriptor<T> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;
    private final boolean storesVoidValues;

    FlinkBagState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        String stateId,
        StateNamespace namespace,
        Coder<T> coder,
        SerializablePipelineOptions pipelineOptions) {

      this.namespace = namespace;
      this.stateId = stateId;
      this.flinkStateBackend = flinkStateBackend;
      this.storesVoidValues = coder instanceof VoidCoder;
      this.flinkStateDescriptor =
          new ListStateDescriptor<>(stateId, new CoderTypeSerializer<>(coder, pipelineOptions));
    }

    @Override
    public void add(T input) {
      try {
        ListState<T> partitionedState =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);
        if (storesVoidValues) {
          Preconditions.checkState(input == null, "Expected to a null value but was: %s", input);
          // Flink does not allow storing null values
          // If we have null values, we use the structural null value
          input = (T) VoidCoder.of().structuralValue((Void) input);
        }
        partitionedState.add(input);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to bag state.", e);
      }
    }

    @Override
    public BagState<T> readLater() {
      return this;
    }

    @Override
    @Nonnull
    public Iterable<T> read() {
      try {
        ListState<T> partitionedState =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);
        Iterable<T> result = partitionedState.get();
        if (storesVoidValues) {
          return () -> {
            final Iterator underlying = result.iterator();
            return new Iterator<T>() {
              @Override
              public boolean hasNext() {
                return underlying.hasNext();
              }

              @Override
              public T next() {
                // Simply move the iterator forward but ignore the value.
                // The value can be the structural null value or NULL itself,
                // if this has been restored from serialized state.
                underlying.next();
                return null;
              }
            };
          };
        }
        return result != null ? ImmutableList.copyOf(result) : Collections.emptyList();
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
            Iterable<T> result =
                flinkStateBackend
                    .getPartitionedState(
                        namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                    .get();
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
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkBagState<?> that = (FlinkBagState<?>) o;

      return namespace.equals(that.namespace) && stateId.equals(that.stateId);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + stateId.hashCode();
      return result;
    }
  }

  private static class FlinkCombiningState<K, InputT, AccumT, OutputT>
      implements CombiningState<InputT, AccumT, OutputT> {

    private final StateNamespace namespace;
    private final String stateId;
    private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;
    private final ValueStateDescriptor<AccumT> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;

    FlinkCombiningState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        String stateId,
        Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
        StateNamespace namespace,
        Coder<AccumT> accumCoder,
        SerializablePipelineOptions pipelineOptions) {

      this.namespace = namespace;
      this.stateId = stateId;
      this.combineFn = combineFn;
      this.flinkStateBackend = flinkStateBackend;

      flinkStateDescriptor =
          new ValueStateDescriptor<>(
              stateId, new CoderTypeSerializer<>(accumCoder, pipelineOptions));
    }

    @Override
    public CombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public void add(InputT value) {
      try {
        org.apache.flink.api.common.state.ValueState<AccumT> state =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);

        AccumT current = state.value();
        if (current == null) {
          current = combineFn.createAccumulator();
        }
        current = combineFn.addInput(current, value);
        state.update(current);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state.", e);
      }
    }

    @Override
    public void addAccum(AccumT accum) {
      try {
        org.apache.flink.api.common.state.ValueState<AccumT> state =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);

        AccumT current = state.value();
        if (current == null) {
          state.update(accum);
        } else {
          current = combineFn.mergeAccumulators(Lists.newArrayList(current, accum));
          state.update(current);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state.", e);
      }
    }

    @Override
    public AccumT getAccum() {
      try {
        AccumT accum =
            flinkStateBackend
                .getPartitionedState(
                    namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                .value();
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
        org.apache.flink.api.common.state.ValueState<AccumT> state =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);

        AccumT accum = state.value();
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
            return flinkStateBackend
                    .getPartitionedState(
                        namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                    .value()
                == null;
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
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkCombiningState<?, ?, ?, ?> that = (FlinkCombiningState<?, ?, ?, ?>) o;

      return namespace.equals(that.namespace) && stateId.equals(that.stateId);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + stateId.hashCode();
      return result;
    }
  }

  private static class FlinkCombiningStateWithContext<K, InputT, AccumT, OutputT>
      implements CombiningState<InputT, AccumT, OutputT> {

    private final StateNamespace namespace;
    private final String stateId;
    private final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn;
    private final ValueStateDescriptor<AccumT> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;
    private final CombineWithContext.Context context;

    FlinkCombiningStateWithContext(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        String stateId,
        CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn,
        StateNamespace namespace,
        Coder<AccumT> accumCoder,
        CombineWithContext.Context context,
        SerializablePipelineOptions pipelineOptions) {

      this.namespace = namespace;
      this.stateId = stateId;
      this.combineFn = combineFn;
      this.flinkStateBackend = flinkStateBackend;
      this.context = context;

      flinkStateDescriptor =
          new ValueStateDescriptor<>(
              stateId, new CoderTypeSerializer<>(accumCoder, pipelineOptions));
    }

    @Override
    public CombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public void add(InputT value) {
      try {
        org.apache.flink.api.common.state.ValueState<AccumT> state =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);

        AccumT current = state.value();
        if (current == null) {
          current = combineFn.createAccumulator(context);
        }
        current = combineFn.addInput(current, value, context);
        state.update(current);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state.", e);
      }
    }

    @Override
    public void addAccum(AccumT accum) {
      try {
        org.apache.flink.api.common.state.ValueState<AccumT> state =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);

        AccumT current = state.value();
        if (current == null) {
          state.update(accum);
        } else {
          current = combineFn.mergeAccumulators(Lists.newArrayList(current, accum), context);
          state.update(current);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state.", e);
      }
    }

    @Override
    public AccumT getAccum() {
      try {
        AccumT accum =
            flinkStateBackend
                .getPartitionedState(
                    namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                .value();
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
        org.apache.flink.api.common.state.ValueState<AccumT> state =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);

        AccumT accum = state.value();
        if (accum != null) {
          return combineFn.extractOutput(accum, context);
        } else {
          return combineFn.extractOutput(combineFn.createAccumulator(context), context);
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
            return flinkStateBackend
                    .getPartitionedState(
                        namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                    .value()
                == null;
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
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
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

      return namespace.equals(that.namespace) && stateId.equals(that.stateId);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + stateId.hashCode();
      return result;
    }
  }

  private class FlinkWatermarkHoldState implements WatermarkHoldState {

    private final TimestampCombiner timestampCombiner;
    private final String namespaceString;
    private org.apache.flink.api.common.state.MapState<String, Instant> watermarkHoldsState;

    public FlinkWatermarkHoldState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        MapStateDescriptor<String, Instant> watermarkHoldStateDescriptor,
        String stateId,
        StateNamespace namespace,
        TimestampCombiner timestampCombiner) {
      this.timestampCombiner = timestampCombiner;
      // Combines StateNamespace and stateId to generate a unique namespace for
      // watermarkHoldsState. We do not want to use Flink's namespacing to be
      // able to recover watermark holds efficiently during recovery.
      this.namespaceString = namespace.stringKey() + stateId;
      try {
        this.watermarkHoldsState =
            flinkStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                watermarkHoldStateDescriptor);
      } catch (Exception e) {
        throw new RuntimeException("Could not access state for watermark partition view");
      }
    }

    @Override
    public TimestampCombiner getTimestampCombiner() {
      return timestampCombiner;
    }

    @Override
    public WatermarkHoldState readLater() {
      return this;
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            return watermarkHoldsState.get(namespaceString) == null;
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
    public void add(Instant value) {
      try {
        Instant current = watermarkHoldsState.get(namespaceString);
        if (current == null) {
          addWatermarkHoldUsage(value);
          watermarkHoldsState.put(namespaceString, value);
        } else {
          Instant combined = timestampCombiner.combine(current, value);
          if (combined.getMillis() != current.getMillis()) {
            removeWatermarkHoldUsage(current);
            addWatermarkHoldUsage(combined);
            watermarkHoldsState.put(namespaceString, combined);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Error updating state.", e);
      }
    }

    @Override
    public Instant read() {
      try {
        return watermarkHoldsState.get(namespaceString);
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public void clear() {
      Instant current = read();
      if (current != null) {
        removeWatermarkHoldUsage(current);
      }
      try {
        watermarkHoldsState.remove(namespaceString);
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkWatermarkHoldState that = (FlinkWatermarkHoldState) o;

      if (!timestampCombiner.equals(that.timestampCombiner)) {
        return false;
      }
      return namespaceString.equals(that.namespaceString);
    }

    @Override
    public int hashCode() {
      int result = namespaceString.hashCode();
      result = 31 * result + timestampCombiner.hashCode();
      return result;
    }
  }

  private static class FlinkMapState<KeyT, ValueT> implements MapState<KeyT, ValueT> {

    private final StateNamespace namespace;
    private final String stateId;
    private final MapStateDescriptor<KeyT, ValueT> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;

    FlinkMapState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        String stateId,
        StateNamespace namespace,
        Coder<KeyT> mapKeyCoder,
        Coder<ValueT> mapValueCoder,
        SerializablePipelineOptions pipelineOptions) {
      this.namespace = namespace;
      this.stateId = stateId;
      this.flinkStateBackend = flinkStateBackend;
      this.flinkStateDescriptor =
          new MapStateDescriptor<>(
              stateId,
              new CoderTypeSerializer<>(mapKeyCoder, pipelineOptions),
              new CoderTypeSerializer<>(mapValueCoder, pipelineOptions));
    }

    @Override
    public ReadableState<ValueT> get(final KeyT input) {
      return getOrDefault(input, null);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<ValueT> getOrDefault(
        KeyT key, @Nullable ValueT defaultValue) {
      return new ReadableState<ValueT>() {
        @Override
        public @Nullable ValueT read() {
          try {
            ValueT value =
                flinkStateBackend
                    .getPartitionedState(
                        namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                    .get(key);
            return (value != null) ? value : defaultValue;
          } catch (Exception e) {
            throw new RuntimeException("Error get from state.", e);
          }
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized ReadableState<ValueT> readLater() {
          return this;
        }
      };
    }

    @Override
    public void put(KeyT key, ValueT value) {
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .put(key, value);
      } catch (Exception e) {
        throw new RuntimeException("Error put kv to state.", e);
      }
    }

    @Override
    public ReadableState<ValueT> computeIfAbsent(
        final KeyT key, Function<? super KeyT, ? extends ValueT> mappingFunction) {
      try {
        ValueT current =
            flinkStateBackend
                .getPartitionedState(
                    namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                .get(key);

        if (current == null) {
          flinkStateBackend
              .getPartitionedState(
                  namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
              .put(key, mappingFunction.apply(key));
        }
        return ReadableStates.immediate(current);
      } catch (Exception e) {
        throw new RuntimeException("Error put kv to state.", e);
      }
    }

    @Override
    public void remove(KeyT key) {
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .remove(key);
      } catch (Exception e) {
        throw new RuntimeException("Error remove map state key.", e);
      }
    }

    @Override
    public ReadableState<Iterable<KeyT>> keys() {
      return new ReadableState<Iterable<KeyT>>() {
        @Override
        public Iterable<KeyT> read() {
          try {
            Iterable<KeyT> result =
                flinkStateBackend
                    .getPartitionedState(
                        namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                    .keys();
            return result != null ? ImmutableList.copyOf(result) : Collections.emptyList();
          } catch (Exception e) {
            throw new RuntimeException("Error get map state keys.", e);
          }
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
          try {
            Iterable<ValueT> result =
                flinkStateBackend
                    .getPartitionedState(
                        namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                    .values();
            return result != null ? ImmutableList.copyOf(result) : Collections.emptyList();
          } catch (Exception e) {
            throw new RuntimeException("Error get map state values.", e);
          }
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
          try {
            Iterable<Map.Entry<KeyT, ValueT>> result =
                flinkStateBackend
                    .getPartitionedState(
                        namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                    .entries();
            return result != null ? ImmutableList.copyOf(result) : Collections.emptyList();
          } catch (Exception e) {
            throw new RuntimeException("Error get map state entries.", e);
          }
        }

        @Override
        public ReadableState<Iterable<Map.Entry<KeyT, ValueT>>> readLater() {
          return this;
        }
      };
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<
            @UnknownKeyFor @NonNull @Initialized Boolean>
        isEmpty() {
      ReadableState<Iterable<KeyT>> keys = this.keys();
      return new ReadableState<Boolean>() {
        @Override
        public @Nullable Boolean read() {
          return Iterables.isEmpty(keys.read());
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized ReadableState<Boolean> readLater() {
          keys.readLater();
          return this;
        }
      };
    }

    @Override
    public void clear() {
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkMapState<?, ?> that = (FlinkMapState<?, ?>) o;

      return namespace.equals(that.namespace) && stateId.equals(that.stateId);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + stateId.hashCode();
      return result;
    }
  }

  private static class FlinkSetState<T> implements SetState<T> {

    private final StateNamespace namespace;
    private final String stateId;
    private final MapStateDescriptor<T, Boolean> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;

    FlinkSetState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        String stateId,
        StateNamespace namespace,
        Coder<T> coder,
        SerializablePipelineOptions pipelineOptions) {
      this.namespace = namespace;
      this.stateId = stateId;
      this.flinkStateBackend = flinkStateBackend;
      this.flinkStateDescriptor =
          new MapStateDescriptor<>(
              stateId, new CoderTypeSerializer<>(coder, pipelineOptions), new BooleanSerializer());
    }

    @Override
    public ReadableState<Boolean> contains(final T t) {
      try {
        Boolean result =
            flinkStateBackend
                .getPartitionedState(
                    namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                .get(t);
        return ReadableStates.immediate(result != null && result);
      } catch (Exception e) {
        throw new RuntimeException("Error contains value from state.", e);
      }
    }

    @Override
    public ReadableState<Boolean> addIfAbsent(final T t) {
      try {
        org.apache.flink.api.common.state.MapState<T, Boolean> state =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);
        boolean alreadyContained = state.contains(t);
        if (!alreadyContained) {
          state.put(t, true);
        }
        return ReadableStates.immediate(!alreadyContained);
      } catch (Exception e) {
        throw new RuntimeException("Error addIfAbsent value to state.", e);
      }
    }

    @Override
    public void remove(T t) {
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .remove(t);
      } catch (Exception e) {
        throw new RuntimeException("Error remove value to state.", e);
      }
    }

    @Override
    public SetState<T> readLater() {
      return this;
    }

    @Override
    public void add(T value) {
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .put(value, true);
      } catch (Exception e) {
        throw new RuntimeException("Error add value to state.", e);
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            Iterable<T> result =
                flinkStateBackend
                    .getPartitionedState(
                        namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                    .keys();
            return result == null || Iterables.isEmpty(result);
          } catch (Exception e) {
            throw new RuntimeException("Error isEmpty from state.", e);
          }
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public Iterable<T> read() {
      try {
        Iterable<T> result =
            flinkStateBackend
                .getPartitionedState(
                    namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
                .keys();
        return result != null ? ImmutableList.copyOf(result) : Collections.emptyList();
      } catch (Exception e) {
        throw new RuntimeException("Error read from state.", e);
      }
    }

    @Override
    public void clear() {
      try {
        flinkStateBackend
            .getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor)
            .clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkSetState<?> that = (FlinkSetState<?>) o;

      return namespace.equals(that.namespace) && stateId.equals(that.stateId);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + stateId.hashCode();
      return result;
    }
  }

  public void addWatermarkHoldUsage(Instant watermarkHold) {
    watermarkHolds.add(watermarkHold.getMillis());
  }

  public void removeWatermarkHoldUsage(Instant watermarkHold) {
    watermarkHolds.remove(watermarkHold.getMillis());
  }

  /** Restores a view of the watermark holds of all keys of this partition. */
  private void restoreWatermarkHoldsView() throws Exception {
    org.apache.flink.api.common.state.MapState<String, Instant> mapState =
        flinkStateBackend.getPartitionedState(
            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, watermarkHoldStateDescriptor);
    try (Stream<ByteBuffer> keys =
        flinkStateBackend.getKeys(watermarkHoldStateDescriptor.getName(), VoidNamespace.INSTANCE)) {
      Iterator<ByteBuffer> iterator = keys.iterator();
      while (iterator.hasNext()) {
        flinkStateBackend.setCurrentKey(iterator.next());
        mapState.values().forEach(this::addWatermarkHoldUsage);
      }
    }
  }

  /** Eagerly create user state to work around https://jira.apache.org/jira/browse/FLINK-12653. */
  public static class EarlyBinder implements StateBinder {

    private final KeyedStateBackend keyedStateBackend;
    private final SerializablePipelineOptions pipelineOptions;

    public EarlyBinder(
        KeyedStateBackend keyedStateBackend, SerializablePipelineOptions pipelineOptions) {
      this.keyedStateBackend = keyedStateBackend;
      this.pipelineOptions = pipelineOptions;
    }

    @Override
    public <T> ValueState<T> bindValue(String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
      try {
        keyedStateBackend.getOrCreateKeyedState(
            StringSerializer.INSTANCE,
            new ValueStateDescriptor<>(id, new CoderTypeSerializer<>(coder, pipelineOptions)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      return null;
    }

    @Override
    public <T> BagState<T> bindBag(String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
      try {
        keyedStateBackend.getOrCreateKeyedState(
            StringSerializer.INSTANCE,
            new ListStateDescriptor<>(id, new CoderTypeSerializer<>(elemCoder, pipelineOptions)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      return null;
    }

    @Override
    public <T> SetState<T> bindSet(String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
      try {
        keyedStateBackend.getOrCreateKeyedState(
            StringSerializer.INSTANCE,
            new MapStateDescriptor<>(
                id,
                new CoderTypeSerializer<>(elemCoder, pipelineOptions),
                VoidSerializer.INSTANCE));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    }

    @Override
    public <KeyT, ValueT> org.apache.beam.sdk.state.MapState<KeyT, ValueT> bindMap(
        String id,
        StateSpec<org.apache.beam.sdk.state.MapState<KeyT, ValueT>> spec,
        Coder<KeyT> mapKeyCoder,
        Coder<ValueT> mapValueCoder) {
      try {
        keyedStateBackend.getOrCreateKeyedState(
            StringSerializer.INSTANCE,
            new MapStateDescriptor<>(
                id,
                new CoderTypeSerializer<>(mapKeyCoder, pipelineOptions),
                new CoderTypeSerializer<>(mapValueCoder, pipelineOptions)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    }

    @Override
    public <T> OrderedListState<T> bindOrderedList(
        String id, StateSpec<OrderedListState<T>> spec, Coder<T> elemCoder) {
      try {
        keyedStateBackend.getOrCreateKeyedState(
            StringSerializer.INSTANCE,
            new ListStateDescriptor<>(
                id,
                new CoderTypeSerializer<>(TimestampedValueCoder.of(elemCoder), pipelineOptions)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      return null;
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(
        String id,
        StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
      try {
        keyedStateBackend.getOrCreateKeyedState(
            StringSerializer.INSTANCE,
            new ValueStateDescriptor<>(id, new CoderTypeSerializer<>(accumCoder, pipelineOptions)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    }

    @Override
    public <InputT, AccumT, OutputT>
        CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
            String id,
            StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
            Coder<AccumT> accumCoder,
            CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      try {
        keyedStateBackend.getOrCreateKeyedState(
            StringSerializer.INSTANCE,
            new ValueStateDescriptor<>(id, new CoderTypeSerializer<>(accumCoder, pipelineOptions)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    }

    @Override
    public WatermarkHoldState bindWatermark(
        String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
      try {
        keyedStateBackend.getOrCreateKeyedState(
            VoidNamespaceSerializer.INSTANCE,
            new MapStateDescriptor<>(
                "watermark-holds",
                StringSerializer.INSTANCE,
                new CoderTypeSerializer<>(InstantCoder.of(), pipelineOptions)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    }
  }
}
