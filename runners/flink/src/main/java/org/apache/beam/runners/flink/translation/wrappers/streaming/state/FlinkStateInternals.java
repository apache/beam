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

import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.CombineContextFactory;
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
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.joda.time.Instant;

/**
 * {@link StateInternals} that uses a Flink {@link KeyedStateBackend} to manage state.
 *
 * <p>Note: In the Flink streaming runner the key is always encoded
 * using an {@link Coder} and stored in a {@link ByteBuffer}.
 */
public class FlinkStateInternals<K> implements StateInternals {

  private final KeyedStateBackend<ByteBuffer> flinkStateBackend;
  private Coder<K> keyCoder;

  // on recovery, these will no be properly set because we don't
  // know which watermark hold states there are in the Flink State Backend
  private final Map<String, Instant> watermarkHolds = new HashMap<>();

  public FlinkStateInternals(KeyedStateBackend<ByteBuffer> flinkStateBackend, Coder<K> keyCoder) {
    this.flinkStateBackend = flinkStateBackend;
    this.keyCoder = keyCoder;
  }

  /**
   * Returns the minimum over all watermark holds.
   */
  public Instant watermarkHold() {
    long min = Long.MAX_VALUE;
    for (Instant hold: watermarkHolds.values()) {
      min = Math.min(min, hold.getMillis());
    }
    return new Instant(min);
  }

  @Override
  public K getKey() {
    ByteBuffer keyBytes = flinkStateBackend.getCurrentKey();
    try {
      return CoderUtils.decodeFromByteArray(keyCoder, keyBytes.array());
    } catch (CoderException e) {
      throw new RuntimeException("Error decoding key.", e);
    }
  }

  @Override
  public <T extends State> T state(
      final StateNamespace namespace,
      StateTag<T> address) {

    return state(namespace, address, StateContexts.nullContext());
  }

  @Override
  public <T extends State> T state(
      final StateNamespace namespace,
      StateTag<T> address,
      final StateContext<?> context) {

    return address.bind(
        new StateTag.StateBinder() {

          @Override
          public <T> ValueState<T> bindValue(
              StateTag<ValueState<T>> address, Coder<T> coder) {

            return new FlinkValueState<>(flinkStateBackend, address, namespace, coder);
          }

          @Override
          public <T> BagState<T> bindBag(
              StateTag<BagState<T>> address, Coder<T> elemCoder) {

            return new FlinkBagState<>(flinkStateBackend, address, namespace, elemCoder);
          }

          @Override
          public <T> SetState<T> bindSet(
              StateTag<SetState<T>> address, Coder<T> elemCoder) {
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
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {

            return new FlinkCombiningState<>(
                flinkStateBackend, address, combineFn, namespace, accumCoder);
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
            return new FlinkCombiningStateWithContext<>(
                flinkStateBackend,
                address,
                combineFn,
                namespace,
                accumCoder,
                FlinkStateInternals.this,
                CombineContextFactory.createFromStateContext(context));
          }

          @Override
          public WatermarkHoldState bindWatermark(
              StateTag<WatermarkHoldState> address,
              TimestampCombiner timestampCombiner) {

            return new FlinkWatermarkHoldState<>(
                flinkStateBackend, FlinkStateInternals.this, address, namespace, timestampCombiner);
          }
        });
  }

  private static class FlinkValueState<K, T> implements ValueState<T> {

    private final StateNamespace namespace;
    private final StateTag<ValueState<T>> address;
    private final ValueStateDescriptor<T> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;

    FlinkValueState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        StateTag<ValueState<T>> address,
        StateNamespace namespace,
        Coder<T> coder) {

      this.namespace = namespace;
      this.address = address;
      this.flinkStateBackend = flinkStateBackend;

      CoderTypeInformation<T> typeInfo = new CoderTypeInformation<>(coder);

      flinkStateDescriptor = new ValueStateDescriptor<>(address.getId(), typeInfo, null);
    }

    @Override
    public void write(T input) {
      try {
        flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).update(input);
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
        return flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).value();
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public void clear() {
      try {
        flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkValueState<?, ?> that = (FlinkValueState<?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);

    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private static class FlinkBagState<K, T> implements BagState<T> {

    private final StateNamespace namespace;
    private final StateTag<BagState<T>> address;
    private final ListStateDescriptor<T> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;

    FlinkBagState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        StateTag<BagState<T>> address,
        StateNamespace namespace,
        Coder<T> coder) {

      this.namespace = namespace;
      this.address = address;
      this.flinkStateBackend = flinkStateBackend;

      CoderTypeInformation<T> typeInfo = new CoderTypeInformation<>(coder);

      flinkStateDescriptor = new ListStateDescriptor<>(address.getId(), typeInfo);
    }

    @Override
    public void add(T input) {
      try {
        flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).add(input);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to bag state.", e);
      }
    }

    @Override
    public BagState<T> readLater() {
      return this;
    }

    @Override
    public Iterable<T> read() {
      try {
        Iterable<T> result = flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).get();

        return result != null ? result : Collections.<T>emptyList();
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
            Iterable<T> result = flinkStateBackend.getPartitionedState(
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor).get();
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
        flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkBagState<?, ?> that = (FlinkBagState<?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);

    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private static class FlinkCombiningState<K, InputT, AccumT, OutputT>
      implements CombiningState<InputT, AccumT, OutputT> {

    private final StateNamespace namespace;
    private final StateTag<CombiningState<InputT, AccumT, OutputT>> address;
    private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;
    private final ValueStateDescriptor<AccumT> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;

    FlinkCombiningState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
        StateNamespace namespace,
        Coder<AccumT> accumCoder) {

      this.namespace = namespace;
      this.address = address;
      this.combineFn = combineFn;
      this.flinkStateBackend = flinkStateBackend;

      CoderTypeInformation<AccumT> typeInfo = new CoderTypeInformation<>(accumCoder);

      flinkStateDescriptor = new ValueStateDescriptor<>(address.getId(), typeInfo, null);
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
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor);

        AccumT current = state.value();
        if (current == null) {
          current = combineFn.createAccumulator();
        }
        current = combineFn.addInput(current, value);
        state.update(current);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state." , e);
      }
    }

    @Override
    public void addAccum(AccumT accum) {
      try {
        org.apache.flink.api.common.state.ValueState<AccumT> state =
            flinkStateBackend.getPartitionedState(
              namespace.stringKey(),
              StringSerializer.INSTANCE,
              flinkStateDescriptor);

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
        return flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).value();
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
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor);

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
            return flinkStateBackend.getPartitionedState(
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor).value() == null;
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
        flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkCombiningState<?, ?, ?, ?> that =
          (FlinkCombiningState<?, ?, ?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);

    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private static class FlinkKeyedCombiningState<K, InputT, AccumT, OutputT>
      implements CombiningState<InputT, AccumT, OutputT> {

    private final StateNamespace namespace;
    private final StateTag<CombiningState<InputT, AccumT, OutputT>> address;
    private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;
    private final ValueStateDescriptor<AccumT> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;
    private final FlinkStateInternals<K> flinkStateInternals;

    FlinkKeyedCombiningState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
        StateNamespace namespace,
        Coder<AccumT> accumCoder,
        FlinkStateInternals<K> flinkStateInternals) {

      this.namespace = namespace;
      this.address = address;
      this.combineFn = combineFn;
      this.flinkStateBackend = flinkStateBackend;
      this.flinkStateInternals = flinkStateInternals;

      CoderTypeInformation<AccumT> typeInfo = new CoderTypeInformation<>(accumCoder);

      flinkStateDescriptor = new ValueStateDescriptor<>(address.getId(), typeInfo, null);
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
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor);

        AccumT current = state.value();
        if (current == null) {
          current = combineFn.createAccumulator();
        }
        current = combineFn.addInput(current, value);
        state.update(current);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state." , e);
      }
    }

    @Override
    public void addAccum(AccumT accum) {
      try {
        org.apache.flink.api.common.state.ValueState<AccumT> state =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor);

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
        return flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).value();
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
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor);

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
            return flinkStateBackend.getPartitionedState(
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor).value() == null;
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
        flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkKeyedCombiningState<?, ?, ?, ?> that =
          (FlinkKeyedCombiningState<?, ?, ?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);

    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private static class FlinkCombiningStateWithContext<K, InputT, AccumT, OutputT>
      implements CombiningState<InputT, AccumT, OutputT> {

    private final StateNamespace namespace;
    private final StateTag<CombiningState<InputT, AccumT, OutputT>> address;
    private final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn;
    private final ValueStateDescriptor<AccumT> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;
    private final FlinkStateInternals<K> flinkStateInternals;
    private final CombineWithContext.Context context;

    FlinkCombiningStateWithContext(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn,
        StateNamespace namespace,
        Coder<AccumT> accumCoder,
        FlinkStateInternals<K> flinkStateInternals,
        CombineWithContext.Context context) {

      this.namespace = namespace;
      this.address = address;
      this.combineFn = combineFn;
      this.flinkStateBackend = flinkStateBackend;
      this.flinkStateInternals = flinkStateInternals;
      this.context = context;

      CoderTypeInformation<AccumT> typeInfo = new CoderTypeInformation<>(accumCoder);

      flinkStateDescriptor = new ValueStateDescriptor<>(address.getId(), typeInfo, null);
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
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor);

        AccumT current = state.value();
        if (current == null) {
          current = combineFn.createAccumulator(context);
        }
        current = combineFn.addInput(current, value, context);
        state.update(current);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state." , e);
      }
    }

    @Override
    public void addAccum(AccumT accum) {
      try {
        org.apache.flink.api.common.state.ValueState<AccumT> state =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor);

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
        return flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).value();
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
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor);

        AccumT accum = state.value();
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
            return flinkStateBackend.getPartitionedState(
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor).value() == null;
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
        flinkStateBackend.getPartitionedState(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor).clear();
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }

    @Override
    public boolean equals(Object o) {
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

  private static class FlinkWatermarkHoldState<K, W extends BoundedWindow>
      implements WatermarkHoldState {
    private final StateTag<WatermarkHoldState> address;
    private final TimestampCombiner timestampCombiner;
    private final StateNamespace namespace;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;
    private final FlinkStateInternals<K> flinkStateInternals;
    private final ValueStateDescriptor<Instant> flinkStateDescriptor;

    public FlinkWatermarkHoldState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        FlinkStateInternals<K> flinkStateInternals,
        StateTag<WatermarkHoldState> address,
        StateNamespace namespace,
        TimestampCombiner timestampCombiner) {
      this.address = address;
      this.timestampCombiner = timestampCombiner;
      this.namespace = namespace;
      this.flinkStateBackend = flinkStateBackend;
      this.flinkStateInternals = flinkStateInternals;

      CoderTypeInformation<Instant> typeInfo = new CoderTypeInformation<>(InstantCoder.of());
      flinkStateDescriptor = new ValueStateDescriptor<>(address.getId(), typeInfo, null);
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
            return flinkStateBackend.getPartitionedState(
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor).value() == null;
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
        org.apache.flink.api.common.state.ValueState<Instant> state =
            flinkStateBackend.getPartitionedState(
              namespace.stringKey(),
              StringSerializer.INSTANCE,
              flinkStateDescriptor);

        Instant current = state.value();
        if (current == null) {
          state.update(value);
          flinkStateInternals.watermarkHolds.put(namespace.stringKey(), value);
        } else {
          Instant combined = timestampCombiner.combine(current, value);
          state.update(combined);
          flinkStateInternals.watermarkHolds.put(namespace.stringKey(), combined);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error updating state.", e);
      }
    }

    @Override
    public Instant read() {
      try {
        org.apache.flink.api.common.state.ValueState<Instant> state =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor);
        return state.value();
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public void clear() {
      flinkStateInternals.watermarkHolds.remove(namespace.stringKey());
      try {
        org.apache.flink.api.common.state.ValueState<Instant> state =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(),
                StringSerializer.INSTANCE,
                flinkStateDescriptor);
        state.clear();
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkWatermarkHoldState<?, ?> that = (FlinkWatermarkHoldState<?, ?>) o;

      if (!address.equals(that.address)) {
        return false;
      }
      if (!timestampCombiner.equals(that.timestampCombiner)) {
        return false;
      }
      return namespace.equals(that.namespace);

    }

    @Override
    public int hashCode() {
      int result = address.hashCode();
      result = 31 * result + timestampCombiner.hashCode();
      result = 31 * result + namespace.hashCode();
      return result;
    }
  }
}
