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

import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterators;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.OperatorStateBackend;

/**
 * {@link StateInternals} that uses a Flink {@link OperatorStateBackend} to manage the
 * split-distribute state.
 *
 * <p>Elements in ListState will be redistributed in round robin fashion to operators when
 * restarting with a different parallelism.
 *
 * <p>Note: Ignore index of key and namespace. Just implement BagState.
 */
public class FlinkSplitStateInternals<K> implements StateInternals {

  private final OperatorStateBackend stateBackend;

  public FlinkSplitStateInternals(OperatorStateBackend stateBackend) {
    this.stateBackend = stateBackend;
  }

  @Override
  @Nullable
  public K getKey() {
    throw new UnsupportedOperationException("This should not be called");
  }

  @Override
  public <T extends State> T state(
      final StateNamespace namespace, StateTag<T> address, final StateContext<?> context) {

    return address.bind(
        new StateTag.StateBinder() {

          @Override
          public <T2> ValueState<T2> bindValue(StateTag<ValueState<T2>> address, Coder<T2> coder) {
            throw new UnsupportedOperationException(
                String.format("%s is not supported", ValueState.class.getSimpleName()));
          }

          @Override
          public <T2> BagState<T2> bindBag(StateTag<BagState<T2>> address, Coder<T2> elemCoder) {

            return new FlinkSplitBagState<>(stateBackend, address, namespace, elemCoder);
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
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
            throw new UnsupportedOperationException("bindCombiningValue is not supported.");
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
            throw new UnsupportedOperationException(
                "bindCombiningValueWithContext is not supported.");
          }

          @Override
          public WatermarkHoldState bindWatermark(
              StateTag<WatermarkHoldState> address, TimestampCombiner timestampCombiner) {
            throw new UnsupportedOperationException(
                String.format("%s is not supported", CombiningState.class.getSimpleName()));
          }
        });
  }

  private static class FlinkSplitBagState<K, T> implements BagState<T> {

    private final ListStateDescriptor<T> descriptor;
    private OperatorStateBackend flinkStateBackend;
    private final StateNamespace namespace;
    private final StateTag<BagState<T>> address;

    FlinkSplitBagState(
        OperatorStateBackend flinkStateBackend,
        StateTag<BagState<T>> address,
        StateNamespace namespace,
        Coder<T> coder) {
      this.flinkStateBackend = flinkStateBackend;
      this.namespace = namespace;
      this.address = address;

      CoderTypeInformation<T> typeInfo = new CoderTypeInformation<>(coder);

      descriptor =
          new ListStateDescriptor<>(
              address.getId(), typeInfo.createSerializer(new ExecutionConfig()));
    }

    @Override
    public void add(T input) {
      try {
        flinkStateBackend.getListState(descriptor).add(input);
      } catch (Exception e) {
        throw new RuntimeException("Error updating state.", e);
      }
    }

    @Override
    public BagState<T> readLater() {
      return this;
    }

    @Override
    public Iterable<T> read() {
      try {
        Iterable<T> result = flinkStateBackend.getListState(descriptor).get();
        return result != null ? result : Collections.emptyList();
      } catch (Exception e) {
        throw new RuntimeException("Error updating state.", e);
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            Iterable<T> result = flinkStateBackend.getListState(descriptor).get();
            // PartitionableListState.get() return empty collection When there is no element,
            // KeyedListState different. (return null)
            return result == null || Iterators.size(result.iterator()) == 0;
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
        flinkStateBackend.getListState(descriptor).clear();
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

      FlinkSplitBagState<?, ?> that = (FlinkSplitBagState<?, ?>) o;

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
