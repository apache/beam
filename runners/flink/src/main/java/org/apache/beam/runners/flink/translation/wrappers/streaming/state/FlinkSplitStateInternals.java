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

import com.google.common.collect.Iterators;
import java.util.Collections;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.OperatorStateBackend;

/**
 * {@link StateInternals} that uses a Flink {@link OperatorStateBackend}
 * to manage the split-distribute state.
 *
 * <p>Elements in ListState will be redistributed in round robin fashion
 * to operators when restarting with a different parallelism.
 *
 *  <p>Note:
 *  Ignore index of key and namespace.
 *  Just implement BagState.
 */
public class FlinkSplitStateInternals<K> implements StateInternals<K> {

  private final OperatorStateBackend stateBackend;

  public FlinkSplitStateInternals(OperatorStateBackend stateBackend) {
    this.stateBackend = stateBackend;
  }

  @Override
  public K getKey() {
    return null;
  }

  @Override
  public <T extends State> T state(
      final StateNamespace namespace,
      StateTag<? super K, T> address) {

    return state(namespace, address, StateContexts.nullContext());
  }

  @Override
  public <T extends State> T state(
      final StateNamespace namespace,
      StateTag<? super K, T> address,
      final StateContext<?> context) {

    return address.bind(new StateTag.StateBinder<K>() {

      @Override
      public <T> ValueState<T> bindValue(
          StateTag<? super K, ValueState<T>> address,
          Coder<T> coder) {
        throw new UnsupportedOperationException(
            String.format("%s is not supported", ValueState.class.getSimpleName()));
      }

      @Override
      public <T> BagState<T> bindBag(
          StateTag<? super K, BagState<T>> address,
          Coder<T> elemCoder) {

        return new FlinkSplitBagState<>(stateBackend, address, namespace, elemCoder);
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
      public <InputT, AccumT, OutputT>
      CombiningState<InputT, AccumT, OutputT>
      bindCombiningValue(
          StateTag<? super K, CombiningState<InputT, AccumT, OutputT>> address,
          Coder<AccumT> accumCoder,
          Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
        throw new UnsupportedOperationException("bindCombiningValue is not supported.");
      }

      @Override
      public <InputT, AccumT, OutputT>
      CombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValue(
          StateTag<? super K, CombiningState<InputT, AccumT, OutputT>> address,
          Coder<AccumT> accumCoder,
          final Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
        throw new UnsupportedOperationException("bindKeyedCombiningValue is not supported.");

      }

      @Override
      public <InputT, AccumT, OutputT>
      CombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValueWithContext(
          StateTag<? super K, CombiningState<InputT, AccumT, OutputT>> address,
          Coder<AccumT> accumCoder,
          CombineWithContext.KeyedCombineFnWithContext<
              ? super K, InputT, AccumT, OutputT> combineFn) {
        throw new UnsupportedOperationException(
            "bindKeyedCombiningValueWithContext is not supported.");
      }

      @Override
      public <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
          StateTag<? super K, WatermarkHoldState<W>> address,
          OutputTimeFn<? super W> outputTimeFn) {
        throw new UnsupportedOperationException(
            String.format("%s is not supported", CombiningState.class.getSimpleName()));
      }
    });
  }

  private static class FlinkSplitBagState<K, T> implements BagState<T> {

    private final ListStateDescriptor<T> descriptor;
    private OperatorStateBackend flinkStateBackend;
    private final StateNamespace namespace;
    private final StateTag<? super K, BagState<T>> address;

    FlinkSplitBagState(
        OperatorStateBackend flinkStateBackend,
        StateTag<? super K, BagState<T>> address,
        StateNamespace namespace,
        Coder<T> coder) {
      this.flinkStateBackend = flinkStateBackend;
      this.namespace = namespace;
      this.address = address;

      CoderTypeInformation<T> typeInfo =
          new CoderTypeInformation<>(coder);

      descriptor = new ListStateDescriptor<>(address.getId(),
          typeInfo.createSerializer(new ExecutionConfig()));
    }

    @Override
    public void add(T input) {
      try {
        flinkStateBackend.getOperatorState(descriptor).add(input);
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
        Iterable<T> result = flinkStateBackend.getOperatorState(descriptor).get();
        return result != null ? result : Collections.<T>emptyList();
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
            Iterable<T> result = flinkStateBackend.getOperatorState(descriptor).get();
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
        flinkStateBackend.getOperatorState(descriptor).clear();
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
