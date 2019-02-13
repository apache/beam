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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.custom.AllKeyStateFunction;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.custom.AllKeyStateFunction.StateConsumer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.util.Preconditions;

/**
 * {@link StateInternals} which uses Flink's KeyedStateBackend to store BagState which retrieves and
 * clears state for _all_ of its keys when using the read method.
 */
public class FlinkKeyGroupStateInternals<K> implements StateInternals {

  private final Coder<K> keyCoder;
  private KeyedStateBackend keyedStateBackend;

  public FlinkKeyGroupStateInternals(Coder<K> keyCoder, KeyedStateBackend keyedStateBackend) {
    this.keyCoder = Preconditions.checkNotNull(keyCoder, "Coder for key must be provided.");
    this.keyedStateBackend =
        Preconditions.checkNotNull(
            keyedStateBackend, "KeyedStateBackend must not be null. Missing keyBy call?");
  }

  @Override
  public K getKey() {
    ByteBuffer keyBytes = (ByteBuffer) keyedStateBackend.getCurrentKey();
    try {
      byte[] bytes = new byte[keyBytes.remaining()];
      keyBytes.get(bytes);
      keyBytes.position(keyBytes.position() - bytes.length);
      return CoderUtils.decodeFromByteArray(keyCoder, bytes);
    } catch (CoderException e) {
      throw new RuntimeException("Error decoding key.", e);
    }
  }

  @Override
  public <T extends State> T state(
      final StateNamespace namespace, StateTag<T> address, final StateContext<?> context) {
    return address
        .getSpec()
        .bind(
            address.getId(),
            new StateBinder() {
              @Override
              public <T2> ValueState<T2> bindValue(
                  String id, StateSpec<ValueState<T2>> spec, Coder<T2> coder) {
                throw new UnsupportedOperationException(
                    String.format("%s is not supported", ValueState.class.getSimpleName()));
              }

              @Override
              public <T2> BagState<T2> bindBag(
                  String id, StateSpec<BagState<T2>> spec, Coder<T2> elemCoder) {
                // Only implement bag state
                return new FlinkKeyGroupBagState<>(keyedStateBackend, id, namespace, elemCoder);
              }

              @Override
              public <T2> SetState<T2> bindSet(
                  String id, StateSpec<SetState<T2>> spec, Coder<T2> elemCoder) {
                throw new UnsupportedOperationException(
                    String.format("%s is not supported", SetState.class.getSimpleName()));
              }

              @Override
              public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
                  String id,
                  StateSpec<MapState<KeyT, ValueT>> spec,
                  Coder<KeyT> mapKeyCoder,
                  Coder<ValueT> mapValueCoder) {
                throw new UnsupportedOperationException(
                    String.format("%s is not supported", MapState.class.getSimpleName()));
              }

              @Override
              public <InputT, AccumT, OutputT>
                  CombiningState<InputT, AccumT, OutputT> bindCombining(
                      String id,
                      StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
                      Coder<AccumT> accumCoder,
                      Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
                throw new UnsupportedOperationException("bindCombiningValue is not supported.");
              }

              @Override
              public <InputT, AccumT, OutputT>
                  CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
                      String id,
                      StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
                      Coder<AccumT> accumCoder,
                      CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
                throw new UnsupportedOperationException(
                    "bindCombiningValueWithContext is not supported.");
              }

              @Override
              public WatermarkHoldState bindWatermark(
                  String id,
                  StateSpec<WatermarkHoldState> spec,
                  TimestampCombiner timestampCombiner) {
                throw new UnsupportedOperationException(
                    String.format("%s is not supported", CombiningState.class.getSimpleName()));
              }
            });
  }

  private static class FlinkKeyGroupBagState<K, T> implements BagState<T> {

    private final StateNamespace namespace;
    private final CoderTypeSerializer namespaceSerializer;
    private final ListStateDescriptor<T> flinkStateDescriptor;
    private final KeyedStateBackend<ByteBuffer> flinkStateBackend;

    FlinkKeyGroupBagState(
        KeyedStateBackend<ByteBuffer> flinkStateBackend,
        String stateId,
        StateNamespace namespace,
        Coder<T> coder) {

      this.namespace = namespace;
      this.flinkStateBackend = flinkStateBackend;
      this.namespaceSerializer = new CoderTypeSerializer<>(coder);
      this.flinkStateDescriptor = new ListStateDescriptor<>(stateId, namespaceSerializer);
    }

    @Override
    public void add(T input) {
      try {
        ListState<T> partitionedState =
            flinkStateBackend.getPartitionedState(
                namespace.stringKey(), StringSerializer.INSTANCE, flinkStateDescriptor);
        partitionedState.add(input);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to bag state.", e);
      }
    }

    @Override
    public BagState<T> readLater() {
      return this;
    }

    /** Reads the state of _all_ keys and combines it. */
    @Override
    @Nonnull
    public Iterable<T> read() {
      final Deque<T> state = new ArrayDeque<>();
      // Load specific implementation for Flink 1.5/1.6/1.7
      final AllKeyStateFunction allKeyStateFunction =
          new AllKeyStateFunction(
              (StateConsumer<ListState>)
                  listState -> {
                    for (T item : (Iterable<T>) listState.get()) {
                      state.add(item);
                    }
                  });
      try {
        flinkStateBackend.applyToAllKeys(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor,
            allKeyStateFunction);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(
                "Failed to iterate over all key state for giving namespace %s", namespace));
      }
      return () ->
          new Iterator<T>() {
            @Override
            public boolean hasNext() {
              return state.size() > 0;
            }

            @Override
            public T next() {
              return state.pop();
            }
          };
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      throw new UnsupportedOperationException("isEmpty is not implemented and should not be used.");
    }

    @Override
    public void clear() {
      // Load specific implementation for Flink 1.5/1.6/1.7
      final AllKeyStateFunction allKeyStateFunction =
          new AllKeyStateFunction((StateConsumer<ListState>) listState -> listState.clear());
      try {
        flinkStateBackend.applyToAllKeys(
            namespace.stringKey(),
            StringSerializer.INSTANCE,
            flinkStateDescriptor,
            allKeyStateFunction);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(
                "Failed to iterate over all key state for giving namespace %s", namespace));
      }
    }
  }
}
