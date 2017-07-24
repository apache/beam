/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.beam.translation.runtime.state;

import com.alibaba.jstorm.beam.translation.runtime.TimerService;
import com.alibaba.jstorm.cache.ComposedKey;
import com.alibaba.jstorm.cache.IKvStoreManager;

import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * JStorm implementation of {@link StateInternals}.
 */
public class JStormStateInternals<K> implements StateInternals {

    private static final String STATE_INFO = "state-info:";

    @Nullable
    private final K key;
    private final IKvStoreManager kvStoreManager;
    private final TimerService timerService;

    public JStormStateInternals(K key, IKvStoreManager kvStoreManager, TimerService timerService) {
        this.key = key;
        this.kvStoreManager = checkNotNull(kvStoreManager, "kvStoreManager");
        this.timerService = checkNotNull(timerService, "timerService");
    }

    @Nullable
    @Override
    public K getKey() {
        return key;
    }

    @Override
    public <T extends State> T state(
        StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
        // throw new UnsupportedOperationException("StateContext is not supported.");
        /**
         * TODO：
         * Same implementation as state() which is without StateContext. This might be updated after
         * we figure out if we really need StateContext for JStorm state internals.
         */
        return state(namespace, address);
    }

    @Override
    public <T extends State> T state(final StateNamespace namespace, StateTag<T> address) {
        return address.getSpec().bind(address.getId(), new StateBinder() {
            @Override
            public <T> ValueState<T> bindValue(String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
                try {
                    return new JStormValueState<>(
                            getKey(), namespace, kvStoreManager.<ComposedKey, T>getOrCreate(id));
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }

            @Override
            public <T> BagState<T> bindBag(String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
                try {
                    return new JStormBagState(
                            getKey(), namespace, kvStoreManager.<ComposedKey, T>getOrCreate(id),
                            kvStoreManager.<ComposedKey, Object>getOrCreate(STATE_INFO + id));
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }

            @Override
            public <T> SetState<T> bindSet(String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
                String id,
                StateSpec<MapState<KeyT, ValueT>> spec,
                Coder<KeyT> mapKeyCoder,
                Coder<ValueT> mapValueCoder) {
                try {
                    return new JStormMapState<>(kvStoreManager.<KeyT, ValueT>getOrCreate(id));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public <InputT, AccumT, OutputT> CombiningState bindCombining(
                    String id,
                    StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
                    Coder<AccumT> accumCoder,
                    Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
                try {
                    BagState<AccumT> accumBagState = new JStormBagState(
                            getKey(), namespace,
                            kvStoreManager.<ComposedKey, AccumT>getOrCreate(id),
                            kvStoreManager.<ComposedKey, Object>getOrCreate(STATE_INFO + id));
                    return new JStormCombiningState<>(accumBagState, combineFn);
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }


            @Override
            public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT>
            bindCombiningWithContext(
                String id,
                StateSpec<CombiningState<InputT, AccumT, OutputT>> stateSpec, Coder<AccumT> coder,
                CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFnWithContext) {
                throw new UnsupportedOperationException();
            }

            @Override
            public WatermarkHoldState bindWatermark(
                String id,
                StateSpec<WatermarkHoldState> spec,
                final TimestampCombiner timestampCombiner) {
                try {
                    BagState<Combine.Holder<Instant>> accumBagState = new JStormBagState(
                            getKey(), namespace,
                            kvStoreManager.<ComposedKey, Combine.Holder<Instant>>getOrCreate(id),
                            kvStoreManager.<ComposedKey, Object>getOrCreate(STATE_INFO + id));

                    Combine.CombineFn<Instant, Combine.Holder<Instant>, Instant> outputTimeCombineFn =
                            new BinaryCombineFn<Instant>() {
                                @Override
                                public Instant apply(Instant left, Instant right) {
                                  return timestampCombiner.combine(left, right);
                                }};
                    return new JStormWatermarkHoldState(
                            namespace,
                            new JStormCombiningState<>(
                                    accumBagState,
                                    outputTimeCombineFn),
                            timestampCombiner,
                            timerService);
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }
        });
    }
}
