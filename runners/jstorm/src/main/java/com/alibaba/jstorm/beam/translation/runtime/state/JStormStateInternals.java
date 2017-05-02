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
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.util.state.*;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * JStorm implementation of {@link StateInternals}.
 */
public class JStormStateInternals<K> implements StateInternals<K> {

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
    public <T extends State> T state(StateNamespace namespace, StateTag<? super K, T> address, StateContext<?> c) {
        // throw new UnsupportedOperationException("StateContext is not supported.");
        /**
         * TODO Same implementation as state() which is without StateContext. This might be updated after
         * we figure out if we really need StateContext for JStorm state internals.
         */
        return state(namespace, address);
    }

    @Override
    public <T extends State> T state(final StateNamespace namespace, StateTag<? super K, T> address) {
        return address.getSpec().bind(address.getId(), new StateBinder<K>() {
            @Override
            public <T> ValueState<T> bindValue(String id, StateSpec<? super K, ValueState<T>> spec, Coder<T> coder) {
                try {
                    return new JStormValueState<>(
                            getKey(), namespace, kvStoreManager.<ComposedKey, T>getOrCreate(id));
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }

            @Override
            public <T> BagState<T> bindBag(String id, StateSpec<? super K, BagState<T>> spec, Coder<T> elemCoder) {
                try {
                    return new JStormBagState<>(
                            getKey(), namespace, kvStoreManager.<ComposedKey, List<T>>getOrCreate(id));
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }

            @Override
            public <T> SetState<T> bindSet(String id, StateSpec<? super K, SetState<T>> spec, Coder<T> elemCoder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(String id, StateSpec<? super K, MapState<KeyT, ValueT>> spec, Coder<KeyT> mapKeyCoder,
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
                    StateSpec<? super K, CombiningState<InputT, AccumT, OutputT>> spec,
                    Coder<AccumT> accumCoder,
                    Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
                return bindKeyedCombining(id, spec, accumCoder, combineFn.<K>asKeyedFn());
            }

            @Override
            public <InputT, AccumT, OutputT> CombiningState bindKeyedCombining(
                    String id,
                    StateSpec<? super K, CombiningState<InputT, AccumT, OutputT>> spec,
                    Coder<AccumT> accumCoder,
                    Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> keyedCombineFn) {
                try {
                    BagState<AccumT> accumBagState = new JStormBagState<>(
                            getKey(), namespace, kvStoreManager.<ComposedKey, List<AccumT>>getOrCreate(id));
                    return new JStormKeyedCombiningState<>(key, accumBagState, keyedCombineFn);
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }

            @Override
            public <InputT, AccumT, OutputT> CombiningState bindKeyedCombiningWithContext(
                    String id,
                    StateSpec<? super K, CombiningState<InputT, AccumT, OutputT>> spec,
                    Coder<AccumT> accumCoder,
                    CombineWithContext.KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT> combineFn) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <W extends BoundedWindow> WatermarkHoldState bindWatermark(
                    String id,
                    StateSpec<? super K, WatermarkHoldState<W>> spec,
                    final OutputTimeFn<? super W> outputTimeFn) {
                try {
                    BagState<Combine.Holder<Instant>> accumBagState = new JStormBagState<>(
                            getKey(),
                            namespace,
                            kvStoreManager.<ComposedKey,List<Combine.Holder<Instant>>>getOrCreate(id));

                    Combine.CombineFn<Instant, Combine.Holder<Instant>, Instant> outputTimeCombineFn =
                            new BinaryCombineFn<Instant>() {
                                @Override
                                public Instant apply(Instant left, Instant right) {
                                    return outputTimeFn.combine(left, right);
                                }};
                    return new JStormWatermarkHoldState<>(
                            namespace,
                            new JStormKeyedCombiningState<>(
                                    key,
                                    accumBagState,
                                    outputTimeCombineFn.asKeyedFn()),
                            outputTimeFn,
                            timerService);
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }
        });
    }
}
