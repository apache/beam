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

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.util.state.CombiningState;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.CombiningState;
import org.apache.beam.sdk.util.state.ReadableState;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * JStorm implementation of {@link CombiningState}.
 */
public class JStormKeyedCombiningState<K, InputT, AccumT, OutputT>
        implements CombiningState<InputT, AccumT, OutputT> {

    @Nullable
    private final K key;
    private final BagState<AccumT> accumBagState;
    private final Combine.KeyedCombineFn<K, InputT, AccumT, OutputT> keyedCombineFn;

    JStormKeyedCombiningState(
            @Nullable K key,
            BagState<AccumT> accumBagState,
            Combine.KeyedCombineFn<K, InputT, AccumT, OutputT> keyedCombineFn) {
        this.key = key;
        this.accumBagState = checkNotNull(accumBagState, "accumBagState");
        this.keyedCombineFn = checkNotNull(keyedCombineFn, "keyedCombineFn");
    }

    @Override
    public AccumT getAccum() {
        // TODO: replacing the accumBagState with the merged accum.
        return keyedCombineFn.mergeAccumulators(key, accumBagState.read());
    }

    @Override
    public void addAccum(AccumT accumT) {
        accumBagState.add(accumT);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> iterable) {
        return keyedCombineFn.mergeAccumulators(key, iterable);
    }

    @Override
    public void add(InputT input) {
        accumBagState.add(
                keyedCombineFn.addInput(key, keyedCombineFn.createAccumulator(key), input));
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
        return accumBagState.isEmpty();
    }

    @Override
    public OutputT read() {
        return keyedCombineFn.extractOutput(
                key,
                keyedCombineFn.mergeAccumulators(key, accumBagState.read()));
    }

    @Override
    public CombiningState<InputT, AccumT, OutputT> readLater() {
        // TODO: support prefetch.
        return this;
    }

    @Override
    public void clear() {
        accumBagState.clear();
    }
}
