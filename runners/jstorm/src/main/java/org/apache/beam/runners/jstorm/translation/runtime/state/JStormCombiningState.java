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
package org.apache.beam.runners.jstorm.translation.runtime.state;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.transforms.Combine;

/**
 * JStorm implementation of {@link CombiningState}.
 */
public class JStormCombiningState<InputT, AccumT, OutputT>
        implements CombiningState<InputT, AccumT, OutputT> {

    @Nullable
    private final BagState<AccumT> accumBagState;
    private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;
    JStormCombiningState(
            BagState<AccumT> accumBagState,
            Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
        this.accumBagState = checkNotNull(accumBagState, "accumBagState");
        this.combineFn = checkNotNull(combineFn, "combineFn");
    }

    @Override
    public AccumT getAccum() {
        // TODO: replacing the accumBagState with the merged accum.
        return combineFn.mergeAccumulators(accumBagState.read());
    }

    @Override
    public void addAccum(AccumT accumT) {
        accumBagState.add(accumT);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> iterable) {
        return combineFn.mergeAccumulators(iterable);
    }

    @Override
    public void add(InputT input) {
        accumBagState.add(
                combineFn.addInput(combineFn.createAccumulator(), input));
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
        return accumBagState.isEmpty();
    }

    @Override
    public OutputT read() {
        return combineFn.extractOutput(
            combineFn.mergeAccumulators(accumBagState.read()));
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
