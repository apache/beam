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

import avro.shaded.com.google.common.collect.Lists;
import com.alibaba.jstorm.cache.ComposedKey;
import com.alibaba.jstorm.cache.IKvStore;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ReadableState;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * JStorm implementation of {@link BagState}.
 */
class JStormBagState<K, T> implements BagState<T> {

    @Nullable
    private final K key;
    private final StateNamespace namespace;
    private final IKvStore<ComposedKey, T> kvState;
    private final IKvStore<ComposedKey, Object> stateInfoKvState;
    private int elemIndex;

    public JStormBagState(@Nullable K key, StateNamespace namespace, IKvStore<ComposedKey, T> kvState,
                           IKvStore<ComposedKey, Object> stateInfoKvState) throws IOException {
        this.key = key;
        this.namespace = checkNotNull(namespace, "namespace");
        this.kvState = checkNotNull(kvState, "kvState");
        this.stateInfoKvState = checkNotNull(stateInfoKvState, "stateInfoKvState");

        Integer index = (Integer) stateInfoKvState.get(getComposedKey());
        this.elemIndex =  index != null ? ++index : 0;
    }

    @Override
    public void add(T input) {
        try {
            kvState.put(getComposedKey(elemIndex), input);
            stateInfoKvState.put(getComposedKey(), elemIndex);
            elemIndex++;
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
        return new ReadableState<Boolean>() {
            @Override
            public Boolean read() {
                return elemIndex <= 0;
            }

            @Override
            public ReadableState<Boolean> readLater() {
                // TODO: support prefetch.
                return this;
            }
        };
    }

    @Override
    public Iterable<T> read() {
        try {
            List<T> values = Lists.newArrayList();
            for (int i = 0; i < elemIndex; i++) {
                values.add(kvState.get(getComposedKey(i)));
            }
            return values;
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    @Override
    public BagState readLater() {
        // TODO: support prefetch.
        return this;
    }

    @Override
    public void clear() {
        try {
            for (int i = 0; i < elemIndex; i++) {
                kvState.remove(getComposedKey(i));
            }
            stateInfoKvState.remove(getComposedKey());
            elemIndex = 0;
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private ComposedKey getComposedKey() {
        return ComposedKey.of(key, namespace);
    }

    private ComposedKey getComposedKey(int elemIndex) {
        return ComposedKey.of(key, namespace, elemIndex);
    }
}
