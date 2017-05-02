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

import com.alibaba.jstorm.cache.ComposedKey;
import com.alibaba.jstorm.cache.IKvStore;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.ReadableState;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * JStorm implementation of {@link BagState}.
 */
class JStormBagState<K, T> implements BagState<T> {

    @Nullable
    private final K key;
    private final StateNamespace namespace;
    private final IKvStore<ComposedKey, List<T>> kvState;

    JStormBagState(@Nullable K key, StateNamespace namespace, IKvStore<ComposedKey, List<T>> kvState) {
        this.key = key;
        this.namespace = checkNotNull(namespace, "namespace");
        this.kvState = checkNotNull(kvState, "kvState");
    }

    @Override
    public void add(T input) {
        try {
            List<T> values = kvState.get(getComposedKey());
            if (values == null) {
                values = new ArrayList<>();
            }
            values.add(input);
            kvState.put(getComposedKey(), values);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
        return new ReadableState<Boolean>() {
            @Override
            public Boolean read() {
                try {
                    return kvState.get(getComposedKey()) == null;
                } catch (IOException e) {
                    throw new RuntimeException();
                }
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
            List<T> values = kvState.get(getComposedKey());
            if (values == null) {
                return Collections.EMPTY_LIST;
            } else {
                return values;
            }
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
            kvState.remove(getComposedKey());
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private ComposedKey getComposedKey() {
        return ComposedKey.of(key, namespace);
    }
}
