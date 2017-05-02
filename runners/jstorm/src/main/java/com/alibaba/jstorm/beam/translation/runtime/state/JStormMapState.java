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

import com.alibaba.jstorm.cache.IKvStore;
import org.apache.beam.sdk.util.state.MapState;
import org.apache.beam.sdk.util.state.ReadableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class JStormMapState<K, V> implements MapState<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(JStormMapState.class);

    private IKvStore<K, V> kvStore;

    public JStormMapState(IKvStore<K, V> kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public void put(K var1, V var2) {
        try {
            kvStore.put(var1, var2);
        } catch (IOException e) {
            reportError(String.format("Failed to put key=%s, value=%s", var1, var2), e);
        }
    }

    @Override
    public ReadableState<V> putIfAbsent(K var1, V var2) {
        ReadableState<V> ret = null;
        try {
            V value = kvStore.get(var1);
            if (value == null) {
                kvStore.put(var1, var2);
                ret = new MapReadableState(var1, kvStore);
            }
        } catch (IOException e) {
            reportError(String.format("Failed to putIfAbsent key=%s, value=%s", var1, var2), e);
        }
        return ret;
    }

    @Override
    public void remove(K var1) {
        try {
            kvStore.remove(var1);
        } catch (IOException e) {
            reportError(String.format("Failed to remove key=%s", var1), e);
        }
    }

    @Override
    public ReadableState<V> get(K var1) {
        return new MapReadableState(var1, kvStore);
    }

    @Override
    public ReadableState<Iterable<K>> keys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReadableState<Iterable<V>> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReadableState<Iterable<Map.Entry<K, V>>> entries() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        try {
            Collection<K> keys = kvStore.listKeys();
            kvStore.removeBatch(keys);
        } catch (IOException e) {
            reportError(String.format("Failed to clear map state"), e);
        }
    }

    private void reportError(String errorInfo, IOException e) {
        LOG.error(errorInfo, e);
        throw new RuntimeException(errorInfo);
    }

    private class MapReadableState implements ReadableState<V> {
        private IKvStore<K, V> kvStore;
        private K key;

        public MapReadableState(K key, IKvStore<K, V> kvStore) {
            this.key = key;
            this.kvStore = kvStore;
        }

        @Override
        public V read() {
            V ret = null;
            try {
                ret = kvStore.get(key);
            } catch (IOException e) {
                reportError(String.format("Failed to read later, key=%s", key), e);
            }
            return ret;
        }

        @Override
        public ReadableState<V> readLater() {
            return this;
        }
    }
}