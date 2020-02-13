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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

/**
 * {@link PushedBackElementsHandler} that stores elements in Flink keyed state, for use when an
 * operation is keyed and pushed-back data needs to stay in the correct partition when they get
 * moved.
 */
class KeyedPushedBackElementsHandler<K, T> implements PushedBackElementsHandler<T> {

  static <K, T> KeyedPushedBackElementsHandler<K, T> create(
      KeySelector<T, K> keySelector,
      KeyedStateBackend<K> backend,
      ListStateDescriptor<T> stateDescriptor)
      throws Exception {
    return new KeyedPushedBackElementsHandler<>(keySelector, backend, stateDescriptor);
  }

  private final KeySelector<T, K> keySelector;
  private final KeyedStateBackend<K> backend;
  private final String stateName;
  private final ListState<T> state;

  private KeyedPushedBackElementsHandler(
      KeySelector<T, K> keySelector,
      KeyedStateBackend<K> backend,
      ListStateDescriptor<T> stateDescriptor)
      throws Exception {
    this.keySelector = keySelector;
    this.backend = backend;
    this.stateName = stateDescriptor.getName();
    // Eagerly retrieve the state to work around https://jira.apache.org/jira/browse/FLINK-12653
    this.state =
        backend.getPartitionedState(
            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
  }

  @Override
  public Stream<T> getElements() {
    return backend
        .getKeys(stateName, VoidNamespace.INSTANCE)
        .flatMap(
            key -> {
              try {
                backend.setCurrentKey(key);
                return StreamSupport.stream(state.get().spliterator(), false);
              } catch (Exception e) {
                throw new RuntimeException("Error reading keyed state.", e);
              }
            });
  }

  @Override
  public void clear() throws Exception {
    // TODO we have to collect all keys because otherwise we get ConcurrentModificationExceptions
    // from flink. We can change this once it's fixed in Flink
    List<K> keys = backend.getKeys(stateName, VoidNamespace.INSTANCE).collect(Collectors.toList());

    for (K key : keys) {
      backend.setCurrentKey(key);
      state.clear();
    }
  }

  @Override
  public void pushBack(T element) throws Exception {
    backend.setCurrentKey(keySelector.getKey(element));
    state.add(element);
  }

  @Override
  public void pushBackAll(Iterable<T> elements) throws Exception {
    for (T e : elements) {
      pushBack(e);
    }
  }
}
