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
package org.apache.beam.runners.flink.translation.wrappers.streaming.stableinput;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

/** A keyed implementation of a {@link BufferingElementsHandler}. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class KeyedBufferingElementsHandler implements BufferingElementsHandler {

  static KeyedBufferingElementsHandler create(
      KeyedStateBackend backend, ListStateDescriptor<BufferedElement> stateDescriptor)
      throws Exception {
    return new KeyedBufferingElementsHandler(backend, stateDescriptor);
  }

  private final KeyedStateBackend<Object> backend;
  private final String stateName;
  private final ListState<BufferedElement> state;

  private KeyedBufferingElementsHandler(
      KeyedStateBackend<Object> backend, ListStateDescriptor<BufferedElement> stateDescriptor)
      throws Exception {
    this.backend = backend;
    this.stateName = stateDescriptor.getName();
    // Eagerly retrieve the state to work around https://jira.apache.org/jira/browse/FLINK-12653
    this.state =
        backend.getPartitionedState(
            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
  }

  @Override
  public void buffer(BufferedElement element) {
    try {
      // assumes state backend is already keyed
      state.add(element);
    } catch (Exception e) {
      throw new RuntimeException("Failed to buffer element in state backend." + element, e);
    }
  }

  @Override
  public Stream<BufferedElement> getElements() {
    return backend
        .getKeys(stateName, VoidNamespace.INSTANCE)
        .flatMap(
            key -> {
              try {
                backend.setCurrentKey(key);
                return StreamSupport.stream(state.get().spliterator(), false);
              } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to retrieve buffered element from state backend.", e);
              }
            });
  }

  @Override
  public void clear() {
    List keys = backend.getKeys(stateName, VoidNamespace.INSTANCE).collect(Collectors.toList());
    try {
      for (Object key : keys) {
        backend.setCurrentKey(key);
        state.clear();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to clear buffered element state", e);
    }
  }
}
