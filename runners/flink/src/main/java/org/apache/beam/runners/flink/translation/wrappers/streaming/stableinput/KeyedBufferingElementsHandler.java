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
public class KeyedBufferingElementsHandler implements BufferingElementsHandler {

  static KeyedBufferingElementsHandler create(
      KeyedStateBackend backend, ListStateDescriptor<BufferedElement> stateDescriptor) {
    return new KeyedBufferingElementsHandler(backend, stateDescriptor);
  }

  private final KeyedStateBackend backend;
  private final ListStateDescriptor<BufferedElement> stateDescriptor;

  private KeyedBufferingElementsHandler(
      KeyedStateBackend backend, ListStateDescriptor<BufferedElement> stateDescriptor) {
    this.backend = backend;
    this.stateDescriptor = stateDescriptor;
  }

  @Override
  public void buffer(BufferedElement element) {
    try {
      ListState<BufferedElement> state =
          (ListState<BufferedElement>)
              backend.getPartitionedState(
                  VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);

      // assumes state backend is already keyed
      state.add(element);
    } catch (Exception e) {
      throw new RuntimeException("Failed to buffer element in state backend." + element, e);
    }
  }

  @Override
  public Stream<BufferedElement> getElements() {
    return backend
        .getKeys(stateDescriptor.getName(), VoidNamespace.INSTANCE)
        .flatMap(
            key -> {
              try {
                backend.setCurrentKey(key);

                ListState<BufferedElement> state =
                    (ListState<BufferedElement>)
                        backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            stateDescriptor);

                return StreamSupport.stream(state.get().spliterator(), false);
              } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to retrieve buffered element from state backend.", e);
              }
            });
  }

  @Override
  public void clear() {
    List keys =
        (List)
            backend
                .getKeys(stateDescriptor.getName(), VoidNamespace.INSTANCE)
                .collect(Collectors.toList());

    try {
      for (Object key : keys) {
        backend.setCurrentKey(key);

        ListState<BufferedElement> state =
            (ListState<BufferedElement>)
                backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);

        state.clear();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to clear buffered element state", e);
    }
  }
}
