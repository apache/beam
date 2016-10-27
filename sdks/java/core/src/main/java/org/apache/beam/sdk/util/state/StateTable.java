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
package org.apache.beam.sdk.util.state;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.util.state.StateTag.StateBinder;

/**
 * Table mapping {@code StateNamespace} and {@code StateTag<?>} to a {@code State} instance.
 */
public abstract class StateTable<K> {

  private final Table<StateNamespace, StateTag<? super K, ?>, State> stateTable =
      HashBasedTable.create();

  /**
   * Gets the {@link State} in the specified {@link StateNamespace} with the specified {@link
   * StateTag}, binding it using the {@link #binderForNamespace} if it is not
   * already present in this {@link StateTable}.
   */
  public <StateT extends State> StateT get(
      StateNamespace namespace, StateTag<? super K, StateT> tag, StateContext<?> c) {
    State storage = stateTable.get(namespace, tag);
    if (storage != null) {
      @SuppressWarnings("unchecked")
      StateT typedStorage = (StateT) storage;
      return typedStorage;
    }

    StateT typedStorage = tag.bind(binderForNamespace(namespace, c));
    stateTable.put(namespace, tag, typedStorage);
    return typedStorage;
  }

  public void clearNamespace(StateNamespace namespace) {
    stateTable.rowKeySet().remove(namespace);
  }

  public void clear() {
    stateTable.clear();
  }

  public Iterable<State> values() {
    return stateTable.values();
  }

  public boolean isNamespaceInUse(StateNamespace namespace) {
    return stateTable.containsRow(namespace);
  }

  public Map<StateTag<? super K, ?>, State> getTagsInUse(StateNamespace namespace) {
    return stateTable.row(namespace);
  }

  public Set<StateNamespace> getNamespacesInUse() {
    return stateTable.rowKeySet();
  }

  /**
   * Provide the {@code StateBinder} to use for creating {@code Storage} instances
   * in the specified {@code namespace}.
   */
  protected abstract StateBinder<K> binderForNamespace(StateNamespace namespace, StateContext<?> c);
}
