/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util.state;

import com.google.cloud.dataflow.sdk.util.state.StateTag.StateBinder;
import com.google.common.base.Supplier;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Table mapping {@code StateNamespace} and {@code StateTag<?>} to a {@code State} instance.
 */
public abstract class StateTable {

  private final Table<StateNamespace, StateTag<?>, State> stateTable =
      Tables.newCustomTable(new HashMap<StateNamespace, Map<StateTag<?>, State>>(),
          new Supplier<Map<StateTag<?>, State>>() {
        @Override
        public Map<StateTag<?>, State> get() {
          return new HashMap<>();
        }
      });

  public <StateT extends State> StateT get(
      StateNamespace namespace, StateTag<StateT> tag) {
    State storage = stateTable.get(namespace, tag);
    if (storage != null) {
      @SuppressWarnings("unchecked")
      StateT typedStorage = (StateT) storage;
      return typedStorage;
    }

    StateT typedStorage = tag.bind(binderForNamespace(namespace));
    stateTable.put(namespace, tag, typedStorage);
    return typedStorage;
  }

  public void clearNamespace(StateNamespace namespace) {
    stateTable.rowKeySet().remove(namespace);
  }

  protected void clear() {
    stateTable.clear();
  }

  public Iterable<State> values() {
    return stateTable.values();
  }

  public boolean isNamespaceInUse(StateNamespace namespace) {
    return stateTable.containsRow(namespace);
  }

  public Map<StateTag<?>, State> getTagsInUse(StateNamespace namespace) {
    return stateTable.row(namespace);
  }

  public Set<StateNamespace> getNamespacesInUse() {
    return stateTable.rowKeySet();
  }

  /**
   * Provide the {@code StateBinder} to use for creating {@code Storage} instances
   * in the specified {@code namespace}.
   */
  protected abstract StateBinder binderForNamespace(StateNamespace namespace);
}
