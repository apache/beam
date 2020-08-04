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
package org.apache.beam.runners.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.StateTag.StateBinder;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Equivalence;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Table mapping {@code StateNamespace} and {@code StateTag<?>} to a {@code State} instance.
 *
 * <p>Two {@link StateTag StateTags} with the same ID are considered equivalent. The remaining
 * information carried by the {@link StateTag} is used to configure an empty state cell if it is not
 * yet initialized.
 */
public abstract class StateTable {

  private final Table<StateNamespace, Equivalence.Wrapper<StateTag>, State> stateTable =
      HashBasedTable.create();

  /**
   * Gets the {@link State} in the specified {@link StateNamespace} with the specified {@link
   * StateTag}, binding it using the {@link #binderForNamespace} if it is not already present in
   * this {@link StateTable}.
   */
  public <StateT extends State> StateT get(
      StateNamespace namespace, StateTag<StateT> tag, StateContext<?> c) {

    Equivalence.Wrapper<StateTag> tagById = StateTags.ID_EQUIVALENCE.wrap((StateTag) tag);

    @Nullable State storage = getOrNull(namespace, tagById, c);
    if (storage != null) {
      @SuppressWarnings("unchecked")
      StateT typedStorage = (StateT) storage;
      return typedStorage;
    }

    StateT typedStorage = tag.bind(binderForNamespace(namespace, c));
    stateTable.put(namespace, tagById, typedStorage);
    return typedStorage;
  }

  /**
   * Gets the {@link State} in the specified {@link StateNamespace} with the specified identifier or
   * {@code null} if it is not yet present.
   */
  public @Nullable State getOrNull(
      StateNamespace namespace, Equivalence.Wrapper<StateTag> tag, StateContext<?> c) {
    return stateTable.get(namespace, tag);
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

  public Map<StateTag, State> getTagsInUse(StateNamespace namespace) {
    // Because of shading, Equivalence.Wrapper cannot be on the API surface; it won't work.
    // If runners-core ceases to shade Guava then it can (all runners should shade runners-core
    // anyhow)
    Map<Equivalence.Wrapper<StateTag>, State> row = stateTable.row(namespace);
    HashMap<StateTag, State> result = new HashMap<>();

    for (Map.Entry<Equivalence.Wrapper<StateTag>, State> entry : row.entrySet()) {
      result.put(entry.getKey().get(), entry.getValue());
    }

    return result;
  }

  public Set<StateNamespace> getNamespacesInUse() {
    return stateTable.rowKeySet();
  }

  /**
   * Provide the {@code StateBinder} to use for creating {@code Storage} instances in the specified
   * {@code namespace}.
   */
  protected abstract StateBinder binderForNamespace(StateNamespace namespace, StateContext<?> c);
}
