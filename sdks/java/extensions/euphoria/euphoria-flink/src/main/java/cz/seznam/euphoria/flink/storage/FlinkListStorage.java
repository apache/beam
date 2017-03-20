/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.storage;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.KvState;

import java.util.Collections;

/**
 * Implementation of {@link ListStorage} using Flink state API
 */
public class FlinkListStorage<T, W extends Window> implements ListStorage<T> {

  private final ListState<T> state;
  private final W window;

  public FlinkListStorage(ListState<T> state, W window) {
    this.state = state;
    this.window = window;
  }

  @Override
  public void add(T element) {
    setNamespace();
    try {
      state.add(element);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Iterable<T> get() {
    setNamespace();
    try {
      Iterable<T> optional = state.get();
      if (optional == null) {
        return Collections.emptyList();
      }
      return optional;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void clear() {
    setNamespace();
    state.clear();
  }

  /**
   * Make sure that namespace is set correctly in the underlying
   * keyed state backend.
   */
  @SuppressWarnings("unchecked")
  private void setNamespace() {
    ((KvState) state).setCurrentNamespace(window);
  }
}
