/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.runtime.state.internal.InternalKvState;

public class FlinkReducingValueStorage<T, W extends Window> implements ValueStorage<T> {

  private final ReducingState<T> state;
  private final T defaultValue;

  private final W window;

  public FlinkReducingValueStorage(ReducingState<T> state, T defaultValue, W window) {
    this.state = state;
    this.defaultValue = defaultValue;
    this.window = window;
  }

  @Override
  public void set(T value) {
    setNamespace();
    try {
      state.clear();
      state.add(value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T get() {
    setNamespace();
    try {
      T s = state.get();
      return (s == null) ? defaultValue : s;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    setNamespace();
    state.clear();
  }

  /**
   * Make sure that namespace window is set correctly in the underlying
   * keyed state backend.
   */
  @SuppressWarnings("unchecked")
  private void setNamespace() {
    ((InternalKvState) state).setCurrentNamespace(window);
  }
}
