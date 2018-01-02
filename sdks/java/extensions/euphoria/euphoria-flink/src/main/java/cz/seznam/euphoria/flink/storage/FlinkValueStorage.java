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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.internal.InternalKvState;

/**
 * Implementation of {@link ValueStorage} using Flink state API
 */
public class FlinkValueStorage<T, W extends Window> implements ValueStorage<T> {

  private final ValueState<T> state;
  private final W window;

  public FlinkValueStorage(ValueState<T> state, W window) {
    this.state = state;
    this.window = window;
  }

  @Override
  public void set(T value) {
    setNamespace();
    try {
      state.update(value);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public T get() {
    setNamespace();
    try {
      return state.value();
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
   * Make sure that namespace window is set correctly in the underlying
   * keyed state backend.
   */
  @SuppressWarnings("unchecked")
  private void setNamespace() {
    ((InternalKvState) state).setCurrentNamespace(window);
  }
}
