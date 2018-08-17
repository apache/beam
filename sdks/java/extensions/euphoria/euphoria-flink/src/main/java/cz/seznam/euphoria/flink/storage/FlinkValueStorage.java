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

import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import org.apache.flink.api.common.state.ValueState;

/**
 * Implementation of {@link ValueStorage} using Flink state API
 */
public class FlinkValueStorage<T> implements ValueStorage<T> {

  private final ValueState<T> state;

  public FlinkValueStorage(ValueState<T> state) {
    this.state = state;
  }

  @Override
  public void set(T value) {
    try {
      state.update(value);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public T get() {
    try {
      return state.value();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void clear() {
    state.clear();
  }
}
