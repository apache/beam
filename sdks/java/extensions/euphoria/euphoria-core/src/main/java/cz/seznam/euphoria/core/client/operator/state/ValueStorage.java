/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.operator.state;

/**
 * State storage for single value.
 *
 * @param <T> the type of value stored
 */
public interface ValueStorage<T> extends Storage<T> {

  /**
   * Set the value in this state.
   *
   * @param value the value to store
   */
  void set(T value);

  /** @return the currently stored value */
  T get();

}
