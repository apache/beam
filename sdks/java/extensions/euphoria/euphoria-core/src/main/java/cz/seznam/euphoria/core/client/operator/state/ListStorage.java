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
 * State storage storing lists.
 *
 * @param <T> the type of elements stored
 */
public interface ListStorage<T> extends Storage<T> {

  /**
   * Add element to the state.
   *
   * @param element the element to add
   */
  void add(T element);

  /**
   * List all elements.
   *
   * @return a collection of the stored elements (in no particular order)
   */
  Iterable<T> get();

  /**
   * Add all elements.
   *
   * @param what elements to add
   */
  default void addAll(Iterable<T> what) {
    for (T e : what) {
      add(e);
    }
  }

}
