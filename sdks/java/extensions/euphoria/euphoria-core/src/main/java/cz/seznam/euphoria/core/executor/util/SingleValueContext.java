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

package cz.seznam.euphoria.core.executor.util;

import cz.seznam.euphoria.core.client.io.Context;

/**
 * A {@code Context} that holds only single value.
 * There is no window associated with the value, so the {@code getWindow()}
 * will always throw {@code UnsupportedOperationException}.
 * This context will free the value as soon as {@code getAndResetValue()}
 * is called.
 */
public class SingleValueContext<T> implements Context<T> {

  T value;

  /**
   * Replace the stored value with given one.
   * @param elem the element to store
   */
  @Override
  public void collect(T elem) {
    value = elem;
  }

  /**
   * Retrieve window associated with the stored element.
   */
  @Override
  public Object getWindow() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "The window is unknown in this context");
  }

  /**
   * Retrieve and reset the stored value to null.
   * @return the stored value
   */
  public T getAndResetValue() {
    T ret = value;
    value = null;
    return ret;
  }

};

