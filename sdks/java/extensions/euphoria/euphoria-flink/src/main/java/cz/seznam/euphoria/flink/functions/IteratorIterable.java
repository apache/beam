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
package cz.seznam.euphoria.flink.functions;

import java.util.Iterator;

/**
 * Adapter to make an {@link Iterator} instance appear to be an
 * {@link Iterable} instance. The iterable can be used only
 * in one iterative operation.
 */
public class IteratorIterable<T> implements Iterable<T> {

  /** the iterator being adapted into an iterable. */
  private final Iterator<T> iterator;

  private boolean consumed;

  public IteratorIterable(Iterator<T> iterator) {
    this.iterator = iterator;
  }

  @Override
  public Iterator<T> iterator() {
    if (consumed) {
      throw new IllegalStateException("This Iterable can be consumed just once");
    }

    this.consumed = true;
    return iterator;
  }
}
