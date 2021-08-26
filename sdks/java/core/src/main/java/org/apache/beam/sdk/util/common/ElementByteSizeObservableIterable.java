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
package org.apache.beam.sdk.util.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Observer;
import org.apache.beam.sdk.annotations.Internal;

/**
 * An abstract class used for iterables that notify observers about size in bytes of their elements,
 * as they are being iterated over.
 *
 * @param <V> the type of elements returned by this iterable
 * @param <InputT> type type of iterator returned by this iterable
 */
@Internal
public abstract class ElementByteSizeObservableIterable<
        V, InputT extends ElementByteSizeObservableIterator<V>>
    implements Iterable<V> {
  private List<Observer> observers = new ArrayList<>();

  /** Derived classes override this method to return an iterator for this iterable. */
  protected abstract InputT createIterator();

  /**
   * Sets the observer, which will observe the iterator returned in the next call to iterator()
   * method. Future calls to iterator() won't be observed, unless an observer is set again.
   */
  public void addObserver(Observer observer) {
    observers.add(observer);
  }

  /**
   * Returns a new iterator for this iterable. If an observer was set in a previous call to
   * setObserver(), it will observe the iterator returned.
   */
  @Override
  public InputT iterator() {
    InputT iterator = createIterator();
    for (Observer observer : observers) {
      iterator.addObserver(observer);
    }
    observers.clear();
    return iterator;
  }
}
