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

import java.util.Observer;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An abstract class used for iterables that notify observers about size in bytes of their elements,
 * as they are being iterated over.
 *
 * <p>Instances are not thread-safe; concurrent calls to {@link #addObserver(Observer)} and {@link
 * #iterator()} require external synchronization.
 *
 * @param <V> the type of elements returned by this iterable
 * @param <InputT> the type of iterator returned by this iterable
 */
@Internal
@NotThreadSafe
public abstract class ElementByteSizeObservableIterable<
        V, InputT extends ElementByteSizeObservableIterator<V>>
    implements Iterable<V> {
  // Encoded as null (0 observers), an Observer instance (1 observer, 0 extra allocation),
  // or a deduplicated Observer[] array (2+ observers in registration order).
  private @Nullable Object observers;

  /** Derived classes override this method to return an iterator for this iterable. */
  protected abstract InputT createIterator();

  /**
   * Registers an observer, which will observe the iterator returned in the next call to {@link
   * #iterator()}. Future calls to {@link #iterator()} won't be observed unless an observer is added
   * again. Duplicate observers (by reference or {@link Object#equals}) are ignored.
   *
   * @param observer observer to register for the next iterator
   * @throws NullPointerException if {@code observer} is {@code null}
   */
  public void addObserver(Observer observer) {
    if (observer == null) {
      throw new NullPointerException();
    }
    Object current = this.observers;
    if (current == null) {
      this.observers = observer;
    } else if (current.getClass() != Observer[].class) {
      if (observer == current || observer.equals(current)) {
        return;
      }
      this.observers = new Observer[] {(Observer) current, observer};
    } else {
      Observer[] arr = (Observer[]) current;
      for (int i = 0; i < arr.length; i++) {
        if (observer == arr[i] || observer.equals(arr[i])) {
          return;
        }
      }
      Observer[] next = new Observer[arr.length + 1];
      System.arraycopy(arr, 0, next, 0, arr.length);
      next[arr.length] = observer;
      this.observers = next;
    }
  }

  /**
   * Returns a new iterator for this iterable. Any observers registered via prior calls to {@link
   * #addObserver(Observer)} will observe the iterator returned and will be cleared from this
   * iterable.
   *
   * @return a new iterator for this iterable
   */
  @Override
  public InputT iterator() {
    InputT iterator = createIterator();
    Object current = this.observers;
    if (current != null) {
      // Clear before dispatching so observers can be reclaimed and any re-entrant addObserver()
      // inside iterator.addObserver() is preserved for the next iterator().
      this.observers = null;
      if (current.getClass() != Observer[].class) {
        iterator.addObserver((Observer) current);
      } else {
        Observer[] arr = (Observer[]) current;
        for (int i = 0; i < arr.length; i++) {
          iterator.addObserver(arr[i]);
        }
      }
    }
    return iterator;
  }
}
