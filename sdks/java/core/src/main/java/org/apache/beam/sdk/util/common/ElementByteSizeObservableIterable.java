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
  // Mutually exclusive storage: at most one of singleObserver (1 observer, 0 extra allocation) or
  // multipleObservers (2+ deduplicated observers in registration order) is non-null.
  private @Nullable Observer singleObserver;
  private Observer @Nullable [] multipleObservers;

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
    Observer @Nullable [] multi = this.multipleObservers;
    if (multi != null) {
      for (int i = 0; i < multi.length; i++) {
        if (observer == multi[i] || observer.equals(multi[i])) {
          return;
        }
      }
      Observer[] next = new Observer[multi.length + 1];
      System.arraycopy(multi, 0, next, 0, multi.length);
      next[multi.length] = observer;
      this.multipleObservers = next;
      return;
    }
    @Nullable Observer single = this.singleObserver;
    if (single == null) {
      this.singleObserver = observer;
    } else if (observer != single && !observer.equals(single)) {
      this.multipleObservers = new Observer[] {single, observer};
      this.singleObserver = null;
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
    @Nullable Observer single = this.singleObserver;
    if (single != null) {
      // Clear before dispatching so observers can be reclaimed and any re-entrant addObserver()
      // inside iterator.addObserver() is preserved for the next iterator().
      this.singleObserver = null;
      iterator.addObserver(single);
      return iterator;
    }
    Observer @Nullable [] multi = this.multipleObservers;
    if (multi != null) {
      this.multipleObservers = null;
      for (int i = 0; i < multi.length; i++) {
        iterator.addObserver(multi[i]);
      }
    }
    return iterator;
  }
}
