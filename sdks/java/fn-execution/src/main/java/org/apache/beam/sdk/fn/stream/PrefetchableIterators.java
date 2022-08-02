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
package org.apache.beam.sdk.fn.stream;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PrefetchableIterators {

  private static final PrefetchableIterator<Object> EMPTY_ITERATOR =
      new PrefetchableIterator<Object>() {
        @Override
        public boolean isReady() {
          return true;
        }

        @Override
        public void prefetch() {}

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public Object next() {
          throw new NoSuchElementException();
        }
      };

  /** Returns an empty {@link PrefetchableIterator}. */
  public static <T> PrefetchableIterator<T> emptyIterator() {
    return (PrefetchableIterator<T>) EMPTY_ITERATOR;
  }

  /**
   * Returns a {@link PrefetchableIterator} over the specified values.
   *
   * <p>{@link PrefetchableIterator#prefetch()} is a no-op and {@link
   * PrefetchableIterator#isReady()} always returns true.
   */
  public static <T> PrefetchableIterator<T> fromArray(T... values) {
    if (values.length == 0) {
      return emptyIterator();
    }
    return new PrefetchableIterator<T>() {
      int currentIndex;

      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public void prefetch() {}

      @Override
      public boolean hasNext() {
        return currentIndex < values.length;
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return values[currentIndex++];
      }
    };
  }

  /**
   * If the {@link Iterator} is not a {@link PrefetchableIterator} then one is constructed that
   * ensures {@link PrefetchableIterator#prefetch} is a no-op and {@link
   * PrefetchableIterator#isReady} always returns true.
   */
  // package private for PrefetchableIterables.
  static <T> PrefetchableIterator<T> maybePrefetchable(Iterator<T> iterator) {
    if (iterator == null) {
      throw new IllegalArgumentException("Expected non-null iterator.");
    }
    if (iterator instanceof PrefetchableIterator) {
      return (PrefetchableIterator<T>) iterator;
    }
    return new PrefetchableIterator<T>() {
      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public void prefetch() {}

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        return iterator.next();
      }
    };
  }

  public static <T> PrefetchableIterator<T> concatIterators(Iterator<Iterator<T>> iterators) {
    if (!iterators.hasNext()) {
      return emptyIterator();
    }
    return new PrefetchableIterator<T>() {
      PrefetchableIterator<T> delegate = maybePrefetchable(iterators.next());

      @Override
      public boolean isReady() {
        // Ensure that we advance from iterators that don't have the next
        // element to an iterator that supports prefetch or does have an element
        for (; ; ) {
          // If the delegate isn't ready then we aren't ready.
          // We assume that non prefetchable iterators are always ready.
          if (!delegate.isReady()) {
            return false;
          }

          // If the delegate has a next and is ready then we are ready
          if (delegate.hasNext()) {
            return true;
          }

          // Otherwise we should advance to the next index since we know this iterator is empty
          // and re-evaluate whether we are ready
          if (!iterators.hasNext()) {
            return true;
          }
          delegate = maybePrefetchable(iterators.next());
        }
      }

      @Override
      public void prefetch() {
        if (!isReady()) {
          delegate.prefetch();
        }
      }

      @Override
      public boolean hasNext() {
        for (; ; ) {
          if (delegate.hasNext()) {
            return true;
          }
          if (!iterators.hasNext()) {
            return false;
          }
          delegate = maybePrefetchable(iterators.next());
        }
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return delegate.next();
      }
    };
  }

  /**
   * Concatentates the {@link Iterator}s.
   *
   * <p>{@link Iterable}s are first converted into a {@link PrefetchableIterable} via {@link
   * #maybePrefetchable}.
   *
   * <p>The returned {@link PrefetchableIterable} ensures that iterators which are returned
   * guarantee that {@link PrefetchableIterator#isReady} always advances till it finds an {@link
   * Iterable} that is not {@link PrefetchableIterator#isReady}. {@link
   * PrefetchableIterator#prefetch} is also guaranteed to advance past empty iterators till it finds
   * one that is not ready.
   */
  public static <T> PrefetchableIterator<T> concat(Iterator<T>... iterators) {
    for (int i = 0; i < iterators.length; ++i) {
      if (iterators[i] == null) {
        throw new IllegalArgumentException("Iterator at position " + i + " was null.");
      }
    }
    if (iterators.length == 0) {
      return emptyIterator();
    } else if (iterators.length == 1) {
      return maybePrefetchable(iterators[0]);
    }
    return concatIterators(Arrays.asList(iterators).iterator());
  }
}
