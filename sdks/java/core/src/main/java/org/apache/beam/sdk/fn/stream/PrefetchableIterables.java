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

import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;

/**
 * This class contains static utility functions that operate on or return objects of type {@link
 * PrefetchableIterable}.
 */
public class PrefetchableIterables {

  /**
   * A default implementation that caches an iterator to be returned when {@link #prefetch} is
   * invoked.
   */
  public abstract static class Default<T> implements PrefetchableIterable<T> {
    @Nullable private PrefetchableIterator<T> iterator = null;

    @Override
    public final void prefetch() {
      if (iterator != null) {
        return;
      }
      iterator = createIterator();
      iterator.prefetch();
    }

    @Override
    public final PrefetchableIterator<T> iterator() {
      if (iterator == null) {
        return createIterator();
      }
      PrefetchableIterator<T> rval = iterator;
      iterator = null;
      return rval;
    }

    protected abstract PrefetchableIterator<T> createIterator();
  }

  private static final PrefetchableIterable<Object> EMPTY_ITERABLE =
      new Default<Object>() {
        @Override
        protected PrefetchableIterator<Object> createIterator() {
          return PrefetchableIterators.emptyIterator();
        }
      };

  /** Returns an empty {@link PrefetchableIterable}. */
  public static <T> PrefetchableIterable<T> emptyIterable() {
    return (PrefetchableIterable<T>) EMPTY_ITERABLE;
  }

  /**
   * Returns a {@link PrefetchableIterable} over the specified values.
   *
   * <p>{@link PrefetchableIterator#prefetch()} is a no-op and {@link
   * PrefetchableIterator#isReady()} always returns true.
   */
  public static <T> PrefetchableIterable<T> fromArray(T... values) {
    if (values.length == 0) {
      return emptyIterable();
    }
    return new Default<T>() {
      @Override
      public PrefetchableIterator<T> createIterator() {
        return PrefetchableIterators.fromArray(values);
      }
    };
  }

  /**
   * Converts the {@link Iterable} into a {@link PrefetchableIterable}.
   *
   * <p>If the {@link Iterable#iterator} does not return {@link PrefetchableIterator}s then one is
   * constructed that ensures that {@link PrefetchableIterator#prefetch()} is a no-op and {@link
   * PrefetchableIterator#isReady()} always returns true.
   */
  private static <T> PrefetchableIterable<T> maybePrefetchable(Iterable<T> iterable) {
    if (iterable instanceof PrefetchableIterable) {
      return (PrefetchableIterable<T>) iterable;
    }
    return new Default<T>() {
      @Override
      public PrefetchableIterator<T> createIterator() {
        return PrefetchableIterators.maybePrefetchable(iterable.iterator());
      }
    };
  }

  /**
   * Concatentates the {@link Iterable}s.
   *
   * <p>See {@link PrefetchableIterators#concat} for additional details.
   */
  public static <T> PrefetchableIterable<T> concat(Iterable<T>... iterables) {
    for (int i = 0; i < iterables.length; ++i) {
      if (iterables[i] == null) {
        throw new IllegalArgumentException("Iterable at position " + i + " was null.");
      }
    }
    if (iterables.length == 0) {
      return emptyIterable();
    } else if (iterables.length == 1) {
      return maybePrefetchable(iterables[0]);
    }
    return new Default<T>() {
      @SuppressWarnings("methodref.receiver")
      @Override
      public PrefetchableIterator<T> createIterator() {
        return PrefetchableIterators.concatIterators(
            FluentIterable.from(iterables).transform(Iterable::iterator).iterator());
      }
    };
  }

  /** Limits the {@link PrefetchableIterable} to the specified number of elements. */
  public static <T> PrefetchableIterable<T> limit(Iterable<T> iterable, int limit) {
    PrefetchableIterable<T> prefetchableIterable = maybePrefetchable(iterable);
    return new Default<T>() {
      @Override
      public PrefetchableIterator<T> createIterator() {
        return new PrefetchableIterator<T>() {
          PrefetchableIterator<T> delegate = prefetchableIterable.iterator();
          int currentPosition;

          @Override
          public boolean isReady() {
            if (currentPosition < limit) {
              return delegate.isReady();
            }
            return true;
          }

          @Override
          public void prefetch() {
            if (!isReady()) {
              delegate.prefetch();
            }
          }

          @Override
          public boolean hasNext() {
            if (currentPosition != limit) {
              return delegate.hasNext();
            }
            return false;
          }

          @Override
          public T next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            currentPosition += 1;
            return delegate.next();
          }
        };
      }
    };
  }
}
