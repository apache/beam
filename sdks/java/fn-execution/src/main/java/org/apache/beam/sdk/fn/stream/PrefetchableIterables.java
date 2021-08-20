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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

public class PrefetchableIterables {

  private static PrefetchableIterator<Object> EMPTY_ITERATOR =
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
  private static PrefetchableIterable<Object> EMPTY_ITERABLE =
      new PrefetchableIterable<Object>() {
        @Override
        public PrefetchableIterator<Object> iterator() {
          return EMPTY_ITERATOR;
        }
      };

  public static <T> PrefetchableIterable<T> emptyIterable() {
    return (PrefetchableIterable<T>) EMPTY_ITERABLE;
  }

  public static <T> PrefetchableIterator<T> emptyIterator() {
    return (PrefetchableIterator<T>) EMPTY_ITERATOR;
  }

  public static <T> PrefetchableIterable<T> iterableFromArray(T... values) {
    if (values.length == 0) {
      return emptyIterable();
    }
    return new PrefetchableIterable<T>() {
      @Override
      public PrefetchableIterator<T> iterator() {
        return iteratorFromArray(values);
      }
    };
  }

  public static <T> PrefetchableIterator<T> iteratorFromArray(T... values) {
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
        return values[currentIndex];
      }
    };
  }

  private static <T> PrefetchableIterable<T> maybePrefetchable(Iterable<T> iterable) {
    return new PrefetchableIterable<T>() {
      @Override
      public PrefetchableIterator<T> iterator() {
        Iterator<T> delegate = iterable.iterator();
        if (delegate instanceof PrefetchableIterator) {
          return (PrefetchableIterator<T>) delegate;
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
            return delegate.hasNext();
          }

          @Override
          public T next() {
            return delegate.next();
          }
        };
      }
    };
  }

  public static <T> PrefetchableIterable<T> concat(Iterable<T>... iterables) {
    for (int i = 0; i < iterables.length; ++i) {
      if (iterables[i] == null) {
        throw new IllegalArgumentException("Iterable at position " + i + " was null.");
      }
    }
    if (iterables.length == 0) {
      return emptyIterable();
    } else if (iterables.length == 1) {
      if (iterables[0] instanceof PrefetchableIterable) {
        return (PrefetchableIterable<T>) iterables[0];
      }
      return maybePrefetchable(iterables[0]);
    }
    return new PrefetchableIterable<T>() {
      @Override
      public PrefetchableIterator<T> iterator() {
        return new PrefetchableIterator<T>() {
          Iterator<T> delegate = iterables[0].iterator();
          int currentIndex;

          @Override
          public boolean isReady() {
            return false;
          }

          @Override
          public void prefetch() {
            // Ensure that we advance from iterables that don't have the next
            // element to an iterable that supports prefetch or does have an element
            for (; ; ) {
              if (delegate instanceof PrefetchableIterator) {
                // Move to the next iterator if we can know that the prefetchable iterator is
                // empty without blocking.
                if (((PrefetchableIterator<T>) delegate).isReady()) {
                  if (delegate.hasNext()) {
                    return;
                  }
                  // We should advance to the next index since we know this iterator is empty
                  // and re-evaluate whether we can perform a prefetch
                  currentIndex += 1;
                  if (currentIndex == iterables.length) {
                    delegate = null;
                  }
                  delegate = iterables[currentIndex].iterator();
                } else {
                  ((PrefetchableIterator<T>) delegate).prefetch();
                }

                // Assume that iterators that aren't prefetchable are always ready.
              } else if (!delegate.hasNext()) {
                // We should advance to the next index since we know this iterator is empty
                // and re-evaluate whether we can perform a prefetch
                currentIndex += 1;
                if (currentIndex == iterables.length) {
                  delegate = null;
                }
                delegate = iterables[currentIndex].iterator();
              }
            }
          }

          @Override
          public boolean hasNext() {
            for (; ; ) {
              if (delegate.hasNext()) {
                return true;
              }
              currentIndex += 1;
              if (currentIndex == iterables.length) {
                delegate = null;
                return false;
              }
              delegate = iterables[currentIndex].iterator();
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
    };
  }

  public static <T> PrefetchableIterable<T> limit(Iterable<T> iterable, int limit) {
    if (iterable instanceof PrefetchableIterable) {
      return new PrefetchableIterable<T>() {
        @Override
        public PrefetchableIterator<T> iterator() {
          return new PrefetchableIterator<T>() {
            PrefetchableIterator<T> delegate = ((PrefetchableIterable<T>) iterable).iterator();
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
              if (currentPosition < limit) {
                delegate.prefetch();
              }
            }

            @Override
            public boolean hasNext() {
              if (currentPosition != limit) {
                return delegate.hasNext();
              }
              delegate = null;
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
    } else {
      return maybePrefetchable(Iterables.limit(iterable, limit));
    }
  }
}
