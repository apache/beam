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
package org.apache.beam.fn.harness.state;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Converts an iterator to an iterable lazily loading values from the underlying iterator and
 * caching them to support reiteration.
 */
class LazyCachingIteratorToIterable<T> implements Iterable<T> {
  private final List<T> cachedElements;
  private final Iterator<T> iterator;

  public LazyCachingIteratorToIterable(Iterator<T> iterator) {
    this.cachedElements = new ArrayList<>();
    this.iterator = iterator;
  }

  @Override
  public Iterator<T> iterator() {
    return new CachingIterator();
  }

  /** An {@link Iterator} which adds and fetched values into the cached elements list. */
  private class CachingIterator implements Iterator<T> {
    private int position = 0;

    private CachingIterator() {}

    @Override
    public boolean hasNext() {
      // The order of the short circuit is important below.
      return position < cachedElements.size() || iterator.hasNext();
    }

    @Override
    public T next() {
      if (position < cachedElements.size()) {
        return cachedElements.get(position++);
      }

      if (!iterator.hasNext()) {
        throw new NoSuchElementException();
      }

      T rval = iterator.next();
      cachedElements.add(rval);
      position += 1;
      return rval;
    }
  }

  @Override
  public int hashCode() {
    return iterator.hasNext() ? iterator.next().hashCode() : -1789023489;
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    return obj instanceof Iterable && Iterables.elementsEqual(this, (Iterable) obj);
  }

  @Override
  public String toString() {
    return Iterables.toString(this);
  }
}
