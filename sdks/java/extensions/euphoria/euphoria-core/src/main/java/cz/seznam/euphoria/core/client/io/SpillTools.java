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
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.util.IOUtils;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Tools that can be used to externalize a dataset to local storage.
 */
@Audience(Audience.Type.CLIENT)
public interface SpillTools extends Serializable {

  /**
   * Simply externalize {@code Iterable}.
   *
   * @param <T> type of input
   * @param what an {@code Iterable} that is to be externalized
   * @return {@code ExternalIterable} that is backed by policy implemented
   * by this interface
   */
  <T> ExternalIterable<T> externalize(Iterable<T> what);

  /**
   * Externalize and sort given {@code Iterable} to
   * sorted parts. These parts can then be merge sorted.
   *
   * @param <T> type of input
   * @param what the {@code Iterable} that is to be split and sorted
   * @param comparator the {@code Comparator} to use for sorting
   * @return collection of externalized iterables that are sorted
   */
  <T> Collection<ExternalIterable<T>> spillAndSortParts(
      Iterable<T> what, Comparator<T> comparator);

  /**
   * Use external sort to return given {@code Iterable} sorted according
   * to given comparator.
   *
   * @param <T> type of data in the {@code Iterable}.
   * @param what the {@code Iterable} to external sort
   * @param comparator the comparator to use when sorting
   * @return the sorted {@code Iterable}
   */
  default <T> ExternalIterable<T> sorted(Iterable<T> what, Comparator<T> comparator) {

    Collection<ExternalIterable<T>> parts = spillAndSortParts(what, comparator);
    Iterable<T> ret = Iterables.mergeSorted(parts, comparator);

    return new ExternalIterable<T>() {

      @Override
      public Iterator<T> iterator() {
        return ret.iterator();
      }

      @Override
      public void close() throws IOException {
        IOUtils.forEach(parts, Closeable::close);
      }

    };
  }

}
