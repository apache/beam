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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;

import java.util.Iterator;

/** Private helper class to provide utilities around state handling. */
class StateSupport {

  interface MergeFrom<S> {
    /**
     * Requests to merge the <tt>other</tt> state into <tt>this</tt> instance.
     */
    void mergeFrom(S other);
  }

  static class MergeFromStateCombiner<T extends MergeFrom<T>>
          implements CombinableReduceFunction<T> {
    @Override
    public T apply(Iterable<T> xs) {
      final T first;
      Iterator<T> x = xs.iterator();
      first = x.next();
      while (x.hasNext()) {
        first.mergeFrom(x.next());
      }
      return first;
    }
  }

  private StateSupport() {}
}
