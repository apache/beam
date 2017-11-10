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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateMerger;

/** Private helper class to provide utilities around state handling. */
class StateSupport {

  interface MergeFrom<S> {
    /**
     * Requests to merge the <tt>other</tt> state into <tt>this</tt> instance.
     */
    void mergeFrom(S other);
  }

  static class MergeFromStateMerger<I, O, S extends State<I, O> & MergeFrom<S>>
        implements StateMerger<I, O, S> {
    @Override
    public void merge(S target, Iterable<S> others) {
      for (S other : others) {
        target.mergeFrom(other);
      }
    }
  }

  private StateSupport() {}
}
