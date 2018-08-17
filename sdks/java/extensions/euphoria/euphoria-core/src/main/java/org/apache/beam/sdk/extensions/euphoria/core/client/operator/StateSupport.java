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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateMerger;

/** Private helper class to provide utilities around state handling. */
class StateSupport {

  private StateSupport() {}

  interface MergeFrom<StateT> {
    /** Requests to merge the <tt>other</tt> state into <tt>this</tt> instance. */
    void mergeFrom(StateT other);
  }

  static class MergeFromStateMerger<
          InputT, OutputT, StateT extends State<InputT, OutputT> & MergeFrom<StateT>>
      implements StateMerger<InputT, OutputT, StateT> {
    @Override
    public void merge(StateT target, Iterable<StateT> others) {
      for (StateT other : others) {
        target.mergeFrom(other);
      }
    }
  }
}
