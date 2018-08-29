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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator.state;

import java.io.Serializable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.MergingWindowing;

/**
 * A function to merge specific types of states into a given target state. The need for merging
 * states into one arise typically from the utilization of {@link MergingWindowing}, e.g. session
 * windows, where individual session windows need occasionally be merged and, thus, their states.
 *
 * @param <InputT> the type of input elements for the states
 * @param <OutputT> the type of output elements of the states
 * @param <StateT> the type of states being merged
 */
@Audience(Audience.Type.CLIENT)
@FunctionalInterface
public interface StateMerger<InputT, OutputT, StateT extends State<InputT, OutputT>>
    extends Serializable {

  /**
   * Merges <tt>others</tt> into the given <tt>target</tt>, which itself is guaranteed by the caller
   * not to be part of <tt>others</tt>.
   *
   * @param target the target state to receive values from <tt>others</tt>
   * @param others the states to be merged into <tt>target</tt>
   */
  void merge(StateT target, Iterable<StateT> others);
}
