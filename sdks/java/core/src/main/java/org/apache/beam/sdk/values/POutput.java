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
package org.apache.beam.sdk.values;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;

/** The interface for things that might be output from a {@link PTransform}. */
public interface POutput {

  /** Returns the owning {@link Pipeline} of this {@link POutput}. */
  Pipeline getPipeline();

  /**
   * Expands this {@link POutput} into a list of its component output {@link PValue PValues}.
   *
   * <ul>
   *   <li>A {@link PValue} expands to itself.
   *   <li>A tuple or list of {@link PValue PValues} (such as {@link PCollectionTuple} or {@link
   *       PCollectionList}) expands to its component {@code PValue PValues}.
   * </ul>
   *
   * <p>Not intended to be invoked directly by user code.
   */
  Map<TupleTag<?>, PValue> expand();

  /**
   * As part of applying the producing {@link PTransform}, finalizes this output to make it ready
   * for being used as an input and for running.
   *
   * <p>This includes ensuring that all {@link PCollection PCollections} have {@link
   * org.apache.beam.sdk.coders.Coder Coders} specified or defaulted.
   *
   * <p>Automatically invoked whenever this {@link POutput} is output, after {@link
   * PValue#finishSpecifyingOutput(String, PInput, PTransform)} has been called on each component
   * {@link PValue} returned by {@link #expand()}.
   */
  void finishSpecifyingOutput(String transformName, PInput input, PTransform<?, ?> transform);
}
