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

import java.util.Collection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * The interface for things that might be output from a {@link PTransform}.
 */
public interface POutput {

  /**
   * Returns the owning {@link Pipeline} of this {@link POutput}.
   */
  Pipeline getPipeline();

  /**
   * Expands this {@link POutput} into a list of its component output
   * {@link PValue PValues}.
   *
   * <ul>
   *   <li>A {@link PValue} expands to itself.</li>
   *   <li>A tuple or list of {@link PValue PValues} (such as
   *     {@link PCollectionTuple} or {@link PCollectionList})
   *     expands to its component {@code PValue PValues}.</li>
   * </ul>
   *
   * <p>Not intended to be invoked directly by user code.
   */
  Collection<? extends PValue> expand();

  /**
   * Records that this {@code POutput} is an output of the given
   * {@code PTransform}.
   *
   * <p>For a compound {@code POutput}, it is advised to call
   * this method on each component {@code POutput}.
   *
   * <p>This is not intended to be invoked by user code, but
   * is automatically invoked as part of applying the
   * producing {@link PTransform}.
   */
  void recordAsOutput(AppliedPTransform<?, ?, ?> transform);

  /**
   * As part of applying the producing {@link PTransform}, finalizes this
   * output to make it ready for being used as an input and for running.
   *
   * <p>This includes ensuring that all {@link PCollection PCollections}
   * have {@link org.apache.beam.sdk.coders.Coder Coders} specified or defaulted.
   *
   * <p>Automatically invoked whenever this {@link POutput} is used
   * as a {@link PInput} to another {@link PTransform}, or if never
   * used as a {@link PInput}, when {@link Pipeline#run}
   * is called, so users do not normally call this explicitly.
   */
  void finishSpecifyingOutput();
}
