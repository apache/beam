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

/**
 * The interface for things that might be input to a {@link
 * org.apache.beam.sdk.transforms.PTransform}.
 */
public interface PInput {
  /** Returns the owning {@link Pipeline} of this {@link PInput}. */
  Pipeline getPipeline();

  /**
   * Expands this {@link PInput} into a list of its component output {@link PValue PValues}.
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
}
