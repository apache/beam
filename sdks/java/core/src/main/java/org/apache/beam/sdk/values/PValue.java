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
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * <b><i>For internal use. No backwards compatibility guarantees.</i></b>
 *
 * <p>A primitive value within Beam.
 */
@Internal
public interface PValue extends POutput, PInput {

  /** Returns the name of this {@link PValue}. */
  String getName();

  /**
   * {@inheritDoc}.
   *
   * @deprecated A {@link PValue} always expands into itself. Calling {@link #expand()} on a PValue
   *     is almost never appropriate.
   */
  @Deprecated
  @Override
  Map<TupleTag<?>, PValue> expand();

  /**
   * After building, finalizes this {@code PValue} to make it ready for being used as an input to a
   * {@link org.apache.beam.sdk.transforms.PTransform}.
   *
   * <p>Automatically invoked whenever {@code apply()} is invoked on this {@code PValue}. Users
   * should not normally call this explicitly.
   *
   * @param upstreamInput the {@link PInput} the {@link PTransform} was applied to to produce this
   *     output
   * @param upstreamTransform the {@link PTransform} that produced this {@link PValue}
   */
  @Internal
  void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform);
}
