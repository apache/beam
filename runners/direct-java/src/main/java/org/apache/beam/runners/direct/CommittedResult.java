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
package org.apache.beam.runners.direct;

import com.google.auto.value.AutoValue;
import java.util.Optional;
import java.util.Set;
import org.apache.beam.sdk.runners.AppliedPTransform;

/** A {@link TransformResult} that has been committed. */
@AutoValue
abstract class CommittedResult<ExecutableT> {
  /** Returns the {@link AppliedPTransform} that produced this result. */
  public abstract ExecutableT getExecutable();

  /**
   * Returns the {@link CommittedBundle} that contains the input elements that could not be
   * processed by the evaluation. The returned optional is present if there were any unprocessed
   * input elements, and absent otherwise.
   */
  public abstract Optional<? extends CommittedBundle<?>> getUnprocessedInputs();

  /** Returns the outputs produced by the transform. */
  public abstract Iterable<? extends CommittedBundle<?>> getOutputs();

  /** Returns a description of the produced outputs. */
  public abstract Set<OutputType> getProducedOutputTypes();

  public static CommittedResult<AppliedPTransform<?, ?, ?>> create(
      TransformResult<?> original,
      Optional<? extends CommittedBundle<?>> unprocessedElements,
      Iterable<? extends CommittedBundle<?>> outputs,
      Set<OutputType> producedOutputs) {
    return new AutoValue_CommittedResult<>(
        original.getTransform(), unprocessedElements, outputs, producedOutputs);
  }

  enum OutputType {
    PCOLLECTION_VIEW,
    BUNDLE
  }
}
