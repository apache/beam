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

import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

/**
 * A {@link InProcessTransformResult} that has been committed.
 */
@AutoValue
abstract class CommittedResult {
  /**
   * Returns the {@link AppliedPTransform} that produced this result.
   */
  public abstract AppliedPTransform<?, ?, ?> getTransform();

  /**
   * Returns the {@link CommittedBundle} that contains the input elements that could not be
   * processed by the evaluation.
   *
   * <p>{@code null} if the input bundle was null.
   */
  @Nullable
  public abstract CommittedBundle<?> getUnprocessedInputs();

  /**
   * Returns the outputs produced by the transform.
   */
  public abstract Iterable<? extends CommittedBundle<?>> getOutputs();

  public static CommittedResult create(
      InProcessTransformResult original,
      CommittedBundle<?> unprocessedElements,
      Iterable<? extends CommittedBundle<?>> outputs) {
    return new AutoValue_CommittedResult(original.getTransform(),
        unprocessedElements,
        outputs);
  }
}
