/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.auto.value.AutoValue;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;

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
   * Returns the outputs produced by the transform.
   */
  public abstract Iterable<? extends CommittedBundle<?>> getOutputs();

  public static CommittedResult create(
      InProcessTransformResult original, Iterable<? extends CommittedBundle<?>> outputs) {
    return new AutoValue_CommittedResult(original.getTransform(),
        outputs);
  }
}
