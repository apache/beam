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

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.WindowedValue;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A noop evaluator for {@link View.CreatePCollectionView}.
 *
 * <p>The direct runner does not need to execute anything, but the runner is designed so it is
 * difficult to remove from the graph (because it executes the graph without really translating it
 * to an intermediate form). So it is easier to ignore at runtime.
 *
 * <p>Each side input is materialized by inspecting the {@link ParDo} transforms that read them. But
 * until all runners are migrated off the deprecated {@link
 * org.apache.beam.sdk.transforms.View.CreatePCollectionView} it will remain in the expansion for
 * side input producing transforms.
 *
 * <p>See https://issues.apache.org/jira/browse/BEAM-11049
 */
// TODO(https://issues.apache.org/jira/browse/BEAM-11049): remove this when CreatePCollectionView is
// finally removed
class CreateViewNoopEvaluatorFactory implements TransformEvaluatorFactory {
  @Override
  public @Nullable <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) {
    return (TransformEvaluator<InputT>) new CreateViewNoopEvaluator(application);
  }

  @Override
  public void cleanup() {
    // noop
  }

  private static class CreateViewNoopEvaluator<InputT> implements TransformEvaluator<InputT> {
    private AppliedPTransform<?, ?, ?> transform;

    CreateViewNoopEvaluator(AppliedPTransform<?, ?, ?> transform) {
      this.transform = transform;
    }

    @Override
    public void processElement(WindowedValue<InputT> element) {}

    @Override
    public TransformResult<InputT> finishBundle() {
      return StepTransformResult.<InputT>withoutHold(transform).build();
    }
  }
}
