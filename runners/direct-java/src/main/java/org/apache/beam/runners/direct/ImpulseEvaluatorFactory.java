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

import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The evaluator for the {@link Impulse} transform. Produces only empty byte arrays. */
class ImpulseEvaluatorFactory implements TransformEvaluatorFactory {
  private final EvaluationContext ctxt;

  ImpulseEvaluatorFactory(EvaluationContext ctxt) {
    this.ctxt = ctxt;
  }

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) {
    return (TransformEvaluator<InputT>) new ImpulseEvaluator(ctxt, (AppliedPTransform) application);
  }

  @Override
  public void cleanup() {
    // Impulse has no state, so do nothing.
  }

  private static class ImpulseEvaluator implements TransformEvaluator<ImpulseShard> {
    private final EvaluationContext ctxt;
    private final AppliedPTransform<?, PCollection<byte[]>, Impulse> transform;
    private final StepTransformResult.Builder<ImpulseShard> result;

    private ImpulseEvaluator(
        EvaluationContext ctxt, AppliedPTransform<?, PCollection<byte[]>, Impulse> transform) {
      this.ctxt = ctxt;
      this.transform = transform;
      this.result = StepTransformResult.withoutHold(transform);
    }

    @Override
    public void processElement(WindowedValue<ImpulseShard> element) throws Exception {
      PCollection<byte[]> outputPCollection =
          (PCollection<byte[]>) Iterables.getOnlyElement(transform.getOutputs().values());
      result.addOutput(
          ctxt.createBundle(outputPCollection).add(WindowedValue.valueInGlobalWindow(new byte[0])));
    }

    @Override
    public TransformResult<ImpulseShard> finishBundle() throws Exception {
      return result.build();
    }
  }

  /**
   * The {@link RootInputProvider} for the {@link Impulse} {@link PTransform}. Produces a single
   * {@link ImpulseShard}.
   */
  static class ImpulseRootProvider implements RootInputProvider<byte[], ImpulseShard, PBegin> {
    private final EvaluationContext ctxt;

    ImpulseRootProvider(EvaluationContext ctxt) {
      this.ctxt = ctxt;
    }

    @Override
    public Collection<CommittedBundle<ImpulseShard>> getInitialInputs(
        AppliedPTransform<PBegin, PCollection<byte[]>, PTransform<PBegin, PCollection<byte[]>>>
            transform,
        int targetParallelism) {
      return Collections.singleton(
          ctxt.<ImpulseShard>createRootBundle()
              .add(WindowedValue.valueInGlobalWindow(new ImpulseShard()))
              .commit(BoundedWindow.TIMESTAMP_MIN_VALUE));
    }
  }

  @VisibleForTesting
  static class ImpulseShard {}
}
