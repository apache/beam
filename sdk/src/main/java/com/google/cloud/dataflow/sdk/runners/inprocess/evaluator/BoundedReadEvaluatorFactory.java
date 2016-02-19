/*
 * Copyright (C) 2015 Google Inc.
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
package com.google.cloud.dataflow.sdk.runners.inprocess.evaluator;

import com.google.cloud.dataflow.sdk.io.Read.Bounded;
import com.google.cloud.dataflow.sdk.io.Source.Reader;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessEvaluationContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessTransformResult;
import com.google.cloud.dataflow.sdk.runners.inprocess.StepTransformResult;
import com.google.cloud.dataflow.sdk.runners.inprocess.TransformEvaluator;
import com.google.cloud.dataflow.sdk.runners.inprocess.TransformEvaluatorFactory;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Bounded Read.Bounded} primitive {@link PTransform}.
 */
public class BoundedReadEvaluatorFactory implements TransformEvaluatorFactory {
  /*
   * An evaluator for a Source is stateful, to ensure data is not read multiple times.
   * Evaluators are cached here to ensure that the reader is not restarted if the evaluator is
   * retriggered.
   */
  private final Map<EvaluatorKey, BoundedReadEvaluator<?>> sourceEvaluators =
      new ConcurrentHashMap<>();

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    return getTransformEvaluator((AppliedPTransform) application, evaluationContext);
  }

  private <OutputT> TransformEvaluator<?> getTransformEvaluator(
      final AppliedPTransform<?, PCollection<OutputT>, Bounded<OutputT>> transform,
      final InProcessEvaluationContext evaluationContext) {
    EvaluatorKey key = new EvaluatorKey(transform, evaluationContext);
    @SuppressWarnings("unchecked")
    BoundedReadEvaluator<OutputT> result =
        (BoundedReadEvaluator<OutputT>) sourceEvaluators.get(key);
    if (result == null) {
      try {
        result = new BoundedReadEvaluator<OutputT>(transform, evaluationContext);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      sourceEvaluators.put(key, result);
    }
    return result;
  }

  private static class BoundedReadEvaluator<OutputT> implements TransformEvaluator<Object> {
    private final AppliedPTransform<?, PCollection<OutputT>, Bounded<OutputT>> transform;
    private final InProcessEvaluationContext evaluationContext;
    private final Reader<OutputT> reader;
    private boolean contentsRemaining;

    public BoundedReadEvaluator(
        AppliedPTransform<?, PCollection<OutputT>, Bounded<OutputT>> transform,
        InProcessEvaluationContext evaluationContext)
        throws IOException {
      this.transform = transform;
      this.evaluationContext = evaluationContext;
      reader =
          transform.getTransform().getSource().createReader(evaluationContext.getPipelineOptions());
      contentsRemaining = reader.start();
    }

    @Override
    public void processElement(WindowedValue<Object> element) {}

    @Override
    public InProcessTransformResult finishBundle() throws IOException {
      UncommittedBundle<OutputT> output = evaluationContext.createRootBundle(transform.getOutput());
      while (contentsRemaining) {
        output.add(
            WindowedValue.timestampedValueInGlobalWindow(
                reader.getCurrent(), reader.getCurrentTimestamp()));
        contentsRemaining = reader.advance();
      }
      return StepTransformResult
          .withHold(transform, BoundedWindow.TIMESTAMP_MAX_VALUE)
          .addOutput(output)
          .build();
    }
  }
}

