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

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessEvaluationContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.PCollectionViewWriter;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessTransformResult;
import com.google.cloud.dataflow.sdk.runners.inprocess.StepTransformResult;
import com.google.cloud.dataflow.sdk.runners.inprocess.TransformEvaluator;
import com.google.cloud.dataflow.sdk.runners.inprocess.TransformEvaluatorFactory;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.View.CreatePCollectionView;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the
 * {@link CreatePCollectionView} primitive {@link PTransform}.
 */
public class ViewEvaluatorFactory implements TransformEvaluatorFactory {
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public <T> TransformEvaluator<T> forApplication(
      AppliedPTransform<?, ?, ?> application,
      InProcessPipelineRunner.CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    return createEvaluator(
        (AppliedPTransform) application, evaluationContext);
  }

  private <InT, OuT> TransformEvaluator<InT> createEvaluator(
      final AppliedPTransform<PCollection<InT>, PCollectionView<OuT>,
      CreatePCollectionView<InT, OuT>> application,
      InProcessEvaluationContext context) {
    PCollection<InT> input = application.getInput();
    final PCollectionViewWriter<InT, OuT> writer =
        context.createPCollectionViewWriter(input, application.getOutput());
    return new TransformEvaluator<InT>() {
      private final List<WindowedValue<InT>> elements = new ArrayList<>();

      @Override
      public void processElement(WindowedValue<InT> element) {
        elements.add(element);
      }

      @Override
      public InProcessTransformResult finishBundle() {
        writer.add(elements);
        return StepTransformResult.withoutHold(application).build();
      }
    };
  }
}

