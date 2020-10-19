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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.direct.CommittedResult.OutputType;
import org.apache.beam.runners.direct.DirectWriteViewVisitor.WriteView;
import org.apache.beam.runners.direct.StepTransformResult.Builder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the {@link DirectRunner-specific}
 * {@link WriteView} step.
 */
class ViewEvaluatorFactory implements TransformEvaluatorFactory {
  private final EvaluationContext context;

  ViewEvaluatorFactory(EvaluationContext context) {
    this.context = context;
  }

  @Override
  public <T> TransformEvaluator<T> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator = createEvaluator((AppliedPTransform) application);
    return evaluator;
  }

  @Override
  public void cleanup() throws Exception {}

  private <InT, OuT> TransformEvaluator<Iterable<InT>> createEvaluator(
      final AppliedPTransform<
              PCollection<Iterable<InT>>, PCollection<Iterable<InT>>, WriteView<InT, OuT>>
          application) {
    PCollection<Iterable<InT>> input =
        (PCollection<Iterable<InT>>) Iterables.getOnlyElement(application.getInputs().values());
    final PCollectionViewWriter<InT, OuT> writer =
        context.createPCollectionViewWriter(input, application.getTransform().getView());
    return new TransformEvaluator<Iterable<InT>>() {
      private final List<WindowedValue<InT>> elements = new ArrayList<>();

      @Override
      public void processElement(WindowedValue<Iterable<InT>> element) {
        for (InT input : element.getValue()) {
          elements.add(element.withValue(input));
        }
      }

      @Override
      public TransformResult<Iterable<InT>> finishBundle() {
        writer.add(elements);
        Builder resultBuilder = StepTransformResult.withoutHold(application);
        if (!elements.isEmpty()) {
          resultBuilder = resultBuilder.withAdditionalOutput(OutputType.PCOLLECTION_VIEW);
        }
        return resultBuilder.build();
      }
    };
  }
}
