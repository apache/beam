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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.PCollectionViewWriter;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.Values;
import com.google.cloud.dataflow.sdk.transforms.View.CreatePCollectionView;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the
 * {@link CreatePCollectionView} primitive {@link PTransform}.
 *
 * <p>The {@link ViewEvaluatorFactory} produces {@link TransformEvaluator TransformEvaluators} for
 * the {@link WriteView} {@link PTransform}, which is part of the
 * {@link InProcessCreatePCollectionView} composite transform. This transform is an override for the
 * {@link CreatePCollectionView} transform that applies windowing and triggers before the view is
 * written.
 */
class ViewEvaluatorFactory implements TransformEvaluatorFactory {
  @Override
  public <T> TransformEvaluator<T> forApplication(
      AppliedPTransform<?, ?, ?> application,
      InProcessPipelineRunner.CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator = (TransformEvaluator<T>) createEvaluator(
            (AppliedPTransform) application, evaluationContext);
    return evaluator;
  }

  private <InT, OuT> TransformEvaluator<Iterable<InT>> createEvaluator(
      final AppliedPTransform<PCollection<Iterable<InT>>, PCollectionView<OuT>, WriteView<InT, OuT>>
          application,
      InProcessEvaluationContext context) {
    PCollection<Iterable<InT>> input = application.getInput();
    final PCollectionViewWriter<InT, OuT> writer =
        context.createPCollectionViewWriter(input, application.getOutput());
    return new TransformEvaluator<Iterable<InT>>() {
      private final List<WindowedValue<InT>> elements = new ArrayList<>();

      @Override
      public void processElement(WindowedValue<Iterable<InT>> element) {
        for (InT input : element.getValue()) {
          elements.add(element.withValue(input));
        }
      }

      @Override
      public InProcessTransformResult finishBundle() {
        writer.add(elements);
        return StepTransformResult.withoutHold(application).build();
      }
    };
  }

  /**
   * An in-process override for {@link CreatePCollectionView}.
   */
  public static class InProcessCreatePCollectionView<ElemT, ViewT>
      extends PTransform<PCollection<ElemT>, PCollectionView<ViewT>> {
    private final CreatePCollectionView<ElemT, ViewT> og;

    private InProcessCreatePCollectionView(CreatePCollectionView<ElemT, ViewT> og) {
      this.og = og;
    }

    @Override
    public PCollectionView<ViewT> apply(PCollection<ElemT> input) {
      return input.apply(WithKeys.<Void, ElemT>of((Void) null))
          .setCoder(KvCoder.of(VoidCoder.of(), input.getCoder()))
          .apply(GroupByKey.<Void, ElemT>create())
          .apply(Values.<Iterable<ElemT>>create())
          .apply(new WriteView<ElemT, ViewT>(og));
    }
  }

  /**
   * An in-process implementation of the {@link CreatePCollectionView} primitive.
   *
   * This implementation requires the input {@link PCollection} to be an iterable, which is provided
   * to {@link PCollectionView#fromIterableInternal(Iterable)}.
   */
  public static final class WriteView<ElemT, ViewT>
      extends PTransform<PCollection<Iterable<ElemT>>, PCollectionView<ViewT>> {
    private final CreatePCollectionView<ElemT, ViewT> og;

    WriteView(CreatePCollectionView<ElemT, ViewT> og) {
      this.og = og;
    }

    @Override
    public PCollectionView<ViewT> apply(PCollection<Iterable<ElemT>> input) {
      return og.getView();
    }
  }
}
