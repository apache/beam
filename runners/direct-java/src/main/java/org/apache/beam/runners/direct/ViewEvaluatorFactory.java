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

import org.apache.beam.runners.direct.DirectRunner.PCollectionViewWriter;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the
 * {@link CreatePCollectionView} primitive {@link PTransform}.
 *
 * <p>The {@link ViewEvaluatorFactory} produces {@link TransformEvaluator TransformEvaluators} for
 * the {@link WriteView} {@link PTransform}, which is part of the
 * {@link DirectCreatePCollectionView} composite transform. This transform is an override for the
 * {@link CreatePCollectionView} transform that applies windowing and triggers before the view is
 * written.
 */
class ViewEvaluatorFactory implements TransformEvaluatorFactory {
  @Override
  public <T> TransformEvaluator<T> forApplication(
      AppliedPTransform<?, ?, ?> application,
      DirectRunner.CommittedBundle<?> inputBundle,
      EvaluationContext evaluationContext) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator = createEvaluator(
            (AppliedPTransform) application, evaluationContext);
    return evaluator;
  }

  private <InT, OuT> TransformEvaluator<Iterable<InT>> createEvaluator(
      final AppliedPTransform<PCollection<Iterable<InT>>, PCollectionView<OuT>, WriteView<InT, OuT>>
          application,
      EvaluationContext context) {
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
      public TransformResult finishBundle() {
        writer.add(elements);
        return StepTransformResult.withoutHold(application)
            .withAdditionalOutput(!elements.isEmpty())
            .build();
      }
    };
  }

  public static class ViewOverrideFactory implements PTransformOverrideFactory {
    @Override
    public <InputT extends PInput, OutputT extends POutput>
        PTransform<InputT, OutputT> override(PTransform<InputT, OutputT> transform) {
      if (transform instanceof CreatePCollectionView) {

      }
      @SuppressWarnings({"rawtypes", "unchecked"})
      PTransform<InputT, OutputT> createView =
          (PTransform<InputT, OutputT>)
              new DirectCreatePCollectionView<>((CreatePCollectionView) transform);
      return createView;
    }
  }

  /**
   * An in-process override for {@link CreatePCollectionView}.
   */
  private static class DirectCreatePCollectionView<ElemT, ViewT>
      extends ForwardingPTransform<PCollection<ElemT>, PCollectionView<ViewT>> {
    private final CreatePCollectionView<ElemT, ViewT> og;

    private DirectCreatePCollectionView(CreatePCollectionView<ElemT, ViewT> og) {
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

    @Override
    protected PTransform<PCollection<ElemT>, PCollectionView<ViewT>> delegate() {
      return og;
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
