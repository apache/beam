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
package org.apache.beam.runners.samza.translation;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Samza override for {@link View} (side input) transforms. */
class SamzaPublishViewTransformOverride<ElemT, ViewT>
    extends SingleInputOutputOverrideFactory<
        PCollection<ElemT>, PCollection<ElemT>, View.CreatePCollectionView<ElemT, ViewT>> {
  @Override
  public PTransformReplacement<PCollection<ElemT>, PCollection<ElemT>> getReplacementTransform(
      AppliedPTransform<
              PCollection<ElemT>, PCollection<ElemT>, View.CreatePCollectionView<ElemT, ViewT>>
          transform) {

    @SuppressWarnings("unchecked")
    PCollection<ElemT> input =
        (PCollection<ElemT>) Iterables.getOnlyElement(transform.getInputs().values());

    return PTransformReplacement.of(
        input, new SamzaCreatePCollectionViewTransform<>(transform.getTransform().getView()));
  }

  private static class SamzaCreatePCollectionViewTransform<ElemT, ViewT>
      extends PTransform<PCollection<ElemT>, PCollection<ElemT>> {
    private final PCollectionView<ViewT> view;

    public SamzaCreatePCollectionViewTransform(PCollectionView<ViewT> view) {
      this.view = view;
    }

    @Override
    public PCollection<ElemT> expand(PCollection<ElemT> input) {
      // This actually creates a branch in the graph that publishes the view but then returns
      // the original input. This is copied from the Flink runner.
      input
          .apply(Combine.globally(new Concatenate<ElemT>()).withoutDefaults())
          .apply(new SamzaPublishView<>(view));
      return input;
    }
  }

  private static class Concatenate<T> extends Combine.CombineFn<T, List<T>, List<T>> {
    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
      List<T> result = createAccumulator();
      for (List<T> accumulator : accumulators) {
        result.addAll(accumulator);
      }
      return result;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }

    @Override
    public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }
  }
}
