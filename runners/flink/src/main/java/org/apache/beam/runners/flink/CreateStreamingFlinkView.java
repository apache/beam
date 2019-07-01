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
package org.apache.beam.runners.flink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.CreatePCollectionViewTranslation;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;

/** Flink streaming overrides for various view (side input) transforms. */
class CreateStreamingFlinkView<ElemT, ViewT>
    extends PTransform<PCollection<ElemT>, PCollection<ElemT>> {
  private final PCollectionView<ViewT> view;

  public static final String CREATE_STREAMING_FLINK_VIEW_URN =
      "beam:transform:flink:create-streaming-flink-view:v1";

  public CreateStreamingFlinkView(PCollectionView<ViewT> view) {
    this.view = view;
  }

  @Override
  public PCollection<ElemT> expand(PCollection<ElemT> input) {
    input
        .apply(Combine.globally(new Concatenate<ElemT>()).withoutDefaults())
        .apply(CreateFlinkPCollectionView.of(view));
    return input;
  }

  /**
   * Combiner that combines {@code T}s into a single {@code List<T>} containing all inputs.
   *
   * <p>For internal use by {@link CreateStreamingFlinkView}. This combiner requires that the input
   * {@link PCollection} fits in memory. For a large {@link PCollection} this is expected to crash!
   *
   * @param <T> the type of elements to concatenate.
   */
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

  /**
   * Creates a primitive {@link PCollectionView}.
   *
   * <p>For internal use only by runner implementors.
   *
   * @param <ElemT> The type of the elements of the input PCollection
   * @param <ViewT> The type associated with the {@link PCollectionView} used as a side input
   */
  public static class CreateFlinkPCollectionView<ElemT, ViewT>
      extends PTransform<PCollection<List<ElemT>>, PCollection<List<ElemT>>> {
    private PCollectionView<ViewT> view;

    private CreateFlinkPCollectionView(PCollectionView<ViewT> view) {
      this.view = view;
    }

    public static <ElemT, ViewT> CreateFlinkPCollectionView<ElemT, ViewT> of(
        PCollectionView<ViewT> view) {
      return new CreateFlinkPCollectionView<>(view);
    }

    @Override
    public PCollection<List<ElemT>> expand(PCollection<List<ElemT>> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), input.getCoder());
    }

    public PCollectionView<ViewT> getView() {
      return view;
    }
  }

  public static class Factory<ElemT, ViewT>
      implements PTransformOverrideFactory<
          PCollection<ElemT>,
          PCollection<ElemT>,
          PTransform<PCollection<ElemT>, PCollection<ElemT>>> {

    static final Factory INSTANCE = new Factory();

    private Factory() {}

    @Override
    public PTransformReplacement<PCollection<ElemT>, PCollection<ElemT>> getReplacementTransform(
        AppliedPTransform<
                PCollection<ElemT>,
                PCollection<ElemT>,
                PTransform<PCollection<ElemT>, PCollection<ElemT>>>
            transform) {
      PCollection<ElemT> collection =
          (PCollection<ElemT>) Iterables.getOnlyElement(transform.getInputs().values());
      PCollectionView<ViewT> view;
      try {
        view = CreatePCollectionViewTranslation.getView(transform);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      CreateStreamingFlinkView<ElemT, ViewT> createFlinkView = new CreateStreamingFlinkView<>(view);
      return PTransformReplacement.of(collection, createFlinkView);
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<ElemT> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }
}
