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
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.Concatenate;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.construction.CreatePCollectionViewTranslation;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

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
    PCollection<List<ElemT>> iterable;
    // See https://github.com/apache/beam/pull/25940
    if (view.getViewFn() instanceof PCollectionViews.IsSingletonView) {
      TypeDescriptor<ElemT> inputType = Preconditions.checkStateNotNull(input.getTypeDescriptor());
      iterable =
          input
              .apply(MapElements.into(TypeDescriptors.lists(inputType)).via(Lists::newArrayList))
              .setCoder(ListCoder.of(input.getCoder()));
    } else {
      iterable = input.apply(Combine.globally(new Concatenate<ElemT>()).withoutDefaults());
    }

    iterable.apply(CreateFlinkPCollectionView.of(view));
    return input;
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

    static final Factory<?, ?> INSTANCE = new Factory<>();

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
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<ElemT> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }
}
