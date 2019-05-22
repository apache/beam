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

import java.io.IOException;
import java.util.Map;
import org.apache.beam.runners.core.construction.CreatePCollectionViewTranslation;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link PTransformOverrideFactory} that provides overrides for the {@link CreatePCollectionView}
 * {@link PTransform}.
 */
class ViewOverrideFactory<ElemT, ViewT>
    implements PTransformOverrideFactory<
        PCollection<ElemT>,
        PCollection<ElemT>,
        PTransform<PCollection<ElemT>, PCollection<ElemT>>> {

  @Override
  public PTransformReplacement<PCollection<ElemT>, PCollection<ElemT>> getReplacementTransform(
      AppliedPTransform<
              PCollection<ElemT>,
              PCollection<ElemT>,
              PTransform<PCollection<ElemT>, PCollection<ElemT>>>
          transform) {

    PCollectionView<ViewT> view;
    try {
      view = CreatePCollectionViewTranslation.getView(transform);
    } catch (IOException exc) {
      throw new RuntimeException(
          String.format(
              "Could not extract %s from transform %s",
              PCollectionView.class.getSimpleName(), transform),
          exc);
    }

    return PTransformReplacement.of(
        PTransformReplacements.getSingletonMainInput(transform), new GroupAndWriteView<>(view));
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      Map<TupleTag<?>, PValue> outputs, PCollection<ElemT> newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }

  /** The {@link DirectRunner} composite override for {@link CreatePCollectionView}. */
  static class GroupAndWriteView<ElemT, ViewT>
      extends PTransform<PCollection<ElemT>, PCollection<ElemT>> {
    private final PCollectionView<ViewT> view;

    private GroupAndWriteView(PCollectionView<ViewT> view) {
      this.view = view;
    }

    @Override
    public PCollection<ElemT> expand(final PCollection<ElemT> input) {
      input
          .apply(WithKeys.of((Void) null))
          .setCoder(KvCoder.of(VoidCoder.of(), input.getCoder()))
          .apply(GroupByKey.create())
          .apply(Values.create())
          .apply(new WriteView<>(view));
      return input;
    }
  }

  /**
   * The {@link DirectRunner} implementation of the {@link CreatePCollectionView} primitive.
   *
   * <p>This implementation requires the input {@link PCollection} to be an iterable of {@code
   * WindowedValue<ElemT>}, which is provided to {@link PCollectionView#getViewFn()} for conversion
   * to {@link ViewT}.
   */
  static final class WriteView<ElemT, ViewT>
      extends PTransform<PCollection<Iterable<ElemT>>, PCollection<Iterable<ElemT>>> {
    private final PCollectionView<ViewT> view;

    WriteView(PCollectionView<ViewT> view) {
      this.view = view;
    }

    @Override
    @SuppressWarnings("deprecation")
    public PCollection<Iterable<ElemT>> expand(PCollection<Iterable<ElemT>> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), input.getCoder());
    }

    @SuppressWarnings("deprecation")
    public PCollectionView<ViewT> getView() {
      return view;
    }
  }

  public static final String DIRECT_WRITE_VIEW_URN = "beam:directrunner:transforms:write_view:v1";
}
