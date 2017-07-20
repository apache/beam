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

import java.util.Collections;
import java.util.Map;
import org.apache.beam.runners.core.construction.ForwardingPTransform;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.PTransformTranslation.RawPTransform;
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
        PCollection<ElemT>, PCollectionView<ViewT>, CreatePCollectionView<ElemT, ViewT>> {

  @Override
  public PTransformReplacement<PCollection<ElemT>, PCollectionView<ViewT>> getReplacementTransform(
      AppliedPTransform<
              PCollection<ElemT>, PCollectionView<ViewT>, CreatePCollectionView<ElemT, ViewT>>
          transform) {
    return PTransformReplacement.of(
        PTransformReplacements.getSingletonMainInput(transform),
        new GroupAndWriteView<>(transform.getTransform()));
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      Map<TupleTag<?>, PValue> outputs, PCollectionView<ViewT> newOutput) {
    return Collections.emptyMap();
  }

  /** The {@link DirectRunner} composite override for {@link CreatePCollectionView}. */
  static class GroupAndWriteView<ElemT, ViewT>
      extends ForwardingPTransform<PCollection<ElemT>, PCollectionView<ViewT>> {
    private final CreatePCollectionView<ElemT, ViewT> og;

    private GroupAndWriteView(CreatePCollectionView<ElemT, ViewT> og) {
      this.og = og;
    }

    @Override
    public PCollectionView<ViewT> expand(PCollection<ElemT> input) {
      return input
          .apply(WithKeys.<Void, ElemT>of((Void) null))
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
   * The {@link DirectRunner} implementation of the {@link CreatePCollectionView} primitive.
   *
   * <p>This implementation requires the input {@link PCollection} to be an iterable of {@code
   * WindowedValue<ElemT>}, which is provided to {@link PCollectionView#getViewFn()} for conversion
   * to {@link ViewT}.
   */
  static final class WriteView<ElemT, ViewT>
      extends RawPTransform<PCollection<Iterable<ElemT>>, PCollectionView<ViewT>> {
    private final CreatePCollectionView<ElemT, ViewT> og;

    WriteView(CreatePCollectionView<ElemT, ViewT> og) {
      this.og = og;
    }

    @Override
    @SuppressWarnings("deprecation")
    public PCollectionView<ViewT> expand(PCollection<Iterable<ElemT>> input) {
      return og.getView();
    }

    @SuppressWarnings("deprecation")
    public PCollectionView<ViewT> getView() {
      return og.getView();
    }

    @Override
    public String getUrn() {
      return DIRECT_WRITE_VIEW_URN;
    }
  }

  public static final String DIRECT_WRITE_VIEW_URN =
      "urn:beam:directrunner:transforms:write_view:v1";
}
