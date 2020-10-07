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

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Adds a {@link DirectRunner}-specific {@link WriteView} step for each {@link PCollectionView} for
 * scheduling materialization of side inputs.
 */
class DirectWriteViewVisitor extends PipelineVisitor.Defaults {

  /** Private URN for identifying {@link DirectRunner}-specific view writing transform. */
  static final String DIRECT_WRITE_VIEW_URN = "beam:directrunner:transforms:write_view:v1";

  private Set<PCollectionView<?>> viewsToWrite;

  @Override
  public void enterPipeline(Pipeline p) {
    viewsToWrite = new HashSet<>();
  }

  @Override
  public void leavePipeline(Pipeline p) {
    // This modifies the transform hierarchy so cannot be performed until the traversal is complete
    for (PCollectionView<?> view : viewsToWrite) {
      visitView(view);
    }
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    if (node.getTransform() instanceof ParDo.MultiOutput) {
      ParDo.MultiOutput<?, ?> parDo = (ParDo.MultiOutput<?, ?>) node.getTransform();
      viewsToWrite.addAll(parDo.getSideInputs().values());
    }
  }

  private <ElemT, ViewT> void visitView(PCollectionView<ViewT> view) {
    PCollection<ElemT> collectionToMaterialize = (PCollection<ElemT>) view.getPCollection();
    collectionToMaterialize.apply("GroupAndWriteView", new GroupAndWriteView<>(view));
  }

  /**
   * The {@link DirectRunner} composite for materializing each side input {@link PCollectionView}.
   */
  private static class GroupAndWriteView<ElemT, ViewT>
      extends PTransform<PCollection<ElemT>, PCollection<ElemT>> {
    private final PCollectionView<ViewT> view;

    private GroupAndWriteView(PCollectionView<ViewT> view) {
      this.view = view;
    }

    @Override
    public PCollection<ElemT> expand(final PCollection<ElemT> input) {
      input
          .apply("Key by null", WithKeys.of((Void) null))
          .setCoder(KvCoder.of(VoidCoder.of(), input.getCoder()))
          .apply("GBK", GroupByKey.create())
          .apply("Get values", Values.create())
          .apply("WriteView", new WriteView<>(view));
      return input;
    }
  }

  /**
   * The {@link DirectRunner}-specific primitive step for materializing a side input.
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
}
