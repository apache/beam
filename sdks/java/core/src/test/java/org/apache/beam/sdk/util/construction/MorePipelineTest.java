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
package org.apache.beam.sdk.util.construction;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.not;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Pipeline. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class MorePipelineTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testReplaceAllPCollectionView() {
    pipeline.enableAbandonedNodeEnforcement(false);
    pipeline.apply(GenerateSequence.from(0).to(100)).apply(View.asList());

    pipeline.replaceAll(
        ImmutableList.of(
            PTransformOverride.of(
                application -> application.getTransform() instanceof View.AsList,
                new ViewAsListOverride())));
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (!node.isRootNode()) {
              assertThat(
                  node.getTransform().getClass(), not(anyOf(Matchers.equalTo(View.AsList.class))));
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
  }

  static class ViewAsListOverride<T>
      extends SingleInputOutputOverrideFactory<
          PCollection<T>, PCollectionView<List<T>>, View.AsList<T>> {
    @Override
    public PTransformReplacement<PCollection<T>, PCollectionView<List<T>>> getReplacementTransform(
        AppliedPTransform<PCollection<T>, PCollectionView<List<T>>, View.AsList<T>> transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new FakeViewAsList<>(findPCollectionView(transform)));
    }
  }

  static class FakeViewAsList<T> extends PTransform<PCollection<T>, PCollectionView<List<T>>> {
    private final PCollectionView<List<T>> originalView;

    FakeViewAsList(PCollectionView<List<T>> originalView) {
      this.originalView = originalView;
    }

    @Override
    public PCollectionView<List<T>> expand(PCollection<T> input) {
      Coder<T> inputCoder = input.getCoder();
      PCollection<KV<Long, PCollectionViews.ValueOrMetadata<T, OffsetRange>>> materializationInput =
          input
              .apply("IndexElements", ParDo.of(new View.ToListViewDoFn<>()))
              .setCoder(
                  KvCoder.of(
                      BigEndianLongCoder.of(),
                      PCollectionViews.ValueOrMetadataCoder.create(
                          inputCoder, OffsetRange.Coder.of())));
      PCollectionView<List<T>> view =
          PCollectionViews.listViewWithRandomAccess(
              materializationInput,
              (TupleTag<
                      Materializations.MultimapView<
                          Long, PCollectionViews.ValueOrMetadata<T, OffsetRange>>>)
                  originalView.getTagInternal(),
              (PCollectionViews.TypeDescriptorSupplier<T>) inputCoder::getEncodedTypeDescriptor,
              materializationInput.getWindowingStrategy());
      materializationInput.apply(View.CreatePCollectionView.of(view));
      return view;
    }
  }

  private static <InputT, ViewT> PCollectionView<ViewT> findPCollectionView(
      final AppliedPTransform<
              PCollection<InputT>,
              PCollectionView<ViewT>,
              ? extends PTransform<PCollection<InputT>, PCollectionView<ViewT>>>
          transform) {
    final AtomicReference<PCollectionView<ViewT>> viewRef = new AtomicReference<>();
    transform
        .getPipeline()
        .traverseTopologically(
            new PipelineVisitor.Defaults() {
              // Stores whether we have entered the expected composite view transform.
              private boolean tracking = false;

              @Override
              public CompositeBehavior enterCompositeTransform(Node node) {
                if (transform.getTransform() == node.getTransform()) {
                  tracking = true;
                }
                return super.enterCompositeTransform(node);
              }

              @Override
              public void visitPrimitiveTransform(Node node) {
                if (tracking && node.getTransform() instanceof View.CreatePCollectionView) {
                  View.CreatePCollectionView createViewTransform =
                      (View.CreatePCollectionView) node.getTransform();
                  checkState(
                      viewRef.compareAndSet(null, createViewTransform.getView()),
                      "Found more than one instance of a CreatePCollectionView when"
                          + " attempting to replace %s, found [%s, %s]",
                      transform.getTransform(),
                      viewRef.get(),
                      createViewTransform.getView());
                }
              }

              @Override
              public void leaveCompositeTransform(Node node) {
                if (transform.getTransform() == node.getTransform()) {
                  tracking = false;
                }
              }
            });
    checkState(
        viewRef.get() != null,
        "Expected to find CreatePCollectionView contained within %s",
        transform.getTransform());
    return viewRef.get();
  }
}
