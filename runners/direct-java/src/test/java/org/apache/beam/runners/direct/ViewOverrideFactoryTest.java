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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.direct.ViewOverrideFactory.WriteView;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ViewOverrideFactory}. */
@RunWith(JUnit4.class)
public class ViewOverrideFactoryTest implements Serializable {
  @Rule
  public transient TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  private transient ViewOverrideFactory<Integer, List<Integer>> factory =
      new ViewOverrideFactory<>();

  @Test
  public void replacementGetViewReturnsOriginal() {
    final PCollection<Integer> ints = p.apply("CreateContents", Create.of(1, 2, 3));
    final PCollectionView<List<Integer>> view = ints.apply(View.asList());
    PTransformReplacement<PCollection<Integer>, PCollection<Integer>> replacement =
        factory.getReplacementTransform(
            AppliedPTransform.of(
                "foo", ints.expand(), view.expand(), CreatePCollectionView.of(view), p));
    ints.apply(replacement.getTransform());
    final AtomicBoolean writeViewVisited = new AtomicBoolean();
    p.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void visitPrimitiveTransform(Node node) {
            if (node.getTransform() instanceof WriteView) {
              assertThat(
                  "There should only be one WriteView primitive in the graph",
                  writeViewVisited.getAndSet(true),
                  is(false));
              PCollectionView<?> replacementView = ((WriteView) node.getTransform()).getView();

              // replacementView.getPCollection() is null, but that is not a requirement
              // so not asserted one way or the other
              assertThat(
                  replacementView.getTagInternal(), equalTo((TupleTag) view.getTagInternal()));
              assertThat(replacementView.getViewFn(), equalTo(view.getViewFn()));
              assertThat(replacementView.getWindowMappingFn(), equalTo(view.getWindowMappingFn()));
              assertThat(node.getInputs().entrySet(), hasSize(1));
            }
          }
        });

    assertThat(writeViewVisited.get(), is(true));
  }
}
