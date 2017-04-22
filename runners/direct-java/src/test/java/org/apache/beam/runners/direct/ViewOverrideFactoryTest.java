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

import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.direct.ViewOverrideFactory.WriteView;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.hamcrest.Matchers;
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
  public void replacementSucceeds() {
    PCollection<Integer> ints = p.apply("CreateContents", Create.of(1, 2, 3));
    final PCollectionView<List<Integer>> view =
        PCollectionViews.listView(ints, WindowingStrategy.globalDefault(), ints.getCoder());
    PTransformReplacement<PCollection<Integer>, PCollectionView<List<Integer>>>
        replacementTransform =
            factory.getReplacementTransform(
                AppliedPTransform
                    .<PCollection<Integer>, PCollectionView<List<Integer>>,
                        CreatePCollectionView<Integer, List<Integer>>>
                        of(
                            "foo",
                            ints.expand(),
                            view.expand(),
                            CreatePCollectionView.<Integer, List<Integer>>of(view),
                            p));
    PCollectionView<List<Integer>> afterReplacement =
        ints.apply(replacementTransform.getTransform());
    assertThat(
        "The CreatePCollectionView replacement should return the same View",
        afterReplacement,
        equalTo(view));

    PCollection<Set<Integer>> outputViewContents =
        p.apply("CreateSingleton", Create.of(0))
            .apply(
                "OutputContents",
                ParDo.of(
                        new DoFn<Integer, Set<Integer>>() {
                          @ProcessElement
                          public void outputSideInput(ProcessContext context) {
                            context.output(ImmutableSet.copyOf(context.sideInput(view)));
                          }
                        })
                    .withSideInputs(view));
    PAssert.thatSingleton(outputViewContents).isEqualTo(ImmutableSet.of(1, 2, 3));

    p.run();
  }

  @Test
  public void replacementGetViewReturnsOriginal() {
    final PCollection<Integer> ints = p.apply("CreateContents", Create.of(1, 2, 3));
    final PCollectionView<List<Integer>> view =
        PCollectionViews.listView(ints, WindowingStrategy.globalDefault(), ints.getCoder());
    PTransformReplacement<PCollection<Integer>, PCollectionView<List<Integer>>> replacement =
        factory.getReplacementTransform(
            AppliedPTransform
                .<PCollection<Integer>, PCollectionView<List<Integer>>,
                    CreatePCollectionView<Integer, List<Integer>>>
                    of(
                        "foo",
                        ints.expand(),
                        view.expand(),
                        CreatePCollectionView.<Integer, List<Integer>>of(view),
                        p));
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
              PCollectionView replacementView = ((WriteView) node.getTransform()).getView();
              assertThat(replacementView, Matchers.<PCollectionView>theInstance(view));
              assertThat(node.getInputs().entrySet(), hasSize(1));
            }
          }
        });

    assertThat(writeViewVisited.get(), is(true));
  }
}
