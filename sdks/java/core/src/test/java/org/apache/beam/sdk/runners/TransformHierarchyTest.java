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
package org.apache.beam.sdk.runners;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link TransformHierarchy}.
 */
@RunWith(JUnit4.class)
public class TransformHierarchyTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private transient TransformHierarchy hierarchy;

  @Before
  public void setup() {
    hierarchy = new TransformHierarchy();
  }

  @Test
  public void getCurrentNoPushReturnsRoot() {
    assertThat(hierarchy.getCurrent().isRootNode(), is(true));
  }

  @Test
  public void pushWithoutPushFails() {
    thrown.expect(IllegalStateException.class);
    hierarchy.popNode();
  }

  @Test
  public void pushThenPopSucceeds() {
    TransformHierarchy.Node root = hierarchy.getCurrent();
    TransformHierarchy.Node node = hierarchy.pushNode("Create", PBegin.in(pipeline), Create.of(1));
    assertThat(hierarchy.getCurrent(), equalTo(node));
    hierarchy.popNode();
    assertThat(node.finishedSpecifying, is(true));
    assertThat(hierarchy.getCurrent(), equalTo(root));
  }

  @Test
  public void emptyCompositeSucceeds() {
    PCollection<Long> created =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);
    TransformHierarchy.Node node = hierarchy.pushNode("Create", PBegin.in(pipeline), Create.of(1));
    hierarchy.setOutput(created);
    hierarchy.popNode();
    PCollectionList<Long> pcList = PCollectionList.of(created);

    TransformHierarchy.Node emptyTransform =
        hierarchy.pushNode(
            "Extract",
            pcList,
            new PTransform<PCollectionList<Long>, PCollection<Long>>() {
              @Override
              public PCollection<Long> expand(PCollectionList<Long> input) {
                return input.get(0);
              }
            });
    hierarchy.setOutput(created);
    hierarchy.popNode();
    assertThat(hierarchy.getProducer(created), equalTo(node));
    assertThat(
        "A Transform that produces non-primtive output should be composite",
        emptyTransform.isCompositeNode(),
        is(true));
  }

  @Test
  public void producingOwnAndOthersOutputsFails() {
    PCollection<Long> created =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);
    hierarchy.pushNode("Create", PBegin.in(pipeline), Create.of(1));
    hierarchy.setOutput(created);
    hierarchy.popNode();
    PCollectionList<Long> pcList = PCollectionList.of(created);

    final PCollectionList<Long> appended =
        pcList.and(
            PCollection.<Long>createPrimitiveOutputInternal(
                pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED));
    hierarchy.pushNode(
        "AddPc",
        pcList,
        new PTransform<PCollectionList<Long>, PCollectionList<Long>>() {
          @Override
          public PCollectionList<Long> expand(PCollectionList<Long> input) {
            return appended;
          }
        });
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("produced by it as well as other Transforms");
    thrown.expectMessage("primitive transform must produce all of its outputs");
    thrown.expectMessage("composite transform must be produced by a component transform");
    thrown.expectMessage("AddPc");
    thrown.expectMessage("Create");
    thrown.expectMessage(appended.expand().toString());
    hierarchy.setOutput(appended);
  }

  @Test
  public void visitVisitsAllPushed() {
    TransformHierarchy.Node root = hierarchy.getCurrent();
    PBegin begin = PBegin.in(pipeline);

    Create.Values<Long> create = Create.of(1L);
    Read.Bounded<Long> read = Read.from(CountingSource.upTo(1L));

    PCollection<Long> created =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);

    ParDo.Bound<Long, Long> pardo =
        ParDo.of(
            new DoFn<Long, Long>() {
              @ProcessElement
              public void processElement(ProcessContext ctxt) {
                ctxt.output(ctxt.element());
              }
            });

    PCollection<Long> mapped =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);

    TransformHierarchy.Node compositeNode = hierarchy.pushNode("Create", begin, create);
    hierarchy.finishSpecifyingInput();
    assertThat(hierarchy.getCurrent(), equalTo(compositeNode));
    assertThat(compositeNode.getInputs(), Matchers.emptyIterable());
    assertThat(compositeNode.getTransform(), Matchers.<PTransform<?, ?>>equalTo(create));
    // Not yet set
    assertThat(compositeNode.getOutputs(), Matchers.emptyIterable());
    assertThat(compositeNode.getEnclosingNode().isRootNode(), is(true));

    TransformHierarchy.Node primitiveNode = hierarchy.pushNode("Create/Read", begin, read);
    assertThat(hierarchy.getCurrent(), equalTo(primitiveNode));
    hierarchy.finishSpecifyingInput();
    hierarchy.setOutput(created);
    hierarchy.popNode();
    assertThat(
        fromTaggedValues(primitiveNode.getOutputs()), Matchers.<PValue>containsInAnyOrder(created));
    assertThat(primitiveNode.getInputs(), Matchers.<TaggedPValue>emptyIterable());
    assertThat(primitiveNode.getTransform(), Matchers.<PTransform<?, ?>>equalTo(read));
    assertThat(primitiveNode.getEnclosingNode(), equalTo(compositeNode));

    hierarchy.setOutput(created);
    // The composite is listed as outputting a PValue created by the contained primitive
    assertThat(
        fromTaggedValues(compositeNode.getOutputs()), Matchers.<PValue>containsInAnyOrder(created));
    // The producer of that PValue is still the primitive in which it is first output
    assertThat(hierarchy.getProducer(created), equalTo(primitiveNode));
    hierarchy.popNode();

    TransformHierarchy.Node otherPrimitive = hierarchy.pushNode("ParDo", created, pardo);
    hierarchy.finishSpecifyingInput();
    hierarchy.setOutput(mapped);
    hierarchy.popNode();

    final Set<TransformHierarchy.Node> visitedCompositeNodes = new HashSet<>();
    final Set<TransformHierarchy.Node> visitedPrimitiveNodes = new HashSet<>();
    final Set<PValue> visitedValuesInVisitor = new HashSet<>();

    Set<PValue> visitedValues =
        hierarchy.visit(
            new PipelineVisitor.Defaults() {
              @Override
              public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
                visitedCompositeNodes.add(node);
                return CompositeBehavior.ENTER_TRANSFORM;
              }

              @Override
              public void visitPrimitiveTransform(TransformHierarchy.Node node) {
                visitedPrimitiveNodes.add(node);
              }

              @Override
              public void visitValue(PValue value, TransformHierarchy.Node producer) {
                visitedValuesInVisitor.add(value);
              }
            });

    assertThat(visitedCompositeNodes, containsInAnyOrder(root, compositeNode));
    assertThat(visitedPrimitiveNodes, containsInAnyOrder(primitiveNode, otherPrimitive));
    assertThat(visitedValuesInVisitor, Matchers.<PValue>containsInAnyOrder(created, mapped));
    assertThat(visitedValuesInVisitor, equalTo(visitedValues));
  }

  private static List<PValue> fromTaggedValues(List<TaggedPValue> taggedValues) {
    return Lists.transform(
        taggedValues,
        new Function<TaggedPValue, PValue>() {
          @Override
          public PValue apply(TaggedPValue input) {
            return input.getValue();
          }
        });
  }
}
