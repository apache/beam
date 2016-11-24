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

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
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
public class TransformHierarchyTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private TransformHierarchy hierarchy;
  private TestPipeline pipeline;

  @Before
  public void setup() {
    hierarchy = new TransformHierarchy();
    pipeline = TestPipeline.create();
  }

  @Test
  public void getCurrentNoPushReturnsRoot() {
    assertThat(hierarchy.getCurrent().isRootNode(), is(true));
  }

  @Test
  public void popWithoutPushThrows() {
    thrown.expect(IllegalStateException.class);
    hierarchy.popNode();
  }

  @Test
  public void pushThenPopSucceeds() {
    TransformTreeNode root = hierarchy.getCurrent();
    TransformTreeNode node =
        new TransformTreeNode(hierarchy.getCurrent(), Create.of(1), "Create", PBegin.in(pipeline));
    hierarchy.pushNode(node);
    assertThat(hierarchy.getCurrent(), equalTo(node));
    hierarchy.popNode();
    assertThat(hierarchy.getCurrent(), equalTo(root));
  }

  @Test
  public void visitVisitsAllPushed() {
    TransformTreeNode root = hierarchy.getCurrent();
    Create.Values<Integer> create = Create.of(1);
    PCollection<Integer> created = pipeline.apply(create);
    PBegin begin = PBegin.in(pipeline);

    TransformTreeNode compositeNode =
        new TransformTreeNode(root, create, "Create", begin);
    root.addComposite(compositeNode);
    TransformTreeNode primitiveNode =
        new TransformTreeNode(
            compositeNode, Read.from(CountingSource.upTo(1L)), "Create/Read", begin);
    compositeNode.addComposite(primitiveNode);

    TransformTreeNode otherPrimitive =
        new TransformTreeNode(
            root, MapElements.via(new SimpleFunction<Integer, Integer>() {
          @Override
          public Integer apply(Integer input) {
            return input;
          }
        }), "ParDo", created);
    root.addComposite(otherPrimitive);
    otherPrimitive.addInputProducer(created, primitiveNode);

    hierarchy.pushNode(compositeNode);
    hierarchy.pushNode(primitiveNode);
    hierarchy.popNode();
    hierarchy.popNode();
    hierarchy.pushNode(otherPrimitive);
    hierarchy.popNode();

    final Set<TransformTreeNode> visitedCompositeNodes = new HashSet<>();
    final Set<TransformTreeNode> visitedPrimitiveNodes = new HashSet<>();
    final Set<PValue> visitedValuesInVisitor = new HashSet<>();

    Set<PValue> visitedValues = new HashSet<>();
    hierarchy.visit(new PipelineVisitor.Defaults() {
      @Override
      public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
        visitedCompositeNodes.add(node);
        return CompositeBehavior.ENTER_TRANSFORM;
      }

      @Override
      public void visitPrimitiveTransform(TransformTreeNode node) {
        visitedPrimitiveNodes.add(node);
      }

      @Override
      public void visitValue(PValue value, TransformTreeNode producer) {
        visitedValuesInVisitor.add(value);
      }
    }, visitedValues);

    assertThat(visitedCompositeNodes, containsInAnyOrder(root, compositeNode));
    assertThat(visitedPrimitiveNodes, containsInAnyOrder(primitiveNode, otherPrimitive));
    assertThat(visitedValuesInVisitor, Matchers.<PValue>containsInAnyOrder(created));
  }
}
