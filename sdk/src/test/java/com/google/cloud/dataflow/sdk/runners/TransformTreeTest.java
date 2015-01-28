/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.First;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PValue;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;

/**
 * Tests for {@link TransformTreeNode} and {@link TransformHierarchy}.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class TransformTreeTest {

  enum TransformsSeen {
    READ,
    WRITE,
    FIRST
  }

  /**
   * INVALID TRANSFORM, DO NOT COPY.
   *
   * <p> This is an invalid composite transform, which returns unbound outputs.
   * This should never happen, and is here to test that it is properly rejected.
   */
  private static class InvalidCompositeTransform
      extends PTransform<PBegin, PCollectionList<String>> {

    @Override
    public PCollectionList<String> apply(PBegin b) {
      // Composite transform: apply delegates to other transformations,
      // here a Create transform.
      PCollection<String> result = b.apply(Create.of("hello", "world"));

      // Issue below: PCollection.createPrimitiveOutput should not be used
      // from within a composite transform.
      return PCollectionList.of(
          Arrays.asList(result, PCollection.<String>createPrimitiveOutputInternal(
              new GlobalWindows())));
    }
  }

  /**
   * A composite transform which returns an output which is unbound.
   */
  private static class UnboundOutputCreator
      extends PTransform<PCollection<Integer>, PDone> {

    @Override
    public PDone apply(PCollection<Integer> input) {
      // Apply an operation so that this is a composite transform.
      input.apply(Count.<Integer>perElement());

      return new PDone();
    }

    @Override
    protected Coder<?> getDefaultOutputCoder() {
      return VoidCoder.of();
    }
  }

  // Builds a pipeline containing a composite operation (First), then
  // visits the nodes and verifies that the hierarchy was captured.
  @Test
  public void testCompositeCapture() throws Exception {
    Pipeline p = DirectPipeline.createForTest();

    p.apply(TextIO.Read.named("ReadMyFile").from("gs://bucket/object"))
        .apply(First.<String>of(10))
        .apply(TextIO.Write.named("WriteMyFile").to("gs://bucket/object"));

    final EnumSet<TransformsSeen> visited =
        EnumSet.noneOf(TransformsSeen.class);
    final EnumSet<TransformsSeen> left =
        EnumSet.noneOf(TransformsSeen.class);

    p.traverseTopologically(new Pipeline.PipelineVisitor() {
      @Override
      public void enterCompositeTransform(TransformTreeNode node) {
        PTransform<?, ?> transform = node.getTransform();
        if (transform instanceof First) {
          Assert.assertTrue(visited.add(TransformsSeen.FIRST));
          Assert.assertNotNull(node.getEnclosingNode());
          Assert.assertTrue(node.isCompositeNode());
        }
        Assert.assertThat(transform, not(instanceOf(TextIO.Read.Bound.class)));
        Assert.assertThat(transform, not(instanceOf(TextIO.Write.Bound.class)));
      }

      @Override
      public void leaveCompositeTransform(TransformTreeNode node) {
        PTransform<?, ?> transform = node.getTransform();
        if (transform instanceof First) {
          Assert.assertTrue(left.add(TransformsSeen.FIRST));
        }
      }

      @Override
      public void visitTransform(TransformTreeNode node) {
        PTransform<?, ?> transform = node.getTransform();
        // First is a composite, should not be visited here.
        Assert.assertThat(transform, not(instanceOf(First.class)));
        if (transform instanceof TextIO.Read.Bound) {
          Assert.assertTrue(visited.add(TransformsSeen.READ));
        } else if (transform instanceof TextIO.Write.Bound) {
          Assert.assertTrue(visited.add(TransformsSeen.WRITE));
        }
      }

      @Override
      public void visitValue(PValue value, TransformTreeNode producer) {
      }
    });

    Assert.assertTrue(visited.equals(EnumSet.allOf(TransformsSeen.class)));
    Assert.assertTrue(left.equals(EnumSet.of(TransformsSeen.FIRST)));
  }

  @Test(expected = IllegalStateException.class)
  public void testOutputChecking() throws Exception {
    Pipeline p = DirectPipeline.createForTest();

    p.apply(new InvalidCompositeTransform());

    p.traverseTopologically(new RecordingPipelineVisitor());
    Assert.fail("traversal should have failed with an IllegalStateException");
  }

  @Test
  public void testMultiGraphSetup() throws IOException {
    Pipeline p = DirectPipeline.createForTest();

    PCollection<Integer> input = p.begin()
        .apply(Create.of(1, 2, 3));

    input.apply(new UnboundOutputCreator());

    p.run();
  }
}
