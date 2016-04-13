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
package com.google.cloud.dataflow.sdk.runners;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.Write;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.Sample;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PValue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.util.Arrays;
import java.util.EnumSet;

/**
 * Tests for {@link TransformTreeNode} and {@link TransformHierarchy}.
 */
@RunWith(JUnit4.class)
public class TransformTreeTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  enum TransformsSeen {
    READ,
    WRITE,
    SAMPLE_ANY
  }

  /**
   * INVALID TRANSFORM, DO NOT COPY.
   *
   * <p>This is an invalid composite transform that returns unbound outputs.
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
              b.getPipeline(),
              WindowingStrategy.globalDefault(),
              result.isBounded())));
    }
  }

  /**
   * A composite transform that returns an output that is unbound.
   */
  private static class UnboundOutputCreator
      extends PTransform<PCollection<Integer>, PDone> {

    @Override
    public PDone apply(PCollection<Integer> input) {
      // Apply an operation so that this is a composite transform.
      input.apply(Count.<Integer>perElement());

      return PDone.in(input.getPipeline());
    }

    @Override
    protected Coder<?> getDefaultOutputCoder() {
      return VoidCoder.of();
    }
  }

  // Builds a pipeline containing a composite operation (Pick), then
  // visits the nodes and verifies that the hierarchy was captured.
  @Test
  public void testCompositeCapture() throws Exception {
    File inputFile = tmpFolder.newFile();
    File outputFile = tmpFolder.newFile();

    Pipeline p = DirectPipeline.createForTest();

    p.apply(TextIO.Read.named("ReadMyFile").from(inputFile.getPath()))
        .apply(Sample.<String>any(10))
        .apply(TextIO.Write.named("WriteMyFile").to(outputFile.getPath()));

    final EnumSet<TransformsSeen> visited =
        EnumSet.noneOf(TransformsSeen.class);
    final EnumSet<TransformsSeen> left =
        EnumSet.noneOf(TransformsSeen.class);

    p.traverseTopologically(new Pipeline.PipelineVisitor() {
      @Override
      public void enterCompositeTransform(TransformTreeNode node) {
        PTransform<?, ?> transform = node.getTransform();
        if (transform instanceof Sample.SampleAny) {
          assertTrue(visited.add(TransformsSeen.SAMPLE_ANY));
          assertNotNull(node.getEnclosingNode());
          assertTrue(node.isCompositeNode());
        } else if (transform instanceof Write.Bound) {
          assertTrue(visited.add(TransformsSeen.WRITE));
          assertNotNull(node.getEnclosingNode());
          assertTrue(node.isCompositeNode());
        }
        assertThat(transform, not(instanceOf(Read.Bounded.class)));
      }

      @Override
      public void leaveCompositeTransform(TransformTreeNode node) {
        PTransform<?, ?> transform = node.getTransform();
        if (transform instanceof Sample.SampleAny) {
          assertTrue(left.add(TransformsSeen.SAMPLE_ANY));
        }
      }

      @Override
      public void visitTransform(TransformTreeNode node) {
        PTransform<?, ?> transform = node.getTransform();
        // Pick is a composite, should not be visited here.
        assertThat(transform, not(instanceOf(Sample.SampleAny.class)));
        assertThat(transform, not(instanceOf(Write.Bound.class)));
        if (transform instanceof Read.Bounded) {
          assertTrue(visited.add(TransformsSeen.READ));
        }
      }

      @Override
      public void visitValue(PValue value, TransformTreeNode producer) {
      }
    });

    assertTrue(visited.equals(EnumSet.allOf(TransformsSeen.class)));
    assertTrue(left.equals(EnumSet.of(TransformsSeen.SAMPLE_ANY)));
  }

  @Test(expected = IllegalStateException.class)
  public void testOutputChecking() throws Exception {
    Pipeline p = DirectPipeline.createForTest();

    p.apply(new InvalidCompositeTransform());

    p.traverseTopologically(new RecordingPipelineVisitor());
    fail("traversal should have failed with an IllegalStateException");
  }

  @Test
  public void testMultiGraphSetup() {
    Pipeline p = DirectPipeline.createForTest();

    PCollection<Integer> input = p.begin()
        .apply(Create.of(1, 2, 3));

    input.apply(new UnboundOutputCreator());

    p.run();
  }
}
