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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.EnumSet;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TransformHierarchy.Node} and {@link TransformHierarchy}. */
@RunWith(JUnit4.class)
public class TransformTreeTest {

  @Rule public final TestPipeline p = TestPipeline.create();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  enum TransformsSeen {
    IMPULSE,
    WRITE,
    SAMPLE
  }

  /**
   * INVALID TRANSFORM, DO NOT COPY.
   *
   * <p>This is an invalid composite transform that returns unbound outputs. This should never
   * happen, and is here to test that it is properly rejected.
   */
  private static class InvalidCompositeTransform
      extends PTransform<PBegin, PCollectionList<String>> {

    @Override
    public PCollectionList<String> expand(PBegin b) {
      // Composite transform: apply delegates to other transformations,
      // here a Create transform.
      PCollection<String> result = b.apply(Create.of("hello", "world"));

      // Issue below: PCollection.createPrimitiveOutput should not be used
      // from within a composite transform.
      return PCollectionList.of(
          Arrays.asList(
              result,
              PCollection.createPrimitiveOutputInternal(
                  b.getPipeline(),
                  WindowingStrategy.globalDefault(),
                  result.isBounded(),
                  StringUtf8Coder.of())));
    }
  }

  /** A composite transform that returns an output that is unbound. */
  private static class UnboundOutputCreator extends PTransform<PCollection<Integer>, PDone> {

    @Override
    public PDone expand(PCollection<Integer> input) {
      // Apply an operation so that this is a composite transform.
      input.apply(Count.perElement());

      return PDone.in(input.getPipeline());
    }
  }

  @Test
  public void testCompositeCapture() throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    File inputFile = tmpFolder.newFile();
    File outputFile = tmpFolder.newFile();

    final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample =
        Sample.fixedSizeGlobally(10);
    p.apply("ReadMyFile", TextIO.read().from(inputFile.getPath()))
        .apply(sample)
        .apply(Flatten.iterables())
        .apply("WriteMyFile", TextIO.write().to(outputFile.getPath()));

    final EnumSet<TransformsSeen> visited = EnumSet.noneOf(TransformsSeen.class);
    final EnumSet<TransformsSeen> left = EnumSet.noneOf(TransformsSeen.class);

    p.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            if (node.isRootNode()) {
              return CompositeBehavior.ENTER_TRANSFORM;
            }
            PTransform<?, ?> transform = node.getTransform();
            if (sample.getClass().equals(transform.getClass())) {
              assertTrue(visited.add(TransformsSeen.SAMPLE));
              assertNotNull(node.getEnclosingNode());
              assertTrue(node.isCompositeNode());
            } else if (transform instanceof WriteFiles) {
              assertTrue(visited.add(TransformsSeen.WRITE));
              assertNotNull(node.getEnclosingNode());
              assertTrue(node.isCompositeNode());
            }
            assertThat(transform, not(instanceOf(Impulse.class)));
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void leaveCompositeTransform(TransformHierarchy.Node node) {
            PTransform<?, ?> transform = node.getTransform();
            if (!node.isRootNode() && transform.getClass().equals(sample.getClass())) {
              assertTrue(left.add(TransformsSeen.SAMPLE));
            }
          }

          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            PTransform<?, ?> transform = node.getTransform();
            // Composites should not be visited here.
            assertThat(transform, not(instanceOf(Combine.Globally.class)));
            assertThat(transform, not(instanceOf(WriteFiles.class)));
            assertThat(transform, not(instanceOf(TextIO.Read.class)));
            // There are multiple impulses in the graph so we don't validate that we haven't
            // seen one before.
            visited.add(TransformsSeen.IMPULSE);
          }
        });

    assertEquals(visited, EnumSet.allOf(TransformsSeen.class));
    assertEquals(left, EnumSet.of(TransformsSeen.SAMPLE));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOutputChecking() throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    p.apply(new InvalidCompositeTransform());
    p.traverseTopologically(new Pipeline.PipelineVisitor.Defaults() {});
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMultiGraphSetup() {
    PCollection<Integer> input = p.begin().apply(Create.of(1, 2, 3));

    input.apply(new UnboundOutputCreator());

    p.run();
  }
}
