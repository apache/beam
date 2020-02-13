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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.construction.JavaReadViaImpulse.ReadFromBoundedSourceFn;
import org.apache.beam.runners.core.construction.JavaReadViaImpulse.SplitBoundedSourceFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JavaReadViaImpulse}. */
@RunWith(JUnit4.class)
public class JavaReadViaImpulseTest {
  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testBoundedRead() {
    PCollection<Long> read = p.apply(JavaReadViaImpulse.bounded(CountingSource.upTo(10L)));

    PAssert.that(read).containsInAnyOrder(0L, 9L, 8L, 1L, 2L, 7L, 6L, 3L, 4L, 5L);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSplitSourceFn() {
    PCollection<BoundedSource<Long>> splits =
        p.apply(Impulse.create())
            .apply(
                "SplitSource",
                /*
                 * Split the source of 1 million longs into bundles of size 300 thousand bytes.
                 * This should produce some small number of bundles, but more than one.
                 */
                ParDo.of(new SplitBoundedSourceFn<>(CountingSource.upTo(1_000_000L), 300_000L)))
            .setCoder(new JavaReadViaImpulse.BoundedSourceCoder<>());

    PAssert.that(splits)
        .satisfies(
            input -> {
              assertThat(Iterables.size(input), greaterThan(1));
              return null;
            });

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadFromSourceFn() {
    BoundedSource<Long> source = CountingSource.upTo(10L);
    PCollection<BoundedSource<Long>> sourcePC =
        p.apply(Create.of(source).withCoder(new JavaReadViaImpulse.BoundedSourceCoder<>()));
    PCollection<Long> elems =
        sourcePC.apply(ParDo.of(new ReadFromBoundedSourceFn<>())).setCoder(VarLongCoder.of());

    PAssert.that(elems).containsInAnyOrder(0L, 9L, 8L, 1L, 2L, 7L, 6L, 3L, 4L, 5L);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadToImpulseOverride() {
    BoundedSource<Long> source = CountingSource.upTo(10L);
    // Use an explicit read transform to ensure the override is exercised.
    PCollection<Long> input = p.apply(Read.from(source));
    PAssert.that(input).containsInAnyOrder(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
    p.replaceAll(Collections.singletonList(JavaReadViaImpulse.boundedOverride()));
    p.traverseTopologically(
        new Pipeline.PipelineVisitor() {
          @Override
          public void enterPipeline(Pipeline p) {}

          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            assertNotReadTransform(node.getTransform());
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void leaveCompositeTransform(TransformHierarchy.Node node) {}

          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            assertNotReadTransform(node.getTransform());
          }

          @Override
          public void visitValue(PValue value, TransformHierarchy.Node producer) {}

          @Override
          public void leavePipeline(Pipeline pipeline) {}
        });
    p.run();
  }

  @Test
  public void testOutputCoder() {
    p.enableAbandonedNodeEnforcement(false);
    BoundedSource<Integer> fixedCoderSource = new BigEndianIntegerSource();
    assertThat(
        p.apply(JavaReadViaImpulse.bounded(fixedCoderSource)).getCoder(),
        equalTo(BigEndianIntegerCoder.of()));
  }

  private static void assertNotReadTransform(PTransform<?, ?> transform) {
    if (transform != null) {
      String urn = PTransformTranslation.urnForTransformOrNull(transform);
      assertThat(urn, not(equalTo(PTransformTranslation.READ_TRANSFORM_URN)));
    }
  }

  private static class BigEndianIntegerSource extends BoundedSource<Integer> {
    @Override
    public List<? extends BoundedSource<Integer>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public BoundedReader<Integer> createReader(PipelineOptions options) throws IOException {
      throw new AssertionError("Not the point");
    }

    @Override
    public Coder<Integer> getOutputCoder() {
      return BigEndianIntegerCoder.of();
    }
  }
}
