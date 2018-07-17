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

package org.apache.beam.runners.spark;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedReadFromUnboundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.junit.Test;

/**
 * Test that we can "force streaming" on pipelines with {@link BoundedReadFromUnboundedSource}
 * inputs using the {@link TestSparkRunner}.
 *
 * <p>The test validates that when a pipeline reads from a {@link BoundedReadFromUnboundedSource},
 * with {@link SparkPipelineOptions#setStreaming(boolean)} true and using the {@link
 * TestSparkRunner}; the {@link Read.Bounded} transform is replaced by an {@link Read.Unbounded}
 * transform.
 *
 * <p>This test does not execute a pipeline.
 */
public class ForceStreamingTest {

  @Test
  public void test() throws IOException {
    TestSparkPipelineOptions options =
        PipelineOptionsFactory.create().as(TestSparkPipelineOptions.class);
    options.setRunner(TestSparkRunner.class);
    options.setForceStreaming(true);

    // pipeline with a bounded read.
    Pipeline pipeline = Pipeline.create(options);

    // apply the BoundedReadFromUnboundedSource.
    BoundedReadFromUnboundedSource<?> boundedRead =
        Read.from(CountingSource.unbounded()).withMaxNumRecords(-1);
    pipeline.apply(boundedRead);

    // adapt reads
    TestSparkRunner runner = TestSparkRunner.fromOptions(options);
    runner.adaptBoundedReads(pipeline);

    UnboundedReadDetector unboundedReadDetector = new UnboundedReadDetector();
    pipeline.traverseTopologically(unboundedReadDetector);

    // assert that the applied BoundedReadFromUnboundedSource
    // is being treated as an unbounded read.
    assertThat("Expected to have an unbounded read.", unboundedReadDetector.isUnbounded);
  }

  /** Traverses the Pipeline to check if the input is indeed a {@link Read.Unbounded}. */
  private static class UnboundedReadDetector extends Pipeline.PipelineVisitor.Defaults {
    private boolean isUnbounded = false;

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      Class<? extends PTransform> transformClass = node.getTransform().getClass();
      if (Read.Unbounded.class.equals(transformClass)) {
        isUnbounded = true;
      }
    }
  }
}
