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

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PValue;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link UnconsumedReads}. */
@RunWith(JUnit4.class)
public class UnconsumedReadsTest {
  @Rule public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void matcherProducesUnconsumedValueBoundedRead() {
    Bounded<Long> transform = Read.from(CountingSource.upTo(20L));
    pipeline.apply(transform);
    UnconsumedReads.ensureAllReadsConsumed(pipeline);
    validateConsumed();
  }

  @Test
  public void matcherProducesUnconsumedValueUnboundedRead() {
    Unbounded<Long> transform = Read.from(CountingSource.unbounded());
    pipeline.apply(transform);
    UnconsumedReads.ensureAllReadsConsumed(pipeline);
    validateConsumed();
  }

  @Test
  public void doesNotConsumeAlreadyConsumedRead() {
    Unbounded<Long> transform = Read.from(CountingSource.unbounded());
    final PCollection<Long> output = pipeline.apply(transform);
    final Flatten.PCollections<Long> consumer = Flatten.pCollections();
    PCollectionList.of(output).apply(consumer);
    UnconsumedReads.ensureAllReadsConsumed(pipeline);
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void visitPrimitiveTransform(Node node) {
            // The output should only be consumed by a single consumer
            if (node.getInputs().values().contains(output)) {
              assertThat(node.getTransform(), Matchers.is(consumer));
            }
          }
        });
  }

  private void validateConsumed() {
    final Set<PValue> consumedOutputs = new HashSet<>();
    final Set<PValue> allReadOutputs = new HashSet<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void visitPrimitiveTransform(Node node) {
            consumedOutputs.addAll(node.getInputs().values());
          }

          @Override
          public void visitValue(PValue value, Node producer) {
            if (producer.getTransform() instanceof Read.Bounded
                || producer.getTransform() instanceof Read.Unbounded) {
              allReadOutputs.add(value);
            }
          }
        });
    assertThat(consumedOutputs, Matchers.hasItems(allReadOutputs.toArray(new PValue[0])));
  }
}
