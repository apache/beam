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

package org.apache.beam.runners.direct.portable;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.portable.CommittedResult.OutputType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link StepTransformResult}. */
@RunWith(JUnit4.class)
public class StepTransformResultTest {
  private PTransformNode transform;
  private BundleFactory bundleFactory;
  private PCollectionNode pc;

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Before
  public void setup() {
    pc =
        PipelineNode.pCollection(
            "pc", RunnerApi.PCollection.newBuilder().setUniqueName("pc").build());
    transform =
        PipelineNode.pTransform("pt", PTransform.newBuilder().putOutputs("out", "pc").build());

    bundleFactory = ImmutableListBundleFactory.create();
  }

  @Test
  public void producedBundlesProducedOutputs() {
    UncommittedBundle<Integer> bundle = bundleFactory.createBundle(pc);
    TransformResult<Integer> result =
        StepTransformResult.<Integer>withoutHold(transform).addOutput(bundle).build();

    assertThat(result.getOutputBundles(), Matchers.containsInAnyOrder(bundle));
  }

  @Test
  public void withAdditionalOutputProducedOutputs() {
    TransformResult<Integer> result =
        StepTransformResult.<Integer>withoutHold(transform)
            .withAdditionalOutput(OutputType.PCOLLECTION_VIEW)
            .build();

    assertThat(result.getOutputTypes(), containsInAnyOrder(OutputType.PCOLLECTION_VIEW));
  }

  @Test
  public void producedBundlesAndAdditionalOutputProducedOutputs() {
    TransformResult<Integer> result =
        StepTransformResult.<Integer>withoutHold(transform)
            .addOutput(bundleFactory.createBundle(pc))
            .withAdditionalOutput(OutputType.PCOLLECTION_VIEW)
            .build();

    assertThat(result.getOutputTypes(), hasItem(OutputType.PCOLLECTION_VIEW));
  }

  @Test
  public void noBundlesNoAdditionalOutputProducedOutputsFalse() {
    TransformResult<Integer> result = StepTransformResult.<Integer>withoutHold(transform).build();

    assertThat(result.getOutputTypes(), emptyIterable());
  }
}
