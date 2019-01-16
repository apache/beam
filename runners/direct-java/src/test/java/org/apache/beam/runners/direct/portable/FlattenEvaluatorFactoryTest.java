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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables.getOnlyElement;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FlattenEvaluatorFactory}. */
@RunWith(JUnit4.class)
public class FlattenEvaluatorFactoryTest {
  private BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  @Test
  public void testFlattenInMemoryEvaluator() throws Exception {
    PCollectionNode left =
        PipelineNode.pCollection("left", PCollection.newBuilder().setUniqueName("left").build());
    PCollectionNode right =
        PipelineNode.pCollection("right", PCollection.newBuilder().setUniqueName("right").build());
    // Include a root node for a sane-looking graph
    PTransformNode source =
        PipelineNode.pTransform(
            "source",
            PTransform.newBuilder()
                .putOutputs("left", left.getId())
                .putOutputs("right", right.getId())
                .build());

    PCollectionNode flattened =
        PipelineNode.pCollection("flat", PCollection.newBuilder().setUniqueName("flat").build());
    PTransformNode flatten =
        PipelineNode.pTransform(
            "flatten",
            PTransform.newBuilder()
                .setUniqueName("flatten")
                .putInputs("left", left.getId())
                .putInputs("right", right.getId())
                .putOutputs("out", flattened.getId())
                .setSpec(
                    FunctionSpec.newBuilder().setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN))
                .build());

    PortableGraph graph =
        PortableGraph.forPipeline(
            RunnerApi.Pipeline.newBuilder()
                .addRootTransformIds(source.getId())
                .addRootTransformIds(flatten.getId())
                .setComponents(
                    RunnerApi.Components.newBuilder()
                        .putTransforms(source.getId(), source.getTransform())
                        .putPcollections(left.getId(), left.getPCollection())
                        .putPcollections(right.getId(), right.getPCollection())
                        .putTransforms(flatten.getId(), flatten.getTransform())
                        .putPcollections(flattened.getId(), flattened.getPCollection()))
                .build());

    CommittedBundle<Integer> leftBundle =
        bundleFactory.<Integer>createBundle(left).commit(Instant.now());
    CommittedBundle<Integer> rightBundle =
        bundleFactory.<Integer>createBundle(right).commit(Instant.now());

    FlattenEvaluatorFactory factory = new FlattenEvaluatorFactory(graph, bundleFactory);
    TransformEvaluator<Integer> leftSideEvaluator = factory.forApplication(flatten, leftBundle);
    TransformEvaluator<Integer> rightSideEvaluator = factory.forApplication(flatten, rightBundle);

    leftSideEvaluator.processElement(WindowedValue.valueInGlobalWindow(1));
    rightSideEvaluator.processElement(WindowedValue.valueInGlobalWindow(-1));
    leftSideEvaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow(2, new Instant(1024)));
    leftSideEvaluator.processElement(WindowedValue.valueInGlobalWindow(4, PaneInfo.NO_FIRING));
    rightSideEvaluator.processElement(
        WindowedValue.valueInGlobalWindow(2, PaneInfo.ON_TIME_AND_ONLY_FIRING));
    rightSideEvaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow(-4, new Instant(-4096)));

    TransformResult<Integer> rightSideResult = rightSideEvaluator.finishBundle();
    TransformResult<Integer> leftSideResult = leftSideEvaluator.finishBundle();

    assertThat(
        getOnlyElement(leftSideResult.getOutputBundles()).commit(Instant.now()),
        containsInAnyOrder(
            WindowedValue.timestampedValueInGlobalWindow(2, new Instant(1024)),
            WindowedValue.valueInGlobalWindow(4, PaneInfo.NO_FIRING),
            WindowedValue.valueInGlobalWindow(1)));
    assertThat(
        getOnlyElement(rightSideResult.getOutputBundles()).commit(Instant.now()),
        containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(2, PaneInfo.ON_TIME_AND_ONLY_FIRING),
            WindowedValue.timestampedValueInGlobalWindow(-4, new Instant(-4096)),
            WindowedValue.valueInGlobalWindow(-1)));
  }
}
