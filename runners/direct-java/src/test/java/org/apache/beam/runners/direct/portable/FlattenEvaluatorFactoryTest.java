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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FlattenEvaluatorFactory}. */
@RunWith(JUnit4.class)
public class FlattenEvaluatorFactoryTest {
  private BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Ignore("TODO: BEAM-4240 Enable when the Flatten Evaluator Factory is fully migrated")
  @Test
  public void testFlattenInMemoryEvaluator() throws Exception {
    PCollectionNode left =
        PipelineNode.pCollection("left", PCollection.newBuilder().setUniqueName("left").build());
    PCollectionNode right =
        PipelineNode.pCollection("right", PCollection.newBuilder().setUniqueName("right").build());

    PTransformNode flatten =
        PipelineNode.pTransform(
            "flatten",
            PTransform.newBuilder()
                .setUniqueName("flatten")
                .setSpec(
                    FunctionSpec.newBuilder().setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN))
                .build());

    PCollectionNode flattened =
        PipelineNode.pCollection("flat", PCollection.newBuilder().setUniqueName("flat").build());

    CommittedBundle<Integer> leftBundle =
        bundleFactory.<Integer>createBundle(left).commit(Instant.now());
    CommittedBundle<Integer> rightBundle =
        bundleFactory.<Integer>createBundle(right).commit(Instant.now());

    EvaluationContext context = mock(EvaluationContext.class);

    UncommittedBundle<Integer> flattenedLeftBundle = bundleFactory.createBundle(flattened);
    UncommittedBundle<Integer> flattenedRightBundle = bundleFactory.createBundle(flattened);

    when(context.<Integer>createBundle(flattened))
        .thenReturn(flattenedLeftBundle, flattenedRightBundle);

    FlattenEvaluatorFactory factory = new FlattenEvaluatorFactory(context);
    TransformEvaluator<Integer> leftSideEvaluator =
        factory.forApplication(flatten, leftBundle);
    TransformEvaluator<Integer> rightSideEvaluator =
        factory.forApplication(flatten, rightBundle);

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

    assertThat(rightSideResult.getOutputBundles(), Matchers.contains(flattenedRightBundle));
    assertThat(rightSideResult.getTransform(), Matchers.equalTo(flatten));
    assertThat(leftSideResult.getOutputBundles(), Matchers.contains(flattenedLeftBundle));
    assertThat(leftSideResult.getTransform(), Matchers.equalTo(flatten));

    assertThat(
        flattenedLeftBundle.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            WindowedValue.timestampedValueInGlobalWindow(2, new Instant(1024)),
            WindowedValue.valueInGlobalWindow(4, PaneInfo.NO_FIRING),
            WindowedValue.valueInGlobalWindow(1)));
    assertThat(
        flattenedRightBundle.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(2, PaneInfo.ON_TIME_AND_ONLY_FIRING),
            WindowedValue.timestampedValueInGlobalWindow(-4, new Instant(-4096)),
            WindowedValue.valueInGlobalWindow(-1)));
  }
}
