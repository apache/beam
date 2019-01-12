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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.direct.portable.ImpulseEvaluatorFactory.ImpulseRootProvider;
import org.apache.beam.runners.direct.portable.ImpulseEvaluatorFactory.ImpulseShard;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;

/** Tests for {@link ImpulseEvaluatorFactory}. */
@RunWith(JUnit4.class)
public class ImpulseEvaluatorFactoryTest {
  private BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  private PTransformNode impulseApplication =
      PipelineNode.pTransform(
          "impulse",
          PTransform.newBuilder()
              .setSpec(
                  FunctionSpec.newBuilder()
                      .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN)
                      .build())
              .putOutputs("output", "impulse.out")
              .build());
  private PCollectionNode impulseOut =
      PipelineNode.pCollection(
          "impulse.out", RunnerApi.PCollection.newBuilder().setUniqueName("impulse.out").build());
  private ExecutableGraph<PTransformNode, PCollectionNode> graph =
      PortableGraph.forPipeline(
          RunnerApi.Pipeline.newBuilder()
              .addRootTransformIds("impulse")
              .setComponents(
                  Components.newBuilder()
                      .putTransforms("impulse", impulseApplication.getTransform())
                      .putPcollections("impulse.out", impulseOut.getPCollection()))
              .build());

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testImpulse() throws Exception {

    ImpulseEvaluatorFactory factory = new ImpulseEvaluatorFactory(graph, bundleFactory);

    WindowedValue<ImpulseShard> inputShard = WindowedValue.valueInGlobalWindow(new ImpulseShard());
    CommittedBundle<ImpulseShard> inputShardBundle =
        bundleFactory.<ImpulseShard>createRootBundle().add(inputShard).commit(Instant.now());

    TransformEvaluator<ImpulseShard> evaluator =
        factory.forApplication(impulseApplication, inputShardBundle);
    evaluator.processElement(inputShard);
    TransformResult<ImpulseShard> result = evaluator.finishBundle();
    assertThat(
        "Exactly one output from a single ImpulseShard",
        Iterables.size(result.getOutputBundles()),
        equalTo(1));
    UncommittedBundle<?> outputBundle = result.getOutputBundles().iterator().next();
    CommittedBundle<?> committedOutputBundle = outputBundle.commit(Instant.now());
    assertThat(
        committedOutputBundle.getMinimumTimestamp(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
    assertThat(committedOutputBundle.getPCollection(), equalTo(impulseOut));
    assertThat(
        "Should only be one impulse element",
        Iterables.size(committedOutputBundle.getElements()),
        equalTo(1));
    assertThat(
        committedOutputBundle.getElements().iterator().next().getWindows(),
        contains(GlobalWindow.INSTANCE));
    assertArrayEquals(
        "Output should be an empty byte array",
        new byte[0],
        (byte[]) committedOutputBundle.getElements().iterator().next().getValue());
  }

  @Test
  public void testRootProvider() {
    ImpulseRootProvider rootProvider = new ImpulseRootProvider(bundleFactory);

    Collection<? extends CommittedBundle<?>> inputs =
        rootProvider.getInitialInputs(impulseApplication, 100);

    assertThat("Only one impulse bundle per application", inputs, hasSize(1));
    assertThat(
        "Only one impulse shard per bundle",
        Iterables.size(inputs.iterator().next().getElements()),
        equalTo(1));
  }
}
