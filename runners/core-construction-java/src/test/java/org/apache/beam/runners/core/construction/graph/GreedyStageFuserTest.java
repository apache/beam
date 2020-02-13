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
package org.apache.beam.runners.core.construction.graph;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.getOnlyElement;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.model.pipeline.v1.RunnerApi.StateSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.TimerSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GreedyStageFuser}. */
@RunWith(JUnit4.class)
public class GreedyStageFuserTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private final PCollection impulseDotOut =
      PCollection.newBuilder().setUniqueName("impulse.out").build();
  private final PCollectionNode impulseOutputNode =
      PipelineNode.pCollection("impulse.out", impulseDotOut);

  private Components partialComponents;

  @Before
  public void setup() {
    partialComponents =
        Components.newBuilder()
            .putTransforms(
                "impulse",
                PTransform.newBuilder()
                    .putOutputs("output", "impulse.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN))
                    .build())
            .putPcollections("impulse.out", impulseDotOut)
            .build();
  }

  @Test
  public void noInitialConsumersThrows() {
    // (impulse.out) -> () is not a meaningful stage, so it should never be called
    QueryablePipeline p = QueryablePipeline.forPrimitivesIn(partialComponents);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("at least one PTransform");
    GreedyStageFuser.forGrpcPortRead(p, impulseOutputNode, Collections.emptySet());
  }

  @Test
  public void differentEnvironmentsThrows() {
    // (impulse.out) -> read -> read.out --> go -> go.out
    //                                   \
    //                                    -> py -> py.out
    // read.out can't be fused with both 'go' and 'py', so we should refuse to create this stage
    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms(
                    "read",
                    PTransform.newBuilder()
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                                .build())
                        .putInputs("input", "impulse.out")
                        .putOutputs("output", "read.out")
                        .build())
                .putPcollections(
                    "read.out", PCollection.newBuilder().setUniqueName("read.out").build())
                .putTransforms(
                    "goTransform",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "go.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                                .setPayload(
                                    ParDoPayload.newBuilder()
                                        .setDoFn(FunctionSpec.newBuilder())
                                        .build()
                                        .toByteString()))
                        .setEnvironmentId("go")
                        .build())
                .putPcollections("go.out", PCollection.newBuilder().setUniqueName("go.out").build())
                .putTransforms(
                    "pyTransform",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "py.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                                .setPayload(
                                    WindowIntoPayload.newBuilder()
                                        .setWindowFn(FunctionSpec.newBuilder())
                                        .build()
                                        .toByteString()))
                        .setEnvironmentId("py")
                        .build())
                .putPcollections("py.out", PCollection.newBuilder().setUniqueName("py.out").build())
                .putEnvironments("go", Environments.createDockerEnvironment("go"))
                .putEnvironments("py", Environments.createDockerEnvironment("py"))
                .build());
    Set<PTransformNode> differentEnvironments =
        p.getPerElementConsumers(
            PipelineNode.pCollection(
                "read.out", PCollection.newBuilder().setUniqueName("read.out").build()));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("go");
    thrown.expectMessage("py");
    thrown.expectMessage("same");
    GreedyStageFuser.forGrpcPortRead(
        p,
        PipelineNode.pCollection(
            "read.out", PCollection.newBuilder().setUniqueName("read.out").build()),
        differentEnvironments);
  }

  @Test
  public void noEnvironmentThrows() {
    // (impulse.out) -> runnerTransform -> gbk.out
    // runnerTransform can't be executed in an environment, so trying to construct it should fail
    PTransform gbkTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .setSpec(
                FunctionSpec.newBuilder().setUrn(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN))
            .putOutputs("output", "gbk.out")
            .build();
    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("runnerTransform", gbkTransform)
                .putPcollections(
                    "gbk.out", PCollection.newBuilder().setUniqueName("gbk.out").build())
                .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Environment must be populated");
    GreedyStageFuser.forGrpcPortRead(
        p,
        impulseOutputNode,
        ImmutableSet.of(PipelineNode.pTransform("runnerTransform", gbkTransform)));
  }

  @Test
  public void fusesCompatibleEnvironments() {
    // (impulse.out) -> parDo -> parDo.out -> window -> window.out
    // parDo and window both have the environment "common" and can be fused together
    PTransform parDoTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "parDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();
    PTransform windowTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "window.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                    .setPayload(
                        WindowIntoPayload.newBuilder()
                            .setWindowFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();

    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("parDo", parDoTransform)
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms("window", windowTransform)
                .putPcollections(
                    "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
                .putEnvironments("common", Environments.createDockerEnvironment("common"))
                .build());

    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(
            p,
            impulseOutputNode,
            ImmutableSet.of(
                PipelineNode.pTransform("parDo", parDoTransform),
                PipelineNode.pTransform("window", windowTransform)));
    // Nothing consumes the outputs of ParDo or Window, so they don't have to be materialized
    assertThat(subgraph.getOutputPCollections(), emptyIterable());
    assertThat(subgraph, hasSubtransforms("parDo", "window"));
  }

  @Test
  public void materializesWithStatefulConsumer() {
    // (impulse.out) -> parDo -> (parDo.out)
    // (parDo.out) -> stateful -> stateful.out
    // stateful has a state spec which prevents it from fusing with an upstream ParDo
    PTransform parDoTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "parDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();
    PTransform statefulTransform =
        PTransform.newBuilder()
            .putInputs("input", "parDo.out")
            .putOutputs("output", "stateful.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .putStateSpecs("state", StateSpec.getDefaultInstance())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();

    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("parDo", parDoTransform)
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms("stateful", statefulTransform)
                .putPcollections(
                    "stateful.out", PCollection.newBuilder().setUniqueName("stateful.out").build())
                .putEnvironments("common", Environments.createDockerEnvironment("common"))
                .build());

    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(
            p,
            impulseOutputNode,
            ImmutableSet.of(PipelineNode.pTransform("parDo", parDoTransform)));
    assertThat(
        subgraph.getOutputPCollections(),
        contains(
            PipelineNode.pCollection(
                "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())));
    assertThat(subgraph, hasSubtransforms("parDo"));
  }

  @Test
  public void materializesWithConsumerWithTimer() {
    // (impulse.out) -> parDo -> (parDo.out)
    // (parDo.out) -> timer -> timer.out
    // timer has a timer spec which prevents it from fusing with an upstream ParDo
    PTransform parDoTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "parDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();
    PTransform timerTransform =
        PTransform.newBuilder()
            .putInputs("input", "parDo.out")
            .putOutputs("output", "timer.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .putTimerSpecs("timer", TimerSpec.getDefaultInstance())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();

    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("parDo", parDoTransform)
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms("timer", timerTransform)
                .putPcollections(
                    "timer.out", PCollection.newBuilder().setUniqueName("timer.out").build())
                .putEnvironments("common", Environments.createDockerEnvironment("common"))
                .build());

    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(
            p,
            impulseOutputNode,
            ImmutableSet.of(PipelineNode.pTransform("parDo", parDoTransform)));
    assertThat(
        subgraph.getOutputPCollections(),
        contains(
            PipelineNode.pCollection(
                "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())));
    assertThat(subgraph, hasSubtransforms("parDo"));
  }

  @Test
  public void fusesFlatten() {
    // (impulse.out) -> parDo -> parDo.out --> flatten -> flatten.out -> window -> window.out
    //               \                     /
    //                -> read -> read.out -
    // The flatten can be executed within the same environment as any transform; the window can
    // execute in the same environment as the rest of the transforms, and can fuse with the stage
    PTransform readTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();
    PTransform parDoTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "parDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();
    PTransform flattenTransform =
        PTransform.newBuilder()
            .putInputs("readInput", "read.out")
            .putInputs("parDoInput", "parDo.out")
            .putOutputs("output", "flatten.out")
            .setSpec(FunctionSpec.newBuilder().setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN))
            .build();
    PTransform windowTransform =
        PTransform.newBuilder()
            .putInputs("input", "flatten.out")
            .putOutputs("output", "window.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                    .setPayload(
                        WindowIntoPayload.newBuilder()
                            .setWindowFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();

    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("read", readTransform)
                .putPcollections(
                    "read.out", PCollection.newBuilder().setUniqueName("read.out").build())
                .putTransforms("parDo", parDoTransform)
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms("flatten", flattenTransform)
                .putPcollections(
                    "flatten.out", PCollection.newBuilder().setUniqueName("flatten.out").build())
                .putTransforms("window", windowTransform)
                .putPcollections(
                    "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
                .putEnvironments("common", Environments.createDockerEnvironment("common"))
                .build());

    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(
            p, impulseOutputNode, p.getPerElementConsumers(impulseOutputNode));
    assertThat(subgraph.getOutputPCollections(), emptyIterable());
    assertThat(subgraph, hasSubtransforms("read", "parDo", "flatten", "window"));
  }

  @Test
  public void fusesFlattenWithDifferentEnvironmentInputs() {
    // (impulse.out) -> read -> read.out \                                 -> window -> window.out
    //                                    -------> flatten -> flatten.out /
    // (impulse.out) -> envRead -> envRead.out /
    // fuses into
    // read -> read.out -> flatten -> flatten.out -> window -> window.out
    // envRead -> envRead.out -> flatten -> (flatten.out)
    // (flatten.out) -> window -> window.out
    PTransform readTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();
    PTransform otherEnvRead =
        PTransform.newBuilder()
            .putInputs("impulse", "impulse.out")
            .putOutputs("output", "envRead.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("rare")
            .build();
    PTransform flattenTransform =
        PTransform.newBuilder()
            .putInputs("readInput", "read.out")
            .putInputs("otherEnvInput", "envRead.out")
            .putOutputs("output", "flatten.out")
            .setSpec(FunctionSpec.newBuilder().setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN))
            .build();
    PTransform windowTransform =
        PTransform.newBuilder()
            .putInputs("input", "flatten.out")
            .putOutputs("output", "window.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                    .setPayload(
                        WindowIntoPayload.newBuilder()
                            .setWindowFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();

    Components components =
        partialComponents
            .toBuilder()
            .putTransforms("read", readTransform)
            .putPcollections("read.out", PCollection.newBuilder().setUniqueName("read.out").build())
            .putTransforms("envRead", otherEnvRead)
            .putPcollections(
                "envRead.out", PCollection.newBuilder().setUniqueName("envRead.out").build())
            .putTransforms("flatten", flattenTransform)
            .putPcollections(
                "flatten.out", PCollection.newBuilder().setUniqueName("flatten.out").build())
            .putTransforms("window", windowTransform)
            .putPcollections(
                "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
            .putEnvironments("common", Environments.createDockerEnvironment("common"))
            .putEnvironments("rare", Environments.createDockerEnvironment("rare"))
            .build();
    QueryablePipeline p = QueryablePipeline.forPrimitivesIn(components);

    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(
            p, impulseOutputNode, ImmutableSet.of(PipelineNode.pTransform("read", readTransform)));
    assertThat(subgraph.getOutputPCollections(), emptyIterable());
    assertThat(subgraph, hasSubtransforms("read", "flatten", "window"));

    // Flatten shows up in both of these subgraphs, but elements only go through a path to the
    // flatten once.
    ExecutableStage readFromOtherEnv =
        GreedyStageFuser.forGrpcPortRead(
            p,
            impulseOutputNode,
            ImmutableSet.of(PipelineNode.pTransform("envRead", otherEnvRead)));
    assertThat(
        readFromOtherEnv.getOutputPCollections(),
        contains(
            PipelineNode.pCollection(
                "flatten.out", components.getPcollectionsOrThrow("flatten.out"))));
    assertThat(readFromOtherEnv, hasSubtransforms("envRead", "flatten"));
  }

  @Test
  public void flattenWithHeterogeneousInputsAndOutputs() {
    // (impulse.out) -> pyRead -> pyRead.out \                           -> pyParDo -> pyParDo.out
    // (impulse.out) ->                       -> flatten -> flatten.out |
    // (impulse.out) -> goRead -> goRead.out /                           -> goWindow -> goWindow.out
    // fuses into
    // (impulse.out) -> pyRead -> pyRead.out -> flatten -> (flatten.out)
    // (impulse.out) -> goRead -> goRead.out -> flatten -> (flatten.out)
    // (flatten.out) -> pyParDo -> pyParDo.out
    // (flatten.out) -> goWindow -> goWindow.out
    PTransform pyRead =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "pyRead.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString())
                    .build())
            .setEnvironmentId("py")
            .build();
    PTransform goRead =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "goRead.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString())
                    .build())
            .setEnvironmentId("go")
            .build();

    PTransform pyParDo =
        PTransform.newBuilder()
            .putInputs("input", "flatten.out")
            .putOutputs("output", "pyParDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString())
                    .build())
            .setEnvironmentId("py")
            .build();
    PTransform goWindow =
        PTransform.newBuilder()
            .putInputs("input", "flatten.out")
            .putOutputs("output", "goWindow.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                    .setPayload(
                        WindowIntoPayload.newBuilder()
                            .setWindowFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString())
                    .build())
            .setEnvironmentId("go")
            .build();

    PCollection flattenPc = PCollection.newBuilder().setUniqueName("flatten.out").build();
    Components components =
        partialComponents
            .toBuilder()
            .putTransforms("pyRead", pyRead)
            .putPcollections(
                "pyRead.out", PCollection.newBuilder().setUniqueName("pyRead.out").build())
            .putTransforms("goRead", goRead)
            .putPcollections(
                "goRead.out", PCollection.newBuilder().setUniqueName("goRead.out").build())
            .putTransforms(
                "flatten",
                PTransform.newBuilder()
                    .putInputs("py_input", "pyRead.out")
                    .putInputs("go_input", "goRead.out")
                    .putOutputs("output", "flatten.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN)
                            .build())
                    .build())
            .putPcollections("flatten.out", flattenPc)
            .putTransforms("pyParDo", pyParDo)
            .putPcollections(
                "pyParDo.out", PCollection.newBuilder().setUniqueName("pyParDo.out").build())
            .putTransforms("goWindow", goWindow)
            .putPcollections(
                "goWindow.out", PCollection.newBuilder().setUniqueName("goWindow.out").build())
            .putEnvironments("go", Environments.createDockerEnvironment("go"))
            .putEnvironments("py", Environments.createDockerEnvironment("py"))
            .build();
    QueryablePipeline p = QueryablePipeline.forPrimitivesIn(components);

    ExecutableStage readFromPy =
        GreedyStageFuser.forGrpcPortRead(
            p, impulseOutputNode, ImmutableSet.of(PipelineNode.pTransform("pyRead", pyRead)));
    ExecutableStage readFromGo =
        GreedyStageFuser.forGrpcPortRead(
            p, impulseOutputNode, ImmutableSet.of(PipelineNode.pTransform("goRead", goRead)));

    assertThat(
        readFromPy.getOutputPCollections(),
        contains(PipelineNode.pCollection("flatten.out", flattenPc)));
    // The stage must materialize the flatten, so the `go` stage can read it; this means that this
    // parDo can't be in the stage, as it'll be a reader of that materialized PCollection. The same
    // is true for the go window.
    assertThat(
        readFromPy.getTransforms(), not(hasItem(PipelineNode.pTransform("pyParDo", pyParDo))));

    assertThat(
        readFromGo.getOutputPCollections(),
        contains(PipelineNode.pCollection("flatten.out", flattenPc)));
    assertThat(
        readFromGo.getTransforms(), not(hasItem(PipelineNode.pTransform("goWindow", goWindow))));
  }

  @Test
  public void materializesWithDifferentEnvConsumer() {
    // (impulse.out) -> parDo -> parDo.out -> window -> window.out
    // Fuses into
    // (impulse.out) -> parDo -> (parDo.out)
    // (parDo.out) -> window -> window.out
    Environment env = Environments.createDockerEnvironment("common");
    PTransform parDoTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("out", "parDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();

    PCollection parDoOutput = PCollection.newBuilder().setUniqueName("parDo.out").build();
    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("parDo", parDoTransform)
                .putPcollections("parDo.out", parDoOutput)
                .putTransforms(
                    "window",
                    PTransform.newBuilder()
                        .putInputs("input", "parDo.out")
                        .putOutputs("output", "window.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                                .setPayload(
                                    WindowIntoPayload.newBuilder()
                                        .setWindowFn(FunctionSpec.newBuilder())
                                        .build()
                                        .toByteString()))
                        .setEnvironmentId("rare")
                        .build())
                .putPcollections(
                    "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
                .putEnvironments("rare", Environments.createDockerEnvironment("rare"))
                .putEnvironments("common", env)
                .build());

    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(
            p, impulseOutputNode, p.getPerElementConsumers(impulseOutputNode));
    assertThat(
        subgraph.getOutputPCollections(),
        contains(PipelineNode.pCollection("parDo.out", parDoOutput)));
    assertThat(subgraph.getInputPCollection(), equalTo(impulseOutputNode));
    assertThat(subgraph.getEnvironment(), equalTo(env));
    assertThat(
        subgraph.getTransforms(), contains(PipelineNode.pTransform("parDo", parDoTransform)));
  }

  @Test
  public void materializesWithDifferentEnvSibling() {
    // (impulse.out) -> read -> read.out -> parDo -> parDo.out
    //                                   \
    //                                    -> window -> window.out
    // Fuses into
    // (impulse.out) -> read -> (read.out)
    // (read.out) -> parDo -> parDo.out
    // (read.out) -> window -> window.out
    // The window can't be fused into the stage, which forces the PCollection to be materialized.
    // ParDo in this case _could_ be fused into the stage, but is not for simplicity of
    // implementation
    Environment env = Environments.createDockerEnvironment("common");
    PTransform readTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();
    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("read", readTransform)
                .putPcollections(
                    "read.out", PCollection.newBuilder().setUniqueName("read.out").build())
                .putTransforms(
                    "parDo",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "parDo.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                                .setPayload(
                                    ParDoPayload.newBuilder()
                                        .setDoFn(FunctionSpec.newBuilder())
                                        .build()
                                        .toByteString()))
                        .setEnvironmentId("common")
                        .build())
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms(
                    "window",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "window.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                                .setPayload(
                                    WindowIntoPayload.newBuilder()
                                        .setWindowFn(FunctionSpec.newBuilder())
                                        .build()
                                        .toByteString()))
                        .setEnvironmentId("rare")
                        .build())
                .putPcollections(
                    "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
                .putEnvironments("rare", Environments.createDockerEnvironment("rare"))
                .putEnvironments("common", env)
                .build());

    PTransformNode readNode = PipelineNode.pTransform("read", readTransform);
    PCollectionNode readOutput = getOnlyElement(p.getOutputPCollections(readNode));
    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(
            p, impulseOutputNode, ImmutableSet.of(PipelineNode.pTransform("read", readTransform)));
    assertThat(subgraph.getOutputPCollections(), contains(readOutput));
    assertThat(subgraph.getTransforms(), contains(readNode));
  }

  @Test
  public void materializesWithSideInputConsumer() {
    // (impulse.out) -> read -> read.out -----------> parDo -> parDo.out -> window -> window.out
    // (impulse.out) -> side_read -> side_read.out /
    // Where parDo takes side_read as a side input, fuses into
    // (impulse.out) -> read -> (read.out)
    // (impulse.out) -> side_read -> (side_read.out)
    // (read.out) -> parDo -> parDo.out -> window -> window.out
    // parDo doesn't have a per-element consumer from side_read.out, so it can't root a stage
    // which consumes from that materialized collection. Nodes with side inputs must root a stage,
    // but do not restrict fusion of consumers.
    Environment env = Environments.createDockerEnvironment("common");
    PTransform readTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();

    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("read", readTransform)
                .putPcollections(
                    "read.out", PCollection.newBuilder().setUniqueName("read.out").build())
                .putTransforms(
                    "side_read",
                    PTransform.newBuilder()
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN))
                        .putInputs("input", "impulse.out")
                        .putOutputs("output", "side_read.out")
                        .build())
                .putPcollections(
                    "side_read.out",
                    PCollection.newBuilder().setUniqueName("side_read.out").build())
                .putTransforms(
                    "parDo",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putInputs("side_input", "side_read.out")
                        .putOutputs("output", "parDo.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                                .setPayload(
                                    ParDoPayload.newBuilder()
                                        .setDoFn(FunctionSpec.newBuilder())
                                        .putSideInputs("side_input", SideInput.getDefaultInstance())
                                        .build()
                                        .toByteString()))
                        .setEnvironmentId("common")
                        .build())
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms(
                    "window",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "window.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                                .setPayload(
                                    WindowIntoPayload.newBuilder()
                                        .setWindowFn(FunctionSpec.newBuilder())
                                        .build()
                                        .toByteString()))
                        .setEnvironmentId("common")
                        .build())
                .putPcollections(
                    "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
                .putEnvironments("common", env)
                .build());

    PTransformNode readNode = PipelineNode.pTransform("read", readTransform);
    PCollectionNode readOutput = getOnlyElement(p.getOutputPCollections(readNode));
    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(p, impulseOutputNode, ImmutableSet.of(readNode));
    assertThat(subgraph.getOutputPCollections(), contains(readOutput));
    assertThat(subgraph, hasSubtransforms(readNode.getId()));
  }

  @Test
  public void sideInputIncludedInStage() {
    Environment env = Environments.createDockerEnvironment("common");
    PTransform readTransform =
        PTransform.newBuilder()
            .setUniqueName("read")
            .putInputs("input", "impulse.out")
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();

    PTransform parDoTransform =
        PTransform.newBuilder()
            .setUniqueName("parDo")
            .putInputs("input", "read.out")
            .putInputs("side_input", "side_read.out")
            .putOutputs("output", "parDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .putSideInputs("side_input", SideInput.getDefaultInstance())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();
    PCollection sideInputPCollection =
        PCollection.newBuilder().setUniqueName("side_read.out").build();
    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("read", readTransform)
                .putPcollections(
                    "read.out", PCollection.newBuilder().setUniqueName("read.out").build())
                .putTransforms(
                    "side_read",
                    PTransform.newBuilder()
                        .setUniqueName("side_read")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN))
                        .putInputs("input", "impulse.out")
                        .putOutputs("output", "side_read.out")
                        .build())
                .putPcollections("side_read.out", sideInputPCollection)
                .putTransforms("parDo", parDoTransform)
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putEnvironments("common", env)
                .build());

    PCollectionNode readOutput =
        getOnlyElement(p.getOutputPCollections(PipelineNode.pTransform("read", readTransform)));
    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(
            p, readOutput, ImmutableSet.of(PipelineNode.pTransform("parDo", parDoTransform)));
    PTransformNode parDoNode = PipelineNode.pTransform("parDo", parDoTransform);
    SideInputReference sideInputRef =
        SideInputReference.of(
            parDoNode,
            "side_input",
            PipelineNode.pCollection("side_read.out", sideInputPCollection));
    assertThat(subgraph.getSideInputs(), contains(sideInputRef));
    assertThat(subgraph.getOutputPCollections(), emptyIterable());
  }

  @Test
  public void executableStageProducingSideInputMaterializesIt() {
    // impulse -- ParDo(createSide)
    //         \_ ParDo(processMain) with side input from createSide
    // The ExecutableStage executing createSide must have an output.
    Environment env = Environments.createDockerEnvironment("common");
    PTransform impulse =
        PTransform.newBuilder()
            .setUniqueName("impulse")
            .putOutputs("output", "impulsePC")
            .setSpec(FunctionSpec.newBuilder().setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN))
            .build();
    PTransform createSide =
        PTransform.newBuilder()
            .setUniqueName("createSide")
            .putInputs("input", "impulsePC")
            .putOutputs("output", "sidePC")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();
    PTransform processMain =
        PTransform.newBuilder()
            .setUniqueName("processMain")
            .putInputs("main", "impulsePC")
            .putInputs("side", "sidePC")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .putSideInputs("side", SideInput.getDefaultInstance())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();

    PCollection sidePC = PCollection.newBuilder().setUniqueName("sidePC").build();
    PCollection impulsePC = PCollection.newBuilder().setUniqueName("impulsePC").build();
    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("impulse", impulse)
                .putTransforms("createSide", createSide)
                .putTransforms("processMain", processMain)
                .putPcollections("impulsePC", impulsePC)
                .putPcollections("sidePC", sidePC)
                .putEnvironments("common", env)
                .build());

    PCollectionNode impulseOutput =
        getOnlyElement(p.getOutputPCollections(PipelineNode.pTransform("impulse", impulse)));
    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(
            p, impulseOutput, ImmutableSet.of(PipelineNode.pTransform("createSide", createSide)));
    assertThat(
        subgraph.getOutputPCollections(), contains(PipelineNode.pCollection("sidePC", sidePC)));
  }

  @Test
  public void userStateIncludedInStage() {
    Environment env = Environments.createDockerEnvironment("common");
    PTransform readTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();
    PTransform parDoTransform =
        PTransform.newBuilder()
            .putInputs("input", "read.out")
            .putOutputs("output", "parDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .putStateSpecs("state_spec", StateSpec.getDefaultInstance())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();
    PCollection userStateMainInputPCollection =
        PCollection.newBuilder().setUniqueName("read.out").build();

    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("read", readTransform)
                .putPcollections("read.out", userStateMainInputPCollection)
                .putTransforms(
                    "user_state",
                    PTransform.newBuilder()
                        .putInputs("input", "impulse.out")
                        .putOutputs("output", "user_state.out")
                        .build())
                .putPcollections(
                    "user_state.out",
                    PCollection.newBuilder().setUniqueName("user_state.out").build())
                .putTransforms("parDo", parDoTransform)
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putEnvironments("common", env)
                .build());

    PCollectionNode readOutput =
        getOnlyElement(p.getOutputPCollections(PipelineNode.pTransform("read", readTransform)));
    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(
            p, readOutput, ImmutableSet.of(PipelineNode.pTransform("parDo", parDoTransform)));
    PTransformNode parDoNode = PipelineNode.pTransform("parDo", parDoTransform);
    UserStateReference userStateRef =
        UserStateReference.of(
            parDoNode,
            "state_spec",
            PipelineNode.pCollection("read.out", userStateMainInputPCollection));
    assertThat(subgraph.getUserStates(), contains(userStateRef));
    assertThat(subgraph.getOutputPCollections(), emptyIterable());
  }

  @Test
  public void materializesWithGroupByKeyConsumer() {
    // (impulse.out) -> read -> read.out -> gbk -> gbk.out
    // Fuses to
    // (impulse.out) -> read -> (read.out)
    // GBK is the responsibility of the runner, so it is not included in a stage.
    Environment env = Environments.createDockerEnvironment("common");
    PTransform readTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(FunctionSpec.newBuilder())
                            .build()
                            .toByteString()))
            .setEnvironmentId("common")
            .build();

    QueryablePipeline p =
        QueryablePipeline.forPrimitivesIn(
            partialComponents
                .toBuilder()
                .putTransforms("read", readTransform)
                .putPcollections(
                    "read.out", PCollection.newBuilder().setUniqueName("read.out").build())
                .putTransforms(
                    "gbk",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "gbk.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN))
                        .build())
                .putPcollections(
                    "gbk.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putEnvironments("common", env)
                .build());

    PTransformNode readNode = PipelineNode.pTransform("read", readTransform);
    PCollectionNode readOutput = getOnlyElement(p.getOutputPCollections(readNode));
    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(p, impulseOutputNode, ImmutableSet.of(readNode));
    assertThat(subgraph.getOutputPCollections(), contains(readOutput));
    assertThat(subgraph, hasSubtransforms(readNode.getId()));
  }

  private static TypeSafeMatcher<ExecutableStage> hasSubtransforms(String id, String... ids) {
    Set<String> expectedTransforms = ImmutableSet.<String>builder().add(id).add(ids).build();
    return new TypeSafeMatcher<ExecutableStage>() {
      @Override
      protected boolean matchesSafely(ExecutableStage executableStage) {
        // NOTE: Transform names must be unique, so it's fine to throw here if this does not hold.
        Set<String> stageTransforms =
            executableStage.getTransforms().stream()
                .map(PTransformNode::getId)
                .collect(Collectors.toSet());
        return stageTransforms.containsAll(expectedTransforms)
            && expectedTransforms.containsAll(stageTransforms);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("ExecutableStage with subtransform ids: " + expectedTransforms);
      }
    };
  }
}
