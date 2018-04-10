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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.model.pipeline.v1.RunnerApi.StateSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.TimerSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GreedyPipelineFuser}. */
@RunWith(JUnit4.class)
public class GreedyPipelineFuserTest {
  // Contains the 'go' and 'py' environments, and a default 'impulse' step and output.
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
            .putPcollections(
                "impulse.out", PCollection.newBuilder().setUniqueName("impulse.out").build())
            .putEnvironments("go", Environment.newBuilder().setUrl("go").build())
            .putEnvironments("py", Environment.newBuilder().setUrl("py").build())
            .build();
  }

  /*
   * impulse -> .out -> read -> .out -> parDo -> .out -> window -> .out
   * becomes
   * (impulse.out) -> read -> read.out -> parDo -> parDo.out -> window
   */
  @Test
  public void singleEnvironmentBecomesASingleStage() {
    Components components =
        partialComponents
            .toBuilder()
            .putTransforms(
                "read",
                PTransform.newBuilder()
                    .putInputs("input", "impulse.out")
                    .putOutputs("output", "read.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections("read.out", PCollection.newBuilder().setUniqueName("read.out").build())
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
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections(
                "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
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
                                    .setWindowFn(
                                        SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections(
                "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
            .build();
    FusedPipeline fused =
        GreedyPipelineFuser.fuse(Pipeline.newBuilder().setComponents(components).build());

    assertThat(
        fused.getRunnerExecutedTransforms(),
        contains(PipelineNode.pTransform("impulse", components.getTransformsOrThrow("impulse"))));
    assertThat(
        fused.getFusedStages(),
        contains(
            ExecutableStageMatcher.withInput("impulse.out")
                .withNoOutputs()
                .withTransforms("read", "parDo", "window")));
  }

  /*
   * impulse -> .out -> mystery -> .out
   *                 \
   *                  -> enigma -> .out
   * becomes all runner-executed
   */
  @Test
  public void unknownTransformsNoEnvironmentBecomeRunnerExecuted() {
    Components components =
        partialComponents
            .toBuilder()
            .putTransforms(
                "mystery",
                PTransform.newBuilder()
                    .putInputs("input", "impulse.out")
                    .putOutputs("output", "mystery.out")
                    .setSpec(FunctionSpec.newBuilder().setUrn("beam:transform:mystery:v1.4"))
                    .build())
            .putPcollections(
                "mystery.out", PCollection.newBuilder().setUniqueName("mystery.out").build())
            .putTransforms(
                "enigma",
                PTransform.newBuilder()
                    .putInputs("input", "impulse.out")
                    .putOutputs("output", "enigma.out")
                    .setSpec(FunctionSpec.newBuilder().setUrn("beam:transform:enigma:v1"))
                    .build())
            .putPcollections(
                "enigma.out", PCollection.newBuilder().setUniqueName("enigma.out").build())
            .build();
    FusedPipeline fused =
        GreedyPipelineFuser.fuse(Pipeline.newBuilder().setComponents(components).build());

    assertThat(
        fused.getRunnerExecutedTransforms(),
        containsInAnyOrder(
            PipelineNode.pTransform("impulse", components.getTransformsOrThrow("impulse")),
            PipelineNode.pTransform("mystery", components.getTransformsOrThrow("mystery")),
            PipelineNode.pTransform("enigma", components.getTransformsOrThrow("enigma"))));
    assertThat(fused.getFusedStages(), emptyIterable());
  }

  /*
   * impulse -> .out -> read -> .out -> groupByKey -> .out -> parDo -> .out
   * becomes
   * (impulse.out) -> read -> (read.out)
   * (groupByKey.out) -> parDo
   */
  @Test
  public void singleEnvironmentAcrossGroupByKeyMultipleStages() {
    Components components =
        partialComponents
            .toBuilder()
            .putTransforms(
                "read",
                PTransform.newBuilder()
                    .putInputs("input", "impulse.out")
                    .putOutputs("output", "read.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections("read.out", PCollection.newBuilder().setUniqueName("read.out").build())
            .putTransforms(
                "groupByKey",
                PTransform.newBuilder()
                    .putInputs("input", "read.out")
                    .putOutputs("output", "groupByKey.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN))
                    .build())
            .putPcollections(
                "groupByKey.out", PCollection.newBuilder().setUniqueName("groupByKey.out").build())
            .putTransforms(
                "parDo",
                PTransform.newBuilder()
                    .putInputs("input", "groupByKey.out")
                    .putOutputs("output", "parDo.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections(
                "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
            .build();
    FusedPipeline fused =
        GreedyPipelineFuser.fuse(Pipeline.newBuilder().setComponents(components).build());

    assertThat(
        fused.getRunnerExecutedTransforms(),
        contains(
            PipelineNode.pTransform("impulse", components.getTransformsOrThrow("impulse")),
            PipelineNode.pTransform("groupByKey", components.getTransformsOrThrow("groupByKey"))));
    assertThat(
        fused.getFusedStages(),
        containsInAnyOrder(
            ExecutableStageMatcher.withInput("impulse.out")
                .withOutputs("read.out")
                .withTransforms("read"),
            ExecutableStageMatcher.withInput("groupByKey.out")
                .withNoOutputs()
                .withTransforms("parDo")));
  }

  /*
   * impulse -> .out -> read -> .out --> goTransform -> .out
   *                                  \
   *                                   -> pyTransform -> .out
   * becomes (impulse.out) -> read -> (read.out)
   *         (read.out) -> goTransform
   *         (read.out) -> pyTransform
   */
  @Test
  public void multipleEnvironmentsBecomesMultipleStages() {
    Components components =
        partialComponents
            .toBuilder()
            .putTransforms(
                "read",
                PTransform.newBuilder()
                    .putInputs("input", "impulse.out")
                    .putOutputs("output", "read.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections("read.out", PCollection.newBuilder().setUniqueName("read.out").build())
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
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("go"))
                                    .build()
                                    .toByteString()))
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
                                    .setWindowFn(
                                        SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections("py.out", PCollection.newBuilder().setUniqueName("py.out").build())
            .build();
    FusedPipeline fused =
        GreedyPipelineFuser.fuse(Pipeline.newBuilder().setComponents(components).build());

    // Impulse is the runner transform
    assertThat(fused.getRunnerExecutedTransforms(), hasSize(1));
    assertThat(fused.getFusedStages(), hasSize(3));
    assertThat(
        fused.getFusedStages(),
        containsInAnyOrder(
            ExecutableStageMatcher.withInput("impulse.out")
                .withOutputs("read.out")
                .withTransforms("read"),
            ExecutableStageMatcher.withInput("read.out")
                .withNoOutputs()
                .withTransforms("pyTransform"),
            ExecutableStageMatcher.withInput("read.out")
                .withNoOutputs()
                .withTransforms("goTransform")));
  }

  /*
   * goImpulse -> .out -> goRead -> .out \                    -> goParDo -> .out
   *                                      -> flatten -> .out |
   * pyImpulse -> .out -> pyRead -> .out /                    -> pyParDo -> .out
   *
   * becomes
   * (goImpulse.out) -> goRead -> goRead.out -> flatten -> (flatten.out)
   * (pyImpulse.out) -> pyRead -> pyRead.out -> flatten -> (flatten.out)
   * (flatten.out) -> goParDo
   * (flatten.out) -> pyParDo
   */
  @Test
  public void flattenWithHeterogenousInputsAndOutputsEntirelyMaterialized() {
    Components components =
        Components.newBuilder()
            .putTransforms(
                "pyImpulse",
                PTransform.newBuilder()
                    .putOutputs("output", "pyImpulse.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN))
                    .build())
            .putPcollections(
                "pyImpulse.out", PCollection.newBuilder().setUniqueName("pyImpulse.out").build())
            .putTransforms(
                "pyRead",
                PTransform.newBuilder()
                    .putInputs("input", "pyImpulse.out")
                    .putOutputs("output", "pyRead.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections(
                "pyRead.out", PCollection.newBuilder().setUniqueName("pyRead.out").build())
            .putTransforms(
                "goImpulse",
                PTransform.newBuilder()
                    .putOutputs("output", "goImpulse.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN))
                    .build())
            .putPcollections(
                "goImpulse.out", PCollection.newBuilder().setUniqueName("goImpulse.out").build())
            .putTransforms(
                "goRead",
                PTransform.newBuilder()
                    .putInputs("input", "goImpulse.out")
                    .putOutputs("output", "goRead.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("go"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections(
                "goRead.out", PCollection.newBuilder().setUniqueName("goRead.out").build())
            .putTransforms(
                "flatten",
                PTransform.newBuilder()
                    .putInputs("goReadInput", "goRead.out")
                    .putInputs("pyReadInput", "pyRead.out")
                    .putOutputs("output", "flatten.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN))
                    .build())
            .putPcollections(
                "flatten.out", PCollection.newBuilder().setUniqueName("flatten.out").build())
            .putTransforms(
                "pyParDo",
                PTransform.newBuilder()
                    .putInputs("input", "flatten.out")
                    .putOutputs("output", "pyParDo.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections(
                "pyParDo.out", PCollection.newBuilder().setUniqueName("pyParDo.out").build())
            .putTransforms(
                "goParDo",
                PTransform.newBuilder()
                    .putInputs("input", "flatten.out")
                    .putOutputs("output", "goParDo.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("go"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections(
                "goParDo.out", PCollection.newBuilder().setUniqueName("goParDo.out").build())
            .putEnvironments("go", Environment.newBuilder().setUrl("go").build())
            .putEnvironments("py", Environment.newBuilder().setUrl("py").build())
            .build();
    FusedPipeline fused =
        GreedyPipelineFuser.fuse(Pipeline.newBuilder().setComponents(components).build());

    assertThat(
        fused.getRunnerExecutedTransforms(),
        containsInAnyOrder(
            PipelineNode.pTransform("pyImpulse", components.getTransformsOrThrow("pyImpulse")),
            PipelineNode.pTransform("goImpulse", components.getTransformsOrThrow("goImpulse"))));
    assertThat(
        fused.getFusedStages(),
        containsInAnyOrder(
            ExecutableStageMatcher.withInput("goImpulse.out")
                .withOutputs("flatten.out")
                .withTransforms("goRead", "flatten"),
            ExecutableStageMatcher.withInput("pyImpulse.out")
                .withOutputs("flatten.out")
                .withTransforms("pyRead", "flatten"),
            ExecutableStageMatcher.withInput("flatten.out")
                .withNoOutputs()
                .withTransforms("goParDo"),
            ExecutableStageMatcher.withInput("flatten.out")
                .withNoOutputs()
                .withTransforms("pyParDo")));
  }

  /*
   * impulseA -> .out -> goRead -> .out \
   *                                     -> flatten -> .out -> goParDo -> .out
   * impulseB -> .out -> pyRead -> .out /
   *
   * becomes
   * (impulseA.out) -> goRead -> goRead.out -> flatten -> flatten.out -> goParDo
   * (impulseB.out) -> pyRead -> pyRead.out -> flatten -> (flatten.out)
   * (flatten.out) -> goParDo
   */
  @Test
  public void flattenWithHeterogeneousInputsSingleEnvOutputPartiallyMaterialized() {
    Components components =
        Components.newBuilder()
            .putTransforms(
                "pyImpulse",
                PTransform.newBuilder()
                    .putOutputs("output", "pyImpulse.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN))
                    .build())
            .putPcollections(
                "pyImpulse.out", PCollection.newBuilder().setUniqueName("pyImpulse.out").build())
            .putTransforms(
                "pyRead",
                PTransform.newBuilder()
                    .putInputs("input", "pyImpulse.out")
                    .putOutputs("output", "pyRead.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections(
                "pyRead.out", PCollection.newBuilder().setUniqueName("pyRead.out").build())
            .putTransforms(
                "goImpulse",
                PTransform.newBuilder()
                    .putOutputs("output", "goImpulse.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN))
                    .build())
            .putPcollections(
                "goImpulse.out", PCollection.newBuilder().setUniqueName("goImpulse.out").build())
            .putTransforms(
                "goRead",
                PTransform.newBuilder()
                    .putInputs("input", "goImpulse.out")
                    .putOutputs("output", "goRead.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("go"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections(
                "goRead.out", PCollection.newBuilder().setUniqueName("goRead.out").build())
            .putTransforms(
                "flatten",
                PTransform.newBuilder()
                    .putInputs("goReadInput", "goRead.out")
                    .putInputs("pyReadInput", "pyRead.out")
                    .putOutputs("output", "flatten.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN))
                    .build())
            .putPcollections(
                "flatten.out", PCollection.newBuilder().setUniqueName("flatten.out").build())
            .putTransforms(
                "goParDo",
                PTransform.newBuilder()
                    .putInputs("input", "flatten.out")
                    .putOutputs("output", "goParDo.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("go"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections(
                "goParDo.out", PCollection.newBuilder().setUniqueName("goParDo.out").build())
            .putEnvironments("go", Environment.newBuilder().setUrl("go").build())
            .putEnvironments("py", Environment.newBuilder().setUrl("py").build())
            .build();
    FusedPipeline fused =
        GreedyPipelineFuser.fuse(Pipeline.newBuilder().setComponents(components).build());

    assertThat(
        fused.getRunnerExecutedTransforms(),
        containsInAnyOrder(
            PipelineNode.pTransform("pyImpulse", components.getTransformsOrThrow("pyImpulse")),
            PipelineNode.pTransform("goImpulse", components.getTransformsOrThrow("goImpulse"))));

    assertThat(
        fused.getFusedStages(),
        containsInAnyOrder(
            ExecutableStageMatcher.withInput("goImpulse.out")
                .withNoOutputs()
                .withTransforms("goRead", "flatten", "goParDo"),
            ExecutableStageMatcher.withInput("pyImpulse.out")
                .withOutputs("flatten.out")
                .withTransforms("pyRead", "flatten"),
            ExecutableStageMatcher.withInput("flatten.out")
                .withNoOutputs()
                .withTransforms("goParDo")));
  }

  /*
   * impulseA -> .out -> flatten -> .out -> read -> .out -> parDo -> .out
   * becomes
   * (flatten.out) -> read -> parDo
   *
   * Flatten, specifically, doesn't fuse greedily into downstream environments or act as a sibling
   * to any of those nodes, but the routing is instead handled by the Runner.
   */
  @Test
  public void flattenAfterNoEnvDoesNotFuse() {
    Components components = partialComponents.toBuilder()
        .putTransforms("flatten",
            PTransform.newBuilder()
                .putInputs("impulseInput", "impulse.out")
                .putOutputs("output", "flatten.out")
                .setSpec(FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN)
                    .build())
                .build())
        .putPcollections("flatten.out",
            PCollection.newBuilder().setUniqueName("flatten.out").build())
        .putTransforms("read",
            PTransform.newBuilder()
                .putInputs("input", "flatten.out")
                .putOutputs("output", "read.out")
                .setSpec(FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(ParDoPayload.newBuilder()
                        .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                        .build()
                        .toByteString()))
                .build())
        .putPcollections("read.out", PCollection.newBuilder().setUniqueName("read.out").build())
        .putTransforms("parDo",
            PTransform.newBuilder()
                .putInputs("input", "read.out")
                .putOutputs("output", "parDo.out")
                .setSpec(FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(ParDoPayload.newBuilder()
                        .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py").build())
                        .build()
                        .toByteString()))
                .build())
        .putPcollections("parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
        .build();
    FusedPipeline fused =
        GreedyPipelineFuser.fuse(Pipeline.newBuilder().setComponents(components).build());

    assertThat(
        fused.getRunnerExecutedTransforms(),
        containsInAnyOrder(
            PipelineNode.pTransform("impulse", components.getTransformsOrThrow("impulse")),
            PipelineNode.pTransform("flatten", components.getTransformsOrThrow("flatten"))));
    assertThat(
        fused.getFusedStages(),
        contains(
            ExecutableStageMatcher.withInput("flatten.out")
                .withNoOutputs()
                .withTransforms("read", "parDo")));
  }

  /*
   * impulseA -> .out -> read -> .out -> leftParDo -> .out
   *                                  \ -> rightParDo -> .out
   *                                   ------> sideInputParDo -> .out
   *                                        /
   * impulseB -> .out -> side_read -> .out /
   *
   * becomes
   * (impulseA.out) -> read -> (read.out)
   * (read.out) -> leftParDo
   *            \
   *             -> rightParDo
   * (read.out) -> sideInputParDo
   * (impulseB.out) -> side_read
   */
  @Test
  public void sideInputRootsNewStage() {
    Components components =
        Components.newBuilder()
            .putTransforms(
                "mainImpulse",
                PTransform.newBuilder()
                    .putOutputs("output", "mainImpulse.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN))
                    .build())
            .putPcollections(
                "mainImpulse.out",
                PCollection.newBuilder().setUniqueName("mainImpulse.out").build())
            .putTransforms(
                "read",
                PTransform.newBuilder()
                    .putInputs("input", "mainImpulse.out")
                    .putOutputs("output", "read.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections("read.out", PCollection.newBuilder().setUniqueName("read.out").build())
            .putTransforms(
                "sideImpulse",
                PTransform.newBuilder()
                    .putOutputs("output", "sideImpulse.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN))
                    .build())
            .putPcollections(
                "sideImpulse.out",
                PCollection.newBuilder().setUniqueName("sideImpulse.out").build())
            .putTransforms(
                "sideRead",
                PTransform.newBuilder()
                    .putInputs("input", "sideImpulse.out")
                    .putOutputs("output", "sideRead.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections(
                "sideRead.out", PCollection.newBuilder().setUniqueName("sideRead.out").build())
            .putTransforms(
                "leftParDo",
                PTransform.newBuilder()
                    .putInputs("main", "read.out")
                    .putOutputs("output", "leftParDo.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString())
                            .build())
                    .build())
            .putPcollections(
                "leftParDo.out", PCollection.newBuilder().setUniqueName("leftParDo.out").build())
            .putTransforms(
                "rightParDo",
                PTransform.newBuilder()
                    .putInputs("main", "read.out")
                    .putOutputs("output", "rightParDo.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString())
                            .build())
                    .build())
            .putPcollections(
                "rightParDo.out", PCollection.newBuilder().setUniqueName("rightParDo.out").build())
            .putTransforms(
                "sideParDo",
                PTransform.newBuilder()
                    .putInputs("main", "read.out")
                    .putInputs("side", "sideRead.out")
                    .putOutputs("output", "sideParDo.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .putSideInputs("side", SideInput.getDefaultInstance())
                                    .build()
                                    .toByteString())
                            .build())
                    .build())
            .putPcollections(
                "sideParDo.out", PCollection.newBuilder().setUniqueName("sideParDo.out").build())
            .putEnvironments("py", Environment.newBuilder().setUrl("py").build())
            .build();

    FusedPipeline fused =
        GreedyPipelineFuser.fuse(Pipeline.newBuilder().setComponents(components).build());

    assertThat(
        fused.getRunnerExecutedTransforms(),
        containsInAnyOrder(
            PipelineNode.pTransform("mainImpulse", components.getTransformsOrThrow("mainImpulse")),
            PipelineNode.pTransform(
                "sideImpulse", components.getTransformsOrThrow("sideImpulse"))));
    assertThat(
        fused.getFusedStages(),
        containsInAnyOrder(
            ExecutableStageMatcher.withInput("mainImpulse.out")
                .withOutputs("read.out")
                .withTransforms("read"),
            ExecutableStageMatcher.withInput("read.out")
                .withNoOutputs()
                .withTransforms("leftParDo", "rightParDo"),
            ExecutableStageMatcher.withInput("read.out")
                .withNoOutputs()
                .withTransforms("sideParDo"),
            ExecutableStageMatcher.withInput("sideImpulse.out")
                .withNoOutputs()
                .withTransforms("sideRead")));
  }

  /*
   * impulse -> .out -> parDo -> .out -> stateful -> .out
   * becomes
   * (impulse.out) -> parDo -> (parDo.out)
   * (parDo.out) -> stateful
   */
  @Test
  public void statefulParDoRootsStage() {
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
                            .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
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
                            .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .putStateSpecs("state", StateSpec.getDefaultInstance())
                            .build()
                            .toByteString()))
            .build();

    Components components =
        partialComponents
            .toBuilder()
            .putTransforms("parDo", parDoTransform)
            .putPcollections(
                "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
            .putTransforms("stateful", statefulTransform)
            .putPcollections(
                "stateful.out", PCollection.newBuilder().setUniqueName("stateful.out").build())
            .putEnvironments("common", Environment.newBuilder().setUrl("common").build())
            .build();
    FusedPipeline fused =
        GreedyPipelineFuser.fuse(Pipeline.newBuilder().setComponents(components).build());

    assertThat(
        fused.getRunnerExecutedTransforms(),
        containsInAnyOrder(
            PipelineNode.pTransform("impulse", components.getTransformsOrThrow("impulse"))));
    assertThat(
        fused.getFusedStages(),
        containsInAnyOrder(
            ExecutableStageMatcher.withInput("impulse.out")
                .withOutputs("parDo.out")
                .withTransforms("parDo"),
            ExecutableStageMatcher.withInput("parDo.out")
                .withNoOutputs()
                .withTransforms("stateful")));
  }

  /*
   * impulse -> .out -> parDo -> .out -> timer -> .out
   * becomes
   * (impulse.out) -> parDo -> (parDo.out)
   * (parDo.out) -> timer
   */
  @Test
  public void parDoWithTimerRootsStage() {
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
                            .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
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
                            .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .putTimerSpecs("timer", TimerSpec.getDefaultInstance())
                            .build()
                            .toByteString()))
            .build();

    Components components =
        partialComponents
            .toBuilder()
            .putTransforms("parDo", parDoTransform)
            .putPcollections(
                "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
            .putTransforms("timer", timerTransform)
            .putPcollections(
                "timer.out", PCollection.newBuilder().setUniqueName("timer.out").build())
            .putEnvironments("common", Environment.newBuilder().setUrl("common").build())
            .build();

    FusedPipeline fused =
        GreedyPipelineFuser.fuse(Pipeline.newBuilder().setComponents(components).build());

    assertThat(
        fused.getRunnerExecutedTransforms(),
        containsInAnyOrder(
            PipelineNode.pTransform("impulse", components.getTransformsOrThrow("impulse"))));
    assertThat(
        fused.getFusedStages(),
        containsInAnyOrder(
            ExecutableStageMatcher.withInput("impulse.out")
                .withOutputs("parDo.out")
                .withTransforms("parDo"),
            ExecutableStageMatcher.withInput("parDo.out")
                .withNoOutputs()
                .withTransforms("timer")));
  }

  /*
   * impulse -> .out -> ( read -> .out --> goTransform -> .out )
   *                                    \
   *                                     -> pyTransform -> .out )
   * becomes (impulse.out) -> read -> (read.out)
   *         (read.out) -> goTransform
   *         (read.out) -> pyTransform
   */
  @Test
  public void compositesIgnored() {
    Components components =
        partialComponents
            .toBuilder()
            .putTransforms(
                "read",
                PTransform.newBuilder()
                    .putInputs("input", "impulse.out")
                    .putOutputs("output", "read.out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections("read.out", PCollection.newBuilder().setUniqueName("read.out").build())
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
                                    .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("go"))
                                    .build()
                                    .toByteString()))
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
                                    .setWindowFn(
                                        SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                    .build()
                                    .toByteString()))
                    .build())
            .putPcollections("py.out", PCollection.newBuilder().setUniqueName("py.out").build())
            .putTransforms(
                "compositeMultiLang",
                PTransform.newBuilder()
                    .putInputs("input", "impulse.out")
                    .putOutputs("pyOut", "py.out")
                    .putOutputs("goOut", "go.out")
                    .addSubtransforms("read")
                    .addSubtransforms("goTransform")
                    .addSubtransforms("pyTransform")
                    .build())
            .build();
    FusedPipeline fused =
        GreedyPipelineFuser.fuse(
            Pipeline.newBuilder()
                .addRootTransformIds("impulse")
                .addRootTransformIds("compositeMultiLang")
                .setComponents(components)
                .build());

    // Impulse is the runner transform
    assertThat(fused.getRunnerExecutedTransforms(), hasSize(1));
    assertThat(fused.getFusedStages(), hasSize(3));
    assertThat(
        fused.getFusedStages(),
        containsInAnyOrder(
            ExecutableStageMatcher.withInput("impulse.out")
                .withOutputs("read.out")
                .withTransforms("read"),
            ExecutableStageMatcher.withInput("read.out")
                .withNoOutputs()
                .withTransforms("pyTransform"),
            ExecutableStageMatcher.withInput("read.out")
                .withNoOutputs()
                .withTransforms("goTransform")));
  }
}
