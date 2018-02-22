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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasValue;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the default and static methods of {@link ExecutableStage}.
 */
@RunWith(JUnit4.class)
public class ExecutableStageTest {
  @Test
  public void testRoundTripToFromTransform() {
    Environment env = Environment.newBuilder().setUrl("foo").build();
    PTransform pt =
        PTransform.newBuilder()
            .putInputs("input", "input.out")
            .putOutputs("output", "output.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("foo"))
                            .build()
                            .toByteString()))
            .build();
    PCollection input = PCollection.newBuilder().setUniqueName("input.out").build();
    PCollection output = PCollection.newBuilder().setUniqueName("output.out").build();

    ImmutableExecutableStage stage =
        ImmutableExecutableStage.of(
            env,
            PipelineNode.pCollection("input.out", input),
            Collections.singleton(PipelineNode.pTransform("pt", pt)),
            Collections.singleton(PipelineNode.pCollection("output.out", output)));

    Components components =
        Components.newBuilder()
            .putTransforms("pt", pt)
            .putPcollections("input.out", input)
            .putPcollections("output.out", output)
            .putEnvironments("foo", env)
            .build();

    PTransform stagePTransform = stage.toPTransform();
    assertThat(stagePTransform.getOutputsMap(), hasValue("output.out"));
    assertThat(stagePTransform.getOutputsCount(), equalTo(1));
    assertThat(stagePTransform.getInputsMap(), hasValue("input.out"));
    assertThat(stagePTransform.getInputsCount(), equalTo(1));
    assertThat(stagePTransform.getSubtransformsList(), contains("pt"));

    assertThat(ExecutableStage.fromPTransform(stagePTransform, components), equalTo(stage));
  }

  @Test
  public void testRoundTripToFromTransformFused() {
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
    PTransform windowTransform =
        PTransform.newBuilder()
            .putInputs("input", "impulse.out")
            .putOutputs("output", "window.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                    .setPayload(
                        WindowIntoPayload.newBuilder()
                            .setWindowFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();

    Components components = Components.newBuilder()
        .putTransforms("impulse",
            PTransform.newBuilder()
                .putOutputs("output", "impulse.out")
                .setSpec(FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN))
                .build())
        .putPcollections("impulse.out",
            PCollection.newBuilder().setUniqueName("impulse.out").build())
        .putTransforms("parDo", parDoTransform)
        .putPcollections("parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
        .putTransforms("window", windowTransform)
        .putPcollections("window.out", PCollection.newBuilder().setUniqueName("window.out").build())
        .putEnvironments("common", Environment.newBuilder().setUrl("common").build())
        .build();
    QueryablePipeline p = QueryablePipeline.forPrimitivesIn(components);

    ExecutableStage subgraph =
        GreedyStageFuser.forGrpcPortRead(
            p,
            PipelineNode.pCollection(
                "impulse.out", PCollection.newBuilder().setUniqueName("impulse.out").build()),
            ImmutableSet.of(
                PipelineNode.pTransform("parDo", parDoTransform),
                PipelineNode.pTransform("window", windowTransform)));

    PTransform ptransform = subgraph.toPTransform();
    assertThat(ptransform.getSpec().getUrn(), equalTo(ExecutableStage.URN));
    assertThat(ptransform.getInputsMap().values(), containsInAnyOrder("impulse.out"));
    assertThat(ptransform.getOutputsMap().values(), emptyIterable());
    assertThat(ptransform.getSubtransformsList(), contains("parDo", "window"));

    ExecutableStage desered = ExecutableStage.fromPTransform(ptransform, components);
    assertThat(desered, equalTo(subgraph));
  }
}
