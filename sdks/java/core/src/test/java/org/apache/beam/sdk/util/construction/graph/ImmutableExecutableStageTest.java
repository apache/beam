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
package org.apache.beam.sdk.util.construction.graph;

import static org.apache.beam.sdk.util.construction.graph.ExecutableStage.DEFAULT_WIRE_CODER_SETTINGS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PTransformNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ImmutableExecutableStage}. */
@RunWith(JUnit4.class)
public class ImmutableExecutableStageTest {
  @Test
  public void ofFullComponentsOnlyHasStagePTransforms() throws Exception {
    Environment env = Environments.createDockerEnvironment("foo");
    PTransform pt =
        PTransform.newBuilder()
            .putInputs("input", "input.out")
            .putInputs("side_input", "sideInput.in")
            .putInputs("timer", "timer.pc")
            .putOutputs("output", "output.out")
            .putOutputs("timer", "timer.pc")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(RunnerApi.FunctionSpec.newBuilder())
                            .putSideInputs("side_input", RunnerApi.SideInput.getDefaultInstance())
                            .putStateSpecs("user_state", RunnerApi.StateSpec.getDefaultInstance())
                            .putTimerFamilySpecs(
                                "timer", RunnerApi.TimerFamilySpec.getDefaultInstance())
                            .build()
                            .toByteString()))
            .build();
    PCollection input = PCollection.newBuilder().setUniqueName("input.out").build();
    PCollection sideInput = PCollection.newBuilder().setUniqueName("sideInput.in").build();
    PCollection timer = PCollection.newBuilder().setUniqueName("timer.pc").build();
    PCollection output = PCollection.newBuilder().setUniqueName("output.out").build();

    Components components =
        Components.newBuilder()
            .putTransforms("pt", pt)
            .putTransforms("other_pt", PTransform.newBuilder().setUniqueName("other").build())
            .putPcollections("input.out", input)
            .putPcollections("sideInput.in", sideInput)
            .putPcollections("timer.pc", timer)
            .putPcollections("output.out", output)
            .putEnvironments("foo", env)
            .build();

    PTransformNode transformNode = PipelineNode.pTransform("pt", pt);
    SideInputReference sideInputRef =
        SideInputReference.of(
            transformNode, "side_input", PipelineNode.pCollection("sideInput.in", sideInput));
    UserStateReference userStateRef =
        UserStateReference.of(
            transformNode, "user_state", PipelineNode.pCollection("input.out", input));
    TimerReference timerRef = TimerReference.of(transformNode, "timer");
    ImmutableExecutableStage stage =
        ImmutableExecutableStage.ofFullComponents(
            components,
            env,
            PipelineNode.pCollection("input.out", input),
            Collections.singleton(sideInputRef),
            Collections.singleton(userStateRef),
            Collections.singleton(timerRef),
            Collections.singleton(PipelineNode.pTransform("pt", pt)),
            Collections.singleton(PipelineNode.pCollection("output.out", output)),
            DEFAULT_WIRE_CODER_SETTINGS);

    assertThat(stage.getComponents().containsTransforms("pt"), is(true));
    assertThat(stage.getComponents().containsTransforms("other_pt"), is(false));

    PTransform stagePTransform = stage.toPTransform("foo");
    assertThat(stagePTransform.getOutputsMap(), hasValue("output.out"));
    assertThat(stagePTransform.getOutputsCount(), equalTo(1));
    assertThat(
        stagePTransform.getInputsMap(), allOf(hasValue("input.out"), hasValue("sideInput.in")));
    assertThat(stagePTransform.getInputsCount(), equalTo(2));

    ExecutableStagePayload payload =
        ExecutableStagePayload.parseFrom(stagePTransform.getSpec().getPayload());
    assertThat(payload.getTransformsList(), contains("pt"));
    assertThat(ExecutableStage.fromPayload(payload), equalTo(stage));
  }
}
