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
package org.apache.beam.runners.prism;

import static com.google.common.truth.Truth.assertThat;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismArtifactResolver}. */
@RunWith(JUnit4.class)
public class PrismArtifactResolverTest {
  @Test
  public void resolvesPipeline() {
    Pipeline pipeline = Pipeline.create();
    pipeline.apply(Impulse.create());
    PrismArtifactResolver underTest = PrismArtifactResolver.of(pipeline);
    RunnerApi.Pipeline pipelineProto = underTest.resolvePipelineProto();
    RunnerApi.Components components = pipelineProto.getComponents();
    assertThat(components.getTransformsMap()).containsKey("Impulse");
    assertThat(components.getCodersMap()).containsKey("ByteArrayCoder");
    assertThat(components.getEnvironmentsMap())
        .containsKey(BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.DOCKER));
  }
}
