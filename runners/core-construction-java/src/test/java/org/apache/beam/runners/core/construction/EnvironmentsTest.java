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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.DockerPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.ProcessPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardEnvironments;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Environments}. */
@RunWith(JUnit4.class)
public class EnvironmentsTest implements Serializable {
  @Test
  public void createEnvironments() throws IOException {
    PortablePipelineOptions options = PipelineOptionsFactory.as(PortablePipelineOptions.class);
    options.setDefaultEnvironmentType(Environments.ENVIRONMENT_DOCKER);
    options.setDefaultEnvironmentConfig("java");
    assertThat(
        Environments.createOrGetDefaultEnvironment(options),
        is(
            Environment.newBuilder()
                .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.DOCKER))
                .setPayload(
                    DockerPayload.newBuilder().setContainerImage("java").build().toByteString())
                .addAllCapabilities(Environments.getJavaCapabilities())
                .build()));
    options.setDefaultEnvironmentType(Environments.ENVIRONMENT_PROCESS);
    options.setDefaultEnvironmentConfig(
        "{\"os\": \"linux\", \"arch\": \"amd64\", \"command\": \"run.sh\", \"env\":{\"k1\": \"v1\", \"k2\": \"v2\"} }");
    assertThat(
        Environments.createOrGetDefaultEnvironment(options),
        is(
            Environment.newBuilder()
                .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS))
                .setPayload(
                    ProcessPayload.newBuilder()
                        .setOs("linux")
                        .setArch("amd64")
                        .setCommand("run.sh")
                        .putEnv("k1", "v1")
                        .putEnv("k2", "v2")
                        .build()
                        .toByteString())
                .addAllCapabilities(Environments.getJavaCapabilities())
                .build()));
    options.setDefaultEnvironmentType(Environments.ENVIRONMENT_PROCESS);
    options.setDefaultEnvironmentConfig("{\"command\": \"run.sh\"}");
    assertThat(
        Environments.createOrGetDefaultEnvironment(options),
        is(
            Environment.newBuilder()
                .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS))
                .setPayload(ProcessPayload.newBuilder().setCommand("run.sh").build().toByteString())
                .addAllCapabilities(Environments.getJavaCapabilities())
                .build()));
  }

  @Test
  public void testCapabilities() {
    assertThat(Environments.getJavaCapabilities(), hasItem(ModelCoders.LENGTH_PREFIX_CODER_URN));
    assertThat(Environments.getJavaCapabilities(), hasItem(ModelCoders.ROW_CODER_URN));
    assertThat(
        Environments.getJavaCapabilities(),
        hasItem(BeamUrns.getUrn(RunnerApi.StandardProtocols.Enum.MULTI_CORE_BUNDLE_PROCESSING)));
    // TODO(BEAM-10505): Add the check back.
    // assertThat(
    //     Environments.getJavaCapabilities(),
    //     hasItem(
    //         BeamUrns.getUrn(
    //             RunnerApi.StandardPTransforms.SplittableParDoComponents
    //                 .TRUNCATE_SIZED_RESTRICTION)));
  }

  @Test
  public void getEnvironmentUnknownFnType() throws IOException {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(components.toComponents());
    PTransform builder =
        PTransform.newBuilder()
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN)
                    .build())
            .build();
    Optional<Environment> env = Environments.getEnvironment(builder, rehydratedComponents);
    assertThat(env.isPresent(), is(false));
  }

  @Test
  public void getEnvironmentPTransform() throws IOException {
    Pipeline p = Pipeline.create();
    SdkComponents components = SdkComponents.create();
    Environment env = Environments.createDockerEnvironment("java");
    components.registerEnvironment(env);
    ParDoPayload payload =
        ParDoTranslation.translateParDo(
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {}
                    })
                .withOutputTags(new TupleTag<>(), TupleTagList.empty()),
            PCollection.createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, StringUtf8Coder.of()),
            DoFnSchemaInformation.create(),
            Pipeline.create(),
            components);
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(components.toComponents());
    PTransform ptransform =
        PTransform.newBuilder()
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(payload.toByteString())
                    .build())
            .setEnvironmentId(components.getOnlyEnvironmentId())
            .build();
    Environment env1 = Environments.getEnvironment(ptransform, rehydratedComponents).get();
    assertThat(
        env1,
        equalTo(components.toComponents().getEnvironmentsOrThrow(ptransform.getEnvironmentId())));
  }
}
