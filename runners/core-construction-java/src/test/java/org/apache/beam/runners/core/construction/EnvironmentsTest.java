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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.DockerPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.ProcessPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.ReadPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardEnvironments;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PartitioningWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Environments}. */
@RunWith(JUnit4.class)
public class EnvironmentsTest implements Serializable {
  @Test
  public void createEnvironments() throws IOException {
    assertThat(
        Environments.createOrGetDefaultEnvironment(Environments.ENVIRONMENT_DOCKER, "java"),
        is(
            Environment.newBuilder()
                .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.DOCKER))
                .setPayload(
                    DockerPayload.newBuilder().setContainerImage("java").build().toByteString())
                .build()));
    assertThat(
        Environments.createOrGetDefaultEnvironment(
            Environments.ENVIRONMENT_PROCESS,
            "{\"os\": \"linux\", \"arch\": \"amd64\", \"command\": \"run.sh\", \"env\":{\"k1\": \"v1\", \"k2\": \"v2\"} }"),
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
                .build()));
    assertThat(
        Environments.createOrGetDefaultEnvironment(
            Environments.ENVIRONMENT_PROCESS, "{\"command\": \"run.sh\"}"),
        is(
            Environment.newBuilder()
                .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS))
                .setPayload(ProcessPayload.newBuilder().setCommand("run.sh").build().toByteString())
                .build()));
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
  public void getEnvironmentParDo() throws IOException {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    ParDoPayload payload =
        ParDoTranslation.translateParDo(
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {}
                    })
                .withOutputTags(new TupleTag<>(), TupleTagList.empty()),
            DoFnSchemaInformation.create(),
            Pipeline.create(),
            components);
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(components.toComponents());
    PTransform builder =
        PTransform.newBuilder()
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(payload.toByteString())
                    .build())
            .build();
    Environment env = Environments.getEnvironment(builder, rehydratedComponents).get();
    assertThat(
        env,
        equalTo(
            components
                .toComponents()
                .getEnvironmentsOrThrow(payload.getDoFn().getEnvironmentId())));
  }

  @Test
  public void getEnvironmentWindowIntoKnown() throws IOException {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    WindowIntoPayload payload =
        WindowIntoPayload.newBuilder()
            .setWindowFn(
                WindowingStrategyTranslation.toProto(
                    FixedWindows.of(Duration.standardMinutes(5L)), components))
            .build();
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(components.toComponents());
    PTransform builder =
        PTransform.newBuilder()
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                    .setPayload(payload.toByteString())
                    .build())
            .build();
    Environment env = Environments.getEnvironment(builder, rehydratedComponents).get();
    assertThat(
        env,
        equalTo(
            components
                .toComponents()
                .getEnvironmentsOrThrow(payload.getWindowFn().getEnvironmentId())));
  }

  @Test
  public void getEnvironmentWindowIntoCustom() throws IOException {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    WindowIntoPayload payload =
        WindowIntoPayload.newBuilder()
            .setWindowFn(
                WindowingStrategyTranslation.toProto(
                    new PartitioningWindowFn<Object, BoundedWindow>() {
                      @Override
                      public BoundedWindow assignWindow(Instant timestamp) {
                        return null;
                      }

                      @Override
                      public boolean isCompatible(WindowFn<?, ?> other) {
                        return false;
                      }

                      @Override
                      public Coder<BoundedWindow> windowCoder() {
                        return null;
                      }
                    },
                    components))
            .build();
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(components.toComponents());
    PTransform builder =
        PTransform.newBuilder()
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                    .setPayload(payload.toByteString())
                    .build())
            .build();
    Environment env = Environments.getEnvironment(builder, rehydratedComponents).get();
    assertThat(
        env,
        equalTo(
            components
                .toComponents()
                .getEnvironmentsOrThrow(payload.getWindowFn().getEnvironmentId())));
  }

  @Test
  public void getEnvironmentRead() throws IOException {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    ReadPayload payload = ReadTranslation.toProto(Read.from(CountingSource.upTo(10)), components);
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(components.toComponents());
    PTransform builder =
        PTransform.newBuilder()
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN)
                    .setPayload(payload.toByteString())
                    .build())
            .build();
    Environment env = Environments.getEnvironment(builder, rehydratedComponents).get();
    assertThat(
        env,
        equalTo(
            components
                .toComponents()
                .getEnvironmentsOrThrow(payload.getSource().getEnvironmentId())));
  }

  @Test
  public void getEnvironmentCombine() throws IOException {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    CombinePayload payload =
        CombinePayload.newBuilder()
            .setCombineFn(CombineTranslation.toProto(Sum.ofLongs(), components))
            .build();
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(components.toComponents());
    PTransform builder =
        PTransform.newBuilder()
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN)
                    .setPayload(payload.toByteString())
                    .build())
            .build();
    Environment env = Environments.getEnvironment(builder, rehydratedComponents).get();
    assertThat(
        env,
        equalTo(
            components
                .toComponents()
                .getEnvironmentsOrThrow(payload.getCombineFn().getEnvironmentId())));
  }
}
