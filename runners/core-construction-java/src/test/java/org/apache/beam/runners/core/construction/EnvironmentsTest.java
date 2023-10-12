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

import static org.apache.beam.runners.core.construction.Environments.JAVA_SDK_HARNESS_CONTAINER_URL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ArtifactInformation;
import org.apache.beam.model.pipeline.v1.RunnerApi.DockerPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.ProcessPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardEnvironments;
import org.apache.beam.runners.core.construction.Environments.JavaVersion;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Environments}. */
@RunWith(JUnit4.class)
public class EnvironmentsTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  @Rule public transient ExpectedLogs expectedLogs = ExpectedLogs.none(Environments.class);

  @Test
  public void createEnvironmentDockerFromEnvironmentConfig() throws IOException {
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
  }

  @Test
  public void createEnvironmentDockerFromEnvironmentOptions() {
    PortablePipelineOptions options = PipelineOptionsFactory.as(PortablePipelineOptions.class);
    options.setDefaultEnvironmentType(Environments.ENVIRONMENT_DOCKER);
    options.setEnvironmentOptions(ImmutableList.of("docker_container_image=java"));
    assertThat(
        Environments.createOrGetDefaultEnvironment(options),
        is(
            Environment.newBuilder()
                .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.DOCKER))
                .setPayload(
                    DockerPayload.newBuilder().setContainerImage("java").build().toByteString())
                .addAllCapabilities(Environments.getJavaCapabilities())
                .build()));
  }

  @Test
  public void createEnvironmentProcessFromEnvironmentConfig() throws IOException {
    PortablePipelineOptions options = PipelineOptionsFactory.as(PortablePipelineOptions.class);
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
  public void createEnvironmentProcessFromEnvironmentOptions() {
    PortablePipelineOptions options = PipelineOptionsFactory.as(PortablePipelineOptions.class);
    options.setDefaultEnvironmentType(Environments.ENVIRONMENT_PROCESS);
    options.setEnvironmentOptions(
        ImmutableList.of("process_command=run.sh", "process_variables=k1=v1,k2=v2"));
    assertThat(
        Environments.createOrGetDefaultEnvironment(options),
        is(
            Environment.newBuilder()
                .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS))
                .setPayload(
                    ProcessPayload.newBuilder()
                        .setCommand("run.sh")
                        .putEnv("k1", "v1")
                        .putEnv("k2", "v2")
                        .build()
                        .toByteString())
                .addAllCapabilities(Environments.getJavaCapabilities())
                .build()));
  }

  @Test
  public void createEnvironmentExternalFromEnvironmentOptions() {
    PortablePipelineOptions options = PipelineOptionsFactory.as(PortablePipelineOptions.class);
    options.setDefaultEnvironmentType(Environments.ENVIRONMENT_EXTERNAL);
    options.setEnvironmentOptions(ImmutableList.of("external_service_address=foo"));
    assertThat(
        Environments.createOrGetDefaultEnvironment(options),
        is(
            Environment.newBuilder()
                .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.EXTERNAL))
                .setPayload(
                    RunnerApi.ExternalPayload.newBuilder()
                        .setEndpoint(
                            Endpoints.ApiServiceDescriptor.newBuilder().setUrl("foo").build())
                        .build()
                        .toByteString())
                .addAllCapabilities(Environments.getJavaCapabilities())
                .build()));
  }

  @Test
  public void environmentConfigAndEnvironmentOptionsAreMutuallyExclusive() {
    PortablePipelineOptions options = PipelineOptionsFactory.as(PortablePipelineOptions.class);
    options.setDefaultEnvironmentType(Environments.ENVIRONMENT_DOCKER);
    options.setDefaultEnvironmentConfig("foo");
    options.setEnvironmentOptions(ImmutableList.of("bar"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Pipeline options defaultEnvironmentConfig and environmentOptions are mutually exclusive.");
    Environments.createOrGetDefaultEnvironment(options);
  }

  @Test
  public void testCapabilities() {
    // Check a subset of coders
    assertThat(Environments.getJavaCapabilities(), hasItem(ModelCoders.LENGTH_PREFIX_CODER_URN));
    assertThat(Environments.getJavaCapabilities(), hasItem(ModelCoders.ROW_CODER_URN));
    // Check all protocol based capabilities
    assertThat(
        Environments.getJavaCapabilities(),
        hasItem(BeamUrns.getUrn(RunnerApi.StandardProtocols.Enum.MULTI_CORE_BUNDLE_PROCESSING)));
    assertThat(
        Environments.getJavaCapabilities(),
        hasItem(BeamUrns.getUrn(RunnerApi.StandardProtocols.Enum.PROGRESS_REPORTING)));
    assertThat(
        Environments.getJavaCapabilities(),
        hasItem(BeamUrns.getUrn(RunnerApi.StandardProtocols.Enum.HARNESS_MONITORING_INFOS)));
    assertThat(
        Environments.getJavaCapabilities(),
        hasItem(
            BeamUrns.getUrn(RunnerApi.StandardProtocols.Enum.CONTROL_REQUEST_ELEMENTS_EMBEDDING)));
    assertThat(
        Environments.getJavaCapabilities(),
        hasItem(BeamUrns.getUrn(RunnerApi.StandardProtocols.Enum.STATE_CACHING)));
    assertThat(
        Environments.getJavaCapabilities(),
        hasItem(BeamUrns.getUrn(RunnerApi.StandardProtocols.Enum.DATA_SAMPLING)));
    // Check that SDF truncation is supported
    assertThat(
        Environments.getJavaCapabilities(),
        hasItem(
            BeamUrns.getUrn(
                RunnerApi.StandardPTransforms.SplittableParDoComponents
                    .TRUNCATE_SIZED_RESTRICTION)));
    // Check that the sdk_base is inserted
    assertThat(
        Environments.getJavaCapabilities(),
        hasItem("beam:version:sdk_base:" + JAVA_SDK_HARNESS_CONTAINER_URL));
    // Check that ToString is supported for pretty printing user data.
    assertThat(
        Environments.getJavaCapabilities(),
        hasItem(BeamUrns.getUrn(RunnerApi.StandardPTransforms.Primitives.TO_STRING)));
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

  @Test
  public void testLtsJavaVersion() {
    assertEquals(JavaVersion.java8, JavaVersion.forSpecification("1.8"));
    assertEquals("java", JavaVersion.java8.legacyName());
    assertEquals(JavaVersion.java11, JavaVersion.forSpecification("11"));
    assertEquals("java11", JavaVersion.java11.legacyName());
    assertEquals(JavaVersion.java17, JavaVersion.forSpecification("17"));
    assertEquals("java17", JavaVersion.java17.legacyName());
    assertEquals(JavaVersion.java21, JavaVersion.forSpecification("21"));
    assertEquals("java21", JavaVersion.java21.legacyName());
  }

  @Test
  public void testNonLtsJavaVersion() {
    assertEquals(JavaVersion.java8, JavaVersion.forSpecification("9"));
    assertEquals(JavaVersion.java11, JavaVersion.forSpecification("10"));
    assertEquals(JavaVersion.java11, JavaVersion.forSpecification("12"));
    assertEquals(JavaVersion.java11, JavaVersion.forSpecification("13"));
    assertEquals(JavaVersion.java17, JavaVersion.forSpecification("14"));
    assertEquals(JavaVersion.java17, JavaVersion.forSpecification("15"));
    assertEquals(JavaVersion.java17, JavaVersion.forSpecification("16"));
    assertEquals(JavaVersion.java17, JavaVersion.forSpecification("18"));
    assertEquals(JavaVersion.java21, JavaVersion.forSpecification("19"));
    assertEquals(JavaVersion.java21, JavaVersion.forSpecification("20"));
    assertEquals(JavaVersion.java21, JavaVersion.forSpecification("21"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testJavaVersionStrictInvalid() {
    assertEquals(JavaVersion.java8, JavaVersion.forSpecificationStrict("invalid"));
  }

  @Test
  public void testGetArtifactsExistingNoLogs() throws Exception {
    File file1 = File.createTempFile("file1-", ".txt");
    file1.deleteOnExit();
    File file2 = File.createTempFile("file2-", ".txt");
    file2.deleteOnExit();

    List<ArtifactInformation> artifacts =
        Environments.getArtifacts(ImmutableList.of(file1.getAbsolutePath(), "file2=" + file2));

    assertThat(artifacts, hasSize(2));
    expectedLogs.verifyNotLogged("was not found");
  }

  @Test
  public void testGetArtifactsBadFileLogsInfo() throws Exception {
    File file1 = File.createTempFile("file1-", ".txt");
    file1.deleteOnExit();

    List<ArtifactInformation> artifacts =
        Environments.getArtifacts(ImmutableList.of(file1.getAbsolutePath(), "spurious_file"));

    assertThat(artifacts, hasSize(1));
    expectedLogs.verifyInfo("'spurious_file' was not found");
  }

  @Test
  public void testGetArtifactsBadNamedFileLogsWarn() throws Exception {
    File file1 = File.createTempFile("file1-", ".txt");
    file1.deleteOnExit();

    List<ArtifactInformation> artifacts =
        Environments.getArtifacts(
            ImmutableList.of(file1.getAbsolutePath(), "file_name=spurious_file"));

    assertThat(artifacts, hasSize(1));
    expectedLogs.verifyWarn("name 'file_name' was not found");
  }
}
