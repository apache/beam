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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.DockerPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExternalPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ProcessPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardEnvironments;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;

/** Utilities for interacting with portability {@link Environment environments}. */
public class Environments {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));
  public static final String ENVIRONMENT_DOCKER = "DOCKER";
  public static final String ENVIRONMENT_PROCESS = "PROCESS";
  public static final String ENVIRONMENT_EXTERNAL = "EXTERNAL";
  public static final String ENVIRONMENT_EMBEDDED = "EMBEDDED"; // Non Public urn for testing
  public static final String ENVIRONMENT_LOOPBACK = "LOOPBACK"; // Non Public urn for testing

  /* For development, use the container build by the current user to ensure that the SDK harness and
   * the SDK agree on how they should interact. This should be changed to a version-specific
   * container during a release.
   *
   * See https://beam.apache.org/contribute/docker-images/ for more information on how to build a
   * container.
   */
  private static final String JAVA_SDK_HARNESS_CONTAINER_URL =
      "apachebeam/java_sdk:" + ReleaseInfo.getReleaseInfo().getVersion();
  public static final Environment JAVA_SDK_HARNESS_ENVIRONMENT =
      createDockerEnvironment(JAVA_SDK_HARNESS_CONTAINER_URL);

  private Environments() {}

  public static Environment createOrGetDefaultEnvironment(String type, String config) {
    if (Strings.isNullOrEmpty(type)) {
      return JAVA_SDK_HARNESS_ENVIRONMENT;
    }

    switch (type) {
      case ENVIRONMENT_EMBEDDED:
        return createEmbeddedEnvironment(config);
      case ENVIRONMENT_EXTERNAL:
      case ENVIRONMENT_LOOPBACK:
        return createExternalEnvironment(config);
      case ENVIRONMENT_PROCESS:
        return createProcessEnvironment(config);
      case ENVIRONMENT_DOCKER:
      default:
        return createDockerEnvironment(config);
    }
  }

  public static Environment createDockerEnvironment(String dockerImageUrl) {
    if (Strings.isNullOrEmpty(dockerImageUrl)) {
      return JAVA_SDK_HARNESS_ENVIRONMENT;
    }
    return Environment.newBuilder()
        .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.DOCKER))
        .setPayload(
            DockerPayload.newBuilder().setContainerImage(dockerImageUrl).build().toByteString())
        .build();
  }

  private static Environment createExternalEnvironment(String config) {
    return Environment.newBuilder()
        .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.EXTERNAL))
        .setPayload(
            ExternalPayload.newBuilder()
                .setEndpoint(ApiServiceDescriptor.newBuilder().setUrl(config).build())
                .build()
                .toByteString())
        .build();
  }

  private static Environment createProcessEnvironment(String config) {
    try {
      ProcessPayloadReferenceJSON payloadReferenceJSON =
          MAPPER.readValue(config, ProcessPayloadReferenceJSON.class);
      return createProcessEnvironment(
          payloadReferenceJSON.getOs(),
          payloadReferenceJSON.getArch(),
          payloadReferenceJSON.getCommand(),
          payloadReferenceJSON.getEnv());
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Unable to parse process environment config: %s", config), e);
    }
  }

  private static Environment createEmbeddedEnvironment(String config) {
    return Environment.newBuilder()
        .setUrn(ENVIRONMENT_EMBEDDED)
        .setPayload(ByteString.copyFromUtf8(MoreObjects.firstNonNull(config, "")))
        .build();
  }

  public static Environment createProcessEnvironment(
      String os, String arch, String command, Map<String, String> env) {
    ProcessPayload.Builder builder = ProcessPayload.newBuilder();
    if (!Strings.isNullOrEmpty(os)) {
      builder.setOs(os);
    }
    if (!Strings.isNullOrEmpty(arch)) {
      builder.setArch(arch);
    }
    if (!Strings.isNullOrEmpty(command)) {
      builder.setCommand(command);
    }
    if (env != null) {
      builder.putAllEnv(env);
    }
    return Environment.newBuilder()
        .setUrn(BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS))
        .setPayload(builder.build().toByteString())
        .build();
  }

  public static Optional<Environment> getEnvironment(String ptransformId, Components components) {
    PTransform ptransform = components.getTransformsOrThrow(ptransformId);
    String envId = ptransform.getEnvironmentId();
    if (Strings.isNullOrEmpty(envId)) {
      // Some PTransform payloads may have an unspecified (empty) Environment ID, for example a
      // WindowIntoPayload with a known WindowFn. Others will never have an Environment ID, such
      // as a GroupByKeyPayload, and we return null in this case.
      return Optional.empty();
    } else {
      return Optional.of(components.getEnvironmentsOrThrow(envId));
    }
  }

  public static Optional<Environment> getEnvironment(
      PTransform ptransform, RehydratedComponents components) {
    String envId = ptransform.getEnvironmentId();
    if (Strings.isNullOrEmpty(envId)) {
      return Optional.empty();
    } else {
      // Some PTransform payloads may have an empty (default) Environment ID, for example a
      // WindowIntoPayload with a known WindowFn. Others will never have an Environment ID, such
      // as a GroupByKeyPayload, and we return null in this case.
      return Optional.of(components.getEnvironment(envId));
    }
  }

  private static class ProcessPayloadReferenceJSON {
    @Nullable private String os;
    @Nullable private String arch;
    @Nullable private String command;
    @Nullable private Map<String, String> env;

    @Nullable
    public String getOs() {
      return os;
    }

    @Nullable
    public String getArch() {
      return arch;
    }

    @Nullable
    public String getCommand() {
      return command;
    }

    @Nullable
    public Map<String, String> getEnv() {
      return env;
    }
  }
}
