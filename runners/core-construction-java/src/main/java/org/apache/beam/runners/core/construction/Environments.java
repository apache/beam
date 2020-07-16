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

import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ArtifactInformation;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.DockerPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExternalPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ProcessPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardArtifacts;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardEnvironments;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardProtocols;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.util.ZipFiles;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.HashCode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for interacting with portability {@link Environment environments}. */
public class Environments {
  private static final Logger LOG = LoggerFactory.getLogger(Environments.class);

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
      ReleaseInfo.getReleaseInfo().getDefaultDockerRepoRoot()
          + "/"
          + ReleaseInfo.getReleaseInfo().getDefaultDockerRepoPrefix()
          + "java_sdk:"
          + ReleaseInfo.getReleaseInfo().getSdkVersion();
  public static final Environment JAVA_SDK_HARNESS_ENVIRONMENT =
      createDockerEnvironment(JAVA_SDK_HARNESS_CONTAINER_URL);

  private Environments() {}

  public static Environment createOrGetDefaultEnvironment(PortablePipelineOptions options) {
    String type = options.getDefaultEnvironmentType();
    String config = options.getDefaultEnvironmentConfig();

    Environment defaultEnvironment;
    if (Strings.isNullOrEmpty(type)) {
      defaultEnvironment = JAVA_SDK_HARNESS_ENVIRONMENT;
    } else {
      switch (type) {
        case ENVIRONMENT_EMBEDDED:
          defaultEnvironment = createEmbeddedEnvironment(config);
          break;
        case ENVIRONMENT_EXTERNAL:
        case ENVIRONMENT_LOOPBACK:
          defaultEnvironment = createExternalEnvironment(config);
          break;
        case ENVIRONMENT_PROCESS:
          defaultEnvironment = createProcessEnvironment(config);
          break;
        case ENVIRONMENT_DOCKER:
        default:
          defaultEnvironment = createDockerEnvironment(config);
      }
    }
    return defaultEnvironment
        .toBuilder()
        .addAllDependencies(getDeferredArtifacts(options))
        .addAllCapabilities(getJavaCapabilities())
        .build();
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

  public static List<ArtifactInformation> getArtifacts(List<String> stagingFiles) {
    ImmutableList.Builder<ArtifactInformation> artifactsBuilder = ImmutableList.builder();
    Set<String> deduplicatedStagingFiles = new LinkedHashSet<>(stagingFiles);
    for (String path : deduplicatedStagingFiles) {
      File file;
      String stagedName = null;
      if (path.contains("=")) {
        String[] components = path.split("=", 2);
        file = new File(components[1]);
        stagedName = components[0];
      } else {
        file = new File(path);
      }
      // Spurious items get added to the classpath. Filter by just those that exist.
      if (file.exists()) {
        ArtifactInformation.Builder artifactBuilder = ArtifactInformation.newBuilder();
        artifactBuilder.setTypeUrn(BeamUrns.getUrn(StandardArtifacts.Types.FILE));
        artifactBuilder.setRoleUrn(BeamUrns.getUrn(StandardArtifacts.Roles.STAGING_TO));
        HashCode hashCode;
        if (file.isDirectory()) {
          File zippedFile;
          try {
            zippedFile = zipDirectory(file);
            hashCode = Files.asByteSource(zippedFile).hash(Hashing.sha256());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }

          artifactBuilder.setTypePayload(
              RunnerApi.ArtifactFilePayload.newBuilder()
                  .setPath(zippedFile.getPath())
                  .setSha256(hashCode.toString())
                  .build()
                  .toByteString());

        } else {
          try {
            hashCode = Files.asByteSource(file).hash(Hashing.sha256());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          artifactBuilder.setTypePayload(
              RunnerApi.ArtifactFilePayload.newBuilder()
                  .setPath(file.getPath())
                  .setSha256(hashCode.toString())
                  .build()
                  .toByteString());
        }
        if (stagedName == null) {
          stagedName = createStagingFileName(file, hashCode);
        }
        artifactBuilder.setRolePayload(
            RunnerApi.ArtifactStagingToRolePayload.newBuilder()
                .setStagedName(stagedName)
                .build()
                .toByteString());
        artifactsBuilder.add(artifactBuilder.build());
      }
    }
    return artifactsBuilder.build();
  }

  public static List<ArtifactInformation> getDeferredArtifacts(PipelineOptions options) {
    List<String> stagingFiles = options.as(PortablePipelineOptions.class).getFilesToStage();
    if (stagingFiles == null || stagingFiles.isEmpty()) {
      return ImmutableList.of();
    }

    String key = UUID.randomUUID().toString();
    DefaultArtifactResolver.INSTANCE.register(
        (info) -> {
          if (BeamUrns.getUrn(StandardArtifacts.Types.DEFERRED).equals(info.getTypeUrn())) {
            RunnerApi.DeferredArtifactPayload deferredArtifactPayload;
            try {
              deferredArtifactPayload =
                  RunnerApi.DeferredArtifactPayload.parseFrom(info.getTypePayload());
            } catch (InvalidProtocolBufferException e) {
              throw new RuntimeException("Error parsing deferred artifact payload.", e);
            }
            if (key.equals(deferredArtifactPayload.getKey())) {
              return Optional.of(getArtifacts(stagingFiles));
            } else {
              return Optional.empty();
            }
          } else {
            return Optional.empty();
          }
        });

    return ImmutableList.of(
        ArtifactInformation.newBuilder()
            .setTypeUrn(BeamUrns.getUrn(StandardArtifacts.Types.DEFERRED))
            .setTypePayload(
                RunnerApi.DeferredArtifactPayload.newBuilder().setKey(key).build().toByteString())
            .build());
  }

  public static Set<String> getJavaCapabilities() {
    ImmutableSet.Builder<String> capabilities = ImmutableSet.builder();
    capabilities.addAll(ModelCoders.urns());
    capabilities.add(BeamUrns.getUrn(StandardProtocols.Enum.MULTI_CORE_BUNDLE_PROCESSING));
    capabilities.add(BeamUrns.getUrn(StandardProtocols.Enum.PROGRESS_REPORTING));
    capabilities.add("beam:version:sdk_base:" + JAVA_SDK_HARNESS_CONTAINER_URL);
    // TODO(BEAM-BEAM-10505): Add the capability back.
    // capabilities.add(BeamUrns.getUrn(SplittableParDoComponents.TRUNCATE_SIZED_RESTRICTION));
    return capabilities.build();
  }

  public static String createStagingFileName(File path, HashCode hash) {
    String encodedHash = Base64Variants.MODIFIED_FOR_URL.encode(hash.asBytes());
    String fileName = Files.getNameWithoutExtension(path.getAbsolutePath());
    String ext = path.isDirectory() ? "jar" : Files.getFileExtension(path.getAbsolutePath());
    String suffix = Strings.isNullOrEmpty(ext) ? "" : "." + ext;
    return String.format("%s-%s%s", fileName, encodedHash, suffix);
  }

  private static File zipDirectory(File directory) throws IOException {
    File zipFile = File.createTempFile(directory.getName(), ".zip");
    try (FileOutputStream fos = new FileOutputStream(zipFile)) {
      ZipFiles.zipDirectory(directory, fos);
    }
    return zipFile;
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
