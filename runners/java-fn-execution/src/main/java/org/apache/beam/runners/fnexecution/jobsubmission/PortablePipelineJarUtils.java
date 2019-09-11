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
package org.apache.beam.runners.fnexecution.jobsubmission;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest.Location;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.ArtifactServiceStager;
import org.apache.beam.runners.core.construction.ArtifactServiceStager.StagedFile;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.artifact.BeamFileSystemArtifactStagingService;
import org.apache.beam.sdk.fn.test.InProcessManagedChannelFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Message.Builder;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.util.JsonFormat;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains common code for writing and reading portable pipeline jars.
 *
 * <p>Jar layout:
 *
 * <ul>
 *   <li>META-INF/
 *       <ul>
 *         <li>MANIFEST.MF
 *       </ul>
 *   <li>BEAM-PIPELINE/
 *       <ul>
 *         <li>pipeline.json
 *         <li>pipeline-options.json
 *       </ul>
 *   <li>BEAM-ARTIFACT-STAGING/
 *       <ul>
 *         <li>artifact-manifest.json
 *         <li>artifacts/
 *             <ul>
 *               <li>...artifact files...
 *             </ul>
 *       </ul>
 *   <li>...Java classes...
 * </ul>
 */
public abstract class PortablePipelineJarUtils {
  private static final String ARTIFACT_STAGING_FOLDER_PATH = "BEAM-ARTIFACT-STAGING";
  static final String ARTIFACT_FOLDER_PATH = ARTIFACT_STAGING_FOLDER_PATH + "/artifacts";
  private static final String PIPELINE_FOLDER_PATH = "BEAM-PIPELINE";
  static final String ARTIFACT_MANIFEST_PATH =
      ARTIFACT_STAGING_FOLDER_PATH + "/artifact-manifest.json";
  static final String PIPELINE_PATH = PIPELINE_FOLDER_PATH + "/pipeline.json";
  static final String PIPELINE_OPTIONS_PATH = PIPELINE_FOLDER_PATH + "/pipeline-options.json";

  private static final Logger LOG = LoggerFactory.getLogger(PortablePipelineJarCreator.class);

  private static InputStream getResourceFromClassPath(String resourcePath) throws IOException {
    InputStream inputStream = PortablePipelineJarUtils.class.getResourceAsStream(resourcePath);
    if (inputStream == null) {
      throw new FileNotFoundException(
          String.format("Resource %s not found on classpath.", resourcePath));
    }
    return inputStream;
  }

  /** Populates {@code builder} using the JSON resource specified by {@code resourcePath}. */
  private static void parseJsonResource(String resourcePath, Builder builder) throws IOException {
    try (InputStream inputStream = getResourceFromClassPath(resourcePath)) {
      String contents = new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8);
      JsonFormat.parser().merge(contents, builder);
    }
  }

  public static Pipeline getPipelineFromClasspath() throws IOException {
    Pipeline.Builder builder = Pipeline.newBuilder();
    parseJsonResource("/" + PIPELINE_PATH, builder);
    return builder.build();
  }

  public static Struct getPipelineOptionsFromClasspath() throws IOException {
    Struct.Builder builder = Struct.newBuilder();
    parseJsonResource("/" + PIPELINE_OPTIONS_PATH, builder);
    return builder.build();
  }

  public static ProxyManifest getArtifactManifestFromClassPath() throws IOException {
    ProxyManifest.Builder builder = ProxyManifest.newBuilder();
    parseJsonResource("/" + ARTIFACT_MANIFEST_PATH, builder);
    return builder.build();
  }

  /** Writes artifacts listed in {@code proxyManifest}. */
  public static String stageArtifacts(
      ProxyManifest proxyManifest,
      PipelineOptions options,
      String invocationId,
      String artifactStagingPath)
      throws Exception {
    Collection<StagedFile> filesToStage =
        prepareArtifactsForStaging(proxyManifest, options, invocationId);
    try (GrpcFnServer artifactServer =
        GrpcFnServer.allocatePortAndCreateFor(
            new BeamFileSystemArtifactStagingService(), InProcessServerFactory.create())) {
      ManagedChannel grpcChannel =
          InProcessManagedChannelFactory.create()
              .forDescriptor(artifactServer.getApiServiceDescriptor());
      ArtifactServiceStager stager = ArtifactServiceStager.overChannel(grpcChannel);
      String stagingSessionToken =
          BeamFileSystemArtifactStagingService.generateStagingSessionToken(
              invocationId, artifactStagingPath);
      String retrievalToken = stager.stage(stagingSessionToken, filesToStage);
      // Clean up.
      for (StagedFile file : filesToStage) {
        if (!file.getFile().delete()) {
          LOG.warn("Failed to delete file {}", file.getFile());
        }
      }
      grpcChannel.shutdown();
      return retrievalToken;
    }
  }

  /**
   * Artifacts are expected to exist as resources on the classpath, located using {@code
   * proxyManifest}. Write them to tmp files so they can be staged.
   */
  private static Collection<StagedFile> prepareArtifactsForStaging(
      ProxyManifest proxyManifest, PipelineOptions options, String invocationId)
      throws IOException {
    List<StagedFile> filesToStage = new ArrayList<>();
    Path outputFolderPath =
        Paths.get(
            MoreObjects.firstNonNull(
                options.getTempLocation(), System.getProperty("java.io.tmpdir")),
            invocationId);
    if (!outputFolderPath.toFile().mkdir()) {
      throw new IOException("Failed to create folder " + outputFolderPath);
    }
    for (Location location : proxyManifest.getLocationList()) {
      try (InputStream inputStream = getResourceFromClassPath(location.getUri())) {
        Path outputPath = outputFolderPath.resolve(UUID.randomUUID().toString());
        LOG.trace("Writing artifact {} to file {}", location.getName(), outputPath);
        File file = outputPath.toFile();
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
          IOUtils.copy(inputStream, outputStream);
          filesToStage.add(StagedFile.of(file, location.getName()));
        }
      }
    }
    return filesToStage;
  }
}
