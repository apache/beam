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
package org.apache.beam.runners.fnexecution.artifact;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest.Location;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceImplBase;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation is experimental.
 *
 * <p>{@link ArtifactStagingServiceImplBase} based on beam file system. {@link
 * BeamFileSystemArtifactStagingService} requires {@link StagingSessionToken} in every me call. The
 * manifest is put in {@link StagingSessionToken#getBasePath()}/{@link
 * StagingSessionToken#getSessionId()} and artifacts are put in {@link
 * StagingSessionToken#getBasePath()}/{@link StagingSessionToken#getSessionId()}/{@link
 * BeamFileSystemArtifactStagingService#ARTIFACTS}.
 *
 * <p>The returned token is the path to the manifest file.
 *
 * <p>The manifest file is encoded in {@link ProxyManifest}.
 */
public class BeamFileSystemArtifactStagingService extends AbstractArtifactStagingService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BeamFileSystemArtifactStagingService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  // Use UTF8 for all text encoding.
  public static final String MANIFEST = "MANIFEST";
  public static final String ARTIFACTS = "artifacts";

  @Override
  public String getArtifactUri(String stagingSession, String encodedFileName) throws Exception {
    StagingSessionToken stagingSessionToken = StagingSessionToken.decode(stagingSession);
    ResourceId artifactDirResourceId = getArtifactDirResourceId(stagingSessionToken);
    return artifactDirResourceId
        .resolve(encodedFileName, StandardResolveOptions.RESOLVE_FILE)
        .toString();
  }

  @Override
  public WritableByteChannel openUri(String uri) throws IOException {
    return FileSystems.create(FileSystems.matchNewResource(uri, false), MimeTypes.BINARY);
  }

  @Override
  public void removeUri(String uri) throws IOException {
    FileSystems.delete(
        Collections.singletonList(FileSystems.matchNewResource(uri, false)),
        StandardMoveOptions.IGNORE_MISSING_FILES);
  }

  @Override
  public void removeArtifacts(String stagingSessionToken) throws Exception {
    StagingSessionToken parsedToken = StagingSessionToken.decode(stagingSessionToken);
    ResourceId dir = getJobDirResourceId(parsedToken);
    ResourceId manifestResourceId = dir.resolve(MANIFEST, StandardResolveOptions.RESOLVE_FILE);

    LOG.debug("Removing dir {}", dir);

    ProxyManifest proxyManifest =
        BeamFileSystemArtifactRetrievalService.loadManifest(manifestResourceId);
    for (Location location : proxyManifest.getLocationList()) {
      String uri = location.getUri();
      LOG.debug("Removing artifact: {}", uri);
      FileSystems.delete(
          Collections.singletonList(FileSystems.matchNewResource(uri, false /* is directory */)));
    }

    ResourceId artifactsResourceId =
        dir.resolve(ARTIFACTS, StandardResolveOptions.RESOLVE_DIRECTORY);
    if (!proxyManifest.getLocationList().isEmpty()) {
      // directory only exists when there is at least one artifact
      LOG.debug("Removing artifacts dir: {}", artifactsResourceId);
      FileSystems.delete(Collections.singletonList(artifactsResourceId));
    }
    LOG.debug("Removing manifest: {}", manifestResourceId);
    FileSystems.delete(Collections.singletonList(manifestResourceId));
    LOG.debug("Removing empty dir: {}", dir);
    FileSystems.delete(Collections.singletonList(dir));
    LOG.info("Removed dir {}", dir);
  }

  @Override
  public WritableByteChannel openManifest(String stagingSession) throws Exception {
    return FileSystems.create(
        getManifestFileResourceId(StagingSessionToken.decode(stagingSession)), MimeTypes.TEXT);
  }

  @Override
  public String getRetrievalToken(String stagingSession) throws Exception {
    StagingSessionToken stagingSessionToken = StagingSessionToken.decode(stagingSession);
    ResourceId manifestResourceId = getManifestFileResourceId(stagingSessionToken);
    return manifestResourceId.toString();
  }

  private ResourceId getJobDirResourceId(StagingSessionToken stagingSessionToken) {
    ResourceId baseResourceId;
    // Get or Create the base path
    baseResourceId =
        FileSystems.matchNewResource(stagingSessionToken.getBasePath(), true /* isDirectory */);
    // Using sessionId as the subDir to store artifacts and manifest.
    return baseResourceId.resolve(
        stagingSessionToken.getSessionId(), StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  private ResourceId getManifestFileResourceId(StagingSessionToken stagingSessionToken) {
    return getJobDirResourceId(stagingSessionToken)
        .resolve(MANIFEST, StandardResolveOptions.RESOLVE_FILE);
  }

  private ResourceId getArtifactDirResourceId(StagingSessionToken stagingSessionToken) {
    return getJobDirResourceId(stagingSessionToken)
        .resolve(ARTIFACTS, StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  /**
   * Generate a stagingSessionToken compatible with {@link BeamFileSystemArtifactStagingService}.
   *
   * @param sessionId Unique sessionId for artifact staging.
   * @param basePath Base path to upload artifacts.
   * @return Encoded stagingSessionToken.
   */
  public static String generateStagingSessionToken(String sessionId, String basePath) {
    StagingSessionToken stagingSessionToken = new StagingSessionToken();
    stagingSessionToken.setSessionId(sessionId);
    stagingSessionToken.setBasePath(basePath);
    return stagingSessionToken.encode();
  }

  /**
   * Serializable StagingSessionToken used to stage files with {@link
   * BeamFileSystemArtifactStagingService}.
   */
  protected static class StagingSessionToken implements Serializable {

    private String sessionId;
    private String basePath;

    /** Access is public for json conversion. */
    public String getSessionId() {
      return sessionId;
    }

    private void setSessionId(String sessionId) {
      this.sessionId = sessionId;
    }

    /** Access is public for json conversion. */
    public String getBasePath() {
      return basePath;
    }

    private void setBasePath(String basePath) {
      this.basePath = basePath;
    }

    public String encode() {
      try {
        return MAPPER.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        String message =
            String.format("Error %s occurred while serializing %s", e.getMessage(), this);
        throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(message));
      }
    }

    public static StagingSessionToken decode(String stagingSessionToken) throws Exception {
      try {
        return MAPPER.readValue(stagingSessionToken, StagingSessionToken.class);
      } catch (JsonProcessingException e) {
        String message =
            String.format(
                "Unable to deserialize staging token %s. Expected format: %s. Error: %s",
                stagingSessionToken,
                "{\"sessionId\": \"sessionId\", \"basePath\": \"basePath\"}",
                e.getMessage());
        throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(message));
      }
    }

    @Override
    public String toString() {
      return "StagingSessionToken{"
          + "sessionId='"
          + sessionId
          + "', "
          + "basePath='"
          + basePath
          + "'"
          + "}";
    }
  }
}
