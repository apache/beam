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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest.Location;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceImplBase;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.util.JsonFormat;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v20_0.com.google.common.hash.Hasher;
import org.apache.beam.vendor.guava.v20_0.com.google.common.hash.Hashing;
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
public class BeamFileSystemArtifactStagingService extends ArtifactStagingServiceImplBase
    implements FnService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BeamFileSystemArtifactStagingService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  // Use UTF8 for all text encoding.
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  public static final String MANIFEST = "MANIFEST";
  public static final String ARTIFACTS = "artifacts";

  @Override
  public StreamObserver<PutArtifactRequest> putArtifact(
      StreamObserver<PutArtifactResponse> responseObserver) {
    return new PutArtifactStreamObserver(responseObserver);
  }

  @Override
  public void commitManifest(
      CommitManifestRequest request, StreamObserver<CommitManifestResponse> responseObserver) {
    try {
      StagingSessionToken stagingSessionToken =
          StagingSessionToken.decode(request.getStagingSessionToken());
      ResourceId manifestResourceId = getManifestFileResourceId(stagingSessionToken);
      ResourceId artifactDirResourceId = getArtifactDirResourceId(stagingSessionToken);
      ProxyManifest.Builder proxyManifestBuilder =
          ProxyManifest.newBuilder().setManifest(request.getManifest());
      for (ArtifactMetadata artifactMetadata : request.getManifest().getArtifactList()) {
        proxyManifestBuilder.addLocation(
            Location.newBuilder()
                .setName(artifactMetadata.getName())
                .setUri(
                    artifactDirResourceId
                        .resolve(
                            encodedFileName(artifactMetadata), StandardResolveOptions.RESOLVE_FILE)
                        .toString())
                .build());
      }
      try (WritableByteChannel manifestWritableByteChannel =
          FileSystems.create(manifestResourceId, MimeTypes.TEXT)) {
        manifestWritableByteChannel.write(
            CHARSET.encode(JsonFormat.printer().print(proxyManifestBuilder.build())));
      }
      // TODO: Validate integrity of staged files.
      responseObserver.onNext(
          CommitManifestResponse.newBuilder()
              .setRetrievalToken(manifestResourceId.toString())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      // TODO: Cleanup all the artifacts.
      LOG.error("Unable to commit manifest.", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void close() throws Exception {
    // Nothing to close here.
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

  private String encodedFileName(ArtifactMetadata artifactMetadata) {
    return "artifact_"
        + Hashing.sha256().hashString(artifactMetadata.getName(), CHARSET).toString();
  }

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

  private class PutArtifactStreamObserver implements StreamObserver<PutArtifactRequest> {

    private final StreamObserver<PutArtifactResponse> outboundObserver;
    private PutArtifactMetadata metadata;
    private ResourceId artifactId;
    private WritableByteChannel artifactWritableByteChannel;
    private Hasher hasher;

    PutArtifactStreamObserver(StreamObserver<PutArtifactResponse> outboundObserver) {
      this.outboundObserver = outboundObserver;
    }

    @Override
    public void onNext(PutArtifactRequest putArtifactRequest) {
      // Create the directory structure for storing artifacts in the first call.
      if (metadata == null) {
        checkNotNull(putArtifactRequest);
        checkNotNull(putArtifactRequest.getMetadata());
        metadata = putArtifactRequest.getMetadata();
        LOG.debug("stored metadata: {}", metadata);
        // Check the base path exists or create the base path
        try {
          ResourceId artifactsDirId =
              getArtifactDirResourceId(
                  StagingSessionToken.decode(
                      putArtifactRequest.getMetadata().getStagingSessionToken()));
          artifactId =
              artifactsDirId.resolve(
                  encodedFileName(metadata.getMetadata()), StandardResolveOptions.RESOLVE_FILE);
          LOG.debug(
              "Going to stage artifact {} to {}.", metadata.getMetadata().getName(), artifactId);
          artifactWritableByteChannel = FileSystems.create(artifactId, MimeTypes.BINARY);
          hasher = Hashing.sha256().newHasher();
        } catch (Exception e) {
          String message =
              String.format(
                  "Failed to begin staging artifact %s", metadata.getMetadata().getName());
          outboundObserver.onError(
              new StatusRuntimeException(Status.DATA_LOSS.withDescription(message).withCause(e)));
        }
      } else {
        try {
          ByteString data = putArtifactRequest.getData().getData();
          artifactWritableByteChannel.write(data.asReadOnlyByteBuffer());
          hasher.putBytes(data.toByteArray());
        } catch (IOException e) {
          String message =
              String.format(
                  "Failed to write chunk of artifact %s to %s",
                  metadata.getMetadata().getName(), artifactId);
          outboundObserver.onError(
              new StatusRuntimeException(Status.DATA_LOSS.withDescription(message).withCause(e)));
        }
      }
    }

    @Override
    public void onError(Throwable throwable) {
      // Delete the artifact.
      LOG.error("Staging artifact failed for " + artifactId, throwable);
      try {
        if (artifactWritableByteChannel != null) {
          artifactWritableByteChannel.close();
        }
        if (artifactId != null) {
          FileSystems.delete(
              Collections.singletonList(artifactId), StandardMoveOptions.IGNORE_MISSING_FILES);
        }

      } catch (IOException e) {
        outboundObserver.onError(
            new StatusRuntimeException(
                Status.DATA_LOSS.withDescription(
                    String.format("Failed to clean up artifact file %s", artifactId))));
        return;
      }
      outboundObserver.onError(
          new StatusRuntimeException(
              Status.DATA_LOSS
                  .withDescription(String.format("Failed to stage artifact %s", artifactId))
                  .withCause(throwable)));
    }

    @Override
    public void onCompleted() {
      // Close the stream.
      LOG.debug("Staging artifact completed for " + artifactId);
      if (artifactWritableByteChannel != null) {
        try {
          artifactWritableByteChannel.close();
        } catch (IOException e) {
          onError(e);
          return;
        }
      }
      String expectedSha256 = metadata.getMetadata().getSha256();
      if (expectedSha256 != null && !expectedSha256.isEmpty()) {
        String actualSha256 = hasher.hash().toString();
        if (!actualSha256.equals(expectedSha256)) {
          outboundObserver.onError(
              new StatusRuntimeException(
                  Status.INVALID_ARGUMENT.withDescription(
                      String.format(
                          "Artifact %s is corrupt: expected sah256 %s, but has sha256 %s",
                          metadata.getMetadata().getName(), expectedSha256, actualSha256))));
          return;
        }
      }
      outboundObserver.onNext(PutArtifactResponse.newBuilder().build());
      outboundObserver.onCompleted();
    }
  }

  /**
   * Serializable StagingSessionToken used to stage files with {@link
   * BeamFileSystemArtifactStagingService}.
   */
  private static class StagingSessionToken implements Serializable {

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
