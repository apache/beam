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
import com.google.common.hash.Hashing;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.FileNotFoundException;
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
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest.Builder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ArtifactStagingServiceImplBase} based on beam file system.
 */
public class BeamFileSystemArtifactStagingService extends ArtifactStagingServiceImplBase implements
    FnService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BeamFileSystemArtifactStagingService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  // Use UTF8 for all text encoding.
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  public static final String MANIFEST = "MANIFEST";

  @Override
  public StreamObserver<PutArtifactRequest> putArtifact(
      StreamObserver<PutArtifactResponse> responseObserver) {
    return new PutArtifactStreamObserver(responseObserver);
  }

  @Override
  public void commitManifest(
      CommitManifestRequest request, StreamObserver<CommitManifestResponse> responseObserver) {
    try {
      ResourceId jobResourceDirId = getJobDirResourceId(request.getStagingSessionToken());
      ResourceId manifestResourceId = jobResourceDirId
          .resolve(MANIFEST, StandardResolveOptions.RESOLVE_FILE);
      ResourceId artifactDirResourceId = getArtifactDirResourceId(request.getStagingSessionToken());
      Builder proxyManifestBuilder = ProxyManifest.newBuilder().setManifest(request.getManifest());
      for (ArtifactMetadata artifactMetadata : request.getManifest().getArtifactList()) {
        proxyManifestBuilder.addLocation(Location.newBuilder().setName(artifactMetadata.getName())
            .setUri(artifactDirResourceId
                .resolve(encodedFileName(artifactMetadata), StandardResolveOptions.RESOLVE_FILE)
                .toString()).build());
      }
      try (WritableByteChannel manifestWritableByteChannel = FileSystems
          .create(manifestResourceId, MimeTypes.TEXT)) {
        manifestWritableByteChannel
            .write(CHARSET.encode(JsonFormat.printer().print(proxyManifestBuilder.build())));
      }
      // TODO: Validate integrity of staged files.
      responseObserver.onNext(
          CommitManifestResponse.newBuilder().setStagingToken(manifestResourceId.toString())
              .build());
      responseObserver.onCompleted();
    } catch (IOException e) {
      // TODO: Cleanup all the artifacts.
      LOG.error("Unable to commit manifest.", e);
      responseObserver.onError(e);
    }
  }

  /**
   * Generate a stagingSessionToken compatible with {@link BeamFileSystemArtifactStagingService}.
   *
   * @param sessionId Unique sessionId for artifact staging.
   * @param basePath Base path to upload artifacts.
   * @return Encoded stagingSessionToken.
   */
  public String generateStagingSessionToken(String sessionId, String basePath) throws Exception {
    StagingSessionToken stagingSessionToken = new StagingSessionToken();
    stagingSessionToken.setSessionId(sessionId);
    stagingSessionToken.setBasePath(basePath);
    return encodeStagingSessionToken(stagingSessionToken);
  }

  private String encodedFileName(ArtifactMetadata artifactMetadata) {
    return "artifact_" + Hashing.md5().hashString(artifactMetadata.getName(), CHARSET).toString();
  }

  private StagingSessionToken decodeStagingSessionToken(String stagingSessionToken)
      throws IOException {
    try {
      return MAPPER.readValue(stagingSessionToken, StagingSessionToken.class);
    } catch (IOException e) {
      try {
        LOG.error(
            "Unable to deserialize staging token {}. Expected format {}. Error {}",
            stagingSessionToken, MAPPER.writeValueAsString(new StagingSessionToken()),
            e.getMessage());
      } catch (JsonProcessingException e1) {
        LOG.error("Error {} occurred while serializing {}.", e.getMessage(),
            StagingSessionToken.class);
      }
      throw e;
    }
  }

  private String encodeStagingSessionToken(StagingSessionToken stagingSessionToken)
      throws Exception {
    try {
      return MAPPER.writeValueAsString(stagingSessionToken);
    } catch (JsonProcessingException e) {
      LOG.error("Error {} occurred while serializing {}.", e.getMessage(),
          StagingSessionToken.class);
      throw e;
    }
  }

  private ResourceId getJobDirResourceId(String stagingSessionToken) throws IOException {
    ResourceId baseResourceId;
    StagingSessionToken parsedToken = decodeStagingSessionToken(stagingSessionToken);
    try {
      baseResourceId = FileSystems.matchSingleFileSpec(parsedToken.getBasePath())
          .resourceId();
    } catch (FileNotFoundException fne) {
      // Create the base path
      baseResourceId = FileSystems
          .matchNewResource(parsedToken.getBasePath(), true /* isDirectory */);
    }
    // Using sessionId as the subDir to store artifacts and manifest.
    return baseResourceId
        .resolve(parsedToken.getSessionId(), StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  private ResourceId getArtifactDirResourceId(String stagingSessionToken) throws IOException {
    return getJobDirResourceId(stagingSessionToken)
        .resolve("artifacts", StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  @Override
  public void close() throws Exception {
    // Nothing to close here.
  }

  private class PutArtifactStreamObserver implements StreamObserver<PutArtifactRequest> {

    private final StreamObserver<PutArtifactResponse> outboundObserver;
    private PutArtifactMetadata metadata;
    private ResourceId artifactId;
    private WritableByteChannel artifactWritableByteChannel;

    PutArtifactStreamObserver(StreamObserver<PutArtifactResponse> outboundObserver) {
      this.outboundObserver = outboundObserver;
    }

    @Override
    public void onNext(PutArtifactRequest putArtifactRequest) {
      // Create the directory structure for storing artifacts in the first call.
      if (metadata == null) {
        metadata = putArtifactRequest.getMetadata();
        // Check the base path exists or create the base path
        try {
          ResourceId artifactsDirId = getArtifactDirResourceId(
              putArtifactRequest.getMetadata().getStagingSessionToken());
          LOG.info("Going to stage artifact {} in {}.", metadata.getMetadata().getName(),
              artifactsDirId);
          artifactId = artifactsDirId
              .resolve(encodedFileName(metadata.getMetadata()),
                  StandardResolveOptions.RESOLVE_FILE);
          artifactWritableByteChannel = FileSystems.create(artifactId, MimeTypes.BINARY);
        } catch (IOException e) {
          LOG.error("Staging failed for artifact {} for staging token {}",
              encodedFileName(metadata.getMetadata()), metadata.getStagingSessionToken());
          outboundObserver.onError(e);
        }
      } else {
        try {
          artifactWritableByteChannel
              .write(putArtifactRequest.getData().getData().asReadOnlyByteBuffer());
        } catch (IOException e) {
          LOG.error("Staging failed for artifact {} to file {}.", metadata.getMetadata().getName(),
              artifactId);
          outboundObserver.onError(e);
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
          FileSystems.delete(Collections.singletonList(artifactId),
              StandardMoveOptions.IGNORE_MISSING_FILES);
        }

      } catch (IOException e) {
        LOG.error("Unable to save artifact {}", artifactId);
        outboundObserver.onError(e);
        return;
      }
      outboundObserver.onCompleted();
    }

    @Override
    public void onCompleted() {
      // Close the stream.
      LOG.info("Staging artifact completed for " + artifactId);
      if (artifactWritableByteChannel != null) {
        try {
          artifactWritableByteChannel.close();
        } catch (IOException e) {
          onError(e);
          return;
        }
      }
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

    /**
     * Access is public for json conversion.
     */
    public String getSessionId() {
      return sessionId;
    }

    private void setSessionId(String sessionId) {
      this.sessionId = sessionId;
    }

    /**
     * Access is public for json conversion.
     */
    public String getBasePath() {
      return basePath;
    }

    private void setBasePath(String basePath) {
      this.basePath = basePath;
    }
  }
}
