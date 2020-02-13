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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest.Location;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceImplBase;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.util.JsonFormat;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hasher;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ArtifactStagingServiceImplBase} that handles everything aside from actually opening the
 * backing resources.
 */
public abstract class AbstractArtifactStagingService extends ArtifactStagingServiceImplBase
    implements FnService {

  public static final String NO_ARTIFACTS_STAGED_TOKEN =
      ArtifactApi.CommitManifestResponse.Constants.NO_ARTIFACTS_STAGED_TOKEN
          .getValueDescriptor()
          .getOptions()
          .getExtension(RunnerApi.beamConstant);

  private static final Logger LOG = LoggerFactory.getLogger(AbstractArtifactStagingService.class);

  private static final Charset CHARSET = StandardCharsets.UTF_8;

  public abstract String getArtifactUri(String stagingSessionToken, String encodedFileName)
      throws Exception;

  public abstract WritableByteChannel openUri(String uri) throws IOException;

  public abstract void removeUri(String uri) throws IOException;

  public abstract WritableByteChannel openManifest(String stagingSessionToken) throws Exception;

  public abstract void removeArtifacts(String stagingSessionToken) throws Exception;

  public abstract String getRetrievalToken(String stagingSessionToken) throws Exception;

  @Override
  public StreamObserver<PutArtifactRequest> putArtifact(
      StreamObserver<PutArtifactResponse> responseObserver) {
    return new PutArtifactStreamObserver(responseObserver);
  }

  @Override
  public void commitManifest(
      CommitManifestRequest request, StreamObserver<CommitManifestResponse> responseObserver) {
    try {
      final String retrievalToken;
      if (request.getManifest().getArtifactCount() > 0) {
        String stagingSessionToken = request.getStagingSessionToken();
        ProxyManifest.Builder proxyManifestBuilder =
            ProxyManifest.newBuilder().setManifest(request.getManifest());
        for (ArtifactMetadata artifactMetadata : request.getManifest().getArtifactList()) {
          proxyManifestBuilder.addLocation(
              Location.newBuilder()
                  .setName(artifactMetadata.getName())
                  .setUri(getArtifactUri(stagingSessionToken, encodedFileName(artifactMetadata)))
                  .build());
        }
        try (WritableByteChannel manifestWritableByteChannel = openManifest(stagingSessionToken)) {
          manifestWritableByteChannel.write(
              CHARSET.encode(JsonFormat.printer().print(proxyManifestBuilder.build())));
        }
        retrievalToken = getRetrievalToken(stagingSessionToken);
        // TODO: Validate integrity of staged files.
      } else {
        retrievalToken = NO_ARTIFACTS_STAGED_TOKEN;
      }
      responseObserver.onNext(
          CommitManifestResponse.newBuilder().setRetrievalToken(retrievalToken).build());
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

  private String encodedFileName(ArtifactMetadata artifactMetadata) {
    return "artifact_"
        + Hashing.sha256().hashString(artifactMetadata.getName(), CHARSET).toString();
  }

  private class PutArtifactStreamObserver implements StreamObserver<PutArtifactRequest> {

    private final StreamObserver<PutArtifactResponse> outboundObserver;
    private PutArtifactMetadata metadata;
    private String artifactId;
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
          artifactId =
              getArtifactUri(
                  putArtifactRequest.getMetadata().getStagingSessionToken(),
                  encodedFileName(metadata.getMetadata()));
          LOG.debug(
              "Going to stage artifact {} to {}.", metadata.getMetadata().getName(), artifactId);
          artifactWritableByteChannel = openUri(artifactId);
          hasher = Hashing.sha256().newHasher();
        } catch (Exception e) {
          String message =
              String.format(
                  "Failed to begin staging artifact %s", metadata.getMetadata().getName());
          LOG.error(message, e);
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
          LOG.error(message, e);
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
          removeUri(artifactId);
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
}
