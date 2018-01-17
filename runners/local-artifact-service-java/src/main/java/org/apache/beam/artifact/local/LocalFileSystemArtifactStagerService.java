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

package org.apache.beam.artifact.local;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.runners.fnexecution.FnService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An {@code ArtifactStagingService} which stages files to a local temp directory. */
public class LocalFileSystemArtifactStagerService
    extends ArtifactStagingServiceGrpc.ArtifactStagingServiceImplBase implements FnService {
  private static final Logger LOG =
      LoggerFactory.getLogger(LocalFileSystemArtifactStagerService.class);

  public static LocalFileSystemArtifactStagerService withRootDirectory(File base) {
    return new LocalFileSystemArtifactStagerService(base);
  }

  private final LocalArtifactStagingLocation location;

  private LocalFileSystemArtifactStagerService(File stagingBase) {
    this.location = LocalArtifactStagingLocation.createAt(stagingBase);
  }

  @Override
  public StreamObserver<ArtifactApi.PutArtifactRequest> putArtifact(
      final StreamObserver<ArtifactApi.PutArtifactResponse> responseObserver) {
    return new CreateAndWriteFileObserver(responseObserver);
  }

  @Override
  public void commitManifest(
      ArtifactApi.CommitManifestRequest request,
      StreamObserver<ArtifactApi.CommitManifestResponse> responseObserver) {
    try {
      commitManifestOrThrow(request, responseObserver);
    } catch (StatusRuntimeException e) {
      responseObserver.onError(e);
      LOG.error("Failed to commit Manifest {}", request.getManifest(), e);
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withCause(e)
              .withDescription(Throwables.getStackTraceAsString(e))
              .asRuntimeException());
      LOG.error("Failed to commit Manifest {}", request.getManifest(), e);
    }
  }

  private void commitManifestOrThrow(
      ArtifactApi.CommitManifestRequest request,
      StreamObserver<ArtifactApi.CommitManifestResponse> responseObserver)
      throws IOException {
    Collection<ArtifactApi.ArtifactMetadata> missing = new ArrayList<>();
    for (ArtifactApi.ArtifactMetadata artifact : request.getManifest().getArtifactList()) {
      // TODO: Validate the checksums on the server side, to fail more aggressively if require
      if (!location.getArtifactFile(artifact.getName()).exists()) {
        missing.add(artifact);
      }
    }
    if (!missing.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Attempted to commit manifest with missing Artifacts: [%s]", missing))
          .asRuntimeException();
    }
    File mf = location.getManifestFile();
    checkState(mf.createNewFile(), "Could not create file to store manifest");
    try (OutputStream mfOut = new FileOutputStream(mf)) {
      request.getManifest().writeTo(mfOut);
    }
    responseObserver.onNext(
        ArtifactApi.CommitManifestResponse.newBuilder()
            .setStagingToken(location.getRootPath())
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void close() throws Exception {
    // TODO: Close all active staging calls, signalling errors to the caller.
  }

  @VisibleForTesting
  LocalArtifactStagingLocation getLocation() {
    return location;
  }

  private class CreateAndWriteFileObserver
      implements StreamObserver<ArtifactApi.PutArtifactRequest> {
    private final StreamObserver<ArtifactApi.PutArtifactResponse> responseObserver;
    private FileWritingObserver writer;

    private CreateAndWriteFileObserver(
        StreamObserver<ArtifactApi.PutArtifactResponse> responseObserver) {
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(ArtifactApi.PutArtifactRequest value) {
      try {
        if (writer == null) {
          if (!value.getContentCase().equals(ArtifactApi.PutArtifactRequest.ContentCase.METADATA)) {
            throw Status.INVALID_ARGUMENT
                .withDescription(
                    String.format(
                        "Expected the first %s to contain the Artifact Name, got %s",
                        ArtifactApi.PutArtifactRequest.class.getSimpleName(),
                        value.getContentCase()))
                .asRuntimeException();
          }
          writer = createFile(value.getMetadata());
        } else {
          writer.onNext(value);
        }
      } catch (StatusRuntimeException e) {
        responseObserver.onError(e);
      } catch (Exception e) {
        responseObserver.onError(
            Status.INTERNAL
                .withCause(e)
                .withDescription(Throwables.getStackTraceAsString(e))
                .asRuntimeException());
      }
    }

    private FileWritingObserver createFile(ArtifactApi.ArtifactMetadata metadata)
        throws IOException {
      File destination = location.getArtifactFile(metadata.getName());
      if (!destination.createNewFile()) {
        throw Status.ALREADY_EXISTS
            .withDescription(String.format("Artifact with name %s already exists", metadata))
            .asRuntimeException();
      }
      return new FileWritingObserver(
          destination, new FileOutputStream(destination), responseObserver);
    }

    @Override
    public void onError(Throwable t) {
      if (writer != null) {
        writer.onError(t);
      } else {
        responseObserver.onCompleted();
      }
    }

    @Override
    public void onCompleted() {
      if (writer != null) {
        writer.onCompleted();
      } else {
        responseObserver.onCompleted();
      }
    }
  }

  private static class FileWritingObserver
      implements StreamObserver<ArtifactApi.PutArtifactRequest> {
    private final File destination;
    private final OutputStream target;
    private final StreamObserver<ArtifactApi.PutArtifactResponse> responseObserver;

    private FileWritingObserver(
        File destination,
        OutputStream target,
        StreamObserver<ArtifactApi.PutArtifactResponse> responseObserver) {
      this.destination = destination;
      this.target = target;
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(ArtifactApi.PutArtifactRequest value) {
      try {
        if (value.getData() == null) {
          StatusRuntimeException e = Status.INVALID_ARGUMENT.withDescription(String.format(
              "Expected all chunks in the current stream state to contain data, got %s",
              value.getContentCase())).asRuntimeException();
          throw e;
        }
        value.getData().getData().writeTo(target);
      } catch (Exception e) {
        cleanedUp(e);
      }
    }

    @Override
    public void onError(Throwable t) {
      if (cleanedUp(null)) {
        responseObserver.onCompleted();
      }
    }

    @Override
    public void onCompleted() {
      try {
        target.close();
      } catch (IOException e) {
        LOG.error("Failed to complete writing file {}", destination, e);
        cleanedUp(e);
        return;
      }
      responseObserver.onNext(ArtifactApi.PutArtifactResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    /**
     * Cleans up after the file writing failed exceptionally, due to an error either in the service
     * or sent from the client.
     *
     * @return false if an error was reported, true otherwise
     */
    private boolean cleanedUp(@Nullable Throwable whyFailed) {
      Throwable actual = whyFailed;
      try {
        target.close();
        if (!destination.delete()) {
          LOG.debug("Couldn't delete failed write at {}", destination);
        }
      } catch (IOException e) {
        if (whyFailed == null) {
          actual = e;
        } else {
          actual.addSuppressed(e);
        }
        LOG.error("Failed to clean up after writing file {}", destination, e);
      }
      if (actual != null) {
        if (actual instanceof StatusException || actual instanceof StatusRuntimeException) {
          responseObserver.onError(actual);
        } else {
          Status status =
              Status.INTERNAL
                  .withCause(actual)
                  .withDescription(Throwables.getStackTraceAsString(actual));
          responseObserver.onError(status.asException());
        }
      }
      return actual == null;
    }
  }
}
