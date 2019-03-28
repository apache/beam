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
package org.apache.beam.runners.direct.portable.artifact;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.stub.StreamObserver;

/** An {@code ArtifactRetrievalService} which stages files to a local temp directory. */
public class LocalFileSystemArtifactRetrievalService
    extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements ArtifactRetrievalService {
  private static final int DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024;

  public static LocalFileSystemArtifactRetrievalService forRootDirectory(File base) {
    return new LocalFileSystemArtifactRetrievalService(base);
  }

  private final LocalArtifactStagingLocation location;
  private final Manifest manifest;

  private LocalFileSystemArtifactRetrievalService(File rootDirectory) {
    this.location = LocalArtifactStagingLocation.forExistingDirectory(rootDirectory);
    try (FileInputStream manifestStream = new FileInputStream(location.getManifestFile())) {
      this.manifest = ArtifactApi.Manifest.parseFrom(manifestStream);
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "No %s in root directory %s", Manifest.class.getSimpleName(), rootDirectory),
          e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final void getManifest(
      ArtifactApi.GetManifestRequest request,
      StreamObserver<GetManifestResponse> responseObserver) {
    try {
      responseObserver.onNext(GetManifestResponse.newBuilder().setManifest(manifest).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  /** Get the artifact with the provided name as a sequence of bytes. */
  private ByteBuffer getArtifact(String name) throws IOException {
    File artifact = location.getArtifactFile(name);
    if (!artifact.exists()) {
      throw new FileNotFoundException(String.format("No such artifact %s", name));
    }
    FileChannel input = new FileInputStream(artifact).getChannel();
    return input.map(MapMode.READ_ONLY, 0L, input.size());
  }

  @Override
  public void getArtifact(
      ArtifactApi.GetArtifactRequest request,
      StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) {
    try {
      ByteBuffer artifact = getArtifact(request.getName());
      do {
        responseObserver.onNext(
            ArtifactChunk.newBuilder()
                .setData(
                    ByteString.copyFrom(
                        artifact, Math.min(artifact.remaining(), DEFAULT_CHUNK_SIZE)))
                .build());
      } while (artifact.hasRemaining());
      responseObserver.onCompleted();
    } catch (FileNotFoundException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(String.format("No such artifact %s", request.getName()))
              .withCause(e)
              .asException());
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  String.format("Could not retrieve artifact with name %s", request.getName()))
              .withCause(e)
              .asException());
    }
  }

  @Override
  public void close() throws Exception {}
}
