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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest.ContentCase;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceImplBase;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;

/**
 * An {@link ArtifactStagingServiceImplBase ArtifactStagingService} which stores the bytes of the
 * artifacts in memory..
 */
public class InMemoryArtifactStagerService extends ArtifactStagingServiceImplBase {
  private final ConcurrentMap<ArtifactMetadata, byte[]> artifactBytes;
  private final AtomicReference<Manifest> manifest = new AtomicReference<>();

  public InMemoryArtifactStagerService() {
    artifactBytes = new ConcurrentHashMap<>();
  }

  @Override
  public StreamObserver<ArtifactApi.PutArtifactRequest> putArtifact(
      StreamObserver<ArtifactApi.PutArtifactResponse> responseObserver) {
    return new BufferingObserver(responseObserver);
  }

  @Override
  public void commitManifest(
      ArtifactApi.CommitManifestRequest request,
      StreamObserver<ArtifactApi.CommitManifestResponse> responseObserver) {
    checkState(
        this.manifest.compareAndSet(null, request.getManifest()),
        "Already committed a %s %s",
        Manifest.class.getSimpleName(),
        manifest.get());
    responseObserver.onNext(CommitManifestResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

  public Map<ArtifactMetadata, byte[]> getStagedArtifacts() {
    return Collections.unmodifiableMap(artifactBytes);
  }

  public Manifest getManifest() {
    return manifest.get();
  }

  private class BufferingObserver implements StreamObserver<PutArtifactRequest> {
    private final StreamObserver<PutArtifactResponse> responseObserver;
    private ArtifactMetadata destination = null;
    private BufferWritingObserver writer = null;

    public BufferingObserver(StreamObserver<PutArtifactResponse> responseObserver) {
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(PutArtifactRequest value) {
      if (writer == null) {
        checkArgument(value.getContentCase().equals(ContentCase.METADATA));
        writer = new BufferWritingObserver();
        destination = value.getMetadata().getMetadata();
      } else {
        writer.onNext(value);
      }
    }

    @Override
    public void onError(Throwable t) {
      if (writer != null) {
        writer.onError(t);
      }
      onCompleted();
    }

    @Override
    public void onCompleted() {
      if (writer != null) {
        writer.onCompleted();
        artifactBytes.put(
            destination
                .toBuilder()
                .setSha256(
                    Hashing.sha256()
                        .newHasher()
                        .putBytes(writer.stream.toByteArray())
                        .hash()
                        .toString())
                .build(),
            writer.stream.toByteArray());
      }
      responseObserver.onNext(PutArtifactResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static class BufferWritingObserver implements StreamObserver<PutArtifactRequest> {
    private final ByteArrayOutputStream stream;

    BufferWritingObserver() {
      stream = new ByteArrayOutputStream();
    }

    @Override
    public void onNext(PutArtifactRequest value) {
      try {
        stream.write(value.getData().getData().toByteArray());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onError(Throwable t) {
      onCompleted();
    }

    @Override
    public void onCompleted() {}
  }
}
