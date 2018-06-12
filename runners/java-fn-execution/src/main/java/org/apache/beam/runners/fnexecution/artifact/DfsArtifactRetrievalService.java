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


import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;

/**
 * An {@link ArtifactRetrievalService} that uses distributed file systems as its backing storage.
 */
public class DfsArtifactRetrievalService
    extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements ArtifactRetrievalService {

  private static int artifactChunkSizeBytes = 2000000; // 2MB

  public static DfsArtifactRetrievalService create() {
    return new DfsArtifactRetrievalService();
  }

  @Override
  public void getManifest(
      ArtifactApi.GetManifestRequest request,
      StreamObserver<ArtifactApi.GetManifestResponse> responseObserver) {
    try {
      ArtifactApi.ProxyManifest proxyManifest = loadManifestFrom(request.getRetrievalToken());
      ArtifactApi.GetManifestResponse response =
          ArtifactApi.GetManifestResponse
              .newBuilder()
              .setManifest(proxyManifest.getManifest())
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getArtifact(
      ArtifactApi.GetArtifactRequest request,
      StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) {
    try {
      ArtifactApi.ProxyManifest proxyManifest = loadManifestFrom(request.getRetrievalToken());
      // validate that name is contained in manifest and location list
      boolean containsArtifactName =
          ImmutableList.copyOf(proxyManifest.getManifest().getArtifactList())
              .stream()
              .anyMatch(metadata -> metadata.getName().equals(request.getName()));
      if (!containsArtifactName)  {
        throw new ArtifactNotFoundException(request.getName());
      }
      // look for file at URI specified by proxy manifest location
      ImmutableList<ArtifactApi.ProxyManifest.Location> locationList =
          ImmutableList.copyOf(proxyManifest.getLocationList());
      ArtifactApi.ProxyManifest.Location location =
          locationList
              .stream()
              .filter(loc -> loc.getName().equals(request.getName()))
              .findFirst()
              .orElseThrow(() -> new ArtifactNotFoundException(request.getName()));
      ResourceId artifactResourceId =
          FileSystems.matchNewResource(location.getUri(), false /* is directory */);
      ReadableByteChannel artifactByteChannel = FileSystems.open(artifactResourceId);
      ByteBuffer byteBuffer = ByteBuffer.allocate(artifactChunkSizeBytes);
      while (artifactByteChannel.read(byteBuffer) > -1) {
        ByteString data = ByteString.copyFrom(byteBuffer);
        byteBuffer.clear();
        ArtifactApi.ArtifactChunk artifactChunk =
            ArtifactApi.ArtifactChunk.newBuilder().setData(data).build();
        responseObserver.onNext(artifactChunk);
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }

  }

  @Override
  public void close() throws Exception {

  }

  private ArtifactApi.ProxyManifest loadManifestFrom(String retrievalToken) throws Exception {
    // look for file at $retrieval_token/$manifestName
    ResourceId manifestResourceId =
        FileSystems.matchNewResource(retrievalToken, false /* is directory */);
    InputStream manifestStream = Channels.newInputStream(FileSystems.open(manifestResourceId));
    ArtifactApi.ProxyManifest proxyManifest =  ArtifactApi.ProxyManifest.parseFrom(manifestStream);
    // ensure that manifest field is set (it always should be).
    // otherwise we'd be returning a null field.
    if (!proxyManifest.hasManifest()) {
      throw new InvalidObjectException(
          "Encountered invalid ProxyManifest: did not have a Manifest set");
    }
    return proxyManifest;
  }

  /**
   * Thrown when an artifact name is requested that is not found in the corresponding manifest.
   */
  public static class ArtifactNotFoundException extends RuntimeException {
    ArtifactNotFoundException(String artifactName) {
      super(String.format("Could not find artifact with name: %s", artifactName));
    }
  }
}
