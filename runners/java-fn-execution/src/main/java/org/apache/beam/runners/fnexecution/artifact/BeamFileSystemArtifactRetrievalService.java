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
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InvalidObjectException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: unit tests

/**
 * An {@link ArtifactRetrievalService} that uses distributed file systems as its backing storage.
 */
public class BeamFileSystemArtifactRetrievalService
    extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements ArtifactRetrievalService {

  private static final int ARTIFACT_CHUNK_SIZE_BYTES = 2 * 1000 * 1000; // 2MB
  private static final Logger LOG =
      LoggerFactory.getLogger(BeamFileSystemArtifactRetrievalService.class);

  public static BeamFileSystemArtifactRetrievalService create() {
    return new BeamFileSystemArtifactRetrievalService();
  }

  @Override
  public void getManifest(
      ArtifactApi.GetManifestRequest request,
      StreamObserver<ArtifactApi.GetManifestResponse> responseObserver) {
    // TODO: add logging: retrieval token
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
    // TODO: add logging: retrieval token, name, location
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
      ByteBuffer byteBuffer = ByteBuffer.allocate(ARTIFACT_CHUNK_SIZE_BYTES);
      try (ReadableByteChannel artifactByteChannel = FileSystems.open(artifactResourceId)){
        while (artifactByteChannel.read(byteBuffer) > -1) {
          byteBuffer.flip();
          ByteString data = ByteString.copyFrom(byteBuffer);
          byteBuffer.clear();
          ArtifactApi.ArtifactChunk artifactChunk =
              ArtifactApi.ArtifactChunk.newBuilder().setData(data).build();
          responseObserver.onNext(artifactChunk);
        }
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
    // look for manifest file at $retrieval_token
    ResourceId manifestResourceId =
        FileSystems.matchNewResource(retrievalToken, false /* is directory */);
    ArtifactApi.ProxyManifest proxyManifest;
    try(InputStream manifestStream = Channels.newInputStream(FileSystems.open(manifestResourceId))) {
      BufferedReader manifestReader = new BufferedReader(new InputStreamReader(manifestStream));
      ArtifactApi.ProxyManifest.Builder manifestBuilder = ArtifactApi.ProxyManifest.newBuilder();
      JsonFormat.parser().merge(manifestReader, manifestBuilder);
      proxyManifest = manifestBuilder.build();
    }
    // ensure that manifest field is set (it always should be).
    // otherwise we'd be returning a null field.
    if (!proxyManifest.hasManifest()) {
      throw new InvalidObjectException(
          "Encountered invalid ProxyManifest: did not have a Manifest set");
    }
    return proxyManifest;
  }

  // TODO: implement LoadingCache for proxy manifest
  // TODO: log when loading cache from file

  /**
   * Thrown when an artifact name is requested that is not found in the corresponding manifest.
   */
  public static class ArtifactNotFoundException extends RuntimeException {
    ArtifactNotFoundException(String artifactName) {
      super(String.format("Could not find artifact with name: %s", artifactName));
    }
  }
}
