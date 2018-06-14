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

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.sdk.io.FileSystems;

/**
 * An ArtifactSource suitable for retrieving artifacts uploaded via
 * {@link BeamFileSystemArtifactStagingService}.
 */
public class BeamFileSystemArtifactSource implements ArtifactSource {

  private static final int CHUNK_SIZE = 2 * 1024 * 1024;

  private final String retrievalToken;
  private ArtifactApi.ProxyManifest proxyManifest;

  public BeamFileSystemArtifactSource(String retrievalToken) {
    this.retrievalToken = retrievalToken;
  }

  public static BeamFileSystemArtifactSource create(String artifactToken) {
    return new BeamFileSystemArtifactSource(artifactToken);
  }

  @Override
  public ArtifactApi.Manifest getManifest() throws IOException {
    return getProxyManifest().getManifest();
  }

  @Override
  public void getArtifact(String name,
      StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) throws IOException {
    ReadableByteChannel artifact = FileSystems
        .open(FileSystems.matchNewResource(lookupUri(name), false));
    ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE);
    while (artifact.read(buffer) > -1) {
      buffer.flip();
      responseObserver.onNext(
          ArtifactApi.ArtifactChunk.newBuilder().setData(ByteString.copyFrom(buffer)).build());
      buffer.clear();
    }
  }

  private String lookupUri(String name) throws IOException {
    for (ArtifactApi.ProxyManifest.Location location : getProxyManifest().getLocationList()) {
      if (location.getName().equals(name)) {
        return location.getUri();
      }
    }
    throw new IllegalArgumentException("No such artifact: " + name);
  }

  private ArtifactApi.ProxyManifest getProxyManifest() throws IOException {
    if (proxyManifest == null) {
      ArtifactApi.ProxyManifest.Builder builder = ArtifactApi.ProxyManifest.newBuilder();
      JsonFormat.parser().merge(Channels.newReader(
          FileSystems.open(FileSystems.matchNewResource(retrievalToken, false /* is directory */)),
          StandardCharsets.UTF_8.name()), builder);
      proxyManifest = builder.build();
    }
    return proxyManifest;
  }
}
