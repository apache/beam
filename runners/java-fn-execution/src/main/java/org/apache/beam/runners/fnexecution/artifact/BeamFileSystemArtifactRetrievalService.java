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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.util.JsonFormat;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hasher;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ArtifactRetrievalService} that uses {@link FileSystems} as its backing storage and uses
 * the artifact layout and retrieval token format produced by {@link
 * BeamFileSystemArtifactStagingService}.
 */
public class BeamFileSystemArtifactRetrievalService
    extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements ArtifactRetrievalService {
  private static final Logger LOG =
      LoggerFactory.getLogger(BeamFileSystemArtifactRetrievalService.class);

  private static final int ARTIFACT_CHUNK_SIZE_BYTES = 2 << 20; // 2MB

  public static BeamFileSystemArtifactRetrievalService create() {
    return new BeamFileSystemArtifactRetrievalService();
  }

  @Override
  public void getManifest(
      ArtifactApi.GetManifestRequest request,
      StreamObserver<ArtifactApi.GetManifestResponse> responseObserver) {
    final String token = request.getRetrievalToken();
    if (Strings.isNullOrEmpty(token)) {
      throw new StatusRuntimeException(
          Status.INVALID_ARGUMENT.withDescription("Empty artifact token"));
    }

    LOG.info("GetManifest for {}", token);
    try {
      ArtifactApi.ProxyManifest proxyManifest = MANIFEST_CACHE.get(token);
      ArtifactApi.GetManifestResponse response =
          ArtifactApi.GetManifestResponse.newBuilder()
              .setManifest(proxyManifest.getManifest())
              .build();
      LOG.info(
          "GetManifest for {} -> {} artifacts",
          token,
          proxyManifest.getManifest().getArtifactCount());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.info("GetManifest for {} failed", token, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getArtifact(
      ArtifactApi.GetArtifactRequest request,
      StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) {
    LOG.debug("GetArtifact {}", request);
    String name = request.getName();
    try {
      ArtifactApi.ProxyManifest proxyManifest = MANIFEST_CACHE.get(request.getRetrievalToken());
      // look for file at URI specified by proxy manifest location
      ArtifactApi.ProxyManifest.Location location =
          proxyManifest.getLocationList().stream()
              .filter(loc -> loc.getName().equals(name))
              .findFirst()
              .orElseThrow(
                  () ->
                      new StatusRuntimeException(
                          Status.NOT_FOUND.withDescription(
                              String.format("Artifact location not found in manifest: %s", name))));

      List<ArtifactMetadata> existingArtifacts = proxyManifest.getManifest().getArtifactList();
      ArtifactMetadata metadata =
          existingArtifacts.stream()
              .filter(meta -> meta.getName().equals(name))
              .findFirst()
              .orElseThrow(
                  () ->
                      new StatusRuntimeException(
                          Status.NOT_FOUND.withDescription(
                              String.format("Artifact metadata not found in manifest: %s", name))));

      ResourceId artifactResourceId =
          FileSystems.matchNewResource(location.getUri(), false /* is directory */);
      LOG.debug("Artifact {} located in {}", name, artifactResourceId);
      Hasher hasher = Hashing.sha256().newHasher();
      byte[] data = new byte[ARTIFACT_CHUNK_SIZE_BYTES];
      try (InputStream stream = Channels.newInputStream(FileSystems.open(artifactResourceId))) {
        int len;
        while ((len = stream.read(data)) != -1) {
          hasher.putBytes(data, 0, len);
          responseObserver.onNext(
              ArtifactApi.ArtifactChunk.newBuilder()
                  .setData(ByteString.copyFrom(data, 0, len))
                  .build());
        }
      }
      if (metadata.getSha256() != null && !metadata.getSha256().isEmpty()) {
        String expected = metadata.getSha256();
        String actual = hasher.hash().toString();
        if (!actual.equals(expected)) {
          throw new StatusRuntimeException(
              Status.DATA_LOSS.withDescription(
                  String.format(
                      "Artifact %s is corrupt: expected sha256 %s, actual %s",
                      name, expected, actual)));
        }
      }
      responseObserver.onCompleted();
    } catch (IOException | ExecutionException e) {
      LOG.info("GetArtifact {} failed", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void close() throws Exception {}

  private static final LoadingCache<String, ArtifactApi.ProxyManifest> MANIFEST_CACHE =
      CacheBuilder.newBuilder()
          .expireAfterAccess(1, TimeUnit.HOURS /* arbitrary */)
          .maximumSize(100 /* arbitrary */)
          .build(
              new CacheLoader<String, ProxyManifest>() {
                @Override
                public ProxyManifest load(String retrievalToken) throws Exception {
                  return loadManifest(retrievalToken);
                }
              });

  @VisibleForTesting
  static ProxyManifest loadManifest(String retrievalToken) throws IOException {
    LOG.info("Loading manifest for retrieval token {}", retrievalToken);
    // look for manifest file at $retrieval_token
    ResourceId manifestResourceId = getManifestLocationFromToken(retrievalToken);
    return loadManifest(manifestResourceId);
  }

  static ProxyManifest loadManifest(ResourceId manifestResourceId) throws IOException {
    ProxyManifest.Builder manifestBuilder = ProxyManifest.newBuilder();
    try (InputStream stream = Channels.newInputStream(FileSystems.open(manifestResourceId))) {
      String contents = new String(ByteStreams.toByteArray(stream), StandardCharsets.UTF_8);
      JsonFormat.parser().merge(contents, manifestBuilder);
    }
    ProxyManifest proxyManifest = manifestBuilder.build();
    checkArgument(
        proxyManifest.hasManifest(),
        String.format("Invalid ProxyManifest at %s: doesn't have a Manifest", manifestResourceId));
    checkArgument(
        proxyManifest.getLocationCount() == proxyManifest.getManifest().getArtifactCount(),
        String.format(
            "Invalid ProxyManifestat %s: %d locations but %d artifacts",
            manifestResourceId,
            proxyManifest.getLocationCount(),
            proxyManifest.getManifest().getArtifactCount()));
    LOG.info(
        "Manifest at {} has {} artifact locations",
        manifestResourceId,
        proxyManifest.getManifest().getArtifactCount());
    return proxyManifest;
  }

  private static ResourceId getManifestLocationFromToken(String retrievalToken) {
    return FileSystems.matchNewResource(retrievalToken, false /* is directory */);
  }
}
