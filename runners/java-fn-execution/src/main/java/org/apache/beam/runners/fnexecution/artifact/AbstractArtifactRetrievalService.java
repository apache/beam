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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.util.JsonFormat;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hasher;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ArtifactRetrievalService} that handles everything aside from actually opening the
 * backing resources.
 */
public abstract class AbstractArtifactRetrievalService
    extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements ArtifactRetrievalService {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractArtifactRetrievalService.class);

  private static final int ARTIFACT_CHUNK_SIZE_BYTES = 2 << 20; // 2MB

  public AbstractArtifactRetrievalService() {
    this(
        CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS /* arbitrary */)
            .maximumSize(100 /* arbitrary */)
            .build());
  }

  public AbstractArtifactRetrievalService(Cache<String, ArtifactApi.ProxyManifest> manifestCache) {
    this.manifestCache = manifestCache;
  }

  public abstract InputStream openManifest(String retrievalToken) throws IOException;

  public abstract InputStream openUri(String retrievalToken, String uri) throws IOException;

  private final Cache<String, ArtifactApi.ProxyManifest> manifestCache;

  public ArtifactApi.ProxyManifest getManifestProxy(String retrievalToken)
      throws IOException, ExecutionException {
    return manifestCache.get(
        retrievalToken,
        () -> {
          try (InputStream stream = openManifest(retrievalToken)) {
            return loadManifest(stream, retrievalToken);
          }
        });
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
      final ArtifactApi.Manifest manifest;
      if (AbstractArtifactStagingService.NO_ARTIFACTS_STAGED_TOKEN.equals(token)) {
        manifest = ArtifactApi.Manifest.newBuilder().build();
      } else {
        ArtifactApi.ProxyManifest proxyManifest = getManifestProxy(token);
        LOG.info(
            "GetManifest for {} -> {} artifacts",
            token,
            proxyManifest.getManifest().getArtifactCount());
        manifest = proxyManifest.getManifest();
      }
      ArtifactApi.GetManifestResponse response =
          ArtifactApi.GetManifestResponse.newBuilder().setManifest(manifest).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.warn("GetManifest for {} failed.", token, e);
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
      ArtifactApi.ProxyManifest proxyManifest = getManifestProxy(request.getRetrievalToken());
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

      Hasher hasher = Hashing.sha256().newHasher();
      byte[] data = new byte[ARTIFACT_CHUNK_SIZE_BYTES];
      try (InputStream stream = openUri(request.getRetrievalToken(), location.getUri())) {
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

  static ProxyManifest loadManifest(InputStream stream, String manifestName) throws IOException {
    ProxyManifest.Builder manifestBuilder = ProxyManifest.newBuilder();
    String contents = new String(ByteStreams.toByteArray(stream), StandardCharsets.UTF_8);
    JsonFormat.parser().merge(contents, manifestBuilder);
    ProxyManifest proxyManifest = manifestBuilder.build();
    checkArgument(
        proxyManifest.hasManifest(),
        String.format("Invalid ProxyManifest at %s: doesn't have a Manifest", manifestName));
    checkArgument(
        proxyManifest.getLocationCount() == proxyManifest.getManifest().getArtifactCount(),
        String.format(
            "Invalid ProxyManifestat %s: %d locations but %d artifacts",
            manifestName,
            proxyManifest.getLocationCount(),
            proxyManifest.getManifest().getArtifactCount()));
    LOG.info(
        "Manifest at {} has {} artifact locations",
        manifestName,
        proxyManifest.getManifest().getArtifactCount());
    return proxyManifest;
  }
}
