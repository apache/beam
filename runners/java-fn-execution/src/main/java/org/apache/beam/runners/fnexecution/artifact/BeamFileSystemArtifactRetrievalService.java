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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ArtifactRetrievalService} that uses {@link FileSystems} as its backing storage and uses
 * the artifact layout and retrieval token format produced by {@link
 * BeamFileSystemArtifactStagingService}.
 */
public class BeamFileSystemArtifactRetrievalService extends AbstractArtifactRetrievalService {
  private static final Logger LOG =
      LoggerFactory.getLogger(BeamFileSystemArtifactRetrievalService.class);

  private static final Cache<String, ArtifactApi.ProxyManifest> MANIFEST_CACHE =
      CacheBuilder.newBuilder()
          .expireAfterAccess(1, TimeUnit.HOURS /* arbitrary */)
          .maximumSize(100 /* arbitrary */)
          .build();

  public BeamFileSystemArtifactRetrievalService() {
    super(MANIFEST_CACHE);
  }

  public static BeamFileSystemArtifactRetrievalService create() {
    return new BeamFileSystemArtifactRetrievalService();
  }

  @Override
  public InputStream openUri(String retrievalToken, String uri) throws IOException {
    ResourceId artifactResourceId = FileSystems.matchNewResource(uri, false /* is directory */);
    return Channels.newInputStream(FileSystems.open(artifactResourceId));
  }

  @Override
  public InputStream openManifest(String retrievalToken) throws IOException {
    ResourceId manifestResourceId = getManifestLocationFromToken(retrievalToken);
    try {
      return Channels.newInputStream(FileSystems.open(manifestResourceId));
    } catch (IOException e) {
      LOG.warn(
          "GetManifest for {} failed. Make sure the artifact staging directory (configurable "
              + "via --artifacts-dir argument to the job server) is accessible to workers.",
          retrievalToken,
          e);
      throw e;
    }
  }

  @VisibleForTesting
  static ProxyManifest loadManifest(String retrievalToken) throws IOException {
    LOG.info("Loading manifest for retrieval token {}", retrievalToken);
    // look for manifest file at $retrieval_token
    ResourceId manifestResourceId = getManifestLocationFromToken(retrievalToken);
    return loadManifest(manifestResourceId);
  }

  static ProxyManifest loadManifest(ResourceId manifestResourceId) throws IOException {
    return loadManifest(
        Channels.newInputStream(FileSystems.open(manifestResourceId)),
        manifestResourceId.toString());
  }

  private static ResourceId getManifestLocationFromToken(String retrievalToken) {
    return FileSystems.matchNewResource(retrievalToken, false /* is directory */);
  }
}
