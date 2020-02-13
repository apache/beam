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

import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;

/**
 * An {@link ArtifactStagingServiceGrpc.ArtifactStagingServiceImplBase} that loads artifacts into a
 * Java {@link FileSystem}.
 */
public class JavaFilesystemArtifactStagingService extends AbstractArtifactStagingService {

  public static final String MANIFEST = "MANIFEST.json";
  public static final String ARTIFACTS = "ARTIFACTS";

  private final FileSystem fileSystem;
  private final Path artifactRootDir;

  public JavaFilesystemArtifactStagingService(FileSystem fileSystem, String artifactRootDir) {
    this.fileSystem = fileSystem;
    this.artifactRootDir = fileSystem.getPath(artifactRootDir);
  }

  @Override
  public String getArtifactUri(String stagingSessionToken, String encodedFileName)
      throws Exception {
    return artifactRootDir
        .resolve(stagingSessionToken)
        .resolve(ARTIFACTS)
        .resolve(encodedFileName)
        .toString();
  }

  @Override
  public WritableByteChannel openUri(String uri) throws IOException {
    Path parent = fileSystem.getPath(uri).getParent();
    if (parent == null) {
      throw new RuntimeException("Provided URI did not have a parent: " + uri);
    }
    Files.createDirectories(parent);
    return Channels.newChannel(Files.newOutputStream(fileSystem.getPath(uri)));
  }

  @Override
  public void removeUri(String uri) throws IOException {
    Files.deleteIfExists(fileSystem.getPath(uri));
  }

  @Override
  public WritableByteChannel openManifest(String stagingSessionToken) throws Exception {
    return openUri(getManifestUri(stagingSessionToken));
  }

  @Override
  public void removeArtifacts(String stagingSessionToken) throws Exception {
    try (Stream<Path> paths = Files.walk(artifactRootDir.resolve(stagingSessionToken))) {
      paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  @Override
  public String getRetrievalToken(String stagingSessionToken) throws Exception {
    return getManifestUri(stagingSessionToken);
  }

  private String getManifestUri(String stagingSessionToken) {
    return artifactRootDir.resolve(stagingSessionToken).resolve(MANIFEST).toString();
  }
}
