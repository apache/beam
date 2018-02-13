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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;

/**
 * A location where the results of an {@link LocalFileSystemArtifactStagerService} are stored and
 * where the retrieval service retrieves them from.
 */
public class LocalArtifactStagingLocation {
  /**
   * Create a new {@link LocalArtifactStagingLocation} rooted at the specified location, creating
   * any directories or subdirectories as necessary.
   */
  public static LocalArtifactStagingLocation createAt(File rootDirectory) {
    return new LocalArtifactStagingLocation(rootDirectory).createDirectories();
  }

  /**
   * Create a {@link LocalArtifactStagingLocation} for an existing directory. The directory must
   * contain a manifest and an artifact directory.
   */
  public static LocalArtifactStagingLocation forExistingDirectory(File rootDirectory) {
    return new LocalArtifactStagingLocation(rootDirectory).verifyExistence();
  }

  private final File rootDirectory;
  private final File artifactsDirectory;

  private LocalArtifactStagingLocation(File base) {
    this.rootDirectory = base;
    this.artifactsDirectory = new File(base, "artifacts");
  }

  private LocalArtifactStagingLocation createDirectories() {
    if (((rootDirectory.exists() && rootDirectory.isDirectory()) || rootDirectory.mkdirs())
        && rootDirectory.canWrite()) {
      checkState(
          ((artifactsDirectory.exists() && artifactsDirectory.isDirectory())
                  || artifactsDirectory.mkdir())
              && artifactsDirectory.canWrite(),
          "Could not create artifact staging directory at %s",
          artifactsDirectory);
    } else {
      throw new IllegalStateException(
          String.format("Could not create staging directory structure at root %s", rootDirectory));
    }
    return this;
  }

  private LocalArtifactStagingLocation verifyExistence() {
    checkArgument(rootDirectory.exists(), "Nonexistent staging location root %s", rootDirectory);
    checkArgument(
        rootDirectory.isDirectory(), "Staging location %s is not a directory", rootDirectory);
    checkArgument(
        artifactsDirectory.exists(), "Nonexistent artifact directory %s", artifactsDirectory);
    checkArgument(
        artifactsDirectory.isDirectory(),
        "Artifact location %s is not a directory",
        artifactsDirectory);
    checkArgument(getManifestFile().exists(), "No Manifest in existing location %s", rootDirectory);
    return this;
  }

  /**
   * Returns the {@link File} which contains the artifact with the provided name.
   *
   * <p>The file may not exist.
   */
  public File getArtifactFile(String artifactName) {
    return new File(artifactsDirectory, artifactName);
  }

  /**
   * Returns the {@link File} which contains the {@link Manifest}.
   *
   * <p>The file may not exist.
   */
  public File getManifestFile() {
    return new File(rootDirectory, "MANIFEST");
  }

  /**
   * Returns the local location of this {@link LocalArtifactStagingLocation}.
   *
   * <p>This can be used to refer to the staging location when creating a retrieval service.
   */
  public String getRootPath() {
    try {
      return rootDirectory.getCanonicalPath();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
