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
package org.apache.beam.sdk.io;

import static com.google.common.base.Preconditions.checkState;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.lang3.SystemUtils;

/**
 * {@link ResourceId} implementation for local files.
 */
class LocalResourceId implements ResourceId {

  private final Path path;
  private final boolean isDirectory;

  static LocalResourceId fromPath(Path path, boolean isDirectory) {
    return new LocalResourceId(path, isDirectory);
  }

  private LocalResourceId(Path path, boolean isDirectory) {
    this.path = path.normalize();
    this.isDirectory = isDirectory;
  }

  @Override
  public ResourceId resolve(String other) {
    checkState(
        isDirectory,
        String.format("Expected the path is a directory, but had [%s].", path));
    if (SystemUtils.IS_OS_WINDOWS) {
      return resolveLocalPathWindowsOS(other);
    } else {
      return resolveLocalPath(other);
    }
  }

  @Override
  public ResourceId getCurrentDirectory() {
    if (isDirectory) {
      return this;
    } else {
      return fromPath(path.getParent(), true /* isDirectory */);
    }
  }

  private LocalResourceId resolveLocalPath(String other) {
    return new LocalResourceId(
        path.resolve(other),
        other.endsWith("/"));
  }

  private LocalResourceId resolveLocalPathWindowsOS(String other) {
    checkState(
        path.endsWith("\\"),
        String.format("Expected the path is a directory, but had [%s].", path));
    String uuid = UUID.randomUUID().toString();
    Path pathAsterisksReplaced = Paths.get(path.toString().replaceAll("\\*", uuid));
    String otherAsterisksReplaced = other.replaceAll("\\*", uuid);

    return new LocalResourceId(
        Paths.get(
            pathAsterisksReplaced.resolve(otherAsterisksReplaced)
                .toString()
                .replaceAll(uuid, "\\*")),
        other.endsWith("\\"));
  }

  @Override
  public String getScheme() {
    return LocalFileSystemRegistrar.LOCAL_FILE_SCHEME;
  }

  @Override
  public String toString() {
    return String.format("LocalResourceId: [%s]", path);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof LocalResourceId)) {
      return false;
    }
    LocalResourceId other = (LocalResourceId) obj;
    return this.path.equals(other.path)
        && this.isDirectory == other.isDirectory;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, isDirectory);
  }
}
