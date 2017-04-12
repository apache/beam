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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.lang3.SystemUtils;

/**
 * {@link ResourceId} implementation for local files.
 */
class LocalResourceId implements ResourceId {

  private final Path path;
  private final boolean isDirectory;

  static LocalResourceId fromPath(Path path, boolean isDirectory) {
    checkNotNull(path, "path");
    return new LocalResourceId(path, isDirectory);
  }

  private LocalResourceId(Path path, boolean isDirectory) {
    this.path = path.normalize();
    this.isDirectory = isDirectory;
  }

  @Override
  public LocalResourceId resolve(String other, ResolveOptions resolveOptions) {
    checkState(
        isDirectory,
        String.format("Expected the path is a directory, but had [%s].", path));
    checkArgument(
        resolveOptions.equals(StandardResolveOptions.RESOLVE_FILE)
            || resolveOptions.equals(StandardResolveOptions.RESOLVE_DIRECTORY),
        String.format("ResolveOptions: [%s] is not supported.", resolveOptions));
    checkArgument(
        !(resolveOptions.equals(StandardResolveOptions.RESOLVE_FILE)
            && other.endsWith("/")),
        "The resolved file: [%s] should not end with '/'.", other);
    if (SystemUtils.IS_OS_WINDOWS) {
      return resolveLocalPathWindowsOS(other, resolveOptions);
    } else {
      return resolveLocalPath(other, resolveOptions);
    }
  }

  @Override
  public LocalResourceId getCurrentDirectory() {
    if (isDirectory) {
      return this;
    } else {
      Path parent = path.getParent();
      if (parent == null && path.getNameCount() == 1) {
        parent = Paths.get(".");
      }
      checkState(
          parent != null,
          String.format("Failed to get the current directory for path: [%s].", path));
      return fromPath(
          parent,
          true /* isDirectory */);
    }
  }

  private LocalResourceId resolveLocalPath(String other, ResolveOptions resolveOptions) {
    return new LocalResourceId(
        path.resolve(other),
        resolveOptions.equals(StandardResolveOptions.RESOLVE_DIRECTORY));
  }

  private LocalResourceId resolveLocalPathWindowsOS(String other, ResolveOptions resolveOptions) {
    String uuid = UUID.randomUUID().toString();
    Path pathAsterisksReplaced = Paths.get(path.toString().replaceAll("\\*", uuid));
    String otherAsterisksReplaced = other.replaceAll("\\*", uuid);

    return new LocalResourceId(
        Paths.get(
            pathAsterisksReplaced.resolve(otherAsterisksReplaced)
                .toString()
                .replaceAll(uuid, "\\*")),
        resolveOptions.equals(StandardResolveOptions.RESOLVE_DIRECTORY));
  }

  @Override
  public String getScheme() {
    return LocalFileSystemRegistrar.LOCAL_FILE_SCHEME;
  }

  Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return path.toString();
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
