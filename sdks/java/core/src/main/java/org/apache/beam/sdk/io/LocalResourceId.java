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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.lang3.SystemUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/** {@link ResourceId} implementation for local files. */
class LocalResourceId implements ResourceId {

  private final String pathString;

  private transient @Nullable volatile Path cachedPath;

  private final boolean isDirectory;

  static LocalResourceId fromPath(Path path, boolean isDirectory) {
    checkNotNull(path, "path");
    return new LocalResourceId(path, isDirectory);
  }

  private LocalResourceId(Path path, boolean isDirectory) {
    this.pathString =
        path.toAbsolutePath().normalize().toString() + (isDirectory ? File.separatorChar : "");
    this.isDirectory = isDirectory;
  }

  @Override
  public LocalResourceId resolve(String other, ResolveOptions resolveOptions) {
    checkState(isDirectory, "Expected the path is a directory, but had [%s].", pathString);
    checkArgument(
        resolveOptions.equals(StandardResolveOptions.RESOLVE_FILE)
            || resolveOptions.equals(StandardResolveOptions.RESOLVE_DIRECTORY),
        "ResolveOptions: [%s] is not supported.",
        resolveOptions);
    checkArgument(
        !(resolveOptions.equals(StandardResolveOptions.RESOLVE_FILE) && other.endsWith("/")),
        "The resolved file: [%s] should not end with '/'.",
        other);
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
      Path path = getPath();
      Path parent = path.getParent();
      if (parent == null && path.getNameCount() == 1) {
        parent = Paths.get(".");
      }
      checkState(parent != null, "Failed to get the current directory for path: [%s].", pathString);
      return fromPath(parent, true /* isDirectory */);
    }
  }

  @Override
  public @Nullable String getFilename() {
    Path fileName = getPath().getFileName();
    return fileName == null ? null : fileName.toString();
  }

  private LocalResourceId resolveLocalPath(String other, ResolveOptions resolveOptions) {
    return new LocalResourceId(
        getPath().resolve(other), resolveOptions.equals(StandardResolveOptions.RESOLVE_DIRECTORY));
  }

  private LocalResourceId resolveLocalPathWindowsOS(String other, ResolveOptions resolveOptions) {
    String uuid = UUID.randomUUID().toString();
    Path pathAsterisksReplaced = Paths.get(pathString.replaceAll("\\*", uuid));
    String otherAsterisksReplaced = other.replaceAll("\\*", uuid);

    return new LocalResourceId(
        Paths.get(
            pathAsterisksReplaced
                .resolve(otherAsterisksReplaced)
                .toString()
                .replaceAll(uuid, "\\*")),
        resolveOptions.equals(StandardResolveOptions.RESOLVE_DIRECTORY));
  }

  @Override
  public String getScheme() {
    return "file";
  }

  @Override
  public boolean isDirectory() {
    return isDirectory;
  }

  Path getPath() {
    if (cachedPath == null) {
      cachedPath = Paths.get(pathString);
    }
    return cachedPath;
  }

  @Override
  public String toString() {
    return pathString;
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (!(obj instanceof LocalResourceId)) {
      return false;
    }
    LocalResourceId other = (LocalResourceId) obj;
    return this.pathString.equals(other.pathString) && this.isDirectory == other.isDirectory;
  }

  @Override
  public int hashCode() {
    return Objects.hash(pathString, isDirectory);
  }
}
