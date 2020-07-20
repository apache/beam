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
package org.apache.beam.sdk.extensions.gcp.storage;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.checkerframework.checker.nullness.qual.Nullable;

/** {@link ResourceId} implementation for Google Cloud Storage. */
public class GcsResourceId implements ResourceId {

  private final GcsPath gcsPath;

  static GcsResourceId fromGcsPath(GcsPath gcsPath) {
    checkNotNull(gcsPath, "gcsPath");
    return new GcsResourceId(gcsPath);
  }

  private GcsResourceId(GcsPath gcsPath) {
    this.gcsPath = gcsPath;
  }

  @Override
  public GcsResourceId resolve(String other, ResolveOptions resolveOptions) {
    checkState(
        isDirectory(),
        String.format("Expected the gcsPath is a directory, but had [%s].", gcsPath));
    checkArgument(
        resolveOptions.equals(StandardResolveOptions.RESOLVE_FILE)
            || resolveOptions.equals(StandardResolveOptions.RESOLVE_DIRECTORY),
        String.format("ResolveOptions: [%s] is not supported.", resolveOptions));
    if (resolveOptions.equals(StandardResolveOptions.RESOLVE_FILE)) {
      checkArgument(
          !other.endsWith("/"), "The resolved file: [%s] should not end with '/'.", other);
      return fromGcsPath(gcsPath.resolve(other));
    } else {
      // StandardResolveOptions.RESOLVE_DIRECTORY
      if (other.endsWith("/")) {
        // other already contains the delimiter for gcs.
        // It is not recommended for callers to set the delimiter.
        // However, we consider it as a valid input.
        return fromGcsPath(gcsPath.resolve(other));
      } else {
        return fromGcsPath(gcsPath.resolve(other + "/"));
      }
    }
  }

  @Override
  public GcsResourceId getCurrentDirectory() {
    if (isDirectory()) {
      return this;
    } else {
      GcsPath parent = gcsPath.getParent();
      checkState(
          parent != null,
          String.format("Failed to get the current directory for path: [%s].", gcsPath));
      return fromGcsPath(parent);
    }
  }

  @Override
  public boolean isDirectory() {
    return gcsPath.endsWith("/");
  }

  @Override
  public String getScheme() {
    return "gs";
  }

  @Override
  public @Nullable String getFilename() {
    if (gcsPath.getNameCount() <= 1) {
      return null;
    } else {
      GcsPath gcsFilename = gcsPath.getFileName();
      return gcsFilename == null ? null : gcsFilename.toString();
    }
  }

  GcsPath getGcsPath() {
    return gcsPath;
  }

  @Override
  public String toString() {
    return gcsPath.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof GcsResourceId)) {
      return false;
    }
    GcsResourceId other = (GcsResourceId) obj;
    return this.gcsPath.equals(other.gcsPath);
  }

  @Override
  public int hashCode() {
    return gcsPath.hashCode();
  }
}
