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
package org.apache.beam.sdk.io.hdfs;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.net.URI;
import java.util.Objects;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.hadoop.fs.Path;
import org.checkerframework.checker.nullness.qual.Nullable;

/** {@link ResourceId} implementation for the {@link HadoopFileSystem}. */
class HadoopResourceId implements ResourceId {
  private final URI uri;

  HadoopResourceId(URI uri) {
    this.uri = uri;
  }

  @Override
  public ResourceId resolve(String other, ResolveOptions resolveOptions) {
    checkState(
        isDirectory(), String.format("Expected this resource is a directory, but had [%s].", uri));
    if (resolveOptions == StandardResolveOptions.RESOLVE_DIRECTORY) {
      if (!other.endsWith("/")) {
        other += "/";
      }
      return new HadoopResourceId(uri.resolve(other));
    } else if (resolveOptions == StandardResolveOptions.RESOLVE_FILE) {
      checkArgument(!other.endsWith("/"), "Resolving a file with a directory path: %s", other);
      return new HadoopResourceId(uri.resolve(other));
    } else {
      throw new UnsupportedOperationException(
          String.format("Unexpected StandardResolveOptions %s", resolveOptions));
    }
  }

  @Override
  public ResourceId getCurrentDirectory() {
    return new HadoopResourceId(uri.getPath().endsWith("/") ? uri : uri.resolve("."));
  }

  @Override
  public boolean isDirectory() {
    return uri.getPath().endsWith("/");
  }

  @Override
  public String getFilename() {
    if (isDirectory()) {
      Path parentPath = new Path(uri).getParent();
      return parentPath == null ? null : parentPath.getName();
    }
    return new Path(uri).getName();
  }

  @Override
  public String getScheme() {
    return uri.getScheme();
  }

  @Override
  public String toString() {
    return uri.toString();
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (!(obj instanceof HadoopResourceId)) {
      return false;
    }
    return Objects.equals(uri, ((HadoopResourceId) obj).uri);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(uri);
  }

  Path toPath() {
    return new Path(uri);
  }
}
