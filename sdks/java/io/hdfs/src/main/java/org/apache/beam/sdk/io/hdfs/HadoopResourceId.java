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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.hadoop.fs.Path;

/**
 * {@link ResourceId} implementation for the {@link HadoopFileSystem}.
 */
public class HadoopResourceId implements ResourceId {

  private final URI uri;

  /**
   * Constructs a HadoopResourceId from the provided absolute path. If only a relative path is
   * available, you can create a {@link HadoopResourceId} from the absolute path of the root of the
   * server, and then use resolve to add the relative path to the root.
   */
  public static HadoopResourceId fromPath(Path path) {
    checkNotNull(path, "path must not be null");
    checkArgument(path.isAbsolute(), "path must be absolute");
    return new HadoopResourceId(path);
  }

  private HadoopResourceId(Path path) {
    this.uri = path.toUri();
  }

  @Override
  public ResourceId resolve(String other, ResolveOptions resolveOptions) {
    checkArgument(
        resolveOptions.equals(ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
            || resolveOptions.equals(ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY),
        String.format("ResolveOptions: [%s] is not supported. "
            + "Supported ResolveOptions are RESOLVE_FILE and RESOLVE_DIRECTORY.", resolveOptions));
    if (resolveOptions.equals(ResolveOptions.StandardResolveOptions.RESOLVE_FILE)) {
      checkArgument(
          !other.endsWith("/"),
          "ResolveOptions: [%s] ends with '/', which is not supported for RESOLVE_FILE.",
          other);
    }
    return new HadoopResourceId(new Path(new Path(uri), other));
  }

  @Override
  public ResourceId getCurrentDirectory() {
    // See BEAM-2069. Possible workaround: inject FileSystem into this class, and call
    // org.apache.hadoop.fs.FileSystem#isDirectory.
    throw new UnsupportedOperationException();
  }

  @Override
  public String getScheme() {
    return uri.getScheme();
  }

  public Path getPath() {
    return new Path(uri);
  }

  @Override
  public String toString() {
    return uri.toString();
  }
}
