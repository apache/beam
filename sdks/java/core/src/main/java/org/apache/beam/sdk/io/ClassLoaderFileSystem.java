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

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A read-only {@link FileSystem} implementation looking up resources using a ClassLoader. */
public class ClassLoaderFileSystem extends FileSystem<ClassLoaderFileSystem.ClassLoaderResourceId> {

  public static final String SCHEMA = "classpath";
  private static final String PREFIX = SCHEMA + "://";

  ClassLoaderFileSystem() {}

  @Override
  protected List<MatchResult> match(List<String> specs) throws IOException {
    throw new UnsupportedOperationException("Un-globbable filesystem.");
  }

  @Override
  protected WritableByteChannel create(
      ClassLoaderResourceId resourceId, CreateOptions createOptions) throws IOException {
    throw new UnsupportedOperationException("Read-only filesystem.");
  }

  @Override
  protected ReadableByteChannel open(ClassLoaderResourceId resourceId) throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    InputStream inputStream =
        classLoader.getResourceAsStream(resourceId.path.substring(PREFIX.length()));
    if (inputStream == null) {

      throw new IOException(
          "Unable to load "
              + resourceId.path
              + " with "
              + classLoader
              + " URL "
              + classLoader.getResource(resourceId.path.substring(PREFIX.length())));
    }
    return Channels.newChannel(inputStream);
  }

  @Override
  protected void copy(
      List<ClassLoaderResourceId> srcResourceIds, List<ClassLoaderResourceId> destResourceIds)
      throws IOException {
    throw new UnsupportedOperationException("Read-only filesystem.");
  }

  @Override
  protected void rename(
      List<ClassLoaderResourceId> srcResourceIds, List<ClassLoaderResourceId> destResourceIds)
      throws IOException {
    throw new UnsupportedOperationException("Read-only filesystem.");
  }

  @Override
  protected void delete(Collection<ClassLoaderResourceId> resourceIds) throws IOException {
    throw new UnsupportedOperationException("Read-only filesystem.");
  }

  @Override
  protected ClassLoaderResourceId matchNewResource(String path, boolean isDirectory) {
    return new ClassLoaderResourceId(path);
  }

  @Override
  protected String getScheme() {
    return SCHEMA;
  }

  public static class ClassLoaderResourceId implements ResourceId {

    private final String path;

    private ClassLoaderResourceId(String path) {
      checkArgument(path.startsWith(PREFIX), path);
      this.path = path;
    }

    @Override
    public ClassLoaderResourceId resolve(String other, ResolveOptions resolveOptions) {
      if (other.startsWith(PREFIX)) {
        return new ClassLoaderResourceId(other);
      } else if (other.startsWith("/")) {
        return new ClassLoaderResourceId(SCHEMA + ":/" + other);
      } else {
        return new ClassLoaderResourceId(path + "/" + other);
      }
    }

    @Override
    public ClassLoaderResourceId getCurrentDirectory() {
      int ix = path.lastIndexOf('/');
      if (ix <= PREFIX.length()) {
        return new ClassLoaderResourceId(PREFIX);
      } else {
        return new ClassLoaderResourceId(path.substring(0, ix));
      }
    }

    @Override
    public String getScheme() {
      return SCHEMA;
    }

    @Nullable
    @Override
    public String getFilename() {
      return path;
    }

    @Override
    public boolean isDirectory() {
      return false;
    }
  }

  /** {@link AutoService} registrar for the {@link ClassLoaderFileSystem}. */
  @AutoService(FileSystemRegistrar.class)
  @Experimental(Experimental.Kind.FILESYSTEM)
  public static class ClassLoaderFileSystemRegistrar implements FileSystemRegistrar {
    @Override
    public Iterable<FileSystem> fromOptions(@Nullable PipelineOptions options) {
      return ImmutableList.of(new ClassLoaderFileSystem());
    }
  }
}
