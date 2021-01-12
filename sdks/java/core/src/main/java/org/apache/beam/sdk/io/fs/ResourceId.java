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
package org.apache.beam.sdk.io.fs;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An identifier which represents a file-like resource.
 *
 * <p>{@link ResourceId} is hierarchical and composed of a sequence of directory and file name
 * elements separated by a special separator or delimiter.
 *
 * <p>{@link ResourceId ResourceIds} are created using {@link FileSystems}. The two primary
 * mechanisms are:
 *
 * <ul>
 *   <li>{@link FileSystems#match(java.util.List)}, which takes a list of {@link String} resource
 *       names or globs, queries the {@link FileSystem} for resources matching these specifications,
 *       and returns a {@link MatchResult} for each glob. This is typically used when reading from
 *       files.
 *   <li>{@link FileSystems#matchNewResource(String, boolean)}, which takes a {@link String} full
 *       resource name and type (file or directory) and generates a {@link FileSystem}-specific
 *       {@code ResourceId} for that resource. This call does not verify the presence or absence of
 *       that resource in the file system. This call is typically used when creating new directories
 *       or files to generate {@link ResourceId ResourceIds} for resources that may not yet exist.
 * </ul>
 */
@Experimental(Kind.FILESYSTEM)
public interface ResourceId extends Serializable {

  /**
   * Returns a child {@code ResourceId} under {@code this}.
   *
   * <p>In order to write file system agnostic code, callers should not include delimiters in {@code
   * other}, and should use {@link StandardResolveOptions} to specify whether to resolve a file or a
   * directory.
   *
   * <p>For example:
   *
   * <pre>{@code
   * ResourceId homeDir = ...;
   * ResourceId tempOutput = homeDir
   *     .resolve("tempDir", StandardResolveOptions.RESOLVE_DIRECTORY)
   *     .resolve("output", StandardResolveOptions.RESOLVE_FILE);
   * }</pre>
   *
   * <p>This {@link ResourceId} should represents a directory.
   *
   * <p>It is up to each file system to resolve in their own way.
   *
   * <p>Resolving special characters:
   *
   * <ul>
   *   <li>{@code resourceId.resolve("..", StandardResolveOptions.RESOLVE_DIRECTORY)} returns the
   *       parent directory of this {@code ResourceId}.
   *   <li>{@code resourceId.resolve("*", StandardResolveOptions.RESOLVE_FILE)} returns a {@code
   *       ResourceId} which matches all files in this {@code ResourceId}.
   *   <li>{@code resourceId.resolve("*", StandardResolveOptions.RESOLVE_DIRECTORY)} returns a
   *       {@code ResourceId} which matches all directories in this {@code ResourceId}.
   * </ul>
   *
   * @throws IllegalStateException if this {@link ResourceId} is not a directory.
   * @throws IllegalArgumentException if {@code other} contains illegal characters or is an illegal
   *     name. It is recommended that callers use common characters, such as {@code [_a-zA-Z0-9.-]},
   *     in {@code other}.
   */
  ResourceId resolve(String other, ResolveOptions resolveOptions);

  /**
   * Returns the {@code ResourceId} that represents the current directory of this {@code
   * ResourceId}.
   *
   * <p>If it is already a directory, trivially returns this.
   */
  ResourceId getCurrentDirectory();

  /**
   * Get the scheme which defines the namespace of the {@link ResourceId}.
   *
   * <p>The scheme is required to follow URI scheme syntax. See <a
   * href="https://www.ietf.org/rfc/rfc2396.txt">RFC 2396</a>
   */
  String getScheme();

  /**
   * Returns the name of the file or directory denoted by this {@code ResourceId}. The file name is
   * the farthest element from the root in the directory hierarchy.
   *
   * @return a string representing the name of file or directory, or null if there are zero
   *     components.
   */
  @Nullable
  String getFilename();

  /** Returns {@code true} if this {@link ResourceId} represents a directory, false otherwise. */
  boolean isDirectory();

  /**
   * Returns the string representation of this {@link ResourceId}.
   *
   * <p>The corresponding {@link FileSystem#match} is required to accept this string representation.
   */
  @Override
  String toString();
}
