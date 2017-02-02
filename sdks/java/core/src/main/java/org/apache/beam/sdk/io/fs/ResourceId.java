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

import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;

/**
 * An identifier which represents a resource.
 *
 * <p>{@code ResourceId} is hierarchical and composed of a sequence of directory
 * and file name elements separated by a special separator or delimiter.
 */
public interface ResourceId {

  /**
   * Resolves the given {@code String} against this {@code ResourceId}.
   *
   * <p>In order to write file system agnostic code, callers should not include delimiters
   * in {@code other}, and should use {@link StandardResolveOptions} to specify
   * whether to resolve a file or a directory.
   *
   * <p>For example:
   *
   * <pre>{@code
   * ResourceId homeDir = ...
   * ResourceId tempOutput = homeDir
   *     .resolve("tempDir", StandardResolveOptions.RESOLVE_DIRECTORY)
   *     .resolve("output", StandardResolveOptions.RESOLVE_FILE);
   * }</pre>
   *
   * <p>This {@code ResourceId} should represents a directory.
   *
   * <p>It is up to each file system to resolve in their own way.
   */
  ResourceId resolve(String other, ResolveOptions resolveOptions);

  /**
   * Returns the {@code ResourceId} that represents the current directory of
   * this {@code ResourceId}.
   *
   * <p>If it is already a directory, trivially returns this.
   */
  ResourceId getCurrentDirectory();

  /**
   * Get the scheme which defines the namespace of the {@link ResourceId}.
   *
   * <p>The scheme is required to follow URI scheme syntax.
   * @see <a href="https://www.ietf.org/rfc/rfc2396.txt">RFC 2396</a>
   */
  String getScheme();
}
