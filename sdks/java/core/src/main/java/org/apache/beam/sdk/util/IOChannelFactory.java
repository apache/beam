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
package org.apache.beam.sdk.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;

import javax.annotation.Nullable;

/**
 * Defines a factory for working with read and write channels.
 *
 * <p>Channels provide an abstract API for IO operations.
 *
 * <p>See <a href="http://docs.oracle.com/javase/7/docs/api/java/nio/channels/package-summary.html"
 * >Java NIO Channels</a>
 */
public interface IOChannelFactory {

  /**
   * Matches a specification, which may contain globs, against available
   * resources.
   *
   * <p>Glob handling is dependent on the implementation.  Implementations should
   * all support globs in the final component of a path (eg /foo/bar/*.txt),
   * however they are not required to support globs in the directory paths.
   *
   * <p>The list of resources returned are required to exist and not represent abstract
   * resources such as symlinks and directories.
   */
  Collection<String> match(String spec) throws IOException;

  /**
   * Returns a read channel for the given specification.
   *
   * <p>The specification is not expanded; it is used verbatim.
   *
   * <p>If seeking is supported, then this returns a
   * {@link java.nio.channels.SeekableByteChannel}.
   */
  ReadableByteChannel open(String spec) throws IOException;

  /**
   * Returns a write channel for the given specification.
   *
   * <p>The specification is not expanded; is it used verbatim.
   */
  WritableByteChannel create(String spec, String mimeType) throws IOException;

  /**
   * Returns the size in bytes for the given specification.
   *
   * <p>The specification is not expanded; it is used verbatim.
   *
   * <p>{@link FileNotFoundException} will be thrown if the resource does not exist.
   */
  long getSizeBytes(String spec) throws IOException;

  /**
   * Returns {@code true} if the channel created when invoking method {@link #open} for the given
   * file specification is guaranteed to be of type {@link java.nio.channels.SeekableByteChannel
   * SeekableByteChannel} and if seeking into positions of the channel is recommended. Returns
   * {@code false} if the channel returned is not a {@code SeekableByteChannel}. May return
   * {@code false} even if the channel returned is a {@code SeekableByteChannel}, if seeking is not
   * efficient for the given file specification.
   *
   * <p>Only efficiently seekable files can be split into offset ranges.
   *
   * <p>The specification is not expanded; it is used verbatim.
   */
  boolean isReadSeekEfficient(String spec) throws IOException;

  /**
   * Resolve the given {@code other} against the {@code path}.
   *
   * <p>If the {@code other} parameter is an absolute path then this method trivially returns
   * other. If {@code other} is an empty path then this method trivially returns the given
   * {@code path}. Otherwise this method considers the given {@code path} to be a directory and
   * resolves the {@code other} path against this path. In the simplest case, the {@code other}
   * path does not have a root component, in which case this method joins the {@code other} path
   * to the given {@code path} and returns a resulting path that ends with the {@code other} path.
   * Where the {@code other} path has a root component then resolution is highly implementation
   * dependent and therefore unspecified.
   */
  String resolve(String path, String other) throws IOException;

  /**
   * Retrieve a URL where the given {@code path} can be viewed and browsed, or null if browse URLs
   * are not supported.
   *
   * <p>The returned URL should be suitable for a user to enter into a web browser and browse
   * interactively. If the {@code path} refers to a file or data resource, the URL should
   * point to a location where the resource can be viewed. If the {@code path} points to a
   * directory or contains wildcards, the URL should point to a location where the inner resources
   * can be browsed.
   *
   * <p>This method does not validate that a resource exists or is accessible for the given
   * {@code path}.
   */
  @Nullable
  String getBrowseUrl(String path);
}
