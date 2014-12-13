/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;

/**
 * Defines a factory for working with read and write channels.
 *
 * Channels provide an abstract API for IO operations.
 *
 * See <a href="http://docs.oracle.com/javase/7/docs/api/java/nio/channels/package-summary.html
 * >Java NIO Channels</a>
 */
public interface IOChannelFactory {

  /**
   * Matches a specification, which may contain globs, against available
   * resources.
   *
   * Glob handling is dependent on the implementation.  Implementations should
   * all support globs in the final component of a path (eg /foo/bar/*.txt),
   * however they are not required to support globs in the directory paths.
   *
   * The result is the (possibly empty) set of specifications which match.
   */
  Collection<String> match(String spec) throws IOException;

  /**
   * Returns a read channel for the given specification.
   *
   * The specification is not expanded; it is used verbatim.
   *
   * If seeking is supported, then this returns a
   * {@link java.nio.channels.SeekableByteChannel}.
   */
  ReadableByteChannel open(String spec) throws IOException;

  /**
   * Returns a write channel for the given specification.
   *
   * The specification is not expanded; is it used verbatim.
   */
  WritableByteChannel create(String spec, String mimeType) throws IOException;

  /**
   * Returns the size in bytes for the given specification.
   *
   * The specification is not expanded; it is used verbatim.
   */
  long getSizeBytes(String spec) throws IOException;
}
