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

import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;

/** Implements a resource for FakeFileSystem. */
class FakeResourceId implements ResourceId {

  private final String scheme;
  private final String filename;
  private final boolean isDirectory;

  FakeResourceId(String scheme, String filename, boolean isDirectory) {
    this.scheme = scheme;
    this.filename = filename;
    this.isDirectory = isDirectory;
  }

  @Override
  public FakeResourceId getCurrentDirectory() {
    return this;
  }

  @Override
  public String getScheme() {
    return scheme;
  }

  @Override
  public String getFilename() {
    return filename;
  }

  @Override
  public boolean isDirectory() {
    return isDirectory;
  }

  @Override
  public ResourceId resolve(String other, ResolveOptions resolveOptions) {
    if (other.equals("..")
        || other.equals("*")
        || !(resolveOptions instanceof StandardResolveOptions)) {
      throw new UnsupportedOperationException();
    }
    switch ((StandardResolveOptions) resolveOptions) {
      case RESOLVE_DIRECTORY:
        return new FakeResourceId(getScheme(), other, true);
      case RESOLVE_FILE:
        return new FakeResourceId(getScheme(), other, false);
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public String toString() {
    return String.format("%s://%s", getScheme(), getFilename());
  }
}
