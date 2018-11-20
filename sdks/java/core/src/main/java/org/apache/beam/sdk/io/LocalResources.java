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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;

/**
 * Helper functions for producing a {@link ResourceId} that references a local file or directory.
 */
@Experimental(Kind.FILESYSTEM)
public final class LocalResources {

  public static ResourceId fromFile(File file, boolean isDirectory) {
    return LocalResourceId.fromPath(file.toPath(), isDirectory);
  }

  public static ResourceId fromPath(Path path, boolean isDirectory) {
    return LocalResourceId.fromPath(path, isDirectory);
  }

  public static ResourceId fromString(String filename, boolean isDirectory) {
    return LocalResourceId.fromPath(Paths.get(filename), isDirectory);
  }

  public static ValueProvider<ResourceId> fromString(
      ValueProvider<String> resourceProvider, final boolean isDirectory) {
    return NestedValueProvider.of(resourceProvider, input -> fromString(input, isDirectory));
  }

  private LocalResources() {} // prevent instantiation
}
