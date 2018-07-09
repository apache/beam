/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.vfs;

import static java.util.stream.Collectors.toList;

import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registration entry point for VFS available implementation of FileSystems. */
@Experimental(Experimental.Kind.FILESYSTEM)
public class VfsFileSystemRegistrar implements FileSystemRegistrar {
  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

  @Override
  public Iterable<FileSystem> fromOptions(@Nonnull final PipelineOptions options) {
    final FileSystemManager manager;
    try {
      manager = VFS.getManager();
    } catch (final FileSystemException e) {
      throw new IllegalStateException(e);
    }
    final VfsOptions vfsOptions = options.as(VfsOptions.class);
    final String prefix = vfsOptions.getSchemePrefix();
    return Stream.of(manager.getSchemes())
        .map(scheme -> prefix + scheme)
        .peek(scheme -> LOG.debug("Registering VFS filesystem for scheme {}", scheme))
        .map(scheme -> new VfsFileSystem(manager, scheme, prefix))
        .collect(toList());
  }
}
