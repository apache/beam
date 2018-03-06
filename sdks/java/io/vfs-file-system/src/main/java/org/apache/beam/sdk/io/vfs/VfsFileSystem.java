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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.vfs2.Capability.RANDOM_ACCESS_READ;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;

/**
 * A filesystem backed by a VFS filesystem.
 */
public class VfsFileSystem extends FileSystem<VfsResourceId> {
  private final FileSystemManager manager;
  private final String scheme;
  private final String schemePrefix;
  private final boolean isReadSeekEfficient;

  public VfsFileSystem(
      final FileSystemManager manager, final String scheme, final String schemePrefix) {
    this.manager = manager;
    this.scheme = scheme;
    this.schemePrefix = schemePrefix;
    try {
      this.isReadSeekEfficient =
          manager
              .getProviderCapabilities(scheme.substring(schemePrefix.length()))
              .contains(RANDOM_ACCESS_READ);
    } catch (final FileSystemException e) {
      // unlikely since we create a fs per existing scheme
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  protected String getScheme() {
    return scheme;
  }

  @Override
  protected VfsResourceId matchNewResource(
      final String singleResourceSpec, final boolean isDirectory) {
    try {
      return new VfsResourceId(
          schemePrefix,
          new URL(
              singleResourceSpec.substring(schemePrefix.length(), singleResourceSpec.length())));
    } catch (final MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  protected List<MatchResult> match(final List<String> specs) {
    // todo: support globs with PatternSelector?
    return specs
        .stream()
        .map(
            spec -> {
              if (!spec.startsWith(schemePrefix)) {
                return null;
              }
              try {
                return manager.resolveFile(URI.create(spec.substring(schemePrefix.length())));
              } catch (final FileSystemException e) {
                throw new IllegalArgumentException(e);
              }
            })
        .map(
            fo -> {
              try {
                if (fo == null || !fo.exists()) {
                  return MatchResult.create(MatchResult.Status.NOT_FOUND, emptyList());
                }
                return MatchResult.create(
                    MatchResult.Status.OK,
                    singletonList(
                        MatchResult.Metadata.builder()
                            .setIsReadSeekEfficient(isReadSeekEfficient)
                            .setResourceId(new VfsResourceId(schemePrefix, fo.getURL()))
                            .setSizeBytes(fo.getContent().getSize())
                            .build()));
              } catch (final FileSystemException e) {
                return MatchResult.create(MatchResult.Status.ERROR, new IOException(e));
              }
            })
        .collect(toList());
  }

  @Override
  protected WritableByteChannel create(
      final VfsResourceId resourceId, final CreateOptions createOptions) throws IOException {
    return new VfsWritableByteChannel(manager.resolveFile(resourceId.getUrl()).getContent());
  }

  @Override
  protected ReadableByteChannel open(final VfsResourceId resourceId) throws IOException {
    return new VfsReadableByteChannel(manager.resolveFile(resourceId.getUrl()).getContent());
  }

  @Override
  protected void rename(
      final List<VfsResourceId> srcResourceIds, final List<VfsResourceId> destResourceIds)
      throws IOException {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source files %s must equal number of destination files %s",
        srcResourceIds.size(),
        destResourceIds.size());

    final Iterator<VfsResourceId> froms = srcResourceIds.iterator();
    final Iterator<VfsResourceId> tos = destResourceIds.iterator();
    while (froms.hasNext() && tos.hasNext()) {
      final VfsResourceId from = froms.next();
      final VfsResourceId to = tos.next();
      final FileObject toFo = manager.resolveFile(to.getUrl());
      final FileObject fromFo = manager.resolveFile(from.getUrl());
      if (fromFo.canRenameTo(toFo)) { // guard
        fromFo.moveTo(toFo);
      }
    }
  }

  @Override
  protected void copy(
      final List<VfsResourceId> srcResourceIds, final List<VfsResourceId> destResourceIds)
      throws IOException {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source files %s must equal number of destination files %s",
        srcResourceIds.size(),
        destResourceIds.size());
    final Iterator<VfsResourceId> froms = srcResourceIds.iterator();
    final Iterator<VfsResourceId> tos = destResourceIds.iterator();
    while (froms.hasNext() && tos.hasNext()) {
      final VfsResourceId from = froms.next();
      final VfsResourceId to = tos.next();
      final FileObject toFo = manager.resolveFile(to.getUrl());
      final FileObject fromFo = manager.resolveFile(from.getUrl());
      toFo.copyFrom(fromFo, new AllFileSelector());
    }
  }

  @Override
  protected void delete(final Collection<VfsResourceId> resourceIds) {
    final Collection<Exception> errors =
        resourceIds
            .stream()
            .map(
                id -> {
                  try {
                    manager.resolveFile(id.getUrl()).deleteAll();
                    return null;
                  } catch (final FileSystemException e) {
                    return e;
                  }
                })
            .filter(Objects::nonNull)
            .collect(toList());
    if (!errors.isEmpty()) {
      final IllegalStateException ise = new IllegalStateException("Some files were not deleted");
      errors.forEach(ise::addSuppressed);
      throw ise;
    }
  }
}
