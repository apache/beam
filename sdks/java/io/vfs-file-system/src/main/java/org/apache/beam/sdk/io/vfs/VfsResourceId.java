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

import java.net.URL;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;

/**
 * A resource representation of a VFS file.
 */
public class VfsResourceId implements ResourceId {
  private final String schemePrefix;
  private final URL url;
  private transient Integer hash;
  private transient FileObject fileObject;

  public VfsResourceId(final String schemePrefix, final URL url) {
    this.schemePrefix = schemePrefix;
    this.url = url;
  }

  public URL getUrl() {
    return url;
  }

  @Override
  public ResourceId resolve(final String other, final ResolveOptions resolveOptions) {
    try {
      return new VfsResourceId(schemePrefix, getFileObject().resolveFile(other).getURL());
    } catch (final FileSystemException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public ResourceId getCurrentDirectory() {
    try {
      return isDirectory()
          ? this
          : new VfsResourceId(schemePrefix, fileObject.getParent().getURL());
    } catch (final FileSystemException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String getScheme() {
    return schemePrefix + url.getProtocol();
  }

  @Nullable
  @Override
  public String getFilename() {
    return getFileObject().getName().getBaseName();
  }

  @Override
  public boolean isDirectory() {
    try {
      return getFileObject().isFolder();
    } catch (final FileSystemException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean equals(final Object o) {
    return this == o
        || o != null
            && getClass() == o.getClass()
            && Objects.equals(url, VfsResourceId.class.cast(o).url);
  }

  @Override
  public int hashCode() {
    if (hash == null) {
      synchronized (this) {
        if (hash == null) {
          hash = Objects.hash(url);
        }
      }
    }
    return hash;
  }

  @Override
  public String toString() {
    return schemePrefix + url.toExternalForm();
  }

  private FileObject getFileObject() {
    if (fileObject != null) {
      return fileObject;
    }
    synchronized (this) {
      if (fileObject == null) {
        try {
          return fileObject = getManager().resolveFile(url);
        } catch (final FileSystemException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    return fileObject;
  }

  // todo: the file API should propagate the filesystem to the id pby
  private FileSystemManager getManager() {
    try {
      return VFS.getManager();
    } catch (final FileSystemException e) {
      throw new IllegalStateException(e);
    }
  }
}
