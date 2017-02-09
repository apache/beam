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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FileSystem} implementation for local files.
 */
class LocalFileSystem extends FileSystem<LocalResourceId> {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystem.class);

  LocalFileSystem() {
  }

  @Override
  protected WritableByteChannel create(LocalResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    LOG.debug("creating file {}", resourceId);
    File absoluteFile = resourceId.getPath().toFile().getAbsoluteFile();
    if (absoluteFile.getParentFile() != null
        && !absoluteFile.getParentFile().exists()
        && !absoluteFile.getParentFile().mkdirs()
        && !absoluteFile.getParentFile().exists()) {
      throw new IOException("Unable to create parent directories for '" + resourceId + "'");
    }
    return Channels.newChannel(
        new BufferedOutputStream(new FileOutputStream(absoluteFile)));
  }

  @Override
  protected ReadableByteChannel open(LocalResourceId resourceId) throws IOException {
    LOG.debug("opening file {}", resourceId);
    @SuppressWarnings("resource") // The caller is responsible for closing the channel.
    FileInputStream inputStream = new FileInputStream(resourceId.getPath().toFile());
    // Use this method for creating the channel (rather than new FileChannel) so that we get
    // regular FileNotFoundException. Closing the underyling channel will close the inputStream.
    return inputStream.getChannel();
  }

  @Override
  protected void copy(
      List<LocalResourceId> srcResourceIds,
      List<LocalResourceId> destResourceIds) throws IOException {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source files %s must equal number of destination files %s",
        srcResourceIds.size(),
        destResourceIds.size());
    int numFiles = srcResourceIds.size();
    for (int i = 0; i < numFiles; i++) {
      LocalResourceId src = srcResourceIds.get(i);
      LocalResourceId dst = destResourceIds.get(i);
      LOG.debug("Copying {} to {}", src, dst);
      try {
        // Copy the source file, replacing the existing destination.
        // Paths.get(x) will not work on Windows OSes cause of the ":" after the drive letter.
        Files.copy(
            src.getPath(),
            dst.getPath(),
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.COPY_ATTRIBUTES);
      } catch (NoSuchFileException e) {
        LOG.debug("{} does not exist.", src);
        // Suppress exception if file does not exist.
        // TODO: re-throw FileNotFoundException once FileSystems supports ignoreMissingFile.
      }
    }
  }

  @Override
  protected void rename(
      List<LocalResourceId> srcResourceIds,
      List<LocalResourceId> destResourceIds) throws IOException {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source files %s must equal number of destination files %s",
        srcResourceIds.size(),
        destResourceIds.size());
    int numFiles = srcResourceIds.size();
    for (int i = 0; i < numFiles; i++) {
      LocalResourceId src = srcResourceIds.get(i);
      LocalResourceId dst = destResourceIds.get(i);
      LOG.debug("Renaming {} to {}", src, dst);
      try {
        // Rename the source file, replacing the existing destination.
        Files.move(
            src.getPath(),
            dst.getPath(),
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.COPY_ATTRIBUTES,
            StandardCopyOption.ATOMIC_MOVE);
      } catch (NoSuchFileException e) {
        LOG.debug("{} does not exist.", src);
        // Suppress exception if file does not exist.
        // TODO: re-throw FileNotFoundException once FileSystems supports ignoreMissingFile.
      }
    }
  }

  @Override
  protected void delete(Collection<LocalResourceId> resourceIds) throws IOException {
    for (LocalResourceId resourceId : resourceIds) {
      LOG.debug("deleting file {}", resourceId);
      // Delete the file if it exists.
      // TODO: use Files.delete() once FileSystems supports ignoreMissingFile.
      boolean exists = Files.deleteIfExists(resourceId.getPath());
      if (!exists) {
        LOG.debug("Tried to delete {}, but it did not exist", resourceId);
      }
    }
  }
}
