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
package org.apache.beam.examples.subprocess.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.beam.examples.subprocess.configuration.SubProcessConfiguration;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for dealing with movement of files from object stores and workers. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class FileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  public static ResourceId getFileResourceId(String directory, String fileName) {
    ResourceId resourceID = FileSystems.matchNewResource(directory, true);
    return resourceID.getCurrentDirectory().resolve(fileName, StandardResolveOptions.RESOLVE_FILE);
  }

  public static String toStringParams(ProcessBuilder builder) {
    return String.join(",", builder.command());
  }

  public static String copyFileFromWorkerToGCS(
      SubProcessConfiguration configuration, Path fileToUpload) throws Exception {

    Path fileName;

    if ((fileName = fileToUpload.getFileName()) == null) {
      throw new IllegalArgumentException("FileName can not be null.");
    }

    ResourceId sourceFile = getFileResourceId(configuration.getWorkerPath(), fileName.toString());

    LOG.info("Copying file from worker " + sourceFile);

    ResourceId destinationFile =
        getFileResourceId(configuration.getSourcePath(), fileName.toString());
    // TODO currently not supported with different schemas for example GCS to local, else could use
    // FileSystems.copy(ImmutableList.of(sourceFile), ImmutableList.of(destinationFile));
    try {
      return copyFile(sourceFile, destinationFile);
    } catch (Exception ex) {
      LOG.error(
          String.format("Error copying file from %s  to %s", sourceFile, destinationFile), ex);
      throw ex;
    }
  }

  public static String copyFileFromGCSToWorker(ExecutableFile execuableFile) throws Exception {

    ResourceId sourceFile =
        FileSystems.matchNewResource(execuableFile.getSourceGCSLocation(), false);
    ResourceId destinationFile =
        FileSystems.matchNewResource(execuableFile.getDestinationLocation(), false);
    try {
      LOG.info(
          String.format(
              "Moving File %s to %s ",
              execuableFile.getSourceGCSLocation(), execuableFile.getDestinationLocation()));
      Path path = Paths.get(execuableFile.getDestinationLocation());

      if (path.toFile().exists()) {
        LOG.warn(
            String.format(
                "Overwriting file %s, should only see this once per worker.",
                execuableFile.getDestinationLocation()));
      }
      copyFile(sourceFile, destinationFile);
      path.toFile().setExecutable(true);
      return path.toString();

    } catch (Exception ex) {
      LOG.error(String.format("Error moving file : %s ", execuableFile.fileName), ex);
      throw ex;
    }
  }

  public static String copyFile(ResourceId sourceFile, ResourceId destinationFile)
      throws IOException {

    try (WritableByteChannel writeChannel = FileSystems.create(destinationFile, "text/plain")) {
      try (ReadableByteChannel readChannel = FileSystems.open(sourceFile)) {

        final ByteBuffer buffer = ByteBuffer.allocateDirect(16 * 1024);
        while (readChannel.read(buffer) != -1) {
          buffer.flip();
          writeChannel.write(buffer);
          buffer.compact();
        }
        buffer.flip();
        while (buffer.hasRemaining()) {
          writeChannel.write(buffer);
        }
      }
    }

    return destinationFile.toString();
  }

  /**
   * Create directories needed based on configuration.
   *
   * @param configuration
   * @throws IOException
   */
  public static void createDirectoriesOnWorker(SubProcessConfiguration configuration)
      throws IOException {

    try {

      Path path = Paths.get(configuration.getWorkerPath());

      if (!path.toFile().exists()) {
        Files.createDirectories(path);
        LOG.info(String.format("Created Folder %s ", path.toFile()));
      }
    } catch (FileAlreadyExistsException ex) {
      LOG.warn(
          String.format(
              " Tried to create folder %s which already existsed, this should not happen!",
              configuration.getWorkerPath()),
          ex);
    }
  }

  public static String readLineOfLogFile(Path path) {

    try (BufferedReader br = Files.newBufferedReader(Paths.get(path.toString()), UTF_8)) {
      return br.readLine();
    } catch (IOException e) {
      LOG.error("Error reading the first line of file", e);
    }

    // `return empty string rather than NULL string as this data is often used in further logging
    return "";
  }
}
