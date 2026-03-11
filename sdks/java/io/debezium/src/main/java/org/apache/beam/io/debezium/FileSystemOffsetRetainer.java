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
package org.apache.beam.io.debezium;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OffsetRetainer} that persists the Debezium connector offset as a JSON file using Beam's
 * {@link FileSystems} abstraction.
 *
 * <p>The {@code path} argument can point to any filesystem supported by the active Beam runner,
 * including local disk, Google Cloud Storage, Amazon S3, and others
 *
 * <p>On every {@code task.commit()}, the latest offset is serialised to JSON and written to the
 * given path (overwriting the previous file). On pipeline startup the file is read back and the
 * connector resumes from the stored position. If the file does not yet exist the connector starts
 * from the beginning of the change stream.
 *
 * <p>Example — resume from GCS:
 *
 * <pre>{@code
 * DebeziumIO.read()
 *     .withConnectorConfiguration(config)
 *     .withOffsetRetainer(
 *         FileSystemOffsetRetainer.of("gs://my-bucket/debezium/orders-offset.json"))
 *     .withFormatFunction(myMapper);
 * }</pre>
 *
 * <p>Example — local filesystem (useful for testing):
 *
 * <pre>{@code
 * DebeziumIO.read()
 *     .withConnectorConfiguration(config)
 *     .withOffsetRetainer(FileSystemOffsetRetainer.of("/tmp/debezium-offset.json"))
 *     .withFormatFunction(myMapper);
 * }</pre>
 *
 * <p><b>Note:</b> writes are performed atomically: the offset is first written to a {@code .tmp}
 * sibling file and then renamed to the final path, so a mid-write crash leaves the previous offset
 * intact.
 */
public class FileSystemOffsetRetainer implements OffsetRetainer {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemOffsetRetainer.class);
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final String path;

  // ObjectMapper is thread-safe after configuration and does not need to be serialised.
  private transient @Nullable ObjectMapper objectMapper;

  // Tracks the last successfully saved offset so repeated identical saves are skipped.
  private transient @Nullable Map<String, Object> lastSavedOffset;

  private FileSystemOffsetRetainer(String path) {
    this.path = path;
  }

  /** Creates a new {@code FileSystemOffsetRetainer} that stores the offset at {@code path}. */
  public static FileSystemOffsetRetainer of(String path) {
    return new FileSystemOffsetRetainer(path);
  }

  private ObjectMapper mapper() {
    if (objectMapper == null) {
      objectMapper = new ObjectMapper();
    }
    return objectMapper;
  }

  /**
   * Reads the offset JSON file and returns its contents, or {@code null} if the file does not yet
   * exist (first run). Throws {@link RuntimeException} if the file exists but cannot be read, to
   * prevent silently reprocessing data from the beginning.
   */
  @Override
  public @Nullable Map<String, Object> loadOffset() {
    try {
      ResourceId resourceId = FileSystems.matchNewResource(path, /* isDirectory= */ false);
      try (ReadableByteChannel channel = FileSystems.open(resourceId);
          InputStream stream = Channels.newInputStream(channel)) {
        Map<String, Object> offset = mapper().readValue(stream, MAP_TYPE);
        LOG.info("OffsetRetainer: loaded offset from {}: {}", path, offset);
        return offset;
      }
    } catch (FileNotFoundException e) {
      LOG.info("OffsetRetainer: no offset file found at {}; starting from the beginning.", path);
      return null;
    } catch (IOException e) {
      throw new RuntimeException(
          "OffsetRetainer: failed to read offset from "
              + path
              + ". "
              + "Delete the file to restart from the beginning.",
          e);
    }
  }

  /**
   * Serialises {@code offset} to JSON and writes it atomically to the configured path.
   *
   * <p>If the offset is identical to the last successfully written one, the write is skipped to
   * avoid unnecessary I/O on every checkpoint.
   *
   * <p>Otherwise the data is first written to a {@code .tmp} sibling file and then renamed to the
   * final path, so a mid-write crash leaves the previous offset intact.
   *
   * <p>Errors are logged as warnings and swallowed so the pipeline continues.
   */
  @Override
  public void saveOffset(Map<String, Object> offset) {
    if (offset.equals(lastSavedOffset)) {
      LOG.debug("OffsetRetainer: offset unchanged, skipping write to {}", path);
      return;
    }
    String tmpPath = path + ".tmp";
    try {
      ResourceId tmpResourceId = FileSystems.matchNewResource(tmpPath, /* isDirectory= */ false);
      try (WritableByteChannel channel = FileSystems.create(tmpResourceId, "application/json");
          OutputStream stream = Channels.newOutputStream(channel)) {
        mapper().writeValue(stream, offset);
      }
      ResourceId finalResourceId = FileSystems.matchNewResource(path, /* isDirectory= */ false);
      FileSystems.rename(
          Collections.singletonList(tmpResourceId), Collections.singletonList(finalResourceId));
      lastSavedOffset = offset;
      LOG.debug("OffsetRetainer: saved offset to {}: {}", path, offset);
    } catch (IOException e) {
      LOG.warn(
          "OffsetRetainer: failed to save offset to {}."
              + " The offset will be lost if the pipeline restarts.",
          path,
          e);
    }
  }
}
