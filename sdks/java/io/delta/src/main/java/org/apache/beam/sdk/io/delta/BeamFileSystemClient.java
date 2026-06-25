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
package org.apache.beam.sdk.io.delta;

import io.delta.kernel.engine.FileReadRequest;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;

/** A Delta Kernel {@link FileSystemClient} backed by Beam's {@link FileSystems}. */
public class BeamFileSystemClient implements FileSystemClient {
  @Override
  public CloseableIterator<FileStatus> listFrom(String path) throws IOException {
    String glob = globForSiblings(path);
    List<FileStatus> statuses = new ArrayList<>();
    String normalizedInput = FileSystems.matchNewResource(path, false).toString();
    for (MatchResult.Metadata metadata :
      FileSystems.match(glob, EmptyMatchTreatment.ALLOW).metadata()) {
      if (metadata.resourceId().isDirectory()) {
        continue;
      }
      String metadataPath = metadata.resourceId().toString();
      if (normalizeForOrdering(metadataPath).compareTo(normalizeForOrdering(normalizedInput)) >= 0) {
        statuses.add(toDeltaFileStatus(metadata));
      }
    }
    statuses.sort(
        (first, second) ->
            normalizeForOrdering(first.getPath())
                .compareTo(normalizeForOrdering(second.getPath())));
    return closeableIterator(statuses.iterator());
  }

  @Override
  public String resolvePath(String path) throws IOException {
    try {
      return getFileStatus(path).getPath();
    } catch (IOException e) {
      return FileSystems.matchNewResource(path, false).toString();
    }
  }

  @Override
  public CloseableIterator<ByteArrayInputStream> readFiles(
      CloseableIterator<FileReadRequest> readRequests) {
    return new CloseableIterator<ByteArrayInputStream>() {
      @Override
      public boolean hasNext() {
        return readRequests.hasNext();
      }

      @Override
      public ByteArrayInputStream next() {
        FileReadRequest request = readRequests.next();
        try {
          return readRange(request.getPath(), request.getStartOffset(), request.getReadLength());
        } catch (IOException e) {
          throw new UncheckedIOException(
              String.format(
                  "IOException reading from file %s at offset %s size %s",
                  request.getPath(), request.getStartOffset(), request.getReadLength()),
              e);
        }
      }

      @Override
      public void close() throws IOException {
        readRequests.close();
      }
    };
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    if (isLocalPath(path)) {
      Files.createDirectories(toLocalPath(path));
    }
    return true;
  }

  @Override
  public boolean delete(String path) throws IOException {
    FileSystems.delete(
        Collections.singletonList(FileSystems.matchNewResource(path, false)),
        MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
    return true;
  }

  @Override
  public FileStatus getFileStatus(String path) throws IOException {
    return toDeltaFileStatus(FileSystems.matchSingleFileSpec(path));
  }

  @Override
  public void copyFileAtomically(String src, String dst, boolean overwrite) throws IOException {
    ResourceId srcResource = FileSystems.matchNewResource(src, false);
    ResourceId dstResource = FileSystems.matchNewResource(dst, false);
    MatchResult dstMatch = FileSystems.match(dst, EmptyMatchTreatment.ALLOW);
    if (!overwrite
        && dstMatch.status() == MatchResult.Status.OK
        && !dstMatch.metadata().isEmpty()) {
      throw new IOException("Destination already exists: " + dst);
    }

    FileSystems.copy(
            Collections.singletonList(srcResource), Collections.singletonList(dstResource));
  }

  private static ByteArrayInputStream readRange(String path, int startOffset, int readLength)
    throws IOException {
    ResourceId resourceId = FileSystems.matchNewResource(path, false);
    try (ReadableByteChannel channel = FileSystems.open(resourceId)) {
      byte[] data = new byte[readLength];
      if (channel instanceof SeekableByteChannel) {
        ((SeekableByteChannel) channel).position(startOffset);
        readFully(channel, ByteBuffer.wrap(data));
      } else {
        try (InputStream stream = Channels.newInputStream(channel)) {
          com.google.common.io.ByteStreams.skipFully(stream, startOffset);
          com.google.common.io.ByteStreams.readFully(stream, data);
        }
      }
      return new ByteArrayInputStream(data);
    }
  }

  private static void readFully(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      if (channel.read(buffer) < 0) {
        throw new EOFException("Unexpected end of file");
      }
    }
  }

  private static FileStatus toDeltaFileStatus(MatchResult.Metadata metadata) {
    return FileStatus.of(
        metadata.resourceId().toString(), metadata.sizeBytes(), metadata.lastModifiedMillis());
  }

  private static String globForSiblings(String path) {
    String normalized = path.replace('\\', '/');
    int lastSlash = normalized.lastIndexOf('/');
    if (lastSlash < 0) {
      return "*";
    }
    return normalized.substring(0, lastSlash + 1) + "*";
  }

  private static String normalizeForOrdering(String path) {
    return path.replace('\\', '/');
  }

  private static boolean isLocalPath(String path) {
    int schemeSeparator = path.indexOf(':');
    if (schemeSeparator < 0) {
      return true;
    }
    String scheme = path.substring(0, schemeSeparator).toLowerCase(Locale.ROOT);
    return scheme.length() == 1 || "file".equals(scheme);
  }

  private static Path toLocalPath(String path) {
    if (path.toLowerCase(Locale.ROOT).startsWith("file:")) {
      try {
        return Paths.get(new URI(path));
      } catch (Exception e) {
        String schemeStripped = path.substring(5);
        if (schemeStripped.startsWith("///")) {
          schemeStripped = schemeStripped.substring(3);
        }
        return Paths.get(schemeStripped);
      }
    }
    return Paths.get(path);
  }

  private static <T> CloseableIterator<T> closeableIterator(Iterator<T> iterator) {
    return new CloseableIterator<T>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return iterator.next();
      }

      @Override
      public void close() {}
    };
  }
}
