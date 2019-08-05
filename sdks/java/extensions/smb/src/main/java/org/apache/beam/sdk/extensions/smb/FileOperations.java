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
package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.display.HasDisplayData;

/**
 * Abstracts IO operations for file-based formats.
 *
 * <p>Since the SMB algorithm doesn't support {@link org.apache.beam.sdk.io.Source} splitting, I/O
 * operations must be abstracted at a per-record granularity. {@link Reader} and {@link Writer} must
 * be {@link Serializable} to be used in {@link SortedBucketSource} and {@link SortedBucketSink}
 * transforms.
 */
public abstract class FileOperations<V> implements Serializable, HasDisplayData {

  private final Compression compression;
  private final String mimeType;

  protected FileOperations(Compression compression, String mimeType) {
    this.compression = compression;
    this.mimeType = mimeType;
  }

  protected abstract Reader<V> createReader();

  // Delegate to FileIO.Sink<V> for writer logic
  protected abstract FileIO.Sink<V> createSink();

  public abstract Coder<V> getCoder();

  public final Iterator<V> iterator(ResourceId resourceId) throws IOException {
    final ReadableFile readableFile = toReadableFile(resourceId);

    final Reader<V> reader = createReader();
    reader.prepareRead(readableFile.open());
    return reader.iterator();
  }

  public Writer<V> createWriter(ResourceId resourceId) throws IOException {
    final Writer<V> writer = new Writer<>(createSink(), compression);
    writer.prepareWrite(FileSystems.create(resourceId, mimeType));
    return writer;
  }

  @Override
  public void populateDisplayData(Builder builder) {
    builder.add(DisplayData.item("FileOperations", getClass()));
    builder.add(DisplayData.item("compression", compression.toString()));
    builder.add(DisplayData.item("mimeType", mimeType));
  }

  /** Per-element file reader. */
  public abstract static class Reader<V> implements Serializable {
    public abstract void prepareRead(ReadableByteChannel channel) throws IOException;

    /**
     * Reads next record in the collection. Should return null if EOF is reached. (@Todo: should we
     * have more clearly defined behavior for EOF?)
     */
    public abstract V read() throws IOException;

    public abstract void finishRead() throws IOException;

    Iterator<V> iterator() throws IOException {
      return new Iterator<V>() {
        private V next = read();

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        public V next() {
          if (next == null) {
            throw new NoSuchElementException();
          }
          V result = next;
          try {
            next = read();
            if (next == null) {
              finishRead();
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return result;
        }
      };
    }
  }

  /** Per-element file writer. */
  public static class Writer<V> implements Serializable, AutoCloseable {

    private final FileIO.Sink<V> sink;
    private transient WritableByteChannel channel;
    private Compression compression;

    Writer(FileIO.Sink<V> sink, Compression compression) {
      this.sink = sink;
      this.compression = compression;
    }

    private void prepareWrite(WritableByteChannel channel) throws IOException {
      this.channel = compression.writeCompressed(channel);
      sink.open(this.channel);
    }

    public void write(V value) throws IOException {
      sink.write(value);
    }

    @Override
    public void close() throws IOException {
      try {
        sink.flush();
      } catch (IOException e) {
        // always close channel
        channel.close();
        throw e;
      }
      channel.close();
    }
  }

  private ReadableFile toReadableFile(ResourceId resourceId) {
    try {
      final Metadata metadata = FileSystems.matchSingleFileSpec(resourceId.toString());

      return new ReadableFile(
          metadata,
          compression == Compression.AUTO
              ? Compression.detect(resourceId.getFilename())
              : compression);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Exception fetching metadata for %s", resourceId), e);
    }
  }
}
