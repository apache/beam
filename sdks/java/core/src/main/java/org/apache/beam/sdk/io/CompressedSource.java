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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.joda.time.Instant;

/**
 * A Source that reads from compressed files. A {@code CompressedSources} wraps a delegate
 * {@link FileBasedSource} that is able to read the decompressed file format.
 *
 * <p>For example, use the following to read from a gzip-compressed file-based source:
 *
 * <pre> {@code
 * FileBasedSource<T> mySource = ...;
 * PCollection<T> collection = p.apply(Read.from(CompressedSource
 *     .from(mySource)
 *     .withDecompression(CompressedSource.CompressionMode.GZIP)));
 * } </pre>
 *
 * <p>Supported compression algorithms are {@link CompressionMode#GZIP},
 * {@link CompressionMode#BZIP2}, {@link CompressionMode#ZIP} and {@link CompressionMode#DEFLATE}.
 * User-defined compression types are supported by implementing
 * {@link DecompressingChannelFactory}.
 *
 * <p>By default, the compression algorithm is selected from those supported in
 * {@link CompressionMode} based on the file name provided to the source, namely
 * {@code ".bz2"} indicates {@link CompressionMode#BZIP2}, {@code ".gz"} indicates
 * {@link CompressionMode#GZIP}, {@code ".zip"} indicates {@link CompressionMode#ZIP} and
 * {@code ".deflate"} indicates {@link CompressionMode#DEFLATE}. If the file name does not match
 * any of the supported
 * algorithms, it is assumed to be uncompressed data.
 *
 * @param <T> The type to read from the compressed file.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class CompressedSource<T> extends FileBasedSource<T> {
  /**
   * Factory interface for creating channels that decompress the content of an underlying channel.
   */
  public interface DecompressingChannelFactory extends Serializable {
    /**
     * Given a channel, create a channel that decompresses the content read from the channel.
     */
    ReadableByteChannel createDecompressingChannel(ReadableByteChannel channel)
        throws IOException;
  }

  /**
   * Factory interface for creating channels that decompress the content of an underlying channel,
   * based on both the channel and the file name.
   */
  private interface FileNameBasedDecompressingChannelFactory
      extends DecompressingChannelFactory {
    /**
     * Given a channel, create a channel that decompresses the content read from the channel.
     */
    ReadableByteChannel createDecompressingChannel(String fileName, ReadableByteChannel channel)
        throws IOException;
  }

  /**
   * Default compression types supported by the {@code CompressedSource}.
   */
  public enum CompressionMode implements DecompressingChannelFactory {
    /**
     * Reads a byte channel assuming it is compressed with gzip.
     */
    GZIP {
      @Override
      public boolean matches(String fileName) {
          return fileName.toLowerCase().endsWith(".gz");
      }

      @Override
      public ReadableByteChannel createDecompressingChannel(ReadableByteChannel channel)
          throws IOException {
        // Determine if the input stream is gzipped. The input stream returned from the
        // GCS connector may already be decompressed; GCS does this based on the
        // content-encoding property.
        PushbackInputStream stream = new PushbackInputStream(Channels.newInputStream(channel), 2);
        byte[] headerBytes = new byte[2];
        int bytesRead = ByteStreams.read(
            stream /* source */, headerBytes /* dest */, 0 /* offset */, 2 /* len */);
        stream.unread(headerBytes, 0, bytesRead);
        if (bytesRead >= 2) {
          byte zero = 0x00;
          int header = Ints.fromBytes(zero, zero, headerBytes[1], headerBytes[0]);
          if (header == GZIPInputStream.GZIP_MAGIC) {
            return Channels.newChannel(new GzipCompressorInputStream(stream, true));
          }
        }
        return Channels.newChannel(stream);
      }
    },

    /**
     * Reads a byte channel assuming it is compressed with bzip2.
     */
    BZIP2 {
      @Override
      public boolean matches(String fileName) {
          return fileName.toLowerCase().endsWith(".bz2");
      }

      @Override
      public ReadableByteChannel createDecompressingChannel(ReadableByteChannel channel)
          throws IOException {
        return Channels.newChannel(
            new BZip2CompressorInputStream(Channels.newInputStream(channel)));
      }
    },

    /**
     * Reads a byte channel assuming it is compressed with zip.
     * If the zip file contains multiple entries, files in the zip are concatenated all together.
     */
    ZIP {
      @Override
      public boolean matches(String fileName) {
        return fileName.toLowerCase().endsWith(".zip");
      }

      public ReadableByteChannel createDecompressingChannel(ReadableByteChannel channel)
        throws IOException {
        FullZipInputStream zip = new FullZipInputStream(Channels.newInputStream(channel));
        return Channels.newChannel(zip);
      }
    },

    /**
     * Reads a byte channel assuming it is compressed with deflate.
     */
    DEFLATE {
      @Override
      public boolean matches(String fileName) {
        return fileName.toLowerCase().endsWith(".deflate");
      }

      public ReadableByteChannel createDecompressingChannel(ReadableByteChannel channel)
          throws IOException {
        return Channels.newChannel(
            new DeflateCompressorInputStream(Channels.newInputStream(channel)));
      }
    };

    /**
     * Extend of {@link ZipInputStream} to automatically read all entries in the zip.
     */
    private static class FullZipInputStream extends InputStream {

      private ZipInputStream zipInputStream;
      private ZipEntry currentEntry;

      public FullZipInputStream(InputStream is) throws IOException {
        super();
        zipInputStream = new ZipInputStream(is);
        currentEntry = zipInputStream.getNextEntry();
      }

      @Override
      public int read() throws IOException {
        int result = zipInputStream.read();
        while (result == -1) {
          currentEntry = zipInputStream.getNextEntry();
          if (currentEntry == null) {
            return -1;
          } else {
            result = zipInputStream.read();
          }
        }
        return result;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        int result = zipInputStream.read(b, off, len);
        while (result == -1) {
          currentEntry = zipInputStream.getNextEntry();
          if (currentEntry == null) {
            return -1;
          } else {
            result = zipInputStream.read(b, off, len);
          }
        }
        return result;
      }

    }

    /**
     * Returns {@code true} if the given file name implies that the contents are compressed
     * according to the compression embodied by this factory.
     */
    public abstract boolean matches(String fileName);

    @Override
    public abstract ReadableByteChannel createDecompressingChannel(ReadableByteChannel channel)
        throws IOException;

    /** Returns whether the file's extension matches of one of the known compression formats. */
    public static boolean isCompressed(String filename) {
      for (CompressionMode type : CompressionMode.values()) {
        if  (type.matches(filename)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Reads a byte channel detecting compression according to the file name. If the filename
   * is not any other known {@link CompressionMode}, it is presumed to be uncompressed.
   */
  private static class DecompressAccordingToFilename
      implements FileNameBasedDecompressingChannelFactory {

    @Override
    public ReadableByteChannel createDecompressingChannel(
        String fileName, ReadableByteChannel channel) throws IOException {
      for (CompressionMode type : CompressionMode.values()) {
        if (type.matches(fileName)) {
          return type.createDecompressingChannel(channel);
        }
      }
      // Uncompressed
      return channel;
    }

    @Override
    public ReadableByteChannel createDecompressingChannel(ReadableByteChannel channel) {
      throw new UnsupportedOperationException(
          String.format("%s does not support createDecompressingChannel(%s) but only"
              + " createDecompressingChannel(%s,%s)",
              getClass().getSimpleName(),
              String.class.getSimpleName(),
              ReadableByteChannel.class.getSimpleName(),
              ReadableByteChannel.class.getSimpleName()));
    }
  }

  private final FileBasedSource<T> sourceDelegate;
  private final DecompressingChannelFactory channelFactory;

  /**
   * Creates a {@code CompressedSource} from an underlying {@code FileBasedSource}. The type
   * of compression used will be based on the file name extension unless explicitly
   * configured via {@link CompressedSource#withDecompression}.
   */
  public static <T> CompressedSource<T> from(FileBasedSource<T> sourceDelegate) {
    return new CompressedSource<>(sourceDelegate, new DecompressAccordingToFilename());
  }

  /**
   * Return a {@code CompressedSource} that is like this one but will decompress its underlying file
   * with the given {@link DecompressingChannelFactory}.
   */
  public CompressedSource<T> withDecompression(DecompressingChannelFactory channelFactory) {
    return new CompressedSource<>(this.sourceDelegate, channelFactory);
  }

  /**
   * Creates a {@code CompressedSource} from a delegate file based source and a decompressing
   * channel factory.
   */
  private CompressedSource(
      FileBasedSource<T> sourceDelegate, DecompressingChannelFactory channelFactory) {
    super(sourceDelegate.getFileOrPatternSpecProvider(), Long.MAX_VALUE);
    this.sourceDelegate = sourceDelegate;
    this.channelFactory = channelFactory;
  }

  /**
   * Creates a {@code CompressedSource} for an individual file. Used by {@link
   * CompressedSource#createForSubrangeOfFile}.
   */
  private CompressedSource(FileBasedSource<T> sourceDelegate,
      DecompressingChannelFactory channelFactory, Metadata metadata, long minBundleSize,
      long startOffset, long endOffset) {
    super(metadata, minBundleSize, startOffset, endOffset);
    this.sourceDelegate = sourceDelegate;
    this.channelFactory = channelFactory;
    boolean splittable;
    try {
      splittable = isSplittable();
    } catch (Exception e) {
      throw new RuntimeException("Failed to determine if the source is splittable", e);
    }
    checkArgument(
        splittable || startOffset == 0,
        "CompressedSources must start reading at offset 0. Requested offset: %s",
        startOffset);
  }

  /**
   * Validates that the delegate source is a valid source and that the channel factory is not null.
   */
  @Override
  public void validate() {
    super.validate();
    checkNotNull(sourceDelegate);
    sourceDelegate.validate();
    checkNotNull(channelFactory);
  }

  /**
   * Creates a {@code CompressedSource} for a subrange of a file. Called by superclass to create a
   * source for a single file.
   */
  @Override
  protected FileBasedSource<T> createForSubrangeOfFile(Metadata metadata, long start, long end) {
    return new CompressedSource<>(sourceDelegate.createForSubrangeOfFile(metadata, start, end),
        channelFactory, metadata, sourceDelegate.getMinBundleSize(), start, end);
  }

  /**
   * Determines whether a single file represented by this source is splittable. Returns true
   * if we are using the default decompression factory and and it determines
   * from the requested file name that the file is not compressed.
   */
  @Override
  protected final boolean isSplittable() throws Exception {
    return channelFactory instanceof FileNameBasedDecompressingChannelFactory
        && !CompressionMode.isCompressed(getFileOrPatternSpec())
        && sourceDelegate.isSplittable();
  }

  /**
   * Creates a {@code FileBasedReader} to read a single file.
   *
   * <p>Uses the delegate source to create a single file reader for the delegate source.
   * Utilizes the default decompression channel factory to not wrap the source reader
   * if the file name does not represent a compressed file allowing for splitting of
   * the source.
   */
  @Override
  protected final FileBasedReader<T> createSingleFileReader(PipelineOptions options) {
    if (channelFactory instanceof FileNameBasedDecompressingChannelFactory) {
      if (!CompressionMode.isCompressed(getFileOrPatternSpec())) {
        return sourceDelegate.createSingleFileReader(options);
      }
    }
    return new CompressedReader<T>(
        this, sourceDelegate.createSingleFileReader(options));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    // We explicitly do not register base-class data, instead we use the delegate inner source.
    builder
        .include("source", sourceDelegate)
        .add(DisplayData.item("source", sourceDelegate.getClass())
          .withLabel("Read Source"));

    if (channelFactory instanceof Enum) {
      // GZIP, BZIP, ZIP and DEFLATE are implemented as enums; Enum classes are anonymous, so use
      // the .name() value instead
      builder.add(DisplayData.item("compressionMode", ((Enum) channelFactory).name())
        .withLabel("Compression Mode"));
    } else {
      builder.add(DisplayData.item("compressionMode", channelFactory.getClass())
        .withLabel("Compression Mode"));
    }
  }

  /**
   * Returns the delegate source's default output coder.
   */
  @Override
  public final Coder<T> getDefaultOutputCoder() throws CannotProvideCoderException {
    return sourceDelegate.getDefaultOutputCoder();
  }

  public final DecompressingChannelFactory getChannelFactory() {
    return channelFactory;
  }

  /**
   * Reader for a {@link CompressedSource}. Decompresses its input and uses a delegate
   * reader to read elements from the decompressed input.
   * @param <T> The type of records read from the source.
   */
  public static class CompressedReader<T> extends FileBasedReader<T> {

    private final FileBasedReader<T> readerDelegate;
    private final CompressedSource<T> source;
    private final Object progressLock = new Object();
    @GuardedBy("progressLock")
    private int numRecordsRead;
    @GuardedBy("progressLock")
    private CountingChannel channel;

    /**
     * Create a {@code CompressedReader} from a {@code CompressedSource} and delegate reader.
     */
    public CompressedReader(CompressedSource<T> source, FileBasedReader<T> readerDelegate) {
      super(source);
      this.source = source;
      this.readerDelegate = readerDelegate;
    }

    /**
     * Gets the current record from the delegate reader.
     */
    @Override
    public T getCurrent() throws NoSuchElementException {
      return readerDelegate.getCurrent();
    }

    @Override
    public boolean allowsDynamicSplitting() {
      return false;
    }

    @Override
    public final long getSplitPointsConsumed() {
      synchronized (progressLock) {
        return (isDone() && numRecordsRead > 0) ? 1 : 0;
      }
    }

    @Override
    public final long getSplitPointsRemaining() {
      return isDone() ? 0 : 1;
    }

    /**
     * Returns true only for the first record; compressed sources cannot be split.
     */
    @Override
    protected final boolean isAtSplitPoint() {
      // We have to return true for the first record, but not for the state before reading it,
      // and not for the state after reading any other record. Hence == rather than >= or <=.
      // This is required because FileBasedReader is intended for readers that can read a range
      // of offsets in a file and where the range can be split in parts. CompressedReader,
      // however, is a degenerate case because it cannot be split, but it has to satisfy the
      // semantics of offsets and split points anyway.
      synchronized (progressLock) {
        return numRecordsRead == 1;
      }
    }

    private static class CountingChannel implements ReadableByteChannel {
      long count;
      private final ReadableByteChannel inner;

      public CountingChannel(ReadableByteChannel inner, long count) {
        this.inner = inner;
        this.count = count;
      }

      public long getCount() {
        return count;
      }

      @Override
      public int read(ByteBuffer dst) throws IOException {
        int bytes = inner.read(dst);
        if (bytes > 0) {
          // Avoid the -1 from EOF.
          count += bytes;
        }
        return bytes;
      }

      @Override
      public boolean isOpen() {
        return inner.isOpen();
      }

      @Override
      public void close() throws IOException {
        inner.close();
      }
    }

    /**
     * Creates a decompressing channel from the input channel and passes it to its delegate reader's
     * {@link FileBasedReader#startReading(ReadableByteChannel)}.
     */
    @Override
    protected final void startReading(ReadableByteChannel channel) throws IOException {
      synchronized (progressLock) {
        this.channel = new CountingChannel(channel, getCurrentSource().getStartOffset());
        channel = this.channel;
      }

      if (source.getChannelFactory() instanceof FileNameBasedDecompressingChannelFactory) {
        FileNameBasedDecompressingChannelFactory channelFactory =
            (FileNameBasedDecompressingChannelFactory) source.getChannelFactory();
        readerDelegate.startReading(channelFactory.createDecompressingChannel(
            getCurrentSource().getFileOrPatternSpec(),
            channel));
      } else {
        readerDelegate.startReading(source.getChannelFactory().createDecompressingChannel(
            channel));
      }
    }

    /**
     * Reads the next record via the delegate reader.
     */
    @Override
    protected final boolean readNextRecord() throws IOException {
      if (!readerDelegate.readNextRecord()) {
        return false;
      }
      synchronized (progressLock) {
        ++numRecordsRead;
      }
      return true;
    }

    // Unsplittable: returns the offset in the input stream that has been read by the input.
    // these positions are likely to be coarse-grained (in the event of buffering) and
    // over-estimates (because they reflect the number of bytes read to produce an element, not its
    // start) but both of these provide better data than e.g., reporting the start of the file.
    @Override
    protected final long getCurrentOffset() throws NoSuchElementException {
      synchronized (progressLock) {
        if (numRecordsRead <= 1) {
          // Since the first record is at a split point, it should start at the beginning of the
          // file. This avoids the bad case where the decompressor read the entire file, which
          // would cause the file to be treated as empty when returning channel.getCount() as it
          // is outside the valid range.
          return 0;
        }
        return channel.getCount();
      }
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return readerDelegate.getCurrentTimestamp();
    }
  }
}
