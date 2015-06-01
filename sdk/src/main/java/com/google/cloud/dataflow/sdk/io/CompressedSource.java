/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.FileBasedSource.FileBasedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.common.base.Preconditions;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.NoSuchElementException;

/**
 * A Source that reads from compressed files. A {@code CompressedSources} wraps a delegate
 * {@link FileBasedSource} that is able to read the decompressed file format.
 *
 * <p>For example, use the following to read from a gzip-compressed XML file:
 *
 * {@code
 * XmlSource mySource = XmlSource.from(...);
 * PCollection<T> collection = p.apply(CompressedSource.readFromSource(mySource,
 * CompressedSource.CompressionMode.GZIP);}
 *
 * Or, alternatively:
 * XmlSource mySource = XmlSource.from(...);
 * {@code PCollection<T> collection = p.apply(Read.from(CompressedSource.from(mySource,
 * CompressedSource.CompressionMode.GZIP)));}
 *
 * <p>Default compression modes are {@link CompressionMode#GZIP} and {@link CompressionMode#BZIP2}.
 * User-defined compression types are supported by implementing {@link DecompressingChannelFactory}.
 *
 * @param <T> The type to read from the compressed file.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class CompressedSource<T> extends FileBasedSource<T> {
  private static final long serialVersionUID = 0;

  /**
   * Factory interface for creating channels that decompress the content of an underlying channel.
   *
   * <p>TODO: Refactor decompressing channel/stream creation and default instances to util classes.
   */
  public static interface DecompressingChannelFactory extends Serializable {
    /**
     * Given a channel, create a channel that decompresses the content read from the channel.
     * @throws IOException
     */
    public ReadableByteChannel createDecompressingChannel(ReadableByteChannel channel)
        throws IOException;
  }

  /**
   * Default compression types supported by the {@code CompressedSource}.
   */
  public enum CompressionMode implements DecompressingChannelFactory {
    GZIP {
      @Override
      public ReadableByteChannel createDecompressingChannel(ReadableByteChannel channel)
          throws IOException {
        return Channels.newChannel(new GzipCompressorInputStream(Channels.newInputStream(channel)));
      }
    },
    BZIP2 {
      @Override
      public ReadableByteChannel createDecompressingChannel(ReadableByteChannel channel)
          throws IOException {
        return Channels.newChannel(
            new BZip2CompressorInputStream(Channels.newInputStream(channel)));
      }
    };

    @Override
    public abstract ReadableByteChannel createDecompressingChannel(ReadableByteChannel channel)
        throws IOException;
  }

  private final FileBasedSource<T> sourceDelegate;
  private final DecompressingChannelFactory channelFactory;

  /**
   * Creates a {@link Read} transform that reads from a {@code CompressedSource} that reads from an
   * underlying {@link FileBasedSource} after decompressing it with a {@link
   * DecompressingChannelFactory}.
   */
  public static <T> Read.Bound<T> readFromSource(
      FileBasedSource<T> sourceDelegate, DecompressingChannelFactory channelFactory) {
    return Read.from(new CompressedSource<>(sourceDelegate, channelFactory));
  }

  /**
   * Creates a {@code CompressedSource} from an underlying {@code FileBasedSource} that must be
   * further configured with {@link CompressedSource#withDecompression}.
   */
  public static <T> CompressedSource<T> from(FileBasedSource<T> sourceDelegate) {
    return new CompressedSource<>(sourceDelegate, null);
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
    super(sourceDelegate.getFileOrPatternSpec(), Long.MAX_VALUE);
    this.sourceDelegate = sourceDelegate;
    this.channelFactory = channelFactory;
  }

  /**
   * Creates a {@code CompressedSource} for an individual file. Used by {@link
   * CompressedSource#createForSubrangeOfFile}.
   */
  private CompressedSource(FileBasedSource<T> sourceDelegate,
      DecompressingChannelFactory channelFactory, String filePatternOrSpec, long minBundleSize,
      long startOffset, long endOffset) {
    super(filePatternOrSpec, minBundleSize, startOffset, endOffset);
    Preconditions.checkArgument(
        startOffset == 0,
        "CompressedSources must start reading at offset 0. Requested offset: " + startOffset);
    this.sourceDelegate = sourceDelegate;
    this.channelFactory = channelFactory;
  }

  /**
   * Validates that the delegate source is a valid source and that the channel factory is not null.
   */
  @Override
  public void validate() {
    super.validate();
    Preconditions.checkNotNull(sourceDelegate);
    sourceDelegate.validate();
    Preconditions.checkNotNull(channelFactory);
  }

  /**
   * Creates a {@code CompressedSource} for a subrange of a file. Called by superclass to create a
   * source for a single file.
   */
  @Override
  public CompressedSource<T> createForSubrangeOfFile(String fileName, long start, long end) {
    return new CompressedSource<>(sourceDelegate.createForSubrangeOfFile(fileName, start, end),
        channelFactory, fileName, Long.MAX_VALUE, start, end);
  }

  /**
   * Determines whether a single file represented by this source is splittable. Returns false:
   * compressed sources are not splittable.
   */
  @Override
  protected final boolean isSplittable() throws Exception {
    return false;
  }

  /**
   * Creates a {@code CompressedReader} to read a single file.
   *
   * <p>Uses the delegate source to create a single file reader for the delegate source.
   */
  @Override
  public final CompressedReader<T> createSingleFileReader(
      PipelineOptions options, ExecutionContext executionContext) {
    return new CompressedReader<T>(
        this, sourceDelegate.createSingleFileReader(options, executionContext));
  }

  /**
   * Returns whether the delegate source produces sorted keys.
   */
  @Override
  public final boolean producesSortedKeys(PipelineOptions options) throws Exception {
    return sourceDelegate.producesSortedKeys(options);
  }

  /**
   * Returns the delegate source's default output coder.
   */
  @Override
  public final Coder<T> getDefaultOutputCoder() {
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
    static final long serialVersionUID = 0;

    private final FileBasedReader<T> readerDelegate;
    private final CompressedSource<T> source;

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

    /**
     * Returns false; compressed sources cannot be split.
     */
    @Override
    protected final boolean isAtSplitPoint() {
      return false;
    }

    /**
     * Creates a decompressing channel from the input channel and passes it to its delegate reader's
     * {@link FileBasedReader#startReading(ReadableByteChannel)}.
     */
    @Override
    protected final void startReading(ReadableByteChannel channel) throws IOException {
      readerDelegate.startReading(source.getChannelFactory().createDecompressingChannel(channel));
    }

    /**
     * Reads the next record via the delegate reader.
     */
    @Override
    protected final boolean readNextRecord() throws IOException {
      return readerDelegate.readNextRecord();
    }

    /**
     * Returns the delegate reader's current offset in the decompressed input.
     */
    @Override
    protected final long getCurrentOffset() {
      return readerDelegate.getCurrentOffset();
    }
  }
}
