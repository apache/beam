/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.MimeTypes;
import com.google.cloud.dataflow.sdk.util.ShardingWritableByteChannel;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Random;

import javax.annotation.Nullable;

/**
 * A sink that writes text files.
 *
 * @param <T> the type of the elements written to the sink
 */
public class TextSink<T> extends Sink<T> {

  static final byte[] NEWLINE = getNewline();

  private static byte[] getNewline() {
    String newline = "\n";
    try {
      return newline.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("UTF-8 not supported", e);
    }
  }

  final String namePrefix;
  final String shardFormat;
  final String nameSuffix;
  final int shardCount;
  final boolean appendTrailingNewlines;
  final String header;
  final String footer;
  final Coder<T> coder;

  /**
   * For testing only.
   *
   * <p> Used by simple tests which write to a single unsharded file.
   */
  public static <V> TextSink<WindowedValue<V>> createForTest(
      String filename,
      boolean appendTrailingNewlines,
      @Nullable String header,
      @Nullable String footer,
      Coder<V> coder) {
    return create(filename,
                  "",
                  "",
                  1,
                  appendTrailingNewlines,
                  header,
                  footer,
                  WindowedValue.getValueOnlyCoder(coder));
  }

  /**
   * For DirectPipelineRunner only.
   * It wraps the coder with {@code WindowedValue.ValueOnlyCoder}.
   */
  public static <V> TextSink<WindowedValue<V>> createForDirectPipelineRunner(
      String filenamePrefix,
      String shardFormat,
      String filenameSuffix,
      int shardCount,
      boolean appendTrailingNewlines,
      @Nullable String header,
      @Nullable String footer,
      Coder<V> coder) {
    return create(filenamePrefix,
                  shardFormat,
                  filenameSuffix,
                  shardCount,
                  appendTrailingNewlines,
                  header,
                  footer,
                  WindowedValue.getValueOnlyCoder(coder));
  }

  /**
   * Constructs a new TextSink.
   *
   * @param filenamePrefix the prefix of output filenames.
   * @param shardFormat the shard name template to use for output filenames.
   * @param filenameSuffix the suffix of output filenames.
   * @param shardCount the number of outupt shards to produce.
   * @param appendTrailingNewlines true to append newlines to each output line.
   * @param header text to place at the beginning of each output file.
   * @param footer text to place at the end of each output file.
   * @param coder the code used to encode elements for output.
   */
  public static <V> TextSink<V> create(String filenamePrefix,
                                       String shardFormat,
                                       String filenameSuffix,
                                       int shardCount,
                                       boolean appendTrailingNewlines,
                                       @Nullable String header,
                                       @Nullable String footer,
                                       Coder<V> coder) {
    return new TextSink<>(filenamePrefix,
                          shardFormat,
                          filenameSuffix,
                          shardCount,
                          appendTrailingNewlines,
                          header,
                          footer,
                          coder);
  }

  private TextSink(String filenamePrefix,
                   String shardFormat,
                   String filenameSuffix,
                   int shardCount,
                   boolean appendTrailingNewlines,
                   @Nullable String header,
                   @Nullable String footer,
                   Coder<T> coder) {
    this.namePrefix = filenamePrefix;
    this.shardFormat = shardFormat;
    this.nameSuffix = filenameSuffix;
    this.shardCount = shardCount;
    this.appendTrailingNewlines = appendTrailingNewlines;
    this.header = header;
    this.footer = footer;
    this.coder = coder;
  }

  @Override
  public SinkWriter<T> writer() throws IOException {
    String mimeType;

    if (!(coder instanceof WindowedValueCoder)) {
      throw new IOException(
          "Expected WindowedValueCoder for inputCoder, got: "
          + coder.getClass().getName());
    }
    Coder valueCoder = ((WindowedValueCoder) coder).getValueCoder();
    if (valueCoder.equals(StringUtf8Coder.of())) {
      mimeType = MimeTypes.TEXT;
    } else {
      mimeType = MimeTypes.BINARY;
    }

    WritableByteChannel writer = IOChannelUtils.create(namePrefix, shardFormat,
        nameSuffix, shardCount, mimeType);

    if (writer instanceof ShardingWritableByteChannel) {
      return new ShardingTextFileWriter((ShardingWritableByteChannel) writer);
    } else {
      return new TextFileWriter(writer);
    }
  }

  /**
   * Abstract SinkWriter base class shared by sharded and unsharded Text
   * writer implementations.
   */
  abstract class AbstractTextFileWriter implements SinkWriter<T> {
    protected void init() throws IOException {
      if (header != null) {
        printLine(ShardingWritableByteChannel.ALL_SHARDS,
            CoderUtils.encodeToByteArray(StringUtf8Coder.of(), header));
      }
    }

    /**
     * Adds a value to the sink. Returns the size in bytes of the data written.
     * The return value does -not- include header/footer size.
     */
    @Override
    public long add(T value) throws IOException {
      return printLine(getShardNum(value),
          CoderUtils.encodeToByteArray(coder, value));
    }

    @Override
    public void close() throws IOException {
      if (footer != null) {
        printLine(ShardingWritableByteChannel.ALL_SHARDS,
            CoderUtils.encodeToByteArray(StringUtf8Coder.of(), footer));
      }
    }

    protected long printLine(int shardNum, byte[] line) throws IOException {
      long length = line.length;
      write(shardNum, ByteBuffer.wrap(line));

      if (appendTrailingNewlines) {
        write(shardNum, ByteBuffer.wrap(NEWLINE));
        length += NEWLINE.length;
      }

      return length;
    }

    protected abstract void write(int shardNum, ByteBuffer buf)
        throws IOException;
    protected abstract int getShardNum(T value);
  }

  /** An unsharded SinkWriter for a TextSink. */
  class TextFileWriter extends AbstractTextFileWriter {
    private final WritableByteChannel outputChannel;

    TextFileWriter(WritableByteChannel outputChannel) throws IOException {
      this.outputChannel = outputChannel;
      init();
    }

    @Override
    public void close() throws IOException {
      super.close();
      outputChannel.close();
    }

    @Override
    protected void write(int shardNum, ByteBuffer buf) throws IOException {
      outputChannel.write(buf);
    }

    @Override
    protected int getShardNum(T value) {
      return 0;
    }
  }

  /** A sharding SinkWriter for a TextSink. */
  class ShardingTextFileWriter extends AbstractTextFileWriter {
    private final Random rng = new Random();
    private final int numShards;
    private final ShardingWritableByteChannel outputChannel;

    // TODO: add support for user-defined sharding function.
    ShardingTextFileWriter(ShardingWritableByteChannel outputChannel)
        throws IOException {
      this.outputChannel = outputChannel;
      numShards = outputChannel.getNumShards();
      init();
    }

    @Override
    public void close() throws IOException {
      super.close();
      outputChannel.close();
    }

    @Override
    protected void write(int shardNum, ByteBuffer buf) throws IOException {
      outputChannel.writeToShard(shardNum, buf);
    }

    @Override
    protected int getShardNum(T value) {
      return rng.nextInt(numShards);
    }
  }
}
