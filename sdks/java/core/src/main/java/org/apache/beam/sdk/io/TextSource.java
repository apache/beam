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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation detail of {@link TextIO.Read}.
 *
 * <p>A {@link FileBasedSource} which can decode records delimited by newline characters.
 *
 * <p>This source splits the data into records using {@code UTF-8} {@code \n}, {@code \r}, or {@code
 * \r\n} as the delimiter. This source is not strict and supports decoding the last record even if
 * it is not delimited. Finally, no records are decoded if the stream is empty.
 *
 * <p>This source supports reading from any arbitrary byte position within the stream. If the
 * starting position is not {@code 0}, then bytes are skipped until the first delimiter is found
 * representing the beginning of the first record to be decoded.
 */
@VisibleForTesting
class TextSource extends FileBasedSource<String> {
  byte[] delimiter;

  TextSource(
      ValueProvider<String> fileSpec, EmptyMatchTreatment emptyMatchTreatment, byte[] delimiter) {
    super(fileSpec, emptyMatchTreatment, 1L);
    this.delimiter = delimiter;
  }

  private TextSource(MatchResult.Metadata metadata, long start, long end, byte[] delimiter) {
    super(metadata, 1L, start, end);
    this.delimiter = delimiter;
  }

  @Override
  protected FileBasedSource<String> createForSubrangeOfFile(
      MatchResult.Metadata metadata, long start, long end) {
    return new TextSource(metadata, start, end, delimiter);
  }

  @Override
  protected FileBasedReader<String> createSingleFileReader(PipelineOptions options) {
    return new TextBasedReader(this, delimiter);
  }

  @Override
  public Coder<String> getOutputCoder() {
    return StringUtf8Coder.of();
  }

  /**
   * A {@link FileBasedReader FileBasedReader} which can decode records delimited by delimiter
   * characters.
   *
   * <p>See {@link TextSource} for further details.
   */
  @VisibleForTesting
  static class TextBasedReader extends FileBasedReader<String> {
    private static final int READ_BUFFER_SIZE = 8192;
    private static final ByteString UTF8_BOM =
        ByteString.copyFrom(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF});
    private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
    private ByteString buffer;
    private int startOfDelimiterInBuffer;
    private int endOfDelimiterInBuffer;
    private long startOfRecord;
    private volatile long startOfNextRecord;
    private volatile boolean eof;
    private volatile boolean elementIsPresent;
    private @Nullable String currentValue;
    private @Nullable ReadableByteChannel inChannel;
    private byte @Nullable [] delimiter;

    private TextBasedReader(TextSource source, byte[] delimiter) {
      super(source);
      buffer = ByteString.EMPTY;
      this.delimiter = delimiter;
    }

    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
      if (!elementIsPresent) {
        throw new NoSuchElementException();
      }
      return startOfRecord;
    }

    @Override
    public long getSplitPointsRemaining() {
      if (isStarted() && startOfNextRecord >= getCurrentSource().getEndOffset()) {
        return isDone() ? 0 : 1;
      }
      return super.getSplitPointsRemaining();
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      if (!elementIsPresent) {
        throw new NoSuchElementException();
      }
      return currentValue;
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      this.inChannel = channel;
      // If the first offset is greater than zero, we need to skip bytes until we see our
      // first delimiter.
      long startOffset = getCurrentSource().getStartOffset();
      if (startOffset > 0) {
        checkState(
            channel instanceof SeekableByteChannel,
            "%s only supports reading from a SeekableByteChannel when given a start offset"
                + " greater than 0.",
            TextSource.class.getSimpleName());
        long requiredPosition = startOffset - 1;
        if (delimiter != null && startOffset >= delimiter.length) {
          // we need to move back the offset of at worse delimiter.size to be sure to see
          // all the bytes of the delimiter in the call to findDelimiterBounds() below
          requiredPosition = startOffset - delimiter.length;
        }
        ((SeekableByteChannel) channel).position(requiredPosition);
        findDelimiterBounds();
        buffer = buffer.substring(endOfDelimiterInBuffer);
        startOfNextRecord = requiredPosition + endOfDelimiterInBuffer;
        endOfDelimiterInBuffer = 0;
        startOfDelimiterInBuffer = 0;
      }
    }

    /**
     * Locates the start position and end position of the next delimiter. Will consume the channel
     * till either EOF or the delimiter bounds are found.
     *
     * <p>This fills the buffer and updates the positions as follows:
     *
     * <pre>{@code
     * ------------------------------------------------------
     * | element bytes | delimiter bytes | unconsumed bytes |
     * ------------------------------------------------------
     * 0            start of          end of              buffer
     *              delimiter         delimiter           size
     *              in buffer         in buffer
     * }</pre>
     */
    private void findDelimiterBounds() throws IOException {
      int bytePositionInBuffer = 0;
      while (true) {
        if (!tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 1)) {
          startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
          break;
        }

        byte currentByte = buffer.byteAt(bytePositionInBuffer);

        if (delimiter == null) {
          // default delimiter
          if (currentByte == '\n') {
            startOfDelimiterInBuffer = bytePositionInBuffer;
            endOfDelimiterInBuffer = startOfDelimiterInBuffer + 1;
            break;
          } else if (currentByte == '\r') {
            startOfDelimiterInBuffer = bytePositionInBuffer;
            endOfDelimiterInBuffer = startOfDelimiterInBuffer + 1;

            if (tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 2)) {
              currentByte = buffer.byteAt(bytePositionInBuffer + 1);
              if (currentByte == '\n') {
                endOfDelimiterInBuffer += 1;
              }
            }
            break;
          }
        } else {
          // user defined delimiter
          int i = 0;
          // initialize delimiter not found
          startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
          while ((i <= delimiter.length - 1) && (currentByte == delimiter[i])) {
            // read next byte
            i++;
            if (tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + i + 1)) {
              currentByte = buffer.byteAt(bytePositionInBuffer + i);
            } else {
              // corner case: delimiter truncated at the end of the file
              startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
              break;
            }
          }
          if (i == delimiter.length) {
            // all bytes of delimiter found
            endOfDelimiterInBuffer = bytePositionInBuffer + i;
            break;
          }
        }
        // Move to the next byte in buffer.
        bytePositionInBuffer += 1;
      }
    }

    @Override
    protected boolean readNextRecord() throws IOException {
      startOfRecord = startOfNextRecord;
      findDelimiterBounds();

      // If we have reached EOF file and consumed all of the buffer then we know
      // that there are no more records.
      if (eof && buffer.isEmpty()) {
        elementIsPresent = false;
        return false;
      }

      decodeCurrentElement();
      startOfNextRecord = startOfRecord + endOfDelimiterInBuffer;
      return true;
    }

    /**
     * Decodes the current element updating the buffer to only contain the unconsumed bytes.
     *
     * <p>This invalidates the currently stored {@code startOfDelimiterInBuffer} and {@code
     * endOfDelimiterInBuffer}.
     */
    private void decodeCurrentElement() throws IOException {
      ByteString dataToDecode = buffer.substring(0, startOfDelimiterInBuffer);
      // If present, the UTF8 Byte Order Mark (BOM) will be removed.
      if (startOfRecord == 0 && dataToDecode.startsWith(UTF8_BOM)) {
        dataToDecode = dataToDecode.substring(UTF8_BOM.size());
      }
      currentValue = dataToDecode.toStringUtf8();
      elementIsPresent = true;
      buffer = buffer.substring(endOfDelimiterInBuffer);
    }

    /** Returns false if we were unable to ensure the minimum capacity by consuming the channel. */
    private boolean tryToEnsureNumberOfBytesInBuffer(int minCapacity) throws IOException {
      // While we aren't at EOF or haven't fulfilled the minimum buffer capacity,
      // attempt to read more bytes.
      while (buffer.size() <= minCapacity && !eof) {
        eof = inChannel.read(readBuffer) == -1;
        readBuffer.flip();
        buffer = buffer.concat(ByteString.copyFrom(readBuffer));
        readBuffer.clear();
      }
      // Return true if we were able to honor the minimum buffer capacity request
      return buffer.size() >= minCapacity;
    }
  }
}
