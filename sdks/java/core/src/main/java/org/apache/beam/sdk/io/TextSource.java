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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Implementation detail of {@link TextIO.Read}.
 *
 * <p>A {@link FileBasedSource} which can decode records delimited by newline characters.
 *
 * <p>This source splits the data into records using {@code UTF-8} {@code \n}, {@code \r}, or
 * {@code \r\n} as the delimiter. This source is not strict and supports decoding the last record
 * even if it is not delimited. Finally, no records are decoded if the stream is empty.
 *
 * <p>This source supports reading from any arbitrary byte position within the stream. If the
 * starting position is not {@code 0}, then bytes are skipped until the first delimiter is found
 * representing the beginning of the first record to be decoded.
 */
@VisibleForTesting
class TextSource extends FileBasedSource<String> {
  TextSource(ValueProvider<String> fileSpec) {
    super(fileSpec, 1L);
  }

  private TextSource(MatchResult.Metadata metadata, long start, long end) {
    super(metadata, 1L, start, end);
  }

  @Override
  protected FileBasedSource<String> createForSubrangeOfFile(
      MatchResult.Metadata metadata,
      long start,
      long end) {
    return new TextSource(metadata, start, end);
  }

  @Override
  protected FileBasedReader<String> createSingleFileReader(PipelineOptions options) {
    return new TextBasedReader(this);
  }

  @Override
  public Coder<String> getOutputCoder() {
    return StringUtf8Coder.of();
  }

  /**
   * A {@link FileBasedReader FileBasedReader}
   * which can decode records delimited by newline characters.
   *
   * <p>See {@link TextSource} for further details.
   */
  @VisibleForTesting
  static class TextBasedReader extends FileBasedReader<String> {
    private static final int READ_BUFFER_SIZE = 8192;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
    private ByteString buffer;
    private int startOfSeparatorInBuffer;
    private int endOfSeparatorInBuffer;
    private long startOfRecord;
    private volatile long startOfNextRecord;
    private volatile boolean eof;
    private volatile boolean elementIsPresent;
    private String currentValue;
    private ReadableByteChannel inChannel;

    private TextBasedReader(TextSource source) {
      super(source);
      buffer = ByteString.EMPTY;
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
      // first separator.
      if (getCurrentSource().getStartOffset() > 0) {
        checkState(channel instanceof SeekableByteChannel,
            "%s only supports reading from a SeekableByteChannel when given a start offset"
            + " greater than 0.", TextSource.class.getSimpleName());
        long requiredPosition = getCurrentSource().getStartOffset() - 1;
        ((SeekableByteChannel) channel).position(requiredPosition);
        findSeparatorBounds();
        buffer = buffer.substring(endOfSeparatorInBuffer);
        startOfNextRecord = requiredPosition + endOfSeparatorInBuffer;
        endOfSeparatorInBuffer = 0;
        startOfSeparatorInBuffer = 0;
      }
    }

    /**
     * Locates the start position and end position of the next delimiter. Will
     * consume the channel till either EOF or the delimiter bounds are found.
     *
     * <p>This fills the buffer and updates the positions as follows:
     * <pre>{@code
     * ------------------------------------------------------
     * | element bytes | delimiter bytes | unconsumed bytes |
     * ------------------------------------------------------
     * 0            start of          end of              buffer
     *              separator         separator           size
     *              in buffer         in buffer
     * }</pre>
     */
    private void findSeparatorBounds() throws IOException {
      int bytePositionInBuffer = 0;
      while (true) {
        if (!tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 1)) {
          startOfSeparatorInBuffer = endOfSeparatorInBuffer = bytePositionInBuffer;
          break;
        }

        byte currentByte = buffer.byteAt(bytePositionInBuffer);

        if (currentByte == '\n') {
          startOfSeparatorInBuffer = bytePositionInBuffer;
          endOfSeparatorInBuffer = startOfSeparatorInBuffer + 1;
          break;
        } else if (currentByte == '\r') {
          startOfSeparatorInBuffer = bytePositionInBuffer;
          endOfSeparatorInBuffer = startOfSeparatorInBuffer + 1;

          if (tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 2)) {
            currentByte = buffer.byteAt(bytePositionInBuffer + 1);
            if (currentByte == '\n') {
              endOfSeparatorInBuffer += 1;
            }
          }
          break;
        }

        // Move to the next byte in buffer.
        bytePositionInBuffer += 1;
      }
    }

    @Override
    protected boolean readNextRecord() throws IOException {
      startOfRecord = startOfNextRecord;
      findSeparatorBounds();

      // If we have reached EOF file and consumed all of the buffer then we know
      // that there are no more records.
      if (eof && buffer.size() == 0) {
        elementIsPresent = false;
        return false;
      }

      decodeCurrentElement();
      startOfNextRecord = startOfRecord + endOfSeparatorInBuffer;
      return true;
    }

    /**
     * Decodes the current element updating the buffer to only contain the unconsumed bytes.
     *
     * <p>This invalidates the currently stored {@code startOfSeparatorInBuffer} and
     * {@code endOfSeparatorInBuffer}.
     */
    private void decodeCurrentElement() throws IOException {
      ByteString dataToDecode = buffer.substring(0, startOfSeparatorInBuffer);
      currentValue = dataToDecode.toStringUtf8();
      elementIsPresent = true;
      buffer = buffer.substring(endOfSeparatorInBuffer);
    }

    /**
     * Returns false if we were unable to ensure the minimum capacity by consuming the channel.
     */
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
