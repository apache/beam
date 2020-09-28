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
package org.apache.beam.sdk.io.contextualtextio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation detail of {@link ContextualTextIO.Read}.
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
class ContextualTextIOSource extends FileBasedSource<Row> {
  byte[] delimiter;

  private static final Logger LOG = LoggerFactory.getLogger(ContextualTextIOSource.class);

  // Used to Override isSplittable
  private boolean hasMultilineCSVRecords;

  @Override
  protected boolean isSplittable() throws Exception {
    if (hasMultilineCSVRecords) {
      // When Having Multiline CSV Records,
      // Splitting the file may cause a split to be within a record,
      // Disabling split prevents this from happening
      return false;
    }
    return super.isSplittable();
  }

  ContextualTextIOSource(
      ValueProvider<String> fileSpec,
      EmptyMatchTreatment emptyMatchTreatment,
      byte[] delimiter,
      boolean hasMultilineCSVRecords) {
    super(fileSpec, emptyMatchTreatment, 1L);
    this.delimiter = delimiter;
    this.hasMultilineCSVRecords = hasMultilineCSVRecords;
  }

  private ContextualTextIOSource(
      MatchResult.Metadata metadata,
      long start,
      long end,
      byte[] delimiter,
      boolean hasMultilineCSVRecords) {
    super(metadata, 1L, start, end);
    this.delimiter = delimiter;
    this.hasMultilineCSVRecords = hasMultilineCSVRecords;
  }

  @Override
  protected FileBasedSource<Row> createForSubrangeOfFile(
      MatchResult.Metadata metadata, long start, long end) {
    return new ContextualTextIOSource(metadata, start, end, delimiter, hasMultilineCSVRecords);
  }

  @Override
  protected FileBasedReader<Row> createSingleFileReader(PipelineOptions options) {
    return new MultiLineTextBasedReader(this, delimiter, hasMultilineCSVRecords);
  }

  @Override
  public Coder<Row> getOutputCoder() {
    return SchemaCoder.of(RecordWithMetadata.getSchema());
  }

  /**
   * A {@link FileBasedReader FileBasedReader} which can decode records delimited by delimiter
   * characters.
   *
   * <p>See {@link ContextualTextIOSource} for further details.
   */
  @VisibleForTesting
  static class MultiLineTextBasedReader extends FileBasedReader<Row> {
    public static final int READ_BUFFER_SIZE = 8192;
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
    private @Nullable Row currentValue;
    private @Nullable ReadableByteChannel inChannel;
    private byte @Nullable [] delimiter;

    // Add to override the isSplittable
    private boolean hasMultilineCSVRecords;

    private long startingOffset;
    private long totalRecordCount;

    private MultiLineTextBasedReader(
        ContextualTextIOSource source, byte[] delimiter, boolean hasMultilineCSVRecords) {
      super(source);
      buffer = ByteString.EMPTY;
      this.delimiter = delimiter;
      this.hasMultilineCSVRecords = hasMultilineCSVRecords;
      startingOffset = getCurrentSource().getStartOffset(); // Start offset;
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
    public Row getCurrent() throws NoSuchElementException {
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
        Preconditions.checkState(
            channel instanceof SeekableByteChannel,
            "%s only supports reading from a SeekableByteChannel when given a start offset"
                + " greater than 0.",
            ContextualTextIOSource.class.getSimpleName());
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
     * <p>If {@link ContextualTextIOSource#hasMultilineCSVRecords} is set then the behaviour will
     * change from the standard read seen in {@link org.apache.beam.sdk.io.TextIO}. The assumption
     * when {@link ContextualTextIOSource#hasMultilineCSVRecords} is set is that the file is being
     * read with a single thread.
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
      boolean doubleQuoteClosed = true;

      while (true) {
        if (!tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 1)) {
          startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
          break;
        }

        byte currentByte = buffer.byteAt(bytePositionInBuffer);
        if (hasMultilineCSVRecords) {
          // Check if we are inside an open Quote
          if (currentByte == '"') {
            doubleQuoteClosed = !doubleQuoteClosed;
          }
        } else {
          doubleQuoteClosed = true;
        }

        if (delimiter == null) {
          // default delimiter
          if (currentByte == '\n') {
            startOfDelimiterInBuffer = bytePositionInBuffer;
            endOfDelimiterInBuffer = startOfDelimiterInBuffer + 1;
            if (doubleQuoteClosed) {
              break;
            }
          } else if (currentByte == '\r') {
            startOfDelimiterInBuffer = bytePositionInBuffer;
            endOfDelimiterInBuffer = startOfDelimiterInBuffer + 1;
            if (tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 2)) {
              currentByte = buffer.byteAt(bytePositionInBuffer + 1);
              if (currentByte == '\n') {
                endOfDelimiterInBuffer += 1;
              }
            }
            if (doubleQuoteClosed) {
              break;
            }
          }
        } else {
          // when the user defines a delimiter
          int i = 0;
          startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
          while ((i < delimiter.length) && (currentByte == delimiter[i])) {
            // read next byte;
            i++;
            if (tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + i + 1)) {
              currentByte = buffer.byteAt(bytePositionInBuffer + i);
            } else {
              // corner case: delimiter truncate at the end of file
              startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
              break;
            }
          }
          if (i == delimiter.length) {
            endOfDelimiterInBuffer = bytePositionInBuffer + i;
            if (doubleQuoteClosed) {
              break;
            }
          }
        }
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

      // The line num is:
      long recordUniqueNum = totalRecordCount++;

      // The single filename can be found as:
      // fileName.substring(fileName.lastIndexOf('/') + 1);

      currentValue =
          Row.withSchema(RecordWithMetadata.getSchema())
              .withFieldValue(RecordWithMetadata.RECORD_NUM_IN_OFFSET, recordUniqueNum)
              .withFieldValue(RecordWithMetadata.RANGE_OFFSET, startingOffset)
              .withFieldValue(RecordWithMetadata.RECORD_OFFSET, startOfRecord)
              .withFieldValue(
                  RecordWithMetadata.RESOURCE_ID,
                  getCurrentSource().getSingleFileMetadata().resourceId())
              .withFieldValue(RecordWithMetadata.VALUE, dataToDecode.toStringUtf8())
              .build();

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
