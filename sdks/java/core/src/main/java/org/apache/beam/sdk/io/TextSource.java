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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Bytes;
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
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TextSource extends FileBasedSource<String> {
  byte[] delimiter;

  int skipHeaderLines;

  public TextSource(
      ValueProvider<String> fileSpec,
      EmptyMatchTreatment emptyMatchTreatment,
      byte[] delimiter,
      int skipHeaderLines) {
    super(fileSpec, emptyMatchTreatment, 1L);
    this.delimiter = delimiter;
    this.skipHeaderLines = skipHeaderLines;
  }

  public TextSource(
      ValueProvider<String> fileSpec, EmptyMatchTreatment emptyMatchTreatment, byte[] delimiter) {
    this(fileSpec, emptyMatchTreatment, delimiter, 0);
  }

  public TextSource(
      MatchResult.Metadata metadata, long start, long end, byte[] delimiter, int skipHeaderLines) {
    super(metadata, 1L, start, end);
    this.delimiter = delimiter;
    this.skipHeaderLines = skipHeaderLines;
  }

  public TextSource(MatchResult.Metadata metadata, long start, long end, byte[] delimiter) {
    this(metadata, start, end, delimiter, 0);
  }

  @Override
  protected FileBasedSource<String> createForSubrangeOfFile(
      MatchResult.Metadata metadata, long start, long end) {
    return new TextSource(metadata, start, end, delimiter, skipHeaderLines);
  }

  @Override
  protected FileBasedReader<String> createSingleFileReader(PipelineOptions options) {
    return new TextBasedReader(this, delimiter, skipHeaderLines);
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
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    private final byte @Nullable [] delimiter;
    private final int skipHeaderLines;

    // The output stream can contain the delimiter at the last. It must exclude the delimiter when
    // converting into currentValue.
    private final SubstringByteArrayOutputStream str;

    // Buffer for text read from the underlying file.
    private final byte[] buffer;
    // A wrapper of the `buffer` field;
    private final ByteBuffer byteBuffer;

    private ReadableByteChannel inChannel;
    private long startOfRecord;
    private volatile long startOfNextRecord;
    private volatile boolean eof;
    private volatile @Nullable String currentValue;
    private int bufferLength = 0; // the number of bytes of real data in the buffer
    private int bufferPosn = 0; // the current position in the buffer
    private boolean skipLineFeedAtStart; // skip an LF if at the start of the next buffer

    // Finder for custom delimiter.
    private @Nullable DelimiterFinder delimiterFinder;

    private TextBasedReader(TextSource source, byte[] delimiter) {
      this(source, delimiter, 0);
    }

    private TextBasedReader(TextSource source, byte[] delimiter, int skipHeaderLines) {
      super(source);
      this.buffer = new byte[READ_BUFFER_SIZE];
      this.str = new SubstringByteArrayOutputStream();
      this.byteBuffer = ByteBuffer.wrap(buffer);
      this.delimiter = delimiter;
      this.skipHeaderLines = skipHeaderLines;

      if (delimiter != null) {
        delimiterFinder = new DelimiterFinder(delimiter);
      }
    }

    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
      if (currentValue == null) {
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
      if (currentValue == null) {
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

        // Handle the case where the requiredPosition is at the beginning of the file so we can
        // skip over UTF8_BOM if present.
        if (requiredPosition < UTF8_BOM.size()) {
          ((SeekableByteChannel) channel).position(0);
          if (fileStartsWithBom()) {
            startOfNextRecord = bufferPosn = UTF8_BOM.size();
          } else {
            startOfNextRecord = bufferPosn = (int) requiredPosition;
          }
          skipHeader(skipHeaderLines, true);
        } else {
          skipHeader(skipHeaderLines, false);
          if (requiredPosition > startOfNextRecord) {
            ((SeekableByteChannel) channel).position(requiredPosition);
            startOfNextRecord = requiredPosition;
            bufferLength = bufferPosn = 0;
          }
          // Read and discard the next record ensuring that startOfNextRecord and bufferPosn point
          // to the beginning of the next record.
          readNextRecord();
          currentValue = null;
        }

      } else {
        // Check to see if we start with the UTF_BOM bytes skipping them if present.
        if (fileStartsWithBom()) {
          startOfNextRecord = bufferPosn = UTF8_BOM.size();
        }
        skipHeader(skipHeaderLines, false);
      }
    }

    private void skipHeader(int headerLines, boolean skipFirstLine) throws IOException {
      if (headerLines == 1) {
        readNextRecord();
      } else if (headerLines > 1) {
        // this will be expensive
        ((SeekableByteChannel) inChannel).position(0);
        for (int line = 0; line < headerLines; ++line) {
          readNextRecord();
        }
      } else if (headerLines == 0 && skipFirstLine) {
        readNextRecord();
      }
      currentValue = null;
    }

    private boolean fileStartsWithBom() throws IOException {
      for (; ; ) {
        int bytesRead = inChannel.read(byteBuffer);
        if (bytesRead == -1) {
          return false;
        } else {
          bufferLength += bytesRead;
        }
        if (bufferLength >= UTF8_BOM.size()) {
          int i;
          for (i = 0; i < UTF8_BOM.size() && buffer[i] == UTF8_BOM.byteAt(i); ++i) {}
          if (i == UTF8_BOM.size()) {
            return true;
          }
          return false;
        }
      }
    }

    @Override
    protected boolean readNextRecord() throws IOException {
      startOfRecord = startOfNextRecord;

      // If we have reached EOF file last time around then we will mark that we don't have an
      // element and return false.
      if (eof) {
        currentValue = null;
        return false;
      }

      if (delimiter == null) {
        return readDefaultLine();
      } else {
        return readCustomLine();
      }
    }

    /**
     * Loosely based upon <a
     * href="https://github.com/hanborq/hadoop/blob/master/src/core/org/apache/hadoop/util/LineReader.java">Hadoop
     * LineReader.java</a>
     *
     * <p>We're reading data from inChannel, but the head of the stream may be already buffered in
     * buffer, so we have several cases:
     *
     * <ol>
     *   <li>No newline characters are in the buffer, so we need to copy everything and read another
     *       buffer from the stream.
     *   <li>An unambiguously terminated line is in buffer, so we just create currentValue
     *   <li>Ambiguously terminated line is in buffer, i.e. buffer ends in CR. In this case we copy
     *       everything up to CR to str, but we also need to see what follows CR: if it's LF, then
     *       we need consume LF as well, so next call to readLine will read from after that.
     * </ol>
     *
     * <p>We use a flag prevCharCR to signal if previous character was CR and, if it happens to be
     * at the end of the buffer, delay consuming it until we have a chance to look at the char that
     * follows.
     */
    private boolean readDefaultLine() throws IOException {
      assert !eof;

      int newlineLength = 0; // length of terminating newline
      boolean prevCharCR = false; // true if prev char was CR
      long bytesConsumed = 0;
      EOF:
      for (; ; ) {
        int startPosn = bufferPosn; // starting from where we left off the last time

        // Read the next chunk from the file, ensure that we read at least one byte
        // or reach EOF.
        while (bufferPosn == bufferLength) {
          startPosn = bufferPosn = 0;
          byteBuffer.clear();
          bufferLength = inChannel.read(byteBuffer);

          // If we are at EOF then try to create the last value from the buffer.
          if (bufferLength < 0) {
            eof = true;

            // Don't return an empty record if the file ends with a delimiter
            if (str.size() == 0) {
              return false;
            }

            currentValue = str.toString(StandardCharsets.UTF_8.name());
            break EOF;
          }
        }

        // Consume any LF after CR if it is the first character of the next buffer
        if (skipLineFeedAtStart && buffer[bufferPosn] == LF) {
          ++startPosn;
          ++bufferPosn;
          skipLineFeedAtStart = false;

          // Right now, startOfRecord is pointing at the position of LF, but the actual start
          // position of the new record should be the position after LF.
          ++startOfRecord;
        }

        // Search for the newline
        for (; bufferPosn < bufferLength; ++bufferPosn) {
          if (buffer[bufferPosn] == LF) {
            newlineLength = (prevCharCR) ? 2 : 1;
            ++bufferPosn; // at next invocation proceed from following byte
            break;
          }
          if (prevCharCR) { // CR + notLF, we are at notLF
            newlineLength = 1;
            break;
          }
          prevCharCR = (buffer[bufferPosn] == CR);
        }

        // CR at the end of the buffer
        if (newlineLength == 0 && prevCharCR) {
          skipLineFeedAtStart = true;
          newlineLength = 1;
        } else {
          skipLineFeedAtStart = false;
        }

        int readLength = bufferPosn - startPosn;
        bytesConsumed += readLength;
        int appendLength = readLength - newlineLength;
        if (newlineLength == 0) {
          // Append the prefix of the value to str skipping the partial delimiter
          str.write(buffer, startPosn, appendLength);
        } else {
          if (str.size() == 0) {
            // Optimize for the common case where the string is wholly contained within the buffer
            currentValue = new String(buffer, startPosn, appendLength, StandardCharsets.UTF_8);
          } else {
            str.write(buffer, startPosn, appendLength);
            currentValue = str.toString(StandardCharsets.UTF_8.name());
          }
          break;
        }
      }

      startOfNextRecord = startOfRecord + bytesConsumed;
      str.reset();
      return true;
    }

    private boolean readCustomLine() throws IOException {
      checkState(!eof);
      checkNotNull(delimiter);
      checkNotNull(
          delimiterFinder, "DelimiterFinder must not be null if custom delimiter is used.");

      long bytesConsumed = 0;
      delimiterFinder.reset();

      while (true) {
        int startPosn = bufferPosn;
        while (bufferPosn >= bufferLength) {
          startPosn = bufferPosn = 0;
          byteBuffer.clear();
          bufferLength = inChannel.read(byteBuffer);

          if (bufferLength < 0) {
            eof = true;

            if (str.size() == 0) {
              return false;
            }

            // Not ending with a delimiter.
            currentValue = str.toString(StandardCharsets.UTF_8.name());
            break;
          }
        }

        if (eof) {
          break;
        }

        boolean delimiterFound = false;
        for (; bufferPosn < bufferLength; ++bufferPosn) {
          if (delimiterFinder.feed(buffer[bufferPosn])) {
            ++bufferPosn;
            delimiterFound = true;
            break;
          }
        }

        int readLength = bufferPosn - startPosn;
        bytesConsumed += readLength;
        if (!delimiterFound) {
          str.write(buffer, startPosn, readLength);
        } else {
          if (str.size() == 0) {
            // Optimize for the common case where the string is wholly contained within the buffer
            currentValue =
                new String(
                    buffer, startPosn, readLength - delimiter.length, StandardCharsets.UTF_8);
          } else {
            str.write(buffer, startPosn, readLength);
            currentValue = str.toString(0, str.size() - delimiter.length, StandardCharsets.UTF_8);
          }
          break;
        }
      }

      startOfNextRecord = startOfRecord + bytesConsumed;
      str.reset();
      return true;
    }
  }

  /**
   * This class is created to avoid multiple bytes-copy when making a substring of the output.
   * Without this class, it requires two bytes copies.
   *
   * <pre>{@code
   * ByteArrayOutputStream out = ...;
   * byte[] buffer = out.toByteArray(); // 1st-copy
   * String s = new String(buffer, offset, length); // 2nd-copy
   * }</pre>
   */
  static class SubstringByteArrayOutputStream extends ByteArrayOutputStream {
    public String toString(int offset, int length, Charset charset) {
      if (offset < 0) {
        throw new IllegalArgumentException("offset is negative: " + offset);
      }
      if (offset > count) {
        throw new IllegalArgumentException(
            "offset exceeds the buffer limit. offset: " + offset + ", limit: " + count);
      }

      if (length < 0) {
        throw new IllegalArgumentException("length is negative: " + length);
      }

      if (offset + length > count) {
        throw new IllegalArgumentException(
            "offset + length exceeds the buffer limit. offset: "
                + offset
                + ", length: "
                + length
                + ", limit: "
                + count);
      }

      return new String(buf, offset, length, charset);
    }
  }

  /**
   * A state machine to match the delimiter in a byte stream.
   *
   * <pre>{@code
   * DelimiterFinder finder = new DelimiterFinder([65, 65, 66]); // "AAB"
   * finder.feed(65); // false. "A"
   * finder.feed(66); // false. "AB"
   * finder.feed(65); // false. "ABA"
   * finder.feed(65); // false. "ABAA"
   * finder.feed(66); // true.  "ABAAB"
   * finder.feed(65); // false. "ABAABA"
   *
   * }</pre>
   */
  static class DelimiterFinder {
    private final byte[] delimiter;
    private final int[] subsequences;
    private final int[][] trans;

    int position;

    public DelimiterFinder(byte[] delimiter) {
      this.delimiter = delimiter;
      subsequences = new int[delimiter.length];
      trans = new int[delimiter.length][256];
      compile();
    }

    public boolean feed(byte b) {
      position = trans[position][byteToIndex(b)];
      if (position == delimiter.length) {
        position = 0;
        return true;
      } else {
        return false;
      }
    }

    public void reset() {
      position = 0;
    }

    public String describe() {
      StringBuilder sb = new StringBuilder();
      sb.append("delimiter:\n").append(Bytes.asList(delimiter)).append('\n');
      sb.append("subsequences and trans:\n");
      for (int i = 0; i < delimiter.length; ++i) {
        sb.append(i).append(" (").append(subsequences[i]).append("): ");
        for (int b = Byte.MIN_VALUE; b <= Byte.MAX_VALUE; ++b) {
          int tran = trans[i][byteToIndex((byte) b)];
          if (tran > 0) {
            sb.append(b).append(" -> ").append(tran).append(", ");
          }
        }
        sb.append("\n");
      }
      sb.append("current position: ").append(position).append("\n");
      return sb.toString();
    }

    private void compile() {
      // e.g. "AABAAC":
      // 0 -> 0, ""      -> ""
      // 1 -> 0: "A"     -> ""
      // 2 -> 1: "AA"    -> "A"
      // 3 -> 0: "AAB"   -> "B"
      // 4 -> 1: "AABA"  -> "A"
      // 5 -> 2: "AABAA" -> "AA"

      for (int i = 2; i < delimiter.length; i++) {
        if (delimiter[i - 1] == delimiter[subsequences[i - 1]]) {
          subsequences[i] = subsequences[i - 1] + 1;
        } else if (delimiter[i - 1] == delimiter[0]) {
          subsequences[i] = 1;
        } else {
          subsequences[i] = 0;
        }
      }
      // e.g. "AABAAC":
      // index (subsequence): trans
      // 0 (0): "A" -> 1
      // 1 (0): "A" -> 2
      // 2 (1): "A" -> 2, "B" -> 3
      // 3 (0): "A" -> 4
      // 4 (1): "A" -> 5
      // 5 (2): "A" -> 2, "B" -> 3, "C" -> 6
      trans[0][byteToIndex(delimiter[0])] = 1;

      for (int i = 1; i < delimiter.length; i++) {
        for (int b = Byte.MIN_VALUE; b <= Byte.MAX_VALUE; b++) {
          if (b == delimiter[i]) {
            trans[i][byteToIndex((byte) b)] = i + 1;
          } else {
            trans[i][byteToIndex((byte) b)] = trans[subsequences[i]][byteToIndex((byte) b)];
          }
        }
      }
    }

    private int byteToIndex(byte b) {
      return b + 128;
    }
  }
}
