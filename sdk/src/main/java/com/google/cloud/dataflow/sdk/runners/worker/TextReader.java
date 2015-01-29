/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.util.IOChannelFactory;
import com.google.cloud.dataflow.sdk.util.common.worker.ProgressTracker;
import com.google.cloud.dataflow.sdk.util.common.worker.ProgressTrackerGroup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Collection;
import java.util.Iterator;

import javax.annotation.Nullable;

/**
 * A source that reads text files.
 *
 * @param <T> the type of the elements read from the source
 */
public class TextReader<T> extends FileBasedReader<T> {
  final boolean stripTrailingNewlines;
  final TextIO.CompressionType compressionType;

  public TextReader(String filename, boolean stripTrailingNewlines, @Nullable Long startPosition,
      @Nullable Long endPosition, Coder<T> coder, TextIO.CompressionType compressionType) {
    this(filename, stripTrailingNewlines, startPosition, endPosition, coder, true,
        compressionType);
  }

  protected TextReader(String filename, boolean stripTrailingNewlines, @Nullable Long startPosition,
      @Nullable Long endPosition, Coder<T> coder, boolean useDefaultBufferSize,
      TextIO.CompressionType compressionType) {
    super(filename, startPosition, endPosition, coder, useDefaultBufferSize);
    this.stripTrailingNewlines = stripTrailingNewlines;
    this.compressionType = compressionType;
  }

  @Override
  protected ReaderIterator<T> newReaderIteratorForRangeInFile(IOChannelFactory factory,
      String oneFile, long startPosition, @Nullable Long endPosition) throws IOException {
    // Position before the first record, so we can find the record beginning.
    final long start = startPosition > 0 ? startPosition - 1 : 0;

    TextFileIterator iterator = newReaderIteratorForRangeWithStrictStart(
        factory, oneFile, stripTrailingNewlines, start, endPosition);

    // Skip the initial record if start position was set.
    if (startPosition > 0 && iterator.hasNext()) {
      iterator.advance();
    }

    return iterator;
  }

  @Override
  protected ReaderIterator<T> newReaderIteratorForFiles(
      IOChannelFactory factory, Collection<String> files) throws IOException {
    if (files.size() == 1) {
      return newReaderIteratorForFile(factory, files.iterator().next(), stripTrailingNewlines);
    }

    return new TextFileMultiIterator(factory, files.iterator(), stripTrailingNewlines);
  }

  private TextFileIterator newReaderIteratorForFile(
      IOChannelFactory factory, String input, boolean stripTrailingNewlines) throws IOException {
    return newReaderIteratorForRangeWithStrictStart(factory, input, stripTrailingNewlines, 0, null);
  }

  /**
   * Returns a new iterator for lines in the given range in the given
   * file.  Does NOT skip the first line if the range starts in the
   * middle of a line (instead, the latter half that starts at
   * startOffset will be returned as the first element).
   */
  private TextFileIterator newReaderIteratorForRangeWithStrictStart(IOChannelFactory factory,
      String input, boolean stripTrailingNewlines, long startOffset, @Nullable Long endOffset)
      throws IOException {
    ReadableByteChannel reader = factory.open(input);
    if (!(reader instanceof SeekableByteChannel)) {
      throw new UnsupportedOperationException("Unable to seek in stream for " + input);
    }

    SeekableByteChannel seeker = (SeekableByteChannel) reader;

    return new TextFileIterator(
        new CopyableSeekableByteChannel(seeker), stripTrailingNewlines, startOffset, endOffset,
        new FileBasedReader.FilenameBasedStreamFactory(input, compressionType));
  }

  class TextFileMultiIterator extends LazyMultiReaderIterator<T> {
    private final IOChannelFactory factory;
    private final boolean stripTrailingNewlines;

    public TextFileMultiIterator(
        IOChannelFactory factory, Iterator<String> inputs, boolean stripTrailingNewlines) {
      super(inputs);
      this.factory = factory;
      this.stripTrailingNewlines = stripTrailingNewlines;
    }

    @Override
    protected ReaderIterator<T> open(String input) throws IOException {
      return newReaderIteratorForFile(factory, input, stripTrailingNewlines);
    }
  }

  class TextFileIterator extends FileBasedIterator {
    private final boolean stripTrailingNewlines;
    private ScanState state;

    TextFileIterator(CopyableSeekableByteChannel seeker, boolean stripTrailingNewlines,
        long startOffset, @Nullable Long endOffset,
        FileBasedReader.DecompressingStreamFactory compressionStreamFactory) throws IOException {
      this(seeker, stripTrailingNewlines, startOffset, startOffset, endOffset,
          new ProgressTrackerGroup<Integer>() {
            @Override
            protected void report(Integer lineLength) {
              notifyElementRead(lineLength.longValue());
            }
          }.start(),
          new ScanState(BUF_SIZE, !stripTrailingNewlines),
          compressionStreamFactory);
    }

    private TextFileIterator(CopyableSeekableByteChannel seeker, boolean stripTrailingNewlines,
        long startOffset, long offset, @Nullable Long endOffset, ProgressTracker<Integer> tracker,
        ScanState state, FileBasedReader.DecompressingStreamFactory compressionStreamFactory)
            throws IOException {
      super(seeker, startOffset, offset, endOffset, tracker, compressionStreamFactory);

      this.stripTrailingNewlines = stripTrailingNewlines;
      this.state = state;
    }

    private TextFileIterator(TextFileIterator it) throws IOException {
      // Correctly adjust the start position of the seeker given
      // that it may hold bytes that have been read and now reside
      // in the read buffer (that is copied during cloning).
      this(it.seeker.copy(), it.stripTrailingNewlines, it.startOffset + it.state.totalBytesRead,
          it.offset, it.endOffset, it.tracker.copy(), it.state.copy(), it.compressionStreamFactory);
    }

    @Override
    public ReaderIterator<T> copy() throws IOException {
      return new TextFileIterator(this);
    }

    /**
     * Reads a line of text. A line is considered to be terminated by any
     * one of a line feed ({@code '\n'}), a carriage return
     * ({@code '\r'}), or a carriage return followed immediately by a linefeed
     * ({@code "\r\n"}).
     *
     * @return a {@code ByteArrayOutputStream} containing the contents of the
     *     line, with any line-termination characters stripped if
     *     keepNewlines==false, or {@code null} if the end of the stream has
     *     been reached.
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected ByteArrayOutputStream readElement() throws IOException {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream(BUF_SIZE);

      int charsConsumed = 0;
      while (true) {
        // Attempt to read blocks of data at a time
        // until a separator is found.
        if (!state.readBytes(stream)) {
          break;
        }

        int consumed = state.consumeUntilSeparator(buffer);
        charsConsumed += consumed;
        if (consumed > 0 && state.separatorFound()) {
          if (state.lastByteRead() == '\r') {
            charsConsumed += state.copyCharIfLinefeed(buffer, stream);
          }
          break;
        }
      }

      if (charsConsumed == 0) {
        // Note that charsConsumed includes the size of any separators that may
        // have been stripped off -- so if we didn't get anything, we're at the
        // end of the file.
        return null;
      }

      offset += charsConsumed;
      tracker.saw(charsConsumed);
      return buffer;
    }
  }

  /**
   * ScanState encapsulates the state for the current buffer of text
   * being scanned.
   */
  private static class ScanState {
    private int start; // Valid bytes in buf start at this index
    private int pos; // Where the separator is in the buf (if one was found)
    private int end; // the index of the end of bytes in buf
    private byte[] buf;
    private boolean keepNewlines;
    private byte lastByteRead;
    private long totalBytesRead;

    public ScanState(int size, boolean keepNewlines) {
      this.start = 0;
      this.pos = 0;
      this.end = 0;
      this.buf = new byte[size];
      this.keepNewlines = keepNewlines;
      totalBytesRead = 0;
    }

    public ScanState copy() {
      byte[] bufCopy = new byte[buf.length]; // copy :(
      System.arraycopy(buf, start, bufCopy, start, end - start);
      return new ScanState(
          this.keepNewlines, this.start, this.pos, this.end, bufCopy, this.lastByteRead, 0);
    }

    private ScanState(boolean keepNewlines, int start, int pos, int end, byte[] buf,
        byte lastByteRead, long totalBytesRead) {
      this.start = start;
      this.pos = pos;
      this.end = end;
      this.buf = buf;
      this.keepNewlines = keepNewlines;
      this.lastByteRead = lastByteRead;
      this.totalBytesRead = totalBytesRead;
    }

    public boolean readBytes(PushbackInputStream stream) throws IOException {
      if (start < end) {
        return true;
      }
      assert end <= buf.length : end + " > " + buf.length;
      int bytesRead = stream.read(buf, end, buf.length - end);
      if (bytesRead == -1) {
        return false;
      }
      totalBytesRead += bytesRead;
      end += bytesRead;
      return true;
    }

    /**
     * Consumes characters until a separator character is found or the
     * end of buffer is reached.
     *
     * <p>Updates the state to indicate the position of the separator
     * character. If pos==len, no separator was found.
     *
     * @return the number of characters consumed.
     */
    public int consumeUntilSeparator(ByteArrayOutputStream out) {
      for (pos = start; pos < end; ++pos) {
        lastByteRead = buf[pos];
        if (separatorFound()) {
          int charsConsumed = (pos - start + 1); // The separator is consumed
          copyToOutputBuffer(out);
          start = pos + 1; // skip the separator
          return charsConsumed;
        }
      }
      // No separator found
      assert pos == end;
      int charsConsumed = (pos - start);
      out.write(buf, start, charsConsumed);
      start = 0;
      end = 0;
      pos = 0;
      return charsConsumed;
    }

    public boolean separatorFound() {
      return lastByteRead == '\n' || lastByteRead == '\r';
    }

    public byte lastByteRead() {
      return buf[pos];
    }

    public int bytesBuffered() {
      assert end >= start : end + " must be >= " + start;
      return end - start;
    }

    /**
     * Copies data from the input buffer to the output buffer.
     *
     * <p>If keepNewlines==true, line-termination characters are included in the copy.
     */
    private void copyToOutputBuffer(ByteArrayOutputStream out) {
      int charsCopied = pos - start;
      if (keepNewlines && separatorFound()) {
        charsCopied++;
      }
      out.write(buf, start, charsCopied);
    }

    /**
     * Scans the input buffer to determine if a matched carriage return
     * has an accompanying linefeed and process the input buffer accordingly.
     *
     * <p>If keepNewlines==true and a linefeed character is detected,
     * it is included in the copy.
     *
     * @return the number of characters consumed
     */
    private int copyCharIfLinefeed(ByteArrayOutputStream out, PushbackInputStream stream)
        throws IOException {
      int charsConsumed = 0;
      // Check to make sure we don't go off the end of the buffer
      if ((pos + 1) < end) {
        if (buf[pos + 1] == '\n') {
          charsConsumed++;
          pos++;
          start++;
          if (keepNewlines) {
            out.write('\n');
          }
        }
      } else {
        // We are at the end of the buffer and need one more
        // byte. Get it the slow but safe way.
        int b = stream.read();
        if (b == '\n') {
          charsConsumed++;
          totalBytesRead++;
          if (keepNewlines) {
            out.write(b);
          }
        } else if (b != -1) {
          // Consider replacing unread() since it may be slow if
          // iterators are cloned frequently.
          stream.unread(b);
        }
      }
      return charsConsumed;
    }
  }
}
