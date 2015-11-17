/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.api.client.util.Preconditions.checkNotNull;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudPositionToReaderPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudProgressToReaderProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.splitRequestToApproximateProgress;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.range.OffsetRangeTracker;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.IOChannelFactory;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.AbstractBoundedReaderIterator;
import com.google.cloud.dataflow.sdk.util.common.worker.ProgressTracker;
import com.google.cloud.dataflow.sdk.util.common.worker.ProgressTrackerGroup;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.channels.Channels;
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
public class TextReader<T> extends Reader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(TextReader.class);

  @VisibleForTesting static final int BUF_SIZE = 200;

  // The following fields are package-private to be visible in tests.
  @VisibleForTesting final String filepattern;
  @VisibleForTesting @Nullable final Long startPosition;
  @VisibleForTesting @Nullable final Long endPosition;
  @VisibleForTesting final Coder<T> coder;
  @VisibleForTesting final TextIO.CompressionType compressionType;
  @VisibleForTesting final boolean stripTrailingNewlines;
  @VisibleForTesting @Nullable private Collection<String> expandedFilepattern;

  public TextReader(String filepattern, boolean stripTrailingNewlines,
                    @Nullable Long startPosition, @Nullable Long endPosition, Coder<T> coder,
                    TextIO.CompressionType compressionType) {
    this.filepattern = filepattern;
    this.startPosition = startPosition;
    this.endPosition = endPosition;
    this.coder = coder;
    this.stripTrailingNewlines = stripTrailingNewlines;
    this.compressionType = compressionType;
  }

  public double getTotalParallelism() {
    try {
      if (compressionType == TextIO.CompressionType.UNCOMPRESSED) {
        // All files are splittable.
        return getTotalParallelismSplittable();
      } else if (compressionType == TextIO.CompressionType.AUTO) {
        for (String file : expandedFilepattern()) {
          if (FilenameBasedStreamFactory.getCompressionTypeForAuto(file)
              == TextIO.CompressionType.UNCOMPRESSED) {
            // At least one file is splittable.
            return getTotalParallelismSplittable();
          }
        }
        // All files were compressed.
        return getTotalParallelismUnsplittable();
      } else {
        // No compressed formats support liquid sharding yet.
        return getTotalParallelismUnsplittable();
      }
    } catch (IOException exn) {
      throw new RuntimeException(exn);
    }
  }

  private double getTotalParallelismSplittable() {
    // Assume splittable at every byte.
    return (endPosition == null ? Double.POSITIVE_INFINITY : endPosition)
        - (startPosition == null ? 0 : startPosition);
  }

  private double getTotalParallelismUnsplittable() throws IOException {
    // Total parallelism is the number of files matched by the filepattern.
    return expandedFilepattern().size();
  }

  private ReaderIterator<T> newReaderIteratorForRangeInFile(IOChannelFactory factory,
      String oneFile, long startPosition, @Nullable Long endPosition) throws IOException {
    // Position before the first record, so we can find the record beginning.
    final long start = startPosition > 0 ? startPosition - 1 : 0;

    TextFileIterator iterator = newReaderIteratorForRangeWithStrictStart(
        factory, oneFile, stripTrailingNewlines, start, endPosition);

    // Skip the initial record if start position was set.
    if (startPosition > 0) {
      iterator.hasNextImpl();
    }

    return iterator;
  }

  private ReaderIterator<T> newReaderIteratorForFiles(
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
        new FilenameBasedStreamFactory(input, compressionType));
  }

  private Collection<String> expandedFilepattern() throws IOException {
    if (expandedFilepattern == null) {
      IOChannelFactory factory = IOChannelUtils.getFactory(filepattern);
      expandedFilepattern = factory.match(filepattern);
    }
    return expandedFilepattern;
  }

  @Override
  public ReaderIterator<T> iterator() throws IOException {
    IOChannelFactory factory = IOChannelUtils.getFactory(filepattern);
    Collection<String> inputs = expandedFilepattern();
    if (inputs.isEmpty()) {
      throw new FileNotFoundException("No match for file pattern '" + filepattern + "'");
    }

    if (startPosition != null || endPosition != null) {
      if (inputs.size() != 1) {
        throw new IllegalArgumentException(
            "Offset range specified: [" + startPosition + ", " + endPosition + "), so "
            + "an exact filename was expected, but more than 1 file matched \"" + filepattern
            + "\" (total " + inputs.size() + "): apparently a filepattern was given.");
      }

      return newReaderIteratorForRangeInFile(factory, inputs.iterator().next(),
          startPosition == null ? 0 : startPosition, endPosition);
    } else {
      return newReaderIteratorForFiles(factory, inputs);
    }
  }

  /**
   * Factory interface for creating a decompressing {@link InputStream}.
   */
  public interface DecompressingStreamFactory {
    /**
     * Create a decompressing {@link InputStream} from an existing {@link InputStream}.
     *
     * @param inputStream the existing stream
     * @return a stream that decompresses the contents of the existing stream
     * @throws IOException
     */
    public InputStream createInputStream(InputStream inputStream) throws IOException;
  }

  /**
   * Factory for creating decompressing input streams based on a filename and
   * a {@link TextIO.CompressionType}.  If the compression mode is AUTO, the filename
   * is checked against known extensions to determine a compression type to use.
   */
  protected static class FilenameBasedStreamFactory
      implements DecompressingStreamFactory {
    private String filename;
    private TextIO.CompressionType compressionType;

    public FilenameBasedStreamFactory(String filename, TextIO.CompressionType compressionType) {
      this.filename = filename;
      this.compressionType = compressionType;
    }

    protected TextIO.CompressionType getCompressionTypeForAuto() {
      return getCompressionTypeForAuto(filename);
    }

    protected static TextIO.CompressionType getCompressionTypeForAuto(String filepattern) {
      for (TextIO.CompressionType type : TextIO.CompressionType.values()) {
        if (type.matches(filepattern) && type != TextIO.CompressionType.AUTO
            && type != TextIO.CompressionType.UNCOMPRESSED) {
          return type;
        }
      }
      return TextIO.CompressionType.UNCOMPRESSED;
    }

    @Override
    public InputStream createInputStream(InputStream inputStream) throws IOException {
      if (compressionType == TextIO.CompressionType.AUTO) {
        return getCompressionTypeForAuto().createInputStream(inputStream);
      }
      return compressionType.createInputStream(inputStream);
    }
  }

  private class TextFileMultiIterator extends LazyMultiReaderIterator<T> {
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

  class TextFileIterator extends AbstractBoundedReaderIterator<T> {
    private final CopyableSeekableByteChannel seeker;
    private final PushbackInputStream stream;
    private final OffsetRangeTracker rangeTracker;
    private final ProgressTracker<Integer> progressTracker;
    private long offset;
    private ByteArrayOutputStream nextElement;
    private ScanState state;

    TextFileIterator(CopyableSeekableByteChannel seeker, boolean stripTrailingNewlines,
        long startOffset, @Nullable Long endOffset,
        DecompressingStreamFactory compressionStreamFactory) throws IOException {
      this.seeker = checkNotNull(seeker);
      this.seeker.position(startOffset);
      InputStream inputStream =
          compressionStreamFactory.createInputStream(Channels.newInputStream(seeker));
      BufferedInputStream bufferedStream = new BufferedInputStream(inputStream);
      this.stream = new PushbackInputStream(bufferedStream, BUF_SIZE);
      long stopOffset = (endOffset == null) ? OffsetRangeTracker.OFFSET_INFINITY : endOffset;
      this.rangeTracker = new OffsetRangeTracker(startOffset, stopOffset);
      this.offset = startOffset;
      this.progressTracker = checkNotNull(new ProgressTrackerGroup<Integer>() {
            @Override
            protected void report(Integer lineLength) {
              notifyElementRead(lineLength.longValue());
            }
          }.start());

      this.state = new ScanState(BUF_SIZE, stripTrailingNewlines);
    }

    /**
     * Reads a line of text. A line is considered to be terminated by any
     * one of a line feed ({@code '\n'}), a carriage return
     * ({@code '\r'}), or a carriage return followed immediately by a linefeed
     * ({@code "\r\n"}).
     *
     * @return a {@code ByteArrayOutputStream} containing the contents of the
     *     line, with any line-termination characters stripped if
     *     stripTrailingNewlines==true, or {@code null} if the end of the stream has
     *     been reached.
     * @throws IOException if an I/O error occurs
     */
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
      return buffer;
    }

    @Override
    protected boolean hasNextImpl() throws IOException {
      long startOffset = offset;
      ByteArrayOutputStream element = readElement(); // As a side effect, updates "offset"
      if (element != null && rangeTracker.tryReturnRecordAt(true, startOffset)) {
        nextElement = element;
        progressTracker.saw((int) (offset - startOffset));
      } else {
        nextElement = null;
      }
      return nextElement != null;
    }

    @Override
    protected T nextImpl() throws IOException {
      return CoderUtils.decodeFromByteArray(coder, nextElement.toByteArray());
    }

    @Override
    public Progress getProgress() {
      // Currently we assume that only a offset position and fraction are reported as
      // current progress. An implementor can override this method to update
      // other metrics, e.g. report a different completion percentage or remaining time.
      com.google.api.services.dataflow.model.Position currentPosition =
          new com.google.api.services.dataflow.model.Position();
      currentPosition.setByteOffset(offset);

      ApproximateProgress progress = new ApproximateProgress();
      progress.setPosition(currentPosition);

      // If endOffset is unspecified, we don't know the fraction consumed.
      if (rangeTracker.getStopPosition() != Long.MAX_VALUE) {
        progress.setPercentComplete((float) rangeTracker.getFractionConsumed());
      }

      return cloudProgressToReaderProgress(progress);
    }

    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      checkNotNull(splitRequest);

      // Currently, file-based Reader only supports split at a byte offset.
      ApproximateProgress splitProgress = splitRequestToApproximateProgress(splitRequest);
      com.google.api.services.dataflow.model.Position splitPosition = splitProgress.getPosition();
      if (splitPosition == null) {
        if (splitProgress.getPercentComplete() != null) {
          float percentageComplete = splitProgress.getPercentComplete().floatValue();
          if (percentageComplete <= 0 || percentageComplete >= 1) {
            LOG.warn(
                "TextReader cannot be split since the provided percentage of "
                + "work to be completed is out of the valid range (0, 1). Requested: {}",
                splitRequest);
          }

          splitPosition = new com.google.api.services.dataflow.model.Position();
          if (getEndOffset() == Long.MAX_VALUE) {
            LOG.warn(
                "TextReader cannot be split since the end offset is set to Long.MAX_VALUE."
                + " Requested: {}",
                splitRequest);
            return null;
          }

          splitPosition.setByteOffset(
              rangeTracker.getPositionForFractionConsumed(percentageComplete));
        } else {
          LOG.warn(
              "TextReader requires either a position or percentage of work to be complete to"
              + " perform a dynamic split request. Requested: {}",
              splitRequest);
          return null;
        }
      } else if (splitPosition.getByteOffset() == null) {
        LOG.warn(
            "TextReader cannot be split since the provided split position "
            + "does not contain a valid offset. Requested: {}",
            splitRequest);
        return null;
      }
      Long splitOffset = splitPosition.getByteOffset();

      if (rangeTracker.trySplitAtPosition(splitOffset)) {
        return new DynamicSplitResultWithPosition(cloudPositionToReaderPosition(splitPosition));
      } else {
        return null;
      }
    }

    /**
     * Returns the end offset of the iterator or Long.MAX_VALUE if unspecified.
     * This method is called for test ONLY.
     */
    long getEndOffset() {
      return rangeTracker.getStopPosition();
    }

    /**
     * Returns the start offset of the iterator.
     * This method is called for test ONLY.
     */
    long getStartOffset() {
      return rangeTracker.getStartPosition();
    }

    @Override
    public void close() throws IOException {
      stream.close();
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
    private final byte[] buf;
    private final boolean stripTrailingNewlines;
    private byte lastByteRead;

    public ScanState(int size, boolean stripTrailingNewlines) {
      this.start = 0;
      this.pos = 0;
      this.end = 0;
      this.buf = new byte[size];
      this.stripTrailingNewlines = stripTrailingNewlines;
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

    /**
     * Copies data from the input buffer to the output buffer.
     *
     * <p>If stripTrailing==false, line-termination characters are included in the copy.
     */
    private void copyToOutputBuffer(ByteArrayOutputStream out) {
      int charsCopied = pos - start;
      if (!stripTrailingNewlines && separatorFound()) {
        charsCopied++;
      }
      out.write(buf, start, charsCopied);
    }

    /**
     * Scans the input buffer to determine if a matched carriage return
     * has an accompanying linefeed and process the input buffer accordingly.
     *
     * <p>If stripTrailingNewlines==false and a linefeed character is detected,
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
          if (!stripTrailingNewlines) {
            out.write('\n');
          }
        }
      } else {
        // We are at the end of the buffer and need one more
        // byte. Get it the slow but safe way.
        int b = stream.read();
        if (b == '\n') {
          charsConsumed++;
          if (!stripTrailingNewlines) {
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
