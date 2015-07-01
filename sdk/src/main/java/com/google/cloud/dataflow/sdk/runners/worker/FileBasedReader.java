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
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.channels.Channels;
import java.util.Collection;

import javax.annotation.Nullable;

/**
 * Abstract base class for sources that read from files.
 *
 * @param <T> the type of the elements read from the source
 */
public abstract class FileBasedReader<T> extends Reader<T> {
  protected static final int BUF_SIZE = 200;
  protected final String filename;

  @Nullable
  protected final Long startPosition;

  @Nullable
  protected final Long endPosition;
  protected final Coder<T> coder;
  protected final boolean useDefaultBufferSize;

  private static final Logger LOG = LoggerFactory.getLogger(FileBasedReader.class);

  protected FileBasedReader(String filename, @Nullable Long startPosition,
      @Nullable Long endPosition, Coder<T> coder, boolean useDefaultBufferSize) {
    this.filename = filename;
    this.startPosition = startPosition;
    this.endPosition = endPosition;
    this.coder = coder;
    this.useDefaultBufferSize = useDefaultBufferSize;
  }

  /**
   * Returns a new iterator for elements in the given range in the
   * given file.  If the range starts in the middle an element, this
   * element is skipped as it is considered part of the previous
   * range; if the last element that starts in the range finishes
   * beyond the end position, it is still considered part of this
   * range.  In other words, the start position and the end position
   * are "rounded up" to element boundaries.
   *
   * @param endPosition offset of the end position; null means end-of-file
   */
  protected abstract ReaderIterator<T> newReaderIteratorForRangeInFile(IOChannelFactory factory,
      String oneFile, long startPosition, @Nullable Long endPosition) throws IOException;

  /**
   * Returns a new iterator for elements in the given files.  Caller
   * must ensure that the file collection is not empty.
   */
  protected abstract ReaderIterator<T> newReaderIteratorForFiles(
      IOChannelFactory factory, Collection<String> files) throws IOException;

  @Override
  public ReaderIterator<T> iterator() throws IOException {
    IOChannelFactory factory = IOChannelUtils.getFactory(filename);
    Collection<String> inputs = factory.match(filename);
    if (inputs.isEmpty()) {
      throw new FileNotFoundException("No match for file pattern '" + filename + "'");
    }

    if (startPosition != null || endPosition != null) {
      if (inputs.size() != 1) {
        throw new IllegalArgumentException(
            "Offset range specified: [" + startPosition + ", " + endPosition + "), so "
            + "an exact filename was expected, but more than 1 file matched \"" + filename
            + "\" (total " + inputs.size() + "): apparently a filepattern was given.");
      }

      return newReaderIteratorForRangeInFile(factory, inputs.iterator().next(),
          startPosition == null ? 0 : startPosition, endPosition);
    } else {
      return newReaderIteratorForFiles(factory, inputs);
    }
  }

  /**
   * Abstract base class for file-based source iterators.
   */
  protected abstract class FileBasedIterator extends AbstractBoundedReaderIterator<T> {
    protected final CopyableSeekableByteChannel seeker;
    protected final PushbackInputStream stream;
    protected final OffsetRangeTracker rangeTracker;
    protected long offset;
    protected final ProgressTracker<Integer> progressTracker;
    protected ByteArrayOutputStream nextElement;
    protected DecompressingStreamFactory compressionStreamFactory;

    FileBasedIterator(
        CopyableSeekableByteChannel seeker,
        long startOffset,
        long offset,
        @Nullable Long endOffset,
        ProgressTracker<Integer> progressTracker,
        DecompressingStreamFactory compressionStreamFactory)
        throws IOException {
      this.seeker = checkNotNull(seeker);
      this.seeker.position(startOffset);
      this.compressionStreamFactory = compressionStreamFactory;
      InputStream inputStream =
          compressionStreamFactory.createInputStream(Channels.newInputStream(seeker));
      BufferedInputStream bufferedStream =
          useDefaultBufferSize
              ? new BufferedInputStream(inputStream)
              : new BufferedInputStream(inputStream, BUF_SIZE);
      this.stream = new PushbackInputStream(bufferedStream, BUF_SIZE);
      long stopOffset = (endOffset == null) ? OffsetRangeTracker.OFFSET_INFINITY : endOffset;
      this.rangeTracker = new OffsetRangeTracker(startOffset, stopOffset);
      this.offset = offset;
      this.progressTracker = checkNotNull(progressTracker);
    }

    /**
     * Reads the next element.
     *
     * @return a {@code ByteArrayOutputStream} containing the contents
     *     of the element, or {@code null} if the end of the stream
     *     has been reached.
     * @throws IOException if an I/O error occurs
     */
    protected abstract ByteArrayOutputStream readElement() throws IOException;

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
        LOG.warn("FileBasedReader only supports split at a Position. Requested: {}",
            splitRequest);
        return null;
      }
      Long splitOffset = splitPosition.getByteOffset();
      if (splitOffset == null) {
        LOG.warn("FileBasedReader only supports split at byte offset. Requested: {}",
            splitPosition);
        return null;
      }
      if (rangeTracker.trySplitAtPosition(splitOffset)) {
        return new DynamicSplitResultWithPosition(cloudPositionToReaderPosition(splitPosition));
      } else {
        return null;
      }
    }

    /**
     * Returns the end offset of the iterator or Long.MAX_VALUE if unspecified.
     * The method is called for test ONLY.
     */
    long getEndOffset() {
      return rangeTracker.getStopPosition();
    }

    @Override
    public void close() throws IOException {
      stream.close();
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
      for (TextIO.CompressionType type : TextIO.CompressionType.values()) {
        if (type.matches(filename) && type != TextIO.CompressionType.AUTO
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
}
