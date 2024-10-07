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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Verify.verify;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A common base class for all file-based {@link Source}s. Extend this class to implement your own
 * file-based custom source.
 *
 * <p>A file-based {@code Source} is a {@code Source} backed by a file pattern defined as a Java
 * glob, a single file, or a offset range for a single file. See {@link OffsetBasedSource} and
 * {@link org.apache.beam.sdk.io.range.RangeTracker} for semantics of offset ranges.
 *
 * <p>This source stores a {@code String} that is a {@link FileSystems} specification for a file or
 * file pattern. There should be a {@link FileSystem} registered for the file specification
 * provided. Please refer to {@link FileSystems} and {@link FileSystem} for more information on
 * this.
 *
 * <p>In addition to the methods left abstract from {@code BoundedSource}, subclasses must implement
 * methods to create a sub-source and a reader for a range of a single file - {@link
 * #createForSubrangeOfFile} and {@link #createSingleFileReader}. Please refer to {@link TextIO
 * TextIO.TextSource} for an example implementation of {@code FileBasedSource}.
 *
 * @param <T> Type of records represented by the source.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class FileBasedSource<T> extends OffsetBasedSource<T> {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedSource.class);

  private final ValueProvider<String> fileOrPatternSpec;
  private final EmptyMatchTreatment emptyMatchTreatment;
  private MatchResult.@Nullable Metadata singleFileMetadata;
  private final Mode mode;

  private final AtomicReference<@Nullable Long> filesSizeBytes;

  /** A given {@code FileBasedSource} represents a file resource of one of these types. */
  public enum Mode {
    FILEPATTERN,
    SINGLE_FILE_OR_SUBRANGE
  }

  /**
   * Create a {@code FileBaseSource} based on a file or a file pattern specification, with the given
   * strategy for treating filepatterns that do not match any files.
   */
  protected FileBasedSource(
      ValueProvider<String> fileOrPatternSpec,
      EmptyMatchTreatment emptyMatchTreatment,
      long minBundleSize) {
    super(0, Long.MAX_VALUE, minBundleSize);
    this.mode = Mode.FILEPATTERN;
    this.emptyMatchTreatment = emptyMatchTreatment;
    this.fileOrPatternSpec = fileOrPatternSpec;
    this.filesSizeBytes = new AtomicReference<>();
  }

  /**
   * Like {@link #FileBasedSource(ValueProvider, EmptyMatchTreatment, long)}, but uses the default
   * value of {@link EmptyMatchTreatment#DISALLOW}.
   */
  protected FileBasedSource(ValueProvider<String> fileOrPatternSpec, long minBundleSize) {
    this(fileOrPatternSpec, EmptyMatchTreatment.DISALLOW, minBundleSize);
  }

  /**
   * Create a {@code FileBasedSource} based on a single file. This constructor must be used when
   * creating a new {@code FileBasedSource} for a subrange of a single file. Additionally, this
   * constructor must be used to create new {@code FileBasedSource}s when subclasses implement the
   * method {@link #createForSubrangeOfFile}.
   *
   * <p>See {@link OffsetBasedSource} for detailed descriptions of {@code minBundleSize}, {@code
   * startOffset}, and {@code endOffset}.
   *
   * @param fileMetadata specification of the file represented by the {@link FileBasedSource}, in
   *     suitable form for use with {@link FileSystems#match(List)}.
   * @param minBundleSize minimum bundle size in bytes.
   * @param startOffset starting byte offset.
   * @param endOffset ending byte offset. If the specified value {@code >= #getMaxEndOffset()} it
   *     implies {@code #getMaxEndOffSet()}.
   */
  protected FileBasedSource(
      Metadata fileMetadata, long minBundleSize, long startOffset, long endOffset) {
    super(startOffset, endOffset, minBundleSize);
    mode = Mode.SINGLE_FILE_OR_SUBRANGE;
    this.singleFileMetadata = checkNotNull(fileMetadata, "fileMetadata");
    this.fileOrPatternSpec = StaticValueProvider.of(fileMetadata.resourceId().toString());
    this.filesSizeBytes = new AtomicReference<>();

    // This field will be unused in this mode.
    this.emptyMatchTreatment = EmptyMatchTreatment.DISALLOW;
  }

  /**
   * Returns the information about the single file that this source is reading from.
   *
   * @throws IllegalArgumentException if this source is in {@link Mode#FILEPATTERN} mode.
   */
  public final MatchResult.Metadata getSingleFileMetadata() {
    checkArgument(
        mode == Mode.SINGLE_FILE_OR_SUBRANGE,
        "This function should only be called for a single file, not %s",
        this);
    checkState(
        singleFileMetadata != null,
        "It should not be possible to construct a %s in mode %s with null metadata: %s",
        FileBasedSource.class,
        mode,
        this);
    return singleFileMetadata;
  }

  public final String getFileOrPatternSpec() {
    return fileOrPatternSpec.get();
  }

  public final ValueProvider<String> getFileOrPatternSpecProvider() {
    return fileOrPatternSpec;
  }

  public final EmptyMatchTreatment getEmptyMatchTreatment() {
    return emptyMatchTreatment;
  }

  public final Mode getMode() {
    return mode;
  }

  @Override
  public final FileBasedSource<T> createSourceForSubrange(long start, long end) {
    checkArgument(
        mode != Mode.FILEPATTERN, "Cannot split a file pattern based source based on positions");
    checkArgument(
        start >= getStartOffset(),
        "Start offset value %s of the subrange cannot be smaller than the start offset value %s"
            + " of the parent source",
        start,
        getStartOffset());
    checkArgument(
        end <= getEndOffset(),
        "End offset value %s of the subrange cannot be larger than the end offset value %s",
        end,
        getEndOffset());
    checkState(
        singleFileMetadata != null, "A single file source should not have null metadata: %s", this);

    FileBasedSource<T> source = createForSubrangeOfFile(singleFileMetadata, start, end);
    if (start > 0 || end != Long.MAX_VALUE) {
      checkArgument(
          source.getMode() == Mode.SINGLE_FILE_OR_SUBRANGE,
          "Source created for the range [%s,%s) must be a subrange source",
          start,
          end);
    }
    return source;
  }

  /**
   * Creates and returns a new {@code FileBasedSource} of the same type as the current {@code
   * FileBasedSource} backed by a given file and an offset range. When current source is being
   * split, this method is used to generate new sub-sources. When creating the source subclasses
   * must call the constructor {@link #FileBasedSource(Metadata, long, long, long)} of {@code
   * FileBasedSource} with corresponding parameter values passed here.
   *
   * @param fileMetadata file backing the new {@code FileBasedSource}.
   * @param start starting byte offset of the new {@code FileBasedSource}.
   * @param end ending byte offset of the new {@code FileBasedSource}. May be Long.MAX_VALUE, in
   *     which case it will be inferred using {@link FileBasedSource#getMaxEndOffset}.
   */
  protected abstract FileBasedSource<T> createForSubrangeOfFile(
      Metadata fileMetadata, long start, long end);

  /**
   * Creates and returns an instance of a {@code FileBasedReader} implementation for the current
   * source assuming the source represents a single file. File patterns will be handled by {@code
   * FileBasedSource} implementation automatically.
   */
  protected abstract FileBasedReader<T> createSingleFileReader(PipelineOptions options);

  @Override
  public final long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
    // This implementation of method getEstimatedSizeBytes is provided to simplify subclasses. Here
    // we perform the size estimation of files and file patterns using the interface provided by
    // FileSystem.
    String fileOrPattern = fileOrPatternSpec.get();

    if (mode == Mode.FILEPATTERN) {
      Long maybeNumBytes = filesSizeBytes.get();
      if (maybeNumBytes != null) {
        return maybeNumBytes;
      }

      long totalSize = 0;
      List<Metadata> allMatches = FileSystems.match(fileOrPattern, emptyMatchTreatment).metadata();
      for (Metadata metadata : allMatches) {
        totalSize += metadata.sizeBytes();
      }
      LOG.info(
          "Filepattern {} matched {} files with total size {}",
          fileOrPattern,
          allMatches.size(),
          totalSize);

      filesSizeBytes.compareAndSet(null, totalSize);
      return totalSize;
    } else {
      long start = getStartOffset();
      long end = Math.min(getEndOffset(), getMaxEndOffset(options));
      return end - start;
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    if (mode == Mode.FILEPATTERN) {
      builder.add(
          DisplayData.item("filePattern", getFileOrPatternSpecProvider())
              .withLabel("File Pattern"));
    }
  }

  @Override
  public final List<? extends FileBasedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    // This implementation of method split is provided to simplify subclasses. Here we
    // split a FileBasedSource based on a file pattern to FileBasedSources based on full single
    // files. For files that can be efficiently seeked, we further split FileBasedSources based on
    // those files to FileBasedSources based on sub ranges of single files.
    String fileOrPattern = fileOrPatternSpec.get();

    if (mode == Mode.FILEPATTERN) {
      long startTime = System.currentTimeMillis();
      List<Metadata> expandedFiles =
          FileSystems.match(fileOrPattern, emptyMatchTreatment).metadata();
      List<FileBasedSource<T>> splitResults = new ArrayList<>(expandedFiles.size());
      for (Metadata metadata : expandedFiles) {
        FileBasedSource<T> split = createForSubrangeOfFile(metadata, 0, metadata.sizeBytes());
        verify(
            split.getMode() == Mode.SINGLE_FILE_OR_SUBRANGE,
            "%s.createForSubrangeOfFile must return a source in mode %s",
            split,
            Mode.SINGLE_FILE_OR_SUBRANGE);
        // The split is NOT in FILEPATTERN mode, so we can call its split without fear
        // of recursion. This will break a single file into multiple splits when the file is
        // splittable and larger than the desired bundle size.
        splitResults.addAll(split.split(desiredBundleSizeBytes, options));
      }
      LOG.info(
          "Splitting filepattern {} into bundles of size {} took {} ms "
              + "and produced {} files and {} bundles",
          fileOrPattern,
          desiredBundleSizeBytes,
          System.currentTimeMillis() - startTime,
          expandedFiles.size(),
          splitResults.size());

      reportSourceLineage(expandedFiles);
      return splitResults;
    } else {
      if (isSplittable()) {
        @SuppressWarnings("unchecked")
        List<FileBasedSource<T>> splits =
            (List<FileBasedSource<T>>) super.split(desiredBundleSizeBytes, options);
        return splits;
      } else {
        LOG.debug(
            "The source for file {} is not split into sub-range based sources since "
                + "the file is not seekable",
            fileOrPattern);
        return ImmutableList.of(this);
      }
    }
  }

  /** Report source Lineage. Depend on the number of files, report full file name or only dir. */
  private void reportSourceLineage(List<Metadata> expandedFiles) {
    if (expandedFiles.size() <= 100) {
      for (Metadata metadata : expandedFiles) {
        FileSystems.reportSourceLineage(metadata.resourceId());
      }
    } else {
      for (Metadata metadata : expandedFiles) {
        // TODO(yathu) Currently it simply report one level up if num of files exceeded 100.
        //  Consider more dedicated strategy (e.g. resolve common ancestor) for accurancy, and work
        //  with metrics size limit.
        FileSystems.reportSourceLineage(metadata.resourceId().getCurrentDirectory());
      }
    }
  }

  /**
   * Determines whether a file represented by this source is can be split into bundles.
   *
   * <p>By default, a source in mode {@link Mode#FILEPATTERN} is always splittable, because
   * splitting will involve expanding the file pattern and producing single-file/subrange sources,
   * which may or may not be splittable themselves.
   *
   * <p>By default, a source in {@link Mode#SINGLE_FILE_OR_SUBRANGE} is splittable if it is on a
   * file system that supports efficient read seeking.
   *
   * <p>Subclasses may override to provide different behavior.
   */
  protected boolean isSplittable() throws Exception {
    if (mode == Mode.FILEPATTERN) {
      // split will expand file pattern and return single file or subrange sources that
      // may or may not be splittable.
      return true;
    }

    return getSingleFileMetadata().isReadSeekEfficient();
  }

  @Override
  public final BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    // Validate the current source prior to creating a reader for it.
    this.validate();
    String fileOrPattern = fileOrPatternSpec.get();

    if (mode == Mode.FILEPATTERN) {
      long startTime = System.currentTimeMillis();
      List<Metadata> fileMetadata =
          FileSystems.match(fileOrPattern, emptyMatchTreatment).metadata();
      LOG.info("Matched {} files for pattern {}", fileMetadata.size(), fileOrPattern);
      List<FileBasedReader<T>> fileReaders = new ArrayList<>();
      for (Metadata metadata : fileMetadata) {
        long endOffset = metadata.sizeBytes();
        fileReaders.add(
            createForSubrangeOfFile(metadata, 0, endOffset).createSingleFileReader(options));
      }
      LOG.debug(
          "Creating a reader for file pattern {} took {} ms",
          fileOrPattern,
          System.currentTimeMillis() - startTime);
      if (fileReaders.size() == 1) {
        return fileReaders.get(0);
      }
      return new FilePatternReader(this, fileReaders);
    } else {
      return createSingleFileReader(options);
    }
  }

  @Override
  public String toString() {
    switch (mode) {
      case FILEPATTERN:
        return fileOrPatternSpec.toString();
      case SINGLE_FILE_OR_SUBRANGE:
        return fileOrPatternSpec + " range " + super.toString();
      default:
        throw new IllegalStateException("Unexpected mode: " + mode);
    }
  }

  @Override
  public void validate() {
    super.validate();
    switch (mode) {
      case FILEPATTERN:
        checkArgument(
            getStartOffset() == 0,
            "FileBasedSource is based on a file pattern or a full single file "
                + "but the starting offset proposed %s is not zero",
            getStartOffset());
        checkArgument(
            getEndOffset() == Long.MAX_VALUE,
            "FileBasedSource is based on a file pattern or a full single file "
                + "but the ending offset proposed %s is not Long.MAX_VALUE",
            getEndOffset());
        break;
      case SINGLE_FILE_OR_SUBRANGE:
        // Nothing more to validate.
        break;
      default:
        throw new IllegalStateException("Unknown mode: " + mode);
    }
  }

  @Override
  public final long getMaxEndOffset(PipelineOptions options) throws IOException {
    checkArgument(
        mode != Mode.FILEPATTERN, "Cannot determine the exact end offset of a file pattern");
    Metadata metadata = getSingleFileMetadata();
    return metadata.sizeBytes();
  }

  /**
   * A {@link Source.Reader reader} that implements code common to readers of {@code
   * FileBasedSource}s.
   *
   * <h2>Seekability</h2>
   *
   * <p>This reader uses a {@link ReadableByteChannel} created for the file represented by the
   * corresponding source to efficiently move to the correct starting position defined in the
   * source. Subclasses of this reader should implement {@link #startReading} to get access to this
   * channel. If the source corresponding to the reader is for a subrange of a file the {@code
   * ReadableByteChannel} provided is guaranteed to be an instance of the type {@link
   * SeekableByteChannel}, which may be used by subclass to traverse back in the channel to
   * determine the correct starting position.
   *
   * <h2>Reading Records</h2>
   *
   * <p>Sequential reading is implemented using {@link #readNextRecord}.
   *
   * <p>Then {@code FileBasedReader} implements "reading a range [A, B)" in the following way.
   *
   * <ol>
   *   <li>{@link #start} opens the file
   *   <li>{@link #start} seeks the {@code SeekableByteChannel} to A (reading offset ranges for
   *       non-seekable files is not supported) and calls {@code startReading()}
   *   <li>{@link #start} calls {@link #advance} once, which, via {@link #readNextRecord}, locates
   *       the first record which is at a split point AND its offset is at or after A. If this
   *       record is at or after B, {@link #advance} returns false and reading is finished.
   *   <li>if the previous advance call returned {@code true} sequential reading starts and {@code
   *       advance()} will be called repeatedly
   * </ol>
   *
   * {@code advance()} calls {@code readNextRecord()} on the subclass, and stops (returns false) if
   * the new record is at a split point AND the offset of the new record is at or after B.
   *
   * <h2>Thread Safety</h2>
   *
   * <p>Since this class implements {@link Source.Reader} it guarantees thread safety. Abstract
   * methods defined here will not be accessed by more than one thread concurrently.
   */
  public abstract static class FileBasedReader<T> extends OffsetBasedReader<T> {

    // Initialized in startImpl
    private @Nullable ReadableByteChannel channel = null;

    /**
     * Subclasses should not perform IO operations at the constructor. All IO operations should be
     * delayed until the {@link #startReading} method is invoked.
     */
    public FileBasedReader(FileBasedSource<T> source) {
      super(source);
      checkArgument(
          source.getMode() != Mode.FILEPATTERN,
          "FileBasedReader does not support reading file patterns");
    }

    @Override
    public synchronized FileBasedSource<T> getCurrentSource() {
      return (FileBasedSource<T>) super.getCurrentSource();
    }

    @Override
    protected final boolean startImpl() throws IOException {
      FileBasedSource<T> source = getCurrentSource();
      ResourceId resourceId = source.getSingleFileMetadata().resourceId();
      try {
        this.channel = FileSystems.open(resourceId);
        if (channel instanceof SeekableByteChannel) {
          SeekableByteChannel seekChannel = (SeekableByteChannel) channel;
          seekChannel.position(source.getStartOffset());
        } else {
          // Channel is not seekable. Must not be a subrange.
          checkArgument(
              source.mode != Mode.SINGLE_FILE_OR_SUBRANGE,
              "Subrange-based sources must only be defined for file types that support seekable "
                  + " read channels");
          checkArgument(
              source.getStartOffset() == 0,
              "Start offset %s is not zero but channel for reading the file is not seekable.",
              source.getStartOffset());
        }

        startReading(channel);
      } catch (IOException e) {
        LOG.error(
            "Failed to process {}, which could be corrupted or have a wrong format.", resourceId);
        throw new IOException(e);
      }

      // Advance once to load the first record.
      return advanceImpl();
    }

    @Override
    protected final boolean advanceImpl() throws IOException {
      return readNextRecord();
    }

    /**
     * Closes any {@link ReadableByteChannel} created for the current reader. This implementation is
     * idempotent. Any {@code close()} method introduced by a subclass must be idempotent and must
     * call the {@code close()} method in the {@code FileBasedReader}.
     */
    @Override
    public void close() throws IOException {
      if (channel != null) {
        channel.close();
      }
    }

    @Override
    public boolean allowsDynamicSplitting() {
      try {
        return getCurrentSource().isSplittable();
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Error determining if %s allows dynamic splitting", this), e);
      }
    }

    /**
     * Performs any initialization of the subclass of {@code FileBasedReader} that involves IO
     * operations. Will only be invoked once and before that invocation the base class will seek the
     * channel to the source's starting offset.
     *
     * <p>Provided {@link ReadableByteChannel} is for the file represented by the source of this
     * reader. Subclass may use the {@code channel} to build a higher level IO abstraction, e.g., a
     * BufferedReader or an XML parser.
     *
     * <p>If the corresponding source is for a subrange of a file, {@code channel} is guaranteed to
     * be an instance of the type {@link SeekableByteChannel}.
     *
     * <p>After this method is invoked the base class will not be reading data from the channel or
     * adjusting the position of the channel. But the base class is responsible for properly closing
     * the channel.
     *
     * @param channel a byte channel representing the file backing the reader.
     */
    protected abstract void startReading(ReadableByteChannel channel) throws IOException;

    /**
     * Reads the next record from the channel provided by {@link #startReading}. Methods {@link
     * #getCurrent}, {@link #getCurrentOffset}, and {@link #isAtSplitPoint()} should return the
     * corresponding information about the record read by the last invocation of this method.
     *
     * <p>Note that this method will be called the same way for reading the first record in the
     * source (file or offset range in the file) and for reading subsequent records. It is up to the
     * subclass to do anything special for locating and reading the first record, if necessary.
     *
     * @return {@code true} if a record was successfully read, {@code false} if the end of the
     *     channel was reached before successfully reading a new record.
     */
    protected abstract boolean readNextRecord() throws IOException;
  }

  // An internal Reader implementation that concatenates a sequence of FileBasedReaders.
  private class FilePatternReader extends BoundedReader<T> {
    private final FileBasedSource<T> source;
    private final List<FileBasedReader<T>> fileReaders;
    final ListIterator<FileBasedReader<T>> fileReadersIterator;

    // Initialized in start
    @Nullable FileBasedReader<T> currentReader = null;

    public FilePatternReader(FileBasedSource<T> source, List<FileBasedReader<T>> fileReaders) {
      this.source = source;
      this.fileReaders = fileReaders;
      this.fileReadersIterator = fileReaders.listIterator();
    }

    @Override
    public boolean start() throws IOException {
      return startNextNonemptyReader();
    }

    @Override
    public boolean advance() throws IOException {
      checkState(currentReader != null, "Call start() before advance()");
      if (currentReader.advance()) {
        return true;
      }
      return startNextNonemptyReader();
    }

    private boolean startNextNonemptyReader() throws IOException {
      while (fileReadersIterator.hasNext()) {
        currentReader = fileReadersIterator.next();
        if (currentReader.start()) {
          return true;
        }
        currentReader.close();
      }
      return false;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      // A NoSuchElement will be thrown by the last FileBasedReader if getCurrent() is called after
      // advance() returns false.
      return currentReader.getCurrent();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      // A NoSuchElement will be thrown by the last FileBasedReader if getCurrentTimestamp()
      // is called after advance() returns false.
      return currentReader.getCurrentTimestamp();
    }

    @Override
    public void close() throws IOException {
      // Close all readers that may have not yet been closed.
      // If this reader has not been started, currentReader is null.
      if (currentReader != null) {
        currentReader.close();
      }
      while (fileReadersIterator.hasNext()) {
        fileReadersIterator.next().close();
      }
    }

    @Override
    public FileBasedSource<T> getCurrentSource() {
      return source;
    }

    @Override
    public FileBasedSource<T> splitAtFraction(double fraction) {
      // Unsupported. TODO: implement.
      LOG.debug("Dynamic splitting of FilePatternReader is unsupported.");
      return null;
    }

    @Override
    public Double getFractionConsumed() {
      if (currentReader == null) {
        return 0.0;
      }
      if (fileReaders.isEmpty()) {
        return 1.0;
      }
      int index = fileReadersIterator.previousIndex();
      int numReaders = fileReaders.size();
      if (index == numReaders) {
        return 1.0;
      }
      double before = 1.0 * index / numReaders;
      double after = 1.0 * (index + 1) / numReaders;
      Double fractionOfCurrentReader = currentReader.getFractionConsumed();
      if (fractionOfCurrentReader == null) {
        return before;
      }
      return before + fractionOfCurrentReader * (after - before);
    }
  }
}
