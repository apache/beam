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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
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
 * <p>This source stores a {@code String} that is an {@link IOChannelFactory} specification for a
 * file or file pattern. There should be an {@code IOChannelFactory} defined for the file
 * specification provided. Please refer to {@link IOChannelUtils} and {@link IOChannelFactory} for
 * more information on this.
 *
 * <p>In addition to the methods left abstract from {@code BoundedSource}, subclasses must implement
 * methods to create a sub-source and a reader for a range of a single file -
 * {@link #createForSubrangeOfFile} and {@link #createSingleFileReader}. Please refer to
 * {@link XmlSource} for an example implementation of {@code FileBasedSource}.
 *
 * @param <T> Type of records represented by the source.
 */
public abstract class FileBasedSource<T> extends OffsetBasedSource<T> {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedSource.class);
  private static final float FRACTION_OF_FILES_TO_STAT = 0.01f;

  // Package-private for testing
  static final int MAX_NUMBER_OF_FILES_FOR_AN_EXACT_STAT = 100;

  // Size of the thread pool to be used for performing file operations in parallel.
  // Package-private for testing.
  static final int THREAD_POOL_SIZE = 128;

  private final ValueProvider<String> fileOrPatternSpec;
  private final Mode mode;

  /**
   * A given {@code FileBasedSource} represents a file resource of one of these types.
   */
  public enum Mode {
    FILEPATTERN,
    SINGLE_FILE_OR_SUBRANGE
  }

  /**
   * Create a {@code FileBaseSource} based on a file or a file pattern specification. This
   * constructor must be used when creating a new {@code FileBasedSource} for a file pattern.
   *
   * <p>See {@link OffsetBasedSource} for a detailed description of {@code minBundleSize}.
   *
   * @param fileOrPatternSpec {@link IOChannelFactory} specification of file or file pattern
   *        represented by the {@link FileBasedSource}.
   * @param minBundleSize minimum bundle size in bytes.
   */
  public FileBasedSource(String fileOrPatternSpec, long minBundleSize) {
    this(StaticValueProvider.of(fileOrPatternSpec), minBundleSize);
  }

  /**
   * Create a {@code FileBaseSource} based on a file or a file pattern specification.
   * Same as the {@code String} constructor, but accepting a {@link ValueProvider}
   * to allow for runtime configuration of the source.
   */
  public FileBasedSource(ValueProvider<String> fileOrPatternSpec, long minBundleSize) {
    super(0, Long.MAX_VALUE, minBundleSize);
    mode = Mode.FILEPATTERN;
    this.fileOrPatternSpec = fileOrPatternSpec;
  }

  /**
   * Create a {@code FileBasedSource} based on a single file. This constructor must be used when
   * creating a new {@code FileBasedSource} for a subrange of a single file.
   * Additionally, this constructor must be used to create new {@code FileBasedSource}s when
   * subclasses implement the method {@link #createForSubrangeOfFile}.
   *
   * <p>See {@link OffsetBasedSource} for detailed descriptions of {@code minBundleSize},
   * {@code startOffset}, and {@code endOffset}.
   *
   * @param fileName {@link IOChannelFactory} specification of the file represented by the
   *        {@link FileBasedSource}.
   * @param minBundleSize minimum bundle size in bytes.
   * @param startOffset starting byte offset.
   * @param endOffset ending byte offset. If the specified value {@code >= #getMaxEndOffset()} it
   *        implies {@code #getMaxEndOffSet()}.
   */
  public FileBasedSource(String fileName, long minBundleSize,
      long startOffset, long endOffset) {
    super(startOffset, endOffset, minBundleSize);
    mode = Mode.SINGLE_FILE_OR_SUBRANGE;
    this.fileOrPatternSpec = StaticValueProvider.of(fileName);
  }

  public final String getFileOrPatternSpec() {
    return fileOrPatternSpec.get();
  }

  public final ValueProvider<String> getFileOrPatternSpecProvider() {
    return fileOrPatternSpec;
  }

  public final Mode getMode() {
    return mode;
  }

  @Override
  public final FileBasedSource<T> createSourceForSubrange(long start, long end) {
    checkArgument(mode != Mode.FILEPATTERN,
        "Cannot split a file pattern based source based on positions");
    checkArgument(start >= getStartOffset(),
        "Start offset value %s of the subrange cannot be smaller than the start offset value %s"
            + " of the parent source",
        start,
        getStartOffset());
    checkArgument(end <= getEndOffset(),
        "End offset value %s of the subrange cannot be larger than the end offset value %s",
        end,
        getEndOffset());

    checkState(fileOrPatternSpec.isAccessible(),
               "Subrange creation should only happen at execution time.");
    FileBasedSource<T> source = createForSubrangeOfFile(fileOrPatternSpec.get(), start, end);
    if (start > 0 || end != Long.MAX_VALUE) {
      checkArgument(source.getMode() == Mode.SINGLE_FILE_OR_SUBRANGE,
          "Source created for the range [%s,%s) must be a subrange source", start, end);
    }
    return source;
  }

  /**
   * Creates and returns a new {@code FileBasedSource} of the same type as the current
   * {@code FileBasedSource} backed by a given file and an offset range. When current source is
   * being split, this method is used to generate new sub-sources. When creating the source
   * subclasses must call the constructor {@link #FileBasedSource(String, long, long, long)} of
   * {@code FileBasedSource} with corresponding parameter values passed here.
   *
   * @param fileName file backing the new {@code FileBasedSource}.
   * @param start starting byte offset of the new {@code FileBasedSource}.
   * @param end ending byte offset of the new {@code FileBasedSource}. May be Long.MAX_VALUE,
   *        in which case it will be inferred using {@link #getMaxEndOffset}.
   */
  protected abstract FileBasedSource<T> createForSubrangeOfFile(
      String fileName, long start, long end);

  /**
   * Creates and returns an instance of a {@code FileBasedReader} implementation for the current
   * source assuming the source represents a single file. File patterns will be handled by
   * {@code FileBasedSource} implementation automatically.
   */
  protected abstract FileBasedReader<T> createSingleFileReader(
      PipelineOptions options);

  @Override
  public final long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
    // This implementation of method getEstimatedSizeBytes is provided to simplify subclasses. Here
    // we perform the size estimation of files and file patterns using the interface provided by
    // IOChannelFactory.

    if (mode == Mode.FILEPATTERN) {
      checkState(fileOrPatternSpec.isAccessible(),
                 "Size estimation should be done at execution time.");
      IOChannelFactory factory = IOChannelUtils.getFactory(fileOrPatternSpec.get());
      // TODO Implement a more efficient parallel/batch size estimation mechanism for file patterns.
      long startTime = System.currentTimeMillis();
      long totalSize = 0;
      Collection<String> inputs = factory.match(fileOrPatternSpec.get());
      if (inputs.size() <= MAX_NUMBER_OF_FILES_FOR_AN_EXACT_STAT) {
        totalSize = getExactTotalSizeOfFiles(inputs, factory);
        LOG.debug("Size estimation of all files of pattern {} took {} ms",
            fileOrPatternSpec,
            System.currentTimeMillis() - startTime);
      } else {
        totalSize = getEstimatedSizeOfFilesBySampling(inputs, factory);
        LOG.debug("Size estimation of pattern {} by sampling took {} ms",
            fileOrPatternSpec,
            System.currentTimeMillis() - startTime);
      }
      LOG.info(
          "Filepattern {} matched {} files with total size {}",
          fileOrPatternSpec.get(),
          inputs.size(),
          totalSize);
      return totalSize;
    } else {
      long start = getStartOffset();
      long end = Math.min(getEndOffset(), getMaxEndOffset(options));
      return end - start;
    }
  }

  // Get the exact total size of the given set of files.
  // Invokes multiple requests for size estimation in parallel using a thread pool.
  // TODO: replace this with bulk request API when it is available. Will require updates
  // to IOChannelFactory interface.
  private static long getExactTotalSizeOfFiles(
      Collection<String> files, IOChannelFactory ioChannelFactory) throws IOException {
    List<ListenableFuture<Long>> futures = new ArrayList<>();
    ListeningExecutorService service =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(THREAD_POOL_SIZE));
    try {
      long totalSize = 0;
      for (String file : files) {
        futures.add(createFutureForSizeEstimation(file, ioChannelFactory, service));
      }

      for (Long val : Futures.allAsList(futures).get()) {
        totalSize += val;
      }

      return totalSize;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }  finally {
      service.shutdown();
    }
  }

  private static ListenableFuture<Long> createFutureForSizeEstimation(
      final String file,
      final IOChannelFactory ioChannelFactory,
      ListeningExecutorService service) {
    return service.submit(
        new Callable<Long>() {
          @Override
          public Long call() throws IOException {
            return ioChannelFactory.getSizeBytes(file);
          }
        });
  }

  // Estimate the total size of the given set of files through sampling and extrapolation.
  // Currently we use uniform sampling which requires a linear sampling size for a reasonable
  // estimate.
  // TODO: Implement a more efficient sampling mechanism.
  private static long getEstimatedSizeOfFilesBySampling(
      Collection<String> files, IOChannelFactory ioChannelFactory) throws IOException {
    int sampleSize = (int) (FRACTION_OF_FILES_TO_STAT * files.size());
    sampleSize = Math.max(MAX_NUMBER_OF_FILES_FOR_AN_EXACT_STAT, sampleSize);

    List<String> selectedFiles = new ArrayList<String>(files);
    Collections.shuffle(selectedFiles);
    selectedFiles = selectedFiles.subList(0, sampleSize);

    long exactTotalSampleSize = getExactTotalSizeOfFiles(selectedFiles, ioChannelFactory);
    double avgSize = 1.0 * exactTotalSampleSize / selectedFiles.size();
    long totalSize = Math.round(files.size() * avgSize);
    LOG.info(
        "Sampling {} files gave {} total bytes ({} average per file), "
            + "inferring total size of {} files to be {}",
        selectedFiles.size(),
        exactTotalSampleSize,
        avgSize,
        files.size(),
        totalSize);
    return totalSize;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    String patternDisplay = getFileOrPatternSpecProvider().isAccessible()
      ? getFileOrPatternSpecProvider().get()
      : getFileOrPatternSpecProvider().toString();
    builder.add(DisplayData.item("filePattern", patternDisplay)
      .withLabel("File Pattern"));
  }

  private ListenableFuture<List<? extends FileBasedSource<T>>> createFutureForFileSplit(
      final String file,
      final long desiredBundleSizeBytes,
      final PipelineOptions options,
      ListeningExecutorService service) {
    return service.submit(new Callable<List<? extends FileBasedSource<T>>>() {
      @Override
      public List<? extends FileBasedSource<T>> call() throws Exception {
        return createForSubrangeOfFile(file, 0, Long.MAX_VALUE)
            .split(desiredBundleSizeBytes, options);
      }
    });
  }

  @Override
  public final List<? extends FileBasedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    // This implementation of method split is provided to simplify subclasses. Here we
    // split a FileBasedSource based on a file pattern to FileBasedSources based on full single
    // files. For files that can be efficiently seeked, we further split FileBasedSources based on
    // those files to FileBasedSources based on sub ranges of single files.

    if (mode == Mode.FILEPATTERN) {
      long startTime = System.currentTimeMillis();
      List<ListenableFuture<List<? extends FileBasedSource<T>>>> futures = new ArrayList<>();

      ListeningExecutorService service =
          MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(THREAD_POOL_SIZE));
      try {
        checkState(fileOrPatternSpec.isAccessible(),
                   "Bundle splitting should only happen at execution time.");
        Collection<String> expandedFiles =
            FileBasedSource.expandFilePattern(fileOrPatternSpec.get());
        checkArgument(!expandedFiles.isEmpty(),
            "Unable to find any files matching %s", fileOrPatternSpec.get());
        for (final String file : expandedFiles) {
          futures.add(createFutureForFileSplit(file, desiredBundleSizeBytes, options, service));
        }
        List<? extends FileBasedSource<T>> splitResults =
            ImmutableList.copyOf(Iterables.concat(Futures.allAsList(futures).get()));
        LOG.info(
            "Splitting filepattern {} into bundles of size {} took {} ms "
                + "and produced {} files and {} bundles",
            fileOrPatternSpec,
            desiredBundleSizeBytes,
            System.currentTimeMillis() - startTime,
            expandedFiles.size(),
            splitResults.size());
        return splitResults;
      } finally {
        service.shutdown();
      }
    } else {
      if (isSplittable()) {
        List<FileBasedSource<T>> splitResults = new ArrayList<>();
        for (OffsetBasedSource<T> split :
            super.split(desiredBundleSizeBytes, options)) {
          splitResults.add((FileBasedSource<T>) split);
        }
        return splitResults;
      } else {
        LOG.debug("The source for file {} is not split into sub-range based sources since "
            + "the file is not seekable",
            fileOrPatternSpec);
        return ImmutableList.of(this);
      }
    }
  }

  /**
   * Determines whether a file represented by this source is can be split into bundles.
   *
   * <p>By default, a file is splittable if it is on a file system that supports efficient read
   * seeking. Subclasses may override to provide different behavior.
   */
  protected boolean isSplittable() throws Exception {
    // We split a file-based source into subranges only if the file is efficiently seekable.
    // If a file is not efficiently seekable it would be highly inefficient to create and read a
    // source based on a subrange of that file.
    checkState(fileOrPatternSpec.isAccessible(),
        "isSplittable should only be called at runtime.");
    IOChannelFactory factory = IOChannelUtils.getFactory(fileOrPatternSpec.get());
    return factory.isReadSeekEfficient(fileOrPatternSpec.get());
  }

  @Override
  public final BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    // Validate the current source prior to creating a reader for it.
    this.validate();

    if (mode == Mode.FILEPATTERN) {
      long startTime = System.currentTimeMillis();
      Collection<String> files = FileBasedSource.expandFilePattern(fileOrPatternSpec.get());
      List<FileBasedReader<T>> fileReaders = new ArrayList<>();
      for (String fileName : files) {
        long endOffset;
        try {
          endOffset = IOChannelUtils.getFactory(fileName).getSizeBytes(fileName);
        } catch (IOException e) {
          LOG.warn("Failed to get size of {}", fileName, e);
          endOffset = Long.MAX_VALUE;
        }
        fileReaders.add(
            createForSubrangeOfFile(fileName, 0, endOffset).createSingleFileReader(options));
      }
      LOG.debug(
          "Creating a reader for file pattern {} took {} ms",
          fileOrPatternSpec,
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
    String fileString = fileOrPatternSpec.isAccessible()
        ? fileOrPatternSpec.get() : fileOrPatternSpec.toString();
    switch (mode) {
      case FILEPATTERN:
        return fileString;
      case SINGLE_FILE_OR_SUBRANGE:
        return fileString + " range " + super.toString();
      default:
        throw new IllegalStateException("Unexpected mode: " + mode);
    }
  }

  @Override
  public void validate() {
    super.validate();
    switch (mode) {
      case FILEPATTERN:
        checkArgument(getStartOffset() == 0,
            "FileBasedSource is based on a file pattern or a full single file "
            + "but the starting offset proposed %s is not zero", getStartOffset());
        checkArgument(getEndOffset() == Long.MAX_VALUE,
            "FileBasedSource is based on a file pattern or a full single file "
            + "but the ending offset proposed %s is not Long.MAX_VALUE", getEndOffset());
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
    if (getEndOffset() == Long.MAX_VALUE) {
      IOChannelFactory factory = IOChannelUtils.getFactory(fileOrPatternSpec.get());
      return factory.getSizeBytes(fileOrPatternSpec.get());
    } else {
      return getEndOffset();
    }
  }

  protected static final Collection<String> expandFilePattern(String fileOrPatternSpec)
      throws IOException {
    IOChannelFactory factory = IOChannelUtils.getFactory(fileOrPatternSpec);
    Collection<String> matches = factory.match(fileOrPatternSpec);
    LOG.info("Matched {} files for pattern {}", matches.size(), fileOrPatternSpec);
    return matches;
  }

  /**
   * A {@link Source.Reader reader} that implements code common to readers of
   * {@code FileBasedSource}s.
   *
   * <h2>Seekability</h2>
   *
   * <p>This reader uses a {@link ReadableByteChannel} created for the file represented by the
   * corresponding source to efficiently move to the correct starting position defined in the
   * source. Subclasses of this reader should implement {@link #startReading} to get access to this
   * channel. If the source corresponding to the reader is for a subrange of a file the
   * {@code ReadableByteChannel} provided is guaranteed to be an instance of the type
   * {@link SeekableByteChannel}, which may be used by subclass to traverse back in the channel to
   * determine the correct starting position.
   *
   * <h2>Reading Records</h2>
   *
   * <p>Sequential reading is implemented using {@link #readNextRecord}.
   *
   * <p>Then {@code FileBasedReader} implements "reading a range [A, B)" in the following way.
   * <ol>
   * <li>{@link #start} opens the file
   * <li>{@link #start} seeks the {@code SeekableByteChannel} to A (reading offset ranges for
   * non-seekable files is not supported) and calls {@code startReading()}
   * <li>{@link #start} calls {@link #advance} once, which, via {@link #readNextRecord},
   * locates the first record which is at a split point AND its offset is at or after A.
   * If this record is at or after B, {@link #advance} returns false and reading is finished.
   * <li>if the previous advance call returned {@code true} sequential reading starts and
   * {@code advance()} will be called repeatedly
   * </ol>
   * {@code advance()} calls {@code readNextRecord()} on the subclass, and stops (returns false) if
   * the new record is at a split point AND the offset of the new record is at or after B.
   *
   * <h2>Thread Safety</h2>
   *
   * <p>Since this class implements {@link Source.Reader} it guarantees thread safety. Abstract
   * methods defined here will not be accessed by more than one thread concurrently.
   */
  public abstract static class FileBasedReader<T> extends OffsetBasedReader<T> {
    private ReadableByteChannel channel = null;

    /**
     * Subclasses should not perform IO operations at the constructor. All IO operations should be
     * delayed until the {@link #startReading} method is invoked.
     */
    public FileBasedReader(FileBasedSource<T> source) {
      super(source);
      checkArgument(source.getMode() != Mode.FILEPATTERN,
          "FileBasedReader does not support reading file patterns");
    }

    @Override
    public synchronized FileBasedSource<T> getCurrentSource() {
      return (FileBasedSource<T>) super.getCurrentSource();
    }

    @Override
    protected final boolean startImpl() throws IOException {
      FileBasedSource<T> source = getCurrentSource();
      IOChannelFactory factory = IOChannelUtils.getFactory(
        source.getFileOrPatternSpecProvider().get());
      this.channel = factory.open(source.getFileOrPatternSpecProvider().get());

      if (channel instanceof SeekableByteChannel) {
        SeekableByteChannel seekChannel = (SeekableByteChannel) channel;
        seekChannel.position(source.getStartOffset());
      } else {
        // Channel is not seekable. Must not be a subrange.
        checkArgument(source.mode != Mode.SINGLE_FILE_OR_SUBRANGE,
            "Subrange-based sources must only be defined for file types that support seekable "
            + " read channels");
        checkArgument(source.getStartOffset() == 0,
            "Start offset %s is not zero but channel for reading the file is not seekable.",
            source.getStartOffset());
      }

      startReading(channel);

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
     * Reads the next record from the channel provided by {@link #startReading}. Methods
     * {@link #getCurrent}, {@link #getCurrentOffset}, and {@link #isAtSplitPoint()} should return
     * the corresponding information about the record read by the last invocation of this method.
     *
     * <p>Note that this method will be called the same way for reading the first record in the
     * source (file or offset range in the file) and for reading subsequent records. It is up to the
     * subclass to do anything special for locating and reading the first record, if necessary.
     *
     * @return {@code true} if a record was successfully read, {@code false} if the end of the
     *         channel was reached before successfully reading a new record.
     */
    protected abstract boolean readNextRecord() throws IOException;
  }

  // An internal Reader implementation that concatenates a sequence of FileBasedReaders.
  private class FilePatternReader extends BoundedReader<T> {
    private final FileBasedSource<T> source;
    private final List<FileBasedReader<T>> fileReaders;
    final ListIterator<FileBasedReader<T>> fileReadersIterator;
    FileBasedReader<T> currentReader = null;

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
