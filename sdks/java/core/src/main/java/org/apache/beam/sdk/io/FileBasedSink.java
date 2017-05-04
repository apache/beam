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

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verifyNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.Context;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.WindowedContext;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for file-based output. An implementation of FileBasedSink writes file-based
 * output and defines the format of output files (how values are written, headers/footers, MIME
 * type, etc.).
 *
 * <p>At pipeline construction time, the methods of FileBasedSink are called to validate the sink
 * and to create a {@link FileBasedWriteOperation} that manages the process of writing to the sink.
 *
 * <p>The process of writing to file-based sink is as follows:
 * <ol>
 * <li>An optional subclass-defined initialization,
 * <li>a parallel write of bundles to temporary files, and finally,
 * <li>these temporary files are renamed with final output filenames.
 * </ol>
 *
 * <p>In order to ensure fault-tolerance, a bundle may be executed multiple times (e.g., in the
 * event of failure/retry or for redundancy). However, exactly one of these executions will have its
 * result passed to the finalize method. Each call to {@link FileBasedWriter#openWindowed}
 * or {@link FileBasedWriter#openUnwindowed} is passed a unique <i>bundle id</i> when it is called
 * by the WriteFiles transform, so even redundant or retried bundles will have a unique way of
 * identifying
 * their output.
 *
 * <p>The bundle id should be used to guarantee that a bundle's output is unique. This uniqueness
 * guarantee is important; if a bundle is to be output to a file, for example, the name of the file
 * will encode the unique bundle id to avoid conflicts with other writers.
 *
 * {@link FileBasedSink} can take a custom {@link FilenamePolicy} object to determine output
 * filenames, and this policy object can be used to write windowed or triggered
 * PCollections into separate files per window pane. This allows file output from unbounded
 * PCollections, and also works for bounded PCollecctions.
 *
 * <p>Supported file systems are those registered with {@link FileSystems}.
 *
 * @param <T> the type of values written to the sink.
 */
public abstract class FileBasedSink<T> implements Serializable, HasDisplayData {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedSink.class);

  /**
   * Directly supported file output compression types.
   */
  public enum CompressionType implements WritableByteChannelFactory {
    /**
     * No compression, or any other transformation, will be used.
     */
    UNCOMPRESSED("", null) {
      @Override
      public WritableByteChannel create(WritableByteChannel channel) throws IOException {
        return channel;
      }
    },
    /**
     * Provides GZip output transformation.
     */
    GZIP(".gz", MimeTypes.BINARY) {
      @Override
      public WritableByteChannel create(WritableByteChannel channel) throws IOException {
        return Channels.newChannel(new GZIPOutputStream(Channels.newOutputStream(channel), true));
      }
    },
    /**
     * Provides BZip2 output transformation.
     */
    BZIP2(".bz2", MimeTypes.BINARY) {
      @Override
      public WritableByteChannel create(WritableByteChannel channel) throws IOException {
        return Channels
            .newChannel(new BZip2CompressorOutputStream(Channels.newOutputStream(channel)));
      }
    },
    /**
     * Provides deflate output transformation.
     */
    DEFLATE(".deflate", MimeTypes.BINARY) {
      @Override
      public WritableByteChannel create(WritableByteChannel channel) throws IOException {
        return Channels
            .newChannel(new DeflateCompressorOutputStream(Channels.newOutputStream(channel)));
      }
    };

    private String filenameSuffix;
    @Nullable private String mimeType;

    CompressionType(String suffix, @Nullable String mimeType) {
      this.filenameSuffix = suffix;
      this.mimeType = mimeType;
    }

    @Override
    public String getFilenameSuffix() {
      return filenameSuffix;
    }

    @Override
    @Nullable public String getMimeType() {
      return mimeType;
    }
  }

  /**
   * This is a helper function for turning a user-provided output filename prefix and converting it
   * into a {@link ResourceId} for writing output files. See {@link TextIO.Write#to(String)} for an
   * example use case.
   *
   * <p>Typically, the input prefix will be something like {@code /tmp/foo/bar}, and the user would
   * like output files to be named as {@code /tmp/foo/bar-0-of-3.txt}. Thus, this function tries to
   * interpret the provided string as a file {@link ResourceId} path.
   *
   * <p>However, this may fail, for example if the user gives a prefix that is a directory. E.g.,
   * {@code /}, {@code gs://my-bucket}, or {@code c://}. In that case, interpreting the string as a
   * file will fail and this function will return a directory {@link ResourceId} instead.
   */
  public static ResourceId convertToFileResourceIfPossible(String outputPrefix) {
    try {
      return FileSystems.matchNewResource(outputPrefix, false /* isDirectory */);
    } catch (Exception e) {
      return FileSystems.matchNewResource(outputPrefix, true /* isDirectory */);
    }
  }

  /**
   * The {@link WritableByteChannelFactory} that is used to wrap the raw data output to the
   * underlying channel. The default is to not compress the output using
   * {@link CompressionType#UNCOMPRESSED}.
   */
  private final WritableByteChannelFactory writableByteChannelFactory;

  /**
   * A naming policy for output files.
   */
  public abstract static class FilenamePolicy implements Serializable {
    /**
     * Context used for generating a name based on shard number, and num shards.
     * The policy must produce unique filenames for unique {@link Context} objects.
     *
     * <p>Be careful about adding fields to this as existing strategies will not notice the new
     * fields, and may not produce unique filenames.
     */
    public static class Context {
      private int shardNumber;
      private int numShards;


      public Context(int shardNumber, int numShards) {
        this.shardNumber = shardNumber;
        this.numShards = numShards;
      }

      public int getShardNumber() {
        return shardNumber;
      }


      public int getNumShards() {
        return numShards;
      }
    }

    /**
     * Context used for generating a name based on window, pane, shard number, and num shards.
     * The policy must produce unique filenames for unique {@link WindowedContext} objects.
     *
     * <p>Be careful about adding fields to this as existing strategies will not notice the new
     * fields, and may not produce unique filenames.
     */
    public static class WindowedContext {
      private int shardNumber;
      private int numShards;
      private BoundedWindow window;
      private PaneInfo paneInfo;

      public WindowedContext(
          BoundedWindow window,
          PaneInfo paneInfo,
          int shardNumber,
          int numShards) {
        this.window = window;
        this.paneInfo = paneInfo;
        this.shardNumber = shardNumber;
        this.numShards = numShards;
      }

      public BoundedWindow getWindow() {
        return window;
      }

      public PaneInfo getPaneInfo() {
        return paneInfo;
      }

      public int getShardNumber() {
        return shardNumber;
      }

      public int getNumShards() {
        return numShards;
      }
    }

    /**
     * When a sink has requested windowed or triggered output, this method will be invoked to return
     * the file {@link ResourceId resource} to be created given the base output directory and a
     * (possibly empty) extension from {@link FileBasedSink} configuration
     * (e.g., {@link CompressionType}).
     *
     * <p>The {@link WindowedContext} object gives access to the window and pane,
     * as well as sharding information. The policy must return unique and consistent filenames
     * for different windows and panes.
     */
    public abstract ResourceId windowedFilename(
        ResourceId outputDirectory, WindowedContext c, String extension);

    /**
     * When a sink has not requested windowed or triggered output, this method will be invoked to
     * return the file {@link ResourceId resource} to be created given the base output directory and
     * a (possibly empty) extension applied by additional {@link FileBasedSink} configuration
     * (e.g., {@link CompressionType}).
     *
     * <p>The {@link Context} object only provides sharding information, which is used by the policy
     * to generate unique and consistent filenames.
     */
    @Nullable public abstract ResourceId unwindowedFilename(
        ResourceId outputDirectory, Context c, String extension);

    /**
     * Populates the display data.
     */
    public void populateDisplayData(DisplayData.Builder builder) {
    }
  }

  /** The policy used to generate names of files to be produced. */
  @VisibleForTesting
  final FilenamePolicy filenamePolicy;
  /** The directory to which files will be written. */
  private final ValueProvider<ResourceId> baseOutputDirectoryProvider;

  /**
   * Construct a {@link FileBasedSink} with the given filename policy, producing uncompressed files.
   */
  public FileBasedSink(
      ValueProvider<ResourceId> baseOutputDirectoryProvider, FilenamePolicy filenamePolicy) {
    this(baseOutputDirectoryProvider, filenamePolicy, CompressionType.UNCOMPRESSED);
  }

  private static class ExtractDirectory implements SerializableFunction<ResourceId, ResourceId> {
    @Override
    public ResourceId apply(ResourceId input) {
      return input.getCurrentDirectory();
    }
  }

  /**
   * Construct a {@link FileBasedSink} with the given filename policy and output channel type.
   */
  public FileBasedSink(
      ValueProvider<ResourceId> baseOutputDirectoryProvider,
      FilenamePolicy filenamePolicy,
      WritableByteChannelFactory writableByteChannelFactory) {
    this.baseOutputDirectoryProvider =
        NestedValueProvider.of(baseOutputDirectoryProvider, new ExtractDirectory());
    this.filenamePolicy = filenamePolicy;
    this.writableByteChannelFactory = writableByteChannelFactory;
  }

  /**
   * Returns the base directory inside which files will be written according to the configured
   * {@link FilenamePolicy}.
   */
  public ValueProvider<ResourceId> getBaseOutputDirectoryProvider() {
    return baseOutputDirectoryProvider;
  }

  /**
   * Returns the policy by which files will be named inside of the base output directory. Note that
   * the {@link FilenamePolicy} may itself specify one or more inner directories before each output
   * file, say when writing windowed outputs in a {@code output/YYYY/MM/DD/file.txt} format.
   */
  public final FilenamePolicy getFilenamePolicy() {
    return filenamePolicy;
  }

  public void validate(PipelineOptions options) {}

  /**
   * Return a subclass of {@link FileBasedSink.FileBasedWriteOperation} that will manage the write
   * to the sink.
   */
  public abstract FileBasedWriteOperation<T> createWriteOperation();

  public void populateDisplayData(DisplayData.Builder builder) {
    getFilenamePolicy().populateDisplayData(builder);
  }

  /**
   * Abstract operation that manages the process of writing to {@link FileBasedSink}.
   *
   * <p>The primary responsibilities of the FileBasedWriteOperation is the management of output
   * files. During a write, {@link FileBasedSink.FileBasedWriter}s write bundles to temporary file
   * locations. After the bundles have been written,
   * <ol>
   * <li>{@link FileBasedSink.FileBasedWriteOperation#finalize} is given a list of the temporary
   * files containing the output bundles.
   * <li>During finalize, these temporary files are copied to final output locations and named
   * according to a file naming template.
   * <li>Finally, any temporary files that were created during the write are removed.
   * </ol>
   *
   * <p>Subclass implementations of FileBasedWriteOperation must implement
   * {@link FileBasedSink.FileBasedWriteOperation#createWriter} to return a concrete
   * FileBasedSinkWriter.
   *
   * <h2>Temporary and Output File Naming:</h2> During the write, bundles are written to temporary
   * files using the tempDirectory that can be provided via the constructor of
   * FileBasedWriteOperation. These temporary files will be named
   * {@code {tempDirectory}/{bundleId}}, where bundleId is the unique id of the bundle.
   * For example, if tempDirectory is "gs://my-bucket/my_temp_output", the output for a
   * bundle with bundle id 15723 will be "gs://my-bucket/my_temp_output/15723".
   *
   * <p>Final output files are written to baseOutputFilename with the format
   * {@code {baseOutputFilename}-0000i-of-0000n.{extension}} where n is the total number of bundles
   * written and extension is the file extension. Both baseOutputFilename and extension are required
   * constructor arguments.
   *
   * <p>Subclass implementations can change the file naming template by supplying a value for
   * fileNamingTemplate.
   *
   * <p>Note that in the case of permanent failure of a bundle's write, no clean up of temporary
   * files will occur.
   *
   * <p>If there are no elements in the PCollection being written, no output will be generated.
   *
   * @param <T> the type of values written to the sink.
   */
  public abstract static class FileBasedWriteOperation<T> implements Serializable {
    /**
     * The Sink that this WriteOperation will write to.
     */
    protected final FileBasedSink<T> sink;

    /** Directory for temporary output files. */
    protected final ValueProvider<ResourceId> tempDirectory;

    /** Whether windowed writes are being used. */
    protected  boolean windowedWrites;

    /** Constructs a temporary file resource given the temporary directory and a filename. */
    protected static ResourceId buildTemporaryFilename(ResourceId tempDirectory, String filename)
        throws IOException {
      return tempDirectory.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }

    /**
     * Constructs a FileBasedWriteOperation using the default strategy for generating a temporary
     * directory from the base output filename.
     *
     * <p>Default is a uniquely named sibling of baseOutputFilename, e.g. if baseOutputFilename is
     * /path/to/foo, the temporary directory will be /path/to/temp-beam-foo-$date.
     *
     * @param sink the FileBasedSink that will be used to configure this write operation.
     */
    public FileBasedWriteOperation(FileBasedSink<T> sink) {
      this(sink, NestedValueProvider.of(
          sink.getBaseOutputDirectoryProvider(), new TemporaryDirectoryBuilder()));
    }

    private static class TemporaryDirectoryBuilder
        implements SerializableFunction<ResourceId, ResourceId> {
      private static final AtomicLong TEMP_COUNT = new AtomicLong(0);
      private static final DateTimeFormatter TEMPDIR_TIMESTAMP =
          DateTimeFormat.forPattern("yyyy-MM-DD_HH-mm-ss");
      // The intent of the code is to have a consistent value of tempDirectory across
      // all workers, which wouldn't happen if now() was called inline.
      private final String timestamp = Instant.now().toString(TEMPDIR_TIMESTAMP);
      // Multiple different sinks may be used in the same output directory; use tempId to create a
      // separate temp directory for each.
      private final Long tempId = TEMP_COUNT.getAndIncrement();

      @Override
      public ResourceId apply(ResourceId baseOutputDirectory) {
        // Temp directory has a timestamp and a unique ID
        String tempDirName = String.format(".temp-beam-%s-%s", timestamp, tempId);
        return baseOutputDirectory.resolve(tempDirName, StandardResolveOptions.RESOLVE_DIRECTORY);
      }
    }

    /**
     * Create a new FileBasedWriteOperation.
     *
     * @param sink the FileBasedSink that will be used to configure this write operation.
     * @param tempDirectory the base directory to be used for temporary output files.
     */
    public FileBasedWriteOperation(FileBasedSink<T> sink, ResourceId tempDirectory) {
      this(sink, StaticValueProvider.of(tempDirectory));
    }

    private FileBasedWriteOperation(
        FileBasedSink<T> sink, ValueProvider<ResourceId> tempDirectory) {
      this.sink = sink;
      this.tempDirectory = tempDirectory;
      this.windowedWrites = false;
    }

    /**
     * Clients must implement to return a subclass of {@link FileBasedSink.FileBasedWriter}. This
     * method must not mutate the state of the object.
     */
    public abstract FileBasedWriter<T> createWriter() throws Exception;

    /**
     * Indicates that the operation will be performing windowed writes.
     */
    public void setWindowedWrites(boolean windowedWrites) {
      this.windowedWrites = windowedWrites;
    }

    /**
     * Finalizes writing by copying temporary output files to their final location and optionally
     * removing temporary files.
     *
     * <p>Finalization may be overridden by subclass implementations to perform customized
     * finalization (e.g., initiating some operation on output bundles, merging them, etc.).
     * {@code writerResults} contains the filenames of written bundles.
     *
     * <p>If subclasses override this method, they must guarantee that its implementation is
     * idempotent, as it may be executed multiple times in the case of failure or for redundancy. It
     * is a best practice to attempt to try to make this method atomic.
     *
     * @param writerResults the results of writes (FileResult).
     */
    public void finalize(Iterable<FileResult> writerResults) throws Exception {
      // Collect names of temporary files and rename them.
      Map<ResourceId, ResourceId> outputFilenames = buildOutputFilenames(writerResults);
      copyToOutputFiles(outputFilenames);

      // Optionally remove temporary files.
      // We remove the entire temporary directory, rather than specifically removing the files
      // from writerResults, because writerResults includes only successfully completed bundles,
      // and we'd like to clean up the failed ones too.
      // Note that due to GCS eventual consistency, matching files in the temp directory is also
      // currently non-perfect and may fail to delete some files.
      //
      // When windows or triggers are specified, files are generated incrementally so deleting
      // the entire directory in finalize is incorrect.
      removeTemporaryFiles(outputFilenames.keySet(), !windowedWrites);
    }

    protected final Map<ResourceId, ResourceId> buildOutputFilenames(
        Iterable<FileResult> writerResults) {
      Map<ResourceId, ResourceId> outputFilenames = new HashMap<>();
      List<ResourceId> files = new ArrayList<>();
      for (FileResult result : writerResults) {
        if (result.getDestinationFilename() != null) {
          outputFilenames.put(result.getFilename(), result.getDestinationFilename());
        } else {
          files.add(result.getFilename());
        }
      }

      // writerResults won't contain destination filenames, so we dynamically generate them here.
      if (files.size() > 0) {
        checkArgument(outputFilenames.isEmpty());
        // Sort files for idempotence.
        files = Ordering.usingToString().sortedCopy(files);
        ResourceId outputDirectory = getSink().getBaseOutputDirectoryProvider().get();
        FilenamePolicy filenamePolicy = getSink().filenamePolicy;
        for (int i = 0; i < files.size(); i++) {
          outputFilenames.put(files.get(i),
              filenamePolicy.unwindowedFilename(outputDirectory, new Context(i, files.size()),
                  getSink().getExtension()));
        }
      }

      int numDistinctShards = new HashSet<ResourceId>(outputFilenames.values()).size();
      checkState(numDistinctShards == outputFilenames.size(),
         "Only generated %s distinct file names for %s files.",
         numDistinctShards, outputFilenames.size());

      return outputFilenames;
    }

    /**
     * Copy temporary files to final output filenames using the file naming template.
     *
     * <p>Can be called from subclasses that override {@link FileBasedWriteOperation#finalize}.
     *
     * <p>Files will be named according to the file naming template. The order of the output files
     * will be the same as the sorted order of the input filenames.  In other words, if the input
     * filenames are ["C", "A", "B"], baseOutputFilename is "file", the extension is ".txt", and
     * the fileNamingTemplate is "-SSS-of-NNN", the contents of A will be copied to
     * file-000-of-003.txt, the contents of B will be copied to file-001-of-003.txt, etc.
     *
     * @param filenames the filenames of temporary files.
     */
    @VisibleForTesting
    final void copyToOutputFiles(Map<ResourceId, ResourceId> filenames)
        throws IOException {
      int numFiles = filenames.size();
      if (numFiles > 0) {
        LOG.debug("Copying {} files.", numFiles);
        List<ResourceId> srcFiles = new ArrayList<>(filenames.size());
        List<ResourceId> dstFiles = new ArrayList<>(filenames.size());
        for (Map.Entry<ResourceId, ResourceId> srcDestPair : filenames.entrySet()) {
          srcFiles.add(srcDestPair.getKey());
          dstFiles.add(srcDestPair.getValue());
        }
        // During a failure case, files may have been deleted in an earlier step. Thus
        // we ignore missing files here.
        FileSystems.copy(srcFiles, dstFiles, StandardMoveOptions.IGNORE_MISSING_FILES);
      } else {
        LOG.info("No output files to write.");
      }
    }

    /**
     * Removes temporary output files. Uses the temporary directory to find files to remove.
     *
     * <p>Can be called from subclasses that override {@link FileBasedWriteOperation#finalize}.
     * <b>Note:</b>If finalize is overridden and does <b>not</b> rename or otherwise finalize
     * temporary files, this method will remove them.
     */
    @VisibleForTesting
    final void removeTemporaryFiles(
        Set<ResourceId> knownFiles, boolean shouldRemoveTemporaryDirectory) throws IOException {
      ResourceId tempDir = tempDirectory.get();
      LOG.debug("Removing temporary bundle output files in {}.", tempDir);

      // To partially mitigate the effects of filesystems with eventually-consistent
      // directory matching APIs, we remove not only files that the filesystem says exist
      // in the directory (which may be incomplete), but also files that are known to exist
      // (produced by successfully completed bundles).

      // This may still fail to remove temporary outputs of some failed bundles, but at least
      // the common case (where all bundles succeed) is guaranteed to be fully addressed.
      Set<ResourceId> matches = new HashSet<>();
      // TODO: Windows OS cannot resolves and matches '*' in the path,
      // ignore the exception for now to avoid failing the pipeline.
      if (shouldRemoveTemporaryDirectory) {
        try {
          MatchResult singleMatch = Iterables.getOnlyElement(
              FileSystems.match(Collections.singletonList(tempDir.toString() + "*")));
          for (Metadata matchResult : singleMatch.metadata()) {
            matches.add(matchResult.resourceId());
          }
        } catch (Exception e) {
          LOG.warn("Failed to match temporary files under: [{}].", tempDir);
        }
      }
      Set<ResourceId> allMatches = new HashSet<>(matches);
      allMatches.addAll(knownFiles);
      LOG.debug(
          "Removing {} temporary files found under {} ({} matched glob, {} known files)",
          allMatches.size(),
          tempDir,
          matches.size(),
          allMatches.size() - matches.size());
      FileSystems.delete(allMatches, StandardMoveOptions.IGNORE_MISSING_FILES);

      // Deletion of the temporary directory might fail, if not all temporary files are removed.
      try {
        FileSystems.delete(
            Collections.singletonList(tempDir), StandardMoveOptions.IGNORE_MISSING_FILES);
      } catch (Exception e) {
        LOG.warn("Failed to remove temporary directory: [{}].", tempDir);
      }
    }

    /**
     * Returns the FileBasedSink for this write operation.
     */
    public FileBasedSink<T> getSink() {
      return sink;
    }
  }

  /** Returns the extension that will be written to the produced files. */
  protected final String getExtension() {
    String extension = MoreObjects.firstNonNull(writableByteChannelFactory.getFilenameSuffix(), "");
    if (!extension.isEmpty() && !extension.startsWith(".")) {
      extension = "." + extension;
    }
    return extension;
  }

  /**
   * Abstract writer that writes a bundle to a {@link FileBasedSink}. Subclass
   * implementations provide a method that can write a single value to a
   * {@link WritableByteChannel}.
   *
   * <p>Subclass implementations may also override methods that write headers and footers before and
   * after the values in a bundle, respectively, as well as provide a MIME type for the output
   * channel.
   *
   * <p>Multiple {@link FileBasedWriter} instances may be created on the same worker, and therefore
   * any access to static members or methods should be thread safe.
   *
   * @param <T> the type of values to write.
   */
  public abstract static class FileBasedWriter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(FileBasedWriter.class);

    private final FileBasedWriteOperation<T> writeOperation;

    /** Unique id for this output bundle. */
    private String id;

    private BoundedWindow window;
    private PaneInfo paneInfo;
    private int shard = -1;
    private int numShards = -1;

    /** The output file for this bundle. May be null if opening failed. */
    private @Nullable ResourceId outputFile;

    /**
     * The channel to write to.
     */
    private WritableByteChannel channel;

    /**
     * The MIME type used in the creation of the output channel (if the file system supports it).
     *
     * <p>This is the default for the sink, but it may be overridden by a supplied
     * {@link WritableByteChannelFactory}. For example, {@link TextIO.Write} uses
     * {@link MimeTypes#TEXT} by default but if {@link CompressionType#BZIP2} is set then
     * the MIME type will be overridden to {@link MimeTypes#BINARY}.
     */
    private final String mimeType;

    /**
     * Construct a new {@link FileBasedWriter} that will produce files of the given MIME type.
     */
    public FileBasedWriter(FileBasedWriteOperation<T> writeOperation, String mimeType) {
      checkNotNull(writeOperation);
      this.writeOperation = writeOperation;
      this.mimeType = mimeType;
    }

    /**
     * Called with the channel that a subclass will write its header, footer, and values to.
     * Subclasses should either keep a reference to the channel provided or create and keep a
     * reference to an appropriate object that they will use to write to it.
     *
     * <p>Called before any subsequent calls to writeHeader, writeFooter, and write.
     */
    protected abstract void prepareWrite(WritableByteChannel channel) throws Exception;

    /**
     * Writes header at the beginning of output files. Nothing by default; subclasses may override.
     */
    protected void writeHeader() throws Exception {}

    /**
     * Writes footer at the end of output files. Nothing by default; subclasses may override.
     */
    protected void writeFooter() throws Exception {}

    /**
     * Called after all calls to {@link #writeHeader}, {@link #write} and {@link #writeFooter}.
     * If any resources opened in the write processes need to be flushed, flush them here.
     */
    protected void finishWrite() throws Exception {}

    /**
     *  Performs bundle initialization. For example, creates a temporary file for writing or
     * initializes any state that will be used across calls to {@link FileBasedWriter#write}.
     *
     * <p>The unique id that is given to open should be used to ensure that the writer's output
     * does not interfere with the output of other Writers, as a bundle may be executed many
     * times for fault tolerance.
     *
     * <p>The window and paneInfo arguments are populated when windowed writes are requested.
     * shard and numShards are populated for the case of static sharding. In cases where the
     * runner is dynamically picking sharding, shard and numShards might both be set to -1.
     */
    public final void openWindowed(
        String uId, BoundedWindow window, PaneInfo paneInfo, int shard, int numShards)
        throws Exception {
      if (!getWriteOperation().windowedWrites) {
        throw new IllegalStateException("openWindowed called a non-windowed sink.");
      }
      open(uId, window, paneInfo, shard, numShards);
    }

    /**
     * Called for each value in the bundle.
     */
    public abstract void write(T value) throws Exception;

    /**
     * Similar to {@link #openWindowed} however for the case where unwindowed writes were
     * requested.
     */
    public final void openUnwindowed(String uId,
                                     int shard,
                                     int numShards) throws Exception {
      if (getWriteOperation().windowedWrites) {
        throw new IllegalStateException("openUnwindowed called a windowed sink.");
      }
      open(uId, null, null, shard, numShards);
    }

    // Helper function to close a channel, on exception cases.
    // Always throws prior exception, with any new closing exception suppressed.
    private static void closeChannelAndThrow(
        WritableByteChannel channel, ResourceId filename, Exception prior) throws Exception {
      try {
        channel.close();
      } catch (Exception e) {
        LOG.error("Closing channel for {} failed.", filename, e);
        prior.addSuppressed(e);
        throw prior;
      }
    }

    private void open(String uId,
                      @Nullable BoundedWindow window,
                      @Nullable PaneInfo paneInfo,
                      int shard,
                      int numShards) throws Exception {
      this.id = uId;
      this.window = window;
      this.paneInfo = paneInfo;
      this.shard = shard;
      this.numShards = numShards;
      ResourceId tempDirectory = getWriteOperation().tempDirectory.get();
      outputFile = tempDirectory.resolve(id, StandardResolveOptions.RESOLVE_FILE);
      verifyNotNull(
          outputFile, "FileSystems are not allowed to return null from resolve: %s", tempDirectory);

      final WritableByteChannelFactory factory =
          getWriteOperation().getSink().writableByteChannelFactory;
      // The factory may force a MIME type or it may return null, indicating to use the sink's MIME.
      String channelMimeType = firstNonNull(factory.getMimeType(), mimeType);
      LOG.debug("Opening {} for write with MIME type {}.", outputFile, channelMimeType);
      WritableByteChannel tempChannel = FileSystems.create(outputFile, channelMimeType);
      try {
        channel = factory.create(tempChannel);
      } catch (Exception e) {
        // If we have opened the underlying channel but fail to open the compression channel,
        // we should still close the underlying channel.
        closeChannelAndThrow(tempChannel, outputFile, e);
      }

      // The caller shouldn't have to close() this FileBasedWriter if it fails to open(), so close
      // the channel if prepareWrite() or writeHeader() fails.
      String step = "";
      try {
        LOG.debug("Preparing write to {}.", outputFile);
        prepareWrite(channel);

        LOG.debug("Writing header to {}.", outputFile);
        writeHeader();
      } catch (Exception e) {
        LOG.error("Beginning write to {} failed, closing channel.", step, outputFile, e);
        closeChannelAndThrow(channel, outputFile, e);
      }

      LOG.debug("Starting write of bundle {} to {}.", this.id, outputFile);
    }

    public final void cleanup() throws Exception {
      if (outputFile != null) {
        // outputFile may be null if open() was not called or failed.
        FileSystems.delete(
            Collections.singletonList(outputFile), StandardMoveOptions.IGNORE_MISSING_FILES);
      }
    }

    /** Closes the channel and returns the bundle result. */
    public final FileResult close() throws Exception {
      checkState(outputFile != null, "FileResult.close cannot be called with a null outputFile");

      LOG.debug("Writing footer to {}.", outputFile);
      try {
        writeFooter();
      } catch (Exception e) {
        LOG.error("Writing footer to {} failed, closing channel.", outputFile, e);
        closeChannelAndThrow(channel, outputFile, e);
      }

      LOG.debug("Finishing write to {}.", outputFile);
      try {
        finishWrite();
      } catch (Exception e) {
        LOG.error("Finishing write to {} failed, closing channel.", outputFile, e);
        closeChannelAndThrow(channel, outputFile, e);
      }

      checkState(
          channel.isOpen(),
          "Channel %s to %s should only be closed by its owner: %s", channel, outputFile);

      LOG.debug("Closing channel to {}.", outputFile);
      try {
        channel.close();
      } catch (Exception e) {
        throw new IOException(String.format("Failed closing channel to %s", outputFile), e);
      }

      FileBasedSink<T> sink = getWriteOperation().getSink();
      ResourceId outputDirectory = sink.getBaseOutputDirectoryProvider().get();
      FilenamePolicy filenamePolicy = sink.filenamePolicy;
      String extension = sink.getExtension();
      @Nullable ResourceId destinationFile;
      if (window != null) {
        destinationFile = filenamePolicy.windowedFilename(outputDirectory, new WindowedContext(
            window, paneInfo, shard, numShards), extension);
      } else if (numShards > 0) {
        destinationFile = filenamePolicy.unwindowedFilename(
            outputDirectory, new Context(shard, numShards), extension);
      } else {
        // Destination filename to be generated in the next step.
        destinationFile = null;
      }
      FileResult result = new FileResult(outputFile, destinationFile);
      LOG.debug("Result for bundle {}: {} {}", this.id, outputFile, destinationFile);
      return result;
    }

    /**
     * Return the FileBasedWriteOperation that this Writer belongs to.
     */
    public FileBasedWriteOperation<T> getWriteOperation() {
      return writeOperation;
    }
  }

  /**
   * Result of a single bundle write. Contains the filename produced by the bundle, and if known
   * the final output filename.
   */
  public static final class FileResult implements Serializable {
    private final ResourceId filename;
    @Nullable private final ResourceId destinationFilename;

    public FileResult(ResourceId filename, @Nullable ResourceId destinationFilename) {
      this.filename = filename;
      this.destinationFilename = destinationFilename;
    }

    public ResourceId getFilename() {
      return filename;
    }

    /**
     * The filename to be written. Will be null if the output filename is unknown because the number
     * of shards is determined dynamically by the runner.
     */
    @Nullable public ResourceId getDestinationFilename() {
      return destinationFilename;
    }

    public String toString() {
      return MoreObjects.toStringHelper(FileResult.class)
          .add("filename", filename)
          .add("destinationFilename", destinationFilename)
          .toString();
    }
  }

  /**
   * A coder for {@link FileResult} objects.
   */
  public static final class FileResultCoder extends CustomCoder<FileResult> {
    private static final FileResultCoder INSTANCE = new FileResultCoder();
    private final NullableCoder<String> stringCoder = NullableCoder.of(StringUtf8Coder.of());

    public static FileResultCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(FileResult value, OutputStream outStream, Context context)
        throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null value");
      }
      stringCoder.encode(value.getFilename().toString(), outStream, context.nested());
      if (value.getDestinationFilename() == null) {
        stringCoder.encode(null, outStream, context);
      } else {
        stringCoder.encode(value.getDestinationFilename().toString(), outStream, context);
      }
    }

    @Override
    public FileResult decode(InputStream inStream, Context context)
        throws IOException {
      String filename = stringCoder.decode(inStream, context.nested());
      assert filename != null;  // fixes a compiler warning
      @Nullable String destinationFilename = stringCoder.decode(inStream, context);
      return new FileResult(
          FileSystems.matchNewResource(filename, false /* isDirectory */),
          destinationFilename == null
              ? null
              : FileSystems.matchNewResource(destinationFilename, false /* isDirectory */));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      stringCoder.verifyDeterministic();
    }
  }

  /**
   * Implementations create instances of {@link WritableByteChannel} used by {@link FileBasedSink}
   * and related classes to allow <em>decorating</em>, or otherwise transforming, the raw data that
   * would normally be written directly to the {@link WritableByteChannel} passed into
   * {@link WritableByteChannelFactory#create(WritableByteChannel)}.
   *
   * <p>Subclasses should override {@link #toString()} with something meaningful, as it is used when
   * building {@link DisplayData}.
   */
  public interface WritableByteChannelFactory extends Serializable {
    /**
     * @param channel the {@link WritableByteChannel} to wrap
     * @return the {@link WritableByteChannel} to be used during output
     */
    WritableByteChannel create(WritableByteChannel channel) throws IOException;

    /**
     * Returns the MIME type that should be used for the files that will hold the output data. May
     * return {@code null} if this {@code WritableByteChannelFactory} does not meaningfully change
     * the MIME type (e.g., for {@link CompressionType#UNCOMPRESSED}).
     *
     * @see MimeTypes
     * @see <a href=
     *      'http://www.iana.org/assignments/media-types/media-types.xhtml'>http://www.iana.org/assignments/media-types/media-types.xhtml</a>
     */
    @Nullable
    String getMimeType();

    /**
     * @return an optional filename suffix, eg, ".gz" is returned by {@link CompressionType#GZIP}
     */
    @Nullable
    String getFilenameSuffix();
  }
}
