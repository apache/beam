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
import static org.apache.beam.sdk.io.WriteFiles.UNKNOWN_SHARDNUM;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
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
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for file-based output. An implementation of FileBasedSink writes file-based output
 * and defines the format of output files (how values are written, headers/footers, MIME type,
 * etc.).
 *
 * <p>At pipeline construction time, the methods of FileBasedSink are called to validate the sink
 * and to create a {@link WriteOperation} that manages the process of writing to the sink.
 *
 * <p>The process of writing to file-based sink is as follows:
 *
 * <ol>
 * <li>An optional subclass-defined initialization,
 * <li>a parallel write of bundles to temporary files, and finally,
 * <li>these temporary files are renamed with final output filenames.
 * </ol>
 *
 * <p>In order to ensure fault-tolerance, a bundle may be executed multiple times (e.g., in the
 * event of failure/retry or for redundancy). However, exactly one of these executions will have its
 * result passed to the finalize method. Each call to {@link Writer#openWindowed} or {@link
 * Writer#openUnwindowed} is passed a unique <i>bundle id</i> when it is called by the WriteFiles
 * transform, so even redundant or retried bundles will have a unique way of identifying their
 * output.
 *
 * <p>The bundle id should be used to guarantee that a bundle's output is unique. This uniqueness
 * guarantee is important; if a bundle is to be output to a file, for example, the name of the file
 * will encode the unique bundle id to avoid conflicts with other writers.
 *
 * <p>{@link FileBasedSink} can take a custom {@link FilenamePolicy} object to determine output
 * filenames, and this policy object can be used to write windowed or triggered PCollections into
 * separate files per window pane. This allows file output from unbounded PCollections, and also
 * works for bounded PCollecctions.
 *
 * <p>Supported file systems are those registered with {@link FileSystems}.
 *
 * @param <T> the type of values written to the sink.
 */
@Experimental(Kind.FILESYSTEM)
public abstract class FileBasedSink<T, DestinationT> implements Serializable, HasDisplayData {
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
    public String getSuggestedFilenameSuffix() {
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
  @Experimental(Kind.FILESYSTEM)
  public static ResourceId convertToFileResourceIfPossible(String outputPrefix) {
    try {
      return FileSystems.matchNewResource(outputPrefix, false /* isDirectory */);
    } catch (Exception e) {
      return FileSystems.matchNewResource(outputPrefix, true /* isDirectory */);
    }
  }

  private final DynamicDestinations<?, DestinationT> dynamicDestinations;

  /**
   * The {@link WritableByteChannelFactory} that is used to wrap the raw data output to the
   * underlying channel. The default is to not compress the output using
   * {@link CompressionType#UNCOMPRESSED}.
   */
  private final WritableByteChannelFactory writableByteChannelFactory;

  /**
   * A class that allows value-dependent writes in {@link FileBasedSink}.
   *
   * <p>Users can define a custom type to represent destinations, and provide a mapping to turn
   * this destination type into an instance of {@link FilenamePolicy}.
   */
  public abstract static class DynamicDestinations<T, DestinationT>
      implements HasDisplayData, Serializable {
    /**
     * Returns an object that represents at a high level the destination being written to. May not
     * return null.
     */
    public abstract DestinationT getDestination(T element);

    /**
     * Returns the default destination. This is used for collections that have no elements as the
     * destination to write empty files to.
     */
    public abstract DestinationT getDefaultDestination();

    /**
     * Returns the coder for {@link DestinationT}. If this is not overridden, then
     * the coder registry will be use to find a suitable coder. This must be a
     * deterministic coder, as {@link DestinationT} will be used as a key type in a
     * {@link org.apache.beam.sdk.transforms.GroupByKey}.
     */
    @Nullable
    public Coder<DestinationT> getDestinationCoder() {
      return null;
    }

    /**
     * Converts a destination into a {@link FilenamePolicy}. May not return null.
     */
    public abstract FilenamePolicy getFilenamePolicy(DestinationT destination);

    /**
    * Populates the display data.
     */
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
    }

    // Gets the destination coder. If the user does not provide one, try to find one in the coder
    // registry. If no coder can be found, throws CannotProvideCoderException.
    final Coder<DestinationT> getDestinationCoderWithDefault(CoderRegistry registry)
        throws CannotProvideCoderException {
      Coder<DestinationT> destinationCoder = getDestinationCoder();
      if (destinationCoder != null) {
        return destinationCoder;
      }
      // If dynamicDestinations doesn't provide a coder, try to find it in the coder registry.
      // We must first use reflection to figure out what the type parameter is.
      for (Type superclass = getClass().getGenericSuperclass();
           superclass != null;
           superclass = ((Class) superclass).getGenericSuperclass()) {
        if (superclass instanceof ParameterizedType) {
          ParameterizedType parameterized = (ParameterizedType) superclass;
          if (parameterized.getRawType() == DynamicDestinations.class) {
            // DestinationT is the second parameter.
            Type parameter = parameterized.getActualTypeArguments()[1];
            @SuppressWarnings("unchecked")
            Class<DestinationT> parameterClass = (Class<DestinationT>) parameter;
            return registry.getCoder(parameterClass);
          }
        }
      }
      throw new AssertionError(
          "Couldn't find the DynamicDestinations superclass of " + this.getClass());
    }
  }

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
     * {@link OutputFileHints} containing information about the file, including a suggested
     * extension (e.g. coming from {@link CompressionType}).
     *
     * <p>The {@link WindowedContext} object gives access to the window and pane,
     * as well as sharding information. The policy must return unique and consistent filenames
     * for different windows and panes.
     */
    @Experimental(Kind.FILESYSTEM)
    public abstract ResourceId windowedFilename(WindowedContext c,
                                                OutputFileHints outputFileHints);

    /**
     * When a sink has not requested windowed or triggered output, this method will be invoked to
     * return the file {@link ResourceId resource} to be created given the base output directory and
     * a {@link OutputFileHints} containing information about the file, including a suggested
     *  (e.g. coming from {@link CompressionType}).
     *
     * <p>The {@link Context} object only provides sharding information, which is used by the policy
     * to generate unique and consistent filenames.
     */
    @Experimental(Kind.FILESYSTEM)
    @Nullable public abstract ResourceId unwindowedFilename(
    Context c, OutputFileHints outputFileHints);

    /**
     * Populates the display data.
     */
    public void populateDisplayData(DisplayData.Builder builder) {
    }
  }

  /** The directory to which files will be written. */
  private final ValueProvider<ResourceId> tempDirectoryProvider;

  private static class ExtractDirectory implements SerializableFunction<ResourceId, ResourceId> {
    @Override
    public ResourceId apply(ResourceId input) {
      return input.getCurrentDirectory();
    }
  }

  /**
   * Construct a {@link FileBasedSink} with the given temp directory, producing uncompressed
   * files.
   */
  @Experimental(Kind.FILESYSTEM)
  public FileBasedSink(ValueProvider<ResourceId> tempDirectoryProvider,
                       DynamicDestinations<?, DestinationT> dynamicDestinations) {
    this(tempDirectoryProvider, dynamicDestinations, CompressionType.UNCOMPRESSED);

  }

  /**
   * Construct a {@link FileBasedSink} with the given temp directory and output channel type.
   */
  @Experimental(Kind.FILESYSTEM)
  public FileBasedSink(
      ValueProvider<ResourceId> tempDirectoryProvider,
      DynamicDestinations<?, DestinationT> dynamicDestinations,
      WritableByteChannelFactory writableByteChannelFactory) {
    this.tempDirectoryProvider =
        NestedValueProvider.of(tempDirectoryProvider, new ExtractDirectory());
    this.dynamicDestinations = checkNotNull(dynamicDestinations);
    this.writableByteChannelFactory = writableByteChannelFactory;
  }

  /**
   * Return the {@link DynamicDestinations} used.
   */
  public <UserT> DynamicDestinations<UserT, DestinationT> getDynamicDestinations() {
    return (DynamicDestinations<UserT, DestinationT>) dynamicDestinations;
  }

  /**
   * Returns the directory inside which temprary files will be written according to the configured
   * {@link FilenamePolicy}.
   */
  @Experimental(Kind.FILESYSTEM)
  public ValueProvider<ResourceId> getTempDirectoryProvider() {
    return tempDirectoryProvider;
  }

  public void validate(PipelineOptions options) {}

  /**
   * Return a subclass of {@link WriteOperation} that will manage the write
   * to the sink.
   */
  public abstract WriteOperation<T, DestinationT> createWriteOperation();

  public void populateDisplayData(DisplayData.Builder builder) {
    getDynamicDestinations().populateDisplayData(builder);
  }

  /**
   * Abstract operation that manages the process of writing to {@link FileBasedSink}.
   *
   * <p>The primary responsibilities of the WriteOperation is the management of output
   * files. During a write, {@link Writer}s write bundles to temporary file
   * locations. After the bundles have been written,
   * <ol>
   * <li>{@link WriteOperation#finalize} is given a list of the temporary
   * files containing the output bundles.
   * <li>During finalize, these temporary files are copied to final output locations and named
   * according to a file naming template.
   * <li>Finally, any temporary files that were created during the write are removed.
   * </ol>
   *
   * <p>Subclass implementations of WriteOperation must implement
   * {@link WriteOperation#createWriter} to return a concrete
   * FileBasedSinkWriter.
   *
   * <h2>Temporary and Output File Naming:</h2> During the write, bundles are written to temporary
   * files using the tempDirectory that can be provided via the constructor of
   * WriteOperation. These temporary files will be named
   * {@code {tempDirectory}/{bundleId}}, where bundleId is the unique id of the bundle.
   * For example, if tempDirectory is "gs://my-bucket/my_temp_output", the output for a
   * bundle with bundle id 15723 will be "gs://my-bucket/my_temp_output/15723".
   *
   * <p>Final output files are written to the location specified by the {@link FilenamePolicy}. If
   * no filename policy is specified, then the {@link DefaultFilenamePolicy} will be used. The
   * directory that the files are written to is determined by the {@link FilenamePolicy} instance.
   *
   * <p>Note that in the case of permanent failure of a bundle's write, no clean up of temporary
   * files will occur.
   *
   * <p>If there are no elements in the PCollection being written, no output will be generated.
   *
   * @param <T> the type of values written to the sink.
   */
  public abstract static class WriteOperation<T, DestinationT> implements Serializable {
    /**
     * The Sink that this WriteOperation will write to.
     */
    protected final FileBasedSink<T, DestinationT> sink;

    /** Directory for temporary output files. */
    protected final ValueProvider<ResourceId> tempDirectory;

    /** Whether windowed writes are being used. */
    @Experimental(Kind.FILESYSTEM)
    protected boolean windowedWrites;

    /** Constructs a temporary file resource given the temporary directory and a filename. */
    @Experimental(Kind.FILESYSTEM)
    protected static ResourceId buildTemporaryFilename(ResourceId tempDirectory, String filename)
        throws IOException {
      return tempDirectory.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }

    /**
     * Constructs a WriteOperation using the default strategy for generating a temporary
     * directory from the base output filename.
     *
     * <p>Default is a uniquely named subdirectory of the provided tempDirectory, e.g. if
     * tempDirectory is /path/to/foo/, the temporary directory will be
     * /path/to/foo/temp-beam-foo-$date.
     *
     * @param sink the FileBasedSink that will be used to configure this write operation.
     */
    public WriteOperation(FileBasedSink<T, DestinationT> sink) {
      this(sink, NestedValueProvider.of(
          sink.getTempDirectoryProvider(), new TemporaryDirectoryBuilder()));
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
      public ResourceId apply(ResourceId tempDirectory) {
        // Temp directory has a timestamp and a unique ID
        String tempDirName = String.format(".temp-beam-%s-%s", timestamp, tempId);
        return tempDirectory.getCurrentDirectory().resolve(
            tempDirName, StandardResolveOptions.RESOLVE_DIRECTORY);
      }
    }

    /**
     * Create a new WriteOperation.
     *
     * @param sink the FileBasedSink that will be used to configure this write operation.
     * @param tempDirectory the base directory to be used for temporary output files.
     */
    @Experimental(Kind.FILESYSTEM)
    public WriteOperation(FileBasedSink<T, DestinationT> sink, ResourceId tempDirectory) {
      this(sink, StaticValueProvider.of(tempDirectory));
    }

    private WriteOperation(FileBasedSink<T, DestinationT> sink,
        ValueProvider<ResourceId> tempDirectory) {
      this.sink = sink;
      this.tempDirectory = tempDirectory;
      this.windowedWrites = false;
    }

    /**
     * Clients must implement to return a subclass of {@link Writer}. This
     * method must not mutate the state of the object.
     */
    public abstract Writer<T, DestinationT> createWriter() throws Exception;

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
    public void finalize(Iterable<FileResult<DestinationT>> writerResults) throws Exception {
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

    @Experimental(Kind.FILESYSTEM)
    protected final Map<ResourceId, ResourceId> buildOutputFilenames(
        Iterable<FileResult<DestinationT>> writerResults) {
      int numShards = Iterables.size(writerResults);
      Map<ResourceId, ResourceId> outputFilenames = new HashMap<>();

      // Either all results have a shard number set (if the sink is configured with a fixed
      // number of shards), or they all don't (otherwise).
      Boolean isShardNumberSetEverywhere = null;
      for (FileResult<DestinationT> result : writerResults) {
        boolean isShardNumberSetHere = (result.getShard() != UNKNOWN_SHARDNUM);
        if (isShardNumberSetEverywhere == null) {
          isShardNumberSetEverywhere = isShardNumberSetHere;
        } else {
          checkArgument(
              isShardNumberSetEverywhere == isShardNumberSetHere,
              "Found a mix of files with and without shard number set: %s",
              result);
        }
      }

      if (isShardNumberSetEverywhere == null) {
        isShardNumberSetEverywhere = true;
      }

      List<FileResult<DestinationT>> resultsWithShardNumbers = Lists.newArrayList();
      if (isShardNumberSetEverywhere) {
        resultsWithShardNumbers = Lists.newArrayList(writerResults);
      } else {
        // Sort files for idempotence. Sort by temporary filename.
        // Note that this codepath should not be used when processing triggered windows. In the
        // case of triggers, the list of FileResult objects in the Finalize iterable is not
        // deterministic, and might change over retries. This breaks the assumption below that
        // sorting the FileResult objects provides idempotency.
        List<FileResult<DestinationT>> sortedByTempFilename =
            Ordering.from(
                new Comparator<FileResult<DestinationT>>() {
                  @Override
                  public int compare(FileResult<DestinationT> first,
                                     FileResult<DestinationT> second) {
                    String firstFilename = first.getTempFilename().toString();
                    String secondFilename = second.getTempFilename().toString();
                    return firstFilename.compareTo(secondFilename);
                  }
                })
                .sortedCopy(writerResults);
        for (int i = 0; i < sortedByTempFilename.size(); i++) {
          resultsWithShardNumbers.add(sortedByTempFilename.get(i).withShard(i));
        }
      }

      for (FileResult<DestinationT> result : resultsWithShardNumbers) {
        checkArgument(
            result.getShard() != UNKNOWN_SHARDNUM, "Should have set shard number on %s", result);
        outputFilenames.put(
            result.getTempFilename(),
            result.getDestinationFile(
                getSink().getDynamicDestinations(), numShards,
                getSink().getWritableByteChannelFactory()));
      }

      int numDistinctShards = new HashSet<>(outputFilenames.values()).size();
      checkState(numDistinctShards == outputFilenames.size(),
         "Only generated %s distinct file names for %s files.",
         numDistinctShards, outputFilenames.size());

      return outputFilenames;
    }

    /**
     * Copy temporary files to final output filenames using the file naming template.
     *
     * <p>Can be called from subclasses that override {@link WriteOperation#finalize}.
     *
     * <p>Files will be named according to the {@link FilenamePolicy}. The order of the output files
     * will be the same as the sorted order of the input filenames. In other words (when using
     * {@link DefaultFilenamePolicy}), if the input filenames are ["C", "A", "B"], baseFilename (int
     * the policy) is "dir/file", the extension is ".txt", and the fileNamingTemplate is
     * "-SSS-of-NNN", the contents of A will be copied to dir/file-000-of-003.txt, the contents of B
     * will be copied to dir/file-001-of-003.txt, etc.
     *
     * @param filenames the filenames of temporary files.
     */
    @VisibleForTesting
    @Experimental(Kind.FILESYSTEM)
    final void copyToOutputFiles(Map<ResourceId, ResourceId> filenames) throws IOException {
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
     * <p>Can be called from subclasses that override {@link WriteOperation#finalize}.
     * <b>Note:</b>If finalize is overridden and does <b>not</b> rename or otherwise finalize
     * temporary files, this method will remove them.
     */
    @VisibleForTesting
    @Experimental(Kind.FILESYSTEM)
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
    public FileBasedSink<T, DestinationT> getSink() {
      return sink;
    }

    @Override
    public String toString() {
      String tempDirectoryStr =
          tempDirectory.isAccessible() ? tempDirectory.get().toString() : tempDirectory.toString();
      return getClass().getSimpleName()
          + "{"
          + "tempDirectory="
          + tempDirectoryStr
          + ", windowedWrites="
          + windowedWrites
          + '}';
    }
  }

  /** Returns the {@link WritableByteChannelFactory} used. */
  protected final WritableByteChannelFactory getWritableByteChannelFactory() {
    return writableByteChannelFactory;
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
   * <p>Multiple {@link Writer} instances may be created on the same worker, and therefore
   * any access to static members or methods should be thread safe.
   *
   * @param <T> the type of values to write.
   */
  public abstract static class Writer<T, DestinationT> {
    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

    private final WriteOperation<T, DestinationT> writeOperation;

    /** Unique id for this output bundle. */
    private String id;

    private BoundedWindow window;
    private PaneInfo paneInfo;
    private int shard = -1;
    private DestinationT destination;

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
     * Construct a new {@link Writer} that will produce files of the given MIME type.
     */
    public Writer(WriteOperation<T, DestinationT> writeOperation, String mimeType) {
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
     * Performs bundle initialization. For example, creates a temporary file for writing or
     * initializes any state that will be used across calls to {@link Writer#write}.
     *
     * <p>The unique id that is given to open should be used to ensure that the writer's output does
     * not interfere with the output of other Writers, as a bundle may be executed many times for
     * fault tolerance.
     *
     * <p>The window and paneInfo arguments are populated when windowed writes are requested. shard
     * id populated for the case of static sharding. In cases where the runner is dynamically
     * picking sharding, shard might be set to -1.
     */
    public final void openWindowed(String uId, BoundedWindow window, PaneInfo paneInfo, int shard,
                                   DestinationT destination)
        throws Exception {
      if (!getWriteOperation().windowedWrites) {
        throw new IllegalStateException("openWindowed called a non-windowed sink.");
      }
      open(uId, window, paneInfo, shard, destination);
    }

    /**
     * Called for each value in the bundle.
     */
    public abstract void write(T value) throws Exception;

    /**
     * Similar to {@link #openWindowed} however for the case where unwindowed writes were
     * requested.
     */
    public final void openUnwindowed(String uId, int shard,
                                     DestinationT destination) throws Exception {
      if (getWriteOperation().windowedWrites) {
        throw new IllegalStateException("openUnwindowed called a windowed sink.");
      }
      open(uId, null, null, shard, destination);
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
                      DestinationT destination) throws Exception {
      this.id = uId;
      this.window = window;
      this.paneInfo = paneInfo;
      this.shard = shard;
      this.destination = destination;
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

      // The caller shouldn't have to close() this Writer if it fails to open(), so close
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
    public final FileResult<DestinationT> close() throws Exception {
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

      FileResult<DestinationT> result = new FileResult<>(
          outputFile, shard, window, paneInfo, destination);
      LOG.debug("Result for bundle {}: {}", this.id, outputFile);
      return result;
    }

    /**
     * Return the WriteOperation that this Writer belongs to.
     */
    public WriteOperation<T, DestinationT> getWriteOperation() {
      return writeOperation;
    }
  }

  /**
   * Result of a single bundle write. Contains the filename produced by the bundle, and if known
   * the final output filename.
   */
  public static final class FileResult<DestinationT> {
    private final ResourceId tempFilename;
    private final int shard;
    private final BoundedWindow window;
    private final PaneInfo paneInfo;
    private final DestinationT destination;

    @Experimental(Kind.FILESYSTEM)
    public FileResult(ResourceId tempFilename, int shard, BoundedWindow window, PaneInfo
        paneInfo, DestinationT destination) {
      this.tempFilename = tempFilename;
      this.shard = shard;
      this.window = window;
      this.paneInfo = paneInfo;
      this.destination = destination;
    }

    @Experimental(Kind.FILESYSTEM)
    public ResourceId getTempFilename() {
      return tempFilename;
    }

    public int getShard() {
      return shard;
    }

    public FileResult<DestinationT> withShard(int shard) {
      return new FileResult<>(tempFilename, shard, window, paneInfo, destination);
    }

    public BoundedWindow getWindow() {
      return window;
    }

    public PaneInfo getPaneInfo() {
      return paneInfo;
    }

    public DestinationT getDestination() {
      return destination;
    }

    @Experimental(Kind.FILESYSTEM)
    public ResourceId getDestinationFile(DynamicDestinations<?, DestinationT> dynamicDestinations,
                                         int numShards,
                                         OutputFileHints outputFileHints) {
      checkArgument(getShard() != UNKNOWN_SHARDNUM);
      checkArgument(numShards > 0);
      FilenamePolicy policy = dynamicDestinations.getFilenamePolicy(destination);
      if (getWindow() != null) {
        return policy.windowedFilename(new WindowedContext(
            getWindow(), getPaneInfo(), getShard(), numShards), outputFileHints);
      } else {
        return policy.unwindowedFilename(new Context(getShard(), numShards), outputFileHints);
      }
    }

    public String toString() {
      return MoreObjects.toStringHelper(FileResult.class)
          .add("tempFilename", tempFilename)
          .add("shard", shard)
          .add("window", window)
          .add("paneInfo", paneInfo)
          .toString();
    }
  }

  /**
   * A coder for {@link FileResult} objects.
   */
  public static final class FileResultCoder<DestinationT>
      extends StructuredCoder<FileResult<DestinationT>> {
    private static final Coder<String> FILENAME_CODER = StringUtf8Coder.of();
    private static final Coder<Integer> SHARD_CODER = VarIntCoder.of();
    private static final Coder<PaneInfo> PANE_INFO_CODER = NullableCoder.of(PaneInfoCoder.INSTANCE);
    private final Coder<BoundedWindow> windowCoder;
    private final Coder<DestinationT> destinationCoder;

    protected FileResultCoder(Coder<BoundedWindow> windowCoder,
                              Coder<DestinationT> destinationCoder) {
      this.windowCoder = NullableCoder.of(windowCoder);
      this.destinationCoder = destinationCoder;
    }

    public static <DestinationT> FileResultCoder<DestinationT> of(
        Coder<BoundedWindow> windowCoder, Coder<DestinationT> destinationCoder) {
      return new FileResultCoder<>(windowCoder, destinationCoder);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(windowCoder);
    }

    @Override
    public void encode(FileResult<DestinationT> value, OutputStream outStream)
        throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null value");
      }
      FILENAME_CODER.encode(value.getTempFilename().toString(), outStream);
      windowCoder.encode(value.getWindow(), outStream);
      PANE_INFO_CODER.encode(value.getPaneInfo(), outStream);
      SHARD_CODER.encode(value.getShard(), outStream);
      destinationCoder.encode(value.getDestination(), outStream);
    }

    @Override
    public FileResult<DestinationT> decode(InputStream inStream) throws IOException {
      String tempFilename = FILENAME_CODER.decode(inStream);
      BoundedWindow window = windowCoder.decode(inStream);
      PaneInfo paneInfo = PANE_INFO_CODER.decode(inStream);
      int shard = SHARD_CODER.decode(inStream);
      DestinationT destination = destinationCoder.decode(inStream);
      return new FileResult<>(FileSystems.matchNewResource(tempFilename,
          false /* isDirectory */), shard, window, paneInfo, destination);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      FILENAME_CODER.verifyDeterministic();
      windowCoder.verifyDeterministic();
      PANE_INFO_CODER.verifyDeterministic();
      SHARD_CODER.verifyDeterministic();
      destinationCoder.verifyDeterministic();
    }
  }

  /**
   * Provides hints about how to generate output files, such as a suggested filename suffix (based
   * on the compression type), and the file MIME type.
   */
  public interface OutputFileHints extends Serializable {
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
    String getSuggestedFilenameSuffix();
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
  public interface WritableByteChannelFactory extends OutputFileHints {
    /**
     * @param channel the {@link WritableByteChannel} to wrap
     * @return the {@link WritableByteChannel} to be used during output
     */
    WritableByteChannel create(WritableByteChannel channel) throws IOException;

  }
}
