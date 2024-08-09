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

import static org.apache.beam.sdk.io.WriteFiles.UNKNOWN_SHARDNUM;
import static org.apache.beam.sdk.values.TypeDescriptors.extractFromTypeParameters;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Verify.verifyNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors.TypeVariableExtractor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
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
 *   <li>An optional subclass-defined initialization,
 *   <li>a parallel write of bundles to temporary files, and finally,
 *   <li>these temporary files are renamed with final output filenames.
 * </ol>
 *
 * <p>In order to ensure fault-tolerance, a bundle may be executed multiple times (e.g., in the
 * event of failure/retry or for redundancy). However, exactly one of these executions will have its
 * result passed to the finalize method. Each call to {@link Writer#open} is passed a unique
 * <i>bundle id</i> when it is called by the WriteFiles transform, so even redundant or retried
 * bundles will have a unique way of identifying their output.
 *
 * <p>The bundle id should be used to guarantee that a bundle's output is unique. This uniqueness
 * guarantee is important; if a bundle is to be output to a file, for example, the name of the file
 * will encode the unique bundle id to avoid conflicts with other writers.
 *
 * <p>{@link FileBasedSink} can take a custom {@link FilenamePolicy} object to determine output
 * filenames, and this policy object can be used to write windowed or triggered PCollections into
 * separate files per window pane. This allows file output from unbounded PCollections, and also
 * works for bounded PCollections.
 *
 * <p>Supported file systems are those registered with {@link FileSystems}.
 *
 * @param <OutputT> the type of values written to the sink.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class FileBasedSink<UserT, DestinationT, OutputT>
    implements Serializable, HasDisplayData {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedSink.class);
  static final String TEMP_DIRECTORY_PREFIX = ".temp-beam";

  /** @deprecated use {@link Compression}. */
  @Deprecated
  public enum CompressionType implements WritableByteChannelFactory {
    /** @see Compression#UNCOMPRESSED */
    UNCOMPRESSED(Compression.UNCOMPRESSED),

    /** @see Compression#GZIP */
    GZIP(Compression.GZIP),

    /** @see Compression#BZIP2 */
    BZIP2(Compression.BZIP2),

    /** @see Compression#ZSTD */
    ZSTD(Compression.ZSTD),

    /** @see Compression#LZO */
    LZO(Compression.LZO),

    /** @see Compression#LZOP */
    LZOP(Compression.LZOP),

    /** @see Compression#DEFLATE */
    DEFLATE(Compression.DEFLATE),

    /** @see Compression#SNAPPY */
    SNAPPY(Compression.SNAPPY);

    private final Compression canonical;

    CompressionType(Compression canonical) {
      this.canonical = canonical;
    }

    @Override
    public String getSuggestedFilenameSuffix() {
      return canonical.getSuggestedSuffix();
    }

    @Override
    public @Nullable String getMimeType() {
      return (canonical == Compression.UNCOMPRESSED) ? null : MimeTypes.BINARY;
    }

    @Override
    public WritableByteChannel create(WritableByteChannel channel) throws IOException {
      return canonical.writeCompressed(channel);
    }

    public static CompressionType fromCanonical(Compression canonical) {
      switch (canonical) {
        case AUTO:
          throw new IllegalArgumentException("AUTO is not supported for writing");

        case UNCOMPRESSED:
          return UNCOMPRESSED;

        case GZIP:
          return GZIP;

        case BZIP2:
          return BZIP2;

        case ZIP:
          throw new IllegalArgumentException("ZIP is unsupported");

        case ZSTD:
          return ZSTD;

        case LZO:
          return LZO;

        case LZOP:
          return LZOP;

        case DEFLATE:
          return DEFLATE;

        case SNAPPY:
          return SNAPPY;

        default:
          throw new UnsupportedOperationException("Unsupported compression type: " + canonical);
      }
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

  private final DynamicDestinations<?, DestinationT, OutputT> dynamicDestinations;

  /**
   * The {@link WritableByteChannelFactory} that is used to wrap the raw data output to the
   * underlying channel. The default is to not compress the output using {@link
   * Compression#UNCOMPRESSED}.
   */
  private final WritableByteChannelFactory writableByteChannelFactory;

  /**
   * A class that allows value-dependent writes in {@link FileBasedSink}.
   *
   * <p>Users can define a custom type to represent destinations, and provide a mapping to turn this
   * destination type into an instance of {@link FilenamePolicy}.
   */
  public abstract static class DynamicDestinations<UserT, DestinationT, OutputT>
      implements HasDisplayData, Serializable {
    interface SideInputAccessor {
      <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view);
    }

    private transient @Nullable SideInputAccessor sideInputAccessor;

    static class SideInputAccessorViaProcessContext implements SideInputAccessor {
      private DoFn<?, ?>.ProcessContext processContext;

      SideInputAccessorViaProcessContext(DoFn<?, ?>.ProcessContext processContext) {
        this.processContext = processContext;
      }

      @Override
      public <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view) {
        return processContext.sideInput(view);
      }
    }

    /**
     * Override to specify that this object needs access to one or more side inputs. This side
     * inputs must be globally windowed, as they will be accessed from the global window.
     */
    public List<PCollectionView<?>> getSideInputs() {
      return ImmutableList.of();
    }

    /**
     * Returns the value of a given side input. The view must be present in {@link
     * #getSideInputs()}.
     */
    protected final <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view) {
      checkState(
          sideInputAccessor != null,
          "sideInput called on %s but side inputs have not been initialized",
          getClass().getName());
      return sideInputAccessor.sideInput(view);
    }

    final void setSideInputAccessor(SideInputAccessor sideInputAccessor) {
      this.sideInputAccessor = sideInputAccessor;
    }

    final void setSideInputAccessorFromProcessContext(DoFn<?, ?>.ProcessContext context) {
      this.sideInputAccessor = new SideInputAccessorViaProcessContext(context);
    }

    /** Convert an input record type into the output type. */
    public abstract OutputT formatRecord(UserT record);

    /**
     * Returns an object that represents at a high level the destination being written to. May not
     * return null. A destination must have deterministic hash and equality methods defined.
     */
    public abstract DestinationT getDestination(UserT element);

    /**
     * Returns the default destination. This is used for collections that have no elements as the
     * destination to write empty files to.
     */
    public abstract DestinationT getDefaultDestination();

    /**
     * Returns the coder for {@link DestinationT}. If this is not overridden, then the coder
     * registry will be use to find a suitable coder. This must be a deterministic coder, as {@link
     * DestinationT} will be used as a key type in a {@link
     * org.apache.beam.sdk.transforms.GroupByKey}.
     */
    public @Nullable Coder<DestinationT> getDestinationCoder() {
      return null;
    }

    /** Converts a destination into a {@link FilenamePolicy}. May not return null. */
    public abstract FilenamePolicy getFilenamePolicy(DestinationT destination);

    /** Populates the display data. */
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {}

    // Gets the destination coder. If the user does not provide one, try to find one in the coder
    // registry. If no coder can be found, throws CannotProvideCoderException.
    final Coder<DestinationT> getDestinationCoderWithDefault(CoderRegistry registry)
        throws CannotProvideCoderException {
      Coder<DestinationT> destinationCoder = getDestinationCoder();
      if (destinationCoder != null) {
        return destinationCoder;
      }
      // If dynamicDestinations doesn't provide a coder, try to find it in the coder registry.
      @Nullable
      TypeDescriptor<DestinationT> descriptor =
          extractFromTypeParameters(
              this,
              DynamicDestinations.class,
              new TypeVariableExtractor<
                  DynamicDestinations<UserT, DestinationT, OutputT>, DestinationT>() {});
      try {
        return registry.getCoder(descriptor);
      } catch (CannotProvideCoderException e) {
        throw new CannotProvideCoderException(
            "Failed to infer coder for DestinationT from type "
                + descriptor
                + ", please provide it explicitly by overriding getDestinationCoder()",
            e);
      }
    }
  }

  /** A naming policy for output files. */
  public abstract static class FilenamePolicy implements Serializable {
    /**
     * When a sink has requested windowed or triggered output, this method will be invoked to return
     * the file {@link ResourceId resource} to be created given the base output directory and a
     * {@link OutputFileHints} containing information about the file, including a suggested
     * extension (e.g. coming from {@link Compression}).
     *
     * <p>The policy must return unique and consistent filenames for different windows and panes.
     */
    public abstract ResourceId windowedFilename(
        int shardNumber,
        int numShards,
        BoundedWindow window,
        PaneInfo paneInfo,
        OutputFileHints outputFileHints);

    /**
     * When a sink has not requested windowed or triggered output, this method will be invoked to
     * return the file {@link ResourceId resource} to be created given the base output directory and
     * a {@link OutputFileHints} containing information about the file, including a suggested (e.g.
     * coming from {@link Compression}).
     *
     * <p>The shardNumber and numShards parameters, should be used by the policy to generate unique
     * and consistent filenames.
     */
    public abstract @Nullable ResourceId unwindowedFilename(
        int shardNumber, int numShards, OutputFileHints outputFileHints);

    /** Populates the display data. */
    public void populateDisplayData(DisplayData.Builder builder) {}
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
   * Construct a {@link FileBasedSink} with the given temp directory, producing uncompressed files.
   */
  public FileBasedSink(
      ValueProvider<ResourceId> tempDirectoryProvider,
      DynamicDestinations<?, DestinationT, OutputT> dynamicDestinations) {
    this(tempDirectoryProvider, dynamicDestinations, Compression.UNCOMPRESSED);
  }

  /** Construct a {@link FileBasedSink} with the given temp directory and output channel type. */
  public FileBasedSink(
      ValueProvider<ResourceId> tempDirectoryProvider,
      DynamicDestinations<?, DestinationT, OutputT> dynamicDestinations,
      WritableByteChannelFactory writableByteChannelFactory) {
    this.tempDirectoryProvider =
        NestedValueProvider.of(tempDirectoryProvider, new ExtractDirectory());
    this.dynamicDestinations = checkNotNull(dynamicDestinations);
    this.writableByteChannelFactory = writableByteChannelFactory;
  }

  /** Construct a {@link FileBasedSink} with the given temp directory and output channel type. */
  public FileBasedSink(
      ValueProvider<ResourceId> tempDirectoryProvider,
      DynamicDestinations<?, DestinationT, OutputT> dynamicDestinations,
      Compression compression) {
    this(tempDirectoryProvider, dynamicDestinations, CompressionType.fromCanonical(compression));
  }

  /** Return the {@link DynamicDestinations} used. */
  @SuppressWarnings("unchecked")
  public DynamicDestinations<UserT, DestinationT, OutputT> getDynamicDestinations() {
    return (DynamicDestinations<UserT, DestinationT, OutputT>) dynamicDestinations;
  }

  /**
   * Returns the directory inside which temporary files will be written according to the configured
   * {@link FilenamePolicy}.
   */
  public ValueProvider<ResourceId> getTempDirectoryProvider() {
    return tempDirectoryProvider;
  }

  public void validate(PipelineOptions options) {}

  /** Return a subclass of {@link WriteOperation} that will manage the write to the sink. */
  public abstract WriteOperation<DestinationT, OutputT> createWriteOperation();

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    getDynamicDestinations().populateDisplayData(builder);
  }

  /**
   * Abstract operation that manages the process of writing to {@link FileBasedSink}.
   *
   * <p>The primary responsibilities of the WriteOperation is the management of output files. During
   * a write, {@link Writer}s write bundles to temporary file locations. After the bundles have been
   * written,
   *
   * <ol>
   *   <li>{@link WriteOperation#finalizeDestination} is given a list of the temporary files
   *       containing the output bundles.
   *   <li>During finalize, these temporary files are copied to final output locations and named
   *       according to a file naming template.
   *   <li>Finally, any temporary files that were created during the write are removed.
   * </ol>
   *
   * <p>Subclass implementations of WriteOperation must implement {@link
   * WriteOperation#createWriter} to return a concrete FileBasedSinkWriter.
   *
   * <h2>Temporary and Output File Naming:</h2>
   *
   * <p>During the write, bundles are written to temporary files using the tempDirectory that can be
   * provided via the constructor of WriteOperation. These temporary files will be named {@code
   * {tempDirectory}/{bundleId}}, where bundleId is the unique id of the bundle. For example, if
   * tempDirectory is "gs://my-bucket/my_temp_output", the output for a bundle with bundle id 15723
   * will be "gs://my-bucket/my_temp_output/15723".
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
   * @param <OutputT> the type of values written to the sink.
   */
  public abstract static class WriteOperation<DestinationT, OutputT> implements Serializable {
    /** The Sink that this WriteOperation will write to. */
    protected final FileBasedSink<?, DestinationT, OutputT> sink;

    /**
     * Base directory for temporary output files. A subdirectory of this may be used based upon
     * tempSubdirType.
     */
    private final ValueProvider<ResourceId> baseTempDirectory;

    private enum TempSubDirType {
      NONE, // baseTempDirectory is used without a subdirectory.
      UNIQUE, // a subdirectory based upon subdirUUID is used.
      CONSISTENT, // a subdirectory common across all pipelines is used.
    }

    private TempSubDirType tempSubdirType;
    private final UUID subdirUUID;

    /** Whether windowed writes are being used. */
    protected boolean windowedWrites;

    /** Constructs a temporary file resource given the temporary directory and a filename. */
    protected static ResourceId buildTemporaryFilename(ResourceId tempDirectory, String filename)
        throws IOException {
      return tempDirectory.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }

    /**
     * Constructs a WriteOperation using the default strategy for generating a temporary directory
     * from the base output filename.
     *
     * <p>Without windowing, the default is a uniquely named subdirectory of the provided
     * tempDirectory, e.g. if tempDirectory is /path/to/foo/, the temporary directory will be
     * /path/to/foo/.temp-beam-$uuid.
     *
     * <p>With windowing, the default is a consistent named subdirectory of the provided
     * tempDirectory, e.g. if tempDirectory is /path/to/foo/, the temporary directory will be
     * /path/to/foo/.temp-beam. With windowing, unique subdirectories of the tempDirectory are not
     * beneficial as they cannot be used for cleanup. By using a consistent directory, the created
     * temp files are well-distributed beneath a common directory prefix, across both worker and
     * pipeline executions. This is beneficial for filesystems such as GCS which can reuse
     * autoscaling of the file metadata.
     *
     * @param sink the FileBasedSink that will be used to configure this write operation.
     */
    public WriteOperation(FileBasedSink<?, DestinationT, OutputT> sink) {
      // The use of the unique subdir will be disabled if setWindowedWrites is called.
      this(sink, sink.getTempDirectoryProvider(), TempSubDirType.UNIQUE);
    }

    /**
     * Create a new WriteOperation.
     *
     * @param sink the FileBasedSink that will be used to configure this write operation.
     * @param tempDirectory the base directory to be used for temporary output files.
     */
    public WriteOperation(FileBasedSink<?, DestinationT, OutputT> sink, ResourceId tempDirectory) {
      this(sink, StaticValueProvider.of(tempDirectory), TempSubDirType.NONE);
    }

    private WriteOperation(
        FileBasedSink<?, DestinationT, OutputT> sink,
        ValueProvider<ResourceId> tempDirectory,
        TempSubDirType tempSubdirType) {
      this.sink = sink;
      this.baseTempDirectory = tempDirectory;
      this.tempSubdirType = tempSubdirType;
      this.subdirUUID = UUID.randomUUID();
      this.windowedWrites = false;
    }

    public ResourceId getTempDirectory() {
      if (tempSubdirType == TempSubDirType.NONE) {
        return baseTempDirectory.get();
      }
      String tempDirName;
      if (tempSubdirType == TempSubDirType.UNIQUE) {
        tempDirName = String.format(TEMP_DIRECTORY_PREFIX + "-%s", subdirUUID);
      } else {
        assert (tempSubdirType == TempSubDirType.CONSISTENT);
        tempDirName = TEMP_DIRECTORY_PREFIX;
      }
      return baseTempDirectory
          .get()
          .getCurrentDirectory()
          .resolve(tempDirName, StandardResolveOptions.RESOLVE_DIRECTORY);
    }

    /**
     * Clients must implement to return a subclass of {@link Writer}. This method must not mutate
     * the state of the object.
     */
    public abstract Writer<DestinationT, OutputT> createWriter() throws Exception;

    /** Indicates that the operation will be performing windowed writes. */
    public void setWindowedWrites() {
      this.windowedWrites = true;
      this.tempSubdirType = TempSubDirType.CONSISTENT;
    }

    /*
     * Remove temporary files after finalization.
     *
     * <p>In the case where we are doing global-window, untriggered writes, we remove the entire
     * temporary directory, rather than specifically removing the files from writerResults, because
     * writerResults includes only successfully completed bundles, and we'd like to clean up the
     * failed ones too. The reason we remove files here rather than in finalize is that finalize
     * might be called multiple times (e.g. if the bundle contained multiple destinations), and
     * deleting the entire directory can't be done until all calls to finalize.
     *
     * <p>When windows or triggers are specified, files are generated incrementally so deleting the
     * entire directory in finalize is incorrect. If windowedWrites is true, we instead delete the
     * files individually. This means that some temporary files generated by failed bundles might
     * not be cleaned up. Note that {@link WriteFiles} does attempt clean up files if exceptions
     * are thrown, however there are still some scenarios where temporary files might be left.
     */
    public void removeTemporaryFiles(Collection<ResourceId> filenames) throws IOException {
      removeTemporaryFiles(filenames, !windowedWrites);
    }

    protected final List<KV<FileResult<DestinationT>, ResourceId>> finalizeDestination(
        @Nullable DestinationT dest,
        @Nullable BoundedWindow window,
        @Nullable Integer numShards,
        Collection<FileResult<DestinationT>> existingResults)
        throws Exception {
      Collection<FileResult<DestinationT>> completeResults =
          windowedWrites
              ? existingResults
              : createMissingEmptyShards(dest, numShards, existingResults);

      for (FileResult<DestinationT> res : completeResults) {
        checkArgument(
            Objects.equals(dest, res.getDestination()),
            "File result has wrong destination: expected %s, got %s",
            dest,
            res.getDestination());
        checkArgument(
            Objects.equals(window, res.getWindow()),
            "File result has wrong window: expected %s, got %s",
            window,
            res.getWindow());
      }
      List<KV<FileResult<DestinationT>, ResourceId>> outputFilenames = Lists.newArrayList();

      final int effectiveNumShards;
      if (numShards != null) {
        effectiveNumShards = numShards;
        for (FileResult<DestinationT> res : completeResults) {
          checkArgument(
              res.getShard() != UNKNOWN_SHARDNUM,
              "Fixed sharding into %s shards was specified, "
                  + "but file result %s does not specify a shard",
              numShards,
              res);
        }
      } else {
        effectiveNumShards = Iterables.size(completeResults);
        for (FileResult<DestinationT> res : completeResults) {
          checkArgument(
              res.getShard() == UNKNOWN_SHARDNUM,
              "Runner-chosen sharding was specified, "
                  + "but file result %s explicitly specifies a shard",
              res);
        }
      }

      List<FileResult<DestinationT>> resultsWithShardNumbers = Lists.newArrayList();
      if (numShards != null) {
        resultsWithShardNumbers = Lists.newArrayList(completeResults);
      } else {
        int i = 0;
        for (FileResult<DestinationT> res : completeResults) {
          resultsWithShardNumbers.add(res.withShard(i++));
        }
      }

      Map<ResourceId, FileResult<DestinationT>> distinctFilenames = Maps.newHashMap();
      for (FileResult<DestinationT> result : resultsWithShardNumbers) {
        checkArgument(
            result.getShard() != UNKNOWN_SHARDNUM, "Should have set shard number on %s", result);
        ResourceId finalFilename =
            result.getDestinationFile(
                windowedWrites,
                getSink().getDynamicDestinations(),
                effectiveNumShards,
                getSink().getWritableByteChannelFactory());
        checkArgument(
            !distinctFilenames.containsKey(finalFilename),
            "Filename policy must generate unique filenames, but generated the same name %s "
                + "for file results %s and %s",
            finalFilename,
            result,
            distinctFilenames.get(finalFilename));
        distinctFilenames.put(finalFilename, result);
        outputFilenames.add(KV.of(result, finalFilename));
        FileSystems.reportSinkLineage(finalFilename);
      }
      return outputFilenames;
    }

    private Collection<FileResult<DestinationT>> createMissingEmptyShards(
        @Nullable DestinationT dest,
        @Nullable Integer numShards,
        Collection<FileResult<DestinationT>> existingResults)
        throws Exception {
      Collection<FileResult<DestinationT>> completeResults;
      LOG.info("Finalizing for destination {} num shards {}.", dest, existingResults.size());
      if (numShards != null) {
        checkArgument(
            existingResults.size() <= numShards,
            "Fixed sharding into %s shards was specified, but got %s file results",
            numShards,
            existingResults.size());
      }
      // We must always output at least 1 shard, and honor user-specified numShards
      // if set.
      Set<Integer> missingShardNums;
      if (numShards == null) {
        missingShardNums =
            existingResults.isEmpty() ? ImmutableSet.of(UNKNOWN_SHARDNUM) : ImmutableSet.of();
      } else {
        missingShardNums = Sets.newHashSet();
        for (int i = 0; i < numShards; ++i) {
          missingShardNums.add(i);
        }
        for (FileResult<DestinationT> res : existingResults) {
          checkArgument(
              res.getShard() != UNKNOWN_SHARDNUM,
              "Fixed sharding into %s shards was specified, "
                  + "but file result %s does not specify a shard",
              numShards,
              res);
          missingShardNums.remove(res.getShard());
        }
      }
      completeResults = Lists.newArrayList(existingResults);
      if (!missingShardNums.isEmpty()) {
        LOG.info(
            "Creating {} empty output shards in addition to {} written for destination {}.",
            missingShardNums.size(),
            existingResults.size(),
            dest);
        for (int shard : missingShardNums) {
          String uuid = UUID.randomUUID().toString();
          LOG.info("Opening empty writer {} for destination {}", uuid, dest);
          Writer<DestinationT, ?> writer = createWriter();
          writer.setDestination(dest);
          // Currently this code path is only called in the unwindowed case.
          writer.open(uuid);
          writer.close();
          completeResults.add(
              new FileResult<>(
                  writer.getOutputFile(),
                  shard,
                  GlobalWindow.INSTANCE,
                  PaneInfo.ON_TIME_AND_ONLY_FIRING,
                  dest));
        }
        LOG.debug("Done creating extra shards for {}.", dest);
      }
      return completeResults;
    }

    /**
     * Copy temporary files to final output filenames using the file naming template.
     *
     * <p>Can be called from subclasses that override {@link WriteOperation#finalizeDestination}.
     *
     * <p>Files will be named according to the {@link FilenamePolicy}. The order of the output files
     * will be the same as the sorted order of the input filenames. In other words (when using
     * {@link DefaultFilenamePolicy}), if the input filenames are ["C", "A", "B"], baseFilename (int
     * the policy) is "dir/file", the extension is ".txt", and the fileNamingTemplate is
     * "-SSS-of-NNN", the contents of A will be copied to dir/file-000-of-003.txt, the contents of B
     * will be copied to dir/file-001-of-003.txt, etc.
     */
    @VisibleForTesting
    final void moveToOutputFiles(
        List<KV<FileResult<DestinationT>, ResourceId>> resultsToFinalFilenames) throws IOException {
      int numFiles = resultsToFinalFilenames.size();

      LOG.debug("Copying {} files.", numFiles);
      List<ResourceId> srcFiles = new ArrayList<>();
      List<ResourceId> dstFiles = new ArrayList<>();
      for (KV<FileResult<DestinationT>, ResourceId> entry : resultsToFinalFilenames) {
        srcFiles.add(entry.getKey().getTempFilename());
        dstFiles.add(entry.getValue());
        LOG.info(
            "Will copy temporary file {} to final location {}", entry.getKey(), entry.getValue());
      }
      // During a failure case, files may have been deleted in an earlier step. Thus
      // we ignore missing files here.
      FileSystems.rename(
          srcFiles,
          dstFiles,
          StandardMoveOptions.IGNORE_MISSING_FILES,
          StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);

      // The rename ensures that the source files are deleted.  However we may still need to clean
      // up the directory or orphaned files.
      removeTemporaryFiles(Collections.emptyList());
    }

    /**
     * Removes temporary output files. Uses the temporary directory to find files to remove.
     *
     * <p>Can be called from subclasses that override {@link WriteOperation#finalizeDestination}.
     * <b>Note:</b>If finalize is overridden and does <b>not</b> rename or otherwise finalize
     * temporary files, this method will remove them.
     */
    @VisibleForTesting
    final void removeTemporaryFiles(
        Collection<ResourceId> knownFiles, boolean shouldRemoveTemporaryDirectory)
        throws IOException {
      // To partially mitigate the effects of filesystems with eventually-consistent
      // directory matching APIs, we remove not only files that the filesystem says exist
      // in the directory (which may be incomplete), but also files that are known to exist
      // (produced by successfully completed bundles).

      // This may still fail to remove temporary outputs of some failed bundles, but at least
      // the common case (where all bundles succeed) is guaranteed to be fully addressed.
      Set<ResourceId> allMatches = new HashSet<>(knownFiles);
      for (ResourceId match : allMatches) {
        LOG.info("Will remove known temporary file {}", match);
      }
      // TODO: Windows OS cannot resolves and matches '*' in the path,
      // ignore the exception for now to avoid failing the pipeline.
      ResourceId tempDir = getTempDirectory();
      if (shouldRemoveTemporaryDirectory) {
        LOG.debug("Removing temporary bundle output files in {}.", tempDir);
        try {
          MatchResult singleMatch =
              Iterables.getOnlyElement(
                  FileSystems.match(Collections.singletonList(tempDir.toString() + "*")));
          for (Metadata matchResult : singleMatch.metadata()) {
            if (allMatches.add(matchResult.resourceId())) {
              LOG.warn(
                  "Will also remove unknown temporary file {}. This might indicate that other process/job is using "
                      + "the same temporary folder and result in data consistency issues.",
                  matchResult.resourceId());
            }
          }
        } catch (Exception e) {
          LOG.warn("Failed to match temporary files under: [{}].", tempDir);
        }
      }
      FileSystems.delete(allMatches, StandardMoveOptions.IGNORE_MISSING_FILES);

      if (shouldRemoveTemporaryDirectory) {
        // Deletion of the temporary directory might fail, if not all temporary files are removed.
        try {
          FileSystems.delete(
              Collections.singletonList(tempDir), StandardMoveOptions.IGNORE_MISSING_FILES);
        } catch (Exception e) {
          LOG.warn("Failed to remove temporary directory: [{}].", tempDir);
        }
      }
    }

    /** Returns the FileBasedSink for this write operation. */
    public FileBasedSink<?, DestinationT, OutputT> getSink() {
      return sink;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName()
          + "{"
          + "tempDirectory="
          + getTempDirectory()
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
   * Abstract writer that writes a bundle to a {@link FileBasedSink}. Subclass implementations
   * provide a method that can write a single value to a {@link WritableByteChannel}.
   *
   * <p>Subclass implementations may also override methods that write headers and footers before and
   * after the values in a bundle, respectively, as well as provide a MIME type for the output
   * channel.
   *
   * <p>Multiple {@link Writer} instances may be created on the same worker, and therefore any
   * access to static members or methods should be thread safe.
   *
   * @param <OutputT> the type of values to write.
   */
  public abstract static class Writer<DestinationT, OutputT> {
    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

    private final WriteOperation<DestinationT, OutputT> writeOperation;

    /** Unique id for this output bundle. */
    private @Nullable String id;

    private @Nullable DestinationT destination;

    /** The output file for this bundle. May be null if opening failed. */
    private @Nullable ResourceId outputFile;

    /** The channel to write to. */
    private @Nullable WritableByteChannel channel;

    /**
     * The MIME type used in the creation of the output channel (if the file system supports it).
     *
     * <p>This is the default for the sink, but it may be overridden by a supplied {@link
     * WritableByteChannelFactory}. For example, {@link TextIO.Write} uses {@link MimeTypes#TEXT} by
     * default but if {@link Compression#BZIP2} is set then the MIME type will be overridden to
     * {@link MimeTypes#BINARY}.
     */
    private final @Nullable String mimeType;

    /** Construct a new {@link Writer} that will produce files of the given MIME type. */
    public Writer(WriteOperation<DestinationT, OutputT> writeOperation, String mimeType) {
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

    /** Writes footer at the end of output files. Nothing by default; subclasses may override. */
    protected void writeFooter() throws Exception {}

    /**
     * Called after all calls to {@link #writeHeader}, {@link #write} and {@link #writeFooter}. If
     * any resources opened in the write processes need to be flushed, flush them here.
     */
    protected void finishWrite() throws Exception {}

    @VisibleForTesting
    static String spreadUid(String uId) {
      // We prepend the hash of the uId to ensure that the temporary
      // filenames used do not have common prefix. In some filesystems
      // (for example GCS) such filenames can lead to hotspots.
      return String.format("%08x%s", uId.hashCode(), uId);
    }

    /**
     * Opens a uniquely named temporary file and initializes the writer using {@link #prepareWrite}.
     *
     * <p>The unique id that is given to open should be used to ensure that the writer's output does
     * not interfere with the output of other Writers, as a bundle may be executed many times for
     * fault tolerance.
     */
    public final void open(String uId) throws Exception {
      this.id = spreadUid(uId);
      ResourceId tempDirectory = getWriteOperation().getTempDirectory();
      outputFile = tempDirectory.resolve(id, StandardResolveOptions.RESOLVE_FILE);
      verifyNotNull(
          outputFile, "FileSystems are not allowed to return null from resolve: %s", tempDirectory);

      final WritableByteChannelFactory factory =
          getWriteOperation().getSink().writableByteChannelFactory;
      // The factory may force a MIME type or it may return null, indicating to use the sink's MIME.
      String channelMimeType = firstNonNull(factory.getMimeType(), mimeType);
      CreateOptions createOptions =
          StandardCreateOptions.builder()
              .setMimeType(channelMimeType)
              // The file is based upon a uuid and thus we expect it to be unique and to not already
              // exist. A new uuid is generated on each bundle processing and thus this also holds
              // across bundle retries. Collisions of filenames would result in data loss as we
              // would otherwise overwrite already finalized data.
              .setExpectFileToNotExist(true)
              .build();
      WritableByteChannel tempChannel = FileSystems.create(outputFile, createOptions);
      try {
        channel = factory.create(tempChannel);
      } catch (Exception e) {
        // If we have opened the underlying channel but fail to open the compression channel,
        // we should still close the underlying channel.
        closeChannelAndThrow(tempChannel, outputFile, e);
      }

      // The caller shouldn't have to close() this Writer if it fails to open(), so close
      // the channel if prepareWrite() or writeHeader() fails.
      try {
        LOG.debug("Preparing write to {}.", outputFile);
        prepareWrite(channel);

        LOG.debug("Writing header to {}.", outputFile);
        writeHeader();
      } catch (Exception e) {
        LOG.error("Beginning write to {} failed, closing channel.", outputFile, e);
        closeChannelAndThrow(channel, outputFile, e);
      }

      LOG.debug("Starting write of bundle {} to {}.", this.id, outputFile);
    }

    /** Called for each value in the bundle. */
    public abstract void write(OutputT value) throws Exception;

    public ResourceId getOutputFile() {
      return outputFile;
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
      }
      // We should fail here regardless of whether above channel.close() call failed or not.
      throw prior;
    }

    public final void cleanup() throws Exception {
      if (outputFile != null) {
        LOG.info("Deleting temporary file {}", outputFile);
        // outputFile may be null if open() was not called or failed.
        FileSystems.delete(
            Collections.singletonList(outputFile), StandardMoveOptions.IGNORE_MISSING_FILES);
      }
    }

    /** Closes the channel and returns the bundle result. */
    public final void close() throws Exception {
      checkState(outputFile != null, "FileResult.close cannot be called with a null outputFile");
      LOG.debug("Closing {}", outputFile);

      try {
        writeFooter();
      } catch (Exception e) {
        closeChannelAndThrow(channel, outputFile, e);
      }

      try {
        finishWrite();
      } catch (Exception e) {
        closeChannelAndThrow(channel, outputFile, e);
      }

      // It is valid for a subclass to either close the channel or not.
      // They would typically close the channel e.g. if they are wrapping it in another channel
      // and the wrapper needs to be closed.
      if (channel.isOpen()) {
        LOG.debug("Closing channel to {}.", outputFile);
        try {
          channel.close();
        } catch (Exception e) {
          throw new IOException(String.format("Failed closing channel to %s", outputFile), e);
        }
      }
      LOG.info("Successfully wrote temporary file {}", outputFile);
    }

    /** Return the WriteOperation that this Writer belongs to. */
    public WriteOperation<DestinationT, OutputT> getWriteOperation() {
      return writeOperation;
    }

    void setDestination(DestinationT destination) {
      this.destination = destination;
    }

    /** Return the user destination object for this writer. */
    public DestinationT getDestination() {
      return destination;
    }
  }

  /**
   * Result of a single bundle write. Contains the filename produced by the bundle, and if known the
   * final output filename.
   */
  public static final class FileResult<DestinationT> {
    private final ResourceId tempFilename;
    private final int shard;
    private final BoundedWindow window;
    private final PaneInfo paneInfo;
    private final DestinationT destination;

    public FileResult(
        ResourceId tempFilename,
        int shard,
        BoundedWindow window,
        PaneInfo paneInfo,
        DestinationT destination) {
      checkArgument(window != null, "window can not be null");
      checkArgument(paneInfo != null, "paneInfo can not be null");
      this.tempFilename = tempFilename;
      this.shard = shard;
      this.window = window;
      this.paneInfo = paneInfo;
      this.destination = destination;
    }

    public ResourceId getTempFilename() {
      return tempFilename;
    }

    public int getShard() {
      return shard;
    }

    public FileResult<DestinationT> withShard(int shard) {
      return new FileResult<>(tempFilename, shard, window, paneInfo, destination);
    }

    public @Nullable BoundedWindow getWindow() {
      return window;
    }

    public PaneInfo getPaneInfo() {
      return paneInfo;
    }

    public DestinationT getDestination() {
      return destination;
    }

    public ResourceId getDestinationFile(
        boolean windowedWrites,
        DynamicDestinations<?, DestinationT, ?> dynamicDestinations,
        int numShards,
        OutputFileHints outputFileHints) {
      checkArgument(getShard() != UNKNOWN_SHARDNUM);
      checkArgument(numShards > 0);
      FilenamePolicy policy = dynamicDestinations.getFilenamePolicy(destination);
      if (windowedWrites) {
        return policy.windowedFilename(
            getShard(), numShards, getWindow(), getPaneInfo(), outputFileHints);
      } else {
        return policy.unwindowedFilename(getShard(), numShards, outputFileHints);
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(FileResult.class)
          .add("tempFilename", tempFilename)
          .add("shard", shard)
          .add("window", window)
          .add("paneInfo", paneInfo)
          .toString();
    }
  }

  /** A coder for {@link FileResult} objects. */
  public static final class FileResultCoder<DestinationT>
      extends StructuredCoder<FileResult<DestinationT>> {
    private static final Coder<String> FILENAME_CODER = StringUtf8Coder.of();
    private static final Coder<Integer> SHARD_CODER = VarIntCoder.of();
    private static final Coder<PaneInfo> PANE_INFO_CODER = NullableCoder.of(PaneInfoCoder.INSTANCE);
    private final Coder<BoundedWindow> windowCoder;
    private final Coder<DestinationT> destinationCoder;

    protected FileResultCoder(
        Coder<BoundedWindow> windowCoder, Coder<DestinationT> destinationCoder) {
      this.windowCoder = NullableCoder.of(windowCoder);
      this.destinationCoder = destinationCoder;
    }

    public static <DestinationT> FileResultCoder<DestinationT> of(
        Coder<BoundedWindow> windowCoder, Coder<DestinationT> destinationCoder) {
      return new FileResultCoder<>(windowCoder, destinationCoder);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(destinationCoder);
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
      return Arrays.asList(windowCoder, destinationCoder);
    }

    @Override
    public void encode(FileResult<DestinationT> value, OutputStream outStream) throws IOException {
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
      return new FileResult<>(
          FileSystems.matchNewResource(tempFilename, false /* isDirectory */),
          shard,
          window,
          paneInfo,
          destination);
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
   * Provides hints about how to generate output files, such as a suggested filename suffix (e.g.
   * based on the compression type), and the file MIME type.
   */
  public interface OutputFileHints extends Serializable {
    /**
     * Returns the MIME type that should be used for the files that will hold the output data. May
     * return {@code null} if this {@code WritableByteChannelFactory} does not meaningfully change
     * the MIME type (e.g., for {@link Compression#UNCOMPRESSED}).
     *
     * @see MimeTypes
     * @see <a href=
     *     'http://www.iana.org/assignments/media-types/media-types.xhtml'>http://www.iana.org/assignments/media-types/media-types.xhtml</a>
     */
    @Nullable
    String getMimeType();

    /** @return an optional filename suffix, eg, ".gz" is returned for {@link Compression#GZIP} */
    @Nullable
    String getSuggestedFilenameSuffix();
  }

  /**
   * Implementations create instances of {@link WritableByteChannel} used by {@link FileBasedSink}
   * and related classes to allow <em>decorating</em>, or otherwise transforming, the raw data that
   * would normally be written directly to the {@link WritableByteChannel} passed into {@link
   * WritableByteChannelFactory#create(WritableByteChannel)}.
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
