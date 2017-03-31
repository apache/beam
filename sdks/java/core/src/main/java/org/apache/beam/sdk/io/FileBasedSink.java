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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.Context;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.WindowedContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract {@link Sink} for file-based output. An implementation of FileBasedSink writes file-based
 * output and defines the format of output files (how values are written, headers/footers, MIME
 * type, etc.).
 *
 * <p>At pipeline construction time, the methods of FileBasedSink are called to validate the sink
 * and to create a {@link Sink.WriteOperation} that manages the process of writing to the sink.
 *
 * <p>The process of writing to file-based sink is as follows:
 * <ol>
 * <li>An optional subclass-defined initialization,
 * <li>a parallel write of bundles to temporary files, and finally,
 * <li>these temporary files are renamed with final output filenames.
 * </ol>
 *
 * <p>Supported file systems are those registered with {@link IOChannelUtils}.
 *
 * @param <T> the type of values written to the sink.
 */
public abstract class FileBasedSink<T> extends Sink<T> {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedSink.class);

  /**
   * Directly supported file output compression types.
   */
  public enum CompressionType implements WritableByteChannelFactory {
    /**
     * No compression, or any other transformation, will be used.
     */
    UNCOMPRESSED("", MimeTypes.TEXT) {
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
    private String mimeType;

    private CompressionType(String suffix, String mimeType) {
      this.filenameSuffix = suffix;
      this.mimeType = mimeType;
    }

    @Override
    public String getFilenameSuffix() {
      return filenameSuffix;
    }

    @Override
    public String getMimeType() {
      return mimeType;
    }
  }

  /**
   * The {@link WritableByteChannelFactory} that is used to wrap the raw data output to the
   * underlying channel. The default is to not compress the output using
   * {@link CompressionType#UNCOMPRESSED}.
   */
  protected final WritableByteChannelFactory writableByteChannelFactory;


  /**
   * A naming policy for output files.
   */
  public abstract static class FilenamePolicy implements Serializable {
    /**
     * Context used for generating a name based on shard numer, and num shards.
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
     * Context used for generating a name based on window, pane, shard numer, and num shards.
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
     * the filename. The {@link WindowedContext} object gives access to the window and pane, as
     * well as sharding information. The policy must return unique and consistent filenames
     * for different windows and panes.
     */
    public abstract String windowedFilename(WindowedContext c);

    /**
     * When a sink has not requested windowed output, this method will be invoked to return the
     * filename. The {@link Context} object only provides sharding information, which is used by
     * the policy to generate unique and consistent filenames.
     */
    public abstract String unwindowedFilename(Context c);

    /**
     * @return The base filename for all output files.
     */
    public abstract ValueProvider<String> getBaseOutputFilenameProvider();

    /**
     * Populates the display data.
     */
    public void populateDisplayData(DisplayData.Builder builder) {
    }
  }

  /**
   * A default filename policy.
   */
  protected class DefaultFilenamePolicy extends FilenamePolicy {
    ValueProvider<String> baseOutputFilename;
    String extension;
    String fileNamingTemplate;

    public DefaultFilenamePolicy(ValueProvider<String> baseOutputFilename, String extension,
                                 String fileNamingTemplate) {
      this.baseOutputFilename = baseOutputFilename;
      if (!isNullOrEmpty(writableByteChannelFactory.getFilenameSuffix())) {
        this.extension = extension + getFileExtension(
            writableByteChannelFactory.getFilenameSuffix());
      } else {
        this.extension = extension;
      }
      this.fileNamingTemplate = fileNamingTemplate;
    }

    @Override
    public String unwindowedFilename(FilenamePolicy.Context context) {
      if (context.numShards <= 0) {
        return null;
      }

      String suffix = getFileExtension(extension);
      String filename = IOChannelUtils.constructName(
          baseOutputFilename.get(), fileNamingTemplate, suffix, context.getShardNumber(),
          context.getNumShards());
      return filename;
    }

    @Override
    public String windowedFilename(FilenamePolicy.WindowedContext c) {
      throw new UnsupportedOperationException("There is no default policy for windowed file"
          + " output. Please provide an explicit FilenamePolicy to generate filenames.");
    }

    @Override
    public ValueProvider<String> getBaseOutputFilenameProvider() {
      return baseOutputFilename;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
    String fileNamePattern = String.format("%s%s%s",
        baseOutputFilename.isAccessible()
        ? baseOutputFilename.get() : baseOutputFilename.toString(),
        fileNamingTemplate, getFileExtension(extension));
    builder.add(DisplayData.item("fileNamePattern", fileNamePattern)
      .withLabel("File Name Pattern"));
    }
  }

  /**
   * The policy used to generate output filenames.
   */
  protected FilenamePolicy fileNamePolicy;

  /**
   * Construct a FileBasedSink with the given base output filename and extension. A
   * {@link WritableByteChannelFactory} of type {@link CompressionType#UNCOMPRESSED} will be used.
   */
  public FileBasedSink(String baseOutputFilename, String extension) {
    this(baseOutputFilename, extension, ShardNameTemplate.INDEX_OF_MAX);
  }

  /**
   * Construct a FileBasedSink with the given base output filename, extension, and
   * {@link WritableByteChannelFactory}.
   */
  public FileBasedSink(String baseOutputFilename, String extension,
      WritableByteChannelFactory writableByteChannelFactory) {
    this(StaticValueProvider.of(baseOutputFilename), extension,
        ShardNameTemplate.INDEX_OF_MAX, writableByteChannelFactory);
  }

  /**
   * Construct a FileBasedSink with the given base output filename, extension, and file naming
   * template. A {@link WritableByteChannelFactory} of type {@link CompressionType#UNCOMPRESSED}
   * will be used.
   *
   * <p>See {@link ShardNameTemplate} for a description of file naming templates.
   */
  public FileBasedSink(String baseOutputFilename, String extension, String fileNamingTemplate) {
    this(StaticValueProvider.of(baseOutputFilename), extension, fileNamingTemplate,
        CompressionType.UNCOMPRESSED);
  }

  /**
   * Construct a FileBasedSink with the given base output filename, extension, file naming template,
   * and {@link WritableByteChannelFactory}.
   *
   * <p>See {@link ShardNameTemplate} for a description of file naming templates.
   */
  public FileBasedSink(ValueProvider<String> baseOutputFilename, String extension,
      String fileNamingTemplate, WritableByteChannelFactory writableByteChannelFactory) {
    this.writableByteChannelFactory = writableByteChannelFactory;
    this.fileNamePolicy = new DefaultFilenamePolicy(baseOutputFilename, extension,
        fileNamingTemplate);
  }

  public FileBasedSink(FilenamePolicy fileNamePolicy) {
    this(fileNamePolicy, CompressionType.UNCOMPRESSED);

  }

  public FileBasedSink(FilenamePolicy fileNamePolicy,
                       WritableByteChannelFactory writableByteChannelFactory) {
    this.fileNamePolicy = fileNamePolicy;
    this.writableByteChannelFactory = writableByteChannelFactory;
  }

  /**
   * Returns the base output filename for this file based sink.
   */
  public ValueProvider<String> getBaseOutputFilenameProvider() {
    return fileNamePolicy.getBaseOutputFilenameProvider();
  }

  public FilenamePolicy getFileNamePolicy() {
    return fileNamePolicy;
  }

  @Override
  public void validate(PipelineOptions options) {}

  /**
   * Return a subclass of {@link FileBasedSink.FileBasedWriteOperation} that will manage the write
   * to the sink.
   */
  @Override
  public abstract FileBasedWriteOperation<T> createWriteOperation(PipelineOptions options);

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    getFileNamePolicy().populateDisplayData(builder);
  }

  /**
   * Returns the file extension to be used. If the user did not request a file
   * extension then this method returns the empty string. Otherwise this method
   * adds a {@code "."} to the beginning of the users extension if one is not present.
   */
  private static String getFileExtension(String usersExtension) {
    if (usersExtension == null || usersExtension.isEmpty()) {
      return "";
    }
    if (usersExtension.startsWith(".")) {
      return usersExtension;
    }
    return "." + usersExtension;
  }

  /**
   * Abstract {@link Sink.WriteOperation} that manages the process of writing to a
   * {@link FileBasedSink}.
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
  public abstract static class FileBasedWriteOperation<T> extends WriteOperation<T, FileResult> {
    /**
     * The Sink that this WriteOperation will write to.
     */
    protected final FileBasedSink<T> sink;

    /** Directory for temporary output files. */
    protected final ValueProvider<String> tempDirectory;

    /** Whether windowed writes are being used. */
    protected  boolean windowedWrites;

    /** Constructs a temporary file path given the temporary directory and a filename. */
    protected static String buildTemporaryFilename(String tempDirectory, String filename)
        throws IOException {
      return IOChannelUtils.getFactory(tempDirectory).resolve(tempDirectory, filename);
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
          sink.getBaseOutputFilenameProvider(), new TemporaryDirectoryBuilder()));
    }

    private static class TemporaryDirectoryBuilder
        implements SerializableFunction<String, String> {
      // The intent of the code is to have a consistent value of tempDirectory across
      // all workers, which wouldn't happen if now() was called inline.
      Instant now = Instant.now();

      @Override
      public String apply(String baseOutputFilename) {
        try {
          IOChannelFactory factory = IOChannelUtils.getFactory(baseOutputFilename);
          Path baseOutputPath = factory.toPath(baseOutputFilename);
          return baseOutputPath
              .resolveSibling(
                  "temp-beam-"
                  + baseOutputPath.getFileName()
                  + "-"
                  + now.toString(DateTimeFormat.forPattern("yyyy-MM-DD_HH-mm-ss")))
              .toString();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Create a new FileBasedWriteOperation.
     *
     * @param sink the FileBasedSink that will be used to configure this write operation.
     * @param tempDirectory the base directory to be used for temporary output files.
     */
    public FileBasedWriteOperation(FileBasedSink<T> sink, String tempDirectory) {
      this(sink, StaticValueProvider.of(tempDirectory));
    }

    private FileBasedWriteOperation(FileBasedSink<T> sink, ValueProvider<String> tempDirectory) {
      this.sink = sink;
      this.tempDirectory = tempDirectory;
      this.windowedWrites = false;
    }

    /**
     * Clients must implement to return a subclass of {@link FileBasedSink.FileBasedWriter}. This
     * method must satisfy the restrictions placed on implementations of
     * {@link Sink.WriteOperation#createWriter}. Namely, it must not mutate the state of the object.
     */
    @Override
    public abstract FileBasedWriter<T> createWriter(PipelineOptions options) throws Exception;

    @Override
    public void setWindowedWrites(boolean windowedWrites) {
      this.windowedWrites = windowedWrites;
    }

    /**
     * Initialization of the sink. Default implementation is a no-op. May be overridden by subclass
     * implementations to perform initialization of the sink at pipeline runtime. This method must
     * be idempotent and is subject to the same implementation restrictions as
     * {@link Sink.WriteOperation#initialize}.
     */
    @Override
    public void initialize(PipelineOptions options) throws Exception {}

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
    @Override
    public void finalize(Iterable<FileResult> writerResults,
                         PipelineOptions options)
        throws Exception {
      // Collect names of temporary files and rename them.
      Map<String, String> outputFilenames = buildOutputFilenames(writerResults);
      copyToOutputFiles(outputFilenames, options);

      // Optionally remove temporary files.
      // We remove the entire temporary directory, rather than specifically removing the files
      // from writerResults, because writerResults includes only successfully completed bundles,
      // and we'd like to clean up the failed ones too.
      // Note that due to GCS eventual consistency, matching files in the temp directory is also
      // currently non-perfect and may fail to delete some files.
      //
      // When windows or triggers are specified, files are generated incrementally so deleting
      // the entire directory in finalize is incorrect.
      removeTemporaryFiles(outputFilenames.keySet(), !windowedWrites, options);
    }

    protected final Map<String, String> buildOutputFilenames(Iterable<FileResult> writerResults) {
      Map<String, String> outputFilenames = new HashMap<>();
      List<String> files = new ArrayList<>();
      for (FileResult result : writerResults) {
        if (result.getDestinationFilename() != null) {
          outputFilenames.put(result.getFilename(), result.getDestinationFilename());
        } else {
          files.add(result.getFilename());
        }
      }

      // If the user does not specify numShards() (not supported with windowing). Then the
      // writerResults won't contain destination filenames, so we dynamically generate them here.
      if (files.size() > 0) {
        checkArgument(outputFilenames.isEmpty());
        // Sort files for idempotence.
        files = Ordering.natural().sortedCopy(files);
        FilenamePolicy filenamePolicy = getSink().fileNamePolicy;
        for (int i = 0; i < files.size(); i++) {
          outputFilenames.put(files.get(i),
              filenamePolicy.unwindowedFilename(new Context(i, files.size())));
        }
      }

      int numDistinctShards = new HashSet<String>(outputFilenames.values()).size();
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
    protected final void copyToOutputFiles(Map<String, String> filenames,
                                           PipelineOptions options)
        throws IOException {
      int numFiles = filenames.size();
      if (numFiles > 0) {
        LOG.debug("Copying {} files.", numFiles);
        IOChannelFactory channelFactory =
            IOChannelUtils.getFactory(filenames.values().iterator().next());
        channelFactory.copy(filenames.keySet(), filenames.values());
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
    protected final void removeTemporaryFiles(Set<String> knownFiles,
                                              boolean shouldRemoveTemporaryDirectory,
                                              PipelineOptions options)
        throws IOException {
      String tempDir = tempDirectory.get();
      LOG.debug("Removing temporary bundle output files in {}.", tempDir);
      IOChannelFactory factory = IOChannelUtils.getFactory(tempDir);

      // To partially mitigate the effects of filesystems with eventually-consistent
      // directory matching APIs, we remove not only files that the filesystem says exist
      // in the directory (which may be incomplete), but also files that are known to exist
      // (produced by successfully completed bundles).

      // This may still fail to remove temporary outputs of some failed bundles, but at least
      // the common case (where all bundles succeed) is guaranteed to be fully addressed.
      Set<String> matches = new HashSet<>();
      // TODO: Windows OS cannot resolves and matches '*' in the path,
      // ignore the exception for now to avoid failing the pipeline.
      if (shouldRemoveTemporaryDirectory) {
        try {
          matches.addAll(factory.match(factory.resolve(tempDir, "*")));
        } catch (Exception e) {
          LOG.warn("Failed to match temporary files under: [{}].", tempDir);
        }
      }
      Set<String> allMatches = new HashSet<>(matches);
      allMatches.addAll(knownFiles);
      LOG.debug(
          "Removing {} temporary files found under {} ({} matched glob, {} known files)",
          allMatches.size(),
          tempDir,
          matches.size(),
          allMatches.size() - matches.size());
      // Deletion of the temporary directory might fail, if not all temporary files are removed.
      try {
        factory.remove(allMatches);
        factory.remove(ImmutableList.of(tempDir));
      } catch (Exception e) {
        LOG.warn("Failed to remove temporary directory: [{}].", tempDir);
      }
    }

    /**
     * Provides a coder for {@link FileBasedSink.FileResult}.
     */
    @Override
    public Coder<FileResult> getWriterResultCoder() {
      return FileResultCoder.of();
    }

    /**
     * Returns the FileBasedSink for this write operation.
     */
    @Override
    public FileBasedSink<T> getSink() {
      return sink;
    }
  }

  /**
   * Abstract {@link Sink.Writer} that writes a bundle to a {@link FileBasedSink}. Subclass
   * implementations provide a method that can write a single value to a {@link WritableByteChannel}
   * ({@link Sink.Writer#write}).
   *
   * <p>Subclass implementations may also override methods that write headers and footers before and
   * after the values in a bundle, respectively, as well as provide a MIME type for the output
   * channel.
   *
   * <p>Multiple FileBasedWriter instances may be created on the same worker, and therefore any
   * access to static members or methods should be thread safe.
   *
   * @param <T> the type of values to write.
   */
  public abstract static class FileBasedWriter<T> extends Writer<T, FileResult> {
    private static final Logger LOG = LoggerFactory.getLogger(FileBasedWriter.class);

    final FileBasedWriteOperation<T> writeOperation;

    /**
     * Unique id for this output bundle.
     */
    private String id;

    private BoundedWindow window;
    private PaneInfo paneInfo;
    private int shard = -1;
    private int numShards = -1;

    /**
     * The filename of the output bundle.
     */
    private String filename;

    /**
     * The channel to write to.
     */
    private WritableByteChannel channel;

    /**
     * The MIME type used in the creation of the output channel (if the file system supports it).
     *
     * <p>GCS, for example, supports writing files with Content-Type metadata.
     *
     * <p>May be overridden. Default is {@link MimeTypes#TEXT}. See {@link MimeTypes} for other
     * options.
     */
    protected String mimeType = MimeTypes.TEXT;

    /**
     * Construct a new FileBasedWriter with a base filename.
     */
    public FileBasedWriter(FileBasedWriteOperation<T> writeOperation) {
      checkNotNull(writeOperation);
      this.writeOperation = writeOperation;
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
     * Opens the channel.
     */
    @Override
    public final void openWindowed(String uId,
                                   BoundedWindow window,
                                   PaneInfo paneInfo,
                                   int shard,
                                   int numShards) throws Exception {
      if (!getWriteOperation().windowedWrites) {
        throw new IllegalStateException("openWindowed called a non-windowed sink.");
      }
      open(uId, window, paneInfo, shard, numShards);
    }

    @Override
    public final void openUnwindowed(String uId,
                                     int shard,
                                     int numShards) throws Exception {
      if (getWriteOperation().windowedWrites) {
        throw new IllegalStateException("openUnwindowed called a windowed sink.");
      }
      open(uId, null, null, shard, numShards);
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
      filename = FileBasedWriteOperation.buildTemporaryFilename(
          getWriteOperation().tempDirectory.get(), uId);
      LOG.debug("Opening {}.", filename);
      final WritableByteChannelFactory factory =
          getWriteOperation().getSink().writableByteChannelFactory;
      mimeType = factory.getMimeType();
      channel = factory.create(IOChannelUtils.create(filename, mimeType));
      try {
        prepareWrite(channel);
        LOG.debug("Writing header to {}.", filename);
        writeHeader();
      } catch (Exception e) {
        // The caller shouldn't have to close() this Writer if it fails to open(), so close the
        // channel if prepareWrite() or writeHeader() fails.
        try {
          LOG.error("Writing header to {} failed, closing channel.", filename);
          channel.close();
        } catch (IOException closeException) {
          // Log exception and mask it.
          LOG.error("Closing channel for {} failed: {}", filename, closeException.getMessage());
        }
        // Throw the exception that caused the write to fail.
        throw e;
      }
      LOG.debug("Starting write of bundle {} to {}.", this.id, filename);
    }

    @Override
    public void cleanup() throws Exception {
      if (filename != null) {
        IOChannelUtils.getFactory(filename).remove(Lists.<String>newArrayList(filename));
      }
    }

    /**
     * Closes the channel and returns the bundle result.
     */
    @Override
    public final FileResult close() throws Exception {
      try (WritableByteChannel theChannel = channel) {
        LOG.debug("Writing footer to {}.", filename);
        writeFooter();
        LOG.debug("Finishing write to {}.", filename);
        finishWrite();
        if (!channel.isOpen()) {
          throw new IllegalStateException("Channel should only be closed by its owner: " + channel);
        }
      }

      FilenamePolicy filenamePolicy = getWriteOperation().getSink().fileNamePolicy;
      String destinationFile;
      if (window != null) {
        destinationFile = filenamePolicy.windowedFilename(new WindowedContext(
            window, paneInfo, shard, numShards));
      } else {
        destinationFile =  filenamePolicy.unwindowedFilename(new Context(shard, numShards));
      }
      FileResult result = new FileResult(filename, destinationFile);
      LOG.debug("Result for bundle {}: {} {}", this.id, filename, destinationFile);
      return result;
    }

    /**
     * Return the FileBasedWriteOperation that this Writer belongs to.
     */
    @Override
    public FileBasedWriteOperation<T> getWriteOperation() {
      return writeOperation;
    }
  }

  /**
   * Result of a single bundle write. Contains the filename of the bundle.
   */
  public static final class FileResult {
    private final String filename;
    private final String destinationFilename;

    public FileResult(String filename, String destinationFilename) {
      this.filename = filename;
      this.destinationFilename = destinationFilename;
    }

    public String getFilename() {
      return filename;
    }

    public String getDestinationFilename() {
      return destinationFilename;
    }

  }

  /**
   * A coder for FileResult objects.
   */
  public static final class FileResultCoder extends AtomicCoder<FileResult> {
    private static final FileResultCoder INSTANCE = new FileResultCoder();
    private final Coder<String> stringCoder = NullableCoder.of(StringUtf8Coder.of());

    @JsonCreator
    public static FileResultCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(FileResult value, OutputStream outStream, Context context)
        throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null value");
      }
      stringCoder.encode(value.getFilename(), outStream, context.nested());
      stringCoder.encode(value.getDestinationFilename(), outStream, context.nested());
    }

    @Override
    public FileResult decode(InputStream inStream, Context context)
        throws IOException {
      return new FileResult(
          stringCoder.decode(inStream, context.nested()),
          stringCoder.decode(inStream, context.nested()));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      throw new NonDeterministicException(this, "TableRows are not deterministic.");
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
     * @return the MIME type that should be used for the files that will hold the output data
     * @see MimeTypes
     * @see <a href=
     *      'http://www.iana.org/assignments/media-types/media-types.xhtml'>http://www.iana.org/assignments/media-types/media-types.xhtml</a>
     */
    String getMimeType();

    /**
     * @return an optional filename suffix, eg, ".gz" is returned by {@link CompressionType#GZIP}
     */
    @Nullable
    String getFilenameSuffix();
  }
}
