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

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.DynamicDestinationHelpers.ConstantFilenamePolicy;
import org.apache.beam.sdk.io.DynamicDestinationHelpers.DefaultDynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * {@link PTransform}s for reading and writing text files.
 *
 * <p>To read a {@link PCollection} from one or more text files, use {@code TextIO.read()} to
 * instantiate a transform and use {@link TextIO.Read#from(String)} to specify the path of the
 * file(s) to be read.
 *
 * <p>{@link TextIO.Read} returns a {@link PCollection} of {@link String Strings}, each
 * corresponding to one line of an input UTF-8 text file (split into lines delimited by '\n', '\r',
 * or '\r\n').
 *
 * <p>Example:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<String> lines = p.apply(TextIO.read().from("/local/path/to/file.txt"));
 * }</pre>
 *
 * <p>To write a {@link PCollection} to one or more text files, use {@code TextIO.write()}, using
 * {@link TextIO.Write#to(String)} to specify the output prefix of the files to write.
 *
 * <p>By default, all input is put into the global window before writing. If per-window writes are
 * desired - for example, when using a streaming runner - {@link TextIO.Write#withWindowedWrites()}
 * will cause windowing and triggering to be preserved. When producing windowed writes with a
 * streaming runner that supports triggers, the number of output shards must be set explicitly using
 * {@link TextIO.Write#withNumShards(int)}; some runners may set this for you to a runner-chosen
 * value, so you may need not set it yourself. A {@link FilenamePolicy} can also be set in case you
 * need better control over naming files created by unique windows. {@link DefaultFilenamePolicy}
 * policy for producing unique filenames might not be appropriate for your use case.
 *
 * <p>Any existing files with the same names as generated output files will be overwritten.
 *
 * <p>For example:
 *
 * <pre>{@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.write().to("/path/to/file.txt"));
 *
 * // Same as above, only with Gzip compression:
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.write().to("/path/to/file.txt"));
 *      .withSuffix(".txt")
 *      .withWritableByteChannelFactory(FileBasedSink.CompressionType.GZIP));
 * }</pre>
 */
public class TextIO {
  private static class IdentityFormatter<T> implements SerializableFunction<T, T> {
    @Override
    public T apply(T input) {
      return input;
    }
  }

  private static class ExtractValueFormatter<K, V> implements SerializableFunction<KV<K, V>, V> {
    @Override
    public V apply(KV<K, V> input) {
      return input.getValue();
    }
  }

  /**
   * A {@link PTransform} that reads from one or more text files and returns a bounded
   * {@link PCollection} containing one element for each line of the input files.
   */
  public static Read read() {
    return new AutoValue_TextIO_Read.Builder().setCompressionType(CompressionType.AUTO).build();
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
   * matching a sharding pattern), with each element of the input collection encoded into its own
   * line.
   */
  public static Write<String> write() {
    return TextIO.writeCustomType(new IdentityFormatter<String>());
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
   * matching a sharding pattern), with each element of the input collection encoded into its own
   * line.
   *
   * <p>This version allows you to apply {@link TextIO} writes to a PCollection of a custom type
   * {@link T}, along with a format function that converts the input type {@link T} to the String
   * that will be written to the file. The advantage of this is it  allows a user-provided
   * {@link DynamicDestinations} object, set via {@link Write#withDynamicDestinations} to examine
   * the user's custom type when choosing a destination.
   */
  public static <T> Write<T> writeCustomType(SerializableFunction<T, String> formatFunction) {
    return new AutoValue_TextIO_Write.Builder<T>()
        .setFilenamePrefix(null)
        .setTempDirectory(null)
        .setShardTemplate(null)
        .setFilenameSuffix(null)
        .setFilenamePolicy(null)
        .setDynamicDestinations(null)
        .setFormatFunction(formatFunction)
        .setWritableByteChannelFactory(FileBasedSink.CompressionType.UNCOMPRESSED)
        .setWindowedWrites(false)
        .setNumShards(0)
        .build();
  }

  /**
   * This is a convenience builder for dynamically writing to multiple file destinations. The
   * transform takes in {@code KV<DefaultFilenamePolicy.Params, String>}s mapping file lines to
   * a default filename policy for writing (the params contains a prefix, suffix, and filename
   * template). A params must also be provided to specify how to write out empty PCollections.
   */
  public static Write<KV<Params, String>> writeDynamic(Params emptyDestination) {
    return writeCustomType(new ExtractValueFormatter<Params, String>())
    .withDynamicDestinations(new DefaultDynamicDestinations(emptyDestination));
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {
    @Nullable abstract ValueProvider<String> getFilepattern();
    abstract CompressionType getCompressionType();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);
      abstract Builder setCompressionType(CompressionType compressionType);

      abstract Read build();
    }

    /**
     * Reads text files that reads from the file(s) with the given filename or filename pattern.
     *
     * <p>This can be a local path (if running locally), or a Google Cloud Storage filename or
     * filename pattern of the form {@code "gs://<bucket>/<filepath>"} (if running locally or using
     * remote execution service).
     *
     * <p>Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java
     * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public Read from(String filepattern) {
      checkNotNull(filepattern, "Filepattern cannot be empty.");
      return from(StaticValueProvider.of(filepattern));
    }

    /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
    public Read from(ValueProvider<String> filepattern) {
      checkNotNull(filepattern, "Filepattern cannot be empty.");
      return toBuilder().setFilepattern(filepattern).build();
    }

    /**
     * Returns a new transform for reading from text files that's like this one but
     * reads from input sources using the specified compression type.
     *
     * <p>If no compression type is specified, the default is {@link TextIO.CompressionType#AUTO}.
     */
    public Read withCompressionType(TextIO.CompressionType compressionType) {
      return toBuilder().setCompressionType(compressionType).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      if (getFilepattern() == null) {
        throw new IllegalStateException("need to set the filepattern of a TextIO.Read transform");
      }

      final Bounded<String> read = org.apache.beam.sdk.io.Read.from(getSource());
      PCollection<String> pcol = input.getPipeline().apply("Read", read);
      // Honor the default output coder that would have been used by this PTransform.
      pcol.setCoder(getDefaultOutputCoder());
      return pcol;
    }

    // Helper to create a source specific to the requested compression type.
    protected FileBasedSource<String> getSource() {
      switch (getCompressionType()) {
        case UNCOMPRESSED:
          return new TextSource(getFilepattern());
        case AUTO:
          return CompressedSource.from(new TextSource(getFilepattern()));
        case BZIP2:
          return
              CompressedSource.from(new TextSource(getFilepattern()))
                  .withDecompression(CompressedSource.CompressionMode.BZIP2);
        case GZIP:
          return
              CompressedSource.from(new TextSource(getFilepattern()))
                  .withDecompression(CompressedSource.CompressionMode.GZIP);
        case ZIP:
          return
              CompressedSource.from(new TextSource(getFilepattern()))
                  .withDecompression(CompressedSource.CompressionMode.ZIP);
        case DEFLATE:
          return
              CompressedSource.from(new TextSource(getFilepattern()))
                  .withDecompression(CompressedSource.CompressionMode.DEFLATE);
        default:
          throw new IllegalArgumentException("Unknown compression type: " + getFilepattern());
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      String filepatternDisplay = getFilepattern().isAccessible()
        ? getFilepattern().get() : getFilepattern().toString();
      builder
          .add(DisplayData.item("compressionType", getCompressionType().toString())
            .withLabel("Compression Type"))
          .addIfNotNull(DisplayData.item("filePattern", filepatternDisplay)
            .withLabel("File Pattern"));
    }

    @Override
    protected Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    /** The prefix of each file written, combined with suffix and shardTemplate. */
    @Nullable abstract ValueProvider<ResourceId> getFilenamePrefix();

    /** The suffix of each file written, combined with prefix and shardTemplate. */
    @Nullable abstract String getFilenameSuffix();

    /** The base directory used for generating temporary files. */
    @Nullable abstract ValueProvider<ResourceId> getTempDirectory();

    /** An optional header to add to each file. */
    @Nullable abstract String getHeader();

    /** An optional footer to add to each file. */
    @Nullable abstract String getFooter();

    /** Requested number of shards. 0 for automatic. */
    abstract int getNumShards();

    /** The shard template of each file written, combined with prefix and suffix. */
    @Nullable abstract String getShardTemplate();

    /** A policy for naming output files. */
    @Nullable abstract FilenamePolicy getFilenamePolicy();

    /** Allows for value-dependent {@link DynamicDestinations} to be vended. */
    @Nullable abstract DynamicDestinations<T, ?> getDynamicDestinations();

    /** A function that converts T to a String, for writing to the file. */
    abstract SerializableFunction<T, String> getFormatFunction();

    /** Whether to write windowed output files. */
    abstract boolean getWindowedWrites();

    /**
     * The {@link WritableByteChannelFactory} to be used by the {@link FileBasedSink}. Default is
     * {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
     */
    abstract WritableByteChannelFactory getWritableByteChannelFactory();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilenamePrefix(ValueProvider<ResourceId> filenamePrefix);
      abstract Builder<T> setTempDirectory(ValueProvider<ResourceId> tempDirectory);
      abstract Builder<T> setShardTemplate(@Nullable String shardTemplate);
      abstract Builder<T> setFilenameSuffix(@Nullable String filenameSuffix);
      abstract Builder<T> setHeader(@Nullable String header);
      abstract Builder<T> setFooter(@Nullable String footer);
      abstract Builder<T> setFilenamePolicy(@Nullable FilenamePolicy filenamePolicy);
      abstract Builder<T> setDynamicDestinations(
          @Nullable DynamicDestinations<T, ?> dynamicDestinations);
      abstract Builder<T> setFormatFunction(SerializableFunction<T, String> formatFunction);
      abstract Builder<T> setNumShards(int numShards);
      abstract Builder<T> setWindowedWrites(boolean windowedWrites);
      abstract Builder<T> setWritableByteChannelFactory(
          WritableByteChannelFactory writableByteChannelFactory);

      abstract Write<T> build();
    }

    /**
     * Writes to text files with the given prefix. The given {@code prefix} can reference any
     * {@link FileSystem} on the classpath. This prefix is used by the {@link DefaultFilenamePolicy}
     * to generate filenames.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will be used built using the specified prefix
     * to define the base output directory and file prefix, a shard identifier (see
     * {@link #withNumShards(int)}), and a common suffix (if supplied using
     * {@link #withSuffix(String)}).
     *
     * <p>This default policy can be overridden using {@link #withFilenamePolicy(FilenamePolicy)},
     * in which case {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should
     * not be set. Custom filename policies do not automatically see this prefix - you should
     * explicitly pass the prefix into your {@link FilenamePolicy} object if you need this.
     *
     * <p>If {@link #withTempDirectory} has not been called, this filename prefix will be used to
     * infer a directory for temporary files.
     */
    public Write<T> to(String filenamePrefix) {
      return to(FileBasedSink.convertToFileResourceIfPossible(filenamePrefix));
    }

    /**
     * Like {@link #to(String)}.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write<T> to(ResourceId filenamePrefix) {
      return toResource(StaticValueProvider.of(filenamePrefix));
    }

    /**
     * Like {@link #to(String)}.
     */
    public Write<T> to(ValueProvider<String> outputPrefix) {
      return toResource(NestedValueProvider.of(outputPrefix,
          new SerializableFunction<String, ResourceId>() {
            @Override
            public ResourceId apply(String input) {
              return FileBasedSink.convertToFileResourceIfPossible(input);
            }
          }));
    }

    /**
     * Like {@link #to(ResourceId)}.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write<T> toResource(ValueProvider<ResourceId> filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /**
     * Set the base directory used to generate temporary files.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write<T> withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
      return toBuilder().setTempDirectory(tempDirectory).build();
    }

    /**
     * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
     * used when {@link #withFilenamePolicy(FilenamePolicy)} has not been configured.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public Write<T> withShardNameTemplate(String shardTemplate) {
      return toBuilder().setShardTemplate(shardTemplate).build();
    }

    /**
     * Configures the filename suffix for written files. This option may only be used when
     * {@link #withFilenamePolicy(FilenamePolicy)} has not been configured.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public Write<T> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /**
     * Use a {@link FileBasedSink.FilenamePolicy} to name written files.
     */
    public Write<T> withFilenamePolicy(FilenamePolicy filenamePolicy) {
      return toBuilder().setFilenamePolicy(filenamePolicy).build();
    }

    /**
     * Use a {@link DynamicDestinations} object to vend {@link FilenamePolicy} objects. These
     * objects can examine the input record when creating a {@link FilenamePolicy}.
     */
    public Write<T> withDynamicDestinations(DynamicDestinations<T, ?> dynamicDestinations) {
      return toBuilder().setDynamicDestinations(dynamicDestinations).build();
    }

    /**
     * Configures the number of output shards produced overall (when using unwindowed writes) or
     * per-window (when using windowed writes).
     *
     * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
     * performance of a pipeline. Setting this value is not recommended unless you require a
     * specific number of output files.
     *
     * @param numShards the number of shards to use, or 0 to let the system decide.
     */
    public Write<T> withNumShards(int numShards) {
      checkArgument(numShards >= 0);
      return toBuilder().setNumShards(numShards).build();
    }

    /**
     * Forces a single file as output and empty shard name template. This option is only compatible
     * with unwindowed writes.
     *
     * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
     * performance of a pipeline. Setting this value is not recommended unless you require a
     * specific number of output files.
     *
     * <p>This is equivalent to {@code .withNumShards(1).withShardNameTemplate("")}
     */
    public Write<T> withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    /**
     * Adds a header string to each file. A newline after the header is added automatically.
     *
     * <p>A {@code null} value will clear any previously configured header.
     */
    public Write<T> withHeader(@Nullable String header) {
      return toBuilder().setHeader(header).build();
    }

    /**
     * Adds a footer string to each file. A newline after the footer is added automatically.
     *
     * <p>A {@code null} value will clear any previously configured footer.
     */
    public Write<T> withFooter(@Nullable String footer) {
      return toBuilder().setFooter(footer).build();
    }

    /**
     * Returns a transform for writing to text files like this one but that has the given
     * {@link WritableByteChannelFactory} to be used by the {@link FileBasedSink} during output.
     * The default is value is {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
     *
     * <p>A {@code null} value will reset the value to the default value mentioned above.
     */
    public Write<T> withWritableByteChannelFactory(
        WritableByteChannelFactory writableByteChannelFactory) {
      return toBuilder().setWritableByteChannelFactory(writableByteChannelFactory).build();
    }

    public Write<T> withWindowedWrites() {
      return toBuilder().setWindowedWrites(true).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      checkState(getFilenamePrefix() != null || getTempDirectory() != null,
          "Need to set either the filename prefix or the tempDirectory of a TextIO.Write "
           + "transform.");
      checkState(getFilenamePolicy() == null || getDynamicDestinations() == null,
      "Cannot specify both a filename policy and dynamic destinations");
      if (getFilenamePolicy() != null || getDynamicDestinations() != null) {
        checkState(getShardTemplate() == null && getFilenameSuffix() == null,
        "shardTemplate and filenameSuffix should only be used with the default "
        + "filename policy");
      }

      DynamicDestinations<T, ?> dynamicDestinations = getDynamicDestinations();
      if (dynamicDestinations == null) {
        FilenamePolicy usedFilenamePolicy = getFilenamePolicy();
        if (usedFilenamePolicy == null) {
          usedFilenamePolicy = DefaultFilenamePolicy.fromParams(
              Params.fromStandardParameters(getFilenamePrefix(),
                  getShardTemplate(),
                  getFilenameSuffix(), getWindowedWrites()));
        }
        dynamicDestinations =
            new ConstantFilenamePolicy<>(usedFilenamePolicy);
      }
      return expandTyped(input, dynamicDestinations);
    }

    public <DestinationT> PDone expandTyped(
        PCollection<T> input, DynamicDestinations<T, DestinationT> dynamicDestinations) {
      ValueProvider<ResourceId> tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = getFilenamePrefix();
      }
      WriteFiles<T, DestinationT, String> write =
          WriteFiles.to(
              new TextSink<>(
                  tempDirectory,
                  dynamicDestinations,
                  getHeader(),
                  getFooter(),
                  getWritableByteChannelFactory()),
      getFormatFunction());
      if (getNumShards() > 0) {
        write = write.withNumShards(getNumShards());
      }
      if (getWindowedWrites()) {
        write = write.withWindowedWrites();
      }
      return input.apply("WriteFiles", write);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      String prefixString = "";
      if (getFilenamePrefix() != null) {
        prefixString = getFilenamePrefix().isAccessible()
            ? getFilenamePrefix().get().toString() : getFilenamePrefix().toString();
      }
      builder
          .addIfNotNull(DisplayData.item("filePrefix", prefixString)
            .withLabel("Output File Prefix"))
          .addIfNotNull(DisplayData.item("fileSuffix", getFilenameSuffix())
            .withLabel("Output File Suffix"))
          .addIfNotNull(DisplayData.item("shardNameTemplate", getShardTemplate())
            .withLabel("Output Shard Name Template"))
          .addIfNotDefault(DisplayData.item("numShards", getNumShards())
            .withLabel("Maximum Output Shards"), 0)
          .addIfNotNull(DisplayData.item("fileHeader", getHeader())
            .withLabel("File Header"))
          .addIfNotNull(DisplayData.item("fileFooter", getFooter())
              .withLabel("File Footer"))
          .add(DisplayData
              .item("writableByteChannelFactory", getWritableByteChannelFactory().toString())
              .withLabel("Compression/Transformation Type"));
    }

    @Override
    protected Coder<Void> getDefaultOutputCoder() {
      return VoidCoder.of();
    }
  }

  /**
   * Possible text file compression types.
   */
  public enum CompressionType {
    /**
     * Automatically determine the compression type based on filename extension.
     */
    AUTO(""),
    /**
     * Uncompressed (i.e., may be split).
     */
    UNCOMPRESSED(""),
    /**
     * GZipped.
     */
    GZIP(".gz"),
    /**
     * BZipped.
     */
    BZIP2(".bz2"),
    /**
     * Zipped.
     */
    ZIP(".zip"),
    /**
     * Deflate compressed.
     */
    DEFLATE(".deflate");

    private String filenameSuffix;

    CompressionType(String suffix) {
      this.filenameSuffix = suffix;
    }

    /**
     * Determine if a given filename matches a compression type based on its extension.
     * @param filename the filename to match
     * @return true iff the filename ends with the compression type's known extension.
     */
    public boolean matches(String filename) {
      return filename.toLowerCase().endsWith(filenameSuffix.toLowerCase());
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Disable construction of utility class. */
  private TextIO() {}
}
