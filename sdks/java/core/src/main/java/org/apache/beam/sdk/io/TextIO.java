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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CompressedSource.CompressionMode;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * {@link PTransform}s for reading and writing text files.
 *
 * <p>To read a {@link PCollection} from one or more text files, use {@code TextIO.read()} to
 * instantiate a transform and use {@link TextIO.Read#from(String)} to specify the path of the
 * file(s) to be read. Alternatively, if the filenames to be read are themselves in a {@link
 * PCollection}, apply {@link TextIO#readAll()}.
 *
 * <p>{@link TextIO.Read} returns a {@link PCollection} of {@link String Strings}, each
 * corresponding to one line of an input UTF-8 text file (split into lines delimited by '\n', '\r',
 * or '\r\n').
 *
 * <p>Example 1: reading a file or filepattern.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<String> lines = p.apply(TextIO.read().from("/local/path/to/file.txt"));
 * }</pre>
 *
 * <p>If it is known that the filepattern will match a very large number of files (e.g. tens of
 * thousands or more), use {@link Read#withHintMatchesManyFiles} for better performance and
 * scalability. Note that it may decrease performance if the filepattern matches only a small number
 * of files.
 *
 * <p>Example 2: reading a PCollection of filenames.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // E.g. the filenames might be computed from other data in the pipeline, or
 * // read from a data source.
 * PCollection<String> filenames = ...;
 *
 * // Read all files in the collection.
 * PCollection<String> lines = filenames.apply(TextIO.readAll());
 * }</pre>
 *
 * <p>To write a {@link PCollection} to one or more text files, use {@code TextIO.write()}, using
 * {@link TextIO.Write#to(String)} to specify the output prefix of the files to write.
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
 * lines.apply(TextIO.write().to("/path/to/file.txt"))
 *      .withSuffix(".txt")
 *      .withWritableByteChannelFactory(FileBasedSink.CompressionType.GZIP));
 * }</pre>
 *
 * <p>By default, all input is put into the global window before writing. If per-window writes are
 * desired - for example, when using a streaming runner - {@link TextIO.Write#withWindowedWrites()}
 * will cause windowing and triggering to be preserved. When producing windowed writes with a
 * streaming runner that supports triggers, the number of output shards must be set explicitly using
 * {@link TextIO.Write#withNumShards(int)}; some runners may set this for you to a runner-chosen
 * value, so you may need not set it yourself. If setting an explicit template using {@link
 * TextIO.Write#withShardNameTemplate(String)}, make sure that the template contains placeholders
 * for the window and the pane; W is expanded into the window text, and P into the pane; the default
 * template will include both the window and the pane in the filename.
 *
 * <p>If you want better control over how filenames are generated than the default policy allows, a
 * custom {@link FilenamePolicy} can also be set using {@link TextIO.Write#to(FilenamePolicy)}.
 *
 * <p>TextIO also supports dynamic, value-dependent file destinations. The most general form of this
 * is done via {@link TextIO.Write#to(DynamicDestinations)}. A {@link DynamicDestinations} class
 * allows you to convert any input value into a custom destination object, and map that destination
 * object to a {@link FilenamePolicy}. This allows using different filename policies (or more
 * commonly, differently-configured instances of the same policy) based on the input record. Often
 * this is used in conjunction with {@link TextIO#writeCustomType}, which allows your {@link
 * DynamicDestinations} object to examine the input type and takes a format function to convert that
 * type to a string for writing.
 *
 * <p>A convenience shortcut is provided for the case where the default naming policy is used, but
 * different configurations of this policy are wanted based on the input record. Default naming
 * policies can be configured using the {@link DefaultFilenamePolicy.Params} object.
 *
 * <pre>{@code
 * PCollection<UserEvent>> lines = ...;
 * lines.apply(TextIO.<UserEvent>writeCustomType(new FormatEvent())
 *      .to(new SerializableFunction<UserEvent, Params>() {
 *         public String apply(UserEvent value) {
 *           return new Params().withBaseFilename(baseDirectory + "/" + value.country());
 *         }
 *       }),
 *       new Params().withBaseFilename(baseDirectory + "/empty");
 * }</pre>
 *
 * <p>Any existing files with the same names as generated output files will be overwritten.
 */
public class TextIO {
  /**
   * A {@link PTransform} that reads from one or more text files and returns a bounded
   * {@link PCollection} containing one element for each line of the input files.
   */
  public static Read read() {
    return new AutoValue_TextIO_Read.Builder()
        .setCompressionType(CompressionType.AUTO)
        .setHintMatchesManyFiles(false)
        .build();
  }

  /**
   * A {@link PTransform} that works like {@link #read}, but reads each file in a {@link
   * PCollection} of filepatterns.
   *
   * <p>Can be applied to both bounded and unbounded {@link PCollection PCollections}, so this is
   * suitable for reading a {@link PCollection} of filepatterns arriving as a stream. However, every
   * filepattern is expanded once at the moment it is processed, rather than watched for new files
   * matching the filepattern to appear. Likewise, every file is read once, rather than watched for
   * new entries.
   */
  public static ReadAll readAll() {
    return new AutoValue_TextIO_ReadAll.Builder()
        .setCompressionType(CompressionType.AUTO)
        // 64MB is a reasonable value that allows to amortize the cost of opening files,
        // but is not so large as to exhaust a typical runner's maximum amount of output per
        // ProcessElement call.
        .setDesiredBundleSizeBytes(64 * 1024 * 1024L)
        .build();
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
   * matching a sharding pattern), with each element of the input collection encoded into its own
   * line.
   */
  public static Write write() {
    return new TextIO.Write();
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
   * matching a sharding pattern), with each element of the input collection encoded into its own
   * line.
   *
   * <p>This version allows you to apply {@link TextIO} writes to a PCollection of a custom type
   * {@link UserT}. A format mechanism that converts the input type {@link UserT} to the String that
   * will be written to the file must be specified. If using a custom {@link DynamicDestinations}
   * object this is done using {@link DynamicDestinations#formatRecord}, otherwise the {@link
   * TypedWrite#withFormatFunction} can be used to specify a format function.
   *
   * <p>The advantage of using a custom type is that is it allows a user-provided {@link
   * DynamicDestinations} object, set via {@link Write#to(DynamicDestinations)} to examine the
   * custom type when choosing a destination.
   */
  public static <UserT> TypedWrite<UserT> writeCustomType() {
    return new AutoValue_TextIO_TypedWrite.Builder<UserT>()
        .setFilenamePrefix(null)
        .setTempDirectory(null)
        .setShardTemplate(null)
        .setFilenameSuffix(null)
        .setFilenamePolicy(null)
        .setDynamicDestinations(null)
        .setWritableByteChannelFactory(FileBasedSink.CompressionType.UNCOMPRESSED)
        .setWindowedWrites(false)
        .setNumShards(0)
        .build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {
    @Nullable abstract ValueProvider<String> getFilepattern();
    abstract CompressionType getCompressionType();
    abstract boolean getHintMatchesManyFiles();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);
      abstract Builder setCompressionType(CompressionType compressionType);
      abstract Builder setHintMatchesManyFiles(boolean hintManyFiles);

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
     *
     * <p>If it is known that the filepattern will match a very large number of files (at least tens
     * of thousands), use {@link #withHintMatchesManyFiles} for better performance and scalability.
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

    /**
     * Hints that the filepattern specified in {@link #from(String)} matches a very large number of
     * files.
     *
     * <p>This hint may cause a runner to execute the transform differently, in a way that improves
     * performance for this case, but it may worsen performance if the filepattern matches only
     * a small number of files (e.g., in a runner that supports dynamic work rebalancing, it will
     * happen less efficiently within individual files).
     */
    public Read withHintMatchesManyFiles() {
      return toBuilder().setHintMatchesManyFiles(true).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      checkNotNull(getFilepattern(), "need to set the filepattern of a TextIO.Read transform");
      return getHintMatchesManyFiles()
          ? input
              .apply(
                  "Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
              .apply(readAll().withCompressionType(getCompressionType()))
          : input.apply("Read", org.apache.beam.sdk.io.Read.from(getSource()));
    }

    // Helper to create a source specific to the requested compression type.
    protected FileBasedSource<String> getSource() {
      return wrapWithCompression(new TextSource(getFilepattern()), getCompressionType());
    }

    private static FileBasedSource<String> wrapWithCompression(
        FileBasedSource<String> source, CompressionType compressionType) {
      switch (compressionType) {
        case UNCOMPRESSED:
          return source;
        case AUTO:
          return CompressedSource.from(source);
        case BZIP2:
          return
              CompressedSource.from(source)
                  .withDecompression(CompressionMode.BZIP2);
        case GZIP:
          return
              CompressedSource.from(source)
                  .withDecompression(CompressionMode.GZIP);
        case ZIP:
          return
              CompressedSource.from(source)
                  .withDecompression(CompressionMode.ZIP);
        case DEFLATE:
          return
              CompressedSource.from(source)
                  .withDecompression(CompressionMode.DEFLATE);
        default:
          throw new IllegalArgumentException("Unknown compression type: " + compressionType);
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
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #readAll}. */
  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<String>, PCollection<String>> {
    abstract CompressionType getCompressionType();
    abstract long getDesiredBundleSizeBytes();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCompressionType(CompressionType compressionType);
      abstract Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract ReadAll build();
    }

    /** Same as {@link Read#withCompressionType(CompressionType)}. */
    public ReadAll withCompressionType(CompressionType compressionType) {
      return toBuilder().setCompressionType(compressionType).build();
    }

    @VisibleForTesting
    ReadAll withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      return input
          .apply(
              "Read all via FileBasedSource",
              new ReadAllViaFileBasedSource<>(
                  new IsSplittableFn(getCompressionType()),
                  getDesiredBundleSizeBytes(),
                  new CreateTextSourceFn(getCompressionType())))
          .setCoder(StringUtf8Coder.of());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.add(
          DisplayData.item("compressionType", getCompressionType().toString())
              .withLabel("Compression Type"));
    }

    private static class CreateTextSourceFn
        implements SerializableFunction<String, FileBasedSource<String>> {
      private final CompressionType compressionType;

      private CreateTextSourceFn(CompressionType compressionType) {
        this.compressionType = compressionType;
      }

      @Override
      public FileBasedSource<String> apply(String input) {
        return Read.wrapWithCompression(
            new TextSource(StaticValueProvider.of(input)), compressionType);
      }
    }

    private static class IsSplittableFn implements SerializableFunction<String, Boolean> {
      private final CompressionType compressionType;

      private IsSplittableFn(CompressionType compressionType) {
        this.compressionType = compressionType;
      }

      @Override
      public Boolean apply(String filename) {
        return compressionType == CompressionType.UNCOMPRESSED
            || (compressionType == CompressionType.AUTO && !CompressionMode.isCompressed(filename));
      }
    }
  }

  // ///////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class TypedWrite<UserT> extends PTransform<PCollection<UserT>, PDone> {
    /** The prefix of each file written, combined with suffix and shardTemplate. */
    @Nullable abstract ValueProvider<ResourceId> getFilenamePrefix();

    /** The suffix of each file written, combined with prefix and shardTemplate. */
    @Nullable abstract String getFilenameSuffix();

    /** The base directory used for generating temporary files. */
    @Nullable
    abstract ValueProvider<ResourceId> getTempDirectory();

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
    @Nullable
    abstract DynamicDestinations<UserT, ?, String> getDynamicDestinations();

    @Nullable
    /** A destination function for using {@link DefaultFilenamePolicy} */
    abstract SerializableFunction<UserT, Params> getDestinationFunction();

    @Nullable
    /** A default destination for empty PCollections. */
    abstract Params getEmptyDestination();

    /** A function that converts UserT to a String, for writing to the file. */
    @Nullable
    abstract SerializableFunction<UserT, String> getFormatFunction();

    /** Whether to write windowed output files. */
    abstract boolean getWindowedWrites();

    /**
     * The {@link WritableByteChannelFactory} to be used by the {@link FileBasedSink}. Default is
     * {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
     */
    abstract WritableByteChannelFactory getWritableByteChannelFactory();

    abstract Builder<UserT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<UserT> {
      abstract Builder<UserT> setFilenamePrefix(ValueProvider<ResourceId> filenamePrefix);

      abstract Builder<UserT> setTempDirectory(ValueProvider<ResourceId> tempDirectory);

      abstract Builder<UserT> setShardTemplate(@Nullable String shardTemplate);

      abstract Builder<UserT> setFilenameSuffix(@Nullable String filenameSuffix);

      abstract Builder<UserT> setHeader(@Nullable String header);

      abstract Builder<UserT> setFooter(@Nullable String footer);

      abstract Builder<UserT> setFilenamePolicy(@Nullable FilenamePolicy filenamePolicy);

      abstract Builder<UserT> setDynamicDestinations(
          @Nullable DynamicDestinations<UserT, ?, String> dynamicDestinations);

      abstract Builder<UserT> setDestinationFunction(
          @Nullable SerializableFunction<UserT, Params> destinationFunction);

      abstract Builder<UserT> setEmptyDestination(Params emptyDestination);

      abstract Builder<UserT> setFormatFunction(SerializableFunction<UserT, String> formatFunction);

      abstract Builder<UserT> setNumShards(int numShards);

      abstract Builder<UserT> setWindowedWrites(boolean windowedWrites);

      abstract Builder<UserT> setWritableByteChannelFactory(
          WritableByteChannelFactory writableByteChannelFactory);

      abstract TypedWrite<UserT> build();
    }

    /**
     * Writes to text files with the given prefix. The given {@code prefix} can reference any {@link
     * FileSystem} on the classpath. This prefix is used by the {@link DefaultFilenamePolicy} to
     * generate filenames.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will be used built using the specified prefix
     * to define the base output directory and file prefix, a shard identifier (see {@link
     * #withNumShards(int)}), and a common suffix (if supplied using {@link #withSuffix(String)}).
     *
     * <p>This default policy can be overridden using {@link #to(FilenamePolicy)}, in which case
     * {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should not be set.
     * Custom filename policies do not automatically see this prefix - you should explicitly pass
     * the prefix into your {@link FilenamePolicy} object if you need this.
     *
     * <p>If {@link #withTempDirectory} has not been called, this filename prefix will be used to
     * infer a directory for temporary files.
     */
    public TypedWrite<UserT> to(String filenamePrefix) {
      return to(FileBasedSink.convertToFileResourceIfPossible(filenamePrefix));
    }

    /** Like {@link #to(String)}. */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT> to(ResourceId filenamePrefix) {
      return toResource(StaticValueProvider.of(filenamePrefix));
    }

    /** Like {@link #to(String)}. */
    public TypedWrite<UserT> to(ValueProvider<String> outputPrefix) {
      return toResource(NestedValueProvider.of(outputPrefix,
          new SerializableFunction<String, ResourceId>() {
            @Override
            public ResourceId apply(String input) {
              return FileBasedSink.convertToFileResourceIfPossible(input);
            }
          }));
    }

    /**
     * Writes to files named according to the given {@link FileBasedSink.FilenamePolicy}. A
     * directory for temporary files must be specified using {@link #withTempDirectory}.
     */
    public TypedWrite<UserT> to(FilenamePolicy filenamePolicy) {
      return toBuilder().setFilenamePolicy(filenamePolicy).build();
    }

    /**
     * Use a {@link DynamicDestinations} object to vend {@link FilenamePolicy} objects. These
     * objects can examine the input record when creating a {@link FilenamePolicy}. A directory for
     * temporary files must be specified using {@link #withTempDirectory}.
     */
    public TypedWrite<UserT> to(DynamicDestinations<UserT, ?, String> dynamicDestinations) {
      return toBuilder().setDynamicDestinations(dynamicDestinations).build();
    }

    /**
     * Write to dynamic destinations using the default filename policy. The destinationFunction maps
     * the input record to a {@link DefaultFilenamePolicy.Params} object that specifies where the
     * records should be written (base filename, file suffix, and shard template). The
     * emptyDestination parameter specified where empty files should be written for when the written
     * {@link PCollection} is empty.
     */
    public TypedWrite<UserT> to(
        SerializableFunction<UserT, Params> destinationFunction, Params emptyDestination) {
      return toBuilder()
          .setDestinationFunction(destinationFunction)
          .setEmptyDestination(emptyDestination)
          .build();
    }

    /** Like {@link #to(ResourceId)}. */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT> toResource(ValueProvider<ResourceId> filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /**
     * Specifies a format function to convert {@link UserT} to the output type. If {@link
     * #to(DynamicDestinations)} is used, {@link DynamicDestinations#formatRecord(Object)} must be
     * used instead.
     */
    public TypedWrite<UserT> withFormatFunction(
        SerializableFunction<UserT, String> formatFunction) {
      return toBuilder().setFormatFunction(formatFunction).build();
    }

    /** Set the base directory used to generate temporary files. */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT> withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
      return toBuilder().setTempDirectory(tempDirectory).build();
    }

    /** Set the base directory used to generate temporary files. */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT> withTempDirectory(ResourceId tempDirectory) {
      return withTempDirectory(StaticValueProvider.of(tempDirectory));
    }

    /**
     * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
     * used when using one of the default filename-prefix to() overrides - i.e. not when using
     * either {@link #to(FilenamePolicy)} or {@link #to(DynamicDestinations)}.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public TypedWrite<UserT> withShardNameTemplate(String shardTemplate) {
      return toBuilder().setShardTemplate(shardTemplate).build();
    }

    /**
     * Configures the filename suffix for written files. This option may only be used when using one
     * of the default filename-prefix to() overrides - i.e. not when using either {@link
     * #to(FilenamePolicy)} or {@link #to(DynamicDestinations)}.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public TypedWrite<UserT> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
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
    public TypedWrite<UserT> withNumShards(int numShards) {
      checkArgument(numShards >= 0);
      return toBuilder().setNumShards(numShards).build();
    }

    /**
     * Forces a single file as output and empty shard name template.
     *
     * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
     * performance of a pipeline. Setting this value is not recommended unless you require a
     * specific number of output files.
     *
     * <p>This is equivalent to {@code .withNumShards(1).withShardNameTemplate("")}
     */
    public TypedWrite<UserT> withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    /**
     * Adds a header string to each file. A newline after the header is added automatically.
     *
     * <p>A {@code null} value will clear any previously configured header.
     */
    public TypedWrite<UserT> withHeader(@Nullable String header) {
      return toBuilder().setHeader(header).build();
    }

    /**
     * Adds a footer string to each file. A newline after the footer is added automatically.
     *
     * <p>A {@code null} value will clear any previously configured footer.
     */
    public TypedWrite<UserT> withFooter(@Nullable String footer) {
      return toBuilder().setFooter(footer).build();
    }

    /**
     * Returns a transform for writing to text files like this one but that has the given {@link
     * WritableByteChannelFactory} to be used by the {@link FileBasedSink} during output. The
     * default is value is {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
     *
     * <p>A {@code null} value will reset the value to the default value mentioned above.
     */
    public TypedWrite<UserT> withWritableByteChannelFactory(
        WritableByteChannelFactory writableByteChannelFactory) {
      return toBuilder().setWritableByteChannelFactory(writableByteChannelFactory).build();
    }

    /**
     * Preserves windowing of input elements and writes them to files based on the element's window.
     *
     * <p>If using {@link #to(FileBasedSink.FilenamePolicy)}. Filenames will be generated using
     * {@link FilenamePolicy#windowedFilename}. See also {@link WriteFiles#withWindowedWrites()}.
     */
    public TypedWrite<UserT> withWindowedWrites() {
      return toBuilder().setWindowedWrites(true).build();
    }

    private DynamicDestinations<UserT, ?, String> resolveDynamicDestinations() {
      DynamicDestinations<UserT, ?, String> dynamicDestinations = getDynamicDestinations();
      if (dynamicDestinations == null) {
        if (getDestinationFunction() != null) {
          dynamicDestinations =
              DynamicFileDestinations.toDefaultPolicies(
                  getDestinationFunction(), getEmptyDestination(), getFormatFunction());
        } else {
          FilenamePolicy usedFilenamePolicy = getFilenamePolicy();
          if (usedFilenamePolicy == null) {
            usedFilenamePolicy =
                DefaultFilenamePolicy.fromStandardParameters(
                    getFilenamePrefix(),
                    getShardTemplate(),
                    getFilenameSuffix(),
                    getWindowedWrites());
          }
          dynamicDestinations =
              DynamicFileDestinations.constant(usedFilenamePolicy, getFormatFunction());
        }
      }
      return dynamicDestinations;
    }

    @Override
    public PDone expand(PCollection<UserT> input) {
      checkState(
          getFilenamePrefix() != null || getTempDirectory() != null,
          "Need to set either the filename prefix or the tempDirectory of a TextIO.Write "
              + "transform.");

      List<?> allToArgs =
          Lists.newArrayList(
              getFilenamePolicy(),
              getDynamicDestinations(),
              getFilenamePrefix(),
              getDestinationFunction());
      checkArgument(
          1 == Iterables.size(Iterables.filter(allToArgs, Predicates.notNull())),
          "Exactly one of filename policy, dynamic destinations, filename prefix, or destination "
              + "function must be set");

      if (getDynamicDestinations() != null) {
        checkArgument(
            getFormatFunction() == null,
            "A format function should not be specified "
                + "with DynamicDestinations. Use DynamicDestinations.formatRecord instead");
      }
      if (getFilenamePolicy() != null || getDynamicDestinations() != null) {
        checkState(
            getShardTemplate() == null && getFilenameSuffix() == null,
            "shardTemplate and filenameSuffix should only be used with the default "
                + "filename policy");
      }
      return expandTyped(input, resolveDynamicDestinations());
    }

    public <DestinationT> PDone expandTyped(
        PCollection<UserT> input,
        DynamicDestinations<UserT, DestinationT, String> dynamicDestinations) {
      ValueProvider<ResourceId> tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = getFilenamePrefix();
      }
      WriteFiles<UserT, DestinationT, String> write =
          WriteFiles.to(
              new TextSink<>(
                  tempDirectory,
                  dynamicDestinations,
                  getHeader(),
                  getFooter(),
                  getWritableByteChannelFactory()));
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

      resolveDynamicDestinations().populateDisplayData(builder);
      String tempDirectory = null;
      if (getTempDirectory() != null) {
        tempDirectory =
            getTempDirectory().isAccessible()
                ? getTempDirectory().get().toString()
                : getTempDirectory().toString();
      }
      builder
          .addIfNotDefault(
              DisplayData.item("numShards", getNumShards()).withLabel("Maximum Output Shards"), 0)
          .addIfNotNull(
              DisplayData.item("tempDirectory", tempDirectory)
                  .withLabel("Directory for temporary files"))
          .addIfNotNull(DisplayData.item("fileHeader", getHeader()).withLabel("File Header"))
          .addIfNotNull(DisplayData.item("fileFooter", getFooter()).withLabel("File Footer"))
          .add(
              DisplayData.item(
                      "writableByteChannelFactory", getWritableByteChannelFactory().toString())
                  .withLabel("Compression/Transformation Type"));
    }
  }

  /**
   * This class is used as the default return value of {@link TextIO#write()}.
   *
   * <p>All methods in this class delegate to the appropriate method of {@link TextIO.TypedWrite}.
   * This class exists for backwards compatibility, and will be removed in Beam 3.0.
   */
  public static class Write extends PTransform<PCollection<String>, PDone> {
    @VisibleForTesting TypedWrite<String> inner;

    Write() {
      this(TextIO.<String>writeCustomType());
    }

    Write(TypedWrite<String> inner) {
      this.inner = inner;
    }

    /** See {@link TypedWrite#to(String)}. */
    public Write to(String filenamePrefix) {
      return new Write(
          inner.to(filenamePrefix).withFormatFunction(SerializableFunctions.<String>identity()));
    }

    /** See {@link TypedWrite#to(ResourceId)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write to(ResourceId filenamePrefix) {
      return new Write(
          inner.to(filenamePrefix).withFormatFunction(SerializableFunctions.<String>identity()));
    }

    /** See {@link TypedWrite#to(ValueProvider)}. */
    public Write to(ValueProvider<String> outputPrefix) {
      return new Write(
          inner.to(outputPrefix).withFormatFunction(SerializableFunctions.<String>identity()));
    }

    /** See {@link TypedWrite#toResource(ValueProvider)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write toResource(ValueProvider<ResourceId> filenamePrefix) {
      return new Write(
          inner
              .toResource(filenamePrefix)
              .withFormatFunction(SerializableFunctions.<String>identity()));
    }

    /** See {@link TypedWrite#to(FilenamePolicy)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write to(FilenamePolicy filenamePolicy) {
      return new Write(
          inner.to(filenamePolicy).withFormatFunction(SerializableFunctions.<String>identity()));
    }

    /** See {@link TypedWrite#to(DynamicDestinations)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write to(DynamicDestinations<String, ?, String> dynamicDestinations) {
      return new Write(inner.to(dynamicDestinations).withFormatFunction(null));
    }

    /** See {@link TypedWrite#to(SerializableFunction, Params)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write to(
        SerializableFunction<String, Params> destinationFunction, Params emptyDestination) {
      return new Write(
          inner
              .to(destinationFunction, emptyDestination)
              .withFormatFunction(SerializableFunctions.<String>identity()));
    }

    /** See {@link TypedWrite#withTempDirectory(ValueProvider)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
      return new Write(inner.withTempDirectory(tempDirectory));
    }

    /** See {@link TypedWrite#withTempDirectory(ResourceId)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write withTempDirectory(ResourceId tempDirectory) {
      return new Write(inner.withTempDirectory(tempDirectory));
    }

    /** See {@link TypedWrite#withShardNameTemplate(String)}. */
    public Write withShardNameTemplate(String shardTemplate) {
      return new Write(inner.withShardNameTemplate(shardTemplate));
    }

    /** See {@link TypedWrite#withSuffix(String)}. */
    public Write withSuffix(String filenameSuffix) {
      return new Write(inner.withSuffix(filenameSuffix));
    }

    /** See {@link TypedWrite#withNumShards(int)}. */
    public Write withNumShards(int numShards) {
      return new Write(inner.withNumShards(numShards));
    }

    /** See {@link TypedWrite#withoutSharding()}. */
    public Write withoutSharding() {
      return new Write(inner.withoutSharding());
    }

    /** See {@link TypedWrite#withHeader(String)}. */
    public Write withHeader(@Nullable String header) {
      return new Write(inner.withHeader(header));
    }

    /** See {@link TypedWrite#withFooter(String)}. */
    public Write withFooter(@Nullable String footer) {
      return new Write(inner.withFooter(footer));
    }

    /** See {@link TypedWrite#withWritableByteChannelFactory(WritableByteChannelFactory)}. */
    public Write withWritableByteChannelFactory(
        WritableByteChannelFactory writableByteChannelFactory) {
      return new Write(inner.withWritableByteChannelFactory(writableByteChannelFactory));
    }

    /** See {@link TypedWrite#withWindowedWrites}. */
    public Write withWindowedWrites() {
      return new Write(inner.withWindowedWrites());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      inner.populateDisplayData(builder);
    }

    @Override
    public PDone expand(PCollection<String> input) {
      return inner.expand(input);
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
