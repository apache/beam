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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * {@link PTransform}s for reading and writing text files.
 *
 * <p>To read a {@link PCollection} from one or more text files, use {@link TextIO.Read}.
 * You can instantiate a transform using {@link TextIO.Read#from(String)} to specify
 * the path of the file(s) to read from (e.g., a local filename or
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or filename pattern of the form
 * {@code "gs://<bucket>/<filepath>"}).
 *
 * <p>{@link TextIO.Read} returns a {@link PCollection} of {@link String Strings},
 * each corresponding to one line of an input UTF-8 text file (split into lines delimited by '\n',
 * '\r', or '\r\n').
 *
 * <p>Example:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<String> lines =
 *     p.apply(TextIO.Read.from("/local/path/to/file.txt"));
 * }</pre>
 *
 * <p>To write a {@link PCollection} to one or more text files, use
 * {@link TextIO.Write}, specifying {@link TextIO.Write#to(String)} to specify
 * the path of the file to write to (e.g., a local filename or sharded
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or sharded filename pattern of the form
 * {@code "gs://<bucket>/<filepath>"}).
 *
 * <p>By default, all input is put into the global window before writing. If per-window writes are
 * desired - for example, when using a streaming runner -
 * {@link AvroIO.Write.Bound#withWindowedWrites()} will cause windowing and triggering to be
 * preserved. When producing windowed writes, the number of output shards must be set explicitly
 * using {@link AvroIO.Write.Bound#withNumShards(int)}; some runners may set this for you to a
 * runner-chosen value, so you may need not set it yourself. A {@link FilenamePolicy} must be
 * set, and unique windows and triggers must produce unique filenames.
 *
 * <p>Any existing files with the same names as generated output files
 * will be overwritten.
 *
 * <p>For example:
 * <pre>{@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.Write.to("/path/to/file.txt"));
 *
 * // Same as above, only with Gzip compression:
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.Write.to("/path/to/file.txt"));
 *      .withSuffix(".txt")
 *      .withWritableByteChannelFactory(FileBasedSink.CompressionType.GZIP));
 * }</pre>
 */
public class TextIO {
  /** The default coder, which returns each line of the input file as a string. */
  public static final Coder<String> DEFAULT_TEXT_CODER = StringUtf8Coder.of();

  /**
   * A {@link PTransform} that reads from a text file (or multiple text
   * files matching a pattern) and returns a {@link PCollection} containing
   * the decoding of each of the lines of the text file(s) as a {@link String}.
   */
  public static class Read {

    /**
     * Returns a transform for reading text files that reads from the file(s)
     * with the given filename or filename pattern. This can be a local path (if running locally),
     * or a Google Cloud Storage filename or filename pattern of the form
     * {@code "gs://<bucket>/<filepath>"} (if running locally or using remote execution)
     * service). Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html"
     * >Java Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public static Bound from(String filepattern) {
      return new Bound().from(filepattern);
    }

    /**
     * Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}.
     */
    public static Bound from(ValueProvider<String> filepattern) {
      return new Bound().from(filepattern);
    }

    /**
     * Returns a transform for reading text files that has GCS path validation on
     * pipeline creation disabled.
     *
     * <p>This can be useful in the case where the GCS input does not
     * exist at the pipeline creation time, but is expected to be
     * available at execution time.
     */
    public static Bound withoutValidation() {
      return new Bound().withoutValidation();
    }

    /**
     * Returns a transform for reading text files that decompresses all input files
     * using the specified compression type.
     *
     * <p>If no compression type is specified, the default is {@link TextIO.CompressionType#AUTO}.
     * In this mode, the compression type of the file is determined by its extension
     * (e.g., {@code *.gz} is gzipped, {@code *.bz2} is bzipped, and all other extensions are
     * uncompressed).
     */
    public static Bound withCompressionType(TextIO.CompressionType compressionType) {
      return new Bound().withCompressionType(compressionType);
    }

    // TODO: strippingNewlines, etc.

    /**
     * A {@link PTransform} that reads from one or more text files and returns a bounded
     * {@link PCollection} containing one element for each line of the input files.
     */
    public static class Bound extends PTransform<PBegin, PCollection<String>> {
      /** The filepattern to read from. */
      @Nullable private final ValueProvider<String> filepattern;

      /** An option to indicate if input validation is desired. Default is true. */
      private final boolean validate;

      /** Option to indicate the input source's compression type. Default is AUTO. */
      private final TextIO.CompressionType compressionType;

      private Bound() {
        this(null, null, true, TextIO.CompressionType.AUTO);
      }

      private Bound(
          @Nullable String name,
          @Nullable ValueProvider<String> filepattern,
          boolean validate,
          TextIO.CompressionType compressionType) {
        super(name);
        this.filepattern = filepattern;
        this.validate = validate;
        this.compressionType = compressionType;
      }

      /**
       * Returns a new transform for reading from text files that's like this one but
       * that reads from the file(s) with the given name or pattern. See {@link TextIO.Read#from}
       * for a description of filepatterns.
       *
       * <p>Does not modify this object.

       */
      public Bound from(String filepattern) {
        checkNotNull(filepattern, "Filepattern cannot be empty.");
        return new Bound(name, StaticValueProvider.of(filepattern), validate,
                           compressionType);
      }

      /**
       * Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}.
       */
      public Bound from(ValueProvider<String> filepattern) {
        checkNotNull(filepattern, "Filepattern cannot be empty.");
        return new Bound(name, filepattern, validate, compressionType);
      }

      /**
       * Returns a new transform for reading from text files that's like this one but
       * that has GCS path validation on pipeline creation disabled.
       *
       * <p>This can be useful in the case where the GCS input does not
       * exist at the pipeline creation time, but is expected to be
       * available at execution time.
       *
       * <p>Does not modify this object.
       */
      public Bound withoutValidation() {
        return new Bound(name, filepattern, false, compressionType);
      }

      /**
       * Returns a new transform for reading from text files that's like this one but
       * reads from input sources using the specified compression type.
       *
       * <p>If no compression type is specified, the default is {@link TextIO.CompressionType#AUTO}.
       * See {@link TextIO.Read#withCompressionType} for more details.
       *
       * <p>Does not modify this object.
       */
      public Bound withCompressionType(TextIO.CompressionType compressionType) {
        return new Bound(name, filepattern, validate, compressionType);
      }

      @Override
      public PCollection<String> expand(PBegin input) {
        if (filepattern == null) {
          throw new IllegalStateException("need to set the filepattern of a TextIO.Read transform");
        }

        if (validate) {
          checkState(filepattern.isAccessible(), "Cannot validate with a RVP.");
          try {
            checkState(
              !IOChannelUtils.getFactory(filepattern.get()).match(filepattern.get()).isEmpty(),
                "Unable to find any files matching %s",
                filepattern);
          } catch (IOException e) {
            throw new IllegalStateException(
              String.format("Failed to validate %s", filepattern.get()), e);
          }
        }

        final Bounded<String> read = org.apache.beam.sdk.io.Read.from(getSource());
        PCollection<String> pcol = input.getPipeline().apply("Read", read);
        // Honor the default output coder that would have been used by this PTransform.
        pcol.setCoder(getDefaultOutputCoder());
        return pcol;
      }

      // Helper to create a source specific to the requested compression type.
      protected FileBasedSource<String> getSource() {
        switch (compressionType) {
          case UNCOMPRESSED:
            return new TextSource(filepattern);
          case AUTO:
            return CompressedSource.from(new TextSource(filepattern));
          case BZIP2:
            return
                CompressedSource.from(new TextSource(filepattern))
                    .withDecompression(CompressedSource.CompressionMode.BZIP2);
          case GZIP:
            return
                CompressedSource.from(new TextSource(filepattern))
                    .withDecompression(CompressedSource.CompressionMode.GZIP);
          case ZIP:
            return
                CompressedSource.from(new TextSource(filepattern))
                    .withDecompression(CompressedSource.CompressionMode.ZIP);
          case DEFLATE:
            return
                CompressedSource.from(new TextSource(filepattern))
                    .withDecompression(CompressedSource.CompressionMode.DEFLATE);
          default:
            throw new IllegalArgumentException("Unknown compression type: " + compressionType);
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);

        String filepatternDisplay = filepattern.isAccessible()
          ? filepattern.get() : filepattern.toString();
        builder
            .add(DisplayData.item("compressionType", compressionType.toString())
              .withLabel("Compression Type"))
            .addIfNotDefault(DisplayData.item("validation", validate)
              .withLabel("Validation Enabled"), true)
            .addIfNotNull(DisplayData.item("filePattern", filepatternDisplay)
              .withLabel("File Pattern"));
      }

      @Override
      protected Coder<String> getDefaultOutputCoder() {
        return DEFAULT_TEXT_CODER;
      }

      public String getFilepattern() {
        return filepattern.get();
      }

      public boolean needsValidation() {
        return validate;
      }

      public TextIO.CompressionType getCompressionType() {
        return compressionType;
      }
    }

    /** Disallow construction of utility classes. */
    private Read() {}
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link PTransform} that writes a {@link PCollection} to text file (or
   * multiple text files matching a sharding pattern), with each
   * element of the input collection encoded into its own line.
   */
  public static class Write {

    /**
     * Returns a transform for writing to text files that writes to the file(s)
     * with the given prefix. This can be a local filename
     * (if running locally), or a Google Cloud Storage filename of
     * the form {@code "gs://<bucket>/<filepath>"}
     * (if running locally or using remote execution).
     *
     * <p>The files written will begin with this prefix, followed by
     * a shard identifier (see {@link Bound#withNumShards(int)}, and end
     * in a common extension, if given by {@link Bound#withSuffix(String)}.
     */
    public static Bound to(String prefix) {
      return new Bound().to(prefix);
    }

    public static Bound to(FilenamePolicy filenamePolicy) {
      return new Bound().to(filenamePolicy);

    }
    /**
     * Like {@link #to(String)}, but with a {@link ValueProvider}.
     */
    public static Bound to(ValueProvider<String> prefix) {
      return new Bound().to(prefix);
    }

    /**
     * Returns a transform for writing to text files that appends the specified suffix
     * to the created files.
     */
    public static Bound withSuffix(String nameExtension) {
      return new Bound().withSuffix(nameExtension);
    }

    /**
     * Returns a transform for writing to text files that uses the provided shard count.
     *
     * <p>Constraining the number of shards is likely to reduce
     * the performance of a pipeline. Setting this value is not recommended
     * unless you require a specific number of output files.
     *
     * @param numShards the number of shards to use, or 0 to let the system
     *                  decide.
     */
    public static Bound withNumShards(int numShards) {
      return new Bound().withNumShards(numShards);
    }

    /**
     * Returns a transform for writing to text files that uses the given shard name
     * template.
     *
     * <p>See {@link ShardNameTemplate} for a description of shard templates.
     */
    public static Bound withShardNameTemplate(String shardTemplate) {
      return new Bound().withShardNameTemplate(shardTemplate);
    }

    /**
     * Returns a transform for writing to text files that forces a single file as
     * output.
     */
    public static Bound withoutSharding() {
      return new Bound().withoutSharding();
    }

    /**
     * Returns a transform for writing to text files that has GCS path validation on
     * pipeline creation disabled.
     *
     * <p>This can be useful in the case where the GCS output location does
     * not exist at the pipeline creation time, but is expected to be available
     * at execution time.
     */
    public static Bound withoutValidation() {
      return new Bound().withoutValidation();
    }

    /**
     * Returns a transform for writing to text files that adds a header string to the files
     * it writes. Note that a newline character will be added after the header.
     *
     * <p>A {@code null} value will clear any previously configured header.
     *
     * @param header the string to be added as file header
     */
    public static Bound withHeader(@Nullable String header) {
      return new Bound().withHeader(header);
    }

    /**
     * Returns a transform for writing to text files that adds a footer string to the files
     * it writes. Note that a newline character will be added after the header.
     *
     * <p>A {@code null} value will clear any previously configured footer.
     *
     * @param footer the string to be added as file footer
     */
    public static Bound withFooter(@Nullable String footer) {
      return new Bound().withFooter(footer);
    }

    /**
     * Returns a transform for writing to text files like this one but that has the given
     * {@link WritableByteChannelFactory} to be used by the {@link FileBasedSink} during output. The
     * default is value is {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
     *
     * <p>A {@code null} value will reset the value to the default value mentioned above.
     *
     * @param writableByteChannelFactory the factory to be used during output
     */
    public static Bound withWritableByteChannelFactory(
        WritableByteChannelFactory writableByteChannelFactory) {
      return new Bound().withWritableByteChannelFactory(writableByteChannelFactory);
    }

    // TODO: appendingNewlines, etc.

    /**
     * A PTransform that writes a bounded PCollection to a text file (or
     * multiple text files matching a sharding pattern), with each
     * PCollection element being encoded into its own line.
     */
    public static class Bound extends PTransform<PCollection<String>, PDone> {
      private static final String DEFAULT_SHARD_TEMPLATE = ShardNameTemplate.INDEX_OF_MAX;

      /** The prefix of each file written, combined with suffix and shardTemplate. */
      private final ValueProvider<String> filenamePrefix;
      /** The suffix of each file written, combined with prefix and shardTemplate. */
      private final String filenameSuffix;

      /** An optional header to add to each file. */
      @Nullable private final String header;

      /** An optional footer to add to each file. */
      @Nullable private final String footer;

      /** Requested number of shards. 0 for automatic. */
      private final int numShards;

      /** The shard template of each file written, combined with prefix and suffix. */
      private final String shardTemplate;

      /** An option to indicate if output validation is desired. Default is true. */
      private final boolean validate;

      /** A policy for naming output files. */
      private final FilenamePolicy filenamePolicy;

      /** Whether to write windowed output files. */
      private boolean windowedWrites;

      /**
       * The {@link WritableByteChannelFactory} to be used by the {@link FileBasedSink}. Default is
       * {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
       */
      private final WritableByteChannelFactory writableByteChannelFactory;

      private Bound() {
        this(null, null, "", null, null, 0, DEFAULT_SHARD_TEMPLATE, true,
            FileBasedSink.CompressionType.UNCOMPRESSED, null, false);
      }

      private Bound(String name, ValueProvider<String> filenamePrefix, String filenameSuffix,
          @Nullable String header, @Nullable String footer, int numShards,
          String shardTemplate, boolean validate,
          WritableByteChannelFactory writableByteChannelFactory,
          FilenamePolicy filenamePolicy,
          boolean windowedWrites) {
        super(name);
        this.header = header;
        this.footer = footer;
        this.filenamePrefix = filenamePrefix;
        this.filenameSuffix = filenameSuffix;
        this.numShards = numShards;
        this.shardTemplate = shardTemplate;
        this.validate = validate;
        this.writableByteChannelFactory =
            firstNonNull(writableByteChannelFactory, FileBasedSink.CompressionType.UNCOMPRESSED);
        this.filenamePolicy = filenamePolicy;
        this.windowedWrites = windowedWrites;
      }

      /**
       * Returns a transform for writing to text files that's like this one but
       * that writes to the file(s) with the given filename prefix.
       *
       * <p>See {@link TextIO.Write#to(String) Write.to(String)} for more information.
       *
       * <p>Does not modify this object.
       */
      public Bound to(String filenamePrefix) {
        validateOutputComponent(filenamePrefix);
        return new Bound(name, StaticValueProvider.of(filenamePrefix), filenameSuffix,
            header, footer, numShards, shardTemplate, validate,
            writableByteChannelFactory, filenamePolicy, windowedWrites);
      }

      /**
       * Like {@link #to(String)}, but with a {@link ValueProvider}.
       */
      public Bound to(ValueProvider<String> filenamePrefix) {
        return new Bound(name, filenamePrefix, filenameSuffix, header, footer, numShards,
            shardTemplate, validate, writableByteChannelFactory, filenamePolicy, windowedWrites);
      }

       /**
        * Like {@link #to(String)}, but with a {@link FilenamePolicy}.
        */
      public Bound to(FilenamePolicy filenamePolicy) {
        return new Bound(name, filenamePrefix, filenameSuffix, header, footer, numShards,
            shardTemplate, validate, writableByteChannelFactory, filenamePolicy, windowedWrites);
      }

      /**
       * Returns a transform for writing to text files that that's like this one but
       * that writes to the file(s) with the given filename suffix.
       *
       * <p>Does not modify this object.
       *
       * @see ShardNameTemplate
       */
      public Bound withSuffix(String nameExtension) {
        validateOutputComponent(nameExtension);
        return new Bound(name, filenamePrefix, nameExtension, header, footer, numShards,
            shardTemplate, validate, writableByteChannelFactory, filenamePolicy, windowedWrites);
      }

      /**
       * Returns a transform for writing to text files that's like this one but
       * that uses the provided shard count.
       *
       * <p>Constraining the number of shards is likely to reduce
       * the performance of a pipeline. Setting this value is not recommended
       * unless you require a specific number of output files.
       *
       * <p>Does not modify this object.
       *
       * @param numShards the number of shards to use, or 0 to let the system
       *                  decide.
       * @see ShardNameTemplate
       */
      public Bound withNumShards(int numShards) {
        checkArgument(numShards >= 0);
        return new Bound(name, filenamePrefix, filenameSuffix, header, footer, numShards,
            shardTemplate, validate, writableByteChannelFactory, filenamePolicy, windowedWrites);
      }

      /**
       * Returns a transform for writing to text files that's like this one but
       * that uses the given shard name template.
       *
       * <p>Does not modify this object.
       *
       * @see ShardNameTemplate
       */
      public Bound withShardNameTemplate(String shardTemplate) {
        return new Bound(name, filenamePrefix, filenameSuffix, header, footer, numShards,
            shardTemplate, validate, writableByteChannelFactory, filenamePolicy, windowedWrites);
      }

      /**
       * Returns a transform for writing to text files that's like this one but
       * that forces a single file as output.
       *
       * <p>Constraining the number of shards is likely to reduce
       * the performance of a pipeline. Using this setting is not recommended
       * unless you truly require a single output file.
       *
       * <p>This is a shortcut for
       * {@code .withNumShards(1).withShardNameTemplate("")}
       *
       * <p>Does not modify this object.
       */
      public Bound withoutSharding() {
        return new Bound(name, filenamePrefix, filenameSuffix, header, footer, 1, "",
            validate, writableByteChannelFactory, filenamePolicy, windowedWrites);
      }

      /**
       * Returns a transform for writing to text files that's like this one but
       * that has GCS output path validation on pipeline creation disabled.
       *
       * <p>This can be useful in the case where the GCS output location does
       * not exist at the pipeline creation time, but is expected to be
       * available at execution time.
       *
       * <p>Does not modify this object.
       */
      public Bound withoutValidation() {
        return new Bound(name, filenamePrefix, filenameSuffix, header, footer, numShards,
            shardTemplate, false, writableByteChannelFactory, filenamePolicy, windowedWrites);
      }

      /**
       * Returns a transform for writing to text files that adds a header string to the files
       * it writes. Note that a newline character will be added after the header.
       *
       * <p>A {@code null} value will clear any previously configured header.
       *
       * <p>Does not modify this object.
       *
       * @param header the string to be added as file header
       */
      public Bound withHeader(@Nullable String header) {
        return new Bound(name, filenamePrefix, filenameSuffix, header, footer, numShards,
            shardTemplate, validate, writableByteChannelFactory, filenamePolicy, windowedWrites);
      }

      /**
       * Returns a transform for writing to text files that adds a footer string to the files
       * it writes. Note that a newline character will be added after the header.
       *
       * <p>A {@code null} value will clear any previously configured footer.
       *
       * <p>Does not modify this object.
       *
       * @param footer the string to be added as file footer
       */
      public Bound withFooter(@Nullable String footer) {
        return new Bound(name, filenamePrefix, filenameSuffix, header, footer, numShards,
            shardTemplate, validate, writableByteChannelFactory, filenamePolicy, windowedWrites);
      }

      /**
       * Returns a transform for writing to text files like this one but that has the given
       * {@link WritableByteChannelFactory} to be used by the {@link FileBasedSink} during output.
       * The default is value is {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
       *
       * <p>A {@code null} value will reset the value to the default value mentioned above.
       *
       * <p>Does not modify this object.
       *
       * @param writableByteChannelFactory the factory to be used during output
       */
      public Bound withWritableByteChannelFactory(
          WritableByteChannelFactory writableByteChannelFactory) {
        return new Bound(name, filenamePrefix, filenameSuffix, header, footer, numShards,
            shardTemplate, validate, writableByteChannelFactory, filenamePolicy, windowedWrites);
      }

      public Bound withWindowedWrites() {
        return new Bound(name, filenamePrefix, filenameSuffix, header, footer, numShards,
            shardTemplate, validate, writableByteChannelFactory, filenamePolicy, true);
      }

      @Override
      public PDone expand(PCollection<String> input) {
        if (filenamePolicy == null && filenamePrefix == null) {
          throw new IllegalStateException(
              "need to set the filename prefix of an TextIO.Write transform");
        }
        if (filenamePolicy != null && filenamePrefix != null) {
          throw new IllegalStateException(
              "cannot set both a filename policy and a filename prefix");
        }
        org.apache.beam.sdk.io.Write<String> write = null;
        if (filenamePolicy != null) {
         write = org.apache.beam.sdk.io.Write.to(
             new TextSink(filenamePolicy, header, footer, writableByteChannelFactory));
        } else {
          write = org.apache.beam.sdk.io.Write.to(
              new TextSink(filenamePrefix, filenameSuffix, header, footer, shardTemplate,
                  writableByteChannelFactory));
        }
        if (getNumShards() > 0) {
          write = write.withNumShards(getNumShards());
        }
        if (windowedWrites) {
          write = write.withWindowedWrites();
        }
        return input.apply("Write", write);
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);

        String prefixString = "";
        if (filenamePrefix != null) {
          prefixString = filenamePrefix.isAccessible()
              ? filenamePrefix.get() : filenamePrefix.toString();
        }
        builder
            .addIfNotNull(DisplayData.item("filePrefix", prefixString)
              .withLabel("Output File Prefix"))
            .addIfNotDefault(DisplayData.item("fileSuffix", filenameSuffix)
              .withLabel("Output File Suffix"), "")
            .addIfNotDefault(DisplayData.item("shardNameTemplate", shardTemplate)
              .withLabel("Output Shard Name Template"),
                DEFAULT_SHARD_TEMPLATE)
            .addIfNotDefault(DisplayData.item("validation", validate)
              .withLabel("Validation Enabled"), true)
            .addIfNotDefault(DisplayData.item("numShards", numShards)
              .withLabel("Maximum Output Shards"), 0)
            .addIfNotNull(DisplayData.item("fileHeader", header)
              .withLabel("File Header"))
            .addIfNotNull(DisplayData.item("fileFooter", footer)
                .withLabel("File Footer"))
            .add(DisplayData
                .item("writableByteChannelFactory", writableByteChannelFactory.toString())
                .withLabel("Compression/Transformation Type"));
      }

      /**
       * Returns the current shard name template string.
       */
      public String getShardNameTemplate() {
        return shardTemplate;
      }

      @Override
      protected Coder<Void> getDefaultOutputCoder() {
        return VoidCoder.of();
      }

      public String getFilenamePrefix() {
        return filenamePrefix.get();
      }

      public String getShardTemplate() {
        return shardTemplate;
      }

      public int getNumShards() {
        return numShards;
      }

      public String getFilenameSuffix() {
        return filenameSuffix;
      }

      @Nullable
      public String getHeader() {
        return header;
      }

      @Nullable
      public String getFooter() {
        return footer;
      }

      public boolean needsValidation() {
        return validate;
      }
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

  // Pattern which matches old-style shard output patterns, which are now
  // disallowed.
  private static final Pattern SHARD_OUTPUT_PATTERN = Pattern.compile("@([0-9]+|\\*)");

  private static void validateOutputComponent(String partialFilePattern) {
    checkArgument(
        !SHARD_OUTPUT_PATTERN.matcher(partialFilePattern).find(),
        "Output name components are not allowed to contain @* or @N patterns: "
        + partialFilePattern);
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Disable construction of utility class. */
  private TextIO() {}

  /**
   * A {@link FileBasedSource} which can decode records delimited by newline characters.
   *
   * <p>This source splits the data into records using {@code UTF-8} {@code \n}, {@code \r}, or
   * {@code \r\n} as the delimiter. This source is not strict and supports decoding the last record
   * even if it is not delimited. Finally, no records are decoded if the stream is empty.
   *
   * <p>This source supports reading from any arbitrary byte position within the stream. If the
   * starting position is not {@code 0}, then bytes are skipped until the first delimiter is found
   * representing the beginning of the first record to be decoded.
   */
  @VisibleForTesting
  static class TextSource extends FileBasedSource<String> {
    /** The Coder to use to decode each line. */
    @VisibleForTesting
    TextSource(String fileSpec) {
      super(fileSpec, 1L);
    }

    @VisibleForTesting
    TextSource(ValueProvider<String> fileSpec) {
      super(fileSpec, 1L);
    }

    private TextSource(String fileName, long start, long end) {
      super(fileName, 1L, start, end);
    }

    @Override
    protected FileBasedSource<String> createForSubrangeOfFile(
        String fileName,
        long start,
        long end) {
      return new TextSource(fileName, start, end);
    }

    @Override
    protected FileBasedReader<String> createSingleFileReader(PipelineOptions options) {
      return new TextBasedReader(this);
    }

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return DEFAULT_TEXT_CODER;
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSource.FileBasedReader FileBasedReader}
     * which can decode records delimited by newline characters.
     *
     * <p>See {@link TextSource} for further details.
     */
    @VisibleForTesting
    static class TextBasedReader extends FileBasedReader<String> {
      private static final int READ_BUFFER_SIZE = 8192;
      private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
      private ByteString buffer;
      private int startOfSeparatorInBuffer;
      private int endOfSeparatorInBuffer;
      private long startOfRecord;
      private volatile long startOfNextRecord;
      private volatile boolean eof;
      private volatile boolean elementIsPresent;
      private String currentValue;
      private ReadableByteChannel inChannel;

      private TextBasedReader(TextSource source) {
        super(source);
        buffer = ByteString.EMPTY;
      }

      @Override
      protected long getCurrentOffset() throws NoSuchElementException {
        if (!elementIsPresent) {
          throw new NoSuchElementException();
        }
        return startOfRecord;
      }

      @Override
      public long getSplitPointsRemaining() {
        if (isStarted() && startOfNextRecord >= getCurrentSource().getEndOffset()) {
          return isDone() ? 0 : 1;
        }
        return super.getSplitPointsRemaining();
      }

      @Override
      public String getCurrent() throws NoSuchElementException {
        if (!elementIsPresent) {
          throw new NoSuchElementException();
        }
        return currentValue;
      }

      @Override
      protected void startReading(ReadableByteChannel channel) throws IOException {
        this.inChannel = channel;
        // If the first offset is greater than zero, we need to skip bytes until we see our
        // first separator.
        if (getCurrentSource().getStartOffset() > 0) {
          checkState(channel instanceof SeekableByteChannel,
              "%s only supports reading from a SeekableByteChannel when given a start offset"
              + " greater than 0.", TextSource.class.getSimpleName());
          long requiredPosition = getCurrentSource().getStartOffset() - 1;
          ((SeekableByteChannel) channel).position(requiredPosition);
          findSeparatorBounds();
          buffer = buffer.substring(endOfSeparatorInBuffer);
          startOfNextRecord = requiredPosition + endOfSeparatorInBuffer;
          endOfSeparatorInBuffer = 0;
          startOfSeparatorInBuffer = 0;
        }
      }

      /**
       * Locates the start position and end position of the next delimiter. Will
       * consume the channel till either EOF or the delimiter bounds are found.
       *
       * <p>This fills the buffer and updates the positions as follows:
       * <pre>{@code
       * ------------------------------------------------------
       * | element bytes | delimiter bytes | unconsumed bytes |
       * ------------------------------------------------------
       * 0            start of          end of              buffer
       *              separator         separator           size
       *              in buffer         in buffer
       * }</pre>
       */
      private void findSeparatorBounds() throws IOException {
        int bytePositionInBuffer = 0;
        while (true) {
          if (!tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 1)) {
            startOfSeparatorInBuffer = endOfSeparatorInBuffer = bytePositionInBuffer;
            break;
          }

          byte currentByte = buffer.byteAt(bytePositionInBuffer);

          if (currentByte == '\n') {
            startOfSeparatorInBuffer = bytePositionInBuffer;
            endOfSeparatorInBuffer = startOfSeparatorInBuffer + 1;
            break;
          } else if (currentByte == '\r') {
            startOfSeparatorInBuffer = bytePositionInBuffer;
            endOfSeparatorInBuffer = startOfSeparatorInBuffer + 1;

            if (tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 2)) {
              currentByte = buffer.byteAt(bytePositionInBuffer + 1);
              if (currentByte == '\n') {
                endOfSeparatorInBuffer += 1;
              }
            }
            break;
          }

          // Move to the next byte in buffer.
          bytePositionInBuffer += 1;
        }
      }

      @Override
      protected boolean readNextRecord() throws IOException {
        startOfRecord = startOfNextRecord;
        findSeparatorBounds();

        // If we have reached EOF file and consumed all of the buffer then we know
        // that there are no more records.
        if (eof && buffer.size() == 0) {
          elementIsPresent = false;
          return false;
        }

        decodeCurrentElement();
        startOfNextRecord = startOfRecord + endOfSeparatorInBuffer;
        return true;
      }

      /**
       * Decodes the current element updating the buffer to only contain the unconsumed bytes.
       *
       * <p>This invalidates the currently stored {@code startOfSeparatorInBuffer} and
       * {@code endOfSeparatorInBuffer}.
       */
      private void decodeCurrentElement() throws IOException {
        ByteString dataToDecode = buffer.substring(0, startOfSeparatorInBuffer);
        currentValue = dataToDecode.toStringUtf8();
        elementIsPresent = true;
        buffer = buffer.substring(endOfSeparatorInBuffer);
      }

      /**
       * Returns false if we were unable to ensure the minimum capacity by consuming the channel.
       */
      private boolean tryToEnsureNumberOfBytesInBuffer(int minCapacity) throws IOException {
        // While we aren't at EOF or haven't fulfilled the minimum buffer capacity,
        // attempt to read more bytes.
        while (buffer.size() <= minCapacity && !eof) {
          eof = inChannel.read(readBuffer) == -1;
          readBuffer.flip();
          buffer = buffer.concat(ByteString.copyFrom(readBuffer));
          readBuffer.clear();
        }
        // Return true if we were able to honor the minimum buffer capacity request
        return buffer.size() >= minCapacity;
      }
    }
  }

  /**
   * A {@link FileBasedSink} for text files. Produces text files with the newline separator
   * {@code '\n'} represented in {@code UTF-8} format as the record separator.
   * Each record (including the last) is terminated.
   */
  @VisibleForTesting
  static class TextSink extends FileBasedSink<String> {
    @Nullable private final String header;
    @Nullable private final String footer;

    @VisibleForTesting
    TextSink(FilenamePolicy filenamePolicy, @Nullable String header, @Nullable String footer,
             WritableByteChannelFactory writableByteChannelFactory) {
      super(filenamePolicy, writableByteChannelFactory);
      this.header = header;
      this.footer = footer;
    }
    @VisibleForTesting
    TextSink(
        ValueProvider<String> baseOutputFilename,
        String extension,
        @Nullable String header,
        @Nullable String footer,
        String fileNameTemplate,
        WritableByteChannelFactory writableByteChannelFactory) {
      super(baseOutputFilename, extension, fileNameTemplate, writableByteChannelFactory);
      this.header = header;
      this.footer = footer;
    }

    @Override
    public FileBasedSink.FileBasedWriteOperation<String> createWriteOperation(
        PipelineOptions options) {
      return new TextWriteOperation(this, header, footer);
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSink.FileBasedWriteOperation
     * FileBasedWriteOperation} for text files.
     */
    private static class TextWriteOperation extends FileBasedWriteOperation<String> {
      @Nullable private final String header;
      @Nullable private final String footer;

      private TextWriteOperation(TextSink sink, @Nullable String header, @Nullable String footer) {
        super(sink);
        this.header = header;
        this.footer = footer;
      }

      @Override
      public FileBasedWriter createWriter(PipelineOptions options) throws Exception {
        return new TextWriter(this, header, footer);
      }
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSink.FileBasedWriter FileBasedWriter}
     * for text files.
     */
    private static class TextWriter extends FileBasedWriter<String> {
      private static final String NEWLINE = "\n";
      @Nullable private final String header;
      @Nullable private final String footer;
      private OutputStreamWriter out;

      public TextWriter(
          FileBasedWriteOperation<String> writeOperation,
          @Nullable String header,
          @Nullable String footer) {
        super(writeOperation);
        this.header = header;
        this.footer = footer;
        this.mimeType = MimeTypes.TEXT;
      }

      /**
       * Writes {@code value} followed by a newline character if {@code value} is not null.
       */
      private void writeIfNotNull(@Nullable String value) throws IOException {
        if (value != null) {
          writeLine(value);
        }
      }

      /**
       * Writes {@code value} followed by newline character.
       */
      private void writeLine(String value) throws IOException {
        out.write(value);
        out.write(NEWLINE);
      }

      @Override
      protected void prepareWrite(WritableByteChannel channel) throws Exception {
        out = new OutputStreamWriter(Channels.newOutputStream(channel), StandardCharsets.UTF_8);
      }

      @Override
      protected void writeHeader() throws Exception {
        writeIfNotNull(header);
      }

      @Override
      public void write(String value) throws Exception {
        writeLine(value);
      }

      @Override
      protected void writeFooter() throws Exception {
        writeIfNotNull(footer);
      }

      @Override
      protected void finishWrite() throws Exception {
        out.flush();
      }
    }
  }
}
