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
import static org.apache.beam.sdk.io.TextIO.CompressionType.UNCOMPRESSED;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.OutputStream;
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
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.options.PipelineOptions;
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
 * <p>By default, {@link TextIO.Read} returns a {@link PCollection} of {@link String Strings},
 * each corresponding to one line of an input UTF-8 text file. To convert directly from the raw
 * bytes (split into lines delimited by '\n', '\r', or '\r\n') to another object of type {@code T},
 * supply a {@code Coder<T>} using {@link TextIO.Read#withCoder(Coder)}.
 *
 * <p>See the following examples:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<String> lines =
 *     p.apply(TextIO.Read.from("/local/path/to/file.txt"));
 *
 * // A fully-specified Read from a GCS file (runs locally and via the
 * // Google Cloud Dataflow service):
 * PCollection<Integer> numbers =
 *     p.apply("ReadNumbers", TextIO.Read
 *         .from("gs://my_bucket/path/to/numbers-*.txt")
 *         .withCoder(TextualIntegerCoder.of()));
 * }</pre>
 *
 * <p>To write a {@link PCollection} to one or more text files, use
 * {@link TextIO.Write}, specifying {@link TextIO.Write#to(String)} to specify
 * the path of the file to write to (e.g., a local filename or sharded
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or sharded filename pattern of the form
 * {@code "gs://<bucket>/<filepath>"}). You can use {@link TextIO.Write#withCoder(Coder)}
 * to specify the {@link Coder} to use to encode the Java values into text lines.
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
 * // A fully-specified Write to a sharded GCS file (runs locally and via the
 * // Google Cloud Dataflow service):
 * PCollection<Integer> numbers = ...;
 * numbers.apply("WriteNumbers", TextIO.Write
 *      .to("gs://my_bucket/path/to/numbers")
 *      .withSuffix(".txt")
 *      .withCoder(TextualIntegerCoder.of()));
 * }</pre>
 *
 * <h3>Permissions</h3>
 * <p>When run using the {@code DirectRunner}, your pipeline can read and write text files
 * on your local drive and remote text files on Google Cloud Storage that you have access to using
 * your {@code gcloud} credentials. When running in the Dataflow service, the pipeline can only
 * read and write files from GCS. For more information about permissions, see the Cloud Dataflow
 * documentation on <a href="https://cloud.google.com/dataflow/security-and-permissions">Security
 * and Permissions</a>.
 */
public class TextIO {
  /** The default coder, which returns each line of the input file as a string. */
  public static final Coder<String> DEFAULT_TEXT_CODER = StringUtf8Coder.of();

  /**
   * A {@link PTransform} that reads from a text file (or multiple text
   * files matching a pattern) and returns a {@link PCollection} containing
   * the decoding of each of the lines of the text file(s). The
   * default decoding just returns each line as a {@link String}, but you may call
   * {@link #withCoder(Coder)} to change the return type.
   */
  public static class Read {

    /**
     * Returns a transform for reading text files that reads from the file(s)
     * with the given filename or filename pattern. This can be a local path (if running locally),
     * or a Google Cloud Storage filename or filename pattern of the form
     * {@code "gs://<bucket>/<filepath>"} (if running locally or via the Google Cloud Dataflow
     * service). Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html"
     * >Java Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public static Bound<String> from(String filepattern) {
      return new Bound<>(DEFAULT_TEXT_CODER).from(filepattern);
    }

    /**
     * Returns a transform for reading text files that uses the given
     * {@code Coder<T>} to decode each of the lines of the file into a
     * value of type {@code T}.
     *
     * <p>By default, uses {@link StringUtf8Coder}, which just
     * returns the text lines as Java strings.
     *
     * @param <T> the type of the decoded elements, and the elements
     * of the resulting PCollection
     */
    public static <T> Bound<T> withCoder(Coder<T> coder) {
      return new Bound<>(coder);
    }

    /**
     * Returns a transform for reading text files that has GCS path validation on
     * pipeline creation disabled.
     *
     * <p>This can be useful in the case where the GCS input does not
     * exist at the pipeline creation time, but is expected to be
     * available at execution time.
     */
    public static Bound<String> withoutValidation() {
      return new Bound<>(DEFAULT_TEXT_CODER).withoutValidation();
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
    public static Bound<String> withCompressionType(TextIO.CompressionType compressionType) {
      return new Bound<>(DEFAULT_TEXT_CODER).withCompressionType(compressionType);
    }

    // TODO: strippingNewlines, etc.

    /**
     * A {@link PTransform} that reads from one or more text files and returns a bounded
     * {@link PCollection} containing one element for each line of the input files.
     *
     * @param <T> the type of each of the elements of the resulting
     * {@link PCollection}. By default, each line is returned as a {@link String}, however you
     * may use {@link #withCoder(Coder)} to supply a {@code Coder<T>} to produce a
     * {@code PCollection<T>} instead.
     */
    public static class Bound<T> extends PTransform<PBegin, PCollection<T>> {
      /** The filepattern to read from. */
      @Nullable private final String filepattern;

      /** The Coder to use to decode each line. */
      private final Coder<T> coder;

      /** An option to indicate if input validation is desired. Default is true. */
      private final boolean validate;

      /** Option to indicate the input source's compression type. Default is AUTO. */
      private final TextIO.CompressionType compressionType;

      Bound(Coder<T> coder) {
        this(null, null, coder, true, TextIO.CompressionType.AUTO);
      }

      private Bound(String name, String filepattern, Coder<T> coder, boolean validate,
          TextIO.CompressionType compressionType) {
        super(name);
        this.coder = coder;
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
      public Bound<T> from(String filepattern) {
        return new Bound<>(name, filepattern, coder, validate, compressionType);
      }

      /**
       * Returns a new transform for reading from text files that's like this one but
       * that uses the given {@link Coder Coder<X>} to decode each of the
       * lines of the file into a value of type {@code X}.
       *
       * <p>Does not modify this object.
       *
       * @param <X> the type of the decoded elements, and the
       * elements of the resulting PCollection
       */
      public <X> Bound<X> withCoder(Coder<X> coder) {
        return new Bound<>(name, filepattern, coder, validate, compressionType);
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
      public Bound<T> withoutValidation() {
        return new Bound<>(name, filepattern, coder, false, compressionType);
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
      public Bound<T> withCompressionType(TextIO.CompressionType compressionType) {
        return new Bound<>(name, filepattern, coder, validate, compressionType);
      }

      @Override
      public PCollection<T> apply(PBegin input) {
        if (filepattern == null) {
          throw new IllegalStateException("need to set the filepattern of a TextIO.Read transform");
        }

        if (validate) {
          try {
            checkState(
                !IOChannelUtils.getFactory(filepattern).match(filepattern).isEmpty(),
                "Unable to find any files matching %s",
                filepattern);
          } catch (IOException e) {
            throw new IllegalStateException(
                String.format("Failed to validate %s", filepattern), e);
          }
        }

        final Bounded<T> read = org.apache.beam.sdk.io.Read.from(getSource());
        PCollection<T> pcol = input.getPipeline().apply("Read", read);
        // Honor the default output coder that would have been used by this PTransform.
        pcol.setCoder(getDefaultOutputCoder());
        return pcol;
      }

      // Helper to create a source specific to the requested compression type.
      protected FileBasedSource<T> getSource() {
        switch (compressionType) {
          case UNCOMPRESSED:
            return new TextSource<T>(filepattern, coder);
          case AUTO:
            return CompressedSource.from(new TextSource<T>(filepattern, coder));
          case BZIP2:
            return
                CompressedSource.from(new TextSource<T>(filepattern, coder))
                    .withDecompression(CompressedSource.CompressionMode.BZIP2);
          case GZIP:
            return
                CompressedSource.from(new TextSource<T>(filepattern, coder))
                    .withDecompression(CompressedSource.CompressionMode.GZIP);
          case ZIP:
            return
                CompressedSource.from(new TextSource<T>(filepattern, coder))
                    .withDecompression(CompressedSource.CompressionMode.ZIP);
          default:
            throw new IllegalArgumentException("Unknown compression type: " + compressionType);
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);

        builder
            .add(DisplayData.item("compressionType", compressionType.toString())
              .withLabel("Compression Type"))
            .addIfNotDefault(DisplayData.item("validation", validate)
              .withLabel("Validation Enabled"), true)
            .addIfNotNull(DisplayData.item("filePattern", filepattern)
              .withLabel("File Pattern"));
      }

      @Override
      protected Coder<T> getDefaultOutputCoder() {
        return coder;
      }

      public String getFilepattern() {
        return filepattern;
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
     * (if running locally or via the Google Cloud Dataflow service).
     *
     * <p>The files written will begin with this prefix, followed by
     * a shard identifier (see {@link Bound#withNumShards(int)}, and end
     * in a common extension, if given by {@link Bound#withSuffix(String)}.
     */
    public static Bound<String> to(String prefix) {
      return new Bound<>(DEFAULT_TEXT_CODER).to(prefix);
    }

    /**
     * Returns a transform for writing to text files that appends the specified suffix
     * to the created files.
     */
    public static Bound<String> withSuffix(String nameExtension) {
      return new Bound<>(DEFAULT_TEXT_CODER).withSuffix(nameExtension);
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
    public static Bound<String> withNumShards(int numShards) {
      return new Bound<>(DEFAULT_TEXT_CODER).withNumShards(numShards);
    }

    /**
     * Returns a transform for writing to text files that uses the given shard name
     * template.
     *
     * <p>See {@link ShardNameTemplate} for a description of shard templates.
     */
    public static Bound<String> withShardNameTemplate(String shardTemplate) {
      return new Bound<>(DEFAULT_TEXT_CODER).withShardNameTemplate(shardTemplate);
    }

    /**
     * Returns a transform for writing to text files that forces a single file as
     * output.
     */
    public static Bound<String> withoutSharding() {
      return new Bound<>(DEFAULT_TEXT_CODER).withoutSharding();
    }

    /**
     * Returns a transform for writing to text files that uses the given
     * {@link Coder} to encode each of the elements of the input
     * {@link PCollection} into an output text line.
     *
     * <p>By default, uses {@link StringUtf8Coder}, which writes input
     * Java strings directly as output lines.
     *
     * @param <T> the type of the elements of the input {@link PCollection}
     */
    public static <T> Bound<T> withCoder(Coder<T> coder) {
      return new Bound<>(coder);
    }

    /**
     * Returns a transform for writing to text files that has GCS path validation on
     * pipeline creation disabled.
     *
     * <p>This can be useful in the case where the GCS output location does
     * not exist at the pipeline creation time, but is expected to be available
     * at execution time.
     */
    public static Bound<String> withoutValidation() {
      return new Bound<>(DEFAULT_TEXT_CODER).withoutValidation();
    }

    /**
     * Returns a transform for writing to text files that adds a header string to the files
     * it writes. Note that a newline character will be added after the header.
     *
     * <p>A {@code null} value will clear any previously configured header.
     *
     * @param header the string to be added as file header
     */
    public static Bound<String> withHeader(@Nullable String header) {
      return new Bound<>(DEFAULT_TEXT_CODER).withHeader(header);
    }

    /**
     * Returns a transform for writing to text files that adds a footer string to the files
     * it writes. Note that a newline character will be added after the header.
     *
     * <p>A {@code null} value will clear any previously configured footer.
     *
     * @param footer the string to be added as file footer
     */
    public static Bound<String> withFooter(@Nullable String footer) {
      return new Bound<>(DEFAULT_TEXT_CODER).withFooter(footer);
    }

    // TODO: appendingNewlines, etc.

    /**
     * A PTransform that writes a bounded PCollection to a text file (or
     * multiple text files matching a sharding pattern), with each
     * PCollection element being encoded into its own line.
     *
     * @param <T> the type of the elements of the input PCollection
     */
    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {
      private static final String DEFAULT_SHARD_TEMPLATE = ShardNameTemplate.INDEX_OF_MAX;

      /** The prefix of each file written, combined with suffix and shardTemplate. */
      @Nullable private final String filenamePrefix;
      /** The suffix of each file written, combined with prefix and shardTemplate. */
      private final String filenameSuffix;

      /** An optional header to add to each file. */
      @Nullable private final String header;

      /** An optional footer to add to each file. */
      @Nullable private final String footer;

      /** The Coder to use to decode each line. */
      private final Coder<T> coder;

      /** Requested number of shards. 0 for automatic. */
      private final int numShards;

      /** The shard template of each file written, combined with prefix and suffix. */
      private final String shardTemplate;

      /** An option to indicate if output validation is desired. Default is true. */
      private final boolean validate;

      Bound(Coder<T> coder) {
        this(null, null, "", null, null, coder, 0, DEFAULT_SHARD_TEMPLATE, true);
      }

      private Bound(String name, String filenamePrefix, String filenameSuffix,
          @Nullable String header, @Nullable String footer, Coder<T> coder, int numShards,
          String shardTemplate, boolean validate) {
        super(name);
        this.header = header;
        this.footer = footer;
        this.coder = coder;
        this.filenamePrefix = filenamePrefix;
        this.filenameSuffix = filenameSuffix;
        this.numShards = numShards;
        this.shardTemplate = shardTemplate;
        this.validate = validate;
      }

      /**
       * Returns a transform for writing to text files that's like this one but
       * that writes to the file(s) with the given filename prefix.
       *
       * <p>See {@link TextIO.Write#to(String) Write.to(String)} for more information.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> to(String filenamePrefix) {
        validateOutputComponent(filenamePrefix);
        return new Bound<>(name, filenamePrefix, filenameSuffix, header, footer, coder, numShards,
            shardTemplate, validate);
      }

      /**
       * Returns a transform for writing to text files that that's like this one but
       * that writes to the file(s) with the given filename suffix.
       *
       * <p>Does not modify this object.
       *
       * @see ShardNameTemplate
       */
      public Bound<T> withSuffix(String nameExtension) {
        validateOutputComponent(nameExtension);
        return new Bound<>(name, filenamePrefix, nameExtension, header, footer, coder, numShards,
            shardTemplate, validate);
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
      public Bound<T> withNumShards(int numShards) {
        checkArgument(numShards >= 0);
        return new Bound<>(name, filenamePrefix, filenameSuffix, header, footer, coder, numShards,
            shardTemplate, validate);
      }

      /**
       * Returns a transform for writing to text files that's like this one but
       * that uses the given shard name template.
       *
       * <p>Does not modify this object.
       *
       * @see ShardNameTemplate
       */
      public Bound<T> withShardNameTemplate(String shardTemplate) {
        return new Bound<>(name, filenamePrefix, filenameSuffix, header, footer, coder, numShards,
            shardTemplate, validate);
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
      public Bound<T> withoutSharding() {
        return new Bound<>(name, filenamePrefix, filenameSuffix, header, footer, coder, 1, "",
            validate);
      }

      /**
       * Returns a transform for writing to text files that's like this one
       * but that uses the given {@link Coder Coder<X>} to encode each of
       * the elements of the input {@link PCollection PCollection<X>} into an
       * output text line. Does not modify this object.
       *
       * @param <X> the type of the elements of the input {@link PCollection}
       */
      public <X> Bound<X> withCoder(Coder<X> coder) {
        return new Bound<>(name, filenamePrefix, filenameSuffix, header, footer, coder, numShards,
            shardTemplate, validate);
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
      public Bound<T> withoutValidation() {
        return new Bound<>(name, filenamePrefix, filenameSuffix, header, footer, coder, numShards,
            shardTemplate, false);
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
      public Bound<T> withHeader(@Nullable String header) {
        return new Bound<>(name, filenamePrefix, filenameSuffix, header, footer, coder, numShards,
            shardTemplate, false);
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
      public Bound<T> withFooter(@Nullable String footer) {
        return new Bound<>(name, filenamePrefix, filenameSuffix, header, footer, coder, numShards,
            shardTemplate, false);
      }

      @Override
      public PDone apply(PCollection<T> input) {
        if (filenamePrefix == null) {
          throw new IllegalStateException(
              "need to set the filename prefix of a TextIO.Write transform");
        }

        org.apache.beam.sdk.io.Write.Bound<T> write =
            org.apache.beam.sdk.io.Write.to(
                new TextSink<>(filenamePrefix, filenameSuffix, header, footer, shardTemplate,
                    coder));
        if (getNumShards() > 0) {
          write = write.withNumShards(getNumShards());
        }
        return input.apply("Write", write);
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);

        builder
            .addIfNotNull(DisplayData.item("filePrefix", filenamePrefix)
              .withLabel("Output File Prefix"))
            .addIfNotDefault(DisplayData.item("fileSuffix", filenameSuffix)
              .withLabel("Output Fix Suffix"), "")
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
                .withLabel("File Footer"));
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
        return filenamePrefix;
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

      public Coder<T> getCoder() {
        return coder;
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
  public static enum CompressionType {
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
    ZIP(".zip");

    private String filenameSuffix;

    private CompressionType(String suffix) {
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
  static class TextSource<T> extends FileBasedSource<T> {
    /** The Coder to use to decode each line. */
    private final Coder<T> coder;

    @VisibleForTesting
    TextSource(String fileSpec, Coder<T> coder) {
      super(fileSpec, 1L);
      this.coder = coder;
    }

    private TextSource(String fileName, long start, long end, Coder<T> coder) {
      super(fileName, 1L, start, end);
      this.coder = coder;
    }

    @Override
    protected FileBasedSource<T> createForSubrangeOfFile(String fileName, long start, long end) {
      return new TextSource<>(fileName, start, end, coder);
    }

    @Override
    protected FileBasedReader<T> createSingleFileReader(PipelineOptions options) {
      return new TextBasedReader<>(this);
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return coder;
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSource.FileBasedReader FileBasedReader}
     * which can decode records delimited by newline characters.
     *
     * See {@link TextSource} for further details.
     */
    @VisibleForTesting
    static class TextBasedReader<T> extends FileBasedReader<T> {
      private static final int READ_BUFFER_SIZE = 8192;
      private final Coder<T> coder;
      private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
      private ByteString buffer;
      private int startOfSeparatorInBuffer;
      private int endOfSeparatorInBuffer;
      private long startOfRecord;
      private volatile long startOfNextRecord;
      private volatile boolean eof;
      private volatile boolean elementIsPresent;
      private T currentValue;
      private ReadableByteChannel inChannel;

      private TextBasedReader(TextSource<T> source) {
        super(source);
        coder = source.coder;
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
      public T getCurrent() throws NoSuchElementException {
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
       * This invalidates the currently stored {@code startOfSeparatorInBuffer} and
       * {@code endOfSeparatorInBuffer}.
       */
      private void decodeCurrentElement() throws IOException {
        ByteString dataToDecode = buffer.substring(0, startOfSeparatorInBuffer);
        currentValue = coder.decode(dataToDecode.newInput(), Context.OUTER);
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
  static class TextSink<T> extends FileBasedSink<T> {
    private final Coder<T> coder;
    @Nullable private final String header;
    @Nullable private final String footer;

    @VisibleForTesting
    TextSink(
        String baseOutputFilename, String extension,
        @Nullable String header, @Nullable String footer,
        String fileNameTemplate, Coder<T> coder) {
      super(baseOutputFilename, extension, fileNameTemplate);
      this.coder = coder;
      this.header = header;
      this.footer = footer;
    }

    @Override
    public FileBasedSink.FileBasedWriteOperation<T> createWriteOperation(PipelineOptions options) {
      return new TextWriteOperation<>(this, coder, header, footer);
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSink.FileBasedWriteOperation
     * FileBasedWriteOperation} for text files.
     */
    private static class TextWriteOperation<T> extends FileBasedWriteOperation<T> {
      private final Coder<T> coder;
      @Nullable private final String header;
      @Nullable private final String footer;

      private TextWriteOperation(TextSink<T> sink, Coder<T> coder,
          @Nullable String header, @Nullable String footer) {
        super(sink);
        this.coder = coder;
        this.header = header;
        this.footer = footer;
      }

      @Override
      public FileBasedWriter<T> createWriter(PipelineOptions options) throws Exception {
        return new TextWriter<>(this, coder, header, footer);
      }
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSink.FileBasedWriter FileBasedWriter}
     * for text files.
     */
    private static class TextWriter<T> extends FileBasedWriter<T> {
      private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
      private final Coder<T> coder;
      @Nullable private final String header;
      @Nullable private final String footer;
      private OutputStream out;

      public TextWriter(FileBasedWriteOperation<T> writeOperation, Coder<T> coder,
          @Nullable String header, @Nullable String footer) {
        super(writeOperation);
        this.header = header;
        this.footer = footer;
        this.mimeType = MimeTypes.TEXT;
        this.coder = coder;
      }

      /**
       * Writes {@code value} followed by a newline if {@code value} is not null.
       */
      private void writeIfNotNull(@Nullable String value) throws IOException {
        if (value != null) {
          out.write(value.getBytes(StandardCharsets.UTF_8));
          out.write(NEWLINE);
        }
      }

      @Override
      protected void prepareWrite(WritableByteChannel channel) throws Exception {
        out = Channels.newOutputStream(channel);
      }

      @Override
      protected void writeHeader() throws Exception {
        writeIfNotNull(header);
      }

      @Override
      protected void writeFooter() throws Exception {
        writeIfNotNull(footer);
      }

      @Override
      public void write(T value) throws Exception {
        coder.encode(value, out, Context.OUTER);
        out.write(NEWLINE);
      }
    }
  }
}
