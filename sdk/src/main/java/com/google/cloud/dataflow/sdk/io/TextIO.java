/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.worker.TextReader;
import com.google.cloud.dataflow.sdk.runners.worker.TextSink;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.ReaderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.annotation.Nullable;

/**
 * {@link PTransform}s for reading and writing text files.
 *
 * <p>To read a {@link PCollection} from one or more text files, use {@link TextIO.Read}.
 * You can instantiate a transform using {@link TextIO.Read#from(String)} to specify
 * the path of the file(s) to read from (e.g., a local filename or
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or filename pattern of the form
 * {@code "gs://<bucket>/<filepath>"}). You may optionally call
 * {@link TextIO.Read#named(String)} to specify the name of the pipeline step.
 *
 * <p>By default, {@link TextIO.Read} returns a {@link PCollection} of {@link String Strings},
 * each corresponding to one line of an input UTF-8 text file. To convert directly from the raw
 * bytes (split into lines delimited by '\n', '\r', or '\r\n') to another object of type {@code T},
 * supply a {@code Coder<T>} using {@link TextIO.Read#withCoder(Coder)}.
 *
 * <p>See the following examples:
 *
 * <pre> {@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<String> lines =
 *     p.apply(TextIO.Read.from("/local/path/to/file.txt"));
 *
 * // A fully-specified Read from a GCS file (runs locally and via the
 * // Google Cloud Dataflow service):
 * PCollection<Integer> numbers =
 *     p.apply(TextIO.Read.named("ReadNumbers")
 *                        .from("gs://my_bucket/path/to/numbers-*.txt")
 *                        .withCoder(TextualIntegerCoder.of()));
 * } </pre>
 *
 * <p>To write a {@link PCollection} to one or more text files, use
 * {@link TextIO.Write}, specifying {@link TextIO.Write#to(String)} to specify
 * the path of the file to write to (e.g., a local filename or sharded
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or sharded filename pattern of the form
 * {@code "gs://<bucket>/<filepath>"}). You can optionally name the resulting transform using
 * {@link TextIO.Write#named(String)}, and you can use {@link TextIO.Write#withCoder(Coder)}
 * to specify the Coder to use to encode the Java values into text lines.
 *
 * <p>Any existing files with the same names as generated output files
 * will be overwritten.
 *
 * <p>For example:
 * <pre> {@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.Write.to("/path/to/file.txt"));
 *
 * // A fully-specified Write to a sharded GCS file (runs locally and via the
 * // Google Cloud Dataflow service):
 * PCollection<Integer> numbers = ...;
 * numbers.apply(TextIO.Write.named("WriteNumbers")
 *                           .to("gs://my_bucket/path/to/numbers")
 *                           .withSuffix(".txt")
 *                           .withCoder(TextualIntegerCoder.of()));
 * } </pre>
 *
 * <h3>Permissions</h3>
 * <p>When run using the {@link DirectPipelineRunner}, your pipeline can read and write text files
 * on your local drive and remote text files on Google Cloud Storage that you have access to using
 * your {@code gcloud} credentials. When running in the Dataflow service using
 * {@link DataflowPipelineRunner}, the pipeline can only read and write files from GCS. For more
 * information about permissions, see the Cloud Dataflow documentation on
 * <a href="https://cloud.google.com/dataflow/security-and-permissions">Security and
 * Permissions</a>.
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
     * Returns a transform for reading text files that uses the given step name.
     */
    public static Bound<String> named(String name) {
      return new Bound<>(DEFAULT_TEXT_CODER).named(name);
    }

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
    public static class Bound<T> extends PTransform<PInput, PCollection<T>> {
      /** The filepattern to read from. */
      @Nullable private final String filepattern;

      /** The Coder to use to decode each line. */
      @Nullable private final Coder<T> coder;

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
       * with the given step name.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> named(String name) {
        return new Bound<>(name, filepattern, coder, validate, compressionType);
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
      public PCollection<T> apply(PInput input) {
        if (filepattern == null) {
          throw new IllegalStateException("need to set the filepattern of a TextIO.Read transform");
        }
        // Force the output's Coder to be what the read is using, and
        // unchangeable later, to ensure that we read the input in the
        // format specified by the Read transform.
        return PCollection.<T>createPrimitiveOutputInternal(
                input.getPipeline(),
                WindowingStrategy.globalDefault(),
                IsBounded.BOUNDED)
            .setCoder(coder);
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

      static {
        DirectPipelineRunner.registerDefaultTransformEvaluator(
            Bound.class, new DirectPipelineRunner.TransformEvaluator<Bound>() {
              @Override
              public void evaluate(
                  Bound transform, DirectPipelineRunner.EvaluationContext context) {
                evaluateReadHelper(transform, context);
              }
            });
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
     * Returns a transform for writing to text files with the given step name.
     */
    public static Bound<String> named(String name) {
      return new Bound<>(DEFAULT_TEXT_CODER).named(name);
    }

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

    // TODO: appendingNewlines, header, footer, etc.

    /**
     * A PTransform that writes a bounded PCollection to a text file (or
     * multiple text files matching a sharding pattern), with each
     * PCollection element being encoded into its own line.
     *
     * @param <T> the type of the elements of the input PCollection
     */
    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {
      /** The prefix of each file written, combined with suffix and shardTemplate. */
      @Nullable private final String filenamePrefix;
      /** The suffix of each file written, combined with prefix and shardTemplate. */
      private final String filenameSuffix;

      /** The Coder to use to decode each line. */
      private final Coder<T> coder;

      /** Requested number of shards. 0 for automatic. */
      private final int numShards;

      /** Insert a shuffle before writing to decouple parallelism when numShards != 0. */
      private final boolean forceReshard;

      /** The shard template of each file written, combined with prefix and suffix. */
      private final String shardTemplate;

      /** An option to indicate if output validation is desired. Default is true. */
      private final boolean validate;

      Bound(Coder<T> coder) {
        this(null, null, "", coder, 0, true, ShardNameTemplate.INDEX_OF_MAX, true);
      }

      private Bound(String name, String filenamePrefix, String filenameSuffix, Coder<T> coder,
          int numShards, boolean forceReshard, String shardTemplate, boolean validate) {
        super(name);
        this.coder = coder;
        this.filenamePrefix = filenamePrefix;
        this.filenameSuffix = filenameSuffix;
        this.numShards = numShards;
        this.forceReshard = forceReshard;
        this.shardTemplate = shardTemplate;
        this.validate = validate;
      }

      /**
       * Returns a transform for writing to text files that's like this one but
       * with the given step name.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> named(String name) {
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards,
            forceReshard, shardTemplate, validate);
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
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards, forceReshard,
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
        return new Bound<>(name, filenamePrefix, nameExtension, coder, numShards, forceReshard,
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
        return withNumShards(numShards, forceReshard);
      }

      /**
       * Returns a transform for writing to text files that's like this one but
       * that uses the provided shard count.
       *
       * <p>Constraining the number of shards is likely to reduce
       * the performance of a pipeline. If forceReshard is true, the output
       * will be shuffled to obtain the desired sharding. If it is false,
       * data will not be reshuffled, but parallelism of preceeding stages
       * may be constrained. Setting this value is not recommended
       * unless you require a specific number of output files.
       *
       * <p>Does not modify this object.
       *
       * @param numShards the number of shards to use, or 0 to let the system
       *                  decide.
       * @param forceReshard whether to force a reshard to obtain the desired sharding.
       * @see ShardNameTemplate
       */
      private Bound<T> withNumShards(int numShards, boolean forceReshard) {
        Preconditions.checkArgument(numShards >= 0);
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards, forceReshard,
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
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards, forceReshard,
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
        return withoutSharding(forceReshard);
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
       * {@code .withNumShards(1, forceReshard).withShardNameTemplate("")}
       *
       * <p>Does not modify this object.
       */
      private Bound<T> withoutSharding(boolean forceReshard) {
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, 1, forceReshard, "",
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
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards, forceReshard,
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
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards, forceReshard,
            shardTemplate, false);
      }

      @Override
      public PDone apply(PCollection<T> input) {
        if (filenamePrefix == null) {
          throw new IllegalStateException(
              "need to set the filename prefix of a TextIO.Write transform");
        }
        if (numShards > 0 && forceReshard) {
          // Reshard and re-apply a version of this write without resharding.
          return input
              .apply(new FileBasedSink.ReshardForWrite<T>())
              .apply(withNumShards(numShards, false));
        } else {
          return PDone.in(input.getPipeline());
        }
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

      public boolean needsValidation() {
        return validate;
      }

      static {
        DirectPipelineRunner.registerDefaultTransformEvaluator(
            Bound.class, new DirectPipelineRunner.TransformEvaluator<Bound>() {
              @Override
              public void evaluate(
                  Bound transform, DirectPipelineRunner.EvaluationContext context) {
                evaluateWriteHelper(transform, context);
              }
            });
      }
    }
  }

  /**
   * Possible text file compression types.
   */
  public static enum CompressionType implements TextReader.DecompressingStreamFactory {
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
    GZIP(".gz") {
      @Override
      public InputStream createInputStream(InputStream inputStream) throws IOException {
        // Determine if the input stream is gzipped. The input stream returned from the
        // GCS connector may already be decompressed, and no action is required.
        PushbackInputStream stream = new PushbackInputStream(inputStream, 2);
        byte[] headerBytes = new byte[2];
        int bytesRead = stream.read(headerBytes);
        stream.unread(headerBytes, 0, bytesRead);
        int header = Ints.fromBytes((byte) 0, (byte) 0, headerBytes[1], headerBytes[0]);
        if (header == GZIPInputStream.GZIP_MAGIC) {
          return new GZIPInputStream(stream);
        }
        return stream;
      }
    },
    /**
     * BZipped.
     */
    BZIP2(".bz2") {
      @Override
      public InputStream createInputStream(InputStream inputStream) throws IOException {
        return new BZip2CompressorInputStream(inputStream);
      }
    };

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

    @Override
    public InputStream createInputStream(InputStream inputStream) throws IOException {
      return inputStream;
    }
  }

  // Pattern which matches old-style shard output patterns, which are now
  // disallowed.
  private static final Pattern SHARD_OUTPUT_PATTERN = Pattern.compile("@([0-9]+|\\*)");

  private static void validateOutputComponent(String partialFilePattern) {
    Preconditions.checkArgument(
        !SHARD_OUTPUT_PATTERN.matcher(partialFilePattern).find(),
        "Output name components are not allowed to contain @* or @N patterns: "
        + partialFilePattern);
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Disable construction of utility class. */
  private TextIO() {}

  private static <T> void evaluateReadHelper(
      Read.Bound<T> transform, DirectPipelineRunner.EvaluationContext context) {
    TextReader<T> reader =
        new TextReader<>(transform.filepattern, true, null, null, transform.coder,
            transform.getCompressionType());
    List<T> elems = ReaderUtils.readElemsFromReader(reader);
    context.setPCollection(context.getOutput(transform), elems);
  }

  private static <T> void evaluateWriteHelper(
      Write.Bound<T> transform, DirectPipelineRunner.EvaluationContext context) {
    List<T> elems = context.getPCollection(context.getInput(transform));
    int numShards = transform.numShards;
    if (numShards < 1) {
      // System gets to choose. For direct mode, choose 1.
      numShards = 1;
    }
    TextSink<WindowedValue<T>> writer = TextSink.createForDirectPipelineRunner(
        transform.filenamePrefix, transform.getShardNameTemplate(), transform.filenameSuffix,
        numShards, true, null, null, transform.coder);
    try (Sink.SinkWriter<WindowedValue<T>> sink = writer.writer()) {
      for (T elem : elems) {
        sink.add(WindowedValue.valueInGlobalWindow(elem));
      }
    } catch (IOException exn) {
      throw new RuntimeException(
          "unable to write to output file \"" + transform.filenamePrefix + "\"", exn);
    }
  }
}
