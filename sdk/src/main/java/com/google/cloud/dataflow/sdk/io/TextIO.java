/*
 * Copyright (C) 2014 Google Inc.
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

import static com.google.cloud.dataflow.sdk.util.CloudSourceUtils.readElemsFromSource;

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.worker.TextSink;
import com.google.cloud.dataflow.sdk.runners.worker.TextSource;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Transforms for reading and writing text files.
 *
 * <p> To read a {@link PCollection} from one or more text files, use
 * {@link TextIO.Read}, specifying {@link TextIO.Read#from} to specify
 * the path of the file(s) to read from (e.g., a local filename or
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or filename pattern of the form
 * {@code "gs://<bucket>/<filepath>"}), and optionally
 * {@link TextIO.Read#named} to specify the name of the pipeline step
 * and/or {@link TextIO.Read#withCoder} to specify the Coder to use to
 * decode the text lines into Java values.  For example:
 *
 * <pre> {@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<String> lines =
 *     p.apply(TextIO.Read.from("/path/to/file.txt"));
 *
 * // A fully-specified Read from a GCS file (runs locally and via the
 * // Google Cloud Dataflow service):
 * PCollection<Integer> numbers =
 *     p.apply(TextIO.Read.named("ReadNumbers")
 *                        .from("gs://my_bucket/path/to/numbers-*.txt")
 *                        .withCoder(TextualIntegerCoder.of()));
 * } </pre>
 *
 * <p> To write a {@link PCollection} to one or more text files, use
 * {@link TextIO.Write}, specifying {@link TextIO.Write#to} to specify
 * the path of the file to write to (e.g., a local filename or sharded
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or sharded filename pattern of the form
 * {@code "gs://<bucket>/<filepath>"}), and optionally
 * {@link TextIO.Write#named} to specify the name of the pipeline step
 * and/or {@link TextIO.Write#withCoder} to specify the Coder to use
 * to encode the Java values into text lines.  For example:
 *
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
 */
public class TextIO {
  public static final Coder<String> DEFAULT_TEXT_CODER = StringUtf8Coder.of();

  /**
   * A root PTransform that reads from a text file (or multiple text
   * files matching a pattern) and returns a PCollection containing
   * the decoding of each of the lines of the text file(s).  The
   * default decoding just returns the lines.
   */
  public static class Read {
    /**
     * Returns a TextIO.Read PTransform with the given step name.
     */
    public static Bound<String> named(String name) {
      return new Bound<>(DEFAULT_TEXT_CODER).named(name);
    }

    /**
     * Returns a TextIO.Read PTransform that reads from the file(s)
     * with the given name or pattern.  This can be a local filename
     * or filename pattern (if running locally), or a Google Cloud
     * Storage filename or filename pattern of the form
     * {@code "gs://<bucket>/<filepath>"}) (if running locally or via
     * the Google Cloud Dataflow service).  Standard
     * <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html"
     * >Java Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public static Bound<String> from(String filepattern) {
      return new Bound<>(DEFAULT_TEXT_CODER).from(filepattern);
    }

    /**
     * Returns a TextIO.Read PTransform that uses the given
     * {@code Coder<T>} to decode each of the lines of the file into a
     * value of type {@code T}.
     *
     * <p> By default, uses {@link StringUtf8Coder}, which just
     * returns the text lines as Java strings.
     *
     * @param <T> the type of the decoded elements, and the elements
     * of the resulting PCollection
     */
    public static <T> Bound<T> withCoder(Coder<T> coder) {
      return new Bound<>(coder);
    }

    // TODO: strippingNewlines, gzipped, etc.

    /**
     * A root PTransform that reads from a text file (or multiple text files
     * matching a pattern) and returns a bounded PCollection containing the
     * decoding of each of the lines of the text file(s).  The default
     * decoding just returns the lines.
     *
     * @param <T> the type of each of the elements of the resulting
     * PCollection, decoded from the lines of the text file
     */
    public static class Bound<T>
        extends PTransform<PInput, PCollection<T>> {
      /** The filepattern to read from. */
      @Nullable final String filepattern;

      /** The Coder to use to decode each line. */
      @Nullable final Coder<T> coder;

      Bound(Coder<T> coder) {
        this(null, null, coder);
      }

      Bound(String name, String filepattern, Coder<T> coder) {
        super(name);
        this.coder = coder;
        this.filepattern = filepattern;
      }

      /**
       * Returns a new TextIO.Read PTransform that's like this one but
       * with the given step name.  Does not modify this object.
       */
      public Bound<T> named(String name) {
        return new Bound<>(name, filepattern, coder);
      }

      /**
       * Returns a new TextIO.Read PTransform that's like this one but
       * that reads from the file(s) with the given name or pattern.
       * (See {@link TextIO.Read#from} for a description of
       * filepatterns.)  Does not modify this object.
       */
      public Bound<T> from(String filepattern) {
        return new Bound<>(name, filepattern, coder);
      }

      /**
       * Returns a new TextIO.Read PTransform that's like this one but
       * that uses the given {@code Coder<T1>} to decode each of the
       * lines of the file into a value of type {@code T1}.  Does not
       * modify this object.
       *
       * @param <T1> the type of the decoded elements, and the
       * elements of the resulting PCollection
       */
      public <T1> Bound<T1> withCoder(Coder<T1> coder) {
        return new Bound<>(name, filepattern, coder);
      }

      @Override
      public PCollection<T> apply(PInput input) {
        if (filepattern == null) {
          throw new IllegalStateException(
              "need to set the filepattern of a TextIO.Read transform");
        }
        // Force the output's Coder to be what the read is using, and
        // unchangeable later, to ensure that we read the input in the
        // format specified by the Read transform.
        return PCollection.<T>createPrimitiveOutputInternal(new GlobalWindow())
            .setCoder(coder);
      }

      @Override
      protected Coder<T> getDefaultOutputCoder() {
        return coder;
      }

      @Override
      protected String getKindString() { return "TextIO.Read"; }

      public String getFilepattern() {
        return filepattern;
      }

      static {
        DirectPipelineRunner.registerDefaultTransformEvaluator(
            Bound.class,
            new DirectPipelineRunner.TransformEvaluator<Bound>() {
              @Override
              public void evaluate(
                  Bound transform,
                  DirectPipelineRunner.EvaluationContext context) {
                evaluateReadHelper(transform, context);
              }
            });
      }
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * A PTransform that writes a PCollection to a text file (or
   * multiple text files matching a sharding pattern), with each
   * PCollection element being encoded into its own line.
   */
  public static class Write {
    /**
     * Returns a TextIO.Write PTransform with the given step name.
     */
    public static Bound<String> named(String name) {
      return new Bound<>(DEFAULT_TEXT_CODER).named(name);
    }

    /**
     * Returns a TextIO.Write PTransform that writes to the file(s)
     * with the given prefix.  This can be a local filename
     * (if running locally), or a Google Cloud Storage filename of
     * the form {@code "gs://<bucket>/<filepath>"})
     * (if running locally or via the Google Cloud Dataflow service).
     *
     * <p> The files written will begin with this prefix, followed by
     * a shard identifier (see {@link Bound#withNumShards}, and end
     * in a common extension, if given by {@link Bound#withSuffix}.
     */
    public static Bound<String> to(String prefix) {
      return new Bound<>(DEFAULT_TEXT_CODER).to(prefix);
    }

    /**
     * Returns a TextIO.Write PTransform that writes to the file(s) with the
     * given filename suffix.
     */
    public static Bound<String> withSuffix(String nameExtension) {
      return new Bound<>(DEFAULT_TEXT_CODER).withSuffix(nameExtension);
    }

    /**
     * Returns a TextIO.Write PTransform that uses the provided shard count.
     *
     * <p> Constraining the number of shards is likely to reduce
     * the performance of a pipeline.  Setting this value is not recommended
     * unless you require a specific number of output files.
     *
     * @param numShards the number of shards to use, or 0 to let the system
     *                  decide.
     */
    public static Bound<String> withNumShards(int numShards) {
      return new Bound<>(DEFAULT_TEXT_CODER).withNumShards(numShards);
    }

    /**
     * Returns a TextIO.Write PTransform that uses the given shard name
     * template.
     *
     * <p> See {@link ShardNameTemplate} for a description of shard templates.
     */
    public static Bound<String> withShardNameTemplate(String shardTemplate) {
      return new Bound<>(DEFAULT_TEXT_CODER)
          .withShardNameTemplate(shardTemplate);
    }

    /**
     * Returns a TextIO.Write PTransform that forces a single file as
     * output.
     */
    public static Bound<String> withoutSharding() {
      return new Bound<>(DEFAULT_TEXT_CODER).withoutSharding();
    }

    /**
     * Returns a TextIO.Write PTransform that uses the given
     * {@code Coder<T>} to encode each of the elements of the input
     * {@code PCollection<T>} into an output text line.
     *
     * <p> By default, uses {@link StringUtf8Coder}, which writes input
     * Java strings directly as output lines.
     *
     * @param <T> the type of the elements of the input PCollection
     */
    public static <T> Bound<T> withCoder(Coder<T> coder) {
      return new Bound<>(coder);
    }

    // TODO: appendingNewlines, gzipped, header, footer, etc.

    /**
     * A PTransform that writes a bounded PCollection to a text file (or
     * multiple text files matching a sharding pattern), with each
     * PCollection element being encoded into its own line.
     *
     * @param <T> the type of the elements of the input PCollection
     */
    public static class Bound<T>
        extends PTransform<PCollection<T>, PDone> {
      /** The filename to write to. */
      @Nullable final String filenamePrefix;
      /** Suffix to use for each filename. */
      final String filenameSuffix;

      /** The Coder to use to decode each line. */
      final Coder<T> coder;

      /** Requested number of shards.  0 for automatic. */
      final int numShards;

      /** Shard template string. */
      final String shardTemplate;

      Bound(Coder<T> coder) {
        this(null, null, "", coder, 0, ShardNameTemplate.INDEX_OF_MAX);
      }

      Bound(String name, String filenamePrefix, String filenameSuffix,
          Coder<T> coder, int numShards,
          String shardTemplate) {
        super(name);
        this.coder = coder;
        this.filenamePrefix = filenamePrefix;
        this.filenameSuffix = filenameSuffix;
        this.numShards = numShards;
        this.shardTemplate = shardTemplate;
      }

      /**
       * Returns a new TextIO.Write PTransform that's like this one but
       * with the given step name.  Does not modify this object.
       */
      public Bound<T> named(String name) {
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards,
            shardTemplate);
      }

      /**
       * Returns a new TextIO.Write PTransform that's like this one but
       * that writes to the file(s) with the given filename prefix.
       *
       * <p> See {@link Write#to(String) Write.to(String)} for more information.
       *
       * <p> Does not modify this object.
       */
      public Bound<T> to(String filenamePrefix) {
        validateOutputComponent(filenamePrefix);
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards,
            shardTemplate);
      }

      /**
       * Returns a new TextIO.Write PTransform that's like this one but
       * that writes to the file(s) with the given filename suffix.
       *
       * <p> Does not modify this object.
       *
       * @see ShardNameTemplate
       */
      public Bound<T> withSuffix(String nameExtension) {
        validateOutputComponent(nameExtension);
        return new Bound<>(name, filenamePrefix, nameExtension, coder, numShards,
            shardTemplate);
      }

      /**
       * Returns a new TextIO.Write PTransform that's like this one but
       * that uses the provided shard count.
       *
       * <p> Constraining the number of shards is likely to reduce
       * the performance of a pipeline.  Setting this value is not recommended
       * unless you require a specific number of output files.
       *
       * <p> Does not modify this object.
       *
       * @param numShards the number of shards to use, or 0 to let the system
       *                  decide.
       * @see ShardNameTemplate
       */
      public Bound<T> withNumShards(int numShards) {
        Preconditions.checkArgument(numShards >= 0);
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards,
            shardTemplate);
      }

      /**
       * Returns a new TextIO.Write PTransform that's like this one but
       * that uses the given shard name template.
       *
       * <p> Does not modify this object.
       *
       * @see ShardNameTemplate
       */
      public Bound<T> withShardNameTemplate(String shardTemplate) {
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards,
            shardTemplate);
      }

      /**
       * Returns a new TextIO.Write PTransform that's like this one but
       * that forces a single file as output.
       *
       * <p> This is a shortcut for
       * {@code .withNumShards(1).withShardNameTemplate("")}
       *
       * <p> Does not modify this object.
       */
      public Bound<T> withoutSharding() {
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, 1, "");
      }

      /**
       * Returns a new TextIO.Write PTransform that's like this one
       * but that uses the given {@code Coder<T1>} to encode each of
       * the elements of the input {@code PCollection<T1>} into an
       * output text line.  Does not modify this object.
       *
       * @param <T1> the type of the elements of the input PCollection
       */
      public <T1> Bound<T1> withCoder(Coder<T1> coder) {
        return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards,
            shardTemplate);
      }

      @Override
      public PDone apply(PCollection<T> input) {
        if (filenamePrefix == null) {
          throw new IllegalStateException(
              "need to set the filename prefix of a TextIO.Write transform");
        }
        return new PDone();
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

      @Override
      protected String getKindString() { return "TextIO.Write"; }

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

      static {
        DirectPipelineRunner.registerDefaultTransformEvaluator(
            Bound.class,
            new DirectPipelineRunner.TransformEvaluator<Bound>() {
              @Override
              public void evaluate(
                  Bound transform,
                  DirectPipelineRunner.EvaluationContext context) {
                evaluateWriteHelper(transform, context);
              }
            });
      }
    }
  }

  // Pattern which matches old-style shard output patterns, which are now
  // disallowed.
  private static final Pattern SHARD_OUTPUT_PATTERN =
      Pattern.compile("@([0-9]+|\\*)");

  private static void validateOutputComponent(String partialFilePattern) {
    Preconditions.checkArgument(
        !SHARD_OUTPUT_PATTERN.matcher(partialFilePattern).find(),
        "Output name components are not allowed to contain @* or @N patterns: "
            + partialFilePattern);
  }

  //////////////////////////////////////////////////////////////////////////////

  private static <T> void evaluateReadHelper(
      Read.Bound<T> transform,
      DirectPipelineRunner.EvaluationContext context) {
    TextSource<T> source = new TextSource<>(
        transform.filepattern, true, null, null, transform.coder);
    List<T> elems = readElemsFromSource(source);
    context.setPCollection(transform.getOutput(), elems);
  }

  private static <T> void evaluateWriteHelper(
      Write.Bound<T> transform,
      DirectPipelineRunner.EvaluationContext context) {
    List<T> elems = context.getPCollection(transform.getInput());
    int numShards = transform.numShards;
    if (numShards < 1) {
      // System gets to choose.  For direct mode, choose 1.
      numShards = 1;
    }
    TextSink<WindowedValue<T>> writer = TextSink.createForDirectPipelineRunner(
        transform.filenamePrefix, transform.getShardNameTemplate(),
        transform.filenameSuffix, numShards,
        true, null, null, transform.coder);
    try (Sink.SinkWriter<WindowedValue<T>> sink = writer.writer()) {
      for (T elem : elems) {
        sink.add(WindowedValue.valueInGlobalWindow(elem));
      }
    } catch (IOException exn) {
      throw new RuntimeException(
          "unable to write to output file \"" + transform.filenamePrefix + "\"",
          exn);
    }
  }
}
