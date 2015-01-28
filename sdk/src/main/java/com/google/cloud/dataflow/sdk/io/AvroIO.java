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

import static com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.ValueWithMetadata;

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.worker.AvroReader;
import com.google.cloud.dataflow.sdk.runners.worker.AvroSink;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.util.ReaderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Transforms for reading and writing Avro files.
 *
 * <p> To read a {@link PCollection} from one or more Avro files, use
 * {@link AvroIO.Read}, specifying {@link AvroIO.Read#from} to specify
 * the path of the file(s) to read from (e.g., a local filename or
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or filename pattern of the form
 * {@code "gs://<bucket>/<filepath>"}), and optionally
 * {@link AvroIO.Read#named} to specify the name of the pipeline step.
 *
 * <p> It is required to specify {@link AvroIO.Read#withSchema}. To
 * read specific records, such as Avro-generated classes, provide an
 * Avro-generated class type. To read GenericRecords, provide either
 * an org.apache.avro.Schema or a schema in a JSON-encoded string form.
 * An exception will be thrown if a record doesn't match the specified
 * schema.
 *
 * <p>For example:
 * <pre> {@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<AvroAutoGenClass> records =
 *     p.apply(AvroIO.Read.from("/path/to/file.avro")
 *                        .withSchema(AvroAutoGenClass.class));
 *
 * // A Read from a GCS file (runs locally and via the Google Cloud
 * // Dataflow service):
 * Schema schema = new Schema.Parser().parse(new File(
 *     "gs://my_bucket/path/to/schema.avsc"));
 * PCollection<GenericRecord> records =
 *     p.apply(AvroIO.Read.named("ReadFromAvro")
 *                        .from("gs://my_bucket/path/to/records-*.avro")
 *                        .withSchema(schema));
 * } </pre>
 *
 * <p> To write a {@link PCollection} to one or more Avro files, use
 * {@link AvroIO.Write}, specifying {@link AvroIO.Write#to} to specify
 * the path of the file to write to (e.g., a local filename or sharded
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or sharded filename pattern of the form
 * {@code "gs://<bucket>/<filepath>"}), and optionally
 * {@link AvroIO.Write#named} to specify the name of the pipeline step.
 *
 * <p> It is required to specify {@link AvroIO.Write#withSchema}. To
 * write specific records, such as Avro-generated classes, provide an
 * Avro-generated class type. To write GenericRecords, provide either
 * an org.apache.avro.Schema or a schema in a JSON-encoded string form.
 * An exception will be thrown if a record doesn't match the specified
 * schema.
 *
 * <p>For example:
 * <pre> {@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<AvroAutoGenClass> records = ...;
 * records.apply(AvroIO.Write.to("/path/to/file.avro")
 *                           .withSchema(AvroAutoGenClass.class));
 *
 * // A Write to a sharded GCS file (runs locally and via the Google Cloud
 * // Dataflow service):
 * Schema schema = new Schema.Parser().parse(new File(
 *     "gs://my_bucket/path/to/schema.avsc"));
 * PCollection<GenericRecord> records = ...;
 * records.apply(AvroIO.Write.named("WriteToAvro")
 *                           .to("gs://my_bucket/path/to/numbers")
 *                           .withSchema(schema)
 *                           .withSuffix(".avro"));
 * } </pre>
 */
public class AvroIO {
  /**
   * A root PTransform that reads from an Avro file (or multiple Avro
   * files matching a pattern) and returns a PCollection containing
   * the decoding of each record.
   */
  public static class Read {
    /**
     * Returns an AvroIO.Read PTransform with the given step name.
     */
    public static Bound<GenericRecord> named(String name) {
      return new Bound<>(GenericRecord.class).named(name);
    }

    /**
     * Returns an AvroIO.Read PTransform that reads from the file(s)
     * with the given name or pattern.  This can be a local filename
     * or filename pattern (if running locally), or a Google Cloud
     * Storage filename or filename pattern of the form
     * {@code "gs://<bucket>/<filepath>"}) (if running locally or via
     * the Google Cloud Dataflow service).  Standard
     * <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html"
     * >Java Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public static Bound<GenericRecord> from(String filepattern) {
      return new Bound<>(GenericRecord.class).from(filepattern);
    }

    /**
     * Returns an AvroIO.Read PTransform that reads Avro file(s)
     * containing records whose type is the specified Avro-generated class.
     *
     * @param <T> the type of the decoded elements, and the elements
     * of the resulting PCollection
     */
    public static <T> Bound<T> withSchema(Class<T> type) {
      return new Bound<>(type).withSchema(type);
    }

    /**
     * Returns an AvroIO.Read PTransform that reads Avro file(s)
     * containing records of the specified schema.
     */
    public static Bound<GenericRecord> withSchema(Schema schema) {
      return new Bound<>(GenericRecord.class).withSchema(schema);
    }

    /**
     * Returns an AvroIO.Read PTransform that reads Avro file(s)
     * containing records of the specified schema in a JSON-encoded
     * string form.
     */
    public static Bound<GenericRecord> withSchema(String schema) {
      return withSchema((new Schema.Parser()).parse(schema));
    }

    /**
     * A PTransform that reads from an Avro file (or multiple Avro
     * files matching a pattern) and returns a bounded PCollection containing
     * the decoding of each record.
     *
     * @param <T> the type of each of the elements of the resulting
     * PCollection
     */
    public static class Bound<T> extends PTransform<PInput, PCollection<T>> {
      private static final long serialVersionUID = 0;

      /** The filepattern to read from. */
      @Nullable
      final String filepattern;
      /** The class type of the records. */
      final Class<T> type;
      /** The schema of the input file. */
      @Nullable
      final Schema schema;

      Bound(Class<T> type) {
        this(null, null, type, null);
      }

      Bound(String name, String filepattern, Class<T> type, Schema schema) {
        super(name);
        this.filepattern = filepattern;
        this.type = type;
        this.schema = schema;
      }

      /**
       * Returns a new AvroIO.Read PTransform that's like this one but
       * with the given step name.  Does not modify this object.
       */
      public Bound<T> named(String name) {
        return new Bound<>(name, filepattern, type, schema);
      }

      /**
       * Returns a new AvroIO.Read PTransform that's like this one but
       * that reads from the file(s) with the given name or pattern.
       * (See {@link AvroIO.Read#from} for a description of
       * filepatterns.)  Does not modify this object.
       */
      public Bound<T> from(String filepattern) {
        return new Bound<>(name, filepattern, type, schema);
      }

      /**
       * Returns a new AvroIO.Read PTransform that's like this one but
       * that reads Avro file(s) containing records whose type is the
       * specified Avro-generated class.  Does not modify this object.
       *
       * @param <T1> the type of the decoded elements, and the elements of
       * the resulting PCollection
       */
      public <T1> Bound<T1> withSchema(Class<T1> type) {
        return new Bound<>(name, filepattern, type, ReflectData.get().getSchema(type));
      }

      /**
       * Returns a new AvroIO.Read PTransform that's like this one but
       * that reads Avro file(s) containing records of the specified schema.
       * Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(Schema schema) {
        return new Bound<>(name, filepattern, GenericRecord.class, schema);
      }

      /**
       * Returns a new AvroIO.Read PTransform that's like this one but
       * that reads Avro file(s) containing records of the specified schema
       * in a JSON-encoded string form.  Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(String schema) {
        return withSchema((new Schema.Parser()).parse(schema));
      }

      @Override
      public PCollection<T> apply(PInput input) {
        if (filepattern == null) {
          throw new IllegalStateException(
              "need to set the filepattern of an AvroIO.Read transform");
        }
        if (schema == null) {
          throw new IllegalStateException("need to set the schema of an AvroIO.Read transform");
        }

        // Force the output's Coder to be what the read is using, and
        // unchangeable later, to ensure that we read the input in the
        // format specified by the Read transform.
        return PCollection.<T>createPrimitiveOutputInternal(new GlobalWindows())
            .setCoder(getDefaultOutputCoder());
      }

      @Override
      protected Coder<T> getDefaultOutputCoder() {
        return AvroCoder.of(type, schema);
      }

      @Override
      protected String getKindString() {
        return "AvroIO.Read";
      }

      public String getFilepattern() {
        return filepattern;
      }

      public Schema getSchema() {
        return schema;
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
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A root PTransform that writes a PCollection to an Avro file (or
   * multiple Avro files matching a sharding pattern).
   */
  public static class Write {
    /**
     * Returns an AvroIO.Write PTransform with the given step name.
     */
    public static Bound<GenericRecord> named(String name) {
      return new Bound<>(GenericRecord.class).named(name);
    }

    /**
     * Returns an AvroIO.Write PTransform that writes to the file(s)
     * with the given prefix.  This can be a local filename
     * (if running locally), or a Google Cloud Storage filename of
     * the form {@code "gs://<bucket>/<filepath>"})
     * (if running locally or via the Google Cloud Dataflow service).
     *
     * <p> The files written will begin with this prefix, followed by
     * a shard identifier (see {@link Bound#withNumShards}, and end
     * in a common extension, if given by {@link Bound#withSuffix}.
     */
    public static Bound<GenericRecord> to(String prefix) {
      return new Bound<>(GenericRecord.class).to(prefix);
    }

    /**
     * Returns an AvroIO.Write PTransform that writes to the file(s) with the
     * given filename suffix.
     */
    public static Bound<GenericRecord> withSuffix(String filenameSuffix) {
      return new Bound<>(GenericRecord.class).withSuffix(filenameSuffix);
    }

    /**
     * Returns an AvroIO.Write PTransform that uses the provided shard count.
     *
     * <p> Constraining the number of shards is likely to reduce
     * the performance of a pipeline. Setting this value is not recommended
     * unless you require a specific number of output files.
     *
     * @param numShards the number of shards to use, or 0 to let the system
     *                  decide.
     */
    public static Bound<GenericRecord> withNumShards(int numShards) {
      return new Bound<>(GenericRecord.class).withNumShards(numShards);
    }

    /**
     * Returns an AvroIO.Write PTransform that uses the given shard name
     * template.
     *
     * See {@link ShardNameTemplate} for a description of shard templates.
     */
    public static Bound<GenericRecord> withShardNameTemplate(String shardTemplate) {
      return new Bound<>(GenericRecord.class).withShardNameTemplate(shardTemplate);
    }

    /**
     * Returns an AvroIO.Write PTransform that forces a single file as
     * output.
     *
     * <p> Constraining the number of shards is likely to reduce
     * the performance of a pipeline.  Setting this value is not recommended
     * unless you require a specific number of output files.
     */
    public static Bound<GenericRecord> withoutSharding() {
      return new Bound<>(GenericRecord.class).withoutSharding();
    }

    /**
     * Returns an AvroIO.Write PTransform that writes Avro file(s)
     * containing records whose type is the specified Avro-generated class.
     *
     * @param <T> the type of the elements of the input PCollection
     */
    public static <T> Bound<T> withSchema(Class<T> type) {
      return new Bound<>(type).withSchema(type);
    }

    /**
     * Returns an AvroIO.Write PTransform that writes Avro file(s)
     * containing records of the specified schema.
     */
    public static Bound<GenericRecord> withSchema(Schema schema) {
      return new Bound<>(GenericRecord.class).withSchema(schema);
    }

    /**
     * Returns an AvroIO.Write PTransform that writes Avro file(s)
     * containing records of the specified schema in a JSON-encoded
     * string form.
     */
    public static Bound<GenericRecord> withSchema(String schema) {
      return withSchema((new Schema.Parser()).parse(schema));
    }

    /**
     * A PTransform that writes a bounded PCollection to an Avro file (or
     * multiple Avro files matching a sharding pattern).
     *
     * @param <T> the type of each of the elements of the input PCollection
     */
    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {
      private static final long serialVersionUID = 0;

      /** The filename to write to. */
      @Nullable
      final String filenamePrefix;
      /** Suffix to use for each filename. */
      final String filenameSuffix;
      /** Requested number of shards.  0 for automatic. */
      final int numShards;
      /** Shard template string. */
      final String shardTemplate;
      /** The class type of the records. */
      final Class<T> type;
      /** The schema of the output file. */
      @Nullable
      final Schema schema;

      Bound(Class<T> type) {
        this(null, null, "", 0, ShardNameTemplate.INDEX_OF_MAX, type, null);
      }

      Bound(String name, String filenamePrefix, String filenameSuffix, int numShards,
          String shardTemplate, Class<T> type, Schema schema) {
        super(name);
        this.filenamePrefix = filenamePrefix;
        this.filenameSuffix = filenameSuffix;
        this.numShards = numShards;
        this.shardTemplate = shardTemplate;
        this.type = type;
        this.schema = schema;
      }

      /**
       * Returns a new AvroIO.Write PTransform that's like this one but
       * with the given step name.  Does not modify this object.
       */
      public Bound<T> named(String name) {
        return new Bound<>(
            name, filenamePrefix, filenameSuffix, numShards, shardTemplate, type, schema);
      }

      /**
       * Returns a new AvroIO.Write PTransform that's like this one but
       * that writes to the file(s) with the given filename prefix.
       *
       * <p> See {@link Write#to(String) Write.to(String)} for more information.
       *
       * <p> Does not modify this object.
       */
      public Bound<T> to(String filenamePrefix) {
        validateOutputComponent(filenamePrefix);
        return new Bound<>(
            name, filenamePrefix, filenameSuffix, numShards, shardTemplate, type, schema);
      }

      /**
       * Returns a new AvroIO.Write PTransform that's like this one but
       * that writes to the file(s) with the given filename suffix.
       *
       * <p> Does not modify this object.
       *
       * @see ShardNameTemplate
       */
      public Bound<T> withSuffix(String filenameSuffix) {
        validateOutputComponent(filenameSuffix);
        return new Bound<>(
            name, filenamePrefix, filenameSuffix, numShards, shardTemplate, type, schema);
      }

      /**
       * Returns a new AvroIO.Write PTransform that's like this one but
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
        return new Bound<>(
            name, filenamePrefix, filenameSuffix, numShards, shardTemplate, type, schema);
      }

      /**
       * Returns a new AvroIO.Write PTransform that's like this one but
       * that uses the given shard name template.
       *
       * <p> Does not modify this object.
       *
       * @see ShardNameTemplate
       */
      public Bound<T> withShardNameTemplate(String shardTemplate) {
        return new Bound<>(
            name, filenamePrefix, filenameSuffix, numShards, shardTemplate, type, schema);
      }

      /**
       * Returns a new AvroIO.Write PTransform that's like this one but
       * that forces a single file as output.
       *
       * <p> This is a shortcut for
       * {@code .withNumShards(1).withShardNameTemplate("")}
       *
       * <p> Does not modify this object.
       */
      public Bound<T> withoutSharding() {
        return new Bound<>(name, filenamePrefix, filenameSuffix, 1, "", type, schema);
      }

      /**
       * Returns a new AvroIO.Write PTransform that's like this one but
       * that writes to Avro file(s) containing records whose type is the
       * specified Avro-generated class.  Does not modify this object.
       *
       * @param <T1> the type of the elements of the input PCollection
       */
      public <T1> Bound<T1> withSchema(Class<T1> type) {
        return new Bound<>(name, filenamePrefix, filenameSuffix, numShards, shardTemplate, type,
            ReflectData.get().getSchema(type));
      }

      /**
       * Returns a new AvroIO.Write PTransform that's like this one but
       * that writes to Avro file(s) containing records of the specified
       * schema.  Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(Schema schema) {
        return new Bound<>(name, filenamePrefix, filenameSuffix, numShards, shardTemplate,
            GenericRecord.class, schema);
      }

      /**
       * Returns a new AvroIO.Write PTransform that's like this one but
       * that writes to Avro file(s) containing records of the specified
       * schema in a JSON-encoded string form.  Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(String schema) {
        return withSchema((new Schema.Parser()).parse(schema));
      }

      @Override
      public PDone apply(PCollection<T> input) {
        if (filenamePrefix == null) {
          throw new IllegalStateException(
              "need to set the filename prefix of an AvroIO.Write transform");
        }
        if (schema == null) {
          throw new IllegalStateException("need to set the schema of an AvroIO.Write transform");
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
      protected String getKindString() {
        return "AvroIO.Write";
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

      public Class<T> getType() {
        return type;
      }

      public Schema getSchema() {
        return schema;
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

  // Pattern which matches old-style shard output patterns, which are now
  // disallowed.
  private static final Pattern SHARD_OUTPUT_PATTERN = Pattern.compile("@([0-9]+|\\*)");

  private static void validateOutputComponent(String partialFilePattern) {
    Preconditions.checkArgument(
        !SHARD_OUTPUT_PATTERN.matcher(partialFilePattern).find(),
        "Output name components are not allowed to contain @* or @N patterns: "
        + partialFilePattern);
  }

  /////////////////////////////////////////////////////////////////////////////

  private static <T> void evaluateReadHelper(
      Read.Bound<T> transform, DirectPipelineRunner.EvaluationContext context) {
    AvroReader<T> reader = new AvroReader<>(transform.filepattern, null, null,
        WindowedValue.getValueOnlyCoder(transform.getDefaultOutputCoder()));
    List<WindowedValue<T>> elems = ReaderUtils.readElemsFromReader(reader);
    List<ValueWithMetadata<T>> output = new ArrayList<>();
    for (WindowedValue<T> elem : elems) {
      output.add(ValueWithMetadata.of(elem));
    }
    context.setPCollectionValuesWithMetadata(transform.getOutput(), output);
  }

  private static <T> void evaluateWriteHelper(
      Write.Bound<T> transform, DirectPipelineRunner.EvaluationContext context) {
    List<WindowedValue<T>> elems = context.getPCollectionWindowedValues(transform.getInput());
    int numShards = transform.numShards;
    if (numShards < 1) {
      // System gets to choose.  For direct mode, choose 1.
      numShards = 1;
    }
    AvroSink<T> writer = new AvroSink<>(transform.filenamePrefix, transform.shardTemplate,
        transform.filenameSuffix, numShards,
        WindowedValue.getValueOnlyCoder(AvroCoder.of(transform.type, transform.schema)));
    try (Sink.SinkWriter<WindowedValue<T>> sink = writer.writer()) {
      for (WindowedValue<T> elem : elems) {
        sink.add(elem);
      }
    } catch (IOException exn) {
      throw new RuntimeException(
          "unable to write to output file \"" + transform.filenamePrefix + "\"", exn);
    }
  }
}
