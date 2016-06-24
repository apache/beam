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

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * {@link PTransform}s for reading and writing Avro files.
 *
 * <p>To read a {@link PCollection} from one or more Avro files, use
 * {@link AvroIO.Read}, specifying {@link AvroIO.Read#from} to specify
 * the path of the file(s) to read from (e.g., a local filename or
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or filename pattern of the form {@code "gs://<bucket>/<filepath>"}).
 *
 * <p>It is required to specify {@link AvroIO.Read#withSchema}. To
 * read specific records, such as Avro-generated classes, provide an
 * Avro-generated class type. To read {@link GenericRecord GenericRecords}, provide either
 * a {@link Schema} object or an Avro schema in a JSON-encoded string form.
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
 *                 .withSchema(AvroAutoGenClass.class));
 *
 * // A Read from a GCS file (runs locally and via the Google Cloud
 * // Dataflow service):
 * Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
 * PCollection<GenericRecord> records =
 *     p.apply(AvroIO.Read
 *                .from("gs://my_bucket/path/to/records-*.avro")
 *                .withSchema(schema));
 * } </pre>
 *
 * <p>To write a {@link PCollection} to one or more Avro files, use
 * {@link AvroIO.Write}, specifying {@link AvroIO.Write#to} to specify
 * the path of the file to write to (e.g., a local filename or sharded
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or sharded filename pattern of the form
 * {@code "gs://<bucket>/<filepath>"}), and optionally
 * {@link AvroIO.Write#named} to specify the name of the pipeline step.
 *
 * <p>It is required to specify {@link AvroIO.Write#withSchema}. To
 * write specific records, such as Avro-generated classes, provide an
 * Avro-generated class type. To write {@link GenericRecord GenericRecords}, provide either
 * a {@link Schema} object or a schema in a JSON-encoded string form.
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
 * Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
 * PCollection<GenericRecord> records = ...;
 * records.apply("WriteToAvro", AvroIO.Write
 *     .to("gs://my_bucket/path/to/numbers")
 *     .withSchema(schema)
 *     .withSuffix(".avro"));
 * } </pre>
 *
 * <p><h3>Permissions</h3>
 * Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * Dataflow job. Please refer to the documentation of corresponding {@link PipelineRunner}s for
 * more details.
 */
public class AvroIO {
  /**
   * A root {@link PTransform} that reads from an Avro file (or multiple Avro
   * files matching a pattern) and returns a {@link PCollection} containing
   * the decoding of each record.
   */
  public static class Read {

    /**
     * Returns a {@link PTransform} that reads from the file(s)
     * with the given name or pattern. This can be a local filename
     * or filename pattern (if running locally), or a Google Cloud
     * Storage filename or filename pattern of the form
     * {@code "gs://<bucket>/<filepath>"} (if running locally or via
     * the Google Cloud Dataflow service). Standard
     * <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html">Java
     * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public static Bound<GenericRecord> from(String filepattern) {
      return new Bound<>(GenericRecord.class).from(filepattern);
    }

    /**
     * Returns a {@link PTransform} that reads Avro file(s)
     * containing records whose type is the specified Avro-generated class.
     *
     * @param <T> the type of the decoded elements, and the elements
     * of the resulting {@link PCollection}
     */
    public static <T> Bound<T> withSchema(Class<T> type) {
      return new Bound<>(type).withSchema(type);
    }

    /**
     * Returns a {@link PTransform} that reads Avro file(s)
     * containing records of the specified schema.
     */
    public static Bound<GenericRecord> withSchema(Schema schema) {
      return new Bound<>(GenericRecord.class).withSchema(schema);
    }

    /**
     * Returns a {@link PTransform} that reads Avro file(s)
     * containing records of the specified schema in a JSON-encoded
     * string form.
     */
    public static Bound<GenericRecord> withSchema(String schema) {
      return withSchema((new Schema.Parser()).parse(schema));
    }

    /**
     * Returns a {@link PTransform} that reads Avro file(s)
     * that has GCS path validation on pipeline creation disabled.
     *
     * <p>This can be useful in the case where the GCS input location does
     * not exist at the pipeline creation time, but is expected to be available
     * at execution time.
     */
    public static Bound<GenericRecord> withoutValidation() {
      return new Bound<>(GenericRecord.class).withoutValidation();
    }

    /**
     * A {@link PTransform} that reads from an Avro file (or multiple Avro
     * files matching a pattern) and returns a bounded {@link PCollection} containing
     * the decoding of each record.
     *
     * @param <T> the type of each of the elements of the resulting
     * PCollection
     */
    public static class Bound<T> extends PTransform<PInput, PCollection<T>> {
      /** The filepattern to read from. */
      @Nullable
      final String filepattern;
      /** The class type of the records. */
      final Class<T> type;
      /** The schema of the input file. */
      @Nullable
      final Schema schema;
      /** An option to indicate if input validation is desired. Default is true. */
      final boolean validate;

      Bound(Class<T> type) {
        this(null, null, type, null, true);
      }

      Bound(String name, String filepattern, Class<T> type, Schema schema, boolean validate) {
        super(name);
        this.filepattern = filepattern;
        this.type = type;
        this.schema = schema;
        this.validate = validate;
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that reads from the file(s) with the given name or pattern.
       * (See {@link AvroIO.Read#from} for a description of
       * filepatterns.)
       *
       * <p>Does not modify this object.
       */
      public Bound<T> from(String filepattern) {
        return new Bound<>(name, filepattern, type, schema, validate);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that reads Avro file(s) containing records whose type is the
       * specified Avro-generated class.
       *
       * <p>Does not modify this object.
       *
       * @param <X> the type of the decoded elements and the elements of
       * the resulting PCollection
       */
      public <X> Bound<X> withSchema(Class<X> type) {
        return new Bound<>(name, filepattern, type, ReflectData.get().getSchema(type), validate);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that reads Avro file(s) containing records of the specified schema.
       *
       * <p>Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(Schema schema) {
        return new Bound<>(name, filepattern, GenericRecord.class, schema, validate);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that reads Avro file(s) containing records of the specified schema
       * in a JSON-encoded string form.
       *
       * <p>Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(String schema) {
        return withSchema((new Schema.Parser()).parse(schema));
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that has GCS input path validation on pipeline creation disabled.
       *
       * <p>Does not modify this object.
       *
       * <p>This can be useful in the case where the GCS input location does
       * not exist at the pipeline creation time, but is expected to be
       * available at execution time.
       */
      public Bound<T> withoutValidation() {
        return new Bound<>(name, filepattern, type, schema, false);
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

        @SuppressWarnings("unchecked")
        Bounded<T> read =
            type == GenericRecord.class
                ? (Bounded<T>) org.apache.beam.sdk.io.Read.from(
                    AvroSource.from(filepattern).withSchema(schema))
                : org.apache.beam.sdk.io.Read.from(
                    AvroSource.from(filepattern).withSchema(type));

        PCollection<T> pcol = input.getPipeline().apply("Read", read);
        // Honor the default output coder that would have been used by this PTransform.
        pcol.setCoder(getDefaultOutputCoder());
        return pcol;
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder
          .addIfNotNull(DisplayData.item("filePattern", filepattern)
            .withLabel("Input File Pattern"))
          .addIfNotDefault(DisplayData.item("validation", validate)
            .withLabel("Validation Enabled"), true);
      }

      @Override
      protected Coder<T> getDefaultOutputCoder() {
        return AvroCoder.of(type, schema);
      }

      public String getFilepattern() {
        return filepattern;
      }

      public Schema getSchema() {
        return schema;
      }

      public boolean needsValidation() {
        return validate;
      }
    }

    /** Disallow construction of utility class. */
    private Read() {}
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A root {@link PTransform} that writes a {@link PCollection} to an Avro file (or
   * multiple Avro files matching a sharding pattern).
   */
  public static class Write {

    /**
     * Returns a {@link PTransform} that writes to the file(s)
     * with the given prefix. This can be a local filename
     * (if running locally), or a Google Cloud Storage filename of
     * the form {@code "gs://<bucket>/<filepath>"}
     * (if running locally or via the Google Cloud Dataflow service).
     *
     * <p>The files written will begin with this prefix, followed by
     * a shard identifier (see {@link Bound#withNumShards}, and end
     * in a common extension, if given by {@link Bound#withSuffix}.
     */
    public static Bound<GenericRecord> to(String prefix) {
      return new Bound<>(GenericRecord.class).to(prefix);
    }

    /**
     * Returns a {@link PTransform} that writes to the file(s) with the
     * given filename suffix.
     */
    public static Bound<GenericRecord> withSuffix(String filenameSuffix) {
      return new Bound<>(GenericRecord.class).withSuffix(filenameSuffix);
    }

    /**
     * Returns a {@link PTransform} that uses the provided shard count.
     *
     * <p>Constraining the number of shards is likely to reduce
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
     * Returns a {@link PTransform} that uses the given shard name
     * template.
     *
     * <p>See {@link ShardNameTemplate} for a description of shard templates.
     */
    public static Bound<GenericRecord> withShardNameTemplate(String shardTemplate) {
      return new Bound<>(GenericRecord.class).withShardNameTemplate(shardTemplate);
    }

    /**
     * Returns a {@link PTransform} that forces a single file as
     * output.
     *
     * <p>Constraining the number of shards is likely to reduce
     * the performance of a pipeline. Setting this value is not recommended
     * unless you require a specific number of output files.
     */
    public static Bound<GenericRecord> withoutSharding() {
      return new Bound<>(GenericRecord.class).withoutSharding();
    }

    /**
     * Returns a {@link PTransform} that writes Avro file(s)
     * containing records whose type is the specified Avro-generated class.
     *
     * @param <T> the type of the elements of the input PCollection
     */
    public static <T> Bound<T> withSchema(Class<T> type) {
      return new Bound<>(type).withSchema(type);
    }

    /**
     * Returns a {@link PTransform} that writes Avro file(s)
     * containing records of the specified schema.
     */
    public static Bound<GenericRecord> withSchema(Schema schema) {
      return new Bound<>(GenericRecord.class).withSchema(schema);
    }

    /**
     * Returns a {@link PTransform} that writes Avro file(s)
     * containing records of the specified schema in a JSON-encoded
     * string form.
     */
    public static Bound<GenericRecord> withSchema(String schema) {
      return withSchema((new Schema.Parser()).parse(schema));
    }

    /**
     * Returns a {@link PTransform} that writes Avro file(s) that has GCS path validation on
     * pipeline creation disabled.
     *
     * <p>This can be useful in the case where the GCS output location does
     * not exist at the pipeline creation time, but is expected to be available
     * at execution time.
     */
    public static Bound<GenericRecord> withoutValidation() {
      return new Bound<>(GenericRecord.class).withoutValidation();
    }

    /**
     * A {@link PTransform} that writes a bounded {@link PCollection} to an Avro file (or
     * multiple Avro files matching a sharding pattern).
     *
     * @param <T> the type of each of the elements of the input PCollection
     */
    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {
      private static final String DEFAULT_SHARD_TEMPLATE = ShardNameTemplate.INDEX_OF_MAX;

      /** The filename to write to. */
      @Nullable
      final String filenamePrefix;
      /** Suffix to use for each filename. */
      final String filenameSuffix;
      /** Requested number of shards. 0 for automatic. */
      final int numShards;
      /** Shard template string. */
      final String shardTemplate;
      /** The class type of the records. */
      final Class<T> type;
      /** The schema of the output file. */
      @Nullable
      final Schema schema;
      /** An option to indicate if output validation is desired. Default is true. */
      final boolean validate;

      Bound(Class<T> type) {
        this(null, null, "", 0, DEFAULT_SHARD_TEMPLATE, type, null, true);
      }

      Bound(
          String name,
          String filenamePrefix,
          String filenameSuffix,
          int numShards,
          String shardTemplate,
          Class<T> type,
          Schema schema,
          boolean validate) {
        super(name);
        this.filenamePrefix = filenamePrefix;
        this.filenameSuffix = filenameSuffix;
        this.numShards = numShards;
        this.shardTemplate = shardTemplate;
        this.type = type;
        this.schema = schema;
        this.validate = validate;
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that writes to the file(s) with the given filename prefix.
       *
       * <p>See {@link AvroIO.Write#to(String)} for more information
       * about filenames.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> to(String filenamePrefix) {
        validateOutputComponent(filenamePrefix);
        return new Bound<>(
            name, filenamePrefix, filenameSuffix, numShards, shardTemplate, type, schema, validate);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that writes to the file(s) with the given filename suffix.
       *
       * <p>See {@link ShardNameTemplate} for a description of shard templates.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> withSuffix(String filenameSuffix) {
        validateOutputComponent(filenameSuffix);
        return new Bound<>(
            name, filenamePrefix, filenameSuffix, numShards, shardTemplate, type, schema, validate);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
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
        Preconditions.checkArgument(numShards >= 0);
        return new Bound<>(
            name, filenamePrefix, filenameSuffix, numShards, shardTemplate, type, schema, validate);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that uses the given shard name template.
       *
       * <p>Does not modify this object.
       *
       * @see ShardNameTemplate
       */
      public Bound<T> withShardNameTemplate(String shardTemplate) {
        return new Bound<>(
            name, filenamePrefix, filenameSuffix, numShards, shardTemplate, type, schema, validate);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that forces a single file as output.
       *
       * <p>This is a shortcut for
       * {@code .withNumShards(1).withShardNameTemplate("")}
       *
       * <p>Does not modify this object.
       */
      public Bound<T> withoutSharding() {
        return new Bound<>(name, filenamePrefix, filenameSuffix, 1, "", type, schema, validate);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that writes to Avro file(s) containing records whose type is the
       * specified Avro-generated class.
       *
       * <p>Does not modify this object.
       *
       * @param <X> the type of the elements of the input PCollection
       */
      public <X> Bound<X> withSchema(Class<X> type) {
        return new Bound<>(
            name,
            filenamePrefix,
            filenameSuffix,
            numShards,
            shardTemplate,
            type,
            ReflectData.get().getSchema(type),
            validate);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that writes to Avro file(s) containing records of the specified
       * schema.
       *
       * <p>Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(Schema schema) {
        return new Bound<>(
            name,
            filenamePrefix,
            filenameSuffix,
            numShards,
            shardTemplate,
            GenericRecord.class,
            schema,
            validate);
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that writes to Avro file(s) containing records of the specified
       * schema in a JSON-encoded string form.
       *
       * <p>Does not modify this object.
       */
      public Bound<GenericRecord> withSchema(String schema) {
        return withSchema((new Schema.Parser()).parse(schema));
      }

      /**
       * Returns a new {@link PTransform} that's like this one but
       * that has GCS output path validation on pipeline creation disabled.
       *
       * <p>Does not modify this object.
       *
       * <p>This can be useful in the case where the GCS output location does
       * not exist at the pipeline creation time, but is expected to be
       * available at execution time.
       */
      public Bound<T> withoutValidation() {
        return new Bound<>(
            name, filenamePrefix, filenameSuffix, numShards, shardTemplate, type, schema, false);
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

        // Note that custom sinks currently do not expose sharding controls.
        // Thus pipeline runner writers need to individually add support internally to
        // apply user requested sharding limits.
        return input.apply(
            "Write",
            org.apache.beam.sdk.io.Write.to(
                new AvroSink<>(
                    filenamePrefix, filenameSuffix, shardTemplate, AvroCoder.of(type, schema))));
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder
            .add(DisplayData.item("schema", type)
              .withLabel("Record Schema"))
            .addIfNotNull(DisplayData.item("filePrefix", filenamePrefix)
              .withLabel("Output File Prefix"))
            .addIfNotDefault(DisplayData.item("shardNameTemplate", shardTemplate)
                .withLabel("Output Shard Name Template"),
                DEFAULT_SHARD_TEMPLATE)
            .addIfNotDefault(DisplayData.item("fileSuffix", filenameSuffix)
                .withLabel("Output File Suffix"),
                "")
            .addIfNotDefault(DisplayData.item("numShards", numShards)
                .withLabel("Maximum Output Shards"),
                0)
            .addIfNotDefault(DisplayData.item("validation", validate)
                .withLabel("Validation Enabled"),
                true);
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

      public Class<T> getType() {
        return type;
      }

      public Schema getSchema() {
        return schema;
      }

      public boolean needsValidation() {
        return validate;
      }
    }

    /** Disallow construction of utility class. */
    private Write() {}
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

  /** Disallow construction of utility class. */
  private AvroIO() {}

  /**
   * A {@link FileBasedSink} for Avro files.
   */
  @VisibleForTesting
  static class AvroSink<T> extends FileBasedSink<T> {
    private final AvroCoder<T> coder;

    @VisibleForTesting
    AvroSink(
        String baseOutputFilename, String extension, String fileNameTemplate, AvroCoder<T> coder) {
      super(baseOutputFilename, extension, fileNameTemplate);
      this.coder = coder;
    }

    @Override
    public FileBasedSink.FileBasedWriteOperation<T> createWriteOperation(PipelineOptions options) {
      return new AvroWriteOperation<>(this, coder);
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSink.FileBasedWriteOperation
     * FileBasedWriteOperation} for Avro files.
     */
    private static class AvroWriteOperation<T> extends FileBasedWriteOperation<T> {
      private final AvroCoder<T> coder;

      private AvroWriteOperation(AvroSink<T> sink, AvroCoder<T> coder) {
        super(sink);
        this.coder = coder;
      }

      @Override
      public FileBasedWriter<T> createWriter(PipelineOptions options) throws Exception {
        return new AvroWriter<>(this, coder);
      }
    }

    /**
     * A {@link org.apache.beam.sdk.io.FileBasedSink.FileBasedWriter FileBasedWriter}
     * for Avro files.
     */
    private static class AvroWriter<T> extends FileBasedWriter<T> {
      private final AvroCoder<T> coder;
      private DataFileWriter<T> dataFileWriter;

      public AvroWriter(FileBasedWriteOperation<T> writeOperation, AvroCoder<T> coder) {
        super(writeOperation);
        this.mimeType = MimeTypes.BINARY;
        this.coder = coder;
      }

      @SuppressWarnings("deprecation") // uses internal test functionality.
      @Override
      protected void prepareWrite(WritableByteChannel channel) throws Exception {
        dataFileWriter = new DataFileWriter<>(coder.createDatumWriter());
        dataFileWriter.create(coder.getSchema(), Channels.newOutputStream(channel));
      }

      @Override
      public void write(T value) throws Exception {
        dataFileWriter.append(value);
      }

      @Override
      protected void writeFooter() throws Exception {
        dataFileWriter.flush();
      }
    }
  }
}
