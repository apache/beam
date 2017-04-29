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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

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
 *     p.apply(AvroIO.read().from("/path/to/file.avro")
 *                 .withSchema(AvroAutoGenClass.class));
 *
 * // A Read from a GCS file (runs locally and using remote execution):
 * Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
 * PCollection<GenericRecord> records =
 *     p.apply(AvroIO.read()
 *                .from("gs://my_bucket/path/to/records-*.avro")
 *                .withSchema(schema));
 * } </pre>
 *
 * <p>To write a {@link PCollection} to one or more Avro files, use
 * {@link AvroIO.Write}, specifying {@code AvroIO.write().to(String)} to specify
 * the path of the file to write to (e.g., a local filename or sharded
 * filename pattern if running locally, or a Google Cloud Storage
 * filename or sharded filename pattern of the form
 * {@code "gs://<bucket>/<filepath>"}). {@code AvroIO.write().to(FileBasedSink.FilenamePolicy)}
 * can also be used to specify a custom file naming policy.
 *
 * <p>By default, all input is put into the global window before writing. If per-window writes are
 * desired - for example, when using a streaming runner -
 * {@link AvroIO.Write#withWindowedWrites()} will cause windowing and triggering to be
 * preserved. When producing windowed writes, the number of output shards must be set explicitly
 * using {@link AvroIO.Write#withNumShards(int)}; some runners may set this for you to a
 * runner-chosen value, so you may need not set it yourself. A
 * {@link FileBasedSink.FilenamePolicy} must be set, and unique windows and triggers must produce
 * unique filenames.
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
 * records.apply(AvroIO.write().to("/path/to/file.avro")
 *                           .withSchema(AvroAutoGenClass.class));
 *
 * // A Write to a sharded GCS file (runs locally and using remote execution):
 * Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
 * PCollection<GenericRecord> records = ...;
 * records.apply("WriteToAvro", AvroIO.write()
 *     .to("gs://my_bucket/path/to/numbers")
 *     .withSchema(schema)
 *     .withSuffix(".avro"));
 * } </pre>
 *
 * <p>By default, {@link AvroIO.Write} produces output files that are compressed using the
 * {@link org.apache.avro.file.Codec CodecFactory.deflateCodec(6)}. This default can
 * be changed or overridden using {@link AvroIO.Write#withCodec}.
 *
 * <h3>Permissions</h3>
 * Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * pipeline. Please refer to the documentation of corresponding {@link PipelineRunner}s for
 * more details.
 */
public class AvroIO {
  /**
   * Reads records of the given type from an Avro file (or multiple Avro files matching a pattern).
   *
   * <p>The schema must be specified using one of the {@code withSchema} functions.
   */
  public static <T> Read<T> read() {
    return new AutoValue_AvroIO_Read.Builder<T>().build();
  }

  /** Reads Avro file(s) containing records of the specified schema. */
  public static Read<GenericRecord> readGenericRecords(Schema schema) {
    return new AutoValue_AvroIO_Read.Builder<GenericRecord>()
        .setRecordClass(GenericRecord.class)
        .setSchema(schema)
        .build();
  }

  /**
   * Like {@link #readGenericRecords(Schema)} but the schema is specified as a JSON-encoded string.
   */
  public static Read<GenericRecord> readGenericRecords(String schema) {
    return readGenericRecords(new Schema.Parser().parse(schema));
  }

  /**
   * Writes a {@link PCollection} to an Avro file (or multiple Avro files matching a sharding
   * pattern).
   */
  public static <T> Write<T> write() {
    return new Write<>(null);
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable abstract String getFilepattern();
    @Nullable abstract Class<T> getRecordClass();
    @Nullable abstract Schema getSchema();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilepattern(String filepattern);
      abstract Builder<T> setRecordClass(Class<T> recordClass);
      abstract Builder<T> setSchema(Schema schema);

      abstract Read<T> build();
    }

    /**
     * Reads from the file(s) with the given name or pattern. This can be a local filename
     * or filename pattern (if running locally), or a Google Cloud
     * Storage filename or filename pattern of the form
     * {@code "gs://<bucket>/<filepath>"} (if running locally or
     * using remote execution). Standard
     * <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html">Java
     * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public Read<T> from(String filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /**
     * Returns a new {@link PTransform} that's like this one but
     * that reads Avro file(s) containing records whose type is the
     * specified Avro-generated class.
     */
    public Read<T> withSchema(Class<T> type) {
      return toBuilder().setRecordClass(type).setSchema(ReflectData.get().getSchema(type)).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      if (getFilepattern() == null) {
        throw new IllegalStateException(
            "need to set the filepattern of an AvroIO.Read transform");
      }
      if (getSchema() == null) {
        throw new IllegalStateException("need to set the schema of an AvroIO.Read transform");
      }

      @SuppressWarnings("unchecked")
      Bounded<T> read =
          getRecordClass() == GenericRecord.class
              ? (Bounded<T>) org.apache.beam.sdk.io.Read.from(
                  AvroSource.from(getFilepattern()).withSchema(getSchema()))
              : org.apache.beam.sdk.io.Read.from(
                  AvroSource.from(getFilepattern()).withSchema(getRecordClass()));

      PCollection<T> pcol = input.getPipeline().apply("Read", read);
      // Honor the default output coder that would have been used by this PTransform.
      pcol.setCoder(getDefaultOutputCoder());
      return pcol;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
        .addIfNotNull(DisplayData.item("filePattern", getFilepattern())
          .withLabel("Input File Pattern"));
    }

    @Override
    protected Coder<T> getDefaultOutputCoder() {
      return AvroCoder.of(getRecordClass(), getSchema());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #write}. */
  public static class Write<T> extends PTransform<PCollection<T>, PDone> {
    /**
     * A {@link PTransform} that writes a bounded {@link PCollection} to an Avro file (or
     * multiple Avro files matching a sharding pattern).
     *
     * @param <T> the type of each of the elements of the input PCollection
     */
    private static final String DEFAULT_SHARD_TEMPLATE = ShardNameTemplate.INDEX_OF_MAX;
    private static final SerializableAvroCodecFactory DEFAULT_CODEC =
        new SerializableAvroCodecFactory(CodecFactory.deflateCodec(6));
    // This should be a multiple of 4 to not get a partial encoded byte.
    private static final int METADATA_BYTES_MAX_LENGTH = 40;

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
    final boolean windowedWrites;
    FileBasedSink.FilenamePolicy filenamePolicy;

    /**
     * The codec used to encode the blocks in the Avro file. String value drawn from those in
     * https://avro.apache.org/docs/1.7.7/api/java/org/apache/avro/file/CodecFactory.html
     */
    final SerializableAvroCodecFactory codec;
    /** Avro file metadata. */
    final ImmutableMap<String, Object> metadata;

    Write(Class<T> type) {
      this(
          null,
          null,
          "",
          0,
          DEFAULT_SHARD_TEMPLATE,
          type,
          null,
          DEFAULT_CODEC,
          ImmutableMap.<String, Object>of(),
          false,
          null);
    }

    Write(
        String name,
        String filenamePrefix,
        String filenameSuffix,
        int numShards,
        String shardTemplate,
        Class<T> type,
        Schema schema,
        SerializableAvroCodecFactory codec,
        Map<String, Object> metadata,
        boolean windowedWrites,
        FileBasedSink.FilenamePolicy filenamePolicy) {
      super(name);
      this.filenamePrefix = filenamePrefix;
      this.filenameSuffix = filenameSuffix;
      this.numShards = numShards;
      this.shardTemplate = shardTemplate;
      this.type = type;
      this.schema = schema;
      this.codec = codec;
      this.windowedWrites = windowedWrites;
      this.filenamePolicy = filenamePolicy;

      Map<String, String> badKeys = Maps.newLinkedHashMap();
      for (Map.Entry<String, Object> entry : metadata.entrySet()) {
        Object v = entry.getValue();
        if (!(v instanceof String || v instanceof Long || v instanceof byte[])) {
          badKeys.put(entry.getKey(), v.getClass().getSimpleName());
        }
      }
      checkArgument(
          badKeys.isEmpty(),
          "Metadata value type must be one of String, Long, or byte[]. Found {}", badKeys);
      this.metadata = ImmutableMap.copyOf(metadata);
    }

    /**
     * Writes to the file(s) with the given prefix. This can be a local filename
     * (if running locally), or a Google Cloud Storage filename of
     * the form {@code "gs://<bucket>/<filepath>"}
     * (if running locally or using remote execution).
     *
     * <p>The files written will begin with this prefix, followed by
     * a shard identifier (see {@link #withNumShards}, and end
     * in a common extension, if given by {@link #withSuffix}.
     */
    public Write<T> to(String filenamePrefix) {
      validateOutputComponent(filenamePrefix);
      return new Write<>(
          name,
          filenamePrefix,
          filenameSuffix,
          numShards,
          shardTemplate,
          type,
          schema,
          codec,
          metadata,
          windowedWrites,
          filenamePolicy);
    }

    /** Writes to the file(s) specified by the provided {@link FileBasedSink.FilenamePolicy}. */
    public Write<T> to(FileBasedSink.FilenamePolicy filenamePolicy) {
      return new Write<>(
          name,
          filenamePrefix,
          filenameSuffix,
          numShards,
          shardTemplate,
          type,
          schema,
          codec,
          metadata,
          windowedWrites,
          filenamePolicy);
    }

    /**
     * Writes to the file(s) with the given filename suffix.
     *
     * <p>See {@link ShardNameTemplate} for a description of shard templates.
     */
    public Write<T> withSuffix(String filenameSuffix) {
      validateOutputComponent(filenameSuffix);
      return new Write<>(
          name,
          filenamePrefix,
          filenameSuffix,
          numShards,
          shardTemplate,
          type,
          schema,
          codec,
          metadata,
          windowedWrites,
          filenamePolicy);
    }

    /**
     * Uses the provided shard count.
     *
     * <p>Constraining the number of shards is likely to reduce
     * the performance of a pipeline. Setting this value is not recommended
     * unless you require a specific number of output files.
     *
     * @param numShards the number of shards to use, or 0 to let the system
     *                  decide.
     * @see ShardNameTemplate
     */
    public Write<T> withNumShards(int numShards) {
      checkArgument(numShards >= 0);
      return new Write<>(
          name,
          filenamePrefix,
          filenameSuffix,
          numShards,
          shardTemplate,
          type,
          schema,
          codec,
          metadata,
          windowedWrites,
          filenamePolicy);
    }

    /**
     * Returns a new {@link PTransform} that's like this one but
     * that uses the given shard name template.
     *
     * @see ShardNameTemplate
     */
    public Write<T> withShardNameTemplate(String shardTemplate) {
      return new Write<>(
          name,
          filenamePrefix,
          filenameSuffix,
          numShards,
          shardTemplate,
          type,
          schema,
          codec,
          metadata,
          windowedWrites,
          filenamePolicy);
    }

    /**
     * Forces a single file as output.
     *
     * <p>This is a shortcut for {@code .withNumShards(1).withShardNameTemplate("")}
     */
    public Write<T> withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    public Write<T> withWindowedWrites() {
      return new Write<>(
          name,
          filenamePrefix,
          filenameSuffix,
          numShards,
          shardTemplate,
          type,
          schema,
          codec,
          metadata,
          true,
          filenamePolicy);
    }

    /**
     * Writes to Avro file(s) containing records whose type is the specified Avro-generated class.
     */
    public Write<T> withSchema(Class<T> type) {
      return new Write<>(
          name,
          filenamePrefix,
          filenameSuffix,
          numShards,
          shardTemplate,
          type,
          ReflectData.get().getSchema(type),
          codec,
          metadata,
          windowedWrites,
          filenamePolicy);
    }

    /** Writes to Avro file(s) containing records of the specified schema. */
    public Write<GenericRecord> withSchema(Schema schema) {
      return new Write<>(
          name,
          filenamePrefix,
          filenameSuffix,
          numShards,
          shardTemplate,
          GenericRecord.class,
          schema,
          codec,
          metadata,
          windowedWrites,
          filenamePolicy);
    }

    /**
     * Writes to Avro file(s) containing records of the specified schema in a JSON-encoded string
     * form.
     */
    public Write<GenericRecord> withSchema(String schema) {
      return withSchema((new Schema.Parser()).parse(schema));
    }

    /** Writes to Avro file(s) compressed using specified codec. */
    public Write<T> withCodec(CodecFactory codec) {
      return new Write<>(
          name,
          filenamePrefix,
          filenameSuffix,
          numShards,
          shardTemplate,
          type,
          schema,
          new SerializableAvroCodecFactory(codec),
          metadata,
          windowedWrites,
          filenamePolicy);
    }

    /**
     * Writes to Avro file(s) with the specified metadata.
     *
     * <p>Supported value types are String, Long, and byte[].
     */
    public Write<T> withMetadata(Map<String, Object> metadata) {
      return new Write<>(
          name,
          filenamePrefix,
          filenameSuffix,
          numShards,
          shardTemplate,
          type,
          schema,
          codec,
          metadata,
          windowedWrites,
          filenamePolicy);
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (filenamePolicy == null && filenamePrefix == null) {
        throw new IllegalStateException(
            "need to set the filename prefix of an AvroIO.Write transform");
      }
      if (filenamePolicy != null && filenamePrefix != null) {
        throw new IllegalStateException(
            "cannot set both a filename policy and a filename prefix");
      }
      if (schema == null) {
        throw new IllegalStateException("need to set the schema of an AvroIO.Write transform");
      }

      WriteFiles<T> write = null;
      if (filenamePolicy != null) {
        write = WriteFiles.to(
            new AvroSink<>(
                filenamePolicy,
                AvroCoder.of(type, schema),
                codec,
                metadata));
      } else {
        write = WriteFiles.to(
            new AvroSink<>(
                filenamePrefix,
                filenameSuffix,
                shardTemplate,
                AvroCoder.of(type, schema),
                codec,
                metadata));
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
          .addIfNotDefault(DisplayData.item("codec", codec.toString())
              .withLabel("Avro Compression Codec"),
              DEFAULT_CODEC.toString());
      builder.include("Metadata", new Metadata());
    }

    private class Metadata implements HasDisplayData {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
          DisplayData.Type type = DisplayData.inferType(entry.getValue());
          if (type != null) {
            builder.add(DisplayData.item(entry.getKey(), type, entry.getValue()));
          } else {
            String base64 = BaseEncoding.base64().encode((byte[]) entry.getValue());
            String repr = base64.length() <= METADATA_BYTES_MAX_LENGTH
                ? base64 : base64.substring(0, METADATA_BYTES_MAX_LENGTH) + "...";
            builder.add(DisplayData.item(entry.getKey(), repr));
          }
        }
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

    public Class<T> getType() {
      return type;
    }

    public Schema getSchema() {
      return schema;
    }

    public CodecFactory getCodec() {
      return codec.getCodec();
    }

    public Map<String, Object> getMetadata() {
      return metadata;
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

  /////////////////////////////////////////////////////////////////////////////

  /** Disallow construction of utility class. */
  private AvroIO() {}
}
