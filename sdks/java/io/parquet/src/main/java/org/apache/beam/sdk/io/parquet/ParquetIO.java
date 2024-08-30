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
package org.apache.beam.sdk.io.parquet;

import static java.lang.String.format;
import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.parquet.ParquetIO.ReadFiles.SplitReadFn;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write Parquet files.
 *
 * <h3>Reading Parquet files</h3>
 *
 * <p>{@link ParquetIO} source returns a {@link PCollection} for Parquet files. The elements in the
 * {@link PCollection} are Avro {@link GenericRecord}.
 *
 * <p>To configure the {@link Read}, you have to provide the file patterns (from) of the Parquet
 * files and the schema.
 *
 * <p>For example:
 *
 * <pre>{@code
 * PCollection<GenericRecord> records = pipeline.apply(ParquetIO.read(SCHEMA).from("/foo/bar"));
 * ...
 * }</pre>
 *
 * <p>As {@link Read} is based on {@link FileIO}, it supports any filesystem (hdfs, ...).
 *
 * <p>When using schemas created via reflection, it may be useful to generate {@link GenericRecord}
 * instances rather than instances of the class associated with the schema. {@link Read} and {@link
 * ReadFiles} provide {@link ParquetIO.Read#withAvroDataModel(GenericData)} allowing implementations
 * to set the data model associated with the {@link AvroParquetReader}
 *
 * <p>For more advanced use cases, like reading each file in a {@link PCollection} of {@link
 * ReadableFile}, use the {@link ReadFiles} transform.
 *
 * <p>For example:
 *
 * <pre>{@code
 * PCollection<FileIO.ReadableFile> files = pipeline
 *   .apply(FileIO.match().filepattern(options.getInputFilepattern())
 *   .apply(FileIO.readMatches());
 *
 * PCollection<GenericRecord> output = files.apply(ParquetIO.readFiles(SCHEMA));
 * }</pre>
 *
 * <p>ParquetIO leverages splittable reading by using Splittable DoFn. It initially splits the files
 * into the blocks of 64MB and may dynamically split further for higher read efficiency.
 *
 * <p>Reading with projection can be enabled with the projection schema as following. Splittable
 * reading is enabled when reading with projection. The projection_schema contains only the column
 * that we would like to read and encoder_schema contains the schema to encode the output with the
 * unwanted columns changed to nullable. Partial reading provide decrease of reading time due to
 * partial processing of the data and partial encoding. The decrease in the reading time depends on
 * the relative position of the columns. Memory allocation is optimised depending on the encoding
 * schema. Note that the improvement is not as significant comparing to the proportion of the data
 * requested, since the processing time saved is only the time to read the unwanted columns, the
 * reader will still go over the data set according to the encoding schema since data for each
 * column in a row is stored interleaved.
 *
 * <pre>{@code
 * PCollection<GenericRecord> records =
 *   pipeline
 *     .apply(
 *       ParquetIO.read(SCHEMA).from("/foo/bar").withProjection(Projection_schema,Encoder_Schema));
 * }</pre>
 *
 * <h3>Reading records of an unknown schema</h3>
 *
 * <p>To read records from files whose schema is unknown at pipeline construction time or differs
 * between files, use {@link #parseGenericRecords(SerializableFunction)} - in this case, you will
 * need to specify a parsing function for converting each {@link GenericRecord} into a value of your
 * custom type.
 *
 * <p>For example:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<Foo> records =
 *     p.apply(
 *       ParquetIO.parseGenericRecords(
 *           new SerializableFunction<GenericRecord, Foo>() {
 *               public Foo apply(GenericRecord record) {
 *                   // If needed, access the schema of the record using record.getSchema()
 *                   return ...;
 *               }
 *           })
 *           .from(...));
 *
 * // For reading from files
 *  PCollection<FileIO.ReadableFile> files = p.apply(...);
 *
 *  PCollection<Foo> records =
 *     files
 *       .apply(
 *           ParquetIO.parseFilesGenericRecords(
 *               new SerializableFunction<GenericRecord, Foo>() {
 *                   public Foo apply(GenericRecord record) {
 *                       // If needed, access the schema of the record using record.getSchema()
 *                       return ...;
 *                   }
 *           }));
 * }</pre>
 *
 * <h3>Inferring Beam schemas from Parquet files</h3>
 *
 * <p>If you want to use SQL or schema based operations on an Parquet-based PCollection, you must
 * configure the read transform to infer the Beam schema and automatically setup the Beam related
 * coders by doing:
 *
 * <pre>{@code
 * PCollection<GenericRecord> parquetRecords =
 *   p.apply(ParquetIO.read(...).from(...).withBeamSchemas(true));
 * }</pre>
 *
 * You can also use it when reading a list of filenams from a {@code PCollection<String>}:
 *
 * <pre>{@code
 * PCollection<String> filePatterns = p.apply(...);
 *
 * PCollection<GenericRecord> parquetRecords =
 *   filePatterns
 *     .apply(ParquetIO.readFiles(...).withBeamSchemas(true));
 * }</pre>
 *
 * <h3>Writing Parquet files</h3>
 *
 * <p>{@link ParquetIO.Sink} allows you to write a {@link PCollection} of {@link GenericRecord} into
 * a Parquet file. It can be used with the general-purpose {@link FileIO} transforms with
 * FileIO.write/writeDynamic specifically.
 *
 * <p>By default, {@link ParquetIO.Sink} produces output files that are compressed using the {@link
 * org.apache.parquet.format.CompressionCodec#SNAPPY}. This default can be changed or overridden
 * using {@link ParquetIO.Sink#withCompressionCodec(CompressionCodecName)}.
 *
 * <p>For example:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...) // PCollection<GenericRecord>
 *   .apply(FileIO
 *     .<GenericRecord>write()
 *     .via(ParquetIO.sink(SCHEMA)
 *       .withCompressionCodec(CompressionCodecName.SNAPPY))
 *     .to("destination/path")
 *     .withSuffix(".parquet"));
 * }</pre>
 *
 * @see <a href="https://beam.apache.org/documentation/io/built-in/parquet/">Beam ParquetIO
 *     documentation</a>
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ParquetIO {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetIO.class);

  /**
   * Reads {@link GenericRecord} from a Parquet file (or multiple Parquet files matching the
   * pattern).
   */
  public static Read read(Schema schema) {
    return new AutoValue_ParquetIO_Read.Builder()
        .setSchema(schema)
        .setInferBeamSchema(false)
        .build();
  }

  /**
   * Like {@link #read(Schema)}, but reads each file in a {@link PCollection} of {@link
   * ReadableFile}, which allows more flexible usage.
   */
  public static ReadFiles readFiles(Schema schema) {
    return new AutoValue_ParquetIO_ReadFiles.Builder()
        .setSchema(schema)
        .setInferBeamSchema(false)
        .build();
  }

  /**
   * Reads {@link GenericRecord} from a Parquet file (or multiple Parquet files matching the
   * pattern) and converts to user defined type using provided parseFn.
   */
  public static <T> Parse<T> parseGenericRecords(SerializableFunction<GenericRecord, T> parseFn) {
    return new AutoValue_ParquetIO_Parse.Builder<T>().setParseFn(parseFn).build();
  }

  /**
   * Reads {@link GenericRecord} from Parquet files and converts to user defined type using provided
   * {@code parseFn}.
   */
  public static <T> ParseFiles<T> parseFilesGenericRecords(
      SerializableFunction<GenericRecord, T> parseFn) {
    return new AutoValue_ParquetIO_ParseFiles.Builder<T>().setParseFn(parseFn).build();
  }

  /** Implementation of {@link #read(Schema)}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<GenericRecord>> {

    abstract @Nullable ValueProvider<String> getFilepattern();

    abstract @Nullable Schema getSchema();

    abstract @Nullable Schema getProjectionSchema();

    abstract @Nullable Schema getEncoderSchema();

    abstract @Nullable GenericData getAvroDataModel();

    abstract @Nullable SerializableConfiguration getConfiguration();

    abstract boolean getInferBeamSchema();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setInferBeamSchema(boolean inferBeamSchema);

      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Builder setSchema(Schema schema);

      abstract Builder setEncoderSchema(Schema schema);

      abstract Builder setProjectionSchema(Schema schema);

      abstract Builder setAvroDataModel(GenericData model);

      abstract Builder setConfiguration(SerializableConfiguration configuration);

      abstract Read build();
    }

    /** Reads from the given filename or filepattern. */
    public Read from(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Like {@link #from(ValueProvider)}. */
    public Read from(String filepattern) {
      return from(ValueProvider.StaticValueProvider.of(filepattern));
    }
    /** Enable the reading with projection. */
    public Read withProjection(Schema projectionSchema, Schema encoderSchema) {
      return toBuilder()
          .setProjectionSchema(projectionSchema)
          .setEncoderSchema(encoderSchema)
          .build();
    }

    /** Specify Hadoop configuration for ParquetReader. */
    public Read withConfiguration(Map<String, String> configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return toBuilder().setConfiguration(SerializableConfiguration.fromMap(configuration)).build();
    }

    /** Specify Hadoop configuration for ParquetReader. */
    public Read withConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
    }

    public Read withBeamSchemas(boolean inferBeamSchema) {
      return toBuilder().setInferBeamSchema(inferBeamSchema).build();
    }

    /**
     * Define the Avro data model; see {@link AvroParquetReader.Builder#withDataModel(GenericData)}.
     */
    public Read withAvroDataModel(GenericData model) {
      return toBuilder().setAvroDataModel(model).build();
    }

    @Override
    public PCollection<GenericRecord> expand(PBegin input) {
      checkNotNull(getFilepattern(), "Filepattern cannot be null.");
      PCollection<ReadableFile> inputFiles =
          input
              .apply(
                  "Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches());

      ReadFiles readFiles =
          readFiles(getSchema())
              .withBeamSchemas(getInferBeamSchema())
              .withAvroDataModel(getAvroDataModel())
              .withProjection(getProjectionSchema(), getEncoderSchema());
      if (getConfiguration() != null) {
        readFiles = readFiles.withConfiguration(getConfiguration().get());
      }

      return inputFiles.apply(readFiles);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
          .addIfNotNull(DisplayData.item("schema", String.valueOf(getSchema())))
          .add(
              DisplayData.item("inferBeamSchema", getInferBeamSchema())
                  .withLabel("Infer Beam Schema"))
          .addIfNotNull(DisplayData.item("projectionSchema", String.valueOf(getProjectionSchema())))
          .addIfNotNull(DisplayData.item("avroDataModel", String.valueOf(getAvroDataModel())));
      if (this.getConfiguration() != null) {
        Configuration configuration = this.getConfiguration().get();
        for (Entry<String, String> entry : configuration) {
          if (entry.getKey().startsWith("parquet")) {
            builder.addIfNotNull(DisplayData.item(entry.getKey(), entry.getValue()));
          }
        }
      }
    }
  }

  /** Implementation of {@link #parseGenericRecords(SerializableFunction)}. */
  @AutoValue
  public abstract static class Parse<T> extends PTransform<PBegin, PCollection<T>> {
    abstract @Nullable ValueProvider<String> getFilepattern();

    abstract SerializableFunction<GenericRecord, T> getParseFn();

    abstract @Nullable Coder<T> getCoder();

    abstract @Nullable SerializableConfiguration getConfiguration();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilepattern(ValueProvider<String> inputFiles);

      abstract Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setConfiguration(SerializableConfiguration configuration);

      abstract Parse<T> build();
    }

    public Parse<T> from(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    public Parse<T> from(String filepattern) {
      return from(ValueProvider.StaticValueProvider.of(filepattern));
    }

    /** Specify the output coder to use for output of the {@code ParseFn}. */
    public Parse<T> withCoder(Coder<T> coder) {
      return (coder == null) ? this : toBuilder().setCoder(coder).build();
    }

    /** Specify Hadoop configuration for ParquetReader. */
    public Parse<T> withConfiguration(Map<String, String> configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return toBuilder().setConfiguration(SerializableConfiguration.fromMap(configuration)).build();
    }

    /** Specify Hadoop configuration for ParquetReader. */
    public Parse<T> withConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkNotNull(getFilepattern(), "Filepattern cannot be null.");
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply(FileIO.matchAll())
          .apply(FileIO.readMatches())
          .apply(
              parseFilesGenericRecords(getParseFn())
                  .toBuilder()
                  .setCoder(getCoder())
                  .setConfiguration(getConfiguration())
                  .build());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
          .add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"));
      if (this.getCoder() != null) {
        builder.add(DisplayData.item("coder", getCoder().getClass()));
      }
      if (this.getConfiguration() != null) {
        Configuration configuration = this.getConfiguration().get();
        for (Entry<String, String> entry : configuration) {
          if (entry.getKey().startsWith("parquet")) {
            builder.addIfNotNull(DisplayData.item(entry.getKey(), entry.getValue()));
          }
        }
      }
    }
  }

  /** Implementation of {@link #parseFilesGenericRecords(SerializableFunction)}. */
  @AutoValue
  public abstract static class ParseFiles<T>
      extends PTransform<PCollection<ReadableFile>, PCollection<T>> {

    abstract SerializableFunction<GenericRecord, T> getParseFn();

    abstract @Nullable Coder<T> getCoder();

    abstract @Nullable SerializableConfiguration getConfiguration();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setConfiguration(SerializableConfiguration configuration);

      abstract ParseFiles<T> build();
    }

    /** Specify the output coder to use for output of the {@code ParseFn}. */
    public ParseFiles<T> withCoder(Coder<T> coder) {
      return (coder == null) ? this : toBuilder().setCoder(coder).build();
    }

    /** Specify Hadoop configuration for ParquetReader. */
    public ParseFiles<T> withConfiguration(Map<String, String> configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return toBuilder().setConfiguration(SerializableConfiguration.fromMap(configuration)).build();
    }

    /** Specify Hadoop configuration for ParquetReader. */
    public ParseFiles<T> withConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
    }

    @Override
    public PCollection<T> expand(PCollection<ReadableFile> input) {
      checkArgument(!isGenericRecordOutput(), "Parse can't be used for reading as GenericRecord.");

      return input
          .apply(ParDo.of(new SplitReadFn<>(null, null, getParseFn(), getConfiguration())))
          .setCoder(inferCoder(input.getPipeline().getCoderRegistry()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"));
      if (this.getCoder() != null) {
        builder.add(DisplayData.item("coder", getCoder().getClass()));
      }
      if (this.getConfiguration() != null) {
        Configuration configuration = this.getConfiguration().get();
        for (Entry<String, String> entry : configuration) {
          if (entry.getKey().startsWith("parquet")) {
            builder.addIfNotNull(DisplayData.item(entry.getKey(), entry.getValue()));
          }
        }
      }
    }

    /** Returns true if expected output is {@code PCollection<GenericRecord>}. */
    private boolean isGenericRecordOutput() {
      String outputType = TypeDescriptors.outputOf(getParseFn()).getType().getTypeName();
      return outputType.equals(GenericRecord.class.getTypeName());
    }

    /**
     * Identifies the {@code Coder} to be used for the output PCollection.
     *
     * <p>throws an exception if expected output is of type {@link GenericRecord}.
     *
     * @param coderRegistry the {@link org.apache.beam.sdk.Pipeline}'s CoderRegistry to identify
     *     Coder for expected output type of {@link #getParseFn()}
     */
    private Coder<T> inferCoder(CoderRegistry coderRegistry) {
      if (isGenericRecordOutput()) {
        throw new IllegalArgumentException("Parse can't be used for reading as GenericRecord.");
      }

      // Use explicitly provided coder
      if (getCoder() != null) {
        return getCoder();
      }

      // If not GenericRecord infer it from ParseFn.
      try {
        return coderRegistry.getCoder(TypeDescriptors.outputOf(getParseFn()));
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(
            "Unable to infer coder for output of parseFn. Specify it explicitly using .withCoder().",
            e);
      }
    }
  }

  /** Implementation of {@link #readFiles(Schema)}. */
  @AutoValue
  public abstract static class ReadFiles
      extends PTransform<PCollection<ReadableFile>, PCollection<GenericRecord>> {

    abstract @Nullable Schema getSchema();

    abstract @Nullable GenericData getAvroDataModel();

    abstract @Nullable Schema getEncoderSchema();

    abstract @Nullable Schema getProjectionSchema();

    abstract @Nullable SerializableConfiguration getConfiguration();

    abstract boolean getInferBeamSchema();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSchema(Schema schema);

      abstract Builder setAvroDataModel(GenericData model);

      abstract Builder setEncoderSchema(Schema schema);

      abstract Builder setProjectionSchema(Schema schema);

      abstract Builder setConfiguration(SerializableConfiguration configuration);

      abstract Builder setInferBeamSchema(boolean inferBeamSchema);

      abstract ReadFiles build();
    }

    /**
     * Define the Avro data model; see {@link AvroParquetReader.Builder#withDataModel(GenericData)}.
     */
    public ReadFiles withAvroDataModel(GenericData model) {
      return toBuilder().setAvroDataModel(model).build();
    }

    public ReadFiles withProjection(Schema projectionSchema, Schema encoderSchema) {
      return toBuilder()
          .setProjectionSchema(projectionSchema)
          .setEncoderSchema(encoderSchema)
          .build();
    }

    /** Specify Hadoop configuration for ParquetReader. */
    public ReadFiles withConfiguration(Map<String, String> configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return toBuilder().setConfiguration(SerializableConfiguration.fromMap(configuration)).build();
    }

    /** Specify Hadoop configuration for ParquetReader. */
    public ReadFiles withConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
    }

    public ReadFiles withBeamSchemas(boolean inferBeamSchema) {
      return toBuilder().setInferBeamSchema(inferBeamSchema).build();
    }

    @Override
    public PCollection<GenericRecord> expand(PCollection<ReadableFile> input) {
      checkNotNull(getSchema(), "Schema can not be null");
      return input
          .apply(
              ParDo.of(
                  new SplitReadFn<>(
                      getAvroDataModel(),
                      getProjectionSchema(),
                      GenericRecordPassthroughFn.create(),
                      getConfiguration())))
          .setCoder(getCollectionCoder());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("schema", String.valueOf(getSchema())))
          .add(
              DisplayData.item("inferBeamSchema", getInferBeamSchema())
                  .withLabel("Infer Beam Schema"))
          .addIfNotNull(DisplayData.item("projectionSchema", String.valueOf(getProjectionSchema())))
          .addIfNotNull(DisplayData.item("avroDataModel", String.valueOf(getAvroDataModel())));
      if (this.getConfiguration() != null) {
        Configuration configuration = this.getConfiguration().get();
        for (Entry<String, String> entry : configuration) {
          if (entry.getKey().startsWith("parquet")) {
            builder.addIfNotNull(DisplayData.item(entry.getKey(), entry.getValue()));
          }
        }
      }
    }

    /**
     * Returns {@link org.apache.beam.sdk.schemas.SchemaCoder} when using Beam schemas, {@link
     * AvroCoder} when not using Beam schema.
     */
    private Coder<GenericRecord> getCollectionCoder() {
      Schema coderSchema = getProjectionSchema() != null ? getEncoderSchema() : getSchema();

      return getInferBeamSchema() ? AvroUtils.schemaCoder(coderSchema) : AvroCoder.of(coderSchema);
    }

    @DoFn.BoundedPerElement
    static class SplitReadFn<T> extends DoFn<ReadableFile, T> {
      private final Class<? extends GenericData> modelClass;
      private final String requestSchemaString;
      // Default initial splitting the file into blocks of 64MB. Unit of SPLIT_LIMIT is byte.
      private static final long SPLIT_LIMIT = 64000000;

      private @Nullable final SerializableConfiguration configuration;

      private final SerializableFunction<GenericRecord, T> parseFn;

      SplitReadFn(
          GenericData model,
          Schema requestSchema,
          SerializableFunction<GenericRecord, T> parseFn,
          @Nullable SerializableConfiguration configuration) {

        this.modelClass = model != null ? model.getClass() : null;
        this.requestSchemaString = requestSchema != null ? requestSchema.toString() : null;
        this.parseFn = checkNotNull(parseFn, "GenericRecord parse function can't be null");
        this.configuration = configuration;
      }

      private ParquetFileReader getParquetFileReader(ReadableFile file) throws Exception {
        ParquetReadOptions options = HadoopReadOptions.builder(getConfWithModelClass()).build();
        return ParquetFileReader.open(new BeamParquetInputFile(file.openSeekable()), options);
      }

      @ProcessElement
      public void processElement(
          @Element ReadableFile file,
          RestrictionTracker<OffsetRange, Long> tracker,
          OutputReceiver<T> outputReceiver)
          throws Exception {
        LOG.debug(
            "start {} to {}",
            tracker.currentRestriction().getFrom(),
            tracker.currentRestriction().getTo());
        Configuration conf = getConfWithModelClass();
        GenericData model = null;
        if (modelClass != null) {
          model = (GenericData) modelClass.getMethod("get").invoke(null);
        }
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>(model);
        if (requestSchemaString != null) {
          AvroReadSupport.setRequestedProjection(
              conf, new Schema.Parser().parse(requestSchemaString));
        }
        ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
        try (ParquetFileReader reader =
            ParquetFileReader.open(new BeamParquetInputFile(file.openSeekable()), options)) {
          Filter filter = checkNotNull(options.getRecordFilter(), "filter");
          Configuration hadoopConf = ((HadoopReadOptions) options).getConf();
          FileMetaData parquetFileMetadata = reader.getFooter().getFileMetaData();
          MessageType fileSchema = parquetFileMetadata.getSchema();
          Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
          ReadSupport.ReadContext readContext =
              readSupport.init(
                  new InitContext(
                      hadoopConf,
                      Maps.transformValues(fileMetadata, ImmutableSet::of),
                      fileSchema));
          ColumnIOFactory columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());

          RecordMaterializer<GenericRecord> recordConverter =
              readSupport.prepareForRead(hadoopConf, fileMetadata, fileSchema, readContext);
          reader.setRequestedSchema(readContext.getRequestedSchema());
          MessageColumnIO columnIO =
              columnIOFactory.getColumnIO(readContext.getRequestedSchema(), fileSchema, true);
          long currentBlock = tracker.currentRestriction().getFrom();
          for (int i = 0; i < currentBlock; i++) {
            reader.skipNextRowGroup();
          }
          while (tracker.tryClaim(currentBlock)) {
            PageReadStore pages = reader.readNextRowGroup();
            LOG.debug("block {} read in memory. row count = {}", currentBlock, pages.getRowCount());
            currentBlock += 1;
            RecordReader<GenericRecord> recordReader =
                columnIO.getRecordReader(
                    pages, recordConverter, options.useRecordFilter() ? filter : FilterCompat.NOOP);
            long currentRow = 0;
            long totalRows = pages.getRowCount();
            while (currentRow < totalRows) {
              try {
                GenericRecord record;
                currentRow += 1;
                try {
                  record = recordReader.read();
                } catch (RecordMaterializer.RecordMaterializationException e) {
                  LOG.warn(
                      "skipping a corrupt record at {} in block {} in file {}",
                      currentRow,
                      currentBlock,
                      file.toString());
                  continue;
                }
                if (record == null) {
                  // it happens when a record is filtered out in this block
                  LOG.debug(
                      "record is filtered out by reader in block {} in file {}",
                      currentBlock,
                      file.toString());
                  continue;
                }
                if (recordReader.shouldSkipCurrentRecord()) {
                  // this record is being filtered via the filter2 package
                  LOG.debug(
                      "skipping record at {} in block {} in file {}",
                      currentRow,
                      currentBlock,
                      file.toString());
                  continue;
                }
                outputReceiver.output(parseFn.apply(record));
              } catch (RuntimeException e) {

                throw new ParquetDecodingException(
                    format(
                        "Can not read value at %d in block %d in file %s",
                        currentRow, currentBlock, file.toString()),
                    e);
              }
            }
            LOG.debug(
                "Finish processing {} rows from block {} in file {}",
                currentRow,
                currentBlock - 1,
                file.toString());
          }
        }
      }

      public Configuration getConfWithModelClass() throws ReflectiveOperationException {
        Configuration conf = SerializableConfiguration.newConfiguration(configuration);
        GenericData model = buildModelObject(modelClass);

        if (model != null
            && (model.getClass() == GenericData.class || model.getClass() == SpecificData.class)) {
          conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);
        } else {
          conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
        }
        return conf;
      }

      @GetInitialRestriction
      public OffsetRange getInitialRestriction(@Element ReadableFile file) throws Exception {
        try (ParquetFileReader reader = getParquetFileReader(file)) {
          return new OffsetRange(0, reader.getRowGroups().size());
        }
      }

      @SplitRestriction
      public void split(
          @Restriction OffsetRange restriction,
          OutputReceiver<OffsetRange> out,
          @Element ReadableFile file)
          throws Exception {
        try (ParquetFileReader reader = getParquetFileReader(file)) {
          List<BlockMetaData> rowGroups = reader.getRowGroups();
          for (OffsetRange offsetRange :
              splitBlockWithLimit(
                  restriction.getFrom(), restriction.getTo(), rowGroups, SPLIT_LIMIT)) {
            out.output(offsetRange);
          }
        }
      }

      public ArrayList<OffsetRange> splitBlockWithLimit(
          long start, long end, List<BlockMetaData> blockList, long limit) {
        ArrayList<OffsetRange> offsetList = new ArrayList<>();
        long totalSize = 0;
        long rangeStart = start;
        for (long rangeEnd = start; rangeEnd < end; rangeEnd++) {
          totalSize += blockList.get((int) rangeEnd).getTotalByteSize();
          if (totalSize >= limit) {
            offsetList.add(new OffsetRange(rangeStart, rangeEnd + 1));
            rangeStart = rangeEnd + 1;
            totalSize = 0;
          }
        }
        if (totalSize != 0) {
          offsetList.add(new OffsetRange(rangeStart, end));
        }
        return offsetList;
      }

      @NewTracker
      public RestrictionTracker<OffsetRange, Long> newTracker(
          @Restriction OffsetRange restriction, @Element ReadableFile file) throws Exception {
        CountAndSize recordCountAndSize = getRecordCountAndSize(file, restriction);
        return new BlockTracker(
            restriction,
            Math.round(recordCountAndSize.getSize()),
            Math.round(recordCountAndSize.getCount()));
      }

      @GetRestrictionCoder
      public OffsetRange.Coder getRestrictionCoder() {
        return new OffsetRange.Coder();
      }

      @GetSize
      public double getSize(@Element ReadableFile file, @Restriction OffsetRange restriction)
          throws Exception {
        return getRecordCountAndSize(file, restriction).getSize();
      }

      private CountAndSize getRecordCountAndSize(ReadableFile file, OffsetRange restriction)
          throws Exception {
        try (ParquetFileReader reader = getParquetFileReader(file)) {
          double size = 0;
          double recordCount = 0;
          for (long i = restriction.getFrom(); i < restriction.getTo(); i++) {
            BlockMetaData block = reader.getRowGroups().get((int) i);
            recordCount += block.getRowCount();
            size += block.getTotalByteSize();
          }
          return CountAndSize.create(recordCount, size);
        }
      }

      @AutoValue
      abstract static class CountAndSize {
        static CountAndSize create(double count, double size) {
          return new AutoValue_ParquetIO_ReadFiles_SplitReadFn_CountAndSize(count, size);
        }

        abstract double getCount();

        abstract double getSize();
      }
    }

    public static class BlockTracker extends OffsetRangeTracker {
      private long totalWork;
      private long progress;
      private long approximateRecordSize;

      public BlockTracker(OffsetRange range, long totalByteSize, long recordCount) {
        super(range);
        if (recordCount != 0) {
          this.approximateRecordSize = totalByteSize / recordCount;
          // Ensure that totalWork = approximateRecordSize * recordCount
          this.totalWork = approximateRecordSize * recordCount;
          this.progress = 0;
        }
      }

      public void makeProgress() throws IOException {
        progress += approximateRecordSize;
        if (progress > totalWork) {
          throw new IOException("Making progress out of range");
        }
      }

      @Override
      // TODO(BEAM-10842): Refine the BlockTracker to provide better progress.
      public Progress getProgress() {
        return super.getProgress();
      }
    }

    private static class BeamParquetInputFile implements InputFile {
      private final SeekableByteChannel seekableByteChannel;

      BeamParquetInputFile(SeekableByteChannel seekableByteChannel) {
        this.seekableByteChannel = seekableByteChannel;
      }

      @Override
      public long getLength() throws IOException {
        return seekableByteChannel.size();
      }

      @Override
      public SeekableInputStream newStream() {
        return new DelegatingSeekableInputStream(Channels.newInputStream(seekableByteChannel)) {

          @Override
          public long getPos() throws IOException {
            return seekableByteChannel.position();
          }

          @Override
          public void seek(long newPos) throws IOException {
            seekableByteChannel.position(newPos);
          }
        };
      }
    }
  }

  /** Creates a {@link Sink} that, for use with {@link FileIO#write}. */
  public static Sink sink(Schema schema) {
    return new AutoValue_ParquetIO_Sink.Builder()
        .setJsonSchema(schema.toString())
        .setCompressionCodec(CompressionCodecName.SNAPPY)
        // This resembles the default value for ParquetWriter.rowGroupSize.
        .setRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
        .build();
  }

  /** Implementation of {@link #sink}. */
  @AutoValue
  public abstract static class Sink implements FileIO.Sink<GenericRecord> {

    abstract @Nullable String getJsonSchema();

    abstract CompressionCodecName getCompressionCodec();

    abstract @Nullable SerializableConfiguration getConfiguration();

    abstract int getRowGroupSize();

    abstract @Nullable Class<? extends GenericData> getAvroDataModelClass();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setJsonSchema(String jsonSchema);

      abstract Builder setCompressionCodec(CompressionCodecName compressionCodec);

      abstract Builder setConfiguration(SerializableConfiguration configuration);

      abstract Builder setRowGroupSize(int rowGroupSize);

      abstract Builder setAvroDataModelClass(Class<? extends GenericData> modelClass);

      abstract Sink build();
    }

    /** Specifies compression codec. By default, CompressionCodecName.SNAPPY. */
    public Sink withCompressionCodec(CompressionCodecName compressionCodecName) {
      return toBuilder().setCompressionCodec(compressionCodecName).build();
    }

    /** Specify Hadoop configuration for ParquetWriter. */
    public Sink withConfiguration(Map<String, String> configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return toBuilder().setConfiguration(SerializableConfiguration.fromMap(configuration)).build();
    }

    /** Specify Hadoop configuration for ParquetWriter. */
    public Sink withConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
    }

    /** Specify row-group size; if not set or zero, a default is used by the underlying writer. */
    public Sink withRowGroupSize(int rowGroupSize) {
      checkArgument(rowGroupSize > 0, "rowGroupSize must be positive");
      return toBuilder().setRowGroupSize(rowGroupSize).build();
    }

    /**
     * Define the Avro data model; see {@link AvroParquetWriter.Builder#withDataModel(GenericData)}.
     */
    public Sink withAvroDataModel(GenericData model) {
      return toBuilder().setAvroDataModelClass(model.getClass()).build();
    }

    private transient @Nullable ParquetWriter<GenericRecord> writer;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      checkNotNull(getJsonSchema(), "Schema cannot be null");

      Schema schema = new Schema.Parser().parse(getJsonSchema());
      Class<? extends GenericData> modelClass = getAvroDataModelClass();

      BeamParquetOutputFile beamParquetOutputFile =
          new BeamParquetOutputFile(Channels.newOutputStream(channel));

      AvroParquetWriter.Builder<GenericRecord> builder =
          AvroParquetWriter.<GenericRecord>builder(beamParquetOutputFile)
              .withSchema(schema)
              .withCompressionCodec(getCompressionCodec())
              .withWriteMode(OVERWRITE)
              .withConf(SerializableConfiguration.newConfiguration(getConfiguration()))
              .withRowGroupSize(getRowGroupSize());
      if (modelClass != null) {
        try {
          builder.withDataModel(buildModelObject(modelClass));
        } catch (ReflectiveOperationException e) {
          throw new IOException(
              "Couldn't set the specified Avro data model " + modelClass.getName(), e);
        }
      }
      this.writer = builder.build();
    }

    @Override
    public void write(GenericRecord element) throws IOException {
      checkNotNull(writer, "Writer cannot be null");
      writer.write(element);
    }

    @Override
    public void flush() throws IOException {
      // the only way to completely flush the output is to call writer.close() here
      writer.close();
    }

    private static class BeamParquetOutputFile implements OutputFile {

      private final OutputStream outputStream;

      BeamParquetOutputFile(OutputStream outputStream) {
        this.outputStream = outputStream;
      }

      @Override
      public PositionOutputStream create(long blockSizeHint) {
        return new BeamOutputStream(outputStream);
      }

      @Override
      public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return new BeamOutputStream(outputStream);
      }

      @Override
      public boolean supportsBlockSize() {
        return false;
      }

      @Override
      public long defaultBlockSize() {
        return 0;
      }
    }

    private static class BeamOutputStream extends PositionOutputStream {
      private long position = 0;
      private final OutputStream outputStream;

      private BeamOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
      }

      @Override
      public long getPos() {
        return position;
      }

      @Override
      public void write(int b) throws IOException {
        position++;
        outputStream.write(b);
      }

      @Override
      public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
        position += len;
      }

      @Override
      public void flush() throws IOException {
        outputStream.flush();
      }

      @Override
      public void close() throws IOException {
        outputStream.close();
      }
    }
  }

  /** Returns a model object created using provided modelClass or null. */
  private static GenericData buildModelObject(@Nullable Class<? extends GenericData> modelClass)
      throws ReflectiveOperationException {
    return (modelClass == null) ? null : (GenericData) modelClass.getMethod("get").invoke(null);
  }

  /**
   * Passthrough function to provide seamless backward compatibility to ParquetIO's functionality.
   */
  @VisibleForTesting
  static final class GenericRecordPassthroughFn
      implements SerializableFunction<GenericRecord, GenericRecord> {

    private static final GenericRecordPassthroughFn singleton = new GenericRecordPassthroughFn();

    static GenericRecordPassthroughFn create() {
      return singleton;
    }

    @Override
    public GenericRecord apply(GenericRecord input) {
      return input;
    }

    /** Enforce singleton pattern, by disallowing construction with {@code new} operator. */
    private GenericRecordPassthroughFn() {}
  }

  /** Disallow construction of utility class. */
  private ParquetIO() {}
}
