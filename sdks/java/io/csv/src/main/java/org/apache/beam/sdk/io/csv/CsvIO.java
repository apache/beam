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
package org.apache.beam.sdk.io.csv;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.values.TypeDescriptors.rows;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Transforms for reading and writing CSV files. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CsvIO {

  static final String DEFAULT_FILENAME_SUFFIX = ".csv";

  /** Instantiates a {@link Write} for writing user types in CSV format. */
  public static <T> Write<T> write() {
    return new AutoValue_CsvIO_Write.Builder<T>().build();
  }

  /** Instantiates a {@link Write} for {@link Row}s in CSV format. */
  public static Write<Row> writeRows() {
    return new AutoValue_CsvIO_Write.Builder<Row>().build();
  }

  /** Implementation of {@link FileIO.Sink}. */
  @AutoValue
  public abstract static class Sink<T> implements FileIO.Sink<T> {

    public static <T> Builder<T> builder() {
      return new AutoValue_CsvIO_Sink.Builder<>();
    }

    /**
     * Not to be confused with the CSV header, it is content written to the top of every sharded
     * file prior to the header. In the example below, all the text proceeding the header
     * 'column1,column2,column3' is the preamble.
     *
     * <p><code>Fake company, Inc.
     * Lab experiment: abcdefg123456
     * Experiment date: 2022-12-05
     * Operator: John Doe
     * column1,column2,colum3
     * 1,2,3
     * 4,5,6
     * </code>
     */
    public Sink<T> withPreamble(String preamble) {
      return toBuilder().setPreamble(preamble).build();
    }

    private transient @Nullable PrintWriter writer;

    /**
     * Opens a {@link WritableByteChannel} for writing CSV files. Writes the {@link #getPreamble()}
     * if available followed by the {@link #getHeader()}.
     */
    @Override
    public void open(WritableByteChannel channel) throws IOException {
      writer =
          new PrintWriter(
              new BufferedWriter(new OutputStreamWriter(Channels.newOutputStream(channel), UTF_8)));
      checkNotNull(writer);
      if (getPreamble() != null) {
        writer.println(getPreamble());
        ;
      }
      writer.println(getHeader());
    }

    /** Serializes and writes the {@param element} to a file. */
    @Override
    public void write(T element) throws IOException {
      if (writer == null) {
        throw new IllegalStateException(
            String.format("%s writer is null", PrintWriter.class.getName()));
      }
      String line = getFormatFunction().apply(element);
      writer.println(line);
    }

    @Override
    public void flush() throws IOException {
      if (writer == null) {
        throw new IllegalStateException(
            String.format("%s writer is null", PrintWriter.class.getName()));
      }
      writer.flush();
    }

    /**
     * Not to be confused with the CSV header, it is content written to the top of every sharded
     * file prior to the header. In the example below, all the text proceeding the header
     * 'column1,column2,column3' is the preamble.
     *
     * <p><code>Fake company, Inc.
     * Lab experiment: abcdefg123456
     * Experiment date: 2022-12-05
     * Operator: John Doe
     * column1,column2,colum3
     * 1,2,3
     * 4,5,6
     * </code>
     */
    abstract @Nullable String getPreamble();

    /**
     * The column names of the CSV file written at the top line of each shard after the preamble, if
     * available. Named fields in header must conform to types listed in {@link
     * CsvUtils#VALID_FIELD_TYPE_SET}.
     */
    abstract String getHeader();

    /** A {@link SerializableFunction} for converting a {@param T} to a CSV formatted string. */
    abstract SerializableFunction<T, String> getFormatFunction();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      /**
       * Not to be confused with the CSV header, it is content written to the top of every sharded
       * file prior to the header. In the example below, all the text proceeding the header
       * 'column1,column2,column3' is the preamble.
       *
       * <p>Fake company, Inc. Lab experiment: abcdefg123456 Experiment date: 2022-12-05 Operator:
       * John Doe
       *
       * <p>column1,column2,column3 1,2,3 4,5,6
       */
      abstract Builder<T> setPreamble(String value);

      /**
       * The column names of the CSV file written at the top line of each shard after the preamble,
       * if available. Named fields in header must conform to types listed in {@link
       * CsvUtils#VALID_FIELD_TYPE_SET}.
       */
      abstract Builder<T> setHeader(String value);

      /** A {@link SerializableFunction} for converting a {@param T} to a CSV formatted string. */
      abstract Builder<T> setFormatFunction(SerializableFunction<T, String> value);

      abstract Sink<T> build();
    }
  }

  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone>
      implements HasDisplayData {

    /** Specifies a common prefix for all generated files. */
    public Write<T> to(String filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /** The {@link CSVFormat} of the destination CSV file data. */
    public Write<T> withCSVFormat(CSVFormat format) {
      return toBuilder().setCSVFormat(format).build();
    }

    /**
     * Not to be confused with the CSV header, it is content written to the top of every sharded
     * file prior to the header. In the example below, all the text proceeding the header
     * 'column1,column2,column3' is the preamble.
     *
     * <p><code>Fake company, Inc.
     * Lab experiment: abcdefg123456
     * Experiment date: 2022-12-05
     * Operator: John Doe
     * column1,column2,colum3
     * 1,2,3
     * 4,5,6
     * </code>
     */
    public Write<T> withPreamble(String preamble) {
      return toBuilder().setPreamble(preamble).build();
    }

    /** Specifies the {@link Compression} of all generated shard files. */
    public Write<T> withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    /**
     * Specifies to use a given fixed number of shards per window. See {@link
     * FileIO.Write#withNumShards(int)} for details.
     */
    public Write<T> withNumShards(Integer numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    /**
     * Specifies a directory into which all temporary files will be placed. See {@link
     * FileIO.Write#withTempDirectory(String)}.
     */
    public Write<T> withTempDirectory(ResourceId value) {
      return toBuilder().setTempDirectory(value).build();
    }

    abstract @Nullable String getFilenamePrefix();

    abstract CSVFormat getCSVFormat();

    abstract @Nullable String getPreamble();

    abstract @Nullable Compression getCompression();

    abstract @Nullable Integer getNumShards();

    abstract String getFilenameSuffix();

    abstract @Nullable ResourceId getTempDirectory();

    abstract @Nullable List<String> getSchemaFields();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setFilenamePrefix(String value);

      abstract Builder<T> setCSVFormat(CSVFormat value);

      abstract Optional<CSVFormat> getCSVFormat();

      abstract Builder<T> setPreamble(String value);

      abstract Builder<T> setCompression(Compression value);

      abstract Builder<T> setNumShards(Integer value);

      abstract Builder<T> setFilenameSuffix(String value);

      abstract Optional<String> getFilenameSuffix();

      abstract Builder<T> setTempDirectory(ResourceId value);

      abstract Builder<T> setSchemaFields(List<String> value);

      abstract Write<T> autoBuild();

      final Write<T> build() {
        if (!getCSVFormat().isPresent()) {
          setCSVFormat(CSVFormat.DEFAULT);
        }

        if (!getFilenameSuffix().isPresent()) {
          setFilenameSuffix(DEFAULT_FILENAME_SUFFIX);
        }

        return autoBuild();
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      if (getFilenamePrefix() != null) {
        builder.add(DisplayData.item("filenamePrefix", getFilenamePrefix()));
      }
      if (getCSVFormat() != null) {
        builder.add(DisplayData.item("csvFormat", getCSVFormat().toString()));
      }
      if (getPreamble() != null) {
        builder.add(DisplayData.item("preamble", getPreamble()));
      }
      if (getCompression() != null) {
        builder.add(DisplayData.item("compression", getCompression().name()));
      }
      if (getNumShards() != null) {
        builder.add(DisplayData.item("numShards", getNumShards()));
      }

      builder.add(DisplayData.item("filenameSuffix", getFilenameSuffix()));

      if (getTempDirectory() != null) {
        builder.add(DisplayData.item("tempDirectory", getTempDirectory().getFilename()));
      }
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (!input.hasSchema()) {
        throw new IllegalArgumentException(
            String.format(
                "%s requires an input Schema. Note that only Row or user classes are supported. Consider using TextIO or FileIO directly when writing primitive types",
                Write.class.getName()));
      }

      Schema schema = input.getSchema();
      SerializableFunction<T, Row> toRowFn = input.getToRowFunction();
      PCollection<Row> rows =
          input.apply(MapElements.into(rows()).via(toRowFn)).setRowSchema(schema);
      rows.apply("writeRowsToCsv", buildFileIOWrite().via(buildSink(schema)));
      return PDone.in(input.getPipeline());
    }

    /**
     * Builds a header using {@link CSVFormat} based on either a {@link Schema#sorted()} {@link
     * Schema#getFieldNames()} if {@link #getSchemaFields()} is null or {@link #getSchemaFields()}.
     * {@link Schema} {@link Schema.Field}s must conform to types listed in {@link
     * CsvUtils#VALID_FIELD_TYPE_SET}.
     */
    String buildHeader(Schema schema) {
      if (getSchemaFields() != null) {
        return CsvUtils.buildHeaderFrom(getSchemaFields(), getCSVFormat());
      }
      return CsvUtils.buildHeaderFrom(schema.sorted(), getCSVFormat());
    }

    /** Builds a {@link Sink} for writing {@link Row} serialized using {@link CSVFormat}. */
    Sink<Row> buildSink(Schema schema) {
      List<String> schemaFields = null;
      if (getSchemaFields() != null) {
        schemaFields = getSchemaFields();
      }
      return Sink.<Row>builder()
          .setPreamble(getPreamble())
          .setHeader(buildHeader(schema))
          .setFormatFunction(
              CsvUtils.getRowToCsvStringFunction(schema, getCSVFormat(), schemaFields))
          .build();
    }

    /** Builds a {@link FileIO.Write} with a {@link Sink}. */
    FileIO.Write<Void, Row> buildFileIOWrite() {
      checkArgument(getFilenamePrefix() != null, "to() is required");

      ResourceId prefix =
          FileSystems.matchNewResource(getFilenamePrefix(), false /* isDirectory */);

      FileIO.Write<Void, Row> write =
          FileIO.<Row>write()
              .to(prefix.getCurrentDirectory().toString())
              .withPrefix(Objects.requireNonNull(prefix.getFilename()));

      if (getCompression() != null) {
        write = write.withCompression(getCompression());
      }

      if (getNumShards() != null) {
        write = write.withNumShards(getNumShards());
      }

      if (getFilenameSuffix() != null) {
        write = write.withSuffix(getFilenameSuffix());
      }

      if (getTempDirectory() != null) {
        write = write.withTempDirectory(Objects.requireNonNull(getTempDirectory().getFilename()));
      }

      return write;
    }
  }
}
