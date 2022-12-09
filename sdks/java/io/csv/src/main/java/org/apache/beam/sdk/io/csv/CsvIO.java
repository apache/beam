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
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Transforms for reading and writing CSV files. */
@SuppressWarnings({
  "unused",
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CsvIO {

  public static <T> Write<T> write() {
    return new AutoValue_CsvIO_Write.Builder<T>().build();
  }

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
     * <p>Fake company, Inc. Lab experiment: abcdefg123456 Experiment date: 2022-12-05 Operator:
     * John Doe
     *
     * <p>column1,column2,column3 1,2,3 4,5,6
     */
    public Sink<T> withPreamble(String preamble) {
      return toBuilder().setPreamble(preamble).build();
    }

    private transient @Nullable PrintWriter writer;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      writer =
          new PrintWriter(
              new BufferedWriter(new OutputStreamWriter(Channels.newOutputStream(channel), UTF_8)));
      if (getPreamble() != null) {
        writer.println(getPreamble());
      }
      writer.println(getHeader());
    }

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

    abstract @Nullable String getPreamble();

    abstract String getHeader();

    abstract SimpleFunction<T, String> getFormatFunction();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setPreamble(String value);

      abstract Builder<T> setHeader(String value);

      abstract Builder<T> setFormatFunction(SimpleFunction<T, String> value);

      abstract Sink<T> build();
    }
  }

  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone>
      implements HasDisplayData {

    public Write<T> to(String filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    public Write<T> withCSVFormat(CSVFormat format) {
      return toBuilder().setCSVFormat(format).build();
    }

    public Write<T> withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    public Write<T> withNumShards(Integer numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    abstract @Nullable String getFilenamePrefix();

    abstract CSVFormat getCSVFormat();

    abstract @Nullable String getPreamble();

    abstract @Nullable Compression getCompression();

    abstract @Nullable Integer getNumShards();

    abstract @Nullable String getFilenameSuffix();

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

      abstract Builder<T> setTempDirectory(ResourceId value);

      abstract Builder<T> setSchemaFields(List<String> value);

      abstract Write<T> autoBuild();

      final Write<T> build() {
        if (!getCSVFormat().isPresent()) {
          setCSVFormat(CSVFormat.DEFAULT);
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
        builder.add(DisplayData.item("compression", DisplayData.Type.JAVA_CLASS, getCompression()));
      }
      if (getNumShards() != null) {
        builder.add(DisplayData.item("numShards", getNumShards()));
      }
      if (getFilenameSuffix() != null) {
        builder.add(DisplayData.item("filenameSuffix", getFilenameSuffix()));
      }
      if (getTempDirectory() != null) {
        builder.add(
            DisplayData.item("tempDirectory", DisplayData.Type.JAVA_CLASS, getTempDirectory()));
      }
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (!input.hasSchema()) {
        throw new IllegalArgumentException(
            String.format("%s requires an input Schema", Write.class.getName()));
      }

      Schema schema = input.getSchema();
      SerializableFunction<T, Row> toRowFn = input.getToRowFunction();
      PCollection<Row> rows =
          input.apply(MapElements.into(rows()).via(toRowFn)).setRowSchema(schema);
      rows.apply("writeRowsToCsv", buildFileIOWrite().via(buildSink(schema)));
      return PDone.in(input.getPipeline());
    }

    String buildHeader(Schema schema) {
      if (getSchemaFields() != null) {
        return CsvUtils.buildHeaderFrom(getSchemaFields(), getCSVFormat());
      }
      return CsvUtils.buildHeaderFrom(schema.sorted(), getCSVFormat());
    }

    Sink<Row> buildSink(Schema schema) {
      List<String> schemaFields = null;
      String header = null;
      if (getSchemaFields() != null) {
        schemaFields = getSchemaFields();
      }
      return Sink.<Row>builder()
          .setHeader(buildHeader(schema))
          .setFormatFunction(
              CsvUtils.getRowToCsvStringFunction(schema, getCSVFormat(), schemaFields))
          .build();
    }

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
