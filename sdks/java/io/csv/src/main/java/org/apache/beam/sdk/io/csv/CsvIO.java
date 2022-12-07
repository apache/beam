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

import com.google.auto.value.AutoValue;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;
import java.util.Optional;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.JavaFieldSchema.JavaFieldTypeSupplier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
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

  /** Implementation of {@link FileIO.Sink}. */
  @AutoValue
  public abstract static class Sink<T> implements FileIO.Sink<T> {

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

    /** The {@link CSVFormat} of the written CSV file. */
    public Sink<T> withCSVFormat(CSVFormat csvFormat) {
      return toBuilder().setCSVFormat(csvFormat).build();
    }

    private transient @Nullable PrintWriter writer;
    private SimpleFunction<T, Row> elementToRowFn;
    private SimpleFunction<Row, String> rowToStringFn;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      writer =
          new PrintWriter(
              new BufferedWriter(new OutputStreamWriter(Channels.newOutputStream(channel), UTF_8)));
      if (getPreamble() != null) {
        writer.println(getPreamble());
      }

    }

    @Override
    public void write(T element) throws IOException {}

    @Override
    public void flush() throws IOException {
      if (writer == null) {
        throw new IllegalStateException(String.format("%s writer is null", PrintWriter.class.getName()));
      }
      writer.flush();
    }

    SimpleFunction<T, String> getOrCreateUserTypeToRowFunction() {
      return DEFA
    }

    abstract @Nullable String getPreamble();

    abstract CSVFormat getCSVFormat();

    abstract Class<T> getElementClass();

    abstract Schema getSchema();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setPreamble(String value);

      abstract Builder<T> setCSVFormat(CSVFormat value);

      abstract Optional<CSVFormat> getCSVFormat();

      abstract Builder<T> setElementClass(Class<T> value);

      abstract Sink<T> autoBuild();

      final Sink<T> build() {

        if (!getCSVFormat().isPresent()) {
          setCSVFormat(CSVFormat.DEFAULT);
        }

        return autoBuild();
      }
    }
  }

  public static class Write<T> extends PTransform<PCollection<T>, PDone> implements HasDisplayData {
    @Override
    public PDone expand(PCollection<T> input) {
      return null;
    }
  }
}
