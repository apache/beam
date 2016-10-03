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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

/**
 * IO to read and write Parquet files.
 *
 * <h3>Reading Parquet files</h3>
 *
 * <p>{@link ParquetIO} source returns a {@link PCollection} for
 * Parquet files. The elements in the {@link PCollection} are Avro {@link GenericRecord}.
 *
 * <p>To configure the {@link Read}, you have to provide the location (path) of the Parquet file
 * and the Avro schema.
 *
 * <p>For example:
 *
 * <pre>{@code
 *  pipeline.apply(ParquetIO.read().withPath("/foo/bar").withSchema(schema))
 *  ...
 * }
 * </pre>
 *
 * <h3>Writing Parquet files</h3>
 *
 * <p>{@link Write} allows you to write a {@link PCollection} of {@link GenericRecord} into a
 * Parquet file.
 *
 * <p>Like for the {@link Read}, you have to provide the location (path) of the Parquet file and
 * the Avro schema.
 *
 * <p>For example:
 *
 * <pre>{@code
 *  pipeline
 *    .apply(...) // PCollection<GenericRecord>
 *    .apply(ParquetIO.write().withPath("/foo/bar").withSchema(schema));
 * }</pre>
 */
public class ParquetIO {

  public static Read read() {
    return new AutoValue_ParquetIO_Read.Builder().build();
  }

  public static Write write() {
    return new AutoValue_ParquetIO_Write.Builder().build();
  }

  /**
   * A {@link PTransform} to read from Parquet file.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<GenericRecord>> {

    @Nullable abstract String path();
    @Nullable abstract Schema schema();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setPath(String path);
      abstract Builder setSchema(Schema schema);
      abstract Read build();
    }

    /**
     * Define the location of the Parquet file you want to read.
     */
    public Read withPath(String path) {
      checkArgument(path != null, "ParquetIO.read().withPath(path) called with null path");
      return builder().setPath(path).build();
    }

    /**
     * Define the Avro schema of the record to read from the Parquet file.
     */
    public Read withSchema(Schema schema) {
      checkArgument(schema != null,
          "ParquetIO.read().withSchema(schema) called with null schema");
      return builder().setSchema(schema).build();
    }

    @Override
    public PCollection<GenericRecord> expand(PBegin input) {
      return input
          .apply(Create.of(path()))
          .apply(ParDo.of(new ReadFn()))
          .setCoder(AvroCoder.of(schema()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("path", path()));
      builder.add(DisplayData.item("schema", schema().toString()));
    }

    static class ReadFn extends DoFn<String, GenericRecord> {

      @ProcessElement
      public void processElement(ProcessContext processContext) throws Exception {
        Path path = new Path(processContext.element());
        try (ParquetReader<GenericRecord> reader =
                 AvroParquetReader.<GenericRecord>builder(path).build()) {
          GenericRecord read;
          while ((read = reader.read()) != null) {
            processContext.output(read);
          }
        }
      }

    }

  }

  /**
   * {@link PTransform} writing a {@link PCollection} of {@link GenericRecord} into a Parquet file.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<GenericRecord>, PDone> {

    @Nullable abstract String path();
    @Nullable abstract String schema();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setPath(String path);
      abstract Builder setSchema(String schema);
      abstract Write build();
    }

    /**
     * Define the location (path) of the Parquet file to write.
     */
    public Write withPath(String path) {
      checkArgument(path != null, "ParquetIO.write().withPath(path) called with null path");
      return builder().setPath(path).build();
    }

    /**
     * Define the Avro schema of the Avro {@link GenericRecord} to be written in the Parquet file.
     */
    public Write withSchema(String schema) {
      checkArgument(schema != null,
          "ParquetIO.write().withSchema(schema) called with null schema");
      return builder().setSchema(schema).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("path", path()));
      builder.add(DisplayData.item("schema", schema()));
    }

    @Override
    public PDone expand(PCollection<GenericRecord> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    static class WriteFn extends DoFn<GenericRecord, Void> {

      private Write spec;
      private transient ParquetWriter<GenericRecord> writer;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        Path path = new Path(spec.path());
        Schema schema = new Schema.Parser().parse(spec.schema());
        writer = AvroParquetWriter.<GenericRecord>builder(path).withSchema(schema).build();
      }

      @ProcessElement
      public void processElement(ProcessContext processContext) throws Exception {
        GenericRecord record = processContext.element();
        writer.write(record);
      }

      @Teardown
      public void teardown() throws Exception {
        writer.close();
      }

    }

  }

}
