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
import static org.apache.hadoop.crypto.key.kms.KMSClientProvider.checkNotNull;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.joda.time.Duration;

/**
 * IO to read and write Parquet files.
 *
 * <h3>Reading Parquet files</h3>
 *
 * <p>{@link ParquetIO} source returns a {@link PCollection} for
 * Parquet files. The elements in the {@link PCollection} are Avro {@link GenericRecord}.
 *
 * <p>To configure the {@link Read}, you have to provide the file patterns (from) of the
 * Parquet files and the Avro schema.
 *
 * <p>For example:
 *
 * <pre>{@code
 *  pipeline.apply(ParquetIO.read().from("/foo/bar").withSchema(schema))
 *  ...
 * }
 * </pre>
 *
 * <p>As {@link Read} is based on {@link FileIO}, it supports any filesystem (hdfs, ...).
 *
 * <h3>Writing Parquet files</h3>
 *
 * <p>{@link Write} allows you to write a {@link PCollection} of {@link GenericRecord} into a
 * Parquet file.
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

  /**
   * Reads {@link GenericRecord} from a Parquet file (or multiple Parquet files matching
   * the pattern).
   */
  public static Read read() {
    return new AutoValue_ParquetIO_Read.Builder().setHintMatchesManyFiles(false)
        .setMatchConfiguration(FileIO.MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .build();
  }

  /**
   * Like {@link #read()}, but reads each filepattern in the input {@link PCollection}.
   */
  public static ReadAll readAll() {
    return new AutoValue_ParquetIO_ReadAll.Builder()
        .setMatchConfiguration(FileIO.MatchConfiguration
            .create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .build();
  }

  /**
   * Like {@link #read()}, but reads each file in a {@link PCollection}
   * of {@link org.apache.beam.sdk.io.FileIO.ReadableFile},
   * which allows more flexible usage.
   */
  public static ReadFiles readFiles() {
    return new AutoValue_ParquetIO_ReadFiles.Builder().build();
  }

  /**
   * Writes a {@link PCollection} to an Parquet file.
   */
  public static Write write() {
    return new AutoValue_ParquetIO_Write.Builder().build();
  }

  /**
   * Implementation of {@link #read()}.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<GenericRecord>> {

    @Nullable abstract ValueProvider<String> filepattern();
    abstract FileIO.MatchConfiguration matchConfiguration();
    @Nullable abstract Schema schema();
    abstract boolean hintMatchesManyFiles();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);
      abstract Builder setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration);
      abstract Builder setSchema(Schema schema);
      abstract Builder setHintMatchesManyFiles(boolean hintManyFiles);

      abstract Read build();
    }

    /**
     * Reads from the given filename or filepattern.
     *
     * <p>If it is known that the filepattern will match a very large number of files (at least tens
     * of thousands), use {@link #withHintMatchesManyFiles} for better performance and scalability.
     */
    public Read from(ValueProvider<String> filepattern) {
      return builder().setFilepattern(filepattern).build();
    }

    /** Like {@link #from(ValueProvider)}. */
    public Read from(String filepattern) {
      return from(ValueProvider.StaticValueProvider.of(filepattern));
    }

    /**
     * Schema of the record in the Parquet file.
     */
    public Read withSchema(Schema schema) {
      return builder().setSchema(schema).build();
    }

    /** Sets the {@link FileIO.MatchConfiguration}. */
    public Read withMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
      return builder().setMatchConfiguration(matchConfiguration).build();
    }

    /** Configures whether or not a filepattern matching no files is allowed. */
    public Read withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(matchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /**
     * Continuously watches for new files matching the filepattern, polling it at the given
     * interval, until the given termination condition is reached. The returned {@link PCollection}
     * is unbounded.
     *
     * <p>This works only in runners supporting {@link Experimental.Kind#SPLITTABLE_DO_FN}.
     */
    @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
    public Read watchForNewFiles(
        Duration pollInterval, Watch.Growth.TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
          matchConfiguration().continuously(pollInterval, terminationCondition));
    }

    /**
     * Hints that the filepattern specified in {@link #from(String)} matches a very large number of
     * files.
     *
     * <p>This hint may cause a runner to execute the transform differently, in a way that improves
     * performance for this case, but it may worsen performance if the filepattern matches only a
     * small number of files (e.g., in a runner that supports dynamic work rebalancing, it will
     * happen less efficiently within individual files).
     */
    public Read withHintMatchesManyFiles() {
      return builder().setHintMatchesManyFiles(true).build();
    }

    @Override
    public PCollection<GenericRecord> expand(PBegin input) {
      checkNotNull(filepattern(), "filepattern");
      checkNotNull(schema(), "schema");

      ReadAll readAll = readAll().withMatchConfiguration(matchConfiguration()).withSchema(schema());
      return input
          .apply("Create filepattern", Create.ofProvider(filepattern(),
              StringUtf8Coder.of()))
          .apply("Via ReadAll", readAll);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("filePattern", filepattern()).withLabel("Input File Pattern"))
          .include("matchConfiguration", matchConfiguration());
    }
  }

  /**
   * Implementation of {@link #readAll()}.
   */
  @AutoValue
  public abstract static class ReadAll extends PTransform<PCollection<String>,
      PCollection<GenericRecord>> {

    abstract FileIO.MatchConfiguration matchConfiguration();
    @Nullable abstract Schema schema();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration);
      abstract Builder setSchema(Schema schema);

      abstract ReadAll build();
    }

    /**
     * Sets the {@link org.apache.beam.sdk.io.FileIO.MatchConfiguration}.
     */
    public ReadAll withMatchConfiguration(FileIO.MatchConfiguration configuration) {
      return builder().setMatchConfiguration(configuration).build();
    }

    /**
     * Sets the {@link EmptyMatchTreatment}.
     */
    public ReadAll withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(matchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /**
     * Sets the schema of the records.
     */
    public ReadAll withSchema(Schema schema) {
      return builder().setSchema(schema).build();
    }

    @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
    public ReadAll watchForNewFiles(
        Duration pollInterval, Watch.Growth.TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
          matchConfiguration().continuously(pollInterval, terminationCondition));
    }

    @Override
    public PCollection<GenericRecord> expand(PCollection<String> input) {
      checkNotNull(schema(), "schema");
      return input
          .apply(FileIO.matchAll().withConfiguration(matchConfiguration()))
          .apply(FileIO.readMatches())
          .apply(readFiles().withSchema(schema()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.include("matchConfiguration", matchConfiguration());
    }

  }

  /**
   * Implementation of {@link #readFiles()}.
   */
  @AutoValue
  public abstract static class ReadFiles extends PTransform<PCollection<FileIO.ReadableFile>,
      PCollection<GenericRecord>> {

    @Nullable abstract Schema schema();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSchema(Schema schema);
      abstract ReadFiles build();
    }
    /**
     * Define the Avro schema of the record to read from the Parquet file.
     */
    public ReadFiles withSchema(Schema schema) {
      checkArgument(schema != null,
          "schema can not be null");
      return builder().setSchema(schema).build();
    }

    @Override
    public PCollection<GenericRecord> expand(PCollection<FileIO.ReadableFile> input) {
      return input
          .apply(ParDo.of(new ReadFn()))
          .setCoder(AvroCoder.of(schema()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("schema", schema().toString()));
    }

    static class ReadFn extends DoFn<FileIO.ReadableFile, GenericRecord> {

      @ProcessElement
      public void processElement(ProcessContext processContext) throws Exception {
        Path path = new Path(processContext.element().getMetadata().resourceId().toString());
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
