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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider.BigQueryStorageWriteApiSchemaTransformConfiguration;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for BigQuery Storage Write API jobs
 * configured via {@link BigQueryStorageWriteApiSchemaTransformConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Experimental(Kind.SCHEMAS)
@AutoService(SchemaTransformProvider.class)
public class BigQueryStorageWriteApiSchemaTransformProvider
    extends TypedSchemaTransformProvider<BigQueryStorageWriteApiSchemaTransformConfiguration> {
  private static final Integer DEFAULT_TRIGGER_FREQUENCY_SECS = 5;
  private static final Duration DEFAULT_TRIGGERING_FREQUENCY =
      Duration.standardSeconds(DEFAULT_TRIGGER_FREQUENCY_SECS);
  private static final String INPUT_ROWS_TAG = "input";
  private static final String OUTPUT_ERRORS_TAG = "errors";

  @Override
  protected Class<BigQueryStorageWriteApiSchemaTransformConfiguration> configurationClass() {
    return BigQueryStorageWriteApiSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(
      BigQueryStorageWriteApiSchemaTransformConfiguration configuration) {
    return new BigQueryStorageWriteApiSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return String.format("beam:schematransform:org.apache.beam:bigquery_storage_write:v1");
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_ROWS_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_ERRORS_TAG);
  }

  /** Configuration for writing to BigQuery with Storage Write API. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class BigQueryStorageWriteApiSchemaTransformConfiguration {
    static final Map<String, CreateDisposition> CREATE_DISPOSITIONS =
        ImmutableMap.<String, CreateDisposition>builder()
            .put(CreateDisposition.CREATE_IF_NEEDED.name(), CreateDisposition.CREATE_IF_NEEDED)
            .put(CreateDisposition.CREATE_NEVER.name(), CreateDisposition.CREATE_NEVER)
            .build();

    static final Map<String, WriteDisposition> WRITE_DISPOSITIONS =
        ImmutableMap.<String, WriteDisposition>builder()
            .put(WriteDisposition.WRITE_TRUNCATE.name(), WriteDisposition.WRITE_TRUNCATE)
            .put(WriteDisposition.WRITE_EMPTY.name(), WriteDisposition.WRITE_EMPTY)
            .put(WriteDisposition.WRITE_APPEND.name(), WriteDisposition.WRITE_APPEND)
            .build();

    public void validate() {
      String invalidConfigMessage = "Invalid BigQuery Storage Write configuration: ";

      // validate output table spec
      checkArgument(
          !Strings.isNullOrEmpty(this.getTable()),
          invalidConfigMessage + "Table spec for a BigQuery Write must be specified.");
      checkNotNull(BigQueryHelpers.parseTableSpec(this.getTable()));

      // validate create and write dispositions
      if (!Strings.isNullOrEmpty(this.getCreateDisposition())) {
        checkArgument(
            CREATE_DISPOSITIONS.get(this.getCreateDisposition().toUpperCase()) != null,
            invalidConfigMessage
                + "Invalid create disposition was specified. Available dispositions are: ",
            CREATE_DISPOSITIONS.keySet());
      }
      if (!Strings.isNullOrEmpty(this.getWriteDisposition())) {
        checkNotNull(
            WRITE_DISPOSITIONS.get(this.getWriteDisposition().toUpperCase()),
            invalidConfigMessage
                + "Invalid write disposition was specified. Available dispositions are: ",
            WRITE_DISPOSITIONS.keySet());
      }
    }

    /**
     * Instantiates a {@link BigQueryStorageWriteApiSchemaTransformConfiguration.Builder} instance.
     */
    public static Builder builder() {
      return new AutoValue_BigQueryStorageWriteApiSchemaTransformProvider_BigQueryStorageWriteApiSchemaTransformConfiguration
          .Builder();
    }

    @SchemaFieldDescription(
        "The bigquery table to write to. Format: [${PROJECT}:]${DATASET}.${TABLE}")
    public abstract String getTable();

    @SchemaFieldDescription(
        "Optional field that specifies whether the job is allowed to create new tables. "
            + "The following values are supported: CREATE_IF_NEEDED (the job may create the table), CREATE_NEVER ("
            + "the job must fail if the table does not exist already).")
    @Nullable
    public abstract String getCreateDisposition();

    @SchemaFieldDescription(
        "Specifies the action that occurs if the destination table already exists. "
            + "The following values are supported: "
            + "WRITE_TRUNCATE (overwrites the table data), "
            + "WRITE_APPEND (append the data to the table), "
            + "WRITE_EMPTY (job must fail if the table is not empty).")
    @Nullable
    public abstract String getWriteDisposition();

    @SchemaFieldDescription(
        "Determines how often to 'commit' progress into BigQuery. Default is every 5 seconds.")
    @Nullable
    public abstract Long getTriggeringFrequencySeconds();

    @SchemaFieldDescription(
        "This option enables lower latency for insertions to BigQuery but may ocassionally "
            + "duplicate data elements.")
    @Nullable
    public abstract Boolean getUseAtLeastOnceSemantics();

    /** Builder for {@link BigQueryStorageWriteApiSchemaTransformConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTable(String table);

      public abstract Builder setCreateDisposition(String createDisposition);

      public abstract Builder setWriteDisposition(String writeDisposition);

      public abstract Builder setTriggeringFrequencySeconds(Long seconds);

      public abstract Builder setUseAtLeastOnceSemantics(Boolean use);

      /** Builds a {@link BigQueryStorageWriteApiSchemaTransformConfiguration} instance. */
      public abstract BigQueryStorageWriteApiSchemaTransformProvider
              .BigQueryStorageWriteApiSchemaTransformConfiguration
          build();
    }
  }

  /**
   * A {@link SchemaTransform} for BigQuery Storage Write API, configured with {@link
   * BigQueryStorageWriteApiSchemaTransformConfiguration} and instantiated by {@link
   * BigQueryStorageWriteApiSchemaTransformProvider}.
   */
  private static class BigQueryStorageWriteApiSchemaTransform implements SchemaTransform {
    private final BigQueryStorageWriteApiSchemaTransformConfiguration configuration;

    BigQueryStorageWriteApiSchemaTransform(
        BigQueryStorageWriteApiSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new BigQueryStorageWriteApiPCollectionRowTupleTransform(configuration);
    }
  }

  static class BigQueryStorageWriteApiPCollectionRowTupleTransform
      extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {
    private final BigQueryStorageWriteApiSchemaTransformConfiguration configuration;
    private BigQueryServices testBigQueryServices = null;

    BigQueryStorageWriteApiPCollectionRowTupleTransform(
        BigQueryStorageWriteApiSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @VisibleForTesting
    public void setBigQueryServices(BigQueryServices testBigQueryServices) {
      this.testBigQueryServices = testBigQueryServices;
    }

    // A generic counter for PCollection of Row. Will be initialized with the given
    // name argument. Performs element-wise counter of the input PCollection.
    private static class ElementCounterFn extends DoFn<Row, Row> {
      private Counter bqGenericElementCounter;
      private Long elementsInBundle = 0L;

      ElementCounterFn(String name) {
        this.bqGenericElementCounter =
            Metrics.counter(BigQueryStorageWriteApiPCollectionRowTupleTransform.class, name);
      }

      @ProcessElement
      public void process(ProcessContext c) {
        this.elementsInBundle += 1;
        c.output(c.element());
      }

      @FinishBundle
      public void finish(FinishBundleContext c) {
        this.bqGenericElementCounter.inc(this.elementsInBundle);
        this.elementsInBundle = 0L;
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      // Check that the input exists
      checkArgument(input.has(INPUT_ROWS_TAG), "Missing expected input tag: %s", INPUT_ROWS_TAG);
      PCollection<Row> inputRows = input.get(INPUT_ROWS_TAG);

      BigQueryIO.Write<Row> write = createStorageWriteApiTransform();

      if (inputRows.isBounded() == IsBounded.UNBOUNDED) {
        Long triggeringFrequency = configuration.getTriggeringFrequencySeconds();
        write =
            write
                .withAutoSharding()
                .withTriggeringFrequency(
                    (triggeringFrequency == null || triggeringFrequency <= 0)
                        ? DEFAULT_TRIGGERING_FREQUENCY
                        : Duration.standardSeconds(triggeringFrequency));
      }

      Schema inputSchema = inputRows.getSchema();
      WriteResult result =
          inputRows
              .apply(
                  "element-count", ParDo.of(new ElementCounterFn("BigQuery-write-element-counter")))
              .setRowSchema(inputSchema)
              .apply(write);

      Schema errorSchema =
          Schema.of(
              Field.of("failed_row", FieldType.STRING),
              Field.of("error_message", FieldType.STRING));

      // Errors consisting of failed rows along with their error message
      PCollection<Row> errorRows =
          result
              .getFailedStorageApiInserts()
              .apply(
                  "Extract Errors",
                  MapElements.into(TypeDescriptor.of(Row.class))
                      .via(
                          (storageError) ->
                              Row.withSchema(errorSchema)
                                  .withFieldValue("error_message", storageError.getErrorMessage())
                                  .withFieldValue("failed_row", storageError.getRow().toString())
                                  .build()))
              .setRowSchema(errorSchema);

      PCollection<Row> errorOutput =
          errorRows
              .apply("error-count", ParDo.of(new ElementCounterFn("BigQuery-write-error-counter")))
              .setRowSchema(errorSchema);

      return PCollectionRowTuple.of(OUTPUT_ERRORS_TAG, errorOutput);
    }

    BigQueryIO.Write<Row> createStorageWriteApiTransform() {
      Method writeMethod =
          configuration.getUseAtLeastOnceSemantics() != null
                  && configuration.getUseAtLeastOnceSemantics()
              ? Method.STORAGE_API_AT_LEAST_ONCE
              : Method.STORAGE_WRITE_API;

      BigQueryIO.Write<Row> write =
          BigQueryIO.<Row>write()
              .to(configuration.getTable())
              .withMethod(writeMethod)
              .useBeamSchema()
              .withFormatFunction(BigQueryUtils.toTableRow())
              .withWriteDisposition(WriteDisposition.WRITE_APPEND);

      if (!Strings.isNullOrEmpty(configuration.getCreateDisposition())) {
        CreateDisposition createDisposition =
            BigQueryStorageWriteApiSchemaTransformConfiguration.CREATE_DISPOSITIONS.get(
                configuration.getCreateDisposition());
        write = write.withCreateDisposition(createDisposition);
      }
      if (!Strings.isNullOrEmpty(configuration.getWriteDisposition())) {
        WriteDisposition writeDisposition =
            BigQueryStorageWriteApiSchemaTransformConfiguration.WRITE_DISPOSITIONS.get(
                configuration.getWriteDisposition());
        write = write.withWriteDisposition(writeDisposition);
      }

      if (this.testBigQueryServices != null) {
        write = write.withTestServices(testBigQueryServices);
      }

      return write;
    }
  }
}
