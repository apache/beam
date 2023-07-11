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

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider.BigQueryStorageWriteApiSchemaTransformConfiguration;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@AutoService(SchemaTransformProvider.class)
public class BigQueryStorageWriteApiSchemaTransformProvider
    extends TypedSchemaTransformProvider<BigQueryStorageWriteApiSchemaTransformConfiguration> {
  private static final Logger LOG =
      LoggerFactory.getLogger(BigQueryStorageWriteApiSchemaTransformProvider.class);
  private static final Integer DEFAULT_TRIGGER_FREQUENCY_SECS = 5;
  private static final Duration DEFAULT_TRIGGERING_FREQUENCY =
      Duration.standardSeconds(DEFAULT_TRIGGER_FREQUENCY_SECS);
  private static final String INPUT_ROWS_TAG = "input";
  private static final String FAILED_ROWS_TAG = "FailedRows";
  private static final String FAILED_ROWS_WITH_ERRORS_TAG = "FailedRowsWithErrors";

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
    return Arrays.asList(FAILED_ROWS_TAG, FAILED_ROWS_WITH_ERRORS_TAG, "errors");
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
        checkNotNull(
            CREATE_DISPOSITIONS.get(this.getCreateDisposition().toUpperCase()),
            invalidConfigMessage
                + "Invalid create disposition (%s) was specified. Available dispositions are: %s",
            this.getCreateDisposition(),
            CREATE_DISPOSITIONS.keySet());
      }
      if (!Strings.isNullOrEmpty(this.getWriteDisposition())) {
        checkNotNull(
            WRITE_DISPOSITIONS.get(this.getWriteDisposition().toUpperCase()),
            invalidConfigMessage
                + "Invalid write disposition (%s) was specified. Available dispositions are: %s",
            this.getWriteDisposition(),
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

    @SchemaFieldDescription(
        "This option enables using a dynamically determined number of shards to write to "
            + "BigQuery. Only applicable to unbounded data.")
    @Nullable
    public abstract Boolean getAutoSharding();

    /** Builder for {@link BigQueryStorageWriteApiSchemaTransformConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setTable(String table);

      public abstract Builder setCreateDisposition(String createDisposition);

      public abstract Builder setWriteDisposition(String writeDisposition);

      public abstract Builder setTriggeringFrequencySeconds(Long seconds);

      public abstract Builder setUseAtLeastOnceSemantics(Boolean use);

      public abstract Builder setAutoSharding(Boolean autoSharding);

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
  protected static class BigQueryStorageWriteApiSchemaTransform extends SchemaTransform {

    private BigQueryServices testBigQueryServices = null;
    private final BigQueryStorageWriteApiSchemaTransformConfiguration configuration;

    BigQueryStorageWriteApiSchemaTransform(
        BigQueryStorageWriteApiSchemaTransformConfiguration configuration) {
      configuration.validate();
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
            Metrics.counter(BigQueryStorageWriteApiSchemaTransform.class, name);
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
        Boolean autoSharding = configuration.getAutoSharding();
        write =
            write.withTriggeringFrequency(
                (triggeringFrequency == null || triggeringFrequency <= 0)
                    ? DEFAULT_TRIGGERING_FREQUENCY
                    : Duration.standardSeconds(triggeringFrequency));
        // use default value true for autoSharding if not configured for STORAGE_WRITE_API
        if (autoSharding == null || autoSharding) {
          write = write.withAutoSharding();
        }
      }

      Schema inputSchema = inputRows.getSchema();

      // check if input schema is assignable to the output schema with nullability
      // check disabled for field
      if (write.getTable() != null) {
        TableReference tableRef = write.getTable().get();
        validateSchema(input.getPipeline().getOptions(), inputSchema, tableRef);
      }
      WriteResult result =
          inputRows
              .apply(
                  "element-count", ParDo.of(new ElementCounterFn("BigQuery-write-element-counter")))
              .setRowSchema(inputSchema)
              .apply(write);

      Schema rowSchema = inputRows.getSchema();
      Schema errorSchema =
          Schema.of(
              Field.of("failed_row", FieldType.row(rowSchema)),
              Field.of("error_message", FieldType.STRING));

      // Failed rows
      PCollection<Row> failedRows =
          result
              .getFailedStorageApiInserts()
              .apply(
                  "Construct failed rows",
                  MapElements.into(TypeDescriptors.rows())
                      .via(
                          (storageError) ->
                              BigQueryUtils.toBeamRow(rowSchema, storageError.getRow())))
              .setRowSchema(rowSchema);

      // Failed rows with error message
      PCollection<Row> failedRowsWithErrors =
          result
              .getFailedStorageApiInserts()
              .apply(
                  "Construct failed rows and errors",
                  MapElements.into(TypeDescriptors.rows())
                      .via(
                          (storageError) ->
                              Row.withSchema(errorSchema)
                                  .withFieldValue("error_message", storageError.getErrorMessage())
                                  .withFieldValue(
                                      "failed_row",
                                      BigQueryUtils.toBeamRow(rowSchema, storageError.getRow()))
                                  .build()))
              .setRowSchema(errorSchema);

      PCollection<Row> failedRowsOutput =
          failedRows
              .apply("error-count", ParDo.of(new ElementCounterFn("BigQuery-write-error-counter")))
              .setRowSchema(rowSchema);

      return PCollectionRowTuple.of(FAILED_ROWS_TAG, failedRowsOutput)
          .and(FAILED_ROWS_WITH_ERRORS_TAG, failedRowsWithErrors)
          .and("errors", failedRowsWithErrors);
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
                configuration.getCreateDisposition().toUpperCase());
        write = write.withCreateDisposition(createDisposition);
      }
      if (!Strings.isNullOrEmpty(configuration.getWriteDisposition())) {
        WriteDisposition writeDisposition =
            BigQueryStorageWriteApiSchemaTransformConfiguration.WRITE_DISPOSITIONS.get(
                configuration.getWriteDisposition().toUpperCase());
        write = write.withWriteDisposition(writeDisposition);
      }

      if (this.testBigQueryServices != null) {
        write = write.withTestServices(testBigQueryServices);
      }

      return write;
    }

    private void validateSchema(
        PipelineOptions pipelineOptions, Schema inputSchema, TableReference tableRef) {
      LOG.info("Validating schema ...");
      BigQueryOptions options = pipelineOptions.as(BigQueryOptions.class);
      try {
        Table table = null;
        if (this.testBigQueryServices != null) {
          DatasetService datasetService = testBigQueryServices.getDatasetService(options);
          if (datasetService != null) {
            table = datasetService.getTable(tableRef);
          }
        } else {
          table = BigQueryHelpers.getTable(options, tableRef);
        }
        if (table == null) {
          LOG.info("Table [{}] not found, skipping schema validation.", tableRef.getTableId());
          return;
        }
        Schema outputSchema = BigQueryUtils.fromTableSchema(table.getSchema());
        if (!inputSchema.assignableToIgnoreNullable(outputSchema)) {
          throw new IllegalArgumentException(
              "Input schema is not assignable to output schema. Input schema="
                  + inputSchema
                  + ", Output schema="
                  + outputSchema);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
