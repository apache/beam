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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.TableConstraints;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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
@AutoService(SchemaTransformProvider.class)
public class BigQueryStorageWriteApiSchemaTransformProvider
    extends TypedSchemaTransformProvider<BigQueryStorageWriteApiSchemaTransformConfiguration> {
  private static final Integer DEFAULT_TRIGGER_FREQUENCY_SECS = 5;
  private static final Duration DEFAULT_TRIGGERING_FREQUENCY =
      Duration.standardSeconds(DEFAULT_TRIGGER_FREQUENCY_SECS);
  private static final String INPUT_ROWS_TAG = "input";
  private static final String FAILED_ROWS_TAG = "FailedRows";
  private static final String FAILED_ROWS_WITH_ERRORS_TAG = "FailedRowsWithErrors";
  // magic string that tells us to write to dynamic destinations
  protected static final String DYNAMIC_DESTINATIONS = "DYNAMIC_DESTINATIONS";
  protected static final String ROW_PROPERTY_MUTATION_INFO = "row_mutation_info";
  protected static final String ROW_PROPERTY_MUTATION_TYPE = "mutation_type";
  protected static final String ROW_PROPERTY_MUTATION_SQN = "change_sequence_number";
  protected static final Schema ROW_SCHEMA_MUTATION_INFO =
      Schema.builder()
          .addStringField("mutation_type")
          .addStringField("change_sequence_number")
          .build();

  @Override
  protected SchemaTransform from(
      BigQueryStorageWriteApiSchemaTransformConfiguration configuration) {
    return new BigQueryStorageWriteApiSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return String.format("beam:schematransform:org.apache.beam:bigquery_storage_write:v2");
  }

  @Override
  public String description() {
    return String.format(
        "Writes data to BigQuery using the Storage Write API (https://cloud.google.com/bigquery/docs/write-api)."
            + "\n\nThis expects a single PCollection of Beam Rows and outputs two dead-letter queues (DLQ) that "
            + "contain failed rows. The first DLQ has tag [%s] and contains the failed rows. The second DLQ has "
            + "tag [%s] and contains failed rows and along with their respective errors.",
        FAILED_ROWS_TAG, FAILED_ROWS_WITH_ERRORS_TAG);
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

    @AutoValue
    public abstract static class ErrorHandling {
      @SchemaFieldDescription("The name of the output PCollection containing failed writes.")
      public abstract String getOutput();

      public static Builder builder() {
        return new AutoValue_BigQueryStorageWriteApiSchemaTransformProvider_BigQueryStorageWriteApiSchemaTransformConfiguration_ErrorHandling
            .Builder();
      }

      @AutoValue.Builder
      public abstract static class Builder {
        public abstract Builder setOutput(String output);

        public abstract ErrorHandling build();
      }
    }

    public void validate() {
      String invalidConfigMessage = "Invalid BigQuery Storage Write configuration: ";

      // validate output table spec
      checkArgument(
          !Strings.isNullOrEmpty(this.getTable()),
          invalidConfigMessage + "Table spec for a BigQuery Write must be specified.");

      // if we have an input table spec, validate it
      if (!this.getTable().equals(DYNAMIC_DESTINATIONS)) {
        checkNotNull(BigQueryHelpers.parseTableSpec(this.getTable()));
      }

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

      if (this.getErrorHandling() != null) {
        checkArgument(
            !Strings.isNullOrEmpty(this.getErrorHandling().getOutput()),
            invalidConfigMessage + "Output must not be empty if error handling specified.");
      }

      if (this.getAutoSharding() != null
          && this.getAutoSharding()
          && this.getNumStreams() != null) {
        checkArgument(
            this.getNumStreams() == 0,
            invalidConfigMessage
                + "Cannot set a fixed number of streams when auto-sharding is enabled. Please pick only one of the two options.");
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
        "This option enables using a dynamically determined number of Storage Write API streams to write to "
            + "BigQuery. Only applicable to unbounded data.")
    @Nullable
    public abstract Boolean getAutoSharding();

    @SchemaFieldDescription(
        "Specifies the number of write streams that the Storage API sink will use. "
            + "This parameter is only applicable when writing unbounded data.")
    @Nullable
    public abstract Integer getNumStreams();

    @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
    @Nullable
    public abstract ErrorHandling getErrorHandling();

    @SchemaFieldDescription(
        "This option enables the use of BigQuery CDC functionality. The expected PCollection"
            + " should contain Beam Rows with a schema wrapping the record to be inserted and"
            + " adding the CDC info similar to: {row_mutation_info: {mutation_type:\"...\", "
            + "change_sequence_number:\"...\"}, record: {...}}")
    @Nullable
    public abstract Boolean getUseCdcWrites();

    @SchemaFieldDescription(
        "If CREATE_IF_NEEDED disposition is set, BigQuery table(s) will be created with this"
            + " primary key. Required when CDC writes are enabled with CREATE_IF_NEEDED.")
    @Nullable
    public abstract List<String> getPrimaryKey();

    /** Builder for {@link BigQueryStorageWriteApiSchemaTransformConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setTable(String table);

      public abstract Builder setCreateDisposition(String createDisposition);

      public abstract Builder setWriteDisposition(String writeDisposition);

      public abstract Builder setTriggeringFrequencySeconds(Long seconds);

      public abstract Builder setUseAtLeastOnceSemantics(Boolean use);

      public abstract Builder setAutoSharding(Boolean autoSharding);

      public abstract Builder setNumStreams(Integer numStreams);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract Builder setUseCdcWrites(Boolean cdcWrites);

      public abstract Builder setPrimaryKey(List<String> pkColumns);

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
    private static class ElementCounterFn<T> extends DoFn<T, T> {

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

    private static class FailOnError extends DoFn<BigQueryStorageApiInsertError, Void> {
      @ProcessElement
      public void process(ProcessContext c) {
        throw new RuntimeException(c.element().getErrorMessage());
      }
    }

    private static class NoOutputDoFn<T> extends DoFn<T, Row> {
      @ProcessElement
      public void process(ProcessContext c) {}
    }

    private static class RowDynamicDestinations extends DynamicDestinations<Row, String> {
      Schema schema;

      RowDynamicDestinations(Schema schema) {
        this.schema = schema;
      }

      @Override
      public String getDestination(ValueInSingleWindow<Row> element) {
        return element.getValue().getString("destination");
      }

      @Override
      public TableDestination getTable(String destination) {
        return new TableDestination(destination, null);
      }

      @Override
      public TableSchema getSchema(String destination) {
        return BigQueryUtils.toTableSchema(schema);
      }
    }

    private static class CdcWritesDynamicDestination extends RowDynamicDestinations {
      final String fixedDestination;
      final List<String> primaryKey;

      public CdcWritesDynamicDestination(
          Schema schema, String fixedDestination, List<String> primaryKey) {
        super(schema);
        this.fixedDestination = fixedDestination;
        this.primaryKey = primaryKey;
      }

      @Override
      public String getDestination(ValueInSingleWindow<Row> element) {
        return Optional.ofNullable(fixedDestination).orElseGet(() -> super.getDestination(element));
      }

      @Override
      public TableConstraints getTableConstraints(String destination) {
        return Optional.ofNullable(this.primaryKey)
            .filter(pk -> !pk.isEmpty())
            .map(
                pk ->
                    new TableConstraints()
                        .setPrimaryKey(new TableConstraints.PrimaryKey().setColumns(pk)))
            .orElse(null);
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      // Check that the input exists
      checkArgument(input.has(INPUT_ROWS_TAG), "Missing expected input tag: %s", INPUT_ROWS_TAG);
      PCollection<Row> inputRows = input.get(INPUT_ROWS_TAG);

      BigQueryIO.Write<Row> write = createStorageWriteApiTransform(inputRows.getSchema());

      if (inputRows.isBounded() == IsBounded.UNBOUNDED) {
        Long triggeringFrequency = configuration.getTriggeringFrequencySeconds();
        Boolean autoSharding = configuration.getAutoSharding();
        int numStreams = configuration.getNumStreams() == null ? 0 : configuration.getNumStreams();

        boolean useAtLeastOnceSemantics =
            configuration.getUseAtLeastOnceSemantics() != null
                && configuration.getUseAtLeastOnceSemantics();
        // Triggering frequency is only applicable for exactly-once
        if (!useAtLeastOnceSemantics) {
          write =
              write.withTriggeringFrequency(
                  (triggeringFrequency == null || triggeringFrequency <= 0)
                      ? DEFAULT_TRIGGERING_FREQUENCY
                      : Duration.standardSeconds(triggeringFrequency));
        }
        // set num streams if specified, otherwise default to autoSharding
        if (numStreams > 0) {
          write = write.withNumStorageWriteApiStreams(numStreams);
        } else if (autoSharding == null || autoSharding) {
          write = write.withAutoSharding();
        }
      }

      Schema inputSchema = inputRows.getSchema();

      WriteResult result =
          inputRows
              .apply(
                  "element-count",
                  ParDo.of(new ElementCounterFn<Row>("BigQuery-write-element-counter")))
              .setRowSchema(inputSchema)
              .apply(write);

      // Give something that can be followed.
      PCollection<Row> postWrite =
          result
              .getFailedStorageApiInserts()
              .apply("post-write", ParDo.of(new NoOutputDoFn<BigQueryStorageApiInsertError>()))
              .setRowSchema(Schema.of());

      if (configuration.getErrorHandling() == null) {
        result
            .getFailedStorageApiInserts()
            .apply("Error on failed inserts", ParDo.of(new FailOnError()));
        return PCollectionRowTuple.of("post_write", postWrite);
      } else {
        result
            .getFailedStorageApiInserts()
            .apply(
                "error-count",
                ParDo.of(
                    new ElementCounterFn<BigQueryStorageApiInsertError>(
                        "BigQuery-write-error-counter")));

        // Failed rows with error message
        Schema errorSchema =
            Schema.of(
                Field.of("failed_row", FieldType.row(inputSchema)),
                Field.of("error_message", FieldType.STRING));
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
                                        BigQueryUtils.toBeamRow(inputSchema, storageError.getRow()))
                                    .build()))
                .setRowSchema(errorSchema);
        return PCollectionRowTuple.of("post_write", postWrite)
            .and(configuration.getErrorHandling().getOutput(), failedRowsWithErrors);
      }
    }

    void validateDynamicDestinationsExpectedSchema(Schema schema) {
      checkArgument(
          schema.getFieldNames().containsAll(Arrays.asList("destination", "record")),
          "When writing to dynamic destinations, we expect Row Schema with a "
              + "\"destination\" string field and a \"record\" Row field.");
    }

    BigQueryIO.Write<Row> createStorageWriteApiTransform(Schema schema) {
      Method writeMethod =
          configuration.getUseAtLeastOnceSemantics() != null
                  && configuration.getUseAtLeastOnceSemantics()
              ? Method.STORAGE_API_AT_LEAST_ONCE
              : Method.STORAGE_WRITE_API;

      BigQueryIO.Write<Row> write =
          BigQueryIO.<Row>write()
              .withMethod(writeMethod)
              .withFormatFunction(BigQueryUtils.toTableRow())
              .withWriteDisposition(WriteDisposition.WRITE_APPEND);

      // in case CDC writes are configured we validate and include them in the configuration
      if (Optional.ofNullable(configuration.getUseCdcWrites()).orElse(false)) {
        write = validateAndIncludeCDCInformation(write, schema);
      } else if (configuration.getTable().equals(DYNAMIC_DESTINATIONS)) {
        validateDynamicDestinationsExpectedSchema(schema);
        write =
            write
                .to(new RowDynamicDestinations(schema.getField("record").getType().getRowSchema()))
                .withFormatFunction(row -> BigQueryUtils.toTableRow(row.getRow("record")));
      } else {
        write = write.to(configuration.getTable()).useBeamSchema();
      }

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

    BigQueryIO.Write<Row> validateAndIncludeCDCInformation(
        BigQueryIO.Write<Row> write, Schema schema) {
      checkArgument(
          schema.getFieldNames().containsAll(Arrays.asList(ROW_PROPERTY_MUTATION_INFO, "record")),
          "When writing using CDC functionality, we expect Row Schema with a "
              + "\""
              + ROW_PROPERTY_MUTATION_INFO
              + "\" Row field and a \"record\" Row field.");
      checkArgument(
          schema
              .getField(ROW_PROPERTY_MUTATION_INFO)
              .getType()
              .getRowSchema()
              .equals(ROW_SCHEMA_MUTATION_INFO),
          "When writing using CDC functionality, we expect a \""
              + ROW_PROPERTY_MUTATION_INFO
              + "\" field of Row type with fields \""
              + ROW_PROPERTY_MUTATION_TYPE
              + "\" and \""
              + ROW_PROPERTY_MUTATION_SQN
              + "\" both of type string.");

      String tableDestination = null;

      if (configuration.getTable().equals(DYNAMIC_DESTINATIONS)) {
        validateDynamicDestinationsExpectedSchema(schema);
      } else {
        tableDestination = configuration.getTable();
      }

      return write
          .to(
              new CdcWritesDynamicDestination(
                  schema.getField("record").getType().getRowSchema(),
                  tableDestination,
                  configuration.getPrimaryKey()))
          .withFormatFunction(row -> BigQueryUtils.toTableRow(row.getRow("record")))
          .withPrimaryKey(configuration.getPrimaryKey())
          .withRowMutationInformationFn(
              row ->
                  RowMutationInformation.of(
                      RowMutationInformation.MutationType.valueOf(
                          row.getRow(ROW_PROPERTY_MUTATION_INFO)
                              .getString(ROW_PROPERTY_MUTATION_TYPE)),
                      row.getRow(ROW_PROPERTY_MUTATION_INFO).getString(ROW_PROPERTY_MUTATION_SQN)));
    }
  }
}
