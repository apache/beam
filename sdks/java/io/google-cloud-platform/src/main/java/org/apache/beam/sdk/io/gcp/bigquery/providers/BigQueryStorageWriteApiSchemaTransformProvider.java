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

import static org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryWriteConfiguration.DYNAMIC_DESTINATIONS;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.joda.time.Duration;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for BigQuery Storage Write API jobs
 * configured via {@link BigQueryWriteConfiguration}.
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
    extends TypedSchemaTransformProvider<BigQueryWriteConfiguration> {
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
  protected SchemaTransform from(BigQueryWriteConfiguration configuration) {
    return new BigQueryStorageWriteApiSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:bigquery_storage_write:v2";
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

  /**
   * A {@link SchemaTransform} for BigQuery Storage Write API, configured with {@link
   * BigQueryWriteConfiguration} and instantiated by {@link
   * BigQueryStorageWriteApiSchemaTransformProvider}.
   */
  public static class BigQueryStorageWriteApiSchemaTransform extends SchemaTransform {

    private BigQueryServices testBigQueryServices = null;
    private final BigQueryWriteConfiguration configuration;

    BigQueryStorageWriteApiSchemaTransform(BigQueryWriteConfiguration configuration) {
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

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      // Check that the input exists
      PCollection<Row> inputRows = input.getSinglePCollection();

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

    BigQueryIO.Write<Row> createStorageWriteApiTransform(Schema schema) {
      Method writeMethod =
          configuration.getUseAtLeastOnceSemantics() != null
                  && configuration.getUseAtLeastOnceSemantics()
              ? Method.STORAGE_API_AT_LEAST_ONCE
              : Method.STORAGE_WRITE_API;

      BigQueryIO.Write<Row> write =
          BigQueryIO.<Row>write()
              .withMethod(writeMethod)
              .withWriteDisposition(WriteDisposition.WRITE_APPEND);

      Schema rowSchema = schema;
      boolean fetchNestedRecord = false;
      if (configuration.getTable().equals(DYNAMIC_DESTINATIONS)) {
        validateDynamicDestinationsSchema(schema);
        rowSchema = schema.getField("record").getType().getRowSchema();
        fetchNestedRecord = true;
      }
      if (Boolean.TRUE.equals(configuration.getUseCdcWrites())) {
        validateCdcSchema(schema);
        rowSchema = schema.getField("record").getType().getRowSchema();
        fetchNestedRecord = true;
        write =
            write
                .withPrimaryKey(configuration.getPrimaryKey())
                .withRowMutationInformationFn(
                    row ->
                        RowMutationInformation.of(
                            RowMutationInformation.MutationType.valueOf(
                                row.getRow(ROW_PROPERTY_MUTATION_INFO)
                                    .getString(ROW_PROPERTY_MUTATION_TYPE)),
                            row.getRow(ROW_PROPERTY_MUTATION_INFO)
                                .getString(ROW_PROPERTY_MUTATION_SQN)));
      }
      PortableBigQueryDestinations dynamicDestinations =
          new PortableBigQueryDestinations(rowSchema, configuration);
      write =
          write
              .to(dynamicDestinations)
              .withFormatFunction(dynamicDestinations.getFilterFormatFunction(fetchNestedRecord));

      if (!Strings.isNullOrEmpty(configuration.getCreateDisposition())) {
        CreateDisposition createDisposition =
            CreateDisposition.valueOf(configuration.getCreateDisposition().toUpperCase());
        write = write.withCreateDisposition(createDisposition);
      }

      if (!Strings.isNullOrEmpty(configuration.getWriteDisposition())) {
        WriteDisposition writeDisposition =
            WriteDisposition.valueOf(configuration.getWriteDisposition().toUpperCase());
        write = write.withWriteDisposition(writeDisposition);
      }
      if (!Strings.isNullOrEmpty(configuration.getKmsKey())) {
        write = write.withKmsKey(configuration.getKmsKey());
      }
      if (this.testBigQueryServices != null) {
        write = write.withTestServices(testBigQueryServices);
      }

      return write;
    }

    void validateDynamicDestinationsSchema(Schema schema) {
      checkArgument(
          schema.getFieldNames().containsAll(Arrays.asList("destination", "record")),
          "When writing to dynamic destinations, we expect Row Schema with a "
              + "\"destination\" string field and a \"record\" Row field.");
    }

    private void validateCdcSchema(Schema schema) {
      checkArgument(
          schema.getFieldNames().containsAll(Arrays.asList(ROW_PROPERTY_MUTATION_INFO, "record")),
          "When writing using CDC functionality, we expect Row Schema with a "
              + "\""
              + ROW_PROPERTY_MUTATION_INFO
              + "\" Row field and a \"record\" Row field.");

      Schema mutationSchema = schema.getField(ROW_PROPERTY_MUTATION_INFO).getType().getRowSchema();

      checkArgument(
          mutationSchema != null && mutationSchema.equals(ROW_SCHEMA_MUTATION_INFO),
          "When writing using CDC functionality, we expect a \""
              + ROW_PROPERTY_MUTATION_INFO
              + "\" field of Row type with schema:\n"
              + ROW_SCHEMA_MUTATION_INFO.toString()
              + "\n"
              + "Received \""
              + ROW_PROPERTY_MUTATION_INFO
              + "\" field with schema:\n"
              + mutationSchema);
    }
  }
}
