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
package org.apache.beam.sdk.io.datadog;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.HttpResponse;
import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoService(SchemaTransformProvider.class)
public class DatadogWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<DatadogWriteSchemaTransformConfiguration> {
  private static final String IDENTIFIER = "beam:schematransform:org.apache.beam:datadog_write:v1";
  static final String INPUT = "input";
  static final String OUTPUT = "output";
  static final String ERROR = "errors";
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Void> OUTPUT_TAG = new TupleTag<Void>() {};
  public static final TupleTag<DatadogEvent> EVENT_TAG = new TupleTag<DatadogEvent>() {};

  @Override
  protected Class<DatadogWriteSchemaTransformConfiguration> configurationClass() {
    return DatadogWriteSchemaTransformConfiguration.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(DatadogWriteSchemaTransformConfiguration configuration) {
    return new DatadogWriteSchemaTransform(configuration);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} input collection names method. */
  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} output collection names method. */
  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(ERROR);
  }

  /**
   * An implementation of {@link SchemaTransform} for Datadog Write jobs configured using {@link
   * DatadogWriteSchemaTransformConfiguration}.
   */
  static class DatadogWriteSchemaTransform extends SchemaTransform {
    private final DatadogWriteSchemaTransformConfiguration configuration;

    DatadogWriteSchemaTransform(DatadogWriteSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      // Validate configuration parameters
      configuration.validate();

      // Obtain input rows
      PCollection<Row> inputRows = input.get(INPUT);

      // Check for errors
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());

      Schema inputSchema = inputRows.getSchema();
      Schema dynamicErrorSchema =
          Schema.builder()
              .addNullableRowField("failed_row", inputSchema)
              .addNullableField("payload", Schema.FieldType.STRING)
              .addNullableField("statusCode", Schema.FieldType.INT32)
              .addNullableField("statusMessage", Schema.FieldType.STRING)
              .build();

      PCollectionTuple convertResult =
          inputRows.apply(
              "Convert to DatadogEvent",
              ParDo.of(new RowToEventFn(handleErrors, ERROR_TAG, dynamicErrorSchema))
                  .withOutputTags(EVENT_TAG, TupleTagList.of(ERROR_TAG)));

      PCollection<DatadogEvent> datadogEvents =
          convertResult.get(EVENT_TAG).setCoder(DatadogEventCoder.of());
      PCollection<Row> conversionErrors =
          convertResult
              .get(ERROR_TAG)
              .setCoder(org.apache.beam.sdk.coders.RowCoder.of(dynamicErrorSchema));

      // Configure DatadogIO.Write
      DatadogIO.Write.Builder builder =
          DatadogIO.writeBuilder(configuration.getMinBatchCount())
              .withUrl(configuration.getUrl())
              .withApiKey(configuration.getApiKey());

      Integer batchCount = configuration.getBatchCount();
      if (batchCount != null) {
        builder = builder.withBatchCount(batchCount);
      }
      Long maxBufferSize = configuration.getMaxBufferSize();
      if (maxBufferSize != null) {
        builder = builder.withMaxBufferSize(maxBufferSize);
      }
      Integer parallelism = configuration.getParallelism();
      if (parallelism != null) {
        builder = builder.withParallelism(parallelism);
      }

      DatadogIO.Write write = builder.build();

      // Apply DatadogIO.Write
      PCollection<DatadogWriteError> writeErrors = datadogEvents.apply("Write To Datadog", write);

      // Handle errors
      ErrorHandling errorHandling = configuration.getErrorHandling();
      if (errorHandling != null) {
        PCollection<Row> writeErrorRows =
            writeErrors
                .apply(
                    "Convert Write Errors to Rows",
                    org.apache.beam.sdk.transforms.MapElements.into(
                            org.apache.beam.sdk.values.TypeDescriptors.rows())
                        .via(
                            error ->
                                Row.withSchema(dynamicErrorSchema)
                                    .addValue(null)
                                    .addValue(error.payload())
                                    .addValue(error.statusCode())
                                    .addValue(error.statusMessage())
                                    .build()))
                .setCoder(org.apache.beam.sdk.coders.RowCoder.of(dynamicErrorSchema));

        PCollection<Row> allErrors =
            org.apache.beam.sdk.values.PCollectionList.of(conversionErrors)
                .and(writeErrorRows)
                .apply("Flatten Errors", org.apache.beam.sdk.transforms.Flatten.pCollections())
                .setCoder(org.apache.beam.sdk.coders.RowCoder.of(dynamicErrorSchema));

        return PCollectionRowTuple.of(errorHandling.getOutput(), allErrors);
      } else {
        return PCollectionRowTuple.empty(input.getPipeline());
      }
    }
  }

  static final Schema WRITE_ERROR_SCHEMA =
      Schema.builder()
          .addNullableField("payload", Schema.FieldType.STRING)
          .addNullableField("statusCode", Schema.FieldType.INT32)
          .addNullableField("statusMessage", Schema.FieldType.STRING)
          .build();

  static final Schema DATADOG_EVENT_SCHEMA =
      Schema.builder()
          .addNullableField("ddsource", Schema.FieldType.STRING)
          .addNullableField("ddtags", Schema.FieldType.STRING)
          .addNullableField("hostname", Schema.FieldType.STRING)
          .addNullableField("service", Schema.FieldType.STRING)
          .addNullableField("message", Schema.FieldType.STRING)
          .build();

  static Row eventToRow(DatadogEvent event) {
    return Row.withSchema(DATADOG_EVENT_SCHEMA)
        .addValue(event.ddsource())
        .addValue(event.ddtags())
        .addValue(event.hostname())
        .addValue(event.service())
        .addValue(event.message())
        .build();
  }

  static DatadogEvent rowToEvent(Row row) {
    DatadogEvent.Builder builder = DatadogEvent.newBuilder();
    Schema schema = row.getSchema();

    String ddsource = schema.hasField("ddsource") ? row.getString("ddsource") : null;
    if (ddsource != null) {
      builder.withSource(ddsource);
    }
    String ddtags = schema.hasField("ddtags") ? row.getString("ddtags") : null;
    if (ddtags != null) {
      builder.withTags(ddtags);
    }
    String hostname = schema.hasField("hostname") ? row.getString("hostname") : null;
    if (hostname != null) {
      builder.withHostname(hostname);
    }
    String service = schema.hasField("service") ? row.getString("service") : null;
    if (service != null) {
      builder.withService(service);
    }
    String message = schema.hasField("message") ? row.getString("message") : null;
    builder.withMessage(checkNotNull(message, "Message is required."));

    return builder.build();
  }

  static class RowToEventFn extends DoFn<Row, DatadogEvent> {
    private final boolean handleErrors;
    private final TupleTag<Row> errorOutputTag;
    private final Schema errorSchema;

    RowToEventFn(boolean handleErrors, TupleTag<Row> errorOutputTag, Schema errorSchema) {
      this.handleErrors = handleErrors;
      this.errorOutputTag = errorOutputTag;
      this.errorSchema = errorSchema;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        c.output(rowToEvent(c.element()));
      } catch (Exception e) {
        if (handleErrors) {
          c.output(
              errorOutputTag,
              Row.withSchema(errorSchema)
                  .addValue(c.element())
                  .addValue(c.element().toString())
                  .addValue(java.net.HttpURLConnection.HTTP_BAD_REQUEST)
                  .addValue(e.getMessage())
                  .build());
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }

  static Row errorToRow(DatadogWriteError error) {
    return Row.withSchema(WRITE_ERROR_SCHEMA)
        .addValue(error.payload())
        .addValue(error.statusCode())
        .addValue(error.statusMessage())
        .build();
  }

  static DatadogWriteError rowToError(Row row) {
    DatadogWriteError.Builder builder = DatadogWriteError.newBuilder();
    String payload = row.getString("payload");
    if (payload != null) {
      builder.withPayload(payload);
    }
    Integer statusCode = (Integer) row.getValue("statusCode");
    if (statusCode != null) {
      builder.withStatusCode(statusCode);
    }
    String statusMessage = row.getString("statusMessage");
    if (statusMessage != null) {
      builder.withStatusMessage(statusMessage);
    }
    return builder.build();
  }

  static class CreateRowKeysFn extends DoFn<Row, KV<Integer, Row>> {
    private final int calculatedParallelism;

    CreateRowKeysFn(int parallelism) {
      this.calculatedParallelism = parallelism;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(
          KV.of(ThreadLocalRandom.current().nextInt(calculatedParallelism), context.element()));
    }
  }

  static class RowBasedDatadogEventWriter extends DoFn<KV<Integer, Row>, Void> {
    private final String url;
    private final String apiKey;
    private final Schema errorSchema;
    private final boolean handleErrors;
    private transient @Nullable DatadogEventPublisher publisher;

    RowBasedDatadogEventWriter(
        String url, String apiKey, Schema errorSchema, boolean handleErrors) {
      this.url = url;
      this.apiKey = apiKey;
      this.errorSchema = errorSchema;
      this.handleErrors = handleErrors;
    }

    @Setup
    public void setup() throws Exception {
      checkNotNull(url, "url is required for writing events.");
      checkNotNull(apiKey, "API Key is required for writing events.");

      DatadogEventPublisher.Builder builder =
          DatadogEventPublisher.newBuilder().withUrl(url).withApiKey(apiKey);
      publisher = builder.build();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Row rawRow = c.element().getValue();
      try {
        // First: Convert to structured DatadogEvent object (performing schema validation)
        DatadogEvent event = rowToEvent(rawRow);
        List<DatadogEvent> events = Collections.singletonList(event);

        // Second: Write to endpoint
        HttpResponse response = null;
        try {
          response = checkNotNull(publisher).execute(events);
          if (!response.isSuccessStatusCode()) {
            throw new java.io.IOException(
                String.format(
                    "HTTP Write failure [Status Code %d]: %s",
                    response.getStatusCode(), response.getStatusMessage()));
          }
        } finally {
          if (response != null) {
            try {
              response.ignore();
            } catch (Exception e) {
              // ignore
            }
          }
        }
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(e);
        }
        // Emit standard serialized error record dynamically preserving input data
        c.output(ERROR_TAG, ErrorHandling.errorRecord(errorSchema, rawRow, e));
      }
    }

    @Teardown
    public void tearDown() {
      if (this.publisher != null) {
        try {
          this.publisher.close();
        } catch (Exception e) {
          // ignore
        }
      }
    }
  }
}
