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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
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
    return Arrays.asList(OUTPUT, ERROR);
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

      // Obtain input schema
      Schema inputSchema = inputRows.getSchema();

      // Create error schema based on input schema
      Schema errorSchema = ErrorHandling.errorSchema(inputSchema);

      // Check for errors
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());

      Integer parallelism = configuration.getParallelism();
      int calculatedParallelism = parallelism != null ? parallelism : 1;

      // Inject Keys directly wrapping raw input rows
      PCollection<KV<Integer, Row>> keyedEvents =
          inputRows
              .apply("InjectKeys", ParDo.of(new CreateRowKeysFn(calculatedParallelism)))
              .setCoder(KvCoder.of(VarIntCoder.of(), RowCoder.of(inputSchema)));

      // Apply the write transform directly. All errors flow to Side Output.
      PCollectionTuple resultTuple =
          keyedEvents.apply(
              "WriteToDatadog",
              ParDo.of(
                      new RowBasedDatadogEventWriter(
                          configuration.getUrl(),
                          configuration.getApiKey(),
                          errorSchema,
                          handleErrors))
                  .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      // Obtain the single unified error output stream
      PCollection<Row> errorOutput = resultTuple.get(ERROR_TAG).setRowSchema(errorSchema);

      // Return the unified errors stream if error handling is active
      ErrorHandling errorHandling = configuration.getErrorHandling();
      if (errorHandling != null) {
        return PCollectionRowTuple.of(errorHandling.getOutput(), errorOutput);
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
