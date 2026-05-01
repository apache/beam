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
import java.util.ArrayList;
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
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ValueState;
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
  public static final TupleTag<Row> OUTPUT_TAG_ROW = new TupleTag<Row>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};

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

      // Apply row to Datadog event fn with error handling
      PCollectionTuple pCollectionTuple =
          inputRows.apply(
              "RowToRowEvent",
              ParDo.of(new RowToRowEventFn(errorSchema, handleErrors))
                  .withOutputTags(OUTPUT_TAG_ROW, TupleTagList.of(ERROR_TAG)));

      // Obtain error output
      PCollection<Row> errorOutput = pCollectionTuple.get(ERROR_TAG).setRowSchema(errorSchema);

      // Obtain events ready to be sent to Datadog (as Rows with DATADOG_EVENT_SCHEMA)
      PCollection<Row> events =
          pCollectionTuple.get(OUTPUT_TAG_ROW).setRowSchema(DATADOG_EVENT_SCHEMA);

      Integer parallelism = configuration.getParallelism();
      int calculatedParallelism = parallelism != null ? parallelism : 1;

      // Inject Keys
      PCollection<KV<Integer, Row>> keyedEvents =
          events
              .apply("InjectKeys", ParDo.of(new CreateRowKeysFn(calculatedParallelism)))
              .setCoder(KvCoder.of(VarIntCoder.of(), RowCoder.of(DATADOG_EVENT_SCHEMA)));

      // Apply the write transform to events to write events to Datadog
      PCollection<Row> writeErrorsRows =
          keyedEvents
              .apply(
                  "WriteToDatadog",
                  ParDo.of(
                      new RowBasedDatadogEventWriter(
                          configuration.getUrl(),
                          configuration.getApiKey(),
                          configuration.getBatchCount(),
                          configuration.getMaxBufferSize())))
              .setRowSchema(WRITE_ERROR_SCHEMA);

      // Error handling
      ErrorHandling errorHandling = configuration.getErrorHandling();
      if (errorHandling != null) {
        // Return error output for downstream further processing
        return PCollectionRowTuple.of(errorHandling.getOutput(), errorOutput)
            .and("write_errors", writeErrorsRows);
      } else {
        // Return empty tuple since no errors were encountered but include write errors if any (to
        // avoid serialization issues)
        return PCollectionRowTuple.empty(input.getPipeline()).and("write_errors", writeErrorsRows);
      }
    }
  }

  /**
   * A {@link DoFn} that converts a {@link Row} into a standardized {@link Row} with EVENT_SCHEMA
   * and emits failures to a dead-letter queue.
   */
  static class RowToRowEventFn extends DoFn<Row, Row> {
    private final Schema errorSchema;
    private final boolean handleErrors;

    RowToRowEventFn(Schema errorSchema, boolean handleErrors) {
      this.errorSchema = errorSchema;
      this.handleErrors = handleErrors;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Row row = c.element();
      try {
        Row.Builder builder = Row.withSchema(DATADOG_EVENT_SCHEMA);
        Schema schema = row.getSchema();

        builder.addValue(schema.hasField("ddsource") ? row.getString("ddsource") : null);
        builder.addValue(schema.hasField("ddtags") ? row.getString("ddtags") : null);
        builder.addValue(schema.hasField("hostname") ? row.getString("hostname") : null);
        builder.addValue(schema.hasField("service") ? row.getString("service") : null);

        String message = schema.hasField("message") ? row.getString("message") : null;
        checkNotNull(message, "Message is required.");
        builder.addValue(message);

        c.output(builder.build());
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(e);
        }
        c.output(ERROR_TAG, ErrorHandling.errorRecord(errorSchema, row, e));
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
    String ddsource = row.getString("ddsource");
    if (ddsource != null) {
      builder.withSource(ddsource);
    }
    String ddtags = row.getString("ddtags");
    if (ddtags != null) {
      builder.withTags(ddtags);
    }
    String hostname = row.getString("hostname");
    if (hostname != null) {
      builder.withHostname(hostname);
    }
    String service = row.getString("service");
    if (service != null) {
      builder.withService(service);
    }
    String message = row.getString("message");
    if (message != null) {
      builder.withMessage(message);
    }
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

  static class RowBasedDatadogEventWriter extends DoFn<KV<Integer, Row>, Row> {
    private final String url;
    private final String apiKey;
    private transient @Nullable DatadogEventPublisher publisher;

    RowBasedDatadogEventWriter(
        String url, String apiKey, @Nullable Integer batchCount, @Nullable Long maxBufferSize) {
      this.url = url;
      this.apiKey = apiKey;
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
    public void processElement(@Element KV<Integer, Row> input, OutputReceiver<Row> receiver)
        throws Exception {

      Row eventRow = input.getValue();
      DatadogEvent event = rowToEvent(eventRow);
      List<DatadogEvent> events = Collections.singletonList(event);

      HttpResponse response = null;
      try {
        response = checkNotNull(publisher).execute(events);
        if (!response.isSuccessStatusCode()) {
          flushWriteFailures(
              Collections.singletonList(eventRow),
              response.getStatusMessage(),
              response.getStatusCode(),
              receiver);
        }
      } catch (Exception e) {
        flushWriteFailures(Collections.singletonList(eventRow), e.getMessage(), null, receiver);
      } finally {
        if (response != null) {
          try {
            response.ignore();
          } catch (Exception e) {
            // ignore
          }
        }
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

    private void flush(
        OutputReceiver<Row> receiver,
        BagState<Row> bufferState,
        ValueState<Long> countState,
        ValueState<Long> bufferSizeState)
        throws Exception {

      if (!bufferState.isEmpty().read()) {
        List<Row> eventRows = new ArrayList<>();
        bufferState.read().forEach(eventRows::add);

        List<DatadogEvent> events = new ArrayList<>();
        for (Row r : eventRows) {
          events.add(rowToEvent(r));
        }

        HttpResponse response = null;
        try {
          response = checkNotNull(publisher).execute(events);
          if (!response.isSuccessStatusCode()) {
            flushWriteFailures(
                eventRows, response.getStatusMessage(), response.getStatusCode(), receiver);
          }
        } catch (Exception e) {
          flushWriteFailures(eventRows, e.getMessage(), null, receiver);
        } finally {
          try {
            if (response != null) {
              response.ignore();
            }
          } catch (Exception e) {
            // Ignore errors when ignoring response
          }
          bufferState.clear();
          countState.clear();
          bufferSizeState.clear();
        }
      }
    }

    private void flushWriteFailures(
        List<Row> eventRows,
        @Nullable String statusMessage,
        @Nullable Integer statusCode,
        OutputReceiver<Row> receiver) {

      for (Row eventRow : eventRows) {
        DatadogEvent event = rowToEvent(eventRow);
        String payload = DatadogEventSerializer.getPayloadString(event);

        Row errorRow =
            Row.withSchema(WRITE_ERROR_SCHEMA)
                .addValue(payload)
                .addValue(statusCode)
                .addValue(statusMessage)
                .build();
        receiver.output(errorRow);
      }
    }
  }
}
