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

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class DatadogWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<DatadogWriteSchemaTransformConfiguration> {
  private static final String IDENTIFIER = "beam:schematransform:org.apache.beam:datadog_write:v1";
  static final String INPUT = "input";
  static final String OUTPUT = "output";
  static final String ERROR = "errors";
  public static final TupleTag<DatadogEvent> OUTPUT_TAG = new TupleTag<DatadogEvent>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  private static final Logger LOG =
      LoggerFactory.getLogger(DatadogWriteSchemaTransformProvider.class);

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

      DatadogIO.Write.Builder writeTransform;

      // Create basic transform
      Integer minBatchCount = configuration.getMinBatchCount();
      if (minBatchCount == null) {
        writeTransform =
            DatadogIO.writeBuilder()
                .withUrl(configuration.getUrl())
                .withApiKey(configuration.getApiKey());
      } else {
        writeTransform =
            DatadogIO.writeBuilder(minBatchCount)
                .withUrl(configuration.getUrl())
                .withApiKey(configuration.getApiKey());
      }

      // Add more parameters if not null
      Integer batchCount = configuration.getBatchCount();
      if (batchCount != null) {
        writeTransform = writeTransform.withBatchCount(batchCount);
      }
      Long maxBufferSize = configuration.getMaxBufferSize();
      if (maxBufferSize != null) {
        writeTransform = writeTransform.withMaxBufferSize(maxBufferSize);
      }
      Integer parallelism = configuration.getParallelism();
      if (parallelism != null) {
        writeTransform = writeTransform.withParallelism(parallelism);
      }

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
              "RowToDatadogEvent",
              ParDo.of(new RowToDatadogEventFn(errorSchema, handleErrors))
                  .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      // Obtain error output
      PCollection<Row> errorOutput = pCollectionTuple.get(ERROR_TAG).setRowSchema(errorSchema);

      // Obtain events ready to be sent to Datadog
      PCollection<DatadogEvent> events = pCollectionTuple.get(OUTPUT_TAG);

      // Set coder for events
      events.setCoder(DatadogEventCoder.of());

      // Apply the write transform to events to write events to Datadog
      events.apply("WriteToDatadog", writeTransform.build());

      // Error handling
      ErrorHandling errorHandling = configuration.getErrorHandling();
      if (errorHandling != null) {
        // Return error output for downstream further processing
        return PCollectionRowTuple.of(errorHandling.getOutput(), errorOutput);
      } else {
        // Return empty tuple since no errors were encountered
        return PCollectionRowTuple.empty(input.getPipeline());
      }
    }
  }

  /**
   * A {@link DoFn} that converts a {@link Row} into a {@link DatadogEvent} and emits failures to a
   * dead-letter queue.
   */
  static class RowToDatadogEventFn extends DoFn<Row, DatadogEvent> {
    private final Schema errorSchema;
    private final boolean handleErrors;

    RowToDatadogEventFn(Schema errorSchema, boolean handleErrors) {
      this.errorSchema = errorSchema;
      this.handleErrors = handleErrors;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Row row = c.element();
      try {
        DatadogEvent.Builder builder = DatadogEvent.newBuilder();
        Schema schema = row.getSchema();

        if (schema.hasField("ddsource")) {
          String ddsource = row.getString("ddsource");
          if (ddsource != null) {
            builder.withSource(ddsource);
          }
        }
        if (schema.hasField("ddtags")) {
          String ddtags = row.getString("ddtags");
          if (ddtags != null) {
            builder.withTags(ddtags);
          }
        }
        if (schema.hasField("hostname")) {
          String hostname = row.getString("hostname");
          if (hostname != null) {
            builder.withHostname(hostname);
          }
        }
        if (schema.hasField("service")) {
          String service = row.getString("service");
          if (service != null) {
            builder.withService(service);
          }
        }
        if (schema.hasField("message")) {
          String message = row.getString("message");
          if (message != null) {
            builder.withMessage(message);
          }
        }

        c.output(builder.build());
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(e);
        }
        c.output(ERROR_TAG, ErrorHandling.errorRecord(errorSchema, row, e));
      }
    }
  }
}
