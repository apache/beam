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
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class DatadogWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<DatadogWriteSchemaTransformConfiguration> {
  private static final String IDENTIFIER = "beam:schematransform:org.apache.beam:datadog_write:v1";
  static final String INPUT_TAG = "input";
  private static final Logger LOG =
      LoggerFactory.getLogger(DatadogWriteSchemaTransformProvider.class);

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(DatadogWriteSchemaTransformConfiguration configuration) {
    return new DatadogWriteSchemaTransform(configuration);
  }

  // @Override
  // protected Class<DatadogWriteSchemaTransformConfiguration> configurationClass() {
  //   return DatadogWriteSchemaTransformConfiguration.class;
  // }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} input collection names method. */
  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} output collection names method. */
  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }

  /**
   * An implementation of {@link SchemaTransform} for Datadog Write jobs configured using {@link
   * DatadogWriteSchemaTransformConfiguration}.
   */
  protected static class DatadogWriteSchemaTransform extends SchemaTransform {
    private final DatadogWriteSchemaTransformConfiguration configuration;

    DatadogWriteSchemaTransform(DatadogWriteSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      // Validate configuration parameters
      configuration.validate();

      // Create basic transform
      DatadogIO.Write.Builder writeTransform =
          DatadogIO.writeBuilder()
              .withUrl(configuration.getUrl())
              .withApiKey(configuration.getApiKey());

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

      // Obtain input rows and convert to DatadogEvents
      PCollection<Row> inputRows = input.get(INPUT_TAG);

      PCollection<DatadogEvent> events =
          inputRows.apply("RowToDatadogEvent", ParDo.of(new RowToDatadogEventFn()));
      events.setCoder(DatadogEventCoder.of());

      events.apply("WriteToDatadog", writeTransform.build());

      // // Return empty tuple
      // String output = "";
      // ErrorHandling errorHandler = configuration.getErrorHandling();
      // if (errorHandler != null) {
      //   String outputHandler = errorHandler.getOutput();
      //   if (outputHandler != null) {
      //     output = outputHandler;
      //   } else {
      //     output = "";
      //   }
      // }
      // PCollectionRowTuple outputTuple = PCollectionRowTuple.of(output, events);

      return PCollectionRowTuple.empty(input.getPipeline());
    }
  }

  static class RowToDatadogEventFn extends DoFn<Row, DatadogEvent> {
    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<DatadogEvent> out) {
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

      out.output(builder.build());
    }
  }
}
