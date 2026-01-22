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
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.io.payloads.AutoValuePayloads.AutoValueSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

@AutoService(SchemaTransformProvider.class)
public class DatadogWriteSchemaTransformProvider extends
    TypedSchemaTransformProvider<DatadogWriteSchemaTransformProvider.DatadogWriteSchemaTransformConfiguration> {

  static final String INPUT_TAG = "input";

  @Override
  protected Class<DatadogWriteSchemaTransformConfiguration> configurationClass() {
    return DatadogWriteSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(DatadogWriteSchemaTransformConfiguration configuration) {
    return new DatadogWriteSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:datadog_write:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }

  @DefaultSchema(AutoValueSchema.class)
  @com.google.auto.value.AutoValue
  public abstract static class DatadogWriteSchemaTransformConfiguration {

    public void validate() {}

    public static Builder builder() {
      return new AutoValue_DatadogWriteSchemaTransformProvider_DatadogWriteSchemaTransformConfiguration.Builder();
    }

    @SchemaFieldDescription("The Datadog API URL.")
    public abstract String getUrl();

    @SchemaFieldDescription("The Datadog API key.")
    public abstract String getApiKey();

    @SchemaFieldDescription("The number of events to batch together for each write.")
    @Nullable
    public abstract Integer getBatchCount();

    @SchemaFieldDescription("The maximum buffer size in bytes.")
    @Nullable
    public abstract Long getMaxBufferSize();

    @SchemaFieldDescription("The degree of parallelism for writing.")
    @Nullable
    public abstract Integer getParallelism();


    @com.google.auto.value.AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setUrl(String url);

      public abstract Builder setApiKey(String apiKey);

      public abstract Builder setBatchCount(Integer batchCount);

      public abstract Builder setMaxBufferSize(Long maxBufferSize);

      public abstract Builder setParallelism(Integer parallelism);

      public abstract DatadogWriteSchemaTransformConfiguration build();
    }
  }

  protected static class DatadogWriteSchemaTransform extends SchemaTransform {
    private final DatadogWriteSchemaTransformConfiguration configuration;

    DatadogWriteSchemaTransform(DatadogWriteSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }


    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> inputRows = input.get(INPUT_TAG);

      PCollection<DatadogEvent> events = inputRows.apply("RowToDatadogEvent", ParDo.of(new RowToDatadogEventFn()));

      DatadogIO.Write.Builder writeBuilder = DatadogIO.writeBuilder()
          .withUrl(configuration.getUrl())
          .withApiKey(configuration.getApiKey());

      if (configuration.getBatchCount() != null) {
        writeBuilder = writeBuilder.withBatchCount(configuration.getBatchCount());
      }
      if (configuration.getMaxBufferSize() != null) {
        writeBuilder = writeBuilder.withMaxBufferSize(configuration.getMaxBufferSize());
      }
      if (configuration.getParallelism() != null) {
        writeBuilder = writeBuilder.withParallelism(configuration.getParallelism());
      }

      events.apply("WriteToDatadog", writeBuilder.build());

      return PCollectionRowTuple.empty(input.getPipeline());
    }
  }

  static class RowToDatadogEventFn extends DoFn<Row, DatadogEvent> {
    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<DatadogEvent> out) {
      // The schema of the input Row should match the expected structure for a DatadogEvent.
      // This is a simplified conversion. A real implementation would need to handle nested
      // structures and metadata correctly.
      Schema schema = row.getSchema();
      // This is a placeholder for the actual conversion logic.
      // You need to extract the fields from the Row and create a DatadogEvent.
      // The Row schema is dynamic, so you need to inspect it.
      // For this example, we'll assume a simple schema.
      String message = row.getString("message");
      DatadogEvent event = DatadogEvent.newBuilder()
              .withMessage(message)
              .build();
      out.output(event);
    }
  }
}
