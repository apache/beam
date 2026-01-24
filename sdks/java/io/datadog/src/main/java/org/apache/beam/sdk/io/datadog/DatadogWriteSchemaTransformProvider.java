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

@AutoService(SchemaTransformProvider.class)
public class DatadogWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<DatadogWriteSchemaTransformConfiguration> {

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

  protected static class DatadogWriteSchemaTransform extends SchemaTransform {
    private final DatadogWriteSchemaTransformConfiguration configuration;

    DatadogWriteSchemaTransform(DatadogWriteSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> inputRows = input.get(INPUT_TAG);

      PCollection<DatadogEvent> events =
          inputRows.apply("RowToDatadogEvent", ParDo.of(new RowToDatadogEventFn()));
      events.setCoder(DatadogEventCoder.of());

      DatadogIO.Write.Builder writeBuilder =
          DatadogIO.writeBuilder()
              .withUrl(configuration.getUrl())
              .withApiKey(configuration.getApiKey());

      Integer batchCount = configuration.getBatchCount();
      if (batchCount != null) {
        writeBuilder = writeBuilder.withBatchCount(batchCount);
      }
      Long maxBufferSize = configuration.getMaxBufferSize();
      if (maxBufferSize != null) {
        writeBuilder = writeBuilder.withMaxBufferSize(maxBufferSize);
      }
      Integer parallelism = configuration.getParallelism();
      if (parallelism != null) {
        writeBuilder = writeBuilder.withParallelism(parallelism);
      }

      events.apply("WriteToDatadog", writeBuilder.build());

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
