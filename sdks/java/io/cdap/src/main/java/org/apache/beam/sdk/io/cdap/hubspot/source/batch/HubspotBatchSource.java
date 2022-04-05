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
package org.apache.beam.sdk.io.cdap.hubspot.source.batch;

import com.google.gson.JsonElement;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.LineageRecorder;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.cdap.hubspot.common.HubspotHelper;
import org.apache.beam.sdk.io.cdap.hubspot.common.SourceHubspotConfig;
import org.apache.hadoop.io.NullWritable;

/** Plugin reads Hubspot objects in batch. */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(HubspotBatchSource.NAME)
@Description("Plugin reads Hubspot objects in batch")
public class HubspotBatchSource extends BatchSource<NullWritable, JsonElement, StructuredRecord> {

  private final SourceHubspotConfig config;

  public static final String NAME = "Hubspot";

  public HubspotBatchSource(SourceHubspotConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    validateConfiguration(pipelineConfigurer.getStageConfigurer().getFailureCollector());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  /**
   * Prepare Hubspot objects.
   *
   * @param context the batch source context
   * @throws Exception on issues with reading the hubspot objects
   */
  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    validateConfiguration(context.getFailureCollector());
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());
    lineageRecorder.recordRead(
        "Reads",
        "Reading Hubspot objects",
        config.getSchema().getFields().stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toList()));
    context.setInput(Input.of(NAME, new HubspotInputFormatProvider(config)));
  }

  @Override
  public void transform(
      KeyValue<NullWritable, JsonElement> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(HubspotHelper.transform(input.getValue().toString(), config));
  }

  private void validateConfiguration(FailureCollector failureCollector) {
    IdUtils.validateReferenceName(config.referenceName, failureCollector);
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
  }
}
