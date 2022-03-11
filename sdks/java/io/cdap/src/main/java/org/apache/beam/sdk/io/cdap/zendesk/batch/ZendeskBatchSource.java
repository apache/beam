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
package org.apache.beam.sdk.io.cdap.zendesk.batch;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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
import io.cdap.plugin.common.LineageRecorder;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.cdap.zendesk.common.config.BaseZendeskSourceConfig;
import org.apache.hadoop.io.NullWritable;

/** Source plugin to read data from Zendesk. */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(ZendeskBatchSource.NAME)
@Description("Read data from Zendesk.")
public class ZendeskBatchSource
    extends BatchSource<NullWritable, StructuredRecord, StructuredRecord> {

  public static final String NAME = "Zendesk";

  private final ZendeskBatchSourceConfig config;

  public ZendeskBatchSource(ZendeskBatchSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector =
        pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    if (!config.containsMacro(BaseZendeskSourceConfig.PROPERTY_OBJECTS_TO_PULL)) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema(failureCollector));
    }
  }

  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) {
    FailureCollector failureCollector = batchSourceContext.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    Schema schema = config.getSchema(failureCollector);
    String objectToPull = config.getObjectsToPull().iterator().next();

    LineageRecorder lineageRecorder = new LineageRecorder(batchSourceContext, config.referenceName);
    lineageRecorder.createExternalDataset(schema);
    lineageRecorder.recordRead(
        "Read",
        "Read from Zendesk",
        Preconditions.checkNotNull(schema.getFields()).stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toList()));
    batchSourceContext.setInput(
        Input.of(
            config.referenceName,
            new ZendeskInputFormatProvider(
                config,
                Collections.singletonList(objectToPull),
                ImmutableMap.of(objectToPull, schema.toString()))));
  }

  @Override
  public void transform(
      KeyValue<NullWritable, StructuredRecord> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue());
  }
}
