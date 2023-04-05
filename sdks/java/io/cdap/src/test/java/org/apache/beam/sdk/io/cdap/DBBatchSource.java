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
package org.apache.beam.sdk.io.cdap;

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

/**
 * Imitation of CDAP {@link BatchSource} plugin. Used for integration test {@link CdapIO#read()}.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(DBBatchSource.NAME)
@Description("Plugin reads <ID, NAME> in batch")
public class DBBatchSource extends BatchSource<String, String, StructuredRecord> {

  private final DBConfig config;

  public static final String NAME = "DBSource";

  public DBBatchSource(DBConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    validateConfiguration(pipelineConfigurer.getStageConfigurer().getFailureCollector());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  /**
   * Prepare DB objects as it could be implemented in CDAP plugin.
   *
   * @param context the batch source context
   */
  @Override
  public void prepareRun(BatchSourceContext context) {
    validateConfiguration(context.getFailureCollector());
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());
    lineageRecorder.recordRead(
        "Reads",
        "Reading DB objects",
        config.getSchema().getFields().stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toList()));
    context.setInput(Input.of(NAME, new DBInputFormatProvider(config)));
  }

  @Override
  public void transform(KeyValue<String, String> input, Emitter<StructuredRecord> emitter) {
    StructuredRecord.Builder builder = StructuredRecord.builder(config.getSchema());
    builder.set("id", input.getKey());
    builder.set("name", input.getValue());
    emitter.emit(builder.build());
  }

  private void validateConfiguration(FailureCollector failureCollector) {
    IdUtils.validateReferenceName(config.referenceName, failureCollector);
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
  }
}
