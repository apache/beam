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
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;

/** Imitation of CDAP {@link BatchSink} plugin. Used for integration test {@link CdapIO#write()}. */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(DBBatchSink.NAME)
@Description("Plugin writes <ID, NAME> in batch")
public class DBBatchSink extends BatchSink<StructuredRecord, String, String> {

  public static final String ID_FIELD = "id";
  public static final String NAME_FIELD = "name";
  public static final String NAME = "DBSink";

  private final DBConfig config;

  public DBBatchSink(DBConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
    context.addOutput(Output.of(config.referenceName, new DBOutputFormatProvider(config)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<String, String>> emitter)
      throws Exception {
    emitter.emit(new KeyValue<>(input.get(ID_FIELD), input.get(NAME_FIELD)));
  }
}
