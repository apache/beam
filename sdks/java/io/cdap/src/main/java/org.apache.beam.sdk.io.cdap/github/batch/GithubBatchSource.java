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
package org.apache.beam.sdk.io.cdap.github.batch;

import com.google.common.base.Preconditions;
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
import org.apache.beam.sdk.io.cdap.github.common.DatasetTransformer;
import org.apache.beam.sdk.io.cdap.github.common.model.GitHubModel;
import org.apache.hadoop.io.NullWritable;

/** Plugin returns records from Github API V3. */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(GithubBatchSource.NAME)
@Description(GithubBatchSource.DESCRIPTION)
public class GithubBatchSource extends BatchSource<NullWritable, GitHubModel, StructuredRecord> {

  public static final String NAME = "GithubBatchSource";
  public static final String DESCRIPTION = "Reads data from Github API.";

  private final GithubBatchSourceConfig config;

  public GithubBatchSource(GithubBatchSourceConfig config) {
    this.config = config;
  }

  /**
   * prepareRun for the given batchSourceContext.
   *
   * @param batchSourceContext the batchSourceContext
   */
  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) {
    validateConfiguration(batchSourceContext.getFailureCollector());
    LineageRecorder lineageRecorder = new LineageRecorder(batchSourceContext, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());
    lineageRecorder.recordRead(
        "Read",
        "Reading Github data",
        Preconditions.checkNotNull(config.getSchema().getFields()).stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toList()));

    batchSourceContext.setInput(Input.of(config.referenceName, new GithubFormatProvider(config)));
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector =
        pipelineConfigurer.getStageConfigurer().getFailureCollector();
    IdUtils.validateReferenceName(config.referenceName, failureCollector);
    validateConfiguration(failureCollector);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void transform(
      KeyValue<NullWritable, GitHubModel> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(DatasetTransformer.transform(input.getValue(), config.getSchema()));
  }

  private void validateConfiguration(FailureCollector failureCollector) {
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
  }
}
