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
package org.apache.beam.sdk.loadtests;

import static org.apache.beam.sdk.io.synthetic.SyntheticOptions.fromJsonString;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticStep;
import org.apache.beam.sdk.loadtests.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.BigQueryResultsPublisher;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Base class for all load tests. Provides common operations such as initializing source/step
 * options, creating a pipeline, etc.
 */
abstract class LoadTest<OptionsT extends LoadTestOptions> {

  private String metricsNamespace;

  protected TimeMonitor<byte[], byte[]> runtimeMonitor;

  protected OptionsT options;

  protected SyntheticSourceOptions sourceOptions;

  protected SyntheticStep.Options stepOptions;

  protected Pipeline pipeline;

  LoadTest(String[] args, Class<OptionsT> testOptions, String metricsNamespace) throws IOException {
    this.metricsNamespace = metricsNamespace;
    this.runtimeMonitor = new TimeMonitor<>(metricsNamespace, "runtime");
    this.options = LoadTestOptions.readFromArgs(args, testOptions);
    this.sourceOptions = fromJsonString(options.getSourceOptions(), SyntheticSourceOptions.class);
    this.stepOptions = fromJsonString(options.getStepOptions(), SyntheticStep.Options.class);
    this.pipeline = Pipeline.create(options);
  }

  /** The load test pipeline implementation. */
  abstract void loadTest() throws IOException;

  /**
   * Runs the load test, collects and publishes test results to various data store and/or console.
   */
  public PipelineResult run() throws IOException {
    long testStartTime = System.currentTimeMillis();

    loadTest();

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    LoadTestResult testResult = LoadTestResult.create(result, metricsNamespace, testStartTime);

    ConsoleResultPublisher.publish(testResult);

    if (options.getPublishToBigQuery()) {
      publishResultToBigQuery(testResult);
    }
    return result;
  }

  private void publishResultToBigQuery(LoadTestResult testResult) {
    String dataset = options.getBigQueryDataset();
    String table = options.getBigQueryTable();
    checkBigQueryOptions(dataset, table);

    ImmutableMap<String, String> schema =
        ImmutableMap.<String, String>builder()
            .put("timestamp", "timestamp")
            .put("runtime", "float")
            .put("total_bytes_count", "integer")
            .build();

    BigQueryResultsPublisher.create(dataset, schema).publish(testResult, table);
  }

  private static void checkBigQueryOptions(String dataset, String table) {
    Preconditions.checkArgument(
        dataset != null,
        "Please specify --bigQueryDataset option if you want to publish to BigQuery");

    Preconditions.checkArgument(
        table != null, "Please specify --bigQueryTable option if you want to publish to BigQuery");
  }

  Optional<SyntheticStep> createStep(String stepOptions) throws IOException {
    if (stepOptions != null && !stepOptions.isEmpty()) {
      return Optional.of(
          new SyntheticStep(fromJsonString(stepOptions, SyntheticStep.Options.class)));
    } else {
      return Optional.empty();
    }
  }

  PCollection<KV<byte[], byte[]>> applyStepIfPresent(
      PCollection<KV<byte[], byte[]>> input, String name, Optional<SyntheticStep> syntheticStep) {

    if (syntheticStep.isPresent()) {
      return input.apply(name, ParDo.of(syntheticStep.get()));
    } else {
      return input;
    }
  }
}
