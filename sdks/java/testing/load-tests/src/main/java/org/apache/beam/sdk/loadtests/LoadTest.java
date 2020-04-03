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
import static org.apache.beam.sdk.loadtests.JobFailure.handleFailure;

import com.google.cloud.Timestamp;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticStep;
import org.apache.beam.sdk.io.synthetic.SyntheticUnboundedSource;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.BigQueryResultsPublisher;
import org.apache.beam.sdk.testutils.publishing.ConsoleResultPublisher;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;

/**
 * Base class for all load tests. Provides common operations such as initializing source/step
 * options, creating a pipeline, etc.
 */
abstract class LoadTest<OptionsT extends LoadTestOptions> {

  private String metricsNamespace;

  protected TimeMonitor<KV<byte[], byte[]>> runtimeMonitor;

  protected OptionsT options;

  protected SyntheticSourceOptions sourceOptions;

  protected Pipeline pipeline;

  LoadTest(String[] args, Class<OptionsT> testOptions, String metricsNamespace) throws IOException {
    this.metricsNamespace = metricsNamespace;
    this.runtimeMonitor = new TimeMonitor<>(metricsNamespace, "runtime");
    this.options = LoadTestOptions.readFromArgs(args, testOptions);
    this.sourceOptions = fromJsonString(options.getSourceOptions(), SyntheticSourceOptions.class);

    this.pipeline = Pipeline.create(options);
  }

  PTransform<PBegin, PCollection<KV<byte[], byte[]>>> readFromSource(
      SyntheticSourceOptions sourceOptions) {
    if (options.isStreaming()) {
      return Read.from(new SyntheticUnboundedSource(sourceOptions));
    } else {
      return Read.from(new SyntheticBoundedSource(sourceOptions));
    }
  }

  /** The load test pipeline implementation. */
  abstract void loadTest() throws IOException;

  /**
   * Runs the load test, collects and publishes test results to various data store and/or console.
   */
  public PipelineResult run() throws IOException {
    Timestamp timestamp = Timestamp.now();

    loadTest();

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish(Duration.standardMinutes(options.getLoadTestTimeout()));

    String testId = UUID.randomUUID().toString();
    List metrics = readMetrics(timestamp, pipelineResult, testId);

    ConsoleResultPublisher.publish(metrics, testId, timestamp.toString());

    handleFailure(pipelineResult, metrics);

    if (options.getPublishToBigQuery()) {
      publishResultsToBigQuery(metrics);
    }

    return pipelineResult;
  }

  private List<NamedTestResult> readMetrics(
      Timestamp timestamp, PipelineResult result, String testId) {
    MetricsReader reader = new MetricsReader(result, metricsNamespace);

    NamedTestResult runtime =
        NamedTestResult.create(
            testId,
            timestamp.toString(),
            "runtime_sec",
            (reader.getEndTimeMetric("runtime") - reader.getStartTimeMetric("runtime")) / 1000D);

    NamedTestResult totalBytes =
        NamedTestResult.create(
            testId,
            timestamp.toString(),
            "total_bytes_count",
            reader.getCounterMetric("totalBytes.count"));

    return Arrays.asList(runtime, totalBytes);
  }

  private void publishResultsToBigQuery(List<NamedTestResult> testResults) {
    String dataset = options.getBigQueryDataset();
    String table = options.getBigQueryTable();
    checkBigQueryOptions(dataset, table);

    BigQueryResultsPublisher.create(dataset, NamedTestResult.getSchema())
        .publish(testResults, table);
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

  <T> PCollection<T> applyWindowing(PCollection<T> input) {
    return applyWindowing(input, options.getInputWindowDurationSec());
  }

  <T> PCollection<T> applyWindowing(PCollection<T> input, @Nullable Long windowDuration) {
    if (windowDuration == null) {
      return input.apply(Window.into(new GlobalWindows()));
    } else {
      return input.apply(Window.into(FixedWindows.of(Duration.standardSeconds(windowDuration))));
    }
  }
}
