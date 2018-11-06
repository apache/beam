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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedIO;
import org.apache.beam.sdk.io.synthetic.SyntheticOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticStep;
import org.apache.beam.sdk.loadtests.metrics.MetricsPublisher;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Base class for all load tests. Provides common operations such as initializing source/step
 * options, creating a pipeline, etc.
 */
abstract class LoadTest<OptionsT extends LoadTestOptions> {

  private String metricsNamespace;

  OptionsT options;

  SyntheticBoundedIO.SyntheticSourceOptions sourceOptions;

  SyntheticStep.Options stepOptions;

  Pipeline pipeline;

  LoadTest(String[] args, Class<OptionsT> testOptions, String metricsNamespace) throws IOException {
    this.metricsNamespace = metricsNamespace;

    this.options = LoadTestOptions.readFromArgs(args, testOptions);

    this.sourceOptions =
        fromJsonString(options.getSourceOptions(), SyntheticBoundedIO.SyntheticSourceOptions.class);

    this.stepOptions = fromJsonString(options.getStepOptions(), SyntheticStep.Options.class);

    this.pipeline = Pipeline.create(options);
  }

  /** The load test pipeline implementation. */
  abstract void loadTest() throws IOException;

  /** Runs the load test. */
  public PipelineResult run() throws IOException {
    loadTest();

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    MetricsPublisher.toConsole(result, metricsNamespace);

    return result;
  }

  <T extends SyntheticOptions> T fromJsonString(String json, Class<T> type) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    T result = mapper.readValue(json, type);
    result.validate();
    return result;
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
