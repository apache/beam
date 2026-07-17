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
package org.apache.beam.runners.kafka.streams;

import java.io.IOException;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.construction.Environments;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * A {@link PipelineRunner} for tests, so {@code TestPipeline}-based suites — including Beam's
 * {@code @ValidatesRunner} tests — can run against the Kafka Streams runner without a broker.
 *
 * <p>{@link #run} translates the pipeline and drives it to quiescence through a {@code
 * TopologyTestDriver} (via {@link KafkaStreamsTestRunner}), with the SDK harness in-process
 * (EMBEDDED environment). The returned {@link PipelineResult} is terminal ({@code DONE}) and
 * exposes the metrics the harness reported — which is what {@code
 * TestPipeline.verifyPAssertsSucceeded} uses to check that every {@code PAssert} in the pipeline
 * actually ran and succeeded.
 *
 * <p>A {@code PAssert} failure inside a DoFn fails the pipeline run — over the Fn API the DoFn's
 * error travels as an error string (carrying the assertion text), not as a Java {@link
 * AssertionError} object. {@link #run} additionally unwraps and rethrows an {@link AssertionError}
 * when one is present in the exception chain (e.g. assertions thrown runner-side). A {@code
 * PAssert} that silently never runs is caught by {@code TestPipeline.verifyPAssertsSucceeded}
 * comparing the success-counter metric against the number of assertions in the pipeline.
 *
 * <p>Select it with {@code --runner=org.apache.beam.runners.kafka.streams.TestKafkaStreamsRunner}
 * in {@code beamTestPipelineOptions}.
 */
public final class TestKafkaStreamsRunner extends PipelineRunner<PipelineResult> {

  private TestKafkaStreamsRunner() {}

  /** Called reflectively by {@link PipelineRunner#fromOptions}. */
  public static TestKafkaStreamsRunner fromOptions(PipelineOptions options) {
    return new TestKafkaStreamsRunner();
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    // Tests supply generic options; fill in what the Kafka Streams translation needs. The
    // pipeline's own options are the ones the translation reads.
    KafkaStreamsPipelineOptions options =
        pipeline.getOptions().as(KafkaStreamsPipelineOptions.class);
    if (options.getApplicationId() == null || options.getApplicationId().isEmpty()) {
      options.setApplicationId("ks-validates-runner-" + UUID.randomUUID());
    }
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);

    MetricResults metrics;
    try {
      metrics = KafkaStreamsTestRunner.run(pipeline);
    } catch (Throwable t) {
      // A PAssert failure throws an AssertionError inside the harness DoFn, which reaches us
      // wrapped in the bundle-processing exception chain. Rethrow the innermost AssertionError so
      // the test framework reports the actual assertion.
      for (Throwable current = t; current != null; current = current.getCause()) {
        if (current instanceof AssertionError) {
          throw (AssertionError) current;
        }
      }
      throw t;
    }
    return new TestKafkaStreamsPipelineResult(metrics);
  }

  /** Terminal result of a test run: state {@code DONE} with the harness-reported metrics. */
  private static final class TestKafkaStreamsPipelineResult implements PipelineResult {
    private final MetricResults metrics;

    TestKafkaStreamsPipelineResult(MetricResults metrics) {
      this.metrics = metrics;
    }

    @Override
    public State getState() {
      return State.DONE;
    }

    @Override
    public State cancel() throws IOException {
      throw new UnsupportedOperationException(
          "A TestKafkaStreamsRunner pipeline has already finished when its result is returned.");
    }

    @Override
    public State waitUntilFinish(@Nullable Duration duration) {
      return State.DONE;
    }

    @Override
    public State waitUntilFinish() {
      return State.DONE;
    }

    @Override
    public MetricResults metrics() {
      return metrics;
    }
  }
}
