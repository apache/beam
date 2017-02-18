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

package org.apache.beam.runners.spark;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.IOException;
import org.apache.beam.runners.core.UnboundedReadFromBoundedSource;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedReadFromUnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.ValueWithRecordId;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.commons.io.FileUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The SparkRunner translate operations defined on a pipeline to a representation executable
 * by Spark, and then submitting the job to Spark to be executed. If we wanted to run a Beam
 * pipeline with the default options of a single threaded spark instance in local mode, we would do
 * the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineResult result = (SparkPipelineResult) p.run();
 * }
 *
 * <p>To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
 * options.setSparkMaster("spark://host:port");
 * SparkPipelineResult result = (SparkPipelineResult) p.run();
 * }
 */
public final class TestSparkRunner extends PipelineRunner<SparkPipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(TestSparkRunner.class);

  private SparkRunner delegate;
  private boolean isForceStreaming;
  private int expectedNumberOfAssertions = 0;

  private TestSparkRunner(SparkPipelineOptions options) {
    this.delegate = SparkRunner.fromOptions(options);
    this.isForceStreaming = options.isForceStreaming();
  }

  public static TestSparkRunner fromOptions(PipelineOptions options) {
    // Default options suffice to set it up as a test runner
    SparkPipelineOptions sparkOptions =
        PipelineOptionsValidator.validate(SparkPipelineOptions.class, options);
    return new TestSparkRunner(sparkOptions);
  }

  /**
   * Overrides for the test runner.
   */
  @SuppressWarnings("unchecked")
  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    // if the pipeline forces execution as a streaming pipeline,
    // and the source is an adapted unbounded source (as bounded),
    // read it as unbounded source via UnboundedReadFromBoundedSource.
    if (isForceStreaming && transform instanceof BoundedReadFromUnboundedSource) {
      return (OutputT) delegate.apply(new AdaptedBoundedAsUnbounded(
          (BoundedReadFromUnboundedSource) transform), input);
    } else {
      // no actual override, simply counts asserting transforms in the pipeline.
      if (transform instanceof PAssert.OneSideInputAssert
          || transform instanceof PAssert.GroupThenAssert
          || transform instanceof PAssert.GroupThenAssertForSingleton) {
        expectedNumberOfAssertions += 1;
      }

      return delegate.apply(transform, input);
    }
  }

  @Override
  public SparkPipelineResult run(Pipeline pipeline) {
    SparkPipelineOptions sparkOptions = pipeline.getOptions().as(SparkPipelineOptions.class);
    long timeout = sparkOptions.getForcedTimeout();
    SparkPipelineResult result = null;
    try {
      // clear state of Accumulators and Aggregators.
      AccumulatorSingleton.clear();
      GlobalWatermarkHolder.clear();

      TestPipelineOptions testPipelineOptions = pipeline.getOptions().as(TestPipelineOptions.class);
      LOG.info("About to run test pipeline " + sparkOptions.getJobName());
      result = delegate.run(pipeline);
      result.waitUntilFinish(Duration.millis(timeout));

      assertThat(result, testPipelineOptions.getOnCreateMatcher());
      assertThat(result, testPipelineOptions.getOnSuccessMatcher());

      // if the pipeline was executed in streaming mode, validate aggregators.
      if (isForceStreaming) {
        // validate assertion succeeded (at least once).
        int successAssertions = result.getAggregatorValue(PAssert.SUCCESS_COUNTER, Integer.class);
        assertThat(
            String.format(
                "Expected %d successful assertions, but found %d.",
                expectedNumberOfAssertions, successAssertions),
            successAssertions,
            is(expectedNumberOfAssertions));
        // validate assertion didn't fail.
        int failedAssertions = result.getAggregatorValue(PAssert.FAILURE_COUNTER, Integer.class);
        assertThat(
            String.format("Found %d failed assertions.", failedAssertions),
            failedAssertions,
            is(0));

        LOG.info(
            String.format(
                "Successfully asserted pipeline %s with %d successful assertions.",
                sparkOptions.getJobName(),
                successAssertions));
      }
    } finally {
      try {
        // cleanup checkpoint dir.
        FileUtils.deleteDirectory(new File(sparkOptions.getCheckpointDir()));
      } catch (IOException e) {
        throw new RuntimeException("Failed to clear checkpoint tmp dir.", e);
      }
    }
    return result;
  }

  private static class AdaptedBoundedAsUnbounded<T> extends PTransform<PBegin, PCollection<T>> {
    private final BoundedReadFromUnboundedSource<T> source;

    AdaptedBoundedAsUnbounded(BoundedReadFromUnboundedSource<T> source) {
      this.source = source;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PCollection<T> expand(PBegin input) {
      PTransform<PBegin, ? extends PCollection<ValueWithRecordId<T>>> replacingTransform =
          new UnboundedReadFromBoundedSource<>(source.getAdaptedSource());
      return (PCollection<T>) input.apply(replacingTransform)
          .apply("StripIds", ParDo.of(new ValueWithRecordId.StripIdsDoFn()));
    }
  }

}
