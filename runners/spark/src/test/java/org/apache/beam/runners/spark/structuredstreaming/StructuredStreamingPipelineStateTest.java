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
package org.apache.beam.runners.spark.structuredstreaming;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import java.io.Serializable;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** This suite tests that various scenarios result in proper states of the pipeline. */
@RunWith(JUnit4.class)
public class StructuredStreamingPipelineStateTest implements Serializable {

  private static class MyCustomException extends RuntimeException {

    MyCustomException(final String message) {
      super(message);
    }
  }

  private final transient SparkStructuredStreamingPipelineOptions options =
      PipelineOptionsFactory.create().as(SparkStructuredStreamingPipelineOptions.class);

  @Rule public transient TestName testName = new TestName();

  private static final String FAILED_THE_BATCH_INTENTIONALLY = "Failed the batch intentionally";

  private ParDo.SingleOutput<String, String> printParDo(final String prefix) {
    return ParDo.of(
        new DoFn<String, String>() {

          @ProcessElement
          public void processElement(final ProcessContext c) {
            System.out.println(prefix + " " + c.element());
          }
        });
  }

  private PTransform<PBegin, PCollection<String>> getValues(
      final SparkStructuredStreamingPipelineOptions options) {
    final boolean doNotSyncWithWatermark = false;
    return options.isStreaming()
        ? CreateStream.of(StringUtf8Coder.of(), Duration.millis(1), doNotSyncWithWatermark)
            .nextBatch("one", "two")
        : Create.of("one", "two");
  }

  private SparkStructuredStreamingPipelineOptions getStreamingOptions() {
    options.setRunner(SparkStructuredStreamingRunner.class);
    options.setStreaming(true);
    return options;
  }

  private SparkStructuredStreamingPipelineOptions getBatchOptions() {
    options.setRunner(SparkStructuredStreamingRunner.class);
    options.setStreaming(false); // explicit because options is reused throughout the test.
    return options;
  }

  private Pipeline getPipeline(final SparkStructuredStreamingPipelineOptions options) {

    final Pipeline pipeline = Pipeline.create(options);
    final String name = testName.getMethodName() + "(isStreaming=" + options.isStreaming() + ")";

    pipeline.apply(getValues(options)).setCoder(StringUtf8Coder.of()).apply(printParDo(name));

    return pipeline;
  }

  private void testFailedPipeline(final SparkStructuredStreamingPipelineOptions options)
      throws Exception {

    SparkStructuredStreamingPipelineResult result = null;

    try {
      final Pipeline pipeline = Pipeline.create(options);
      pipeline
          .apply(getValues(options))
          .setCoder(StringUtf8Coder.of())
          .apply(
              MapElements.via(
                  new SimpleFunction<String, String>() {

                    @Override
                    public String apply(final String input) {
                      throw new MyCustomException(FAILED_THE_BATCH_INTENTIONALLY);
                    }
                  }));

      result = (SparkStructuredStreamingPipelineResult) pipeline.run();
      result.waitUntilFinish();
    } catch (final Exception e) {
      assertThat(e, instanceOf(Pipeline.PipelineExecutionException.class));
      assertThat(e.getCause(), instanceOf(MyCustomException.class));
      assertThat(e.getCause().getMessage(), is(FAILED_THE_BATCH_INTENTIONALLY));
      assertThat(result.getState(), is(PipelineResult.State.FAILED));
      result.cancel();
      return;
    }

    fail("An injected failure did not affect the pipeline as expected.");
  }

  private void testTimeoutPipeline(final SparkStructuredStreamingPipelineOptions options)
      throws Exception {

    final Pipeline pipeline = getPipeline(options);

    final SparkStructuredStreamingPipelineResult result =
        (SparkStructuredStreamingPipelineResult) pipeline.run();

    result.waitUntilFinish(Duration.millis(1));

    assertThat(result.getState(), is(PipelineResult.State.RUNNING));

    result.cancel();
  }

  private void testCanceledPipeline(final SparkStructuredStreamingPipelineOptions options)
      throws Exception {

    final Pipeline pipeline = getPipeline(options);

    final SparkStructuredStreamingPipelineResult result =
        (SparkStructuredStreamingPipelineResult) pipeline.run();

    result.cancel();

    assertThat(result.getState(), is(PipelineResult.State.CANCELLED));
  }

  private void testRunningPipeline(final SparkStructuredStreamingPipelineOptions options)
      throws Exception {

    final Pipeline pipeline = getPipeline(options);

    final SparkStructuredStreamingPipelineResult result =
        (SparkStructuredStreamingPipelineResult) pipeline.run();

    assertThat(result.getState(), is(PipelineResult.State.RUNNING));

    result.cancel();
  }

  @Ignore("TODO: Reactivate with streaming.")
  @Test
  public void testStreamingPipelineRunningState() throws Exception {
    testRunningPipeline(getStreamingOptions());
  }

  @Test
  public void testBatchPipelineRunningState() throws Exception {
    testRunningPipeline(getBatchOptions());
  }

  @Ignore("TODO: Reactivate with streaming.")
  @Test
  public void testStreamingPipelineCanceledState() throws Exception {
    testCanceledPipeline(getStreamingOptions());
  }

  @Test
  public void testBatchPipelineCanceledState() throws Exception {
    testCanceledPipeline(getBatchOptions());
  }

  @Ignore("TODO: Reactivate with streaming.")
  @Test
  public void testStreamingPipelineFailedState() throws Exception {
    testFailedPipeline(getStreamingOptions());
  }

  @Test
  public void testBatchPipelineFailedState() throws Exception {
    testFailedPipeline(getBatchOptions());
  }

  @Ignore("TODO: Reactivate with streaming.")
  @Test
  public void testStreamingPipelineTimeoutState() throws Exception {
    testTimeoutPipeline(getStreamingOptions());
  }

  @Test
  public void testBatchPipelineTimeoutState() throws Exception {
    testTimeoutPipeline(getBatchOptions());
  }
}
