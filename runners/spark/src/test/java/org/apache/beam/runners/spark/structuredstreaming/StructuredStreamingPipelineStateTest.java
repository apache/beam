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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.spark.structuredstreaming.translation.SparkSessionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.SparkSession;
import org.joda.time.Duration;
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

  private static final long DEADLINE_SECONDS = 60;

  // Shared with the DoFn running in Spark's local executor threads, reset per test.
  private static volatile CountDownLatch started = new CountDownLatch(1);

  /** Signals started, then blocks until the task is killed. */
  private static class BlockingDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException {
      started.countDown();
      while (!TaskContext.get().isInterrupted()) {
        Thread.sleep(50);
      }
      c.output(c.element());
    }
  }

  private SparkStructuredStreamingPipelineResult runBlockingPipeline() throws InterruptedException {
    started = new CountDownLatch(1);
    Pipeline pipeline = Pipeline.create(getBatchOptions());
    pipeline.apply(Create.of("one", "two")).apply(ParDo.of(new BlockingDoFn()));
    SparkStructuredStreamingPipelineResult result =
        (SparkStructuredStreamingPipelineResult) pipeline.run();
    assertTrue("DoFn did not start", started.await(DEADLINE_SECONDS, TimeUnit.SECONDS));
    return result;
  }

  private ParDo.SingleOutput<String, String> printParDo(final String prefix) {
    return ParDo.of(
        new DoFn<String, String>() {

          @ProcessElement
          public void processElement(final ProcessContext c) {
            System.out.println(prefix + " " + c.element());
          }
        });
  }

  private SparkStructuredStreamingPipelineOptions getBatchOptions() {
    options.setRunner(SparkStructuredStreamingRunner.class);
    options.setStreaming(false); // explicit because options is reused throughout the test.
    return options;
  }

  private Pipeline getPipeline(final SparkStructuredStreamingPipelineOptions options) {

    final Pipeline pipeline = Pipeline.create(options);
    final String name = testName.getMethodName();

    pipeline.apply(Create.of("one", "two")).setCoder(StringUtf8Coder.of()).apply(printParDo(name));

    return pipeline;
  }

  private void testFailedPipeline(final SparkStructuredStreamingPipelineOptions options)
      throws Exception {

    SparkStructuredStreamingPipelineResult result = null;

    try {
      final Pipeline pipeline = Pipeline.create(options);
      pipeline
          .apply(Create.of("one", "two"))
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
    assertThat(result.waitUntilFinish(), is(PipelineResult.State.CANCELLED));
  }

  private void testCanceledPipeline(final SparkStructuredStreamingPipelineOptions options)
      throws Exception {

    final Pipeline pipeline = getPipeline(options);

    final SparkStructuredStreamingPipelineResult result =
        (SparkStructuredStreamingPipelineResult) pipeline.run();

    result.cancel();

    assertThat(result.getState(), is(PipelineResult.State.CANCELLED));
    assertThat(result.waitUntilFinish(), is(PipelineResult.State.CANCELLED));
  }

  private void testRunningPipeline(final SparkStructuredStreamingPipelineOptions options)
      throws Exception {

    final Pipeline pipeline = getPipeline(options);

    final SparkStructuredStreamingPipelineResult result =
        (SparkStructuredStreamingPipelineResult) pipeline.run();

    assertThat(result.getState(), is(PipelineResult.State.RUNNING));

    result.cancel();
    assertThat(result.waitUntilFinish(), is(PipelineResult.State.CANCELLED));
  }

  @Test
  public void testBatchPipelineRunningState() throws Exception {
    testRunningPipeline(getBatchOptions());
  }

  @Test
  public void testBatchPipelineCanceledState() throws Exception {
    testCanceledPipeline(getBatchOptions());
  }

  @Test
  public void testBatchPipelineFailedState() throws Exception {
    testFailedPipeline(getBatchOptions());
  }

  @Test
  public void testBatchPipelineTimeoutState() throws Exception {
    testTimeoutPipeline(getBatchOptions());
  }

  @Test
  public void testBatchCancelStopsRunningJob() throws Exception {
    SparkStructuredStreamingPipelineResult result = runBlockingPipeline();
    assertThat(result.cancel(), is(PipelineResult.State.CANCELLED));
    assertThat(result.waitUntilFinish(), is(PipelineResult.State.CANCELLED));
    assertTrue("owned session not stopped", SparkSession.getDefaultSession().isEmpty());
  }

  @Test
  public void testCancelKeepsSharedSession() throws Exception {
    SparkSession session = SparkSessionFactory.sessionBuilder("local[1]").getOrCreate();
    try {
      SparkStructuredStreamingPipelineResult result = runBlockingPipeline();
      assertThat(result.cancel(), is(PipelineResult.State.CANCELLED));
      assertThat(result.waitUntilFinish(), is(PipelineResult.State.CANCELLED));
      assertFalse("shared session stopped", session.sparkContext().isStopped());
    } finally {
      session.stop();
    }
  }

  /** The second pipeline shares the first session or creates a new one, both must end cleanly. */
  @Test
  public void testCancelFollowedImmediatelyBySecondPipeline() throws Exception {
    SparkStructuredStreamingPipelineResult first = runBlockingPipeline();
    assertThat(first.cancel(), is(PipelineResult.State.CANCELLED));
    Pipeline secondPipeline = Pipeline.create(getBatchOptions());
    secondPipeline.apply(Create.of("a", "b")).apply(printParDo("second"));
    SparkStructuredStreamingPipelineResult second =
        (SparkStructuredStreamingPipelineResult) secondPipeline.run();
    assertThat(first.waitUntilFinish(), is(PipelineResult.State.CANCELLED));
    assertThat(second.waitUntilFinish(), is(PipelineResult.State.DONE));
    assertTrue("session not stopped", SparkSession.getDefaultSession().isEmpty());
  }
}
