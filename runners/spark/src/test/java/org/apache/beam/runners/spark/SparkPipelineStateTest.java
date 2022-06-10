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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.joda.time.Duration.millis;
import static org.junit.Assert.assertThrows;

import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.ClassRule;
import org.junit.Test;

/** This suite tests that various scenarios result in proper states of the pipeline. */
public class SparkPipelineStateTest implements Serializable {

  @ClassRule public static SparkContextRule contextRule = new SparkContextRule();

  private static class CustomException extends RuntimeException {
    CustomException(final String message) {
      super(message);
    }
  }

  private static class FailAlways extends SimpleFunction<String, String> {
    @Override
    public String apply(final String input) {
      throw new CustomException(FAILED_THE_BATCH_INTENTIONALLY);
    }
  }

  private static final String FAILED_THE_BATCH_INTENTIONALLY = "Failed the batch intentionally";

  private Pipeline createPipeline(
      boolean isStreaming, @Nullable SimpleFunction<String, String> mapFun) {
    SparkContextOptions options = contextRule.createPipelineOptions();
    options.setRunner(SparkRunner.class);
    options.setStreaming(isStreaming);

    Pipeline pipeline = Pipeline.create(options);
    PTransform<PBegin, PCollection<String>> values =
        isStreaming
            ? CreateStream.of(StringUtf8Coder.of(), millis(1), false).nextBatch("one", "two")
            : Create.of("one", "two");

    PCollection<String> collection = pipeline.apply(values).setCoder(StringUtf8Coder.of());
    if (mapFun != null) {
      collection.apply(MapElements.via(mapFun));
    }
    return pipeline;
  }

  private void testFailedPipeline(boolean isStreaming) throws Exception {
    Pipeline pipeline = createPipeline(isStreaming, new FailAlways());
    SparkPipelineResult result = (SparkPipelineResult) pipeline.run();

    PipelineExecutionException e =
        assertThrows(PipelineExecutionException.class, () -> result.waitUntilFinish());
    assertThat(e.getCause(), instanceOf(CustomException.class));
    assertThat(e.getCause().getMessage(), is(FAILED_THE_BATCH_INTENTIONALLY));
    assertThat(result.getState(), is(PipelineResult.State.FAILED));
    result.cancel();
  }

  private void testWaitUntilFinishedTimeout(boolean isStreaming) throws Exception {
    Pipeline pipeline = createPipeline(isStreaming, null);
    SparkPipelineResult result = (SparkPipelineResult) pipeline.run();
    result.waitUntilFinish(millis(1));

    // Wait timed out, pipeline is still running
    assertThat(result.getState(), is(PipelineResult.State.RUNNING));
    result.cancel();
  }

  private void testCanceledPipeline(boolean isStreaming) throws Exception {
    Pipeline pipeline = createPipeline(isStreaming, null);
    SparkPipelineResult result = (SparkPipelineResult) pipeline.run();
    result.cancel();
    assertThat(result.getState(), is(PipelineResult.State.CANCELLED));
  }

  private void testRunningPipeline(boolean isStreaming) throws Exception {
    Pipeline pipeline = createPipeline(isStreaming, null);
    SparkPipelineResult result = (SparkPipelineResult) pipeline.run();
    assertThat(result.getState(), is(PipelineResult.State.RUNNING));
    result.cancel();
  }

  @Test
  public void testStreamingPipelineRunningState() throws Exception {
    testRunningPipeline(true);
  }

  @Test
  public void testBatchPipelineRunningState() throws Exception {
    testRunningPipeline(false);
  }

  @Test
  public void testStreamingPipelineCanceledState() throws Exception {
    testCanceledPipeline(true);
  }

  @Test
  public void testBatchPipelineCanceledState() throws Exception {
    testCanceledPipeline(false);
  }

  @Test
  public void testStreamingPipelineFailedState() throws Exception {
    testFailedPipeline(true);
  }

  @Test
  public void testBatchPipelineFailedState() throws Exception {
    testFailedPipeline(false);
  }

  @Test
  public void testStreamingPipelineWaitTimeout() throws Exception {
    testWaitUntilFinishedTimeout(true);
  }

  @Test
  public void testBatchPipelineWaitTimeout() throws Exception {
    testWaitUntilFinishedTimeout(false);
  }
}
