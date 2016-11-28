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

import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.runners.spark.translation.streaming.utils.SparkTestPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.spark.SparkException;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import avro.shaded.com.google.common.collect.Lists;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * This suite tests that verifies a Beam over spark pipeline fails fast upon a failed batch and
 * does not keep on running upon encountering a batch failure.
 */
public class FailFastOnMicrobatchFailureTest implements Serializable {

  private static final transient AtomicInteger counter = new AtomicInteger(0);
  private static final String FAILED_THE_BATCH_INTENTIONALLY = "Failed the batch intentionally";
  private static final String[] WORDS_ARRAY_1 = { "one", "two", "three", "four" };
  private static final String[] WORDS_ARRAY_2 = { "five", "six", "seven", "eight" };

  private static final List<Iterable<String>> WORDS_QUEUE = Lists.<Iterable<String>>newArrayList(
      Arrays.asList(WORDS_ARRAY_1),
      Arrays.asList(WORDS_ARRAY_2));

  @Rule
  public transient SparkTestPipelineOptions commonOptions = new SparkTestPipelineOptions();

  private Pipeline buildPipeline(SparkPipelineOptions options) {
    final Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(CreateStream.fromQueue(WORDS_QUEUE)).setCoder(StringUtf8Coder.of())
        .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(1))))
        .apply(MapElements.via(new SimpleFunction<String, String>() {

          @Override
          public String apply(String input) {
            if (counter.getAndIncrement() == 2) {
              throw new RuntimeException(FAILED_THE_BATCH_INTENTIONALLY);
            } else {
              return input;
            }
          }
        }))
        .apply(WithKeys.<String, String>of("dummy"))
        .apply(GroupByKey.<String, String>create())
        .apply(Values.<Iterable<String>>create());

    return pipeline;
  }

  private EvaluationResult runPipeline() {
    //System.setProperty("beam.spark.test.reuseSparkContext", "false");
    final SparkPipelineOptions options = commonOptions.getOptions();
    options.setStreaming(true);
    final Pipeline pipeline = buildPipeline(options);
    return (EvaluationResult) pipeline.run();
  }

  @Test
  public void testFailFastOnMicrobatchFailure() throws Exception {

    EvaluationResult evaluationResult = null;

    try {
      evaluationResult = runPipeline();
      evaluationResult.waitUntilFinish();
    } catch (Exception exception) {
      final Throwable firstCause = exception.getCause();
      final Throwable rootCause = firstCause.getCause();
      assertThat(exception, instanceOf(SparkException.class));
      assertThat(firstCause, instanceOf(UserCodeException.class));
      assertThat(rootCause.getMessage(), is(FAILED_THE_BATCH_INTENTIONALLY));
      assertThat(evaluationResult.getState(), is(PipelineResult.State.FAILED));
      return;
    }

    Assert.fail("An injected failure did not seem to terminate the Streaming pipeline...");
  }
}
