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
package org.apache.beam.runners.spark.translation.streaming;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableSet;

import org.apache.beam.runners.spark.SparkStreamingPipelineOptions;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SimpleWordCountTest;
import org.apache.beam.runners.spark.SparkPipelineRunner;
import org.apache.beam.runners.spark.translation.streaming.utils.DataflowAssertStreaming;

import org.joda.time.Duration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SimpleStreamingWordCountTest {

  private static final String[] WORDS_ARRAY = {
      "hi there", "hi", "hi sue bob", "hi sue", "", "bob hi"};
  private static final List<Iterable<String>> WORDS_QUEUE =
      Collections.<Iterable<String>>singletonList(Arrays.asList(WORDS_ARRAY));
  private static final Set<String> EXPECTED_COUNT_SET =
      ImmutableSet.of("hi: 5", "there: 1", "sue: 2", "bob: 2");
  private static final long TEST_TIMEOUT_MSEC = 1000L;

  @Test
  public void testRun() throws Exception {
    SparkStreamingPipelineOptions options = SparkStreamingPipelineOptionsFactory.create();
    options.setAppName(this.getClass().getSimpleName());
    options.setRunner(SparkPipelineRunner.class);
    options.setTimeout(TEST_TIMEOUT_MSEC);// run for one interval
    Pipeline p = Pipeline.create(options);

    PCollection<String> inputWords =
        p.apply(CreateStream.fromQueue(WORDS_QUEUE)).setCoder(StringUtf8Coder.of());
    PCollection<String> windowedWords = inputWords
        .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(1))));

    PCollection<String> output = windowedWords.apply(new SimpleWordCountTest.CountWords());

    DataflowAssert.thatIterable(output.apply(View.<String>asIterable()))
        .containsInAnyOrder(EXPECTED_COUNT_SET);

    EvaluationResult res = SparkPipelineRunner.create(options).run(p);
    res.close();

    DataflowAssertStreaming.assertNoFailures(res);
  }
}