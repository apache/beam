/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.joda.time.Duration;
import org.junit.Test;

public class WindowedWordCountTest {
  private static final String[] WORDS_ARRAY = {
          "hi there", "hi", "hi sue bob",
          "hi sue", "", "bob hi"};
  private static final Long[] TIMESTAMPS_ARRAY = {
          60000L, 60000L, 60000L,
          120000L, 120000L, 120000L};
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
  private static final List<Long> TIMESTAMPS = Arrays.asList(TIMESTAMPS_ARRAY);
  private static final List<String> EXPECTED_COUNT_SET =
          ImmutableList.of("hi: 3", "there: 1", "sue: 1", "bob: 1",
                  "hi: 2", "sue: 1", "bob: 1");

  @Test
  public void testRun() throws Exception {
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    options.setRunner(SparkPipelineRunner.class);
    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    PCollection<String> inputWords = p.apply(Create.timestamped(WORDS, TIMESTAMPS))
            .setCoder(StringUtf8Coder.of());
    PCollection<String> windowedWords = inputWords
            .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<String> output = windowedWords.apply(new SimpleWordCountTest.CountWords());

    DataflowAssert.that(output).containsInAnyOrder(EXPECTED_COUNT_SET);

    EvaluationResult res = SparkPipelineRunner.create().run(p);
    res.close();
  }

}
