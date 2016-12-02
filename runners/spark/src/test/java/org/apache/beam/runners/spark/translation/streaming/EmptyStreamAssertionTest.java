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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.aggregators.ClearAggregatorsRule;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.runners.spark.translation.streaming.utils.PAssertStreaming;
import org.apache.beam.runners.spark.translation.streaming.utils.SparkTestPipelineOptionsForStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Test that {@link PAssertStreaming} can tell if the stream is empty.
 */
public class EmptyStreamAssertionTest implements Serializable {

  private static final String EXPECTED_ERR =
      "Success aggregator should be greater than zero.\n"
          + "Expected: not <0>\n"
          + "     but: was <0>";

  @Rule
  public TemporaryFolder checkpointParentDir = new TemporaryFolder();

  @Rule
  public SparkTestPipelineOptionsForStreaming commonOptions =
      new SparkTestPipelineOptionsForStreaming();

  @Rule
  public ClearAggregatorsRule clearAggregatorsRule = new ClearAggregatorsRule();

  @Test
  public void testAssertion() throws Exception {
    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(checkpointParentDir);
    options.setStreaming(true);

    Duration windowDuration = new Duration(options.getBatchIntervalMillis());

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> output =
        pipeline
            .apply(CreateStream.fromQueue(Collections.<Iterable<String>>emptyList()))
            .setCoder(StringUtf8Coder.of())
            .apply(Window.<String>into(FixedWindows.of(windowDuration)));

    try {
      PAssertStreaming.runAndAssertContents(pipeline, output, new String[0],
          Duration.standardSeconds(1L));
    } catch (AssertionError e) {
      assertTrue("Expected error message: " + EXPECTED_ERR + " but got: " + e.getMessage(),
          e.getMessage().equals(EXPECTED_ERR));
      return;
    }
    fail("assertion should have failed");
    throw new RuntimeException("unreachable");
  }
}
