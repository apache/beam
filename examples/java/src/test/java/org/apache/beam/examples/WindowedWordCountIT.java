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
package org.apache.beam.examples;

import java.io.IOException;
import java.util.Date;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.BigqueryMatcher;
import org.apache.beam.sdk.testing.StreamingIT;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end integration test of {@link WindowedWordCount}.
 */
@RunWith(JUnit4.class)
public class WindowedWordCountIT {

  private static final String DEFAULT_INPUT =
      "gs://apache-beam-samples/shakespeare/winterstale-personae";
  private static final String DEFAULT_OUTPUT_CHECKSUM = "cd5b52939257e12428a9fa085c32a84dd209b180";

  /**
   * Options for the {@link WindowedWordCount} Integration Test.
   */
  public interface WindowedWordCountITOptions
      extends WindowedWordCount.Options, TestPipelineOptions, StreamingOptions {
  }

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testWindowedWordCountInBatch() throws IOException {
    testWindowedWordCountPipeline(false /* isStreaming */);
  }

  @Test
  @Category(StreamingIT.class)
  public void testWindowedWordCountInStreaming() throws IOException {
    testWindowedWordCountPipeline(true /* isStreaming */);
  }

  private void testWindowedWordCountPipeline(boolean isStreaming) throws IOException {
    WindowedWordCountITOptions options =
        TestPipeline.testingPipelineOptions().as(WindowedWordCountITOptions.class);
    options.setStreaming(isStreaming);
    options.setInputFile(DEFAULT_INPUT);

    // Note: currently unused because the example writes to BigQuery, but WindowedWordCount.Options
    // are tightly coupled to WordCount.Options, where the option is required.
    options.setOutput(IOChannelUtils.resolve(
        options.getTempRoot(),
        String.format("WindowedWordCountIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
        "output",
        "results"));

    String query = String.format("SELECT word, SUM(count) FROM [%s:%s.%s] GROUP BY word",
        options.getProject(), options.getBigQueryDataset(), options.getBigQueryTable());
    options.setOnSuccessMatcher(
        new BigqueryMatcher(
            options.getAppName(), options.getProject(), query, DEFAULT_OUTPUT_CHECKSUM));

    WindowedWordCount.main(TestPipeline.convertToArgs(options));
  }
}
