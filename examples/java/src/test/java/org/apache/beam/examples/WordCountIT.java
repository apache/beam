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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.beam.examples.WordCount.WordCountOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestDataflowPipelineRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import com.google.common.base.Joiner;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end tests of WordCount.
 */
@RunWith(JUnit4.class)
public class WordCountIT {

  /**
   * Options for the WordCount Integration Test.
   */
  public static interface WordCountITOptions extends TestPipelineOptions,
         WordCountOptions, DataflowPipelineOptions {
  }

  @Test
  public void testE2EWordCount() throws Exception {
    PipelineOptionsFactory.register(WordCountITOptions.class);
    WordCountITOptions options = TestPipeline.testingPipelineOptions().as(WordCountITOptions.class);
    options.setOutput(Joiner.on("/").join(new String[]{options.getTempRoot(),
        options.getJobName(), "output", "results"}));

    WordCount.main(TestPipeline.convertToArgs(options));
    PipelineResult result =
        TestDataflowPipelineRunner.getPipelineResultByJobName(options.getJobName());

    assertNotNull("Result was null.", result);
    assertEquals("Pipeline state was not done.", PipelineResult.State.DONE, result.getState());
  }
}
