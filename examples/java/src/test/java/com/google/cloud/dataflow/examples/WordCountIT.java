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

package com.google.cloud.dataflow.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.dataflow.examples.WordCount.WordCountOptions;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.TestDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.testing.TestPipelineOptions;
import com.google.common.base.Joiner;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

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

    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> stringOpts = (Map<String, Object>) mapper.readValue(
        mapper.writeValueAsBytes(options), Map.class).get("options");
    System.out.println("TestE2EOutput1 - " + stringOpts);

    ArrayList<String> optArrayList = new ArrayList<>();
    for (Map.Entry<String, Object> entry : stringOpts.entrySet()) {
      optArrayList.add("--" + entry.getKey() + "=" + entry.getValue());
    }
    String[] args = optArrayList.toArray(new String[optArrayList.size()]);

    System.out.println("TestE2EOutput - " + Arrays.toString(args));

    WordCount.main(args);
    PipelineResult result =
        TestDataflowPipelineRunner.getPipelineResultByJobName(options.getJobName());

    assertNotNull(result);
    assertEquals(PipelineResult.State.DONE, result.getState());
  }
}
