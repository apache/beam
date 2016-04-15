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

import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestDataflowPipelineRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/**
 * End-to-end tests of WordCount.
 */
@RunWith(JUnit4.class)
public class WordCountIT extends BatchE2ETest {

  @Test
  @Category(RunnableOnService.class)
  public void testE2EWordCountOnDataflow() throws Exception {
    String jobName = "wordcount-" + generateTestIdentifier() + "-prod";
    String[] args = {
        "--jobName=" + jobName,
        "--runner=com.google.cloud.dataflow.sdk.testing.TestDataflowPipelineRunner",
        "--stagingLocation=gs://apache-beam-testing-temp-storage/staging/" + jobName,
        "--output=gs://apache-beam-testing-temp-storage/output/" + jobName + "/results",
        "--workerLogLevelOverrides="
        + "{\"com.google.cloud.dataflow.sdk.util.UploadIdResponseInterceptor\":\"DEBUG\"}"};

    WordCount.main(args);
    PipelineResult result = TestDataflowPipelineRunner.getPipelineResultByJobName(jobName);

    assertNotNull(result);
    assertEquals(PipelineResult.State.DONE, result.getState());
  }
}
