/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TestPipeline}. */
@RunWith(JUnit4.class)
public class TestPipelineTest {
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void testCreationUsingDefaults() {
    assertNotNull(TestPipeline.create());
  }

  @Test
  public void testCreationOfPipelineOptions() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String stringOptions = mapper.writeValueAsString(
        ImmutableMap.of("options",
          ImmutableMap.<String, String>builder()
          .put("runner", DataflowPipelineRunner.class.getName())
          .put("project", "testProject")
          .put("apiRootUrl", "testApiRootUrl")
          .put("dataflowEndpoint", "testDataflowEndpoint")
          .put("tempLocation", "testTempLocation")
          .put("serviceAccountName", "testServiceAccountName")
          .put("serviceAccountKeyfile", "testServiceAccountKeyfile")
          .put("zone", "testZone")
          .put("numWorkers", "1")
          .put("diskSizeGb", "2")
          .build()));
    System.getProperties().put("dataflowOptions", stringOptions);
    TestDataflowPipelineOptions options = TestPipeline.getPipelineOptions();
    assertEquals(DataflowPipelineRunner.class, options.getRunner());
    assertEquals("TestPipelineTest", options.getAppName());
    assertEquals("testCreationOfPipelineOptions", options.getJobName());
    assertEquals("testProject", options.getProject());
    assertEquals("testApiRootUrl", options.getApiRootUrl());
    assertEquals("testDataflowEndpoint", options.getDataflowEndpoint());
    assertEquals("testTempLocation", options.getTempLocation());
    assertEquals("testServiceAccountName", options.getServiceAccountName());
    assertEquals("testServiceAccountKeyfile", options.getServiceAccountKeyfile());
    assertEquals("testZone", options.getZone());
    assertEquals(2, options.getDiskSizeGb());
  }
}
