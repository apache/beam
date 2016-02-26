/*
 * Copyright (C) 2015 Google Inc.
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

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.options.ApplicationNameOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;

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
    String stringOptions = mapper.writeValueAsString(new String[]{
      "--runner=DataflowPipelineRunner",
      "--project=testProject",
      "--apiRootUrl=testApiRootUrl",
      "--dataflowEndpoint=testDataflowEndpoint",
      "--tempLocation=testTempLocation",
      "--serviceAccountName=testServiceAccountName",
      "--serviceAccountKeyfile=testServiceAccountKeyfile",
      "--zone=testZone",
      "--numWorkers=1",
      "--diskSizeGb=2"
    });
    System.getProperties().put("dataflowOptions", stringOptions);
    DataflowPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowPipelineOptions.class);
    assertEquals(DataflowPipelineRunner.class, options.getRunner());
    assertThat(options.getJobName(), startsWith("testpipelinetest0testcreationofpipelineoptions-"));
    assertEquals("testProject", options.as(GcpOptions.class).getProject());
    assertEquals("testApiRootUrl", options.getApiRootUrl());
    assertEquals("testDataflowEndpoint", options.getDataflowEndpoint());
    assertEquals("testTempLocation", options.getTempLocation());
    assertEquals("testServiceAccountName", options.getServiceAccountName());
    assertEquals(
        "testServiceAccountKeyfile", options.as(GcpOptions.class).getServiceAccountKeyfile());
    assertEquals("testZone", options.getZone());
    assertEquals(2, options.getDiskSizeGb());
  }

  @Test
  public void testCreationOfPipelineOptionsFromReallyVerboselyNamedTestCase() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
    String stringOptions = mapper.writeValueAsString(new String[]{});
    System.getProperties().put("dataflowOptions", stringOptions);
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    assertThat(options.as(ApplicationNameOptions.class).getAppName(), startsWith(
        "TestPipelineTest-testCreationOfPipelineOptionsFromReallyVerboselyNamedTestCase"));
  }

  @Test
  public void testToString() {
    assertEquals("TestPipeline#TestPipelineTest-testToString", TestPipeline.create().toString());
  }
}
