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
package org.apache.beam.sdk.testing;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.DirectPipelineRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

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
      "--runner=DirectPipelineRunner",
      "--project=testProject"
    });
    System.getProperties().put("beamTestPipelineOptions", stringOptions);
    GcpOptions options =
        TestPipeline.testingPipelineOptions().as(GcpOptions.class);
    assertEquals(DirectPipelineRunner.class, options.getRunner());
    assertEquals(options.getProject(), "testProject");
  }

  @Test
  public void testCreationOfPipelineOptionsFromReallyVerboselyNamedTestCase() throws Exception {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    assertThat(options.as(ApplicationNameOptions.class).getAppName(), startsWith(
        "TestPipelineTest-testCreationOfPipelineOptionsFromReallyVerboselyNamedTestCase"));
  }

  @Test
  public void testToString() {
    assertEquals("TestPipeline#TestPipelineTest-testToString", TestPipeline.create().toString());
  }

  @Test
  public void testToStringNestedMethod() {
    TestPipeline p = nestedMethod();

    assertEquals("TestPipeline#TestPipelineTest-testToStringNestedMethod", p.toString());
    assertEquals(
        "TestPipelineTest-testToStringNestedMethod",
        p.getOptions().as(ApplicationNameOptions.class).getAppName());
  }

  private TestPipeline nestedMethod() {
    return TestPipeline.create();
  }

  @Test
  public void testConvertToArgs() {
    String[] args = new String[]{"--tempLocation=Test_Location"};
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    String[] arr = TestPipeline.convertToArgs(options);
    List<String> lst = Arrays.asList(arr);
    assertEquals(lst.size(), 2);
    assertThat(lst, containsInAnyOrder("--tempLocation=Test_Location",
          "--appName=TestPipelineTest"));
  }

  @Test
  public void testToStringNestedClassMethod() {
    TestPipeline p = new NestedTester().p();

    assertEquals("TestPipeline#TestPipelineTest-testToStringNestedClassMethod", p.toString());
    assertEquals(
        "TestPipelineTest-testToStringNestedClassMethod",
        p.getOptions().as(ApplicationNameOptions.class).getAppName());
  }

  private static class NestedTester {
    public TestPipeline p() {
      return TestPipeline.create();
    }
  }
}
