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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/** Tests for {@link TestPipeline}. */
@RunWith(JUnit4.class)
public class TestPipelineTest {
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreationUsingDefaults() {
    assertNotNull(TestPipeline.create());
  }

  @Test
  public void testCreationOfPipelineOptions() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String stringOptions = mapper.writeValueAsString(new String[]{
      "--runner=org.apache.beam.sdk.testing.CrashingRunner",
      "--project=testProject"
    });
    System.getProperties().put("beamTestPipelineOptions", stringOptions);
    GcpOptions options =
        TestPipeline.testingPipelineOptions().as(GcpOptions.class);
    assertEquals(CrashingRunner.class, options.getRunner());
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

  @Test
  public void testMatcherSerializationDeserialization() {
    TestPipelineOptions opts = PipelineOptionsFactory.as(TestPipelineOptions.class);
    SerializableMatcher m1 = new TestMatcher();
    SerializableMatcher m2 = new TestMatcher();

    opts.setOnCreateMatcher(m1);
    opts.setOnSuccessMatcher(m2);

    String[] arr = TestPipeline.convertToArgs(opts);
    TestPipelineOptions newOpts = PipelineOptionsFactory.fromArgs(arr)
        .as(TestPipelineOptions.class);

    assertEquals(m1, newOpts.getOnCreateMatcher());
    assertEquals(m2, newOpts.getOnSuccessMatcher());
  }

  @Test
  public void testRunWithDummyEnvironmentVariableFails() {
    System.getProperties()
        .setProperty(TestPipeline.PROPERTY_USE_DEFAULT_DUMMY_RUNNER, Boolean.toString(true));
    TestPipeline pipeline = TestPipeline.create();
    pipeline.apply(Create.of(1, 2, 3));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Cannot call #run");
    pipeline.run();
  }

  /**
   * TestMatcher is a matcher designed for testing matcher serialization/deserialization.
   */
  public static class TestMatcher extends BaseMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {
    private final UUID uuid = UUID.randomUUID();
    @Override
    public boolean matches(Object o) {
      return true;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText(String.format("%tL", new Date()));
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TestMatcher)) {
        return false;
      }
      TestMatcher other = (TestMatcher) obj;
      return other.uuid.equals(uuid);
    }

    @Override
    public int hashCode() {
      return uuid.hashCode();
    }
  }
}
