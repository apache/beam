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
package org.apache.beam.runners.dataflow.options;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** Tests for {@link DataflowPipelineOptions}. */
@RunWith(JUnit4.class)
public class DataflowPipelineOptionsTest {
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();
  @Rule public ResetDateTimeProvider resetDateTimeProviderRule = new ResetDateTimeProvider();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testJobNameIsSet() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    assertEquals("TestJobName", options.getJobName());
  }

  @Test
  public void testUserNameIsNotSet() {
    resetDateTimeProviderRule.setDateTimeFixed("2014-12-08T19:07:06.698Z");
    System.getProperties().remove("user.name");
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("TestApplication");
    List<String> nameComponents = Splitter.on('-').splitToList(options.getJobName());
    assertEquals(4, nameComponents.size());
    assertEquals("testapplication", nameComponents.get(0));
    assertEquals("", nameComponents.get(1));
    assertEquals("1208190706", nameComponents.get(2));
    // Verify the last component is a hex integer (unsigned).
    Long.parseLong(nameComponents.get(3), 16);
    assertTrue(options.getJobName().length() <= 40);
  }

  @Test
  public void testAppNameAndUserNameAreLong() {
    resetDateTimeProviderRule.setDateTimeFixed("2014-12-08T19:07:06.698Z");
    System.getProperties().put("user.name", "abcdeabcdeabcdeabcdeabcdeabcde");
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("1234567890123456789012345678901234567890");
    List<String> nameComponents = Splitter.on('-').splitToList(options.getJobName());
    assertEquals(4, nameComponents.size());
    assertEquals("a234567890123456789012345678901234567890", nameComponents.get(0));
    assertEquals("abcdeabcdeabcdeabcdeabcdeabcde", nameComponents.get(1));
    assertEquals("1208190706", nameComponents.get(2));
    // Verify the last component is a hex integer (unsigned).
    Long.parseLong(nameComponents.get(3), 16);
  }

  @Test
  public void testAppNameIsLong() {
    resetDateTimeProviderRule.setDateTimeFixed("2014-12-08T19:07:06.698Z");
    System.getProperties().put("user.name", "abcde");
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("1234567890123456789012345678901234567890");
    List<String> nameComponents = Splitter.on('-').splitToList(options.getJobName());
    assertEquals(4, nameComponents.size());
    assertEquals("a234567890123456789012345678901234567890", nameComponents.get(0));
    assertEquals("abcde", nameComponents.get(1));
    assertEquals("1208190706", nameComponents.get(2));
    // Verify the last component is a hex integer (unsigned).
    Long.parseLong(nameComponents.get(3), 16);
  }

  @Test
  public void testUserNameIsLong() {
    resetDateTimeProviderRule.setDateTimeFixed("2014-12-08T19:07:06.698Z");
    System.getProperties().put("user.name", "abcdeabcdeabcdeabcdeabcdeabcde");
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("1234567890");
    List<String> nameComponents = Splitter.on('-').splitToList(options.getJobName());
    assertEquals(4, nameComponents.size());
    assertEquals("a234567890", nameComponents.get(0));
    assertEquals("abcdeabcdeabcdeabcdeabcdeabcde", nameComponents.get(1));
    assertEquals("1208190706", nameComponents.get(2));
    // Verify the last component is a hex integer (unsigned).
    Long.parseLong(nameComponents.get(3), 16);
  }

  @Test
  public void testUtf8UserNameAndApplicationNameIsNormalized() {
    resetDateTimeProviderRule.setDateTimeFixed("2014-12-08T19:07:06.698Z");
    System.getProperties().put("user.name", "ði ıntəˈnæʃənəl ");
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("fəˈnɛtık əsoʊsiˈeıʃn");
    List<String> nameComponents = Splitter.on('-').splitToList(options.getJobName());
    assertEquals(4, nameComponents.size());
    assertEquals("f00n0t0k00so0si0e00n", nameComponents.get(0));
    assertEquals("0i00nt00n000n0l0", nameComponents.get(1));
    assertEquals("1208190706", nameComponents.get(2));
    // Verify the last component is a hex integer (unsigned).
    Long.parseLong(nameComponents.get(3), 16);
  }

  @Test
  public void testStagingLocation() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setTempLocation("gs://temp_location");
    options.setStagingLocation("gs://staging_location");
    assertEquals("gs://temp_location", options.getGcpTempLocation());
    assertEquals("gs://staging_location", options.getStagingLocation());
  }

  @Test
  public void testDefaultToTempLocation() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    FileSystems.setDefaultPipelineOptions(options);
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setTempLocation("gs://temp_location/");
    assertEquals("gs://temp_location/", options.getGcpTempLocation());
    assertEquals("gs://temp_location/staging/", options.getStagingLocation());
  }

  @Test
  public void testDefaultToGcpTempLocation() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    FileSystems.setDefaultPipelineOptions(options);
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setTempLocation("gs://temp_location/");
    options.setGcpTempLocation("gs://gcp_temp_location/");
    assertEquals("gs://gcp_temp_location/staging/", options.getStagingLocation());
  }

  @Test
  public void testDefaultFlexRSGoal() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    assertEquals(
        DataflowPipelineOptions.FlexResourceSchedulingGoal.UNSPECIFIED, options.getFlexRSGoal());
    options.setFlexRSGoal(DataflowPipelineOptions.FlexResourceSchedulingGoal.COST_OPTIMIZED);
    assertEquals(
        DataflowPipelineOptions.FlexResourceSchedulingGoal.COST_OPTIMIZED, options.getFlexRSGoal());
  }

  @Test
  public void testDefaultNoneGcsTempLocation() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setTempLocation("file://temp_location");
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Error constructing default value for stagingLocation: "
            + "failed to retrieve gcpTempLocation.");
    thrown.expectCause(
        hasMessage(containsString("Error constructing default value for gcpTempLocation")));
    options.getStagingLocation();
  }

  @Test
  public void testDefaultInvalidGcpTempLocation() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setGcpTempLocation("file://temp_location");
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Error constructing default value for stagingLocation: gcpTempLocation is not"
            + " a valid GCS path");
    thrown.expectCause(hasMessage(containsString("Expected a valid 'gs://' path")));
    options.getStagingLocation();
  }

  @Test
  public void testDefaultStagingLocationUnset() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject("");
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Error constructing default value for stagingLocation");
    options.getStagingLocation();
  }

  @RunWith(PowerMockRunner.class)
  @PrepareForTest(DefaultGcpRegionFactory.class)
  public static class DefaultGcpRegionFactoryTest {
    @Test
    public void testDefaultGcpRegionUnset() throws IOException, InterruptedException {
      mockStatic(DefaultGcpRegionFactory.class);
      when(DefaultGcpRegionFactory.getRegionFromEnvironment()).thenReturn(null);
      when(DefaultGcpRegionFactory.getRegionFromGcloudCli()).thenReturn("");
      DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      assertEquals("", options.getRegion());
    }

    @Test
    public void testDefaultGcpRegionUnsetIgnoresGcloudException()
        throws IOException, InterruptedException {
      mockStatic(DefaultGcpRegionFactory.class);
      when(DefaultGcpRegionFactory.getRegionFromEnvironment()).thenReturn(null);
      when(DefaultGcpRegionFactory.getRegionFromGcloudCli()).thenThrow(new IOException());
      DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      assertEquals("", options.getRegion());
    }

    @Test
    public void testDefaultGcpRegionFromEnvironment() {
      mockStatic(DefaultGcpRegionFactory.class);
      when(DefaultGcpRegionFactory.getRegionFromEnvironment()).thenReturn("us-west1");
      DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      assertEquals("us-west1", options.getRegion());
    }

    @Test
    public void testDefaultGcpRegionFromGcloud() throws IOException, InterruptedException {
      mockStatic(DefaultGcpRegionFactory.class);
      when(DefaultGcpRegionFactory.getRegionFromEnvironment()).thenReturn(null);
      when(DefaultGcpRegionFactory.getRegionFromGcloudCli()).thenReturn("us-west1");
      DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      assertEquals("us-west1", options.getRegion());
    }

    @Test
    public void testHotKeyLoggingEnabled() {
      DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      options.setHotKeyLoggingEnabled(true);
      assertEquals(true, options.getHotKeyLoggingEnabled());
    }
  }
}
