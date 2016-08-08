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

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.NoopPathValidator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
    assertEquals("testapplication--1208190706", options.getJobName());
    assertTrue(options.getJobName().length() <= 40);
  }

  @Test
  public void testAppNameAndUserNameAreLong() {
    resetDateTimeProviderRule.setDateTimeFixed("2014-12-08T19:07:06.698Z");
    System.getProperties().put("user.name", "abcdeabcdeabcdeabcdeabcdeabcde");
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("1234567890123456789012345678901234567890");
    assertEquals(
        "a234567890123456789012345678901234567890-abcdeabcdeabcdeabcdeabcdeabcde-1208190706",
        options.getJobName());
  }

  @Test
  public void testAppNameIsLong() {
    resetDateTimeProviderRule.setDateTimeFixed("2014-12-08T19:07:06.698Z");
    System.getProperties().put("user.name", "abcde");
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("1234567890123456789012345678901234567890");
    assertEquals("a234567890123456789012345678901234567890-abcde-1208190706", options.getJobName());
  }

  @Test
  public void testUserNameIsLong() {
    resetDateTimeProviderRule.setDateTimeFixed("2014-12-08T19:07:06.698Z");
    System.getProperties().put("user.name", "abcdeabcdeabcdeabcdeabcdeabcde");
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("1234567890");
    assertEquals("a234567890-abcdeabcdeabcdeabcdeabcdeabcde-1208190706", options.getJobName());
  }

  @Test
  public void testUtf8UserNameAndApplicationNameIsNormalized() {
    resetDateTimeProviderRule.setDateTimeFixed("2014-12-08T19:07:06.698Z");
    System.getProperties().put("user.name", "ði ıntəˈnæʃənəl ");
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("fəˈnɛtık əsoʊsiˈeıʃn");
    assertEquals("f00n0t0k00so0si0e00n-0i00nt00n000n0l0-1208190706", options.getJobName());
  }

  @Test
  public void testStagingLocation() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    IOChannelUtils.registerStandardIOFactories(options);
    options.setTempLocation("file://temp_location");
    options.setStagingLocation("gs://staging_location");
    assertTrue(isNullOrEmpty(options.getGcpTempLocation()));
    assertEquals("gs://staging_location", options.getStagingLocation());
  }

  @Test
  public void testDefaultToTempLocation() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    IOChannelUtils.registerStandardIOFactories(options);
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setTempLocation("gs://temp_location");
    assertEquals("gs://temp_location", options.getGcpTempLocation());
    assertEquals("gs://temp_location/staging", options.getStagingLocation());
  }

  @Test
  public void testDefaultToGcpTempLocation() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    IOChannelUtils.registerStandardIOFactories(options);
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setTempLocation("gs://temp_location");
    options.setGcpTempLocation("gs://gcp_temp_location");
    assertEquals("gs://gcp_temp_location/staging", options.getStagingLocation());
  }

  @Test
  public void testDefaultNoneGcsTempLocation() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setTempLocation("file://temp_location");
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Error constructing default value for stagingLocation: gcpTempLocation is missing.");
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
    options.getStagingLocation();
  }

  @Test
  public void testDefaultStagingLocationUnset() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Error constructing default value for stagingLocation: gcpTempLocation is missing.");
    options.getStagingLocation();
  }
}
