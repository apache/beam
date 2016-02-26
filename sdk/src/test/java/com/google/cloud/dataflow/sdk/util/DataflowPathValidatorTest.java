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

package com.google.cloud.dataflow.sdk.util;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DataflowPathValidator}. */
@RunWith(JUnit4.class)
public class DataflowPathValidatorTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Mock private GcsUtil mockGcsUtil;
  private DataflowPathValidator validator;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(mockGcsUtil.bucketExists(any(GcsPath.class))).thenReturn(true);
    when(mockGcsUtil.isGcsPatternSupported(anyString())).thenCallRealMethod();
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
    options.setRunner(DataflowPipelineRunner.class);
    options.setGcsUtil(mockGcsUtil);
    validator = new DataflowPathValidator(options);
  }

  @Test
  public void testValidFilePattern() {
    validator.validateInputFilePatternSupported("gs://bucket/path");
  }

  @Test
  public void testInvalidFilePattern() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "DataflowPipelineRunner expected a valid 'gs://' path but was given '/local/path'");
    validator.validateInputFilePatternSupported("/local/path");
  }

  @Test
  public void testWhenBucketDoesNotExist() throws Exception {
    when(mockGcsUtil.bucketExists(any(GcsPath.class))).thenReturn(false);
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Could not find file gs://non-existent-bucket/location");
    validator.validateInputFilePatternSupported("gs://non-existent-bucket/location");
  }

  @Test
  public void testValidOutputPrefix() {
    validator.validateOutputFilePrefixSupported("gs://bucket/path");
  }

  @Test
  public void testInvalidOutputPrefix() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "DataflowPipelineRunner expected a valid 'gs://' path but was given '/local/path'");
    validator.validateOutputFilePrefixSupported("/local/path");
  }
}

