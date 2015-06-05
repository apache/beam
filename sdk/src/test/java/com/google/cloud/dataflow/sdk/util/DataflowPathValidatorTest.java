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

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataflowPathValidator}. */
@RunWith(JUnit4.class)
public class DataflowPathValidatorTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  private DataflowPathValidator validator;

  @Before
  public void setUp() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
    options.setRunner(DataflowPipelineRunner.class);
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

