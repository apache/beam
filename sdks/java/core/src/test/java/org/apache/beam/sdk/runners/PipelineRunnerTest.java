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
package org.apache.beam.sdk.runners;

import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.TestCredential;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for DataflowRunner.
 */
@RunWith(JUnit4.class)
public class PipelineRunnerTest {

  @Mock private GcsUtil mockGcsUtil;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testLongName() {
    // Check we can create a pipeline runner using the full class name.
    PipelineOptions options = PipelineOptionsFactory.create();
    options.as(ApplicationNameOptions.class).setAppName("test");
    options.as(GcpOptions.class).setProject("test");
    options.as(GcsOptions.class).setGcsUtil(mockGcsUtil);
    options.setRunner(CrashingRunner.class);
    options.as(GcpOptions.class).setGcpCredential(new TestCredential());
    PipelineRunner<?> runner = PipelineRunner.fromOptions(options);
    assertTrue(runner instanceof CrashingRunner);
  }

  @Test
  public void testShortName() {
    // Check we can create a pipeline runner using the short class name.
    PipelineOptions options = PipelineOptionsFactory.create();
    options.as(ApplicationNameOptions.class).setAppName("test");
    options.as(GcpOptions.class).setProject("test");
    options.as(GcsOptions.class).setGcsUtil(mockGcsUtil);
    options.setRunner(CrashingRunner.class);
    options.as(GcpOptions.class).setGcpCredential(new TestCredential());
    PipelineRunner<?> runner = PipelineRunner.fromOptions(options);
    assertTrue(runner instanceof CrashingRunner);
  }

  @Test
  public void testAppNameDefault() {
    ApplicationNameOptions options = PipelineOptionsFactory.as(ApplicationNameOptions.class);
    Assert.assertEquals(PipelineRunnerTest.class.getSimpleName(),
        options.getAppName());
  }
}
