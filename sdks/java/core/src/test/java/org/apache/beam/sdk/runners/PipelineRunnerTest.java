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
package com.google.cloud.dataflow.sdk.runners;

import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.options.ApplicationNameOptions;
import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.TestCredential;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for DataflowPipelineRunner.
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
    DirectPipelineOptions options = PipelineOptionsFactory.as(DirectPipelineOptions.class);
    options.setAppName("test");
    options.setProject("test");
    options.setGcsUtil(mockGcsUtil);
    options.setRunner(DirectPipelineRunner.class);
    options.setGcpCredential(new TestCredential());
    PipelineRunner<?> runner = PipelineRunner.fromOptions(options);
    assertTrue(runner instanceof DirectPipelineRunner);
  }

  @Test
  public void testShortName() {
    // Check we can create a pipeline runner using the short class name.
    DirectPipelineOptions options = PipelineOptionsFactory.as(DirectPipelineOptions.class);
    options.setAppName("test");
    options.setProject("test");
    options.setGcsUtil(mockGcsUtil);
    options.setRunner(DirectPipelineRunner.class);
    options.setGcpCredential(new TestCredential());
    PipelineRunner<?> runner = PipelineRunner.fromOptions(options);
    assertTrue(runner instanceof DirectPipelineRunner);
  }

  @Test
  public void testAppNameDefault() {
    ApplicationNameOptions options = PipelineOptionsFactory.as(ApplicationNameOptions.class);
    Assert.assertEquals(PipelineRunnerTest.class.getSimpleName(),
        options.getAppName());
  }
}
