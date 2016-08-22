/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.testing.TestDataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.util.NoopPathValidator;
import com.google.cloud.dataflow.sdk.util.TestCredential;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;

/**
 * Tests for TemplatingDataflowPipelineRunner.
 */
@RunWith(JUnit4.class)
public class TemplatingDataflowPipelineRunnerTest {

  @Mock private DataflowPipelineJob mockJob;
  @Mock private DataflowPipelineRunner mockRunner;

  @Rule
  public ExpectedLogs expectedLogs = ExpectedLogs.none(TemplatingDataflowPipelineRunner.class);

  @Rule
  public ExpectedException expectedThrown = ExpectedException.none();

  @Rule
  public final TemporaryFolder tmpDir = new TemporaryFolder();

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(mockJob.getProjectId()).thenReturn("project");
    when(mockJob.getJobId()).thenReturn("job");
    when(mockRunner.run(isA(Pipeline.class))).thenReturn(mockJob);
  }

  /**
   * Tests that the {@link TemplatingDataflowPipelineRunner} returns normally when the runner is
   * successfully run.
   */
  @Test
  public void testLoggedCompletion() throws Exception {
    File existingFile = tmpDir.newFile();
    TestDataflowPipelineOptions options =
        PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setProject(mockJob.getProjectId());
    options.setDataflowJobFile(existingFile.getPath());
    TemplatingDataflowPipelineRunner runner =
        new TemplatingDataflowPipelineRunner(mockRunner, options);
    runner.run(DirectPipeline.createForTest());
    expectedLogs.verifyInfo("Template successfully created");
  }


  /**
   * Tests that the {@link TemplatingDataflowPipelineRunner} returns normally when the runner is
   * successfully run.
   */
  @Test
  public void testFullCompletion() throws Exception {
    File existingFile = tmpDir.newFile();
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setProject("test-project");
    options.setDataflowJobFile(existingFile.getPath());
    options.setTempLocation(tmpDir.getRoot().getPath());
    TemplatingDataflowPipelineRunner runner =
        TemplatingDataflowPipelineRunner.fromOptions(options);
    runner.run(DirectPipeline.createForTest());
  }

  /**
   * Tests that the {@link TemplatingDataflowPipelineRunner} throws the appropriate exception when
   * an output file is not writable.
   */
  @Test
  public void testLoggedErrorForFile() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    options.setDataflowJobFile("//bad/path");
    options.setProject("test-project");
    options.setTempLocation(tmpDir.getRoot().getPath());
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    TemplatingDataflowPipelineRunner runner =
        TemplatingDataflowPipelineRunner.fromOptions(options);

    expectedThrown.expectMessage("Cannot create output file at");
    expectedThrown.expect(RuntimeException.class);
    runner.run(DirectPipeline.createForTest());
  }

  @Test
  public void testToString() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    options.setDataflowJobFile("foo");
    options.setProject("test-project");
    options.setTempLocation("gs://test/temp/location");
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    assertEquals("TemplatingDataflowPipelineRunner",
        TemplatingDataflowPipelineRunner.fromOptions(options).toString());
  }
}
