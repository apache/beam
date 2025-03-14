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
package org.apache.beam.runners.dataflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DefaultGcpRegionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.MimeTypes;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Tests for the {@link DataflowRunner} that involves mock static methods.
 *
 * <p>Separated from {@link DataflowRunnerTest}.
 */
@RunWith(JUnit4.class)
public class DataflowRunnerStaticTest {
  private static final String VALID_TEMP_BUCKET = "gs://valid-bucket/temp";
  private static final String PROJECT_ID = "some-project";
  private static final String REGION_ID = "some-region-1";
  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private transient Dataflow.Projects.Locations.Jobs mockJobs;
  private transient GcsUtil mockGcsUtil;

  private DataflowPipelineOptions buildPipelineOptions() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject(PROJECT_ID);
    options.setTempLocation(VALID_TEMP_BUCKET);
    options.setRegion(REGION_ID);
    // Set FILES_PROPERTY to empty to prevent a default value calculated from classpath.
    options.setFilesToStage(new ArrayList<>());
    options.setDataflowClient(DataflowRunnerTest.buildMockDataflow(mockJobs));
    options.setGcsUtil(mockGcsUtil);
    options.setGcpCredential(new TestCredential());

    // Configure the FileSystem registrar to use these options.
    FileSystems.setDefaultPipelineOptions(options);

    return options;
  }

  @Before
  public void setUp() throws IOException {
    mockGcsUtil = DataflowRunnerTest.buildMockGcsUtil();
    mockJobs = mock(Dataflow.Projects.Locations.Jobs.class);
  }

  /**
   * Test that the region is set in the generated JSON pipeline options even when a default value is
   * grabbed from the environment.
   */
  @Test
  public void testDefaultRegionSet() throws Exception {
    try (MockedStatic<DefaultGcpRegionFactory> mocked =
        Mockito.mockStatic(DefaultGcpRegionFactory.class)) {
      mocked.when(DefaultGcpRegionFactory::getRegionFromEnvironment).thenReturn(REGION_ID);
      Dataflow.Projects.Locations.Jobs mockJobs = mock(Dataflow.Projects.Locations.Jobs.class);

      DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      options.setRunner(DataflowRunner.class);
      options.setProject(PROJECT_ID);
      options.setTempLocation(VALID_TEMP_BUCKET);
      // Set FILES_PROPERTY to empty to prevent a default value calculated from classpath.
      options.setFilesToStage(new ArrayList<>());
      options.setDataflowClient(DataflowRunnerTest.buildMockDataflow(mockJobs));
      options.setGcsUtil(DataflowRunnerTest.buildMockGcsUtil());
      options.setGcpCredential(new TestCredential());

      Pipeline p = Pipeline.create(options);
      p.run();

      ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
      Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
      Map<String, Object> sdkPipelineOptions =
          jobCaptor.getValue().getEnvironment().getSdkPipelineOptions();

      assertThat(sdkPipelineOptions, hasKey("options"));
      Map<String, Object> optionsMap = (Map<String, Object>) sdkPipelineOptions.get("options");
      assertThat(optionsMap, hasEntry("region", options.getRegion()));
    }
  }

  /**
   * Tests that the {@link DataflowRunner} with {@code --templateLocation} throws the appropriate
   * exception when an output file throws IOException at close.
   */
  @Test
  public void testTemplateRunnerLoggedErrorForFileCloseError() throws Exception {
    File templateLocation = tmpFolder.newFile();
    String closeErrorMessage = "Unable to close";

    try (MockedStatic<FileSystems> mocked =
        Mockito.mockStatic(FileSystems.class, CALLS_REAL_METHODS)) {
      mocked
          .when(
              () ->
                  FileSystems.create(
                      FileSystems.matchNewResource(templateLocation.getPath(), false),
                      MimeTypes.TEXT))
          .thenReturn(
              DataflowRunnerTest.createWritableByteChannelThrowsIOExceptionAtClose(
                  closeErrorMessage));

      DataflowPipelineOptions options = buildPipelineOptions();
      options.setTemplateLocation(templateLocation.getPath());
      Pipeline p = Pipeline.create(options);

      thrown.expectMessage("Cannot create output file at");
      thrown.expect(RuntimeException.class);
      thrown.expectCause(Matchers.isA(IOException.class));
      thrown.expectCause(hasProperty("message", is(closeErrorMessage)));

      p.run();
    }
  }

  /**
   * Tests that the {@link DataflowRunner} with {@code --templateLocation} throws the appropriate
   * exception when an output file throws IOException at close.
   */
  @Test
  public void testTemplateRunnerLoggedErrorForFileWriteError() throws Exception {
    File templateLocation = tmpFolder.newFile();
    String closeErrorMessage = "Unable to write";

    try (MockedStatic<FileSystems> mocked =
        Mockito.mockStatic(FileSystems.class, CALLS_REAL_METHODS)) {
      mocked
          .when(
              () ->
                  FileSystems.create(
                      FileSystems.matchNewResource(templateLocation.getPath(), false),
                      MimeTypes.TEXT))
          .thenReturn(
              DataflowRunnerTest.createWritableByteChannelThrowsIOExceptionAtWrite(
                  closeErrorMessage));

      thrown.expectMessage("Cannot create output file at");
      thrown.expect(RuntimeException.class);
      thrown.expectCause(Matchers.isA(IOException.class));
      thrown.expectCause(hasProperty("message", is(closeErrorMessage)));

      DataflowPipelineOptions options = buildPipelineOptions();
      options.setTemplateLocation(templateLocation.getPath());
      Pipeline p = Pipeline.create(options);

      p.run();
    }
  }
}
