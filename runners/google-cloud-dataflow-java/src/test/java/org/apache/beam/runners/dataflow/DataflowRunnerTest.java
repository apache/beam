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

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.runners.dataflow.DataflowRunner.BatchViewAsList;
import org.apache.beam.runners.dataflow.DataflowRunner.BatchViewAsMap;
import org.apache.beam.runners.dataflow.DataflowRunner.BatchViewAsMultimap;
import org.apache.beam.runners.dataflow.DataflowRunner.BatchViewAsSingleton;
import org.apache.beam.runners.dataflow.DataflowRunner.TransformedMap;
import org.apache.beam.runners.dataflow.internal.IsmFormat;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.MetadataKeyCoder;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.runners.dataflow.TestCountingSource;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.NoopCredentialFactory;
import org.apache.beam.sdk.util.NoopPathValidator;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.util.TestCredential;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests for the {@link DataflowRunner}.
 */
@RunWith(JUnit4.class)
public class DataflowRunnerTest {

  private static final String PROJECT_ID = "some-project";

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  // Asserts that the given Job has all expected fields set.
  private static void assertValidJob(Job job) {
    assertNull(job.getId());
    assertNull(job.getCurrentState());
    assertTrue(Pattern.matches("[a-z]([-a-z0-9]*[a-z0-9])?", job.getName()));
  }

  private Pipeline buildDataflowPipeline(DataflowPipelineOptions options) {
    options.setStableUniqueNames(CheckEnabled.ERROR);
    options.setRunner(DataflowRunner.class);
    Pipeline p = Pipeline.create(options);

    p.apply("ReadMyFile", TextIO.Read.from("gs://bucket/object"))
        .apply("WriteMyFile", TextIO.Write.to("gs://bucket/object"));

    return p;
  }

  private static Dataflow buildMockDataflow(
      final ArgumentCaptor<Job> jobCaptor) throws IOException {
    Dataflow mockDataflowClient = mock(Dataflow.class);
    Dataflow.Projects mockProjects = mock(Dataflow.Projects.class);
    Dataflow.Projects.Jobs mockJobs = mock(Dataflow.Projects.Jobs.class);
    Dataflow.Projects.Jobs.Create mockRequest =
        mock(Dataflow.Projects.Jobs.Create.class);
    Dataflow.Projects.Jobs.List mockList = mock(Dataflow.Projects.Jobs.List.class);

    when(mockDataflowClient.projects()).thenReturn(mockProjects);
    when(mockProjects.jobs()).thenReturn(mockJobs);
    when(mockJobs.create(eq(PROJECT_ID), jobCaptor.capture()))
        .thenReturn(mockRequest);
    when(mockJobs.list(eq(PROJECT_ID))).thenReturn(mockList);
    when(mockList.setPageToken(anyString())).thenReturn(mockList);
    when(mockList.execute())
        .thenReturn(
            new ListJobsResponse()
                .setJobs(
                    Arrays.asList(
                        new Job()
                            .setName("oldjobname")
                            .setId("oldJobId")
                            .setCurrentState("JOB_STATE_RUNNING"))));

    Job resultJob = new Job();
    resultJob.setId("newid");
    when(mockRequest.execute()).thenReturn(resultJob);
    return mockDataflowClient;
  }

  /**
   * Build a mock {@link GcsUtil} with return values.
   *
   * @param bucketExist first return value
   * @param bucketAccessible next return values
   */
  private GcsUtil buildMockGcsUtil(Boolean bucketExist, Boolean... bucketAccessible)
      throws IOException {
    GcsUtil mockGcsUtil = mock(GcsUtil.class);
    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .then(new Answer<SeekableByteChannel>() {
              @Override
              public SeekableByteChannel answer(InvocationOnMock invocation) throws Throwable {
                return FileChannel.open(
                    Files.createTempFile("channel-", ".tmp"),
                    StandardOpenOption.CREATE, StandardOpenOption.DELETE_ON_CLOSE);
              }
            });

    when(mockGcsUtil.isGcsPatternSupported(anyString())).thenReturn(true);
    when(mockGcsUtil.expand(any(GcsPath.class))).then(new Answer<List<GcsPath>>() {
      @Override
      public List<GcsPath> answer(InvocationOnMock invocation) throws Throwable {
        return ImmutableList.of((GcsPath) invocation.getArguments()[0]);
      }
    });
    when(mockGcsUtil.bucketAccessible(any(GcsPath.class)))
        .thenReturn(bucketExist, bucketAccessible);
    return mockGcsUtil;
  }

  private DataflowPipelineOptions buildPipelineOptions() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    return buildPipelineOptions(jobCaptor);
  }

  private DataflowPipelineOptions buildPipelineOptions(
      ArgumentCaptor<Job> jobCaptor) throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject(PROJECT_ID);
    options.setTempLocation("gs://somebucket/some/path");
    // Set FILES_PROPERTY to empty to prevent a default value calculated from classpath.
    options.setFilesToStage(new LinkedList<String>());
    options.setDataflowClient(buildMockDataflow(jobCaptor));
    options.setGcsUtil(buildMockGcsUtil(true /* bucket exists */));
    options.setGcpCredential(new TestCredential());
    return options;
  }

  @Test
  public void testPathValidation() {
    String[] args = new String[] {
        "--runner=DataflowRunner",
        "--tempLocation=/tmp/not/a/gs/path",
        "--project=test-project",
        "--credentialFactoryClass=" + NoopCredentialFactory.class.getCanonicalName(),
    };

    try {
      TestPipeline.fromOptions(PipelineOptionsFactory.fromArgs(args).create());
      fail();
    } catch (RuntimeException e) {
      assertThat(
          Throwables.getStackTraceAsString(e),
          containsString("DataflowRunner requires gcpTempLocation"));
    }
  }

  @Test
  public void testPathValidatorOverride() {
    String[] args = new String[] {
        "--runner=DataflowRunner",
        "--tempLocation=/tmp/testing",
        "--project=test-project",
        "--credentialFactoryClass=" + NoopCredentialFactory.class.getCanonicalName(),
        "--pathValidatorClass=" + NoopPathValidator.class.getCanonicalName(),
    };
    // Should not crash, because gcpTempLocation should get set from tempLocation
    TestPipeline.fromOptions(PipelineOptionsFactory.fromArgs(args).create());
  }

  @Test
  public void testFromOptionsWithUppercaseConvertsToLowercase() throws Exception {
    String mixedCase = "ThisJobNameHasMixedCase";
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    options.setJobName(mixedCase);

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    assertThat(options.getJobName(), equalTo(mixedCase.toLowerCase()));
  }

  @Test
  public void testRun() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    Pipeline p = buildDataflowPipeline(options);
    DataflowPipelineJob job = (DataflowPipelineJob) p.run();
    assertEquals("newid", job.getJobId());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testRunReturnDifferentRequestId() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    Dataflow mockDataflowClient = options.getDataflowClient();
    Dataflow.Projects.Jobs.Create mockRequest = mock(Dataflow.Projects.Jobs.Create.class);
    when(mockDataflowClient.projects().jobs().create(eq(PROJECT_ID), any(Job.class)))
        .thenReturn(mockRequest);
    Job resultJob = new Job();
    resultJob.setId("newid");
    // Return a different request id.
    resultJob.setClientRequestId("different_request_id");
    when(mockRequest.execute()).thenReturn(resultJob);

    Pipeline p = buildDataflowPipeline(options);
    try {
      p.run();
      fail("Expected DataflowJobAlreadyExistsException");
    } catch (DataflowJobAlreadyExistsException expected) {
      assertThat(expected.getMessage(),
          containsString("If you want to submit a second job, try again by setting a "
            + "different name using --jobName."));
      assertEquals(expected.getJob().getJobId(), resultJob.getId());
    }
  }

  @Test
  public void testUpdate() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    options.setUpdate(true);
    options.setJobName("oldJobName");
    Pipeline p = buildDataflowPipeline(options);
    DataflowPipelineJob job = (DataflowPipelineJob) p.run();
    assertEquals("newid", job.getJobId());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testUpdateNonExistentPipeline() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Could not find running job named badjobname");

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setUpdate(true);
    options.setJobName("badJobName");
    Pipeline p = buildDataflowPipeline(options);
    p.run();
  }

  @Test
  public void testUpdateAlreadyUpdatedPipeline() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setUpdate(true);
    options.setJobName("oldJobName");
    Dataflow mockDataflowClient = options.getDataflowClient();
    Dataflow.Projects.Jobs.Create mockRequest = mock(Dataflow.Projects.Jobs.Create.class);
    when(mockDataflowClient.projects().jobs().create(eq(PROJECT_ID), any(Job.class)))
        .thenReturn(mockRequest);
    final Job resultJob = new Job();
    resultJob.setId("newid");
    // Return a different request id.
    resultJob.setClientRequestId("different_request_id");
    when(mockRequest.execute()).thenReturn(resultJob);

    Pipeline p = buildDataflowPipeline(options);

    thrown.expect(DataflowJobAlreadyUpdatedException.class);
    thrown.expect(new TypeSafeMatcher<DataflowJobAlreadyUpdatedException>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("Expected job ID: " + resultJob.getId());
      }

      @Override
      protected boolean matchesSafely(DataflowJobAlreadyUpdatedException item) {
        return resultJob.getId().equals(item.getJob().getJobId());
      }
    });
    thrown.expectMessage("The job named oldjobname with id: oldJobId has already been updated "
        + "into job id: newid and cannot be updated again.");
    p.run();
  }

  @Test
  public void testRunWithFiles() throws IOException {
    // Test that the function DataflowRunner.stageFiles works as
    // expected.
    GcsUtil mockGcsUtil = buildMockGcsUtil(true /* bucket exists */);
    final String gcsStaging = "gs://somebucket/some/path";
    final String gcsTemp = "gs://somebucket/some/temp/path";
    final String cloudDataflowDataset = "somedataset";

    // Create some temporary files.
    File temp1 = File.createTempFile("DataflowRunnerTest", "txt");
    temp1.deleteOnExit();
    File temp2 = File.createTempFile("DataflowRunnerTest2", "txt");
    temp2.deleteOnExit();

    String overridePackageName = "alias.txt";

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setFilesToStage(ImmutableList.of(
        temp1.getAbsolutePath(),
        overridePackageName + "=" + temp2.getAbsolutePath()));
    options.setStagingLocation(gcsStaging);
    options.setTempLocation(gcsTemp);
    options.setTempDatasetId(cloudDataflowDataset);
    options.setProject(PROJECT_ID);
    options.setJobName("job");
    options.setDataflowClient(buildMockDataflow(jobCaptor));
    options.setGcsUtil(mockGcsUtil);
    options.setGcpCredential(new TestCredential());

    Pipeline p = buildDataflowPipeline(options);

    DataflowPipelineJob job = (DataflowPipelineJob) p.run();
    assertEquals("newid", job.getJobId());

    Job workflowJob = jobCaptor.getValue();
    assertValidJob(workflowJob);

    assertEquals(
        2,
        workflowJob.getEnvironment().getWorkerPools().get(0).getPackages().size());
    DataflowPackage workflowPackage1 =
        workflowJob.getEnvironment().getWorkerPools().get(0).getPackages().get(0);
    assertThat(workflowPackage1.getName(), startsWith(temp1.getName()));
    DataflowPackage workflowPackage2 =
        workflowJob.getEnvironment().getWorkerPools().get(0).getPackages().get(1);
    assertEquals(overridePackageName, workflowPackage2.getName());

    assertEquals(
        "storage.googleapis.com/somebucket/some/temp/path",
        workflowJob.getEnvironment().getTempStoragePrefix());
    assertEquals(
        cloudDataflowDataset,
        workflowJob.getEnvironment().getDataset());
    assertEquals(
        ReleaseInfo.getReleaseInfo().getName(),
        workflowJob.getEnvironment().getUserAgent().get("name"));
    assertEquals(
        ReleaseInfo.getReleaseInfo().getVersion(),
        workflowJob.getEnvironment().getUserAgent().get("version"));
  }

  @Test
  public void runWithDefaultFilesToStage() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setFilesToStage(null);
    DataflowRunner.fromOptions(options);
    assertTrue(!options.getFilesToStage().isEmpty());
  }

  @Test
  public void detectClassPathResourceWithFileResources() throws Exception {
    File file = tmpFolder.newFile("file");
    File file2 = tmpFolder.newFile("file2");
    URLClassLoader classLoader = new URLClassLoader(new URL[]{
        file.toURI().toURL(),
        file2.toURI().toURL()
    });

    assertEquals(ImmutableList.of(file.getAbsolutePath(), file2.getAbsolutePath()),
        DataflowRunner.detectClassPathResourcesToStage(classLoader));
  }

  @Test
  public void detectClassPathResourcesWithUnsupportedClassLoader() {
    ClassLoader mockClassLoader = Mockito.mock(ClassLoader.class);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unable to use ClassLoader to detect classpath elements.");

    DataflowRunner.detectClassPathResourcesToStage(mockClassLoader);
  }

  @Test
  public void detectClassPathResourceWithNonFileResources() throws Exception {
    String url = "http://www.google.com/all-the-secrets.jar";
    URLClassLoader classLoader = new URLClassLoader(new URL[]{
        new URL(url)
    });
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unable to convert url (" + url + ") to file.");

    DataflowRunner.detectClassPathResourcesToStage(classLoader);
  }

  @Test
  public void testGcsStagingLocationInitialization() throws Exception {
    // Test that the staging location is initialized correctly.
    String gcsTemp = "gs://somebucket/some/temp/path";

    // Set temp location (required), and check that staging location is set.
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setTempLocation(gcsTemp);
    options.setProject(PROJECT_ID);
    options.setGcpCredential(new TestCredential());
    options.setGcsUtil(buildMockGcsUtil(true /* bucket exists */));
    options.setRunner(DataflowRunner.class);

    DataflowRunner.fromOptions(options);

    assertNotNull(options.getStagingLocation());
  }

  @Test
  public void testNonGcsFilePathInReadFailure() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    Pipeline p = buildDataflowPipeline(buildPipelineOptions(jobCaptor));
    p.apply("ReadMyNonGcsFile", TextIO.Read.from(tmpFolder.newFile().getPath()));

    thrown.expectCause(Matchers.allOf(
        instanceOf(IllegalArgumentException.class),
        ThrowableMessageMatcher.hasMessage(
            containsString("expected a valid 'gs://' path but was given"))));
    p.run();
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testNonGcsFilePathInWriteFailure() throws IOException {
    Pipeline p = buildDataflowPipeline(buildPipelineOptions());
    PCollection<String> pc = p.apply("ReadMyGcsFile", TextIO.Read.from("gs://bucket/object"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("expected a valid 'gs://' path but was given"));
    pc.apply("WriteMyNonGcsFile", TextIO.Write.to("/tmp/file"));
  }

  @Test
  public void testMultiSlashGcsFileReadPath() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    Pipeline p = buildDataflowPipeline(buildPipelineOptions(jobCaptor));
    p.apply("ReadInvalidGcsFile", TextIO.Read.from("gs://bucket/tmp//file"));

    thrown.expectCause(Matchers.allOf(
        instanceOf(IllegalArgumentException.class),
        ThrowableMessageMatcher.hasMessage(containsString("consecutive slashes"))));
    p.run();
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testMultiSlashGcsFileWritePath() throws IOException {
    Pipeline p = buildDataflowPipeline(buildPipelineOptions());
    PCollection<String> pc = p.apply("ReadMyGcsFile", TextIO.Read.from("gs://bucket/object"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("consecutive slashes");
    pc.apply("WriteInvalidGcsFile", TextIO.Write.to("gs://bucket/tmp//file"));
  }

  @Test
  public void testInvalidGcpTempLocation() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    options.setGcpTempLocation("file://temp/location");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("expected a valid 'gs://' path but was given"));
    DataflowRunner.fromOptions(options);
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testNonGcsTempLocation() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    options.setTempLocation("file://temp/location");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "DataflowRunner requires gcpTempLocation, and it is missing in PipelineOptions.");
    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testInvalidStagingLocation() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setStagingLocation("file://my/staging/location");
    try {
      DataflowRunner.fromOptions(options);
      fail("fromOptions should have failed");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("expected a valid 'gs://' path but was given"));
    }
    options.setStagingLocation("my/staging/location");
    try {
      DataflowRunner.fromOptions(options);
      fail("fromOptions should have failed");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("expected a valid 'gs://' path but was given"));
    }
  }

  @Test
  public void testNonExistentTempLocation() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    GcsUtil mockGcsUtil =
        buildMockGcsUtil(false /* temp bucket exists */, true /* staging bucket exists */);
    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    options.setGcsUtil(mockGcsUtil);
    options.setGcpTempLocation("gs://non-existent-bucket/location");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString(
        "Output path does not exist or is not writeable: gs://non-existent-bucket/location"));
    DataflowRunner.fromOptions(options);
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testNonExistentStagingLocation() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    GcsUtil mockGcsUtil =
        buildMockGcsUtil(true /* temp bucket exists */, false /* staging bucket exists */);
    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    options.setGcpTempLocation(options.getTempLocation()); // bypass validation for GcpTempLocation
    options.setGcsUtil(mockGcsUtil);
    options.setStagingLocation("gs://non-existent-bucket/location");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString(
        "Output path does not exist or is not writeable: gs://non-existent-bucket/location"));
    DataflowRunner.fromOptions(options);
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testNoProjectFails() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

    options.setRunner(DataflowRunner.class);
    // Explicitly set to null to prevent the default instance factory from reading credentials
    // from a user's environment, causing this test to fail.
    options.setProject(null);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Project id");
    thrown.expectMessage("when running a Dataflow in the cloud");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testProjectId() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("foo-12345");

    options.setGcpTempLocation("gs://spam/ham/eggs");
    options.setGcsUtil(buildMockGcsUtil(true /* bucket exists */));
    options.setGcpCredential(new TestCredential());

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testProjectPrefix() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("google.com:some-project-12345");

    options.setGcpTempLocation("gs://spam/ham/eggs");
    options.setGcsUtil(buildMockGcsUtil(true /* bucket exists */));
    options.setGcpCredential(new TestCredential());

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testProjectNumber() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("12345");

    options.setGcpTempLocation("gs://spam/ham/eggs");
    options.setGcsUtil(buildMockGcsUtil(true /* bucket exists */));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Project ID");
    thrown.expectMessage("project number");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testProjectDescription() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("some project");

    options.setGcpTempLocation("gs://spam/ham/eggs");
    options.setGcsUtil(buildMockGcsUtil(true /* bucket exists */));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Project ID");
    thrown.expectMessage("project description");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testInvalidNumberOfWorkerHarnessThreads() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("foo-12345");

    options.setTempLocation("gs://spam/ham/eggs");
    options.setGcsUtil(buildMockGcsUtil(true /* bucket exists */));

    options.as(DataflowPipelineDebugOptions.class).setNumberOfWorkerHarnessThreads(-1);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Number of worker harness threads");
    thrown.expectMessage("Please make sure the value is non-negative.");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testNoStagingLocationAndNoTempLocationFails() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("foo-project");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "DataflowRunner requires gcpTempLocation, and it is missing in PipelineOptions.");
    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testStagingLocationAndNoTempLocationSucceeds() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setGcpCredential(new TestCredential());
    options.setProject("foo-project");
    options.setGcpTempLocation("gs://spam/ham/eggs");
    options.setGcsUtil(buildMockGcsUtil(true /* bucket exists */));

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testTempLocationAndNoStagingLocationSucceeds() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setGcpCredential(new TestCredential());
    options.setProject("foo-project");
    options.setTempLocation("gs://spam/ham/eggs");
    options.setGcsUtil(buildMockGcsUtil(true /* bucket exists */));

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testInvalidJobName() throws IOException {
    List<String> invalidNames = Arrays.asList(
        "invalid_name",
        "0invalid",
        "invalid-");
    List<String> expectedReason = Arrays.asList(
        "JobName invalid",
        "JobName invalid",
        "JobName invalid");

    for (int i = 0; i < invalidNames.size(); ++i) {
      DataflowPipelineOptions options = buildPipelineOptions();
      options.setJobName(invalidNames.get(i));

      try {
        DataflowRunner.fromOptions(options);
        fail("Expected IllegalArgumentException for jobName "
            + options.getJobName());
      } catch (IllegalArgumentException e) {
        assertThat(e.getMessage(),
            containsString(expectedReason.get(i)));
      }
    }
  }

  @Test
  public void testValidJobName() throws IOException {
    List<String> names = Arrays.asList("ok", "Ok", "A-Ok", "ok-123",
        "this-one-is-fairly-long-01234567890123456789");

    for (String name : names) {
      DataflowPipelineOptions options = buildPipelineOptions();
      options.setJobName(name);

      DataflowRunner runner = DataflowRunner
          .fromOptions(options);
      assertNotNull(runner);
    }
  }

  @Test
  public void testGcsUploadBufferSizeIsUnsetForBatchWhenDefault() throws IOException {
    DataflowPipelineOptions batchOptions = buildPipelineOptions();
    batchOptions.setRunner(DataflowRunner.class);
    Pipeline.create(batchOptions);
    assertNull(batchOptions.getGcsUploadBufferSizeBytes());
  }

  @Test
  public void testGcsUploadBufferSizeIsSetForStreamingWhenDefault() throws IOException {
    DataflowPipelineOptions streamingOptions = buildPipelineOptions();
    streamingOptions.setStreaming(true);
    streamingOptions.setRunner(DataflowRunner.class);
    Pipeline.create(streamingOptions);
    assertEquals(
        DataflowRunner.GCS_UPLOAD_BUFFER_SIZE_BYTES_DEFAULT,
        streamingOptions.getGcsUploadBufferSizeBytes().intValue());
  }

  @Test
  public void testGcsUploadBufferSizeUnchangedWhenNotDefault() throws IOException {
    int gcsUploadBufferSizeBytes = 12345678;
    DataflowPipelineOptions batchOptions = buildPipelineOptions();
    batchOptions.setGcsUploadBufferSizeBytes(gcsUploadBufferSizeBytes);
    batchOptions.setRunner(DataflowRunner.class);
    Pipeline.create(batchOptions);
    assertEquals(gcsUploadBufferSizeBytes, batchOptions.getGcsUploadBufferSizeBytes().intValue());

    DataflowPipelineOptions streamingOptions = buildPipelineOptions();
    streamingOptions.setStreaming(true);
    streamingOptions.setGcsUploadBufferSizeBytes(gcsUploadBufferSizeBytes);
    streamingOptions.setRunner(DataflowRunner.class);
    Pipeline.create(streamingOptions);
    assertEquals(
        gcsUploadBufferSizeBytes, streamingOptions.getGcsUploadBufferSizeBytes().intValue());
  }

  /**
   * A fake PTransform for testing.
   */
  public static class TestTransform
      extends PTransform<PCollection<Integer>, PCollection<Integer>> {
    public boolean translated = false;

    @Override
    public PCollection<Integer> apply(PCollection<Integer> input) {
      return PCollection.<Integer>createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          input.isBounded());
    }

    @Override
    protected Coder<?> getDefaultOutputCoder(PCollection<Integer> input) {
      return input.getCoder();
    }
  }

  @Test
  public void testTransformTranslatorMissing() throws IOException {
    // Test that we throw if we don't provide a translation.
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    Pipeline p = Pipeline.create(options);

    p.apply(Create.of(Arrays.asList(1, 2, 3)))
     .apply(new TestTransform());

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(Matchers.containsString("no translator registered"));
    DataflowPipelineTranslator.fromOptions(options)
        .translate(
            p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testTransformTranslator() throws IOException {
    // Test that we can provide a custom translation
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline p = Pipeline.create(options);
    TestTransform transform = new TestTransform();

    p.apply(Create.of(Arrays.asList(1, 2, 3)).withCoder(BigEndianIntegerCoder.of()))
        .apply(transform);

    DataflowPipelineTranslator translator = DataflowRunner
        .fromOptions(options).getTranslator();

    DataflowPipelineTranslator.registerTransformTranslator(
        TestTransform.class,
        new DataflowPipelineTranslator.TransformTranslator<TestTransform>() {
          @SuppressWarnings("unchecked")
          @Override
          public void translate(
              TestTransform transform,
              DataflowPipelineTranslator.TranslationContext context) {
            transform.translated = true;

            // Note: This is about the minimum needed to fake out a
            // translation. This obviously isn't a real translation.
            context.addStep(transform, "TestTranslate");
            context.addOutput("output", context.getOutput(transform));
          }
        });

    translator.translate(
        p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList());
    assertTrue(transform.translated);
  }

  /** Records all the composite transforms visited within the Pipeline. */
  private static class CompositeTransformRecorder extends PipelineVisitor.Defaults {
    private List<PTransform<?, ?>> transforms = new ArrayList<>();

    @Override
    public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
      if (node.getTransform() != null) {
        transforms.add(node.getTransform());
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    public List<PTransform<?, ?>> getCompositeTransforms() {
      return transforms;
    }
  }

  @Test
  public void testApplyIsScopedToExactClass() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline p = Pipeline.create(options);

    Create.TimestampedValues<String> transform =
        Create.timestamped(Arrays.asList(TimestampedValue.of("TestString", Instant.now())));
    p.apply(transform);

    CompositeTransformRecorder recorder = new CompositeTransformRecorder();
    p.traverseTopologically(recorder);

    // The recorder will also have seen a Create.Values composite as well, but we can't obtain that
    // transform.
    assertThat(
        "Expected to have seen CreateTimestamped composite transform.",
        recorder.getCompositeTransforms(),
        hasItem(transform));
    assertThat(
        "Expected to have two composites, CreateTimestamped and Create.Values",
        recorder.getCompositeTransforms(),
        hasItem(Matchers.<PTransform<?, ?>>isA((Class) Create.Values.class)));
  }

  @Test
  public void testToString() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    options.setProject("test-project");
    options.setTempLocation("gs://test/temp/location");
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setRunner(DataflowRunner.class);
    assertEquals(
        "DataflowRunner#testjobname",
        DataflowRunner.fromOptions(options).toString());
  }

  private static PipelineOptions makeOptions(boolean streaming) {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setStreaming(streaming);
    options.setJobName("TestJobName");
    options.setProject("test-project");
    options.setTempLocation("gs://test/temp/location");
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    return options;
  }

  private void testUnsupportedSource(PTransform<PBegin, ?> source, String name, boolean streaming)
      throws Exception {
    String mode = streaming ? "streaming" : "batch";
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(
        "The DataflowRunner in " + mode + " mode does not support " + name);

    Pipeline p = Pipeline.create(makeOptions(streaming));
    p.apply(source);
    p.run();
  }

  @Test
  public void testReadUnboundedUnsupportedInBatch() throws Exception {
    testUnsupportedSource(Read.from(new TestCountingSource(1)), "Read.Unbounded", false);
  }

  @Test
  public void testBatchViewAsSingletonToIsmRecord() throws Exception {
    DoFnTester<KV<Integer, Iterable<KV<GlobalWindow, WindowedValue<String>>>>,
               IsmRecord<WindowedValue<String>>> doFnTester =
               DoFnTester.of(
                   new BatchViewAsSingleton.IsmRecordForSingularValuePerWindowDoFn
                   <String, GlobalWindow>(GlobalWindow.Coder.INSTANCE));

    assertThat(
        doFnTester.processBundle(
            ImmutableList.of(KV.<Integer, Iterable<KV<GlobalWindow, WindowedValue<String>>>>of(
                0, ImmutableList.of(KV.of(GlobalWindow.INSTANCE, valueInGlobalWindow("a")))))),
        contains(IsmRecord.of(ImmutableList.of(GlobalWindow.INSTANCE), valueInGlobalWindow("a"))));
  }

  @Test
  public void testBatchViewAsSingletonToIsmRecordWithMultipleValuesThrowsException()
      throws Exception {
    DoFnTester<KV<Integer, Iterable<KV<GlobalWindow, WindowedValue<String>>>>,
    IsmRecord<WindowedValue<String>>> doFnTester =
    DoFnTester.of(
        new BatchViewAsSingleton.IsmRecordForSingularValuePerWindowDoFn
        <String, GlobalWindow>(GlobalWindow.Coder.INSTANCE));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("found for singleton within window");
    doFnTester.processBundle(ImmutableList.of(
        KV.<Integer, Iterable<KV<GlobalWindow, WindowedValue<String>>>>of(0,
            ImmutableList.of(KV.of(GlobalWindow.INSTANCE, valueInGlobalWindow("a")),
                KV.of(GlobalWindow.INSTANCE, valueInGlobalWindow("b"))))));
  }

  @Test
  public void testBatchViewAsListToIsmRecordForGlobalWindow() throws Exception {
    DoFnTester<String, IsmRecord<WindowedValue<String>>> doFnTester =
        DoFnTester.of(new BatchViewAsList.ToIsmRecordForGlobalWindowDoFn<String>());

    // The order of the output elements is important relative to processing order
    assertThat(doFnTester.processBundle(ImmutableList.of("a", "b", "c")), contains(
        IsmRecord.of(ImmutableList.of(GlobalWindow.INSTANCE, 0L), valueInGlobalWindow("a")),
        IsmRecord.of(ImmutableList.of(GlobalWindow.INSTANCE, 1L), valueInGlobalWindow("b")),
        IsmRecord.of(ImmutableList.of(GlobalWindow.INSTANCE, 2L), valueInGlobalWindow("c"))));
  }

  @Test
  public void testBatchViewAsListToIsmRecordForNonGlobalWindow() throws Exception {
    DoFnTester<KV<Integer, Iterable<KV<IntervalWindow, WindowedValue<Long>>>>,
               IsmRecord<WindowedValue<Long>>> doFnTester =
        DoFnTester.of(
            new BatchViewAsList.ToIsmRecordForNonGlobalWindowDoFn<Long, IntervalWindow>(
                IntervalWindow.getCoder()));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer, Iterable<KV<IntervalWindow, WindowedValue<Long>>>>> inputElements =
        ImmutableList.of(
            KV.of(1, (Iterable<KV<IntervalWindow, WindowedValue<Long>>>) ImmutableList.of(
                KV.of(
                    windowA, WindowedValue.of(110L, new Instant(1), windowA, PaneInfo.NO_FIRING)),
                KV.of(
                    windowA, WindowedValue.of(111L, new Instant(3), windowA, PaneInfo.NO_FIRING)),
                KV.of(
                    windowA, WindowedValue.of(112L, new Instant(4), windowA, PaneInfo.NO_FIRING)),
                KV.of(
                    windowB, WindowedValue.of(120L, new Instant(12), windowB, PaneInfo.NO_FIRING)),
                KV.of(
                    windowB, WindowedValue.of(121L, new Instant(14), windowB, PaneInfo.NO_FIRING))
                )),
            KV.of(2, (Iterable<KV<IntervalWindow, WindowedValue<Long>>>) ImmutableList.of(
                KV.of(
                    windowC, WindowedValue.of(210L, new Instant(25), windowC, PaneInfo.NO_FIRING))
                )));

    // The order of the output elements is important relative to processing order
    assertThat(doFnTester.processBundle(inputElements), contains(
        IsmRecord.of(ImmutableList.of(windowA, 0L),
            WindowedValue.of(110L, new Instant(1), windowA, PaneInfo.NO_FIRING)),
        IsmRecord.of(ImmutableList.of(windowA, 1L),
            WindowedValue.of(111L, new Instant(3), windowA, PaneInfo.NO_FIRING)),
        IsmRecord.of(ImmutableList.of(windowA, 2L),
            WindowedValue.of(112L, new Instant(4), windowA, PaneInfo.NO_FIRING)),
        IsmRecord.of(ImmutableList.of(windowB, 0L),
            WindowedValue.of(120L, new Instant(12), windowB, PaneInfo.NO_FIRING)),
        IsmRecord.of(ImmutableList.of(windowB, 1L),
            WindowedValue.of(121L, new Instant(14), windowB, PaneInfo.NO_FIRING)),
        IsmRecord.of(ImmutableList.of(windowC, 0L),
            WindowedValue.of(210L, new Instant(25), windowC, PaneInfo.NO_FIRING))));
  }

  @Test
  public void testToIsmRecordForMapLikeDoFn() throws Exception {
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForSizeTag = new TupleTag<>();
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForEntrySetTag = new TupleTag<>();

    Coder<Long> keyCoder = VarLongCoder.of();
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    IsmRecordCoder<WindowedValue<Long>> ismCoder = IsmRecordCoder.of(
        1,
        2,
        ImmutableList.<Coder<?>>of(
            MetadataKeyCoder.of(keyCoder),
            IntervalWindow.getCoder(),
            BigEndianLongCoder.of()),
        FullWindowedValueCoder.of(VarLongCoder.of(), windowCoder));

    DoFnTester<KV<Integer, Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>>,
               IsmRecord<WindowedValue<Long>>> doFnTester =
        DoFnTester.of(new BatchViewAsMultimap.ToIsmRecordForMapLikeDoFn<Long, Long, IntervalWindow>(
            outputForSizeTag,
            outputForEntrySetTag,
            windowCoder,
            keyCoder,
            ismCoder,
            false /* unique keys */));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer,
                Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>>> inputElements =
        ImmutableList.of(
            KV.of(1, (Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>) ImmutableList.of(
                KV.of(KV.of(1L, windowA),
                    WindowedValue.of(110L, new Instant(1), windowA, PaneInfo.NO_FIRING)),
                // same window same key as to previous
                KV.of(KV.of(1L, windowA),
                    WindowedValue.of(111L, new Instant(2), windowA, PaneInfo.NO_FIRING)),
                // same window different key as to previous
                KV.of(KV.of(2L, windowA),
                    WindowedValue.of(120L, new Instant(3), windowA, PaneInfo.NO_FIRING)),
                // different window same key as to previous
                KV.of(KV.of(2L, windowB),
                    WindowedValue.of(210L, new Instant(11), windowB, PaneInfo.NO_FIRING)),
                // different window and different key as to previous
                KV.of(KV.of(3L, windowB),
                    WindowedValue.of(220L, new Instant(12), windowB, PaneInfo.NO_FIRING)))),
            KV.of(2, (Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>) ImmutableList.of(
                // different shard
                KV.of(KV.of(4L, windowC),
                    WindowedValue.of(330L, new Instant(21), windowC, PaneInfo.NO_FIRING)))));

    // The order of the output elements is important relative to processing order
    assertThat(doFnTester.processBundle(inputElements), contains(
        IsmRecord.of(
            ImmutableList.of(1L, windowA, 0L),
            WindowedValue.of(110L, new Instant(1), windowA, PaneInfo.NO_FIRING)),
        IsmRecord.of(
            ImmutableList.of(1L, windowA, 1L),
            WindowedValue.of(111L, new Instant(2), windowA, PaneInfo.NO_FIRING)),
        IsmRecord.of(
            ImmutableList.of(2L, windowA, 0L),
            WindowedValue.of(120L, new Instant(3), windowA, PaneInfo.NO_FIRING)),
        IsmRecord.of(
            ImmutableList.of(2L, windowB, 0L),
            WindowedValue.of(210L, new Instant(11), windowB, PaneInfo.NO_FIRING)),
        IsmRecord.of(
            ImmutableList.of(3L, windowB, 0L),
            WindowedValue.of(220L, new Instant(12), windowB, PaneInfo.NO_FIRING)),
        IsmRecord.of(
            ImmutableList.of(4L, windowC, 0L),
            WindowedValue.of(330L, new Instant(21), windowC, PaneInfo.NO_FIRING))));

    // Verify the number of unique keys per window.
    assertThat(doFnTester.takeSideOutputElements(outputForSizeTag), contains(
        KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowA)),
            KV.of(windowA, 2L)),
        KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowB)),
            KV.of(windowB, 2L)),
        KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowC)),
            KV.of(windowC, 1L))
        ));

    // Verify the output for the unique keys.
    assertThat(doFnTester.takeSideOutputElements(outputForEntrySetTag), contains(
        KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowA)),
            KV.of(windowA, 1L)),
        KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowA)),
            KV.of(windowA, 2L)),
        KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowB)),
            KV.of(windowB, 2L)),
        KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowB)),
            KV.of(windowB, 3L)),
        KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowC)),
            KV.of(windowC, 4L))
        ));
  }

  @Test
  public void testToIsmRecordForMapLikeDoFnWithoutUniqueKeysThrowsException() throws Exception {
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForSizeTag = new TupleTag<>();
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForEntrySetTag = new TupleTag<>();

    Coder<Long> keyCoder = VarLongCoder.of();
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    IsmRecordCoder<WindowedValue<Long>> ismCoder = IsmRecordCoder.of(
        1,
        2,
        ImmutableList.<Coder<?>>of(
            MetadataKeyCoder.of(keyCoder),
            IntervalWindow.getCoder(),
            BigEndianLongCoder.of()),
        FullWindowedValueCoder.of(VarLongCoder.of(), windowCoder));

    DoFnTester<KV<Integer, Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>>,
               IsmRecord<WindowedValue<Long>>> doFnTester =
        DoFnTester.of(new BatchViewAsMultimap.ToIsmRecordForMapLikeDoFn<Long, Long, IntervalWindow>(
            outputForSizeTag,
            outputForEntrySetTag,
            windowCoder,
            keyCoder,
            ismCoder,
            true /* unique keys */));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));

    Iterable<KV<Integer,
                Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>>> inputElements =
        ImmutableList.of(
            KV.of(1, (Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>) ImmutableList.of(
                KV.of(KV.of(1L, windowA),
                    WindowedValue.of(110L, new Instant(1), windowA, PaneInfo.NO_FIRING)),
                // same window same key as to previous
                KV.of(KV.of(1L, windowA),
                    WindowedValue.of(111L, new Instant(2), windowA, PaneInfo.NO_FIRING)))));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unique keys are expected but found key");
    doFnTester.processBundle(inputElements);
  }

  @Test
  public void testToIsmMetadataRecordForSizeDoFn() throws Exception {
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForSizeTag = new TupleTag<>();
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForEntrySetTag = new TupleTag<>();

    Coder<Long> keyCoder = VarLongCoder.of();
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    IsmRecordCoder<WindowedValue<Long>> ismCoder = IsmRecordCoder.of(
        1,
        2,
        ImmutableList.<Coder<?>>of(
            MetadataKeyCoder.of(keyCoder),
            IntervalWindow.getCoder(),
            BigEndianLongCoder.of()),
        FullWindowedValueCoder.of(VarLongCoder.of(), windowCoder));

    DoFnTester<KV<Integer, Iterable<KV<IntervalWindow, Long>>>,
               IsmRecord<WindowedValue<Long>>> doFnTester = DoFnTester.of(
        new BatchViewAsMultimap.ToIsmMetadataRecordForSizeDoFn<Long, Long, IntervalWindow>(
            windowCoder));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer, Iterable<KV<IntervalWindow, Long>>>> inputElements =
        ImmutableList.of(
            KV.of(1,
                (Iterable<KV<IntervalWindow, Long>>) ImmutableList.of(
                    KV.of(windowA, 2L),
                    KV.of(windowA, 3L),
                    KV.of(windowB, 7L))),
            KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowB)),
                (Iterable<KV<IntervalWindow, Long>>) ImmutableList.of(
                    KV.of(windowC, 9L))));

    // The order of the output elements is important relative to processing order
    assertThat(doFnTester.processBundle(inputElements), contains(
        IsmRecord.<WindowedValue<Long>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), windowA, 0L),
            CoderUtils.encodeToByteArray(VarLongCoder.of(), 5L)),
        IsmRecord.<WindowedValue<Long>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), windowB, 0L),
            CoderUtils.encodeToByteArray(VarLongCoder.of(), 7L)),
        IsmRecord.<WindowedValue<Long>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), windowC, 0L),
            CoderUtils.encodeToByteArray(VarLongCoder.of(), 9L))
        ));
  }

  @Test
  public void testToIsmMetadataRecordForKeyDoFn() throws Exception {
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForSizeTag = new TupleTag<>();
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForEntrySetTag = new TupleTag<>();

    Coder<Long> keyCoder = VarLongCoder.of();
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    IsmRecordCoder<WindowedValue<Long>> ismCoder = IsmRecordCoder.of(
        1,
        2,
        ImmutableList.<Coder<?>>of(
            MetadataKeyCoder.of(keyCoder),
            IntervalWindow.getCoder(),
            BigEndianLongCoder.of()),
        FullWindowedValueCoder.of(VarLongCoder.of(), windowCoder));

    DoFnTester<KV<Integer, Iterable<KV<IntervalWindow, Long>>>,
               IsmRecord<WindowedValue<Long>>> doFnTester = DoFnTester.of(
        new BatchViewAsMultimap.ToIsmMetadataRecordForKeyDoFn<Long, Long, IntervalWindow>(
            keyCoder, windowCoder));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer, Iterable<KV<IntervalWindow, Long>>>> inputElements =
        ImmutableList.of(
            KV.of(1,
                (Iterable<KV<IntervalWindow, Long>>) ImmutableList.of(
                    KV.of(windowA, 2L),
                    // same window as previous
                    KV.of(windowA, 3L),
                    // different window as previous
                    KV.of(windowB, 3L))),
            KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowB)),
                (Iterable<KV<IntervalWindow, Long>>) ImmutableList.of(
                    KV.of(windowC, 3L))));

    // The order of the output elements is important relative to processing order
    assertThat(doFnTester.processBundle(inputElements), contains(
        IsmRecord.<WindowedValue<Long>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), windowA, 1L),
            CoderUtils.encodeToByteArray(VarLongCoder.of(), 2L)),
        IsmRecord.<WindowedValue<Long>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), windowA, 2L),
            CoderUtils.encodeToByteArray(VarLongCoder.of(), 3L)),
        IsmRecord.<WindowedValue<Long>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), windowB, 1L),
            CoderUtils.encodeToByteArray(VarLongCoder.of(), 3L)),
        IsmRecord.<WindowedValue<Long>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), windowC, 1L),
            CoderUtils.encodeToByteArray(VarLongCoder.of(), 3L))
        ));
  }

  @Test
  public void testToMapDoFn() throws Exception {
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    DoFnTester<KV<Integer, Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>>,
                  IsmRecord<WindowedValue<TransformedMap<Long,
                                                         WindowedValue<Long>,
                                                         Long>>>> doFnTester =
        DoFnTester.of(new BatchViewAsMap.ToMapDoFn<Long, Long, IntervalWindow>(windowCoder));


    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer,
             Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>>> inputElements =
        ImmutableList.of(
            KV.of(1,
                (Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>) ImmutableList.of(
                    KV.of(windowA, WindowedValue.of(
                        KV.of(1L, 11L), new Instant(3), windowA, PaneInfo.NO_FIRING)),
                    KV.of(windowA, WindowedValue.of(
                        KV.of(2L, 21L), new Instant(7), windowA, PaneInfo.NO_FIRING)),
                    KV.of(windowB, WindowedValue.of(
                        KV.of(2L, 21L), new Instant(13), windowB, PaneInfo.NO_FIRING)),
                    KV.of(windowB, WindowedValue.of(
                        KV.of(3L, 31L), new Instant(15), windowB, PaneInfo.NO_FIRING)))),
            KV.of(2,
                (Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>) ImmutableList.of(
                    KV.of(windowC, WindowedValue.of(
                        KV.of(4L, 41L), new Instant(25), windowC, PaneInfo.NO_FIRING)))));

    // The order of the output elements is important relative to processing order
    List<IsmRecord<WindowedValue<TransformedMap<Long,
                                                WindowedValue<Long>,
                                                Long>>>> output =
                                                doFnTester.processBundle(inputElements);
    assertEquals(3, output.size());
    Map<Long, Long> outputMap;

    outputMap = output.get(0).getValue().getValue();
    assertEquals(2, outputMap.size());
    assertEquals(ImmutableMap.of(1L, 11L, 2L, 21L), outputMap);

    outputMap = output.get(1).getValue().getValue();
    assertEquals(2, outputMap.size());
    assertEquals(ImmutableMap.of(2L, 21L, 3L, 31L), outputMap);

    outputMap = output.get(2).getValue().getValue();
    assertEquals(1, outputMap.size());
    assertEquals(ImmutableMap.of(4L, 41L), outputMap);
  }

  @Test
  public void testToMultimapDoFn() throws Exception {
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    DoFnTester<KV<Integer, Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>>,
                  IsmRecord<WindowedValue<TransformedMap<Long,
                                                         Iterable<WindowedValue<Long>>,
                                                         Iterable<Long>>>>> doFnTester =
        DoFnTester.of(
            new BatchViewAsMultimap.ToMultimapDoFn<Long, Long, IntervalWindow>(windowCoder));


    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer,
             Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>>> inputElements =
        ImmutableList.of(
            KV.of(1,
                (Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>) ImmutableList.of(
                    KV.of(windowA, WindowedValue.of(
                        KV.of(1L, 11L), new Instant(3), windowA, PaneInfo.NO_FIRING)),
                    KV.of(windowA, WindowedValue.of(
                        KV.of(1L, 12L), new Instant(5), windowA, PaneInfo.NO_FIRING)),
                    KV.of(windowA, WindowedValue.of(
                        KV.of(2L, 21L), new Instant(7), windowA, PaneInfo.NO_FIRING)),
                    KV.of(windowB, WindowedValue.of(
                        KV.of(2L, 21L), new Instant(13), windowB, PaneInfo.NO_FIRING)),
                    KV.of(windowB, WindowedValue.of(
                        KV.of(3L, 31L), new Instant(15), windowB, PaneInfo.NO_FIRING)))),
            KV.of(2,
                (Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>) ImmutableList.of(
                    KV.of(windowC, WindowedValue.of(
                        KV.of(4L, 41L), new Instant(25), windowC, PaneInfo.NO_FIRING)))));

    // The order of the output elements is important relative to processing order
    List<IsmRecord<WindowedValue<TransformedMap<Long,
                                                Iterable<WindowedValue<Long>>,
                                                Iterable<Long>>>>> output =
                                                doFnTester.processBundle(inputElements);
    assertEquals(3, output.size());
    Map<Long, Iterable<Long>> outputMap;

    outputMap = output.get(0).getValue().getValue();
    assertEquals(2, outputMap.size());
    assertThat(outputMap.get(1L), containsInAnyOrder(11L, 12L));
    assertThat(outputMap.get(2L), containsInAnyOrder(21L));

    outputMap = output.get(1).getValue().getValue();
    assertEquals(2, outputMap.size());
    assertThat(outputMap.get(2L), containsInAnyOrder(21L));
    assertThat(outputMap.get(3L), containsInAnyOrder(31L));

    outputMap = output.get(2).getValue().getValue();
    assertEquals(1, outputMap.size());
    assertThat(outputMap.get(4L), containsInAnyOrder(41L));
  }
}
