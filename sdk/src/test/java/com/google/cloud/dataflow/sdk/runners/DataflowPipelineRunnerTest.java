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

package com.google.cloud.dataflow.sdk.runners;

import static org.hamcrest.Matchers.containsString;
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
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.Pipeline.PipelineVisitor;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.DataflowReleaseInfo;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.PackageUtil;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests for DataflowPipelineRunner.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class DataflowPipelineRunnerTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  // Asserts that the given Job has all expected fields set.
  private static void assertValidJob(Job job) {
    assertNull(job.getId());
    assertNull(job.getCurrentState());
  }

  private DataflowPipeline buildDataflowPipeline(DataflowPipelineOptions options) {
    options.setStableUniqueNames(CheckEnabled.ERROR);
    DataflowPipeline p = DataflowPipeline.create(options);

    p.apply(TextIO.Read.named("ReadMyFile").from("gs://bucket/object"))
        .apply(TextIO.Write.named("WriteMyFile").to("gs://bucket/object"));

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
    when(mockJobs.create(eq("someProject"), jobCaptor.capture()))
        .thenReturn(mockRequest);
    when(mockJobs.list(eq("someProject"))).thenReturn(mockList);
    when(mockList.setPageToken(anyString())).thenReturn(mockList);
    when(mockList.execute())
        .thenReturn(new ListJobsResponse().setJobs(
            Arrays.asList(new Job()
                              .setName("oldJobName")
                              .setId("oldJobId")
                              .setCurrentState("JOB_STATE_RUNNING"))));

    Job resultJob = new Job();
    resultJob.setId("newid");
    when(mockRequest.execute()).thenReturn(resultJob);
    return mockDataflowClient;
  }

  private GcsUtil buildMockGcsUtil() throws IOException {
    GcsUtil mockGcsUtil = mock(GcsUtil.class);
    when(mockGcsUtil.create(
        any(GcsPath.class), anyString()))
        .thenReturn(FileChannel.open(
            Files.createTempFile("channel-", ".tmp"),
            StandardOpenOption.CREATE, StandardOpenOption.DELETE_ON_CLOSE));
    when(mockGcsUtil.isGcsPatternSupported(anyString())).thenReturn(true);
    return mockGcsUtil;
  }

  private DataflowPipelineOptions buildPipelineOptions(
      ArgumentCaptor<Job> jobCaptor) throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject("someProject");
    options.setTempLocation("gs://somebucket/some/path");
    // Set FILES_PROPERTY to empty to prevent a default value calculated from classpath.
    options.setFilesToStage(new LinkedList<String>());
    options.setDataflowClient(buildMockDataflow(jobCaptor));
    options.setGcsUtil(buildMockGcsUtil());
    options.setGcpCredential(new TestCredential());
    return options;
  }

  @Test
  public void testRun() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    DataflowPipeline p = buildDataflowPipeline(options);
    DataflowPipelineJob job = p.run();
    assertEquals("newid", job.getJobId());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testRunReturnDifferentRequestId() throws IOException {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(Matchers.containsString(
        "If you want to submit a new job in parallel, try again with a different name."));

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    Dataflow mockDataflowClient = options.getDataflowClient();
    Dataflow.Projects.Jobs.Create mockRequest = mock(Dataflow.Projects.Jobs.Create.class);
    when(mockDataflowClient.projects().jobs().create(eq("someProject"), jobCaptor.capture()))
        .thenReturn(mockRequest);
    Job resultJob = new Job();
    resultJob.setId("newid");
    // Return a different request id.
    resultJob.setClientRequestId("different_request_id");
    when(mockRequest.execute()).thenReturn(resultJob);

    DataflowPipeline p = buildDataflowPipeline(options);
    p.run();
  }

  @Test
  public void testReload() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    options.setReload(true);
    options.setJobName("oldJobName");
    DataflowPipeline p = buildDataflowPipeline(options);
    DataflowPipelineJob job = p.run();
    assertEquals("newid", job.getJobId());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testReloadNonExistentPipeline() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Could not find running job named badJobName");

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    options.setReload(true);
    options.setJobName("badJobName");
    DataflowPipeline p = buildDataflowPipeline(options);
    p.run();
  }

  @Test
  public void testRunWithFiles() throws IOException {
    // Test that the function DataflowPipelineRunner.stageFiles works as
    // expected.
    GcsUtil mockGcsUtil = buildMockGcsUtil();
    final String gcsStaging = "gs://somebucket/some/path";
    final String gcsTemp = "gs://somebucket/some/temp/path";
    final String cloudDataflowDataset = "somedataset";

    // Create some temporary files.
    File temp1 = File.createTempFile("DataflowPipelineRunnerTest", "txt");
    temp1.deleteOnExit();
    File temp2 = File.createTempFile("DataflowPipelineRunnerTest2", "txt");
    temp2.deleteOnExit();

    DataflowPackage expectedPackage1 = PackageUtil.createPackage(
        temp1, gcsStaging, null);

    String overridePackageName = "alias.txt";
    DataflowPackage expectedPackage2 = PackageUtil.createPackage(
        temp2, gcsStaging, overridePackageName);

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setFilesToStage(ImmutableList.of(
        temp1.getAbsolutePath(),
        overridePackageName + "=" + temp2.getAbsolutePath()));
    options.setStagingLocation(gcsStaging);
    options.setTempLocation(gcsTemp);
    options.setTempDatasetId(cloudDataflowDataset);
    options.setProject("someProject");
    options.setJobName("job");
    options.setDataflowClient(buildMockDataflow(jobCaptor));
    options.setGcsUtil(mockGcsUtil);
    options.setGcpCredential(new TestCredential());

    DataflowPipeline p = buildDataflowPipeline(options);

    DataflowPipelineJob job = p.run();
    assertEquals("newid", job.getJobId());

    Job workflowJob = jobCaptor.getValue();
    assertValidJob(workflowJob);

    assertEquals(
        2,
        workflowJob.getEnvironment().getWorkerPools().get(0).getPackages().size());
    DataflowPackage workflowPackage1 =
        workflowJob.getEnvironment().getWorkerPools().get(0).getPackages().get(0);
    assertEquals(expectedPackage1.getName(), workflowPackage1.getName());
    assertEquals(expectedPackage1.getLocation(), workflowPackage1.getLocation());
    DataflowPackage workflowPackage2 =
        workflowJob.getEnvironment().getWorkerPools().get(0).getPackages().get(1);
    assertEquals(expectedPackage2.getName(), workflowPackage2.getName());
    assertEquals(expectedPackage2.getLocation(), workflowPackage2.getLocation());

    assertEquals(
        "storage.googleapis.com/somebucket/some/temp/path",
        workflowJob.getEnvironment().getTempStoragePrefix());
    assertEquals(
        cloudDataflowDataset,
        workflowJob.getEnvironment().getDataset());
    assertEquals(
        DataflowReleaseInfo.getReleaseInfo().getName(),
        workflowJob.getEnvironment().getUserAgent().get("name"));
    assertEquals(
        DataflowReleaseInfo.getReleaseInfo().getVersion(),
        workflowJob.getEnvironment().getUserAgent().get("version"));
  }

  @Test
  public void runWithDefaultFilesToStage() throws Exception {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    options.setFilesToStage(null);
    DataflowPipelineRunner.fromOptions(options);
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
        DataflowPipelineRunner.detectClassPathResourcesToStage(classLoader));
  }

  @Test
  public void detectClassPathResourcesWithUnsupportedClassLoader() {
    ClassLoader mockClassLoader = Mockito.mock(ClassLoader.class);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unable to use ClassLoader to detect classpath elements.");

    DataflowPipelineRunner.detectClassPathResourcesToStage(mockClassLoader);
  }

  @Test
  public void detectClassPathResourceWithNonFileResources() throws Exception {
    String url = "http://www.google.com/all-the-secrets.jar";
    URLClassLoader classLoader = new URLClassLoader(new URL[]{
        new URL(url)
    });
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unable to convert url (" + url + ") to file.");

    DataflowPipelineRunner.detectClassPathResourcesToStage(classLoader);
  }

  @Test
  public void testGcsStagingLocationInitialization() {
    // Test that the staging location is initialized correctly.
    String gcsTemp = "gs://somebucket/some/temp/path";

    // Set temp location (required), and check that staging location is set.
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setTempLocation(gcsTemp);
    options.setProject("testProject");
    options.setGcpCredential(new TestCredential());
    DataflowPipelineRunner.fromOptions(options);

    assertNotNull(options.getStagingLocation());
  }

  @Test
  public void testGcsRequiredTempLocation() {
    // Error raised if temp location not set.
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject("someProject");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("tempLocation"));
    DataflowPipelineRunner.fromOptions(options);
  }

  @Test
  public void testNonGcsFilePathInReadFailure() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    Pipeline p = buildDataflowPipeline(buildPipelineOptions(jobCaptor));
    p.apply(TextIO.Read.named("ReadMyNonGcsFile").from("/tmp/file"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("expected a valid 'gs://' path but was given"));
    p.run();
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testNonGcsFilePathInWriteFailure() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    Pipeline p = buildDataflowPipeline(buildPipelineOptions(jobCaptor));
    p.apply(TextIO.Read.named("ReadMyGcsFile").from("gs://bucket/object"))
        .apply(TextIO.Write.named("WriteMyNonGcsFile").to("/tmp/file"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("expected a valid 'gs://' path but was given"));
    p.run();
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testMultiSlashGcsFileReadPath() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    Pipeline p = buildDataflowPipeline(buildPipelineOptions(jobCaptor));
    p.apply(TextIO.Read.named("ReadInvalidGcsFile")
        .from("gs://bucket/tmp//file"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("consecutive slashes");
    p.run();
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testMultiSlashGcsFileWritePath() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    Pipeline p = buildDataflowPipeline(buildPipelineOptions(jobCaptor));
    p.apply(TextIO.Read.named("ReadMyGcsFile").from("gs://bucket/object"))
        .apply(TextIO.Write.named("WriteInvalidGcsFile")
            .to("gs://bucket/tmp//file"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("consecutive slashes");
    p.run();
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testInvalidTempLocation() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    options.setTempLocation("file://temp/location");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("expected a valid 'gs://' path but was given"));
    DataflowPipelineRunner.fromOptions(options);
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testInvalidStagingLocation() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    options.setStagingLocation("file://my/staging/location");
    try {
      DataflowPipelineRunner.fromOptions(options);
      fail("fromOptions should have failed");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("expected a valid 'gs://' path but was given"));
    }
    options.setStagingLocation("my/staging/location");
    try {
      DataflowPipelineRunner.fromOptions(options);
      fail("fromOptions should have failed");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("expected a valid 'gs://' path but was given"));
    }
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
      ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

      DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
      options.setJobName(invalidNames.get(i));

      try {
        DataflowPipelineRunner.fromOptions(options);
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
      ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

      DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
      options.setJobName(name);

      DataflowPipelineRunner runner = DataflowPipelineRunner
          .fromOptions(options);
      assertNotNull(runner);
    }
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
    Pipeline p = DataflowPipeline.create(options);

    p.apply(Create.of(Arrays.asList(1, 2, 3)))
     .apply(new TestTransform());

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(Matchers.containsString("no translator registered"));
    DataflowPipelineTranslator.fromOptions(options)
        .translate(p, Collections.<DataflowPackage>emptyList());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testTransformTranslator() throws IOException {
    // Test that we can provide a custom translation
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    DataflowPipeline p = DataflowPipeline.create(options);
    TestTransform transform = new TestTransform();

    p.apply(Create.of(Arrays.asList(1, 2, 3)).withCoder(BigEndianIntegerCoder.of()))
        .apply(transform);

    DataflowPipelineTranslator translator = DataflowPipelineRunner
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

    translator.translate(p, Collections.<DataflowPackage>emptyList());
    assertTrue(transform.translated);
  }

  /** Records all the composite transforms visited within the Pipeline. */
  private static class CompositeTransformRecorder implements PipelineVisitor {
    private List<PTransform<?, ?>> transforms = new ArrayList<>();

    @Override
    public void enterCompositeTransform(TransformTreeNode node) {
      if (node.getTransform() != null) {
        transforms.add(node.getTransform());
      }
    }

    @Override
    public void leaveCompositeTransform(TransformTreeNode node) {
    }

    @Override
    public void visitTransform(TransformTreeNode node) {
    }

    @Override
    public void visitValue(PValue value, TransformTreeNode producer) {
    }

    public List<PTransform<?, ?>> getCompositeTransforms() {
      return transforms;
    }
  }

  @Test
  public void testApplyIsScopedToExactClass() throws IOException {
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    DataflowPipelineOptions options = buildPipelineOptions(jobCaptor);
    DataflowPipeline p = DataflowPipeline.create(options);

    Create.TimestampedValues<String> transform =
        Create.timestamped(Arrays.asList(TimestampedValue.of("TestString", Instant.now())));
    p.apply(transform);

    CompositeTransformRecorder recorder = new CompositeTransformRecorder();
    p.traverseTopologically(recorder);

    assertThat("Expected to have seen CreateTimestamped composite transform.",
        recorder.getCompositeTransforms(),
        Matchers.<PTransform<?, ?>>contains(transform));
  }

  @Test
  public void testToString() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    options.setProject("TestProject");
    options.setTempLocation("gs://test/temp/location");
    options.setGcpCredential(new TestCredential());
    assertEquals("DataflowPipelineRunner#TestJobName",
        DataflowPipelineRunner.fromOptions(options).toString());
  }
}
