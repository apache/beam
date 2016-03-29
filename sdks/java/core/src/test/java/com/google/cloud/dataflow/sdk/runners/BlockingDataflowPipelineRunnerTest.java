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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.testing.TestDataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.util.NoopPathValidator;
import com.google.cloud.dataflow.sdk.util.TestCredential;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.TimeUnit;

/**
 * Tests for BlockingDataflowPipelineRunner.
 */
@RunWith(JUnit4.class)
public class BlockingDataflowPipelineRunnerTest {

  @Rule
  public ExpectedLogs expectedLogs = ExpectedLogs.none(BlockingDataflowPipelineRunner.class);

  @Rule
  public ExpectedException expectedThrown = ExpectedException.none();

  /**
   * A {@link Matcher} for a {@link DataflowJobException} that applies an underlying {@link Matcher}
   * to the {@link DataflowPipelineJob} returned by {@link DataflowJobException#getJob()}.
   */
  private static class DataflowJobExceptionMatcher<T extends DataflowJobException>
      extends TypeSafeMatcher<T> {

    private final Matcher<DataflowPipelineJob> matcher;

    public DataflowJobExceptionMatcher(Matcher<DataflowPipelineJob> matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matchesSafely(T ex) {
      return matcher.matches(ex.getJob());
    }

    @Override
    protected void describeMismatchSafely(T item, Description description) {
        description.appendText("job ");
        matcher.describeMismatch(item.getMessage(), description);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("exception with job matching ");
      description.appendDescriptionOf(matcher);
    }

    @Factory
    public static <T extends DataflowJobException> Matcher<T> expectJob(
        Matcher<DataflowPipelineJob> matcher) {
      return new DataflowJobExceptionMatcher<T>(matcher);
    }
  }

  /**
   * A {@link Matcher} for a {@link DataflowPipelineJob} that applies an underlying {@link Matcher}
   * to the return value of {@link DataflowPipelineJob#getJobId()}.
   */
  private static class JobIdMatcher<T extends DataflowPipelineJob> extends TypeSafeMatcher<T> {

    private final Matcher<String> matcher;

    public JobIdMatcher(Matcher<String> matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matchesSafely(T job) {
      return matcher.matches(job.getJobId());
    }

    @Override
    protected void describeMismatchSafely(T item, Description description) {
        description.appendText("jobId ");
        matcher.describeMismatch(item.getJobId(), description);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("job with jobId ");
      description.appendDescriptionOf(matcher);
    }

    @Factory
    public static <T extends DataflowPipelineJob> Matcher<T> expectJobId(final String jobId) {
      return new JobIdMatcher<T>(equalTo(jobId));
    }
  }

  /**
   * A {@link Matcher} for a {@link DataflowJobUpdatedException} that applies an underlying
   * {@link Matcher} to the {@link DataflowPipelineJob} returned by
   * {@link DataflowJobUpdatedException#getReplacedByJob()}.
   */
  private static class ReplacedByJobMatcher<T extends DataflowJobUpdatedException>
      extends TypeSafeMatcher<T> {

    private final Matcher<DataflowPipelineJob> matcher;

    public ReplacedByJobMatcher(Matcher<DataflowPipelineJob> matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matchesSafely(T ex) {
      return matcher.matches(ex.getReplacedByJob());
    }

    @Override
    protected void describeMismatchSafely(T item, Description description) {
        description.appendText("job ");
        matcher.describeMismatch(item.getMessage(), description);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("exception with replacedByJob() ");
      description.appendDescriptionOf(matcher);
    }

    @Factory
    public static <T extends DataflowJobUpdatedException> Matcher<T> expectReplacedBy(
        Matcher<DataflowPipelineJob> matcher) {
      return new ReplacedByJobMatcher<T>(matcher);
    }
  }

  /**
   * Creates a mocked {@link DataflowPipelineJob} with the given {@code projectId} and {@code jobId}
   * that will immediately terminate in the provided {@code terminalState}.
   *
   * <p>The return value may be further mocked.
   */
  private DataflowPipelineJob createMockJob(
      String projectId, String jobId, State terminalState) throws Exception {
    DataflowPipelineJob mockJob = mock(DataflowPipelineJob.class);
    when(mockJob.getProjectId()).thenReturn(projectId);
    when(mockJob.getJobId()).thenReturn(jobId);
    when(mockJob.waitToFinish(
        anyLong(), isA(TimeUnit.class), isA(MonitoringUtil.JobMessagesHandler.class)))
        .thenReturn(terminalState);
    return mockJob;
  }

  /**
   * Returns a {@link BlockingDataflowPipelineRunner} that will return the provided a job to return.
   * Some {@link PipelineOptions} will be extracted from the job, such as the project ID.
   */
  private BlockingDataflowPipelineRunner createMockRunner(DataflowPipelineJob job)
      throws Exception {
    DataflowPipelineRunner mockRunner = mock(DataflowPipelineRunner.class);
    TestDataflowPipelineOptions options =
        PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setProject(job.getProjectId());

    when(mockRunner.run(isA(Pipeline.class))).thenReturn(job);

    return new BlockingDataflowPipelineRunner(mockRunner, options);
  }

  /**
   * Tests that the {@link BlockingDataflowPipelineRunner} returns normally when a job terminates in
   * the {@link State#DONE DONE} state.
   */
  @Test
  public void testJobDoneComplete() throws Exception {
    createMockRunner(createMockJob("testJobDone-projectId", "testJobDone-jobId", State.DONE))
        .run(DirectPipeline.createForTest());
    expectedLogs.verifyInfo("Job finished with status DONE");
  }

  /**
   * Tests that the {@link BlockingDataflowPipelineRunner} throws the appropriate exception
   * when a job terminates in the {@link State#FAILED FAILED} state.
   */
  @Test
  public void testFailedJobThrowsException() throws Exception {
    expectedThrown.expect(DataflowJobExecutionException.class);
    expectedThrown.expect(DataflowJobExceptionMatcher.expectJob(
        JobIdMatcher.expectJobId("testFailedJob-jobId")));
    createMockRunner(createMockJob("testFailedJob-projectId", "testFailedJob-jobId", State.FAILED))
        .run(DirectPipeline.createForTest());
  }

  /**
   * Tests that the {@link BlockingDataflowPipelineRunner} throws the appropriate exception
   * when a job terminates in the {@link State#CANCELLED CANCELLED} state.
   */
  @Test
  public void testCancelledJobThrowsException() throws Exception {
    expectedThrown.expect(DataflowJobCancelledException.class);
    expectedThrown.expect(DataflowJobExceptionMatcher.expectJob(
        JobIdMatcher.expectJobId("testCancelledJob-jobId")));
    createMockRunner(
        createMockJob("testCancelledJob-projectId", "testCancelledJob-jobId", State.CANCELLED))
        .run(DirectPipeline.createForTest());
  }

  /**
   * Tests that the {@link BlockingDataflowPipelineRunner} throws the appropriate exception
   * when a job terminates in the {@link State#UPDATED UPDATED} state.
   */
  @Test
  public void testUpdatedJobThrowsException() throws Exception {
    expectedThrown.expect(DataflowJobUpdatedException.class);
    expectedThrown.expect(DataflowJobExceptionMatcher.expectJob(
        JobIdMatcher.expectJobId("testUpdatedJob-jobId")));
    expectedThrown.expect(ReplacedByJobMatcher.expectReplacedBy(
        JobIdMatcher.expectJobId("testUpdatedJob-replacedByJobId")));
    DataflowPipelineJob job =
        createMockJob("testUpdatedJob-projectId", "testUpdatedJob-jobId", State.UPDATED);
    DataflowPipelineJob replacedByJob =
        createMockJob("testUpdatedJob-projectId", "testUpdatedJob-replacedByJobId", State.DONE);
    when(job.getReplacedByJob()).thenReturn(replacedByJob);
    createMockRunner(job).run(DirectPipeline.createForTest());
  }

  /**
   * Tests that the {@link BlockingDataflowPipelineRunner} throws the appropriate exception
   * when a job terminates in the {@link State#UNKNOWN UNKNOWN} state, indicating that the
   * Dataflow service returned a state that the SDK is unfamiliar with (possibly because it
   * is an old SDK relative the service).
   */
  @Test
  public void testUnknownJobThrowsException() throws Exception {
    expectedThrown.expect(IllegalStateException.class);
    createMockRunner(
        createMockJob("testUnknownJob-projectId", "testUnknownJob-jobId", State.UNKNOWN))
        .run(DirectPipeline.createForTest());
  }

  /**
   * Tests that the {@link BlockingDataflowPipelineRunner} throws the appropriate exception
   * when a job returns a {@code null} state, indicating that it failed to contact the service,
   * including all of its built-in resilience logic.
   */
  @Test
  public void testNullJobThrowsException() throws Exception {
    expectedThrown.expect(DataflowServiceException.class);
    expectedThrown.expect(DataflowJobExceptionMatcher.expectJob(
        JobIdMatcher.expectJobId("testNullJob-jobId")));
    createMockRunner(
        createMockJob("testNullJob-projectId", "testNullJob-jobId", null))
        .run(DirectPipeline.createForTest());
  }

  @Test
  public void testToString() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    options.setProject("test-project");
    options.setTempLocation("gs://test/temp/location");
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    assertEquals("BlockingDataflowPipelineRunner#TestJobName",
        BlockingDataflowPipelineRunner.fromOptions(options).toString());
  }
}
