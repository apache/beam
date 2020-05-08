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
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs.Messages;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMessage;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.runners.dataflow.util.TimeUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.extensions.gcp.util.FastNanoClockAndSleeper;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for DataflowPipelineJob. */
@RunWith(JUnit4.class)
public class DataflowPipelineJobTest {
  private static final String PROJECT_ID = "some-project";
  private static final String REGION_ID = "some-region-2b";
  private static final String JOB_ID = "1234";
  private static final String REPLACEMENT_JOB_ID = "4321";

  @Mock private DataflowClient mockDataflowClient;
  @Mock private Dataflow mockWorkflowClient;
  @Mock private Dataflow.Projects mockProjects;
  @Mock private Dataflow.Projects.Locations mockLocations;
  @Mock private Dataflow.Projects.Locations.Jobs mockJobs;
  @Mock private MonitoringUtil.JobMessagesHandler mockHandler;
  @Rule public FastNanoClockAndSleeper fastClock = new FastNanoClockAndSleeper();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(DataflowPipelineJob.class);

  private TestDataflowPipelineOptions options;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(mockWorkflowClient.projects()).thenReturn(mockProjects);
    when(mockProjects.locations()).thenReturn(mockLocations);
    when(mockLocations.jobs()).thenReturn(mockJobs);

    options = PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setDataflowClient(mockWorkflowClient);
    options.setProject(PROJECT_ID);
    options.setRegion(REGION_ID);
    options.setRunner(DataflowRunner.class);
    options.setTempLocation("gs://fakebucket/temp");
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setGcpCredential(new TestCredential());
  }

  /**
   * Validates that a given time is valid for the total time slept by a BackOff given the number of
   * retries and an initial polling interval.
   *
   * @param pollingInterval The initial polling interval given.
   * @param retries The number of retries made
   * @param timeSleptMillis The amount of time slept by the clock. This is checked against the valid
   *     interval.
   */
  private void checkValidInterval(Duration pollingInterval, int retries, long timeSleptMillis) {
    long highSum = 0;
    long lowSum = 0;
    for (int i = 0; i < retries; i++) {
      double currentInterval =
          pollingInterval.getMillis() * Math.pow(DataflowPipelineJob.DEFAULT_BACKOFF_EXPONENT, i);
      double randomOffset = 0.5 * currentInterval;
      highSum += Math.round(currentInterval + randomOffset);
      lowSum += Math.round(currentInterval - randomOffset);
    }
    assertThat(timeSleptMillis, allOf(greaterThanOrEqualTo(lowSum), lessThanOrEqualTo(highSum)));
  }

  @Test
  public void testWaitToFinishMessagesFail() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_" + State.DONE.name());
    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    MonitoringUtil.JobMessagesHandler jobHandler = mock(MonitoringUtil.JobMessagesHandler.class);
    Dataflow.Projects.Locations.Jobs.Messages mockMessages =
        mock(Dataflow.Projects.Locations.Jobs.Messages.class);
    Messages.List listRequest = mock(Dataflow.Projects.Locations.Jobs.Messages.List.class);
    when(mockJobs.messages()).thenReturn(mockMessages);
    when(mockMessages.list(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(listRequest);
    when(listRequest.setPageToken(eq((String) null))).thenReturn(listRequest);
    when(listRequest.execute()).thenThrow(SocketTimeoutException.class);

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, ImmutableMap.of());

    State state =
        job.waitUntilFinish(Duration.standardMinutes(5), jobHandler, fastClock, fastClock);
    assertEquals(null, state);
  }

  public State mockWaitToFinishInState(State state) throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_" + state.name());
    if (state == State.UPDATED) {
      statusResponse.setReplacedByJobId(REPLACEMENT_JOB_ID);
    }

    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, ImmutableMap.of());

    return job.waitUntilFinish(Duration.standardMinutes(1), null, fastClock, fastClock);
  }

  /**
   * Tests that the {@link DataflowPipelineJob} understands that the {@link State#DONE DONE} state
   * is terminal.
   */
  @Test
  public void testWaitToFinishDone() throws Exception {
    assertEquals(State.DONE, mockWaitToFinishInState(State.DONE));
    expectedLogs.verifyInfo(String.format("Job %s finished with status DONE.", JOB_ID));
  }

  /**
   * Tests that the {@link DataflowPipelineJob} understands that the {@link State#FAILED FAILED}
   * state is terminal.
   */
  @Test
  public void testWaitToFinishFailed() throws Exception {
    assertEquals(State.FAILED, mockWaitToFinishInState(State.FAILED));
    expectedLogs.verifyInfo(String.format("Job %s failed with status FAILED.", JOB_ID));
  }

  /**
   * Tests that the {@link DataflowPipelineJob} understands that the {@link State#CANCELLED
   * CANCELLED} state is terminal.
   */
  @Test
  public void testWaitToFinishCancelled() throws Exception {
    assertEquals(State.CANCELLED, mockWaitToFinishInState(State.CANCELLED));
    expectedLogs.verifyInfo(String.format("Job %s finished with status CANCELLED", JOB_ID));
  }

  /**
   * Tests that the {@link DataflowPipelineJob} understands that the {@link State#UPDATED UPDATED}
   * state is terminal.
   */
  @Test
  public void testWaitToFinishUpdated() throws Exception {
    assertEquals(State.UPDATED, mockWaitToFinishInState(State.UPDATED));
    expectedLogs.verifyInfo(
        String.format(
            "Job %s has been updated and is running as the new job with id %s.",
            JOB_ID, REPLACEMENT_JOB_ID));
  }

  /**
   * Tests that the {@link DataflowPipelineJob} understands that the {@link State#UPDATED UPDATED}
   * state is terminal.
   */
  @Test
  public void testWaitToFinishLogsError() throws Exception {
    assertEquals(State.UPDATED, mockWaitToFinishInState(State.UPDATED));
    expectedLogs.verifyInfo(
        String.format(
            "Job %s has been updated and is running as the new job with id %s.",
            JOB_ID, REPLACEMENT_JOB_ID));
  }

  /**
   * Tests that the {@link DataflowPipelineJob} understands that the {@link State#UNKNOWN UNKNOWN}
   * state is terminal.
   */
  @Test
  public void testWaitToFinishUnknown() throws Exception {
    assertEquals(null, mockWaitToFinishInState(State.UNKNOWN));
    expectedLogs.verifyWarn(
        "No terminal state was returned within allotted timeout. State value UNKNOWN");
  }

  @Test
  public void testWaitToFinishFail() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenThrow(IOException.class);

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, ImmutableMap.of());

    long startTime = fastClock.nanoTime();
    State state = job.waitUntilFinish(Duration.standardMinutes(5), null, fastClock, fastClock);
    assertEquals(null, state);
    long timeDiff = TimeUnit.NANOSECONDS.toMillis(fastClock.nanoTime() - startTime);
    checkValidInterval(
        DataflowPipelineJob.MESSAGES_POLLING_INTERVAL,
        DataflowPipelineJob.MESSAGES_POLLING_RETRIES,
        timeDiff);
  }

  @Test
  public void testWaitToFinishTimeFail() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenThrow(IOException.class);

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, ImmutableMap.of());
    long startTime = fastClock.nanoTime();
    State state = job.waitUntilFinish(Duration.millis(4), null, fastClock, fastClock);
    assertEquals(null, state);
    long timeDiff = TimeUnit.NANOSECONDS.toMillis(fastClock.nanoTime() - startTime);
    // Should only have slept for the 4 ms allowed.
    assertEquals(4L, timeDiff);
  }

  @Test
  public void testCumulativeTimeOverflow() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_RUNNING");
    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    FastNanoClockAndFuzzySleeper clock = new FastNanoClockAndFuzzySleeper();

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, ImmutableMap.of());
    long startTime = clock.nanoTime();
    State state = job.waitUntilFinish(Duration.millis(4), null, clock, clock);
    assertEquals(null, state);
    long timeDiff = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - startTime);
    // Should only have slept for the 4 ms allowed.
    assertThat(timeDiff, lessThanOrEqualTo(4L));
  }

  @Test
  public void testGetStateReturnsServiceState() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_" + State.RUNNING.name());

    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, ImmutableMap.of());

    assertEquals(
        State.RUNNING,
        job.getStateWithRetriesOrUnknownOnException(
            BackOffAdapter.toGcpBackOff(DataflowPipelineJob.STATUS_BACKOFF_FACTORY.backoff()),
            fastClock));
  }

  @Test
  public void testGetStateWithRetriesPassesExceptionThrough() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenThrow(IOException.class);

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, ImmutableMap.of());

    long startTime = fastClock.nanoTime();
    thrown.expect(IOException.class);
    job.getStateWithRetries(
        BackOffAdapter.toGcpBackOff(DataflowPipelineJob.STATUS_BACKOFF_FACTORY.backoff()),
        fastClock);
  }

  @Test
  public void testGetStateNoThrowWithExceptionReturnsUnknown() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenThrow(IOException.class);

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, ImmutableMap.of());

    long startTime = fastClock.nanoTime();
    assertEquals(
        State.UNKNOWN,
        job.getStateWithRetriesOrUnknownOnException(
            BackOffAdapter.toGcpBackOff(DataflowPipelineJob.STATUS_BACKOFF_FACTORY.backoff()),
            fastClock));
    long timeDiff = TimeUnit.NANOSECONDS.toMillis(fastClock.nanoTime() - startTime);
    checkValidInterval(
        DataflowPipelineJob.STATUS_POLLING_INTERVAL,
        DataflowPipelineJob.STATUS_POLLING_RETRIES,
        timeDiff);
  }

  private AppliedPTransform<?, ?, ?> appliedPTransform(
      String fullName, PTransform<PInput, POutput> transform, Pipeline p) {
    PInput input = mock(PInput.class);
    when(input.getPipeline()).thenReturn(p);
    return AppliedPTransform.of(
        fullName, Collections.emptyMap(), Collections.emptyMap(), transform, p);
  }

  private static class FastNanoClockAndFuzzySleeper implements NanoClock, Sleeper {

    private long fastNanoTime;

    public FastNanoClockAndFuzzySleeper() {
      fastNanoTime = NanoClock.SYSTEM.nanoTime();
    }

    @Override
    public long nanoTime() {
      return fastNanoTime;
    }

    @Override
    public void sleep(long millis) throws InterruptedException {
      fastNanoTime += millis * 1000000L + ThreadLocalRandom.current().nextInt(500000);
    }
  }

  @Test
  public void testCancelUnterminatedJobThatSucceeds() throws IOException {
    Dataflow.Projects.Locations.Jobs.Update update =
        mock(Dataflow.Projects.Locations.Jobs.Update.class);
    when(mockJobs.update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), any(Job.class)))
        .thenReturn(update);
    when(update.execute()).thenReturn(new Job().setCurrentState("JOB_STATE_CANCELLED"));

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, null);

    assertEquals(State.CANCELLED, job.cancel());
    Job content = new Job();
    content.setProjectId(PROJECT_ID);
    content.setId(JOB_ID);
    content.setRequestedState("JOB_STATE_CANCELLED");
    verify(mockJobs).update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), eq(content));
    verifyNoMoreInteractions(mockJobs);
  }

  @Test
  public void testCancelUnterminatedJobThatFails() throws IOException {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_RUNNING");
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    Dataflow.Projects.Locations.Jobs.Update update =
        mock(Dataflow.Projects.Locations.Jobs.Update.class);
    when(mockJobs.update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), any(Job.class)))
        .thenReturn(update);
    when(update.execute()).thenThrow(new IOException("Some random IOException"));

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, null);

    thrown.expect(IOException.class);
    thrown.expectMessage(
        "Failed to cancel job in state RUNNING, "
            + "please go to the Developers Console to cancel it manually:");
    job.cancel();
  }

  /**
   * Test that {@link DataflowPipelineJob#cancel} doesn't throw if the Dataflow service returns
   * non-terminal state even though the cancel API call failed, which can happen in practice.
   *
   * <p>TODO: delete this code if the API calls become consistent.
   */
  @Test
  public void testCancelTerminatedJobWithStaleState() throws IOException {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_RUNNING");
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    Dataflow.Projects.Locations.Jobs.Update update =
        mock(Dataflow.Projects.Locations.Jobs.Update.class);
    when(mockJobs.update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), any(Job.class)))
        .thenReturn(update);
    when(update.execute()).thenThrow(new IOException("Job has terminated in state SUCCESS"));

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, null);
    State returned = job.cancel();
    assertThat(returned, equalTo(State.RUNNING));
    expectedLogs.verifyWarn("Cancel failed because job is already terminated.");
  }

  @Test
  public void testCancelTerminatedJob() throws IOException {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_FAILED");
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    Dataflow.Projects.Locations.Jobs.Update update =
        mock(Dataflow.Projects.Locations.Jobs.Update.class);
    when(mockJobs.update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), any(Job.class)))
        .thenReturn(update);
    when(update.execute()).thenThrow(new IOException());

    DataflowPipelineJob job =
        new DataflowPipelineJob(DataflowClient.create(options), JOB_ID, options, null);

    assertEquals(State.FAILED, job.cancel());
    Job content = new Job();
    content.setProjectId(PROJECT_ID);
    content.setId(JOB_ID);
    content.setRequestedState("JOB_STATE_CANCELLED");
    verify(mockJobs).update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), eq(content));
    verify(mockJobs).get(PROJECT_ID, REGION_ID, JOB_ID);
    verifyNoMoreInteractions(mockJobs);
  }

  /** Tests that a {@link DataflowPipelineJob} does not duplicate messages. */
  @Test
  public void testWaitUntilFinishNoRepeatedLogs() throws Exception {
    DataflowPipelineJob job = new DataflowPipelineJob(mockDataflowClient, JOB_ID, options, null);
    Sleeper sleeper = new ZeroSleeper();
    NanoClock nanoClock = mock(NanoClock.class);

    Instant separatingTimestamp = new Instant(42L);
    JobMessage theMessage = infoMessage(separatingTimestamp, "nothing");

    MonitoringUtil mockMonitor = mock(MonitoringUtil.class);
    when(mockMonitor.getJobMessages(anyString(), anyLong()))
        .thenReturn(ImmutableList.of(theMessage));

    // The Job just always reports "running" across all calls
    Job fakeJob = new Job();
    fakeJob.setCurrentState("JOB_STATE_RUNNING");
    when(mockDataflowClient.getJob(anyString())).thenReturn(fakeJob);

    // After waitUntilFinish the DataflowPipelineJob should record the latest message timestamp
    when(nanoClock.nanoTime()).thenReturn(0L).thenReturn(2000000000L);
    job.waitUntilFinish(Duration.standardSeconds(1), mockHandler, sleeper, nanoClock, mockMonitor);
    verify(mockHandler).process(ImmutableList.of(theMessage));

    // Second waitUntilFinish should request jobs with `separatingTimestamp` so the monitor
    // will only return new messages
    when(nanoClock.nanoTime()).thenReturn(3000000000L).thenReturn(6000000000L);
    job.waitUntilFinish(Duration.standardSeconds(1), mockHandler, sleeper, nanoClock, mockMonitor);
    verify(mockMonitor).getJobMessages(anyString(), eq(separatingTimestamp.getMillis()));
  }

  private static JobMessage infoMessage(Instant timestamp, String text) {
    JobMessage message = new JobMessage();
    message.setTime(TimeUtil.toCloudTime(timestamp));
    message.setMessageText(text);
    return message;
  }

  private class FakeMonitor extends MonitoringUtil {
    // Messages in timestamp order
    private final NavigableMap<Long, JobMessage> timestampedMessages;

    public FakeMonitor(JobMessage... messages) {
      // The client should never be used; this Fake is intended to intercept relevant methods
      super(mockDataflowClient);

      NavigableMap<Long, JobMessage> timestampedMessages = Maps.newTreeMap();
      for (JobMessage message : messages) {
        timestampedMessages.put(Long.parseLong(message.getTime()), message);
      }

      this.timestampedMessages = timestampedMessages;
    }

    @Override
    public List<JobMessage> getJobMessages(String jobId, long startTimestampMs) {
      return ImmutableList.copyOf(timestampedMessages.headMap(startTimestampMs).values());
    }
  }

  private static class ZeroSleeper implements Sleeper {
    @Override
    public void sleep(long l) throws InterruptedException {}
  }
}
